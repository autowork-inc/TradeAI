"""1m-свечи публичного Binance Futures (без ключей) с дисковым кэшем по суткам + симуляция по пути цены.

Зачем: MFE из БД отвечает «дошла ли цена до X ДО фактического выхода», но не умеет менять сам стоп и не видит, что было ПОСЛЕ стопа.
По 1m-пути можно: (1) проверить записанный MFE (и досчитать простой у восстановленных сделок), (2) пересчитать сделку с другим стопом
при том же риске $5, (3) увидеть «выбило и ушло» - цена после стопа вернулась ко входу и дошла до +1R/+2R.
Допущения (пессимистичные, как в бэктесте проекта): свеча, задевшая и стоп, и тейк, считается стопом; минута входа не используется
(её экстремумы могли быть до исполнения); тайм-аут - 48 ч (192 свечи M15) по close последней свечи. Только крипта (символы *USDT).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from .data import LabError, Trade

URL = "https://fapi.binance.com/fapi/v1/klines"
MIN_MS = 60_000
DAY_MS = 86_400_000
HORIZON_MS = 48 * 3_600_000
HUNT_MS = 6 * 3_600_000
OVERSHOOT_MS = 30 * MIN_MS
Bar = tuple[int, float, float, float]  # open_ms, high, low, close


class Unavailable(Exception):
    """Символа нет на Binance Futures (MT5 и т.п.) - сделка пропускается, это не ошибка."""


class KlineStore:
    def __init__(self, cache_dir: Path, now_ms: int | None = None):
        self.dir = cache_dir
        self.now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        self._mem: dict[tuple[str, int], list[Bar]] = {}
        self.requests = 0

    def _fetch(self, symbol: str, start: int, end: int) -> list[Bar]:
        out: list[Bar] = []
        cur = start
        while cur < end:
            url = f"{URL}?symbol={symbol}&interval=1m&startTime={cur}&endTime={end - 1}&limit=1500"
            for attempt in range(3):
                try:
                    self.requests += 1
                    with urllib.request.urlopen(url, timeout=20) as resp:
                        rows = json.load(resp)
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code == 400:
                        raise Unavailable(symbol) from None
                    if exc.code in (418, 429):
                        raise LabError(f"Binance ограничил запросы ({exc.code}): подождите и запустите снова (кэш сохранится)") from None
                    if attempt == 2:
                        raise LabError(f"Binance klines {symbol}: HTTP {exc.code}") from None
                except (urllib.error.URLError, TimeoutError) as exc:
                    if attempt == 2:
                        raise LabError(f"Binance klines {symbol}: нет связи ({exc})") from None
                time.sleep(1.5 * (attempt + 1))
            if not rows:
                break
            out += [(int(r[0]), float(r[2]), float(r[3]), float(r[4])) for r in rows]
            cur = int(rows[-1][0]) + MIN_MS
            time.sleep(0.2)
        return out

    def day(self, symbol: str, day_start: int) -> list[Bar]:
        key = (symbol, day_start)
        if key in self._mem:
            return self._mem[key]
        complete = day_start + DAY_MS <= self.now_ms - MIN_MS
        path = self.dir / symbol / f"{datetime.fromtimestamp(day_start / 1000, tz=timezone.utc):%Y-%m-%d}.json"
        if complete and path.is_file():
            bars = [tuple(b) for b in json.loads(path.read_text(encoding="utf-8"))]
        else:
            bars = self._fetch(symbol, day_start, min(day_start + DAY_MS, self.now_ms))
            if complete:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(bars), encoding="utf-8")
        self._mem[key] = bars  # type: ignore[assignment]
        return self._mem[key]

    def range(self, symbol: str, start: int, end: int) -> list[Bar]:
        if not symbol.endswith("USDT"):
            raise Unavailable(symbol)
        end = min(end, self.now_ms)
        out: list[Bar] = []
        d = start - start % DAY_MS
        while d < end:
            out += [b for b in self.day(symbol, d) if start <= b[0] < end]
            d += DAY_MS
        return out


# ------------------------------------------------------------------------------------------- симуляция

def after_entry(t: Trade, bars: list[Bar]) -> list[Bar]:
    assert t.entry_ms is not None
    first = t.entry_ms - t.entry_ms % MIN_MS + MIN_MS
    return [b for b in bars if first <= b[0] < t.entry_ms + HORIZON_MS]


def path_sim(t: Trade, bars: list[Bar], sl: float, x: float, now_ms: int) -> float | None:
    """Чистый R (в единицах НОВОГО стопа) сделки с тем же входом, стопом `sl` и тейком x R. None - путь ещё не закончился."""
    assert t.entry is not None
    d = abs(t.entry - sl)
    if not d > 0 or (t.dir * (t.entry - sl)) <= 0:
        return None
    tp_price = t.entry + t.dir * x * d
    exit_price = None
    for _, hi, lo, _c in after_entry(t, bars):
        stop_hit = lo <= sl if t.dir == 1 else hi >= sl
        if stop_hit:
            exit_price = sl
            break
        if (hi >= tp_price) if t.dir == 1 else (lo <= tp_price):
            exit_price = tp_price
            break
    if exit_price is None:
        assert t.entry_ms is not None
        if t.entry_ms + HORIZON_MS > now_ms:
            return None
        path = after_entry(t, bars)
        if not path:
            return None
        exit_price = path[-1][3]
    gross = t.dir * (exit_price - t.entry) / d
    return gross - (t.fee_in * t.entry + t.fee_out * exit_price) / d


def kline_mfe(t: Trade, bars: list[Bar]) -> float | None:
    """MFE по 1m от минуты после входа до минуты выхода (не включая её) - нижняя граница; для сверки с MFE по тикам."""
    assert t.entry is not None and t.risk
    if t.exit_ms is None:
        return None
    best = 0.0
    for open_ms, hi, lo, _c in after_entry(t, bars):
        if open_ms + MIN_MS > t.exit_ms:
            break
        best = max(best, t.r_at(hi if t.dir == 1 else lo))
    return best


def stop_hunt(t: Trade, bars: list[Bar], atr: float | None) -> dict[str, float | bool | None]:
    """После выхода по стопу: вернулась ли цена ко входу и дошла ли до +1R/+2R за 6 ч; насколько тень зашла за SL за 30 мин (в ATR)."""
    assert t.exit_ms is not None and t.entry is not None and t.risk
    after = [b for b in bars if t.exit_ms <= b[0] < t.exit_ms + HUNT_MS]
    best = max((t.r_at(hi if t.dir == 1 else lo) for _, hi, lo, _c in after), default=None)
    near = [b for b in after if b[0] < t.exit_ms + OVERSHOOT_MS]
    worst = max((t.dir * (t.sl - (lo if t.dir == 1 else hi)) for _, hi, lo, _c in near), default=None)
    return {"back_to_entry": best is not None and best >= 0, "reach1": best is not None and best >= 1, "reach2": best is not None and best >= 2,
            "best_after_r": best, "overshoot_atr": None if worst is None or not atr else max(worst, 0.0) / atr}
