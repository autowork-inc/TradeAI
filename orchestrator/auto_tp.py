"""Авто-подстройка финального тейка по каждой стратегии (2026-10-24, задание пользователя): бот сам выбирает множитель TP по истории сделок.

План выхода всех R-стратегий - Scale Out: 50% объёма закрывается на +1R, остальные 50% - на финальном тейке X R (`scale_plan(X)`).
X подбирается так: берутся ВСЕ теневые сделки стратегии с фактом входа с `AUTO_TP_SINCE` (по умолчанию 2026-10-22 - после переписывания логики
стратегий), каждая доигрывается по 1m-свечам публичного Binance от минуты после входа (горизонт 48 ч - тайм-аут бота): одна и та же сделка
проверяется со всеми X. Внутри свечи стоп раньше тейка (пессимистично, как в бэктесте). Сетка X - от 1R с шагом 0.25 до САМОГО дальнего хода
цены в нашу сторону, который реально был у сделок стратегии: искусственного потолка нет (1:10 будет выбран, если 1:10 дал бы больше всего).
Лучший X - по сумме чистого R (риск фиксированный $5, значит и по сумме $); при равенстве - меньший X.
Меньше `AUTO_TP_MIN_TRADES` завершённых сделок у стратегии - X не меняется (последний валидный из `DATA_DIR/.auto_tp.json`, иначе план по умолчанию).
Пересчёт - при старте и раз в сутки; новый X действует на НОВЫЕ сделки (у открытых - свой план, записанный при входе), одинаково в Shadow и live (R8).
Сбой сети/Binance при пересчёте - предупреждение и прежние X (подстройка - не торговля); ошибка кода - падение процесса (R1, `check_task`).
Только крипта (минутные свечи Binance); сделки Forex/металлов в расчёт не идут. VWAP_Reversion (тейк по линии VWAP, tp_anchor='price') не подстраивается.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from orchestrator.tracker import ScaleOutPlan
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import HttpServerError, NetworkError, RateLimitError

log = logging.getLogger("orchestrator.auto_tp")

MIN_MS = 60_000
HORIZON_MS = 48 * 3_600_000
FIRST_TAKE_R = 1.0  # «сейф»: 50% на +1R
FIRST_TAKE_FRAC = 0.5
STEP_R = 0.25
MAKER, TAKER = 0.0002, 0.0005
DAY_S = 86_400

SQL = """
SELECT t.id, COALESCE(t.strategy_id, t.strategy_name) AS strategy, t.coin, t.side, t.kind, t.entry_price, t.entry_time, t.sl
  FROM trades t
 WHERE t.mode = 'shadow' AND t.entry_price IS NOT NULL AND t.entry_time IS NOT NULL AND t.entry_time >= $1
 ORDER BY t.entry_time
"""

Bar = tuple[int, float, float, float]  # open_ms, high, low, close


def scale_plan(final_r: float) -> ScaleOutPlan:
    """Scale Out: 50% на +1R, остаток на final_r. Финал не дальше 1R - один тейк на весь объём (частичной фиксации «до» тейка нет)."""
    if final_r <= FIRST_TAKE_R:
        return ScaleOutPlan(((final_r, 1.0),))
    return ScaleOutPlan(((FIRST_TAKE_R, FIRST_TAKE_FRAC), (final_r, 1.0 - FIRST_TAKE_FRAC)))


@dataclass(frozen=True)
class Path_:
    """Путь цены одной сделки в R от входа: максимум хода в нашу сторону до каждой свечи (до стопа), индекс стопа, итог по close на горизонте."""
    fav: tuple[float, ...]  # нарастающий максимум хода в нашу сторону (R), свеча за свечой, до свечи стопа (не включая её)
    stopped: bool
    end_r: float | None  # не задет стоп за горизонт: где цена была на close последней свечи (R); None - стоп задет
    entry: float
    risk: float
    sgn: int  # +1 лонг, -1 шорт: прибыль = sgn * (цена - вход)
    fee_in: float


def build_path(entry: float, sl: float, side: str, kind: str, entry_ms: int, bars: Sequence[Bar], now_ms: int) -> Path_ | None:
    """None - путь ещё не закончился (стоп не задет и 48 ч не прошли) или свечей нет: такая сделка в подстройке не участвует."""
    sgn = 1 if side == "long" else -1
    risk = abs(entry - sl)
    if not risk > 0 or sgn * (entry - sl) <= 0:
        return None
    first = entry_ms - entry_ms % MIN_MS + MIN_MS
    path = [b for b in bars if first <= b[0] < entry_ms + HORIZON_MS]
    if not path:
        return None
    fav: list[float] = []
    best = 0.0
    for _, hi, lo, _c in path:
        if (lo <= sl) if sgn == 1 else (hi >= sl):
            return Path_(tuple(fav), True, None, entry, risk, sgn, MAKER if kind == "limit" else TAKER)
        best = max(best, sgn * ((hi if sgn == 1 else lo) - entry) / risk)
        fav.append(best)
    if entry_ms + HORIZON_MS > now_ms:
        return None
    return Path_(tuple(fav), False, sgn * (path[-1][3] - entry) / risk, entry, risk, sgn, MAKER if kind == "limit" else TAKER)


def outcome(p: Path_, final_r: float) -> float:
    """Чистый R сделки по плану scale_plan(final_r) на этом пути."""
    peak = p.fav[-1] if p.fav else 0.0
    rest_r = -1.0 if p.stopped else (p.end_r or 0.0)  # не взятая часть закрывается стопом или по close на горизонте

    def fee(r: float, frac: float) -> float:
        return frac * TAKER * (p.entry + p.sgn * r * p.risk) / p.risk

    gross = fees = 0.0
    fees = p.fee_in * p.entry / p.risk
    for r, frac in scale_plan(final_r).levels:
        got = r if peak >= r else rest_r
        gross += frac * got
        fees += fee(got, frac)
    return gross - fees


def candidates(paths: Iterable[Path_]) -> list[float]:
    """1R ... самый дальний ход в нашу сторону (шаг 0.25): без искусственного потолка."""
    top = max((p.fav[-1] for p in paths if p.fav), default=FIRST_TAKE_R)
    n = int(max(top, FIRST_TAKE_R) / STEP_R + 1e-9)
    return [round(k * STEP_R, 4) for k in range(int(FIRST_TAKE_R / STEP_R), n + 1)]


def best_final_r(paths: Sequence[Path_]) -> tuple[float, float] | None:
    """(лучший X, сумма R при нём) или None, если путей нет."""
    if not paths:
        return None
    best: tuple[float, float] | None = None
    for x in candidates(paths):
        total = sum(outcome(p, x) for p in paths)
        if best is None or total > best[1] + 1e-12:
            best = (x, total)
    return best


class AutoTpTuner:
    def __init__(self, fetch_rows: Callable[[datetime], Any], api: BinanceRest | None, strategies: Sequence[str], apply: Callable[[str, ScaleOutPlan], None], *,
                 state_path: Path, since: datetime, min_trades: int, default_r: float, skip: Iterable[str] = (), every_s: float = DAY_S,
                 now_ms: Callable[[], int] | None = None):
        self._fetch_rows, self._api, self._apply = fetch_rows, api, apply
        self._strategies = [s for s in strategies if s not in set(skip)]
        self._state_path, self._since, self._min, self._default = state_path, since, min_trades, default_r
        self._every = every_s
        self._now = now_ms or (lambda: int(time.time() * 1000))
        self.state: dict[str, dict[str, Any]] = self._load()
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------------------------------ состояние

    def _load(self) -> dict[str, dict[str, Any]]:
        if not self._state_path.is_file():
            return {}
        data = json.loads(self._state_path.read_text(encoding="utf-8"))
        return {k: v for k, v in data.get("strategies", {}).items() if isinstance(v, dict) and isinstance(v.get("tp_r"), (int, float))}

    def _save(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"updated_ms": self._now(), "since": self._since.date().isoformat(), "strategies": self.state}, ensure_ascii=False, indent=1),
                       encoding="utf-8")
        tmp.replace(self._state_path)

    def apply_saved(self) -> None:
        """При старте, до первой сделки: последний валидный X каждой стратегии (или план по умолчанию: Scale Out 50% на 1R + 1:default)."""
        for name in self._strategies:
            x = float(self.state.get(name, {}).get("tp_r", self._default))
            self._apply(name, scale_plan(x))
            log.info("Авто-TP: %s - старт с 1:%g (%s)", name, x, "последнее подобранное" if name in self.state else "по умолчанию, данных ещё не было")

    # ------------------------------------------------------------------------------------------ расчёт

    async def _bars(self, symbol: str, start: int, end: int) -> list[Bar]:
        assert self._api is not None
        out: list[Bar] = []
        cur = start
        while cur < end:
            rows = await self._api.get("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "startTime": cur, "endTime": end - 1, "limit": 1000},
                                       weight=klines_weight(1000))
            if not rows:
                break
            out += [(int(r[0]), float(r[2]), float(r[3]), float(r[4])) for r in rows]
            cur = int(rows[-1][0]) + MIN_MS
        return out

    async def tune(self) -> dict[str, dict[str, Any]]:
        """Один пересчёт по всем стратегиям -> {стратегия: результат}. Применяет и сохраняет лучшие X."""
        now = self._now()
        rows = [r for r in await self._fetch_rows(self._since) if r["strategy"] in self._strategies and str(r["coin"]).endswith("USDT")]
        by_symbol: dict[str, list[dict]] = {}
        for r in rows:
            by_symbol.setdefault(r["coin"], []).append(r)
        paths: dict[str, list[Path_]] = {}
        for symbol, group in by_symbol.items():
            start = min(_ms(r["entry_time"]) for r in group) // MIN_MS * MIN_MS
            end = min(max(_ms(r["entry_time"]) for r in group) + HORIZON_MS + MIN_MS, now // MIN_MS * MIN_MS)
            bars = await self._bars(symbol, start, end)
            for r in group:
                p = build_path(float(r["entry_price"]), float(r["sl"]), r["side"], r["kind"], _ms(r["entry_time"]), bars, now)
                if p is not None:
                    paths.setdefault(r["strategy"], []).append(p)
        report: dict[str, dict[str, Any]] = {}
        for name in self._strategies:
            ps = paths.get(name, [])
            prev = float(self.state.get(name, {}).get("tp_r", self._default))
            if len(ps) < self._min:
                report[name] = {"tp_r": prev, "n": len(ps), "changed": False, "reason": f"мало завершённых сделок ({len(ps)} < {self._min})"}
                log.info("Авто-TP: %s - 1:%g без изменений: завершённых сделок %d < %d", name, prev, len(ps), self._min)
                continue
            best = best_final_r(ps)
            assert best is not None
            x, total = best
            base = sum(outcome(p, prev) for p in ps)
            self.state[name] = {"tp_r": x, "n": len(ps), "sum_r": round(total, 3), "prev_r": prev, "prev_sum_r": round(base, 3),
                                "max_fav_r": round(max((p.fav[-1] for p in ps if p.fav), default=0.0), 3), "at_ms": now}
            self._apply(name, scale_plan(x))
            report[name] = {**self.state[name], "changed": x != prev}
            log.warning("Авто-TP: %s -> 1:%g (сделок %d, Σ %.2fR / %+.2f$ при риске $5; при прежнем 1:%g было бы %.2fR; 50%% фиксируется на +1R)",
                        name, x, len(ps), total, total * 5, prev, base)
        self._save()
        return report

    # ------------------------------------------------------------------------------------------ фоновая задача

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    def check_task(self) -> None:
        """Упавшая задача (ошибка кода) роняет процесс (R1); сбои сети задача переживает сама."""
        if self._task is not None and self._task.done():
            self._task.result()
            raise RuntimeError("Задача авто-TP завершилась без ошибки: так не должно быть")

    async def _loop(self) -> None:
        while True:
            try:
                await self.tune()
            except (NetworkError, HttpServerError, RateLimitError) as exc:
                log.warning("Авто-TP: пересчёт не выполнен (%s: %s) - действуют прежние тейки, повтор через час", type(exc).__name__, exc)
                await asyncio.sleep(3600)
                continue
            await asyncio.sleep(self._every)


def _ms(v: Any) -> int:
    return int(v.timestamp() * 1000) if isinstance(v, datetime) else int(v)


def since_from(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
