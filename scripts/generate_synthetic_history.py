"""Генератор синтетической сырой истории Этапа 1: свечи, стакан (500 мс) и фандинг за последние N часов.

    python -m scripts.generate_synthetic_history                       # 48 часов, config.SYMBOLS
    python -m scripts.generate_synthetic_history --hours 72 --seed 3

Зачем. Стакан по REST исторически не отдаётся, копить его сборщиком (collectors/depth_stream.py) нужно сутки.
Чтобы прогнать train_walkforward и боевой контур СЕЙЧАС, утилита пишет в data/raw те же файлы, что и сборщики,
в тех же форматах, которые читают загрузчики конвейера (проверяется tests/test_generate_synthetic_history.py):

  data/raw/klines/<SYMBOL>/1m/<YYYY-MM>.parquet   KLINE_SCHEMA (и 1h из полных часов);
  data/raw/depth/<SYMBOL>/<YYYY-MM-DD>/<HH>.jsonl сообщения btcusdt@depth10@500ms как их пишет сборщик:
                                                  {"recv_ts","symbol","stream","data":{e,E,T,s,ps,U,u,pu,b,a}},
                                                  первой строкой - REST-снапшот (stream="snapshot");
  data/raw/funding/<SYMBOL>/<YYYY>.parquet        FUNDING_SCHEMA, списания 00/08/16 UTC с запасом назад.

Модель рынка. Цена - геометрическое блуждание на сетке 500 мс; режим тренда (+/-) меняется каждые 60-120 минут,
дисбаланс стакана (AR(1)) тянется к режиму, taker-доля свечей смещена в ту же сторону. Поэтому у форвардной
доходности есть предсказуемая часть, и модель на признаках стакана находит связь: это проверка проводки конвейера
(загрузка -> гейт -> признаки -> метки -> обучение -> файл модели -> бой), а НЕ доказательство edge на рынке.
Метрики обучения на этих данных ничего не говорят о реальной торговле.

Время. Конец истории - начало текущей минуты (последняя свеча закрыта), все recv_ts строго раньше «сейчас».
recv_ts = E + задержка 25-70 мс (редкие всплески до 300 мс), recv_ts монотонен: DataQualityGate чистое сырьё
принимает целиком.

Безопасность. Скрипт не затирает чужие данные: если в целевых местах уже лежат файлы, а маркера
data/raw/SYNTHETIC.json нет (то есть это данные настоящих сборщиков), - выход с ошибкой; обойти можно только
явным --force. Маркер пишется после генерации, повторный запуск перезаписывает синтетику, но не настоящие данные.
"""

import argparse
import json
import logging
import math
import random
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import config
from pipeline.loaders.schemas import FUNDING_SCHEMA, KLINE_SCHEMA
from utils.errors import EXIT_GENERAL, EXIT_OK, ConfigError, TradingInfraError
from utils.logger import setup_logging
from utils.storage import atomic_write_parquet, atomic_write_text

log = logging.getLogger("generate_synthetic_history")

STEP_MS = 500  # шаг сообщений стакана, как у @depth10@500ms
MINUTE_MS = 60_000
HOUR_MS = 3_600_000
STEPS_PER_MINUTE = MINUTE_MS // STEP_MS
MARKER_NAME = "SYNTHETIC.json"
FUNDING_LOOKBACK_MS = 3 * 24 * HOUR_MS  # запас назад: FrameFundingFeed требует покрытие с интервалом до начала окна


@dataclass(frozen=True)
class SymbolSpec:
    base_price: float  # цена на КОНЕЦ истории (≈ уровень тестовой сети на момент написания)
    tick: float
    px_decimals: int
    qty_decimals: int
    top_qty: float  # типичный объём лучшего уровня
    minute_volume: float  # типичный объём минуты в базовом активе


SPECS = {
    "BTCUSDT": SymbolSpec(84_750.0, 0.1, 2, 3, 2.0, 12.0),
    "ETHUSDT": SymbolSpec(2_690.0, 0.01, 2, 3, 40.0, 220.0),
}


def spec_for(symbol: str) -> SymbolSpec:
    try:
        return SPECS[symbol]
    except KeyError:
        raise ConfigError(f"Нет параметров синтетического рынка для {symbol}; известны {sorted(SPECS)}") from None


# ------------------------------------------------------------------------------------- модель рынка


@dataclass(frozen=True)
class SymbolHistory:
    symbol: str
    candles: pl.DataFrame  # KLINE_SCHEMA, минутные
    depth_lines: list[tuple[int, str]]  # (recv_ts, строка jsonl) по возрастанию recv_ts, первая - снапшот
    funding: pl.DataFrame  # FUNDING_SCHEMA


def _fmt(value: float, decimals: int) -> str:
    return f"{value:.{decimals}f}"


def _regimes(n_minutes: int, rng: random.Random) -> list[int]:
    out, regime, left = [], rng.choice((-1, 1)), 0
    for _ in range(n_minutes):
        if left == 0:
            regime, left = -regime, rng.randint(60, 120)
        out.append(regime)
        left -= 1
    return out


def generate_symbol(
    symbol: str, start_ms: int, n_minutes: int, *, seed: int, levels: int, end_ms: int | None = None
) -> SymbolHistory:
    """Рынок одного символа на [start_ms, start_ms + n_minutes * 1m). start_ms кратен минуте."""
    if start_ms % MINUTE_MS:
        raise ConfigError(f"start_ms должен быть кратен минуте, получено {start_ms}")
    if n_minutes < 1 or levels < 1:
        raise ConfigError("n_minutes и levels должны быть >= 1")
    spec = spec_for(symbol)
    end_ms = start_ms + n_minutes * MINUTE_MS if end_ms is None else end_ms
    rng = random.Random(f"{seed}:{symbol}")
    regimes = _regimes(n_minutes + 1, rng)

    # --- средняя цена на сетке 500 мс (n_steps + 1 точек: последняя - закрытие последней свечи)
    n_steps = n_minutes * STEPS_PER_MINUTE
    sigma_step = 0.0006 / math.sqrt(STEPS_PER_MINUTE)
    drift_step = 0.00015 / STEPS_PER_MINUTE
    log_mid = [0.0]
    for i in range(n_steps):
        log_mid.append(log_mid[-1] + regimes[i // STEPS_PER_MINUTE] * drift_step + rng.gauss(0.0, sigma_step))
    shift = math.log(spec.base_price) - log_mid[-1]  # конец истории приходится на base_price
    mids = [math.exp(x + shift) for x in log_mid]

    # --- свечи
    candles = []
    for m in range(n_minutes):
        lo_i, hi_i = m * STEPS_PER_MINUTE, (m + 1) * STEPS_PER_MINUTE
        path = mids[lo_i:hi_i + 1]
        o, c = round(path[0], spec.px_decimals), round(path[-1], spec.px_decimals)
        h = round(max(max(path), o, c) * (1 + abs(rng.gauss(0, 0.00005))), spec.px_decimals)
        lo = round(min(min(path), o, c) * (1 - abs(rng.gauss(0, 0.00005))), spec.px_decimals)
        h, lo = max(h, o, c), min(lo, o, c)
        ret = abs(math.log(c / o))
        volume = round(spec.minute_volume * math.exp(rng.gauss(0, 0.4)) * (1 + 2.0 * ret / 0.0006), 3)
        avg = (o + c) / 2
        buy_share = min(max(0.5 + 0.1 * regimes[m] + rng.uniform(-0.08, 0.08), 0.02), 0.98)
        taker_buy = round(volume * buy_share, 3)
        candles.append((
            start_ms + m * MINUTE_MS, o, h, lo, c, volume, start_ms + (m + 1) * MINUTE_MS - 1,
            round(volume * avg, 2), int(volume / spec.minute_volume * 600) + rng.randint(0, 40), taker_buy,
            round(taker_buy * avg, 2),
        ))
    candle_frame = pl.DataFrame(candles, schema=KLINE_SCHEMA, orient="row")

    # --- стакан
    stream = f"{symbol.lower()}@depth{levels}@{config.DEPTH_UPDATE_SPEED}"
    bid_q = [spec.top_qty * (1 + 0.35 * i) for i in range(levels)]
    ask_q = list(bid_q)
    imbalance, u = 0.0, rng.randint(10**9, 10**10)
    lines: list[tuple[int, str]] = []

    def book_side(mid: float, spread_ticks: int, qty: list[float], sign: int) -> list[list[str]]:
        # sign = -1: бид (вниз от mid), +1: аск (вверх)
        half = spread_ticks * spec.tick / 2
        px = (math.floor((mid - half) / spec.tick) if sign < 0 else math.ceil((mid + half) / spec.tick)) * spec.tick
        out = []
        for i in range(levels):
            out.append([_fmt(px, spec.px_decimals), _fmt(qty[i], spec.qty_decimals)])
            px += sign * spec.tick * (1 + int(rng.expovariate(1.2)))
        return out

    for i in range(n_steps):
        regime = regimes[i // STEPS_PER_MINUTE]
        imbalance += 0.02 * (0.7 * regime - imbalance) + rng.gauss(0.0, 0.08)
        imbalance = max(-0.9, min(0.9, imbalance))
        for k in range(levels):
            scale = spec.top_qty * (1 + 0.35 * k)
            bid_q[k] = max(0.6 * bid_q[k] + 0.4 * scale * (1 + 0.6 * imbalance) * math.exp(rng.gauss(0, 0.35)), 0.001)
            ask_q[k] = max(0.6 * ask_q[k] + 0.4 * scale * (1 - 0.6 * imbalance) * math.exp(rng.gauss(0, 0.35)), 0.001)
        spread_ticks = 1 + (rng.random() < 0.15) + (rng.random() < 0.03)
        event_ms = start_ms + i * STEP_MS + rng.randint(-15, 15)
        latency = 25 + int(abs(rng.gauss(0, 18))) + (rng.randint(100, 300) if rng.random() < 0.002 else 0)
        recv_ms = event_ms + latency
        if recv_ms >= end_ms:
            break
        pu, u = u, u + rng.randint(500, 20_000)
        message = {
            "e": "depthUpdate", "E": event_ms, "T": event_ms - rng.randint(1, 4), "s": symbol, "ps": symbol,
            "U": pu + 1, "u": u, "pu": pu,
            "b": book_side(mids[i], spread_ticks, bid_q, -1), "a": book_side(mids[i], spread_ticks, ask_q, +1),
        }
        lines.append((recv_ms, json.dumps(
            {"recv_ts": recv_ms, "symbol": symbol, "stream": stream, "data": message}, separators=(",", ":")
        )))

    # REST-снапшот сборщика: первая запись в файле; нормализация его пропускает, но считает
    first_msg = json.loads(lines[0][1])["data"]
    snap_ts = lines[0][0] - 1
    snapshot = {
        "lastUpdateId": first_msg["pu"], "E": first_msg["E"] - 10, "T": first_msg["E"] - 12,
        "bids": first_msg["b"], "asks": first_msg["a"],
    }
    lines.insert(0, (snap_ts, json.dumps(
        {"recv_ts": snap_ts, "symbol": symbol, "stream": "snapshot", "data": snapshot}, separators=(",", ":")
    )))
    lines.sort(key=lambda x: x[0])  # recv_ts монотонен по построению; сортировка страхует снапшот

    return SymbolHistory(symbol, candle_frame, lines, _funding(symbol, spec, start_ms, end_ms, candles, rng))


def _funding(
    symbol: str, spec: SymbolSpec, start_ms: int, end_ms: int, candles: list[tuple], rng: random.Random
) -> pl.DataFrame:
    """Списания 00/08/16 UTC от start - 3 суток до последнего <= end. Ставка - небольшой AR-шум около 0.01%."""
    step = config.FUNDING_INTERVAL_MS
    first = (start_ms - FUNDING_LOOKBACK_MS) // step * step
    times = list(range(first, end_ms + 1, step))
    closes = {c[0]: c[4] for c in candles}
    rows, rate = [], 0.0001
    for t in times:
        rate = 0.7 * rate + 0.3 * 0.0001 + rng.gauss(0, 0.00003)
        minute = t // MINUTE_MS * MINUTE_MS
        mark = closes.get(minute, candles[0][1] if minute < start_ms else candles[-1][4])
        rows.append((t, round(rate, 8), round(mark, spec.px_decimals)))
    return pl.DataFrame(rows, schema=FUNDING_SCHEMA, orient="row")


def hourly_candles(frame_1m: pl.DataFrame) -> pl.DataFrame:
    """1h-свечи из минутных: только ПОЛНЫЕ часы (60 минут подряд), как закрытые свечи биржи."""
    hourly = (
        frame_1m.sort("open_time")
        .with_columns(hour=pl.col("open_time") // HOUR_MS * HOUR_MS)
        .group_by("hour", maintain_order=True)
        .agg(
            pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(),
            pl.col("volume").sum(), pl.col("quote_volume").sum(), pl.col("trades").sum(),
            pl.col("taker_buy_base").sum(), pl.col("taker_buy_quote").sum(), n=pl.len(),
        )
        .filter(pl.col("n") == 60)
        .with_columns(open_time=pl.col("hour"), close_time=pl.col("hour") + HOUR_MS - 1)
    )
    return hourly.select(list(KLINE_SCHEMA)).cast(dict(KLINE_SCHEMA))


# ------------------------------------------------------------------------------------------- запись


def _month(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m")


def _year(ms: int) -> int:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).year


def _upsert(path: Path, new: pl.DataFrame, key: str, lo: int, hi: int) -> None:
    """Файл = старое содержимое ВНЕ [lo, hi) + новое. Чужие записи вне диапазона сохраняются."""
    if path.is_file():
        old = pl.read_parquet(path)
        new = pl.concat([old.filter((pl.col(key) < lo) | (pl.col(key) >= hi)), new]).sort(key)
    atomic_write_parquet(new, path)


def _existing_targets(root: Path, symbols: Sequence[str]) -> list[Path]:
    found = []
    for sub in ("klines", "depth", "funding"):
        for symbol in symbols:
            directory = root / sub / symbol
            if directory.is_dir() and any(p.is_file() for p in directory.rglob("*")):
                found.append(directory)
    return found


def write_history(
    histories: Sequence[SymbolHistory], root: Path, start_ms: int, end_ms: int, *, force: bool = False,
    params: dict | None = None,
) -> dict[str, int]:
    """Пишет сырой слой в root (= data/raw). Возвращает счётчики файлов/строк для лога."""
    marker = root / MARKER_NAME
    existing = _existing_targets(root, [h.symbol for h in histories])
    if existing and not marker.is_file() and not force:
        raise ConfigError(
            "В целевых каталогах уже есть данные, а маркера синтетики нет - это похоже на данные настоящих "
            f"сборщиков: {[str(p) for p in existing]}. Они не будут затёрты; --force - только если вы уверены."
        )
    counts = {"depth_files": 0, "depth_lines": 0, "kline_rows": 0, "funding_rows": 0}
    for h in histories:
        by_hour: dict[str, list[str]] = {}
        for recv_ts, line in h.depth_lines:
            key = datetime.fromtimestamp(recv_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d/%H")
            by_hour.setdefault(key, []).append(line)
        for key, lines in by_hour.items():
            day, hour = key.split("/")
            path = root / "depth" / h.symbol / day / f"{hour}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")  # 'w': часы ПЕРЕзаписываются
            counts["depth_files"] += 1
            counts["depth_lines"] += len(lines)

        for interval, frame in (("1m", h.candles), ("1h", hourly_candles(h.candles))):
            if frame.height == 0:
                continue
            for month in sorted({_month(t) for t in frame["open_time"].to_list()}):
                part = frame.filter(pl.col("open_time").map_elements(_month, return_dtype=pl.String) == month)
                _upsert(root / "klines" / h.symbol / interval / f"{month}.parquet", part, "open_time",
                        int(part["open_time"].min()), int(part["open_time"].max()) + 1)
            counts["kline_rows"] += frame.height

        f_lo, f_hi = int(h.funding["funding_time"].min()), int(h.funding["funding_time"].max()) + 1
        for year in sorted({_year(t) for t in h.funding["funding_time"].to_list()}):
            part = h.funding.filter(pl.col("funding_time").map_elements(_year, return_dtype=pl.Int64) == year)
            _upsert(root / "funding" / h.symbol / f"{year:04d}.parquet", part, "funding_time", f_lo, f_hi)
        counts["funding_rows"] += h.funding.height

    atomic_write_text(marker, json.dumps({
        "synthetic": True, "start_ms": start_ms, "end_ms": end_ms,
        "generated_at": datetime.now(timezone.utc).isoformat(), "symbols": [h.symbol for h in histories],
        **(params or {}),
    }, indent=2, ensure_ascii=False))
    return counts


# ------------------------------------------------------------------------------------------- CLI


def build(symbols: Sequence[str], *, hours: float, seed: int, levels: int, now_ms: int) -> tuple[list[SymbolHistory], int, int]:
    n_minutes = int(round(hours * 60))
    end_ms = now_ms // MINUTE_MS * MINUTE_MS  # последняя свеча закрыта, все recv_ts < now
    start_ms = end_ms - n_minutes * MINUTE_MS
    histories = [
        generate_symbol(s, start_ms, n_minutes, seed=seed, levels=levels, end_ms=end_ms) for s in symbols
    ]
    return histories, start_ms, end_ms


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Синтетическая сырая история (свечи, стакан 500 мс, фандинг)")
    p.add_argument("--symbols", nargs="+", default=list(config.SYMBOLS))
    p.add_argument("--hours", type=float, default=48.0, help="длина истории, часов назад от текущей минуты")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--root", type=Path, default=None, help="каталог сырого слоя (по умолчанию data/raw)")
    p.add_argument("--force", action="store_true", help="писать поверх каталогов с данными без маркера синтетики")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    setup_logging("generate_synthetic_history")
    try:
        if args.hours < 1:
            raise ConfigError("--hours должен быть >= 1")
        root = args.root or config.DATA_DIR / "raw"
        started = time.monotonic()
        histories, start_ms, end_ms = build(
            args.symbols, hours=args.hours, seed=args.seed, levels=config.DEPTH_LEVELS,
            now_ms=time.time_ns() // 1_000_000,
        )
        fmt = lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731
        log.info("сгенерировано за %.0f с: %s .. %s UTC, символы %s", time.monotonic() - started, fmt(start_ms),
                 fmt(end_ms), ", ".join(args.symbols))
        counts = write_history(histories, root, start_ms, end_ms, force=args.force,
                               params={"hours": args.hours, "seed": args.seed, "levels": config.DEPTH_LEVELS})
        log.info("записано в %s: файлов стакана %d (%d сообщений), свечей %d, ставок фандинга %d",
                 root, counts["depth_files"], counts["depth_lines"], counts["kline_rows"], counts["funding_rows"])
        log.warning("ДАННЫЕ СИНТЕТИЧЕСКИЕ (маркер %s): метрики обучения на них не говорят о реальном рынке", root / MARKER_NAME)
        return EXIT_OK
    except TradingInfraError as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return exc.exit_code
    except Exception:
        log.exception("Непредвиденная ошибка")
        return EXIT_GENERAL


if __name__ == "__main__":
    sys.exit(main())
