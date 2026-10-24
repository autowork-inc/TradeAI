"""История для Playground: 1m-свечи Binance USDⓈ-M (публичный REST) с кэшем по месяцам и старшие таймфреймы, собранные из них.

Все таймфреймы (5m, 15m, 1h, 4h, 1d) собираются из одних и тех же минуток на UTC-сетке: OHLCV агрегата совпадает со свечой биржи, а путь цены
внутри рабочей свечи (минутки) согласован со свечами стратегий. Дыры биржи (техработы) закрываются плоскими свечами по прошлому close с нулевым
объёмом (как MT5-фид на выходных) - иначе `validate_candles` отверг бы окно.

Кэш: <root>/<SYMBOL>/1m/<YYYY-MM>.parquet (формат collectors.klines_downloader). Докачиваются только недостающие края диапазона.
"""

import logging
from pathlib import Path

import polars as pl

import config
from collectors.klines_downloader import flush_frames, parse_klines
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import DataError

log = logging.getLogger("playground.history")

MINUTE_MS = 60_000
LIMIT = 1000  # вес 5 на запрос (1500 - уже 10): год минуток ~ 530 запросов
OHLCV = ("open_time", "open", "high", "low", "close", "volume")


def minute_dir(root: Path, symbol: str) -> Path:
    return root / symbol / "1m"


def stored_range(root: Path, symbol: str) -> tuple[int, int] | None:
    files = sorted(minute_dir(root, symbol).glob("????-??.parquet"))
    if not files:
        return None
    first = pl.read_parquet(files[0], columns=["open_time"])["open_time"].min()
    last = pl.read_parquet(files[-1], columns=["open_time"])["open_time"].max()
    return int(first), int(last)


async def _download(api: BinanceRest, root: Path, symbol: str, start_ms: int, end_ms: int, on_progress=None) -> int:
    """Минутки [start_ms, end_ms) -> кэш. Возвращает число полученных свечей."""
    pair_dir = minute_dir(root, symbol)
    pair_dir.mkdir(parents=True, exist_ok=True)
    cursor, frames, got = start_ms, [], 0
    total = max(1, (end_ms - start_ms) // MINUTE_MS)
    while cursor < end_ms:
        rows = await api.get(config.KLINES_PATH, {"symbol": symbol, "interval": "1m", "startTime": cursor, "endTime": end_ms - 1, "limit": LIMIT},
                             weight=klines_weight(LIMIT))
        batch = parse_klines(rows)
        if batch.is_empty():
            break
        frames.append(batch.filter(pl.col("open_time") < end_ms))
        got += batch.height
        cursor = int(batch["open_time"][-1]) + MINUTE_MS
        if len(frames) >= 50:
            flush_frames(pair_dir, frames)
            frames = []
        if on_progress is not None:
            on_progress(min(1.0, (cursor - start_ms) / MINUTE_MS / total))
        if batch.height < LIMIT and cursor < end_ms:
            # биржа отдала меньше лимита, но конец не достигнут: дыра (символ ещё не торговался) - прыжок к следующей доступной свече
            continue
    flush_frames(pair_dir, frames)
    return got


async def ensure_minutes(api: BinanceRest, root: Path, symbol: str, start_ms: int, end_ms: int, on_progress=None) -> None:
    """Докачать в кэш минутки, которых не хватает на [start_ms, end_ms). Внутренние дыры кэша не докачиваются (это дыры биржи)."""
    have = stored_range(root, symbol)
    if have is None:
        log.info("%s: качаю 1m %s .. %s", symbol, start_ms, end_ms)
        await _download(api, root, symbol, start_ms, end_ms, on_progress)
        return
    first, last = have
    if start_ms < first:
        log.info("%s: докачка 1m в начало (%s .. %s)", symbol, start_ms, first)
        await _download(api, root, symbol, start_ms, first, on_progress)
    if last + MINUTE_MS < end_ms:
        log.info("%s: докачка 1m в конец (%s .. %s)", symbol, last + MINUTE_MS, end_ms)
        await _download(api, root, symbol, last + MINUTE_MS, end_ms, on_progress)


def load_minutes(root: Path, symbol: str, start_ms: int, end_ms: int) -> pl.DataFrame:
    """Минутки [start_ms, end_ms) из кэша на непрерывной сетке: пропуски - плоские свечи по прошлому close (объём 0). Нет данных на начало - DataError."""
    files = sorted(minute_dir(root, symbol).glob("????-??.parquet"))
    if not files:
        raise DataError(f"{symbol}: в кэше Playground нет минуток")
    df = (pl.concat([pl.read_parquet(f, columns=list(OHLCV)) for f in files])
          .filter((pl.col("open_time") >= start_ms) & (pl.col("open_time") < end_ms))
          .unique(subset="open_time", keep="last").sort("open_time"))
    if df.is_empty():
        raise DataError(f"{symbol}: нет минуток в диапазоне {start_ms}..{end_ms}")
    grid = pl.DataFrame({"open_time": pl.int_range(int(df["open_time"][0]), end_ms, MINUTE_MS, eager=True, dtype=pl.Int64)})
    full = grid.join(df, on="open_time", how="left").with_columns(pl.col("close").forward_fill())
    filled = full.with_columns(
        pl.col("open").fill_null(pl.col("close")), pl.col("high").fill_null(pl.col("close")), pl.col("low").fill_null(pl.col("close")),
        pl.col("volume").fill_null(0.0))
    missing = full["open"].null_count()
    if missing:
        log.warning("%s: %d минут без свечей на бирже - заполнены плоскими", symbol, missing)
    return filled.drop_nulls("close")


def resample(minutes: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """Свечи таймфрейма из минуток на UTC-сетке (4h и 1d - от полуночи UTC, как у Binance). Неполная последняя группа отбрасывается."""
    per = interval_ms // MINUTE_MS
    out = (minutes.with_columns((pl.col("open_time") // interval_ms * interval_ms).alias("_g"))
           .group_by("_g", maintain_order=True)
           .agg(pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(), pl.col("volume").sum(), pl.len().alias("_n"))
           .rename({"_g": "open_time"}))
    return out.filter(pl.col("_n") == per).drop("_n").sort("open_time")
