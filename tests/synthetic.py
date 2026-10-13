"""Синтетический рынок для тестов ML-конвейера.

Свечи 1m в схеме KLINE_SCHEMA + symbol, стакан раз в секунду (depth_schema). Режим тренда (+/-) меняется
каждые 90 минут, стакан смещён в сторону режима, поэтому у метки (форвардная доходность) есть предсказуемая
часть: модель с признаками стакана ОБЯЗАНА находить на нём связь. Это проверка проводки и отсутствия утечек,
а не доказательство edge на реальном рынке.

poison_from() заменяет данные с заданного момента правдоподобным мусором: ни NaN, ни пропусков (конвейер
отработает штатно), но значения случайны. Если результат, который не должен зависеть от этих данных
(признаки прошлых строк, обученная на train модель, торговля раньше момента отравления), изменился - это
утечка будущего.
"""

import math
import random

import polars as pl

from backtester.contracts import FundingRecord
from pipeline.loaders.schemas import KLINE_SCHEMA, depth_schema

MIN = 60_000
START = 28_333_334 * MIN  # кратно минуте
LEVELS = 5

CANDLE_SCHEMA = {"symbol": pl.String, **KLINE_SCHEMA}


def synthetic_market(
    n_min: int = 900, seed: int = 11, *, symbol: str = "BTCUSDT", start_ms: int = START
) -> tuple[pl.DataFrame, pl.DataFrame]:
    rng = random.Random(seed)
    px, regime = 100.0, 1
    candles, regimes = [], []
    for m in range(n_min):
        if m and m % 90 == 0:
            regime = -regime
        o = px
        px *= math.exp(regime * 0.0004 + rng.gauss(0, 0.0004))
        volume = 10.0 + rng.random()
        taker_buy = volume * min(max(0.5 + 0.1 * regime + rng.uniform(-0.05, 0.05), 0.0), 1.0)
        open_time = start_ms + m * MIN
        candles.append({
            "symbol": symbol, "open_time": open_time, "open": o, "high": max(o, px) * 1.0001,
            "low": min(o, px) * 0.9999, "close": px, "volume": volume, "close_time": open_time + MIN - 1,
            "quote_volume": volume * px, "trades": 100 + m % 7, "taker_buy_base": taker_buy,
            "taker_buy_quote": taker_buy * px,
        })
        regimes.append(regime)

    rows, u = [], 0
    for s in range((n_min + 3) * 60):  # стакан живёт и после последней свечи: ордерам в конце есть против чего исполняться
        m = min(s // 60, n_min - 1)
        c = candles[m]
        mid = c["open"] + (c["close"] - c["open"]) * min((s % 60 + 1) / 60, 1.0) if s // 60 < n_min else c["close"]
        recv = start_ms + s * 1000 + 20
        u += 1
        row = {"recv_ts": recv, "symbol": symbol, "E": recv - 100, "T": recv - 100, "u": u, "pu": u - 1}
        for i in range(LEVELS):
            row[f"bid_px_{i}"], row[f"ask_px_{i}"] = mid - 0.05 - 0.1 * i, mid + 0.05 + 0.1 * i
            row[f"bid_qty_{i}"] = max(5.0 * (1 + 0.4 * regimes[m] + 0.3 * rng.uniform(-1, 1)), 0.5)
            row[f"ask_qty_{i}"] = max(5.0 * (1 - 0.4 * regimes[m] + 0.3 * rng.uniform(-1, 1)), 0.5)
        rows.append(row)
    return (
        pl.DataFrame(candles, schema=CANDLE_SCHEMA),
        pl.DataFrame(rows, schema=depth_schema(LEVELS)),
    )


def poison_from(
    candles: pl.DataFrame, book: pl.DataFrame, *, from_open: int, from_recv: int, seed: int = 99
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Свечи с open_time >= from_open и сообщения стакана с recv_ts >= from_recv - случайные, но валидные.

    Времена (open_time, recv_ts, E, u) не трогаем: водяной знак и сетка решений остаются прежними, меняется
    только содержимое рынка.
    """
    rng = random.Random(seed)

    px = [rng.uniform(50.0, 200.0) for _ in range(candles.height)]
    vol = [rng.uniform(1.0, 50.0) for _ in range(candles.height)]
    late = pl.col("open_time") >= from_open
    candles = candles.with_columns(
        pl.when(late).then(pl.Series(px)).otherwise(pl.col("close")).alias("close"),
        pl.when(late).then(pl.Series(vol)).otherwise(pl.col("volume")).alias("volume"),
        pl.when(late).then(pl.Series(vol) * 0.3).otherwise(pl.col("taker_buy_base")).alias("taker_buy_base"),
    ).with_columns(
        pl.when(late).then(pl.col("close")).otherwise(pl.col("open")).alias("open"),
        pl.when(late).then(pl.col("close") * 1.001).otherwise(pl.col("high")).alias("high"),
        pl.when(late).then(pl.col("close") * 0.999).otherwise(pl.col("low")).alias("low"),
    )

    mids = [rng.uniform(50.0, 200.0) for _ in range(book.height)]
    is_late = pl.col("recv_ts") >= from_recv
    exprs = []
    for i in range(LEVELS):
        exprs += [
            pl.when(is_late).then(pl.Series(mids) - 0.05 - 0.1 * i).otherwise(pl.col(f"bid_px_{i}")).alias(f"bid_px_{i}"),
            pl.when(is_late).then(pl.Series(mids) + 0.05 + 0.1 * i).otherwise(pl.col(f"ask_px_{i}")).alias(f"ask_px_{i}"),
            pl.when(is_late).then(pl.Series([rng.uniform(0.5, 10.0) for _ in range(book.height)]))
            .otherwise(pl.col(f"bid_qty_{i}")).alias(f"bid_qty_{i}"),
            pl.when(is_late).then(pl.Series([rng.uniform(0.5, 10.0) for _ in range(book.height)]))
            .otherwise(pl.col(f"ask_qty_{i}")).alias(f"ask_qty_{i}"),
        ]
    return candles, book.with_columns(exprs)


class NoFunding:
    """FundingFeed без списаний (для тестов, которым фандинг не важен)."""

    def settlements(self, symbol, from_ts, to_ts):
        return []


def funding_records(symbol: str, times: list[int], rate: float = 0.0001, mark: float = 100.0) -> list[FundingRecord]:
    return [FundingRecord(symbol, t, rate, mark) for t in times]
