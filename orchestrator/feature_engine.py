"""FeatureEngine: свечи -> FeatureContext (признаки считает features.smc.add_features, один раз на свечу и монету).

Чистая функция без состояния: тот же код, что в бэктесте (R8). Контракт входа (непрерывная сетка закрытых свечей)
проверяет add_features; здесь только требование достаточной истории - иначе признаки были бы null, а «нет сигнала»
выглядело бы как «нет паттерна».
"""

import polars as pl

from features.smc import SmcParams, add_features
from orchestrator.types import FeatureContext
from utils.errors import ConfigError, DataError

CANDLE_COLUMNS = ("open_time", "open", "high", "low", "close", "volume")


class FeatureEngine:
    def __init__(self, params: SmcParams | None = None, min_bars: int = 100):
        self.params = params or SmcParams()
        if min_bars < self.params.warmup_bars:
            raise ConfigError(f"min_bars={min_bars} меньше прогрева признаков {self.params.warmup_bars}")
        self.min_bars = min_bars

    def build(self, symbol: str, interval: str, interval_ms: int, candles: pl.DataFrame, received_ms: int) -> FeatureContext:
        if candles.height < self.min_bars:
            raise DataError(f"{symbol} {interval}: свечей {candles.height}, нужно не меньше {self.min_bars}")
        frame = add_features(candles.select(CANDLE_COLUMNS), self.params, interval_ms=interval_ms)
        return FeatureContext(symbol, interval, interval_ms, frame, received_ms)
