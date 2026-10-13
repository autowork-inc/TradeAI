"""Чтение закрытых ставок фандинга Этапа 1: data/raw/funding/<SYMBOL>/<YYYY>.parquet."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import config
from pipeline.loaders.schemas import FUNDING_SCHEMA
from pipeline.sync.watermark import WatermarkClock
from utils.errors import DataError

FUNDING_OUT_SCHEMA = pl.Schema({"symbol": pl.String, **FUNDING_SCHEMA, "gap_before": pl.Boolean})

# Пропущенным считаем списание, если до предыдущей записи больше полутора интервалов.
_GAP_FACTOR = 1.5


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _year(ms: int) -> int:
    # Через timedelta, а не fromtimestamp: на Windows отрицательные метки (диапазон с запасом
    # назад от начала эпохи) дают OSError.
    return (_EPOCH + timedelta(milliseconds=ms)).year


def _years(start_ms: int, end_ms: int) -> list[int]:
    return list(range(_year(start_ms), _year(end_ms - 1) + 1))


class FundingLoader:
    def __init__(self, root: Path | None = None, interval_ms: int | None = None):
        self._root = root  # None -> config.FUNDING_DIR в момент вызова
        self._interval_ms = interval_ms

    @property
    def interval_ms(self) -> int:
        return self._interval_ms or config.FUNDING_INTERVAL_MS

    def symbol_dir(self, symbol: str) -> Path:
        return (self._root or config.FUNDING_DIR) / symbol

    def scan(self, symbol: str, start_ms: int, end_ms: int, *, lookback_ms: int = 0) -> pl.LazyFrame:
        """Ставки с funding_time в [start_ms - lookback_ms, end_ms) (серверное время).

        Колонки: symbol + FUNDING_SCHEMA + gap_before (до предыдущей записи больше 1.5
        интервала: пропущено списание; у первой строки False). Отсутствующие годовые файлы
        пропускаются - дыру видно по gap_before. Файл с чужой схемой - DataError.
        """
        if end_ms <= start_ms:
            raise DataError(f"Пустой диапазон: [{start_ms}, {end_ms})")
        if lookback_ms < 0:
            raise DataError(f"lookback_ms не может быть отрицательным: {lookback_ms}")
        begin = start_ms - lookback_ms
        directory = self.symbol_dir(symbol)
        files = [p for y in _years(begin, end_ms) if (p := directory / f"{y:04d}.parquet").is_file()]
        for path in files:
            schema = pl.read_parquet_schema(path)
            if dict(schema) != dict(FUNDING_SCHEMA):
                raise DataError(f"Схема {path} не совпадает с FUNDING_SCHEMA: {dict(schema)}")

        base = pl.scan_parquet(files) if files else pl.DataFrame(schema=FUNDING_SCHEMA).lazy()
        return (
            base.filter((pl.col("funding_time") >= begin) & (pl.col("funding_time") < end_ms))
            .unique(subset="funding_time", keep="last")
            .sort("funding_time")
            .with_columns(
                symbol=pl.lit(symbol),
                gap_before=(pl.col("funding_time").diff() > self.interval_ms * _GAP_FACTOR).fill_null(False),
            )
            .select(list(FUNDING_OUT_SCHEMA))
        )

    def scan_with_availability(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        clock: WatermarkClock,
        wm: pl.LazyFrame,
        *,
        assume_pre_stream: bool = False,
    ) -> pl.LazyFrame:
        """Ставки, готовые к TimelineAligner.attach_funding: + avail_ts, avail_source, evidence_lag_ms.

        Захватывает записи за два интервала ДО start_ms: первая строка решений должна видеть
        ставку, списанную до начала диапазона. wm - водяной знак стакана (clock.watermark).
        """
        events = self.scan(symbol, start_ms, end_ms, lookback_ms=2 * self.interval_ms)
        return clock.funding_availability(events, wm, self.interval_ms, assume_pre_stream=assume_pre_stream)
