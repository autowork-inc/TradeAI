"""Чтение свечей Этапа 1: data/raw/klines/<SYMBOL>/<TF>/<YYYY-MM>.parquet."""

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import config
from pipeline.loaders.schemas import KLINE_SCHEMA
from utils.errors import DataError

KLINES_OUT_SCHEMA = pl.Schema({"symbol": pl.String, **KLINE_SCHEMA, "gap_before": pl.Boolean})


def _months(start_ms: int, end_ms: int) -> list[str]:
    """Месяцы YYYY-MM, пересекающие [start_ms, end_ms)."""
    first = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    last = datetime.fromtimestamp((end_ms - 1) / 1000, tz=timezone.utc)
    out, (y, m) = [], (first.year, first.month)
    while (y, m) <= (last.year, last.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


class KlinesLoader:
    def __init__(self, root: Path | None = None):
        self._root = root  # None -> config.KLINES_DIR в момент вызова (тесты подменяют config)

    def pair_dir(self, symbol: str, interval: str) -> Path:
        return (self._root or config.KLINES_DIR) / symbol / interval

    def scan(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> pl.LazyFrame:
        """Закрытые свечи с open_time в [start_ms, end_ms).

        Колонки: symbol + KLINE_SCHEMA + gap_before (предыдущая свеча не ровно на интервал
        раньше; у первой строки False). Отсутствующие месячные файлы пропускаются - это
        дыра в данных, её видно по gap_before и по контрольным подсчётам вызывающего.
        Файл с чужой схемой - DataError (молча читать его нельзя).
        """
        if interval not in config.INTERVAL_MS:
            raise DataError(f"Неизвестный таймфрейм {interval!r}; известны {sorted(config.INTERVAL_MS)}")
        if end_ms <= start_ms:
            raise DataError(f"Пустой диапазон: [{start_ms}, {end_ms})")
        interval_ms = config.INTERVAL_MS[interval]
        directory = self.pair_dir(symbol, interval)

        files = [p for m in _months(start_ms, end_ms) if (p := directory / f"{m}.parquet").is_file()]
        for path in files:
            schema = pl.read_parquet_schema(path)
            if dict(schema) != dict(KLINE_SCHEMA):
                raise DataError(f"Схема {path} не совпадает с KLINE_SCHEMA: {dict(schema)}")

        base = pl.scan_parquet(files) if files else pl.DataFrame(schema=KLINE_SCHEMA).lazy()
        return (
            base.filter((pl.col("open_time") >= start_ms) & (pl.col("open_time") < end_ms))
            .unique(subset="open_time", keep="last")  # та же семантика, что upsert сборщика
            .sort("open_time")
            .with_columns(
                symbol=pl.lit(symbol),
                gap_before=(pl.col("open_time").diff() != interval_ms).fill_null(False),
            )
            .select(list(KLINES_OUT_SCHEMA))
        )
