"""Мелкие общие помощники для модулей синхронизации."""

from collections.abc import Iterable

import polars as pl

from utils.errors import DataError


def require_columns(frame: pl.LazyFrame, columns: Iterable[str], what: str) -> None:
    """Проверяет наличие колонок по схеме плана (данные не читаются)."""
    have = set(frame.collect_schema().names())
    missing = sorted(set(columns) - have)
    if missing:
        raise DataError(f"{what}: нет колонок {missing}; есть {sorted(have)}")


def asof(left: pl.LazyFrame, right: pl.LazyFrame, **kwargs) -> pl.LazyFrame:
    """join_asof с группировкой по symbol.

    Polars не проверяет сортировку ключей при `by` (молча даёт неверный результат на
    несортированных входах), поэтому проверка отключена, а вызывающий код ОБЯЗАН
    отсортировать оба кадра по ключу join непосредственно перед вызовом.
    """
    return left.join_asof(right, by="symbol", check_sortedness=False, **kwargs)
