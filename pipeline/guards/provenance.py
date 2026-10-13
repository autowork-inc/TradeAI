"""ProvenanceGuard - проверка на готовом кадре, что каждая строка знает только прошлое.

Ловит ошибки присоединения (join), которые не видны по отдельным значениям признаков:
  * src_max_ts >= asof_ts - в строку попали данные, известные не раньше момента решения;
  * src_max_ts = null - происхождение строки неизвестно;
  * дополнительные колонки времён (например h1_avail_ts, book_last_recv_ts) не меньше asof_ts;
  * asof_ts не растёт строго внутри символа - сетка решений сломана, окна признаков неверны.

Интерфейс (FrameGuard) общий для будущих проверок (truncation и др.): check() возвращает
отчёт или бросает LookaheadViolation; кадр не меняется.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import polars as pl

from utils.errors import DataError


class LookaheadViolation(DataError):
    """Строка использует данные, недоступные на момент решения."""


@dataclass(frozen=True)
class ProvenanceReport:
    n_rows: int
    n_symbols: int
    checked_columns: tuple[str, ...]


class FrameGuard(Protocol):
    def check(self, frame: pl.DataFrame) -> ProvenanceReport: ...


class ProvenanceGuard:
    def __init__(
        self,
        *,
        asof_col: str = "asof_ts",
        src_col: str = "src_max_ts",
        symbol_col: str = "symbol",
        extra_ts_cols: Sequence[str] = (),
    ):
        self._asof = asof_col
        self._src = src_col
        self._symbol = symbol_col
        self._ts_cols = (src_col, *extra_ts_cols)

    def check(self, frame: pl.DataFrame) -> ProvenanceReport:
        need = [self._symbol, self._asof, *self._ts_cols]
        missing = [c for c in need if c not in frame.columns]
        if missing:
            raise LookaheadViolation(f"В кадре нет колонок происхождения: {missing}")

        nulls = frame.filter(pl.col(self._src).is_null())
        if nulls.height:
            raise LookaheadViolation(
                f"{nulls.height} строк без {self._src}: происхождение неизвестно\n{nulls.head(3)}"
            )

        # Для extra-колонок null означает «источник не присоединён» - это допустимо.
        for col in self._ts_cols:
            bad = frame.filter(pl.col(col) >= pl.col(self._asof))
            if bad.height:
                raise LookaheadViolation(
                    f"{bad.height} строк, где {col} >= {self._asof} (заглядывание вперёд)\n"
                    f"{bad.select(self._symbol, self._asof, col).head(3)}"
                )

        # diff() первой строки каждого символа - null, sum() его игнорирует.
        steps = frame.select(
            (pl.col(self._asof).diff().over(self._symbol) <= 0).sum().alias("bad")
        )["bad"][0]
        if steps:
            raise LookaheadViolation(
                f"{steps} переходов, где {self._asof} не растёт строго внутри символа "
                "(кадр не отсортирован или есть дубли решений)"
            )
        return ProvenanceReport(frame.height, frame[self._symbol].n_unique(), tuple(self._ts_cols))
