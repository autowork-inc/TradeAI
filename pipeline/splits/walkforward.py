"""Purged Walk-Forward CV: разбиения train/test без утечки меток и признаков.

Откуда берётся утечка. Метка строки i смотрит вперёд на horizon (LabelBuilder: она «живёт» в
[asof_ts_i, end_ts_i]), признак строки j смотрит назад на lookback. Если train и test стоят
вплотную, они делят одни и те же цены:

    train: ... i-2 i-1  i | test: t0 t1 t2 ...        метка строки i (i + horizon >= t0) уже содержит
                                                     цену из теста: модель учится на будущем теста.

Два механизма, оба в барах оси времени:

  * purge_bars - train строго ДО теста: последние purge_bars баров перед началом теста выкидываются.
    Нужно purge_bars >= label_horizon_bars: тогда метка последней train-строки заканчивается раньше
    первой тестовой. Меньше - ConfigError, а не молчаливая утечка.
  * embargo_bars - train ПОСЛЕ теста (только allow_train_after_test=True, т.е. «сложные схемы»,
    где train стоит по обе стороны теста): embargo_bars баров после конца теста выкидываются. Строки
    сразу после теста делят с ним цены двояко: их признаки (lookback) захватывают тест, а метки
    теста (horizon) захватывают их. Нужно embargo_bars >= max(label_horizon_bars, feature_lookback_bars).
    В обычном walk-forward train после теста нет, эмбарго не на что действовать, и ненулевое значение
    без allow_train_after_test - ConfigError (не принимаем параметр, который молча ничего не делает).

Схема. Тест - n_splits подряд идущих окон по test_size баров, они занимают ПОСЛЕДНИЕ
n_splits * test_size баров оси (данные до них - только на обучение). Train по умолчанию растущий
(expanding, от начала оси); train_size > 0 делает его скользящим (последние train_size баров перед purge).

Ось времени - отсортированные уникальные asof_ts. Строки разных символов с одним asof_ts всегда
попадают в одну часть разбиения. Пропуски в сетке (дыры в свечах) делают purge только
консервативнее: горизонт метки H мс покрывает не больше H/interval позиций оси.

Точный purge: split_frame(label_end_cols=...) дополнительно выбрасывает train-строки до теста, у которых
{метка}_end_ts >= начала теста (определение purge из pipeline/labels); работает и при purge_bars=0.
"""

import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

import polars as pl

from utils.errors import ConfigError, DataError


def bars_for(duration_ms: int, interval_ms: int) -> int:
    """Сколько баров сетки покрывает интервал (с округлением вверх): horizon_ms -> label_horizon_bars."""
    for name, value in (("duration_ms", duration_ms), ("interval_ms", interval_ms)):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ConfigError(f"{name} должен быть положительным целым числом мс, получено {value!r}")
    return math.ceil(duration_ms / interval_ms)


def _count(value: object, name: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"{name} должен быть целым >= {minimum}, получено {value!r}")
    return value


@dataclass(frozen=True)
class Fold:
    """Один фолд в позициях оси времени (индексы баров, не строк кадра)."""

    index: int
    train: tuple[range, ...]  # 1 блок (до теста) или 2 (до и после); блоки не пересекаются, по возрастанию
    test: range
    purged: range  # выброшено из train перед тестом (purge)
    embargoed: range  # выброшено после теста (embargo); пусто вне allow_train_after_test

    @property
    def n_train(self) -> int:
        return sum(len(r) for r in self.train)

    @property
    def n_test(self) -> int:
        return len(self.test)

    def train_indices(self) -> list[int]:
        return [i for r in self.train for i in r]

    def test_indices(self) -> list[int]:
        return list(self.test)


@dataclass(frozen=True)
class FoldFrames:
    fold: Fold
    train: pl.DataFrame
    test: pl.DataFrame


class PurgedWalkForward:
    """Генератор разбиений Purged Walk-Forward. Параметры и правила - в докстринге модуля."""

    def __init__(
        self,
        n_splits: int,
        test_size: int,
        *,
        label_horizon_bars: int,
        purge_bars: int,
        embargo_bars: int = 0,
        feature_lookback_bars: int = 0,
        train_size: int | None = None,
        min_train_size: int = 1,
        allow_train_after_test: bool = False,
    ):
        self.n_splits = _count(n_splits, "n_splits", 1)
        self.test_size = _count(test_size, "test_size", 1)
        self.label_horizon_bars = _count(label_horizon_bars, "label_horizon_bars", 0)
        self.purge_bars = _count(purge_bars, "purge_bars", 0)
        self.embargo_bars = _count(embargo_bars, "embargo_bars", 0)
        self.feature_lookback_bars = _count(feature_lookback_bars, "feature_lookback_bars", 0)
        self.min_train_size = _count(min_train_size, "min_train_size", 1)
        self.train_size = None if train_size is None else _count(train_size, "train_size", self.min_train_size)
        self.allow_train_after_test = bool(allow_train_after_test)

        if self.purge_bars < self.label_horizon_bars:
            raise ConfigError(
                f"purge_bars={self.purge_bars} < label_horizon_bars={self.label_horizon_bars}: метки последних "
                "train-строк заходят в тест (утечка). Нужно purge_bars >= горизонта метки в барах"
            )
        if self.allow_train_after_test:
            need = max(self.label_horizon_bars, self.feature_lookback_bars)
            if self.embargo_bars < need:
                raise ConfigError(
                    f"embargo_bars={self.embargo_bars} < max(label_horizon_bars, feature_lookback_bars)={need}: "
                    "train сразу после теста делит с ним цены (утечка)"
                )
            if self.train_size is not None:
                raise ConfigError("train_size (скользящее окно) несовместим с allow_train_after_test")
        elif self.embargo_bars:
            raise ConfigError(
                "embargo_bars > 0 без allow_train_after_test: в walk-forward после теста train нет, эмбарго "
                "ничего бы не делало"
            )

    # ------------------------------------------------------------------ позиции

    def split(self, n_bars: int) -> Iterator[Fold]:
        """Фолды по позициям оси из n_bars баров. Недостаток данных - ConfigError, а не пропуск фолда."""
        _count(n_bars, "n_bars", 0)
        first_test = n_bars - self.n_splits * self.test_size
        if first_test < 0:
            raise ConfigError(
                f"{self.n_splits} окон по {self.test_size} баров не помещаются в {n_bars} баров"
            )
        for k in range(self.n_splits):
            start = first_test + k * self.test_size
            stop = start + self.test_size
            train_end = start - self.purge_bars
            train_start = 0 if self.train_size is None else max(train_end - self.train_size, 0)
            blocks = [range(train_start, train_end)] if train_end > train_start else []
            if self.allow_train_after_test and stop + self.embargo_bars < n_bars:
                blocks.append(range(stop + self.embargo_bars, n_bars))
            fold = Fold(
                index=k,
                train=tuple(blocks),
                test=range(start, stop),
                purged=range(max(train_end, 0), start),
                embargoed=range(stop, min(stop + self.embargo_bars, n_bars)),
            )
            if fold.n_train < self.min_train_size:
                raise ConfigError(
                    f"Фолд {k}: в train {fold.n_train} баров < min_train_size={self.min_train_size} "
                    f"(тест с {start}, purge {self.purge_bars}): мало данных до теста"
                )
            yield fold

    # -------------------------------------------------------------------- кадры

    def split_frame(
        self,
        frame: pl.DataFrame,
        *,
        ts_col: str = "asof_ts",
        label_end_cols: Sequence[str] = (),
    ) -> Iterator[FoldFrames]:
        """Фолды как пары (train, test) кадров. Ось времени - уникальные значения ts_col.

        label_end_cols - колонки {метка}_end_ts (LabelBuilder.end_names): train-строки ДО теста с
        end_ts >= начала теста дополнительно выбрасываются (точный purge, поверх purge_bars).
        Строки с null в end_ts метки не лишаются: в обучение их не берут (drop_unlabeled), утечки нет.
        """
        if not isinstance(frame, pl.DataFrame):
            raise ConfigError("split_frame принимает pl.DataFrame")
        missing = [c for c in (ts_col, *label_end_cols) if c not in frame.columns]
        if missing:
            raise DataError(f"В кадре нет колонок {missing}")
        if frame[ts_col].null_count():
            raise DataError(f"{ts_col} не может быть null")
        axis = frame[ts_col].unique().sort().to_list()
        ts = pl.col(ts_col)

        for fold in self.split(len(axis)):
            test_lo, test_hi = axis[fold.test.start], axis[fold.test.stop - 1]
            train_mask = pl.any_horizontal(*[ts.is_between(axis[r.start], axis[r.stop - 1]) for r in fold.train])
            train = frame.filter(train_mask)
            for col in label_end_cols:
                end = pl.col(col)
                train = train.filter((ts > test_hi) | end.is_null() | (end < test_lo))
            yield FoldFrames(fold, train, frame.filter(ts.is_between(test_lo, test_hi)))
