"""Purged Walk-Forward: раскладка фолдов, purge, embargo и отсутствие утечки меток на настоящих метках."""

import random

import polars as pl
import pytest

from pipeline.labels.base import LabelBuilder, LabelSpec
from pipeline.splits.walkforward import PurgedWalkForward, bars_for
from pipeline.sync.aligner import TimelineAligner
from pipeline.types import TimingConfig
from utils.errors import ConfigError, DataError

MIN = 60_000
TIMING = TimingConfig()


def wf(**kw) -> PurgedWalkForward:
    kw.setdefault("label_horizon_bars", kw.get("purge_bars", 0))
    kw.setdefault("purge_bars", 0)
    kw.setdefault("n_splits", 3)
    kw.setdefault("test_size", 10)
    return PurgedWalkForward(kw.pop("n_splits"), kw.pop("test_size"), **kw)


# ------------------------------------------------------------------ раскладка


def test_expanding_layout_tests_take_the_tail_and_train_stops_before_purge_gap():
    folds = list(wf(purge_bars=5).split(100))
    assert [f.test for f in folds] == [range(70, 80), range(80, 90), range(90, 100)]
    assert [f.train for f in folds] == [(range(0, 65),), (range(0, 75),), (range(0, 85),)]
    assert [f.purged for f in folds] == [range(65, 70), range(75, 80), range(85, 90)]
    assert all(len(f.embargoed) == 0 for f in folds)
    assert (folds[0].n_train, folds[0].n_test) == (65, 10)


def test_rolling_train_takes_last_train_size_bars_before_purge():
    folds = list(wf(purge_bars=5, train_size=20).split(100))
    assert [f.train for f in folds] == [(range(45, 65),), (range(55, 75),), (range(65, 85),)]


def test_train_after_test_cuts_embargo_and_keeps_the_rest():
    folds = list(wf(n_splits=2, purge_bars=3, embargo_bars=4, feature_lookback_bars=2, allow_train_after_test=True).split(100))
    f0, f1 = folds
    assert f0.test == range(80, 90) and f0.train == (range(0, 77), range(94, 100))
    assert f0.purged == range(77, 80) and f0.embargoed == range(90, 94)
    assert f1.test == range(90, 100) and f1.train == (range(0, 87),)  # после теста данных не осталось
    assert len(f1.embargoed) == 0


@pytest.mark.parametrize("seed", range(20))
def test_invariants_hold_for_random_configs(seed):
    rng = random.Random(seed)
    horizon = rng.randint(0, 12)
    purge = horizon + rng.randint(0, 5)
    after = rng.random() < 0.5
    lookback = rng.randint(0, 10)
    embargo = max(horizon, lookback) + rng.randint(0, 4) if after else 0
    n_splits, test_size = rng.randint(1, 5), rng.randint(1, 15)
    n_bars = n_splits * test_size + rng.randint(60, 200)
    gen = PurgedWalkForward(
        n_splits, test_size, label_horizon_bars=horizon, purge_bars=purge, embargo_bars=embargo,
        feature_lookback_bars=lookback, allow_train_after_test=after,
    )
    folds = list(gen.split(n_bars))
    assert len(folds) == n_splits
    for f in folds:
        train, test = set(f.train_indices()), set(f.test_indices())
        assert not train & test and test == set(f.test)
        before = [i for i in train if i < f.test.start]
        assert max(before) + purge < f.test.start  # зазор перед тестом не меньше purge
        assert max(before) + horizon < f.test.start  # метка последней train-строки кончается до теста
        after_test = [i for i in train if i >= f.test.stop]
        if after_test:
            assert min(after_test) - (f.test.stop - 1) > embargo
            assert min(after_test) - (f.test.stop - 1) > max(horizon, lookback)
    # тесты идут подряд без пересечений и упираются в конец оси
    assert [f.test.start for f in folds] == [n_bars - (n_splits - k) * test_size for k in range(n_splits)]
    assert folds[-1].test.stop == n_bars


# ------------------------------------------------------------ защита от неверных настроек


def test_purge_shorter_than_label_horizon_is_refused():
    with pytest.raises(ConfigError, match="утечка"):
        PurgedWalkForward(3, 10, label_horizon_bars=5, purge_bars=4)
    PurgedWalkForward(3, 10, label_horizon_bars=5, purge_bars=5)  # ровно горизонт - можно


def test_embargo_rules():
    with pytest.raises(ConfigError, match="ничего бы не делало"):
        PurgedWalkForward(3, 10, label_horizon_bars=0, purge_bars=0, embargo_bars=3)
    with pytest.raises(ConfigError, match="утечка"):  # эмбарго короче горизонта и lookback
        PurgedWalkForward(3, 10, label_horizon_bars=5, purge_bars=5, embargo_bars=4, allow_train_after_test=True)
    with pytest.raises(ConfigError, match="утечка"):
        PurgedWalkForward(
            3, 10, label_horizon_bars=2, purge_bars=2, embargo_bars=5, feature_lookback_bars=6,
            allow_train_after_test=True,
        )
    with pytest.raises(ConfigError, match="скользящее"):
        PurgedWalkForward(
            3, 10, label_horizon_bars=2, purge_bars=2, embargo_bars=2, train_size=50, allow_train_after_test=True
        )


@pytest.mark.parametrize("bad", [0, -1, 2.5, True, None])
def test_bad_integers(bad):
    with pytest.raises(ConfigError):
        PurgedWalkForward(bad, 10, label_horizon_bars=0, purge_bars=0)
    with pytest.raises(ConfigError):
        PurgedWalkForward(3, bad, label_horizon_bars=0, purge_bars=0)


def test_not_enough_data_fails_loudly():
    with pytest.raises(ConfigError, match="не помещаются"):
        list(wf().split(29))
    with pytest.raises(ConfigError, match="min_train_size"):  # первый фолд: до теста 30 - 5 purge = 25 < 40
        list(wf(purge_bars=5, min_train_size=40).split(60))
    assert len(list(wf(purge_bars=5, min_train_size=25).split(60))) == 3


def test_bars_for_rounds_up():
    assert bars_for(5 * MIN, MIN) == 5 and bars_for(5 * MIN + 1, MIN) == 6
    with pytest.raises(ConfigError):
        bars_for(0, MIN)


# ------------------------------------------------- кадры и утечка на настоящих метках


def decisions(n: int, symbols=("A", "B")) -> pl.DataFrame:
    """Решения через настоящий TimelineAligner: n минутных свечей на символ, цена - случайное блуждание."""
    rng = random.Random(3)
    parts = []
    for sym in symbols:
        px, closes = 100.0, []
        for _ in range(n):
            px *= 1 + rng.gauss(0, 0.001)
            closes.append(px)
        parts.append(pl.DataFrame(
            {
                "symbol": [sym] * n, "open_time": [m * MIN for m in range(n)], "close": closes,
                "avail_ts": [(m + 1) * MIN + TIMING.candle_publish_lag_ms for m in range(n)],
            },
            schema={"symbol": pl.String, "open_time": pl.Int64, "close": pl.Float64, "avail_ts": pl.Int64},
        ))
    return TimelineAligner(TIMING).decisions(pl.concat(parts).lazy(), MIN).collect()


HORIZON_BARS = 5


def labelled(n: int = 200):
    d = decisions(n)
    lb = LabelBuilder([LabelSpec(HORIZON_BARS * MIN)])
    full = lb.attach(d.lazy(), lb.build(d.lazy())).collect()
    return full, lb


def leaks(train: pl.DataFrame, test: pl.DataFrame, end_col: str) -> int:
    """Train-строки до теста, чья метка заканчивается не раньше первой тестовой строки."""
    t0 = test["asof_ts"].min()
    return train.filter((pl.col("asof_ts") < t0) & (pl.col(end_col) >= t0)).height


def test_without_purge_real_labels_leak_into_test():
    """Негативный контроль: проверка утечки действительно ловит перекрытие, когда purge не сделан."""
    full, lb = labelled()
    end = lb.end_names[0]
    naive = PurgedWalkForward(3, 20, label_horizon_bars=0, purge_bars=0)  # «горизонта нет» - ложное утверждение
    assert all(leaks(f.train, f.test, end) > 0 for f in naive.split_frame(full))


def test_purge_bars_equal_to_horizon_removes_all_label_overlap():
    full, lb = labelled()
    end = lb.end_names[0]
    h = bars_for(lb.max_horizon_ms, MIN)
    gen = PurgedWalkForward(3, 20, label_horizon_bars=h, purge_bars=h)
    folds = list(gen.split_frame(full))
    assert len(folds) == 3
    for f in folds:
        assert leaks(f.train, f.test, end) == 0
        # обе бумаги (symbol A и B) в одном баре попали в одну часть разбиения
        assert not set(f.train["asof_ts"]) & set(f.test["asof_ts"])
        assert f.train["symbol"].n_unique() == 2 and f.test["symbol"].n_unique() == 2
        assert f.test.height == 2 * 20 and f.test["asof_ts"].n_unique() == 20


def test_exact_label_purge_alone_removes_overlap():
    """Точный purge по {метка}_end_ts работает и без purge_bars."""
    full, lb = labelled()
    end = lb.end_names[0]
    naive = PurgedWalkForward(3, 20, label_horizon_bars=0, purge_bars=0)
    for exact, plain in zip(naive.split_frame(full, label_end_cols=[end]), naive.split_frame(full)):
        assert leaks(exact.train, exact.test, end) == 0
        assert exact.train.height < plain.train.height  # вырезаны строки с перекрывающимися метками


def test_embargo_on_frame_with_train_on_both_sides():
    full, lb = labelled()  # 200 баров на символ
    end = lb.end_names[0]
    axis = full["asof_ts"].unique().sort().to_list()
    gen = PurgedWalkForward(3, 10, label_horizon_bars=5, purge_bars=5, embargo_bars=5, allow_train_after_test=True)
    f0 = next(gen.split_frame(full, label_end_cols=[end]))  # первое окно [170, 180): справа ещё 15 баров train
    assert f0.fold.test == range(170, 180)
    assert f0.fold.embargoed == range(180, 185) and f0.fold.train == (range(0, 165), range(185, 200))
    right = f0.train.filter(pl.col("asof_ts") > axis[179])
    # ровно 5 баров эмбарго вырезано; точный purge (по end_ts) не трогает строки ПОСЛЕ теста, хотя их end_ts > начала теста
    assert right["asof_ts"].unique().sort().to_list() == axis[185:200]
    assert right.filter(pl.col(end) >= axis[170]).height > 0  # у них метки есть и они «позже начала теста»
    assert leaks(f0.train, f0.test, end) == 0
    left = f0.train.filter(pl.col("asof_ts") < axis[170])
    assert left["asof_ts"].max() == axis[164]  # purge 5 баров перед тестом


def test_split_frame_input_errors():
    full, lb = labelled(60)
    gen = PurgedWalkForward(2, 10, label_horizon_bars=0, purge_bars=0)
    with pytest.raises(DataError, match="нет колонок"):
        list(gen.split_frame(full.drop("asof_ts")))
    with pytest.raises(DataError, match="нет колонок"):
        list(gen.split_frame(full, label_end_cols=["нет_такой"]))
    with pytest.raises(DataError, match="null"):
        list(gen.split_frame(full.with_columns(asof_ts=pl.when(pl.col("asof_ts") > 0).then(None).otherwise(0).cast(pl.Int64))))
    with pytest.raises(ConfigError):
        list(gen.split_frame(full.lazy()))  # type: ignore[arg-type]
