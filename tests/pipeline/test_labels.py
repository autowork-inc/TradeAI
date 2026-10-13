"""LabelBuilder: метки смотрят вперёд ровно на horizon_ms, не пересекают разрывы и не утекают в признаки."""

import math
import random

import polars as pl
import pytest

from pipeline.features.base import (
    Feature,
    FeatureBuilder,
    FeatureBuildError,
    compile_features,
    source_columns,
)
from pipeline.labels.base import LabelBuilder, LabelError, LabelSpec, ReturnKind
from pipeline.sync.aligner import TimelineAligner
from pipeline.types import LABEL_PREFIX, TimingConfig
from utils.errors import ConfigError, DataError

MIN = 60_000
LAG = 1_050  # asof_ts - close_time свечи: candle_publish_lag_ms + decision_compute_ms
TIMING = TimingConfig()


def decisions(closes, *, symbol="A", minutes=None) -> pl.DataFrame:
    """Решения через настоящий TimelineAligner. minutes - номера минут свечей (пропуск = разрыв)."""
    minutes = list(range(len(closes))) if minutes is None else list(minutes)
    candles = pl.DataFrame(
        {
            "symbol": [symbol] * len(closes),
            "open_time": [m * MIN for m in minutes],
            "close": closes,
            "avail_ts": [(m + 1) * MIN + TIMING.candle_publish_lag_ms for m in minutes],
        },
        schema={"symbol": pl.String, "open_time": pl.Int64, "close": pl.Float64, "avail_ts": pl.Int64},
    )
    return TimelineAligner(TIMING).decisions(candles.lazy(), MIN).collect()


def labels(df: pl.DataFrame, *specs: LabelSpec) -> pl.DataFrame:
    return LabelBuilder(specs or [LabelSpec(3 * MIN)]).build(df.lazy()).collect()


def spec(minutes: int, **kw) -> LabelSpec:
    return LabelSpec(minutes * MIN, **kw)


# ------------------------------------------------------------------ значения метки


def test_log_return_looks_exactly_horizon_ahead():
    closes = [100.0, 101.0, 103.0, 102.0, 105.0, 104.0]
    d = decisions(closes)
    out = labels(d, spec(3))
    name = spec(3).name
    expected = [pytest.approx(math.log(closes[k + 3] / closes[k])) for k in range(3)] + [None] * 3
    assert out[name].to_list() == expected  # хвост: будущего ещё нет, а не 0 и не протянутая цена


def test_simple_return():
    d = decisions([100.0, 110.0, 99.0])
    s = spec(2, kind=ReturnKind.SIMPLE)
    out = labels(d, s)
    assert out[s.name].to_list() == [pytest.approx(-0.01), None, None]


def test_end_ts_is_knowledge_time_of_exit_row():
    d = decisions([1.0, 2.0, 3.0, 4.0, 5.0])
    s = spec(2)
    out = labels(d, s)
    asof = d["asof_ts"].to_list()
    assert out[s.end_name].to_list() == [asof[2], asof[3], asof[4], None, None]
    # Метка строго «живёт» после решения: purge опирается на end_ts > asof_ts.
    rows = out.drop_nulls(s.end_name)
    assert (rows[s.end_name] > rows["asof_ts"]).all()
    # null-метка не имеет end_ts (и наоборот).
    assert out[s.name].is_null().to_list() == out[s.end_name].is_null().to_list()


def test_future_price_changes_only_its_own_label():
    base = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    bumped = list(base)
    bumped[4] = 200.0
    s = spec(2)
    a, b = labels(decisions(base), s)[s.name], labels(decisions(bumped), s)[s.name]
    changed = [i for i in range(len(base)) if a[i] != b[i]]
    assert changed == [2]  # цена бара 4 - выход только строки 2; строка 4 сама без выхода (null в обоих)


# ---------------------------------------------------------------------- разрывы


def test_gap_inside_horizon_gives_null_even_if_exit_bar_exists():
    # Минуты 0,1,[2 пропущена],3,4,5: у строки минуты 0 выходной бар (3) есть, но в другом сегменте.
    closes = [100.0, 101.0, 103.0, 104.0, 105.0]
    d = decisions(closes, minutes=[0, 1, 3, 4, 5])
    assert d["segment_id"].to_list() == [0, 0, 1, 1, 1]
    out = labels(d, spec(3))
    assert out[spec(3).name].to_list() == [None, None, None, None, None]  # 0->3 и 1->4 пересекают дыру


def test_gap_does_not_poison_rows_whose_horizon_stays_inside_segment():
    closes = [100.0, 101.0, 103.0, 104.0, 105.0, 106.0, 108.0]
    d = decisions(closes, minutes=[0, 1, 3, 4, 5, 6, 7])  # дыра на минуте 2
    s = spec(2)
    out = labels(d, s)[s.name].to_list()
    # сегмент 0: минуты 0,1 - выхода нет; сегмент 1: минуты 3..7 - строки 3,4,5 имеют выход
    assert out[:2] == [None, None]
    assert out[2:5] == [pytest.approx(math.log(105 / 103)), pytest.approx(math.log(106 / 104)),
                        pytest.approx(math.log(108 / 105))]
    assert out[5:] == [None, None]


def test_symbols_are_isolated():
    a = decisions([100.0, 110.0, 121.0], symbol="A")
    b = decisions([1.0, 1.0, 1.0, 1.0], symbol="B", minutes=[10, 11, 12, 13])
    d = pl.concat([a, b]).sort("symbol", "open_time")
    s = spec(1)
    out = labels(d, s).filter(pl.col("symbol") == "A")[s.name].to_list()
    assert out == [pytest.approx(math.log(1.1)), pytest.approx(math.log(1.1)), None]
    assert labels(d, s).filter(pl.col("symbol") == "B")[s.name].to_list() == [0.0, 0.0, 0.0, None]


def test_label_uses_time_key_not_row_offset():
    """Выброшенная строка внутри сегмента (например, фильтр по book_ok) не сдвигает горизонт."""
    closes = [100.0, 101.0, 102.0, 103.0, 104.0]
    d = decisions(closes).filter(pl.col("open_time") != 2 * MIN)  # segment_id остался прежним
    s = spec(3)
    out = labels(d, s).sort("asof_ts")
    by_open = dict(zip(d.sort("open_time")["open_time"] // MIN, out[s.name].to_list()))
    assert by_open[0] == pytest.approx(math.log(103 / 100))  # сдвиг на 3 строки дал бы 104/100
    assert by_open[1] == pytest.approx(math.log(104 / 101))
    assert by_open[3] is None and by_open[4] is None


def test_row_order_of_input_does_not_matter():
    closes = [100.0 + i * i % 7 for i in range(30)]
    d = decisions(closes, minutes=[m for m in range(31) if m != 12][:30])
    shuffled = d.sample(fraction=1.0, shuffle=True, seed=7)
    s = spec(3)
    assert labels(d, s).equals(labels(shuffled, s))


# --------------------------------------------------------------------- мусор цен


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), 0.0, -5.0])
def test_bad_price_gives_null_at_entry_and_exit(bad):
    closes = [100.0, 101.0, bad, 103.0, 104.0, 105.0]
    s = spec(2)
    out = labels(decisions(closes), s)[s.name].to_list()
    # строка 0 (выход = плохая цена) и строка 2 (вход = плохая цена) - null; остальные валидны
    assert out == [None, pytest.approx(math.log(103 / 101)), None, pytest.approx(math.log(105 / 103)), None, None]


# ---------------------------------------------------------------- несколько меток


def test_multiple_horizons_and_columns():
    d = decisions([100.0 + i for i in range(10)])
    b = LabelBuilder([spec(1), spec(4), spec(4, kind=ReturnKind.SIMPLE)])
    out = b.build(d.lazy()).collect()
    assert out.columns == ["symbol", "asof_ts", *[c for s in b.specs for c in (s.name, s.end_name)]]
    assert out.height == d.height
    assert [out[n].null_count() for n in b.names] == [1, 4, 4]
    assert b.max_horizon_ms == 4 * MIN
    assert all(n.startswith(LABEL_PREFIX) for n in b.names + b.end_names)
    assert all(n.endswith("_ts") for n in b.end_names)
    assert out["symbol"].to_list() == sorted(out["symbol"].to_list())
    assert out["asof_ts"].is_sorted()


def test_custom_price_column():
    d = decisions([1.0, 1.0, 1.0]).with_columns(mark=pl.Series([10.0, 20.0, 40.0]))
    s = spec(1, price_col="mark")
    assert labels(d, s)[s.name].to_list() == [pytest.approx(math.log(2)), pytest.approx(math.log(2)), None]


# -------------------------------------------------------------- изоляция от признаков


def test_features_cannot_read_or_create_labels():
    s = spec(3)
    d = decisions([100.0 + i for i in range(6)]).with_columns(x=pl.lit(1.0))
    leaked = d.with_columns(pl.lit(0.5).alias(s.name), pl.lit(1).alias(s.end_name))
    assert source_columns(leaked.lazy()) == frozenset({"close", "x"})

    with pytest.raises(FeatureBuildError, match="Метки"):
        FeatureBuilder({"x", s.name})

    class Bad(Feature):
        name = "bad"

        def build(self, fb):
            return {s.name: fb.col("x")}

    with pytest.raises(FeatureBuildError):
        compile_features([Bad()], {"x"})


def test_build_returns_only_keys_and_labels():
    d = decisions([100.0, 101.0, 102.0, 103.0]).with_columns(x=pl.lit(1.0))
    out = labels(d, spec(1))
    assert set(out.columns) == {"symbol", "asof_ts", spec(1).name, spec(1).end_name}


# --------------------------------------------------------------------- attach


def test_attach_joins_exactly_on_key_and_can_drop_unlabeled():
    d = decisions([100.0, 101.0, 102.0, 103.0, 104.0]).with_columns(x=pl.int_range(pl.len()).cast(pl.Float64))
    b = LabelBuilder([spec(2)])
    lab = b.build(d.lazy())
    feats = d.select("symbol", "asof_ts", "x").lazy()

    full = b.attach(feats, lab).collect()
    assert full.height == 5 and full["x"].to_list() == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert full[b.names[0]].null_count() == 2

    train = b.attach(feats, lab, drop_unlabeled=True).collect()
    assert train["x"].to_list() == [0.0, 1.0, 2.0]
    assert train[b.names[0]].null_count() == 0


def test_attach_is_left_join_over_features_rows():
    d = decisions([100.0, 101.0, 102.0, 103.0])
    b = LabelBuilder([spec(1)])
    feats = d.select("symbol", "asof_ts").filter(pl.col("asof_ts") != d["asof_ts"][1]).lazy()
    assert b.attach(feats, b.build(d.lazy())).collect().height == 3


def test_attach_rejects_clash_and_duplicate_keys():
    d = decisions([100.0, 101.0, 102.0])
    b = LabelBuilder([spec(1)])
    lab = b.build(d.lazy())
    with pytest.raises(LabelError, match="уже есть"):
        b.attach(d.with_columns(pl.lit(1.0).alias(b.names[0])).lazy(), lab)
    dup = pl.concat([d.select("symbol", "asof_ts"), d.select("symbol", "asof_ts")]).lazy()
    with pytest.raises(pl.exceptions.PolarsError):
        b.attach(dup, lab).collect()


def test_duplicate_decision_rows_are_not_silently_multiplied():
    d = decisions([100.0, 101.0, 102.0])
    with pytest.raises(pl.exceptions.PolarsError):
        labels(pl.concat([d, d]), spec(1))


# ------------------------------------------------------------------- валидация


def test_missing_columns_are_reported():
    d = decisions([1.0, 2.0, 3.0]).drop("segment_id")
    with pytest.raises(DataError, match="segment_id"):
        labels(d, spec(1))
    with pytest.raises(DataError, match="nope"):
        labels(decisions([1.0, 2.0]), spec(1, price_col="nope"))


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(horizon_ms=0),
        dict(horizon_ms=-MIN),
        dict(horizon_ms=True),
        dict(horizon_ms=1.5 * MIN),
        dict(horizon_ms=90_000),  # не кратен минутной сетке: все метки были бы null
        dict(horizon_ms=MIN, interval_ms=0),
        dict(horizon_ms=MIN, price_col=""),
        dict(horizon_ms=MIN, price_col="y_close"),
        dict(horizon_ms=MIN, kind="percent"),
    ],
)
def test_spec_rejects_invalid(kwargs):
    with pytest.raises((ConfigError, ValueError)):
        LabelSpec(**kwargs)


def test_builder_rejects_empty_duplicate_and_foreign_specs():
    with pytest.raises(ConfigError):
        LabelBuilder([])
    with pytest.raises(ConfigError, match="повторяются"):
        LabelBuilder([spec(1), spec(1)])
    with pytest.raises(ConfigError):
        LabelBuilder(["close"])  # type: ignore[list-item]


def test_larger_random_frame_matches_naive_reference():
    rng = random.Random(3)
    minutes = [m for m in range(400) if rng.random() > 0.05]
    closes = [100.0 * math.exp(rng.gauss(0, 0.01)) for _ in minutes]
    d = decisions(closes, minutes=minutes)
    s = spec(5)
    out = labels(d, s).join(d.select("symbol", "asof_ts", "open_time"), on=["symbol", "asof_ts"])
    price = dict(zip(minutes, closes))
    present = set(minutes)
    for open_time, y in zip(out["open_time"] // MIN, out[s.name]):
        m = int(open_time)
        contiguous = all(k in present for k in range(m, m + 6))
        if contiguous:
            assert y == pytest.approx(math.log(price[m + 5] / price[m]))
        else:
            assert y is None

