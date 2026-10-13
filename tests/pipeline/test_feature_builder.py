"""FeatureBuilder: запрещённое отсекается при конструировании; разрешённое каузально."""

import math
import random

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pipeline.features.base import (
    Feature,
    FeatureBuilder,
    FeatureBuildError,
    FeatureExpr,
    ForbiddenOperation,
    apply_features,
    compile_features,
    source_columns,
)

SRC = {"x", "y"}


def fb() -> FeatureBuilder:
    return FeatureBuilder(SRC)


class F(Feature):
    """Признак из словаря готовых узлов (для тестов)."""

    def __init__(self, name, outputs):
        self.name = name
        self._outputs = outputs

    def build(self, fb):
        return self._outputs(fb) if callable(self._outputs) else self._outputs


def frame(rows, symbol="A") -> pl.DataFrame:
    """rows: список (x, y | None, segment_id). asof_ts = 1000 * номер строки."""
    return pl.DataFrame(
        {
            "symbol": [symbol] * len(rows),
            "segment_id": [r[2] for r in rows],
            "asof_ts": [1000 * (i + 1) for i in range(len(rows))],
            "x": [r[0] for r in rows],
            "y": [r[1] for r in rows],
        },
        schema={"symbol": pl.String, "segment_id": pl.Int64, "asof_ts": pl.Int64, "x": pl.Float64, "y": pl.Float64},
    )


def run(df: pl.DataFrame, outputs) -> pl.DataFrame:
    compiled = compile_features([F("t", outputs)], SRC)
    return apply_features(df.lazy(), compiled).collect()


def col_of(df, name):
    return df[name].to_list()


# ------------------------------------------------ запреты на этапе конструирования


@pytest.mark.parametrize("n", [0, -1, -100, 1.5, True, "1", None])
def test_shift_requires_positive_int_at_construction(n):
    with pytest.raises(FeatureBuildError):
        fb().col("x").shift(n)  # ошибка при построении узла, данных ещё нет


@pytest.mark.parametrize("w", [0, -3, 2.5, True, None])
def test_rolling_window_must_be_positive_int(w):
    for method in ("rolling_mean", "rolling_std", "rolling_max", "rolling_min"):
        with pytest.raises(FeatureBuildError):
            getattr(fb().col("x"), method)(w)


def test_rolling_std_needs_two_and_ewm_span_needs_two():
    with pytest.raises(FeatureBuildError):
        fb().col("x").rolling_std(1)
    with pytest.raises(FeatureBuildError):
        fb().col("x").ewm_mean(1)
    fb().col("x").rolling_std(2)
    fb().col("x").ewm_mean(2)


def test_center_and_other_kwargs_do_not_exist():
    with pytest.raises(TypeError):
        fb().col("x").rolling_mean(3, center=True)
    with pytest.raises(TypeError):
        fb().col("x").rolling_mean(window_size=3, min_samples=1)
    with pytest.raises(TypeError):
        fb().col("x").ewm_mean(span=5, adjust=True)


@pytest.mark.parametrize("name", [
    "mean", "sum", "std", "var", "max", "min", "median", "quantile", "first", "last", "count",
    "rank", "sort", "reverse", "cum_sum", "cum_max", "forward_fill", "backward_fill", "fill_null",
    "interpolate", "over", "alias", "pipe", "map_batches", "rolling_sum", "rolling_median",
    "shift_and_fill", "ewm_std", "rolling_mean_by", "diff_back", "meta", "n_unique", "arg_max",
])
def test_global_and_unsafe_methods_do_not_exist(name):
    node = fb().col("x")
    with pytest.raises(ForbiddenOperation):
        getattr(node, name)
    assert not hasattr(node, name)  # AttributeError-совместимо: hasattr работает штатно


def test_unknown_or_reserved_columns_rejected():
    with pytest.raises(FeatureBuildError):
        fb().col("nope")
    with pytest.raises(FeatureBuildError):
        FeatureBuilder({"x", "asof_ts"})  # служебные колонки не могут быть источниками
    with pytest.raises(FeatureBuildError):
        FeatureBuilder({"x", "src_max_ts"})
    with pytest.raises(FeatureBuildError):
        fb().col(pl.col("x"))  # только имя, не выражение


def test_raw_polars_objects_are_rejected_everywhere():
    node = fb().col("x")
    for bad in (pl.col("x"), pl.Series([1.0]), "1", None, True, float("nan"), float("inf")):
        with pytest.raises(FeatureBuildError):
            node + bad
        with pytest.raises(FeatureBuildError):
            bad + node if not isinstance(bad, (pl.Expr, pl.Series)) else node.__radd__(bad)


def test_feature_cannot_return_raw_expr():
    with pytest.raises(FeatureBuildError, match="сырые pl.Expr запрещены"):
        compile_features([F("bad", {"f": pl.col("x").rolling_mean(3, center=True)})], SRC)
    with pytest.raises(FeatureBuildError):
        compile_features([F("bad", {"f": pl.col("x").shift(-1)})], SRC)
    with pytest.raises(FeatureBuildError):
        compile_features([F("empty", {})], SRC)


def test_feature_expr_cannot_be_constructed_directly_and_is_immutable():
    with pytest.raises(FeatureBuildError):
        FeatureExpr(object(), pl.col("x"), 0, frozenset())
    node = fb().col("x")
    with pytest.raises(FeatureBuildError):
        node.anything = 1
    with pytest.raises(FeatureBuildError):
        if node:
            pass
    with pytest.raises(ForbiddenOperation):
        node > 1


def test_output_names_are_validated():
    ok = {"a": fb().col("x")}
    compile_features([F("ok", ok)], SRC)
    for bad_name in ("asof_ts", "_hidden", "has space", "symbol"):
        with pytest.raises(FeatureBuildError):
            compile_features([F("bad", {bad_name: fb().col("x")})], SRC)
    with pytest.raises(FeatureBuildError, match="занято"):
        compile_features([F("a", {"dup": fb().col("x")}), F("b", {"dup": fb().col("y")})], SRC)
    df = frame([(1.0, 1.0, 0)])
    with pytest.raises(FeatureBuildError, match="пересекаются"):
        apply_features(df.lazy(), compile_features([F("c", {"x": fb().col("y")})], SRC))


# ------------------------------------------------------------------ семантика


def test_shift_is_lag_within_symbol_and_segment():
    df = pl.concat([
        frame([(1.0, None, 0), (2.0, None, 0), (3.0, None, 1), (4.0, None, 1)], "A"),
        frame([(10.0, None, 0), (20.0, None, 0)], "B"),
    ])
    out = run(df, lambda b: {"lag1": b.col("x").shift(1), "lag2": b.col("x").shift(2)})
    a = out.filter(pl.col("symbol") == "A")
    b = out.filter(pl.col("symbol") == "B")
    assert col_of(a, "lag1") == [None, 1.0, None, 3.0]  # новый сегмент: прошлое не берётся
    assert col_of(a, "lag2") == [None, None, None, None]
    assert col_of(b, "lag1") == [None, 10.0]  # данные символа A в B не просачиваются


def test_rolling_includes_current_bar_and_is_null_until_window_full():
    df = frame([(1.0, None, 0), (2.0, None, 0), (4.0, None, 0), (8.0, None, 0)])
    out = run(df, lambda b: {
        "m": b.col("x").rolling_mean(3), "s": b.col("x").rolling_std(2),
        "hi": b.col("x").rolling_max(2), "lo": b.col("x").rolling_min(3),
    })
    assert col_of(out, "m") == [None, None, 7 / 3, 14 / 3]
    assert col_of(out, "hi") == [None, 2.0, 4.0, 8.0]
    assert col_of(out, "lo") == [None, None, 1.0, 2.0]
    std = col_of(out, "s")
    assert std[0] is None and std[1] == pytest.approx(math.sqrt(0.5))
    assert std[3] == pytest.approx(math.sqrt(8.0))  # (4, 8): ddof=1


def test_rolling_does_not_cross_segment_gap():
    # Между строками 2 и 3 разрыв сетки (segment_id 0 -> 1): окно не склеивает сегменты.
    df = frame([(1.0, None, 0), (2.0, None, 0), (100.0, None, 1), (200.0, None, 1), (300.0, None, 1)])
    out = run(df, lambda b: {"m": b.col("x").rolling_mean(2)})
    assert col_of(out, "m") == [None, 1.5, None, 150.0, 250.0]


def test_null_input_makes_window_null_without_filling():
    df = frame([(1.0, None, 0), (None, None, 0), (3.0, None, 0), (4.0, None, 0), (5.0, None, 0)])
    out = run(df, lambda b: {"m": b.col("x").rolling_mean(2)})
    assert col_of(out, "m") == [None, None, None, 3.5, 4.5]


def test_ewm_mean_matches_recursion_and_masks_null_input():
    xs = [1.0, 2.0, 4.0, 3.0, 5.0, 9.0]
    df = frame([(v, None, 0) for v in xs])
    out = run(df, lambda b: {"e": b.col("x").ewm_mean(3)})
    alpha, ema, expected = 2 / (3 + 1), None, []
    for i, v in enumerate(xs):
        ema = v if ema is None else alpha * v + (1 - alpha) * ema
        expected.append(ema if i >= 2 else None)  # до span=3 наблюдений - null
    got = col_of(out, "e")
    assert got[:2] == [None, None]
    assert got[2:] == pytest.approx(expected[2:])

    holey = frame([(1.0, None, 0), (2.0, None, 0), (3.0, None, 0), (None, None, 0), (5.0, None, 0)])
    res = col_of(run(holey, lambda b: {"e": b.col("x").ewm_mean(3)}), "e")
    assert res[3] is None  # где вход null, устаревшее среднее вперёд не протягивается


def test_safe_arithmetic_yields_null_instead_of_inf_or_nan():
    df = frame([(0.0, 0.0, 0), (4.0, 2.0, 0), (-1.0, 0.0, 0), (float("nan"), 1.0, 0), (float("inf"), 1.0, 0)])
    out = run(df, lambda b: {
        "div": b.col("x") / b.col("y"),
        "rdiv": 8.0 / b.col("y"),
        "log": b.col("x").log(),
        "sqrt": b.col("x").sqrt(),
        "mix": 1 - b.col("x") * 2 + -b.col("y") + abs(b.col("x")),
    })
    assert col_of(out, "div") == [None, 2.0, None, None, None]  # 0/0, -1/0 -> null; NaN/inf -> null
    assert col_of(out, "rdiv") == [None, 4.0, None, 8.0, 8.0]
    log = col_of(out, "log")
    assert log[0] is None and log[2] is None and log[3] is None and log[1] == pytest.approx(math.log(4.0))
    assert col_of(out, "sqrt")[:3] == [0.0, 2.0, None]
    assert col_of(out, "mix")[1] == pytest.approx(1 - 8.0 - 2.0 + 4.0)
    for name in ("div", "rdiv", "log", "sqrt", "mix"):
        assert all(v is None or math.isfinite(v) for v in col_of(out, name))


def test_diff_and_pct_change_are_built_from_lags():
    df = frame([(2.0, None, 0), (3.0, None, 0), (6.0, None, 0)])
    out = run(df, lambda b: {"d": b.col("x").diff(1), "p": b.col("x").pct_change(1)})
    assert col_of(out, "d") == [None, 1.0, 3.0]
    assert col_of(out, "p") == [None, 0.5, 1.0]


def test_lookback_is_tracked_through_composition():
    b = fb()
    x, y = b.col("x"), b.col("y")
    assert x.lookback_bars == 0
    assert x.shift(3).lookback_bars == 3
    assert x.shift(3).rolling_mean(5).lookback_bars == 7
    assert x.rolling_std(10).diff(2).lookback_bars == 11
    assert (x.shift(2) + y.rolling_max(20)).lookback_bars == 19
    assert x.ewm_mean(10).lookback_bars == 100  # эффективная память EWM = 10 спанов
    assert (x.shift(3) / y).columns == frozenset({"x", "y"})
    compiled = compile_features([F("f", {"a": x.shift(3), "b": y.rolling_mean(30)})], SRC)
    assert compiled[0].lookback.bars_1m == 29
    assert compiled[0].columns == frozenset({"x", "y"})


def test_apply_features_sorts_input_and_result_is_order_independent():
    rows = [(float(i * i), None, 0) for i in range(1, 9)]
    df = frame(rows)
    shuffled = df.sample(fraction=1.0, shuffle=True, seed=1)
    feats = lambda b: {"m": b.col("x").rolling_mean(3), "l": b.col("x").shift(1)}  # noqa: E731
    assert_frame_equal(run(df, feats), run(shuffled, feats))


def test_source_columns_excludes_service_and_non_numeric():
    lf = pl.DataFrame({
        "symbol": ["A"], "segment_id": [0], "asof_ts": [1], "src_max_ts": [0], "open_time": [0],
        "avail_ts": [0], "h1_avail_ts": [0], "book_ok": [True], "avail_source": ["watermark"],
        "close": [1.0], "px_last": [1.0], "n": [3], "flag": [True],
    }).lazy()
    assert source_columns(lf) == frozenset({"close", "px_last", "n", "flag"})


# --------------------------------------- каузальность построенных признаков (свойство)


def _random_frame(seed: int) -> pl.DataFrame:
    rng = random.Random(seed)
    parts = []
    for symbol in ("A", "B"):
        rows, seg = [], 0
        for _ in range(rng.randint(60, 90)):
            if rng.random() < 0.08:
                seg += 1  # разрыв сетки
            x = None if rng.random() < 0.05 else rng.uniform(-5, 5)
            y = None if rng.random() < 0.05 else rng.choice([0.0, rng.uniform(0.5, 3)])
            rows.append((x, y, seg))
        parts.append(frame(rows, symbol))
    return pl.concat(parts)


COMPOSITE = lambda b: {  # noqa: E731
    "f1": (b.col("x") - b.col("x").shift(2)) / b.col("x").rolling_std(5),
    "f2": (b.col("x") * b.col("y")).ewm_mean(4) + b.col("y").rolling_max(3),
    "f3": (b.col("x").rolling_mean(3) - b.col("x").rolling_mean(8)).pct_change(1).log(),
    "f4": b.col("y").rolling_min(4).shift(3) - 1.0 / (b.col("x").sqrt() + 1.0),
}


@pytest.mark.parametrize("seed", range(8))
def test_features_are_prefix_invariant_and_ignore_poisoned_future(seed):
    """P(D)[asof_ts <= c] == P(D[asof_ts <= c]) и не меняется, если будущее заменить мусором."""
    df = _random_frame(seed)
    full = run(df, COMPOSITE)
    rng = random.Random(seed)
    for _ in range(4):
        cut = rng.randint(5_000, 80_000)
        prefix = df.filter(pl.col("asof_ts") <= cut)
        poisoned = df.with_columns(
            x=pl.when(pl.col("asof_ts") > cut).then(pl.lit(1e9)).otherwise(pl.col("x")),
            y=pl.when(pl.col("asof_ts") > cut).then(pl.lit(None, dtype=pl.Float64)).otherwise(pl.col("y")),
        )
        expected = full.filter(pl.col("asof_ts") <= cut)
        by_prefix = run(prefix, COMPOSITE)
        by_poison = run(poisoned, COMPOSITE).filter(pl.col("asof_ts") <= cut)
        assert_frame_equal(by_prefix, expected, check_exact=False, abs_tol=1e-9)
        assert_frame_equal(by_poison, expected, check_exact=False, abs_tol=1e-9)
