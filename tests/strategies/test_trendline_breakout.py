"""Trendline_Breakout (2026-10-18, только Shadow): пробой и свип трендовой линии - ручные сценарии с известным ответом, фильтры геометрии
линии, зеркало лонг/шорт, тест обрезки R4 и паритет окна live R8 на случайных данных, проверка входа (R1)."""

import random

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from orchestrator.multi_strategies import TRENDLINE_BREAKOUT, StrategyTrendlineBreakout
from orchestrator.types import Direction, FeatureContext
from strategies.trendline_breakout import BREAKOUT, SWEEP, TrendlineParams, detect_trendline_breakout, signal_columns
from tests.features.test_smc import assert_truncation_invariant, random_walk
from utils.errors import ConfigError, DataError

STEP = 900_000
J = 55  # сигнальная свеча


def line_at(t: int, p2: float = 110.0) -> float:
    """Сопротивление через Swing High 110 (бар 20) и 106 (бар 40): -0.2 за бар; на баре 55 - 103.0."""
    return 106.0 + (106.0 - p2) / 20 * (t - 40)


def scenario(n: int = 60, *, p2: float = 110.0, bar: dict | None = None, **over) -> pl.DataFrame:
    """Свечи 102 +- 0.5 под нисходящей линией (сопротивление по двум свингам; опоры видны с баров подтверждения 25 и 45).
    Бар 55 - пробой: открытие 102.5, закрытие 103.8 (0.8 ATR над линией 103.0, CLV 0.93), объём x2.0, H4 Bullish."""
    cols = {"open_time": [i * STEP for i in range(n)], "open": [102.0] * n, "high": [102.5] * n, "low": [101.5] * n, "close": [102.0] * n,
            "atr": [1.0] * n, "vol_ratio": [1.0] * n, "htf_bias": [1] * n,
            "sh1_price": [None] * n, "sh1_time": [None] * n, "sh2_price": [None] * n, "sh2_time": [None] * n,
            "sl1_price": [None] * n, "sl1_time": [None] * n, "sl2_price": [None] * n, "sl2_time": [None] * n}
    cols["high"][20], cols["high"][40] = p2, 106.0
    for i in range(25, n):
        cols["sh1_price"][i], cols["sh1_time"][i] = p2, 20 * STEP
    for i in range(45, n):
        cols["sh1_price"][i], cols["sh1_time"][i], cols["sh2_price"][i], cols["sh2_time"][i] = 106.0, 40 * STEP, p2, 20 * STEP
    signal_bar = {"open": 102.5, "high": 103.9, "low": 102.4, "close": 103.8, "vol_ratio": 2.0}
    signal_bar.update(bar or {})
    for k, v in signal_bar.items():
        cols[k][J] = v
    for name, spec in over.items():
        if isinstance(spec, dict):
            for i, v in spec.items():
                cols[name][i] = v
        else:
            cols[name] = [spec] * n
    ints = ("open_time", "sh1_time", "sh2_time", "sl1_time", "sl2_time")
    return pl.DataFrame(cols, schema={k: (pl.Int64 if k in ints else pl.Int8 if k == "htf_bias" else pl.Float64) for k in cols})


SWEEP_BAR = {"open": 102.6, "high": 103.6, "low": 102.2, "close": 102.4}  # прокол линии 103.0 на 0.6 ATR, закрытие под ней, тень 71%


def mirror(df: pl.DataFrame, c: float = 200.0) -> pl.DataFrame:
    """Цены p -> c - p, сопротивление <-> поддержка, bias с обратным знаком."""
    return df.with_columns(
        (c - pl.col("open")).alias("open"), (c - pl.col("low")).alias("high"), (c - pl.col("high")).alias("low"), (c - pl.col("close")).alias("close"),
        (-pl.col("htf_bias")).cast(pl.Int8).alias("htf_bias"),
        (c - pl.col("sl1_price")).alias("sh1_price"), pl.col("sl1_time").alias("sh1_time"), (c - pl.col("sl2_price")).alias("sh2_price"),
        pl.col("sl2_time").alias("sh2_time"),
        (c - pl.col("sh1_price")).alias("sl1_price"), pl.col("sh1_time").alias("sl1_time"), (c - pl.col("sh2_price")).alias("sl2_price"),
        pl.col("sh2_time").alias("sl2_time"),
    )


def idx(out: pl.DataFrame, name: str) -> list[int]:
    return [i for i, v in enumerate(out[name].to_list()) if v]


def run(df: pl.DataFrame, params: TrendlineParams | None = None) -> pl.DataFrame:
    return detect_trendline_breakout(df, STEP, params)


def test_close_above_descending_resistance_gives_long_breakout():
    out = run(scenario())
    assert idx(out, "tl_long_signal") == [J] and idx(out, "tl_short_signal") == []
    r = out.row(J, named=True)
    assert r["tl_long_mode"] == BREAKOUT and r["tl_long_level"] == pytest.approx(line_at(J)) == pytest.approx(103.0)
    assert r["tl_long_slope_atr"] == pytest.approx(-0.2) and r["tl_long_span_bars"] == pytest.approx(20) and r["tl_long_age_bars"] == pytest.approx(15)
    assert r["tl_long_sl"] == pytest.approx(101.5 - 0.2) and r["tl_long_risk_atr"] == pytest.approx(103.8 - 101.3)
    assert (r["tl_long_anchor1_time"], r["tl_long_anchor1_price"], r["tl_long_anchor2_time"], r["tl_long_anchor2_price"]) == (20 * STEP, 110.0, 40 * STEP, 106.0)
    assert r["tl_long_touches"] == 0 and 0 < r["tl_long_quality"] < 1


def test_false_break_of_resistance_gives_short_sweep_with_stop_above_the_wick():
    out = run(scenario(bar=SWEEP_BAR, htf_bias=-1))
    assert idx(out, "tl_short_signal") == [J] and idx(out, "tl_long_signal") == []
    r = out.row(J, named=True)
    assert r["tl_short_mode"] == SWEEP and r["tl_short_level"] == pytest.approx(103.0)
    assert r["tl_short_sl"] == pytest.approx(103.6 + 0.2) and r["tl_short_risk_atr"] == pytest.approx(103.8 - 102.4)
    assert idx(run(scenario(bar=SWEEP_BAR, htf_bias=-1), TrendlineParams(enable_sweep=False)), "tl_short_signal") == []
    assert idx(run(scenario(bar=SWEEP_BAR, htf_bias=1)), "tl_short_signal") == []  # свип против бычьего H4 - запрет


def test_long_is_exact_mirror_of_short_for_both_modes():
    for bar, bias, side in ((None, 1, "long"), (SWEEP_BAR, -1, "short")):
        a, b = run(scenario(bar=bar, htf_bias=bias)), run(mirror(scenario(bar=bar, htf_bias=bias)))
        other = "short" if side == "long" else "long"
        assert idx(a, f"tl_{side}_signal") == idx(b, f"tl_{other}_signal") == [J]
        ra, rb = a.row(J, named=True), b.row(J, named=True)
        assert rb[f"tl_{other}_mode"] == ra[f"tl_{side}_mode"]
        assert rb[f"tl_{other}_level"] == pytest.approx(200 - ra[f"tl_{side}_level"]) and rb[f"tl_{other}_sl"] == pytest.approx(200 - ra[f"tl_{side}_sl"])
        assert rb[f"tl_{other}_slope_atr"] == pytest.approx(-ra[f"tl_{side}_slope_atr"])
        for k in ("risk_atr", "quality", "span_bars", "age_bars"):
            assert rb[f"tl_{other}_{k}"] == pytest.approx(ra[f"tl_{side}_{k}"]), k


@pytest.mark.parametrize(("name", "kw", "params"), [
    ("линия уже сломана закрытием (бар 45)", {"close": {45: 105.5}, "high": {45: 105.6}}, None),
    ("слишком крутая линия (-1.2 ATR/бар)", {"p2": 130.0}, None),
    ("почти горизонталь", {}, TrendlineParams(min_slope_atr=0.3)),
    ("восходящее «сопротивление»", {"p2": 104.0}, None),
    ("линия устарела", {}, TrendlineParams(max_age_bars=10)),
    ("пролёт короче минимума", {}, TrendlineParams(min_span_bars=25)),
    ("нужны доп. касания", {}, TrendlineParams(min_extra_touches=1)),
    ("нет объёма", {"bar": {"vol_ratio": 1.2}}, None),
    ("H4 медвежий", {"htf_bias": -1}, None),
    ("закрытие у линии", {"bar": {"close": 103.02}}, None),
    ("перерастянут", {"bar": {"close": 104.4, "high": 104.5}}, None),
    ("слабое закрытие", {"bar": {"high": 104.9}}, None),
    ("риск больше 3 ATR", {"low": {50: 100.0}}, None),
])
def test_each_filter_blocks_the_breakout(name, kw, params):
    over = dict(kw)
    p2 = over.pop("p2", 110.0)
    bar = over.pop("bar", None)
    assert idx(run(scenario(p2=p2, bar=bar, **over), params), "tl_long_signal") == [], name


def test_extra_touch_is_counted_and_raises_quality():
    base = run(scenario()).row(J, named=True)
    touched = run(scenario(high={30: line_at(30) - 0.1})).row(J, named=True)  # бар 30: хай в 0.1 ATR под линией (опоры 20 и 40 - дальше 2 баров)
    assert touched["tl_long_touches"] == 1 and touched["tl_long_quality"] > base["tl_long_quality"]
    assert idx(run(scenario(high={30: line_at(30) - 0.1}), TrendlineParams(min_extra_touches=1)), "tl_long_signal") == [J]


def test_ambiguous_bar_with_both_sides_gives_nothing():
    """Сужающийся треугольник: свеча с двумя длинными тенями прокалывает и сопротивление (103.0), и поддержку (100 -> 101, на баре 55 - 101.7)
    и закрывается внутри - это одновременно свип вверх (шорт) и свип вниз (лонг). Сторона неоднозначна - сигналов нет."""
    n = 60
    bar = {"open": 102.3, "high": 103.5, "low": 101.2, "close": 102.4}  # тени по 48% размаха
    df = scenario(n=n, bar=bar, htf_bias=0)
    p = TrendlineParams(min_wick_frac=0.4)
    assert idx(run(df, p), "tl_short_signal") == [J]  # без поддержки - чистый свип сопротивления
    sup = {"sl1_price": 101.0, "sl1_time": 41 * STEP, "sl2_price": 100.0, "sl2_time": 21 * STEP}
    df = df.with_columns(*[pl.Series(k, [v if i >= 45 else df[k][i] for i in range(n)], dtype=df[k].dtype) for k, v in sup.items()])
    alone = run(df.with_columns(pl.col("high").clip(upper_bound=102.9)), p)  # без прокола сопротивления - чистый свип поддержки
    assert idx(alone, "tl_long_signal") == [J] and alone.row(J, named=True)["tl_long_mode"] == SWEEP
    both = run(df, p)
    assert idx(both, "tl_long_signal") == [] and idx(both, "tl_short_signal") == []


def test_htf_contract_and_input_contract_r1():
    assert idx(run(scenario(htf_bias=0)), "tl_long_signal") == [J]
    nulls = scenario().with_columns(pl.lit(None, dtype=pl.Int8).alias("htf_bias"))
    assert idx(run(nulls), "tl_long_signal") == []
    with pytest.raises(DataError, match="htf_bias"):
        run(scenario().drop("htf_bias"))
    assert idx(run(scenario().drop("htf_bias"), TrendlineParams(htf_filter=False)), "tl_long_signal") == [J]
    with pytest.raises(DataError, match="sh2_price"):
        run(scenario().drop("sh2_price"))
    with pytest.raises(DataError, match="уже есть"):
        run(run(scenario()))
    with pytest.raises(ConfigError):
        detect_trendline_breakout(scenario(), 0)
    for bad in (dict(min_span_bars=0), dict(min_span_bars=100), dict(min_slope_atr=0.0), dict(min_clv=1.0), dict(min_wick_frac=1.0),
                dict(enable_sweep=1), dict(max_age_bars=2.5), dict(min_extra_touches=-1), dict(max_risk_atr=0.1)):
        with pytest.raises(ConfigError):
            TrendlineParams(**bad)


def _features(seed: int, n: int = 900) -> pl.DataFrame:
    rnd = random.Random(seed)
    f = add_features(random_walk(n, seed, 0.008), SmcParams(), interval_ms=STEP)
    return f.with_columns(pl.Series("htf_bias", [rnd.choice((-1, 0, 1)) for _ in range(n)], dtype=pl.Int8))


SOFT = TrendlineParams(volume_mult=0.8, sweep_volume_mult=0.8, min_clv=0.3, min_body_atr=0.1, max_risk_atr=6.0, min_wick_frac=0.3)


def test_soft_params_find_both_modes_on_random_data():
    out = run(_features(4, 4000), SOFT)
    modes = set(out["tl_long_mode"].drop_nulls().to_list()) | set(out["tl_short_mode"].drop_nulls().to_list())
    assert out["tl_long_signal"].sum() > 3 and out["tl_short_signal"].sum() > 3 and modes == {BREAKOUT, SWEEP}
    assert not (out["tl_long_signal"] & out["tl_short_signal"]).any()


@pytest.mark.parametrize("seed", [31, 32])
def test_truncation_invariant_r4(seed):
    df = _features(seed)
    rnd = random.Random(seed)
    assert_truncation_invariant(lambda d: run(d, SOFT), df, sorted(rnd.sample(range(df.height), 120)))


def test_live_window_of_300_bars_equals_full_history_r8():
    """Глубина проверки линии (144 бара) и свинги помещаются в окно 300: значения на последней строке окна совпадают с полной историей."""
    df = _features(6, 1500)
    full = run(df, SOFT)
    cols = [c for s in ("long", "short") for c in signal_columns(s)]
    for t in range(400, df.height, 29):
        live = run(df.slice(t - 299, 300), SOFT).row(299, named=True)
        whole = full.row(t, named=True)
        assert {c: live[c] for c in cols} == {c: whole[c] for c in cols}, t


def test_strategy_wraps_both_modes_into_market_signals():
    for bar, bias, direction, mode, label in ((None, 1, Direction.LONG, BREAKOUT, "пробой линии"), (SWEEP_BAR, -1, Direction.SHORT, SWEEP, "свип линии")):
        df = scenario(n=J + 1, bar=bar, htf_bias=bias)
        (sig,) = StrategyTrendlineBreakout().detect(FeatureContext("SOLUSDT", "15m", STEP, df, J * STEP + STEP + 500))
        assert sig.strategy == TRENDLINE_BREAKOUT and sig.direction == direction and sig.entry == df["close"][-1] and sig.tp is None
        assert [g["type"] for g in sig.geometry] == ["line", "marker", "marker", "line"] and sig.geometry[2]["label"] == label
        line = sig.geometry[0]
        assert (line["t1"], line["p1"], line["t2"]) == (20 * STEP, 110.0, J * STEP) and line["p2"] == pytest.approx(103.0)
        assert sig.meta["mode"] == mode and sig.meta["slope_atr"] == pytest.approx(-0.2) and 0 < sig.meta["quality"] < 1
