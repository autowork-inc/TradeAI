"""Horizontal_Breakout (2026-10-18, только Shadow): ручные сценарии с известным ответом (сигнал есть / нет по каждому фильтру), зеркало
лонг/шорт, тест обрезки R4 и паритет окна live R8 на случайных данных, проверка входа (R1)."""

import random

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from orchestrator.multi_strategies import HORIZONTAL_BREAKOUT, StrategyHorizontalBreakout
from orchestrator.types import Direction, FeatureContext
from strategies.horizontal_breakout import HorizontalParams, detect_horizontal_breakout, signal_columns
from tests.features.test_smc import assert_truncation_invariant, random_walk
from utils.errors import ConfigError, DataError

STEP = 900_000
J = 54  # пробойная свеча; коробка - бары 30..53 (window = 24)
TOUCH_BARS = (32, 38, 44, 50)


def scenario(n: int = 60, **over) -> pl.DataFrame:
    """Коробка 98.5 - 101.0 (2.5 ATR при ATR = 1): хаи 100.6, четыре касания 101.0 (бары 32, 38, 44, 50), лой 98.5 на баре 31; закрытия
    чередуются 99.8 / 100.2 (ER ~ 0.02). Бар 54: открытие 100.4, закрытие 101.6 (над уровнем, CLV 0.93), объём x2.5, H4 Bullish."""
    o = [100.0] * n
    c = [100.0] * n
    h = [100.6] * n
    lo = [99.5] * n
    for i in range(30, J):
        c[i] = 99.8 if i % 2 else 100.2
    for i in TOUCH_BARS:
        h[i] = 101.0
    lo[31] = 98.5
    o[J], h[J], lo[J], c[J] = 100.4, 101.7, 100.3, 101.6
    vol = [1.0] * n
    vol[J] = 2.5
    cols = {"open_time": [i * STEP for i in range(n)], "open": o, "high": h, "low": lo, "close": c, "atr": [1.0] * n, "vol_ratio": vol,
            "htf_bias": [1] * n}
    for name, spec in over.items():
        if isinstance(spec, dict):
            for i, v in spec.items():
                cols[name][i] = v
        else:
            cols[name] = [spec] * n
    return pl.DataFrame(cols, schema={k: (pl.Int64 if k == "open_time" else pl.Int8 if k == "htf_bias" else pl.Float64) for k in cols})


def mirror(df: pl.DataFrame, c: float = 200.0) -> pl.DataFrame:
    """Цены p -> c - p, high <-> low, H4 bias с обратным знаком: лонг-сценарий становится шорт-сценарием."""
    return df.with_columns((c - pl.col("open")).alias("open"), (c - pl.col("low")).alias("high"), (c - pl.col("high")).alias("low"),
                           (c - pl.col("close")).alias("close"), (-pl.col("htf_bias")).cast(pl.Int8).alias("htf_bias"))


def idx(out: pl.DataFrame, name: str) -> list[int]:
    return [i for i, v in enumerate(out[name].to_list()) if v]


def test_breakout_of_dense_box_gives_long_with_level_stop_and_quality():
    out = detect_horizontal_breakout(scenario())
    assert idx(out, "hb_long_signal") == [J] and idx(out, "hb_short_signal") == []
    r = out.row(J, named=True)
    assert r["hb_box_hi"] == 101.0 and r["hb_box_lo"] == 98.5 and r["hb_box_atr"] == pytest.approx(2.5)
    assert r["hb_long_level"] == 101.0 and r["hb_long_touches"] == 4 and r["hb_long_touch_span"] == 18  # касания на смещениях 4 .. 22
    assert r["hb_long_sl"] == pytest.approx(99.5 - 0.2)  # минимум баров 48..54 минус 0.2 ATR
    assert r["hb_long_risk_atr"] == pytest.approx(101.6 - 99.3)
    assert r["hb_long_clv"] == pytest.approx(1.3 / 1.4) and r["hb_efficiency"] < 0.05
    assert 0.5 < r["hb_long_quality"] < 1.0
    assert out.row(J - 1, named=True)["hb_long_level"] is None  # значения - только на строке сигнала


def test_long_is_exact_mirror_of_short():
    lg, sh = detect_horizontal_breakout(scenario()), detect_horizontal_breakout(mirror(scenario()))
    assert idx(sh, "hb_short_signal") == [J] and idx(sh, "hb_long_signal") == []
    a, b = lg.row(J, named=True), sh.row(J, named=True)
    assert b["hb_short_level"] == pytest.approx(200 - a["hb_long_level"]) and b["hb_short_sl"] == pytest.approx(200 - a["hb_long_sl"])
    for k in ("touches", "touch_span", "clv", "risk_atr", "quality"):
        assert b[f"hb_short_{k}"] == pytest.approx(a[f"hb_long_{k}"]), k


@pytest.mark.parametrize(("name", "over", "params"), [
    ("нет всплеска объёма", {"vol_ratio": {J: 1.5}}, None),
    ("H4 медвежий", {"htf_bias": -1}, None),
    ("закрытие у уровня, не за ним", {"close": {J: 101.02}}, None),
    ("слабое закрытие (CLV 0.47)", {"high": {J: 102.0}, "close": {J: 101.1}}, None),
    ("свеча истощения: тело 3 ATR", {"close": {J: 103.4}, "high": {J: 103.5}}, HorizontalParams(max_extension_atr=5.0)),
    ("перерастянут: закрытие 1.5 ATR над уровнем", {"close": {J: 102.5}, "high": {J: 102.6}}, None),
    ("коробка шире 5 ATR", {"low": {31: 95.5}}, None),
    ("коробка уже 1.5 ATR", {}, HorizontalParams(min_box_atr=3.0)),
    ("тренд в коробке (ER = 1)", {"close": {i: 98.6 + (i - 29) * 0.1 for i in range(29, J)}}, None),
    ("только два касания", {"high": {32: 100.6, 38: 100.6}}, None),
    ("касания подряд (размах 2 бара)", {"high": {32: 100.6, 38: 100.6, 44: 100.6, 50: 101.0, 51: 101.0, 52: 101.0}}, None),
    ("риск больше 3 ATR", {"low": {50: 97.9}}, None),
    ("порог качества", {}, HorizontalParams(min_quality=0.99)),
])
def test_each_filter_blocks_the_signal(name, over, params):
    out = detect_horizontal_breakout(scenario(**over), params)
    assert idx(out, "hb_long_signal") == [], name


def test_breakout_is_an_event_not_a_state():
    """Прошлая свеча уже закрылась над прошлой коробкой - второй сигнал на продолжении не даём."""
    df = scenario(n=62, open={J + 1: 101.6}, high={J + 1: 102.4}, low={J + 1: 101.5}, close={J + 1: 102.3}, vol_ratio={J + 1: 3.0})
    assert idx(detect_horizontal_breakout(df), "hb_long_signal") == [J]


def test_htf_neutral_and_missing_bias_are_blocked_fail_closed():
    assert idx(detect_horizontal_breakout(scenario(htf_bias=0)), "hb_long_signal") == []  # Neutral - запрет (жёсткий фильтр, 2026-10-19)
    assert idx(detect_horizontal_breakout(scenario(htf_bias=1)), "hb_long_signal") == [J]
    nulls = scenario().with_columns(pl.lit(None, dtype=pl.Int8).alias("htf_bias"))
    assert idx(detect_horizontal_breakout(nulls), "hb_long_signal") == []
    no_col = scenario().drop("htf_bias")
    with pytest.raises(DataError, match="htf_bias"):
        detect_horizontal_breakout(no_col)
    assert idx(detect_horizontal_breakout(no_col, HorizontalParams(htf_filter=False)), "hb_long_signal") == [J]


def test_input_contract_r1():
    with pytest.raises(DataError, match="vol_ratio"):
        detect_horizontal_breakout(scenario().drop("vol_ratio"))
    with pytest.raises(DataError, match="уже есть"):
        detect_horizontal_breakout(detect_horizontal_breakout(scenario()))
    for bad in (dict(window=3), dict(min_touches=0), dict(min_box_atr=6.0), dict(max_efficiency=0.0), dict(min_clv=1.0),
                dict(volume_mult=-1.0), dict(htf_filter=1), dict(window=24.0), dict(min_touch_span=24), dict(min_quality=1.5)):
        with pytest.raises(ConfigError):
            HorizontalParams(**bad)


def _features(seed: int, n: int = 900) -> pl.DataFrame:
    rnd = random.Random(seed)
    f = add_features(random_walk(n, seed, 0.008), SmcParams(), interval_ms=STEP)
    return f.with_columns(pl.Series("htf_bias", [rnd.choice((-1, 0, 1)) for _ in range(n)], dtype=pl.Int8))


SOFT = HorizontalParams(volume_mult=0.8, min_clv=0.3, max_box_atr=8.0, min_touches=2, min_body_atr=0.1, max_risk_atr=6.0, max_efficiency=0.6)


def test_soft_params_find_signals_on_random_data():
    out = detect_horizontal_breakout(_features(3, 3000), SOFT)
    assert out["hb_long_signal"].sum() > 3 and out["hb_short_signal"].sum() > 3
    sig = out.filter(pl.col("hb_long_signal"))
    assert (sig["hb_long_sl"] < sig["close"]).all() and (sig["close"] > sig["hb_long_level"]).all()


@pytest.mark.parametrize("seed", [21, 22])
def test_truncation_invariant_r4(seed):
    df = _features(seed)
    rnd = random.Random(seed)
    assert_truncation_invariant(lambda d: detect_horizontal_breakout(d, SOFT), df, sorted(rnd.sample(range(df.height), 150)))


def test_live_window_of_300_bars_equals_full_history_r8():
    df = _features(5, 1500)
    full = detect_horizontal_breakout(df, SOFT)
    cols = [c for s in ("long", "short") for c in signal_columns(s)]
    for t in range(400, df.height, 37):
        live = detect_horizontal_breakout(df.slice(t - 299, 300), SOFT).row(299, named=True)
        whole = full.row(t, named=True)
        assert {c: live[c] for c in cols} == {c: whole[c] for c in cols}, t


def test_strategy_wraps_detector_into_market_signal_with_geometry_and_meta():
    df = scenario(n=J + 1)
    ctx = FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500)
    (sig,) = StrategyHorizontalBreakout().detect(ctx)
    assert sig.strategy == HORIZONTAL_BREAKOUT and sig.direction == Direction.LONG and sig.entry == 101.6
    assert sig.sl == pytest.approx(99.3) and sig.tp is None
    assert [g["type"] for g in sig.geometry] == ["zone", "line", "marker", "line"]
    assert sig.geometry[0]["t1"] == (J - 24) * STEP and (sig.geometry[0]["p1"], sig.geometry[0]["p2"]) == (98.5, 101.0)
    m = sig.meta
    assert m["level"] == 101.0 and m["touches"] == 4 and m["window"] == 24 and m["htf_label"] == "Bullish" and 0 < m["quality"] < 1
    assert StrategyHorizontalBreakout().detect(FeatureContext("BTCUSDT", "15m", STEP, df.head(J), J * STEP + 500)) == ()
