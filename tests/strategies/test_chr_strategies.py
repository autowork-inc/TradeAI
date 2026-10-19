"""Стратегии CHR (2026-10-19, только Shadow): `Horizontal_Breakout_CHR` и `Trendline_Breakout_CHR`.

Ручные сценарии с известным ответом: сигнал есть / нет по каждому правилу (касания, поджатие higher lows / lower highs, полнотелый пробой
строго за уровнем, жёсткий тренд H4), зеркало лонг/шорт, тест обрезки R4 и паритет окна live R8 на случайных данных, обёртка в Signal,
регистрация (только Shadow) и статичный стоп (нет безубытка ни по mark +1R, ни после частичного фикса на +1R)."""

import random

import polars as pl
import pytest

import config
from features.smc import SmcParams, add_features
from orchestrator.journal import MemoryEventLog
from orchestrator.multi_strategies import (
    HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR, StrategyHorizontalBreakoutChr, StrategyTrendlineBreakoutChr,
)
from orchestrator.registry import (
    CHR_PLAN, NEEDS_HTF, SHADOW_ONLY_STRATEGIES, SHADOW_STRATEGIES, build_strategies, plans_for, strategy_allowed,
)
from orchestrator.tracker import CLOSED, OPEN, VirtualTracker
from orchestrator.types import Direction, FeatureContext, Kind
from persistence.recorder import MemoryRecorder
from strategies.chr_common import pivot_chain
from strategies.chr_horizontal import ChrHorizontalParams, detect_chr_horizontal
from strategies.chr_horizontal import signal_columns as h_columns
from strategies.chr_trendline import ChrTrendlineParams, detect_chr_trendline
from strategies.chr_trendline import signal_columns as t_columns
from tests.features.test_smc import assert_truncation_invariant, random_walk
from tests.orchestrator.helpers import mk, tick
from tests.orchestrator.live_helpers import STRATEGY, manager
from utils.errors import ConfigError, DataError

STEP = 900_000
J = 54  # пробойная свеча в обоих сценариях


def frame(o, h, lo, c, bias=1) -> pl.DataFrame:
    n = len(o)
    return pl.DataFrame({"open_time": [i * STEP for i in range(n)], "open": o, "high": h, "low": lo, "close": c, "atr": [1.0] * n,
                         "htf_bias": pl.Series([bias] * n, dtype=pl.Int8)})


def horizontal(n: int = 60, bias=1, **over) -> pl.DataFrame:
    """Лонг: фон high 100.4 / low 99.6 (равные значения - пивотов нет). Сопротивление 101.0 - пивоты-касания на барах 34 и 44.
    Поджатие: минимумы 98.5 (бар 36) < 99.0 (42) < 99.3 (48). Бар 54: открытие 100.5, закрытие 101.7 (тело 86% размаха) - над уровнем."""
    o, c, h, lo = [100.0] * n, [100.0] * n, [100.4] * n, [99.6] * n
    for i in (34, 44):
        h[i] = 101.0
    for i, v in ((36, 98.5), (42, 99.0), (48, 99.3)):
        lo[i] = v
    o[J], h[J], lo[J], c[J] = 100.5, 101.8, 100.4, 101.7
    cols = {"open": o, "high": h, "low": lo, "close": c}
    for name, spec in over.items():
        for i, v in spec.items():
            cols[name][i] = v
    return frame(cols["open"], cols["high"], cols["low"], cols["close"], bias)


def trendline(n: int = 60, bias=1, **over) -> pl.DataFrame:
    """Лонг: фон high 100.0 / low 99.4. Нисходящее сопротивление через пивоты 102.0 (бар 20) и 101.2 (бар 36): -0.05 за бар, на баре 54 - 100.3;
    третье касание - 102.4 на баре 12 (на продолжении линии). Поджатие: 98.6 (40) < 98.9 (45) < 99.1 (50). Бар 54: 99.9 -> 100.9 (тело 83%)."""
    o, c, h, lo = [99.7] * n, [99.7] * n, [100.0] * n, [99.4] * n
    h[12], h[20], h[36] = 102.4, 102.0, 101.2
    for i, v in ((40, 98.6), (45, 98.9), (50, 99.1)):
        lo[i] = v
    o[J], h[J], lo[J], c[J] = 99.9, 101.0, 99.8, 100.9
    cols = {"open": o, "high": h, "low": lo, "close": c}
    for name, spec in over.items():
        for i, v in spec.items():
            cols[name][i] = v
    return frame(cols["open"], cols["high"], cols["low"], cols["close"], bias)


def mirror(df: pl.DataFrame, k: float = 200.0) -> pl.DataFrame:
    """p -> k - p, high <-> low, тренд с обратным знаком: лонг-сценарий становится шорт-сценарием."""
    return df.with_columns((k - pl.col("open")).alias("open"), (k - pl.col("low")).alias("high"), (k - pl.col("high")).alias("low"),
                           (k - pl.col("close")).alias("close"), (-pl.col("htf_bias")).cast(pl.Int8).alias("htf_bias"))


def idx(out: pl.DataFrame, name: str) -> list[int]:
    return [i for i, v in enumerate(out[name].to_list()) if v]


def hz(df, params=None):
    return detect_chr_horizontal(df, params)


def tl(df, params=None):
    return detect_chr_trendline(df, STEP, params)


# ------------------------------------------------------------------------------------------------ пивоты


def test_pivot_chain_is_written_on_confirmation_bar_only():
    """Пивот бара i (k = 2) появляется в цепочке на строке i + 2, не раньше (R3); цепочка хранит последние пивоты по порядку."""
    df = horizontal()
    work = df
    for stage in pivot_chain("low", 2, 3, "x"):
        work = work.with_columns(stage)
    assert work["x1_p"][37] is None and work["x1_p"][38] == 98.5  # бар 36 подтверждён закрытием бара 38
    r = work.row(52, named=True)
    assert (r["x1_p"], r["x2_p"], r["x3_p"]) == (99.3, 99.0, 98.5) and r["x3_t"] == 36 * STEP


# ------------------------------------------------------------------------------------- Horizontal_Breakout_CHR


def test_horizontal_long_signal_with_touches_squeeze_and_static_structural_stop():
    out = hz(horizontal())
    assert idx(out, "chh_long_signal") == [J] and idx(out, "chh_short_signal") == []
    r = out.row(J, named=True)
    assert r["chh_long_level"] == 101.0 and r["chh_long_touches"] == 2
    assert (r["chh_long_squeeze_3"], r["chh_long_squeeze_2"], r["chh_long_squeeze_1"]) == (98.5, 99.0, 99.3)
    assert r["chh_long_sl"] == pytest.approx(99.3 - 0.2)  # последний higher low - 0.2 ATR
    assert r["chh_long_body_frac"] == pytest.approx(1.2 / 1.4)
    assert out.row(J - 1, named=True)["chh_long_level"] is None  # значения - только в строке сигнала


def test_horizontal_short_is_exact_mirror_lower_highs_above_support():
    lg, sh = hz(horizontal()), hz(mirror(horizontal()))
    assert idx(sh, "chh_short_signal") == [J] and idx(sh, "chh_long_signal") == []
    a, b = lg.row(J, named=True), sh.row(J, named=True)
    assert b["chh_short_level"] == pytest.approx(200 - a["chh_long_level"]) and b["chh_short_sl"] == pytest.approx(200 - a["chh_long_sl"])
    assert (b["chh_short_squeeze_3"], b["chh_short_squeeze_2"], b["chh_short_squeeze_1"]) == (101.5, 101.0, 100.7)  # lower highs
    assert b["chh_short_touches"] == a["chh_long_touches"]


@pytest.mark.parametrize(("name", "over", "params"), [
    ("одно касание уровня", {"high": {34: 100.4}}, None),
    ("касание не у уровня (дальше 0.25 ATR)", {"high": {34: 100.7}}, None),
    ("минимумы не повышаются: 99.0 -> 98.8", {"low": {48: 98.8}}, None),
    ("равные минимумы - не строго выше", {"low": {48: 99.0}}, None),
    ("только два минимума при squeeze_points = 3", {"low": {36: 99.6}}, None),
    ("старая точка поджатия вне окна", {}, ChrHorizontalParams(window=18)),
    ("закрытие ровно на уровне", {"close": {J: 101.0}, "high": {J: 101.1}}, None),
    ("неполнотелая свеча (тело 40% размаха)", {"open": {J: 101.06}, "high": {J: 102.2}, "low": {J: 100.7}}, None),
    ("медвежья свеча за уровнем", {"open": {J: 101.75}, "close": {J: 101.1}, "high": {J: 101.8}, "low": {J: 101.0}}, None),
])
def test_horizontal_each_rule_blocks_the_long(name, over, params):
    assert idx(hz(horizontal(**over), params), "chh_long_signal") == [], name


def test_horizontal_min_two_touches_is_the_rule_and_three_points_squeeze_is_needed():
    assert idx(hz(horizontal(), ChrHorizontalParams(min_touches=3)), "chh_long_signal") == []
    assert idx(hz(horizontal(low={36: 99.6}), ChrHorizontalParams(squeeze_points=2)), "chh_long_signal") == [J]
    with pytest.raises(ConfigError):
        ChrHorizontalParams(min_touches=1)


# ------------------------------------------------------------------------------------- жёсткий тренд H4


@pytest.mark.parametrize("bias", [0, -1, None])
def test_trend_filter_is_100_percent_strict_for_long(bias):
    """Лонг - только при Bullish: Neutral, Bearish и отсутствие bias - запрет (обе стратегии)."""
    for build, det, col in ((horizontal, hz, "chh_long_signal"), (trendline, tl, "cht_long_signal")):
        df = build().with_columns(pl.lit(bias, dtype=pl.Int8).alias("htf_bias"))
        assert idx(det(df), col) == [], (col, bias)


@pytest.mark.parametrize("bias", [0, 1, None])
def test_trend_filter_is_100_percent_strict_for_short(bias):
    for build, det, col in ((horizontal, hz, "chh_short_signal"), (trendline, tl, "cht_short_signal")):
        df = mirror(build()).with_columns(pl.lit(bias, dtype=pl.Int8).alias("htf_bias"))
        assert idx(det(df), col) == [], (col, bias)


def test_counter_trend_setup_never_signals_even_with_perfect_geometry():
    """Идеальный лонг-сетап при Bearish H4 - ни лонга, ни шорта: контртрендовые сигналы игнорируются полностью."""
    for build, det, pre in ((horizontal, hz, "chh"), (trendline, tl, "cht")):
        out = det(build(bias=-1))
        assert idx(out, f"{pre}_long_signal") == [] and idx(out, f"{pre}_short_signal") == []


def test_bias_column_is_required_no_switch_to_disable_the_filter():
    with pytest.raises(DataError, match="htf_bias"):
        hz(horizontal().drop("htf_bias"))
    with pytest.raises(DataError, match="htf_bias"):
        tl(trendline().drop("htf_bias"))
    assert "htf_filter" not in ChrHorizontalParams.__dataclass_fields__ and "htf_filter" not in ChrTrendlineParams.__dataclass_fields__


# ------------------------------------------------------------------------------------- Trendline_Breakout_CHR


def test_trendline_long_breaks_descending_resistance_after_higher_lows():
    out = tl(trendline())
    assert idx(out, "cht_long_signal") == [J] and idx(out, "cht_short_signal") == []
    r = out.row(J, named=True)
    assert r["cht_long_level"] == pytest.approx(100.3) and r["cht_long_slope_atr"] == pytest.approx(-0.05)
    assert r["cht_long_touches"] == 3  # две опоры + 102.4 на продолжении линии
    assert (r["cht_long_anchor1_price"], r["cht_long_anchor2_price"]) == (102.0, 101.2)
    assert (r["cht_long_squeeze_3"], r["cht_long_squeeze_2"], r["cht_long_squeeze_1"]) == (98.6, 98.9, 99.1)
    assert r["cht_long_sl"] == pytest.approx(99.1 - 0.2)


def test_trendline_short_is_exact_mirror():
    lg, sh = tl(trendline()), tl(mirror(trendline()))
    assert idx(sh, "cht_short_signal") == [J] and idx(sh, "cht_long_signal") == []
    a, b = lg.row(J, named=True), sh.row(J, named=True)
    assert b["cht_short_level"] == pytest.approx(200 - a["cht_long_level"]) and b["cht_short_sl"] == pytest.approx(200 - a["cht_long_sl"])
    assert b["cht_short_slope_atr"] == pytest.approx(-a["cht_long_slope_atr"]) and b["cht_short_touches"] == a["cht_long_touches"]


@pytest.mark.parametrize(("name", "over", "params"), [
    ("сопротивление восходящее", {"high": {20: 100.8}}, None),
    ("линия почти горизонтальна", {"high": {36: 101.99}}, None),
    ("high вышел за линию после 2-й опоры", {"high": {28: 102.0}}, None),
    ("минимумы не повышаются", {"low": {50: 98.7}}, None),
    ("закрытие под линией", {"close": {J: 100.25}, "high": {J: 100.3}}, None),
    ("неполнотелая свеча", {"open": {J: 100.5}, "high": {J: 101.5}, "low": {J: 99.6}}, None),
    ("опора вне окна", {}, ChrTrendlineParams(window=30)),
    ("нужно 4 касания", {}, ChrTrendlineParams(min_touches=4)),
])
def test_trendline_each_rule_blocks_the_long(name, over, params):
    assert idx(tl(trendline(**over), params), "cht_long_signal") == [], name


def test_breakout_is_an_event_not_a_state():
    """Свеча J-1 уже закрылась над линией (сама - полнотелый пробой): сигнал на ней, продолжение на J - не новый сигнал (обе стратегии)."""
    assert idx(tl(trendline(close={J - 1: 100.36}, high={J - 1: 100.4})), "cht_long_signal") == [J - 1]
    two = horizontal(n=62, open={J + 1: 101.7}, high={J + 1: 102.6}, low={J + 1: 101.6}, close={J + 1: 102.5})
    assert idx(hz(two), "chh_long_signal") == [J]


# ------------------------------------------------------------------------------------- R4 / R8 на случайных данных


def _features(seed: int, n: int = 900) -> pl.DataFrame:
    rnd = random.Random(seed)
    f = add_features(random_walk(n, seed, 0.008), SmcParams(), interval_ms=STEP)
    return f.with_columns(pl.Series("htf_bias", [rnd.choice((-1, 0, 1)) for _ in range(n)], dtype=pl.Int8))


def test_random_data_gives_signals_on_both_sides_and_stops_beyond_the_close():
    """На случайном блуждании с правилами по умолчанию сигналов единицы (3 строго повышающихся пивота + полнотелый пробой + тренд);
    для проверки инвариантов берём поджатие из 2 точек."""
    df = _features(3, 8000)
    out_h, out_t = hz(df, ChrHorizontalParams(squeeze_points=2)), tl(df, ChrTrendlineParams(squeeze_points=2))
    for out, pre in ((out_h, "chh"), (out_t, "cht")):
        assert out[f"{pre}_long_signal"].sum() >= 3 and out[f"{pre}_short_signal"].sum() >= 3, pre
        lg, sh = out.filter(pl.col(f"{pre}_long_signal")), out.filter(pl.col(f"{pre}_short_signal"))
        assert (lg[f"{pre}_long_sl"] < lg["close"]).all() and (lg["close"] > lg[f"{pre}_long_level"]).all() and (lg["htf_bias"] == 1).all()
        assert (sh[f"{pre}_short_sl"] > sh["close"]).all() and (sh["close"] < sh[f"{pre}_short_level"]).all() and (sh["htf_bias"] == -1).all()


@pytest.mark.parametrize("seed", [21, 22])
def test_truncation_invariant_r4(seed):
    df = _features(seed)
    ts = sorted(random.Random(seed).sample(range(df.height), 120))
    assert_truncation_invariant(hz, df, ts)
    assert_truncation_invariant(tl, df, ts)


def test_live_window_of_300_bars_equals_full_history_r8():
    df = _features(5, 1500)
    for det, cols in ((hz, [c for s in ("long", "short") for c in h_columns(s)]), (tl, [c for s in ("long", "short") for c in t_columns(s)])):
        full = det(df)
        for t in range(400, df.height, 37):
            live = det(df.slice(t - 299, 300)).row(299, named=True)
            whole = full.row(t, named=True)
            assert {c: live[c] for c in cols} == {c: whole[c] for c in cols}, (t, cols[0])


def test_input_contract_r1():
    with pytest.raises(DataError, match="уже есть"):
        hz(hz(horizontal()))
    with pytest.raises(DataError, match="уже есть"):
        tl(tl(trendline()))
    with pytest.raises(ConfigError):
        detect_chr_trendline(trendline(), 0)
    for bad in (dict(window=4), dict(pivot_k=0), dict(squeeze_points=1), dict(min_body_frac=0.0), dict(min_body_frac=1.5), dict(window=24.0)):
        with pytest.raises(ConfigError):
            ChrHorizontalParams(**bad)
    for bad in (dict(min_slope_atr=0.0), dict(touch_depth=1), dict(min_touches=1)):
        with pytest.raises(ConfigError):
            ChrTrendlineParams(**bad)


# ------------------------------------------------------------------------------------- обёртка в Signal


def test_strategies_wrap_detectors_into_market_signals_with_squeeze_geometry():
    df = horizontal(n=J + 1)
    ctx = FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500)
    (sig,) = StrategyHorizontalBreakoutChr().detect(ctx)
    assert sig.strategy == HORIZONTAL_BREAKOUT_CHR and sig.direction == Direction.LONG and sig.kind == Kind.MARKET
    assert sig.entry == 101.7 and sig.sl == pytest.approx(99.1) and sig.tp is None
    assert [g["label"] for g in sig.geometry] == ["уровень", "поджатие", "пробой", "SL"]
    sq = sig.geometry[1]
    assert (sq["t1"], sq["t2"], sq["p1"], sq["p2"]) == (36 * STEP, 48 * STEP, 98.5, 99.3)
    m = sig.meta
    assert m["touches"] == 2 and m["squeeze_points"] == [98.5, 99.0, 99.3] and m["stop"] == "static" and m["htf_label"] == "Bullish"
    assert StrategyHorizontalBreakoutChr().detect(FeatureContext("BTCUSDT", "15m", STEP, df.head(J), J * STEP + 500)) == ()

    df = mirror(trendline(n=J + 1))
    (sig,) = StrategyTrendlineBreakoutChr().detect(FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500))
    assert sig.strategy == TRENDLINE_BREAKOUT_CHR and sig.direction == Direction.SHORT and sig.sl == pytest.approx(200 - 98.9)
    assert sig.meta["line_level"] == pytest.approx(99.7) and sig.meta["touches"] == 3 and sig.meta["htf_label"] == "Bearish"
    assert [g["label"] for g in sig.geometry] == ["наклонная линия", "2-я опора", "поджатие", "пробой линии", "SL"]


# ------------------------------------------------------------------------------------- регистрация: только Shadow


def test_chr_strategies_are_registered_shadow_only_with_htf_and_static_plan():
    for name in (HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR):
        assert name in SHADOW_STRATEGIES and name in SHADOW_ONLY_STRATEGIES and name in config.SHADOW_ONLY_STRATEGIES and name in NEEDS_HTF
        assert plans_for([name])[name] is CHR_PLAN
        assert all(strategy_allowed(name, s) for s in config.SHADOW_SYMBOLS)
    built = {s.name: s for s in build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")}
    assert built[HORIZONTAL_BREAKOUT_CHR].interval == built[TRENDLINE_BREAKOUT_CHR].interval == "15m"


@pytest.mark.parametrize("name", [HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR])
def test_chr_cannot_be_approved_for_live(monkeypatch, tmp_path, name):
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", (STRATEGY, name))
    with pytest.raises(ConfigError, match="только в Shadow"):
        config.validate()
    with pytest.raises(ConfigError, match="только в Shadow"):
        manager(tmp_path, approved=(STRATEGY, name))


# ------------------------------------------------------------------------------------- статичный стоп, без безубытка


def test_chr_plan_has_one_take_profit_no_partials_no_runner_no_trailing_no_break_even():
    assert CHR_PLAN.levels == ((4.0, 1.0),) and sum(f for _, f in CHR_PLAN.levels) == pytest.approx(1.0)  # единственный тейк 1:4, остатка-runner нет
    assert getattr(CHR_PLAN, "trail_mode", None) is None and getattr(CHR_PLAN, "be_after_first", False) is False


@pytest.mark.parametrize("name", [HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR])
def test_stop_is_never_moved_to_break_even_after_mark_plus_1r(name):
    """Лонг MARKET: вход 100, SL 95 (R = 5). Mark +1.2R, затем частичный фикс 50% на +1R, откат к входу и ниже: стоп остаётся 95,
    остаток закрывается по исходному SL с убытком, а не в безубыток."""
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=MemoryRecorder(), plans=plans_for([name]))
    sig = mk(strategy=name, direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0)
    assert tr.submit(sig, 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(100.0, 10))
    assert t.status == OPEN and t.fill_price == 100.0
    stop0, kind0 = t.stop_m, t.stop_kind
    tr.on_tick(tick(106.0, 20, kind="mark"))  # mark price +1.2R
    assert (t.stop_m, t.stop_kind) == (stop0, kind0) and not t.be_armed
    tr.on_tick(tick(105.1, 30))  # +1.02R: частичных фиксаций нет
    assert t.levels_done == 0 and t.remaining == 1.0
    assert (t.stop_m, t.stop_kind) == (stop0, kind0) and not t.be_armed
    tr.on_tick(tick(100.0, 40))  # возврат к входу: в безубыток не закрылась
    tr.on_tick(tick(97.0, 50))
    assert t.status == OPEN
    tr.on_tick(tick(94.9, 60))  # исходный SL
    assert t.status == CLOSED and [g.reason for g in t.legs] == ["sl"] and t.legs[-1].price == 94.9
    assert t.legs[-1].gross_r == pytest.approx((94.9 - 100.0) / 5.0)


@pytest.mark.parametrize("name", [HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR])
def test_chr_trade_risks_5_usdt_and_takes_whole_position_at_exactly_1_to_4(name):
    """Лонг MARKET: вход 100, SL 95 (R = 5): объём = 5 USDT / 5 = 1.0; тейк 120 (1:4) закрывает ВЕСЬ объём одной частью, прибыль ~ +4R."""
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=MemoryRecorder(), plans=plans_for([name]))
    assert tr.submit(mk(strategy=name, direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0), 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(100.0, 10))
    assert t.status == OPEN and t.sizing.risk_amount == pytest.approx(5.0) and t.sizing.qty == pytest.approx(1.0)
    tr.on_tick(tick(119.9, 20))  # чуть не дошла до 1:4 - позиция жива, ничего не закрыто
    assert t.status == OPEN and t.remaining == 1.0 and not t.legs
    tr.on_tick(tick(120.0, 30))
    assert t.status == CLOSED and [g.reason for g in t.legs] == ["tp"] and t.legs[0].frac == pytest.approx(1.0) and t.legs[0].price == 120.0
    assert t.legs[0].gross_r == pytest.approx(4.0)


def test_calibration_constants_are_the_defaults_of_params_and_classes():
    import strategies.chr_horizontal as H
    import strategies.chr_trendline as T
    p, q = ChrHorizontalParams(), ChrTrendlineParams()
    assert (p.window, p.pivot_k, p.min_touches, p.touch_atr, p.squeeze_points, p.min_body_frac, p.sl_buffer_atr) == (
        H.CHR_H_WINDOW, H.CHR_H_PIVOT_K, H.CHR_H_MIN_TOUCHES, H.CHR_H_TOUCH_ATR, H.CHR_H_SQUEEZE_POINTS, H.CHR_H_MIN_BODY_FRAC, H.CHR_H_SL_BUFFER_ATR)
    assert (q.window, q.pivot_k, q.min_touches, q.touch_depth, q.touch_atr, q.min_slope_atr, q.squeeze_points, q.min_body_frac, q.sl_buffer_atr) == (
        T.CHR_T_WINDOW, T.CHR_T_PIVOT_K, T.CHR_T_MIN_TOUCHES, T.CHR_T_TOUCH_DEPTH, T.CHR_T_TOUCH_ATR, T.CHR_T_MIN_SLOPE_ATR, T.CHR_T_SQUEEZE_POINTS,
        T.CHR_T_MIN_BODY_FRAC, T.CHR_T_SL_BUFFER_ATR)
    assert StrategyHorizontalBreakoutChr().params == p and StrategyTrendlineBreakoutChr().params == q
    assert (H.CHR_H_WINDOW, T.CHR_T_WINDOW, H.CHR_H_TOUCH_ATR, T.CHR_T_MIN_SLOPE_ATR) == (24, 48, 0.25, 0.02)  # согласованные стартовые значения
