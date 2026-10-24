"""Стратегии CHR (2026-10-19, только Shadow): `Horizontal_Breakout_CHR` и `Trendline_Breakout_CHR`.

Ручные сценарии с известным ответом: сигнал есть / нет по каждому правилу (касания, локальная компрессия 5-10 баров перед пробоем, локальный стоп за свингом 3-8 баров, полнотелый пробой
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
    AUTO_TP_SKIP, CHR_PLAN, NEEDS_HTF, SHADOW_ONLY_STRATEGIES, SHADOW_STRATEGIES, build_strategies, plans_for, strategy_allowed,
)
from orchestrator.tracker import CLOSED, OPEN, VirtualTracker
from orchestrator.types import Direction, FeatureContext, Kind
from persistence.recorder import MemoryRecorder
from strategies.chr_common import pivot_chain
import strategies.chr_horizontal as H
import strategies.chr_trendline as T
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
    """Лонг: фон high 100.4 / low 99.6. Сопротивление 101.0 - пивоты-касания на барах 34 и 44. База (компрессия) - 8 баров 46..53: старая половина 46..49
    (high 100.6, low 99.9 / 99.7 / 99.8 / 99.8 - размах 0.7-0.9), свежая 50..53 (high 100.7, low 100.3 / 100.3 / 100.4 / 100.4 - размах 0.3-0.4): волатильность затухает,
    минимумы подтягиваются, high трутся об уровень (0.3-0.4 ATR). Ближайший свинг low - бар 47 (99.7). Бар 54: 100.9 -> 101.7 (тело 80% размаха) - над уровнем."""
    o, c, h, lo = [100.0] * n, [100.0] * n, [100.4] * n, [99.6] * n
    for i in (34, 44):
        h[i] = 101.0
    for i, hi, low in ((46, 100.6, 99.9), (47, 100.6, 99.7), (48, 100.6, 99.8), (49, 100.6, 99.8), (50, 100.7, 100.3), (51, 100.7, 100.3), (52, 100.7, 100.4), (53, 100.7, 100.4)):
        h[i], lo[i], o[i], c[i] = hi, low, 100.2, 100.2
    o[J], h[J], lo[J], c[J] = 100.9, 101.8, 100.8, 101.7
    cols = {"open": o, "high": h, "low": lo, "close": c}
    for name, spec in over.items():
        for i, v in spec.items():
            cols[name][i] = v
    return frame(cols["open"], cols["high"], cols["low"], cols["close"], bias)


def trendline(n: int = 60, bias=1, **over) -> pl.DataFrame:
    """Лонг: фон high 100.0 / low 99.4. Нисходящее сопротивление через пивоты 102.0 (бар 20) и 101.2 (бар 36): -0.05 за бар, на баре 54 - 100.3; третье касание - 102.4
    на баре 12. После бара 36 цена идёт вдоль линии на 0.25 ниже неё (high строго убывают - других пивотов нет). База - 8 баров 46..53: старая половина 46..49 размах 0.75,
    свежая 50..53 размах 0.3. Бар 54: 99.9 -> 100.9 (тело 83%) - над линией."""
    o, c, h, lo = [99.7] * n, [99.7] * n, [100.0] * n, [99.4] * n
    h[12], h[20], h[36] = 102.4, 102.0, 101.2
    for t in range(37, 54):
        hi = 101.2 - 0.05 * (t - 36) - 0.25
        rng = 0.75 if t <= 49 else 0.3
        h[t], lo[t] = hi, hi - rng
        o[t] = c[t] = hi - rng / 2
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
    # ручные сценарии короче окна по умолчанию (60): на 60 барах пробойная свеча J = 54 не имеет полного окна, поэтому окно сценариев - 50
    return detect_chr_trendline(df, STEP, params or ChrTrendlineParams(window=50))


# ------------------------------------------------------------------------------------------------ пивоты


def test_pivot_chain_is_written_on_confirmation_bar_only():
    """Пивот бара i (k = 2) появляется в цепочке на строке i + 2, не раньше (R3); цепочка хранит последние пивоты по порядку (служит касаниям уровня / линии)."""
    df = horizontal(low={36: 98.5, 42: 99.0})
    work = df
    for stage in pivot_chain("low", 2, 3, "x"):
        work = work.with_columns(stage)
    assert work["x1_p"][37] is None and work["x1_p"][38] == 98.5  # бар 36 подтверждён закрытием бара 38
    r = work.row(44, named=True)
    assert (r["x1_p"], r["x2_p"]) == (99.0, 98.5) and r["x2_t"] == 36 * STEP and work["x1_p"][43] == 98.5  # бар 42 подтверждён на баре 44


# ------------------------------------------------------------------------------------- Horizontal_Breakout_CHR


def test_horizontal_long_signal_with_touches_compression_and_local_swing_stop():
    out = hz(horizontal())
    assert idx(out, "chh_long_signal") == [J] and idx(out, "chh_short_signal") == []
    r = out.row(J, named=True)
    assert r["chh_long_level"] == 101.0 and r["chh_long_touches"] == 2
    assert (r["chh_long_squeeze_2"], r["chh_long_squeeze_1"], r["chh_long_squeeze_3"]) == (99.7, 100.3, None)  # низ старой и свежей половины базы
    assert (r["chh_long_squeeze_start_t"], r["chh_long_squeeze_last_t"]) == (46 * STEP, 53 * STEP)
    assert r["chh_long_sl_extreme"] == 99.7 and r["chh_long_sl"] == pytest.approx(99.7 - 0.2)  # ближайший свинг (бар 47) - 0.2 ATR
    assert r["chh_long_risk_atr"] == pytest.approx(101.7 - 99.5)
    assert 0 < r["chh_long_comp_decay"] < 0.85 and r["chh_long_comp_rub_atr"] <= 0.6 and r["chh_long_comp_height_atr"] <= 2.5
    assert r["chh_long_body_frac"] == pytest.approx(0.8)
    assert out.row(J - 1, named=True)["chh_long_level"] is None  # значения - только в строке сигнала


def test_stop_is_the_nearest_local_swing_not_the_global_minimum():
    """Глобальный минимум окна поиска (бар 40, low 97.0) стоп не двигает; свинг ближе к пробою (бар 51, low 100.1) перебивает более дальний (бар 47, 99.7)."""
    base = hz(horizontal()).row(J, named=True)["chh_long_sl"]
    deep = hz(horizontal(low={40: 97.0}))
    assert idx(deep, "chh_long_signal") == [J] and deep.row(J, named=True)["chh_long_sl"] == base
    near = hz(horizontal(low={51: 100.1})).row(J, named=True)
    assert near["chh_long_sl_extreme"] == 100.1 and near["chh_long_sl"] == pytest.approx(99.9) and near["chh_long_risk_atr"] == pytest.approx(101.7 - 99.9)


def test_stop_without_a_swing_falls_back_to_the_last_three_bars_and_never_sits_inside_the_breakout_candle():
    out = hz(horizontal(), ChrHorizontalParams(sl_swing_bars=3)).row(J, named=True)  # окно свингов - только бары 52..51: свинга нет -> минимум 3 последних баров
    assert out["chh_long_sl_extreme"] == 100.3 and out["chh_long_sl"] == pytest.approx(100.1)
    deep_break = hz(horizontal(low={J: 99.0}, open={J: 99.2}, close={J: 101.7}, high={J: 101.8}), ChrHorizontalParams(max_break_range_atr=3.0, max_risk_atr=4.0, min_body_frac=0.5))
    assert deep_break.row(J, named=True)["chh_long_sl_extreme"] == 99.0  # low пробойной свечи глубже свинга - стоп за ней


def test_risk_cap_makes_the_stop_short():
    assert idx(hz(horizontal(), ChrHorizontalParams(max_risk_atr=2.0)), "chh_long_signal") == []  # риск 2.2 ATR > 2.0
    assert idx(hz(horizontal(), ChrHorizontalParams(max_risk_atr=2.3)), "chh_long_signal") == [J]


def test_horizontal_short_is_exact_mirror_lower_highs_above_support():
    lg, sh = hz(horizontal()), hz(mirror(horizontal()))
    assert idx(sh, "chh_short_signal") == [J] and idx(sh, "chh_long_signal") == []
    a, b = lg.row(J, named=True), sh.row(J, named=True)
    assert b["chh_short_level"] == pytest.approx(200 - a["chh_long_level"]) and b["chh_short_sl"] == pytest.approx(200 - a["chh_long_sl"])
    assert b["chh_short_squeeze_2"] == pytest.approx(200 - 99.7) and b["chh_short_squeeze_1"] == pytest.approx(200 - 100.3)  # зеркало: верх старой и свежей половины базы
    assert b["chh_short_touches"] == a["chh_long_touches"]


@pytest.mark.parametrize(("name", "over", "params"), [
    ("одно касание уровня", {"high": {34: 100.4}}, None),
    ("касание не у уровня (дальше 0.25 ATR)", {"high": {34: 100.7}}, None),
    ("волатильность не затухает (свежая половина как старая)", {"low": {50: 99.8, 51: 99.8, 52: 99.8, 53: 99.8}}, None),
    ("база шире 2.5 ATR", {"low": {47: 98.0}}, None),
    ("high не трутся об уровень (база на 1.1 ATR ниже)", {"high": {46: 99.9, 47: 99.9, 48: 99.9, 49: 99.9, 50: 99.9, 51: 99.9, 52: 99.9, 53: 99.9}}, None),
    ("минимумы не подтягиваются (свежий низ ниже старого)", {"low": {46: 99.95, 47: 99.95, 48: 99.95, 49: 99.95, 50: 99.9}}, None),
    ("высшие минимумы на 50 барах без локальной базы - не поджатие", {"low": {46: 99.2, 47: 99.2, 48: 99.2, 49: 99.2}}, ChrHorizontalParams(comp_decay=0.3)),
    ("закрытие ровно на уровне", {"close": {J: 101.0}, "high": {J: 101.1}}, None),
    ("неполнотелая свеча (тело 40% размаха)", {"open": {J: 101.06}, "high": {J: 102.2}, "low": {J: 100.7}}, None),
    ("медвежья свеча за уровнем", {"open": {J: 101.75}, "close": {J: 101.1}, "high": {J: 101.8}, "low": {J: 101.0}}, None),
    ("затухание строже фактического", {}, ChrHorizontalParams(comp_decay=0.3)),
    ("трение строже фактического", {}, ChrHorizontalParams(comp_rub_atr=0.1)),
    ("узость базы строже фактической", {}, ChrHorizontalParams(comp_height_atr=0.5)),
    ("старая точка вне окна", {}, ChrHorizontalParams(window=18)),
])
def test_horizontal_each_rule_blocks_the_long(name, over, params):
    assert idx(hz(horizontal(**over), params), "chh_long_signal") == [], name


def test_horizontal_min_two_touches_is_the_rule_and_the_compression_window_is_5_to_10_bars():
    assert idx(hz(horizontal(), ChrHorizontalParams(min_touches=3)), "chh_long_signal") == []
    assert idx(hz(horizontal(), ChrHorizontalParams(comp_bars=6)), "chh_long_signal") == [J]  # короче база - те же свежие бары, сигнал сохраняется
    for bad in (dict(min_touches=1), dict(comp_bars=4), dict(comp_bars=11), dict(sl_swing_bars=2), dict(sl_swing_bars=9), dict(comp_bars=30)):
        with pytest.raises(ConfigError):
            ChrHorizontalParams(**bad)


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


def test_trendline_long_breaks_descending_resistance_after_local_compression():
    out = tl(trendline())
    assert idx(out, "cht_long_signal") == [J] and idx(out, "cht_short_signal") == []
    r = out.row(J, named=True)
    assert r["cht_long_level"] == pytest.approx(100.3) and r["cht_long_slope_atr"] == pytest.approx(-0.05)
    assert r["cht_long_touches"] == 3  # две опоры + 102.4 на продолжении линии
    assert (r["cht_long_anchor1_price"], r["cht_long_anchor2_price"]) == (102.0, 101.2)
    assert r["cht_long_squeeze_2"] < r["cht_long_squeeze_1"] and r["cht_long_squeeze_3"] is None  # низ свежей половины базы выше низа старой
    assert r["cht_long_sl_extreme"] == pytest.approx(99.55) and r["cht_long_sl"] == pytest.approx(99.55 - 0.2)  # ближайший свинг (бар 49) - 0.2 ATR
    assert r["cht_long_risk_atr"] == pytest.approx(100.9 - 99.35) and r["cht_long_comp_rub_atr"] == pytest.approx(0.25)  # high держатся в 0.25 ATR под линией


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
    ("волатильность не затухает", {"low": {50: 99.2, 51: 99.2, 52: 99.2, 53: 99.2}}, None),
    ("база шире 2.5 ATR", {"low": {47: 97.0}}, None),
    ("затухание строже фактического", {}, ChrTrendlineParams(window=50, comp_decay=0.2)),
    ("трение строже фактического", {}, ChrTrendlineParams(window=50, comp_rub_atr=0.1)),
    ("риск длиннее допуска", {}, ChrTrendlineParams(window=50, max_risk_atr=1.0)),
    ("закрытие под линией", {"close": {J: 100.25}, "high": {J: 100.3}}, None),
    ("неполнотелая свеча", {"open": {J: 100.5}, "high": {J: 101.5}, "low": {J: 99.6}}, None),
    ("опора вне окна", {}, ChrTrendlineParams(window=30)),
    ("нужно 4 касания", {}, ChrTrendlineParams(min_touches=4)),
])
def test_trendline_each_rule_blocks_the_long(name, over, params):
    assert idx(tl(trendline(**over), params), "cht_long_signal") == [], name


def test_breakout_is_an_event_not_a_state():
    """Продолжение движения на следующей свече над уровнем / линией - не новый сигнал (обе стратегии)."""
    two_t = trendline(n=62, open={55: 100.8}, high={55: 101.6}, low={55: 100.7}, close={55: 101.5})
    assert idx(tl(two_t), "cht_long_signal") == [J]
    two = horizontal(n=62, open={J + 1: 101.7}, high={J + 1: 102.6}, low={J + 1: 101.6}, close={J + 1: 102.5})
    assert idx(hz(two), "chh_long_signal") == [J]


# ------------------------------------------------------------------------------------- «глаза трейдера» (2026-10-21)


def test_huge_breakout_candle_is_not_bought_market():
    """Размах пробойной свечи > 1.5 ATR[j-1] - сигнала нет (обе стратегии, обе стороны); ровно 1.5 - ещё можно."""
    big = dict(high={J: 103.0}, low={J: 99.8})  # размах 3.2 ATR; тело 1.2 из 3.2 < 60% - ловим именно размах, поэтому раздуваем и тело
    h = horizontal(open={J: 99.8}, close={J: 102.9}, **big)
    assert idx(hz(h), "chh_long_signal") == [] and idx(hz(mirror(h)), "chh_short_signal") == []
    t = trendline(open={J: 99.7}, close={J: 102.5}, high={J: 102.6}, low={J: 99.4})
    assert idx(tl(t), "cht_long_signal") == [] and idx(tl(mirror(t)), "cht_short_signal") == []
    edge_h = horizontal(open={J: 100.3}, high={J: 101.8}, low={J: 100.3}, close={J: 101.75})  # размах 1.5
    assert idx(hz(edge_h), "chh_long_signal") == [J]
    assert idx(hz(horizontal(), ChrHorizontalParams(max_break_range_atr=0.8)), "chh_long_signal") == []  # размах 1.0 > 0.8


def test_lookback_is_capped_to_the_local_structure_and_swings_are_3_bars_wide():
    assert H.MAX_LOOKBACK == T.MAX_LOOKBACK == 80 and H.CHR_H_WINDOW <= 80 and T.CHR_T_WINDOW <= 80
    assert H.CHR_H_PIVOT_K >= 3 and T.CHR_T_PIVOT_K >= 3


def test_stale_second_touch_is_rejected():
    """Последнее касание уровня / вторая опора линии слишком далеко от пробоя - не «свежая» структура."""
    assert idx(hz(horizontal(), ChrHorizontalParams(max_touch_age=8)), "chh_long_signal") == []  # последнее касание на баре 44: 10 баров назад
    assert idx(hz(horizontal(), ChrHorizontalParams(max_touch_age=10)), "chh_long_signal") == [J]
    assert idx(tl(trendline(), ChrTrendlineParams(window=50, max_touch_age=10)), "cht_long_signal") == []  # A1 на баре 36: 18 баров назад
    assert idx(tl(trendline(), ChrTrendlineParams(window=50, max_touch_age=18)), "cht_long_signal") == [J]


def test_line_must_not_cut_through_candles_between_touches_and_before_breakout():
    """Закрытие за линией между опорами и после - линия не чистая граница; даже мелкий прокол тенью свыше допуска блокирует."""
    assert idx(tl(trendline(close={28: 101.9}, high={28: 102.0})), "cht_long_signal") == []  # закрытие выше линии (101.6) между опорами
    assert idx(tl(trendline(high={39: 101.12, 40: 101.12})), "cht_long_signal") == []  # тень выше линии (линия на баре 40 = 101.0) - линия режет свечу
    assert idx(tl(trendline(high={39: 101.07, 40: 101.04})), "cht_long_signal") == [J]  # касание в пределах 0.05 ATR допустимо


# ------------------------------------------------------------------------------------- R4 / R8 на случайных данных


def _features(seed: int, n: int = 900) -> pl.DataFrame:
    rnd = random.Random(seed)
    f = add_features(random_walk(n, seed, 0.008), SmcParams(), interval_ms=STEP)
    return f.with_columns(pl.Series("htf_bias", [rnd.choice((-1, 0, 1)) for _ in range(n)], dtype=pl.Int8))


def test_random_data_gives_signals_on_both_sides_and_stops_beyond_the_close():
    """На случайном блуждании со строгими правилами по умолчанию сигналов единицы (локальная компрессия + полнотелый пробой + тренд + короткий стоп);
    для проверки инвариантов компрессия и риск ослаблены."""
    df = _features(3, 20000)
    loose = dict(comp_decay=9.0, comp_max_range_atr=9.0, comp_height_atr=50.0, comp_rub_atr=50.0, max_risk_atr=50.0, max_break_range_atr=10.0)
    out_h = hz(df, ChrHorizontalParams(max_touch_age=40, **loose))  # свежесть и размах ослаблены: проверяем инварианты
    out_t = tl(df, ChrTrendlineParams(max_touch_age=60, cross_atr=0.25, **loose))
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
    for bad in (dict(window=4), dict(pivot_k=0), dict(comp_bars=3), dict(min_body_frac=0.0), dict(min_body_frac=1.5), dict(window=24.0), dict(window=81), dict(max_touch_age=81), dict(max_break_range_atr=0)):
        with pytest.raises(ConfigError):
            ChrHorizontalParams(**bad)
    for bad in (dict(min_slope_atr=0.0), dict(touch_depth=1), dict(min_touches=1), dict(comp_bars=11), dict(sl_swing_bars=9), dict(window=81), dict(max_touch_age=81), dict(max_break_range_atr=0)):
        with pytest.raises(ConfigError):
            ChrTrendlineParams(**bad)


# ------------------------------------------------------------------------------------- обёртка в Signal


def test_strategies_wrap_detectors_into_market_signals_with_squeeze_geometry():
    df = horizontal(n=J + 1)
    ctx = FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500)
    (sig,) = StrategyHorizontalBreakoutChr().detect(ctx)
    assert sig.strategy == HORIZONTAL_BREAKOUT_CHR and sig.direction == Direction.LONG and sig.kind == Kind.MARKET
    assert sig.entry == 101.7 and sig.sl == pytest.approx(99.5) and sig.tp is None
    assert [g["label"] for g in sig.geometry] == ["уровень", "поджатие", "пробой", "SL"]
    sq = sig.geometry[1]
    assert (sq["t1"], sq["t2"], sq["p1"], sq["p2"]) == (46 * STEP, 53 * STEP, 99.7, 100.3)
    m = sig.meta
    assert m["touches"] == 2 and m["squeeze_points"] == [99.7, 100.3] and m["comp_bars"] == 8 and m["sl_extreme"] == 99.7 and m["stop"] == "static" and m["htf_label"] == "Bullish"
    assert StrategyHorizontalBreakoutChr().detect(FeatureContext("BTCUSDT", "15m", STEP, df.head(J), J * STEP + 500)) == ()

    df = mirror(trendline(n=J + 1))
    (sig,) = StrategyTrendlineBreakoutChr(ChrTrendlineParams(window=50)).detect(FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500))
    assert sig.strategy == TRENDLINE_BREAKOUT_CHR and sig.direction == Direction.SHORT and sig.sl == pytest.approx(200 - 99.35)
    assert sig.meta["line_level"] == pytest.approx(99.7) and sig.meta["touches"] == 3 and sig.meta["htf_label"] == "Bearish"
    assert [g["label"] for g in sig.geometry] == ["наклонная линия", "2-я опора", "поджатие", "пробой линии", "SL"]


# ------------------------------------------------------------------------------------- регистрация: только Shadow


def test_chr_strategies_are_registered_shadow_only_with_htf_and_static_plan():
    for name in (HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR):
        assert name in SHADOW_STRATEGIES and name in SHADOW_ONLY_STRATEGIES and name in config.SHADOW_ONLY_STRATEGIES and name in NEEDS_HTF
        assert plans_for([name])[name] is CHR_PLAN and CHR_PLAN.levels == ((4.0, 1.0),) and name in AUTO_TP_SKIP  # CHR: всегда 1:4 на весь объём
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
    """Лонг MARKET: вход 100, SL 95 (R = 5). Mark +1.2R, откат к входу и ниже: стоп остаётся 95, позиция закрывается по исходному SL с убытком,
    а не в безубыток; частичных фиксаций у CHR нет (тейк всегда 1:4 на весь объём)."""
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
    """Лонг MARKET: вход 100, SL 95 (R = 5): объём = 5 USDT / 5 = 1.0; тейк 120 (1:4) закрывает ВЕСЬ объём одной частью (CHR: всегда 1:4)."""
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
    assert (p.window, p.pivot_k, p.min_touches, p.touch_atr, p.comp_bars, p.comp_decay, p.comp_rub_atr, p.sl_swing_bars, p.max_risk_atr, p.min_body_frac, p.sl_buffer_atr) == (
        H.CHR_H_WINDOW, H.CHR_H_PIVOT_K, H.CHR_H_MIN_TOUCHES, H.CHR_H_TOUCH_ATR, H.CHR_H_COMP_BARS, H.CHR_H_COMP_DECAY, H.CHR_H_COMP_RUB_ATR, H.CHR_H_SL_SWING_BARS,
        H.CHR_H_MAX_RISK_ATR, H.CHR_H_MIN_BODY_FRAC, H.CHR_H_SL_BUFFER_ATR)
    assert (q.window, q.pivot_k, q.min_touches, q.touch_depth, q.touch_atr, q.min_slope_atr, q.comp_bars, q.comp_decay, q.comp_rub_atr, q.sl_swing_bars, q.max_risk_atr,
            q.min_body_frac, q.sl_buffer_atr) == (
        T.CHR_T_WINDOW, T.CHR_T_PIVOT_K, T.CHR_T_MIN_TOUCHES, T.CHR_T_TOUCH_DEPTH, T.CHR_T_TOUCH_ATR, T.CHR_T_MIN_SLOPE_ATR, T.CHR_T_COMP_BARS, T.CHR_T_COMP_DECAY,
        T.CHR_T_COMP_RUB_ATR, T.CHR_T_SL_SWING_BARS, T.CHR_T_MAX_RISK_ATR, T.CHR_T_MIN_BODY_FRAC, T.CHR_T_SL_BUFFER_ATR)
    assert 5 <= p.comp_bars <= 10 and 3 <= p.sl_swing_bars <= 8 and p.sl_buffer_atr == 0.2 and q.sl_buffer_atr == 0.2
    assert StrategyHorizontalBreakoutChr().params == p and StrategyTrendlineBreakoutChr().params == q
    assert (H.CHR_H_WINDOW, T.CHR_T_WINDOW, H.CHR_H_TOUCH_ATR, T.CHR_T_MIN_SLOPE_ATR) == (30, 60, 0.25, 0.02)  # согласованные стартовые значения
