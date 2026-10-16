"""OBI_Breakout: защита от входа «на дне / пике» импульса (2026-10-15): Max BOS Distance Guard, Max Candle Body Guard, опциональная проверка поджатия.

Сценарий - `obi_scenario` из test_multi_strategies: уровень BOS 105 (свинг-хай), ATR = 1.0 на всех барах, бар 20 - пробойная свеча со стаканом +0.4, прошлое закрытие 104.5.
Каждый тест меняет ОДНО условие, чтобы остальные защиты не маскировали проверяемую."""

import random

import polars as pl
import pytest

from dataclasses import replace

from strategies.obi_breakout import ObiParams
from strategies.obi_breakout import detect_obi_breakout as _detect
from tests.features.test_smc import assert_truncation_invariant, random_walk
from tests.strategies.test_multi_strategies import LEGACY, FP, idx, obi_scenario, pipeline_features, with_micro
from utils.errors import ConfigError


def detect_obi_breakout(df, params=None):
    return _detect(df, LEGACY if params is None else params)


def long_signals(**kw) -> list[int]:
    params = kw.pop("params", None)
    return idx(detect_obi_breakout(obi_scenario(**kw), params), "obi_long_signal")


def short_scenario(**over) -> pl.DataFrame:
    """Зеркало пробоя: свинг-лоу 95, закрытие 94.0 при прошлом 95.5, тело 1.4 (как в тесте шорта test_multi_strategies)."""
    s = {"open": {20: 95.4}, "close": {19: 95.5, 20: 94.0}, "low": {20: 93.8}, "high": {20: 95.5}}
    for k, v in over.items():
        s.setdefault(k, {}).update(v)
    return obi_scenario(obi=-0.4, **s)


def short_signals(df: pl.DataFrame, params: ObiParams | None = None) -> list[int]:
    return idx(detect_obi_breakout(df, params), "obi_short_signal")


# ------------------------------------------------------------------------------------------ Max BOS Distance Guard


def test_close_far_from_the_bos_level_is_rejected_as_overstretched():
    near = {"open": {20: 104.6}, "close": {20: 106.5}, "high": {20: 106.7}}  # 1.5 ATR от уровня 105 - граница допустима (тело 1.9 <= 2.5)
    far = {"open": {20: 104.6}, "close": {20: 106.6}, "high": {20: 106.8}}  # 1.6 ATR - поздно входить (тело 2.0: защита тела не при чём)
    assert idx(detect_obi_breakout(obi_scenario(**near)), "obi_long_signal") == [20]
    assert idx(detect_obi_breakout(obi_scenario(**far)), "obi_long_signal") == []


def test_short_overstretched_close_is_rejected_symmetrically():
    assert short_signals(short_scenario(close={20: 93.5}, low={20: 93.3})) == [20]  # 1.5 ATR ниже уровня 95: граница допустима (тело 1.9)
    assert short_signals(short_scenario(close={20: 93.4}, low={20: 93.2})) == []  # 1.6 ATR


def test_distance_limit_is_configurable_and_scales_with_atr_before_the_breakout_bar():
    far = {"open": {20: 104.6}, "close": {20: 106.6}, "high": {20: 106.8}}
    assert idx(detect_obi_breakout(obi_scenario(**far), replace(LEGACY, max_bos_distance_atr=2.0)), "obi_long_signal") == [20]
    assert idx(detect_obi_breakout(obi_scenario(**far), replace(LEGACY, max_bos_distance_atr=1.0)), "obi_long_signal") == []
    # ATR самой аномальной свечи (бар 20) раздут её диапазоном и не должен ослаблять собственный фильтр: берётся ATR[j-1] = 1.0
    inflated = obi_scenario(**far).with_columns(pl.when(pl.int_range(pl.len()) == 20).then(4.0).otherwise(pl.col("atr")).alias("atr"))
    assert idx(detect_obi_breakout(inflated), "obi_long_signal") == []


# -------------------------------------------------------------------------------------------- Max Candle Body Guard


def test_exhaustion_candle_with_an_abnormally_large_body_is_rejected():
    ok = {"open": {20: 103.5}, "close": {20: 106.0}, "low": {20: 103.4}}  # тело 2.5 ATR - граница допустима (расстояние до уровня 1.0)
    big = {"open": {20: 103.4}, "close": {20: 106.0}, "low": {20: 103.3}}  # тело 2.6 ATR - свеча истощения
    assert idx(detect_obi_breakout(obi_scenario(**ok)), "obi_long_signal") == [20]
    assert idx(detect_obi_breakout(obi_scenario(**big)), "obi_long_signal") == []


def test_dump_style_candle_is_rejected_by_both_guards_short_side():
    """Дамп на несколько ATR: тело и удаление от уровня превышают пределы - сигнала нет, хотя стакан и импульс подтверждают."""
    s = short_scenario(open={20: 96.0}, close={20: 91.0}, low={20: 90.8}, high={20: 96.1})
    assert short_signals(s) == []
    assert short_signals(s, replace(LEGACY, max_bos_distance_atr=10.0, max_body_atr=10.0)) == [20]  # без защит - прежнее поведение


def test_body_limit_is_configurable():
    big = {"open": {20: 103.4}, "close": {20: 106.0}, "low": {20: 103.3}}
    assert idx(detect_obi_breakout(obi_scenario(**big), replace(LEGACY, max_body_atr=3.0)), "obi_long_signal") == [20]


# --------------------------------------------------------------------------------- опциональное поджатие (Buildup)


def test_new_defaults_are_tight_and_buildup_is_on():
    p = ObiParams()
    assert (p.require_buildup, p.max_bos_distance_atr, p.max_body_atr) == (True, 1.0, 1.5)
    assert long_signals() == [20]  # LEGACY-сценарий без поджатия
    assert idx(_detect(obi_scenario()), "obi_long_signal") == []  # дефолты: нет касаний уровня и сжатия ATR - сигнала нет
    assert idx(_detect(obi_scenario(high={15: 104.9, 17: 105.2}, open={20: 104.7}, close={20: 105.95}, low={20: 104.6})), "obi_long_signal") == [20]


def test_buildup_requires_two_touches_of_the_level_within_the_lookback_window():
    on = replace(LEGACY, require_buildup=True)
    assert long_signals(params=on) == []  # касаний нет, ATR не сжимался (отношение 1.0 > 0.9)
    one = {"high": {17: 104.9}}
    assert idx(detect_obi_breakout(obi_scenario(**one), on), "obi_long_signal") == []  # одно касание
    two = {"high": {15: 104.9, 17: 105.2}}  # в пределах 0.3 ATR от 105 (и ниже, и чуть выше)
    assert idx(detect_obi_breakout(obi_scenario(**two), on), "obi_long_signal") == [20]
    far = {"high": {15: 104.6, 17: 105.4}}  # дальше 0.3 ATR - не касания
    assert idx(detect_obi_breakout(obi_scenario(**far), on), "obi_long_signal") == []


def test_buildup_window_is_the_last_n_bars_before_the_breakout_bar():
    on = replace(LEGACY, require_buildup=True)  # окно 16 баров: бары 4..19
    inside = {"high": {4: 105.0, 19: 105.0}}
    outside = {"high": {3: 105.0, 19: 105.0}}  # бар 3 уже за окном: засчитано одно касание
    assert idx(detect_obi_breakout(obi_scenario(**inside), on), "obi_long_signal") == [20]
    assert idx(detect_obi_breakout(obi_scenario(**outside), on), "obi_long_signal") == []
    on_bar = {"high": {19: 105.0, 20: 106.2}}  # сам пробойный бар не считается касанием
    assert idx(detect_obi_breakout(obi_scenario(**on_bar), on), "obi_long_signal") == []


def test_buildup_is_also_satisfied_by_volatility_compression():
    on = replace(LEGACY, require_buildup=True)
    squeezed = obi_scenario().with_columns(pl.when(pl.int_range(pl.len()) == 19).then(0.8).otherwise(pl.col("atr")).alias("atr"))  # ATR[j-1] = 0.8 <= 0.9 * ATR[j-17] = 0.9
    assert idx(detect_obi_breakout(squeezed, on), "obi_long_signal") == [20]
    mild = obi_scenario().with_columns(pl.when(pl.int_range(pl.len()) == 19).then(0.95).otherwise(pl.col("atr")).alias("atr"))
    assert idx(detect_obi_breakout(mild, on), "obi_long_signal") == []  # сжатие слабее порога


def test_buildup_short_side_counts_touches_of_the_low_side_level():
    on = replace(LEGACY, require_buildup=True)
    assert short_signals(short_scenario(), on) == []
    assert short_signals(short_scenario(low={15: 95.2, 17: 94.9}), on) == [20]


def test_buildup_and_guard_parameters_are_validated():
    for bad in (dict(max_bos_distance_atr=0), dict(max_body_atr=-1), dict(buildup_touch_atr=0), dict(buildup_atr_ratio=0), dict(buildup_lookback=1),
                dict(buildup_touches=0), dict(buildup_touches=20, buildup_lookback=16), dict(require_buildup="yes")):
        with pytest.raises(ConfigError):
            ObiParams(**bad)


# ------------------------------------------------------------------------------- R3/R4 на случайных данных


SOFT = dict(impulse_atr_mult=0.3, obi_min=0.2, delta_min=0.2, delta_volume_mult=0.5)


@pytest.mark.parametrize("params", [ObiParams(**SOFT), ObiParams(**SOFT, require_buildup=True, buildup_touch_atr=1.5, buildup_touches=1)])
def test_truncation_r4_with_guards_and_buildup(params):
    df = random_walk(3000, 41, 0.012)

    def fn(d: pl.DataFrame) -> pl.DataFrame:
        return detect_obi_breakout(with_micro(pipeline_features(d)), params)

    full = fn(df)
    fired = [i for c in ("obi_long_signal", "obi_short_signal") for i in idx(full, c)]
    assert len(fired) >= 3, "на тестовых данных почти нет сигналов - тест обрезки пуст"
    rnd = random.Random(7)
    ts = sorted(set(rnd.sample(range(150, df.height), 100) + fired[:30]))
    assert_truncation_invariant(fn, df, ts)


def test_guards_only_remove_signals_never_add_them():
    """Защиты - дополнительные условия: набор сигналов с защитами вложен в набор без них (на случайных данных, где без защит сигналов много)."""
    df = with_micro(pipeline_features(random_walk(3000, 41, 0.012)))
    off = ObiParams(**SOFT, max_bos_distance_atr=1e9, max_body_atr=1e9)
    on = ObiParams(**SOFT)
    a, b = detect_obi_breakout(df, off), detect_obi_breakout(df, on)
    for side in ("long", "short"):
        before, after = set(idx(a, f"obi_{side}_signal")), set(idx(b, f"obi_{side}_signal"))
        assert after <= before
    assert a["obi_long_signal"].sum() + a["obi_short_signal"].sum() > b["obi_long_signal"].sum() + b["obi_short_signal"].sum()  # что-то отсекли
