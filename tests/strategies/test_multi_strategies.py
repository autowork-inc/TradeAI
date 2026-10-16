"""Четыре новые стратегии мульти-стратегийного Shadow (2026-10-11): SMC_Counter_Trend, OBI_Breakout, ICT_Killzones, VWAP_Reversion.

Ручные сценарии с известным ответом (сигнал есть / нет по каждому условию, зеркало лонг/шорт), тест обрезки R4 на случайных данных,
паритет окна live и полной истории R8, запрет итераций R2, проверка входа (R1).
"""

import random
import re
from pathlib import Path

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from strategies.counter_trend import CounterTrendParams, detect_counter_trend
from strategies.killzones import DAY_MS, HOUR_MS, KillzoneParams, detect_killzones
from strategies.obi_breakout import ObiParams
from strategies.obi_breakout import detect_obi_breakout as _detect_obi
from strategies.vwap_reversion import VwapParams, detect_vwap_reversion
from tests.features.test_smc import assert_truncation_invariant, random_walk
from tests.strategies.test_smc_setups import STEP, frame, mirror
from utils.errors import ConfigError, DataError


def idx(out: pl.DataFrame, name: str) -> list[int]:
    return [i for i, v in enumerate(out[name].to_list()) if v]


def with_cols(df: pl.DataFrame, **cols) -> pl.DataFrame:
    return df.with_columns([pl.Series(k, v if isinstance(v, list) else [v] * df.height, dtype=pl.Float64 if k != "htf_bias" else pl.Int8) for k, v in cols.items()])


# ====================================================================================== B: SMC_Counter_Trend


def ct_scenario(**over) -> pl.DataFrame:
    """Пул 110 (свинг-хай на баре 5, подтверждён на 10). Бар 15: проколол до 113 (глубина (113 - 110) / ATR 1 = 3.0), закрылся 108 (ниже пула и ниже
    открытия 109), объём 40 против SMA20 = 10 (x4). H4 бычий (+1) -> шорт против тренда."""
    s = {"swing_high": {10: True}, "sh1_price": (10, 110.0), "sh1_time": (10, 5 * STEP),
         "high": {15: 113.0}, "low": {15: 107.0}, "open": {15: 109.0}, "close": {15: 108.0}}
    s.update(over.pop("series", {}))
    n = 30
    df = frame(n, **s)
    return with_cols(df, volume=[10.0] * 15 + [over.pop("vol", 40.0)] + [10.0] * (n - 16), vol_sma_prev=10.0, htf_bias=over.pop("bias", 1))


def test_counter_trend_short_fires_on_the_deep_high_volume_sweep_against_bullish_h4():
    out = detect_counter_trend(ct_scenario())
    assert idx(out, "ct_short_signal") == [15] and not any(out["ct_long_signal"])
    r = out.row(15, named=True)
    assert r["ct_short_pool"] == 110.0 and r["ct_short_extreme"] == 113.0 and r["ct_short_depth_atr"] == pytest.approx(3.0)
    assert r["ct_short_vol_ratio"] == pytest.approx(4.0) and r["ct_short_sl"] == pytest.approx(113.2)  # экстремум прокола + 0.2 ATR


@pytest.mark.parametrize("name, kwargs", [
    ("прокол мельче 1.5 ATR (глубина 1.0)", {"series": {"high": {15: 111.0}}}),
    ("объём не выше 1.5 SMA20 (x1.4)", {"vol": 14.0}),
    ("закрытие не вернулось под пул", {"series": {"close": {15: 111.0}, "open": {15: 112.0}}}),
    ("свеча не разворотная (бычья)", {"series": {"open": {15: 107.5}}}),
    ("H4 нейтральный: контртренд требует СТРОГО обратный", {"bias": 0}),
    ("H4 медвежий: шорт - по тренду, а не против", {"bias": -1}),
    ("нет bias H4: fail-closed", {"bias": None}),
])
def test_counter_trend_each_condition_is_required(name, kwargs):
    assert idx(detect_counter_trend(ct_scenario(**kwargs)), "ct_short_signal") == [], name


def test_counter_trend_pool_already_taken_is_not_swept_again():
    out = detect_counter_trend(ct_scenario(series={"high": {13: 111.0, 15: 113.0}, "close": {13: 110.5, 15: 108.0}}))  # бар 13 уже пробил пул
    assert idx(out, "ct_short_signal") == []


def test_counter_trend_long_is_the_mirror_and_needs_bearish_h4():
    base = ct_scenario()
    df = with_cols(mirror(base), volume=base["volume"].to_list(), vol_sma_prev=10.0, htf_bias=-1)
    out = detect_counter_trend(df)
    assert idx(out, "ct_long_signal") == [15] and not any(out["ct_short_signal"])
    assert out["ct_long_sl"][15] == pytest.approx(200.0 - 113.2) and out["ct_long_depth_atr"][15] == pytest.approx(3.0)
    assert idx(detect_counter_trend(with_cols(df, htf_bias=1)), "ct_long_signal") == []  # H4 бычий: лонг по тренду - не наша стратегия


def test_counter_trend_params_and_input_validation():
    assert CounterTrendParams().min_sweep_atr == 1.5 and CounterTrendParams().volume_mult == 1.5  # смягчено 2026-10-11 (было 2.5 / 2.5)
    soft = ct_scenario(series={"high": {15: 112.0}}, vol=20.0)  # глубина 2.0 ATR, объём x2: по умолчанию сигнал есть, по прежним порогам - нет
    assert idx(detect_counter_trend(soft), "ct_short_signal") == [15]
    assert idx(detect_counter_trend(soft, CounterTrendParams(min_sweep_atr=2.5, volume_mult=2.5)), "ct_short_signal") == []
    for bad in ({"min_sweep_atr": 0}, {"volume_mult": -1}, {"sl_buffer_atr": True}):
        with pytest.raises(ConfigError):
            CounterTrendParams(**bad)
    with pytest.raises(DataError, match="htf_bias"):
        detect_counter_trend(ct_scenario().drop("htf_bias"))
    with pytest.raises(DataError, match="уже есть"):
        detect_counter_trend(detect_counter_trend(ct_scenario()))


# ========================================================================================= D: ICT_Killzones


def kz(df: pl.DataFrame, params: KillzoneParams | None = None) -> pl.DataFrame:
    return detect_killzones(df, STEP, params)


def kz_frame(n: int = 60, start: int = 0, **series) -> pl.DataFrame:
    """Свечи 15m с 00:00 UTC (бар i = 00:00 + 15 мин * i): Азия - бары 0..27 (00:00-07:00), London - 28..39 (07:00-10:00), New York - 48..59.
    Азия: High 102 (бар 10), Low 98 (бар 12)."""
    base = {"high": {10: 102.0}, "low": {12: 98.0}}
    for k, v in series.items():
        base.setdefault(k, {}).update(v)
    return frame(n, **base).slice(start)


def test_killzone_short_sweep_of_asia_high_in_london():
    out = kz(kz_frame(high={30: 103.0}, low={30: 100.0}, open={30: 102.0}, close={30: 101.0}))
    assert idx(out, "kz_short_signal") == [30] and not any(out["kz_long_signal"])
    r = out.row(30, named=True)
    assert (r["kz_asia_high"], r["kz_asia_low"], r["kz_session"]) == (102.0, 98.0, "london") and r["kz_short_sl"] == pytest.approx(103.2)


def test_killzone_long_sweep_of_asia_low_in_new_york():
    out = kz(kz_frame(high={50: 100.0}, low={50: 97.0}, open={50: 98.0}, close={50: 99.0}))
    assert idx(out, "kz_long_signal") == [50] and out["kz_session"][50] == "new_york" and out["kz_long_sl"][50] == pytest.approx(96.8)


@pytest.mark.parametrize("name, series", [
    ("закрытие выше Asia High (это пробой, а не ложный пробой)", {"high": {30: 103.0}, "open": {30: 101.0}, "close": {30: 102.5}}),
    ("свеча не медвежья", {"high": {30: 103.0}, "open": {30: 100.0}, "close": {30: 101.0}}),
    ("прокол внутри Азии (бар 20 = 05:00)", {"high": {20: 103.0}, "open": {20: 102.0}, "close": {20: 101.0}}),
    ("вне killzone: 10:30 UTC (бар 42)", {"high": {42: 103.0}, "open": {42: 102.0}, "close": {42: 101.0}}),
    ("свеча снимает обе границы сразу", {"high": {30: 103.0}, "low": {30: 97.0}, "open": {30: 102.0}, "close": {30: 101.0}}),
])
def test_killzone_each_condition_is_required(name, series):
    out = kz(kz_frame(**series))
    assert idx(out, "kz_short_signal") == [], name


def test_killzone_level_is_used_once_per_day_and_incomplete_asia_gives_nothing():
    taken = kz_frame(high={30: 103.0, 50: 103.0}, open={30: 102.0, 50: 102.0}, close={30: 101.0, 50: 101.0})  # London уже снял уровень: NY - повтор
    assert idx(kz(taken), "kz_short_signal") == [30]
    late = kz_frame(high={30: 103.0}, open={30: 102.0}, close={30: 101.0}, start=10)  # кадр начинается в 02:30: диапазон Азии неполный
    assert idx(kz(late), "kz_short_signal") == [] and late.height == 50


def test_killzone_next_day_has_its_own_asia_range():
    n = 96 + 40
    out = kz(frame(n, high={10: 102.0, 96 + 10: 120.0}, low={12: 98.0, 96 + 12: 118.0}, open={96 + 30: 121.0}, close={96 + 30: 119.5}) .with_columns(
        pl.when(pl.int_range(pl.len()) == 96 + 30).then(121.0).otherwise(pl.col("high")).alias("high")))  # бар 30 второго дня: High 121 > 120
    assert idx(out, "kz_short_signal") == [96 + 30]
    assert out["kz_asia_high"][96 + 30] == 120.0  # сутки второго дня, а не первого


def test_killzone_params_validation():
    with pytest.raises(ConfigError):
        KillzoneParams(london=(6, 10))  # London начинается раньше конца Азии
    with pytest.raises(ConfigError):
        KillzoneParams(new_york=(9, 15))  # NY пересекает London
    with pytest.raises(ConfigError):
        detect_killzones(kz_frame(), 7 * 60_000)  # интервал не делит час
    with pytest.raises(DataError, match="atr"):
        detect_killzones(kz_frame().drop("atr"), STEP)
    assert HOUR_MS * 24 == DAY_MS


# ======================================================================================== E: VWAP_Reversion


def vwap_frame(n: int = 40, at: int = 30, offset: int = 0, **series) -> pl.DataFrame:
    """Сутки 00:00 UTC, шаг 15 минут. Закрытие колеблется 99.5 / 100.5 (sd ~ 0.5, полоса 3 sd ~ 101.5), объём 10. Бар 30 - всплеск до 103 (касание полосы),
    бар 31 - медвежье поглощение (открытие >= закрытия 30, закрытие <= открытия 30). `at` сдвигает пару баров (событие на at, at + 1), `offset` - сдвиг сетки
    времени в барах (чтобы полночь пришлась на нужную строку, а кадр был не короче суток)."""
    close = {i: 99.5 if i % 2 else 100.5 for i in range(n)}
    base = {"close": close, "open": {i: 100.0 for i in range(n)}, "high": {i: close[i] + 0.2 for i in range(n)}, "low": {i: close[i] - 0.2 for i in range(n)}}
    base["high"].update({at: 103.0, at + 1: 102.7})
    base["open"].update({at: 100.5, at + 1: 102.6})
    base["close"].update({at: 102.4, at + 1: 100.3})
    base["low"].update({at: 100.4, at + 1: 100.1})
    for k, v in series.items():
        base.setdefault(k, {}).update(v)
    df = with_cols(frame(n, **base), volume=10.0)
    return df.with_columns(((pl.int_range(pl.len(), dtype=pl.Int64) + offset) * STEP).alias("open_time")) if offset else df


def test_vwap_matches_manual_cumulative_calculation():
    df = vwap_frame()
    out = detect_vwap_reversion(df)
    cv = cpv = cpv2 = 0.0
    for i, r in enumerate(df.iter_rows(named=True)):  # эталон на питоне (в самих детекторах циклов по строкам нет)
        tp = (r["high"] + r["low"] + r["close"]) / 3
        cv, cpv, cpv2 = cv + r["volume"], cpv + tp * r["volume"], cpv2 + tp * tp * r["volume"]
        vwap = cpv / cv
        sd = max(cpv2 / cv - vwap * vwap, 0.0) ** 0.5
        assert out["vw_vwap"][i] == pytest.approx(vwap, rel=1e-12) and out["vw_sd"][i] == pytest.approx(sd, abs=1e-9)
        assert out["vw_upper"][i] == pytest.approx(vwap + 3 * sd, abs=1e-9) and out["vw_lower"][i] == pytest.approx(vwap - 3 * sd, abs=1e-9)


def test_vwap_short_on_bearish_engulfing_after_touching_the_third_deviation():
    out = detect_vwap_reversion(vwap_frame())
    assert idx(out, "vw_short_signal") == [31] and not any(out["vw_long_signal"])
    assert out["vw_short_sl"][31] == pytest.approx(103.0 + 0.2)  # максимум за окно касания (бары 29-31) + 0.2 ATR
    assert out["high"][30] >= out["vw_upper"][30] and out["high"][29] < out["vw_upper"][29]  # касание 3-го отклонения случилось на баре 30


def test_vwap_long_is_the_mirror():
    df = vwap_frame()
    m = df.with_columns((200.0 - pl.col("low")).alias("high"), (200.0 - pl.col("high")).alias("low"), (200.0 - pl.col("open")).alias("open"), (200.0 - pl.col("close")).alias("close"))
    out = detect_vwap_reversion(m)
    assert idx(out, "vw_long_signal") == [31] and not any(out["vw_short_signal"])
    assert out["vw_long_sl"][31] == pytest.approx(200.0 - 103.0 - 0.2)


@pytest.mark.parametrize("name, kw", [
    ("нет разворотного паттерна (бар 31 бычий)", {"series": {"open": {31: 100.0}, "close": {31: 101.5}, "high": {31: 101.7}}}),
    ("касания полосы не было (поглощение есть, но цена не доходила до 3 sd)",
     {"series": {"high": {30: 100.9, 31: 100.9}, "open": {30: 100.4, 31: 100.9}, "close": {30: 100.8}, "low": {30: 100.3}}}),
])
def test_vwap_each_condition_is_required(name, kw):
    df = vwap_frame(**kw["series"])
    assert idx(detect_vwap_reversion(df), "vw_short_signal") == [], name


def test_vwap_touch_may_precede_the_reversal_pattern_within_the_lookback_window():
    """Касание на баре 30, нейтральный бар 31, поглощение на баре 32 (до касания - два бара): окно 3 бара это допускает, окно 2 - нет."""
    series = {"high": {31: 100.9, 32: 101.1}, "open": {31: 100.0, 32: 101.0}, "close": {31: 100.8, 32: 99.9}, "low": {31: 99.9, 32: 99.8}}
    df = vwap_frame(**series)
    out = detect_vwap_reversion(df)
    assert idx(out, "vw_short_signal") == [32] and out["high"][32] < out["vw_upper"][32]  # на самом баре 32 касания полосы нет
    assert out["vw_short_sl"][32] == pytest.approx(103.0 + 0.2)  # SL берёт экстремум окна, а не только свечи паттерна
    assert idx(detect_vwap_reversion(df, VwapParams(touch_lookback=2)), "vw_short_signal") == []


def test_vwap_needs_the_day_anchor_and_enough_bars():
    full = vwap_frame()
    assert idx(detect_vwap_reversion(full.slice(5)), "vw_short_signal") == []  # кадр начинается не с начала суток: VWAP был бы другим
    assert idx(detect_vwap_reversion(full, VwapParams(min_day_bars=40)), "vw_short_signal") == []  # на бар 31 суток накоплено меньше 40 свечей
    with pytest.raises(ConfigError):
        VwapParams(touch_lookback=0)
    with pytest.raises(DataError, match="volume"):
        detect_vwap_reversion(full.drop("volume"))


def test_vwap_defaults_are_the_original_m15_windows_and_for_interval_keeps_the_calendar_time():
    p = VwapParams()
    assert (p.min_day_bars, p.touch_lookback) == (16, 3)  # исходные значения 2026-10-11: 4 часа накопления суток, касание за 45 минут
    assert VwapParams.for_interval(STEP) == p
    m5 = VwapParams.for_interval(300_000, sd_mult=2.0)  # 5m поддерживается (эксперимент 2026-10-15), но не используется: те же 4 часа и 45 минут
    assert (m5.min_day_bars, m5.touch_lookback, m5.sd_mult) == (48, 9, 2.0)
    assert VwapParams.for_interval(3_600_000).min_day_bars == 16  # старше M15 - не меньше исходных


def test_vwap_min_history_covers_the_day_anchor_with_a_margin():
    from strategies.vwap_reversion import min_history_bars

    assert min_history_bars(STEP) == 96 + 12 == 108 and min_history_bars(300_000) == 288 + 12 == 300 and min_history_bars(3_600_000) == 24 + 12


def test_vwap_frame_that_lost_the_start_of_the_day_is_silent_so_the_depth_must_be_guarded():
    """Тот же сигнал (бар 99, полночь на строке 48): в кадре из 100 баров он есть, в кадре из 40 последних (начало суток выпало) - детектор молчит без ошибки.
    Поэтому глубину охраняют `ensure_history_depth` (старт) и StrategyVwapReversion.detect (каждая свеча)."""
    df = vwap_frame(n=120, at=98, offset=48).head(100)
    assert idx(detect_vwap_reversion(df), "vw_short_signal") == [99]
    assert idx(detect_vwap_reversion(df.slice(60, 40)), "vw_short_signal") == []


# ========================================================================================== C: OBI_Breakout


def obi_scenario(obi=0.4, delta=None, vol_ratio=1.0, **series) -> pl.DataFrame:
    """Свинг-хай 105 (подтверждён на 10). Бар 20: закрытие 106 > 105 при предыдущем 104.5 (пересечение), тело 1.4 > ATR 1.0 (импульс).
    Микроструктура есть только на последней строке расчёта (здесь - на баре 20)."""
    s = {"swing_high": {10: True}, "sh1_price": (10, 105.0), "sh1_time": (10, 5 * STEP), "swing_low": {10: True}, "sl1_price": (10, 95.0), "sl1_time": (10, 5 * STEP),
         "open": {20: 104.6}, "close": {19: 104.5, 20: 106.0}, "high": {19: 104.6, 20: 106.2}, "low": {20: 104.5}}
    for k, v in series.items():
        s.setdefault(k, {}).update(v)
    n = 30
    micro = {"ms_obi": [None] * n, "ms_delta": [None] * n}
    micro["ms_obi"][20], micro["ms_delta"][20] = obi, delta
    df = frame(n, **s)
    return df.with_columns(pl.Series("vol_ratio", [vol_ratio] * n, dtype=pl.Float64), pl.Series("volume", [10.0] * n), pl.Series("ms_obi", micro["ms_obi"], dtype=pl.Float64),
                           pl.Series("ms_delta", micro["ms_delta"], dtype=pl.Float64))


LEGACY = ObiParams(max_bos_distance_atr=1.5, max_body_atr=2.5, require_buildup=False)  # дефолты до 2026-10-16: сценарии тестов подогнаны под них


def detect_obi_breakout(df, params=None):
    return _detect_obi(df, LEGACY if params is None else params)


def test_obi_long_bos_confirmed_by_order_book_imbalance():
    out = detect_obi_breakout(obi_scenario(obi=0.4))
    assert idx(out, "obi_long_signal") == [20] and not any(out["obi_short_signal"])
    r = out.row(20, named=True)
    assert r["obi_long_level"] == 105.0 and r["obi_long_sl"] == pytest.approx(104.5 - 0.2) and r["obi_long_via_obi"] and not r["obi_long_via_delta"]


def test_obi_long_bos_confirmed_by_tape_delta_spike_only_with_volume():
    assert idx(detect_obi_breakout(obi_scenario(obi=None, delta=0.5, vol_ratio=2.0)), "obi_long_signal") == [20]
    out = detect_obi_breakout(obi_scenario(obi=None, delta=0.5, vol_ratio=1.0))  # дельта есть, всплеска объёма нет
    assert idx(out, "obi_long_signal") == []


@pytest.mark.parametrize("name, kw", [
    ("стакан слабый и дельты нет", {"obi": 0.1, "delta": None}),
    ("стакан против пробоя", {"obi": -0.5, "delta": None}),
    ("нет данных микроструктуры (null): подтверждения нет", {"obi": None, "delta": None}),
    ("нет пересечения: прошлое закрытие уже выше уровня", {"series": {"close": {19: 105.5}}}),
    ("нет импульса: тело меньше ATR", {"series": {"open": {20: 105.5}}}),
])
def test_obi_each_condition_is_required(name, kw):
    kw = dict(kw)
    series = kw.pop("series", {})
    assert idx(detect_obi_breakout(obi_scenario(**kw, **series)), "obi_long_signal") == [], name


def test_obi_short_bos_with_negative_imbalance():
    s = {"open": {20: 95.4}, "close": {19: 95.5, 20: 94.0}, "low": {20: 93.8}, "high": {20: 95.5}}
    out = detect_obi_breakout(obi_scenario(obi=-0.4, **s))
    assert idx(out, "obi_short_signal") == [20] and not any(out["obi_long_signal"])
    assert out["obi_short_level"][20] == 95.0 and out["obi_short_sl"][20] == pytest.approx(95.5 + 0.2)


def test_obi_validation():
    with pytest.raises(DataError, match="ms_obi"):
        detect_obi_breakout(obi_scenario().drop("ms_obi"))
    with pytest.raises(ConfigError):
        ObiParams(obi_min=1.0)
    with pytest.raises(ConfigError):
        ObiParams(delta_min=0)


# ============================================================ R4 (обрезка), R8 (окно live), R2 (без итераций) на случайных данных

FP = SmcParams(swing_atr_mult=0.5)


def pipeline_features(df: pl.DataFrame) -> pl.DataFrame:
    return add_features(df, FP, interval_ms=STEP)


def with_bias(f: pl.DataFrame) -> pl.DataFrame:
    """Детерминированный bias по времени свечи (causal): сменяется каждые 80 свечей."""
    return f.with_columns((((pl.col("open_time") // STEP // 80) % 3) - 1).cast(pl.Int8).alias("htf_bias"))


def with_micro(f: pl.DataFrame) -> pl.DataFrame:
    """Псевдо-микроструктура: детерминированная функция свечи (causal), на всех строках - чтобы проверить R4 по всей длине."""
    return f.with_columns(((pl.col("open_time") // STEP * 7919 % 200 - 100) / 100).alias("ms_obi"), ((pl.col("open_time") // STEP * 104729 % 200 - 100) / 100).alias("ms_delta"))


SOFT_CT = CounterTrendParams(min_sweep_atr=0.05, volume_mult=0.5)  # мягкие пороги: на случайных данных глубоких свипов почти нет
SOFT_OBI = ObiParams(impulse_atr_mult=0.3, obi_min=0.2, delta_min=0.2, delta_volume_mult=0.5)
SOFT_VW = VwapParams(sd_mult=1.5, min_day_bars=8)

CASES = {  # имя -> (функция кадр свечей -> кадр с сигналами, колонки сигналов, число баров случайного ряда: сигналы редкие - нужен длинный)
    "counter_trend": (lambda d: detect_counter_trend(with_bias(pipeline_features(d)), SOFT_CT), ("ct_short_signal", "ct_long_signal"), 6000),
    "obi_breakout": (lambda d: detect_obi_breakout(with_micro(pipeline_features(d)), SOFT_OBI), ("obi_long_signal", "obi_short_signal"), 1500),
    "killzones": (lambda d: kz(pipeline_features(d)), ("kz_short_signal", "kz_long_signal"), 6000),
    "vwap_reversion": (lambda d: detect_vwap_reversion(pipeline_features(d), SOFT_VW), ("vw_short_signal", "vw_long_signal"), 1500),
}


@pytest.mark.parametrize("name", CASES)
def test_truncation_r4_on_random_data(name):
    fn, signals, n = CASES[name]
    df = random_walk(n, 41, 0.012)
    full = fn(df)
    assert sum(full[c].sum() for c in signals) >= 3, f"{name}: на тестовых данных почти нет сигналов - тест обрезки пуст"
    rnd = random.Random(7)
    fired = [i for c in signals for i in idx(full, c)]
    ts = sorted(set(rnd.sample(range(150, df.height), 120) + fired[:30]))  # случайные t и сами моменты сигналов
    assert_truncation_invariant(fn, df, ts)


@pytest.mark.parametrize("name", CASES)
def test_live_window_300_bars_gives_the_same_decision_as_the_full_history_r8(name):
    fn, signals, n = CASES[name]
    df = random_walk(n, 41, 0.012)
    full = fn(df)
    rnd = random.Random(8)
    fired = [i for c in signals for i in idx(full, c) if i >= 400]  # t >= 400: полное окно в 300 баров и прогрев признаков позади
    assert fired, f"{name}: нет сигналов после прогрева"
    ts = sorted(set(rnd.sample(range(400, df.height), 150) + fired[:30]))
    for t in ts:
        window = fn(df.slice(t - 299, 300)).row(299, named=True)
        whole = full.row(t, named=True)
        for c in signals:
            assert window[c] == whole[c], f"{name} t={t}: окно 300 баров и полная история расходятся по {c}"


def test_no_row_iteration_in_new_detectors_r2():
    banned = r"\.apply\(|iter_rows|map_elements|map_rows|map_batches|rolling_map|\.rows\(|\.iter_|to_dicts|\.row\("
    for module in ("counter_trend", "obi_breakout", "killzones", "vwap_reversion"):
        src = (Path(__file__).resolve().parents[2] / "strategies" / f"{module}.py").read_text(encoding="utf-8")
        code = "\n".join(line for line in src.splitlines() if not line.lstrip().startswith("#"))
        assert re.search(banned, code) is None, f"{module}: итерация по строкам запрещена (R2)"
