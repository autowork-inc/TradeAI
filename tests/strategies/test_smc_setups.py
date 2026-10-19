"""Тесты автомата SMC (strategies/smc_setups.py): ручные сценарии с известным ответом, зеркальность лонг/шорт,
тест обрезки R4 на случайных данных, паритет окна R8, запрет итераций R2."""

import math
import random
import re
from pathlib import Path

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from strategies.smc_setups import SmcSetupParams, detect_smc, setup_columns
from tests.features.test_smc import assert_truncation_invariant, random_walk
from utils.errors import ConfigError, DataError

STEP = 900_000
P = SmcSetupParams()


def frame(n: int = 40, **series) -> pl.DataFrame:
    """Кадр «после add_features» с нейтральными значениями; series: колонка -> {строка: значение} (точечно)
    или (строка_начала, значение) для протяжки вперёд."""
    cols: dict[str, list] = {
        "open_time": [i * STEP for i in range(n)], "open": [100.0] * n, "high": [100.5] * n, "low": [99.5] * n,
        "close": [100.0] * n, "atr": [1.0] * n,
        "swing_high": [False] * n, "swing_low": [False] * n,
        "sh1_price": [None] * n, "sh1_time": [None] * n, "sl1_price": [None] * n, "sl1_time": [None] * n,
    }
    for side in ("bear", "bull"):
        cols[f"fvg_{side}"] = [False] * n
        for part in ("mid", "top", "bottom", "t1"):
            cols[f"fvg_{side}_{part}"] = [None] * n
    for name, spec in series.items():
        if isinstance(spec, tuple):
            start, value = spec
            for i in range(start, n):
                cols[name][i] = value
        else:
            for i, v in spec.items():
                cols[name][i] = v
    schema = {k: (pl.Boolean if k in ("swing_high", "swing_low", "fvg_bear", "fvg_bull") else pl.Int64 if k in ("open_time", "sh1_time", "sl1_time", "fvg_bear_t1", "fvg_bull_t1") else pl.Float64) for k in cols}
    return pl.DataFrame(cols, schema=schema)


def short_scenario(n: int = 40, **over) -> pl.DataFrame:
    """Пул 110 (свинг на баре 5, подтверждён на 10), свинг-лоу 104 (бар 8, подтверждён на 12), свип на 15,
    FVG на 17 (mid 105), CHoCH на 18 (close 103 < 104)."""
    s = {
        "swing_high": {10: True}, "sh1_price": (10, 110.0), "sh1_time": (10, 5 * STEP),
        "swing_low": {12: True}, "sl1_price": (12, 104.0), "sl1_time": (12, 8 * STEP),
        "high": {15: 112.0, 16: 109.0, 17: 105.0, 18: 104.0}, "low": {15: 107.0, 16: 105.0, 17: 102.0, 18: 102.0},
        "open": {15: 109.0, 16: 108.0, 17: 105.0, 18: 104.0}, "close": {15: 108.0, 16: 106.0, 17: 104.5, 18: 103.0},
        "fvg_bear": {17: True}, "fvg_bear_top": {17: 107.0}, "fvg_bear_bottom": {17: 103.0}, "fvg_bear_mid": {17: 105.0},
        "fvg_bear_t1": {17: 15 * STEP},
    }
    s.update(over)
    return frame(n, **s)


def run(df: pl.DataFrame, params: SmcSetupParams = P) -> pl.DataFrame:
    return detect_smc(df, params)


def col(out: pl.DataFrame, name: str) -> list:
    return out[name].to_list()


# ------------------------------------------------------------------------------------- шорт: ручной сценарий


def test_short_full_chain_s1_to_s4_with_exact_prices():
    out = run(short_scenario())
    assert [i for i, v in enumerate(col(out, "short_sweep")) if v] == [15]
    st = col(out, "short_state")
    assert st[14] == 0 and st[15] == 1 and st[16] == 1  # S1: свип
    assert st[17] == 2  # S2: FVG
    assert st[18] == 4  # S3 -> S4: CHoCH и заявка в одной строке
    assert st[19] == 0 and st[25] == 0  # сетап отработан
    r = out.row(18, named=True)
    assert r["short_signal"] and r["short_choch"] and r["short_reject"] is None
    assert r["short_entry"] == 105.0  # D7: mid FVG
    assert r["short_sl"] == pytest.approx(112.2)  # хай свипа 112 + 0.2 * ATR(1)
    assert r["short_tp"] == pytest.approx(105.0 - 4 * 7.2)  # 4R (1:4), R = 112.2 - 105
    assert r["short_pool_level"] == 110.0 and r["short_sweep_extreme"] == 112.0
    assert (r["short_fvg_hi"], r["short_fvg_lo"]) == (107.0, 103.0) and r["short_fvg_t1"] == 15 * STEP
    assert sum(col(out, "short_signal")) == 1 and not any(col(out, "long_sweep"))


def test_no_sweep_when_close_stays_above_pool():
    out = run(short_scenario(close={15: 111.0, 16: 106.0, 17: 104.5, 18: 103.0}))
    assert not any(col(out, "short_sweep")) and not any(col(out, "short_signal"))  # high > пул, но close не вернулся


def test_pool_taken_by_earlier_bar_is_not_swept_again():
    # бар 13 пробивает пул (high 111) и закрывается выше -> пул снят; свип на 15 уже не считается
    out = run(short_scenario(high={13: 111.0, 15: 112.0, 16: 109.0, 17: 105.0, 18: 104.0}, close={13: 110.5, 15: 108.0, 16: 106.0, 17: 104.5, 18: 103.0}))
    assert not any(col(out, "short_sweep"))


def test_first_sweep_wins_second_attempt_on_same_pool_is_ignored():
    out = run(short_scenario(high={13: 111.0, 15: 112.0, 16: 109.0, 17: 105.0, 18: 104.0}, close={13: 109.0, 15: 108.0, 16: 106.0, 17: 104.5, 18: 103.0}))
    assert [i for i, v in enumerate(col(out, "short_sweep")) if v] == [13]  # первый свип на 13; на 15 пул уже снят


def test_invalidated_by_close_above_sweep_high():
    out = run(short_scenario(close={15: 108.0, 16: 112.5, 17: 104.5, 18: 103.0}, high={15: 112.0, 16: 113.0, 17: 105.0, 18: 104.0}))
    assert col(out, "short_dead")[16] and not any(col(out, "short_signal"))
    assert col(out, "short_state")[17] == 0


def test_new_high_after_sweep_raises_sl_and_is_not_invalidation_by_itself():
    # high 113 на баре 16 при close 106: хай свипа вырос (SL выше), закрытие не выше -> сетап жив
    out = run(short_scenario(high={15: 112.0, 16: 113.0, 17: 105.0, 18: 104.0}))
    r = out.row(18, named=True)
    assert r["short_signal"] and r["short_sl"] == pytest.approx(113.2)


def test_choch_without_fvg_kills_setup_even_if_fvg_appears_later():
    # FVG на баре 17 убираем вручную
    df = short_scenario().with_columns(
        pl.when(pl.int_range(pl.len()) == 17).then(False).otherwise(pl.col("fvg_bear")).alias("fvg_bear")
    )
    out = run(df)
    assert col(out, "short_nofvg_break")[18] and not any(col(out, "short_signal")) and not any(col(out, "short_choch"))


def test_fvg_before_the_sweep_does_not_count():
    # FVG на баре 16 -> его первая свеча (14) раньше свипа (15): не относится к сетапу
    df = short_scenario().with_columns(
        pl.when(pl.int_range(pl.len()) == 17).then(False).when(pl.int_range(pl.len()) == 16).then(True).otherwise(pl.col("fvg_bear")).alias("fvg_bear")
    ).with_columns(
        pl.when(pl.int_range(pl.len()) == 16).then(105.0).otherwise(pl.col("fvg_bear_mid")).alias("fvg_bear_mid")
    )
    out = run(df)
    assert not any(col(out, "short_signal")) and col(out, "short_nofvg_break")[18]


def test_fvg_with_first_candle_on_sweep_bar_counts():
    # FVG завершается на баре 17 -> i-2 = 15 = бар свипа: допустимо (базовый сценарий уже такой)
    assert run(short_scenario())["short_signal"].sum() == 1


def test_latest_fvg_is_used_for_entry():
    df = short_scenario(fvg_bear={17: True, 18: True}, fvg_bear_top={17: 107.0, 18: 106.0}, fvg_bear_bottom={17: 103.0, 18: 104.0},
                        fvg_bear_mid={17: 105.0, 18: 105.0})
    out = run(df)
    assert out.row(18, named=True)["short_entry"] == 105.0
    df = short_scenario(fvg_bear={17: True, 18: True}, fvg_bear_mid={17: 105.0, 18: 106.0}, fvg_bear_top={17: 107.0, 18: 107.0}, fvg_bear_bottom={17: 103.0, 18: 105.0})
    assert run(df).row(18, named=True)["short_entry"] == 106.0  # FVG на самом баре CHoCH - последний


def test_setup_ttl_choch_at_15_bars_ok_16_too_late_d8():
    def late(break_row: int) -> pl.DataFrame:
        # CHoCH откладываем: close в баре break_row падает ниже 104, до этого остаётся выше
        close = {i: 106.0 for i in range(16, break_row)} | {break_row: 103.0}
        high = {i: 108.0 for i in range(16, break_row + 1)} | {15: 112.0}
        low = {i: 105.5 for i in range(16, break_row)} | {break_row: 102.0}
        return short_scenario(60, close=close | {15: 108.0}, high=high, low=low | {15: 107.0}, open={break_row: 105.0},
                              fvg_bear={17: True}, fvg_bear_top={17: 107.0}, fvg_bear_bottom={17: 103.0}, fvg_bear_mid={17: 105.0})
    p15 = SmcSetupParams(setup_ttl_bars=15)  # срок D8 был 15 (до О12), граница проверяется на явном значении
    assert run(late(30), p15)["short_signal"].sum() == 1  # 30 - 15 = 15 баров после свипа
    assert run(late(31), p15)["short_signal"].sum() == 0  # 16 баров - просрочено
    assert col(run(late(31), p15), "short_state")[31] == 0
    assert SmcSetupParams().setup_ttl_bars == 30  # дефолт после О12


def test_reference_low_must_be_formed_after_swept_high_o10():
    out = run(short_scenario(sl1_time=(12, 2 * STEP)))  # свинг-лоу старше пула (бар 2 < бар 5)
    assert not any(col(out, "short_signal")) and not any(col(out, "short_nofvg_break"))


def test_limit_must_be_passive_else_rejected_and_logged():
    out = run(short_scenario(fvg_bear_mid={17: 102.0}, close={15: 108.0, 16: 106.0, 17: 104.5, 18: 103.0}))
    r = out.row(18, named=True)  # mid 102 <= close 103: sell-limit была бы ниже рынка
    assert r["short_choch"] and not r["short_signal"] and r["short_reject"] == "not_passive" and r["short_state"] == 3
    assert r["short_entry"] is None


def test_limit_exactly_at_market_is_not_passive():
    out = run(short_scenario(fvg_bear_mid={17: 103.0}))  # mid == close CHoCH-бара (103): GTX отклонил бы как тейкер
    assert out.row(18, named=True)["short_reject"] == "not_passive" and not any(col(out, "short_signal"))


def test_exposure_cap_rejects_instead_of_shrinking_o8():
    # R = 7.2 при entry 105: плечо = 0.01 * 105 / 7.2 = 0.146 -> проходит; cap 0.1 -> reject
    assert run(short_scenario())["short_signal"].sum() == 1
    out = run(short_scenario(), SmcSetupParams(max_exposure=0.001))
    r = out.row(18, named=True)
    assert not r["short_signal"] and r["short_reject"] == "exposure" and r["short_choch"]


def test_new_sweep_replaces_old_setup():
    # второй свип другого пула на баре 20 (пул 115: свинг подтверждён на 19) заменяет сетап; CHoCH на 18 уже случился
    df = short_scenario(45, swing_high={10: True, 19: True}, sh1_price={**{i: 110.0 for i in range(10, 19)}, **{i: 115.0 for i in range(19, 45)}},
                        sh1_time={**{i: 5 * STEP for i in range(10, 19)}, **{i: 17 * STEP for i in range(19, 45)}},
                        high={15: 112.0, 16: 109.0, 17: 105.0, 18: 104.0, 21: 117.0}, close={15: 108.0, 16: 106.0, 17: 104.5, 18: 103.0, 21: 114.0},
                        low={15: 107.0, 16: 105.0, 17: 102.0, 18: 102.0, 21: 110.0})
    out = run(df)
    assert [i for i, v in enumerate(col(out, "short_sweep")) if v] == [15, 21]
    assert col(out, "short_sid")[20] == 1 and col(out, "short_sid")[21] == 2


# ------------------------------------------------------------------------------------------- зеркальность


def mirror(df: pl.DataFrame, c: float = 200.0) -> pl.DataFrame:
    """Зеркало цен p -> c - p: шорт-сценарий становится лонг-сценарием (high<->low, swing_high<->swing_low, bear<->bull)."""
    return df.select(
        "open_time", (c - pl.col("open")).alias("open"), (c - pl.col("low")).alias("high"), (c - pl.col("high")).alias("low"),
        (c - pl.col("close")).alias("close"), "atr",
        pl.col("swing_low").alias("swing_high"), pl.col("swing_high").alias("swing_low"),
        (c - pl.col("sl1_price")).alias("sh1_price"), pl.col("sl1_time").alias("sh1_time"),
        (c - pl.col("sh1_price")).alias("sl1_price"), pl.col("sh1_time").alias("sl1_time"),
        pl.col("fvg_bear").alias("fvg_bull"), (c - pl.col("fvg_bear_bottom")).alias("fvg_bull_top"),
        (c - pl.col("fvg_bear_top")).alias("fvg_bull_bottom"), (c - pl.col("fvg_bear_mid")).alias("fvg_bull_mid"), pl.col("fvg_bear_t1").alias("fvg_bull_t1"),
        pl.col("fvg_bull").alias("fvg_bear"), pl.col("fvg_bull_top").alias("fvg_bear_top"),
        pl.col("fvg_bull_bottom").alias("fvg_bear_bottom"), pl.col("fvg_bull_mid").alias("fvg_bear_mid"), pl.col("fvg_bull_t1").alias("fvg_bear_t1"),
    )


def test_long_is_exact_mirror_of_short_on_handmade_scenario():
    s, lg = run(short_scenario()), run(mirror(short_scenario()))
    for name in ("sweep", "sid", "sweep_idx", "dead", "nofvg_break", "state", "choch", "signal", "reject"):
        assert col(s, f"short_{name}") == col(lg, f"long_{name}"), name
    r = lg.row(18, named=True)
    assert r["long_entry"] == pytest.approx(200 - 105.0) and r["long_sl"] == pytest.approx(200 - 112.2)
    assert r["long_tp"] == pytest.approx(200 - (105.0 - 28.8)) and r["long_pool_level"] == pytest.approx(200 - 110.0)
    assert (r["long_fvg_hi"], r["long_fvg_lo"]) == (pytest.approx(200 - 103.0), pytest.approx(200 - 107.0))


def mirrored_candles(df: pl.DataFrame, c: float) -> pl.DataFrame:
    return df.with_columns(
        (c - pl.col("open")).alias("open"), (c - pl.col("low")).alias("high"), (c - pl.col("high")).alias("low"), (c - pl.col("close")).alias("close")
    )


def test_long_equals_short_on_mirrored_random_data_through_whole_pipeline():
    big = SmcSetupParams(max_exposure=1e9)
    fp = SmcParams(swing_atr_mult=0.5)
    df = random_walk(2500, 31, 0.012)
    c = 2 * float(df["close"].max())
    f1, f2 = add_features(df, fp, interval_ms=STEP), add_features(mirrored_candles(df, c), fp, interval_ms=STEP)
    a, b = detect_smc(f1, big), detect_smc(f2, big)
    for name in ("sweep", "sid", "sweep_idx", "dead", "nofvg_break", "state", "choch", "signal", "reject"):
        assert a[f"short_{name}"].to_list() == b[f"long_{name}"].to_list(), name
        assert a[f"long_{name}"].to_list() == b[f"short_{name}"].to_list(), name
    assert a["short_sweep"].sum() > 10 and a["long_sweep"].sum() > 10 and (a["short_choch"].sum() + a["long_choch"].sum()) > 0
    sig = a.filter(pl.col("short_signal"))
    if sig.height:  # цены сигналов зеркальны
        mirror_rows = b.filter(pl.col("long_signal"))
        assert [round(c - x, 6) for x in sig["short_entry"]] == [round(x, 6) for x in mirror_rows["long_entry"]]


# ---------------------------------------------------------------------------------- R4/R8 и защита кода


def pipeline(d: pl.DataFrame, fp: SmcParams, sp: SmcSetupParams) -> pl.DataFrame:
    return detect_smc(add_features(d, fp, interval_ms=STEP), sp).select(
        "open_time", *[c for side in ("short", "long") for c in setup_columns(side)]
    )


@pytest.mark.parametrize(("seed", "vol"), [(41, 0.012), (42, 0.02)])
def test_truncation_r4_all_setup_columns(seed, vol):
    fp = SmcParams(k=3, atr_period=5, swing_atr_mult=0.3, impulse_atr_mult=1.0, eq_tolerance=0.02, volume_sma=5)
    sp = SmcSetupParams(max_exposure=1e9)
    df = random_walk(1500, seed, vol)
    fn = lambda d: pipeline(d, fp, sp)  # noqa: E731
    ts = sorted(random.Random(seed).sample(range(df.height), 250))
    assert_truncation_invariant(fn, df, ts)
    full = fn(df)  # нетривиальность: автомат дошёл до всех состояний
    assert full["short_sweep"].sum() > 10 and full["long_sweep"].sum() > 10
    assert (full["short_state"] >= 2).sum() + (full["long_state"] >= 2).sum() > 0


def test_truncation_covers_every_row_of_the_handmade_chain():
    df = short_scenario()
    fn = lambda d: detect_smc(d, P).select([c for side in ("short", "long") for c in setup_columns(side)])  # noqa: E731
    assert_truncation_invariant(fn, df, list(range(df.height)))


def test_live_window_matches_full_history_after_warmup_r8():
    fp = SmcParams(k=3, atr_period=5, swing_atr_mult=0.3, impulse_atr_mult=1.0, eq_tolerance=0.02, volume_sma=5)
    sp = SmcSetupParams(max_exposure=1e9)
    df = random_walk(1400, 51, 0.015)
    full = pipeline(df, fp, sp)
    base = add_features(df, fp, interval_ms=STEP)
    window, checked, compared = 300, 0, 0
    for t in range(window, df.height, 3):
        win = pipeline(df.slice(t - window + 1, window), fp, sp).row(window - 1, named=True)
        ref = full.row(t, named=True)
        cutoff = df["open_time"][t - window + 1] + fp.warmup_bars * STEP
        times = [ref[f"{s}_pool_time"] for s in ("short", "long")]
        feat = base.row(t, named=True)
        used = [feat[k] for k in ("sh1_time", "sl1_time")]
        if any(v is None for v in used) or min(used) < cutoff or any(v is not None and v < cutoff for v in times):
            continue
        checked += 1
        names = [c for s in ("short", "long") for c in setup_columns(s) if not c.endswith(("_sid", "_sweep_idx"))]
        assert {c: win[c] for c in names} == {c: ref[c] for c in names}, f"t={t}"
        compared += win["short_state"] + win["long_state"]
    assert checked > 30 and compared > 0  # в сравнение попали живые сетапы


def test_no_row_iteration_in_state_machine_r2():
    root = Path(__file__).resolve().parents[2]
    for rel in ("strategies/smc_setups.py", "strategies/breakout.py"):
        src = (root / rel).read_text(encoding="utf-8")
        code = "\n".join(line.split("#")[0] for line in src.splitlines())
        banned = r"\.apply\(|iter_rows|map_elements|map_rows|map_batches|rolling_map|\.rows\(|\.iter_|to_dicts|\.iter_rows|\.row\("
        assert re.search(banned, code) is None, (rel, re.findall(banned, code))


def test_inputs_and_params_are_validated():
    with pytest.raises(DataError, match="add_features"):
        detect_smc(frame().drop("fvg_bull_mid"))
    with pytest.raises(DataError, match="уже есть"):
        detect_smc(run(frame()))
    with pytest.raises(ConfigError):
        SmcSetupParams(setup_ttl_bars=0)
    with pytest.raises(ConfigError):
        SmcSetupParams(max_exposure=0)
    assert math.isfinite(P.tp_r)
