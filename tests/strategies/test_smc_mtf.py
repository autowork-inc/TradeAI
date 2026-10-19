"""Мульти-таймфреймовый автомат SMC (strategies/smc_setups.py, mtf=True): свип - событие 1h, FVG/CHoCH/вход - на 5m строго после него, bias - с 1d.

Ручные сценарии берут готовую цепочку одно-таймфреймового теста (`short_scenario`) и подкладывают 1h-колонки так, что событие свипа приходится на ту же строку:
тогда MTF-автомат обязан дать ТОЧНО тот же сигнал (цены, состояния). Остальное - сдвиги события, фильтр 1d, зеркальность, R4 на настоящем 5m->1h конвейере."""

import random

import polars as pl
import pytest

from features.mtf import D1_BIAS, H1_AVAIL, H1_OPEN, attach_table, h1_columns, h1_sweep_columns, h1_table
from features.smc import SmcParams, add_features
from strategies.smc_setups import SmcSetupParams, detect_smc, setup_columns
from tests.features.test_mtf import BASE, FP, H1, M5, resample, walk5
from tests.strategies.test_smc_setups import STEP, mirror, short_scenario
from utils.errors import ConfigError, DataError

MTF = SmcSetupParams(mtf=True)
MTF_F = SmcSetupParams(mtf=True, htf_filter=True)
SINGLE = SmcSetupParams()


def with_mtf(df: pl.DataFrame, side: str, event: int | None, *, pool: float | None = None, pool_time: int | None = None, extreme: float | None = None,
             bias: int | None = 0, bias_col: bool = True) -> pl.DataFrame:
    """Кадр + колонки 1h/1d: на строке `event` (и далее, как forward-fill) - свип 1h стороны `side`; None - свипов нет совсем. Значения по умолчанию - как у
    одно-таймфреймового автомата на этой же строке (пул, время пула, экстремум = хай/лоу свечи свипа)."""
    n = df.height
    ot = df["open_time"].to_list()
    base = detect_smc(df, SINGLE)
    row = base.row(event, named=True) if event is not None and base[f"{side}_sweep"][event] else None
    if event is not None:
        pool = pool if pool is not None else (row[f"{side}_pool_level"] if row else 100.0)
        pool_time = pool_time if pool_time is not None else (row[f"{side}_pool_time"] if row else 0)
        extreme = extreme if extreme is not None else (row[f"{side}_sweep_extreme"] if row else 100.0)
    cols: dict[str, list] = {name: [None] * n for name in h1_columns()}
    for i in range(n):
        live = event is not None and i >= event
        cols[H1_AVAIL][i] = ot[event] if live else ot[0] - 1  # «последняя закрытая 1h» - до события другая (не совпадает ни с одним open_time)
        cols[H1_OPEN][i] = ot[event] - H1 if live else ot[0] - 2 * H1
        for s in ("short", "long"):
            names = h1_sweep_columns(s)
            on = live and s == side
            cols[names[0]][i] = on
            cols[names[1]][i] = pool if on else None
            cols[names[2]][i] = pool_time if on else None
            cols[names[3]][i] = extreme if on else None
    out = df.with_columns([pl.Series(k, v, dtype=pl.Boolean if k.endswith("_sweep") else pl.Float64 if "pool_time" not in k and k not in (H1_AVAIL, H1_OPEN) else pl.Int64)
                           for k, v in cols.items() if k not in df.columns])
    return out.with_columns(pl.lit(bias, dtype=pl.Int8).alias(D1_BIAS)) if bias_col else out


def sweeps(out: pl.DataFrame, side: str) -> list[int]:
    return [i for i, v in enumerate(out[f"{side}_sweep"].to_list()) if v]


def assert_same_chain(a: pl.DataFrame, b: pl.DataFrame, side: str) -> None:
    for c in setup_columns(side):
        assert a[c].to_list() == b[c].to_list(), c


# ------------------------------------------------------------------------------------------- шорт: ручные сценарии


def test_event_on_the_single_timeframe_sweep_row_gives_exactly_the_same_chain():
    df = short_scenario()
    single = detect_smc(df, SINGLE)
    mtf = detect_smc(with_mtf(df, "short", 15), MTF)
    assert sweeps(mtf, "short") == [15] and single["short_signal"].sum() == 1
    assert_same_chain(single, mtf, "short")  # S1..S4, цены, причина: бит-в-бит
    assert mtf.row(18, named=True)["short_sl"] == pytest.approx(112.2) and not any(mtf["long_sweep"])


def test_sweep_is_one_row_event_and_the_5m_bars_of_the_same_hour_do_not_repeat_it():
    out = detect_smc(with_mtf(short_scenario(), "short", 15), MTF)
    assert sweeps(out, "short") == [15]  # на строках 16..39 колонки 1h те же (forward-fill), но событием они не являются
    assert sum(out["short_sid"].to_list()[16:]) == len(out["short_sid"].to_list()[16:])  # один сетап


def test_sweep_on_5m_without_a_1h_sweep_starts_nothing():
    """На самой 5m-свече свип пула есть (high 112 > 110, close 108 < 110), но 1h-свипа нет: сетап не начинается (S0/S1 только с 1h)."""
    df = short_scenario()
    assert detect_smc(df, SINGLE)["short_signal"].sum() == 1
    out = detect_smc(with_mtf(df, "short", None), MTF)
    assert not any(out["short_sweep"]) and not any(out["short_signal"]) and set(out["short_state"].to_list()) == {0}


def test_fvg_and_choch_before_the_1h_sweep_is_known_do_not_count():
    """Событие 1h пришло на строке 17 (позже FVG, первая свеча которого - 15, и раньше CHoCH на 18): FVG до свипа не считается -> CHoCH без FVG убивает сетап."""
    df = short_scenario()
    out = detect_smc(with_mtf(df, "short", 17, pool=110.0, pool_time=5 * STEP, extreme=112.0), MTF)
    assert sweeps(out, "short") == [17] and not any(out["short_signal"]) and out["short_nofvg_break"].to_list()[18]


def test_event_before_the_fvg_still_gives_the_signal_and_state_walks_s1_s2_s4():
    df = short_scenario()
    out = detect_smc(with_mtf(df, "short", 14, pool=110.0, pool_time=5 * STEP, extreme=112.0), MTF)
    assert sweeps(out, "short") == [14]
    st = out["short_state"].to_list()
    assert st[14] == 1 and st[16] == 1 and st[17] == 2 and st[18] == 4 and out["short_signal"].to_list()[18]


def test_sl_is_behind_the_1h_sweep_extreme_not_only_behind_the_5m_highs():
    df = short_scenario()
    out = detect_smc(with_mtf(df, "short", 15, extreme=118.0), MTF)  # свипающая 1h-свеча сделала хай выше любого 5m-хая сетапа
    r = out.row(18, named=True)
    assert r["short_sweep_extreme"] == 118.0 and r["short_sl"] == pytest.approx(118.2) and r["short_signal"]
    assert r["short_tp"] == pytest.approx(105.0 - 4 * (118.2 - 105.0))


def test_setup_ttl_is_counted_in_5m_bars_from_the_event_row():
    df = short_scenario(n=130)
    ok = detect_smc(with_mtf(df, "short", 15), SmcSetupParams(mtf=True, setup_ttl_bars=3))  # CHoCH на 18 = 3 бара от события
    assert ok["short_signal"].to_list()[18]
    late = detect_smc(with_mtf(df, "short", 15), SmcSetupParams(mtf=True, setup_ttl_bars=2))
    assert not any(late["short_signal"])


# ------------------------------------------------------------------------------------------------- фильтр 1d


def test_1d_bias_gates_only_the_event_row_like_the_h4_filter():
    df = short_scenario()
    for bias, allowed in ((1, False), (0, False), (-1, True), (None, False)):
        out = detect_smc(with_mtf(df, "short", 15, bias=bias), MTF_F)
        assert (out["short_signal"].sum() == 1) is allowed, bias
        if not allowed:
            assert set(out["short_state"].to_list()) == {0}
    n = df.height  # bias перевернулся ПОСЛЕ события: начатый сетап доводится до сигнала
    flip = with_mtf(df, "short", 15, bias_col=False).with_columns(pl.Series(D1_BIAS, [-1 if i <= 16 else 1 for i in range(n)], dtype=pl.Int8))
    assert detect_smc(flip, MTF_F)["short_signal"].sum() == 1


def test_filter_reads_1d_bias_and_does_not_need_h4_column():
    df = with_mtf(short_scenario(), "short", 15, bias=1)
    assert "htf_bias" not in df.columns and detect_smc(df, MTF_F)["short_signal"].sum() == 0
    no_filter = detect_smc(df, MTF)
    assert no_filter["short_signal"].sum() == 1  # без htf_filter колонка 1d_bias не используется
    with pytest.raises(DataError, match="1d_bias"):
        detect_smc(with_mtf(short_scenario(), "short", 15, bias_col=False), MTF_F)
    with pytest.raises(DataError, match="1h_avail_ms"):
        detect_smc(short_scenario(), MTF)
    with pytest.raises(ConfigError):
        SmcSetupParams(mtf="yes")  # type: ignore[arg-type]


# --------------------------------------------------------------------------------------------------- лонг и R-свойства


def test_long_is_exact_mirror_of_short_on_the_handmade_chain():
    s = detect_smc(with_mtf(short_scenario(), "short", 15), MTF)
    lg_df = mirror(short_scenario())
    lg = detect_smc(with_mtf(lg_df, "long", 15), MTF)
    for name in ("sweep", "sid", "sweep_idx", "dead", "nofvg_break", "state", "choch", "signal", "reject"):
        assert s[f"short_{name}"].to_list() == lg[f"long_{name}"].to_list(), name
    r = lg.row(18, named=True)
    assert r["long_entry"] == pytest.approx(200 - 105.0) and r["long_sl"] == pytest.approx(200 - 112.2) and r["long_pool_level"] == pytest.approx(200 - 110.0)


def mtf_pipeline(df5: pl.DataFrame, bias: int = 0) -> pl.DataFrame:
    f5 = add_features(df5, FP, interval_ms=M5)
    h1 = add_features(resample(df5, H1), FP, interval_ms=H1)
    return attach_table(f5, h1_table(h1, H1), H1_AVAIL).with_columns(pl.lit(bias, dtype=pl.Int8).alias(D1_BIAS))


def test_long_equals_short_on_mirrored_random_data_through_the_whole_5m_1h_pipeline():
    big = SmcSetupParams(mtf=True, max_exposure=1e9)  # без фильтра: постоянный bias жёстко разрешает лишь одну сторону, а тест проверяет симметрию автомата
    df = walk5(6000, 31, 0.012)
    c = 2 * float(df["close"].max())
    mirrored = df.with_columns((c - pl.col("open")).alias("open"), (c - pl.col("low")).alias("high"), (c - pl.col("high")).alias("low"), (c - pl.col("close")).alias("close"))
    a, b = detect_smc(mtf_pipeline(df), big), detect_smc(mtf_pipeline(mirrored), big)
    for name in ("sweep", "sid", "sweep_idx", "dead", "nofvg_break", "state", "choch", "signal", "reject"):
        assert a[f"short_{name}"].to_list() == b[f"long_{name}"].to_list(), name
        assert a[f"long_{name}"].to_list() == b[f"short_{name}"].to_list(), name
    assert a["short_sweep"].sum() > 3 and a["long_sweep"].sum() > 3 and (a["short_choch"].sum() + a["long_choch"].sum()) > 0  # тест не пустой


def test_sweeps_only_happen_on_the_first_5m_row_after_a_1h_close_and_truncation_is_invariant_r4():
    p = SmcSetupParams(mtf=True, max_exposure=1e9, setup_ttl_bars=90)  # фильтр тренда проверен отдельно (test_1d_bias_*)
    full_in = mtf_pipeline(walk5(4200, 17, 0.012))
    full = detect_smc(full_in, p)
    for side in ("short", "long"):
        rows = [i for i, v in enumerate(full[f"{side}_sweep"].to_list()) if v]
        assert rows and all(full_in["open_time"][i] == full_in[H1_AVAIL][i] for i in rows)  # свип - только на первой 5m-строке после закрытия 1h
    rnd = random.Random(2)
    for t in sorted(rnd.sample(range(300, 4200), 40)):
        cut = detect_smc(full_in.head(t + 1), p).row(t, named=True)
        whole = full.row(t, named=True)
        assert {c: (cut[c], whole[c]) for c in whole if cut[c] != whole[c]} == {}, f"t={t}"
