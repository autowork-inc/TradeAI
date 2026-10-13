"""HTF-фильтр автомата SMC (2026-10-11): против структуры H4 сетап не начинается (S0 -> S1 запрещён); R2/R3/R4 сохранены."""

import random

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from strategies.smc_setups import SmcSetupParams, detect_smc
from tests.features.test_smc import random_walk
from tests.strategies.test_smc_setups import STEP, mirror, short_scenario
from utils.errors import ConfigError, DataError

ON, OFF = SmcSetupParams(htf_filter=True), SmcSetupParams()


def with_bias(df: pl.DataFrame, bias: int | None) -> pl.DataFrame:
    return df.with_columns(pl.lit(bias, dtype=pl.Int8).alias("htf_bias"))


def sweeps(out: pl.DataFrame, side: str) -> list[int]:
    return [i for i, v in enumerate(out[f"{side}_sweep"].to_list()) if v]


def test_short_is_blocked_against_bullish_h4_and_allowed_otherwise():
    base = detect_smc(short_scenario(), OFF)
    assert base["short_signal"].sum() == 1
    blocked = detect_smc(with_bias(short_scenario(), 1), ON)  # H4 бычий: шорты не ищем
    assert sweeps(blocked, "short") == [] and blocked["short_signal"].sum() == 0
    assert set(blocked["short_state"].to_list()) == {0}  # автомат не покинул S0: в S1 перехода нет
    for bias in (-1, 0):  # по тренду и в нейтральной структуре сигнал тот же, что без фильтра, до цены
        out = detect_smc(with_bias(short_scenario(), bias), ON)
        assert out["short_signal"].to_list() == base["short_signal"].to_list()
        assert out["short_entry"].to_list() == base["short_entry"].to_list() and out["short_state"].to_list() == base["short_state"].to_list()


def test_unknown_bias_fails_closed():
    out = detect_smc(with_bias(short_scenario(), None), ON)  # прогрев H4 / нет данных: торговать нельзя
    assert sweeps(out, "short") == [] and out["short_signal"].sum() == 0


def test_long_is_the_mirror_blocked_against_bearish_h4():
    df = mirror(short_scenario())
    assert detect_smc(df, OFF)["long_signal"].sum() == 1
    assert detect_smc(with_bias(df, -1), ON)["long_signal"].sum() == 0
    assert set(detect_smc(with_bias(df, -1), ON)["long_state"].to_list()) == {0}
    for bias in (1, 0):
        assert detect_smc(with_bias(df, bias), ON)["long_signal"].sum() == 1
    # шорт на зеркальных данных не возникает ни при каком bias - и фильтр этого не меняет
    assert detect_smc(with_bias(df, 1), ON)["short_signal"].sum() == 0


def test_filter_gates_only_the_sweep_bar_not_the_rest_of_the_setup():
    """Bias на баре свипа (15) разрешает шорт, а позже H4 стал бычьим: уже начатый сетап доводится до сигнала (запрещён лишь переход в S1)."""
    n = short_scenario().height
    flip = pl.Series("htf_bias", [-1 if i <= 16 else 1 for i in range(n)], dtype=pl.Int8)
    out = detect_smc(short_scenario().with_columns(flip), ON)
    assert sweeps(out, "short") == [15] and out["short_signal"].sum() == 1
    # и наоборот: бычий на баре свипа - сетапа нет, даже если дальше bias стал медвежьим
    flip2 = pl.Series("htf_bias", [1 if i <= 15 else -1 for i in range(n)], dtype=pl.Int8)
    assert detect_smc(short_scenario().with_columns(flip2), ON)["short_signal"].sum() == 0


def test_filter_off_ignores_the_column_and_does_not_require_it():
    assert detect_smc(short_scenario(), OFF)["short_signal"].sum() == 1  # колонки htf_bias нет - фильтр выключен, всё как раньше
    assert detect_smc(with_bias(short_scenario(), 1), OFF)["short_signal"].sum() == 1  # колонка есть, но не используется


def test_filter_on_without_the_column_is_an_error_not_a_silent_pass_r1():
    with pytest.raises(DataError, match="htf_bias"):
        detect_smc(short_scenario(), ON)
    with pytest.raises(ConfigError):
        SmcSetupParams(htf_filter="yes")  # type: ignore[arg-type]


def random_bias(n: int, seed: int) -> pl.Series:
    rnd, cur, vals = random.Random(seed), 0, []
    for _ in range(n):
        if rnd.random() < 0.02:  # bias меняется редко, как структура H4
            cur = rnd.choice((-1, 0, 1))
        vals.append(cur)
    return pl.Series("htf_bias", vals, dtype=pl.Int8)


def test_sweeps_with_filter_are_exactly_the_unfiltered_sweeps_at_allowed_bars():
    """Фильтр только вычёркивает свипы (taken_prev от него не зависит): sweep_on == sweep_off & сторона_разрешена."""
    p = SmcParams(swing_atr_mult=0.5)
    df = add_features(random_walk(3000, 17, 0.012), p, interval_ms=STEP)
    df = df.with_columns(random_bias(df.height, 3))
    on, off = detect_smc(df, SmcSetupParams(htf_filter=True, max_exposure=1e9)), detect_smc(df.drop("htf_bias"), SmcSetupParams(max_exposure=1e9))
    bias = df["htf_bias"]
    for side, allowed in (("short", bias <= 0), ("long", bias >= 0)):
        want = (off[f"{side}_sweep"] & allowed).to_list()
        assert on[f"{side}_sweep"].to_list() == want
        assert sum(off[f"{side}_sweep"]) > 5 and sum(on[f"{side}_sweep"]) < sum(off[f"{side}_sweep"])  # тест не пустой и фильтр что-то режет


def test_truncation_invariant_with_filter_r4():
    """R4: строка t расчёта по префиксу [0..t] совпадает с строкой t по всей истории (bias - входная колонка, сама каузальная)."""
    df = add_features(random_walk(900, 23, 0.012), SmcParams(swing_atr_mult=0.5), interval_ms=STEP)
    df = df.with_columns(random_bias(df.height, 8))
    p = SmcSetupParams(htf_filter=True, max_exposure=1e9)
    full = detect_smc(df, p)
    rnd = random.Random(1)
    for t in sorted(rnd.sample(range(150, 900), 60)):
        cut = detect_smc(df.head(t + 1), p).row(t, named=True)
        whole = full.row(t, named=True)
        assert {c: (cut[c], whole[c]) for c in whole if cut[c] != whole[c]} == {}, f"t={t}"
