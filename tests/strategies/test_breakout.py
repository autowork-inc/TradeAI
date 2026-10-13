"""Тесты детектора Breakout Buildup (strategies/breakout.py) и сборки сигналов (strategies/runner.py)."""

import random

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from strategies.breakout import BreakoutParams, detect_breakout
from strategies.runner import breakout_signals, smc_signals
from tests.features.test_smc import STEP, assert_truncation_invariant, random_walk
from utils.errors import ConfigError, DataError


def frame(n: int = 20, **series) -> pl.DataFrame:
    cols = {
        "open_time": [i * STEP for i in range(n)], "close": [100.0] * n, "volume": [10.0] * n, "atr": [1.0] * n,
        "buildup": [False] * n, "buildup_level": [None] * n, "buildup_low3": [None] * n, "vol_ratio": [1.0] * n,
    }
    for name, spec in series.items():
        if isinstance(spec, tuple):
            for i in range(spec[0], n):
                cols[name][i] = spec[1]
        else:
            for i, v in spec.items():
                cols[name][i] = v
    schema = {k: pl.Boolean if k == "buildup" else pl.Int64 if k == "open_time" else pl.Float64 for k in cols}
    return pl.DataFrame(cols, schema=schema)


def armed(**over) -> pl.DataFrame:
    """Поджатие известно с бара 5 (уровень 110, Low3 = 100); пробой на баре 10 (close 111) с объёмом 3x."""
    s = {"buildup": (5, True), "buildup_level": (5, 110.0), "buildup_low3": (5, 100.0),
         "close": {9: 109.0, 10: 111.0, 11: 112.0}, "vol_ratio": {10: 3.0}}
    s.update(over)
    return frame(20, **s)


def test_breakout_trigger_on_cross_with_volume_and_sl_below_low3():
    out = detect_breakout(armed())
    assert out["breakout_signal"].to_list() == [i == 10 for i in range(20)]
    r = out.row(10, named=True)
    assert r["breakout_level"] == 110.0 and r["breakout_sl"] == pytest.approx(100.0 - 0.2 * 1.0)


def test_no_trigger_without_volume_spike():
    assert not any(detect_breakout(armed(vol_ratio={10: 2.0}))["breakout_signal"])  # строго больше 2x


def test_no_trigger_when_already_above_level_only_the_cross_counts():
    out = detect_breakout(armed(close={9: 111.0, 10: 112.0, 11: 113.0}, vol_ratio={9: 3.0, 10: 3.0, 11: 3.0}))
    assert not any(out["breakout_signal"][10:])  # бар 9 - пробой (cross), далее close уже выше уровня
    assert out["breakout_signal"][9]


def test_no_trigger_when_buildup_confirmed_only_on_the_breakout_bar():
    out = detect_breakout(armed(buildup=(10, True)))  # на баре j-1 = 9 поджатия ещё не было
    assert not any(out["breakout_signal"])


def test_volume_is_taken_from_the_breakout_bar_not_the_previous_one():
    assert not any(detect_breakout(armed(vol_ratio={9: 3.0, 10: 1.0}))["breakout_signal"])


def test_breakout_inputs_validated():
    with pytest.raises(DataError, match="add_features"):
        detect_breakout(frame().drop("vol_ratio"))
    with pytest.raises(DataError, match="уже есть"):
        detect_breakout(detect_breakout(frame()))
    with pytest.raises(ConfigError):
        BreakoutParams(volume_mult=0)


def test_breakout_end_to_end_on_zigzag_buildup_and_truncation_r4():
    from tests.features.test_smc import BUILDUP, SMALL, zigzag

    wp = [*BUILDUP[:-1], (27, 98.0), (30, 100.0), (31, 101.5), (34, 102.0)]  # поджатие, затем рост и пробой уровня 100.06
    df = zigzag(wp).with_columns(volume=pl.Series([100.0 if i == 31 else 10.0 for i in range(35)]))

    def fn(d: pl.DataFrame) -> pl.DataFrame:
        return detect_breakout(add_features(d, SMALL, interval_ms=STEP)).select(
            "open_time", "buildup", "breakout_signal", "breakout_level", "breakout_sl"
        )

    full = fn(df)
    assert [i for i, v in enumerate(full["breakout_signal"]) if v] == [31]  # пересечение 100.06 с объёмом 10x
    r = full.row(31, named=True)
    assert r["breakout_level"] == pytest.approx(100.06) and r["breakout_sl"] < 93.99  # под Low 3 с буфером 0.2*ATR
    assert_truncation_invariant(fn, df, list(range(df.height)))  # все t подряд, включая бар пробоя
    # и на случайных данных (сигналов там почти нет, но проверка обрезки остаётся в силе для остальных колонок)
    rnd = random_walk(1500, 61, 0.012)
    fp = SmcParams(k=3, atr_period=5, swing_atr_mult=0.2, eq_tolerance=0.03, volume_sma=5)
    g = lambda d: detect_breakout(add_features(d, fp, interval_ms=STEP)).select("open_time", "buildup", "buildup_level", "breakout_signal")  # noqa: E731
    assert_truncation_invariant(g, rnd, sorted(random.Random(3).sample(range(rnd.height), 200)))
    assert g(rnd)["buildup"].sum() > 0  # на случайных данных поджатия встречаются, и тест их проходит


def test_signal_tables_have_the_columns_the_simulator_needs():
    from strategies.simulate import SIGNAL_COLUMNS

    det = detect_breakout(armed()).with_columns(pl.col("close").alias("open"))
    b = breakout_signals(det.with_columns(pl.col("close").alias("high"), pl.col("close").alias("low")))
    assert b.height == 1 and b["kind"][0] == "market" and b["sgn"][0] == -1.0 and b["idx"][0] == 10
    assert {"idx", "sgn", "kind", "entry", "sl", "tp", "inval"} <= set(b.columns)
    assert set(SIGNAL_COLUMNS) - {"sig_id"} <= set(b.columns)
    assert smc_signals is not None
