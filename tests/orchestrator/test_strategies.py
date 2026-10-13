"""StrategySMC / StrategyBreakout / FeatureEngine: сигналы из последней строки; паритет live и бэктеста (R8)."""

import polars as pl
import pytest

from features.smc import SmcParams, add_features
from orchestrator.feature_engine import FeatureEngine
from orchestrator.strategies import StrategyBreakout, StrategySMC
from orchestrator.types import Direction, FeatureContext, Kind
from strategies.breakout import detect_breakout
from strategies.smc_setups import SmcSetupParams, detect_smc
from tests.features.test_smc import BUILDUP, SMALL, STEP, random_walk, zigzag
from tests.strategies.test_smc_setups import mirror, short_scenario
from utils.errors import ConfigError, DataError

SYMBOL, IV = "BTCUSDT", STEP


def context(frame: pl.DataFrame, rows: int, received_after_close_ms: int = 500) -> FeatureContext:
    f = frame.head(rows)
    return FeatureContext(SYMBOL, "15m", IV, f, int(f["open_time"][-1]) + IV + received_after_close_ms)


def test_smc_short_signal_comes_only_from_the_last_row_with_exact_values():
    s = StrategySMC()
    frame = short_scenario()
    assert s.detect(context(frame, 18)) == () and s.detect(context(frame, 20)) == ()  # до CHoCH и после (сетап отработан)
    (sig,) = s.detect(context(frame, 19))  # последняя строка = бар 18
    assert sig.direction is Direction.SHORT and sig.kind is Kind.LIMIT and sig.strategy == "SMC_Pro_Trend"
    assert sig.signal_id == f"SMC_Pro_Trend:{SYMBOL}:15m:{18 * IV}:short" and sig.bar_open_time == 18 * IV and sig.bar_close_ms == 19 * IV
    assert (sig.entry, sig.sl, sig.inval) == (105.0, pytest.approx(112.2), 112.0) and sig.tp == pytest.approx(83.4)
    assert sig.meta["pool_level"] == 110.0 and sig.meta["bars_since_sweep"] == 3
    labels = [g["label"] for g in sig.geometry]
    assert labels == ["FVG", "пул ликвидности", "свип", "CHoCH", "вход (50% FVG)", "SL", "TP 3R"]
    zone = sig.geometry[0]
    assert (zone["type"], zone["t1"], zone["p1"], zone["p2"]) == ("zone", 15 * IV, 107.0, 103.0)


def test_smc_long_signal_is_the_mirror():
    (sig,) = StrategySMC().detect(context(mirror(short_scenario()), 19))
    assert sig.direction is Direction.LONG and sig.entry == pytest.approx(95.0) and sig.sl == pytest.approx(200 - 112.2)
    assert sig.tp == pytest.approx(200 - 83.4) and sig.inval == pytest.approx(200 - 112.0)


def test_smc_params_flow_through_and_exposure_rejection_gives_no_signal():
    assert StrategySMC(SmcSetupParams(max_exposure=0.1)).detect(context(short_scenario(), 19)) == ()


def test_breakout_signal_market_long_with_geometry():
    wp = [*BUILDUP[:-1], (27, 98.0), (30, 100.0), (31, 101.5), (34, 102.0)]
    df = zigzag(wp).with_columns(volume=pl.Series([100.0 if i == 31 else 10.0 for i in range(35)]))
    frame = add_features(df, SMALL, interval_ms=IV)
    s = StrategyBreakout()
    assert s.detect(context(frame, 31)) == () and s.detect(context(frame, 33)) == ()
    (sig,) = s.detect(context(frame, 32))  # последняя строка = бар 31
    assert sig.direction is Direction.LONG and sig.kind is Kind.MARKET and sig.tp is None and sig.inval is None
    assert sig.sl < sig.entry and sig.meta["level"] == pytest.approx(100.06) and sig.meta["vol_ratio"] == pytest.approx(10.0)
    assert sum(g["label"].startswith("High") for g in sig.geometry) == 3 and sum(g["label"].startswith("Low") for g in sig.geometry) == 3


def test_feature_engine_builds_context_and_validates_history():
    eng = FeatureEngine(SmcParams(), min_bars=100)
    candles = random_walk(150, 3).select("open_time", "open", "high", "low", "close", "volume")
    c = eng.build(SYMBOL, "15m", IV, candles, received_ms=7)
    assert c.frame.height == 150 and "swing_high" in c.frame.columns and c.bar_open_time == 149 * IV and c.received_ms == 7
    with pytest.raises(DataError, match="нужно не меньше"):
        eng.build(SYMBOL, "15m", IV, candles.head(50), 0)
    with pytest.raises(ConfigError):
        FeatureEngine(SmcParams(), min_bars=5)
    with pytest.raises(DataError):
        eng.build(SYMBOL, "15m", IV, candles.filter(pl.col("open_time") != 60 * IV), 0)  # дыра в сетке - не склеиваем молча


@pytest.mark.parametrize("seed", [91, 92])
def test_live_window_signals_equal_backtest_signals_r8(seed):
    """Те же свечи двумя путями: (а) бэктест - признаки и детекторы по всей истории; (б) live - по свече за раз,
    FeatureEngine на окне 300 баров, StrategySMC/StrategyBreakout.detect. Сигналы совпадают (после прогрева)."""
    fp = SmcParams(k=3, atr_period=5, swing_atr_mult=0.3, impulse_atr_mult=0.5, eq_tolerance=0.03, volume_sma=5)
    sp = SmcSetupParams(max_exposure=1e9)
    n, window, warm = 1200, 300, 400
    df = random_walk(n, seed, 0.015).with_columns(volume=pl.Series([60.0 if i % 7 == 0 else 10.0 for i in range(n)]))
    full = detect_breakout(detect_smc(add_features(df, fp, interval_ms=IV), sp))

    def key(t, direction, entry, sl, tp):
        return (t, direction, round(entry, 6), round(sl, 6), None if tp is None else round(tp, 6))

    expected = set()
    for side in ("short", "long"):
        for r in full.with_row_index("i").filter(pl.col(f"{side}_signal") & (pl.col("i") >= warm)).iter_rows(named=True):
            expected.add(key(r["open_time"], side, r[f"{side}_entry"], r[f"{side}_sl"], r[f"{side}_tp"]))
    for r in full.with_row_index("i").filter(pl.col("breakout_signal") & (pl.col("i") >= warm)).iter_rows(named=True):
        expected.add(key(r["open_time"], "long", r["close"], r["breakout_sl"], None))

    engine, smc, brk = FeatureEngine(fp, min_bars=100), StrategySMC(sp), StrategyBreakout()
    got = set()
    for t in range(warm, n):
        ctx = engine.build(SYMBOL, "15m", IV, df.slice(t - window + 1, window), received_ms=0)
        for sig in (*smc.detect(ctx), *brk.detect(ctx)):
            got.add(key(sig.bar_open_time, sig.direction.value, sig.entry, sig.sl, sig.tp))
    assert len(expected) >= 4, f"в сравнении только {len(expected)} сигналов бэктеста: паритет почти не проверен"
    assert got == expected
