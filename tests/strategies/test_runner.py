"""Тесты сборки прогона (strategies/runner.py): сегменты, сквозной прогон, итоговая статистика."""

import polars as pl
import pytest

from features.smc import SmcParams
from strategies.runner import RunConfig, run_symbol, summarize
from strategies.simulate import SimParams
from strategies.smc_setups import SmcSetupParams
from tests.features.test_smc import STEP, random_walk


def trades(rows: list[dict]) -> pl.DataFrame:
    base = {"outcome": "tp", "net_r": 0.0, "gross_r": 0.0, "idx": 0, "exit_off": 1, "fill_off": 1, "risk": 1.0, "entry_px": 100.0,
            "skipped_due_to_lock": False}
    return pl.DataFrame([{**base, **r, "idx": i} for i, r in enumerate(rows)])


def test_summarize_counts_expectancy_profit_factor_and_drawdown():
    t = trades([
        {"outcome": "tp", "net_r": 2.9, "gross_r": 3.0}, {"outcome": "sl", "net_r": -1.1, "gross_r": -1.0},
        {"outcome": "sl", "net_r": -1.1, "gross_r": -1.0}, {"outcome": "be", "net_r": -0.1, "gross_r": 0.0},
        {"outcome": "unfilled", "net_r": None, "gross_r": None}, {"outcome": "tp", "net_r": 9.0, "skipped_due_to_lock": True},
        {"outcome": "rejected_exposure", "net_r": None, "gross_r": None},
    ])
    s = summarize(t)
    assert (s["signals"], s["locked"], s["unfilled"], s["rejected"], s["trades"]) == (7, 1, 1, 1, 4)
    assert (s["tp"], s["sl"], s["be"]) == (1, 2, 1)
    assert s["sum_net_r"] == pytest.approx(0.6) and s["expectancy_net_r"] == pytest.approx(0.15)
    assert s["profit_factor"] == pytest.approx(2.9 / 2.3) and s["win_rate_net"] == pytest.approx(0.25)
    assert s["max_dd_r"] == pytest.approx(2.3)  # кривая: 2.9, 1.8, 0.7, 0.6 -> просадка от пика 2.9 до 0.6


def test_summarize_with_no_trades():
    s = summarize(trades([{"outcome": "unfilled", "net_r": None, "gross_r": None}]))
    assert s["trades"] == 0 and "sum_net_r" not in s


def test_run_symbol_end_to_end_is_deterministic_and_lock_never_overlaps():
    df = random_walk(4000, 77, 0.02)
    fp = SmcParams(k=3, atr_period=5, swing_atr_mult=0.3, impulse_atr_mult=0.7, eq_tolerance=0.03, volume_sma=5)
    cfg = RunConfig(smc_features=fp, smc=SmcSetupParams(max_exposure=1e9), sim=SimParams(max_exposure=1e9, max_hold=60))
    det1, t1 = run_symbol("X", [df], STEP, cfg)
    det2, t2 = run_symbol("X", [df], STEP, cfg)
    assert t1.equals(t2) and det1[0].equals(det2[0])
    assert t1.height > 3 and t1["skipped_due_to_lock"].sum() >= 0
    kept = t1.filter(~pl.col("skipped_due_to_lock")).sort("idx")
    ends, starts = kept["end_idx"].to_list(), kept["idx"].to_list()
    assert all(s > e for e, s in zip(ends, starts[1:]))  # владелец монеты: сделки не пересекаются
    assert set(t1["strategy"].unique()) <= {"smc_short", "smc_long", "breakout"}
    done = kept.filter(pl.col("outcome").is_in(["tp", "sl", "be", "timeout"]))
    assert done.height > 0 and done["net_r"].is_not_null().all()
    assert (done.filter(pl.col("outcome") == "tp")["gross_r"] - cfg.sim.tp_r).abs().max() < 1e-9 if (done["outcome"] == "tp").any() else True
