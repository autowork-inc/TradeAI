import json
import math

import numpy as np
import pandas as pd
import pytest

from analyzer.metrics import (bootstrap_diff, bootstrap_mean_ci, clean, compute_metrics, daily_returns, downsample_idx, equity_curve, max_drawdown, permutation_p,
                              profit_factor)

T0 = pd.Timestamp("2026-09-01 12:00", tz="UTC")


def df_of(r, pnl=None, step_h=24.0):
    n = len(r)
    d = pd.DataFrame({"r": r, "exit_time": [T0 + pd.Timedelta(hours=step_h * i) for i in range(n)]})
    if pnl is not None:
        d["pnl"] = pnl
    return d


def test_profit_factor_and_expectancy_exact():
    r = [2.0, -1.0, 2.0, -1.0, -1.0, 1.0]  # прибыль 5, убыток 3
    m = compute_metrics(df_of(r), with_ci=False)
    assert m["n"] == 6 and m["wins"] == 3 and m["losses"] == 3
    assert m["profit_factor"] == pytest.approx(5 / 3) and m["profit_factor_infinite"] is False
    assert m["expectancy_r"] == pytest.approx(2 / 6) and m["net_r"] == pytest.approx(2.0)
    assert m["winrate"] == pytest.approx(0.5)
    assert m["avg_win_r"] == pytest.approx(5 / 3) and m["avg_loss_r"] == pytest.approx(-1.0) and m["payoff"] == pytest.approx(5 / 3)


def test_profit_factor_without_losses_is_undefined_not_infinite_number():
    pf, inf = profit_factor(np.array([1.0, 2.0]))
    assert pf is None and inf is True
    assert profit_factor(np.array([0.0, 0.0])) == (None, False)


def test_sharpe_sortino_per_trade():
    r = np.array([2.0, -1.0, 2.0, -1.0, -1.0, 1.0])
    m = compute_metrics(df_of(r), with_ci=False)
    assert m["sharpe_trade"] == pytest.approx(r.mean() / r.std(ddof=1))
    assert m["sortino_trade"] == pytest.approx(r.mean() / math.sqrt(np.mean(np.minimum(r, 0) ** 2)))


def test_daily_sharpe_fills_empty_days_with_zero_and_annualizes():
    d = pd.DataFrame({"r": [1.0, 1.0], "exit_time": [T0, T0 + pd.Timedelta(days=2)]})  # дневной ряд [1, 0, 1]
    assert daily_returns(d["r"].to_numpy(), d["exit_time"]).tolist() == [1.0, 0.0, 1.0]
    m = compute_metrics(d, with_ci=False)
    x = np.array([1.0, 0.0, 1.0])
    assert m["sharpe_daily"] == pytest.approx(x.mean() / x.std(ddof=1) * math.sqrt(365)) and m["days"] == 3


def test_max_drawdown_r_usd_and_pct():
    r = [1.0, 1.0, -1.0, -2.0, 1.0]  # кривая 0,1,2,1,-1,0: пик 2, дно -1 -> 3R
    pnl = [100.0, 100.0, -100.0, -200.0, 100.0]
    m = compute_metrics(df_of(r, pnl), initial_equity=10_000, with_ci=False)
    assert m["max_drawdown_r"] == pytest.approx(3.0)
    assert m["max_drawdown_usd"] == pytest.approx(300.0)
    assert m["max_drawdown_pct"] == pytest.approx(300 / 10_200 * 100)
    assert m["expectancy_usd"] == pytest.approx(0.0) and m["net_usd"] == pytest.approx(0.0)


def test_drawdown_counts_start_as_peak():
    assert max_drawdown(np.array([0.0, -1.0, -2.0])) == pytest.approx(2.0)
    assert max_drawdown(np.array([])) == 0.0


def test_missing_pnl_gives_none_usd_metrics_but_keeps_r():
    m = compute_metrics(df_of([1.0, -1.0, 1.0]), with_ci=False)
    assert m["expectancy_usd"] is None and m["max_drawdown_usd"] is None and m["max_drawdown_r"] == pytest.approx(1.0)


def test_empty_and_small_samples_are_flagged_not_concluded():
    m = compute_metrics(df_of([]), min_conclusions=200)
    assert m["n"] == 0 and m["expectancy_r"] is None and m["significance"]["enough"] is False
    m = compute_metrics(df_of([1.0, -1.0]), min_conclusions=200, with_ci=False)
    assert m["significance"]["enough"] is False and "выводов делать нельзя" in m["significance"]["note"]
    assert compute_metrics(df_of([0.5] * 5), min_conclusions=5, with_ci=False)["significance"]["note"] is None


def test_bootstrap_is_deterministic_and_covers_the_mean():
    x = np.random.default_rng(3).normal(0.2, 1.0, 400)
    a, b = bootstrap_mean_ci(x, 500), bootstrap_mean_ci(x, 500)
    assert a == b and a[0] < x.mean() < a[1]
    assert bootstrap_mean_ci(x[:5], 500) is None  # слишком мало для интервала


def test_bootstrap_diff_detects_real_gap_and_not_noise():
    rng = np.random.default_rng(4)
    a, b = rng.normal(0.0, 1.0, 300), rng.normal(0.5, 1.0, 300)
    d = bootstrap_diff(a, b, 500)
    assert d["ci"][0] > 0 and d["p_not_better"] < 0.05
    pool = rng.normal(0.0, 1.0, 600)  # две половины одной выборки: настоящей разницы нет
    same = bootstrap_diff(pool[:300], pool[300:], 500)
    assert same["ci"][0] < 0 < same["ci"][1]


def test_permutation_p_small_for_clear_difference():
    rng = np.random.default_rng(5)
    assert permutation_p(rng.normal(0, 1, 80), rng.normal(1.5, 1, 80)) < 0.01
    assert permutation_p(rng.normal(0, 1, 3), rng.normal(0, 1, 3)) is None


def test_equity_curve_columns_and_downsampling():
    d = df_of(np.ones(1000))
    c = equity_curve(d, max_points=100)
    assert len(c["t"]) == len(c["i"]) == len(c["cum_r"]) <= 100 and c["cum_r"][-1] == pytest.approx(1000.0) and c["i"][-1] == 1000
    assert equity_curve(d.iloc[0:0])["t"] == []
    assert downsample_idx(5, 100).tolist() == [0, 1, 2, 3, 4]


def test_clean_makes_json_safe():
    out = clean({"a": np.float64("nan"), "b": np.int64(3), "c": [np.inf, 1.5], "d": pd.Timestamp("2026-01-01", tz="UTC"), "e": pd.NaT, "f": np.bool_(True), "g": (1, 2)})
    assert out == {"a": None, "b": 3, "c": [None, 1.5], "d": "2026-01-01T00:00:00+00:00", "e": None, "f": True, "g": [1, 2]}
    json.dumps(out, allow_nan=False)
