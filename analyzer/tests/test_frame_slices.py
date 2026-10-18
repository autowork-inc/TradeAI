import numpy as np
import pandas as pd
import pytest

from analyzer.errors import DataError
from analyzer.frame import build_frame, candle_features, plan_hash
from analyzer.slices import feature_columns, feature_slices, stop_tightening, target_reach

from .conftest import PLAN, T0, make_trades


def row(i, *, r, gross, mfe, mae, plan=None, meta=None, coin="BTCUSDT", sid="S"):
    sig = T0 + pd.Timedelta(hours=i)
    return {"id": f"t{i}", "strategy_id": sid, "coin": coin, "side": "long", "kind": "limit", "mode": "shadow", "signal_time": sig, "entry_time": sig, "exit_time": sig + pd.Timedelta(hours=1),
            "pnl": r * 100, "r_multiple": r, "gross_r": gross, "mfe_r": mfe, "mae_r": mae, "plan": dict(PLAN) if plan is None else plan, "meta": meta or {}}


def candles(n, vol=10.0, last_vol=30.0, hi=102.0, lo=100.0, atr=1.0):
    cs = [{"open_time": i, "open": 100, "high": 101, "low": 100, "close": 100.5, "volume": vol, "atr": atr} for i in range(n - 1)]
    return cs + [{"open_time": n, "open": 100, "high": hi, "low": lo, "close": 101, "volume": last_vol, "atr": atr}]


def test_candle_features_volume_vs_previous_20_bars_and_range_in_atr():
    vr, ra = candle_features(candles(30))
    assert vr == pytest.approx(3.0) and ra == pytest.approx(2.0)
    assert candle_features(candles(20))[0] is None  # меньше 21 бара: объёмный признак не выдумывается
    assert candle_features(candles(30, atr=0))[1] is None
    assert candle_features(None) == (None, None) and candle_features([]) == (None, None)
    assert candle_features([{"volume": "x"}] * 25)[0] is None


def test_plan_hash_tracks_exit_configuration_only():
    a = plan_hash(PLAN)
    assert a == plan_hash({**PLAN, "fills": [1, 2], "exit_price_source": "exchange"})  # факты исполнения не меняют версию
    assert a != plan_hash({**PLAN, "levels": [[1.0, 0.5], [2.0, 0.5]]})
    assert plan_hash({}) == "noplan" and plan_hash(None) == "noplan"


def test_build_frame_normalizes_and_rejects_corrupt_rows():
    f = build_frame(make_trades(5))
    assert str(f["exit_time"].dtype).startswith("datetime64") and f["r"].dtype == float
    assert (f["fee_r"].round(6) == 0.05).all() and f["plan_hash"].nunique() == 1
    rows = make_trades(3)
    rows[0]["entry_time"] = None
    g = build_frame(rows)
    assert g.loc[g["id"] == rows[0]["id"], "entry_ts"].iloc[0] == rows[0]["signal_time"]  # нет входа -> время сигнала
    bad = make_trades(3)
    bad[1]["r_multiple"] = None
    with pytest.raises(DataError, match="r_multiple"):
        build_frame(bad)
    with pytest.raises(DataError, match="нет колонок"):
        build_frame([{"id": "x"}])
    assert len(build_frame([])) == 0


def test_stop_tightening_matches_hand_calculation():
    f = build_frame([row(0, r=-1.05, gross=-1.0, mfe=0.3, mae=1.0), row(1, r=1.95, gross=2.0, mfe=2.4, mae=0.9),
                     row(2, r=1.95, gross=2.0, mfe=2.4, mae=0.3), row(3, r=-0.05, gross=0.0, mfe=1.1, mae=0.1)])
    cfg = type("C", (), {"initial_equity": 1e4, "min_conclusions": 50, "bootstrap_n": 100, "min_group_trades": 2})()
    t = stop_tightening(f, cfg, grid=(0.8,))
    assert t["baseline"]["expectancy_r"] == pytest.approx(0.7)
    r = t["rows"][0]
    assert r["n_changed"] == 2 and r["winners_cut"] == 1
    assert r["expectancy_r"] == pytest.approx((-0.85 - 0.85 + 1.95 - 0.05) / 4)
    assert r["delta_expectancy_r"] == pytest.approx(0.05 - 0.7)  # тут подтяжка ВРЕДИТ: оценка не обязана быть положительной
    assert t["eligible"] == 4


def test_stop_tightening_ignores_trades_without_breakeven_plan_or_mae():
    cfg = type("C", (), {"initial_equity": 1e4, "min_conclusions": 50, "bootstrap_n": 100, "min_group_trades": 2})()
    f = build_frame([row(0, r=-1.05, gross=-1.0, mfe=0.3, mae=1.0, plan={}), row(1, r=-1.05, gross=-1.0, mfe=0.3, mae=None)])
    t = stop_tightening(f, cfg, grid=(0.5,))
    assert t["eligible"] == 0 and t["rows"][0]["n_changed"] == 0 and t["rows"][0]["delta_expectancy_r"] == pytest.approx(0.0)


def test_target_reach_uses_mfe_and_is_a_lower_bound():
    cfg = type("C", (), {"initial_equity": 1e4, "min_conclusions": 50, "bootstrap_n": 100, "min_group_trades": 2})()
    f = build_frame([row(0, r=-1.05, gross=-1.0, mfe=2.2, mae=1.0), row(1, r=1.95, gross=2.0, mfe=3.5, mae=0.1), row(2, r=-0.05, gross=0.0, mfe=1.1, mae=0.1)])
    t = target_reach(f, cfg, grid=(2.0,))
    r = t["rows"][0]
    assert r["reach_rate"] == pytest.approx(2 / 3) and r["n_changed"] == 2
    assert r["expectancy_r"] == pytest.approx((1.95 + 1.95 - 0.05) / 3)  # стоп-сделка, дошедшая до 2R, считается выигрышем; цель обрезает победителя
    assert t["bias"] == "pessimistic"


def test_feature_columns_use_known_before_outcome_fields_only():
    metas = [{"require_buildup": bool(i % 2), "quality": float(i % 5), "mode": ["breakout", "sweep"][i % 2], "entry": 100.0, "sweep_high": 5.0, "interval": "5m"} for i in range(60)]
    f = build_frame([row(i, r=0.1, gross=0.15, mfe=1, mae=0.5, meta=metas[i]) for i in range(60)])
    cols = feature_columns(f)
    assert "meta.require_buildup" in cols and "meta.quality" in cols and "meta.mode" in cols and "hour_utc" in cols
    assert not any(k in cols for k in ("meta.entry", "meta.interval", "meta.sweep_high"))  # цены/служебное отсеиваются
    assert "mfe_r" not in cols and "exit_reason" not in cols  # утечка исхода запрещена


def test_feature_slices_group_by_values_and_quantile_bins():
    cfg = type("C", (), {"initial_equity": 1e4, "min_conclusions": 50, "bootstrap_n": 100, "min_group_trades": 10})()
    rng = np.random.default_rng(1)
    rows = [row(i, r=(1.0 if (i % 2) else -1.0), gross=0, mfe=1, mae=0.5, meta={"require_buildup": bool(i % 2), "quality": float(rng.uniform(0, 1))}) for i in range(80)]
    out = {s["feature"]: s for s in feature_slices(build_frame(rows), cfg)}
    b = {x["label"]: x for x in out["meta.require_buildup"]["bins"]}
    assert b["true"]["expectancy_r"] == pytest.approx(1.0) and b["false"]["expectancy_r"] == pytest.approx(-1.0)
    assert out["meta.quality"]["kind"] == "numeric" and len(out["meta.quality"]["bins"]) >= 2
