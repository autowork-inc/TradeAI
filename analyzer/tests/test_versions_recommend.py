import numpy as np
import pandas as pd
import pytest

from analyzer.frame import build_frame
from analyzer.recommend import dry_run, generate, load_manifest, strategy_params
from analyzer.versions import VersionDef, auto_versions, compare_versions, degradation, select

from .conftest import PLAN, T0, make_trades


def test_select_by_period_plan_hash_and_meta(frame):
    rows = make_trades(40, start=T0, meta_fn=lambda i, rng: {"require_buildup": bool(i % 2)}) + make_trades(40, start=T0 + pd.Timedelta(days=30), plan={**PLAN, "levels": [[1.0, 1.0]]}, prefix="b-")
    df = build_frame(rows)
    cut = T0 + pd.Timedelta(days=20)
    assert len(select(df, VersionDef("v1", "SMC_Pro_Trend", ts_to=cut))) == 40 and len(select(df, VersionDef("v2", "SMC_Pro_Trend", ts_from=cut))) == 40
    ph = df.loc[df["id"].str.startswith("b-"), "plan_hash"].iloc[0]
    assert len(select(df, VersionDef("p", "SMC_Pro_Trend", plan_hash=ph))) == 40
    assert len(select(df, VersionDef("m", "SMC_Pro_Trend", ts_to=cut, meta_equals={"require_buildup": True}))) == 20
    assert len(select(df, VersionDef("x", "Other"))) == 0
    assert {a["plan_hash"] for a in auto_versions(df)} == set(df["plan_hash"])


def test_version_json_roundtrip_normalizes_timestamps():
    v = VersionDef.from_json({"name": "SMC v1.1 strict", "strategy_id": "S", "ts_from": "2026-10-18T00:00:00", "meta_equals": {"require_buildup": True}})
    assert v.ts_from == pd.Timestamp("2026-10-18", tz="UTC") and VersionDef.from_json(v.to_json()) == v


def test_degradation_verdicts():
    rng = np.random.default_rng(1)
    base = rng.normal(0.3, 1.0, 300)
    assert degradation(base, rng.normal(-0.4, 1.0, 300), min_n=30)["verdict"] == "degraded"
    assert degradation(base, rng.normal(1.0, 1.0, 300), min_n=30)["verdict"] == "improved"
    assert degradation(base[:150], base[150:], min_n=30)["verdict"] == "inconclusive"
    assert degradation(base[:10], base, min_n=30)["verdict"] == "insufficient_data"


def test_compare_versions_overlap_cuts_both_to_common_window(cfg):
    a = make_trades(100, start=T0, step_h=6.0, win_p=0.5)                      # 25 суток
    b = make_trades(100, start=T0 + pd.Timedelta(days=10), step_h=6.0, win_p=0.2, plan={**PLAN, "levels": [[1.0, 1.0]]}, prefix="b-", seed=2)
    df = build_frame(a + b)
    ha = df.loc[~df["id"].str.startswith("b-"), "plan_hash"].iloc[0]
    hb = df.loc[df["id"].str.startswith("b-"), "plan_hash"].iloc[0]
    va = VersionDef("A", "SMC_Pro_Trend", ts_to=T0 + pd.Timedelta(days=10), plan_hash=ha)
    vb = VersionDef("B", "SMC_Pro_Trend", ts_from=T0 + pd.Timedelta(days=10), plan_hash=hb)
    rel = compare_versions(df, va, vb, cfg, align="relative")
    assert rel["align"] == "relative" and rel["baseline"]["metrics"]["n"] == 40 and rel["candidate"]["metrics"]["n"] == 100 and rel["note"] is None
    # пересекающиеся версии по плану выхода: одно и то же окно
    pa, pb = VersionDef("PA", "SMC_Pro_Trend", plan_hash=ha), VersionDef("PB", "SMC_Pro_Trend", plan_hash=hb)
    ov = compare_versions(df, pa, pb, cfg, align="overlap")
    assert ov["align"] == "overlap" and ov["window"]["from"] is not None
    assert ov["baseline"]["curve"]["cum_r"] and ov["candidate"]["curve"]["cum_r"]
    # версии не пересекаются -> явная пометка вместо молчаливой подмены
    seq = compare_versions(df, va, vb, cfg, align="overlap")
    assert seq["align"] == "relative" and "не пересекаются" in seq["note"]
    assert ov["verdict"]["verdict"] in {"degraded", "suspected", "inconclusive", "improved", "insufficient_data"}


def interleaved(good_p, bad_p, n=80, **kw):
    rows = []
    for coin, p, seed in (("GOODUSDT", good_p, 1), ("BADUSDT", bad_p, 2)):
        rows += make_trades(n, coin=coin, win_p=p, seed=seed, **kw)
    return build_frame(rows)


def test_recommendations_drop_a_significantly_losing_coin_only(cfg):
    df = interleaved(0.6, 0.05)
    manifest = load_manifest(cfg.manifest_path)
    recs = generate(df, cfg, manifest)
    coin_recs = [r for r in recs if r["kind"] == "exclude_coin"]
    assert [r["scope"]["coin"] for r in coin_recs] == ["BADUSDT"]
    r = coin_recs[0]
    assert r["effect"]["after"] > r["effect"]["before"] and r["effect"]["n_after"] == 80 and r["proposed"] == ["BADUSDT"] and r["confidence"] in {"low", "medium", "high"}
    assert len(r["id"]) == 10 and r["id"] == generate(df, cfg, manifest)[recs.index(r)]["id"]  # id стабилен между запусками


def test_no_recommendations_for_noise_or_small_samples(cfg):
    manifest = load_manifest(cfg.manifest_path)
    assert [r for r in generate(interleaved(0.45, 0.45), cfg, manifest) if r["kind"] == "exclude_coin"] == []
    assert generate(build_frame(make_trades(15)), cfg, manifest) == []  # меньше 2*min_group_trades


def test_stop_tightening_recommendation_needs_stable_gain(cfg):
    # половина сделок выбита стопом (MAE 1.0), остальные - прибыльные с малой просадкой: подтяжка стопа явно выгодна
    rows = []
    for i in range(80):
        loser = i % 2 == 0
        rows.append(make_trades(1, seed=i)[0] | {"id": f"x{i}", "signal_time": T0 + pd.Timedelta(hours=6 * i), "entry_time": T0 + pd.Timedelta(hours=6 * i), "exit_time": T0 + pd.Timedelta(hours=6 * i + 2),
                                                 "r_multiple": -1.05 if loser else 1.95, "gross_r": -1.0 if loser else 2.0, "mfe_r": 0.2 if loser else 2.4, "mae_r": 1.0 if loser else 0.1})
    recs = generate(build_frame(rows), cfg, load_manifest(cfg.manifest_path))
    stop = [r for r in recs if r["kind"] == "tighten_stop"]
    assert len(stop) == 1 and stop[0]["proposed"] == 0.5 and stop[0]["current"] == 1.0 and stop[0]["effect"]["delta"] == pytest.approx(0.25, abs=0.01)


def test_dry_run_builds_unified_diff_and_writes_nothing(cfg, tmp_path):
    manifest = load_manifest(cfg.manifest_path)
    recs = [r for r in generate(interleaved(0.6, 0.05), cfg, manifest) if r["kind"] == "exclude_coin"]
    before = cfg.manifest_path.read_text(encoding="utf-8")
    out = dry_run(recs, manifest)
    assert cfg.manifest_path.read_text(encoding="utf-8") == before  # файл манифеста не тронут
    assert out["files"][0]["scope"] == "SMC_Pro_Trend"
    assert '-    "exclude": []' in out["diff"] and '+    "exclude": [' in out["diff"] and "BADUSDT" in out["diff"] and "Dry run" in out["note"]
    assert dry_run([], manifest)["diff"] == ""


def test_dry_run_unions_list_params_and_handles_global(cfg):
    manifest = load_manifest(cfg.manifest_path)
    base = {"strategy_id": "S", "kind": "exclude_coin", "param": "symbols.exclude", "current": [], "title": "t"}
    out = dry_run([{**base, "id": "a" * 10, "proposed": ["A"]}, {**base, "id": "b" * 10, "proposed": ["B"]},
                   {"strategy_id": None, "kind": "news_blackout", "param": "news.blackout_minutes", "current": 15, "proposed": 30, "id": "c" * 10, "title": "n"}], manifest)
    text = out["diff"]
    assert '"A"' in text and '"B"' in text and "-    \"blackout_minutes\": 15" in text.replace("  ", " ").replace(" ", " ") or "blackout_minutes" in text
    assert {f["scope"] for f in out["files"]} == {"S", "global"}


def test_manifest_defaults_merge_per_strategy(cfg):
    m = load_manifest(cfg.manifest_path)
    p = strategy_params(m, "OBI_Breakout")
    assert p["entry"]["require_buildup"] is True and p["risk"]["stop_fraction_of_r"] == 1.0 and p["symbols"]["exclude"] == []
    assert strategy_params(m, "Unknown")["filters"] == {}
