"""Одобренные изменения стратегий (orchestrator/overrides.py) и реестр предложений (scripts/strategy_proposals.py): apply / reject / rollback."""

import json

import pytest

from orchestrator import overrides
from orchestrator.registry import build_strategies, plans_for
from scripts import strategy_proposals as sp
from utils.errors import ConfigError


def write(path, strategies):
    path.write_text(json.dumps({"version": 1, "strategies": strategies}), encoding="utf-8")


@pytest.fixture
def ovr(tmp_path, monkeypatch):
    path = tmp_path / "strategy_overrides.json"
    monkeypatch.setenv("STRATEGY_OVERRIDES_PATH", str(path))
    return path


def ict():
    return build_strategies(["ICT_Killzones"], mtf=True, interval="15m")[0]


def test_no_file_means_defaults(ovr):
    assert ict().params.require_htf_trend is True
    assert plans_for(["ICT_Killzones"])["ICT_Killzones"].levels == ((4.0, 1.0),)


def test_params_and_take_profit_are_applied(ovr):
    write(ovr, {"ICT_Killzones": {"params": {"require_htf_trend": False, "sl_buffer_atr": 0.5}, "tp_r": 2.5}})
    p = ict().params
    assert p.require_htf_trend is False and p.sl_buffer_atr == 0.5 and p.min_wick_frac == 0.55  # остальное - как в коде
    assert plans_for(["ICT_Killzones"])["ICT_Killzones"].levels == ((2.5, 1.0),)
    assert plans_for(["VWAP_Reversion"])["VWAP_Reversion"].levels == ((4.0, 1.0),)


@pytest.mark.parametrize("entry", [
    {"params": {"no_such_param": 1}},
    {"params": {"require_htf_trend": "yes"}},
    {"params": {"min_wick_frac": 2.0}},  # __post_init__ стратегии: доля 0..1
    {"params": {}, "tp_r": 0},
    {"params": {}, "tp_r": True},
])
def test_bad_override_stops_the_start(ovr, entry):
    write(ovr, {"ICT_Killzones": entry})
    with pytest.raises(ConfigError):
        ict()
        plans_for(["ICT_Killzones"])


def test_broken_file_is_config_error(ovr):
    ovr.write_text("{oops", encoding="utf-8")
    with pytest.raises(ConfigError):
        overrides.load()


# ------------------------------------------------------------------------------------------- реестр

@pytest.fixture
def lab(tmp_path, monkeypatch, ovr):
    monkeypatch.setattr(sp, "PROPOSALS", tmp_path / "lab" / "proposals")
    monkeypatch.setattr(sp, "HISTORY", tmp_path / "lab" / "history")
    monkeypatch.setattr(sp, "ROOT", tmp_path)
    monkeypatch.setattr(sp, "git_head", lambda: "abc1234")

    def add(pid, change, kind="params", status="pending"):
        sp.write_json(sp.PROPOSALS / f"{pid}.json", {"id": pid, "strategy": "ICT_Killzones", "kind": kind, "title": pid,
                                                     "change": change, "status": status, "created_at": "2026-10-23T00:00:00Z"})
    return add


def status(pid):
    return sp.load_proposal(pid)[1]["status"]


def test_apply_saves_snapshot_then_rollback_restores_previous_version(lab, ovr):
    lab("p1", {"params": {"require_htf_trend": False}})
    lab("p2", {"tp_r": 2.0})
    assert sp.main(["apply", "p1"]) == 0
    assert ict().params.require_htf_trend is False and status("p1") == "applied"
    assert sp.main(["apply", "p2"]) == 0  # второе изменение копится поверх первого
    assert ict().params.require_htf_trend is False and plans_for(["ICT_Killzones"])["ICT_Killzones"].levels == ((2.0, 1.0),)
    snaps = sp.snapshots("ICT_Killzones")
    assert len(snaps) == 2
    first = sp.read_json(snaps[0])
    assert first["override_entry"] is None and first["effective_params"]["require_htf_trend"] is True and first["git_commit"] == "abc1234"
    assert sp.main(["rollback", "ICT_Killzones"]) == 0  # к последнему снимку: до p2
    assert ict().params.require_htf_trend is False and plans_for(["ICT_Killzones"])["ICT_Killzones"].levels == ((4.0, 1.0),)
    assert status("p2") == "rolled_back" and status("p1") == "applied"
    assert sp.main(["rollback", "ICT_Killzones", "--to", snaps[0].name]) == 0  # к исходной версии кода
    assert ict().params.require_htf_trend is True and status("p1") == "rolled_back"
    assert len(sp.snapshots("ICT_Killzones")) == 4  # каждый откат тоже сохраняет «как было» - откат откатываем


def test_rejected_proposal_stays_as_template_and_can_be_applied_later(lab, ovr):
    lab("p1", {"params": {"require_htf_trend": False}})
    assert sp.main(["reject", "p1", "--note", "мало данных"]) == 0
    p = sp.load_proposal("p1")[1]
    assert p["status"] == "template" and p["reject_note"] == "мало данных" and not ovr.exists()
    assert sp.main(["apply", "p1"]) == 0 and status("p1") == "applied"


def test_invalid_change_is_refused_and_nothing_is_written(lab, ovr):
    lab("bad", {"params": {"min_wick_frac": 5.0}})
    with pytest.raises(ConfigError):
        sp.main(["apply", "bad"])
    assert not ovr.exists() and sp.snapshots("ICT_Killzones") == [] and status("bad") == "pending"


def test_manual_proposal_cannot_be_applied_by_the_tool(lab):
    lab("m", {"server_env": {"APPROVED_STRATEGIES": "SMC_Pro_Trend"}}, kind="manual", status="manual")
    assert sp.main(["apply", "m"]) == 3
    assert sp.main(["reject", "m"]) == 0 and status("m") == "template"


def test_real_proposals_in_the_repo_are_valid():
    """Предложения в strategy_lab/proposals применимы к текущему коду (параметры существуют, значения проходят проверку)."""
    for p in sp.PROPOSALS.glob("*.json"):
        d = sp.read_json(p)
        assert d["status"] in sp.STATUSES and d["id"] == p.stem
        params = (d.get("change") or {}).get("params")
        if params:
            s = build_strategies([d["strategy"]], mtf=True, interval="15m")[0]
            overrides.apply_params(s.params, params, d["strategy"])
