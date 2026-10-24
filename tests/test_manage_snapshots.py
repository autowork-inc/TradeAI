"""scripts/manage_snapshots.py: общий список снимков (параметры стратегий + коммиты) и откат по номеру."""

import json
import subprocess

import pytest

from scripts import manage_snapshots as ms
from scripts import strategy_proposals as sp


def sh(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8", check=True).stdout.strip()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    r = tmp_path / "repo"
    r.mkdir()
    sh(r, "init", "-q", "-b", "main")
    sh(r, "config", "user.email", "t@t")
    sh(r, "config", "user.name", "t")
    for i, text in enumerate(("v1", "v2", "v3"), 1):
        (r / "code.py").write_text(text, encoding="utf-8")
        sh(r, "add", "code.py")
        sh(r, "commit", "-q", "-m", f"commit {i}")
    monkeypatch.setattr(ms, "ROOT", r)
    monkeypatch.setattr(ms, "HISTORY", r / "strategy_lab" / "history")
    return r


def test_code_rollback_by_number_is_one_new_commit_and_history_is_kept(repo, capsys):
    snaps = ms.all_snapshots()
    assert [s.kind for s in snaps] == ["C", "C", "C"] and "commit 1" in snaps[2].title
    assert ms.main(["--pick", "3", "--yes"]) == 0
    assert (repo / "code.py").read_text(encoding="utf-8") == "v1"
    log = sh(repo, "log", "--pretty=%s").splitlines()
    assert len(log) == 4 and log[0].startswith("откат кода к") and log[1] == "commit 3"  # история не переписана
    assert "Как пользоваться" in capsys.readouterr().out


def test_code_rollback_refuses_dirty_tree_and_current_version(repo):
    (repo / "code.py").write_text("dirty", encoding="utf-8")
    assert ms.main(["--pick", "2", "--yes"]) == 3
    sh(repo, "checkout", "--", "code.py")
    assert ms.main(["--pick", "1", "--yes"]) == 3  # это HEAD


def test_param_snapshot_rollback_restores_strategy_parameters(repo, monkeypatch, tmp_path):
    ovr = tmp_path / "overrides.json"
    monkeypatch.setenv("STRATEGY_OVERRIDES_PATH", str(ovr))
    monkeypatch.setattr(sp, "ROOT", repo)
    monkeypatch.setattr(sp, "PROPOSALS", repo / "strategy_lab" / "proposals")
    monkeypatch.setattr(sp, "HISTORY", repo / "strategy_lab" / "history")
    monkeypatch.setattr(sp, "git_head", lambda: "abc")
    sp.write_json(sp.PROPOSALS / "p1.json", {"id": "p1", "strategy": "ICT_Killzones", "kind": "params", "title": "t", "status": "pending",
                                             "change": {"params": {"require_htf_trend": False}}})
    assert sp.main(["apply", "p1"]) == 0
    assert json.loads(ovr.read_text(encoding="utf-8"))["strategies"]["ICT_Killzones"]["params"] == {"require_htf_trend": False}
    snaps = ms.all_snapshots()
    pick = next(i for i, s in enumerate(snaps, 1) if s.kind == "P")
    assert "ICT_Killzones" in snaps[pick - 1].title and "как в коде" in snaps[pick - 1].title
    assert ms.main(["--pick", str(pick), "--yes"]) == 0
    assert json.loads(ovr.read_text(encoding="utf-8"))["strategies"] == {}


def test_bad_pick_is_an_error(repo):
    assert ms.main(["--pick", "99", "--yes"]) == 3
