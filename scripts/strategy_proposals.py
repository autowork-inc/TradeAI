"""Предложения по стратегиям: одобрить (применить), отклонить (остаётся шаблоном), откатить к снимку по дате.

Хранилище (в git, в образ не едет): strategy_lab/
  proposals/<id>.json            предложение: стратегия, суть, доводы, примеры сделок, изменение, статус
                                 статусы: pending (ждёт решения) | template (отклонено - хранится как запасной шаблон, можно применить позже)
                                          | applied | rolled_back | manual (меняется не параметром - кодом/настройкой сервера, применяется руками)
  history/<strategy>/<UTC>__<why>.json   снимок настройки стратегии ДО каждого применения/отката: запись overrides и полные действующие параметры
Действующие изменения - orchestrator/strategy_overrides.json (бот читает при старте: orchestrator/overrides.py).

  python -X utf8 -m scripts.strategy_proposals list [--status pending]
  python -X utf8 -m scripts.strategy_proposals show <id>
  python -X utf8 -m scripts.strategy_proposals apply <id>        # снимок «до» -> изменение в strategy_overrides.json
  python -X utf8 -m scripts.strategy_proposals reject <id> [--note ...]   # -> template
  python -X utf8 -m scripts.strategy_proposals history <strategy>
  python -X utf8 -m scripts.strategy_proposals rollback <strategy> [--to <снимок>]   # по умолчанию - последний снимок
После apply/rollback изменение действует на сервере только после выкатки (commit + scripts.auto_deploy).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
LAB = ROOT / "strategy_lab"
PROPOSALS = LAB / "proposals"
HISTORY = LAB / "history"
STATUSES = ("pending", "template", "applied", "rolled_back", "manual")
APPLICABLE_KEYS = ("params", "tp_r")


class ProposalError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S_%f")


def git_head() -> str | None:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() or None


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def write_json(p: Path, data: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)


def load_proposal(pid: str) -> tuple[Path, dict[str, Any]]:
    p = PROPOSALS / f"{pid}.json"
    if not p.is_file():
        raise ProposalError(f"нет предложения {pid} ({p})")
    data = read_json(p)
    if data.get("status") not in STATUSES:
        raise ProposalError(f"{pid}: неизвестный статус {data.get('status')!r}")
    return p, data


# ------------------------------------------------------------------------------------------- overrides

def overrides_path() -> Path:
    from orchestrator import overrides

    return overrides.path()


def read_overrides() -> dict[str, Any]:
    p = overrides_path()
    return read_json(p) if p.is_file() else {"version": 1, "strategies": {}}


def effective_params(strategy: str) -> dict[str, Any] | None:
    """Полные действующие параметры стратегии (код по умолчанию + текущие overrides) - для снимка «как было»."""
    from orchestrator.registry import build_strategies

    built = build_strategies([strategy], mtf=True, interval="15m")
    params = getattr(built[0], "params", None)
    return asdict(params) if params is not None else None


def snapshot(strategy: str, why: str) -> Path:
    entry = read_overrides()["strategies"].get(strategy)
    snap = {"strategy": strategy, "saved_at": now_iso(), "why": why, "git_commit": git_head(),
            "override_entry": entry, "effective_params": effective_params(strategy)}
    p = HISTORY / strategy / f"{stamp()}__{why}.json"
    write_json(p, snap)
    return p


def validate_entry(strategy: str, entry: dict[str, Any] | None) -> None:
    """Проверка ДО записи - те же функции, что у бота на старте (orchestrator/overrides.py): ConfigError - отказ, файл не трогается."""
    if entry is None:
        return
    import tempfile

    from orchestrator import overrides
    from orchestrator.registry import build_strategies

    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d) / "o.json"
        write_json(tmp, {"version": overrides.VERSION, "strategies": {strategy: entry}})
        checked = overrides.load(tmp)[strategy]
    built = build_strategies([strategy], mtf=True, interval="15m")  # уже с текущими overrides; новая запись содержит их + изменение
    overrides.apply_params(built[0].params, checked["params"], strategy)


def write_entry(strategy: str, entry: dict[str, Any] | None) -> None:
    data = read_overrides()
    if entry is None:
        data["strategies"].pop(strategy, None)
    else:
        data["strategies"][strategy] = entry
    write_json(overrides_path(), data)


# ------------------------------------------------------------------------------------------- команды

def cmd_list(status: str | None) -> None:
    rows = sorted((read_json(p) for p in PROPOSALS.glob("*.json")), key=lambda d: (d.get("created_at", ""), d["id"]))
    for d in rows:
        if status and d["status"] != status:
            continue
        print(f"{d['id']:40s} {d['status']:11s} {d['strategy']:22s} {d['title']}")


def cmd_show(pid: str) -> None:
    print(json.dumps(load_proposal(pid)[1], ensure_ascii=False, indent=2))


def cmd_apply(pid: str) -> None:
    path, prop = load_proposal(pid)
    change = prop.get("change") or {}
    if prop["status"] == "manual" or not any(k in change for k in APPLICABLE_KEYS):
        raise ProposalError(f"{pid}: это не изменение параметров ({prop['status']}); как применить - в поле how_to_apply")
    if prop["status"] == "applied":
        raise ProposalError(f"{pid}: уже применено")
    strategy = prop["strategy"]
    current = read_overrides()["strategies"].get(strategy) or {"params": {}, "tp_r": None}
    entry = {"params": {**current.get("params", {}), **change.get("params", {})},
             "tp_r": change["tp_r"] if "tp_r" in change else current.get("tp_r"),
             "proposal_id": pid, "applied_at": now_iso()}
    validate_entry(strategy, entry)
    snap = snapshot(strategy, f"before_{pid}")
    write_entry(strategy, entry)
    prop.update(status="applied", applied_at=entry["applied_at"], snapshot_before=str(snap.relative_to(ROOT)))
    write_json(path, prop)
    print(f"Применено {pid} -> {strategy}: {entry}\nСнимок «до» (для отката): {snap.relative_to(ROOT)}\nНа сервере - после выкатки (commit + auto_deploy).")


def cmd_reject(pid: str, note: str | None) -> None:
    path, prop = load_proposal(pid)
    if prop["status"] == "applied":
        raise ProposalError(f"{pid}: уже применено - сначала rollback {prop['strategy']}")
    prop.update(status="template", rejected_at=now_iso(), reject_note=note)
    write_json(path, prop)
    print(f"{pid}: отклонено, сохранено как шаблон (можно применить позже: apply {pid})")


def snapshots(strategy: str) -> list[Path]:
    return sorted((HISTORY / strategy).glob("*.json"))


def cmd_history(strategy: str) -> None:
    for p in snapshots(strategy):
        d = read_json(p)
        print(f"{p.name}  git={d.get('git_commit')}  overrides={d.get('override_entry')}")


def cmd_rollback(strategy: str, to: str | None) -> None:
    snaps = snapshots(strategy)
    if not snaps:
        raise ProposalError(f"у {strategy} нет снимков - откатывать не к чему")
    target = next((p for p in snaps if p.name == to or p.stem == to), None) if to else snaps[-1]
    if target is None:
        raise ProposalError(f"снимок {to} не найден; есть: {[p.name for p in snaps]}")
    entry = read_json(target).get("override_entry")
    validate_entry(strategy, entry)
    cur = snapshot(strategy, f"before_rollback_to_{target.stem.split('__')[0]}")
    write_entry(strategy, entry)
    for p in PROPOSALS.glob("*.json"):  # предложения, применённые начиная с этого снимка, больше не действуют
        d = read_json(p)
        before = Path(d.get("snapshot_before") or "").name
        if d["strategy"] == strategy and d["status"] == "applied" and before >= target.name:
            d.update(status="rolled_back", rolled_back_at=now_iso())
            write_json(p, d)
    print(f"{strategy}: откат к {target.name} (overrides={entry}); текущее состояние сохранено в {cur.relative_to(ROOT)}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Предложения по стратегиям: apply / reject (шаблон) / rollback по снимкам")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list")
    p.add_argument("--status", choices=STATUSES)
    sub.add_parser("show").add_argument("id")
    sub.add_parser("apply").add_argument("id")
    p = sub.add_parser("reject")
    p.add_argument("id")
    p.add_argument("--note")
    sub.add_parser("history").add_argument("strategy")
    p = sub.add_parser("rollback")
    p.add_argument("strategy")
    p.add_argument("--to")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "list":
            cmd_list(a.status)
        elif a.cmd == "show":
            cmd_show(a.id)
        elif a.cmd == "apply":
            cmd_apply(a.id)
        elif a.cmd == "reject":
            cmd_reject(a.id, a.note)
        elif a.cmd == "history":
            cmd_history(a.strategy)
        else:
            cmd_rollback(a.strategy, a.to)
    except ProposalError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
