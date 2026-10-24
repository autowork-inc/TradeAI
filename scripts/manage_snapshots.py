"""Снимки и откаты: один список всего, к чему можно вернуться, и откат по номеру.

  python -X utf8 -m scripts.manage_snapshots            # список + ввод номера + подтверждение + (по желанию) выкатка на сервер
  python -X utf8 -m scripts.manage_snapshots --list     # только список
  python -X utf8 -m scripts.manage_snapshots --pick 3 --yes --deploy   # без вопросов

Два вида снимков:
  P  параметры стратегии (strategy_lab/history/<стратегия>/...): что стояло ДО применения предложения. Откат - вернуть ту запись в
     orchestrator/strategy_overrides.json (scripts.strategy_proposals rollback), код не меняется.
  C  версия кода (коммит в main): откат - ОДИН новый коммит «откат к <коммит>», который отменяет все коммиты после выбранного
     (git revert, история не переписывается; откат тоже можно откатить, выбрав в списке коммит до него).
После отката изменения действуют на сервере только после выкатки: скрипт предлагает сделать commit + push + scripts.auto_deploy.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "strategy_lab" / "history"
VPS_HOST = "193.233.75.254"
SSH_KEY = "~/.ssh/id_ed25519"
COMMITS = 25

USAGE = """
Как пользоваться:
  1. Посмотрите список: слева номер, тип (P - параметры стратегии, C - версия кода), дата UTC и описание.
  2. Введите номер снимка, к которому нужно вернуться, и подтвердите словом «да».
     P - стратегия получит те параметры, что стояли в этот момент (остальные стратегии не меняются).
     C - весь код вернётся к этой версии одним новым коммитом «откат» (историю можно вернуть, выбрав более поздний коммит в списке).
  3. На вопрос «выкатить на сервер» ответьте «да» - скрипт закоммитит, запушит и выкатит (scripts.auto_deploy, ~3 минуты).
Без вопросов: python -X utf8 -m scripts.manage_snapshots --pick <номер> --yes --deploy
""".rstrip()


class SnapshotError(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    kind: str  # P | C
    when: str  # UTC
    title: str
    ref: str  # P: путь к файлу снимка; C: полный хеш коммита
    strategy: str | None = None


def git(*args: str, check: bool = True) -> str:
    out = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", env={**os.environ, "TZ": "UTC"})
    if check and out.returncode != 0:
        raise SnapshotError(f"git {' '.join(args)}: {out.stderr.strip() or out.stdout.strip()}")
    return out.stdout.strip()


def param_snapshots() -> list[Snapshot]:
    out = []
    for p in sorted(HISTORY.glob("*/*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        entry = d.get("override_entry")
        params = "как в коде (без изменений)" if not entry else json.dumps({**entry.get("params", {}), **({"tp_r": entry["tp_r"]} if entry.get("tp_r") else {})},
                                                                            ensure_ascii=False)
        why = d.get("why", p.stem.split("__", 1)[-1])
        env = f"; на сервере {d['server_env']}" if d.get("server_env") else ""
        out.append(Snapshot("P", d.get("saved_at", "").replace("T", " ").rstrip("Z"), f"{d['strategy']}: {why} -> параметры {params}{env}", str(p),
                            d["strategy"]))
    return out


def code_snapshots(n: int = COMMITS) -> list[Snapshot]:
    lines = git("log", f"-{n}", "--date=format-local:%Y-%m-%d %H:%M:%S", "--pretty=%H\t%ad\t%s").splitlines()
    return [Snapshot("C", when, f"{h[:7]} {subj[:110]}", h) for h, when, subj in (ln.split("\t", 2) for ln in lines if ln)]


def all_snapshots() -> list[Snapshot]:
    return sorted(param_snapshots() + code_snapshots(), key=lambda s: s.when, reverse=True)


def print_list(snaps: list[Snapshot]) -> None:
    print(f"{'№':>3}  тип  {'дата (UTC)':19s}  описание")
    for i, s in enumerate(snaps, 1):
        print(f"{i:>3}  {s.kind:^3}  {s.when[:19]:19s}  {s.title}")
    print(USAGE)


def rollback_params(s: Snapshot) -> str:
    from scripts import strategy_proposals as sp

    data = json.loads(Path(s.ref).read_text(encoding="utf-8"))
    if data.get("server_env") and not data.get("override_entry"):
        print(f"Внимание: этот снимок также менял настройку сервера {data['server_env']}. Её возврат: {data.get('rollback_how', 'вручную в ~/trading/.env.shadow')}")
    if sp.main(["rollback", s.strategy or "", "--to", Path(s.ref).name]) != 0:
        raise SnapshotError("откат параметров не выполнен (см. сообщение выше)")
    return f"откат параметров {s.strategy} к снимку {Path(s.ref).name}"


def rollback_code(s: Snapshot) -> str:
    if git("status", "--porcelain"):
        raise SnapshotError("в рабочей папке есть незакоммиченные изменения: закоммитьте или уберите их перед откатом кода")
    head = git("rev-parse", "HEAD")
    if head == s.ref:
        raise SnapshotError("это и есть текущая версия - откатывать нечего")
    if subprocess.run(["git", "merge-base", "--is-ancestor", s.ref, "HEAD"], cwd=ROOT).returncode != 0:
        raise SnapshotError("выбранный коммит не в истории текущей ветки")
    res = subprocess.run(["git", "revert", "--no-commit", f"{s.ref}..HEAD"], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if res.returncode != 0:
        subprocess.run(["git", "revert", "--abort"], cwd=ROOT, capture_output=True)
        raise SnapshotError(f"git revert не прошёл (конфликт): {res.stderr.strip()[:400]}. Ничего не изменено.")
    git("commit", "-m", f"откат кода к {s.ref[:7]} ({s.title[8:80]}) - scripts.manage_snapshots")
    return f"код возвращён к {s.ref[:7]} новым коммитом {git('rev-parse', '--short', 'HEAD')}"


def deploy() -> int:
    if git("status", "--porcelain"):
        git("add", "orchestrator/strategy_overrides.json", "strategy_lab")
        git("commit", "-m", "откат параметров стратегии - scripts.manage_snapshots")
    push = subprocess.run(["git", "push", "origin", "HEAD"], cwd=ROOT)
    if push.returncode != 0:
        print("git push не прошёл - выкатываю локальную версию всё равно (сервер получает код из этой папки)")
    return subprocess.run([sys.executable, "-X", "utf8", "-m", "scripts.auto_deploy", "--host", VPS_HOST, "--key", str(Path(SSH_KEY).expanduser()),
                           "--skip-client-build"], cwd=ROOT).returncode


def ask(prompt: str) -> str:
    try:
        return input(prompt).strip().lower()
    except EOFError:
        return ""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Список снимков (параметры стратегий и версии кода) и откат по номеру")
    ap.add_argument("--list", action="store_true", help="только показать список")
    ap.add_argument("--pick", type=int, help="номер снимка из списка")
    ap.add_argument("--yes", action="store_true", help="не спрашивать подтверждение отката")
    ap.add_argument("--deploy", action="store_true", help="после отката сразу commit + push + выкатка на сервер")
    a = ap.parse_args(argv)
    try:
        snaps = all_snapshots()
        print_list(snaps)
        if a.list:
            return 0
        pick = a.pick
        if pick is None:
            raw = ask("\nНомер снимка для отката (Enter - выход): ")
            if not raw:
                return 0
            if not raw.isdigit():
                raise SnapshotError("нужен номер из списка")
            pick = int(raw)
        if not 1 <= pick <= len(snaps):
            raise SnapshotError(f"номера {pick} нет в списке (1..{len(snaps)})")
        s = snaps[pick - 1]
        print(f"\nВыбран №{pick}: [{s.kind}] {s.when} {s.title}")
        if not a.yes and ask("Откатить к этому снимку? (да/нет): ") not in ("да", "yes", "y", "д"):
            print("Отменено.")
            return 0
        done = rollback_params(s) if s.kind == "P" else rollback_code(s)
        print(f"Готово: {done}.")
        if a.deploy or (not a.yes and ask("Выкатить на сервер сейчас? (да/нет): ") in ("да", "yes", "y", "д")):
            return deploy()
        print("На сервере изменение начнёт действовать после выкатки. Выкатить позже: "
              f"python -X utf8 -m scripts.auto_deploy --host {VPS_HOST} --key {SSH_KEY} --skip-client-build")
        return 0
    except SnapshotError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
