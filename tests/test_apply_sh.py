"""deploy/apply.sh на поддельных docker/shadow.sh: сохранение контура, отсрочка при боевых позициях, проверка здоровья и откат (решение пользователя 2026-10-18)."""

import hashlib
import io
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SH = shutil.which("sh")
pytestmark = pytest.mark.skipif(SH is None or os.name == "nt" and "usr" not in (SH or "").lower() and "git" not in (SH or "").lower(), reason="нужен POSIX sh")

DOCKER_STUB = """#!/bin/sh
# поддельный docker: состояние сервисов лежит в $STUB/<сервис>.state, лог - в <сервис>.log
svc=""
for a in "$@"; do case "$a" in label=com.docker.compose.service=*) svc="${a#label=com.docker.compose.service=}" ;; esac; done
case "$1" in
    info) exit 0 ;;
    ps) if [ -f "$STUB/$svc.state" ]; then echo "cid_$svc"; fi ;;
    inspect) id="$(echo "$@" | sed 's/.* cid_//')"; cat "$STUB/cid_$id.state" ;;
    logs) id="$(echo "$@" | sed 's/.* cid_//')"; cat "$STUB/cid_$id.log" 2>/dev/null || true ;;
    *) exit 0 ;;
esac
"""
SHADOW_STUB = """#!/bin/sh
# поддельный shadow.sh: запоминает вызов, поднимает сервис контура (или роняет, если задан $STUB/fail_version)
echo "$*" >> "$STUB/calls"
case "$1" in up-live) svc=shadow-live; dir=live ;; *) svc=shadow-run; dir=shadow ;; esac
mkdir -p "data/$dir"
version="$(cat VERSION 2>/dev/null || echo none)"
rm -f "$STUB/shadow-run.state" "$STUB/shadow-live.state"
if [ "$version" = "$(cat "$STUB/fail_version" 2>/dev/null || echo -)" ]; then
    : > "$STUB/$svc.state"; echo exited > "$STUB/cid_$svc.state"; echo "Traceback boom" > "$STUB/cid_$svc.log"; exit 0
fi
echo running > "$STUB/cid_$svc.state"; : > "$STUB/$svc.state"; echo ok > "$STUB/cid_$svc.log"
touch "data/$dir/.shadow_heartbeat"
"""


def make_archive(path: Path, version: str) -> str:
    """Архив «версии»: файл VERSION, настоящий deploy/apply.sh и поддельный shadow.sh."""
    files = {"VERSION": version.encode(), "deploy/apply.sh": (ROOT / "deploy" / "apply.sh").read_bytes().replace(b"\r\n", b"\n"),
             "deploy/shadow.sh": SHADOW_STUB.encode(), "docker-compose.yml": b"services: {}\n"}
    with tarfile.open(path, "w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o755 if name.endswith(".sh") else 0o644
            tar.addfile(info, io.BytesIO(data))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha256").write_text(f"{digest}  {path.name}\n", encoding="utf-8")
    return digest


@pytest.fixture
def env(tmp_path):
    proj, stub, incoming, bindir = tmp_path / "trading", tmp_path / "stub", tmp_path / "incoming", tmp_path / "bin"
    for d in (proj / "deploy", proj / "data", proj / "logs", stub, incoming, bindir):
        d.mkdir(parents=True, exist_ok=True)
    (bindir / "docker").write_text(DOCKER_STUB, encoding="utf-8", newline="\n")
    (bindir / "docker").chmod(0o755)
    (proj / ".env.shadow").write_text("POSTGRES_PASSWORD=x\nLIVE_ALLOW_MAINNET=true\n", encoding="utf-8")
    (proj / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    (proj / "VERSION").write_text("v1", encoding="utf-8")
    shutil.copy(ROOT / "deploy" / "apply.sh", proj / "deploy" / "apply.sh")
    (proj / "deploy" / "shadow.sh").write_text(SHADOW_STUB, encoding="utf-8", newline="\n")
    (proj / "deploy" / "shadow.sh").chmod(0o755)

    def run(*args, extra_env=None):
        e = {**os.environ, "PATH": f"{bindir.as_posix()}{os.pathsep}{os.environ['PATH']}", "STUB": stub.as_posix(), "APPLY_INCOMING_DIR": incoming.as_posix(), "HOME": tmp_path.as_posix(),
             "APPLY_HEALTH_TRIES": "3", "APPLY_HEALTH_SLEEP": "0", **(extra_env or {})}
        return subprocess.run([SH, (proj / "deploy" / "apply.sh").as_posix(), *args], capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=proj, env=e, timeout=120)

    return proj, stub, incoming, run


def calls(stub: Path) -> list[str]:
    f = stub / "calls"
    return f.read_text(encoding="utf-8").splitlines() if f.exists() else []


def set_running(stub: Path, svc: str):
    (stub / f"{svc}.state").write_text("", encoding="utf-8")
    (stub / f"cid_{svc}.state").write_text("running", encoding="utf-8")


def test_shadow_contour_is_updated_with_shadow_up_and_marked_applied(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-run")
    digest = make_archive(incoming / "trading_shadow.tar.gz", "v2")
    r = run("--pending")
    assert r.returncode == 0, r.stdout + r.stderr
    assert calls(stub) == ["up"] and (proj / "VERSION").read_text() == "v2"
    assert (proj / ".deploy_applied").read_text().strip() == digest and (proj / ".deploy_contour").read_text().strip() == "shadow"
    assert not (incoming / "trading_shadow.tar.gz").exists() and (incoming / "applied" / "trading_shadow.tar.gz").exists() and (incoming / "prev.tar.gz").exists()
    again = run("--pending")  # архива в incoming больше нет, ничего не делаем
    assert again.returncode == 0 and calls(stub) == ["up"]


def test_live_contour_stays_live_and_uses_non_interactive_up_live(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-live")
    make_archive(incoming / "trading_shadow.tar.gz", "v2")
    r = run("--pending")
    assert r.returncode == 0, r.stdout + r.stderr
    assert calls(stub) == ["up-live --yes"] and (proj / ".deploy_contour").read_text().strip() == "live"  # контур не сброшен в Shadow, REAL вводить не нужно


def test_open_live_positions_defer_update_until_flat_then_apply(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-live")
    (proj / "data" / "live").mkdir(parents=True)
    state = proj / "data" / "live" / "live_engine_state.json"
    state.write_text('{"version": 1, "trades": [\n {"signal_id": "x"}\n]}\n', encoding="utf-8")
    make_archive(incoming / "trading_shadow.tar.gz", "v2")
    r = run("--pending")
    assert r.returncode == 75 and "ОТЛОЖЕНО" in r.stdout and calls(stub) == [] and (proj / "VERSION").read_text() == "v1"  # позиции не трогаем
    assert (incoming / "trading_shadow.tar.gz").exists()  # архив ждёт
    state.write_text('{"version": 1, "trades": []}\n', encoding="utf-8")  # позиции закрылись
    r = run("--pending")
    assert r.returncode == 0 and calls(stub) == ["up-live --yes"] and (proj / "VERSION").read_text() == "v2"


def test_deferral_is_overridden_by_force_and_by_max_wait(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-live")
    (proj / "data" / "live").mkdir(parents=True)
    (proj / "data" / "live" / "live_engine_state.json").write_text('{"version": 1, "trades": [\n {"signal_id": "x"}\n]}\n', encoding="utf-8")
    make_archive(incoming / "trading_shadow.tar.gz", "v2")
    assert run("--pending", "--force").returncode == 0 and calls(stub) == ["up-live --yes"]
    make_archive(incoming / "trading_shadow.tar.gz", "v3")
    old = (incoming / "trading_shadow.tar.gz").stat().st_mtime - 13 * 3600
    os.utime(incoming / "trading_shadow.tar.gz", (old, old))
    set_running(stub, "shadow-live")
    r = run("--pending")  # архив ждёт дольше предела APPLY_MAX_DEFER_H=12
    assert r.returncode == 0 and (proj / "VERSION").read_text() == "v3"


def test_unhealthy_new_version_is_rolled_back_to_previous_code(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-live")
    (stub / "fail_version").write_text("v2", encoding="utf-8")  # новая версия падает при старте
    make_archive(incoming / "trading_shadow.tar.gz", "v2")
    r = run("--pending")
    assert r.returncode == 1 and "откат" in r.stdout + r.stderr
    assert (proj / "VERSION").read_text() == "v1"  # прежний код возвращён
    assert calls(stub) == ["up-live --yes", "up-live --yes"]  # новая попытка и повторный запуск прежней версии
    assert not (proj / ".deploy_applied").exists()
    assert (stub / "cid_shadow-live.state").read_text().strip() == "running"  # прежняя версия снова работает


def test_corrupted_archive_is_refused_and_status_reports_pending(env):
    proj, stub, incoming, run = env
    set_running(stub, "shadow-run")
    make_archive(incoming / "trading_shadow.tar.gz", "v2")
    (incoming / "trading_shadow.tar.gz.sha256").write_text("0" * 64 + "  trading_shadow.tar.gz\n", encoding="utf-8")
    r = run("--pending")
    assert r.returncode == 1 and "sha256" in r.stdout + r.stderr and calls(stub) == [] and (proj / "VERSION").read_text() == "v1"
    s = run("--status")
    assert s.returncode == 0 and "ОТЛОЖЕНО" in s.stdout and "нет" in s.stdout
