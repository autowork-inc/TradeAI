import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from aiohttp import web

import config
from collectors import depth_stream as ds
from collectors import klines_downloader as kd
from tests.conftest import serve
from utils import ban_lock, runner
from utils.errors import BanActive, InstanceAlreadyRunning
from utils.instance_lock import InstanceLock

ROOT = Path(__file__).resolve().parent.parent

HOLDER = """
import sys, time
sys.path.insert(0, {root!r})
from utils.instance_lock import InstanceLock
lock = InstanceLock({name!r})
lock.acquire()
import os
print("ready", os.getpid(), flush=True)
time.sleep(120)
"""


def child_env(sandbox):
    env = dict(os.environ)
    env.update(TRADING_DATA_DIR=str(config.DATA_DIR), TRADING_LOG_DIR=str(sandbox / "childlogs"),
               PYTHONIOENCODING="utf-8", PYTHONPATH=str(ROOT))
    return env


def spawn_holder(sandbox, name):
    proc = subprocess.Popen(
        [sys.executable, "-c", HOLDER.format(root=str(ROOT), name=name)],
        stdout=subprocess.PIPE, env=child_env(sandbox), text=True,
    )
    tag, pid = proc.stdout.readline().split()
    assert tag == "ready"
    # На Windows python.exe из venv - лаунчер: pid Popen != pid интерпретатора, держащего замок.
    return proc, int(pid)


def hard_kill(proc, holder_pid):
    """SIGKILL / TerminateProcess держателя замка: код очистки в нём не выполняется."""
    try:
        os.kill(holder_pid, getattr(signal, "SIGKILL", signal.SIGTERM))
    except ProcessLookupError:
        pass
    proc.wait(timeout=30)


def test_second_acquire_fails_without_waiting(sandbox):
    first = InstanceLock("depth_stream")
    first.acquire()
    try:
        with pytest.raises(InstanceAlreadyRunning) as info:
            InstanceLock("depth_stream").acquire()
        assert info.value.exit_code == 1
        assert f"pid={os.getpid()}" in str(info.value)  # диагностика: кто держит замок
    finally:
        first.release()


def test_release_allows_reacquire_and_file_is_kept(sandbox):
    lock = InstanceLock("depth_stream")
    with lock:
        pass
    assert lock.path.exists()  # файл замка не удаляется
    with InstanceLock("depth_stream"):
        pass


def test_context_manager_releases_on_exception(sandbox):
    with pytest.raises(RuntimeError):
        with InstanceLock("klines_downloader"):
            raise RuntimeError("boom")
    with InstanceLock("klines_downloader"):
        pass


def test_different_scripts_do_not_block_each_other(sandbox):
    with InstanceLock("depth_stream"), InstanceLock("klines_downloader"):
        pass


def test_dead_process_leaves_no_stale_lock_even_after_hard_kill(sandbox):
    proc, holder_pid = spawn_holder(sandbox, "depth_stream")
    try:
        with pytest.raises(InstanceAlreadyRunning) as info:
            InstanceLock("depth_stream").acquire()
        assert f"pid={holder_pid}" in str(info.value)
    finally:
        hard_kill(proc, holder_pid)
    assert InstanceLock("depth_stream").path.exists()  # файл остался...
    with InstanceLock("depth_stream"):  # ...но замок снят самой ОС
        pass


@pytest.mark.parametrize("entry", ["klines", "depth"])
def test_main_fails_fast_when_instance_running_and_touches_no_network(sandbox, monkeypatch, entry):
    hits = []

    async def trap(request):
        hits.append(request.path)
        return web.Response(status=500)

    name = "klines_downloader" if entry == "klines" else "depth_stream"

    async def scenario():
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", trap)
        async with serve(app) as host:
            monkeypatch.setattr(config, "REST_BASE_URL", f"http://{host}")
            monkeypatch.setattr(config, "WS_BASE_URL", f"ws://{host}/public/stream")
            with InstanceLock(name):
                with pytest.raises(InstanceAlreadyRunning):
                    await (kd.main() if entry == "klines" else ds.main())

    asyncio.run(scenario())
    assert hits == []


def test_lock_is_released_after_main_fails(sandbox, monkeypatch):
    async def scenario():
        monkeypatch.setattr(config, "REST_BASE_URL", "http://127.0.0.1:1")
        with pytest.raises(Exception):
            await ds.main()  # снапшот не получить - main падает внутри `with`

    asyncio.run(scenario())
    with InstanceLock("depth_stream"):
        pass


def test_ban_lock_is_checked_before_instance_lock(sandbox):
    ban_lock.record_ban(300, 418, "test")
    with InstanceLock("depth_stream"):
        with pytest.raises(BanActive):  # не InstanceAlreadyRunning
            asyncio.run(ds.main())


def test_runner_logs_critical_and_exits_1(sandbox):
    with InstanceLock("klines_downloader"):
        with pytest.raises(SystemExit) as info:
            runner.run(kd.main, "test_instance")
    assert info.value.code == 1
    log_text = (config.LOG_DIR / "test_instance.log").read_text(encoding="utf-8")
    assert "CRITICAL" in log_text and "Экземпляр уже запущен" in log_text


@pytest.mark.parametrize("module", ["collectors.depth_stream", "collectors.klines_downloader"])
def test_real_script_process_exits_1_with_critical(sandbox, module):
    name = module.split(".")[1]
    with InstanceLock(name):
        done = subprocess.run(
            [sys.executable, "-m", module], cwd=ROOT, env=child_env(sandbox),
            capture_output=True, text=True, encoding="utf-8", timeout=60,
        )
    assert done.returncode == 1
    assert "CRITICAL" in done.stdout and "Экземпляр уже запущен" in done.stdout
