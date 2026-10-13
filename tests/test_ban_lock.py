import asyncio
import json
import time

import pytest
from aiohttp import web

import config
from collectors import depth_stream as ds
from collectors import klines_downloader as kd
from tests.conftest import serve
from utils import ban_lock
from utils.errors import BanActive, DataError


def lock_file():
    return config.DATA_DIR / config.BAN_LOCK_NAME


def test_no_lock_means_not_banned(sandbox):
    ban_lock.ensure_not_banned()


def test_active_ban_raises_with_exit_code_2(sandbox):
    until = ban_lock.record_ban(120, 418, "https://x")
    assert until == pytest.approx(time.time() + 120 + config.BAN_SAFETY_MARGIN_S, abs=2)
    with pytest.raises(BanActive) as info:
        ban_lock.ensure_not_banned()
    assert info.value.exit_code == 2 and 100 < info.value.remaining <= 125


def test_expired_ban_is_ignored_and_file_is_left_alone(sandbox):
    lock_file().parent.mkdir(parents=True)
    lock_file().write_text(json.dumps({"banned_until": time.time() - 1}))
    ban_lock.ensure_not_banned()
    assert lock_file().exists()  # истёкший замок не удаляется (нет гонки с новой записью)


def test_new_ban_never_shortens_existing_one(sandbox):
    long = ban_lock.record_ban(3600, 418, "a")
    short = ban_lock.record_ban(10, 429, "b")
    assert short == long
    assert json.loads(lock_file().read_text())["banned_until"] == long


def test_longer_ban_extends(sandbox):
    short = ban_lock.record_ban(10, 429, "a")
    long = ban_lock.record_ban(3600, 418, "b")
    assert long > short


@pytest.mark.parametrize("content", ["", "garbage", "{}", '{"banned_until": "soon"}', '{"banned_until": NaN}'])
def test_corrupt_lock_fails_fast_instead_of_being_ignored(sandbox, content):
    lock_file().parent.mkdir(parents=True)
    lock_file().write_text(content)
    with pytest.raises(DataError):
        ban_lock.ensure_not_banned()


def test_record_ban_replaces_corrupt_lock(sandbox):
    lock_file().parent.mkdir(parents=True)
    lock_file().write_text("garbage")
    ban_lock.record_ban(30, 429, "x")
    with pytest.raises(BanActive):
        ban_lock.ensure_not_banned()


def test_no_tmp_left_after_write(sandbox):
    ban_lock.record_ban(30, 429, "x")
    assert [p.name for p in lock_file().parent.iterdir()] == [config.BAN_LOCK_NAME]


@pytest.mark.parametrize("entry", ["klines", "depth"])
def test_both_scripts_exit_immediately_while_banned_without_touching_network(sandbox, monkeypatch, entry):
    """Сервер-ловушка: при любом обращении к бирже тест падает."""
    hits = []

    async def trap(request):
        hits.append(request.path)
        return web.Response(status=500)

    async def scenario():
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", trap)
        async with serve(app) as host:
            monkeypatch.setattr(config, "REST_BASE_URL", f"http://{host}")
            monkeypatch.setattr(config, "WS_BASE_URL", f"ws://{host}/public/stream")
            ban_lock.record_ban(300, 418, "test")
            with pytest.raises(BanActive):
                await (kd.main() if entry == "klines" else ds.main())

    asyncio.run(scenario())
    assert hits == []


def test_runner_exit_code_2_when_banned_at_start(sandbox):
    from utils import runner
    ban_lock.record_ban(300, 418, "test")
    with pytest.raises(SystemExit) as info:
        runner.run(kd.main, "test_runner")
    assert info.value.code == 2
