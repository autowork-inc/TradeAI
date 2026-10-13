import asyncio
import hashlib
from datetime import datetime, timezone

import aiohttp
import polars as pl
import pytest
from aiohttp import web

import config
from collectors import klines_downloader as kd
from tests.conftest import serve
from utils import runner
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import (
    DataError,
    HttpClientError,
    HttpServerError,
    RateLimitError,
)
from utils.storage import atomic_write_parquet

MIN = 60_000
BASE = int(datetime(2024, 1, 31, 23, 0, tzinfo=timezone.utc).timestamp() * 1000)
CLOSED = 2500  # свечей 0..2499 закрыты, свеча 2500 ещё формируется
NOW = BASE + CLOSED * MIN + 30_000


def make_exchange(fail_after=None, status=None, headers=None):
    """Имитация Binance: /time и /klines с настоящей семантикой startTime/limit."""
    state = {"klines_calls": 0}

    async def time_handler(_):
        return web.json_response({"serverTime": NOW})

    async def klines_handler(request):
        state["klines_calls"] += 1
        if status is not None:
            return web.Response(status=status, headers=headers or {}, text="{}")
        if fail_after is not None and state["klines_calls"] > fail_after:
            return web.Response(status=500, text="boom")
        start = int(request.query["startTime"])
        limit = int(request.query["limit"])
        first = max(0, -(-(start - BASE) // MIN))  # ceil
        rows = []
        for i in range(first, first + limit):
            open_time = BASE + i * MIN
            if open_time > NOW:
                break  # будущих свечей нет; формирующаяся (i == CLOSED) присутствует
            rows.append([open_time, "100.5", "101.0", "99.5", "100.7", "12.5",
                         open_time + MIN - 1, "1250.0", 42, "6.0", "600.0", "0"])
        return web.json_response(rows, headers={"X-MBX-USED-WEIGHT-1M": "10"})

    app = web.Application()
    app.router.add_get("/fapi/v1/time", time_handler)
    app.router.add_get("/fapi/v1/klines", klines_handler)
    return app, state


@pytest.fixture
def one_pair(sandbox, monkeypatch):
    monkeypatch.setattr(config, "SYMBOLS", ("BTCUSDT",))
    monkeypatch.setattr(config, "KLINE_INTERVALS", ("1m",))
    monkeypatch.setattr(config, "HISTORY_START_MS", BASE)
    return sandbox


async def run_download(host):
    monkey = config.REST_BASE_URL
    config.REST_BASE_URL = f"http://{host}"
    try:
        await kd.main()
    finally:
        config.REST_BASE_URL = monkey


def read_all(pair_dir):
    return pl.concat([pl.read_parquet(f) for f in kd.month_files(pair_dir)])


def test_full_download_splits_months_and_drops_unclosed(one_pair):
    async def scenario():
        app, state = make_exchange()
        async with serve(app) as host:
            await run_download(host)
        return state

    state = asyncio.run(scenario())
    pair_dir = config.KLINES_DIR / "BTCUSDT" / "1m"
    assert [f.name for f in kd.month_files(pair_dir)] == ["2024-01.parquet", "2024-02.parquet"]
    df = read_all(pair_dir)
    assert df.height == CLOSED  # формирующаяся свеча не сохранена
    assert df.schema == kd.KLINE_SCHEMA
    assert df["open_time"].is_sorted() and df["open_time"].n_unique() == CLOSED
    assert df["open_time"][0] == BASE and df["open_time"][-1] == BASE + (CLOSED - 1) * MIN
    assert (df["open_time"].diff().drop_nulls() == MIN).all()
    assert df["open"][0] == 100.5 and df["trades"][0] == 42  # строки превращены в числа
    assert not list(pair_dir.glob("*.tmp.parquet"))
    assert state["klines_calls"] == 3  # 1000 + 1000 + 501 (последняя неполная)


def test_rerun_is_idempotent_and_makes_no_requests(one_pair):
    async def scenario():
        app, state = make_exchange()
        async with serve(app) as host:
            await run_download(host)
            pair_dir = config.KLINES_DIR / "BTCUSDT" / "1m"
            before = {f.name: hashlib.md5(f.read_bytes()).hexdigest() for f in kd.month_files(pair_dir)}
            calls = state["klines_calls"]
            await run_download(host)
            after = {f.name: hashlib.md5(f.read_bytes()).hexdigest() for f in kd.month_files(pair_dir)}
            return before, after, calls, state["klines_calls"]

    before, after, calls_before, calls_after = asyncio.run(scenario())
    assert before == after
    assert calls_before == calls_after


def test_resume_after_failure_has_no_gaps_or_duplicates(one_pair):
    async def scenario():
        app, _ = make_exchange(fail_after=1)
        async with serve(app) as host:
            with pytest.raises(HttpServerError):
                await run_download(host)
        partial = read_all(config.KLINES_DIR / "BTCUSDT" / "1m").height
        app, _ = make_exchange()
        async with serve(app) as host:
            await run_download(host)
        return partial

    partial = asyncio.run(scenario())
    assert partial == 1000  # проверенный пакет сохранён перед выходом
    df = read_all(config.KLINES_DIR / "BTCUSDT" / "1m")
    assert df.height == CLOSED and df["open_time"].n_unique() == CLOSED
    assert (df["open_time"].diff().drop_nulls() == MIN).all()


def test_stale_tmp_file_is_removed_and_ignored(one_pair):
    pair_dir = config.KLINES_DIR / "BTCUSDT" / "1m"
    pair_dir.mkdir(parents=True)
    (pair_dir / "2024-01.tmp.parquet").write_bytes(b"half-written garbage")

    async def scenario():
        app, _ = make_exchange()
        async with serve(app) as host:
            await run_download(host)

    asyncio.run(scenario())
    assert not list(pair_dir.glob("*.tmp.parquet"))
    assert read_all(pair_dir).height == CLOSED


def test_429_raises_with_retry_after(one_pair):
    async def scenario():
        app, _ = make_exchange(status=429, headers={"Retry-After": "7"})
        async with serve(app) as host:
            with pytest.raises(RateLimitError) as info:
                await run_download(host)
        return info.value

    err = asyncio.run(scenario())
    assert err.retry_after == 7 and err.status == 429 and err.exit_code == 2


def test_418_without_header_uses_default(one_pair):
    async def scenario():
        app, _ = make_exchange(status=418)
        async with serve(app) as host:
            with pytest.raises(RateLimitError) as info:
                await run_download(host)
        return info.value

    assert asyncio.run(scenario()).retry_after == config.RETRY_AFTER_DEFAULT_418_S


def test_4xx_is_client_error_exit_3(one_pair):
    async def scenario():
        app, _ = make_exchange(status=400)
        async with serve(app) as host:
            with pytest.raises(HttpClientError) as info:
                await run_download(host)
        return info.value

    assert asyncio.run(scenario()).exit_code == 3


def test_timeout_is_network_error(one_pair, monkeypatch):
    monkeypatch.setattr(config, "HTTP_TOTAL_TIMEOUT_S", 0.3)

    async def slow(_):
        await asyncio.sleep(2)

    async def scenario():
        app = web.Application()
        app.router.add_get("/fapi/v1/time", slow)
        async with serve(app) as host:
            from utils.errors import NetworkError
            with pytest.raises(NetworkError):
                await run_download(host)

    asyncio.run(scenario())


def test_runner_on_429_writes_ban_lock_and_exits_2_without_sleeping(one_pair, monkeypatch):
    import json
    import time

    async def no_sleep(seconds, log):
        raise AssertionError("слепой сон недопустим, если замок записан")

    monkeypatch.setattr(runner, "sleep_with_heartbeat", no_sleep)

    async def main():
        app, _ = make_exchange(status=429, headers={"Retry-After": "11"})
        async with serve(app) as host:
            await run_download(host)

    t0 = time.time()
    with pytest.raises(SystemExit) as info:
        runner.run(main, "test_runner")
    assert info.value.code == 2
    lock = json.loads((config.DATA_DIR / config.BAN_LOCK_NAME).read_text())
    assert t0 + 11 + config.BAN_SAFETY_MARGIN_S <= lock["banned_until"] <= time.time() + 11 + config.BAN_SAFETY_MARGIN_S
    assert lock["status"] == 429


def test_runner_falls_back_to_in_process_sleep_if_lock_cannot_be_written(one_pair, monkeypatch):
    from utils import ban_lock
    slept = []

    async def fake_sleep(seconds, log):
        slept.append(seconds)

    def broken(*a, **k):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(runner, "sleep_with_heartbeat", fake_sleep)
    monkeypatch.setattr(ban_lock, "record_ban", broken)

    async def main():
        app, _ = make_exchange(status=429, headers={"Retry-After": "11"})
        async with serve(app) as host:
            await run_download(host)

    with pytest.raises(SystemExit) as info:
        runner.run(main, "test_runner")
    assert info.value.code == 2 and slept == [11 + config.BAN_SAFETY_MARGIN_S]


def test_runner_exit_codes(sandbox):
    from utils.errors import ConfigError

    async def bad_config():
        raise ConfigError("x")

    async def crash():
        raise RuntimeError("x")

    for fn, code in ((bad_config, 3), (crash, 1)):
        with pytest.raises(SystemExit) as info:
            runner.run(fn, "test_runner")
        assert info.value.code == code


def test_atomic_write_failure_keeps_old_file(sandbox, monkeypatch):
    path = sandbox / "x" / "2024-01.parquet"
    good = pl.DataFrame({"a": [1, 2, 3]})
    atomic_write_parquet(good, path)
    original = path.read_bytes()

    def broken(self, file, *a, **k):
        open(file, "wb").write(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", broken)
    with pytest.raises(OSError):
        atomic_write_parquet(pl.DataFrame({"a": [9]}), path)
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.tmp.parquet"))


def test_upsert_overlap_does_not_duplicate(sandbox):
    rows = [[BASE + i * MIN, "1", "2", "0.5", "1.5", "3", BASE + i * MIN + MIN - 1, "4", 5, "6", "7", "0"]
            for i in range(10)]
    full = kd.parse_klines(rows)
    path = sandbox / "2024-01.parquet"
    assert kd.upsert_month(path, full.head(6)) == 6
    assert kd.upsert_month(path, full.tail(6)) == 4  # перекрытие 2 свечи
    assert kd.upsert_month(path, full) == 0
    assert pl.read_parquet(path).height == 10


def test_parse_klines_rejects_bad_data():
    with pytest.raises(DataError):
        kd.parse_klines({"code": -1121})
    with pytest.raises(DataError):
        kd.parse_klines([[1, 2, 3]])
    with pytest.raises(DataError):
        kd.parse_klines([[BASE, "abc", "1", "1", "1", "1", BASE, "1", 1, "1", "1", "0"]])


def test_klines_weight_table():
    assert [klines_weight(n) for n in (1, 99, 100, 499, 500, 1000, 1001, 1500)] == [1, 1, 2, 2, 5, 5, 10, 10]


def test_throttle_waits_near_soft_limit(sandbox, monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def scenario():
        async with aiohttp.ClientSession() as session:
            api = BinanceRest(session, "http://unused")
            api._used_weight = config.WEIGHT_SOFT_LIMIT_1M - 2
            await api._throttle(5)
            return api._used_weight

    assert asyncio.run(scenario()) == 0
    assert len(sleeps) == 1 and 1 <= sleeps[0] <= 61
