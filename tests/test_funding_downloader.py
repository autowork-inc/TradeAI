import asyncio
import hashlib
from datetime import datetime, timezone

import polars as pl
import pytest
from aiohttp import web

import config
from collectors import funding_downloader as fd
from pipeline.loaders.schemas import FUNDING_SCHEMA as PIPELINE_FUNDING_SCHEMA
from tests.conftest import serve
from utils.errors import ConfigError, DataError, HttpClientError, HttpServerError, RateLimitError

H8 = 8 * 3_600_000
START = int(datetime(2024, 12, 30, tzinfo=timezone.utc).timestamp() * 1000)
N = 10  # списаний 0..9: с 30.12 00:00 до 2.01 00:00 - пересекают границу года
JITTER = 3  # реальные fundingTime не лежат ровно на сетке
NOW = START + N * H8 - 3_600_000
LIMIT = 4


def funding_time(i: int) -> int:
    return START + i * H8 + JITTER


def make_exchange(fail_after=None, status=None, headers=None, tweak=None):
    """Имитация Binance: /time и /fundingRate с семантикой startTime/limit (по возрастанию).
    Содержит и одну «будущую» запись (i == N), которую настоящая биржа не отдаёт."""
    state = {"calls": 0, "starts": []}

    async def time_handler(_):
        return web.json_response({"serverTime": NOW})

    async def funding_handler(request):
        state["calls"] += 1
        state["starts"].append(int(request.query["startTime"]))
        if status is not None:
            return web.Response(status=status, headers=headers or {}, text="{}")
        if fail_after is not None and state["calls"] > fail_after:
            return web.Response(status=500, text="boom")
        start, limit = int(request.query["startTime"]), int(request.query["limit"])
        rows = []
        for i in range(0, N + 1):
            if funding_time(i) < start:
                continue
            row = {
                "symbol": request.query["symbol"],
                "fundingTime": funding_time(i),
                "fundingRate": f"{0.0001 * (i % 5 - 2):.8f}",
                "markPrice": "" if i == 0 else f"{42000 + i}.10",
            }
            rows.append(tweak(i, row) if tweak else row)
            if len(rows) == limit:
                break
        return web.json_response(rows)

    app = web.Application()
    app.router.add_get("/fapi/v1/time", time_handler)
    app.router.add_get(config.FUNDING_PATH, funding_handler)
    return app, state


@pytest.fixture
def one_symbol(sandbox, monkeypatch):
    monkeypatch.setattr(config, "FUNDING_DIR", sandbox / "funding")
    monkeypatch.setattr(config, "SYMBOLS", ("BTCUSDT",))
    monkeypatch.setattr(config, "HISTORY_START_MS", START)
    monkeypatch.setattr(config, "FUNDING_LIMIT", LIMIT)
    return sandbox


async def run_download(host, monkeypatch):
    monkeypatch.setattr(config, "REST_BASE_URL", f"http://{host}")
    await fd.main()


def read_all():
    return pl.concat([pl.read_parquet(f) for f in fd.year_files(config.FUNDING_DIR / "BTCUSDT")])


def test_full_download_paginates_splits_years_and_drops_future(one_symbol, monkeypatch):
    async def scenario():
        app, state = make_exchange()
        async with serve(app) as host:
            await run_download(host, monkeypatch)
        return state

    state = asyncio.run(scenario())
    directory = config.FUNDING_DIR / "BTCUSDT"
    assert [f.name for f in fd.year_files(directory)] == ["2024.parquet", "2025.parquet"]
    df = read_all()
    assert df.schema == fd.FUNDING_SCHEMA
    assert df.height == N  # будущая запись (i == N) не сохранена
    assert df["funding_time"].to_list() == [funding_time(i) for i in range(N)]  # без округления к сетке
    assert df["mark_price"][0] is None and df["mark_price"][1] == 42001.10  # пустая строка -> null
    assert df["funding_rate"][2] == 0.0 and df["funding_rate"][0] == pytest.approx(-0.0002)
    assert not list(directory.glob("*.tmp.parquet"))
    assert state["calls"] == 3  # 4 + 4 + (2 + будущая)
    assert state["starts"] == [START, funding_time(3) + 1, funding_time(7) + 1]


def test_rerun_is_idempotent(one_symbol, monkeypatch):
    async def scenario():
        app, state = make_exchange()
        async with serve(app) as host:
            await run_download(host, monkeypatch)
            directory = config.FUNDING_DIR / "BTCUSDT"
            before = {f.name: hashlib.md5(f.read_bytes()).hexdigest() for f in fd.year_files(directory)}
            calls = state["calls"]
            await run_download(host, monkeypatch)
            after = {f.name: hashlib.md5(f.read_bytes()).hexdigest() for f in fd.year_files(directory)}
            return before, after, calls, state

    before, after, calls_before, state = asyncio.run(scenario())
    assert before == after
    assert state["calls"] == calls_before + 1  # одна проверка «нет ли нового» с last+1
    assert state["starts"][-1] == funding_time(N - 1) + 1


def test_resume_after_failure_has_no_gaps_or_duplicates(one_symbol, monkeypatch):
    async def scenario():
        app, _ = make_exchange(fail_after=1)
        async with serve(app) as host:
            with pytest.raises(HttpServerError):
                await run_download(host, monkeypatch)
        partial = read_all().height
        app, _ = make_exchange()
        async with serve(app) as host:
            await run_download(host, monkeypatch)
        return partial

    assert asyncio.run(scenario()) == LIMIT  # проверенный пакет уже на диске
    df = read_all()
    assert df["funding_time"].to_list() == [funding_time(i) for i in range(N)]


def test_changed_rate_for_stored_funding_time_is_an_error(one_symbol, monkeypatch):
    async def scenario():
        app, _ = make_exchange()
        async with serve(app) as host:
            await run_download(host, monkeypatch)

    asyncio.run(scenario())
    stored_before = read_all()
    # Биржа «пересчитала» историю: тот же fundingTime, другая ставка.
    batch = fd.parse_funding(
        [{"symbol": "BTCUSDT", "fundingTime": funding_time(N - 1), "fundingRate": "0.00999", "markPrice": "1"}],
        "BTCUSDT",
    )
    with pytest.raises(DataError, match="другую ставку"):
        fd.upsert_year(config.FUNDING_DIR / "BTCUSDT" / "2025.parquet", batch)
    assert read_all().equals(stored_before)  # файл не тронут


def test_upsert_is_idempotent_and_counts_additions(sandbox):
    rows = [{"symbol": "BTCUSDT", "fundingTime": funding_time(i), "fundingRate": "0.0001", "markPrice": "5"}
            for i in range(6)]
    full = fd.parse_funding(rows, "BTCUSDT")
    path = sandbox / "2025.parquet"
    assert fd.upsert_year(path, full.head(4)) == 4
    assert fd.upsert_year(path, full.tail(4)) == 2  # перекрытие 2 записи
    assert fd.upsert_year(path, full) == 0
    assert pl.read_parquet(path).height == 6


@pytest.mark.parametrize("bad", [
    {"x": 1},  # не список
    [[1, 2]],  # запись не объект
    [{"symbol": "ETHUSDT", "fundingTime": 1, "fundingRate": "0.0001", "markPrice": "1"}],  # чужой символ
    [{"symbol": "BTCUSDT", "fundingTime": "1", "fundingRate": "0.0001", "markPrice": "1"}],  # время строкой
    [{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "abc", "markPrice": "1"}],
    [{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "nan", "markPrice": "1"}],
    [{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "5.0", "markPrice": "1"}],  # >= 100% за интервал
    [{"symbol": "BTCUSDT", "fundingTime": 1, "fundingRate": "0.0001", "markPrice": "-3"}],
    [{"symbol": "BTCUSDT", "fundingTime": 1, "markPrice": "1"}],  # нет fundingRate
])
def test_parse_funding_rejects_bad_data(bad):
    with pytest.raises(DataError):
        fd.parse_funding(bad, "BTCUSDT")


def test_off_schedule_steps_only_logged_not_rounded():
    rows = [{"symbol": "BTCUSDT", "fundingTime": t, "fundingRate": "0.0001", "markPrice": "1"}
            for t in (START, START + H8 + 5, START + 2 * H8 + 5, START + 2 * H8 + 5 + H8 // 2)]
    batch = fd.parse_funding(rows, "BTCUSDT")
    assert fd.off_schedule_steps(batch, H8) == 1  # 4-часовой шаг; дрожание в 5 мс - норма


def test_429_and_4xx_use_stage1_error_semantics(one_symbol, monkeypatch):
    async def scenario(status, headers=None):
        app, _ = make_exchange(status=status, headers=headers)
        async with serve(app) as host:
            await run_download(host, monkeypatch)

    with pytest.raises(RateLimitError) as info:
        asyncio.run(scenario(429, {"Retry-After": "7"}))
    assert info.value.retry_after == 7 and info.value.exit_code == 2
    with pytest.raises(HttpClientError) as info:
        asyncio.run(scenario(400))
    assert info.value.exit_code == 3


def test_stale_tmp_file_is_removed(one_symbol, monkeypatch):
    directory = config.FUNDING_DIR / "BTCUSDT"
    directory.mkdir(parents=True)
    (directory / "2024.tmp.parquet").write_bytes(b"half-written garbage")

    async def scenario():
        app, _ = make_exchange()
        async with serve(app) as host:
            await run_download(host, monkeypatch)

    asyncio.run(scenario())
    assert not list(directory.glob("*.tmp.parquet")) and read_all().height == N


def test_schema_matches_pipeline_and_config_limits(monkeypatch):
    assert dict(fd.FUNDING_SCHEMA) == dict(PIPELINE_FUNDING_SCHEMA)
    for bad in (0, 1001):
        monkeypatch.setattr(config, "FUNDING_LIMIT", bad)
        with pytest.raises(ConfigError):
            config.validate()
