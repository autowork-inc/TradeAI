import asyncio
import json
from datetime import datetime, timezone

import pytest
from aiohttp import web

import config
from collectors import depth_stream as ds
from tests.conftest import serve
from utils.errors import (
    DataError,
    RateLimitError,
    ServerShutdown,
    StreamStalled,
    WebSocketClosed,
)
from utils.storage import HourlyJsonlWriter


def depth_msg(symbol="BTCUSDT", u=100, **overrides):
    stream = f"{symbol.lower()}@depth{config.DEPTH_LEVELS}@{config.DEPTH_UPDATE_SPEED}"
    data = {"e": "depthUpdate", "E": 1700000000000, "T": 1700000000000 - 1, "s": symbol,
            "U": u - 5, "u": u, "pu": u - 6,
            "b": [["60000.10", "1.500"], ["59999.90", "0.250"]],
            "a": [["60000.20", "2.000"]]}
    data.update(overrides)
    return {"stream": stream, "data": data}


SNAPSHOT = {"lastUpdateId": 777, "E": 1700000000000, "T": 1699999999999,
            "bids": [["60000.10", "1.5"], ["59999.90", "0.2"]], "asks": [["60000.20", "2.0"]]}


def ws_app(script, status=None, headers=None, snapshot=None, snapshot_status=200):
    """script: async-функция(ws), выполняющая сценарий сервера.

    seen["events"] - порядок обращений к серверу ("rest:<symbol>", "ws").
    """
    seen = {"events": []}

    async def handler(request):
        seen["events"].append("ws")
        seen["query"] = request.query_string
        if status is not None:
            return web.Response(status=status, headers=headers or {})
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await script(ws)
        return ws

    async def depth_handler(request):
        seen["events"].append(f"rest:{request.query['symbol']}")
        seen["rest_query"] = dict(request.query)
        if snapshot_status != 200:
            return web.Response(status=snapshot_status, headers=headers or {}, text="{}")
        return web.json_response(snapshot if snapshot is not None else SNAPSHOT)

    app = web.Application()
    app.router.add_get("/public/stream", handler)
    app.router.add_get("/fapi/v1/depth", depth_handler)
    return app, seen


async def run_depth(host, **cfg):
    saved = config.WS_BASE_URL, config.REST_BASE_URL
    config.WS_BASE_URL = f"ws://{host}/public/stream"
    config.REST_BASE_URL = f"http://{host}"
    try:
        await ds.main()
    finally:
        config.WS_BASE_URL, config.REST_BASE_URL = saved


def read_lines(root, symbol):
    return [json.loads(line) for f in sorted((root / symbol).rglob("*.jsonl"))
            for line in f.read_text(encoding="utf-8").splitlines()]


def test_records_messages_then_fails_on_close(sandbox):
    async def script(ws):
        await ws.send_str(json.dumps(depth_msg("BTCUSDT", 100)))
        await ws.send_str(json.dumps(depth_msg("ETHUSDT", 200)))
        await ws.send_str(json.dumps(depth_msg("BTCUSDT", 101)))
        await ws.close()

    async def scenario():
        app, seen = ws_app(script)
        async with serve(app) as host:
            with pytest.raises(WebSocketClosed):
                await run_depth(host)
        return seen

    seen = asyncio.run(scenario())
    assert seen["query"] == "streams=btcusdt@depth10@500ms/ethusdt@depth10@500ms"
    assert seen["events"] == ["rest:BTCUSDT", "rest:ETHUSDT", "ws"]  # снапшоты строго до сокета
    assert seen["rest_query"]["limit"] == str(config.DEPTH_SNAPSHOT_LIMIT)
    btc = read_lines(config.DEPTH_DIR, "BTCUSDT")
    eth = read_lines(config.DEPTH_DIR, "ETHUSDT")
    # первой строкой идёт снапшот, затем сообщения потока
    assert [r["stream"] for r in btc] == ["snapshot", "btcusdt@depth10@500ms", "btcusdt@depth10@500ms"]
    assert [r["stream"] for r in eth] == ["snapshot", "ethusdt@depth10@500ms"]
    assert btc[0]["data"] == SNAPSHOT and btc[0]["symbol"] == "BTCUSDT"
    assert [r["data"]["u"] for r in btc[1:]] == [100, 101]
    assert [r["data"]["u"] for r in eth[1:]] == [200]
    assert btc[1]["symbol"] == "BTCUSDT"
    assert isinstance(btc[1]["recv_ts"], int) and abs(btc[1]["recv_ts"] / 1000 - datetime.now(timezone.utc).timestamp()) < 30
    assert btc[0]["recv_ts"] <= btc[1]["recv_ts"]
    assert btc[1]["data"]["b"][0] == ["60000.10", "1.500"]  # строки биржи не искажены


@pytest.mark.parametrize("kwargs,exc_name", [
    ({"snapshot_status": 500}, "HttpServerError"),
    ({"snapshot_status": 400}, "HttpClientError"),
    ({"snapshot": {"lastUpdateId": 1}}, "DataError"),
    ({"snapshot": {"lastUpdateId": "x", "bids": [[1]], "asks": [[1]]}}, "DataError"),
    ({"snapshot": []}, "DataError"),
])
def test_snapshot_failure_means_socket_is_never_opened(sandbox, kwargs, exc_name):
    import utils.errors as errors

    async def script(ws):
        await ws.close()

    async def scenario():
        app, seen = ws_app(script, **kwargs)
        async with serve(app) as host:
            with pytest.raises(getattr(errors, exc_name)):
                await run_depth(host)
        return seen

    seen = asyncio.run(scenario())
    assert "ws" not in seen["events"]


def test_snapshot_429_raises_rate_limit_without_socket(sandbox):
    async def script(ws):
        await ws.close()

    async def scenario():
        app, seen = ws_app(script, snapshot_status=429, headers={"Retry-After": "30"})
        async with serve(app) as host:
            with pytest.raises(RateLimitError) as info:
                await run_depth(host)
        return info.value, seen

    err, seen = asyncio.run(scenario())
    assert err.retry_after == 30 and "ws" not in seen["events"]


def test_stalled_connection_fails(sandbox, monkeypatch):
    monkeypatch.setattr(config, "WS_STALL_TIMEOUT_S", 0.4)

    async def script(ws):
        await ws.send_str(json.dumps(depth_msg()))
        await asyncio.sleep(3)  # соединение открыто, данных нет

    async def scenario():
        app, _ = ws_app(script)
        async with serve(app) as host:
            with pytest.raises(StreamStalled):
                await run_depth(host)

    asyncio.run(scenario())
    assert len(read_lines(config.DEPTH_DIR, "BTCUSDT")) == 2  # снапшот + одно сообщение


@pytest.mark.parametrize("bad", [
    "not json",
    json.dumps([1, 2]),
    json.dumps({"stream": "unknown@depth10@500ms", "data": {}}),
    json.dumps({"stream": "btcusdt@depth10@500ms", "data": {"s": "BTCUSDT"}}),
    json.dumps({"stream": "btcusdt@depth10@500ms",
                "data": {**depth_msg()["data"], "s": "ETHUSDT"}}),
])
def test_unexpected_format_fails_and_nothing_bad_is_written(sandbox, bad):
    async def script(ws):
        await ws.send_str(bad)
        await asyncio.sleep(1)

    async def scenario():
        app, _ = ws_app(script)
        async with serve(app) as host:
            with pytest.raises(DataError):
                await run_depth(host)

    asyncio.run(scenario())
    lines = read_lines(config.DEPTH_DIR, "BTCUSDT")
    assert [r["stream"] for r in lines] == ["snapshot"]  # мусор из потока в файл не попал


def test_binary_frame_fails(sandbox):
    async def script(ws):
        await ws.send_bytes(b"\x00\x01")
        await asyncio.sleep(1)

    async def scenario():
        app, _ = ws_app(script)
        async with serve(app) as host:
            with pytest.raises(DataError):
                await run_depth(host)

    asyncio.run(scenario())


def test_server_shutdown_event(sandbox):
    async def script(ws):
        await ws.send_str(json.dumps({"e": "serverShutdown", "E": 1}))
        await asyncio.sleep(1)

    async def scenario():
        app, _ = ws_app(script)
        async with serve(app) as host:
            with pytest.raises(ServerShutdown):
                await run_depth(host)

    asyncio.run(scenario())


def test_handshake_429_is_rate_limit(sandbox):
    async def scenario():
        app, _ = ws_app(None, status=429, headers={"Retry-After": "42"})
        async with serve(app) as host:
            with pytest.raises(RateLimitError) as info:
                await run_depth(host)
        return info.value

    err = asyncio.run(scenario())
    assert err.retry_after == 42 and err.exit_code == 2


def test_connection_refused_is_network_error(sandbox):
    from utils.errors import NetworkError

    async def scenario():
        with pytest.raises(NetworkError):
            await run_depth("127.0.0.1:1")

    asyncio.run(scenario())


def test_each_line_is_flushed_immediately(sandbox):
    writer = HourlyJsonlWriter(config.DEPTH_DIR)
    ts = int(datetime(2026, 10, 3, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)
    writer.write("BTCUSDT", ts, '{"a":1}')
    path = config.DEPTH_DIR / "BTCUSDT" / "2026-10-03" / "14.jsonl"
    # читаем, пока файл ещё открыт для записи и не закрыт
    assert path.read_text(encoding="utf-8") == '{"a":1}\n'
    writer.close()


def test_hourly_rotation_utc(sandbox):
    writer = HourlyJsonlWriter(config.DEPTH_DIR)
    base = datetime(2026, 10, 3, 14, 59, 59, 900000, tzinfo=timezone.utc).timestamp()
    writer.write("BTCUSDT", int(base * 1000), '{"n":1}')
    writer.write("BTCUSDT", int(base * 1000) + 200, '{"n":2}')  # уже 15:00:00.1
    writer.close()
    day = config.DEPTH_DIR / "BTCUSDT" / "2026-10-03"
    assert (day / "14.jsonl").read_text() == '{"n":1}\n'
    assert (day / "15.jsonl").read_text() == '{"n":2}\n'


def test_truncated_tail_does_not_glue_with_new_record(sandbox):
    path = config.DEPTH_DIR / "BTCUSDT" / "2026-10-03" / "14.jsonl"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"ok":1}\n{"broken":')  # прошлый запуск упал посреди строки
    writer = HourlyJsonlWriter(config.DEPTH_DIR)
    ts = int(datetime(2026, 10, 3, 14, 5, tzinfo=timezone.utc).timestamp() * 1000)
    writer.write("BTCUSDT", ts, '{"ok":2}')
    writer.close()
    lines = path.read_text().splitlines()
    assert lines == ['{"ok":1}', '{"broken":', '{"ok":2}']
    good = []
    for line in lines:
        try:
            good.append(json.loads(line))
        except ValueError:
            pass
    assert good == [{"ok": 1}, {"ok": 2}]


@pytest.mark.parametrize("speed,levels,ok", [
    ("500ms", 10, True), ("250ms", 5, True), ("100ms", 20, True),
    ("1000ms", 10, False), ("500ms", 7, False),
])
def test_config_validation(monkeypatch, speed, levels, ok):
    from utils.errors import ConfigError
    monkeypatch.setattr(config, "DEPTH_UPDATE_SPEED", speed)
    monkeypatch.setattr(config, "DEPTH_LEVELS", levels)
    if ok:
        config.validate()
    else:
        with pytest.raises(ConfigError):
            config.validate()
