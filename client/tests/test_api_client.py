"""ApiClient и LiveStateStream против локального фейкового сервера (aiohttp.web) с теми же ответами, что у настоящего API."""

import asyncio

import aiohttp
import pytest
from aiohttp import web

from tradeai_client.api import ApiClient, ApiError, LiveStateStream
from tradeai_client.binance import BinanceError, BinanceNoData, fetch_klines, parse_klines
from tradeai_client.settings import ClientSettings

KEY = "k" * 40


class Fake:
    """Запоминает запросы; поведение настраивается полями."""

    def __init__(self, fx):
        self.fx, self.seen, self.ws_messages, self.ws_drops = fx, [], [], 0
        self.status_override: dict[str, int] = {}
        self.ws_auth = True

    async def guard(self, request: web.Request) -> web.Response | None:
        self.seen.append((request.path, list(request.query.items()), request.headers.get("X-API-Key")))
        if request.headers.get("X-API-Key") != KEY:
            return web.json_response({"detail": "invalid_api_key"}, status=401)
        forced = self.status_override.get(request.path)
        if forced:
            return web.json_response({"detail": "forced"}, status=forced)
        return None

    def routes(self) -> web.Application:
        app = web.Application()

        def json_route(name):
            async def handler(request):
                return await self.guard(request) or web.json_response(self.fx(name))
            return handler

        app.router.add_get("/api/info", json_route("info.json"))
        app.router.add_get("/api/stats", json_route("stats.json"))
        app.router.add_get("/api/trades", json_route("trades.json"))
        app.router.add_get("/api/trades/{tid}/snapshot", json_route("snapshot_smc_short.json"))

        async def html(request):
            return web.Response(text="<html>not the api</html>", content_type="text/html")

        app.router.add_get("/notapi/api/info", html)
        app.router.add_get("/ws/live_state", self.ws)
        return app

    async def ws(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("X-API-Key") != KEY or not self.ws_auth:
            raise web.HTTPForbidden()
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        if self.ws_drops > 0:
            self.ws_drops -= 1
            await ws.close()
            return ws
        for m in self.ws_messages:
            await ws.send_json(m)
        await asyncio.sleep(0.5)
        await ws.close()
        return ws


async def serve(fake: Fake):
    runner = web.AppRunner(fake.routes())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, ClientSettings(base_url=f"http://127.0.0.1:{port}", api_key=KEY)


def run(coro):
    return asyncio.run(coro)


def test_rest_calls_send_key_in_header_only_and_parse_json(fx):
    async def body():
        fake = Fake(fx)
        runner, s = await serve(fake)
        client = ApiClient(s)
        try:
            info = await client.info()
            stats = await client.stats(coins=["BTCUSDT", "ETHUSDT"], strategies=["smc"], sides=["short"], since="2026-10-01T00:00:00Z")
            trades = await client.trades(limit=10, offset=20, statuses=["open", "closed"], coins=["DOGEUSDT"])
            snap = await client.snapshot("smc:BTCUSDT:15m:1791606600000:short")
            return fake, info, stats, trades, snap
        finally:
            await client.close()
            await runner.cleanup()

    fake, info, stats, trades, snap = run(body())
    assert info["schema_version"] == 1 and stats["overall"]["trades"] > 0 and trades["items"] and snap["candles"]
    assert all(key == KEY for _, _, key in fake.seen)
    assert all(KEY not in str(query) for _, query, _ in fake.seen)  # ключ в адрес не попадает
    stats_q = fake.seen[1][1]
    assert ("coin", "BTCUSDT") in stats_q and ("coin", "ETHUSDT") in stats_q and ("side", "short") in stats_q and ("mode", "shadow") in stats_q
    trades_q = fake.seen[2][1]
    assert ("limit", "10") in trades_q and ("offset", "20") in trades_q and trades_q.count(("status", "open")) == 1 and ("status", "closed") in trades_q
    assert fake.seen[3][0] == "/api/trades/smc:BTCUSDT:15m:1791606600000:short/snapshot"


@pytest.mark.parametrize("status,kind", [(401, "auth"), (429, "throttled"), (404, "not_found"), (422, "bad_request"), (500, "server"), (503, "server")])
def test_http_errors_become_typed_api_errors(fx, status, kind):
    async def body():
        fake = Fake(fx)
        fake.status_override["/api/info"] = status
        runner, s = await serve(fake)
        client = ApiClient(s)
        try:
            with pytest.raises(ApiError) as exc:
                await client.info()
            return exc.value
        finally:
            await client.close()
            await runner.cleanup()

    e = run(body())
    assert e.kind == kind and e.status == status and str(e)


def test_wrong_key_is_reported_as_auth_error_with_a_hint(fx):
    async def body():
        runner, s = await serve(Fake(fx))
        s.api_key = "wrong" * 10
        client = ApiClient(s)
        try:
            with pytest.raises(ApiError) as exc:
                await client.stats()
            return exc.value
        finally:
            await client.close()
            await runner.cleanup()

    e = run(body())
    assert e.kind == "auth" and "shadow.sh apikey" in str(e)


def test_unreachable_server_and_non_json_answer(fx):
    async def body():
        client = ApiClient(ClientSettings(base_url="http://127.0.0.1:1", api_key=KEY), timeout_s=2)
        try:
            with pytest.raises(ApiError) as exc:
                await client.info()
            net = exc.value
        finally:
            await client.close()
        runner, s = await serve(Fake(fx))
        s.base_url += "/notapi"
        client = ApiClient(s)
        try:
            with pytest.raises(ApiError) as exc:
                await client.info()
            return net, exc.value
        finally:
            await client.close()
            await runner.cleanup()

    net, proto = run(body())
    assert net.kind == "network" and "SSH-туннель" in str(net) and proto.kind == "protocol"


def test_live_stream_delivers_messages_and_reports_status(fx):
    async def body():
        fake = Fake(fx)
        fake.ws_messages = [fx("live_state.json"), {"type": "ping", "server_time": "x"}]
        runner, s = await serve(fake)
        got, statuses = [], []
        stream = LiveStateStream(s, got.append, lambda k, t: statuses.append(k))
        task = asyncio.create_task(stream.run())
        for _ in range(50):
            if len(got) >= 2:
                break
            await asyncio.sleep(0.05)
        await stream.stop()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
        return got, statuses

    got, statuses = run(body())
    assert got[0]["type"] == "state" and got[0]["coins"] and got[1]["type"] == "ping"
    assert statuses[:2] == ["connecting", "connected"]


def test_live_stream_reconnects_after_a_drop(fx, monkeypatch):
    monkeypatch.setattr("tradeai_client.api.RECONNECT_DELAYS", (0.05,))

    async def body():
        fake = Fake(fx)
        fake.ws_drops, fake.ws_messages = 1, [fx("live_state.json")]
        runner, s = await serve(fake)
        got, statuses = [], []
        stream = LiveStateStream(s, got.append, lambda k, t: statuses.append(k))
        task = asyncio.create_task(stream.run())
        for _ in range(80):
            if got:
                break
            await asyncio.sleep(0.05)
        await stream.stop()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
        return got, statuses

    got, statuses = run(body())
    assert got and "reconnecting" in statuses and statuses.count("connected") >= 2


def test_live_stream_stops_on_rejected_key_instead_of_hammering_the_server(fx):
    async def body():
        runner, s = await serve(Fake(fx))
        s.api_key = "bad" * 20
        statuses = []
        stream = LiveStateStream(s, lambda m: None, lambda k, t: statuses.append(k))
        await asyncio.wait_for(stream.run(), 5)  # завершается сам, без повторов
        await runner.cleanup()
        return statuses

    assert run(body()) == ["connecting", "auth"]


# ------------------------------------------------------------------------------------------ Binance


def test_parse_klines_drops_the_unfinished_candle():
    rows = [[0, "1", "2", "0.5", "1.5", "10", 0], [900_000, "1.5", "2", "1", "1.8", "11", 0], [1_800_000, "1.8", "2", "1", "1.9", "12", 0]]
    out = parse_klines(rows, now_ms=1_800_000 + 60_000, interval_ms=900_000)
    assert [k["t"] for k in out] == [0, 900_000] and out[0] == {"t": 0, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0}
    with pytest.raises(BinanceError):
        parse_klines({"code": -1}, now_ms=1, interval_ms=1)
    with pytest.raises(BinanceError):
        parse_klines([[0, "x", "2", "0.5", "1.5", "10"]], now_ms=10**12, interval_ms=1)


def test_fetch_klines_maps_http_errors_and_network_failures():
    async def body():
        async def ok(request):
            assert request.query["symbol"] == "BTCUSDT" and request.query["interval"] == "15m" and request.query["limit"] == "1500"
            return web.json_response([[0, "1", "2", "0.5", "1.5", "10", 0]])

        async def blocked(request):
            return web.json_response({"msg": "restricted"}, status=451)

        app = web.Application()
        app.router.add_get("/fapi/v1/klines", ok)
        app2 = web.Application()
        app2.router.add_get("/fapi/v1/klines", blocked)
        res = []
        for application in (app, app2):
            runner = web.AppRunner(application)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
            async with aiohttp.ClientSession() as session:
                try:
                    res.append(await fetch_klines(session, base, "BTCUSDT", "15m", 0, 10**9, interval_ms=900_000, now_ms=10**12))
                except BinanceError as exc:
                    res.append(exc)
            await runner.cleanup()
        async with aiohttp.ClientSession() as session:
            try:
                await fetch_klines(session, "http://127.0.0.1:1", "BTCUSDT", "15m", 0, 1, interval_ms=1, now_ms=10**12, timeout_s=2)
            except BinanceError as exc:
                res.append(exc)
        return res

    ok, blocked, down = run(body())
    assert ok[0]["c"] == 1.5 and "451" in str(blocked) and "блокировка региона" in str(blocked) and "недоступен" in str(down)


@pytest.mark.parametrize("status", [400, 404])
def test_fetch_klines_400_404_is_no_data_not_an_error(status):
    """Символа нет на Binance Futures (Forex/металлы из MT5): отдельное исключение, интерфейс покажет снимок из БД без ошибки."""
    async def body():
        async def gone(request):
            return web.json_response({"code": -1121, "msg": "Invalid symbol."}, status=status)

        app = web.Application()
        app.router.add_get("/fapi/v1/klines", gone)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        try:
            async with aiohttp.ClientSession() as session:
                await fetch_klines(session, base, "EURUSD", "15m", 0, 10**9, interval_ms=900_000, now_ms=10**12)
        finally:
            await runner.cleanup()

    with pytest.raises(BinanceNoData):
        run(body())
