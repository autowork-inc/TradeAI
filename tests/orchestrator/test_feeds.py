"""SmcKlineFeed, TickFeed, ReconnectingStream: протокол на локальном сервере и логика буфера без сети."""

import asyncio
import json

import aiohttp
import polars as pl
import pytest
from aiohttp import web

import config
from orchestrator.feature_engine import FeatureEngine
from orchestrator.kline_feed import SmcKlineFeed
from orchestrator.tick_feed import TickFeed
from orchestrator.types import Tick
from orchestrator.ws import decode_combined
from tests.conftest import serve
from tests.features.test_smc import random_walk
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError, DataError, ServerShutdown, WebSocketClosed

IV = 900_000
SYM = "BTCUSDT"
CANDLES = random_walk(260, 5, 0.01)  # бары 0..259; open_time = i * IV
ENGINE = FeatureEngine(min_bars=100)


@pytest.fixture(autouse=True)
def no_weight_throttle(monkeypatch):
    monkeypatch.setattr(config, "WEIGHT_SOFT_LIMIT_1M", 10**9)


def run(coro):
    return asyncio.run(coro)


def raw_row(i: int) -> list:
    r = CANDLES.row(i, named=True)
    return [r["open_time"], str(r["open"]), str(r["high"]), str(r["low"]), str(r["close"]), str(r["volume"]),
            r["open_time"] + IV - 1, "1", 10, "0.5", "0.5", "0"]


def kline_msg(i: int, *, closed: bool = True, symbol: str = SYM, interval: str = "15m") -> dict:
    r = CANDLES.row(i, named=True)
    return {"e": "kline", "E": r["open_time"] + IV + 20, "s": symbol, "k": {
        "t": r["open_time"], "T": r["open_time"] + IV - 1, "s": symbol, "i": interval, "o": str(r["open"]), "c": str(r["close"]),
        "h": str(r["high"]), "l": str(r["low"]), "v": str(r["volume"]), "n": 10, "x": closed, "q": "1", "V": "0.5", "Q": "0.5"}}


class FakeApi:
    """REST без сети: N закрытых свечей (0..N-1), свеча N - формирующаяся."""

    def __init__(self, closed: int):
        self.closed = closed

    async def server_time_ms(self) -> int:
        return self.closed * IV + 5_000

    async def get(self, path, params, weight=1):
        assert path == config.KLINES_PATH and params["interval"] == "15m"
        hi = self.closed + 1  # + формирующаяся
        return [raw_row(i) for i in range(max(0, hi - int(params["limit"])), hi)]


def feed_without_net(closed: int = 200, clock=lambda: 123) -> tuple[SmcKlineFeed, FakeApi]:
    api = FakeApi(closed)
    return SmcKlineFeed(api, None, ENGINE, symbols=(SYM,), history_bars=120, clock_ms=clock), api  # type: ignore[arg-type]


# ----------------------------------------------------------------------------- буфер свечей без сети


def test_history_is_not_emitted_new_closed_bar_is_and_forming_or_duplicates_are_not():
    async def body():
        feed, _ = feed_without_net()
        await feed._bootstrap()
        assert await feed.poll() == []  # история на старте не эмитится
        assert feed.on_kline_message(SYM, kline_msg(200, closed=False), 1) is False  # формирующаяся
        assert await feed.poll() == []
        assert feed.on_kline_message(SYM, kline_msg(200), 77) is True
        (ctx,) = await feed.poll()
        assert ctx.bar_open_time == 200 * IV and ctx.frame.height == 120 and ctx.symbol == SYM and ctx.interval == "15m"
        assert ctx.frame["open_time"][-1] == 200 * IV and ctx.frame["open_time"][0] == 81 * IV
        assert "swing_high" in ctx.frame.columns and ctx.received_ms == 77
        feed.on_kline_message(SYM, kline_msg(200), 78)  # дубль закрытия
        assert await feed.poll() == []
        assert feed.stats()["last_emitted"][SYM] == 200 * IV

    run(body())


def test_exchange_clock_offset_is_used_for_received_time():
    async def body():
        feed, api = feed_without_net(clock=lambda: 1_000)
        await feed._bootstrap()
        assert feed.exchange_now_ms() == await api.server_time_ms()  # смещение = серверное время - локальное

    run(body())


def test_missed_bars_after_reconnect_are_replayed_in_order_with_now_as_received_time():
    async def body():
        feed, api = feed_without_net()
        await feed._bootstrap()
        feed.on_kline_message(SYM, kline_msg(200), 50)
        assert [c.bar_open_time for c in await feed.poll()] == [200 * IV]
        api.closed = 204  # за обрыв закрылись свечи 201..203 (REST при переподключении знает о них)
        await feed._bootstrap()
        ctxs = await feed.poll()
        assert [c.bar_open_time for c in ctxs] == [201 * IV, 202 * IV, 203 * IV]
        now = await api.server_time_ms()
        assert [c.received_ms for c in ctxs] == [now] * 3  # «сейчас»: оркестратор увидит их возраст
        assert await feed.poll() == []

    run(body())


def test_unclosable_gap_in_candles_is_an_error_not_a_silent_splice_r1():
    async def body():
        feed, _ = feed_without_net()
        await feed._bootstrap()
        feed.on_kline_message(SYM, kline_msg(202), 1)  # свеча 201 пропущена
        with pytest.raises(DataError, match="дыра"):
            await feed.poll()

    run(body())


def test_malformed_messages_raise_data_error():
    feed, _ = feed_without_net()
    with pytest.raises(DataError):
        feed.on_kline_message(SYM, kline_msg(200, interval="1m"), 1)
    with pytest.raises(DataError):
        feed.on_kline_message(SYM, kline_msg(200, symbol="ETHUSDT"), 1)
    bad = kline_msg(200)
    del bad["k"]["h"]
    with pytest.raises(DataError, match="закрытая свеча"):
        feed.on_kline_message(SYM, bad, 1)
    with pytest.raises(DataError):
        decode_combined("не json", {})
    with pytest.raises(DataError, match="Неожиданное"):
        decode_combined(json.dumps({"stream": "x", "data": {}}), {"y": 1})
    with pytest.raises(ServerShutdown):
        decode_combined(json.dumps({"stream": "y", "data": {"e": "serverShutdown"}}), {"y": 1})


def test_constructor_validation():
    with pytest.raises(ConfigError):
        SmcKlineFeed(None, None, ENGINE, interval="7m")
    with pytest.raises(ConfigError):
        SmcKlineFeed(None, None, ENGINE, history_bars=50)
    with pytest.raises(ConfigError):
        SmcKlineFeed(None, None, ENGINE).start()  # нет api/session


# ------------------------------------------------------------------------------------ локальный сервер


class FakeMarket:
    def __init__(self, closed_per_connection=(200, 204)):
        self.connections = 0
        self.queries: list[str] = []
        self.klines_calls: list[dict] = []
        self.closed_per_connection = closed_per_connection
        self.script = self._default
        self.app = web.Application()
        self.app.router.add_get("/market/stream", self._ws)
        self.app.router.add_get("/fapi/v1/time", self._time)
        self.app.router.add_get("/fapi/v1/klines", self._klines)

    @property
    def closed(self) -> int:
        return self.closed_per_connection[min(self.connections, len(self.closed_per_connection)) - 1]

    async def _ws(self, request):
        self.connections += 1
        self.queries.append(request.query_string)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await self.script(ws, self.connections)
        return ws

    @staticmethod
    async def _default(ws, n):
        await asyncio.sleep(1)

    async def _time(self, request):
        return web.json_response({"serverTime": self.closed * IV + 5_000})

    async def _klines(self, request):
        self.klines_calls.append(dict(request.query))
        hi = self.closed + 1
        return web.json_response([raw_row(i) for i in range(max(0, hi - int(request.query["limit"])), hi)])


async def settle(predicate, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def test_kline_stream_end_to_end_subscription_bootstrap_and_replay_after_disconnect():
    fake = FakeMarket()

    async def script(ws, n):
        if n == 1:
            await ws.send_str(json.dumps({"stream": "btcusdt@kline_15m", "data": kline_msg(200, closed=False)}))
            await ws.send_str(json.dumps({"stream": "btcusdt@kline_15m", "data": kline_msg(200)}))
            await asyncio.sleep(0.3)
            await ws.close()  # обрыв: за это время REST узнает о 201..203
        else:
            await asyncio.sleep(1)

    fake.script = script

    async def body():
        async with serve(fake.app) as host, aiohttp.ClientSession() as session:
            feed = SmcKlineFeed(BinanceRest(session, f"http://{host}"), session, ENGINE, symbols=(SYM,), history_bars=120,
                                clock_ms=lambda: 123, market_url=f"ws://{host}/market/stream", backoff_s=0.01, backoff_max_s=0.02)
            got: list = []
            try:
                feed.start()
                async with asyncio.timeout(10):
                    while len(got) < 4:
                        got.extend(await feed.poll())
                        await asyncio.sleep(0.02)
            finally:
                await feed.aclose()
            return got, feed.stats()

    got, stats = run(body())
    assert [c.bar_open_time for c in got] == [200 * IV, 201 * IV, 202 * IV, 203 * IV]  # WS-свеча, затем replay по порядку
    assert fake.queries[0] == "streams=btcusdt@kline_15m" and stats["reconnects"] == 1 and fake.connections == 2
    assert [c["limit"] for c in fake.klines_calls] == ["121", "121"] and fake.klines_calls[0]["interval"] == "15m"
    assert got[0].received_ms == 200 * IV + 5_000  # recv (123) + смещение часов биржи; у replay - «сейчас» (второе подключение)
    assert got[1].received_ms == 204 * IV + 5_000


def frame_msg(stream: str, data: dict) -> str:
    return json.dumps({"stream": stream, "data": data})


AGG = {"e": "aggTrade", "E": 1005, "a": 1, "s": SYM, "p": "100.50", "q": "0.1", "f": 1, "l": 1, "T": 1000, "m": False}
MARK = {"e": "markPriceUpdate", "E": 2000, "s": SYM, "p": "100.60", "P": "100.7", "i": "100.6", "r": "0.0001", "T": 3000}


def test_tick_feed_streams_parse_gap_callback_and_reconnect():
    fake = FakeMarket()

    async def script(ws, n):
        await ws.send_str(frame_msg("btcusdt@aggTrade", AGG))
        await ws.send_str(frame_msg("btcusdt@markPrice@1s", MARK))
        if n == 1:
            await asyncio.sleep(0.1)
            await ws.close()
        else:
            await asyncio.sleep(1)

    fake.script = script

    async def body():
        ticks, gaps = [], []
        async with serve(fake.app) as host, aiohttp.ClientSession() as session:
            feed = TickFeed(session, ticks.append, gaps.append, symbols=(SYM,), market_url=f"ws://{host}/market/stream",
                            backoff_s=0.01, backoff_max_s=0.02, clock_ms=lambda: 555)
            try:
                feed.start()
                await settle(lambda: len(ticks) >= 4)
                feed.check()  # поток жив
            finally:
                await feed.aclose()
        return ticks, gaps, feed.stats()

    ticks, gaps, stats = run(body())
    assert ticks[:2] == [Tick(SYM, 100.5, 1000, "trade", 0.1, False), Tick(SYM, 100.6, 2000, "mark")]  # время сделки T, у mark - E
    assert gaps == [555] and stats["reconnects"] == 1  # обрыв -> on_gap ровно раз
    assert fake.queries[0] == "streams=btcusdt@aggTrade/btcusdt@markPrice@1s"


def test_tick_feed_garbage_message_kills_the_feed_task_fail_fast():
    fake = FakeMarket()

    async def script(ws, n):
        await ws.send_str(frame_msg("btcusdt@aggTrade", {**AGG, "p": "не число"}))
        await asyncio.sleep(1)

    fake.script = script

    async def body():
        async with serve(fake.app) as host, aiohttp.ClientSession() as session:
            feed = TickFeed(session, lambda t: None, lambda ts: None, symbols=(SYM,), market_url=f"ws://{host}/market/stream")
            try:
                feed.start()
                await settle(lambda: feed._task.done())
                with pytest.raises(DataError, match="Неожиданный тик"):
                    feed.check()
            finally:
                await feed.aclose()

    run(body())


def test_reconnect_attempts_are_limited_then_the_process_must_fall_r1():
    fake = FakeMarket()

    async def script(ws, n):
        await ws.close()  # каждое подключение сразу обрывается без данных

    fake.script = script

    async def body():
        gaps = []
        async with serve(fake.app) as host, aiohttp.ClientSession() as session:
            feed = TickFeed(session, lambda t: None, gaps.append, symbols=(SYM,), market_url=f"ws://{host}/market/stream",
                            max_reconnects=2, backoff_s=0.01, backoff_max_s=0.02)
            try:
                feed.start()
                await settle(lambda: feed._task.done())
                with pytest.raises(WebSocketClosed, match="не восстановился"):
                    feed.check()
            finally:
                await feed.aclose()
        return len(gaps)

    assert run(body()) == 3  # первая попытка + 2 переподключения, каждая - с отметкой обрыва
