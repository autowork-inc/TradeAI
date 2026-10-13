import asyncio

import aiohttp
import pytest
from aiohttp import web

import config
from execution.live_feed import KlineObiFeed, order_book_imbalance, parse_closed_klines
from tests.conftest import serve
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError, DataError

MIN = 60_000
T0 = 1_700_000_040_000  # кратно минуте


def kline(i, close):
    open_time = T0 + i * MIN
    return [open_time, "1", "1", "1", str(close), "1", open_time + MIN - 1, "1", 1, "1", "1", "0"]


class FakeMarket:
    def __init__(self, closes, book=None):
        self.klines = [kline(i, c) for i, c in enumerate(closes)]
        self.book = book or {"bids": [["59999", "3"], ["59998", "1"]], "asks": [["60001", "1"]]}
        self.kline_calls = 0
        self.depth_calls = 0
        self.app = web.Application()
        self.app.router.add_get("/fapi/v1/klines", self._klines)
        self.app.router.add_get("/fapi/v1/depth", self._depth)

    async def _klines(self, request):
        self.kline_calls += 1
        limit = int(request.query["limit"])
        return web.json_response(self.klines[-limit:])

    async def _depth(self, request):
        self.depth_calls += 1
        return web.json_response(self.book)


def run(coro):
    return asyncio.run(coro)


async def make_feed(market, host, **kw):
    session = aiohttp.ClientSession()
    kw.setdefault("fast", 2)
    kw.setdefault("slow", 4)
    kw.setdefault("delay_s", 0.0)
    return session, KlineObiFeed(BinanceRest(session, f"http://{host}"), ("BTCUSDT",), **kw)


def test_row_has_features_of_last_closed_candle():
    async def go():
        market = FakeMarket([10, 20, 30, 40, 50, 60])
        async with serve(market.app) as host:
            session, feed = await make_feed(market, host)
            async with session:
                # now: свеча №5 закрыта, №6 (несуществующая) - нет
                return await feed.poll(T0 + 6 * MIN + 5)

    (row,) = run(go())
    assert row["symbol"] == "BTCUSDT" and row["close"] == 60.0
    assert row["asof_ts"] == T0 + 6 * MIN
    assert row["ma_fast"] == pytest.approx((50 + 60) / 2) and row["ma_slow"] == pytest.approx((30 + 40 + 50 + 60) / 4)
    assert row["obi"] == pytest.approx((4 - 1) / 5)


def test_unclosed_candle_is_not_used():
    async def go():
        market = FakeMarket([10, 20, 30, 40, 50, 60])
        async with serve(market.app) as host:
            session, feed = await make_feed(market, host)
            async with session:
                return await feed.poll(T0 + 6 * MIN - 1)  # свеча №5 закроется только в T0+6*MIN-1 (включительно)

    (row,) = run(go())
    assert row["close"] == 50.0  # последняя СТРОГО закрытая


def test_requests_only_when_new_candle_is_due():
    async def go():
        market = FakeMarket([10, 20, 30, 40, 50, 60])
        async with serve(market.app) as host:
            session, feed = await make_feed(market, host)
            async with session:
                now = T0 + 6 * MIN + 5
                first = await feed.poll(now)
                again = await feed.poll(now + 1_000)  # та же свеча, новой ещё нет
                calls_after_idle = (market.kline_calls, market.depth_calls)
                market.klines.append(kline(6, 70))
                late = await feed.poll(now + 2 * MIN)
                return first, again, calls_after_idle, late, (market.kline_calls, market.depth_calls)

    first, again, idle, late, calls = run(go())
    assert len(first) == 1 and again == [] and idle == (1, 1)
    assert late[0]["close"] == 70.0 and calls == (2, 2)


def test_warmup_gives_no_decision():
    async def go():
        market = FakeMarket([10, 20, 30])  # меньше slow=4
        async with serve(market.app) as host:
            session, feed = await make_feed(market, host)
            async with session:
                return await feed.poll(T0 + 10 * MIN)

    assert run(go()) == []


def test_empty_book_gives_null_obi_not_a_crash():
    assert order_book_imbalance({"bids": [], "asks": []}, 10) is None


@pytest.mark.parametrize("raw", [[], {}, {"bids": [["x", "y"]], "asks": []}, {"bids": [["1"]], "asks": []}, None])
def test_garbage_book_is_data_error(raw):
    with pytest.raises(DataError):
        order_book_imbalance(raw, 10)


@pytest.mark.parametrize("raw", [{}, None, [[1, 2]], [[T0, "1", "1", "1", "x", "1", T0 + 1]], [[T0, "1", "1", "1", "-5", "1", T0 + 1]]])
def test_garbage_klines_are_data_error(raw):
    with pytest.raises(DataError):
        parse_closed_klines(raw, T0 + 10 * MIN)


def test_non_monotonic_klines_are_data_error():
    with pytest.raises(DataError):
        parse_closed_klines([kline(2, 1), kline(1, 1)], T0 + 10 * MIN)


def test_bad_parameters_are_config_errors():
    api = BinanceRest(None, "http://x")
    for kw in ({"fast": 5, "slow": 5}, {"fast": 0, "slow": 5}, {"interval": "7m"}):
        with pytest.raises(ConfigError):
            KlineObiFeed(api, ("BTCUSDT",), **kw)
    assert config.LIVE_MA_FAST < config.LIVE_MA_SLOW
