import asyncio
import json

import aiohttp
import pytest

import config
from backtester.contracts import Fill, Side
from execution.user_stream import (
    AccountUpdate,
    ListenKeyExpired,
    OrderUpdate,
    RawEvent,
    UserDataStream,
    parse_event,
)
from tests.execution.fake_exchange import FakeExchange
from tests.execution.helpers import limit, make_env
from utils.errors import DataError, RateLimitError, ServerShutdown, WebSocketClosed


def run(coro):
    return asyncio.run(coro)


async def settle(predicate, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("условие не наступило")
        await asyncio.sleep(0.01)


class Collector:
    def __init__(self):
        self.events = []

    def __call__(self, event):
        self.events.append(event)


def start(e, handler, **kw):
    kw.setdefault("ws_base_url", f"ws://{e.host}/private")
    kw.setdefault("backoff_s", 0.01)
    kw.setdefault("backoff_max_s", 0.02)
    return UserDataStream(e.rest, e.session, handler, **kw)


async def stop(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ------------------------------------------------------------------- разбор


def test_order_trade_update_is_parsed_into_typed_event():
    ex = FakeExchange()
    o = ex.seed_order("tb-x-1", qty=0.5, price=59_000.0)
    trade = ex._book_trade(o, 0.2, 59_000.0, maker=True)
    o["status"] = "PARTIALLY_FILLED"
    event = parse_event(json.dumps(ex.order_event(o, trade, "TRADE")))
    assert isinstance(event, OrderUpdate)
    assert (event.symbol, event.client_order_id, event.side, event.status, event.exec_type) == \
           ("BTCUSDT", "tb-x-1", Side.BUY, "PARTIALLY_FILLED", "TRADE")
    assert (event.last_qty, event.last_price, event.cum_qty, event.orig_qty) == (0.2, 59_000.0, 0.2, 0.5)
    assert event.is_maker and event.has_trade and event.trade_id == trade["id"]
    assert event.commission == pytest.approx(0.2 * 59_000 * 0.0002) and event.commission_asset == "USDT"


def test_non_trade_update_has_no_trade_and_no_commission():
    ex = FakeExchange()
    o = ex.seed_order("tb-x-2")
    event = parse_event(json.dumps(ex.order_event(o, None, "NEW")))
    assert not event.has_trade and event.commission is None and event.commission_asset is None


def test_account_update_is_parsed():
    event = parse_event(json.dumps(FakeExchange().account_event("ETHUSDT", -1.5, "FUNDING_FEE")))
    assert isinstance(event, AccountUpdate) and event.reason == "FUNDING_FEE"
    assert (event.positions[0].symbol, event.positions[0].amount) == ("ETHUSDT", -1.5)
    assert event.balances[0].asset == "USDT"


def test_stream_mode_envelope_is_unwrapped():
    inner = FakeExchange().account_event("BTCUSDT", 1.0)
    assert isinstance(parse_event(json.dumps({"stream": "x", "data": inner})), AccountUpdate)


def test_other_events_pass_through_and_lifecycle_events_raise():
    assert parse_event(json.dumps({"e": "MARGIN_CALL", "E": 1})) == RawEvent("MARGIN_CALL", {"e": "MARGIN_CALL", "E": 1})
    with pytest.raises(ListenKeyExpired):
        parse_event(json.dumps({"e": "listenKeyExpired", "E": 1}))
    with pytest.raises(ServerShutdown):
        parse_event(json.dumps({"e": "serverShutdown", "E": 1}))


@pytest.mark.parametrize("raw", [
    "not json", "[1, 2]", "{}", json.dumps({"e": "ORDER_TRADE_UPDATE", "E": 1}),
    json.dumps({"e": "ORDER_TRADE_UPDATE", "E": 1, "o": {"s": "BTCUSDT"}}),
    json.dumps({"e": "ACCOUNT_UPDATE", "E": 1}),
    json.dumps({"e": "ACCOUNT_UPDATE", "E": 1, "a": {"P": [{"s": "BTCUSDT", "pa": "x", "ep": "1"}]}}),
])
def test_malformed_events_raise_data_error(raw):
    with pytest.raises(DataError):
        parse_event(raw)


def test_bad_side_and_non_integer_ids_raise_data_error():
    ex = FakeExchange()
    o = ex.seed_order("tb-x-3")
    event = ex.order_event(o)
    event["o"]["S"] = "SIDEWAYS"
    with pytest.raises(DataError):
        parse_event(json.dumps(event))
    event = ex.order_event(o)
    event["o"]["i"] = "12"
    with pytest.raises(DataError):
        parse_event(json.dumps(event))


# ----------------------------------------------------------- поток и listenKey


def test_stream_connects_to_private_route_and_delivers_events(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            got = Collector()
            task = asyncio.create_task(start(e, got).run())
            await e.ex.wait_clients(1)
            o = e.ex.seed_order("tb-s-1", qty=0.5)
            await e.ex.push(e.ex.order_event(o, exec_type="NEW"))
            await e.ex.push(e.ex.account_event("BTCUSDT", 0.5))
            await settle(lambda: len(got.events) == 2)
            await stop(task)
            return e, got

    e, got = run(scenario())
    (q,) = e.ex.ws_requests
    assert q["listenKey"] == "lk1" and q["events"] == "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
    assert [type(x) for x in got.events] == [OrderUpdate, AccountUpdate]
    assert "POST /fapi/v1/listenKey" in e.ex.requests


def test_async_handler_is_awaited(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            seen = []

            async def handler(event):
                await asyncio.sleep(0)
                seen.append(event)

            task = asyncio.create_task(start(e, handler).run())
            await e.ex.wait_clients(1)
            await e.ex.push(e.ex.account_event("BTCUSDT", 1.0))
            await settle(lambda: seen)
            await stop(task)
            return seen

    assert len(run(scenario())) == 1


def test_listen_key_is_kept_alive_and_deleted_on_exit(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            task = asyncio.create_task(start(e, Collector(), keepalive_s=0.05, sleep=asyncio.sleep).run())
            await e.ex.wait_clients(1)
            await settle(lambda: e.ex.listen_key_log.count("PUT") >= 2)
            await stop(task)
            await settle(lambda: "DELETE" in e.ex.listen_key_log)
            return e

    e = run(scenario())
    assert e.ex.listen_key_log[0] == "POST" and e.ex.listen_key_log.count("PUT") >= 2
    assert e.ex.listen_keys == set()  # ключ закрыт при выходе


def test_keepalive_rejected_listen_key_forces_reconnect_with_new_key(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            task = asyncio.create_task(start(e, Collector(), keepalive_s=0.1).run())
            await e.ex.wait_clients(1)
            e.ex.listen_keys.discard("lk1")  # биржа забыла ключ
            await settle(lambda: len(e.ex.ws_requests) >= 2)
            await stop(task)
            return e

    assert run(scenario()).ex.ws_requests[1]["listenKey"] == "lk2"


# ---------------------------------------------------------- переподключения


def test_reconnects_after_server_close_and_calls_on_connected_each_time(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            got, connected = Collector(), []

            async def on_connected(reconnect):
                connected.append((reconnect, len(e.ex.ws_requests)))  # сокет уже открыт, когда идёт сверка

            task = asyncio.create_task(start(e, got, on_connected=on_connected).run())
            await e.ex.wait_clients(1)
            await e.ex.close_clients()  # обрыв (в т.ч. плановый на 24 часах)
            await settle(lambda: len(connected) == 2)
            await e.ex.wait_clients(1)
            await e.ex.push(e.ex.account_event("BTCUSDT", 1.0))
            await settle(lambda: len(got.events) == 1)
            await stop(task)
            return e, connected

    e, connected = run(scenario())
    assert connected == [(False, 1), (True, 2)]  # сверка идёт после (пере)подключения сокета, не до
    assert [q["listenKey"] for q in e.ex.ws_requests] == ["lk1", "lk2"]  # новый ключ на каждое подключение


def test_listen_key_expired_event_reconnects(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            task = asyncio.create_task(start(e, Collector()).run())
            await e.ex.wait_clients(1)
            await e.ex.push({"e": "listenKeyExpired", "E": 1})
            await settle(lambda: len(e.ex.ws_requests) >= 2)
            await stop(task)
            return e

    assert len(run(scenario()).ex.ws_requests) >= 2


def test_gives_up_after_max_reconnects_with_exponential_backoff(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            async def close_at_once(ws):
                await ws.close()

            e.ex.ws_scripts = [close_at_once] * 10
            delays = []

            async def record(seconds):
                delays.append(seconds)

            with pytest.raises(WebSocketClosed) as info:
                await start(e, Collector(), max_reconnects=3, backoff_s=0.5, backoff_max_s=1.5, sleep=record).run()
            return e, delays, info.value

    e, delays, err = run(scenario())
    assert delays == [0.5, 1.0, 1.5]  # удвоение до потолка
    assert "3 попыток" in str(err) and len(e.ex.ws_requests) == 4


def test_malformed_message_stops_stream_without_reconnect(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            task = asyncio.create_task(start(e, Collector()).run())
            await e.ex.wait_clients(1)
            for ws in e.ex.ws_clients:
                await ws.send_str("{broken")
            with pytest.raises(DataError):
                await asyncio.wait_for(task, 3)
            return e

    assert len(run(scenario()).ex.ws_requests) == 1  # мусор в торговом потоке не маскируется переподключением


def test_handler_error_propagates(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            def boom(event):
                raise RuntimeError("ошибка обработчика")

            task = asyncio.create_task(start(e, boom).run())
            await e.ex.wait_clients(1)
            await e.ex.push(e.ex.account_event("BTCUSDT", 1.0))
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(task, 3)

    run(scenario())


def test_rate_limit_on_listen_key_propagates_for_ban_lock(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            e.ex.fault("listen_key_post", "http429")
            with pytest.raises(RateLimitError) as info:
                await start(e, Collector()).run()
            return info.value

    assert run(scenario()).retry_after == 11


def test_connection_refused_is_retried_then_gives_up(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            with pytest.raises(WebSocketClosed):
                await start(e, Collector(), ws_base_url="ws://127.0.0.1:1/private", max_reconnects=2,
                            sleep=lambda s: asyncio.sleep(0)).run()

    run(scenario())


# ----------------------------------------------- поток + барьер: без опроса REST


def test_stream_drives_barrier_fills_without_polling(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            resyncs = []

            async def on_connected(reconnect):
                resyncs.append(reconnect)
                await e.barrier.resync_all()

            def handler(event):
                if isinstance(event, OrderUpdate):
                    e.barrier.on_order_update(event)
                elif isinstance(event, AccountUpdate):
                    e.barrier.on_account_update(event)

            task = asyncio.create_task(start(e, handler, on_connected=on_connected).run())
            await e.ex.wait_clients(1)
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            requests_before = len(e.ex.requests)
            await e.ex.fill(cid, 0.2)
            await e.ex.push(e.ex.account_event("BTCUSDT", 0.2))
            await e.ex.fill(cid, 0.3)
            await e.ex.push(e.ex.account_event("BTCUSDT", 0.5))
            await settle(lambda: e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {})
            requests_after = len(e.ex.requests)
            await stop(task)
            return e, resyncs, requests_after - requests_before

    e, resyncs, polled = run(scenario())
    assert polled == 0  # ни одного REST-запроса: всё пришло событиями
    assert resyncs == [False] and e.barrier.check_drift() == [] and not e.safe.active
    fills = [x for x in e.barrier.advance(10**15) if isinstance(x, Fill)]
    assert [f.qty for f in fills] == [0.2, 0.3]


def test_fill_during_outage_is_recovered_on_reconnect(tmp_path):
    async def scenario():
        async with make_env(tmp_path, timeout_s=None) as e:
            def handler(event):
                if isinstance(event, OrderUpdate):
                    e.barrier.on_order_update(event)

            async def on_connected(reconnect):
                await e.barrier.resync_all()

            task = asyncio.create_task(start(e, handler, on_connected=on_connected).run())
            await e.ex.wait_clients(1)
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            await e.ex.close_clients()
            await e.ex.fill(cid, 0.5, push=False)  # исполнилась, пока поток был оборван
            await settle(lambda: e.barrier.position("BTCUSDT") == 0.5)
            await stop(task)
            return e

    e = run(scenario())
    assert e.state.orders == {} and e.safe.active is False
