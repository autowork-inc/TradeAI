import asyncio
import json
import re

import pytest

from backtester.contracts import CloseReason, Fill, Liquidity, OrderClosed, Side
from execution.live_barrier import CLIENT_ORDER_ID_RE, fmt_decimal, new_client_order_id
from execution.rest import BinanceAPIError, Credentials, SignedRest
from execution.state import LocalState, StateStore
from execution.user_stream import OrderUpdate
from tests.execution.fake_exchange import API_KEY
from tests.execution.helpers import ASOF, EXEC_CFG, limit, make_env, market
from utils.errors import OrderStatusUnknown, RateLimitError, SafeModeActive

FAR = 10**15  # события имеют серверное время «сейчас», а не ASOF решения


def run(coro):
    return asyncio.run(coro)


def update(ex, cid, *, trade=None, exec_type="TRADE"):
    """OrderUpdate так, как его разобрал бы user_stream из события биржи."""
    from execution.user_stream import parse_order_update
    return parse_order_update(ex.order_event(ex.order(cid), trade, exec_type))


# ------------------------------------------------------------- clientOrderId, формат чисел


def test_client_order_id_format_and_uniqueness():
    ids = {new_client_order_id() for _ in range(5000)}
    assert len(ids) == 5000
    for cid in list(ids)[:200]:
        assert CLIENT_ORDER_ID_RE.match(cid) and len(cid) <= 36 and cid.startswith("tb-")
    # время в id: base36 от мс
    assert int(new_client_order_id(1_700_000_000_000).split("-")[1], 36) == 1_700_000_000_000


@pytest.mark.parametrize("value,text", [
    (0.5, "0.5"), (1e-05, "0.00001"), (60000.0, "60000"), (0.1 + 0.2, "0.30000000000000004"), (123456789.0, "123456789"),
])
def test_fmt_decimal_has_no_exponent_or_trailing_zeros(value, text):
    assert fmt_decimal(value) == text


# ----------------------------------------------------------------- базовый путь


def test_market_order_is_signed_and_fill_is_booked_with_real_fee(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            ticket = await e.barrier.submit(market(Side.BUY, 0.5))
            events = e.barrier.advance(FAR)
            return e, ticket, events

    e, ticket, events = run(scenario())
    assert len(e.ex.post_log) == 1 and len(e.ex.orders) == 1  # подпись принята биржей (иначе -1022)
    p = e.ex.post_log[0]
    assert (p["type"], p["side"], p["quantity"], p["newOrderRespType"]) == ("MARKET", "BUY", "0.5", "RESULT")
    assert CLIENT_ORDER_ID_RE.match(p["newClientOrderId"]) and "reduceOnly" not in p
    assert ticket.exec_ts == ASOF + EXEC_CFG.order_latency_ms
    fills = [x for x in events if isinstance(x, Fill)]
    assert len(fills) == 1
    assert (fills[0].qty, fills[0].price, fills[0].liquidity) == (0.5, 60_000.0, Liquidity.TAKER)
    assert fills[0].fee == pytest.approx(0.5 * 60_000 * 0.0005)  # комиссия биржи в USDT, не расчётная
    assert e.barrier.position("BTCUSDT") == 0.5 and e.barrier.open_ticket_ids() == () and e.state.orders == {}
    assert json.loads(e.store.path.read_text())["positions"] == {"BTCUSDT": 0.5}  # состояние на диске


def test_state_is_written_before_the_post(tmp_path):
    seen = {}

    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.hooks["pre_order"] = lambda p: seen.update(
                disk=json.loads(e.store.path.read_text()), cid=p["newClientOrderId"])
            await e.barrier.submit(market())

    run(scenario())
    order = seen["disk"]["orders"][0]  # на момент POST ордер уже на диске, статус PENDING
    assert order["client_order_id"] == seen["cid"] and order["status"] == "PENDING"


def test_advance_shows_only_events_strictly_before_now(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(market())
            ts = e.ex.trades[0]["time"]
            return e.barrier.advance(ts), e.barrier.advance(ts + 1)

    early, later = run(scenario())
    assert early == () and len(later) == 1  # событие в ту же мс, что now_ts, ещё не видно


# ------------------------------------------------------------ ИДЕМПОТЕНТНОСТЬ


def test_lost_response_after_execution_does_not_create_second_order(tmp_path):
    """Биржа исполнила MARKET, ответ потерян. Слепой повтор удвоил бы позицию: id свободен после FILLED."""
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.fault("order_post", "lost_response")
            ticket = await e.barrier.submit(market(Side.BUY, 0.5))
            return e, ticket, e.barrier.advance(FAR)

    e, ticket, events = run(scenario())
    assert len(e.ex.post_log) == 1  # повтора не было: ордер найден по clientOrderId
    assert len(e.ex.orders) == 1 and e.ex.positions["BTCUSDT"] == 0.5
    assert e.barrier.position("BTCUSDT") == 0.5  # ровно одно исполнение, не 1.0
    assert [type(x) for x in events] == [Fill] and not e.safe.active


def test_exchange_model_frees_client_order_id_after_fill():
    """Контрольный: имитация воспроизводит ловушку Binance (id уникален лишь среди открытых), от которой
    защищают тесты выше. Без этого теста «нет второго ордера» могло бы быть заслугой самой имитации."""
    from tests.execution.fake_exchange import FakeExchange
    ex = FakeExchange()
    p = {"newClientOrderId": "x", "symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.5"}
    ex._process_order(p)
    ex._process_order(p)
    assert len(ex.orders) == 2 and ex.positions["BTCUSDT"] == 1.0


@pytest.mark.parametrize("fault", ["http500_after", "slow_after"])
def test_unknown_outcome_with_processed_order_is_adopted(tmp_path, fault):
    """5xx -1007 после исполнения и таймаут клиента при медленном ответе - тот же исход: ордер найден, повтора нет."""
    async def scenario():
        async with make_env(tmp_path, timeout_s=0.3) as e:
            e.ex.fault("order_post", fault)
            await e.barrier.submit(market(Side.SELL, 0.25))
            return e

    e = run(scenario())
    assert len(e.ex.post_log) == 1 and len(e.ex.orders) == 1
    assert e.barrier.position("BTCUSDT") == -0.25


@pytest.mark.parametrize("fault", ["drop_before", "http500_before"])
def test_unknown_outcome_with_unprocessed_order_resends_same_client_order_id(tmp_path, fault):
    async def scenario():
        async with make_env(tmp_path, recv_window_ms=1000) as e:
            e.ex.fault("order_post", fault)
            await e.barrier.submit(market(Side.BUY, 0.5))
            return e

    e = run(scenario())
    assert len(e.ex.post_log) == 2
    assert len({p["newClientOrderId"] for p in e.ex.post_log}) == 1  # id тот же
    assert len(e.ex.orders) == 1 and e.barrier.position("BTCUSDT") == 0.5
    # перед повтором выдержано ожидание, пока исходный запрос протух: timestamp + recvWindow(1000 мс) + запас
    assert max(e.sleeps) >= 1.0


def test_unresolvable_outcome_trips_safe_mode_and_keeps_pending_order(tmp_path):
    async def scenario():
        async with make_env(tmp_path, attempts=2) as e:
            e.ex.always["order_post"] = "drop_before"
            e.ex.always["order_get"] = "drop"  # и спросить биржу не получается
            with pytest.raises(OrderStatusUnknown):
                await e.barrier.submit(market())
            with pytest.raises(SafeModeActive):
                await e.barrier.submit(market(asof=ASOF + 1))
            return e

    e = run(scenario())
    assert e.safe.active and len(e.alerter.alerts) == 1 and "неизвестен" in e.alerter.alerts[0][0]
    assert len(e.ex.orders) == 0
    assert len({p["newClientOrderId"] for p in e.ex.post_log}) == 1  # новых id не плодилось
    disk = json.loads(e.store.path.read_text())
    assert [o["status"] for o in disk["orders"]] == ["PENDING"]  # reconcile разберёт по clientOrderId


def test_unknown_outcome_does_not_resend_when_lookup_fails_even_if_attempts_remain(tmp_path):
    async def scenario():
        async with make_env(tmp_path, attempts=5) as e:
            e.ex.always["order_post"] = "lost_response"  # каждый POST создавал бы ордер, но id занят (MARKET уже FILLED?)
            e.ex.always["order_get"] = "drop"
            with pytest.raises(OrderStatusUnknown):
                await e.barrier.submit(market())
            return e

    e = run(scenario())
    assert len(e.ex.post_log) == 1  # не зная исхода, второй POST не отправляется


def test_definitive_rejection_is_not_retried(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            ticket = await e.barrier.submit(market(Side.BUY, 0.0001))  # ниже LOT_SIZE: -1013
            return e, ticket, e.barrier.advance(FAR)

    e, ticket, events = run(scenario())
    assert len(e.ex.post_log) == 1
    assert len(events) == 1 and isinstance(events[0], OrderClosed)
    assert (events[0].reason, events[0].remaining_qty, events[0].ticket_id) == (CloseReason.REJECTED, 0.0001, ticket.ticket_id)
    assert e.state.orders == {} and not e.safe.active


def test_rate_limit_on_post_propagates_and_order_is_forgotten(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.fault("order_post", "http429")
            with pytest.raises(RateLimitError) as info:
                await e.barrier.submit(market())
            return e, info.value

    e, err = run(scenario())
    assert err.retry_after == 30 and len(e.ex.post_log) == 1
    assert e.state.orders == {} and e.barrier.open_ticket_ids() == () and not e.safe.active


def test_rate_limit_while_resolving_unknown_outcome_is_safe_mode(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.fault("order_post", "lost_response")
            e.ex.fault("order_get", "http429")
            with pytest.raises(RateLimitError):
                await e.barrier.submit(market())
            return e

    e = run(scenario())
    assert e.safe.active  # ордер мог исполниться: забывать его нельзя
    assert len(e.state.orders) == 1


def test_duplicate_id_response_adopts_existing_order(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            def preexisting(p):
                e.ex.seed_order(p["newClientOrderId"], qty=0.5, price=59_000.0)
            e.ex.hooks["pre_order"] = preexisting  # такой id уже открыт на бирже -> -4116
            await e.barrier.submit(limit())
            return e

    e = run(scenario())
    assert len(e.ex.orders) == 1 and e.barrier.open_ticket_ids() != () and not e.safe.active


def test_duplicate_stream_and_rest_deliveries_book_trade_once(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            trade = await e.ex.fill(cid, 0.5, push=False)
            upd = update(e.ex, cid, trade=trade)
            for _ in range(3):  # поток прислал одно и то же событие трижды
                e.barrier.on_order_update(upd)
            await e.barrier.resync_all()  # сверка тоже видит эту сделку
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.5
    assert [type(x) for x in e.barrier.advance(FAR)] == [Fill]
    assert e.safe.active is False  # запоздалые дубли закрытого ордера - не «чужое исполнение»


def test_duplicate_delivery_of_partial_fill_on_open_order_books_once(tmp_path):
    """Ордер остаётся открытым: дубль доходит до _book_trade и должен быть отброшен именно там."""
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            trade = await e.ex.fill(cid, 0.2, push=False)
            upd = update(e.ex, cid, trade=trade)
            for _ in range(3):
                e.barrier.on_order_update(upd)
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.2
    assert [x.qty for x in e.barrier.advance(FAR)] == [0.2] and not e.safe.active
    assert e.state.orders and next(iter(e.state.orders.values())).filled_qty == 0.2


def test_rest_replay_of_already_streamed_trade_is_not_double_counted(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            t1 = await e.ex.fill(cid, 0.2, push=False)
            e.barrier.on_order_update(update(e.ex, cid, trade=t1))  # A дошла потоком
            t2 = await e.ex.fill(cid, 0.3, push=False)  # B потеряна: userTrades вернёт обе, A повторно
            e.barrier.on_order_update(update(e.ex, cid, trade=t2, exec_type="CALCULATED"))  # без сделки, но cum=0.5
            await e.barrier.resync_pending()
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {}
    assert sorted(x.qty for x in e.barrier.advance(FAR)) == [0.2, 0.3]


# ------------------------------------------------------- LIMIT: частичные, срок жизни


def test_limit_partial_fills_via_stream_then_complete(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            assert e.barrier.position("BTCUSDT") == 0.0 and e.state.orders[cid].status == "NEW"
            for qty in (0.2, 0.3):
                t = await e.ex.fill(cid, qty, push=False)
                e.barrier.on_order_update(update(e.ex, cid, trade=t))
                if qty == 0.2:
                    assert e.state.orders[cid].status == "PARTIALLY_FILLED" and e.barrier.position("BTCUSDT") == 0.2
            return e

    e = run(scenario())
    events = e.barrier.advance(FAR)
    assert [x.qty for x in events] == [0.2, 0.3] and all(x.liquidity is Liquidity.MAKER for x in events)
    assert e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {}


def test_missed_stream_events_are_recovered_from_user_trades_without_double_count(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            await e.ex.fill(cid, 0.2, push=False)  # это событие потеряно
            t2 = await e.ex.fill(cid, 0.3, push=False)
            e.barrier.on_order_update(update(e.ex, cid, trade=t2))  # пришло только второе, cum_qty = 0.5
            assert e.barrier.position("BTCUSDT") == 0.3  # пока неполно
            await e.barrier.resync_pending()
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {}
    assert sorted(x.qty for x in e.barrier.advance(FAR)) == [0.2, 0.3]


def test_limit_ttl_cancel_closes_with_remaining(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            t = await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            cid = e.ex.orders[0]["cid"]
            trade = await e.ex.fill(cid, 0.2, push=False)
            e.barrier.on_order_update(update(e.ex, cid, trade=trade))
            await e.barrier.expire_due(t.expire_ts - 1)  # срок ещё не вышел
            assert e.ex.order(cid)["status"] == "PARTIALLY_FILLED"
            await e.barrier.expire_due(t.expire_ts)
            return e, t

    e, t = run(scenario())
    assert e.ex.order(e.ex.orders[0]["cid"])["status"] == "CANCELED"
    closed = [x for x in e.barrier.advance(FAR) if isinstance(x, OrderClosed)]
    assert len(closed) == 1 and closed[0].reason is CloseReason.EXPIRED
    assert closed[0].remaining_qty == pytest.approx(0.3) and e.barrier.position("BTCUSDT") == 0.2
    assert e.barrier.open_ticket_ids() == ()


def test_cancel_of_already_filled_order_learns_the_fill(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            t = await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            await e.ex.fill(e.ex.orders[0]["cid"], 0.5, push=False)  # исполнился, поток молчит
            await e.barrier.cancel(t.ticket_id)  # DELETE -> -2011 -> сверка
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {}
    assert [type(x) for x in e.barrier.advance(FAR)] == [Fill]


# ----------------------------------------- Этап 8.2: post-only, TTL-отмена, ротация clientOrderId


def test_limit_is_sent_post_only_gtx_by_default_and_gtc_when_disabled(tmp_path, monkeypatch):
    async def scenario(post_only_cfg, root):
        monkeypatch.setattr("config.LIVE_POST_ONLY", post_only_cfg)
        async with make_env(root) as e:
            await e.barrier.submit(limit(qty=0.5, price=59_000.0))
            return e.ex.post_log[0]

    (tmp_path / "on").mkdir()
    (tmp_path / "off").mkdir()
    gtx, gtc = run(scenario(True, tmp_path / "on")), run(scenario(False, tmp_path / "off"))
    assert (gtx["type"], gtx["timeInForce"], gtx["price"]) == ("LIMIT", "GTX", "59000")
    assert gtc["timeInForce"] == "GTC"


def test_post_only_that_would_cross_the_book_closes_as_not_resting_not_rejected(tmp_path):
    """-5022: цена ушла за время пути. Это «не встал» (EXPIRED) - повод перевыставить, а не отказ биржи."""
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.book = (59_999.0, 60_000.0)
            ticket = await e.barrier.submit(limit(Side.BUY, qty=0.5, price=60_000.0))  # по ask: стал бы тейкером
            return e, ticket

    e, ticket = run(scenario())
    closed = [x for x in e.barrier.advance(FAR) if isinstance(x, OrderClosed)]
    assert len(closed) == 1 and closed[0].reason is CloseReason.EXPIRED and closed[0].remaining_qty == 0.5
    assert e.barrier.position("BTCUSDT") == 0.0 and e.state.orders == {} and e.barrier.open_ticket_ids() == ()
    assert not e.safe.active and e.ex.orders == []  # на бирже ордера нет, Safe Mode не включён


def test_other_exchange_rejections_stay_rejected(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.0005))  # LOT_SIZE (-1013)
            return e

    closed = [x for x in run(scenario()).barrier.advance(FAR) if isinstance(x, OrderClosed)]
    assert [c.reason for c in closed] == [CloseReason.REJECTED]


def test_listener_gets_events_immediately_without_advance_and_its_failure_is_harmless(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            seen = []
            e.barrier.add_listener(seen.append)

            def broken(event):
                raise RuntimeError("слушатель упал")

            e.barrier.add_listener(broken)
            t = await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            cid = e.ex.orders[0]["cid"]
            trade = await e.ex.fill(cid, 0.2, push=False)
            e.barrier.on_order_update(update(e.ex, cid, trade=trade))
            assert [type(x) for x in seen] == [Fill]  # advance() ещё не вызывался
            await e.barrier.cancel(t.ticket_id)
            return e, seen

    e, seen = run(scenario())
    assert [type(x) for x in seen] == [Fill, OrderClosed] and seen[1].remaining_qty == pytest.approx(0.3)
    assert e.barrier.position("BTCUSDT") == 0.2  # учёт исполнений не пострадал от упавшего слушателя


def test_ttl_expiry_cancels_via_delete_and_a_resubmit_gets_a_fresh_client_order_id(tmp_path):
    """Чейзинг: отмена по TTL (DELETE /fapi/v1/order), затем новая лимитка. clientOrderId не переиспользуется."""
    async def scenario():
        async with make_env(tmp_path) as e:
            first = await e.barrier.submit(limit(Side.BUY, qty=0.5, price=59_000.0, ttl_ms=100))
            cid1 = e.ex.orders[0]["cid"]
            await e.barrier.expire_due(first.expire_ts - 1)
            assert "DELETE /fapi/v1/order" not in e.ex.requests  # срок ещё не вышел
            await e.barrier.expire_due(first.expire_ts)
            assert e.ex.requests.count("DELETE /fapi/v1/order") == 1 and e.ex.order(cid1)["status"] == "CANCELED"
            assert e.barrier.open_ticket_ids() == ()  # старый ордер закрыт ДО перевыставления
            second = await e.barrier.submit(limit(Side.BUY, qty=0.5, price=59_100.0, ttl_ms=100))
            third = await e.barrier.submit(limit(Side.BUY, qty=0.5, price=59_200.0, ttl_ms=100))
            return e, (first, second, third)

    e, tickets = run(scenario())
    cids = [p["newClientOrderId"] for p in e.ex.post_log]
    assert len(cids) == len(set(cids)) == 3 and all(CLIENT_ORDER_ID_RE.match(c) and len(c) <= 36 for c in cids)
    assert len({t.ticket_id for t in tickets}) == 3  # у каждого перевыставления свой тикет
    assert [e.ex.order(c)["status"] for c in cids] == ["CANCELED", "NEW", "NEW"]
    assert set(e.state.orders) == set(cids[1:])  # в локальном состоянии только живые


def test_unknown_outcome_retry_keeps_the_same_id_but_a_repost_does_not(tmp_path):
    """Повтор ОДНОГО ордера после потерянного ответа - тот же id (идемпотентность); перевыставление - новый."""
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.fault("order_post", "drop_before")
            await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            retry_ids = [p["newClientOrderId"] for p in e.ex.post_log]
            await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            return e, retry_ids

    e, retry_ids = run(scenario())
    assert len(retry_ids) == 2 and retry_ids[0] == retry_ids[1]  # повтор после обрыва: тот же clientOrderId
    assert e.ex.post_log[2]["newClientOrderId"] != retry_ids[0]  # новый ордер - новый id


def test_orders_survive_restart_and_can_be_cancelled(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            t = await e.barrier.submit(limit(qty=0.5, ttl_ms=100))
            from execution.live_barrier import LiveBarrier
            revived = LiveBarrier(e.rest, EXEC_CFG, StateStore(tmp_path / "state.json").load(), e.safe,
                                  store=e.store, backoff_s=0.0)
            assert revived.open_ticket_ids() == (t.ticket_id,)
            await revived.cancel(t.ticket_id)
            return e, revived

    e, revived = run(scenario())
    assert e.ex.orders[0]["status"] == "CANCELED"
    assert [x.reason for x in revived.advance(FAR)] == [CloseReason.EXPIRED]


# ------------------------------------------------------------------ Safe Mode


def test_safe_mode_blocks_new_risk_but_allows_reduce_only(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(market(Side.BUY, 0.5))
            e.safe.trip("тест")
            posts_before = len(e.ex.post_log)
            with pytest.raises(SafeModeActive):
                await e.barrier.submit(market(Side.BUY, 0.1, asof=ASOF + 1))
            assert len(e.ex.post_log) == posts_before and len(e.state.orders) == 0  # даже не записан
            closing = await e.barrier.close_position("BTCUSDT", ASOF + 2)
            return e, closing

    e, closing = run(scenario())
    assert closing is not None and e.ex.post_log[-1]["reduceOnly"] == "true" and e.ex.post_log[-1]["side"] == "SELL"
    assert e.barrier.position("BTCUSDT") == 0.0


def test_close_position_when_flat_does_nothing(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            return await e.barrier.close_position("BTCUSDT", ASOF), e

    result, e = run(scenario())
    assert result is None and e.ex.post_log == []


def test_foreign_fill_trips_safe_mode_but_foreign_noise_does_not(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order("autoclose-123", qty=0.5)
            noise = update(e.ex, "autoclose-123", exec_type="NEW")
            e.barrier.on_order_update(noise)
            assert not e.safe.active  # чужой ордер без исполнений - только лог
            o = e.ex.order("autoclose-123")
            trade = e.ex._book_trade(o, 0.5, 58_000.0, maker=False)
            e.barrier.on_order_update(update(e.ex, "autoclose-123", trade=trade))
            return e

    e = run(scenario())
    assert e.safe.active and "нет в локальном состоянии" in e.safe.reason
    assert len(e.alerter.alerts) == 1


def test_position_drift_is_detected_after_grace_only(tmp_path):
    async def scenario():
        async with make_env(tmp_path, drift_grace_ms=2_000) as e:
            await e.barrier.submit(market(Side.BUY, 0.5))
            from execution.user_stream import parse_account_update
            e.barrier.on_account_update(parse_account_update(e.ex.account_event("BTCUSDT", 0.5)))
            assert e.barrier.check_drift(1_000) == []  # совпадает
            e.barrier.on_account_update(parse_account_update(e.ex.account_event("BTCUSDT", 0.0)))  # закрыто по стопу
            assert e.barrier.check_drift(10_000) == [] and not e.safe.active  # гонка ORDER/ACCOUNT: ждём
            assert e.barrier.check_drift(11_999) == [] and not e.safe.active
            drifted = e.barrier.check_drift(12_000)
            return e, drifted

    e, drifted = run(scenario())
    assert drifted == ["BTCUSDT"] and e.safe.active
    assert e.alerter.alerts[0][1] == {"symbol": "BTCUSDT", "exchange": 0.0, "local": 0.5}


def test_drift_that_resolves_within_grace_is_forgotten(tmp_path):
    async def scenario():
        async with make_env(tmp_path, drift_grace_ms=2_000) as e:
            from execution.user_stream import parse_account_update
            e.barrier.on_account_update(parse_account_update(e.ex.account_event("BTCUSDT", 0.5)))
            assert e.barrier.check_drift(1_000) == []  # биржа впереди локального (ACCOUNT_UPDATE раньше ORDER_...)
            await e.barrier.submit(market(Side.BUY, 0.5))  # локальная позиция догнала
            assert e.barrier.check_drift(1_500) == []
            return e.barrier.check_drift(100_000), e

    drifted, e = run(scenario())
    assert drifted == [] and not e.safe.active


def test_hedge_mode_account_update_trips_safe_mode(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            from execution.user_stream import parse_account_update
            ev = e.ex.account_event("BTCUSDT", 0.5)
            ev["a"]["P"][0]["ps"] = "LONG"
            e.barrier.on_account_update(parse_account_update(ev))
            return e

    assert run(scenario()).safe.active


def test_overfill_trips_safe_mode_and_is_not_booked(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            o = e.ex.order(cid)
            trade = e.ex._book_trade(o, 0.8, 59_000.0, maker=True)  # сделка больше ордера
            e.barrier.on_order_update(update(e.ex, cid, trade=trade))
            return e

    e = run(scenario())
    assert e.safe.active and e.barrier.position("BTCUSDT") == 0.0 and e.barrier.advance(FAR) == ()


def test_fee_in_foreign_asset_falls_back_to_config_rate(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            cid = e.ex.orders[0]["cid"]
            t = await e.ex.fill(cid, 0.5, price=59_000.0, push=False, commission_asset="BNB", maker=True)
            e.barrier.on_order_update(update(e.ex, cid, trade=t))
            return e

    (fill,) = run(scenario()).barrier.advance(FAR)
    assert fill.fee == pytest.approx(0.5 * 59_000.0 * EXEC_CFG.maker_fee_rate)


def test_resync_all_adopts_fill_missed_during_stream_outage(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            await e.ex.fill(e.ex.orders[0]["cid"], 0.5, push=False)  # поток был оборван
            await e.barrier.resync_all()
            return e

    e = run(scenario())
    assert e.barrier.position("BTCUSDT") == 0.5 and e.state.orders == {}


def test_order_vanishing_from_exchange_trips_safe_mode(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.barrier.submit(limit(qty=0.5))
            e.ex.orders.clear()  # биржа «забыла» живой ордер
            await e.barrier.resync_all()
            return e

    assert run(scenario()).safe.active


def test_advance_rejects_time_going_back(tmp_path):
    from backtester.contracts import ContractViolation

    async def scenario():
        async with make_env(tmp_path) as e:
            e.barrier.advance(100)
            with pytest.raises(ContractViolation):
                e.barrier.advance(99)

    run(scenario())


# ----------------------------------------------------------- подписанный клиент


def test_wrong_secret_is_rejected_by_signature_check(tmp_path):
    async def scenario():
        async with make_env(tmp_path, secret="not-the-secret") as e:
            with pytest.raises(BinanceAPIError) as info:
                await e.rest.request("GET", "/fapi/v1/openOrders")
            return info.value

    err = run(scenario())
    assert err.code == -1022 and err.status == 400


def test_clock_skew_is_corrected_once_and_request_succeeds(tmp_path):
    async def scenario():
        async with make_env(tmp_path, recv_window_ms=2_000) as e:
            e.ex.time_offset_ms = 30_000  # часы биржи убежали
            data = await e.rest.request("GET", "/fapi/v1/openOrders")
            return e, data

    e, data = run(scenario())
    assert data == []
    assert e.ex.requests == ["GET /fapi/v1/openOrders", "GET /fapi/v1/time", "GET /fapi/v1/openOrders"]
    assert abs(e.rest._offset_ms - 30_000) < 500


def test_secret_never_appears_in_repr():
    creds = Credentials("k", "super-secret")
    assert "super-secret" not in repr(creds) and "super-secret" not in str(creds)


def test_signed_request_keeps_query_untouched(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            await e.rest.request("GET", "/fapi/v1/openOrders", {"symbol": "BTCUSDT", "flag": True})
            return e

    run(scenario())  # символы вроде & и = в значениях не ломают подпись: проверяется самой биржей (иначе -1022)
