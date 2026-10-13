"""Этап 4: Algo API в LiveBarrier. Защитные стопы/тейки, ALGO_UPDATE, «чужие» исполнения, безубыток, сверка.

Тесты идут на FakeAlgoExchange (tests/execution/fake_algo.py), чьи формы запросов/ответов сверены с документацией Binance.
Защита - quantity + reduceOnly (О18), безубыток - POST нового стопа, затем DELETE старых.
События приватного потока доставляются барьеру вручную и в заданном порядке (ex.pushed -> deliver), чтобы проверять гонки:
исполнение раньше ALGO_UPDATE, исполнение без ALGO_UPDATE, опоздавшее эхо.
"""

import asyncio
import json
import re

import pytest

import config
from backtester.contracts import Fill, Side
from execution.live_barrier import CLIENT_ORDER_ID_RE, LiveBarrier, new_client_algo_id
from execution.reconcile import DiscrepancyKind, reconcile
from execution.state import LocalAlgoOrder, LocalState, Protection, SafeMode, StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, UserDataStream, parse_algo_update, parse_event
from tests.execution.fake_algo import NORMAL_SEQUENCE, FakeAlgoExchange
from tests.execution.helpers import EXEC_CFG, limit, make_env, market
from utils.errors import ConfigError, DataError, OrderStatusUnknown

SYMBOL = "BTCUSDT"


def run(coro):
    return asyncio.run(coro)


def algo_env(tmp_path, **kw):
    return make_env(tmp_path, exchange=FakeAlgoExchange(kw.get("market_price", 60_000.0)), **kw)


def dispatch(barrier: LiveBarrier, event) -> None:
    if isinstance(event, OrderUpdate):
        barrier.on_order_update(event)
    elif isinstance(event, AccountUpdate):
        barrier.on_account_update(event)
    elif isinstance(event, AlgoUpdate):
        barrier.on_algo_update(event)


def deliver(e, only: tuple[str, ...] | None = None) -> list:
    """Доставляет накопленные события приватного потока барьеру в порядке отправки."""
    events = [parse_event(json.dumps(x)) for x in e.ex.pushed]
    e.ex.pushed.clear()
    for ev in events:
        dispatch(e.barrier, ev)
    return events


async def open_position(e, *, direction=1, sl=59_000.0, tp=62_000.0, be_r=None, qty=0.5):
    """arm_protection -> вход MARKET по рынку (без await между ними)."""
    price = e.ex.market_price
    e.barrier.arm_protection(SYMBOL, direction=direction, entry_price=price, sl_price=sl, tp_price=tp, be_r=be_r,
                             ticket_id=e.barrier.next_ticket_id)
    await e.barrier.submit(market(Side.BUY if direction > 0 else Side.SELL, qty))


def alive(e, role=None):
    return [a for a in e.ex.algo_orders if a["status"] == "NEW" and (role is None or a["type"] == role)]


# ============================================================================= разбор ALGO_UPDATE


def algo_payload(**o):
    body = {"caid": "tba-x", "aid": 77, "at": "CONDITIONAL", "o": "STOP_MARKET", "s": SYMBOL, "S": "SELL", "ps": "BOTH",
            "q": "0", "X": "NEW", "ai": "", "ap": "0", "aq": "0", "tp": "59000", "wt": "CONTRACT_PRICE", "cp": True, "R": False, "rm": ""}
    body.update(o)
    return {"e": "ALGO_UPDATE", "T": 1, "E": 2, "o": body}


def test_algo_update_is_parsed_into_typed_event():
    u = parse_event(json.dumps(algo_payload()))
    assert isinstance(u, AlgoUpdate)
    assert (u.client_algo_id, u.algo_id, u.symbol, u.side, u.order_type, u.status) == ("tba-x", 77, SYMBOL, Side.SELL, "STOP_MARKET", "NEW")
    assert u.actual_order_id is None and u.trigger_price == 59_000.0 and u.close_position and not u.reduce_only


def test_actual_order_id_after_trigger_and_rejection_reason():
    u = parse_algo_update(algo_payload(X="TRIGGERED", ai="123456", ap="58990.5", aq="0.5"))
    assert u.actual_order_id == 123_456 and u.avg_price == 58_990.5 and u.executed_qty == 0.5
    assert parse_algo_update(algo_payload(X="TRIGGERED", ai=123456)).actual_order_id == 123_456
    assert parse_algo_update(algo_payload(X="REJECTED", rm="ReduceOnly Order Failed")).reason == "ReduceOnly Order Failed"


@pytest.mark.parametrize("bad", [
    {"X": "WEIRD"}, {"caid": ""}, {"aid": "x"}, {"S": "SIDEWAYS"}, {"ai": "abc"}, {"q": "много"}, {"s": None},
])
def test_malformed_algo_update_raises_data_error(bad):
    with pytest.raises(DataError):
        parse_algo_update(algo_payload(**bad))
    with pytest.raises(DataError):
        parse_algo_update({"e": "ALGO_UPDATE", "E": 1})


def test_algo_events_are_subscribed_only_by_explicit_set():
    assert "ALGO_UPDATE" not in config.USER_STREAM_EVENTS and "ALGO_UPDATE" in config.USER_STREAM_EVENTS_WITH_ALGO


def test_algo_update_arrives_through_the_real_private_websocket(tmp_path):
    async def body():
        async with algo_env(tmp_path, timeout_s=None) as e:
            got = []
            stream = UserDataStream(e.rest, e.session, got.append, ws_base_url=f"ws://{e.host}/private", events=config.USER_STREAM_EVENTS_WITH_ALGO,
                                    backoff_s=0.01, backoff_max_s=0.02)
            task = asyncio.create_task(stream.run())
            await e.ex.wait_clients(1)
            assert "ALGO_UPDATE" in e.ex.ws_requests[0]["events"]
            a = e.ex.seed_algo("tba-ws")
            await e.ex.push(e.ex.algo_event(a, "NEW"))
            while not got:
                await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return got

    (u,) = run(body())
    assert isinstance(u, AlgoUpdate) and u.client_algo_id == "tba-ws" and u.status == "NEW"


# ============================================================================= состояние


def test_state_roundtrip_v2_and_v1_compatibility(tmp_path):
    empty = LocalState()
    assert '"version": 1' in empty.to_json() and LocalState.from_json(empty.to_json()).algo_orders == {}  # без algo - формат прежний
    s = LocalState()
    s.algo_orders["a"] = LocalAlgoOrder("a", SYMBOL, Side.SELL, "sl", "STOP_MARKET", 59_000.0, 5, algo_id=9, status="NEW", replaces="b")
    s.protections[SYMBOL] = Protection(SYMBOL, 1, 60_000.0, 59_000.0, "3", 1, tp_price=62_000.0, be_r=1.0, be_done=True, be_failed=True,
                                       fill_qty=0.5, fill_cost=30_000.0)
    s.remember_triggered(555, "a")
    raw = s.to_json()
    assert '"version": 2' in raw
    r = LocalState.from_json(raw)
    assert r.algo_orders["a"] == s.algo_orders["a"] and r.protections[SYMBOL] == s.protections[SYMBOL] and r.triggered_order_ids == {"555": "a"}
    with pytest.raises(DataError, match="версия"):
        LocalState.from_json(raw.replace('"version": 2', '"version": 3'))
    with pytest.raises(DataError):
        LocalState.from_json(raw.replace('"role": "sl"', '"roll": "sl"'))


def test_client_algo_id_format_and_uniqueness():
    ids = {new_client_algo_id() for _ in range(200)}
    assert len(ids) == 200 and all(CLIENT_ORDER_ID_RE.match(i) and len(i) <= 36 for i in ids)


# ============================================================================= arm_protection


def test_arm_protection_validates_rounds_and_persists(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            b = e.barrier
            with pytest.raises(DataError, match="убыточную"):
                b.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=60_500.0, ticket_id=1)
            with pytest.raises(DataError, match="убыточную"):
                b.arm_protection(SYMBOL, direction=-1, entry_price=60_000.0, sl_price=59_500.0, ticket_id=1)
            with pytest.raises(DataError):
                b.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, tp_price=59_900.0, ticket_id=1)
            with pytest.raises(DataError):
                b.arm_protection(SYMBOL, direction=2, entry_price=60_000.0, sl_price=59_000.0, ticket_id=1)
            with pytest.raises(ConfigError, match="шага цены"):
                b.arm_protection("XRPUSDT", direction=1, entry_price=2.0, sl_price=1.0, ticket_id=1)
            p = b.arm_protection(SYMBOL, direction=1, entry_price=60_000.04, sl_price=58_999.97, tp_price=62_000.06, ticket_id=7, be_r=1.0)
            assert (p.sl_price, p.tp_price, p.entry_price, p.parent) == (58_999.9, 62_000.0, 60_000.0, "7")  # стоп дальше, тейк ближе к входу
            short = LocalState()
            with pytest.raises(DataError, match="уже есть"):
                b.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, ticket_id=8)
            assert LocalState.from_json(e.store.path.read_text(encoding="utf-8")).protections[SYMBOL].sl_price == 58_999.9
            return short

    run(body())


def test_short_rounding_is_mirrored(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            p = e.barrier.arm_protection(SYMBOL, direction=-1, entry_price=60_000.0, sl_price=61_000.01, tp_price=57_999.97, ticket_id=1)
            assert (p.sl_price, p.tp_price) == (61_000.1, 58_000.0)

    run(body())


# ============================================================================= выставление защиты


def test_pump_places_sl_and_tp_with_exact_wire_params_and_write_ahead(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            seen = {}

            def pre_algo(params):  # в момент POST запись об ордере уже на диске (write-ahead)
                disk = LocalState.from_json(e.store.path.read_text(encoding="utf-8"))
                seen[params["clientAlgoId"]] = disk.algo_orders[params["clientAlgoId"]].status

            e.ex.hooks["pre_algo"] = pre_algo
            await open_position(e)
            await e.barrier.pump()
            return e.ex.algo_post_log, dict(seen), e.barrier._state.algo_orders, alive(e)

    posts, seen, local, live = run(body())
    sl, tp = posts
    assert sl["type"] == "STOP_MARKET" and sl["side"] == "SELL" and sl["triggerPrice"] == "59000"
    assert tp["type"] == "TAKE_PROFIT_MARKET" and tp["side"] == "SELL" and tp["triggerPrice"] == "62000"
    for p in posts:  # О18: объём и reduceOnly, а не closePosition
        assert p["algoType"] == "CONDITIONAL" and p["symbol"] == SYMBOL and p["workingType"] == "CONTRACT_PRICE"
        assert p["quantity"] == "0.5" and p["reduceOnly"] == "true" and "closePosition" not in p and CLIENT_ORDER_ID_RE.match(p["clientAlgoId"])
    assert set(seen.values()) == {"PENDING"} and set(seen) == {sl["clientAlgoId"], tp["clientAlgoId"]}
    assert {a.role: a.status for a in local.values()} == {"sl": "NEW", "tp": "NEW"} and all(a.algo_id for a in local.values())
    assert all(not a.close_position and a.qty == 0.5 for a in local.values())
    assert len(live) == 2


def test_pump_waits_for_the_limit_entry_then_protects_once_and_idempotently(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=59_800.0, sl_price=59_000.0, tp_price=62_000.0, ticket_id=e.barrier.next_ticket_id)
            await e.barrier.submit(limit(Side.BUY, 0.5, 59_800.0, ttl_ms=10**9))
            await e.barrier.pump()
            assert e.ex.algo_post_log == [] and SYMBOL in e.state.protections  # вход ещё жив, позиции нет: стоп без позиции не ставим
            cid = next(iter(e.state.orders))
            await e.ex.fill(cid, 0.5, price=59_800.0)
            deliver(e)  # ORDER_TRADE_UPDATE исполнения входа
            assert e.state.position(SYMBOL) == 0.5 and e.state.protections[SYMBOL].fill_qty == 0.5
            for _ in range(3):
                await e.barrier.pump()
            return len(e.ex.algo_post_log), e.state.protections[SYMBOL].entry_price

    n, entry = run(body())
    assert n == 2 and entry == 59_800.0  # ровно стоп и тейк, сколько бы раз ни вызывали pump


def test_pump_drops_protection_when_the_entry_never_happened(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, ticket_id=999)  # входного ордера нет
            await e.barrier.pump()
            return e.state.protections, e.ex.algo_post_log

    prots, posts = run(body())
    assert prots == {} and posts == []


def test_entry_fill_price_drives_breakeven_and_tp_by_r(tmp_path):
    async def body():
        async with algo_env(tmp_path, market_price=60_100.0) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, tp_r=3.0, be_r=1.0, ticket_id=e.barrier.next_ticket_id)
            await e.barrier.submit(market(Side.BUY, 0.5))  # MARKET исполнился по 60 100, а не по плановым 60 000
            p = e.barrier._state.protections[SYMBOL]
            await e.barrier.pump()
            return p.entry_price, p.effective_tp(), p.be_trigger(), e.ex.algo_post_log

    entry, tp, be, posts = run(body())
    assert entry == 60_100.0 and tp == pytest.approx(60_100.0 + 3 * 1_100.0) and be == pytest.approx(61_200.0)
    assert [p["triggerPrice"] for p in posts] == ["59000", "63400"]


def test_lost_response_is_resolved_by_query_without_duplicate(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.fault("algo_post", "lost_response")  # ордер создан, ответ потерян
            await open_position(e)
            await e.barrier.pump()
            return e.ex.algo_post_log, e.ex.algo_orders, e.barrier._state.algo_orders

    posts, exchange, local = run(body())
    sl_posts = [p for p in posts if p["type"] == "STOP_MARKET"]
    assert len(sl_posts) == 1 and len([a for a in exchange if a["type"] == "STOP_MARKET"]) == 1  # повтора нет: найден по clientAlgoId
    assert {a.role for a in local.values()} == {"sl", "tp"} and all(a.status == "NEW" for a in local.values())


def test_request_that_never_reached_the_exchange_is_retried_with_the_same_id_after_expiry(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.fault("algo_post", "drop_before")
            await open_position(e)
            await e.barrier.pump()
            return e.ex.algo_post_log, e.ex.algo_orders, e.sleeps

    posts, exchange, sleeps = run(body())
    sl_posts = [p for p in posts if p["type"] == "STOP_MARKET"]
    assert len(sl_posts) == 2 and sl_posts[0]["clientAlgoId"] == sl_posts[1]["clientAlgoId"]  # тот же id при повторе
    assert len([a for a in exchange if a["type"] == "STOP_MARKET"]) == 1 and any(s > 0 for s in sleeps)  # ждали протухания запроса


def test_unknown_outcome_means_safe_mode_and_reduce_only_flatten(tmp_path):
    async def body():
        async with algo_env(tmp_path, attempts=2) as e:
            e.ex.fault("algo_post", "drop_before", 5)
            e.ex.fault("algo_get", "http500", 20)
            await open_position(e)
            with pytest.raises(OrderStatusUnknown):
                await e.barrier.pump()
            return e.safe.active, e.ex.positions[SYMBOL], e.ex.post_log[-1]

    active, position, last_order = run(body())
    assert active and position == 0.0  # позиция без подтверждённого стопа немедленно закрыта
    assert last_order["reduceOnly"] == "true" and last_order["side"] == "SELL" and last_order["type"] == "MARKET"


def test_stop_already_beyond_price_flattens_without_safe_mode(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            e.ex.market_price = 58_900.0  # цена ушла за стоп до его выставления: -2021
            await e.barrier.pump()
            return e.safe.active, e.ex.positions[SYMBOL], e.barrier._state.position(SYMBOL), [a["status"] for a in e.ex.algo_orders]

    active, ex_pos, local_pos, statuses = run(body())
    assert not active and ex_pos == 0.0 and local_pos == 0.0 and "NEW" not in statuses  # обычный рыночный случай, не авария


def test_other_rejection_of_protection_trips_safe_mode_and_flattens(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            e.ex.fault("algo_post", "reject")  # биржа отказывает в стопе (например, -4164)
            await e.barrier.pump()
            return e.safe.active, e.safe.reason, e.ex.positions[SYMBOL]

    active, reason, position = run(body())
    assert active and "Защита позиции" in reason and "4164" in reason and position == 0.0


# ============================================================================= срабатывание и «чужие» исполнения


def trade_ids(events):
    return [ev for ev in events if isinstance(ev, OrderUpdate) and ev.has_trade]


def test_stop_triggered_normal_event_order_is_not_foreign_and_cleans_up(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)  # NEW-эхо своих ордеров
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            e.barrier.advance(10**15)
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0)
            deliver(e)
            fills = [x for x in e.barrier.advance(10**15) if isinstance(x, Fill)]
            await e.barrier.pump()
            return e, fills

    # e живёт только внутри make_env, поэтому проверки внутри
    async def body2():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            e.barrier.advance(10**15)
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0)
            deliver(e)
            fills = [x for x in e.barrier.advance(10**15) if isinstance(x, Fill)]
            await e.barrier.pump()
            st = e.barrier._state
            return (e.safe.active, st.position(SYMBOL), fills, dict(st.algo_orders), dict(st.protections),
                    [a["status"] for a in e.ex.algo_orders], e.alerter.alerts)

    active, pos, fills, algos, prots, statuses, alerts = run(body2())
    assert not active and alerts == [] and pos == 0.0  # стоп сработал - это своё исполнение, Safe Mode не включился
    assert len(fills) == 1 and fills[0].side is Side.SELL and fills[0].price == 58_990.0 and fills[0].qty == 0.5
    assert algos == {} and prots == {} and "NEW" not in statuses  # тейк-«брат» истёк/снят, защита снята


def test_trade_event_before_algo_update_is_buffered_then_explained(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            e.barrier.advance(10**15)
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0, sequence=("trade", "a_triggering", "a_triggered", "a_finished"))
            evs = [parse_event(json.dumps(x)) for x in e.ex.pushed]
            e.ex.pushed.clear()
            dispatch(e.barrier, evs[0])  # ORDER_TRADE_UPDATE пришёл первым
            mid = (e.safe.active, e.barrier._state.position(SYMBOL), len(e.barrier._unexplained))
            for ev in evs[1:]:
                dispatch(e.barrier, ev)
            fills = [x for x in e.barrier.advance(10**15) if isinstance(x, Fill)]
            return mid, e.safe.active, e.barrier._state.position(SYMBOL), len(fills), len(e.barrier._unexplained)

    mid, active, pos, nfills, left = run(body())
    assert mid == (False, 0.5, 1)  # ждёт объяснения: Safe Mode не включён, позиция ещё не списана
    assert not active and pos == 0.0 and nfills == 1 and left == 0  # после ALGO_UPDATE(ai) исполнение учтено ровно один раз


def test_trade_without_any_algo_update_is_explained_through_rest_query(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0, sequence=("trade",))  # ALGO_UPDATE не пришёл вовсе
            deliver(e)
            assert e.barrier._state.position(SYMBOL) == 0.5 and len(e.barrier._unexplained) == 1
            await e.barrier.pump()  # GET algoOrder -> actualOrderId объясняет исполнение
            return e.safe.active, e.barrier._state.position(SYMBOL), len(e.barrier._unexplained), e.barrier._state.protections

    active, pos, left, prots = run(body())
    assert not active and pos == 0.0 and left == 0 and prots == {}


def test_unexplained_trade_with_live_algo_orders_trips_safe_mode_after_grace(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            e.barrier._grace_ms = 0
            e.ex.seed_order("manual-1", side="SELL", qty=0.2, price=60_500.0)
            await e.ex.fill("manual-1", 0.2)  # чужое ручное исполнение
            deliver(e)
            assert not e.safe.active  # сначала буфер: вдруг это algo-исполнение
            await e.barrier.pump()
            return e.safe.active, e.safe.reason

    active, reason = run(body())
    assert active and "не объясняет" in reason


def test_foreign_trade_without_algo_context_trips_immediately_as_before(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_order("manual-1", side="SELL", qty=0.2, price=60_500.0)
            await e.ex.fill("manual-1", 0.2)
            deliver(e)
            return e.safe.active

    assert run(body())  # поведение ML-контура не изменилось


def test_algo_fill_that_increases_the_position_trips_safe_mode(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            sl.status = "TRIGGERED"
            e.barrier._learn_actual_order(sl, 9001)
            e.barrier._book_algo_trade(SYMBOL, Side.BUY, 4242, 0.3, 60_000.0, 1, 0.1, "USDT", False, sl)  # «стоп» купил: позиция выросла
            return e.safe.active, e.safe.reason

    active, reason = run(body())
    assert active and "увеличило позицию" in reason


# ============================================================================= события ALGO_UPDATE


def test_foreign_algo_update_trips_safe_mode_only_when_barrier_uses_algo(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            foreign = parse_event(json.dumps(algo_payload(caid="manual-stop", X="NEW")))
            dispatch(e.barrier, foreign)
            before = e.safe.active  # ML-контур без algo-данных: событие чужого стопа пропущено
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, ticket_id=1)
            dispatch(e.barrier, foreign)
            return before, e.safe.active, e.safe.reason

    before, after, reason = run(body())
    assert before is False and after is True and "чужой" in reason


def test_late_echo_of_retired_algo_order_is_ignored_and_statuses_are_monotonic(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            tp = next(a for a in e.barrier._state.algo_orders.values() if a.role == "tp")
            await e.barrier._cancel_algo_confirmed(tp)
            deliver(e)  # CANCELED
            dispatch(e.barrier, parse_event(json.dumps(algo_payload(caid=tp.client_algo_id, X="NEW", o="TAKE_PROFIT_MARKET"))))  # NEW после отмены
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            dispatch(e.barrier, parse_event(json.dumps(algo_payload(caid=sl.client_algo_id, X="TRIGGERING"))))
            dispatch(e.barrier, parse_event(json.dumps(algo_payload(caid=sl.client_algo_id, X="NEW"))))  # откат статуса запрещён
            return e.safe.active, sl.status

    active, status = run(body())
    assert not active and status == "TRIGGERING"


def test_mismatching_algo_update_trips_safe_mode(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            dispatch(e.barrier, parse_event(json.dumps(algo_payload(caid=sl.client_algo_id, S="BUY"))))
            return e.safe.active

    assert run(body())


def test_exchange_rejecting_our_stop_flattens_the_position(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            dispatch(e.barrier, parse_event(json.dumps(algo_payload(caid=sl.client_algo_id, X="REJECTED", rm="Margin is insufficient"))))
            await e.barrier.pump()
            return e.safe.active, e.safe.reason, e.ex.positions[SYMBOL]

    active, reason, position = run(body())
    assert active and "отклон" in reason and position == 0.0


# ============================================================================= безубыток


def requests_after(e, mark: int) -> list[str]:
    return e.ex.algo_requests[mark:]


def test_breakeven_is_post_new_then_delete_old_at_entry(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)  # вход 60 000, SL 59 000 -> R = 1000, безубыток при 61 000
            await e.barrier.pump()
            deliver(e)
            old = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            tp_before = next(a for a in e.barrier._state.algo_orders.values() if a.role == "tp")
            alive_stops_at_post = []
            e.ex.hooks["pre_algo"] = lambda params: alive_stops_at_post.append(len([a for a in alive(e, "STOP_MARKET")]))
            mark = len(e.ex.algo_requests)
            e.barrier.on_mark_price(SYMBOL, 60_999.0)
            await e.barrier.pump()
            assert len(e.ex.algo_requests) == mark  # +1R ещё не достигнут
            e.ex.market_price = 61_050.0
            e.barrier.on_mark_price(SYMBOL, 61_000.0)
            await e.barrier.pump()
            deliver(e)
            new = next(a for a in e.barrier._state.algo_orders.values() if a.role == "be_sl")
            prot = e.barrier._state.protections[SYMBOL]
            disk = LocalState.from_json(e.store.path.read_text(encoding="utf-8"))
            return (requests_after(e, mark), old, new, tp_before, prot.be_done, e.ex.algo(old.client_algo_id)["status"],
                    [(a["type"], a["trigger"], a["quantity"]) for a in alive(e)], disk.protections[SYMBOL].be_done, e.safe.active,
                    alive_stops_at_post, e.ex.algo_post_log[-1])

    reqs, old, new, tp, be_done, old_status, live, disk_be, active, at_post, last_post = run(body())
    assert reqs == [f"POST {new.client_algo_id}", f"DELETE {old.client_algo_id}"]  # сначала новый, потом старый: окна без стопа нет
    assert at_post == [1]  # в момент POST нового стопа старый ещё стоял
    assert new.client_algo_id != old.client_algo_id and new.trigger_price == 60_000.0 and new.replaces == old.client_algo_id
    assert last_post["quantity"] == "0.5" and last_post["reduceOnly"] == "true" and "closePosition" not in last_post
    assert be_done and disk_be and old_status == "CANCELED" and not active
    assert sorted(live) == [("STOP_MARKET", 60_000.0, 0.5), ("TAKE_PROFIT_MARKET", 62_000.0, 0.5)]  # тейк не тронут, стоп ровно один


def test_breakeven_is_requested_only_once_and_never_for_a_closed_position(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            e.ex.market_price = 61_050.0
            for _ in range(3):
                e.barrier.on_mark_price(SYMBOL, 61_500.0)
            await e.barrier.pump()
            n = len([r for r in e.ex.algo_requests if r.startswith("DELETE")])
            e.barrier.on_mark_price(SYMBOL, 62_000.0)  # уже перенесён
            await e.barrier.pump()
            return n, len([r for r in e.ex.algo_requests if r.startswith("DELETE")]), "BTCUSDT" in e.barrier._be_requested

    first, second, requested = run(body())
    assert first == 1 and second == 1 and not requested


def restart(e):
    state2 = StateStore(e.store.path).load()  # перезапуск: состояние с диска
    return state2, LiveBarrier(e.rest, EXEC_CFG, state2, e.safe, store=e.store, attempts=3, backoff_s=0.0, sleep=lambda s: asyncio.sleep(0))


def test_crash_after_intent_before_any_network_action_is_repaired_after_restart(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            e.barrier._state.protections[SYMBOL].be_done = True  # намерение записано... и процесс «умер»
            e.barrier._save(strict=True)
            e.ex.market_price = 61_050.0
            state2, barrier2 = restart(e)
            await barrier2.pump()
            return sorted((a["type"], a["trigger"]) for a in alive(e)), sorted(a.role for a in state2.algo_orders.values())

    live, roles = run(body())
    assert live == [("STOP_MARKET", 60_000.0), ("TAKE_PROFIT_MARKET", 62_000.0)] and roles == ["be_sl", "tp"]


def test_crash_between_post_new_and_delete_old_is_repaired_after_restart(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            prot = e.barrier._state.protections[SYMBOL]
            prot.be_done = True
            e.barrier._save(strict=True)
            e.ex.market_price = 61_050.0
            await e.barrier._try_place_algo(prot, "be_sl", 60_000.0, 0.5)  # новый стоп поставлен... и процесс «умер» до DELETE старого
            assert len(alive(e, "STOP_MARKET")) == 2  # оба стопа на бирже: позиция защищена
            state2, barrier2 = restart(e)
            await barrier2.pump()
            return sorted((a["type"], a["trigger"]) for a in alive(e)), sorted(a.role for a in state2.algo_orders.values())

    live, roles = run(body())
    assert live == [("STOP_MARKET", 60_000.0), ("TAKE_PROFIT_MARKET", 62_000.0)] and roles == ["be_sl", "tp"]


def test_crash_between_delete_and_post_old_scheme_is_still_repaired(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            old = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            prot = e.barrier._state.protections[SYMBOL]
            prot.be_done = True
            e.barrier._save(strict=True)
            await e.barrier._cancel_algo_confirmed(old)  # старый стоп снят, нового нет (состояние от прошлой версии кода)
            assert [a["type"] for a in alive(e)] == ["TAKE_PROFIT_MARKET"]
            e.ex.market_price = 61_050.0
            state2, barrier2 = restart(e)
            await barrier2.pump()
            return [(a["type"], a["trigger"]) for a in alive(e)], [a.role for a in state2.algo_orders.values()]

    live, roles = run(body())
    assert sorted(live) == [("STOP_MARKET", 60_000.0), ("TAKE_PROFIT_MARKET", 62_000.0)] and "be_sl" in roles


def test_breakeven_when_the_stop_has_just_triggered_does_not_flatten_or_trip(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0, sequence=())  # стоп сработал, события ещё не дошли
            e.ex.market_price = 61_050.0
            n_orders = len(e.ex.post_log)
            await e.barrier.move_stop_to_breakeven(SYMBOL)  # биржа новый reduceOnly по закрытой позиции отвергает (-2022)
            deliver(e)
            return e.safe.active, len(e.ex.post_log) - n_orders, e.ex.positions[SYMBOL], alive(e, "STOP_MARKET")

    active, new_orders, position, stops = run(body())
    assert not active and new_orders == 0 and position == 0.0 and stops == []  # лишних рыночных ордеров нет, Safe Mode не включён


def test_breakeven_stop_rejected_flattens_the_position(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            e.ex.market_price = 59_900.0  # цена вернулась под вход: стоп на входе сработал бы немедленно (-2021)
            await e.barrier.move_stop_to_breakeven(SYMBOL)
            return e.ex.positions[SYMBOL], e.safe.active

    position, active = run(body())
    assert position == 0.0 and not active  # безубыточный выход по рынку - обычный рыночный исход


def test_breakeven_stop_refused_by_exchange_keeps_the_original_stop(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            e.ex.market_price = 61_050.0
            e.ex.fault("algo_post", "reject")  # биржа отказывает именно в новом стопе
            e.barrier.on_mark_price(SYMBOL, 61_000.0)
            await e.barrier.pump()
            prot = e.barrier._state.protections[SYMBOL]
            first = (prot.be_done, prot.be_failed, e.safe.active, e.ex.positions[SYMBOL], sorted((a["type"], a["trigger"]) for a in alive(e)))
            mark = len(e.ex.algo_requests)
            e.barrier.on_mark_price(SYMBOL, 61_500.0)  # повторно перенос не запрашивается
            requested = SYMBOL in e.barrier._be_requested
            await e.barrier.pump()
            return first, len(e.ex.algo_requests) - mark, requested

    (be_done, be_failed, active, position, live), extra, requested = run(body())
    assert not requested
    assert (be_done, be_failed, active, position) == (False, True, False, 0.5)  # позиция жива, Safe Mode не включён
    assert live == [("STOP_MARKET", 59_000.0), ("TAKE_PROFIT_MARKET", 62_000.0)] and extra == 0  # исходные стоп и тейк на месте


def test_breakeven_refused_when_no_other_stop_exists_flattens_and_trips_safe_mode(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            old = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            e.barrier._state.protections[SYMBOL].be_done = True
            await e.barrier._cancel_algo_confirmed(old)  # исходного стопа уже нет (состояние от прошлой версии кода)
            e.ex.market_price = 61_050.0
            e.ex.fault("algo_post", "reject")  # и новый стоп биржа не принимает
            await e.barrier.pump()
            return e.safe.active, e.safe.reason, e.ex.positions[SYMBOL]

    active, reason, position = run(body())
    assert active and "безубытка" in reason and position == 0.0  # позиция без стопа недопустима (R6)


def test_exchange_with_strict_reduce_only_sum_refuses_breakeven_but_keeps_position_protected(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.strict_reduce_sum = True  # гипотеза: сумма reduceOnly-ордеров не может превышать позицию
            await open_position(e, tp=None, be_r=1.0)  # только стоп 0.5
            await e.barrier.pump()
            deliver(e)
            e.ex.market_price = 61_050.0
            e.barrier.on_mark_price(SYMBOL, 61_000.0)
            await e.barrier.pump()
            prot = e.barrier._state.protections[SYMBOL]
            return prot.be_failed, e.safe.active, e.ex.positions[SYMBOL], [(a["type"], a["trigger"]) for a in alive(e)]

    be_failed, active, position, live = run(body())
    assert be_failed and not active and position == 0.5 and live == [("STOP_MARKET", 59_000.0)]


def test_exchange_with_strict_reduce_only_sum_fails_safe_when_sl_and_tp_do_not_fit(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.strict_reduce_sum = True
            await open_position(e)  # SL 0.5 + TP 0.5 > позиции 0.5: тейк отклонён
            await e.barrier.pump()
            return e.safe.active, e.safe.reason, e.ex.positions[SYMBOL]

    active, reason, position = run(body())
    assert active and "tp" in reason and position == 0.0  # неподтверждённая защита = закрытие позиции + Safe Mode, не молчание


def test_breakeven_stop_with_off_tick_average_entry_is_rounded_to_tick(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=60_000.0, sl_price=59_000.0, tp_price=62_000.0, be_r=1.0,
                                     ticket_id=e.barrier.next_ticket_id)
            await e.barrier.submit(limit(Side.BUY, 0.5, 60_000.1, ttl_ms=10**9))
            cid = next(iter(e.state.orders))
            await e.ex.fill(cid, 0.2, price=60_000.0)
            await e.ex.fill(cid, 0.3, price=60_000.1)
            deliver(e)
            prot = e.state.protections[SYMBOL]
            await e.barrier.pump()
            e.ex.market_price = 61_100.0
            await e.barrier.move_stop_to_breakeven(SYMBOL)
            return prot.entry_price, [p for p in e.ex.algo_post_log if p["type"] == "STOP_MARKET"][-1]

    entry, last_stop = run(body())
    assert entry == pytest.approx(60_000.06) and last_stop["triggerPrice"] == "60000.1"  # шаг цены BTCUSDT 0.1


# ============================================================================= покрытие позиции по объёму


def stop_quantities(e, type_="STOP_MARKET"):
    return [a["quantity"] for a in alive(e, type_)]


def test_partial_entry_fills_get_additional_stops_and_takes_covering_exactly_the_position(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=59_800.0, sl_price=59_000.0, tp_price=62_000.0, ticket_id=e.barrier.next_ticket_id)
            await e.barrier.submit(limit(Side.BUY, 0.5, 59_800.0, ttl_ms=10**9))
            cid = next(iter(e.state.orders))
            await e.ex.fill(cid, 0.2, price=59_800.0)
            deliver(e)
            await e.barrier.pump()
            first = (stop_quantities(e), stop_quantities(e, "TAKE_PROFIT_MARKET"))
            await e.barrier.pump()  # повторный вызов ничего не добавляет
            await e.ex.fill(cid, 0.3, price=59_800.0)
            deliver(e)
            for _ in range(2):
                await e.barrier.pump()
            return first, (stop_quantities(e), stop_quantities(e, "TAKE_PROFIT_MARKET")), len(e.ex.algo_post_log)

    first, second, n_posts = run(body())
    assert first == ([0.2], [0.2]) and second == ([0.2, 0.3], [0.2, 0.3]) and n_posts == 4  # добавка только на недостающий объём


def test_float_noise_in_position_does_not_cause_extra_orders(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.barrier.arm_protection(SYMBOL, direction=1, entry_price=59_800.0, sl_price=59_000.0, ticket_id=e.barrier.next_ticket_id)
            await e.barrier.submit(limit(Side.BUY, 0.3, 59_800.0, ttl_ms=10**9))
            cid = next(iter(e.state.orders))
            await e.ex.fill(cid, 0.1, price=59_800.0)
            await e.ex.fill(cid, 0.2, price=59_800.0)
            deliver(e)
            pos = e.state.position(SYMBOL)
            for _ in range(3):
                await e.barrier.pump()
            return pos, stop_quantities(e)

    pos, qtys = run(body())
    assert pos == pytest.approx(0.3)  # 0.1 + 0.2 в float = 0.30000000000000004: объём стопа всё равно ровно 0.3
    assert qtys == [0.3]


# ============================================================================= сверка: объём защиты


def test_reconcile_flags_partially_covered_position_and_mismatching_algo_quantity(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_position(SYMBOL, 0.5)
            e.state.positions[SYMBOL] = 0.5
            e.state.algo_orders["tba-a"] = LocalAlgoOrder("tba-a", SYMBOL, Side.SELL, "sl", "STOP_MARKET", 59_000.0, 1, status="NEW", qty=0.2)
            e.ex.seed_algo("tba-a", trigger=59_000.0, quantity=0.2, close_position=False, reduce_only=True)
            partial = await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])
            e.safe.clear("test")
            e.state.algo_orders["tba-a"].qty = 0.5  # бот считает 0.5, на бирже 0.2
            wrong_qty = await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])
            return partial, wrong_qty

    partial, wrong_qty = run(body())
    assert DiscrepancyKind.POSITION_UNPROTECTED in {d.kind for d in partial.discrepancies}
    assert DiscrepancyKind.ALGO_MISMATCH in {d.kind for d in wrong_qty.discrepancies}


def test_reconcile_accepts_sum_of_stops_covering_the_position(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_position(SYMBOL, 0.5)
            e.state.positions[SYMBOL] = 0.5
            for cid, qty in (("tba-a", 0.2), ("tba-b", 0.3)):
                e.state.algo_orders[cid] = LocalAlgoOrder(cid, SYMBOL, Side.SELL, "sl", "STOP_MARKET", 59_000.0, 1, status="NEW", qty=qty)
                e.ex.seed_algo(cid, trigger=59_000.0, quantity=qty, close_position=False, reduce_only=True)
            return await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])

    assert run(body()).clean


def test_delete_with_lost_response_is_confirmed_by_query(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            e.ex.market_price = 61_050.0
            e.ex.fault("algo_delete", "drop_after")  # отмена прошла, ответ потерян
            await e.barrier.move_stop_to_breakeven(SYMBOL)
            return sorted((a["type"], a["trigger"]) for a in alive(e)), e.safe.active

    live, active = run(body())
    assert live == [("STOP_MARKET", 60_000.0), ("TAKE_PROFIT_MARKET", 62_000.0)] and not active


# ============================================================================= сверка


def test_reconcile_clean_with_protection_and_algo_orders_on_exchange(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            return await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])

    report = run(body())
    assert report.clean and report.exchange_positions == {SYMBOL: 0.5}


def test_reconcile_flags_unprotected_position_unknown_and_mismatching_algo(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_position(SYMBOL, 0.5)
            e.state.positions[SYMBOL] = 0.5
            e.state.algo_orders["tba-mine"] = LocalAlgoOrder("tba-mine", SYMBOL, Side.SELL, "tp", "TAKE_PROFIT_MARKET", 62_000.0, 1, status="NEW")
            e.ex.seed_algo("tba-mine", type_="TAKE_PROFIT_MARKET", trigger=62_500.0)  # триггер на бирже другой
            e.ex.seed_algo("tba-alien", type_="TAKE_PROFIT_MARKET", trigger=63_000.0)
            return await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL]), e.safe.active

    report, active = run(body())
    kinds = {d.kind for d in report.discrepancies}
    assert {DiscrepancyKind.POSITION_UNPROTECTED, DiscrepancyKind.UNKNOWN_EXCHANGE_ALGO, DiscrepancyKind.ALGO_MISMATCH} <= kinds and active


def test_reconcile_resolves_offline_changes_of_local_algo_orders(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.state.protections[SYMBOL] = Protection(SYMBOL, 1, 60_000.0, 59_000.0, "1", 1)
            e.state.algo_orders["tba-gone"] = LocalAlgoOrder("tba-gone", SYMBOL, Side.SELL, "sl", "STOP_MARKET", 59_000.0, 1, status="NEW")
            e.ex.seed_algo("tba-gone", status="CANCELED")
            e.state.algo_orders["tba-never"] = LocalAlgoOrder("tba-never", SYMBOL, Side.SELL, "tp", "TAKE_PROFIT_MARKET", 62_000.0, 1, status="PENDING")
            clean = await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])
            left = dict(e.state.algo_orders)
            e.state.algo_orders["tba-fin"] = LocalAlgoOrder("tba-fin", SYMBOL, Side.SELL, "sl", "STOP_MARKET", 59_000.0, 1, status="NEW")
            e.ex.seed_algo("tba-fin", status="FINISHED")
            dirty = await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])
            return clean, left, dirty

    clean, left, dirty = run(body())
    assert clean.clean and left == {}  # отменённый и недошедший - безобидны, удалены
    assert not dirty.clean and DiscrepancyKind.ALGO_RESOLVED_OFFLINE in {d.kind for d in dirty.discrepancies}  # стоп сработал без бота


def test_reconcile_does_not_touch_algo_endpoints_for_ml_only_state(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            report = await reconcile(e.rest, e.state, e.safe, store=e.store, symbols=[SYMBOL])
            return report.clean, [r for r in e.ex.requests if "lgo" in r]

    clean, algo_requests = run(body())
    assert clean and algo_requests == []  # ML-контур не получает лишних запросов и не зависит от непроверенного пути


# ============================================================================= Fail-Fast в новом коде


def test_new_algo_code_does_not_suppress_errors_r1():
    src = open("execution/live_barrier.py", encoding="utf-8").read()
    block = src[src.index("# ======================================================================== Algo API"): src.index("# --------------------------------------------------------------------- сверка")]
    assert len(block) > 5_000
    code = "\n".join(line.split("#")[0] for line in block.splitlines())
    assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", code)


def test_resync_after_reconnect_refreshes_algo_status_and_flags_vanished_orders(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            tp = next(a for a in e.barrier._state.algo_orders.values() if a.role == "tp")
            e.ex.algo(tp.client_algo_id).update(status="EXPIRED")  # пока поток был оборван, биржа отменила тейк
            await e.barrier.resync_all()
            first = (e.safe.active, tp.client_algo_id in e.barrier._state.algo_orders)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            e.ex.algo_orders.remove(e.ex.algo(sl.client_algo_id))  # а стоп вообще исчез
            await e.barrier.resync_all()
            return first, e.safe.active

    first, second = run(body())
    assert first == (False, False) and second is True


def test_trade_event_arriving_after_algo_finished_is_still_ours(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0, sequence=("a_triggering", "a_triggered", "a_finished", "trade"))
            deliver(e)  # исполнение пришло ПОСЛЕ завершения algo-ордера (тот уже удалён локально): id порождённого ордера помнится
            return e.safe.active, e.barrier._state.position(SYMBOL), len(e.barrier._unexplained), sl.client_algo_id in e.barrier._state.algo_orders

    active, pos, unexplained, still_local = run(body())
    assert not active and pos == 0.0 and unexplained == 0 and not still_local


def test_breakeven_intent_reaches_disk_before_any_network_action(tmp_path):
    async def body():
        async with algo_env(tmp_path, attempts=2) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            e.ex.market_price = 61_050.0
            e.ex.fault("algo_delete", "drop", 10)
            e.ex.fault("algo_get", "http500", 20)
            with pytest.raises(OrderStatusUnknown):
                await e.barrier.move_stop_to_breakeven(SYMBOL)  # новый стоп поставлен, исход отмены старого неизвестен
            disk = LocalState.from_json(e.store.path.read_text(encoding="utf-8"))
            return disk.protections[SYMBOL].be_done, e.safe.active

    be_done_on_disk, active = run(body())
    assert be_done_on_disk and active  # намерение «стоп на входе» записано до DELETE; неясный исход - Safe Mode


def test_mark_price_after_the_position_is_closed_requests_nothing(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            await open_position(e, be_r=1.0)
            await e.barrier.pump()
            deliver(e)
            sl = next(a for a in e.barrier._state.algo_orders.values() if a.role == "sl")
            await e.ex.trigger_algo(sl.client_algo_id, price=58_990.0)
            deliver(e)
            e.barrier.on_mark_price(SYMBOL, 61_500.0)  # защита ещё не снята pump'ом, но позиции уже нет
            return SYMBOL in e.barrier._be_requested

    assert run(body()) is False
