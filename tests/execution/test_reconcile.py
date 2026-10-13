import asyncio
import json

import pytest

import config
from backtester.contracts import OrderType, Side
from execution.reconcile import (
    DiscrepancyKind as K,
    ExchangeOrder,
    compare_states,
    parse_orders,
    parse_positions,
    reconcile,
)
from execution.state import LocalOrder, LocalState, SafeMode, StateStore
from tests.execution.fake_exchange import FakeExchange
from tests.execution.helpers import RecordingAlerter, make_env
from utils.errors import DataError, RateLimitError

CID = "tb-abc-0123456789abcdef0123"


def run(coro):
    return asyncio.run(coro)


def local_order(cid=CID, *, qty=0.5, price=59_000.0, filled=0.0, status="NEW", side=Side.BUY, symbol="BTCUSDT"):
    return LocalOrder(cid, 1, symbol, side, OrderType.LIMIT, qty, price, 1000, 1, 2, 1002, False, status, filled)


async def do_reconcile(e, **kw):
    return await reconcile(e.rest, e.state, e.safe, store=e.store, **kw)


def kinds(report):
    return sorted(d.kind.value for d in report.discrepancies)


# ---------------------------------------------------------------- чистые случаи


def test_flat_both_sides_is_clean(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert report.clean and report.verified and not e.safe.active and e.alerter.alerts == []
    assert sorted(set(r.split()[1] for r in e.ex.requests)) == ["/fapi/v1/openOrders", "/fapi/v3/positionRisk"]


def test_matching_position_and_order_are_clean(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 0.5)
            e.ex.seed_order(CID, qty=0.5, price=59_000.0)
            e.state.positions["BTCUSDT"] = 0.5
            e.state.orders[CID] = local_order()
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert report.clean and not e.safe.active
    assert report.exchange_positions == {"BTCUSDT": 0.5}
    assert CID in e.state.orders  # живой ордер остался


def test_position_difference_within_tolerance_is_clean_and_adopts_exchange_value(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 0.5)
            e.state.positions["BTCUSDT"] = 0.5 + 1e-12
            return e, await do_reconcile(e, tol=1e-9)

    e, report = run(scenario())
    assert report.clean and e.state.positions == {"BTCUSDT": 0.5}
    assert json.loads(e.store.path.read_text())["positions"] == {"BTCUSDT": 0.5}


# ----------------------------------------------------- расхождения -> Safe Mode


def test_exchange_has_position_bot_does_not_enters_safe_mode(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 0.5)  # бот упал после исполнения
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert not report.clean and report.safe_mode_entered and e.safe.active
    (d,) = report.discrepancies
    assert d.kind is K.POSITION_MISMATCH and (d.local, d.exchange) == (0.0, 0.5) and "о которой бот не знает" in d.detail
    assert len(e.alerter.alerts) == 1 and "SAFE MODE" in e.alerter.alerts[0][0]
    assert e.alerter.alerts[0][1]["discrepancies"][0]["kind"] == "position_mismatch"


def test_bot_has_position_exchange_closed_by_stop_enters_safe_mode(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.state.positions["BTCUSDT"] = -0.3  # на бирже закрыта по стопу
            return e, await do_reconcile(e)

    e, report = run(scenario())
    (d,) = report.discrepancies
    assert d.kind is K.POSITION_MISMATCH and "по стопу" in d.detail and e.safe.active
    assert e.state.positions == {"BTCUSDT": -0.3}  # локальное состояние сохранено для разбора, не затёрто


def test_sign_flip_is_a_mismatch(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", -0.5)
            e.state.positions["BTCUSDT"] = 0.5
            return await do_reconcile(e)

    report = run(scenario())
    assert kinds(report) == ["position_mismatch"] and "объёмы различаются" in report.discrepancies[0].detail


def test_unknown_order_on_exchange(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order("web-ui-order", qty=1.0)
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert kinds(report) == ["unknown_exchange_order"] and e.safe.active
    assert "наш префикс" not in report.discrepancies[0].detail


def test_unknown_order_with_our_prefix_hints_lost_state(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order(CID, qty=1.0)
            return await do_reconcile(e)

    assert "наш префикс" in run(scenario()).discrepancies[0].detail


@pytest.mark.parametrize("mutate,fragment", [
    (lambda ex: ex.order(CID).update(qty=0.7), "объём"),
    (lambda ex: ex.order(CID).update(price=58_000.0), "цена"),
    (lambda ex: ex.order(CID).update(executed=0.2), "исполнено"),
    (lambda ex: ex.order(CID).update(side="SELL"), "символ/сторона"),
])
def test_order_parameters_mismatch(tmp_path, mutate, fragment):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order(CID, qty=0.5, price=59_000.0)
            mutate(e.ex)
            e.state.orders[CID] = local_order()
            return e, await do_reconcile(e)

    e, report = run(scenario())
    mismatch = [d for d in report.discrepancies if d.kind is K.ORDER_MISMATCH]
    assert len(mismatch) == 1 and fragment in mismatch[0].detail and e.safe.active


def test_own_order_filled_while_bot_was_down(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order(CID, qty=0.5, price=59_000.0, executed=0.5, status="FILLED")
            e.ex.seed_position("BTCUSDT", 0.5)
            e.state.orders[CID] = local_order()
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert kinds(report) == ["order_resolved_offline", "position_mismatch"] and e.safe.active
    assert "пропустил исполнения" in next(d for d in report.discrepancies if d.kind is K.ORDER_RESOLVED_OFFLINE).detail


def test_order_that_was_on_exchange_and_vanished(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.state.orders[CID] = local_order(status="NEW")  # бот видел его живым, биржа не знает
            return await do_reconcile(e)

    assert kinds(run(scenario())) == ["order_resolved_offline"]


def test_hedge_mode_position_is_unsupported(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.hedge_positions.append({"symbol": "BTCUSDT", "positionAmt": "0.1", "positionSide": "LONG"})
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert kinds(report) == ["unsupported_position_mode"] and e.safe.active


def test_hedge_entry_with_zero_amount_is_ignored(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.hedge_positions += [{"symbol": "BTCUSDT", "positionAmt": "0.000", "positionSide": s} for s in ("LONG", "SHORT")]
            return await do_reconcile(e)

    assert run(scenario()).clean


def test_alert_is_sent_once_per_safe_mode_even_if_reconcile_repeats(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 0.5)
            first, second = await do_reconcile(e), await do_reconcile(e)
            return e, first, second

    e, first, second = run(scenario())
    assert first.safe_mode_entered and not second.safe_mode_entered and not second.clean
    assert len(e.alerter.alerts) == 1


# ------------------------------------------------------- безобидные расхождения


def test_pending_order_that_never_reached_exchange_is_dropped(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.state.orders[CID] = local_order(status="PENDING")  # процесс умер между записью и POST
            e.store.save(e.state)
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert report.clean and not e.safe.active and e.state.orders == {}
    assert json.loads(e.store.path.read_text())["orders"] == [] and "не дошёл до биржи" in report.notes[0]
    assert ("GET /fapi/v1/order" in e.ex.requests)  # судьбу спросили по clientOrderId


def test_order_cancelled_without_fills_while_down_is_dropped(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order(CID, status="CANCELED")
            e.state.orders[CID] = local_order()
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert report.clean and e.state.orders == {} and "CANCELED" in report.notes[0]


def test_pending_order_that_did_reach_exchange_and_fills_is_a_discrepancy(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_order(CID, qty=0.5, executed=0.5, status="FILLED")
            e.ex.seed_position("BTCUSDT", 0.5)
            e.state.orders[CID] = local_order(status="PENDING")  # POST дошёл, ответ потерян, процесс умер
            return await do_reconcile(e)

    assert "order_resolved_offline" in kinds(run(scenario()))


# --------------------------------------------------------------- fail-closed


@pytest.mark.parametrize("fault_name,kind", [
    ("position_risk", "drop"), ("open_orders", "drop"), ("open_orders", "http500"), ("position_risk", "bad_key"),
])
def test_cannot_verify_means_safe_mode(tmp_path, fault_name, kind):
    async def scenario():
        async with make_env(tmp_path) as e:
            # aiohttp сам повторяет идемпотентный GET один раз при обрыве keep-alive-соединения (POST - никогда)
            e.ex.fault(fault_name, kind, times=3)
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert not report.verified and not report.clean and e.safe.active and kinds(report) == ["unverified"]
    assert "торговать вслепую нельзя" in e.alerter.alerts[0][0]


def test_unparseable_exchange_answers_raise_data_error(tmp_path):
    # DataError - TradingInfraError, поэтому reconcile() превращает его в Safe Mode (см. test_cannot_verify_...)
    with pytest.raises(DataError):
        parse_positions({"not": "a list"})
    with pytest.raises(DataError):
        parse_orders([{"clientOrderId": "x"}])
    with pytest.raises(DataError):
        parse_positions([{"symbol": "BTCUSDT", "positionAmt": "abc"}])


def test_rate_limit_is_not_a_discrepancy_and_is_not_swallowed(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.fault("position_risk", "http429")
            with pytest.raises(RateLimitError) as info:
                await do_reconcile(e)
            return e, info.value

    e, err = run(scenario())
    assert err.retry_after == 45 and not e.safe.active and e.alerter.alerts == []  # .ban_lock запишет runner


def test_safe_mode_latch_survives_restart_and_clean_reconcile_does_not_release_it(tmp_path):
    async def first_run():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 0.5)
            await do_reconcile(e)
            return e.safe.path

    latch = run(first_run())
    assert latch.exists() and json.loads(latch.read_text(encoding="utf-8"))["reason"].startswith("Расхождение")

    async def second_run():  # новый процесс: SafeMode читает защёлку с диска; на бирже уже всё чисто
        async with make_env(tmp_path) as e:
            assert e.safe.active and "Расхождение" in e.safe.reason
            report = await do_reconcile(e)
            return e, report

    e, report = run(second_run())
    assert report.clean and e.safe.active  # сверка чистая, но защёлку снимает только человек
    e.safe.clear("оператор")
    assert not e.safe.active and not latch.exists()
    assert not SafeMode(latch, RecordingAlerter()).active


def test_corrupted_latch_is_treated_as_active(tmp_path):
    path = tmp_path / ".safe_mode"
    path.write_text("{oops", encoding="utf-8")
    assert SafeMode(path, RecordingAlerter()).active


def test_symbols_filter_uses_per_symbol_open_orders_and_ignores_others(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("DOGEUSDT", 1000.0)  # чужой символ вне зоны бота
            e.ex.seed_order("other", symbol="DOGEUSDT")
            report = await do_reconcile(e, symbols=("BTCUSDT", "ETHUSDT"))
            return e, report

    e, report = run(scenario())
    assert report.clean
    assert e.ex.requests.count("GET /fapi/v1/openOrders") == 2  # по символу (вес 1), а не общий (вес 40)


def test_discrepancy_does_not_overwrite_state_file(tmp_path):
    async def scenario():
        async with make_env(tmp_path) as e:
            e.ex.seed_position("BTCUSDT", 1.0)
            return e, await do_reconcile(e)

    e, report = run(scenario())
    assert e.safe.active and not e.store.path.exists()  # при расхождении состояние не перезаписывается


# ------------------------------------------------------------ чистое сравнение / хранилище


def test_compare_states_is_pure_and_reports_all_findings():
    state = LocalState(positions={"BTCUSDT": 1.0, "ETHUSDT": 2.0}, orders={CID: local_order()})
    exch = {"x": ExchangeOrder("x", "BTCUSDT", Side.BUY, "LIMIT", 1.0, 1.0, 0.0, "NEW")}
    found, notes, drop = compare_states(state, {"BTCUSDT": 1.0}, [], exch, {CID: None}, tol=1e-9)
    assert sorted(d.kind.value for d in found) == ["order_resolved_offline", "position_mismatch", "unknown_exchange_order"]
    assert {d.symbol for d in found if d.kind is K.POSITION_MISMATCH} == {"ETHUSDT"} and drop == []


def test_local_state_roundtrip_and_corruption(tmp_path):
    store = StateStore(tmp_path / "s.json")
    assert store.load() == LocalState()  # первый запуск
    state = LocalState({"BTCUSDT": -0.25}, {CID: local_order(filled=0.1, status="PARTIALLY_FILLED")}, 7)
    store.save(state)
    assert store.load() == state
    store.path.write_text('{"version": 1, "positions": {}', encoding="utf-8")
    with pytest.raises(DataError):
        store.load()  # битый файл не подменяется пустым состоянием ("позиций нет")
    store.path.write_text(json.dumps({"version": 99, "positions": {}, "orders": [], "next_ticket_id": 1}), encoding="utf-8")
    with pytest.raises(DataError):
        store.load()


def test_default_paths_follow_config(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "d")
    assert StateStore().path == tmp_path / "d" / config.LIVE_STATE_NAME
    assert SafeMode(None, RecordingAlerter()).path == tmp_path / "d" / config.SAFE_MODE_LOCK_NAME
