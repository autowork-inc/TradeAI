"""Сопровождение позиции в живом движке (порядок замены стопа, трейлинг) и ПЕРЕЗАПУСК: файл состояния + сверка с биржей.

Принцип проверок: «процесс упал» = создан новый движок (`Env.reborn()`) над ТОЙ ЖЕ биржей и ТЕМ ЖЕ файлом состояния; позиция, уровни
и стопы должны быть подхвачены и сопровождаться дальше без повторных ордеров и без падения.
"""

import json

import pytest

import config
from orchestrator.live_engine import ENTRY, OPEN, LiveExecutionError, LiveTrade
from orchestrator.types import Kind, Tick
from tests.orchestrator.fake_live_rest import api_error
from tests.orchestrator.live_helpers import STRATEGY, T0, signal
from tests.orchestrator.test_live_engine import NOW, SYM, Env, run
from utils.errors import DataError


def open_long(tmp_path, **kw) -> Env:
    env = Env(tmp_path, **kw)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))  # 1.0 SOL: SL 98, единственный тейк 108 (1:4) на весь объём
    return env


# ------------------------------------------------------------------------------------ сопровождение


def test_take_profit_at_4r_closes_the_whole_position_and_cleans_up_the_stop(tmp_path):
    env = open_long(tmp_path)
    for price in (102.0, 104.0, 106.0):  # +1R..+3R: ничего не происходит, стоп и тейк на месте
        env.rest.set_price(price)
        run(env.engine.poll())
        assert env.rest.positions[SYM] == 1.0 and not env.journal.of_kind("live_partial_close")
    env.rest.set_price(108.0)  # +4R: единственный тейк
    run(env.engine.poll())
    assert SYM not in env.engine.trades and env.rest.positions[SYM] == 0.0
    assert env.event("live_closed").detail["reason"] == "tp"
    assert all(a["algoStatus"] != "NEW" for a in env.rest.algos.values())  # стоп снят вместе с закрытием


def test_manual_partial_close_resizes_the_stop_at_the_same_price_new_stop_first(tmp_path):
    env = open_long(tmp_path)
    old_stop = env.trade().stop_cid
    n = len(env.rest.requests)
    env.rest.positions[SYM] = 0.5  # половину закрыли руками на бирже
    run(env.engine.poll())
    t = env.trade()
    assert (t.stop_price, t.stop_kind, t.stop_qty) == (98.0, "sl", 0.5) and not t.be_armed
    tail = env.rest.names()[n:]
    assert tail.index("POST algo:STOP_MARKET") < tail.index("DELETE algo")  # окна без стопа нет: новый стоп раньше снятия старого
    assert env.rest.algos[old_stop]["algoStatus"] == "CANCELED"
    assert env.rest.algos[t.stop_cid]["quantity"] == "0.5" and env.rest.algos[t.stop_cid]["triggerPrice"] == "98"


def test_mark_price_at_1r_does_not_touch_the_stop(tmp_path):
    env = open_long(tmp_path)
    n = len(env.rest.requests)
    env.rest.set_price(101.5)
    env.engine.on_tick(Tick(SYM, 102.0, NOW, "mark"))  # mark коснулась +1R, цена сделок - ещё нет
    run(env.engine.poll())
    t = env.trade()
    assert not t.be_armed and t.stop_price == 98.0 and t.stop_kind == "sl" and t.levels_done == 0
    assert "POST algo:STOP_MARKET" not in env.rest.names()[n:]  # стоп на бирже не перевыставлялся


def test_returning_to_entry_after_1r_does_not_close_the_position(tmp_path):
    env = open_long(tmp_path)
    env.engine.on_tick(Tick(SYM, 102.0, NOW, "mark"))
    env.rest.set_price(99.5)  # цена вернулась ниже входа: безубытка нет, стоп 98 на месте
    run(env.engine.poll())
    assert SYM in env.engine.trades and env.rest.positions[SYM] == 1.0 and not env.safe.active
    assert env.trade().stop_price == 98.0


def test_stop_replace_failure_is_an_emergency(tmp_path):
    env = open_long(tmp_path)
    env.rest.fail[("POST", config.ALGO_ORDER_PATH)] = [api_error(-4130, "boom")]
    env.rest.positions[SYM] = 0.5  # часть позиции закрыта руками: стоп надо уменьшить, а биржа отказывает
    with pytest.raises(LiveExecutionError):
        run(env.engine.poll())
    assert env.safe.active and env.rest.positions[SYM] == 0.0


# --------------------------------------------------------------------------------------- состояние


def test_state_is_written_before_the_entry_order_and_before_the_protective_orders(tmp_path):
    env = Env(tmp_path)
    seen = {}
    orig = env.rest.request

    async def spy(method, path, params=None, **kw):
        if (method, path) == ("POST", config.ORDER_PATH):
            rec = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))["trades"][0]
            seen["entry"] = (rec["status"], rec["entry_sent"], rec["entry_cid"] == params["newClientOrderId"])
        if (method, path) == ("POST", config.ALGO_ORDER_PATH) and "algo" not in seen:
            rec = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))["trades"][0]
            seen["algo"] = (rec["status"], rec["stop_cid"], [lvl["cid"] for lvl in rec["levels"]])
        return await orig(method, path, params, **kw)

    env.rest.request = spy
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    assert seen["entry"] == (ENTRY, True, True)  # намерение и id входа на диске ДО ордера
    status, stop_cid, level_cids = seen["algo"]
    assert status == OPEN and stop_cid and len(level_cids) == 1  # id защитных ордеров на диске ДО их выставления


def test_record_roundtrip_keeps_every_field(tmp_path):
    env = open_long(tmp_path)
    env.rest.set_price(102.0)
    run(env.engine.poll())
    t = env.trade()
    clone = LiveTrade.from_record(json.loads(json.dumps(t.to_record())))
    assert clone == t


def test_corrupt_or_foreign_state_file_stops_the_start(tmp_path):
    env = Env(tmp_path)
    (tmp_path / "state.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(DataError):
        run(env.reborn().recover([SYM]))
    (tmp_path / "state.json").write_text(json.dumps({"version": 99, "trades": []}), encoding="utf-8")
    with pytest.raises(DataError):
        run(env.reborn().recover([SYM]))
    (tmp_path / "state.json").write_text(json.dumps({"version": 1, "trades": [{"plan": {}}]}), encoding="utf-8")
    with pytest.raises(DataError):
        run(env.reborn().recover([SYM]))


# -------------------------------------------------------------------------------------- перезапуск


def test_restart_with_an_open_position_continues_without_new_orders(tmp_path):
    env = open_long(tmp_path)
    env.rest.set_price(104.0)
    run(env.engine.poll())  # +2R: ничего не происходит
    before = (len(env.rest.orders), len(env.rest.algos), dict(env.rest.positions))
    engine = env.reborn()
    run(engine.recover([SYM]))
    t = engine.trades[SYM]
    assert (t.status, t.levels_done, t.be_armed, t.stop_price, t.remaining) == (OPEN, 0, False, 98.0, 1.0)
    assert env.risk.busy() == {SYM: t.trade_id}  # монета занята восстановленной сделкой
    assert (len(env.rest.orders), len(env.rest.algos), dict(env.rest.positions)) == before  # ни одного нового ордера
    assert env.event("live_recovered").detail["levels_done"] == 0
    env.rest.set_price(108.0)  # продолжение: единственный тейк закрывает всё
    run(engine.poll())
    assert SYM not in engine.trades and env.risk.busy() == {} and env.rest.positions[SYM] == 0.0
    assert [e.detail["reason"] for e in env.journal.of_kind("live_closed")] == ["tp"]


def test_take_profit_that_fired_while_the_bot_was_down_closes_the_trade_as_tp(tmp_path):
    env = open_long(tmp_path)
    env.rest.set_price(108.0)  # тейк сработал, пока процесса не было (poll не вызывался)
    assert env.rest.positions[SYM] == 0.0
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert SYM not in engine.trades and env.risk.busy() == {}
    assert env.event("live_closed").detail["reason"] == "tp"
    assert all(a["algoStatus"] != "NEW" for a in env.rest.algos.values())  # стоп снят


def test_stop_that_fired_while_the_bot_was_down_closes_the_trade_and_cleans_up(tmp_path):
    env = open_long(tmp_path)
    env.rest.set_price(97.0)  # стоп 98 сработал в отсутствие бота
    assert env.rest.positions[SYM] == 0.0
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert SYM not in engine.trades and env.risk.busy() == {}
    assert env.event("live_closed").detail["reason"] == "sl"
    assert all(a["algoStatus"] != "NEW" for a in env.rest.algos.values())  # уровни сняты
    assert json.loads((tmp_path / "state.json").read_text())["trades"] == []


def test_protection_that_was_planned_but_never_placed_is_placed_after_restart(tmp_path):
    env = open_long(tmp_path)
    env.rest.algos.clear()  # упали между записью плана и выставлением защиты: на бирже её нет
    engine = env.reborn()
    run(engine.recover([SYM]))
    placed = sorted((a["orderType"], a["triggerPrice"], a["quantity"]) for a in env.rest.algos.values())
    assert placed == [("STOP_MARKET", "98", "1"), ("TAKE_PROFIT_MARKET", "108", "1")]
    assert all(a["algoStatus"] == "NEW" for a in env.rest.algos.values())


def test_restart_with_a_pending_limit_keeps_waiting_and_then_protects(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    cid = env.trade().entry_cid
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert engine.trades[SYM].status == ENTRY and len(env.rest.orders) == 1 and env.rest.algos == {}
    env.rest.fill_limit(cid)
    run(engine.poll())
    assert engine.trades[SYM].status == OPEN and len(env.rest.algos) == 2


def test_entry_that_the_exchange_does_not_know_after_restart_is_cancelled(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    env.rest.orders.clear()  # (запись «отправлен», а на бирже ордера нет)
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert SYM not in engine.trades and env.risk.busy() == {}
    assert env.event("live_trade_cancelled").detail["reason"] == "entry_not_found"


def test_trade_that_never_sent_its_entry_is_dropped_on_restart(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    state["trades"][0]["entry_sent"] = False  # упали после записи намерения, до отправки ордера
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")
    env.rest.orders.clear()
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert SYM not in engine.trades and env.risk.busy() == {} and env.rest.orders == {}
    assert env.event("live_trade_cancelled").detail["reason"] == "never_sent"


# ------------------------------------------------------------------- позиции без записи сделки


def place_manual_orders(env, *, with_stop=True):
    async def body():
        env.rest.positions[SYM], env.rest.entry_prices[SYM] = 1.0, 100.0
        if with_stop:
            await env.client.stop_loss(SYM, "SELL", 98.0, client_id="manualSL", qty=1.0)
        await env.client.take_profit(SYM, "SELL", 102.0, client_id="manualTP1", qty=0.5)
        await env.client.take_profit(SYM, "SELL", 106.0, client_id="manualTP2", qty=0.4)

    run(body())


def test_position_with_exchange_stop_but_no_record_is_adopted_and_managed(tmp_path):
    env = Env(tmp_path)
    place_manual_orders(env)
    run(env.engine.recover([SYM]))
    t = env.engine.trades[SYM]
    assert (t.plan.strategy, t.adopted, t.stop_price, t.stop_cid, t.remaining) == ("adopted", True, 98.0, "manualSL", 1.0)
    assert [(v.r, v.qty, v.price) for v in t.levels] == [(1.0, 0.5, 102.0), (3.0, 0.4, 106.0)]
    assert env.risk.busy() == {SYM: t.trade_id} and env.event("live_adopted").detail["stop"] == 98.0
    n_algos = len(env.rest.algos)
    env.rest.set_price(102.0)
    run(env.engine.poll())  # дальше - обычное сопровождение: уровень
    t = env.engine.trades[SYM]
    assert t.levels_done == 1 and t.stop_price == 98.0 and env.rest.positions[SYM] == 0.5
    assert len(env.rest.algos) == n_algos + 1  # один новый стоп; чужие ордера не дублировались


def test_position_without_a_stop_and_without_a_record_is_not_touched_but_blocks_trading(tmp_path):
    env = Env(tmp_path)
    place_manual_orders(env, with_stop=False)
    with pytest.raises(LiveExecutionError, match="без защитного стопа"):
        run(env.engine.recover([SYM]))
    assert env.safe.active and env.rest.positions[SYM] == 1.0  # позицию не закрываем: решает оператор


def test_orphan_bot_algo_orders_without_a_position_are_cancelled_but_foreign_ones_are_left(tmp_path):
    env = Env(tmp_path)

    async def setup():
        env.rest.price = 100.0
        await env.client.stop_loss(SYM, "SELL", 90.0, client_id="tbS0_leftover", qty=1.0)
        await env.client.stop_loss(SYM, "SELL", 90.0, client_id="alienStop", qty=1.0)

    run(setup())
    run(env.engine.recover([SYM]))
    assert env.rest.algos["tbS0_leftover"]["algoStatus"] == "CANCELED" and env.rest.algos["alienStop"]["algoStatus"] == "NEW"
    assert env.event("live_orphan_algo_cancelled").detail["client_algo_id"] == "tbS0_leftover"


def test_restart_without_anything_to_recover_is_a_noop(tmp_path):
    env = Env(tmp_path)
    run(env.engine.recover([SYM]))
    assert env.engine.trades == {} and env.rest.orders == {} and env.rest.algos == {}
