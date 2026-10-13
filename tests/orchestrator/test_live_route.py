"""LiveBarrierSink: сигналы Оркестратора -> arm_protection + вход -> защита -> безубыток -> закрытие, на имитации биржи с Algo API.

Весь путь настоящий (Оркестратор, блокировка монеты, приёмник, LiveBarrier, REST к FakeAlgoExchange); подделаны только биржа и
поток событий: события приватного потока доставляются вручную (deliver), чтобы управлять порядком.
"""

import asyncio
import json
import time
from dataclasses import dataclass

import pytest

import config
from backtester.contracts import Fill
from execution.live_barrier import LiveBarrier
from execution.state import StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, parse_event
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.live_route import ENDING, OPEN, LiveBarrierSink, LiveTradeStore
from orchestrator.types import Direction, Kind, Mode, Tick
from tests.execution.fake_algo import FakeAlgoExchange
from tests.execution.helpers import EXEC_CFG, make_env
from tests.orchestrator.helpers import IV, Scripted, ctx, mk
from utils.errors import ConfigError, DataError

SYM = "BTCUSDT"


def run(coro):
    return asyncio.run(coro)


def kinds(journal):
    return [e.kind for e in journal.events]


def deliver(e) -> None:
    events = [parse_event(json.dumps(x)) for x in e.ex.pushed]
    e.ex.pushed.clear()
    for ev in events:
        if isinstance(ev, OrderUpdate):
            e.barrier.on_order_update(ev)
        elif isinstance(ev, AccountUpdate):
            e.barrier.on_account_update(ev)
        elif isinstance(ev, AlgoUpdate):
            e.barrier.on_algo_update(ev)


@dataclass
class H:
    e: object
    sink: LiveBarrierSink
    orch: Orchestrator
    journal: MemoryEventLog
    store: LiveTradeStore
    base: int  # номер свечи, на закрытии которой «пришёл» первый сигнал

    def bar(self, k: int) -> int:
        return self.base + k

    async def feed(self, k: int, close: float = 60_000.0, symbol: str = SYM):
        """Свеча номер base+k закрылась: Оркестратор + шаг исполнителя приёмника."""
        events = self.orch.on_context(ctx(symbol, self.bar(k), close, received=(self.bar(k) + 1) * IV + 1000))
        await self.sink.step()
        deliver(self.e)
        return events

    async def step(self) -> None:
        deliver(self.e)
        await self.sink.step()
        deliver(self.e)

    def cid(self) -> str:
        return next(iter(self.e.state.orders))

    def alive(self, type_=None):
        return [a for a in self.e.ex.algo_orders if a["status"] == "NEW" and (type_ is None or a["type"] == type_)]


def make_harness(e, tmp_path, by_bar, *, strategies=("smc",), equity=10_000.0, bars_ago=1, **sink_kw) -> H:
    base = int(time.time() * 1000) // IV - bars_ago  # сигнальная свеча только что закрылась: срок жизни LIMIT впереди
    journal = MemoryEventLog()
    store = LiveTradeStore(tmp_path / "live_trades.json")

    async def equity_source() -> float:
        return equity

    sink = LiveBarrierSink(e.barrier, e.safe, journal, store, equity_source, e.rest.now_ms, interval_ms=IV, symbols=(SYM,), **sink_kw)
    scripted = [Scripted(name, {base + k: sigs for k, sigs in by_bar.get(name, {}).items()}) for name in strategies]
    orch = Orchestrator(scripted, {s.name: Route(Mode.LIVE, sink) for s in scripted}, journal)
    return H(e, sink, orch, journal, store, base)


def algo_env(tmp_path, **kw):
    return make_env(tmp_path, exchange=FakeAlgoExchange(kw.pop("market_price", 60_000.0)), **kw)


def short_limit(h: H, k: int = 0, **kw):
    kw = {"entry": 60_500.0, "sl": 61_000.0, "tp": 59_000.0, "inval": 61_200.0, **kw}
    return mk(direction=Direction.SHORT, kind=Kind.LIMIT, bar=h.base + k, **kw)


def long_limit(h: H, k: int = 0, **kw):
    kw = {"entry": 59_500.0, "sl": 59_000.0, "tp": 61_000.0, "inval": 58_800.0, **kw}
    return mk(direction=Direction.LONG, kind=Kind.LIMIT, bar=h.base + k, **kw)


async def accepted_short(e, tmp_path, **kw):
    """Harness + принятый сигнал на шорт (LIMIT 60 500, SL 61 000, TP 59 000, qty 0.2 при эквити 10 000)."""
    h = make_harness(e, tmp_path, {}, **kw)
    sig = short_limit(h)
    h.orch._strategies[0].by_bar[h.base] = [sig]
    await h.sink.refresh_equity()
    events = await h.feed(0)
    return h, sig, events


async def fill_entry(h: H, qty: float | None = None, price: float = 60_500.0):
    cid = h.cid()
    await h.e.ex.fill(cid, qty or h.e.state.orders[cid].qty, price=price)
    await h.step()


# ================================================================================= приём сигнала


def test_signal_is_sized_logged_before_any_order_and_sent_post_only(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            seen = {}

            def pre_order(params):  # в момент POST: журнал и защита уже записаны
                seen.update(journal=kinds(h.journal), protection=SYM in StateStore(e.store.path).load().protections, order=dict(params))

            e.ex.hooks["pre_order"] = pre_order
            h.orch._strategies[0].by_bar[h.base] = [short_limit(h)]
            await h.sink.refresh_equity()
            events = h.orch.on_context(ctx(SYM, h.base, 60_000.0, received=(h.base + 1) * IV + 1000))
            assert not e.ex.post_log  # submit() сам ничего не отправляет
            intent_first = kinds(h.journal)
            await h.sink.step()
            return events, intent_first, seen, h.sink.trades[SYM], h.store.load()

    events, intent_first, seen, trade, stored = run(body())
    assert [x.kind for x in events] == ["signal_accepted"] and intent_first == ["live_intent", "signal_accepted"]
    assert seen["journal"] == ["live_intent", "signal_accepted"]  # R7: причина входа записана до ордера
    assert seen["protection"]  # R6: намерение защиты на диске раньше входа
    o = seen["order"]
    assert (o["side"], o["type"], o["quantity"], o["price"], o["timeInForce"]) == ("SELL", "LIMIT", "0.2", "60500", "GTX")
    assert trade.qty == pytest.approx(0.2) and trade.risk_amount == pytest.approx(100.0) and [t.trade_id for t in stored] == [trade.trade_id]


@pytest.mark.parametrize("what, reason", [("safe", "safe_mode"), ("stale", "equity_unavailable"), ("tiny", "below_min_size"), ("leverage", "exposure")])
def test_rejections_are_values_not_exceptions_and_release_the_lock(tmp_path, what, reason):
    async def body():
        async with algo_env(tmp_path) as e:
            equity = {"tiny": 50.0}.get(what, 10_000.0)
            h = make_harness(e, tmp_path, {}, equity=equity)
            sig = short_limit(h, sl=60_520.0 if what == "leverage" else 61_000.0)
            h.orch._strategies[0].by_bar[h.base] = [sig]
            if what != "stale":
                await h.sink.refresh_equity()
            if what == "safe":
                e.safe.trip("тест")
            events = await h.feed(0)
            return events, h.orch.held(), e.ex.post_log, h.sink.trades

    events, held, orders, trades = run(body())
    assert [x.kind for x in events] == ["skipped_risk"] and events[0].detail["reason"] == reason
    assert held == {} and orders == [] and trades == {}


def test_unknown_symbol_and_duplicate_signal_are_bugs_not_rejections(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            await h.sink.refresh_equity()
            with pytest.raises(ConfigError):
                h.sink.submit(mk(symbol="XRPUSDT", entry=2.0, sl=2.1, tp=1.5, inval=2.2), 1)
            sig = short_limit(h)
            assert h.sink.submit(sig, 1).accepted
            with pytest.raises(DataError, match="уже принят"):
                h.sink.submit(sig, 1)

    run(body())


def test_volume_is_rounded_down_to_the_lot_step_so_risk_never_exceeds_one_percent(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            sig = short_limit(h, sl=60_800.0, inval=61_000.0)  # R = 300: 100 / 300 = 0.3333...
            h.orch._strategies[0].by_bar[h.base] = [sig]
            await h.sink.refresh_equity()
            await h.feed(0)
            return h.sink.trades[SYM], e.ex.post_log[-1]["quantity"]

    trade, wire_qty = run(body())
    assert trade.qty == pytest.approx(0.333) and wire_qty == "0.333" and trade.risk_amount <= 100.0


def test_trade_is_not_finished_while_protection_is_still_on_the_exchange(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            await fill_entry(h)
            sl = h.alive("STOP_MARKET")[0]
            await e.ex.trigger_algo(sl["cid"], price=61_000.0, expire_siblings=False)  # тейк-«брат» ещё жив
            deliver(e)
            h.sink._poll(0)  # позиция списана, но защита не снята: сделка обязана остаться, монета - занята
            before = (SYM in h.sink.trades, h.sink.trades[SYM].status, h.orch.held())
            await h.step()
            return before, SYM in h.sink.trades, h.orch.held()

    before, after_trades, after_held = run(body())
    assert before[0] and before[1] == ENDING and before[2]
    assert not after_trades and after_held == {}  # pump снял защиту - сделка закрыта, монета свободна


# ===================================================================== жизненный цикл: шорт и лонг


def test_short_limit_fills_is_protected_moves_to_breakeven_and_closes_by_be_releasing_the_lock(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            assert h.alive() == []  # входа ещё нет: стопа без позиции нет
            await fill_entry(h)
            stops_after_fill = sorted((a["type"], a["side"], a["quantity"], a["trigger"], a["reduce_only"]) for a in h.alive())
            e.ex.market_price = 60_000.0
            h.sink.on_tick(Tick(SYM, 60_000.0, 1, "mark"))  # mark дошла до +1R (вход 60 500, R = 500)
            await h.step()
            stops_after_be = sorted((a["type"], a["trigger"]) for a in h.alive())
            held_open = h.orch.held()
            sl_now = next(a for a in h.alive("STOP_MARKET"))
            await e.ex.trigger_algo(sl_now["cid"], price=60_510.0)
            deliver(e)
            before_pump = (SYM in h.sink.trades, h.orch.held())
            await h.step()
            return (h, stops_after_fill, stops_after_be, held_open, before_pump)

    h, after_fill, after_be, held_open, before_pump = run(body())
    assert before_pump[0] and before_pump[1]  # позиция списана, но защита ещё не снята: монета остаётся занятой
    assert after_fill == [("STOP_MARKET", "BUY", 0.2, 61_000.0, True), ("TAKE_PROFIT_MARKET", "BUY", 0.2, 59_000.0, True)]
    assert after_be == [("STOP_MARKET", 60_500.0), ("TAKE_PROFIT_MARKET", 59_000.0)]  # SL перенесён на вход, тейк на месте
    assert held_open  # пока сделка жива, монета заблокирована
    closed = [x for x in h.journal.events if x.kind == "live_trade_closed"]
    assert len(closed) == 1 and closed[0].detail["reason"] == "be" and -0.2 < closed[0].detail["net_r"] < 0.0  # BE минус комиссии
    assert "be_armed" in kinds(h.journal) and h.orch.held() == {} and h.sink.trades == {}  # монета свободна
    assert h.e.state.protections == {} and h.e.state.algo_orders == {}


def test_take_profit_and_stop_loss_exits_have_the_right_reason_and_r(tmp_path):
    async def one(exit_role, price):
        async with algo_env(tmp_path / exit_role) as e:
            h, _, _ = await accepted_short(e, tmp_path / exit_role)
            await fill_entry(h)
            order = next(a for a in h.alive() if a["type"] == exit_role)
            e.ex.market_price = price
            await e.ex.trigger_algo(order["cid"], price=price)
            await h.step()
            return [x for x in h.journal.events if x.kind == "live_trade_closed"], h.orch.held()

    (tp,), held1 = run(one("TAKE_PROFIT_MARKET", 59_000.0))
    (sl,), held2 = run(one("STOP_MARKET", 61_000.0))
    assert tp.detail["reason"] == "tp" and 2.8 < tp.detail["net_r"] < 3.0  # 3R минус комиссии
    assert sl.detail["reason"] == "sl" and -1.2 < sl.detail["net_r"] < -1.0
    assert held1 == {} and held2 == {}


def test_long_limit_is_bought_with_mirrored_protection(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            sig = long_limit(h)
            h.orch._strategies[0].by_bar[h.base] = [sig]
            await h.sink.refresh_equity()
            await h.feed(0)
            o = e.ex.post_log[-1]
            await fill_entry(h, price=59_500.0)
            return o, sorted((a["type"], a["side"], a["trigger"], a["quantity"]) for a in h.alive())

    o, protection = run(body())
    assert (o["side"], o["type"], o["price"], o["quantity"]) == ("BUY", "LIMIT", "59500", "0.2")
    assert protection == [("STOP_MARKET", "SELL", 59_000.0, 0.2), ("TAKE_PROFIT_MARKET", "SELL", 61_000.0, 0.2)]


def test_market_signal_enters_at_once_and_tp_is_3r_from_the_actual_fill(tmp_path):
    async def body():
        async with algo_env(tmp_path, market_price=60_100.0) as e:
            h = make_harness(e, tmp_path, {}, strategies=("breakout",))
            sig = mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=60_000.0, sl=59_000.0, bar=h.base)
            h.orch._strategies[0].by_bar[h.base] = [sig]
            await h.sink.refresh_equity()
            await h.feed(0, close=60_000.0)
            await h.step()
            return e.ex.post_log[0], sorted((a["type"], a["trigger"]) for a in h.alive()), e.ex.positions[SYM]

    order, protection, position = run(body())
    assert order["type"] == "MARKET" and order["side"] == "BUY" and position == pytest.approx(0.1)
    # исполнение по 60 100 (проскальзывание): R = 1 100 от факта, тейк = 60 100 + 3 * 1 100; стоп остаётся на рассчитанном уровне
    assert protection == [("STOP_MARKET", 59_000.0), ("TAKE_PROFIT_MARKET", 63_400.0)]


# ============================================================================= отмены и сроки


def test_close_beyond_the_sweep_high_cancels_the_unfilled_limit_and_frees_the_coin(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            await h.feed(1, close=61_300.0)  # закрытие выше inval 61 200
            return h, [x for x in h.journal.events if x.kind == "live_trade_cancelled"]

    h, cancelled = run(body())
    assert len(cancelled) == 1 and cancelled[0].detail["reason"] == "invalidated"
    assert h.orch.held() == {} and h.sink.trades == {} and h.e.state.protections == {} and h.e.state.orders == {}


def test_price_reaching_tp_before_the_fill_cancels_the_limit(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            h.sink.on_tick(Tick(SYM, 60_100.0, 4, "trade"))  # цена до TP не дошла (шорт: TP ниже)
            await h.step()
            still_alive = SYM in h.sink.trades and bool(e.state.orders)
            h.sink.on_tick(Tick(SYM, 58_900.0, 5, "trade"))  # цена прошла TP, лимитка не исполнена
            await h.step()
            return h, still_alive

    h, still_alive = run(body())
    assert still_alive
    assert [x.detail["reason"] for x in h.journal.events if x.kind == "live_trade_cancelled"] == ["tp_before_fill"]
    assert h.orch.held() == {}


def test_limit_that_expires_unfilled_is_a_cancelled_trade(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path, limit_ttl_bars=1, bars_ago=5)  # свеча закрылась 5 баров назад, срок - 1 бар
            await h.step()
            await h.step()  # первый шаг отменил лимитку, второй снял защиту
            return h

    h = run(body())
    assert [x.detail["reason"] for x in h.journal.events if x.kind == "live_trade_cancelled"] == ["expired"]
    assert h.orch.held() == {} and h.e.state.protections == {}


def test_open_position_is_closed_by_the_bot_after_max_hold_bars(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path, max_hold_bars=2)
            await fill_entry(h)
            e.ex.market_price = 60_300.0
            await h.feed(1, close=60_300.0)
            await h.feed(2, close=60_300.0)
            assert SYM in h.sink.trades and e.ex.positions[SYM] == pytest.approx(-0.2)  # ещё держим (исполнено на баре 1)
            await h.feed(3, close=60_300.0)  # 2 свечи удержания по правилу трекера (bars_closed - fill_bar_no >= 2)
            await h.step()
            await h.step()
            return h, e.ex.positions[SYM], e.ex.post_log[-1]

    h, position, last_order = run(body())
    closed = [x for x in h.journal.events if x.kind == "live_trade_closed"]
    assert len(closed) == 1 and closed[0].detail["reason"] == "timeout" and position == 0.0
    assert last_order["reduceOnly"] == "true" and last_order["type"] == "MARKET" and last_order["side"] == "BUY"
    assert h.orch.held() == {} and h.e.state.protections == {}


def test_safe_mode_between_acceptance_and_send_means_no_order(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            sig = short_limit(h)
            h.orch._strategies[0].by_bar[h.base] = [sig]
            await h.sink.refresh_equity()
            events = h.orch.on_context(ctx(SYM, h.base, 60_000.0, received=(h.base + 1) * IV + 1000))
            e.safe.trip("между приёмом и отправкой")
            await h.sink.step()
            return events, e.ex.post_log, h

    events, orders, h = run(body())
    assert [x.kind for x in events] == ["signal_accepted"] and orders == []
    assert [x.detail["reason"] for x in h.journal.events if x.kind == "live_trade_cancelled"] == ["safe_mode"]
    assert h.orch.held() == {}


# ====================================================================== частичные исполнения


def test_partial_entry_is_protected_by_volume_and_remainder_expiry_keeps_the_trade(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            await fill_entry(h, qty=0.05)
            part = sorted((a["type"], a["quantity"]) for a in h.alive())
            await e.ex.fill(h.cid(), 0.15, price=60_500.0)
            await h.step()
            full = sorted((a["type"], a["quantity"]) for a in h.alive())
            return part, full, h.sink.trades[SYM].status, h.sink.trades[SYM].filled_qty

    part, full, status, filled = run(body())
    assert part == [("STOP_MARKET", 0.05), ("TAKE_PROFIT_MARKET", 0.05)]
    assert full == [("STOP_MARKET", 0.05), ("STOP_MARKET", 0.15), ("TAKE_PROFIT_MARKET", 0.05), ("TAKE_PROFIT_MARKET", 0.15)]
    assert status == OPEN and filled == pytest.approx(0.2)


# ================================================================================ перезапуск


def restart_from_disk(e, tmp_path, by_bar_builder=None):
    """Новый процесс: состояние барьера и сделок читается с диска, Оркестратор и приёмник создаются заново."""
    state2 = StateStore(e.store.path).load()
    barrier2 = LiveBarrier(e.rest, EXEC_CFG, state2, e.safe, store=e.store, attempts=3, backoff_s=0.0, sleep=lambda s: asyncio.sleep(0))
    journal = MemoryEventLog()

    async def equity_source() -> float:
        return 10_000.0

    sink2 = LiveBarrierSink(barrier2, e.safe, journal, LiveTradeStore(tmp_path / "live_trades.json"), equity_source, e.rest.now_ms,
                            interval_ms=IV, symbols=(SYM,))
    return state2, barrier2, sink2, journal


def test_open_trade_survives_a_restart_and_keeps_the_coin_locked(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h, sig, _ = await accepted_short(e, tmp_path)
            await fill_entry(h)
            state2, barrier2, sink2, journal2 = restart_from_disk(e, tmp_path)
            assert sink2.orphans() == [SYM]  # без файла сделок такой старт был бы небезопасен
            scripted = Scripted("smc", {h.base + 1: [short_limit(h, 1)]})
            orch2 = Orchestrator([scripted], {"smc": Route(Mode.LIVE, sink2)}, journal2)
            alive = sink2.recover(orch2)
            assert sink2.orphans() == []
            events = orch2.on_context(ctx(SYM, h.base + 1, 60_000.0, received=(h.base + 2) * IV + 1000))
            # защита живёт и после рестарта: сработал тейк
            tp = next(a for a in e.ex.algo_orders if a["status"] == "NEW" and a["type"] == "TAKE_PROFIT_MARKET")
            e.ex.market_price = 59_000.0
            await e.ex.trigger_algo(tp["cid"], price=59_000.0)
            for ev in [parse_event(json.dumps(x)) for x in e.ex.pushed]:
                if isinstance(ev, OrderUpdate):
                    barrier2.on_order_update(ev)
                elif isinstance(ev, AlgoUpdate):
                    barrier2.on_algo_update(ev)
            e.ex.pushed.clear()
            await sink2.step()
            return alive, events, journal2, orch2.held(), sig.signal_id

    alive, events, journal2, held, signal_id = run(body())
    assert alive == [signal_id] and [x.kind for x in events] == ["skipped_due_to_lock"]
    closed = [x for x in journal2.events if x.kind == "live_trade_closed"]
    assert len(closed) == 1 and closed[0].detail["reason"] == "tp" and held == {}


def test_trade_that_ended_while_the_process_was_down_is_journaled_offline(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            h = make_harness(e, tmp_path, {})
            await h.sink.refresh_equity()
            assert h.sink.submit(short_limit(h), e.rest.now_ms()).accepted  # записан на диск, но вход так и не отправлен
            state2, barrier2, sink2, journal2 = restart_from_disk(e, tmp_path)
            orch2 = Orchestrator([Scripted("smc", {})], {"smc": Route(Mode.LIVE, sink2)}, journal2)
            return sink2.recover(orch2), kinds(journal2), orch2.held()

    alive, journaled, held = run(body())
    assert alive == [] and journaled == ["live_trade_cancelled_offline"] and held == {}


def test_corrupted_trades_file_is_an_error_not_an_empty_start(tmp_path):
    path = tmp_path / "live_trades.json"
    path.write_text("{не json", encoding="utf-8")
    with pytest.raises(DataError, match="повреждён"):
        LiveTradeStore(path).load()


def test_missing_filters_for_a_symbol_fail_at_construction(tmp_path, monkeypatch):
    async def body():
        async with algo_env(tmp_path) as e:
            monkeypatch.setattr(config, "LIVE_MIN_NOTIONAL", {})
            with pytest.raises(ConfigError, match="LIVE_MIN_NOTIONAL"):
                LiveBarrierSink(e.barrier, e.safe, MemoryEventLog(), LiveTradeStore(tmp_path / "t.json"), None, e.rest.now_ms, interval_ms=IV, symbols=(SYM,))

    run(body())


def test_live_route_does_not_suppress_errors_r1():
    import re
    src = open("orchestrator/live_route.py", encoding="utf-8").read()
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", code)
