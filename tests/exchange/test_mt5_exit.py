"""Сопровождение MT5-позиций планом выхода стратегии: ПАРИТЕТ с Shadow (VirtualTracker), закрытый рынок, неизвестный исход, рестарт.

Клиент - настоящий `MetaApiLiveClient` поверх имитации RPC, у которой цена движется: стоп и брокерский TP срабатывают при пересечении
(по цене тика, как стоп-маркет у Shadow), `modify_position` за ценой отвергается кодом 10016. Одна траектория подаётся и `VirtualTracker`, и
`ExecutionRouter`; сравниваются ВСЕ закрытия: причина, доля исходного объёма, цена (сетка цен кратна 0.25 - расхождение = логика, а не округление).
Допустимое физическое отличие - «немедленный стоп» (стоп по закрытой свече на/за ценой; брокер ответил бы 10016): бот закрывает по рынку сразу,
Shadow проверит стоп на следующем тике; такие траектории сравниваются строго до последнего закрытия."""

import asyncio
import random
from collections import Counter

import pytest

from exchange.mt5_metaapi import MetaApiTradeError
from exchange.router import ExecutionRouter, Mt5Tracked, RouteRejected
from orchestrator.journal import MemoryEventLog
from orchestrator.registry import SCALP_1R_PLAN, VWAP_PLAN
from orchestrator.tracker import CLOSED, DEFAULT_PLAN, ScaleOutPlan, VirtualTracker
from orchestrator.types import BarInfo, Direction, Kind, Tick
from tests.exchange.fake_mt5 import FakeMt5Rpc, make_client, make_mapper, trade_error
from tests.orchestrator.live_helpers import BAR, STRATEGY, T0, signal

SYM = "XAUUSD"
WICK_PLAN = ScaleOutPlan(((1.0, 0.5), (3.0, 0.4)), True, "wick", trail_wick_bars=2)
PLANS = {"default": DEFAULT_PLAN, "scalp": SCALP_1R_PLAN, "vwap": VWAP_PLAN, "wick": WICK_PLAN}
STEPS, BAR_EVERY = 170, 6
QTY0 = 2.5  # лоты в паритетных траекториях


class MovingMt5(FakeMt5Rpc):
    """RPC с движущейся ценой (bid = ask): стоп/TP брокера срабатывают по цене тика, модификация за ценой - отказ 10016."""

    def __init__(self, symbol: str, price: float):
        super().__init__(equity=100_000.0)
        self.symbol, self.price, self.fills = symbol, price, []
        self.quotes[symbol] = (price, price, 1.0)

    def _fill(self, qty: float, via: str) -> None:
        self.fills.append({"qty": qty, "price": self.price, "via": via})

    def set_price(self, p: float) -> None:
        self.price = p
        self.quotes[self.symbol] = (p, p, 1.0)
        for pos in list(self.positions):
            buy = pos["type"].endswith("BUY")
            sl, tp = pos["stopLoss"], pos["takeProfit"]
            if sl and ((buy and p <= sl) or (not buy and p >= sl)):
                self._fill(pos["volume"], "broker_sl")
                self.positions.remove(pos)
            elif tp and ((buy and p >= tp) or (not buy and p <= tp)):
                self._fill(pos["volume"], "broker_tp")
                self.positions.remove(pos)

    async def modify_position(self, position_id, sl=None, tp=None):
        pos = next(p for p in self.positions if p["id"] == position_id)
        buy = pos["type"].endswith("BUY")
        if sl and ((buy and sl >= self.price) or (not buy and sl <= self.price)):
            raise trade_error(10016)  # INVALID_STOPS: стоп на цене или за ней
        return await super().modify_position(position_id, sl, tp)

    async def close_position(self, position_id, options=None):
        pos = next((p for p in self.positions if p["id"] == position_id), None)
        res = await super().close_position(position_id, options)
        if pos is not None and not any(p["id"] == position_id for p in self.positions):
            self._fill(pos["volume"], "market")
        return res

    async def close_position_partially(self, position_id, volume, options=None):
        res = await super().close_position_partially(position_id, volume, options)
        self._fill(volume, "market")
        return res


def make_walk(seed: int, direction: Direction, gaps: bool) -> list[float]:
    rnd = random.Random(seed)
    favourable = -direction.sgn
    p, out = 400, [400]  # в четвертях
    drift = rnd.choice([-1, 0, 1])
    for _ in range(STEPS):
        step = rnd.choice([-1, -1, 0, 1, 1]) * rnd.choice([1, 1, 1, 2, 4])
        if rnd.random() < 0.25 + 0.1 * drift:
            step += int(favourable)
        if gaps and rnd.random() < 0.02:
            step += rnd.choice([-1, 1]) * rnd.choice([8, 12])
        p = max(p + step, 240)
        out.append(p)
    return [2000.0 + 2 * (x / 4 - 100) for x in out]  # 2000.00 = вход; шаг сетки 0.5


async def make_router(rpc: MovingMt5, plan, *, max_hold=192, state_path=None):
    client, _ = make_client(rpc)
    mapper = await make_mapper(client)
    return ExecutionRouter(mt5=client, mapper=mapper, plans={STRATEGY: plan}, max_hold_bars=max_hold, state_path=state_path, now_ms=lambda: T0), client


async def duel(seed: int, plan: ScaleOutPlan, direction: Direction, max_hold: int) -> dict:
    prices = make_walk(seed, direction, gaps=plan.runner_frac > 0)  # без runner'а гэп через два уровня сломал бы сравнение: брокерский TP закрыл бы всё разом
    sgn = direction.sgn
    sig = signal(symbol=SYM, direction=direction, kind=Kind.MARKET, entry=2000.0, sl=2000.0 + sgn * 4.0)  # R = 4.0 -> 2.50 лота (при R = 2 плечо 10x выше потолка 5.0 у Shadow)
    tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0, plans={STRATEGY: plan}, max_hold_bars=max_hold)
    tracker.submit(sig, T0)
    rpc = MovingMt5(SYM, 2000.0)
    router, _ = await make_router(rpc, plan, max_hold=max_hold)
    t = await router.open_mt5(sig, T0)
    assert isinstance(t, Mt5Tracked) and t.plan.lots == QTY0
    events, closed_at, window, bar_no, stop_at_close = [], None, [], 0, None
    for i, p in enumerate(prices):
        ts = T0 + i * 1000
        trade_tick, mark_tick = Tick(SYM, p, ts, "trade"), Tick(SYM, p, ts, "mark")
        tracker.on_tick(trade_tick)
        if closed_at is None:
            rpc.set_price(p)
            router.on_tick(trade_tick)
            events += await router.poll_mt5()
        tracker.on_tick(mark_tick)
        if closed_at is None:
            router.on_tick(mark_tick)
            events += await router.poll_mt5()
        window.append(p)
        if i % BAR_EVERY == BAR_EVERY - 1:
            bar_no += 1
            bar = BarInfo(max(window), min(window), 0.5)
            window = []
            tracker.on_bar_closed(SYM, T0 + bar_no * BAR, p, ts, bar)
            if closed_at is None:
                router.on_bar_closed(SYM, bar)
                events += await router.poll_mt5()
        if closed_at is None and any(e.kind == "closed" for e in events):
            closed_at = i
            st = tracker.trades[0]
            stop_at_close = (st.stop_kind, st.stop_m, sgn * p)
        if closed_at is not None and (tracker.trades[0].status == CLOSED or immediate(events, rpc)):
            break
    return {"seed": seed, "shadow": tracker.trades[0], "closed_at": closed_at, "rpc": rpc, "events": events, "stop_at_close": stop_at_close, "router": router}


def live_reasons(events) -> list[str]:
    return [e.detail["reason"] for e in events if e.kind in ("partial_close", "closed")]


def immediate(events, rpc) -> bool:
    closed = [e for e in events if e.kind == "closed"]
    return bool(closed) and closed[-1].detail["reason"] in ("be", "trail") and rpc.fills[-1]["via"] == "market"


def compare(r: dict, label: str) -> bool:
    shadow, rpc = r["shadow"], r["rpc"]
    where = f"{label} seed={r['seed']}"
    fills, legs = list(rpc.fills), list(shadow.legs)
    imm = immediate(r["events"], rpc)
    reasons = live_reasons(r["events"])
    if imm:
        n = len(fills) - 1
        assert len(legs) >= n, f"{where}: у Shadow закрытий {len(legs)} < {n} до немедленного стопа"
        kind, stop_m, q_now = r["stop_at_close"]
        assert q_now >= stop_m - 1e-12 and kind == reasons[-1], f"{where}: бот вышел по {reasons[-1]}, а стоп Shadow {kind} @ {stop_m} не достигнут (q={q_now})"
        assert fills[-1]["qty"] / QTY0 == pytest.approx(1.0 - sum(g.frac for g in legs[:n]), abs=1e-9), f"{where}: остаток при немедленном стопе"
        legs, fills, reasons = legs[:n], fills[:n], reasons[:-1]
    else:
        assert (shadow.status == CLOSED) == (r["closed_at"] is not None), f"{where}: закрыта только одна сторона: shadow={shadow.status} live={r['closed_at']}"
    assert len(fills) == len(legs), f"{where}: live {[(f['qty'], f['price'], f['via']) for f in fills]} vs shadow {[(g.reason, g.frac, g.price) for g in shadow.legs]}"
    for k, (f, leg) in enumerate(zip(fills, legs)):
        assert f["qty"] / QTY0 == pytest.approx(leg.frac, abs=1e-9), f"{where}: доля закрытия {k} ({leg.reason}): live {f['qty']} vs shadow {leg.frac}"
        assert f["price"] == leg.price, f"{where}: цена закрытия {k} ({leg.reason}): live {f['price']} vs shadow {leg.price}"
    live = reasons
    assert live == [g.reason for g in legs], f"{where}: причины live {live} vs shadow {[g.reason for g in legs]}"
    return imm


def test_mt5_exits_match_the_shadow_tracker_on_random_price_paths():
    seen: Counter[str] = Counter()
    n = 0
    for plan_name, plan in PLANS.items():
        for direction in (Direction.LONG, Direction.SHORT):
            for seed in range(22):
                max_hold = 12 if seed % 5 == 0 else 192  # на части траекторий срабатывает тайм-аут
                r = asyncio.run(duel(seed, plan, direction, max_hold))
                imm = compare(r, f"{plan_name}/{direction.value}/hold={max_hold}")
                n += 1
                seen["immediate_stop"] += int(imm)
                legs = r["shadow"].legs[: len(r["shadow"].legs) - int(imm)]
                seen.update(g.reason for g in legs)
                seen["multi_leg"] += int(len(legs) > 1)
    assert n == 176
    for needed in ("sl", "be", "trail", "timeout", "tp", "scale_1r", "scale_3r", "multi_leg"):
        assert seen[needed] >= 3, f"мало траекторий с выходом {needed}: {dict(seen)}"


# ---------------------------------------------------------------------------------------- вспомогательное для сценариев


def sig_long(**kw):
    return signal(symbol=SYM, direction=Direction.LONG, kind=Kind.MARKET, entry=2000.0, sl=1998.0, **kw)


async def opened(plan=DEFAULT_PLAN, *, state_path=None, max_hold=192):
    rpc = MovingMt5(SYM, 2000.0)
    router, client = await make_router(rpc, plan, max_hold=max_hold, state_path=state_path)
    t = await router.open_mt5(sig_long(), T0)
    assert isinstance(t, Mt5Tracked)
    return router, rpc, client, t


async def tick(router, p, kind="trade", ts=0):
    router.on_tick(Tick(SYM, p, T0 + ts, kind))


# ---------------------------------------------------------------------------------------------------- сценарии


def test_scale_out_lots_follow_the_volume_step_and_stops_follow_the_plan():
    async def body():
        router, rpc, _, t = await opened()
        assert [e.kind for e in await router.poll_mt5()] == ["protected"]
        ex = t.exit
        assert ex.initial_lots == 5.0 and [lvl[2] for lvl in ex.levels] == [2.5, 2.0] and ex.runner_lots == pytest.approx(0.5)
        assert [lvl[1] for lvl in ex.levels] == [2002.0, 2006.0] and ex.tp_price is None  # runner -> брокерского TP нет
        rpc.trades.clear()
        rpc.set_price(2002.0)
        await tick(router, 2002.0)
        evs = await router.poll_mt5()
        assert [e.kind for e in evs] == ["partial_close", "be_armed"]
        assert [x[:3] for x in rpc.trades if x[0] in ("close_part", "modify")] == [("close_part", t.position_id, 2.5), ("modify", t.position_id, 2000.0)]
        pos = rpc.positions[0]
        assert pos["volume"] == 2.5 and pos["stopLoss"] == 2000.0  # 50% закрыто, стоп на входе
        rpc.set_price(2006.0)
        await tick(router, 2006.0)
        evs = await router.poll_mt5()
        assert [e.kind for e in evs] == ["partial_close"] and rpc.positions[0]["volume"] == pytest.approx(0.5)
        assert all(e.kind != "closed" for e in evs)
    asyncio.run(body())


def test_no_runner_plan_puts_the_last_level_on_the_broker_as_tp():
    async def body():
        router, rpc, _, t = await opened(VWAP_PLAN)
        await router.poll_mt5()
        assert t.exit.tp_price == 2004.0 and rpc.positions[0]["takeProfit"] == 2004.0  # +2R от фактического входа
    asyncio.run(body())


def test_market_closed_defers_scale_out_without_safe_mode_and_replays_it():
    async def body():
        router, rpc, _, t = await opened()
        await router.poll_mt5()
        rpc.set_price(2002.0)
        await tick(router, 2002.0)
        rpc.effects.append(trade_error(10018))  # частичное закрытие: рынок закрыт
        evs = await router.poll_mt5()
        assert [e.kind for e in evs] == ["deferred"] and evs[0].detail["code"] == 10018 and rpc.positions[0]["volume"] == 5.0
        assert t.exit.levels_done == 0
        rpc.effects.append(trade_error(10018))
        assert await router.poll_mt5() == []  # повтор тоже отказан: в журнал второй раз не пишем
        evs = await router.poll_mt5()  # рынок открылся
        assert [e.kind for e in evs] == ["replayed", "partial_close", "be_armed"] and rpc.positions[0]["volume"] == 2.5
    asyncio.run(body())


def test_market_closed_defers_breakeven_and_timeout_close():
    async def body():
        router, rpc, _, t = await opened(DEFAULT_PLAN, max_hold=2)
        await router.poll_mt5()
        await tick(router, 2002.0, "mark")  # mark price +1R -> безубыток взведён, но уровни по цене сделок ещё не затронуты
        rpc.set_price(2001.0)  # цена выше входа: стоп «на входе» брокер принимает
        rpc.effects.append(trade_error(10018))
        assert [e.kind for e in await router.poll_mt5()] == ["deferred"] and rpc.positions[0]["stopLoss"] == 1998.0
        assert [e.kind for e in await router.poll_mt5()] == ["replayed", "be_armed"] and rpc.positions[0]["stopLoss"] == 2000.0
        rpc.set_price(2001.0)
        for _ in range(3):
            router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5))
        rpc.effects.append(trade_error(10018))
        evs = await router.poll_mt5()  # тайм-аут: закрыть остаток, но рынок закрыт
        assert [e.kind for e in evs] == ["deferred"] and rpc.positions
        evs = await router.poll_mt5()
        assert [e.kind for e in evs] == ["replayed", "closed"] and evs[-1].detail["reason"] == "timeout" and not rpc.positions and not router.tracked_mt5
    asyncio.run(body())


def test_unknown_outcome_of_partial_close_does_not_close_twice():
    async def body():
        router, rpc, _, t = await opened()
        await router.poll_mt5()
        rpc.set_price(2002.0)
        await tick(router, 2002.0)

        def executed_but_reply_lost(name, args):  # ордер дошёл до брокера и исполнился, ответ потерян
            if name == "close_part":
                rpc.positions[0]["volume"] = round(rpc.positions[0]["volume"] - args[1], 8)

        rpc.on_trade = executed_but_reply_lost
        rpc.effects.append(trade_error(10012))  # REQUEST_TIMEOUT: исход неизвестен
        assert await router.poll_mt5() == [] and rpc.positions[0]["volume"] == 2.5
        rpc.on_trade = None
        evs = await router.poll_mt5()  # уровень выведен из лотов брокера, второго закрытия нет
        assert [e.kind for e in evs] == ["partial_close", "be_armed"] and evs[0].detail["source"] == "broker_lots"
        assert [x[0] for x in rpc.trades].count("close_part") == 1 and rpc.positions[0]["volume"] == 2.5
    asyncio.run(body())


def test_stop_too_close_on_breakeven_closes_at_market_without_safe_mode():
    async def body():
        router, rpc, _, t = await opened()
        await router.poll_mt5()
        await tick(router, 2002.0, "mark")  # +1R по mark price
        rpc.set_price(2000.0)  # но сделки вернулись ко входу: стоп «на входе» стоит на цене -> брокер: 10016
        evs = await router.poll_mt5()
        assert [e.kind for e in evs] == ["closed"] and evs[0].detail["reason"] == "be" and not rpc.positions
    asyncio.run(body())


def test_ticks_before_the_first_poll_are_not_lost():
    async def body():
        router, rpc, _, t = await opened()
        for p, kind in ((2001.0, "trade"), (2002.0, "mark"), (1999.0, "trade")):  # опроса ещё не было: состояния сопровождения нет
            await tick(router, p, kind)
        await router.poll_mt5()
        assert t.exit.be_pending and t.exit.best_q == -2001.0 and t.exit.last_q == -1999.0
    asyncio.run(body())


def test_restart_returns_control_of_a_partly_closed_position(tmp_path):
    async def body():
        path = tmp_path / "mt5.json"
        router, rpc, client, t = await opened(state_path=path)
        await router.poll_mt5()
        rpc.set_price(2002.0)
        await tick(router, 2002.0)
        await router.poll_mt5()  # 50% закрыто, стоп на входе - на диске
        assert path.exists()
        before = list(rpc.trades)
        # перезапуск: новый маршрутизатор, тот же брокер и тот же файл
        router2, _ = await make_router(rpc, DEFAULT_PLAN, state_path=path)
        found = await router2.recover_mt5()
        assert [f.symbol for f in found] == [SYM] and not found[0].adopted and found[0].exit.levels_done == 1 and rpc.trades == before
        rpc.set_price(2006.0)
        await tick(router2, 2006.0)
        evs = await router2.poll_mt5()
        assert [e.kind for e in evs] == ["partial_close"] and evs[0].detail["reason"] == "scale_3r"
        assert rpc.positions[0]["volume"] == pytest.approx(0.5) and found[0].exit.stop_price == 2000.0
        # позиция закрыта, пока бота не было: запись уходит в offline_closed
        rpc.positions.clear()
        router3, _ = await make_router(rpc, DEFAULT_PLAN, state_path=path)
        assert await router3.recover_mt5() == [] and [o.symbol for o in router3.offline_closed] == [SYM]
    asyncio.run(body())


def test_foreign_position_is_adopted_without_management():
    async def body():
        rpc = MovingMt5(SYM, 2000.0)
        rpc.positions.append({"id": "9", "type": "POSITION_TYPE_BUY", "symbol": SYM, "volume": 1.0, "openPrice": 2000.0, "stopLoss": 1990.0,
                              "takeProfit": 2020.0, "clientId": "manual"})
        router, _ = await make_router(rpc, DEFAULT_PLAN)
        (t,) = await router.recover_mt5()
        assert t.adopted
        rpc.set_price(2010.0)
        await tick(router, 2010.0)
        assert await router.poll_mt5() == [] and rpc.trades == []  # чужую позицию не двигаем
    asyncio.run(body())


def test_unrepresentable_plan_is_rejected_before_any_order():
    async def body():
        rpc = MovingMt5(SYM, 2000.0)
        rpc.equity = rpc.free_margin = 400.0  # риск 4 USD -> 0.02 лота: 10% = 0.002 не делится
        router, _ = await make_router(rpc, DEFAULT_PLAN)
        rej = await router.open_mt5(sig_long(), T0)
        assert isinstance(rej, RouteRejected) and rej.reason == "plan_not_representable" and rpc.trades == []
    asyncio.run(body())


def test_other_trade_errors_are_not_swallowed():
    async def body():
        router, rpc, _, t = await opened()
        await router.poll_mt5()
        rpc.set_price(2002.0)
        await tick(router, 2002.0)
        rpc.effects.append(trade_error(10019))  # NO_MONEY на закрытии - не рыночная пауза и не неизвестный исход
        with pytest.raises(MetaApiTradeError):
            await router.poll_mt5()
    asyncio.run(body())


def test_mt5_trade_counts_only_bars_of_its_own_timeframe_and_timeout_scales():
    """2026-10-15: SMC_Pro_Trend (5m) и VWAP_Reversion (15m) в одном движке; MT5-сделка ведётся по барам своего таймфрейма, max_hold (в барах M15) умножается."""
    async def body():
        router, rpc, _, t = await opened(max_hold=2)
        await router.poll_mt5()
        assert t.interval == "15m" and t.exit.bars_closed == 0  # сигнал тестового помощника - 15m
        router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5), "5m")  # чужой таймфрейм: пропущен
        assert t.exit.bars_closed == 0
        router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5), "15m")
        assert t.exit.bars_closed == 1 and t.exit.forced_reason is None
        t.interval = "5m"  # та же позиция, но сделка на 5m: бары 15m не считаются, тайм-аут 2 * 3 = 6 баров
        router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5), "15m")
        assert t.exit.bars_closed == 1
        for _ in range(5):
            router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5), "5m")
        assert t.exit.bars_closed == 6 and t.exit.fill_bar_no == 1 and t.exit.forced_reason is None  # 6 - 1 < 6: ещё не вышло
        router.on_bar_closed(SYM, BarInfo(2001.0, 2000.0, 0.5), "5m")
        assert t.exit.forced_reason == "timeout"
        rec = t.to_record()
        assert rec["interval"] == "5m" and Mt5Tracked.from_record(rec).interval == "5m"
        del rec["interval"]
        assert Mt5Tracked.from_record(rec).interval is None  # запись до этой версии: любой бар, как раньше
    asyncio.run(body())
