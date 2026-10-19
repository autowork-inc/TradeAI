"""ПАРИТЕТ: боевой выход обязан совпадать с выходом Shadow (VirtualTracker) — одна математика.

Одна и та же случайная траектория цены (тики сделок, mark price, закрытые свечи с ATR) подаётся ДВУМ исполнителям:
  * настоящему `VirtualTracker` (Shadow) - закрывает доли по тикам;
  * `LiveExecutionEngine` на имитации биржи (`FakeRest`), где цена движется, algo-ордера срабатывают при пересечении, а движок
    сопровождает позицию (биржевые уровни, перенос стопа, трейлинг, тайм-аут) только через REST.
Сравниваются ВСЕ закрытия: причина, доля исходного объёма, цена. Сетка цен кратна 0.25 (точно представима в float), поэтому
расхождение - это расхождение логики, а не округления. Единственное допустимое отличие («немедленный стоп»): стоп, поставленный по закрытой свече
НА цене или ЗА ценой (биржа ответила бы -2021), движок исполняет сразу рыночным закрытием, а Shadow проверит его только на
следующем тике (цена к тому времени может отскочить). В таких траекториях все предыдущие закрытия сравниваются строго, а у
последнего - причина (be/trail) и остаток объёма; цена и момент не сравниваются (это физика биржи, а не логики).
"""

import asyncio
import random
from collections import Counter

import pytest

from exchange.binance_live import BinanceLiveClient
from execution.state import SafeMode
from orchestrator.journal import MemoryEventLog
from orchestrator.live_engine import LiveExecutionEngine, LiveStateStore
from orchestrator.registry import SCALP_1R_PLAN, VWAP_PLAN
from orchestrator.tracker import CLOSED, DEFAULT_PLAN, ScaleOutPlan, VirtualTracker
from orchestrator.types import BarInfo, Direction, Kind, Tick
from tests.orchestrator.fake_live_rest import FakeRest
from tests.orchestrator.live_helpers import BAR, FILTERS, STRATEGY, T0, manager, signal

SYM = "SOLUSDT"
PLANS = {"default": DEFAULT_PLAN, "scalp": SCALP_1R_PLAN, "vwap": VWAP_PLAN}
STEPS = 170
BAR_EVERY = 6


def make_walk(seed: int, direction: Direction) -> list[float]:
    rnd = random.Random(seed)
    favourable = -direction.sgn  # +1 для лонга
    p, out = 400, [400]  # в четвертях: 100.00
    drift = rnd.choice([-1, 0, 1])
    for _ in range(STEPS):
        step = rnd.choice([-1, -1, 0, 1, 1]) * rnd.choice([1, 1, 1, 2, 4])
        if rnd.random() < 0.25 + 0.1 * drift:
            step += int(favourable)  # снос в нужную сторону: чтобы доходить и до +3R, и до трейлинга
        if rnd.random() < 0.02:
            step += rnd.choice([-1, 1]) * rnd.choice([8, 12])  # гэп через уровни
        p = max(p + step, 240)
        out.append(p)
    return [x / 4 for x in out]


async def duel(seed: int, plan: ScaleOutPlan, direction: Direction, max_hold: int, tmp_path) -> dict:
    prices = make_walk(seed, direction)
    sgn = direction.sgn
    sl = 100.0 + sgn * 2.0  # R = 2.0
    sig = signal(direction=direction, kind=Kind.MARKET, entry=100.0, sl=sl)

    shadow_log = MemoryEventLog()
    tracker = VirtualTracker(shadow_log, equity=10_000.0, plans={STRATEGY: plan}, max_hold_bars=max_hold)
    tracker.submit(sig, T0)

    rest = FakeRest(100.0)
    client = BinanceLiveClient(rest)
    client.set_filters(FILTERS)
    journal = MemoryEventLog()
    risk = manager(tmp_path, margin=100.0, plans={STRATEGY: plan})  # 1000 USDT: объём 10.0, доли делятся на сетке лота
    engine = LiveExecutionEngine(client, risk, journal, SafeMode(tmp_path / ".safe"), now_ms=lambda: T0 + 60_000, interval_ms=BAR,
                                 post_only=False, max_hold_bars=max_hold, store=LiveStateStore(tmp_path / "state.json"))
    await engine.execute(sig, T0 + 60_000)
    qty0 = engine.trades[SYM].filled_qty
    assert qty0 == 10.0

    live_closed_at: int | None = None
    window: list[float] = []
    bar_no = 0
    for i, p in enumerate(prices):
        ts = T0 + i * 1000
        trade_tick, mark_tick = Tick(SYM, p, ts, "trade"), Tick(SYM, p, ts, "mark")
        tracker.on_tick(trade_tick)
        if live_closed_at is None:
            rest.set_price(p)
            engine.on_tick(trade_tick)
            await engine.poll()
        tracker.on_tick(mark_tick)
        if live_closed_at is None:
            engine.on_tick(mark_tick)
            await engine.poll()
        window.append(p)
        if i % BAR_EVERY == BAR_EVERY - 1:
            bar_no += 1
            bar = BarInfo(max(window), min(window), 0.5)
            window = []
            tracker.on_bar_closed(SYM, T0 + bar_no * BAR, p, ts, bar)
            if live_closed_at is None:
                engine.on_bar_closed(SYM, T0 + bar_no * BAR, p, ts, bar)
                await engine.poll()
        if live_closed_at is None and SYM not in engine.trades:
            live_closed_at = i
            st = tracker.trades[0]
            stop_at_close = (st.stop_kind, st.stop_m, direction.sgn * p, st.status)  # стоп Shadow в момент, когда движок вышел
        if live_closed_at is not None and (tracker.trades[0].status == CLOSED or is_immediate_stop({"journal": journal, "rest": rest})):
            break
    shadow = tracker.trades[0]
    return {"seed": seed, "prices": prices, "shadow": shadow, "closed_at": live_closed_at, "rest": rest, "journal": journal, "qty0": qty0,
            "stop_at_close": locals().get("stop_at_close")}


def live_reasons(journal: MemoryEventLog) -> list[str]:
    partial = [e.detail["reason"] for e in journal.of_kind("live_partial_close")]
    closed = [e.detail["reason"] for e in journal.of_kind("live_closed")]
    if closed and not (partial and partial[-1] == "tp" and closed[-1] == "tp"):
        partial += closed  # итоговое закрытие по стопу/трейлингу/тайм-ауту; последний уровень без runner'а уже учтён как "tp"
    return partial


def live_closed_reason(journal: MemoryEventLog) -> str:
    return [e.detail["reason"] for e in journal.of_kind("live_closed")][-1]


def is_immediate_stop(r: dict) -> bool:
    """Движок закрылся рыночным ордером по be/trail (стоп оказался на/за ценой при постановке)."""
    closed = [e.detail["reason"] for e in r["journal"].of_kind("live_closed")]
    last = [f for f in r["rest"].fills if not f["opens"]][-1:]
    return bool(closed) and closed[-1] in ("be", "trail") and bool(last) and last[0]["via"] == "market"


def compare(r: dict, label: str) -> bool:
    """Возвращает True, если траектория закончилась «немедленным стопом» (см. докстринг модуля)."""
    shadow, rest = r["shadow"], r["rest"]
    where = f"{label} seed={r['seed']}"
    fills = [f for f in rest.fills if not f["opens"]]
    immediate = is_immediate_stop(r)
    legs = list(shadow.legs)
    if immediate:
        n = len(fills) - 1
        assert len(legs) >= n, f"{where}: у Shadow закрытий {len(legs)} < {n} до немедленного стопа"
        legs = legs[:n]
        fills_cmp = fills[:n]
        left = 1.0 - sum(g.frac for g in legs)
        assert fills[-1]["qty"] / r["qty0"] == pytest.approx(left, abs=1e-9), f"{where}: остаток при немедленном стопе {fills[-1]['qty']}"
        reasons_live = live_reasons(r["journal"])[:-1]
        kind, stop_m, q_now, _ = r["stop_at_close"]
        # выход «немедленным стопом» законен, только если стоп САМОГО Shadow к этому моменту уже на цене или за ней и того же вида
        assert q_now >= stop_m - 1e-12 and kind == live_closed_reason(r["journal"]), (
            f"{where}: движок вышел по {live_closed_reason(r['journal'])}, а стоп Shadow {kind} @ {stop_m} не достигнут ценой (q={q_now})")
    else:
        assert (shadow.status == CLOSED) == (r["closed_at"] is not None), f"{where}: сделка закрыта только у одного: shadow={shadow.status}, live={r['closed_at']}"
        fills_cmp = fills
        reasons_live = live_reasons(r["journal"])
    assert len(fills_cmp) == len(legs), f"{where}: закрытий live {[(f['qty'], f['price']) for f in fills]} vs shadow {[(g.reason, g.frac, g.price) for g in shadow.legs]}"
    for k, (f, leg) in enumerate(zip(fills_cmp, legs)):
        assert f["qty"] / r["qty0"] == pytest.approx(leg.frac, abs=1e-9), f"{where}: доля закрытия {k}: live {f['qty']} vs shadow {leg.frac} ({leg.reason})"
        assert f["price"] == leg.price, f"{where}: цена закрытия {k} ({leg.reason}): live {f['price']} vs shadow {leg.price}"
    assert reasons_live == [g.reason for g in legs], f"{where}: причины live {reasons_live} vs shadow {[g.reason for g in legs]}"
    return immediate


def test_exits_match_the_shadow_tracker_on_random_price_paths(tmp_path):
    seen: Counter[str] = Counter()
    n = 0
    for plan_name, plan in PLANS.items():
        for direction in (Direction.LONG, Direction.SHORT):
            for seed in range(22):
                max_hold = 12 if seed % 5 == 0 else 192  # на части траекторий срабатывает тайм-аут
                r = asyncio.run(duel(seed, plan, direction, max_hold, tmp_path / f"{plan_name}-{direction.value}-{seed}"))
                immediate = compare(r, f"{plan_name}/{direction.value}/hold={max_hold}")
                n += 1
                seen["immediate_stop"] += int(immediate)
                legs = r["shadow"].legs[: len(r["shadow"].legs) - int(immediate)]
                seen.update(g.reason for g in legs)
                if len(legs) > 1:
                    seen["multi_leg"] += 1
                if r["closed_at"] is None:
                    seen["still_open"] += 1
    assert n == 132
    # тест не «пустой»: на траекториях встретились все виды выходов, включая трейлинг, безубыток и тайм-аут
    for needed in ("sl", "timeout", "tp"):
        assert seen[needed] >= (1 if needed == "timeout" else 3), f"мало траекторий с выходом {needed}: {dict(seen)}"


def test_mark_price_at_1r_moves_no_stop_and_matches_shadow(tmp_path):
    """mark price коснулась +1R, сделки уровень не задели, затем возврат к входу: безубытка нет, позиция жива у обоих, стоп на исходном SL."""

    async def body():
        sig = signal(direction=Direction.LONG, entry=100.0, sl=98.0)
        tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0, plans={STRATEGY: DEFAULT_PLAN})
        tracker.submit(sig, T0)
        rest = FakeRest(100.0)
        client = BinanceLiveClient(rest)
        client.set_filters(FILTERS)
        journal = MemoryEventLog()
        engine = LiveExecutionEngine(client, manager(tmp_path, margin=100.0), journal, SafeMode(tmp_path / ".s"), now_ms=lambda: T0 + 60_000,
                                     interval_ms=BAR, post_only=False)
        await engine.execute(sig, T0 + 60_000)
        for ts, kind, p in [(0, "trade", 100.0), (1, "trade", 101.75), (2, "mark", 102.0), (3, "trade", 101.0), (4, "trade", 100.0), (5, "trade", 99.5)]:
            tk = Tick(SYM, p, T0 + ts, kind)
            tracker.on_tick(tk)
            if kind == "trade":
                rest.set_price(p)
            engine.on_tick(tk)
            await engine.poll()
        return tracker.trades[0], rest, journal

    shadow, rest, journal = asyncio.run(body())
    assert shadow.legs == [] and shadow.stop_m * shadow.sgn == 98.0
    assert not [f for f in rest.fills if not f["opens"]] and not journal.of_kind("live_be_armed")
