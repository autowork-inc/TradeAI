"""Тейк по цене из сигнала (ScaleOutPlan tp_anchor='price', 2026-10-23): VWAP_Reversion закрывает позицию на возврате к линии VWAP, а не на 1:4.

Одна функция цены уровня (ScaleOutPlan.level_price) у VirtualTracker и боевого движка (R8): Shadow и live ставят тейк в одну и ту же цену.
MT5-сопровождение считает уровни только в R - такой план там отклоняется (plan_not_representable), а не торгуется другой математикой.
"""

import asyncio
import dataclasses

import pytest

import config
from orchestrator.journal import MemoryEventLog
from orchestrator.registry import VWAP_PLAN, plans_for
from orchestrator.tracker import OPEN, ScaleOutPlan, VirtualTracker
from orchestrator.types import Direction, Kind
from tests.orchestrator.helpers import mk, tick
from tests.orchestrator.live_helpers import STRATEGY, signal
from tests.orchestrator.test_live_engine import Env
from utils.errors import ConfigError

NOW = 1_800_000_000_000


def test_plan_rules():
    assert VWAP_PLAN.tp_anchor == "price" and plans_for(["VWAP_Reversion"])["VWAP_Reversion"] == VWAP_PLAN
    with pytest.raises(ConfigError):
        ScaleOutPlan(((1.0, 0.5), (2.0, 0.5)), tp_anchor="price")  # только один тейк на весь объём
    # лонг: вход 100, SL 98; VWAP 101.5 в прибыли -> тейк ровно там; VWAP ниже входа (проскальзывание) -> запасной 1R от факта
    assert VWAP_PLAN.level_price(1.0, side_sgn=-1, fill=100.0, sl=98.0, signal_entry=100.0, signal_tp=101.5) == 101.5
    assert VWAP_PLAN.level_price(1.0, side_sgn=-1, fill=102.0, sl=98.0, signal_entry=100.0, signal_tp=101.5) == 106.0
    assert VWAP_PLAN.level_price(1.0, side_sgn=-1, fill=100.0, sl=98.0, signal_entry=100.0, signal_tp=None) == 102.0
    assert ScaleOutPlan(((4.0, 1.0),)).level_price(4.0, side_sgn=-1, fill=100.0, sl=98.0, signal_entry=100.0, signal_tp=101.5) == 108.0


def market_short(tp):
    """Шорт MARKET: сигнал по 100, SL 105, тейк (VWAP) 97."""
    return dataclasses.replace(mk(strategy="VWAP_Reversion", kind=Kind.MARKET), tp=tp)


def test_shadow_takes_profit_at_the_vwap_line():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, plans={"VWAP_Reversion": VWAP_PLAN})
    s = market_short(97.0)
    tr.submit(s, 1000)
    tr.on_tick(tick(100.0, 2))  # исполнение по 100, R = 5
    t = tr.trades[0]
    assert t.status == OPEN and t.tp_m * t.sgn == 97.0
    tr.on_tick(tick(98.0, 3))
    assert t.status == OPEN
    tr.on_tick(tick(96.9, 4))  # дошла до VWAP
    assert t.status == "closed" and t.exit_reason == "tp" and t.gross_r == pytest.approx((100.0 - 96.9) / 5.0)


def test_shadow_restart_keeps_the_vwap_take_profit():
    """После рестарта (восстановление из БД) тейк берётся из цены сигнала (meta.tp снапшота) - тот же уровень."""
    from persistence.recorder import DbShadowTrade, MemoryRecorder

    rec = MemoryRecorder()
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec, plans={"VWAP_Reversion": VWAP_PLAN})
    s = market_short(97.0)
    tr.submit(s, 1000)
    tr.on_tick(tick(100.0, 2))
    row = dict(rec.trades[s.signal_id])
    meta = {"entry": s.entry, "sl": s.sl, "tp": s.tp, "inval": None, "interval": s.interval}
    tr2 = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=MemoryRecorder(), plans={"VWAP_Reversion": VWAP_PLAN})
    (r,), _ = tr2.restore([DbShadowTrade(row, (), meta, ())], 10 * 60_000)
    assert r.tp_m * r.sgn == 97.0


def test_live_engine_places_take_profit_at_the_same_price_as_shadow(tmp_path):
    env = Env(tmp_path, price=100.5, risk_kw={"plans": {STRATEGY: VWAP_PLAN}})
    sig = dataclasses.replace(signal(entry=100.0, sl=98.0), tp=103.0)  # лонг, VWAP 103
    asyncio.run(env.engine.execute(sig, NOW))
    sl, tp = env.rest.calls("POST", config.ALGO_ORDER_PATH)
    assert (tp["triggerPrice"], tp["quantity"], tp["side"]) == ("103", "1", "SELL")  # не 1:4 от факта (110.5), а линия VWAP
    shadow = VWAP_PLAN.level_price(1.0, side_sgn=Direction.LONG.sgn, fill=100.5, sl=98.0, signal_entry=100.0, signal_tp=103.0)
    assert shadow == 103.0


def test_mt5_router_refuses_a_price_anchored_plan():
    """MT5-сопровождение считает уровни только в R: план «тейк по цене сигнала» там не торгуется другой математикой - отказ, ордеров нет."""
    from exchange.router import RouteRejected
    from tests.exchange.test_router import eur_long, make_router
    from tests.orchestrator.live_helpers import STRATEGY

    router, _, rpc = make_router(plans={STRATEGY: VWAP_PLAN})
    res = asyncio.run(router.prepare_mt5(eur_long(), NOW))
    assert isinstance(res, RouteRejected) and res.reason == "plan_not_representable" and rpc.trades == []
