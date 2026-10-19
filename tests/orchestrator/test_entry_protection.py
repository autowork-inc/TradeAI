"""Блок 3 (2026-10-18): SMC_Pro_Trend и проскальзывания.

1. (Отменено 2026-10-19: тейк строго от факта.) Уровни SMC_Pro_Trend были от цен сигнала (вход 50% FVG и SL за экстремумом свипа), а не от фактической цены входа: проскальзывание
   не сдвигает тейки (VirtualTracker и боевой движок - одной функцией `ScaleOutPlan.level_price`).
2. Импульсная свеча (диапазон > 2.5 * ATR(14)[i-1]), дошедшая до зоны ждущей лимитки, снимает её (`impulse_bar`).
3. MARKET-вход во время импульса против сделки - только после поглощения на последней закрытой 1m-свече (`MarketEntryGuard`).
"""

import polars as pl
import pytest

import config
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.minute_bars import MINUTE_MS, MarketEntryGuard, MinuteBars, _Bar, impulse_check
from orchestrator.registry import SMC_PRO_PLAN, plans_for
from orchestrator.strategies import SMC_PRO_TREND
from orchestrator.tracker import CANCELLED, DEFAULT_PLAN, OPEN, VirtualTracker, impulse_hits_limit
from orchestrator.types import BarInfo, Direction, FeatureContext, Kind, Mode, Tick
from tests.orchestrator.helpers import IV, SYM, Scripted, ctx, mk, tick
from tests.orchestrator.live_helpers import signal
from tests.orchestrator.test_live_engine import NOW, Env, run

# ------------------------------------------------------------------------------------------ 1. якорь TP


def test_every_plan_including_smc_pro_trend_is_anchored_to_the_actual_fill_for_a_strict_1_to_4():
    assert plans_for([SMC_PRO_TREND])[SMC_PRO_TREND] is SMC_PRO_PLAN and SMC_PRO_PLAN.tp_anchor == "fill"  # 2026-10-19: якорь `signal` отменён
    assert SMC_PRO_PLAN.levels == DEFAULT_PLAN.levels == ((4.0, 1.0),) and SMC_PRO_PLAN.describe()["tp_anchor"] == "fill"


def test_level_price_uses_signal_prices_and_falls_back_to_the_fill_when_slipped_past_the_level():
    """Якорь `signal` в боевых планах не используется (2026-10-19), но функция остаётся поддерживаемой: проверяем её на локальном плане."""
    from orchestrator.tracker import ScaleOutPlan

    SMC_PRO_PLAN = ScaleOutPlan(((1.0, 0.5), (3.0, 0.5)), tp_anchor="signal")  # noqa: N806
    long_ = Direction.LONG.sgn
    assert SMC_PRO_PLAN.level_price(1.0, side_sgn=long_, fill=100.5, sl=98.0, signal_entry=100.0) == pytest.approx(102.0)  # не 103 от факта
    assert DEFAULT_PLAN.level_price(1.0, side_sgn=long_, fill=100.5, sl=98.0, signal_entry=100.0) == pytest.approx(103.0)
    # исполнение уже за уровнем (+1R сигнала = 102, вход 102.5): тейк «за ценой» не ставится - уровень от факта
    assert SMC_PRO_PLAN.level_price(1.0, side_sgn=long_, fill=102.5, sl=98.0, signal_entry=100.0) == pytest.approx(107.0)
    short = Direction.SHORT.sgn
    assert SMC_PRO_PLAN.level_price(3.0, side_sgn=short, fill=99.6, sl=105.0, signal_entry=100.0) == pytest.approx(85.0)


def test_tracker_market_fill_with_slippage_puts_the_take_profit_exactly_4r_from_the_fill():
    log = MemoryEventLog()
    tr = VirtualTracker(log, equity=10_000.0, plans=plans_for([SMC_PRO_TREND]))
    sig = mk(strategy=SMC_PRO_TREND, kind=Kind.MARKET, direction=Direction.LONG, entry=100.0, sl=95.0)
    assert tr.submit(sig, 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(101.0, 5))  # исполнение по рынку на 1 выше сигнала
    assert t.status == OPEN and t.fill_price == 101.0 and t.risk == pytest.approx(6.0)
    assert t.plan_tp_price() == pytest.approx(125.0)  # строго 1:4 от ФАКТА: 101 + 4 * 6
    assert t.sizing.qty == pytest.approx(5.0 / 6.0)  # потеря на стопе (101 -> 95) = 5 USDT


def test_live_engine_places_the_single_take_profit_exactly_4r_from_the_fill(tmp_path):
    env = Env(tmp_path, price=100.5, risk_kw={"plans": plans_for([SMC_PRO_TREND])})
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    assert [a["triggerPrice"] for a in env.algos()] == ["98", "110.5"]  # 100.5 + 4 * 2.5 от факта
    assert env.trade().risk == pytest.approx(2.5)


# ------------------------------------------------------------------------------- 2. импульсная свеча


@pytest.mark.parametrize("bar, hit", [
    (BarInfo(99.0, 93.0, 1.0, 2.0), True),  # диапазон 6 > 2.5 * 2, high 99 >= 100 - 5 (зона заявки)
    (BarInfo(99.0, 95.0, 1.0, 2.0), False),  # диапазон 4 <= 5: не импульс
    (BarInfo(94.0, 88.0, 1.0, 2.0), False),  # импульс, но вдали от заявки (high 94 < 95)
    (BarInfo(99.0, 93.0, 1.0, None), False),  # нет ATR[i-1]: правило не срабатывает
])
def test_impulse_hits_limit_for_a_short_limit_above_the_market(bar, hit):
    assert impulse_hits_limit(bar, entry=100.0, sl=105.0) is hit


def test_impulse_rule_is_mirrored_for_a_long_limit():
    assert impulse_hits_limit(BarInfo(107.0, 101.0, 1.0, 2.0), entry=100.0, sl=95.0)
    assert not impulse_hits_limit(BarInfo(113.0, 107.0, 1.0, 2.0), entry=100.0, sl=95.0)


def test_tracker_cancels_a_pending_limit_on_an_impulse_bar():
    log = MemoryEventLog()
    tr = VirtualTracker(log, equity=10_000.0)
    assert tr.submit(mk(), 1000).accepted  # шорт-лимит 100, SL 105
    t = tr.trades[-1]
    tr.on_bar_closed(SYM, IV, 96.0, 2 * IV, BarInfo(99.0, 93.0, 1.0, 2.0), interval="15m")
    assert t.status == CANCELLED and log.events[-1].detail["reason"] == "impulse_bar"


def test_live_engine_requests_cancel_of_a_pending_limit_on_an_impulse_bar(tmp_path):
    env = Env(tmp_path, price=95.0)
    sig = signal(direction=Direction.SHORT, kind=Kind.LIMIT, entry=100.0, sl=102.0, tp=94.0, inval=102.0)
    run(env.engine.execute(sig, NOW))
    env.engine.on_bar_closed("SOLUSDT", sig.bar_open_time + IV, 96.0, NOW + IV, BarInfo(99.5, 93.0, 1.0, 2.0), interval="15m")
    assert env.engine.trades["SOLUSDT"].cancel_reason == "impulse_bar"


def test_bar_info_carries_the_atr_before_the_bar():
    frame = pl.DataFrame({"open_time": [0, IV], "high": [1.0, 2.0], "low": [0.5, 1.0], "close": [1.0, 1.5], "atr": [0.3, 0.9]})
    bar = FeatureContext(SYM, "15m", IV, frame, 2 * IV).bar
    assert (bar.atr, bar.atr_prev) == (0.9, 0.3)


# -------------------------------------------------------------------------------------- 3. 1m-фильтр


def minutes(prices: list[tuple[float, float, float, float]], t0: int = 0) -> list[_Bar]:
    return [_Bar(t0 + i * MINUTE_MS, o, h, lo, c) for i, (o, h, lo, c) in enumerate(prices)]


CALM = [(100.0, 100.5, 99.5, 100.0)] * 14  # ATR = 1.0


def test_dump_without_engulfing_blocks_a_market_long_and_engulfing_allows_it():
    dump = CALM + [(100.0, 100.2, 98.0, 98.2), (98.2, 98.3, 97.0, 97.1), (97.1, 97.2, 96.5, 96.6)]  # 100.2 -> 96.6 = 3.6 ATR
    assert impulse_check(minutes(dump), Direction.LONG) == "impulse_dump"
    engulf = CALM + [(100.0, 100.2, 97.0, 97.2), (97.2, 97.3, 96.6, 96.7), (96.6, 97.4, 96.5, 97.3)]  # дамп 2.9 ATR, но бычья свеча накрыла тело медвежьей
    assert impulse_check(minutes(engulf), Direction.LONG) is None
    assert impulse_check(minutes(dump), Direction.SHORT) is None  # шорт по дампу - не против импульса


def test_pump_is_the_mirror_for_a_market_short():
    pump = CALM + [(100.0, 102.0, 99.9, 101.9), (101.9, 103.0, 101.8, 102.9), (102.9, 103.6, 102.8, 103.5)]
    assert impulse_check(minutes(pump), Direction.SHORT) == "impulse_pump"
    assert impulse_check(minutes(pump), Direction.LONG) is None


def test_not_enough_minutes_is_fail_closed():
    assert impulse_check(minutes(CALM[:10]), Direction.LONG) == "no_1m_data"


def test_minute_bars_are_built_from_trade_ticks_and_reset_on_a_gap():
    mb = MinuteBars()
    for i, p in enumerate([100.0, 101.0, 99.0, 100.5]):
        mb.on_tick(Tick(SYM, p, 1_000 + i, "trade"))
    mb.on_tick(Tick(SYM, 999.0, 2_000, "mark"))  # mark-цена в минутки не входит
    mb.on_tick(Tick(SYM, 100.2, MINUTE_MS + 5, "trade"))
    (b,) = mb.closed(SYM)
    assert (b.t, b.o, b.h, b.lo, b.c) == (0, 100.0, 101.0, 99.0, 100.5)
    mb.on_tick(Tick(SYM, 100.0, 5 * MINUTE_MS, "trade"))  # три минуты без тиков: история рвётся
    assert mb.closed(SYM) == []


def test_guard_only_checks_market_signals_and_orchestrator_skips_with_reason():
    guard = MarketEntryGuard(MinuteBars())
    assert guard.check(mk(kind=Kind.LIMIT), 0) is None
    assert guard.check(mk(kind=Kind.MARKET), 0) == "no_1m_data"
    log = MemoryEventLog()
    tr = VirtualTracker(log, equity=10_000.0)
    sig = mk(kind=Kind.MARKET)
    orch = Orchestrator([Scripted("smc", {0: [sig]})], {"smc": Route(Mode.SHADOW, tr)}, log, signal_gates=[guard])
    (ev,) = orch.on_context(ctx())
    assert ev.kind == "skipped_filter" and ev.detail["filter"] == "market_impulse" and ev.detail["reason"] == "no_1m_data"
    assert tr.trades == [] and orch.held() == {}  # монета не заблокирована, сделки нет
    assert config.TRADE_MODE in ("shadow", "live")
