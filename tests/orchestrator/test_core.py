"""Orchestrator: маршрутизация сигналов, область блокировки монеты (D4), свежесть, порядок свечей, Fail-Fast (R1)."""

import pytest

from orchestrator.core import Orchestrator, OrchestratorError, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import LEGACY_PLAN, VirtualTracker
from orchestrator.types import Direction, Mode
from tests.orchestrator.helpers import IV, Scripted, ctx, mk, tick
from utils.errors import ConfigError


def build(strategies, mode=Mode.SHADOW, **kw):
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0, plan=LEGACY_PLAN)  # механика блокировки: полный выход на 3R
    orch = Orchestrator(strategies, {s.name: Route(mode, tracker) for s in strategies}, log, **kw)
    return orch, tracker, log


def kinds(events):
    return [e.kind for e in events]


def feed(orch, upto, symbol="BTCUSDT"):
    out = []
    for i in range(upto + 1):
        out.extend(orch.on_context(ctx(symbol, i)))
    return out


def test_signal_is_accepted_registered_and_holds_the_lock():
    smc = Scripted("smc", {1: [mk(bar=1)]})
    orch, tracker, log = build([smc])
    events = feed(orch, 1)
    assert kinds(events) == ["signal_accepted"] and events[0].detail["mode"] == "shadow"
    assert [t.trade_id for t in tracker.active()] == ["smc:BTCUSDT:15m:1:short"]
    assert orch.held() == {"smc:BTCUSDT:15m:1:short": ("BTCUSDT", "shadow:smc")}
    assert orch.lock_for("smc").owner("BTCUSDT").trade_id == "smc:BTCUSDT:15m:1:short"


def test_second_signal_on_busy_coin_is_logged_skipped_due_to_lock_d4():
    smc = Scripted("smc", {1: [mk(bar=1)], 2: [mk(bar=2, direction=Direction.LONG)]})
    orch, tracker, log = build([smc])
    events = feed(orch, 2)
    assert kinds(events) == ["signal_accepted", "skipped_due_to_lock"]
    skipped = events[1]
    assert skipped.detail["held_by"] == {"strategy": "smc", "trade_id": "smc:BTCUSDT:15m:1:short"}
    assert len(tracker.trades) == 1  # второй сигнал в трекер не попал
    assert log.of_kind("skipped_due_to_lock") == [skipped]


def test_two_signals_on_the_same_bar_first_wins():
    both = [mk(bar=1), mk(bar=1, direction=Direction.LONG)]
    orch, tracker, _ = build([Scripted("smc", {1: both})])
    events = feed(orch, 1)
    assert kinds(events) == ["signal_accepted", "skipped_due_to_lock"] and len(tracker.trades) == 1


def test_other_coin_is_independent():
    smc = Scripted("smc", {1: [mk(bar=1), mk(bar=1, symbol="ETHUSDT")]})
    orch, tracker, _ = build([smc])
    orch.on_context(ctx("BTCUSDT", 0)), orch.on_context(ctx("ETHUSDT", 0))
    events = list(orch.on_context(ctx("BTCUSDT", 1)))
    events += orch.on_context(ctx("ETHUSDT", 1))
    assert kinds(events).count("signal_accepted") == 2 and {t.signal.symbol for t in tracker.trades} == {"BTCUSDT", "ETHUSDT"}


def test_shadow_strategies_do_not_block_each_other_but_live_ones_do():
    sigs = lambda: [Scripted("smc", {1: [mk("smc", bar=1)]}), Scripted("breakout", {1: [mk("breakout", bar=1, direction=Direction.LONG)]})]  # noqa: E731
    orch, tracker, _ = build(sigs())  # SHADOW: у каждой стратегии своя блокировка (3.2) - обе берут BTC
    events = feed(orch, 1)
    assert kinds(events) == ["signal_accepted", "signal_accepted"]
    assert {v[1] for v in orch.held().values()} == {"shadow:smc", "shadow:breakout"}
    orch2, _, _ = build(sigs(), mode=Mode.LIVE)  # LIVE: одна позиция на монету (One-Way) - вторая стратегия блокируется
    events2 = feed(orch2, 1)
    assert kinds(events2) == ["signal_accepted", "skipped_due_to_lock"]
    assert {v[1] for v in orch2.held().values()} == {"live"}


def test_lock_is_released_when_limit_expires_and_the_coin_can_be_taken_again():
    smc = Scripted("smc", {1: [mk(bar=1)], 11: [mk(bar=11)]})
    orch, tracker, _ = build([smc])
    events = feed(orch, 11)  # на баре 11 лимитка бара 1 истекает (10 закрытий) ДО детекции нового сигнала
    assert kinds(events) == ["signal_accepted", "signal_accepted"]
    assert tracker.trades[0].exit_reason == "ttl" and list(orch.held()) == ["smc:BTCUSDT:15m:11:short"]


def test_lock_is_released_when_position_closes_by_tick():
    smc = Scripted("smc", {1: [mk(bar=1)], 3: [mk(bar=3)]})
    orch, tracker, _ = build([smc])
    feed(orch, 1)
    tracker.on_tick(tick(100.5, 10))
    tracker.on_tick(tick(84.0, 11))  # TP
    assert orch.held() == {}
    events = feed_from(orch, 2, 3)
    assert kinds(events) == ["signal_accepted"]


def feed_from(orch, start, upto):
    out = []
    for i in range(start, upto + 1):
        out.extend(orch.on_context(ctx("BTCUSDT", i)))
    return out


def test_stale_bar_signals_are_logged_and_never_take_the_lock():
    smc = Scripted("smc", {1: [mk(bar=1)]})
    orch, tracker, _ = build([smc], max_signal_age_ms=30_000)
    orch.on_context(ctx(idx=0))
    events = orch.on_context(ctx(idx=1, received=2 * IV + 31_000))  # закрытие свечи обработано через 31 с
    assert kinds(events) == ["skipped_stale"] and events[0].detail["age_ms"] == 31_000
    assert tracker.trades == [] and orch.held() == {}


def test_replayed_bars_still_advance_tracker_bar_counters():
    smc = Scripted("smc", {1: [mk(bar=1)]})
    orch, tracker, _ = build([smc])
    feed(orch, 1)
    for i in range(2, 12):  # 10 свечей после обрыва вдогонку (все устаревшие)
        orch.on_context(ctx(idx=i, received=(i + 1) * IV + 3_600_000))
    assert tracker.trades[0].exit_reason == "ttl"  # сроки заявок считались и на replay-свечах


def test_risk_refusal_from_sink_releases_the_lock_and_is_logged():
    bad = mk(bar=1, entry=100.0, sl=100.02, tp=85.0, inval=101.0)  # плечо 50 > 5
    ok = mk(bar=1, direction=Direction.LONG)
    orch, tracker, _ = build([Scripted("smc", {1: [bad, ok]})])
    events = feed(orch, 1)
    assert kinds(events) == ["skipped_risk", "signal_accepted"] and events[0].detail["reason"] == "exposure"
    assert len(tracker.trades) == 1 and tracker.trades[0].signal.direction is Direction.LONG


def test_candles_must_arrive_in_order_without_gaps_r1():
    orch, _, _ = build([Scripted("smc", {})])
    orch.on_context(ctx(idx=0))
    with pytest.raises(OrchestratorError, match="не следом"):
        orch.on_context(ctx(idx=2))
    with pytest.raises(OrchestratorError):
        orch.on_context(ctx(idx=0))  # повтор


def test_same_signal_twice_is_a_bug_r1():
    s = mk(bar=1)
    orch, _, _ = build([Scripted("smc", {1: [s], 2: [s]})])
    feed(orch, 1)
    with pytest.raises(OrchestratorError, match="повторно"):
        orch.on_context(ctx(idx=2))


def test_strategy_exceptions_are_not_swallowed_r1():
    class Boom:
        name = "smc"

        def detect(self, c):
            raise ValueError("сбой стратегии")

    orch, _, _ = build([Boom()])
    with pytest.raises(ValueError, match="сбой стратегии"):
        orch.on_context(ctx(idx=0))


def test_wiring_is_validated_at_construction():
    log, tracker = MemoryEventLog(), None
    tracker = VirtualTracker(log)
    with pytest.raises(ConfigError, match="Нет маршрута"):
        Orchestrator([Scripted("smc", {})], {}, log)
    with pytest.raises(ConfigError, match="не уникальны"):
        Orchestrator([Scripted("smc", {}), Scripted("smc", {})], {"smc": Route(Mode.SHADOW, tracker)}, log)
    with pytest.raises(ConfigError):
        Orchestrator([Scripted("smc", {})], {"smc": Route(Mode.SHADOW, tracker)}, log, max_signal_age_ms=0)


def test_adopt_held_restores_the_coin_lock_after_restart():
    smc = Scripted("smc", {2: [mk(bar=2)]})
    orch, tracker, log = build([smc])
    orch.adopt_held("smc", "BTCUSDT", "smc:BTCUSDT:15m:1:short")  # сделка пережила перезапуск
    assert orch.held() == {"smc:BTCUSDT:15m:1:short": ("BTCUSDT", "shadow:smc")}
    events = feed(orch, 2)
    assert kinds(events) == ["skipped_due_to_lock"]  # монета занята вернувшейся сделкой
    with pytest.raises(OrchestratorError, match="занята"):
        orch.adopt_held("smc", "BTCUSDT", "smc:BTCUSDT:15m:9:short")


def test_events_from_orchestrator_and_tracker_share_one_journal_in_order():
    smc = Scripted("smc", {1: [mk(bar=1)]})
    orch, tracker, log = build([smc])
    feed(orch, 1)
    tracker.on_tick(tick(100.5, 10))
    assert [e.kind for e in log.events] == ["trade_registered", "signal_accepted", "limit_filled"]
