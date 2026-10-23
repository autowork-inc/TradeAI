"""Восстановление ТЕНЕВЫХ сделок после рестарта (2026-10-23): деплой не должен убивать виртуальные позиции.

Сценарий каждого теста: первый трекер ведёт сделку и пишет строки в MemoryRecorder (это то, что лежало бы в PostgreSQL) -> из строк собираются `DbShadowTrade`
(как `persistence.pg.fetch_open_shadow`) -> ВТОРОЙ трекер поднимает их `restore()` и доводит до конца. Восстановленная сделка обязана нести gap_affected.
"""

from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import OPEN, PENDING_LIMIT, ScaleOutPlan, VirtualTracker
from orchestrator.types import Direction, Kind, Mode
from persistence.recorder import DbShadowTrade, MemoryRecorder
from tests.orchestrator.helpers import IV, SYM, Scripted, ctx, mk, tick

TWO_LEVELS = ScaleOutPlan(((1.0, 0.5), (4.0, 0.5)))


def db_items(rec: MemoryRecorder, signal, *, with_meta: bool = True) -> list[DbShadowTrade]:
    """Что отдал бы `fetch_open_shadow`: активные строки + их закрытия + meta снапшота (entry/sl/tp/inval/interval)."""
    meta = {"entry": signal.entry, "sl": signal.sl, "tp": signal.tp, "inval": signal.inval, "interval": signal.interval, "kind": "demo"} if with_meta else {}
    out = []
    for row in rec.trades.values():
        if row["status"] in ("pending_limit", "open"):
            closes = tuple(c for c in rec.closes if c["trade_id"] == row["id"])
            out.append(DbShadowTrade(dict(row), closes, meta, tuple(signal.geometry)))
    return out


def first_run(signal, *ticks, plans=None):
    rec = MemoryRecorder()
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec, plans=plans)
    tr.submit(signal, 1000)
    for t in ticks:
        tr.on_tick(t)
    return rec, tr


def second_run(rec, signal, now_ms, *, with_meta=True, **kw):
    rec2 = MemoryRecorder()
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec2, **kw)
    restored, skipped = tr.restore(db_items(rec, signal, with_meta=with_meta), now_ms)
    return rec2, tr, restored, skipped


def test_open_position_survives_restart_and_hits_stop_with_gap_flag():
    s = mk()  # шорт: вход 100 (LIMIT), SL 105, TP 80
    rec, t1 = first_run(s, tick(100.5, 2))  # шорт-лимит продаёт на росте: 100.5 > 100 -> исполнена по 100
    assert t1.active()[0].status == OPEN
    old = t1.active()[0]
    rec2, t2, restored, skipped = second_run(rec, s, now_ms=2 * IV)
    assert skipped == [] and len(restored) == 1
    r = restored[0]
    assert r.status == OPEN and r.gap_affected and r.fill_price == 100.0 and r.sizing.qty == old.sizing.qty and r.stop_m == old.stop_m and r.remaining == 1.0
    assert r.tp_m == old.tp_m and r.risk == old.risk
    assert rec2.trades[s.signal_id]["gap_affected"] is True and rec2.trades[s.signal_id]["status"] == "open"
    t2.on_tick(tick(105.5, 5))  # стоп
    done = t2.trades[0]
    assert done.status == "closed" and done.exit_reason == "sl" and done.net_r < -1 and done.gap_affected
    assert rec2.trades[s.signal_id]["status"] == "closed" and rec2.trades[s.signal_id]["gap_affected"] is True
    assert t2.summary()["smc/short"]["sum_net_r"] == 0.0  # в статистику суммы R восстановленная сделка не входит


def test_open_position_reaches_take_after_restart():
    s = mk()
    rec, _ = first_run(s, tick(100.5, 2))
    _, t2, restored, _ = second_run(rec, s, now_ms=2 * IV)
    t2.on_tick(tick(79.9, 6))
    tr = restored[0]
    assert tr.status == "closed" and tr.exit_reason == "tp" and tr.net_r > 3.5


def test_pending_limit_survives_and_fills_later():
    s = mk()
    rec, t1 = first_run(s)
    assert t1.active()[0].status == PENDING_LIMIT
    _, t2, restored, _ = second_run(rec, s, now_ms=IV + 5)
    r = restored[0]
    assert r.status == PENDING_LIMIT and r.gap_affected and r.entry_m == r.sgn * 100.0
    t2.on_tick(tick(100.5, 7))
    assert r.status == OPEN and r.fill_price == 100.0


def test_partial_closes_are_restored_and_seq_continues():
    s = mk()
    rec, t1 = first_run(s, tick(100.5, 2), tick(94.9, 3), plans={"smc": TWO_LEVELS})  # +1R: 95 -> первое закрытие 50%
    assert len(t1.trades[0].legs) == 1 and abs(t1.trades[0].remaining - 0.5) < 1e-9
    _, t2, restored, _ = second_run(rec, s, now_ms=3 * IV, plans={"smc": TWO_LEVELS})
    r = restored[0]
    assert r.levels_done == 1 and abs(r.remaining - 0.5) < 1e-9 and len(r.legs) == 1 and r.legs[0].price == 94.9
    t2.on_tick(tick(79.0, 9))
    assert r.status == "closed" and [g.seq for g in r.legs] == [1, 2] and r.exit_reason == "tp"


def test_unreadable_trade_is_aborted_not_fatal():
    s = mk()
    rec, _ = first_run(s, tick(100.5, 2))
    rec2, t2, restored, skipped = second_run(rec, s, now_ms=2 * IV, with_meta=False)
    assert restored == [] and skipped == [s.signal_id] and t2.active() == []
    assert rec2.trades[s.signal_id]["status"] == "aborted" and rec2.trades[s.signal_id]["exit_reason"] == "process_restart"


def test_bar_counters_follow_wall_clock_so_timeout_still_fires():
    s = mk()
    rec, _ = first_run(s, tick(100.5, 2))
    _, t2, restored, _ = second_run(rec, s, now_ms=(1 + 192 + 5) * IV)  # простой дольше тайм-аута сделки
    r = restored[0]
    assert r.bars_closed >= 192
    t2.on_bar_closed(SYM, (1 + 192 + 5) * IV, 99.0, (1 + 192 + 6) * IV, interval="15m")
    assert r.status == "closed" and r.exit_reason == "timeout"


def test_restored_trade_keeps_coin_lock_and_second_restore_is_idempotent_by_id():
    s = mk()
    rec, _ = first_run(s, tick(100.5, 2))
    rec2, t2, restored, _ = second_run(rec, s, now_ms=2 * IV)
    orch = Orchestrator([Scripted("smc", {1: [mk(bar=1)]})], {"smc": Route(Mode.SHADOW, t2)}, MemoryEventLog(), recorder=rec2)
    orch.adopt_held("smc", SYM, restored[0].trade_id)
    events = orch.on_context(ctx(idx=1, received=2 * IV + 1))  # свеча бара 1: стратегия выдаёт новый сигнал по занятой монете
    assert [e.kind for e in events] == ["skipped_due_to_lock"]
    t2.on_tick(tick(105.5, 8))
    assert orch.held() == {}  # закрытие восстановленной сделки сняло блокировку


def test_market_trade_restored_when_open():
    s = mk(kind=Kind.MARKET, direction=Direction.LONG, entry=100.0, sl=95.0)
    rec, t1 = first_run(s, tick(100.0, 2))  # MARKET исполнился по первому тику
    assert t1.active()[0].status == OPEN
    _, t2, restored, _ = second_run(rec, s, now_ms=2 * IV)
    r = restored[0]
    assert r.status == OPEN and r.gap_affected and abs(r.tp_m - r.level_q(4.0)) < 1e-9
    t2.on_tick(tick(120.1, 9))
    assert r.status == "closed" and r.exit_reason == "tp"
