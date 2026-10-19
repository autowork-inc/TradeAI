"""Единый план выхода VirtualTracker (решение 2026-10-19): ОДИН тейк на весь объём строго 1:4, без частичных фиксаций, безубытка, остатка и трейлинга.

Шорт: вход 100, SL 105 (R = 5): TP = 80 (4R). Лонг - зеркально (SL 95, TP 120). Риск сделки - 5 USDT, объём = 5 / R = 1.0.
Имя файла историческое (раньше здесь жил Scale Out 50/40/10).
"""

import pytest

from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import CLOSED, DEFAULT_PLAN, LEGACY_PLAN, OPEN, ScaleOutPlan, VirtualTracker
from orchestrator.types import BarInfo, Direction, Kind
from persistence.recorder import MemoryRecorder
from tests.orchestrator.helpers import IV, mk, tick
from utils.errors import ConfigError

MAKER, TAKER = 0.0002, 0.0005


def make(plan=DEFAULT_PLAN, **kw):
    log, rec = MemoryEventLog(), MemoryRecorder()
    return VirtualTracker(log, equity=10_000.0, plan=plan, recorder=rec, **kw), log, rec


def opened(tr, signal=None):
    assert tr.submit(signal or mk(), 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(100.5, 10) if t.sgn > 0 else tick(99.5, 10))  # заявка исполнена по цене заявки (100)
    assert t.status == OPEN and t.fill_price == 100.0
    return t


def bar(k, high=101.0, low=99.0, atr=2.0, close=100.0):
    return dict(symbol="BTCUSDT", bar_open_time=k * IV, close=close, now_ms=k, bar=BarInfo(high, low, atr))


def leg_net(frac, entry, exit_, risk=5.0, sgn=1.0):
    """net R части сделки (шорт: sgn=1): комиссии в долях R."""
    return frac * (sgn * (entry - exit_) / risk) - frac * (MAKER * entry + TAKER * exit_) / risk


# ---------------------------------------------------------------------------------------------- план


@pytest.mark.parametrize("kwargs", [
    dict(levels=()), dict(levels=((3.0, 0.5), (1.0, 0.5))), dict(levels=((1.0, 0.7), (3.0, 0.4))),
    dict(levels=((4.0, 0.5),)),  # доли дают 50%, остатка-runner нет - сумма обязана быть 1
    dict(levels=((0.0, 1.0),)), dict(levels=((4.0, 1.0),), tp_anchor="bogus"),
])
def test_invalid_plans_are_rejected(kwargs):
    with pytest.raises(ConfigError):
        ScaleOutPlan(**kwargs)


def test_default_plan_is_one_take_profit_at_4r_for_the_whole_volume():
    assert DEFAULT_PLAN.levels == ((4.0, 1.0),) and LEGACY_PLAN.levels == ((4.0, 1.0),)
    assert DEFAULT_PLAN.tp_anchor == "fill"


def test_legacy_records_with_a_runner_are_normalised_to_one():
    plan = ScaleOutPlan.from_record([[1.0, 0.5], [3.0, 0.4]])
    assert plan.levels[0][0] == 1.0 and sum(f for _, f in plan.levels) == pytest.approx(1.0)


# ------------------------------------------------------------------------------------- единственный тейк


def test_one_take_profit_closes_everything_at_4r_with_no_partial_fills():
    tr, log, rec = make()
    t = opened(tr)
    for p in (94.0, 90.0, 85.0):  # +1.2R, +2R, +3R: частичных закрытий нет
        tr.on_tick(tick(p, 20))
        assert t.status == OPEN and not t.legs and t.remaining == 1.0
    tr.on_tick(tick(79.5, 30))  # +4.1R
    assert t.status == CLOSED and t.exit_reason == "tp" and [(g.reason, g.frac) for g in t.legs] == [("tp", 1.0)]
    assert t.gross_r == pytest.approx(4.1) and t.net_r == pytest.approx(leg_net(1.0, 100.0, 79.5))
    assert t.sizing.qty == pytest.approx(1.0) and t.pnl_usdt == pytest.approx(t.net_r * 5.0)  # 1R = 5 USDT
    assert "partial_close" not in [e.kind for e in log.events]


def test_stop_is_static_and_loses_exactly_five_usdt_before_fees():
    tr, log, _ = make()
    t = opened(tr)
    tr.on_tick(tick(95.0, 15, "mark"))  # mark +1R ничего не двигает
    tr.on_tick(tick(94.0, 20))
    tr.on_tick(tick(100.2, 25))
    assert t.stop_m == 105.0 and t.stop_kind == "sl" and t.status == OPEN and not t.be_armed
    tr.on_tick(tick(105.0, 30))
    assert t.status == CLOSED and t.exit_reason == "sl"
    assert t.gross_r == pytest.approx(-1.0) and t.sizing.qty * 5.0 == pytest.approx(5.0)
    assert "be_armed" not in [e.kind for e in log.events] and "trail_moved" not in [e.kind for e in log.events]


def test_bars_never_move_the_stop_no_trailing():
    tr, log, _ = make()
    t = opened(tr)
    tr.on_tick(tick(85.0, 20))
    for k in range(1, 5):
        tr.on_bar_closed(**bar(k, high=93.0, low=88.0, atr=2.0))
    assert t.stop_m == 105.0 and t.stop_kind == "sl" and "trail_moved" not in [e.kind for e in log.events]


def test_gap_through_the_take_profit_fills_at_the_gap_price():
    tr, _, _ = make()
    t = opened(tr)
    tr.on_tick(tick(70.0, 20))
    assert [(g.reason, g.price) for g in t.legs] == [("tp", 70.0)] and t.status == CLOSED


def test_timeout_closes_the_whole_position_at_bar_close_price():
    tr, _, _ = make(max_hold_bars=3)
    t = opened(tr)
    tr.on_tick(tick(94.0, 20))
    for k in range(1, 4):
        tr.on_bar_closed(**bar(k, atr=50.0))
    assert t.status == OPEN
    tr.on_bar_closed(**bar(4, atr=50.0, close=96.0))
    assert t.exit_reason == "timeout" and t.legs[-1].price == 96.0 and t.legs[-1].frac == pytest.approx(1.0)


# ------------------------------------------------------------------------------------------ лонг и MARKET


def test_long_mirror_full_path():
    tr, _, _ = make()
    t = opened(tr, mk(direction=Direction.LONG))  # вход 100, SL 95, TP 120
    tr.on_tick(tick(105.3, 20))
    tr.on_tick(tick(119.0, 25))
    assert t.status == OPEN
    tr.on_tick(tick(120.4, 30))
    assert [g.reason for g in t.legs] == ["tp"] and t.status == CLOSED and t.exit_price == pytest.approx(120.4)
    assert t.gross_r == pytest.approx(4.08)


def test_market_trade_tp_is_exactly_four_times_the_stop_distance_from_the_actual_fill():
    tr, _, _ = make()
    assert tr.submit(mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0), 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(100.5, 11))  # факт 100.5: R = 5.5, TP = 100.5 + 22 = 122.5
    assert t.fill_price == 100.5 and t.risk == pytest.approx(5.5)
    assert t.plan_tp_price() == pytest.approx(122.5)
    assert (t.plan_tp_price() - t.fill_price) == pytest.approx(4 * (t.fill_price - 95.0))
    assert t.sizing.qty == pytest.approx(5.0 / 5.5)  # объём от факта: потеря на стопе = 5 USDT
    tr.on_tick(tick(122.4, 12))
    assert not t.legs
    tr.on_tick(tick(122.5, 13))
    assert [g.reason for g in t.legs] == ["tp"]
    assert t.legs[0].fees_r == pytest.approx((TAKER * 100.5 + TAKER * 122.5) / 5.5)


# --------------------------------------------------------------------------------------------- запись в БД


def test_recorder_gets_every_state_of_the_trade_and_its_single_close():
    tr, _, rec = make()
    t = opened(tr)
    assert rec.trade_history[0]["status"] == "pending_limit" and rec.trade_history[0]["planned_entry"] == 100.0
    assert rec.trade_history[0]["entry_price"] is None and rec.trade_history[0]["plan"]["levels"] == [[4.0, 1.0]]
    tr.on_tick(tick(94.0, 20))
    mid = rec.trades[t.trade_id]
    assert mid["status"] == "open" and mid["remaining_frac"] == 1.0 and not mid["be_armed"] and mid["stop_price"] == 105.0
    tr.on_tick(tick(79.0, 21))
    final = rec.trades[t.trade_id]
    assert final["status"] == "closed" and final["exit_reason"] == "tp" and final["remaining_frac"] == 0.0
    assert final["r_multiple"] == pytest.approx(t.net_r) and final["pnl"] == pytest.approx(t.pnl_usdt)
    assert final["qty"] == pytest.approx(1.0) and final["fees"] == pytest.approx(sum(g.fees_r for g in t.legs) * 5.0)
    assert [(c["seq"], c["reason"]) for c in rec.closes] == [(1, "tp")]
    assert sum(c["frac"] for c in rec.closes) == pytest.approx(1.0) and rec.closes[0]["qty"] == pytest.approx(1.0)
    assert rec.closes[0]["net_r"] == pytest.approx(final["r_multiple"]) and rec.closes[0]["pnl"] == pytest.approx(final["pnl"])


def test_cancelled_trade_is_recorded_without_fill_data():
    tr, _, rec = make()
    assert tr.submit(mk(), 1000).accepted
    tr.on_tick(tick(79.0, 5))  # цена дошла до TP до исполнения
    row = rec.trades["smc:BTCUSDT:15m:0:short"]
    assert row["status"] == "cancelled" and row["exit_reason"] == "tp_before_fill" and row["entry_price"] is None and rec.closes == []


def test_gap_flag_is_persisted():
    tr, _, rec = make()
    t = opened(tr)
    tr.mark_gap("BTCUSDT", 99)
    assert rec.trades[t.trade_id]["gap_affected"] is True
