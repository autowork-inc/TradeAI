"""Scale Out в VirtualTracker (решение 2026-10-10): +1R - 50% и безубыток, +3R - 40%, остаток 10% (runner) - трейлинг-стоп.

Шорт: вход 100, SL 105 (R = 5): +1R = 95, +3R = 85. Лонг - зеркально (SL 95, +1R = 105, +3R = 115).
"""

import pytest

from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import (
    CLOSED, DEFAULT_PLAN, LEGACY_PLAN, OPEN, ScaleOutPlan, TrackerError, VirtualTracker,
)
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
    return frac * (sgn * (entry - exit_) / risk) - frac * (MAKER * entry + TAKER * exit_) / risk


# ---------------------------------------------------------------------------------------------- план


@pytest.mark.parametrize("kwargs", [
    dict(levels=()), dict(levels=((3.0, 0.5), (1.0, 0.4)), trail_mode="atr"), dict(levels=((1.0, 0.7), (3.0, 0.4)), trail_mode="atr"),
    dict(levels=((1.0, 0.5),)),  # остаток 50%, а трейлинга нет
    dict(levels=((1.0, 1.0),), trail_mode="atr"),  # остатка нет, а трейлинг задан
    dict(levels=((1.0, 0.5),), trail_mode="atr", trail_atr_mult=0.0), dict(levels=((0.0, 0.5),), trail_mode="atr"),
])
def test_invalid_plans_are_rejected(kwargs):
    with pytest.raises(ConfigError):
        ScaleOutPlan(**kwargs)


def test_default_plan_is_50_40_10_with_atr_trail_and_legacy_is_full_exit():
    assert DEFAULT_PLAN.levels == ((1.0, 0.5), (3.0, 0.4)) and DEFAULT_PLAN.runner_frac == pytest.approx(0.1)
    assert DEFAULT_PLAN.be_after_first and DEFAULT_PLAN.trail_mode == "atr"
    assert LEGACY_PLAN.levels == ((3.0, 1.0),) and LEGACY_PLAN.runner_frac == 0.0


# ---------------------------------------------------------------------------------- первое закрытие и BE


def test_plus_1r_closes_half_at_tick_price_moves_stop_to_breakeven_and_keeps_the_rest_open():
    tr, log, rec = make()
    t = opened(tr)
    tr.on_tick(tick(94.9, 20))  # +1.02R
    assert t.status == OPEN and t.remaining == pytest.approx(0.5) and t.levels_done == 1
    (leg,) = t.legs
    assert (leg.reason, leg.frac, leg.price) == ("scale_1r", 0.5, 94.9)
    assert leg.gross_r == pytest.approx(0.5 * 5.1 / 5) and leg.net_r == pytest.approx(leg_net(0.5, 100.0, 94.9))
    assert t.be_armed and t.be_armed_ts == 20 and t.stop_m == 100.0 and t.stop_kind == "be"  # SL на входе, с этого же тика
    assert [e.kind for e in log.events][-3:] == ["limit_filled", "partial_close", "be_armed"]
    tr.on_tick(tick(100.2, 30))  # откат к входу -> оставшиеся 50% закрыты по безубытку
    assert t.status == CLOSED and t.exit_reason == "be" and [g.reason for g in t.legs] == ["scale_1r", "be"]
    assert t.net_r == pytest.approx(leg_net(0.5, 100.0, 94.9) + leg_net(0.5, 100.0, 100.2))
    assert t.net_r > 0  # половина зафиксирована: сделка в плюсе, хотя вторая половина вышла «в ноль»
    assert t.pnl_usdt == pytest.approx(t.net_r * 100.0) and t.exit_price == pytest.approx(0.5 * 94.9 + 0.5 * 100.2)


def test_mark_price_already_armed_be_is_not_armed_twice():
    tr, log, _ = make()
    t = opened(tr)
    tr.on_tick(tick(95.0, 15, "mark"))  # D11: mark дошла до +1R - стоп на входе ещё до частичного закрытия
    assert t.be_armed and t.stop_m == 100.0 and not t.legs
    tr.on_tick(tick(94.8, 20))
    assert [g.reason for g in t.legs] == ["scale_1r"] and [e.kind for e in log.events].count("be_armed") == 1


def test_stop_before_any_level_is_a_full_loss_and_does_not_scale_out():
    tr, _, _ = make()
    t = opened(tr)
    tr.on_tick(tick(105.2, 20))
    assert t.status == CLOSED and t.exit_reason == "sl" and len(t.legs) == 1 and t.legs[0].frac == 1.0
    assert t.gross_r == pytest.approx(-1.04)


# ------------------------------------------------------------------------------------------- +3R и runner


def test_plus_3r_closes_40_percent_and_leaves_a_10_percent_runner():
    tr, _, rec = make()
    t = opened(tr)
    tr.on_tick(tick(94.0, 20))
    tr.on_tick(tick(84.5, 30))  # +3.1R
    assert [(g.reason, g.frac) for g in t.legs] == [("scale_1r", 0.5), ("scale_3r", 0.4)]
    assert t.status == OPEN and t.remaining == pytest.approx(0.1) and t.runner_active
    assert t.stop_m == 100.0  # пока трейлинга нет: стоп на входе


def test_gap_through_both_levels_closes_both_legs_at_the_gap_price():
    tr, _, _ = make()
    t = opened(tr)
    tr.on_tick(tick(80.0, 20))
    assert [(g.reason, g.price) for g in t.legs] == [("scale_1r", 80.0), ("scale_3r", 80.0)] and t.be_armed
    assert t.remaining == pytest.approx(0.1) and t.status == OPEN


def test_atr_trail_follows_best_price_by_3_atr_and_only_tightens():
    tr, log, _ = make()
    t = opened(tr)
    tr.on_tick(tick(84.0, 20))  # лучшая цена 84 (+3.2R): 1R и 3R закрыты, runner открыт
    tr.on_bar_closed(**bar(1, atr=2.0))  # стоп = 84 + 3 * 2 = 90
    assert t.stop_m == pytest.approx(90.0) and t.stop_kind == "trail"
    tr.on_bar_closed(**bar(2, atr=4.0))  # ATR вырос: стоп НЕ ослабляется (84 + 12 = 96 > 90)
    assert t.stop_m == pytest.approx(90.0)
    tr.on_tick(tick(80.0, 30))  # новая лучшая цена
    tr.on_bar_closed(**bar(3, atr=2.0))  # 80 + 6 = 86
    assert t.stop_m == pytest.approx(86.0)
    tr.on_tick(tick(86.4, 40))  # откат до стопа: выход остатка по цене тика
    assert t.status == CLOSED and t.exit_reason == "trail" and t.legs[-1].frac == pytest.approx(0.1) and t.legs[-1].price == 86.4
    assert t.net_r == pytest.approx(sum(g.net_r for g in t.legs)) and len(t.legs) == 3
    assert [e.kind for e in log.events].count("trail_moved") == 2
    assert tr.summary()["smc/short"]["trail"] == 1 and tr.summary()["smc/short"]["partials"] == 1


def test_trail_never_starts_before_the_last_level_and_never_loosens_breakeven():
    tr, log, _ = make()
    t = opened(tr)
    tr.on_tick(tick(94.0, 20))  # только +1R
    tr.on_bar_closed(**bar(1, atr=0.5))
    assert t.stop_m == 100.0 and t.stop_kind == "be" and "trail_moved" not in [e.kind for e in log.events]
    tr.on_tick(tick(84.0, 30))
    tr.on_bar_closed(**bar(2, atr=40.0))  # огромный ATR: кандидат 84 + 120 хуже безубытка - стоп остаётся на входе
    assert t.stop_m == 100.0 and t.stop_kind == "be"


def test_wick_trail_uses_extreme_of_last_bars_and_ratchets():
    plan = ScaleOutPlan(DEFAULT_PLAN.levels, True, "wick", trail_wick_bars=2)
    tr, _, _ = make(plan)
    t = opened(tr)
    tr.on_tick(tick(84.0, 20))
    tr.on_bar_closed(**bar(1, high=93.0, low=88.0))
    assert t.stop_m == pytest.approx(93.0)  # шорт: стоп над хаем последней свечи
    tr.on_bar_closed(**bar(2, high=90.0, low=86.0))
    assert t.stop_m == pytest.approx(93.0)  # окно из 2 свечей: максимум хаёв (93, 90) = 93
    tr.on_bar_closed(**bar(3, high=89.0, low=85.0))
    assert t.stop_m == pytest.approx(90.0)  # свеча 1 вышла из окна: max(90, 89)
    tr.on_tick(tick(90.1, 30))
    assert t.exit_reason == "trail"


def test_runner_without_bar_data_is_a_protocol_violation_r1():
    tr, _, _ = make()
    t = opened(tr)
    tr.on_tick(tick(84.0, 20))
    with pytest.raises(TrackerError, match="трейлинг"):
        tr.on_bar_closed("BTCUSDT", IV, 100.0, 1)
    tr2, _, _ = make()
    opened(tr2)
    tr2.on_tick(tick(84.0, 20))
    with pytest.raises(TrackerError, match="ATR"):
        tr2.on_bar_closed(**bar(1, atr=None))


def test_trail_takes_effect_only_on_following_ticks_no_lookahead():
    tr, _, _ = make()
    t = opened(tr)
    tr.on_tick(tick(84.0, 20))
    tr.on_tick(tick(87.0, 21))  # цена выше будущего стопа (90 не достигнут) - runner жив
    assert t.status == OPEN
    tr.on_bar_closed(**bar(1, atr=2.0))  # стоп 90 встал только после закрытия свечи
    tr.on_tick(tick(87.5, 22))
    assert t.status == OPEN  # 87.5 < 90: до стопа далеко
    tr.on_tick(tick(90.2, 23))
    assert t.status == CLOSED


def test_timeout_closes_the_runner_at_bar_close_price():
    tr, _, _ = make(max_hold_bars=3)
    t = opened(tr)
    tr.on_tick(tick(84.0, 20))
    for k in range(1, 4):
        tr.on_bar_closed(**bar(k, atr=50.0))
    assert t.status == OPEN
    tr.on_bar_closed(**bar(4, atr=50.0, close=88.0))
    assert t.exit_reason == "timeout" and t.legs[-1].price == 88.0 and t.legs[-1].frac == pytest.approx(0.1)


# ------------------------------------------------------------------------------------------ лонг и MARKET


def test_long_mirror_full_path():
    tr, _, _ = make()
    t = opened(tr, mk(direction=Direction.LONG))  # вход 100, SL 95, +1R = 105, +3R = 115
    tr.on_tick(tick(105.3, 20))
    tr.on_tick(tick(115.4, 30))
    assert [g.reason for g in t.legs] == ["scale_1r", "scale_3r"] and t.stop_m * t.sgn == 100.0
    tr.on_bar_closed(**bar(1, high=117.0, low=114.0, atr=2.0))  # лучшая цена 115.4: стоп = 115.4 - 6 = 109.4
    assert t.stop_m * t.sgn == pytest.approx(109.4)
    tr.on_tick(tick(109.0, 40))
    assert t.exit_reason == "trail" and t.exit_price == pytest.approx(0.5 * 105.3 + 0.4 * 115.4 + 0.1 * 109.0)
    assert t.net_r > 3.0 * 0.4 + 0.5 - 0.1  # 50% на +1R и 40% на +3R дают ~1.7R до комиссий


def test_market_trade_levels_follow_the_actual_entry_risk():
    tr, _, _ = make()
    assert tr.submit(mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0), 1000).accepted
    t = tr.trades[-1]
    tr.on_tick(tick(100.5, 11))  # факт 100.5: R = 5.5, +1R = 106, +3R = 117
    assert t.fill_price == 100.5 and t.risk == pytest.approx(5.5)
    tr.on_tick(tick(105.9, 12))
    assert not t.legs
    tr.on_tick(tick(106.0, 13))
    assert [g.reason for g in t.legs] == ["scale_1r"]
    assert t.plan_tp_price() == pytest.approx(117.0)
    fee_in = TAKER * 100.5  # MARKET-вход - taker
    assert t.legs[0].fees_r == pytest.approx(0.5 * (fee_in + TAKER * 106.0) / 5.5)


def test_plan_without_runner_closes_everything_at_the_last_level_as_tp():
    tr, _, _ = make(ScaleOutPlan(((1.0, 0.5), (3.0, 0.5)), True))
    t = opened(tr)
    tr.on_tick(tick(94.0, 20))
    tr.on_tick(tick(84.0, 30))
    assert t.status == CLOSED and t.exit_reason == "tp" and [g.reason for g in t.legs] == ["scale_1r", "tp"]
    assert tr.summary()["smc/short"]["tp"] == 1


# --------------------------------------------------------------------------------------------- запись в БД


def test_recorder_gets_every_state_of_the_trade_and_every_leg():
    tr, _, rec = make()
    t = opened(tr)
    assert rec.trade_history[0]["status"] == "pending_limit" and rec.trade_history[0]["planned_entry"] == 100.0
    assert rec.trade_history[0]["entry_price"] is None and rec.trade_history[0]["plan"]["runner_frac"] == pytest.approx(0.1)
    tr.on_tick(tick(94.0, 20))
    mid = rec.trades[t.trade_id]
    assert mid["status"] == "open" and mid["remaining_frac"] == pytest.approx(0.5) and mid["be_armed"] and mid["stop_price"] == 100.0
    assert mid["r_multiple"] == pytest.approx(leg_net(0.5, 100.0, 94.0))  # реализованный результат виден по ходу сделки
    tr.on_tick(tick(84.0, 21))
    tr.on_bar_closed(**bar(1, atr=2.0))
    tr.on_tick(tick(90.5, 30))
    final = rec.trades[t.trade_id]
    assert final["status"] == "closed" and final["exit_reason"] == "trail" and final["remaining_frac"] == 0.0
    assert final["r_multiple"] == pytest.approx(t.net_r) and final["pnl"] == pytest.approx(t.pnl_usdt)
    assert final["qty"] == pytest.approx(20.0) and final["fees"] == pytest.approx(sum(g.fees_r for g in t.legs) * 100.0)
    assert [(c["seq"], c["reason"]) for c in rec.closes] == [(1, "scale_1r"), (2, "scale_3r"), (3, "trail")]
    assert sum(c["frac"] for c in rec.closes) == pytest.approx(1.0) and sum(c["qty"] for c in rec.closes) == pytest.approx(20.0)
    assert sum(c["net_r"] for c in rec.closes) == pytest.approx(final["r_multiple"])
    assert sum(c["pnl"] for c in rec.closes) == pytest.approx(final["pnl"])


def test_cancelled_trade_is_recorded_without_fill_data():
    tr, _, rec = make()
    assert tr.submit(mk(), 1000).accepted
    tr.on_tick(tick(84.0, 5))  # цена дошла до TP до исполнения
    row = rec.trades["smc:BTCUSDT:15m:0:short"]
    assert row["status"] == "cancelled" and row["exit_reason"] == "tp_before_fill" and row["entry_price"] is None and rec.closes == []


def test_gap_flag_is_persisted():
    tr, _, rec = make()
    t = opened(tr)
    tr.mark_gap("BTCUSDT", 99)
    assert rec.trades[t.trade_id]["gap_affected"] is True
