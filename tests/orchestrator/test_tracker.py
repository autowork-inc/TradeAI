"""VirtualTracker: исполнение по тикам WebSocket. Шорт: вход 100, SL 105 (R = 5), +1R = 95, TP 85. Лонг - зеркало."""

import pytest

from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import (
    CANCELLED, CLOSED, DEFAULT_PLAN, LEGACY_PLAN, OPEN, PENDING_LIMIT, ScaleOutPlan, Submission, TrackerError, VirtualTracker,
)
from orchestrator.types import Direction, Kind
from tests.orchestrator.helpers import IV, mk, tick

MAKER, TAKER = 0.0002, 0.0005


def make(**kw) -> tuple[VirtualTracker, MemoryEventLog]:
    log = MemoryEventLog()
    kw.setdefault("plan", LEGACY_PLAN)  # механика входа/стопа/безубытка: полный выход на 3R, как до Scale Out (новый план - ниже)
    return VirtualTracker(log, equity=10_000.0, **kw), log


def opened(tr: VirtualTracker, signal=None):
    t = tr.submit(signal or mk(), 1000)
    assert t == Submission(True)
    return tr.trades[-1]


def fill_short(tr, t, price=100.5):
    tr.on_tick(tick(price, 10))
    assert t.status == OPEN and t.fill_price == 100.0  # заявка исполнена по цене заявки, не по тику


def test_limit_fills_only_when_price_passes_the_level_not_on_touch():
    tr, log = make()
    t = opened(tr)
    tr.on_tick(tick(99.0))
    tr.on_tick(tick(100.0))  # касание
    assert t.status == PENDING_LIMIT
    tr.on_tick(tick(100.5, 77))
    assert t.status == OPEN and t.fill_price == 100.0 and t.fill_ts == 77
    assert t.sizing.qty == pytest.approx(20.0)  # риск 100 USDT / R 5
    assert [e.kind for e in log.events] == ["trade_registered", "limit_filled"]


def test_be_is_armed_by_mark_price_and_exits_at_entry_with_slippage_d11():
    tr, log = make()
    t = opened(tr)
    fill_short(tr, t)
    tr.on_tick(tick(95.0, 20, "mark"))  # mark достиг +1R
    assert t.be_armed and t.be_armed_ts == 20
    tr.on_tick(tick(99.0, 21))  # внутри: ничего
    assert t.status == OPEN
    tr.on_tick(tick(100.2, 30))  # цена вернулась к входу -> стоп-маркет на входе, исполнение по цене тика
    assert t.status == CLOSED and t.exit_reason == "be" and t.exit_price == 100.2
    assert t.gross_r == pytest.approx(-0.04)
    fee = (MAKER * 100.0 + TAKER * 100.2) / 5.0
    assert t.net_r == pytest.approx(-0.04 - fee) and t.pnl_usdt == pytest.approx(t.net_r * 100.0)
    assert [e.kind for e in log.events][-3:] == ["limit_filled", "be_armed", "trade_closed"]


def test_trade_ticks_alone_do_not_arm_be_only_mark_does_d11():
    tr, _ = make()
    t = opened(tr)
    fill_short(tr, t)
    tr.on_tick(tick(94.0, 20))  # цена сделки на +1.2R, но mark не пришёл
    assert not t.be_armed
    tr.on_tick(tick(105.2, 30))  # откат -> обычный SL, не BE
    assert t.exit_reason == "sl" and t.gross_r == pytest.approx(-1.04)
    assert t.mfe_r == pytest.approx(1.2) and t.mae_r == pytest.approx(1.04)


def test_mark_before_fill_or_after_close_is_ignored():
    tr, _ = make()
    t = opened(tr)
    tr.on_tick(tick(90.0, 5, "mark"))  # до исполнения
    assert not t.be_armed and t.status == PENDING_LIMIT


def test_stop_loss_exit_price_is_the_tick_not_the_stop_level():
    tr, _ = make()
    t = opened(tr)
    fill_short(tr, t)
    tr.on_tick(tick(107.0, 40))  # гэп через стоп
    assert t.exit_reason == "sl" and t.exit_price == 107.0 and t.gross_r == pytest.approx(-1.4)


def test_take_profit_three_r():
    tr, _ = make()
    t = opened(tr)
    fill_short(tr, t)
    tr.on_tick(tick(84.5, 50))
    assert t.exit_reason == "tp" and t.gross_r == pytest.approx(3.1) and t.status == CLOSED


def test_tp_reached_before_fill_cancels_the_order():
    tr, log = make()
    t = opened(tr)
    tr.on_tick(tick(84.0, 5))
    assert t.status == CANCELLED and t.exit_reason == "tp_before_fill" and log.events[-1].kind == "trade_cancelled"


def test_limit_expires_after_10_closed_bars_and_is_released_d8():
    tr, _ = make()
    t = opened(tr)
    for k in range(1, 10):
        tr.on_bar_closed("BTCUSDT", k * IV, 99.0, k)
    assert t.status == PENDING_LIMIT
    tr.on_bar_closed("BTCUSDT", 10 * IV, 99.0, 10)
    assert t.status == CANCELLED and t.exit_reason == "ttl" and tr.active() == []


def test_close_beyond_sweep_high_invalidates_pending_limit():
    tr, _ = make()
    t = opened(tr)
    tr.on_bar_closed("BTCUSDT", IV, 106.5, 1)
    assert t.exit_reason == "invalidated"


def test_open_position_times_out_after_max_hold_at_bar_close_price():
    tr, _ = make(max_hold_bars=3)
    t = opened(tr)
    fill_short(tr, t)  # исполнена на 1-й свече после сигнала
    for k in range(1, 4):
        tr.on_bar_closed("BTCUSDT", k * IV, 101.0, k)
    assert t.status == OPEN
    tr.on_bar_closed("BTCUSDT", 4 * IV, 101.0, 4)
    assert t.exit_reason == "timeout" and t.exit_price == 101.0 and t.gross_r == pytest.approx(-0.2)


def test_long_limit_mirror_with_be():
    tr, _ = make()
    t = opened(tr, mk(direction=Direction.LONG))  # вход 100, SL 95, TP 115
    tr.on_tick(tick(100.0))
    assert t.status == PENDING_LIMIT
    tr.on_tick(tick(99.5, 10))
    assert t.status == OPEN and t.fill_price == 100.0
    tr.on_tick(tick(105.0, 11, "mark"))
    assert t.be_armed
    tr.on_tick(tick(99.8, 12))
    assert t.exit_reason == "be" and t.gross_r == pytest.approx(-0.04)


def test_market_long_fills_at_first_tick_and_sets_tp_from_actual_entry():
    tr, log = make()
    t = opened(tr, mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0))
    tr.on_tick(tick(100.5, 10, "mark"))  # mark не исполняет MARKET
    assert t.status != OPEN
    tr.on_tick(tick(100.5, 11))
    assert t.status == OPEN and t.fill_price == 100.5 and t.risk == pytest.approx(5.5)
    assert t.tp_m * t.sgn == pytest.approx(117.0)  # 100.5 + 3 * 5.5
    tr.on_tick(tick(106.0, 12, "mark"))  # +1R = 106
    assert t.be_armed
    tr.on_tick(tick(117.1, 13))
    assert t.exit_reason == "tp" and t.gross_r == pytest.approx(16.6 / 5.5)
    fee = (TAKER * 100.5 + TAKER * 117.1) / 5.5  # MARKET: вход тоже taker
    assert t.net_r == pytest.approx(t.gross_r - fee)
    assert "market_filled" in [e.kind for e in log.events]


def test_market_rejected_by_exposure_or_bad_risk_at_fill():
    tr, _ = make()
    t = opened(tr, mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=99.95))
    tr.on_tick(tick(100.0, 1))  # R = 0.05 -> плечо 20 > 5
    assert t.status == CANCELLED and t.exit_reason == "rejected_exposure"
    t2 = opened(tr, mk(strategy="breakout", symbol="ETHUSDT", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0))
    tr.on_tick(tick(94.0, 2, symbol="ETHUSDT"))  # открылись ниже стопа
    assert t2.exit_reason == "bad_risk"


def test_market_not_filled_before_next_bar_close_means_tick_stream_died_r1():
    tr, _ = make()
    opened(tr, mk(strategy="breakout", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=95.0))
    with pytest.raises(TrackerError, match="тиков"):
        tr.on_bar_closed("BTCUSDT", IV, 100.0, 1)


def test_limit_exceeding_leverage_cap_is_refused_at_submit_without_state():
    tr, log = make()
    res = tr.submit(mk(entry=100.0, sl=100.02, tp=85.0, inval=101.0), 5)
    assert res == Submission(False, "exposure") and tr.trades == [] and log.events[-1].kind == "skipped_risk"


def test_protocol_violations_raise():
    tr, _ = make()
    opened(tr)
    with pytest.raises(TrackerError, match="уже зарегистрирован"):
        tr.submit(mk(), 2)
    with pytest.raises(TrackerError, match="не позже сигнальной"):
        tr.on_bar_closed("BTCUSDT", 0, 100.0, 3)


def test_listeners_see_close_and_cancel_and_other_symbols_are_independent():
    tr, _ = make()
    seen = []
    tr.add_close_listener(seen.append)
    a = opened(tr)
    b = opened(tr, mk(symbol="ETHUSDT"))
    tr.on_tick(tick(84.0, 5))  # BTC: TP до исполнения -> отмена; ETH не затронут
    assert seen == [a] and b.status == PENDING_LIMIT and tr.active("ETHUSDT") == [b]


def test_gap_flags_active_trades_and_summary_excludes_them():
    tr, log = make()
    a = opened(tr)
    fill_short(tr, a)
    tr.mark_gap("BTCUSDT", 99)
    assert a.gap_affected and log.events[-1].kind == "data_gap"
    tr.on_tick(tick(84.0, 100))
    ok = opened(tr, mk(bar=1))
    tr.on_tick(tick(100.5, 101))
    tr.on_tick(tick(84.0, 102))
    s = tr.summary()["smc/short"]
    assert s["signals"] == 2 and s["closed"] == 2 and s["tp"] == 2 and s["gap_affected"] == 1
    assert s["sum_net_r"] == pytest.approx(ok.net_r)  # сделка с обрывом в сумму R не входит


def test_record_is_json_ready_and_has_what_the_future_db_needs():
    tr, _ = make()
    t = opened(tr)
    fill_short(tr, t)
    tr.on_tick(tick(84.0, 9))
    r = t.record()
    assert r["status"] == "closed" and r["exit_reason"] == "tp" and r["qty"] == pytest.approx(20.0)
    assert {"trade_id", "strategy", "symbol", "direction", "kind", "fill_price", "exit_price", "net_r", "pnl_usdt", "mfe_r", "mae_r"} <= set(r)
