"""Ручное закрытие позиции на Binance (2026-10-18): объём, исчезнувший вне бота, берётся из `userTrades` - средневзвешенная цена, фактическая
комиссия, причина `manual_binance`, PnL/R по фактической цене, `gap_affected` снят. Нет исполнений у биржи - прежнее поведение (`unknown`)."""

import pytest

import config
from orchestrator.live_engine import MANUAL_EXIT, live_trade_id
from tests.orchestrator.live_helpers import signal
from tests.orchestrator.test_live_engine import NOW, SYM, Env, run
from tests.orchestrator.test_live_recording import make_env

TAKER = config.LIVE_TAKER_FEE_RATE
RATE = 0.0005  # комиссия имитации биржи (FakeRest.commission_rate)


def opened(tmp_path):
    sig = signal(entry=100.0, sl=98.0)  # объём 1.0, риск 2.0 USDT на 1 SOL
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    return env, rec, live_trade_id(sig.signal_id)


def manual(env, qty, price, at, side="SELL"):
    env.rest.now_ms = at
    env.rest._move(SYM, side, qty, price, "manual")


def test_full_manual_close_uses_real_fill_price_and_commission(tmp_path):
    env, rec, tid = opened(tmp_path)
    env.clock.t = NOW + 10_000
    manual(env, 1.0, 101.0, NOW + 5_000)
    run(env.engine.poll())
    row = rec.trades[tid]
    fees = TAKER * 100.0 + RATE * 101.0  # вход - по ставке, выход - фактическая комиссия userTrades
    assert row["status"] == "closed" and row["exit_reason"] == MANUAL_EXIT == "manual_binance"
    assert row["exit_price"] == pytest.approx(101.0) and row["gap_affected"] is False and row["plan"]["exit_price_source"] == "exchange"
    assert row["fees"] == pytest.approx(fees) and row["pnl"] == pytest.approx(1.0 - fees) and row["r_multiple"] == pytest.approx((1.0 - fees) / 2.0)
    (leg,) = rec.closes
    assert leg["reason"] == "manual_binance" and leg["ts"] == NOW + 5_000 and leg["price"] == pytest.approx(101.0) and leg["fees"] == pytest.approx(fees)
    assert env.event("live_manual_close").detail["price"] == pytest.approx(101.0) and env.risk.busy() == {}


def test_manual_close_in_two_fills_is_volume_weighted_and_short_side_is_mirrored(tmp_path):
    env, rec, tid = opened(tmp_path)
    env.clock.t = NOW + 10_000
    manual(env, 0.25, 100.0, NOW + 3_000)
    manual(env, 0.75, 102.0, NOW + 4_000)
    run(env.engine.poll())
    row = rec.trades[tid]
    vwap = (0.25 * 100.0 + 0.75 * 102.0) / 1.0
    assert row["exit_price"] == pytest.approx(vwap) and row["pnl"] == pytest.approx((vwap - 100.0) - TAKER * 100.0 - RATE * vwap)


def test_partial_manual_close_recomputes_pnl_and_r_then_the_stop_closes_the_rest(tmp_path):
    env, rec, tid = opened(tmp_path)
    env.clock.t = NOW + 10_000
    manual(env, 0.3, 101.0, NOW + 4_000)
    manual(env, 0.3, 100.0, NOW + 5_000)
    run(env.engine.poll())
    mid = rec.trades[tid]
    assert mid["status"] == "open" and mid["remaining_frac"] == pytest.approx(0.4) and mid["gap_affected"] is False
    part = (0.3 * 101.0 + 0.3 * 100.0) / 0.6  # 100.5
    fee_part = TAKER * 100.0 * 0.6 + RATE * 0.6 * part
    (leg,) = rec.closes
    assert leg["reason"] == "manual_binance" and leg["qty"] == pytest.approx(0.6) and leg["price"] == pytest.approx(part)
    assert leg["pnl"] == pytest.approx(0.6 * (part - 100.0) - fee_part) and leg["net_r"] == pytest.approx(leg["pnl"] / 2.0)
    assert mid["pnl"] == pytest.approx(leg["pnl"]) and mid["stop_price"] == pytest.approx(98.0)
    env.rest.set_price(98.0)  # оставшиеся 0.4 закрывает стоп бота (его объём подогнан под остаток)
    run(env.engine.poll())
    row = rec.trades[tid]
    stop_fee = TAKER * 100.0 * 0.4 + TAKER * 98.0 * 0.4
    assert [c["reason"] for c in rec.closes] == ["manual_binance", "sl"] and row["exit_reason"] == "sl" and row["status"] == "closed"
    assert row["pnl"] == pytest.approx(leg["pnl"] + 0.4 * (98.0 - 100.0) - stop_fee) and row["r_multiple"] == pytest.approx(row["pnl"] / 2.0)
    assert row["gap_affected"] is False and sum(c["frac"] for c in rec.closes) == pytest.approx(1.0)


def test_non_usdt_commission_falls_back_to_the_fee_rate(tmp_path):
    env, rec, tid = opened(tmp_path)
    env.rest.commission_asset = "BNB"
    env.clock.t = NOW + 10_000
    manual(env, 1.0, 101.0, NOW + 5_000)
    run(env.engine.poll())
    row = rec.trades[tid]
    assert row["fees"] == pytest.approx(TAKER * 100.0 + TAKER * 101.0) and row["exit_reason"] == "manual_binance"


def test_no_fills_from_exchange_keeps_unknown_and_stays_out_of_statistics(tmp_path):
    env, rec, tid = opened(tmp_path)
    env.clock.t = NOW + 10_000
    env.rest.positions[SYM] = 0.0  # позиция исчезла, а исполнений биржа не отдала
    run(env.engine.poll())
    row = rec.trades[tid]
    assert row["exit_reason"] == "unknown" and row["gap_affected"] is True and row["plan"]["exit_price_source"] in ("unknown", "last_tick")


def test_closed_trade_stores_real_exchange_fills_for_the_chart(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig)
    env.rest.now_ms = NOW + 100  # вход исполнен сразу после отправки ордера
    run(env.engine.execute(sig, NOW))
    env.clock.t = NOW + 10_000
    manual(env, 1.0, 101.0, NOW + 5_000)
    run(env.engine.poll())
    fills = rec.trades[live_trade_id(sig.signal_id)]["plan"]["fills"]
    assert [(f["role"], f["side"], f["qty"], f["price"], f["ts"]) for f in fills] == [("entry", "BUY", 1.0, 100.0, NOW + 100), ("exit", "SELL", 1.0, 101.0, NOW + 5_000)]
    assert fills[1]["fee"] == pytest.approx(RATE * 101.0) and fills[1]["fee_asset"] == "USDT"
