"""Ретро-сверка закрытых боевых сделок с userTrades Binance (orchestrator/live_resync.py): unknown / gap_affected -> manual_binance, PnL и net R по филлам."""

import asyncio

import pytest

import config
from exchange.binance_live import UserTrade
from orchestrator.live_resync import resync_closed_trades
from persistence import schema
from persistence.recorder import DbOpenTrade, MemoryRecorder
from utils.errors import DataError

TAKER = config.LIVE_TAKER_FEE_RATE
T0 = 1_760_000_000_000
SYM = "BTCUSDT"


def trade_row(**kw):
    row = {"id": "sig1:live", "strategy_name": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "coin": SYM, "side": "long", "kind": "market", "mode": "live",
           "status": "closed", "qty": 1.0, "remaining_frac": 0.0, "signal_time": T0 - 1000, "entry_time": T0, "exit_time": T0 + 60_000, "entry_price": 100.0,
           "exit_price": 100.0, "sl": 98.0, "risk_amount": 2.0, "pnl": -0.1, "fees": 0.1, "r_multiple": -0.05, "gross_r": 0.0, "exit_reason": "unknown",
           "gap_affected": True, "plan": {"exit_price_sources": ["unknown"], "exit_price_source": "unknown", "fees_estimated": True}}
    row.update(kw)
    return row


def close_row(seq, qty, price, reason, pnl, fees=0.05):
    return {"trade_id": "sig1:live", "seq": seq, "ts": T0 + 30_000 * seq, "reason": reason, "qty": qty, "price": price, "pnl": pnl, "fees": fees, "gross_r": pnl / 2.0}


def ut(side, qty, price, t, fee=0.01, asset="USDT"):
    return UserTrade(SYM, side, qty, price, fee, asset, 0.0, t)


class FakeClient:
    def __init__(self, fills, exc=None):
        self.fills, self.calls, self.exc = fills, [], exc

    async def user_trades(self, symbol, start_ms, end_ms):
        self.calls.append((symbol, start_ms, end_ms))
        if self.exc:
            raise self.exc
        return [f for f in self.fills if start_ms <= f.time_ms <= end_ms]


def run(client, rec, cands, **kw):
    return asyncio.run(resync_closed_trades(client, rec, cands, now_ms=T0 + 10 * 86_400_000, **kw))


def test_manual_close_is_recomputed_from_exchange_fills_and_flags_cleared():
    rec = MemoryRecorder()
    cand = DbOpenTrade(trade_row(), (close_row(1, 1.0, 100.0, "unknown", -0.1),))
    rec.closes.append(dict(cand.closes[0]))
    client = FakeClient([ut("BUY", 1.0, 100.0, T0), ut("SELL", 0.4, 104.0, T0 + 20_000, 0.02), ut("SELL", 0.6, 105.0, T0 + 21_000, 0.03)])
    rep = run(client, rec, [cand])
    assert rep.fixed == ["sig1:live"] and not rep.skipped
    row = rec.trades["sig1:live"]
    exit_price = (0.4 * 104.0 + 0.6 * 105.0) / 1.0
    fees = TAKER * 100.0 + 0.05
    assert row["exit_reason"] == "manual_binance" and row["gap_affected"] is False and row["status"] == "closed"
    assert row["exit_price"] == pytest.approx(exit_price) and row["pnl"] == pytest.approx(exit_price - 100.0 - fees)
    assert row["fees"] == pytest.approx(fees) and row["r_multiple"] == pytest.approx((exit_price - 100.0 - fees) / 2.0)
    assert row["plan"]["exit_price_source"] == "exchange" and row["plan"]["resync"]["status"] == "done" and len(row["plan"]["fills"]) == 2
    (leg,) = rec.closes  # неточное закрытие заменено
    assert leg["seq"] == 2 and leg["reason"] == "manual_binance" and leg["price"] == pytest.approx(exit_price) and leg["net_r"] == pytest.approx(row["r_multiple"])
    assert client.calls[0][0] == SYM and client.calls[0][1] == T0


def test_short_trade_and_bnb_commission_uses_rate_estimate():
    rec = MemoryRecorder()
    cand = DbOpenTrade(trade_row(side="short"), ())
    rep = run(FakeClient([ut("BUY", 1.0, 96.0, T0 + 5_000, 0.001, "BNB")]), rec, [cand])
    assert rep.fixed
    row = rec.trades["sig1:live"]
    fees = TAKER * 100.0 + TAKER * 96.0
    assert row["pnl"] == pytest.approx(4.0 - fees) and row["exit_reason"] == "manual_binance" and not row["gap_affected"]
    assert not row["plan"]["exit_fee_from_exchange"]


def test_recorded_partial_exits_are_kept_and_skipped_in_fills():
    rec = MemoryRecorder()
    good = close_row(1, 0.5, 102.0, "tp1", 0.9)
    cand = DbOpenTrade(trade_row(plan={"exit_price_sources": ["exchange", "unknown"], "exit_price_source": "unknown"}, qty=1.0, pnl=0.9, r_multiple=0.4),
                       (good, close_row(2, 0.5, 100.0, "unknown", -0.05)))
    rec.closes += [dict(c) for c in cand.closes]
    fills = [ut("SELL", 0.5, 102.0, T0 + 10_000), ut("SELL", 0.5, 110.0, T0 + 40_000)]  # первое - закрытие TP1 бота, второе - ручное
    run(FakeClient(fills), rec, [cand])
    row = rec.trades["sig1:live"]
    assert [c["seq"] for c in rec.closes] == [1, 3] and rec.closes[1]["price"] == pytest.approx(110.0) and rec.closes[1]["qty"] == pytest.approx(0.5)
    assert row["exit_price"] == pytest.approx(106.0) and row["pnl"] == pytest.approx(0.9 + 0.5 * 10.0 - TAKER * 100.0 * 0.5 - 0.01 * 0.5 / 0.5)
    assert row["plan"]["exit_price_sources"] == ["exchange", "exchange"]


def test_incomplete_fills_leave_trade_untouched_but_mark_attempt():
    rec = MemoryRecorder()
    cand = DbOpenTrade(trade_row(), (close_row(1, 1.0, 100.0, "unknown", -0.1),))
    rep = run(FakeClient([ut("SELL", 0.3, 104.0, T0 + 20_000)]), rec, [cand])
    assert not rep.fixed and rep.skipped[0][1].startswith("fills_incomplete")
    row = rec.trades["sig1:live"]
    assert row["exit_reason"] == "unknown" and row["gap_affected"] is True and row["plan"]["resync"]["status"] == "incomplete"
    assert not rec.closes


def test_trade_with_all_known_exits_is_skipped_without_request():
    rec = MemoryRecorder()
    cand = DbOpenTrade(trade_row(exit_reason="sl", plan={"exit_price_sources": ["exchange"]}), (close_row(1, 1.0, 98.0, "sl", -2.1),))
    client = FakeClient([])
    rep = run(client, rec, [cand])
    assert rep.skipped == [("sig1:live", "no_unknown_remainder")] and client.calls == [] and not rec.trades


def test_exchange_data_error_skips_one_trade_and_others_continue():
    rec = MemoryRecorder()
    a = DbOpenTrade(trade_row(), ())
    b = DbOpenTrade(trade_row(id="sig2:live", coin="ETHUSDT"), ())

    class Mixed(FakeClient):
        async def user_trades(self, symbol, s, e):
            if symbol == SYM:
                raise DataError("в окне не меньше 1000 исполнений")
            return [UserTrade("ETHUSDT", "SELL", 1.0, 103.0, 0.01, "USDT", 0.0, T0 + 5_000)]

    rep = run(Mixed([]), rec, [a, b])
    assert rep.fixed == ["sig2:live"] and rep.skipped[0][0] == "sig1:live" and rec.trades["sig2:live"]["exit_reason"] == "manual_binance"


def test_non_live_or_open_rows_are_rejected():
    for bad in (trade_row(mode="shadow"), trade_row(status="open"), trade_row(id="sig1")):
        with pytest.raises(DataError):
            run(FakeClient([]), MemoryRecorder(), [DbOpenTrade(bad, ())])


def test_candidate_query_targets_only_live_closed_unknown_or_gap_without_prior_attempt():
    sql = schema.SELECT_RESYNC
    assert "mode = 'live'" in sql and "status = 'closed'" in sql and "exit_reason = 'unknown' OR gap_affected" in sql and "plan->'resync' IS NULL" in sql
