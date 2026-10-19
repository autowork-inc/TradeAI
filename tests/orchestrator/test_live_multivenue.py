"""Мульти-биржевой боевой контур: LiveExecutionEngine + ExecutionRouter + LiveRiskManager на двух имитациях (Binance FakeRest, MT5 FakeMt5Rpc).

Проверяется: маршрут и `unsupported_symbol`; закрытый рынок Forex как обычный отказ (без Safe Mode); сквозной суточный лимит убытка
(Binance + MT5) с блокировкой ОБЕИХ площадок; запись намерения до ордера (R7); сопровождение и восстановление MT5-позиций."""

import asyncio
from datetime import datetime, timezone

import pytest

from exchange.binance_live import BinanceLiveClient
from exchange.router import ExecutionRouter
from execution.state import SafeMode
from orchestrator.journal import MemoryEventLog
from orchestrator.live_engine import LiveExecutionEngine, LiveExecutionError
from orchestrator.live_risk import utc_day_start_ms
from orchestrator.types import Direction, Kind
from tests.exchange.fake_mt5 import FULL, FakeMt5Rpc, make_client, make_mapper, trade_error
from tests.orchestrator.fake_live_rest import FakeRest
from tests.orchestrator.live_helpers import BAR, FILTERS, T0, manager, signal
from exchange.mt5_risk import Mt5RiskParams

NOW = T0 + 60_000
WHEN = datetime.fromtimestamp(NOW / 1000, tz=timezone.utc)  # сделки MT5 «сегодня» относительно часов движка
BINANCE_SYMBOLS = ("SOLUSDT", "XRPUSDT")  # фильтры тестовых монет; боевой набор - config.LIVE_BINANCE_SYMBOLS


def run(coro):
    return asyncio.run(coro)


class Env:
    def __init__(self, tmp_path, *, equity=100_000.0):
        self.rest = FakeRest(100.0)
        self.binance = BinanceLiveClient(self.rest)
        self.binance.set_filters(FILTERS)
        self.mt5, self.rpc = make_client(FakeMt5Rpc(equity=equity))
        self.mapper = run(make_mapper(self.mt5))
        self.router = ExecutionRouter(binance=self.binance, mt5=self.mt5, mapper=self.mapper, binance_symbols=BINANCE_SYMBOLS, now_ms=lambda: NOW, risk_params=Mt5RiskParams(risk_usdt=1000.0))
        self.journal = MemoryEventLog()
        self.safe = SafeMode(tmp_path / ".safe_mode")
        self.risk = manager(tmp_path, venues=self.router.venues)
        self.engine = LiveExecutionEngine(self.binance, self.risk, self.journal, self.safe, now_ms=lambda: NOW, interval_ms=BAR,
                                          post_only=False, router=self.router)

    def kinds(self):
        return [e.kind for e in self.journal.events]

    def last(self, kind):
        return self.journal.of_kind(kind)[-1]

    def rejected(self):
        return [e.detail["reason"] for e in self.journal.of_kind("live_rejected")]


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def eur(**kw):
    base = dict(symbol="EURUSD", direction=Direction.LONG, entry=1.1001, sl=1.0981, tp=1.1061)
    return signal(**{**base, **kw})


def sol(**kw):
    return signal(**{**dict(symbol="SOLUSDT", n=1), **kw})


# ---------------------------------------------------------------------------------------------- маршрут


@pytest.mark.parametrize("symbol", ["DOGEUSDT", "US500", "BRN", "BTCUSDT"])  # BTCUSDT: в этом тестовом роутере боевой набор Binance - SOL/XRP
def test_unsupported_symbol_is_rejected_before_any_request(env, symbol):
    run(env.engine.execute(signal(symbol=symbol), NOW))
    assert env.rejected() == ["unsupported_symbol"] and env.last("live_rejected").detail["symbol"] == symbol
    assert env.rest.requests == [] and env.rpc.trades == [] and not env.safe.active and env.risk.busy() == {}


def test_binance_symbol_still_goes_through_the_binance_engine(env):
    run(env.engine.execute(sol(), NOW))
    assert [p["type"] for p in env.rest.calls("POST", "/fapi/v1/order")] == ["MARKET"] and env.rpc.trades == []
    assert "live_intent" in env.kinds()


def test_mt5_symbol_goes_to_mt5_not_binance(env):
    run(env.engine.execute(eur(), NOW))
    assert env.rpc.trades and env.rpc.trades[0][:3] == ("market_buy", "EURUSD", 5.0) and env.rest.calls("POST", "/fapi/v1/order") == []
    assert env.risk.busy() == {"EURUSD": "SMC_Pro_Trend_EURUSD_long_1"}
    assert env.last("live_entry_filled").detail["venue"] == "mt5"


def test_intent_is_journaled_before_the_mt5_order_is_sent(env):
    seen = []
    env.rpc.on_trade = lambda name, args: seen.append(list(env.kinds()))
    run(env.engine.execute(eur(), NOW))
    assert seen and "live_intent" in seen[0]  # R7: на момент запроса ордера намерение уже в журнале
    intent = env.last("live_intent").detail
    assert intent["venue"] == "mt5" and intent["lots"] == 5.0 and intent["risk_amount"] == pytest.approx(1000.0) and intent["broker_symbol"] == "EURUSD"


def test_flush_runs_between_intent_and_order(tmp_path):
    e = Env(tmp_path)
    order = []

    async def flush():
        order.append("flush")

    e.engine._flush = flush
    e.rpc.on_trade = lambda name, args: order.append("order")
    run(e.engine.execute(eur(), NOW))
    assert order == ["flush", "order"]


# ---------------------------------------------------------------------------------------------- закрытый рынок


def test_closed_forex_market_is_a_plain_rejection_not_safe_mode(env):
    env.rpc.specs["EURUSD"]["tradeMode"] = "SYMBOL_TRADE_MODE_CLOSEONLY"
    run(env.engine.execute(eur(), NOW))
    assert env.rejected() == ["market_closed"] and not env.safe.active and env.rpc.trades == [] and env.risk.busy() == {}
    env.engine.check()  # движок жив, исключений нет
    env.rpc.specs["EURUSD"]["tradeMode"] = FULL
    run(env.engine.execute(eur(n=2), NOW))
    assert env.rpc.trades and "live_entry_filled" in env.kinds()


def test_broker_10018_on_send_is_market_closed_and_releases_the_symbol(env):
    env.rpc.effects = [trade_error(10018)]
    run(env.engine.execute(eur(), NOW))
    assert env.rejected() == ["market_closed"] and not env.safe.active and env.risk.busy() == {}
    assert env.last("live_intent").detail["venue"] == "mt5"  # намерение было записано до отправки
    run(env.engine.execute(eur(n=2), NOW))  # символ свободен: следующий сигнал проходит
    assert "live_entry_filled" in env.kinds()


def test_other_mt5_broker_refusal_is_a_rejection_and_unknown_outcome_crashes(env):
    env.rpc.effects = [trade_error(10019)]
    run(env.engine.execute(eur(), NOW))
    assert env.rejected() == ["broker_rejected"] and not env.safe.active and env.risk.busy() == {}
    env.rpc.effects = [trade_error(10012)]  # таймаут: ордер мог уйти - R1, исключение; резерв монеты снят в finally
    with pytest.raises(Exception, match="10012"):
        run(env.engine.execute(eur(n=3), NOW))
    assert env.risk.busy() == {}


def test_mt5_sizing_rejections_are_journaled_with_reason(tmp_path):
    tiny = Env(tmp_path, equity=100.0)
    tiny.router._params = Mt5RiskParams(risk_usdt=1.0)  # риск 1 USD -> 0.005 лота: меньше минимума
    run(tiny.engine.execute(eur(), NOW))
    assert tiny.rejected() == ["below_min_size"] and tiny.rpc.trades == [] and tiny.risk.busy() == {}


# ---------------------------------------------------------------------------------------------- сквозной суточный лимит


def test_total_daily_pnl_is_the_sum_of_both_accounts(env):
    env.rest.add_income("REALIZED_PNL", -30.0)
    env.rpc.add_deal(-20.0, when=WHEN)
    run(env.engine.refresh_pnl(NOW))
    assert env.risk.venue_pnl == {"binance": -30.0, "mt5": -20.0} and env.risk.daily_pnl == -50.0
    ev = env.last("live_daily_limit").detail
    assert ev["pnl_usdt"] == -50.0 and ev["pnl_by_venue"] == {"binance": -30.0, "mt5": -20.0}


def test_combined_loss_at_the_limit_blocks_new_entries_on_both_venues(env):
    env.rest.add_income("REALIZED_PNL", -30.0)
    env.rpc.add_deal(-20.0, when=WHEN)  # ни одна площадка в одиночку лимита не достигла, вместе - ровно 50
    run(env.engine.execute(eur(), NOW))
    run(env.engine.execute(sol(n=2), NOW))
    assert env.rejected() == ["trading_blocked", "trading_blocked"]
    assert env.rpc.trades == [] and env.rest.calls("POST", "/fapi/v1/order") == [] and not env.safe.active
    assert env.risk.blocked(NOW) and env.risk.blocked_until_ms == utc_day_start_ms(NOW) + 86_400_000


def test_loss_below_the_combined_limit_does_not_block(env):
    env.rest.add_income("REALIZED_PNL", -30.0)
    env.rpc.add_deal(-19.99, when=WHEN)
    run(env.engine.execute(eur(), NOW))
    assert env.rejected() == [] and env.rpc.trades


def test_a_profit_on_one_account_offsets_a_loss_on_the_other(env):
    env.rest.add_income("REALIZED_PNL", -60.0)  # Binance один дал бы блокировку
    env.rpc.add_deal(+25.0, when=WHEN)  # MT5 в плюсе: суммарно -35
    run(env.engine.execute(eur(), NOW))
    assert env.risk.daily_pnl == -35.0 and not env.risk.blocked(NOW) and env.rpc.trades


def test_mt5_loss_alone_blocks_binance_entries_too(env):
    env.rpc.add_deal(-55.0, when=WHEN)
    run(env.engine.execute(sol(), NOW))
    assert env.rejected() == ["trading_blocked"] and env.rest.calls("POST", "/fapi/v1/order") == []


def test_binance_loss_alone_blocks_mt5_entries_too(env):
    env.rest.add_income("REALIZED_PNL", -50.0)
    run(env.engine.execute(eur(), NOW))
    assert env.rejected() == ["trading_blocked"] and env.rpc.trades == []


def test_limit_block_cancels_pending_mt5_entries_but_not_open_positions(env):
    run(env.engine.execute(eur(), NOW))  # открытая позиция EURUSD
    lim = signal(symbol="XAUUSD", direction=Direction.LONG, kind=Kind.LIMIT, entry=1999.0, sl=1989.0, tp=2029.0)
    run(env.engine.execute(lim, NOW))
    assert len(env.rpc.orders) == 1 and len(env.rpc.positions) == 1
    env.rpc.add_deal(-80.0, when=WHEN)
    run(env.engine.refresh_pnl(NOW))
    assert env.rpc.orders == [] and len(env.rpc.positions) == 1  # лимитка снята, позиция со своим стопом осталась
    cancelled = env.last("live_trade_cancelled")
    assert cancelled.detail == {"venue": "mt5", "reason": "risk_blocked"} and list(env.risk.busy()) == ["EURUSD"]


def test_account_without_fresh_result_blocks_everything_fail_closed(tmp_path):
    e = Env(tmp_path)
    e.risk.update_venue_pnl("binance", 0.0, NOW)  # MT5 ни разу не сообщил
    gate = e.risk.gate(sol(), NOW)
    assert gate.reason == "pnl_unavailable" and gate.detail["stale_venues"] == ["mt5"]
    assert e.risk.daily_pnl is None  # суммы нет, пока нет всех слагаемых


# ---------------------------------------------------------------------------------------------- сопровождение и перезапуск


def test_poll_releases_the_symbol_when_the_broker_closes_the_position(env):
    run(env.engine.execute(eur(), NOW))
    pid = env.rpc.positions[0]["id"]
    run(env.engine.poll())
    assert env.risk.busy() and "live_closed" not in env.kinds()
    env.rpc.broker_closes(pid, profit=-10.0, when=WHEN)
    run(env.engine.poll())
    closed = env.last("live_closed")
    assert closed.detail["venue"] == "mt5" and env.risk.busy() == {}
    run(env.engine.execute(eur(n=2), NOW))  # символ снова доступен
    assert len(env.rpc.trades) == 2


def test_pending_mt5_limit_fills_and_is_reported(env):
    lim = eur(kind=Kind.LIMIT)
    run(env.engine.execute(lim, NOW))
    assert "live_entry_placed" in env.kinds()
    run(env.engine.poll())
    assert "live_entry_filled" not in env.kinds()
    env.rpc.fill_pending(env.rpc.orders[0]["id"])
    run(env.engine.poll())
    assert env.last("live_entry_filled").detail["venue"] == "mt5"


def test_recover_adopts_a_protected_mt5_position_and_reserves_the_symbol(env):
    env.rpc.positions.append({"id": "5", "type": "POSITION_TYPE_BUY", "symbol": "XAUUSD", "volume": 0.5, "openPrice": 2000.0, "stopLoss": 1990.0,
                              "takeProfit": 2030.0, "clientId": "tbE_x"})
    run(env.engine.recover(()))
    assert env.last("live_adopted").detail["venue"] == "mt5" and list(env.risk.busy()) == ["XAUUSD"]
    run(env.engine.execute(signal(symbol="XAUUSD", direction=Direction.LONG, entry=2000.3, sl=1990.3, tp=2030.3), NOW))
    assert env.rejected() == ["symbol_busy"] and env.rpc.trades == []


def test_recover_unprotected_mt5_position_trips_safe_mode_and_stops(env):
    env.rpc.positions.append({"id": "5", "type": "POSITION_TYPE_SELL", "symbol": "XAGUSD", "volume": 0.5, "openPrice": 25.0, "stopLoss": 0, "takeProfit": 0})
    with pytest.raises(LiveExecutionError, match="без стопа"):
        run(env.engine.recover(()))
    assert env.safe.active


def test_single_venue_engine_without_router_keeps_the_old_behaviour(tmp_path):
    rest = FakeRest(100.0)
    client = BinanceLiveClient(rest)
    client.set_filters(FILTERS)
    risk = manager(tmp_path)  # venues по умолчанию ("binance",)
    eng = LiveExecutionEngine(client, risk, MemoryEventLog(), SafeMode(tmp_path / ".sm"), now_ms=lambda: NOW, interval_ms=BAR, post_only=False)
    run(eng.execute(sol(), NOW))
    assert [p["type"] for p in rest.calls("POST", "/fapi/v1/order")] == ["MARKET"] and risk.venues == ("binance",)


def test_account_snapshot_sums_binance_and_mt5_for_the_api(tmp_path):
    """Движок пишет снимок счетов (Binance USDT + MT5) в файл: API отдаёт его клиенту для плашки «Баланс / Эквити»."""
    import json

    e = Env(tmp_path, equity=1000.0)
    path = tmp_path / ".live_account.json"
    e.engine._account_path = path
    run(e.engine.refresh_pnl(NOW))
    snap = json.loads(path.read_text(encoding="utf-8"))
    b = run(e.binance.get_balance())
    assert snap["updated_ms"] == NOW and snap["currency"] == "USDT"
    assert snap["venues"]["binance"] == {"balance": b.wallet, "equity": b.margin_balance} and snap["venues"]["mt5"]["equity"] == 1000.0
    assert snap["balance"] == pytest.approx(b.wallet + snap["venues"]["mt5"]["balance"]) and snap["equity"] == pytest.approx(b.margin_balance + 1000.0)


def test_account_snapshot_is_not_written_without_a_path(env, tmp_path):
    run(env.engine.refresh_pnl(NOW))
    assert not list(tmp_path.glob(".live_account.json"))
