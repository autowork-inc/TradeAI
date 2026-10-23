"""ExecutionRouter: маршрут по символу, MT5-вход (лоты, имя брокера, стоп в запросе), закрытый рынок как обычный отказ, сопровождение и сверка.

Клиент MT5 - настоящий `CTraderConnector` поверх имитации RPC с состоянием (`fake_mt5.py`); сети нет."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import config
from exchange.binance_live import BinanceLiveClient
from exchange.ctrader import Quote, MarketClosedError, CTraderTradeError
from exchange.router import ExecutionRouter, Mt5UnprotectedPosition, RouteRejected, Venue, build_router
from orchestrator.types import Direction, Kind
from tests.exchange.fake_mt5 import FULL, FakeMt5Rpc, make_client, make_mapper, trade_error
from orchestrator.registry import SCALP_1R_PLAN
from tests.orchestrator.live_helpers import STRATEGY, T0, signal
from utils.errors import ConfigError
from exchange.mt5_risk import Mt5RiskParams

NOW = T0


def run(coro):
    return asyncio.run(coro)


def make_router(rpc=None, *, with_binance=False, **kw):
    client, rpc = make_client(rpc)
    mapper = run(make_mapper(client))
    binance = BinanceLiveClient(object()) if with_binance else None  # для маршрута сеть не нужна
    return ExecutionRouter(binance=binance, mt5=client, mapper=mapper, now_ms=lambda: NOW, **{"risk_params": Mt5RiskParams(risk_usdt=1000.0), **kw}), client, rpc


def eur_long(**kw):
    base = dict(symbol="EURUSD", direction=Direction.LONG, entry=1.1001, sl=1.0981, tp=1.1061)
    return signal(**{**base, **kw})


def eur_short(**kw):
    base = dict(symbol="EURUSD", direction=Direction.SHORT, entry=1.1000, sl=1.1020, tp=1.0940)
    return signal(**{**base, **kw})


# ---------------------------------------------------------------------------------------------- маршрут


@pytest.mark.parametrize("symbol,venue", [("BTCUSDT", Venue.BINANCE), ("ETHUSDT", Venue.BINANCE), ("EURUSD", Venue.MT5), ("XAUUSD", Venue.MT5), ("XAGUSD", Venue.MT5)])
def test_symbols_go_to_their_venue(symbol, venue):
    router, *_ = make_router(with_binance=True)
    assert router.venue_of(symbol) is venue and router.reject_unsupported(symbol) is None
    assert router.venues == ("binance", "mt5")


@pytest.mark.parametrize("symbol", ["DOGEUSDT", "US500", "BRN", "AUDUSD", "btcusdt", ""])
def test_other_symbols_are_unsupported(symbol):
    router, *_ = make_router(with_binance=True)
    rej = router.reject_unsupported(symbol)
    assert router.venue_of(symbol) is None and isinstance(rej, RouteRejected) and rej.reason == "unsupported_symbol"
    assert rej.detail["mt5"] == list(config.LIVE_MT5_SYMBOLS) and rej.detail["binance"] == list(config.LIVE_BINANCE_SYMBOLS)


def test_disconnected_venue_makes_its_symbols_unsupported():
    router, *_ = make_router(with_binance=False)  # только MT5
    assert router.venue_of("BTCUSDT") is None and router.reject_unsupported("BTCUSDT").detail["why"] == "площадка не подключена"
    assert router.venues == ("mt5",)
    only_binance = ExecutionRouter(binance=BinanceLiveClient(object()))
    assert only_binance.venue_of("EURUSD") is None and only_binance.reject_unsupported("EURUSD").reason == "unsupported_symbol"


def test_router_configuration_is_validated():
    client, _ = make_client()
    mapper = run(make_mapper(client))
    with pytest.raises(ConfigError, match="двух площадках"):
        ExecutionRouter(binance=BinanceLiveClient(object()), mt5=client, mapper=mapper, binance_symbols=["EURUSD"], mt5_symbols=["EURUSD"])
    with pytest.raises(ConfigError, match="хотя бы один"):
        ExecutionRouter()
    with pytest.raises(ConfigError, match="SymbolMapper"):
        ExecutionRouter(mt5=client)
    with pytest.raises(ConfigError, match="max_entry_deviation"):
        ExecutionRouter(mt5=client, mapper=mapper, max_entry_deviation=2)


# ---------------------------------------------------------------------------------------------- MT5-вход


def test_market_long_converts_symbol_sizes_lots_and_sends_stop_in_the_same_request():
    router, client, rpc = make_router()
    t = run(router.open_mt5(eur_long(), NOW))
    assert not isinstance(t, RouteRejected)
    # SL 20 пипсов = 0.0020, потеря на лот 200 USD, риск 1% от 100 000 = 1000 -> 5.00 лота
    (name, symbol, volume, sl, tp, options) = rpc.trades[-1]
    assert (name, symbol, volume, sl, tp) == ("market_buy", "EURUSD", 5.0, 1.0981, 1.1081)  # брокерский TP - единственный тейк, 4R от входа
    assert options["clientId"] == t.client_id
    assert t.plan.risk_amount == pytest.approx(1000.0) and t.status == "open" and t.position_id and router.tracked_mt5["EURUSD"] is t


def test_short_uses_bid_and_gold_lots_respect_contract_size():
    router, client, rpc = make_router()
    t = run(router.open_mt5(eur_short(), NOW))
    assert rpc.trades[-1][:3] == ("market_sell", "EURUSD", 5.0) and rpc.positions[0]["openPrice"] == 1.10000
    # XAUUSD: ask 2000.30, SL 10 USD ниже -> 100 унций * 10 = 1000 USD на лот -> 1.00 лот
    gold = signal(symbol="XAUUSD", direction=Direction.LONG, entry=2000.30, sl=1990.30, tp=2030.30)
    t = run(router.open_mt5(gold, NOW))
    assert rpc.trades[-1][:3] == ("market_buy", "XAUUSD", 1.0) and t.plan.loss_per_lot == pytest.approx(1000.0)


def test_limit_entry_is_snapped_to_tick_grid_and_expires():
    router, client, rpc = make_router(limit_ttl_ms=600_000)
    t = run(router.open_mt5(eur_long(kind=Kind.LIMIT, entry=1.099954, sl=1.097954, tp=1.105954), NOW))
    name, symbol, volume, price, sl, tp, options = rpc.trades[-1]
    assert (name, price) == ("limit_buy", 1.09995) and t.status == "pending" and t.order_id
    assert options["expiration"]["time"] == datetime.fromtimestamp((NOW + 600_000) / 1000, tz=timezone.utc)
    assert rpc.orders[0]["clientId"] == t.client_id


def test_prepare_does_not_send_anything_and_send_is_a_separate_step():
    router, client, rpc = make_router()
    prepared = run(router.prepare_mt5(eur_long(), NOW))
    assert not isinstance(prepared, RouteRejected) and rpc.trades == [] and router.tracked_mt5 == {}
    run(router.send_mt5(prepared))
    assert len(rpc.trades) == 1


def test_fixed_five_usd_risk_trades_small_lots_and_below_the_minimum_lot_is_rejected():
    rpc = FakeMt5Rpc(equity=200.0, free_margin=200.0)  # риск 5 USD, на лот 200 USD -> 0.025 -> вниз до 0.02 лота (риск 4 USD)
    router, client, _ = make_router(rpc, risk_params=Mt5RiskParams(risk_usdt=5.0))
    t = run(router.open_mt5(eur_long(), NOW))
    assert not isinstance(t, RouteRejected) and t.plan.lots == 0.02 and t.plan.risk_amount <= 5.0 + 1e-9
    rpc2 = FakeMt5Rpc(equity=100.0, free_margin=100.0)  # риск 1 USD -> 0.005 лота: меньше минимума, вверх не округляем
    router2, *_ = make_router(rpc2, risk_params=Mt5RiskParams(risk_usdt=1.0))
    rej = run(router2.open_mt5(eur_long(), NOW))
    assert rej.reason == "below_min_size" and rpc2.trades == []


def test_margin_overrun_is_exposure():
    rpc = FakeMt5Rpc(free_margin=1_000.0)  # 5 лотов требуют ~5500 USD
    router, *_ = make_router(rpc)
    rej = run(router.open_mt5(eur_long(), NOW))
    assert isinstance(rej, RouteRejected) and rej.reason == "exposure" and rej.detail["why"] == "margin" and rpc.trades == []


def test_bad_stop_and_price_deviation_and_missing_tick_value():
    router, client, rpc = make_router()
    rej = run(router.open_mt5(eur_long(entry=1.2000, sl=1.1980, tp=1.2060), NOW))  # цена сигнала на 9% от рынка брокера
    assert rej.reason == "price_deviation" and rej.detail["max"] == config.LIVE_MAX_ENTRY_DEVIATION
    rpc2 = FakeMt5Rpc()
    router2, client2, _ = make_router(rpc2)

    async def no_tick(symbol):
        return Quote(symbol, 1.1, 1.1001, None)  # котировка есть, курса конвертации валюты (стоимости тика) нет

    rpc2.get_price = no_tick
    assert run(router2.open_mt5(eur_long(), NOW)).reason == "tick_value_unavailable" and rpc2.trades == []


def test_unknown_symbol_for_the_mapper_is_a_config_error_not_a_rejection():
    router, *_ = make_router()
    with pytest.raises(ConfigError, match="не загружен"):
        run(router.prepare_mt5(signal(symbol="USDJPY", entry=150.0, sl=149.7, tp=151.0), NOW))


# ---------------------------------------------------------------------------------------------- закрытый рынок


@pytest.mark.parametrize("mode", ["SYMBOL_TRADE_MODE_CLOSEONLY", "SYMBOL_TRADE_MODE_DISABLED", "SYMBOL_TRADE_MODE_LONGONLY", "SYMBOL_TRADE_MODE_SHORTONLY"])
def test_trade_mode_not_full_is_market_closed_without_any_order(mode):
    rpc = FakeMt5Rpc()
    router, client, _ = make_router(rpc)
    rpc.specs["EURUSD"]["tradeMode"] = mode  # режим изменился уже ПОСЛЕ загрузки спецификаций (сессия закрылась)
    rej = run(router.open_mt5(eur_long(), NOW))
    assert isinstance(rej, RouteRejected) and rej.reason == "market_closed" and rej.detail["trade_mode"] == mode
    assert rpc.trades == [] and router.tracked_mt5 == {}
    rpc.specs["EURUSD"]["tradeMode"] = FULL  # рынок открылся: тот же сигнал проходит, перезапуск не нужен
    assert not isinstance(run(router.open_mt5(eur_long(), NOW)), RouteRejected)


def test_disabled_symbol_loads_with_allow_disabled_so_startup_outside_session_does_not_crash():
    rpc = FakeMt5Rpc()
    rpc.specs["EURUSD"]["tradeMode"] = "SYMBOL_TRADE_MODE_DISABLED"
    client, _ = make_client(rpc)
    mapper = run(make_mapper(client))  # allow_disabled=True: не ConfigError
    router = ExecutionRouter(mt5=client, mapper=mapper, risk_params=Mt5RiskParams(risk_usdt=1000.0))
    assert run(router.open_mt5(eur_long(), NOW)).reason == "market_closed"


def test_broker_answer_10018_is_market_closed_not_a_crash():
    router, client, rpc = make_router()
    rpc.effects = [trade_error(10018)]
    rej = run(router.open_mt5(eur_long(), NOW))
    assert isinstance(rej, RouteRejected) and rej.reason == "market_closed" and rej.detail["code"] == "MARKET_CLOSED"
    assert router.tracked_mt5 == {} and rpc.positions == []
    rpc.effects = [trade_error(10018)]  # то же для лимитки
    assert run(router.open_mt5(eur_long(kind=Kind.LIMIT), NOW)).reason == "market_closed"


# ---------------------------------------------------------------------------------------------- прочие отказы брокера


def test_other_broker_refusals_are_rejections_and_unknown_outcome_is_raised():
    router, client, rpc = make_router()
    rpc.effects = [trade_error(10019)]  # нет денег
    rej = run(router.open_mt5(eur_long(), NOW))
    assert rej.reason == "broker_rejected" and rej.detail["code"] == "NOT_ENOUGH_MONEY" and rej.detail["retryable"] is False
    rpc.effects = [trade_error(10016)]
    assert run(router.open_mt5(eur_long(), NOW)).detail["name"] == "TRADING_BAD_STOPS"
    rpc.effects = [trade_error(10012)]  # таймаут: ордер мог дойти до брокера
    with pytest.raises(CTraderTradeError) as ei:
        run(router.open_mt5(eur_long(), NOW))
    assert ei.value.outcome_unknown and router.tracked_mt5 == {}


# ---------------------------------------------------------------------------------------------- сопровождение и сверка


def test_poll_detects_broker_side_close_and_limit_fill():
    router, client, rpc = make_router()
    t = run(router.open_mt5(eur_long(), NOW))
    assert [e.kind for e in run(router.poll_mt5())] == ["protected"]  # позиция на месте, сопровождение началось
    assert run(router.poll_mt5()) == []
    rpc.broker_closes(t.position_id, profit=-1000.0)
    (ev,) = run(router.poll_mt5())
    assert ev.kind == "closed" and ev.trade is t and router.tracked_mt5 == {}
    lim = run(router.open_mt5(eur_long(kind=Kind.LIMIT, n=2), NOW))
    assert run(router.poll_mt5()) == [] and lim.status == "pending"
    rpc.fill_pending(lim.order_id)
    (ev, prot) = run(router.poll_mt5())
    assert ev.kind == "opened" and prot.kind == "protected" and lim.status == "open" and lim.position_id
    lim2 = run(router.open_mt5(eur_long(kind=Kind.LIMIT, n=3, symbol="XAUUSD", entry=1999.0, sl=1989.0, tp=2029.0), NOW))
    rpc.orders.clear()  # брокер снял ордер (срок вышел)
    kinds = [e.kind for e in run(router.poll_mt5())]
    assert kinds == ["cancelled"] and "XAUUSD" not in router.tracked_mt5


def test_cancel_pending_leaves_open_positions_alone():
    router, client, rpc = make_router()
    pos = run(router.open_mt5(eur_long(), NOW))
    lim = run(router.open_mt5(signal(symbol="XAUUSD", direction=Direction.LONG, kind=Kind.LIMIT, entry=1999.0, sl=1989.0, tp=2029.0), NOW))
    cancelled = run(router.cancel_pending_mt5())
    assert [c.symbol for c in cancelled] == ["XAUUSD"] and rpc.orders == [] and list(router.tracked_mt5) == ["EURUSD"] and pos.status == "open"


def test_recover_adopts_protected_positions_and_our_pending_orders_only():
    router, client, rpc = make_router()
    rpc.positions += [
        {"id": "1", "type": "POSITION_TYPE_BUY", "symbol": "EURUSD", "volume": 0.5, "openPrice": 1.1, "stopLoss": 1.09, "takeProfit": 1.12, "clientId": "x"},
        {"id": "2", "type": "POSITION_TYPE_BUY", "symbol": "GBPUSD", "volume": 0.5, "openPrice": 1.3, "stopLoss": 0, "takeProfit": 0},  # чужой символ
    ]
    rpc.orders += [
        {"id": "7", "type": "ORDER_TYPE_SELL_LIMIT", "symbol": "XAUUSD", "volume": 0.2, "openPrice": 2010.0, "stopLoss": 2020.0, "clientId": config.CLIENT_ORDER_ID_PREFIX + "E_abc"},
        {"id": "8", "type": "ORDER_TYPE_BUY_LIMIT", "symbol": "XAGUSD", "volume": 0.2, "openPrice": 24.0, "stopLoss": 23.0, "clientId": "manual"},  # не наш
    ]
    found = run(router.recover_mt5())
    assert sorted((t.symbol, t.status, t.adopted) for t in found) == [("EURUSD", "open", True), ("XAUUSD", "pending", True)]


def test_recover_position_without_stop_is_an_emergency():
    router, client, rpc = make_router()
    rpc.positions.append({"id": "1", "type": "POSITION_TYPE_SELL", "symbol": "XAUUSD", "volume": 0.5, "openPrice": 2000.0, "stopLoss": 0, "takeProfit": 0})
    with pytest.raises(Mt5UnprotectedPosition, match="без стопа"):
        run(router.recover_mt5())


# ---------------------------------------------------------------------------------------------- суточный результат и валюта счёта


def test_router_pnl_per_venue_and_account_currency_check():
    rpc = FakeMt5Rpc(currency="EUR")
    router, client, _ = make_router(rpc)
    with pytest.raises(ConfigError, match="EUR"):
        run(router.check_mt5_account())
    rpc.currency = "USD"
    run(router.check_mt5_account())
    rpc.add_deal(-12.5)
    pnl = run(router.realized_pnl(int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000)))
    assert pnl == {"mt5": pytest.approx(-12.5)}


def test_build_router_without_mt5_is_binance_only():
    router = run(build_router(BinanceLiveClient(object()), with_mt5=False))
    assert router.venues == ("binance",) and router.mt5 is None and router.venue_of("EURUSD") is None


def test_live_binance_pool_starts_with_pinned_btc_eth_and_mt5_stays_separate():
    assert config.LIVE_BINANCE_SYMBOLS == ("BTCUSDT", "ETHUSDT")  # остальное добавляет автоселектор по объёму >= 400M (на месте, в тот же объект)
    assert config.LIVE_MT5_SYMBOLS == ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "XAGUSD") and not config.LIVE_ENABLE_MT5
    assert config.TRADE_MODE == "shadow"  # по умолчанию ничего не отправляется
    assert set(config.LIVE_BINANCE_SYMBOLS).isdisjoint(config.LIVE_MT5_SYMBOLS)
    config.validate()
