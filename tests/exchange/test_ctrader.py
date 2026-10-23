"""CTraderConnector целиком против имитации сервера cTrader (tests/exchange/fake_ctrader_server.py): настоящий TCP + Protobuf, без сети и ключей.

Покрыто: авторизация по Client ID/Secret/Account ID/Access Token (+ автообновление токена по Refresh Token), свечи, котировки и стоимость тика, MARKET с жёсткими SL/TP
(относительные уровни -> абсолютные), лимитки, изменение/закрытие/отмена, Position Recovery, PnL по сделкам, обрыв связи и таймауты.
"""

import asyncio
import json
from datetime import datetime, timezone

import pytest

from exchange import ctrader as C
from exchange import ctrader_proto as P
from exchange.ctrader import CTraderConnector, CTraderError, CTraderTradeError, MarketClosedError
from exchange.symbol_mapper import SymbolMapper
from tests.exchange.fake_ctrader_server import CTID, FakeCTraderServer
from utils.errors import ConfigError

ENV = {"CTRADER_CLIENT_ID": "cid", "CTRADER_CLIENT_SECRET": "sec", "CTRADER_ACCOUNT_ID": str(CTID), "CTRADER_ACCESS_TOKEN": "tok-1", "CTRADER_REFRESH_TOKEN": "ref-1"}
M5 = 300_000


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(C, "HISTORY_PAUSE_S", 0.0)


def scenario(body, *, server_kw=None, env=None, tmp_path=None, **client_kw):
    """Поднимает сервер и клиент, выполняет body(srv, client), всё закрывает."""
    async def main():
        srv = await FakeCTraderServer(**(server_kw or {})).start()
        client = CTraderConnector(env or ENV, token_store=(tmp_path / "tokens.json") if tmp_path else None, host="127.0.0.1", port=srv.port, use_tls=False, **client_kw)
        try:
            return await body(srv, client)
        finally:
            await client.close()
            await srv.stop()
    return asyncio.run(main())


# ---------------------------------------------------------------------------------------------- ключи и авторизация


def test_credentials_validated_before_any_network_and_secrets_never_in_repr():
    for missing in ENV:
        if missing == "CTRADER_REFRESH_TOKEN":
            continue
        env = {k: v for k, v in ENV.items() if k != missing}
        with pytest.raises(ConfigError, match=missing):
            CTraderConnector(env)
    with pytest.raises(ConfigError, match="числом"):
        CTraderConnector({**ENV, "CTRADER_ACCOUNT_ID": "abc"})
    with pytest.raises(ConfigError, match="посторонние"):
        CTraderConnector({**ENV, "CTRADER_ACCESS_TOKEN": "to ken"})
    with pytest.raises(ConfigError, match="auto|demo|live"):
        CTraderConnector({**ENV, "CTRADER_ENV": "paper"})
    text = repr(CTraderConnector(ENV))
    assert "sec" not in text and "tok-1" not in text and "ref-1" not in text and str(CTID) in text


def test_connect_runs_the_documented_auth_flow_and_loads_reference_data():
    async def body(srv, c):
        await c.connect()
        names = [n for n, _ in srv.requests]
        assert names[:3] == ["ApplicationAuthReq", "GetAccountListByAccessTokenReq", "AccountAuthReq"]
        assert names[3:7] == ["AssetListReq", "SymbolsListReq", "TraderReq", "ReconcileReq"]
        assert srv.seen("ApplicationAuthReq")[0] == {"clientId": "cid", "clientSecret": "sec"}
        assert srv.seen("AccountAuthReq")[0] == {"ctidTraderAccountId": CTID, "accessToken": "tok-1"}
        assert c.is_live is False and "EURUSD" in await c.get_symbols()
        await c.connect()  # повтор на подключённом клиенте - ничего не делает
        assert len(srv.seen("ApplicationAuthReq")) == 1
    scenario(body)


def test_wrong_secret_or_foreign_account_is_a_config_error():
    async def wrong_secret(srv, c):
        with pytest.raises(ConfigError, match="CH_CLIENT_AUTH_FAILURE"):
            await c.connect()
    scenario(wrong_secret, server_kw={"client_secret": "other"})

    async def foreign(srv, c):
        with pytest.raises(ConfigError, match="нет среди счетов токена"):
            await c.connect()
    scenario(foreign, server_kw={"ctid": 111})


def test_expired_access_token_is_refreshed_saved_and_reused(tmp_path):
    async def body(srv, c):
        await c.connect()
        assert srv.seen("RefreshTokenReq") == [{"refreshToken": "ref-1"}]
        saved = json.loads((tmp_path / "tokens.json").read_text(encoding="utf-8"))
        assert saved["access_token"] == srv.valid_token and saved["refresh_token"] == srv.valid_refresh and saved["account_id"] == CTID
        await c.close()
        c2 = CTraderConnector(ENV, token_store=tmp_path / "tokens.json", host="127.0.0.1", port=srv.port, use_tls=False)  # новый процесс: в окружении всё ещё старый токен
        before = len(srv.seen("RefreshTokenReq"))
        await c2.connect()
        assert len(srv.seen("RefreshTokenReq")) == before  # взял сохранённую пару, обновлять не пришлось
        await c2.close()
    scenario(body, server_kw={"access_token": "tok-fresh"}, tmp_path=tmp_path)


def test_expired_token_without_refresh_token_is_an_error():
    async def body(srv, c):
        with pytest.raises(CTraderError, match="CH_ACCESS_TOKEN_INVALID"):
            await c.connect()
    scenario(body, server_kw={"access_token": "tok-fresh"}, env={k: v for k, v in ENV.items() if k != "CTRADER_REFRESH_TOKEN"})


def test_host_selection_demo_live_auto():
    assert CTraderConnector({**ENV, "CTRADER_ENV": "demo"})._hosts_to_try() == ["demo.ctraderapi.com"]
    assert CTraderConnector({**ENV, "CTRADER_ENV": "live"})._hosts_to_try() == ["live.ctraderapi.com"]
    assert CTraderConnector({**ENV, "CTRADER_ENV": "auto"})._hosts_to_try() == ["demo.ctraderapi.com", "live.ctraderapi.com"]


# ---------------------------------------------------------------------------------------------- символы, свечи, котировки


def test_specification_has_the_shape_symbol_mapper_expects_and_class_check_passes():
    async def body(srv, c):
        await c.connect()
        eur = await c.get_symbol_specification("EURUSD")
        assert (eur["contractSize"], eur["minVolume"], eur["volumeStep"], eur["tickSize"], eur["digits"]) == (100_000, 0.01, 0.01, 1e-5, 5)
        assert eur["path"] == "Forex\\EURUSD" and eur["tradeMode"] == C.TRADE_MODE_FULL and eur["profitCurrency"] == "USD"
        gold = await c.get_symbol_specification("XAUUSD")
        assert gold["path"] == "Metals\\XAUUSD" and gold["contractSize"] == 100 and gold["tickSize"] == 0.01
        mapper = SymbolMapper(c, overrides={})
        await mapper.load(["EURUSD", "XAUUSD"])
        assert mapper.spec("EURUSD").contract_size == 100_000 and mapper.broker_name("XAUUSD") == "XAUUSD"
        with pytest.raises(ConfigError, match="нет у брокера"):
            await c.get_symbol_specification("NOPE")
    scenario(body)


def bars(n, start=1_700_000_000_000 // M5 * M5):
    return [(start + i * M5, 1.1 + i * 1e-4, 1.1 + i * 1e-4 + 5e-5, 1.1 + i * 1e-4 - 5e-5, 1.1 + i * 1e-4 + 2e-5, 100 + i) for i in range(n)]


def test_candles_are_converted_sorted_limited_and_requests_respect_the_window():
    async def body(srv, c):
        await c.connect()
        srv.bars[1] = bars(300)
        last_open = srv.bars[1][-1][0]
        got = await c.get_candles("EURUSD", "5m", 50, start_time=datetime.fromtimestamp((last_open + M5) / 1000, tz=timezone.utc))
        assert len(got) == 50 and got[-1][0] == last_open and got == sorted(got)
        t, o, h, lo, cl, v = got[-1]
        assert (o, h, lo, cl) == pytest.approx((1.1 + 299e-4, 1.1 + 299e-4 + 5e-5, 1.1 + 299e-4 - 5e-5, 1.1 + 299e-4 + 2e-5), abs=1e-9) and v == 100 + 299
        req = srv.seen("GetTrendbarsReq")[-1]
        assert req["period"] == 5 and req["symbolId"] == 1 and req["count"] == 50 and req["toTimestamp"] - req["fromTimestamp"] == P.PERIOD_MAX_WINDOW_MS[5]
        with pytest.raises(ConfigError, match="limit"):
            await c.get_candles("EURUSD", "5m", 5000)
        with pytest.raises(ConfigError, match="таймфрейм"):
            await c.get_candles("EURUSD", "7m", 10)
    scenario(body)


def test_price_comes_from_the_spot_stream_and_tick_value_is_converted_to_the_deposit_currency():
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD", "XAUUSD", "USDJPY"])
        q = await c.get_price("EURUSD")
        assert (q.bid, q.ask) == (1.1, 1.1001) and q.loss_tick_value == pytest.approx(1e-5 * 100_000)  # котируется в USD = валюта депозита
        gold = await c.get_price("XAUUSD")
        assert gold.loss_tick_value == pytest.approx(0.01 * 100)
        jpy = await c.get_price("USDJPY")  # прибыль в иенах: 1 иена = 1/USDJPY доллара
        assert jpy.loss_tick_value == pytest.approx(0.001 * 100_000 / ((150.0 + 150.02) / 2))
        assert len(srv.seen("SubscribeSpotsReq")) == 3  # подписка один раз на символ, а не на каждый запрос цены
        await c.get_price("EURUSD")
        assert len(srv.seen("SubscribeSpotsReq")) == 3
    scenario(body)


# ---------------------------------------------------------------------------------------------- торговля


def test_market_order_sends_relative_levels_then_aligns_them_to_the_absolute_prices():
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD"])
        order = await c.market_order("EURUSD", "BUY", 0.5, sl=1.0981, tp=1.1081, client_id="tb_abc_E")
        req = srv.seen("NewOrderReq")[0]
        assert "stopLoss" not in req and "takeProfit" not in req  # MARKET не принимает абсолютные уровни
        assert req["orderType"] == P.ORDER_MARKET and req["tradeSide"] == P.SIDE_BUY and req["volume"] == 5_000_000 and req["clientOrderId"] == "tb_abc_E"
        assert req["relativeStopLoss"] == round((1.1001 - 1.0981) * 1e5) and req["relativeTakeProfit"] == round((1.1081 - 1.1001) * 1e5)
        amend = srv.seen("AmendPositionSLTPReq")[0]
        assert amend["stopLoss"] == 1.0981 and amend["takeProfit"] == 1.1081 and amend["positionId"] == int(order.position_id)
        pos = srv.positions[0]
        assert (pos["sl"], pos["tp"]) == (1.0981, 1.1081) and order.state == "DONE" and order.fill_price == 1.1001 and order.client_id == "tb_abc_E"
    scenario(body)


def test_events_without_client_msg_id_are_still_matched_by_client_order_id_and_position():
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["XAUUSD"])
        order = await c.market_order("XAUUSD", "SELL", 0.1, sl=2010.0, tp=1960.0, client_id="tb_x")
        assert srv.positions[0]["sl"] == 2010.0 and srv.positions[0]["tp"] == 1960.0 and order.position_id
        await c.modify_position(order.position_id, tp=1950.0)
        assert srv.positions[0]["tp"] == 1950.0
        await c.close_position(order.position_id)
        assert srv.positions == []
    scenario(body, server_kw={"echo_msg_id": False})


@pytest.mark.parametrize("code,exc,retryable,unknown", [
    ("MARKET_CLOSED", MarketClosedError, True, False),
    ("NOT_ENOUGH_MONEY", CTraderTradeError, False, False),
    ("TRADING_BAD_STOPS", CTraderTradeError, False, False),
])
def test_broker_refusals_are_typed_values(code, exc, retryable, unknown):
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD"])
        srv.effects = [code]
        with pytest.raises(exc) as ei:
            await c.market_order("EURUSD", "BUY", 0.1, sl=1.09, tp=1.11)
        assert ei.value.code == code and ei.value.retryable is retryable and ei.value.outcome_unknown is unknown and srv.positions == []
    scenario(body)


def test_trade_timeout_means_unknown_outcome_not_a_silent_success(monkeypatch):
    monkeypatch.setattr(C, "TRADE_TIMEOUT_S", 0.2)

    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD"])
        srv.effects = ["SILENT"]
        with pytest.raises(CTraderTradeError) as ei:
            await c.market_order("EURUSD", "BUY", 0.1, sl=1.09, tp=1.11)
        assert ei.value.outcome_unknown is True and ei.value.code == "TIMEOUT_ERROR"
    scenario(body)


def test_orders_are_checked_before_they_are_sent():
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD"])
        for kwargs, err in (({"volume": 0.005}, "не на сетке"), ({"volume": 0.015}, "не на сетке"), ({"volume": 0.1, "sl": 1.12, "tp": 1.13}, "SL"),
                            ({"volume": 0.1, "sl": 1.12}, "не по ту сторону")):
            with pytest.raises((ConfigError, CTraderTradeError), match=err):
                await c.market_order("EURUSD", "BUY", sl=kwargs.pop("sl", 1.09), tp=kwargs.pop("tp", None), **kwargs)
        with pytest.raises(ConfigError):
            await c.market_order("EURUSD", "HOLD", 0.1)
        assert srv.seen("NewOrderReq") == []  # ни один некорректный ордер до сервера не дошёл
    scenario(body)


def test_limit_order_cancel_modify_and_partial_close():
    async def body(srv, c):
        await c.connect()
        await c.load_symbols(["EURUSD"])
        lim = await c.limit_order("EURUSD", "BUY", 0.2, 1.0950, sl=1.0900, tp=1.1100, client_id="tb_lim",
                                  expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc))
        req = srv.seen("NewOrderReq")[0]
        assert req["orderType"] == P.ORDER_LIMIT and req["limitPrice"] == 1.095 and req["stopLoss"] == 1.09 and req["timeInForce"] == P.TIF_GTD
        orders = await c.get_orders()
        assert [(o["id"], o["symbol"], o["type"], o["clientId"], o["volume"]) for o in orders] == [(lim.order_id, "EURUSD", "BUY_LIMIT", "tb_lim", 0.2)]
        assert (await c.get_order(lim.order_id))["clientId"] == "tb_lim"
        await c.cancel_order(lim.order_id)
        assert srv.orders == [] and await c.get_orders() == []
        with pytest.raises(CTraderTradeError, match="ORDER_NOT_FOUND"):
            await c.cancel_order(lim.order_id)
        with pytest.raises(ConfigError, match="aware"):
            await c.limit_order("EURUSD", "BUY", 0.1, 1.09, expires_at=datetime(2030, 1, 1))
        m = await c.market_order("EURUSD", "SELL", 0.4, sl=1.1100, tp=1.0900)
        await c.modify_position(m.position_id, sl=1.1050)  # tp не трогаем: уровень сохраняется
        assert (srv.positions[0]["sl"], srv.positions[0]["tp"]) == (1.105, 1.09)
        await c.close_position(m.position_id, 0.1)
        assert srv.positions[0]["volume"] == 3_000_000 and srv.seen("ClosePositionReq")[-1]["volume"] == 1_000_000
        with pytest.raises(ConfigError, match="только"):
            await c.close_position(m.position_id, 5.0)
        with pytest.raises(CTraderTradeError, match="POSITION_NOT_FOUND"):
            await c.close_position("999999")
    scenario(body)


# ---------------------------------------------------------------------------------------------- Position Recovery, счёт, PnL


def test_position_recovery_reads_open_positions_from_the_broker_on_connect():
    async def body(srv, c):
        srv.positions.append({"id": 77, "symbolId": 2, "volume": 5_000, "side": P.SIDE_SELL, "price": 2001.5, "sl": 2011.5, "tp": 1961.5, "label": "tb_old_E"})
        srv.positions.append({"id": 78, "symbolId": 1, "volume": 1_000_000, "side": P.SIDE_BUY, "price": 1.1, "sl": None, "tp": None})
        srv.orders.append({"id": 90, "symbolId": 1, "volume": 2_000_000, "side": P.SIDE_BUY, "type": P.ORDER_LIMIT, "limit": 1.09, "sl": 1.08, "tp": 1.12, "cid": "tb_pend"})
        await c.connect()
        assert [(p.position_id, p.symbol, p.side, p.lots, p.open_price, p.stop_loss, p.take_profit, p.client_id) for p in c.positions] == [
            ("77", "XAUUSD", "SELL", 0.5, 2001.5, 2011.5, 1961.5, "tb_old_E"), ("78", "EURUSD", "BUY", 0.1, 1.1, None, None, None)]
        net = await c.get_position("XAUUSD")
        assert (net.symbol, net.amt, net.entry_price) == ("XAUUSD", -0.5, 2001.5)
        assert [o["clientId"] for o in await c.get_orders()] == ["tb_pend"]
    scenario(body)


def test_account_info_equity_comes_from_the_servers_unrealized_pnl():
    async def body(srv, c):
        await c.connect()
        srv.positions.append({"id": 1, "symbolId": 1, "volume": 1_000_000, "side": P.SIDE_BUY, "price": 1.1, "margin": 50_000})
        srv.positions.append({"id": 2, "symbolId": 2, "volume": 5_000, "side": P.SIDE_BUY, "price": 2000.0, "margin": 150_000})
        srv.unrealized = -25_000  # -250.00
        info = await c.get_account_info()
        assert (info.currency, info.balance, info.equity, info.margin, info.free_margin, info.leverage, info.broker) == ("USD", 100_000.0, 99_750.0, 2000.0, 97_750.0, 100.0, "FakeBroker")
        bal = await c.get_balance()
        assert (bal.asset, bal.wallet, bal.available, bal.unrealized_pnl) == ("USD", 100_000.0, 97_750.0, -250.0)
    scenario(body)


def test_realized_pnl_sums_trading_money_and_pages_through_deals():
    async def body(srv, c):
        await c.connect()
        t0 = 1_700_000_000_000
        for i in range(1200):  # больше одной страницы: -0.01 USD комиссии каждая
            srv.deals.append({"dealId": i, "executionTimestamp": t0 + i, "commission": -1, "moneyDigits": 2})
        srv.deals.append({"dealId": 5000, "executionTimestamp": t0 + 10, "commission": -200, "moneyDigits": 2,
                          "closePositionDetail": {"entryPrice": 1.1, "grossProfit": -3000, "swap": -100, "commission": -200, "balance": 0, "moneyDigits": 2}})
        srv.deals.append({"dealId": 5001, "executionTimestamp": t0 - 10_000, "commission": -99999, "moneyDigits": 2})  # вчера: вне окна
        total = await c.realized_pnl_since(t0, t0 + 86_400_000)
        assert total == pytest.approx(-12.0 - 2.0 - 30.0 - 1.0)  # комиссии 1200 сделок + комиссия закрытия + gross + swap
        assert len(srv.seen("DealListReq")) >= 3
    scenario(body, server_kw={"deal_page": 500})


# ---------------------------------------------------------------------------------------------- устойчивость


def test_connection_drop_fails_requests_fast_and_a_new_connect_recovers():
    async def body(srv, c):
        await c.connect()
        srv.drop()
        await asyncio.sleep(0.05)
        with pytest.raises(CTraderError, match="нет связи|потеряна|не подключён"):
            await asyncio.wait_for(c.get_account_info(), 3)  # не зависает до таймаута
        await c.connect()  # Mt5Link делает именно это после паузы
        info = await c.get_account_info()
        assert info.currency == "USD"
    scenario(body)


def test_heartbeats_are_sent_while_connected(monkeypatch):
    monkeypatch.setattr(C, "HEARTBEAT_S", 0.05)

    async def body(srv, c):
        await c.connect()
        await asyncio.sleep(0.4)
        assert srv.heartbeats >= 3
    scenario(body)


def test_server_side_disconnect_event_marks_the_link_dead():
    async def body(srv, c):
        await c.connect()
        c._dispatch("AccountsTokenInvalidatedEvent", {"reason": "revoked"}, None)
        with pytest.raises(CTraderError, match="нет связи|отключил"):
            await c.get_symbols()
    scenario(body)
