"""Клиент MetaApi: Fail-Fast по ключам, перевод исключений SDK, формат торговых вызовов (имитация RPC-соединения, без сети)."""

import asyncio
from datetime import datetime, timezone

import pytest
from metaapi_cloud_sdk.clients.error_handler import UnauthorizedException
from metaapi_cloud_sdk.clients.metaapi.not_synchronized_exception import NotSynchronizedException
from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

from exchange.mt5_metaapi import RETCODES, MetaApiCredentials, MetaApiError, MetaApiLiveClient, MetaApiTradeError
from utils.errors import ConfigError

OK = {"METAAPI_TOKEN": "super-secret-token", "METAAPI_ACCOUNT_ID": "acc-123"}
OK_DONE = {"numericCode": 10009, "stringCode": "TRADE_RETCODE_DONE", "message": "Request completed", "orderId": "77", "positionId": "55"}
SPEC = {"volumeStep": 0.01, "minVolume": 0.01, "maxVolume": 100.0, "tickSize": 0.00001, "digits": 5}


def run(coro):
    return asyncio.run(coro)


class FakeRpc:
    """Имитация RpcMetaApiConnectionInstance: пишет вызовы, ответы задаются полями."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.positions = [
            {"id": "55", "type": "POSITION_TYPE_BUY", "symbol": "EURUSD", "volume": 0.2, "openPrice": 1.1, "stopLoss": 1.09,
             "takeProfit": 0, "profit": 3.0, "clientId": "c1"},
            {"id": "56", "type": "POSITION_TYPE_SELL", "symbol": "EURUSD", "volume": 0.1, "openPrice": 1.2, "profit": -1.0},
        ]
        self.raise_exc: Exception | None = None
        self.effects: list = []  # по одному на торговый вызов: исключение, свой ответ (dict) или None (= OK_DONE)
        self.reply: dict = OK_DONE

    def _maybe_raise(self):
        if self.raise_exc:
            raise self.raise_exc

    async def get_symbol_specification(self, symbol):
        self.calls.append(("spec", symbol))
        return {**SPEC, "fillingModes": ["SYMBOL_FILLING_FOK", "SYMBOL_FILLING_IOC"]}

    async def calculate_margin(self, order):
        self.calls.append(("margin", order))
        return {"margin": 1100.0 * order["volume"]}

    async def get_account_information(self):
        self._maybe_raise()
        return {"broker": "B", "server": "S", "currency": "USD", "balance": 1000.0, "equity": 1010.5, "margin": 10.0,
                "freeMargin": 990.0, "leverage": 100, "tradeAllowed": True,
                "marginMode": "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING", "name": "n", "login": 1}

    async def get_positions(self):
        return self.positions

    async def get_position(self, pid):
        return next(p for p in self.positions if p["id"] == pid)

    async def get_symbol_price(self, symbol):
        return {"symbol": symbol, "bid": 1.1, "ask": 1.1002, "time": datetime(2026, 1, 1, tzinfo=timezone.utc)}

    async def get_symbols(self):
        return ["EURUSD", "BTCUSDm"]

    async def _record(self, name, args):
        self.calls.append((name, *args))
        self._maybe_raise()
        effect = self.effects.pop(0) if self.effects else None
        if isinstance(effect, Exception):
            raise effect
        return effect if isinstance(effect, dict) else self.reply

    async def create_market_buy_order(self, *a):
        return await self._record("market_buy", a)

    async def create_market_sell_order(self, *a):
        return await self._record("market_sell", a)

    async def create_limit_buy_order(self, *a):
        return await self._record("limit_buy", a)

    async def create_limit_sell_order(self, *a):
        return await self._record("limit_sell", a)

    async def modify_position(self, *a):
        return await self._record("modify", a)

    async def close_position(self, *a):
        return await self._record("close", a)

    async def close_position_partially(self, *a):
        return await self._record("close_part", a)

    async def cancel_order(self, *a):
        return await self._record("cancel", a)


@pytest.fixture
def client():
    c = MetaApiLiveClient(OK)
    c._connection = FakeRpc()
    return c


# ---------------------------------------------------------------------------------------------- конфигурация


@pytest.mark.parametrize("env", [{}, {"METAAPI_TOKEN": "t"}, {"ACCOUNT_ID": "a"}, {"METAAPI_TOKEN": "  ", "ACCOUNT_ID": "a"}])
def test_missing_credentials_fail_fast(env):
    with pytest.raises(ConfigError, match="METAAPI_TOKEN|METAAPI_ACCOUNT_ID"):
        MetaApiLiveClient(env)


def test_corrupted_token_is_rejected_before_any_network():
    with pytest.raises(ConfigError, match="позиции 5") as ei:
        MetaApiLiveClient({"METAAPI_TOKEN": "abcde더fgh", "ACCOUNT_ID": "a"})
    assert "abcde" not in str(ei.value)  # сам токен в сообщение не попадает


def test_constructor_does_not_touch_the_network_and_hides_the_token():
    c = MetaApiLiveClient(OK)
    assert c.account_id == "acc-123"
    assert "super-secret-token" not in repr(c) + repr(MetaApiCredentials.from_env(OK))


def test_account_id_alias_and_priority():
    assert MetaApiCredentials.from_env({"METAAPI_TOKEN": "t", "ACCOUNT_ID": "plain"}).account_id == "plain"
    assert MetaApiCredentials.from_env({"METAAPI_TOKEN": "t", "ACCOUNT_ID": "plain", "METAAPI_ACCOUNT_ID": "alias"}).account_id == "alias"


def test_methods_require_connect():
    c = MetaApiLiveClient(OK)
    with pytest.raises(MetaApiError, match="не подключён"):
        run(c.get_balance())
    with pytest.raises(MetaApiError, match="не подключён"):
        c.connection_state()


# ---------------------------------------------------------------------------------------------- чтение


def test_balance_account_price_positions(client):
    bal = run(client.get_balance())
    assert (bal.asset, bal.wallet, bal.available, bal.unrealized_pnl) == ("USD", 1000.0, 990.0, 10.5)
    q = run(client.get_price("EURUSD"))
    assert (q.bid, q.ask) == (1.1, 1.1002) and q.spread == pytest.approx(0.0002)
    pos = run(client.get_positions())
    assert [(p.position_id, p.side, p.lots, p.stop_loss, p.take_profit) for p in pos] == [("55", "BUY", 0.2, 1.09, None), ("56", "SELL", 0.1, None, None)]
    net = run(client.get_position("EURUSD"))  # 0.2 buy - 0.1 sell
    assert net.amt == pytest.approx(0.1) and net.entry_price == pytest.approx((1.1 * 0.2 + 1.2 * 0.1) / 0.3)
    assert run(client.get_position("GBPUSD")).amt == 0


# ---------------------------------------------------------------------------------------------- торговля


def test_market_order_payloads(client):
    res = run(client.market_order("EURUSD", "BUY", 0.1, 1.09, 1.12, client_id="tb1"))
    assert client._connection.calls[-1] == ("market_buy", "EURUSD", 0.1, 1.09, 1.12, {"clientId": "tb1"})
    assert (res.order_id, res.position_id, res.string_code) == ("77", "55", "TRADE_RETCODE_DONE")
    run(client.market_order("EURUSD", "SELL", 0.1))
    assert client._connection.calls[-1] == ("market_sell", "EURUSD", 0.1, None, None, None)


def test_limit_order_expiration_and_validation(client):
    exp = datetime(2026, 1, 2, tzinfo=timezone.utc)
    run(client.limit_order("EURUSD", "SELL", 0.1, 1.2, client_id="tb2", expires_at=exp))
    assert client._connection.calls[-1] == (
        "limit_sell", "EURUSD", 0.1, 1.2, None, None, {"clientId": "tb2", "expiration": {"type": "ORDER_TIME_SPECIFIED", "time": exp}})
    with pytest.raises(ConfigError, match="aware"):
        run(client.limit_order("EURUSD", "BUY", 0.1, 1.0, expires_at=datetime(2026, 1, 2)))


@pytest.mark.parametrize("side,qty", [("LONG", 0.1), ("BUY", 0), ("BUY", -1), ("BUY", True)])
def test_invalid_arguments_rejected_before_sending(client, side, qty):
    with pytest.raises(ConfigError):
        run(client.market_order("EURUSD", side, qty))
    assert client._connection.calls == []


def test_lot_grid_is_enforced_when_specs_loaded(client):
    client._specs["EURUSD"] = SPEC
    with pytest.raises(ConfigError, match="сетке"):
        run(client.market_order("EURUSD", "BUY", 0.015))
    assert client.round_lots("EURUSD", 0.0179) == 0.01
    assert client.round_price("EURUSD", 1.123456) == 1.12346
    with pytest.raises(ConfigError, match="вне допустимых"):
        client.round_lots("EURUSD", 0.004)
    run(client.market_order("EURUSD", "BUY", 0.02))  # на сетке - уходит


def test_stop_loss_and_take_profit_keep_the_other_leg(client):
    run(client.stop_loss("55", 1.1))  # TP у позиции нет -> None; SL новый
    assert client._connection.calls[-1] == ("modify", "55", 1.1, None)
    client._connection.positions[0]["takeProfit"] = 1.2
    run(client.stop_loss("55", 1.1))
    assert client._connection.calls[-1] == ("modify", "55", 1.1, 1.2)  # TP не сброшен
    run(client.take_profit("55", 1.3))
    assert client._connection.calls[-1] == ("modify", "55", 1.09, 1.3)  # SL не сброшен


def test_close_and_cancel(client):
    run(client.close_position("55"))
    assert client._connection.calls[-1] == ("close", "55", None)
    run(client.close_position("55", 0.05, client_id="x"))
    assert client._connection.calls[-1] == ("close_part", "55", 0.05, {"clientId": "x"})
    run(client.cancel_order("99"))
    assert client._connection.calls[-1] == ("cancel", "99")


# ---------------------------------------------------------------------------------------------- исключения SDK


def test_sdk_exceptions_are_translated(client):
    rpc = client._connection
    rpc.raise_exc = TradeException("market closed", 10018, "TRADE_RETCODE_MARKET_CLOSED")
    with pytest.raises(MetaApiTradeError) as ei:
        run(client.market_order("EURUSD", "BUY", 0.1))
    assert (ei.value.numeric_code, ei.value.string_code) == (10018, "TRADE_RETCODE_MARKET_CLOSED")
    rpc.raise_exc = TimeoutException("slow")
    with pytest.raises(MetaApiError, match="таймаут"):
        run(client.get_account_info())
    rpc.raise_exc = NotSynchronizedException("not synced")
    with pytest.raises(MetaApiError, match="не синхронизирован"):
        run(client.get_account_info())
    rpc.raise_exc = UnauthorizedException("bad token", 401)
    with pytest.raises(ConfigError, match="отказал в доступе"):
        run(client.get_account_info())


def test_foreign_exceptions_are_not_swallowed(client):
    client._connection.raise_exc = KeyError("bug")
    with pytest.raises(KeyError):
        run(client.get_account_info())


def test_connect_waits_in_order_and_closes_on_failure(monkeypatch):
    events: list[str] = []
    fail_sync = {"on": False}

    class Acct:
        id, name, server, login, type, version, region = "acc-123", "demo", "Srv", 1, "cloud-g2", 5, "london"
        state, connection_status = "UNDEPLOYED", "DISCONNECTED"

        async def deploy(self):
            events.append("deploy")

        async def wait_deployed(self, timeout_in_seconds):
            events.append(f"deployed:{timeout_in_seconds}")

        async def wait_connected(self, timeout_in_seconds):
            events.append("connected")

        def get_rpc_connection(self):
            rpc = FakeRpc()

            async def connect():
                events.append("rpc")

            async def wait_synchronized(timeout_in_seconds):
                events.append("synced")
                if fail_sync["on"]:
                    raise TimeoutException("sync")

            rpc.connect, rpc.wait_synchronized = connect, wait_synchronized
            return rpc

    class Api:
        def __init__(self, token, opts=None):
            self.metatrader_account_api = self

        async def get_account(self, account_id):
            assert account_id == "acc-123"
            return Acct()

        def close(self):
            events.append("api_closed")

    import metaapi_cloud_sdk
    monkeypatch.setattr(metaapi_cloud_sdk, "MetaApi", Api)
    c = MetaApiLiveClient(OK)
    run(c.connect(timeout_s=7))
    assert events == ["deploy", "deployed:7", "connected", "rpc", "synced"]
    assert c.connection_state().server == "Srv" and c.connection_state().platform == "mt5"

    events.clear()
    fail_sync["on"] = True
    c2 = MetaApiLiveClient(OK)
    with pytest.raises(MetaApiError, match="таймаут"):
        run(c2.connect())
    assert events[-1] == "api_closed" and c2._connection is None  # при неудаче фоновые соединения SDK закрыты


# ---------------------------------------------------------------------------------------------- режим исполнения (filling mode)


def trade_error(code: int) -> TradeException:
    return TradeException(f"retcode {code}", code, f"TRADE_RETCODE_{RETCODES[code][0]}")


def market_calls(client):
    return [c for c in client._connection.calls if c[0] in ("market_buy", "market_sell")]


def test_filling_mode_is_taken_from_the_symbol_spec_fok_first(client):
    run(client.load_symbols(["EURUSD"]))
    assert client.filling_candidates("EURUSD") == ["ORDER_FILLING_FOK", "ORDER_FILLING_IOC"]
    run(client.market_order("EURUSD", "BUY", 0.1, 1.09, 1.12, client_id="c"))
    assert client._connection.calls[-1][-1] == {"clientId": "c", "fillingModes": ["ORDER_FILLING_FOK"]}


def test_ioc_only_symbol_and_unknown_spec():
    c = MetaApiLiveClient(OK)
    c._connection = FakeRpc()
    assert c.filling_candidates("EURUSD") == []  # спецификация не загружена - режим выбирает MetaApi
    c._specs["EURUSD"] = {**SPEC, "fillingModes": ["SYMBOL_FILLING_IOC", "SYMBOL_FILLING_BOC"]}
    assert c.filling_candidates("EURUSD") == ["ORDER_FILLING_IOC"]  # BOC для рыночных ордеров не используется
    run(c.market_order("EURUSD", "SELL", 0.1))
    assert c._connection.calls[-1][-1] == {"fillingModes": ["ORDER_FILLING_IOC"]}
    c._specs["EURUSD"] = {**SPEC, "fillingModes": []}
    run(c.market_order("EURUSD", "SELL", 0.1))
    assert c._connection.calls[-1][-1] is None


def test_invalid_fill_10030_falls_back_to_the_next_mode_only(client):
    run(client.load_symbols(["EURUSD"]))
    client._connection.effects = [trade_error(10030), None]
    res = run(client.market_order("EURUSD", "BUY", 0.1))
    assert [c[-1]["fillingModes"] for c in market_calls(client)] == [["ORDER_FILLING_FOK"], ["ORDER_FILLING_IOC"]]
    assert res.string_code == "TRADE_RETCODE_DONE"
    client._connection.calls.clear()
    client._connection.effects = [trade_error(10030), trade_error(10030)]  # все режимы отклонены - ошибка, не бесконечный цикл
    with pytest.raises(MetaApiTradeError) as ei:
        run(client.market_order("EURUSD", "BUY", 0.1))
    assert ei.value.numeric_code == 10030 and len(market_calls(client)) == 2
    client._connection.calls.clear()
    client._connection.effects = [trade_error(10019)]  # другая ошибка (нет денег) - без повторов
    with pytest.raises(MetaApiTradeError, match="NO_MONEY"):
        run(client.market_order("EURUSD", "BUY", 0.1))
    assert len(market_calls(client)) == 1


# ---------------------------------------------------------------------------------------------- коды возврата брокера


@pytest.mark.parametrize("code,name,retryable,unknown", [
    (10004, "REQUOTE", True, False), (10013, "INVALID", False, False), (10030, "INVALID_FILL", False, False),
    (10018, "MARKET_CLOSED", True, False), (10019, "NO_MONEY", False, False), (10016, "INVALID_STOPS", False, False),
    (10012, "TIMEOUT", False, True), (10031, "CONNECTION", True, True), (10014, "INVALID_VOLUME", False, False),
])
def test_retcodes_become_readable_trade_errors(client, code, name, retryable, unknown):
    client._connection.effects = [trade_error(code)]
    with pytest.raises(MetaApiTradeError) as ei:
        run(client.market_order("EURUSD", "BUY", 0.1))
    e = ei.value
    assert (e.numeric_code, e.retcode_name, e.retryable, e.outcome_unknown) == (code, f"TRADE_RETCODE_{name}", retryable, unknown)
    assert e.reason and str(code) in str(e) and e.reason in str(e)


def test_unknown_retcode_is_not_assumed_harmless(client):
    client._connection.effects = [TradeException("weird", 99999, "TRADE_RETCODE_WEIRD")]
    with pytest.raises(MetaApiTradeError) as ei:
        run(client.market_order("EURUSD", "BUY", 0.1))
    assert ei.value.outcome_unknown and not ei.value.retryable and "неизвестный код" in ei.value.reason


def test_all_retcodes_have_a_description():
    assert {10004, 10013, 10030}.issubset(RETCODES) and all(len(v) == 4 and v[1] for v in RETCODES.values())


def test_success_codes_and_partial_fill(client):
    client._connection.effects = [{**OK_DONE, "numericCode": 10010, "stringCode": "TRADE_RETCODE_DONE_PARTIAL"}]
    res = run(client.market_order("EURUSD", "BUY", 0.1))
    assert res.partial and res.numeric_code == 10010
    assert not run(client.market_order("EURUSD", "BUY", 0.1)).partial
    client._connection.effects = [{"numericCode": 10008, "stringCode": "TRADE_RETCODE_PLACED", "orderId": "5"}]  # отложенный ордер принят
    assert run(client.limit_order("EURUSD", "BUY", 0.1, 1.0)).order_id == "5"


def test_failure_code_in_a_normal_reply_is_still_an_error(client):
    client._connection.effects = [{"numericCode": 10006, "stringCode": "TRADE_RETCODE_REJECT", "message": "no"}]
    with pytest.raises(MetaApiTradeError, match="REJECT"):
        run(client.market_order("EURUSD", "BUY", 0.1))


# ---------------------------------------------------------------------------------------------- проверки до отправки


def test_stops_must_be_on_the_right_side_and_grid(client):
    with pytest.raises(ConfigError, match="расположены неверно"):
        run(client.market_order("EURUSD", "BUY", 0.1, 1.12, 1.09))  # BUY: SL должен быть ниже TP
    with pytest.raises(ConfigError, match="расположены неверно"):
        run(client.market_order("EURUSD", "SELL", 0.1, 1.09, 1.12))  # SELL: TP должен быть ниже SL
    client._specs["EURUSD"] = SPEC
    with pytest.raises(ConfigError, match="не на сетке"):
        run(client.market_order("EURUSD", "BUY", 0.1, 1.090001))
    with pytest.raises(ConfigError, match="не на сетке"):
        run(client.limit_order("EURUSD", "BUY", 0.1, 1.0000001))
    assert not [c for c in client._connection.calls if c[0] in ("market_buy", "market_sell", "limit_buy")]


def test_modify_position_requires_a_level_and_keeps_the_other(client):
    with pytest.raises(ConfigError, match="хотя бы один"):
        run(client.modify_position("55"))
    run(client.modify_position("55", sl=1.1))  # позиция BUY с SL 1.09 без TP
    assert client._connection.calls[-1] == ("modify", "55", 1.1, None)
    run(client.modify_position("55", tp=1.3))
    assert client._connection.calls[-1] == ("modify", "55", 1.09, 1.3)
    run(client.modify_position("55", sl=1.1, tp=1.3))
    assert client._connection.calls[-1] == ("modify", "55", 1.1, 1.3)
    with pytest.raises(ConfigError, match="расположены неверно"):
        run(client.modify_position("55", tp=1.0))  # TP ниже существующего SL у покупки
    client._connection.effects = [{"numericCode": 10025, "stringCode": "TRADE_RETCODE_NO_CHANGES", "message": "no changes"}]
    assert run(client.modify_position("55", sl=1.09)).numeric_code == 10025  # «без изменений» - успех (например, повтор после обрыва)


def test_breakeven_and_trailing_flow_for_a_sell(client):
    client._connection.positions.append({"id": "70", "type": "POSITION_TYPE_SELL", "symbol": "XAUUSD", "volume": 0.5,
                                         "openPrice": 2000.0, "stopLoss": 2010.0, "takeProfit": 1970.0})
    run(client.modify_position("70", sl=2000.0))  # безубыток
    assert client._connection.calls[-1] == ("modify", "70", 2000.0, 1970.0)
    run(client.modify_position("70", sl=1990.0))  # трейлинг (что стоп только подтягивается - решает вызывающий)
    assert client._connection.calls[-1] == ("modify", "70", 1990.0, 1970.0)


def test_partial_close_for_scale_out(client):
    run(client.close_position("55", 0.1))  # из 0.2 лота
    assert client._connection.calls[-1] == ("close_part", "55", 0.1, None)
    run(client.close_position("55", 0.2, client_id="z"))  # объём равен позиции - полное закрытие
    assert client._connection.calls[-1] == ("close", "55", {"clientId": "z"})
    with pytest.raises(ConfigError, match="только 0.2"):
        run(client.close_position("55", 0.3))
    client._specs["EURUSD"] = SPEC
    with pytest.raises(ConfigError, match="сетке"):
        run(client.close_position("55", 0.115))
    client._specs["EURUSD"] = {**SPEC, "minVolume": 0.05}  # у брокера минимум 0.05 при шаге 0.01
    client._connection.positions[0]["volume"] = 0.07
    with pytest.raises(ConfigError, match="меньше minVolume"):  # остаток 0.01 < 0.05 - такой остаток брокер не примет
        run(client.close_position("55", 0.06))
    client._connection.positions[0]["volume"] = 0.10
    run(client.close_position("55", 0.05))  # закрываемый объём и остаток (0.05) не меньше minVolume - допустимо
    assert client._connection.calls[-1] == ("close_part", "55", 0.05, None)


# ---------------------------------------------------------------------------------------------- спецификация, маржа, тики


def test_specification_is_cached_and_margin_uses_the_terminal(client):
    spec = run(client.get_symbol_specification("EURUSD"))
    assert spec["volumeStep"] == 0.01 and client._specs["EURUSD"] is spec  # кеш: по нему же идут проверки сетки
    assert run(client.calculate_margin("EURUSD", "BUY", 2.0, 1.1)) == pytest.approx(2200.0)
    assert client._connection.calls[-1] == ("margin", {"symbol": "EURUSD", "type": "ORDER_TYPE_BUY", "volume": 2.0, "openPrice": 1.1})


def test_quote_carries_tick_values_for_the_risk_calculator(client):
    async def with_ticks(symbol):
        return {"symbol": symbol, "bid": 150.0, "ask": 150.01, "lossTickValue": 0.667, "profitTickValue": 0.666}

    async def without_ticks(symbol):
        return {"symbol": symbol, "bid": 1.1, "ask": 1.1002}

    client._connection.get_symbol_price = with_ticks
    q = run(client.get_price("USDJPY"))
    assert (q.loss_tick_value, q.profit_tick_value) == (0.667, 0.666)
    client._connection.get_symbol_price = without_ticks
    assert run(client.get_price("EURUSD")).loss_tick_value is None


def test_end_to_end_plan_to_order(client):
    """Связка: спецификация -> лоты по риску -> ордер со стопом (всё на моках)."""
    from exchange.mt5_risk import Mt5RiskParams, size_position
    from exchange.symbol_mapper import parse_spec

    raw = run(client.get_symbol_specification("EURUSD"))
    spec = parse_spec("EURUSD", "EURUSD", {**raw, "contractSize": 100_000, "point": 0.00001, "profitCurrency": "USD"})
    plan = size_position(spec, side="BUY", entry=1.1, sl=1.098, tp=1.106, equity=100_000.0, free_margin=100_000.0, tick_value=1.0, params=Mt5RiskParams(risk_usdt=1000.0),
                         margin_per_lot=run(client.calculate_margin("EURUSD", "BUY", 1.0, 1.1)))
    assert plan.lots == 5.0
    res = run(client.market_order(plan.broker_symbol, plan.side, plan.lots, plan.sl, plan.tp, client_id="tb1"))
    assert client._connection.calls[-1][:5] == ("market_buy", "EURUSD", 5.0, 1.098, 1.106) and res.position_id == "55"
