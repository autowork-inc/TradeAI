"""exchange/binance_live.py: ПРАВИЛЬНЫЕ ЗАПРОСЫ к Binance (payload «на проводе», подпись) и строгая локальная проверка.

Клиент ходит на локальный aiohttp-сервер, который записывает каждый запрос и САМ проверяет подпись HMAC-SHA256 по секрету
(как биржа): неверная подпись = 401 -> тест падает. Реальная биржа не используется, ключи фиктивные.
"""

import asyncio
import hashlib
import hmac
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl

import aiohttp
import pytest
from aiohttp import web

import config
from exchange import binance_live as bl
from exchange.binance_live import BinanceLiveClient, OrderValidationError, client_id, fmt_decimal
from execution.exchange_filters import SymbolFilters
from execution.rest import BinanceAPIError, Credentials, SignedRest
from tests.conftest import serve
from utils.errors import ConfigError, DataError

KEY, SECRET = "test-key-0123456789", "test-secret-abcdefghij"
FILTERS = {
    "SOLUSDT": SymbolFilters("SOLUSDT", tick_size=0.01, step_size=0.1, min_qty=0.1, min_notional=5.0),
    "1000PEPEUSDT": SymbolFilters("1000PEPEUSDT", tick_size=0.0000001, step_size=1.0, min_qty=1.0, min_notional=5.0),
}


@dataclass
class Seen:
    method: str
    path: str
    params: dict[str, str]
    headers: dict[str, str]


@dataclass
class Server:
    seen: list[Seen] = field(default_factory=list)
    replies: dict[tuple[str, str], object] = field(default_factory=dict)  # (метод, путь) -> тело | (статус, тело)

    def last(self) -> Seen:
        return self.seen[-1]

    def of(self, method: str, path: str) -> list[Seen]:
        return [s for s in self.seen if (s.method, s.path) == (method, path)]


def _make_app(server: Server) -> web.Application:
    async def handler(request: web.Request) -> web.Response:
        query = request.query_string
        params = dict(parse_qsl(query, keep_blank_values=True))
        if "signature" in params:  # приватный запрос: подпись обязана сойтись с секретом и покрывать всё остальное
            unsigned = query.rsplit("&signature=", 1)[0]
            expected = hmac.new(SECRET.encode(), unsigned.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, params["signature"]) or request.headers.get("X-MBX-APIKEY") != KEY:
                return web.json_response({"code": -1022, "msg": "Signature for this request is not valid."}, status=401)
            assert {"timestamp", "recvWindow"} <= params.keys()
        server.seen.append(Seen(request.method, request.path, {k: v for k, v in params.items() if k != "signature"},
                                dict(request.headers)))
        reply = server.replies.get((request.method, request.path), {})
        status, body = reply if isinstance(reply, tuple) else (200, reply)
        return web.json_response(body, status=status)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    return app


def run_client(scenario, *, replies=None, filters=FILTERS):
    """scenario(client, server) - корутина; клиент подключён к локальному серверу с проверкой подписи."""

    async def main():
        server = Server(replies=dict(replies or {}))
        async with serve(_make_app(server)) as host:
            async with aiohttp.ClientSession() as session:
                client = BinanceLiveClient(SignedRest(session, f"http://{host}", Credentials(KEY, SECRET)))
                client.set_filters(filters)
                return await scenario(client, server)

    return asyncio.run(main())


ORDER_OK = {"orderId": 11, "symbol": "SOLUSDT", "clientOrderId": "x", "status": "FILLED", "side": "BUY", "type": "MARKET",
            "origQty": "1.0", "executedQty": "1.0", "avgPrice": "100.00", "price": "0"}
ALGO_OK = {"algoId": 5, "clientAlgoId": "a", "algoStatus": "NEW", "orderType": "STOP_MARKET", "symbol": "SOLUSDT"}


# ------------------------------------------------------------------------------------------- подпись


def test_private_request_is_signed_and_carries_the_api_key_header():
    async def scenario(c, s):
        await c.get_balance()
        return s.last()

    seen = run_client(scenario, replies={("GET", config.BALANCE_PATH): [
        {"asset": "USDT", "balance": "123.45", "availableBalance": "100.5", "crossUnPnl": "-1.5"}]})
    assert seen.headers["X-MBX-APIKEY"] == KEY and seen.params["recvWindow"] == str(config.RECV_WINDOW_MS)  # подпись проверил сервер


def test_wrong_secret_is_rejected_by_the_exchange_check():
    async def scenario(c, s):
        c.rest._creds = Credentials(KEY, "other-secret-value")
        with pytest.raises(BinanceAPIError) as exc:
            await c.get_balance()
        return exc.value.code

    assert run_client(scenario, replies={("GET", config.BALANCE_PATH): []}) == -1022


def test_credentials_come_from_the_environment_and_the_secret_is_hidden(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", KEY)
    monkeypatch.setenv("BINANCE_API_SECRET", SECRET)
    creds = Credentials.from_env()
    assert (creds.api_key, creds.api_secret) == (KEY, SECRET) and SECRET not in repr(creds)
    monkeypatch.delenv("BINANCE_API_SECRET")
    with pytest.raises(ConfigError):
        Credentials.from_env()

    async def build():
        async with aiohttp.ClientSession() as session:
            return BinanceLiveClient.from_env(session, "http://127.0.0.1:1")

    with pytest.raises(ConfigError):
        asyncio.run(build())


# --------------------------------------------------------------------------------- payload ордеров


def test_market_entry_payload():
    async def scenario(c, s):
        res = await c.market_order("SOLUSDT", "BUY", 1.5, client_id="tbE_abc", ref_price=100.0)
        return res, s.last()

    res, seen = run_client(scenario, replies={("POST", config.ORDER_PATH): ORDER_OK})
    assert (seen.method, seen.path) == ("POST", "/fapi/v1/order")
    p = seen.params
    assert {k: p[k] for k in ("symbol", "side", "type", "quantity", "newClientOrderId", "newOrderRespType")} == {
        "symbol": "SOLUSDT", "side": "BUY", "type": "MARKET", "quantity": "1.5", "newClientOrderId": "tbE_abc",
        "newOrderRespType": "RESULT"}
    assert "reduceOnly" not in p and "price" not in p and "timeInForce" not in p
    assert res.executed_qty == 1.0 and res.avg_price == 100.0 and res.done


def test_reduce_only_market_close_payload_and_no_notional_check():
    async def scenario(c, s):
        await c.market_order("SOLUSDT", "SELL", 0.1, client_id="tbX_1", reduce_only=True)  # 0.1 * цена < MIN_NOTIONAL, но reduceOnly
        return s.last().params

    p = run_client(scenario, replies={("POST", config.ORDER_PATH): ORDER_OK})
    assert p["reduceOnly"] == "true" and p["side"] == "SELL" and p["quantity"] == "0.1"


def test_limit_entry_payload_gtc_and_post_only():
    async def scenario(c, s):
        await c.limit_order("SOLUSDT", "SELL", 2.0, 101.25, client_id="tbE_1")
        gtc = s.last().params
        await c.limit_order("SOLUSDT", "SELL", 2.0, 101.25, client_id="tbE_2", post_only=True)
        return gtc, s.last().params

    gtc, gtx = run_client(scenario, replies={("POST", config.ORDER_PATH): {**ORDER_OK, "status": "NEW", "type": "LIMIT"}})
    assert (gtc["type"], gtc["price"], gtc["quantity"], gtc["timeInForce"]) == ("LIMIT", "101.25", "2", "GTC")
    assert gtx["timeInForce"] == "GTX" and gtx["newClientOrderId"] == "tbE_2"


def test_stop_loss_and_take_profit_go_to_algo_service_with_reduce_only_quantity():
    async def scenario(c, s):
        await c.stop_loss("SOLUSDT", "SELL", 98.0, client_id="tbS_1", qty=1.0)
        sl = s.last()
        await c.take_profit("SOLUSDT", "SELL", 106.0, client_id="tbT_1", qty=1.0)
        return sl, s.last()

    sl, tp = run_client(scenario, replies={("POST", config.ALGO_ORDER_PATH): ALGO_OK})
    assert sl.path == tp.path == "/fapi/v1/algoOrder"
    assert sl.params["algoType"] == tp.params["algoType"] == "CONDITIONAL"  # не /fapi/v1/order: для STOP_MARKET там -4120
    assert (sl.params["type"], sl.params["triggerPrice"], sl.params["clientAlgoId"]) == ("STOP_MARKET", "98", "tbS_1")
    assert (tp.params["type"], tp.params["triggerPrice"], tp.params["clientAlgoId"]) == ("TAKE_PROFIT_MARKET", "106", "tbT_1")
    for p in (sl.params, tp.params):
        assert (p["side"], p["symbol"], p["quantity"], p["reduceOnly"], p["workingType"]) == (
            "SELL", "SOLUSDT", "1", "true", config.ALGO_WORKING_TYPE)
        assert "closePosition" not in p


def test_close_position_variant_has_no_quantity_and_conflicts_are_refused():
    async def scenario(c, s):
        await c.stop_loss("SOLUSDT", "SELL", 98.0, client_id="tbS_1", close_position=True)
        p = s.last().params
        n = len(s.seen)
        with pytest.raises(OrderValidationError):
            await c.stop_loss("SOLUSDT", "SELL", 98.0, client_id="tbS_2", qty=1.0, close_position=True)
        with pytest.raises(OrderValidationError):
            await c.take_profit("SOLUSDT", "SELL", 106.0, client_id="tbT_2")  # ни qty, ни closePosition
        return p, len(s.seen) - n

    p, extra_requests = run_client(scenario, replies={("POST", config.ALGO_ORDER_PATH): ALGO_OK})
    assert p["closePosition"] == "true" and "quantity" not in p and "reduceOnly" not in p
    assert extra_requests == 0  # отказ - до сети


def test_cancel_and_status_payloads():
    async def scenario(c, s):
        await c.cancel_order("SOLUSDT", "tbE_1")
        cancel = s.last()
        await c.get_order("SOLUSDT", "tbE_1")
        get = s.last()
        await c.cancel_algo_order("tbS_1")
        cancel_algo = s.last()
        await c.get_algo_order("tbS_1")
        return cancel, get, cancel_algo, s.last()

    cancel, get, cancel_algo, get_algo = run_client(scenario, replies={
        ("DELETE", config.ORDER_PATH): {**ORDER_OK, "status": "CANCELED"}, ("GET", config.ORDER_PATH): ORDER_OK,
        ("DELETE", config.ALGO_ORDER_PATH): {"algoId": 1, "clientAlgoId": "tbS_1", "code": "200", "msg": "success"},
        ("GET", config.ALGO_ORDER_PATH): {**ALGO_OK, "clientAlgoId": "tbS_1", "algoStatus": "CANCELED"}})
    assert (cancel.method, cancel.path, cancel.params["symbol"], cancel.params["origClientOrderId"]) == ("DELETE", "/fapi/v1/order", "SOLUSDT", "tbE_1")
    assert (get.method, get.params["origClientOrderId"]) == ("GET", "tbE_1")
    assert (cancel_algo.method, cancel_algo.path, cancel_algo.params["clientAlgoId"]) == ("DELETE", "/fapi/v1/algoOrder", "tbS_1")
    assert "symbol" not in cancel_algo.params
    assert get_algo.params["clientAlgoId"] == "tbS_1"


def test_small_prices_and_big_quantities_are_sent_without_exponent():
    assert fmt_decimal(0.00001234) == "0.00001234" and fmt_decimal(1e-07) == "0.0000001"
    assert fmt_decimal(100.0) == "100" and fmt_decimal(1500.5) == "1500.5" and fmt_decimal(1e6) == "1000000"

    async def scenario(c, s):
        await c.limit_order("1000PEPEUSDT", "BUY", 1234500.0, 0.0000123, client_id="tbE_p")
        return s.last().params

    p = run_client(scenario, replies={("POST", config.ORDER_PATH): ORDER_OK})
    assert (p["price"], p["quantity"]) == ("0.0000123", "1234500")


# ----------------------------------------------------------------------------- локальная проверка


@pytest.mark.parametrize("call, expect", [
    (lambda c: c.market_order("SOLUSDT", "BUY", 1.05, client_id="a", ref_price=100.0), "сетке лота"),  # шаг 0.1
    (lambda c: c.market_order("SOLUSDT", "BUY", 0.0, client_id="a", ref_price=100.0), "сетке лота"),
    (lambda c: c.market_order("SOLUSDT", "BUY", 0.1, client_id="a", ref_price=10.0), "MIN_NOTIONAL"),  # 1.0 USDT < 5
    (lambda c: c.market_order("SOLUSDT", "BUY", 1.0, client_id="a"), "ref_price"),
    (lambda c: c.market_order("SOLUSDT", "LONG", 1.0, client_id="a", ref_price=100.0), "side"),
    (lambda c: c.limit_order("SOLUSDT", "BUY", 1.0, 100.005, client_id="a"), "сетке цены"),  # тик 0.01
    (lambda c: c.stop_loss("SOLUSDT", "SELL", 98.123, client_id="a", qty=1.0), "сетке цены"),
    (lambda c: c.stop_loss("SOLUSDT", "SELL", 98.0, client_id="a", qty=0.05), "сетке лота"),
])
def test_invalid_orders_are_refused_before_any_request(call, expect):
    async def scenario(c, s):
        with pytest.raises(OrderValidationError, match=expect):
            await call(c)
        return len(s.seen)

    assert run_client(scenario) == 0


def test_unknown_symbol_filters_stop_the_order():
    async def scenario(c, s):
        with pytest.raises(ConfigError):
            await c.market_order("BTCUSDT", "BUY", 1.0, client_id="a", ref_price=100.0)
        return len(s.seen)

    assert run_client(scenario) == 0


def test_rounding_helpers_round_quantity_down_and_price_to_nearest_tick():
    async def scenario(c, s):
        return c.round_qty("SOLUSDT", 1.999), c.round_qty("SOLUSDT", 0.3), c.round_price("SOLUSDT", 100.004), c.round_price("SOLUSDT", 100.006)

    assert run_client(scenario) == (1.9, 0.3, 100.0, 100.01)  # 0.3 не превращается в 0.2999… (Decimal)


def test_client_id_is_deterministic_short_and_in_binance_format():
    a = client_id("tb", "E", "SMC_Pro_Trend_SOLUSDT_short_1760000000000")
    assert a == client_id("tb", "E", "SMC_Pro_Trend_SOLUSDT_short_1760000000000")
    assert a != client_id("tb", "S", "SMC_Pro_Trend_SOLUSDT_short_1760000000000") != client_id("tb", "S", "other")
    assert len(a) <= 36 and re.fullmatch(r"[\.A-Z\:/a-z0-9_-]{1,36}", a)
    with pytest.raises(OrderValidationError):
        client_id("tb", "bad role", "k")


# ------------------------------------------------------------------------------ счёт, плечо, PnL


def test_balance_parsing_and_errors():
    rows = [{"asset": "BNB", "balance": "1", "availableBalance": "1"},
            {"asset": "USDT", "balance": "123.45", "availableBalance": "100.5", "crossUnPnl": "-1.5"}]

    async def ok(c, s):
        return await c.get_balance()

    b = run_client(ok, replies={("GET", config.BALANCE_PATH): rows})
    assert (b.wallet, b.available, b.unrealized_pnl, b.margin_balance) == (123.45, 100.5, -1.5, 121.95)

    async def missing(c, s):
        with pytest.raises(DataError):
            await c.get_balance("USDC")
        with pytest.raises(DataError):
            await c.get_balance()

    run_client(missing, replies={("GET", config.BALANCE_PATH): rows[:1]})


def test_leverage_and_margin_type():
    async def scenario(c, s):
        assert await c.set_leverage("SOLUSDT", 10) == 10
        lev = s.last().params
        await c.set_margin_type("SOLUSDT", "ISOLATED")
        margin = s.last().params
        with pytest.raises(OrderValidationError):
            await c.set_leverage("SOLUSDT", 0)
        with pytest.raises(OrderValidationError):
            await c.set_margin_type("SOLUSDT", "WEIRD")
        return lev, margin

    lev, margin = run_client(scenario, replies={("POST", config.LEVERAGE_PATH): {"symbol": "SOLUSDT", "leverage": 10},
                                                ("POST", config.MARGIN_TYPE_PATH): {"code": 200, "msg": "success"}})
    assert lev["leverage"] == "10" and margin["marginType"] == "ISOLATED"


def test_margin_type_already_set_is_success_but_other_errors_raise():
    async def already(c, s):
        await c.set_margin_type("SOLUSDT")

    run_client(already, replies={("POST", config.MARGIN_TYPE_PATH): (400, {"code": -4046, "msg": "No need to change margin type."})})

    async def other(c, s):
        with pytest.raises(BinanceAPIError) as exc:
            await c.set_margin_type("SOLUSDT")
        assert exc.value.code == -4048

    run_client(other, replies={("POST", config.MARGIN_TYPE_PATH): (400, {"code": -4048, "msg": "Margin type cannot be changed."})})


def test_leverage_not_applied_by_the_exchange_is_an_error():
    async def scenario(c, s):
        with pytest.raises(DataError):
            await c.set_leverage("SOLUSDT", 10)

    run_client(scenario, replies={("POST", config.LEVERAGE_PATH): {"symbol": "SOLUSDT", "leverage": 5}})


def test_realized_pnl_sums_only_pnl_commission_and_funding():
    rows = [{"incomeType": "REALIZED_PNL", "income": "-12.5", "time": 1}, {"incomeType": "COMMISSION", "income": "-0.5", "time": 2},
            {"incomeType": "FUNDING_FEE", "income": "0.25", "time": 3}, {"incomeType": "TRANSFER", "income": "1000", "time": 4}]

    async def scenario(c, s):
        return await c.realized_pnl_since(1_700_000_000_000), s.last().params

    pnl, params = run_client(scenario, replies={("GET", config.INCOME_PATH): rows})
    assert pnl == -12.75 and params["startTime"] == "1700000000000" and params["limit"] == "1000"


def test_realized_pnl_paginates_and_never_undercounts(monkeypatch):
    monkeypatch.setattr(bl, "INCOME_PAGE", 2)
    pages = [[{"incomeType": "REALIZED_PNL", "income": "-1", "time": 10}, {"incomeType": "REALIZED_PNL", "income": "-2", "time": 20}],
             [{"incomeType": "COMMISSION", "income": "-3", "time": 30}]]

    async def scenario(c, s):
        calls = iter(pages)
        orig = c.rest.request

        async def paged(method, path, params=None, **kw):
            await orig(method, path, params, **kw)  # настоящий запрос (подпись), ответ подменяем страницами
            return next(calls)

        c.rest.request = paged
        return await c.realized_pnl_since(0), [s.seen[0].params["startTime"], s.seen[1].params["startTime"]]

    pnl, starts = run_client(scenario, replies={("GET", config.INCOME_PATH): []})
    assert pnl == -6 and starts == ["0", "21"]  # вторая страница начинается после последней записи первой

    monkeypatch.setattr(bl, "INCOME_MAX_PAGES", 2)

    async def endless(c, s):
        with pytest.raises(DataError, match="недосчитан"):
            await c.realized_pnl_since(0)

    full = [{"incomeType": "REALIZED_PNL", "income": "-1", "time": 1}, {"incomeType": "REALIZED_PNL", "income": "-1", "time": 2}]
    run_client(endless, replies={("GET", config.INCOME_PATH): full})


def test_position_amount_sums_rows_and_requires_the_symbol():
    async def scenario(c, s):
        assert await c.get_position_amt("SOLUSDT") == -3.5
        with pytest.raises(DataError):
            await c.get_position_amt("ADAUSDT")

    run_client(scenario, replies={("GET", config.POSITION_RISK_PATH): [{"symbol": "SOLUSDT", "positionAmt": "-3.5"},
                                                                       {"symbol": "XRPUSDT", "positionAmt": "9"}]})


def test_malformed_order_response_is_a_data_error():
    async def scenario(c, s):
        with pytest.raises(DataError):
            await c.market_order("SOLUSDT", "BUY", 1.0, client_id="a", ref_price=100.0)

    run_client(scenario, replies={("POST", config.ORDER_PATH): {"orderId": 1, "status": "FILLED"}})


def test_client_module_has_no_error_suppression_r1():
    src = open(bl.__file__, encoding="utf-8").read()
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", code)


# ---------------------------------------------------------------- позиция, mark price, algo-ордера, testnet


def test_position_mark_price_and_open_algo_orders_parsing():
    algo = {"clientAlgoId": "tbS0_x", "symbol": "SOLUSDT", "side": "SELL", "orderType": "STOP_MARKET", "triggerPrice": "98",
            "algoStatus": "NEW", "quantity": "1", "reduceOnly": True, "closePosition": False}

    async def scenario(c, s):
        pos = await c.get_position("SOLUSDT")
        mark = await c.get_mark_price("SOLUSDT")
        algos = await c.get_open_algo_orders("SOLUSDT")
        return pos, mark, algos, s.of("GET", config.PREMIUM_INDEX_PATH)[0], s.of("GET", config.OPEN_ALGO_ORDERS_PATH)[0]

    pos, mark, algos, premium_req, open_req = run_client(scenario, replies={
        ("GET", config.POSITION_RISK_PATH): [{"symbol": "SOLUSDT", "positionAmt": "-3.5", "entryPrice": "101.25"}],
        ("GET", config.PREMIUM_INDEX_PATH): {"symbol": "SOLUSDT", "markPrice": "100.50"}, ("GET", config.OPEN_ALGO_ORDERS_PATH): [algo]})
    assert (pos.amt, pos.entry_price) == (-3.5, 101.25) and mark == 100.5
    assert [(a.client_algo_id, a.order_type, a.trigger_price, a.qty, a.reduce_only) for a in algos] == [("tbS0_x", "STOP_MARKET", 98.0, 1.0, True)]
    assert "signature" not in premium_req.params and premium_req.params["symbol"] == "SOLUSDT"  # публичный запрос без подписи
    assert open_req.params["symbol"] == "SOLUSDT" and "timestamp" in open_req.params  # приватный: подписан (сервер проверил HMAC)


def test_client_uses_the_testnet_base_url_only_when_use_testnet_is_set(monkeypatch):
    monkeypatch.setenv("BINANCE_API_KEY", KEY)
    monkeypatch.setenv("BINANCE_API_SECRET", SECRET)

    async def base_url():
        async with aiohttp.ClientSession() as session:
            return BinanceLiveClient.from_env(session).rest._base_url

    monkeypatch.setattr(config, "LIVE_REST_URL", config.REST_BASE_URL)
    assert asyncio.run(base_url()) == config.REST_BASE_URL.rstrip("/")
    monkeypatch.setattr(config, "LIVE_REST_URL", config.TESTNET_REST_URL)
    assert asyncio.run(base_url()) == "https://testnet.binancefuture.com"


def test_use_testnet_flag_switches_only_the_order_url(monkeypatch):
    """config читает USE_TESTNET при импорте: проверяем в отдельном процессе. Данные Shadow (REST_BASE_URL) не затрагиваются."""
    import subprocess
    import sys
    from pathlib import Path

    root = Path(config.__file__).resolve().parent
    code = "import config; print(config.USE_TESTNET, config.LIVE_REST_URL, config.REST_BASE_URL)"
    import os

    def run_(env):
        full = {k: v for k, v in os.environ.items() if k not in ("USE_TESTNET", "BINANCE_REST_URL", "BINANCE_PROFILE")} | env
        return subprocess.run([sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, r'{root}'); {code}"], capture_output=True,
                              text=True, cwd=root, check=True, env=full).stdout.split()

    on = run_({"USE_TESTNET": "true", "BINANCE_PROFILE": "mainnet"})
    off = run_({"BINANCE_PROFILE": "mainnet", "USE_TESTNET": "false"})  # явно: иначе подтянется USE_TESTNET из локального .env
    assert on[:2] == ["True", "https://testnet.binancefuture.com"] and on[2] != on[1]  # данные - mainnet, ордера - testnet
    assert off[0] == "False" and off[1] == off[2]


def test_empty_position_risk_list_means_a_flat_position():
    """Binance отвечает [] на positionRisk?symbol=..., когда по символу позиции нет (так ответил testnet): это ноль, не ошибка."""
    async def scenario(c, s):
        pos = await c.get_position("BTCUSDT")
        return pos.amt, pos.entry_price, await c.get_position_amt("BTCUSDT")

    assert run_client(scenario, replies={("GET", config.POSITION_RISK_PATH): []}) == (0.0, 0.0, 0.0)


def test_position_risk_that_is_not_a_list_is_still_a_data_error():
    async def scenario(c, s):
        with pytest.raises(DataError):
            await c.get_position("BTCUSDT")

    run_client(scenario, replies={("GET", config.POSITION_RISK_PATH): {"code": -1}})
