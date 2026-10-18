"""Имитация приватного REST Binance для тестов Live Execution Engine (подставляется вместо SignedRest).

Не HTTP: у `BinanceLiveClient` один канал к бирже - `rest.request(method, path, params)`, поэтому имитация реализует его с
состоянием «как у биржи»: книга ордеров по clientOrderId, algo-ордера, позиция, журнал начислений (income), баланс. Настоящий
клиент строит и проверяет запросы - в тестах движка они проходят ту же локальную валидацию, что и в бою.
Внедрение сбоев: `fail[(метод, путь)] = исключение | [исключения...]` (поднимается один раз на вызов).
"""

from typing import Any

import config
from execution.rest import BinanceAPIError
from utils.errors import NetworkError


def api_error(code: int, msg: str = "error") -> BinanceAPIError:
    return BinanceAPIError(400, "fake", f'{{"code":{code}}}', code, msg)


class FakeRest:
    def __init__(self, price: float = 100.0):
        self.price = price
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.orders: dict[str, dict[str, Any]] = {}
        self.algos: dict[str, dict[str, Any]] = {}
        self.positions: dict[str, float] = {}
        self.entry_prices: dict[str, float] = {}
        self.hide_avg_price = False  # как testnet: в RESULT-ответе MARKET нет avgPrice (приходит 0)
        self.fills: list[dict[str, Any]] = []  # все исполнения: symbol, side, qty, price, via (market | limit | cid algo-ордера)
        self.strict_immediate = True  # algo-ордер, который сработал бы сразу, отвергается с -2021
        self.income: list[dict[str, Any]] = []
        self.available = 1000.0
        self.fail: dict[tuple[str, str], Any] = {}
        self.limit_status = "NEW"  # статус новой лимитки: NEW | EXPIRED (GTX-отказ приходит как статус) …
        self.margin_type_error: BinanceAPIError | None = None
        self.lose_next_order_response = False  # ордер дошёл до биржи и принят, но ответ потерян (NetworkError у клиента)
        self.filters: dict[str, Any] = {}  # для GET exchangeInfo: symbol -> SymbolFilters
        self._next_id = 1
        self.now_ms = 0  # время исполнений для userTrades (тесты сверки после рестарта двигают его сами)
        self.commission_rate = 0.0005
        self.commission_asset = "USDT"

    # ------------------------------------------------------------------------------ вспомогательное

    def calls(self, method: str, path: str) -> list[dict[str, Any]]:
        return [p for m, pa, p in self.requests if (m, pa) == (method, path)]

    def names(self) -> list[str]:
        """Последовательность вызовов в читаемом виде (для проверки порядка)."""
        short = {config.ORDER_PATH: "order", config.ALGO_ORDER_PATH: "algo", config.LEVERAGE_PATH: "leverage",
                 config.MARGIN_TYPE_PATH: "margin", config.INCOME_PATH: "income", config.BALANCE_PATH: "balance",
                 config.POSITION_RISK_PATH: "position", config.PREMIUM_INDEX_PATH: "mark",
                 config.OPEN_ALGO_ORDERS_PATH: "open_algo"}
        out = []
        for m, path, p in self.requests:
            kind = p.get("type") if path in (config.ORDER_PATH, config.ALGO_ORDER_PATH) and m == "POST" else ""
            out.append(f"{m} {short.get(path, path)}{':' + kind if kind else ''}")
        return out

    def add_income(self, kind: str, value: float, time_ms: int = 1) -> None:
        self.income.append({"incomeType": kind, "income": str(value), "time": time_ms})

    def fill_limit(self, cid: str, qty: float | None = None) -> None:
        """Лимитка исполнилась (полностью или частично) по своей цене."""
        o = self.orders[cid]
        q = float(o["origQty"]) if qty is None else qty
        o["executedQty"], o["avgPrice"] = str(q), o["price"]
        o["status"] = "FILLED" if q >= float(o["origQty"]) else "PARTIALLY_FILLED"
        self._move(o["symbol"], o["side"], q, float(o["price"]), "limit")

    def trigger(self, cid: str) -> None:
        """Сработал algo-ордер (SL или TP) по текущей цене: позиция уменьшена (reduceOnly), ордер FINISHED."""
        a = self.algos[cid]
        pos = self.positions.get(a["symbol"], 0.0)
        closes = pos != 0 and (pos > 0) == (a["side"] == "SELL")
        if not closes:
            a["algoStatus"] = "EXPIRED"
            return
        qty = min(abs(pos), float(a["quantity"] or abs(pos)))
        a["algoStatus"], a["actualPrice"], a["actualQty"] = "FINISHED", str(self.price), str(qty)
        self._move(a["symbol"], a["side"], qty, self.price, cid)

    def set_price(self, price: float) -> None:
        """Рынок пришёл в `price`: сначала срабатывают стопы (худший сценарий), затем тейки; исполнение - по этой цене."""
        self.price = price
        for kind in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            for cid, a in list(self.algos.items()):
                if a["algoStatus"] != "NEW" or a["orderType"] != kind:
                    continue
                trig, sell = float(a["triggerPrice"]), a["side"] == "SELL"
                hit = (price <= trig if sell else price >= trig) if kind == "STOP_MARKET" else (price >= trig if sell else price <= trig)
                if hit:
                    self.trigger(cid)

    def _move(self, symbol: str, side: str, qty: float, price: float, via: str) -> None:
        old = self.positions.get(symbol, 0.0)
        new = round(old + (qty if side == "BUY" else -qty), 10)
        opens = abs(new) > abs(old)
        if opens:  # позиция растёт: средняя цена входа
            self.entry_prices[symbol] = (self.entry_prices.get(symbol, 0.0) * abs(old) + price * (abs(new) - abs(old))) / abs(new)
        if new == 0:
            self.entry_prices[symbol] = 0.0
        self.positions[symbol] = new
        self.fills.append({"symbol": symbol, "side": side, "qty": qty, "price": price, "via": via, "opens": opens, "time": self.now_ms})

    # --------------------------------------------------------------------------------------- запрос

    async def request(self, method: str, path: str, params: dict[str, Any] | None = None, *, signed: bool = True, trace=None) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        self.requests.append((method, path, params))
        pending = self.fail.get((method, path))
        if pending:
            exc = pending.pop(0) if isinstance(pending, list) else pending
            if not isinstance(pending, list) or not pending:
                self.fail.pop((method, path), None)
            if exc is not None:  # None в списке = этот вызов проходит штатно
                raise exc
        handler = {
            ("GET", config.BALANCE_PATH): self._balance, ("GET", config.INCOME_PATH): lambda p: list(self.income),
            ("GET", config.POSITION_RISK_PATH): self._position_risk, ("POST", config.LEVERAGE_PATH): self._leverage,
            ("POST", config.MARGIN_TYPE_PATH): self._margin_type, ("POST", config.ORDER_PATH): self._place_order,
            ("GET", config.ORDER_PATH): self._get_order, ("DELETE", config.ORDER_PATH): self._cancel_order,
            ("POST", config.ALGO_ORDER_PATH): self._place_algo, ("GET", config.ALGO_ORDER_PATH): self._get_algo,
            ("DELETE", config.ALGO_ORDER_PATH): self._cancel_algo, ("GET", config.EXCHANGE_INFO_PATH): self._exchange_info,
            ("GET", config.PREMIUM_INDEX_PATH): self._premium, ("GET", config.OPEN_ALGO_ORDERS_PATH): self._open_algos,
            ("GET", config.USER_TRADES_PATH): self._user_trades,
        }.get((method, path))
        if handler is None:
            raise AssertionError(f"FakeRest: неожиданный запрос {method} {path}")
        return handler(params)

    async def sync_time(self) -> int:
        return 0

    def _exchange_info(self, p):
        return {"symbols": [{"symbol": f.symbol, "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": str(f.tick_size)},
            {"filterType": "LOT_SIZE", "stepSize": str(f.step_size), "minQty": str(f.min_qty)},
            {"filterType": "MIN_NOTIONAL", "notional": str(f.min_notional)}]} for f in self.filters.values()]}

    def _balance(self, p):
        return [{"asset": "USDT", "balance": str(self.available), "availableBalance": str(self.available), "crossUnPnl": "0"}]

    def _position_risk(self, p):
        return [{"symbol": p["symbol"], "positionAmt": str(self.positions.get(p["symbol"], 0.0)),
                 "entryPrice": str(self.entry_prices.get(p["symbol"], 0.0))}]

    def _user_trades(self, p):
        return [{"symbol": f["symbol"], "side": f["side"], "qty": str(f["qty"]), "price": str(f["price"]), "time": f["time"],
                 "commission": str(self.commission_rate * f["qty"] * f["price"]), "commissionAsset": self.commission_asset, "realizedPnl": "0"}
                for f in self.fills if f["symbol"] == p["symbol"] and p["startTime"] <= f["time"] <= p["endTime"]]

    def _premium(self, p):
        return {"symbol": p["symbol"], "markPrice": str(self.price)}

    def _open_algos(self, p):
        return [{"clientAlgoId": a["clientAlgoId"], "symbol": a["symbol"], "side": a["side"], "orderType": a["orderType"],
                 "triggerPrice": a["triggerPrice"], "algoStatus": a["algoStatus"], "quantity": a["quantity"] or "",
                 "reduceOnly": bool(a["reduceOnly"]), "closePosition": bool(a.get("closePosition"))}
                for a in self.algos.values() if a["algoStatus"] == "NEW" and a["symbol"] == p.get("symbol", a["symbol"])]

    def _leverage(self, p):
        return {"symbol": p["symbol"], "leverage": int(p["leverage"])}

    def _margin_type(self, p):
        if self.margin_type_error is not None:
            raise self.margin_type_error
        return {"code": 200, "msg": "success"}

    def _place_order(self, p):
        cid = p["newClientOrderId"]
        if cid in self.orders:
            raise api_error(-4116, "ClientOrderId is duplicated.")
        qty = float(p["quantity"])
        o = {"orderId": self._next_id, "symbol": p["symbol"], "clientOrderId": cid, "side": p["side"], "type": p["type"],
             "origQty": p["quantity"], "executedQty": "0", "avgPrice": "0", "price": p.get("price", "0"), "status": "NEW",
             "reduceOnly": p.get("reduceOnly", False), "timeInForce": p.get("timeInForce")}
        self._next_id += 1
        self.orders[cid] = o
        if p["type"] == "MARKET":
            pos = self.positions.get(p["symbol"], 0.0)
            if p.get("reduceOnly"):  # reduce-only не увеличивает позицию
                qty = min(qty, abs(pos)) if (pos > 0) == (p["side"] == "SELL") else 0.0
            o["executedQty"], o["avgPrice"], o["status"] = str(qty), str(self.price), "FILLED" if qty else "EXPIRED"
            if self.hide_avg_price:
                o["avgPrice"] = "0"
            if qty:
                self._move(p["symbol"], p["side"], qty, self.price, "market")
        else:
            o["status"] = self.limit_status
        if self.lose_next_order_response:
            self.lose_next_order_response = False
            raise network_error()
        return dict(o)

    def _get_order(self, p):
        o = self.orders.get(p["origClientOrderId"])
        if o is None:
            raise api_error(-2013, "Order does not exist.")
        return dict(o)

    def _cancel_order(self, p):
        o = self.orders.get(p["origClientOrderId"])
        if o is None or o["status"] not in ("NEW", "PARTIALLY_FILLED"):
            raise api_error(-2011, "Unknown order sent.")
        o["status"] = "CANCELED"
        return dict(o)

    def _place_algo(self, p):
        cid = p["clientAlgoId"]
        if cid in self.algos:
            raise api_error(-4116, "ClientAlgoId is duplicated.")
        trig, sell = float(p["triggerPrice"]), p["side"] == "SELL"
        immediate = ((trig >= self.price) if sell else (trig <= self.price)) if p["type"] == "STOP_MARKET" else (
            (trig <= self.price) if sell else (trig >= self.price))
        if self.strict_immediate and immediate:
            raise api_error(-2021, "Order would immediately trigger.")
        a = {"algoId": self._next_id, "clientAlgoId": cid, "algoStatus": "NEW", "orderType": p["type"], "symbol": p["symbol"],
             "side": p["side"], "triggerPrice": p["triggerPrice"], "reduceOnly": p.get("reduceOnly"), "quantity": p.get("quantity"),
             "closePosition": p.get("closePosition")}
        self._next_id += 1
        self.algos[cid] = a
        return dict(a)

    def _get_algo(self, p):
        a = self.algos.get(p["clientAlgoId"])
        if a is None:
            raise api_error(-2013, "Algo order does not exist.")
        return dict(a)

    def _cancel_algo(self, p):
        a = self.algos.get(p["clientAlgoId"])
        if a is None or a["algoStatus"] != "NEW":
            raise api_error(-2011, "Unknown algo order.")
        a["algoStatus"] = "CANCELED"
        return {"algoId": a["algoId"], "clientAlgoId": a["clientAlgoId"], "code": "200", "msg": "success"}


def network_error() -> NetworkError:
    return NetworkError("fake: connection reset")
