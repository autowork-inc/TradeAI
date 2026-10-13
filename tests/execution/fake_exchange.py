"""Имитация Binance Futures для тестов боевого моста: REST + приватный WebSocket.

Что имитируется (и сверяется строго):
  * подпись HMAC-SHA256 и заголовок X-MBX-APIKEY (неверные -> 400/401 с кодами биржи);
  * окно recvWindow по timestamp (-1021), сдвиг часов биржи (time_offset_ms);
  * уникальность clientOrderId среди ОТКРЫТЫХ ордеров (как у Binance): MARKET исполняется мгновенно и
    закрывается, поэтому слепой повтор с тем же id создал бы второй ордер - ровно та ловушка, которую
    тесты идемпотентности обязаны ловить (exchange.orders / exchange.post_log это показывают);
  * отказы биржи (-1013, -2013, -2011, -1125), 429 с Retry-After;
  * сбои доставки: потеря ответа ПОСЛЕ исполнения (lost_response), обрыв ДО обработки (drop_before),
    5xx до/после обработки, медленный ответ.
"""

import asyncio
import hashlib
import hmac
import json
import time
from collections import defaultdict
from typing import Any, Callable

from aiohttp import web

API_KEY = "test-key"
API_SECRET = "test-secret"


class FakeExchange:
    def __init__(self, market_price: float = 60_000.0):
        self.market_price = market_price
        self.book: tuple[float, float] | None = None  # (bid, ask): включает проверку post-only (-5022)
        self.time_offset_ms = 0
        self.orders: list[dict] = []  # все ордера, включая закрытые
        self.trades: list[dict] = []
        self.positions: dict[str, float] = defaultdict(float)
        self.post_log: list[dict] = []  # параметры каждого POST /fapi/v1/order, дошедшего до обработчика
        self.requests: list[str] = []  # "METHOD path" всех запросов
        self.listen_keys: set[str] = set()
        self.listen_key_log: list[str] = []  # "POST" / "PUT" / "DELETE"
        self.ws_requests: list[dict] = []
        self.ws_clients: list[web.WebSocketResponse] = []
        self.ws_scripts: list[Callable] = []  # по сценарию на каждое подключение
        self.faults: dict[str, list[str]] = defaultdict(list)  # разовые сбои
        self.always: dict[str, str] = {}  # постоянные сбои
        self.hooks: dict[str, Callable] = {}
        self.hedge_positions: list[dict] = []
        self._order_seq = 1000
        self._trade_seq = 5000
        self._key_seq = 0
        self.app = self._build_app()

    # ----------------------------------------------------------------- сценарии

    def fault(self, name: str, kind: str, times: int = 1) -> None:
        self.faults[name].extend([kind] * times)

    def seed_position(self, symbol: str, qty: float) -> None:
        self.positions[symbol] = qty

    def seed_order(self, cid, symbol="BTCUSDT", side="BUY", qty=0.5, price=59_000.0, type_="LIMIT",
                   executed=0.0, status="NEW") -> dict:
        self._order_seq += 1
        order = {"orderId": self._order_seq, "cid": cid, "symbol": symbol, "side": side, "type": type_, "qty": qty,
                 "price": price, "executed": executed, "status": status, "reduce_only": False,
                 "updateTime": self.now_ms()}
        self.orders.append(order)
        return order

    def order(self, cid: str) -> dict | None:
        found = [o for o in self.orders if o["cid"] == cid]
        return found[-1] if found else None

    def orders_by_cid(self, cid: str) -> list[dict]:
        return [o for o in self.orders if o["cid"] == cid]

    def set_status(self, cid: str, status: str) -> None:
        o = self.order(cid)
        o["status"], o["updateTime"] = status, self.now_ms()

    async def fill(self, cid: str, qty: float, price: float | None = None, push: bool = True,
                   commission_asset: str = "USDT", maker: bool = True) -> dict:
        """Исполняет (часть) живого ордера: сделка, позиция, событие в приватный поток."""
        o = self.order(cid)
        trade = self._book_trade(o, qty, price if price is not None else o["price"], maker, commission_asset)
        o["status"] = "FILLED" if o["executed"] >= o["qty"] - 1e-12 else "PARTIALLY_FILLED"
        if push:
            await self.push(self.order_event(o, trade, "TRADE"))
        return trade

    async def push(self, event: dict) -> None:
        for ws in list(self.ws_clients):
            await ws.send_str(json.dumps(event))

    async def wait_clients(self, n: int = 1, timeout: float = 3.0) -> None:
        deadline = time.monotonic() + timeout
        while len(self.ws_clients) < n:
            if time.monotonic() > deadline:
                raise TimeoutError(f"к приватному потоку подключено {len(self.ws_clients)} из {n}")
            await asyncio.sleep(0.01)

    async def close_clients(self) -> None:
        for ws in list(self.ws_clients):
            await ws.close()

    # ---------------------------------------------------------------- события

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    def order_event(self, o: dict, trade: dict | None = None, exec_type: str = "NEW") -> dict:
        now = self.now_ms()
        body = {
            "s": o["symbol"], "c": o["cid"], "S": o["side"], "o": o["type"], "f": "GTC", "q": str(o["qty"]),
            "p": str(o["price"]), "ap": "0", "x": exec_type, "X": o["status"], "i": o["orderId"],
            "l": str(trade["qty"]) if trade else "0", "z": str(o["executed"]),
            "L": str(trade["price"]) if trade else "0", "N": "USDT", "T": now,
            "t": trade["id"] if trade else 0, "m": bool(trade and trade["maker"]), "R": o["reduce_only"], "ps": "BOTH",
        }
        if trade:
            body["n"], body["N"] = str(trade["commission"]), trade["commissionAsset"]
        return {"e": "ORDER_TRADE_UPDATE", "T": now, "E": now, "o": body}

    def account_event(self, symbol: str, amount: float, reason: str = "ORDER") -> dict:
        now = self.now_ms()
        return {"e": "ACCOUNT_UPDATE", "T": now, "E": now, "a": {
            "m": reason, "B": [{"a": "USDT", "wb": "1000.0", "cw": "1000.0", "bc": "0"}],
            "P": [{"s": symbol, "pa": str(amount), "ep": "60000.0", "cr": "0", "up": "0", "mt": "cross",
                   "iw": "0", "ps": "BOTH", "ma": "USDT"}]}}

    # ------------------------------------------------------------------ приложение

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/fapi/v1/time", self._time)
        app.router.add_post("/fapi/v1/order", self._order_post)
        app.router.add_get("/fapi/v1/order", self._order_get)
        app.router.add_delete("/fapi/v1/order", self._order_delete)
        app.router.add_get("/fapi/v1/openOrders", self._open_orders)
        app.router.add_get("/fapi/v3/positionRisk", self._position_risk)
        app.router.add_get("/fapi/v1/userTrades", self._user_trades)
        app.router.add_post("/fapi/v1/listenKey", self._listen_key)
        app.router.add_put("/fapi/v1/listenKey", self._listen_key)
        app.router.add_delete("/fapi/v1/listenKey", self._listen_key)
        app.router.add_get("/private/ws", self._ws)
        return app

    @staticmethod
    def _err(status: int, code: int, msg: str, headers: dict | None = None) -> web.Response:
        return web.json_response({"code": code, "msg": msg}, status=status, headers=headers)

    def _take(self, name: str) -> str | None:
        if name in self.always:
            return self.always[name]
        queue = self.faults.get(name)
        return queue.pop(0) if queue else None

    def _auth(self, request: web.Request, signed: bool = True):
        """-> (params, None) или (None, Response с ошибкой)."""
        self.requests.append(f"{request.method} {request.path}")
        if request.headers.get("X-MBX-APIKEY") != API_KEY:
            return None, self._err(401, -2015, "Invalid API-key, IP, or permissions for action.")
        params = dict(request.query)
        if not signed:
            return params, None
        raw = request.rel_url.raw_query_string
        base, _, signature = raw.rpartition("&signature=")
        expected = hmac.new(API_SECRET.encode(), base.encode(), hashlib.sha256).hexdigest()
        if not base or not hmac.compare_digest(signature, expected):
            return None, self._err(400, -1022, "Signature for this request is not valid.")
        ts, recv = int(params["timestamp"]), int(params.get("recvWindow", 5000))
        server = self.now_ms()
        if ts > server + 1000 or server - ts > recv:
            return None, self._err(400, -1021, "Timestamp for this request is outside of the recvWindow.")
        return params, None

    async def _time(self, request):
        self.requests.append("GET /fapi/v1/time")
        return web.json_response({"serverTime": self.now_ms()})

    # ------------------------------------------------------------------- ордера

    def _book_trade(self, o, qty, price, maker, asset="USDT") -> dict:
        self._trade_seq += 1
        commission = qty * price * (0.0002 if maker else 0.0005)
        trade = {"id": self._trade_seq, "orderId": o["orderId"], "symbol": o["symbol"], "side": o["side"],
                 "price": str(price), "qty": str(qty), "commission": str(commission), "commissionAsset": asset,
                 "maker": maker, "time": self.now_ms()}
        self.trades.append(trade)
        o["executed"] += qty
        o["updateTime"] = trade["time"]
        self.positions[o["symbol"]] += qty if o["side"] == "BUY" else -qty
        return trade

    @staticmethod
    def _snapshot(o: dict) -> dict:
        return {"orderId": o["orderId"], "clientOrderId": o["cid"], "symbol": o["symbol"], "status": o["status"],
                "side": o["side"], "type": o["type"], "origQty": str(o["qty"]), "executedQty": str(o["executed"]),
                "price": str(o["price"]), "avgPrice": str(o["price"]), "updateTime": o["updateTime"],
                "reduceOnly": o["reduce_only"]}

    def _process_order(self, p: dict) -> web.Response:
        if "pre_order" in self.hooks:
            self.hooks["pre_order"](p)
        cid = p.get("newClientOrderId", "")
        if any(o["cid"] == cid and o["status"] in ("NEW", "PARTIALLY_FILLED") for o in self.orders):
            return self._err(400, -4116, "ClientOrderId is duplicated.")  # уникальность - среди ОТКРЫТЫХ
        if p.get("type") not in ("MARKET", "LIMIT") or p.get("side") not in ("BUY", "SELL"):
            return self._err(400, -1116, "Invalid orderType.")
        qty = float(p["quantity"])
        if qty < 0.001 and "reject_small" not in self.always:
            return self._err(400, -1013, "Filter failure: LOT_SIZE")
        if p["type"] == "LIMIT" and p.get("timeInForce") == "GTX" and self.book is not None:
            bid, ask = self.book  # post-only, который сразу исполнился бы как тейкер, биржа не принимает
            price = float(p["price"])
            if (p["side"] == "BUY" and price >= ask) or (p["side"] == "SELL" and price <= bid):
                return self._err(400, -5022, "Due to the Post Only setting, Order would be immediately and entirely filled.")
        self._order_seq += 1
        o = {"orderId": self._order_seq, "cid": cid, "symbol": p["symbol"], "side": p["side"], "type": p["type"],
             "qty": qty, "price": float(p.get("price", 0.0)), "executed": 0.0, "status": "NEW",
             "reduce_only": p.get("reduceOnly") == "true", "updateTime": self.now_ms()}
        self.orders.append(o)
        if o["type"] == "MARKET":
            self._book_trade(o, qty, self.market_price, maker=False)
            o["status"] = "FILLED"
        return web.json_response(self._snapshot(o))

    async def _order_post(self, request):
        params, error = self._auth(request)
        if error:
            return error
        self.post_log.append(params)
        fault = self._take("order_post")
        if fault == "http429":
            return self._err(429, -1003, "Too many requests", {"Retry-After": "30"})
        if fault == "drop_before":
            request.transport.abort()
            return web.Response()
        if fault == "http500_before":
            return self._err(500, -1000, "Internal error")
        response = self._process_order(params)
        if fault == "lost_response":
            request.transport.abort()  # ордер создан, ответ потерян
            return web.Response()
        if fault == "http500_after":
            return self._err(503, -1007, "Timeout waiting for response from backend server. Send status unknown; execution status unknown.")
        if fault == "slow_after":
            await asyncio.sleep(0.6)
        return response

    async def _order_get(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("order_get")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        if fault == "http429":
            return self._err(429, -1003, "Too many requests", {"Retry-After": "7"})
        found = self.orders_by_cid(params.get("origClientOrderId", ""))
        if not found:
            return self._err(400, -2013, "Order does not exist.")
        return web.json_response(self._snapshot(found[-1]))

    async def _order_delete(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("order_delete")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        open_ = [o for o in self.orders_by_cid(params.get("origClientOrderId", "")) if o["status"] in ("NEW", "PARTIALLY_FILLED")]
        if not open_:
            return self._err(400, -2011, "Unknown order sent.")
        o = open_[-1]
        o["status"], o["updateTime"] = "CANCELED", self.now_ms()
        await self.push(self.order_event(o, exec_type="CANCELED"))
        return web.json_response(self._snapshot(o))

    async def _open_orders(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("open_orders")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        if fault == "http500":
            return self._err(500, -1000, "Internal error")
        symbol = params.get("symbol")
        live = [self._snapshot(o) for o in self.orders
                if o["status"] in ("NEW", "PARTIALLY_FILLED") and (symbol is None or o["symbol"] == symbol)]
        return web.json_response(live)

    async def _position_risk(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("position_risk")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        if fault == "http429":
            return self._err(429, -1003, "Too many requests", {"Retry-After": "45"})
        if fault == "bad_key":
            return self._err(401, -2015, "Invalid API-key, IP, or permissions for action.")
        rows = [{"symbol": s, "positionAmt": str(q), "positionSide": "BOTH", "entryPrice": "60000.0", "markPrice": "60000.0",
                 "unRealizedProfit": "0", "updateTime": self.now_ms()} for s, q in self.positions.items() if q != 0.0]
        rows.extend(self.hedge_positions)
        return web.json_response(rows)

    async def _user_trades(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("user_trades")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        order_id = params.get("orderId")
        rows = [t for t in self.trades if t["symbol"] == params["symbol"] and (order_id is None or str(t["orderId"]) == order_id)]
        return web.json_response(rows)

    # ---------------------------------------------------------- приватный поток

    async def _listen_key(self, request):
        params, error = self._auth(request, signed=False)
        if error:
            return error
        self.listen_key_log.append(request.method)
        if request.method == "POST":
            fault = self._take("listen_key_post")
            if fault == "http429":
                return self._err(429, -1003, "Too many requests", {"Retry-After": "11"})
            if fault == "http500":
                return self._err(500, -1000, "Internal error")
            self._key_seq += 1
            key = f"lk{self._key_seq}"
            self.listen_keys.add(key)
            return web.json_response({"listenKey": key})
        key = params.get("listenKey")
        if key not in self.listen_keys:
            return self._err(400, -1125, "This listenKey does not exist.")
        if request.method == "DELETE":
            self.listen_keys.discard(key)
        return web.json_response({"listenKey": key} if request.method == "PUT" else {})

    async def _ws(self, request):
        self.ws_requests.append(dict(request.query))
        if request.query.get("listenKey") not in self.listen_keys:
            return web.Response(status=400, text="bad listenKey")
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.ws_clients.append(ws)
        try:
            if self.ws_scripts:
                await self.ws_scripts.pop(0)(ws)
            async for _ in ws:
                pass
        finally:
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)
        return ws


def event_dumps(event: Any) -> str:
    return json.dumps(event)
