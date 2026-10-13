"""FakeExchange + Algo Service: POST/DELETE/GET /fapi/v1/algoOrder, GET /fapi/v1/openAlgoOrders, событие ALGO_UPDATE.

Формы запросов и ответов сверены с документацией Binance (New / Cancel / Query Algo Order): параметры algoType, type,
triggerPrice, closePosition, workingType, clientAlgoId; ответ - algoId, clientAlgoId, algoStatus, actualOrderId.
Событие ALGO_UPDATE: поля o.caid, aid, at, o, s, S, ps, q, X, ai, ap, aq, tp, wt, cp, R, rm.

Правила, которые имитируются (и ловят ошибки бота):
  * clientAlgoId - формат ^[\\.A-Z\\:/a-z0-9_-]{1,36}$ и уникальность среди живых (NEW);
  * второй closePosition-стоп (STOP) или тейк (TAKE_PROFIT) в том же направлении по символу - отказ -4130
    (правило classic futures; бот closePosition больше не использует - О18, но правило оставлено на случай чужих ордеров);
  * триггер уже «за ценой» - отказ -2021 (STOP_MARKET SELL: цена <= триггера; BUY: цена >= триггера; для тейка наоборот);
  * closePosition / reduceOnly без позиции нужного знака - отказ -2022;
  * strict_reduce_sum=True: сумма объёмов открытых reduceOnly-ордеров в одну сторону больше позиции - отказ -2022. По умолчанию
    выключено: что делает настоящая биржа, ❓ неизвестно (проверяет scripts/test_algo_live.py), тесты покрывают оба варианта;
  * algo_reject = (код, текст) + fault "reject": отказ следующего POST с заданным кодом;
  * срабатывание (trigger_algo) порождает MARKET-ордер в «движке» с id, НЕ связанным с clientAlgoId (худший случай:
    в документации id порождённого ордера не описан), и порядок событий задаётся сценарием, в т.ч. исполнение
    раньше ALGO_UPDATE или вовсе без него (бот должен опознать исполнение по REST);
  * при закрытии позиции оставшиеся closePosition-ордера символа получают EXPIRED (документация: «system canceled»).
"""

import asyncio
import json
import re
from collections.abc import Sequence

from aiohttp import web

from tests.execution.fake_exchange import FakeExchange

CLIENT_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")
NORMAL_SEQUENCE = ("a_triggering", "a_triggered", "trade", "a_finished")


class FakeAlgoExchange(FakeExchange):
    def __init__(self, market_price: float = 60_000.0):
        self.algo_orders: list[dict] = []
        self.algo_post_log: list[dict] = []
        self.algo_requests: list[str] = []  # "METHOD clientAlgoId" в порядке прихода: порядок DELETE -> POST виден
        self.algo_cancel_unknown_code = -2011
        self.algo_get_unknown_code = -2013
        self.strict_reduce_sum = False
        self.algo_reject: tuple[int, str] = (-4164, "Order's notional must be no smaller than 100")
        self._algo_seq = 7000
        self.pushed: list[dict] = []  # все события приватного потока в порядке отправки (тесты доставляют их вручную)
        super().__init__(market_price)

    async def push(self, event: dict) -> None:
        self.pushed.append(event)
        await super().push(event)

    # ---------------------------------------------------------------- приложение

    def _build_app(self) -> web.Application:
        app = super()._build_app()
        app.router.add_post("/fapi/v1/algoOrder", self._algo_post)
        app.router.add_delete("/fapi/v1/algoOrder", self._algo_delete)
        app.router.add_get("/fapi/v1/algoOrder", self._algo_get)
        app.router.add_get("/fapi/v1/openAlgoOrders", self._algo_open)
        app.router.add_get("/fapi/v1/exchangeInfo", self._exchange_info)
        app.router.add_get("/fapi/v1/premiumIndex", self._premium_index)
        return app

    # публичные данные для scripts/test_algo_live.py
    exchange_filters = {"BTCUSDT": ("0.10", "0.001", "0.001", "100"), "ETHUSDT": ("0.01", "0.001", "0.001", "20")}

    async def _exchange_info(self, request):
        self.requests.append("GET /fapi/v1/exchangeInfo")
        return web.json_response({"symbols": [
            {"symbol": sym, "filters": [{"filterType": "PRICE_FILTER", "tickSize": tick},
                                        {"filterType": "LOT_SIZE", "stepSize": step, "minQty": min_qty},
                                        {"filterType": "MIN_NOTIONAL", "notional": notional}]}
            for sym, (tick, step, min_qty, notional) in self.exchange_filters.items()]})

    async def _premium_index(self, request):
        self.requests.append("GET /fapi/v1/premiumIndex")
        return web.json_response({"symbol": request.query.get("symbol"), "markPrice": str(self.market_price)})

    # ------------------------------------------------------------------- события

    def algo(self, caid: str) -> dict | None:
        found = [a for a in self.algo_orders if a["cid"] == caid]
        return found[-1] if found else None

    def algo_event(self, a: dict, status: str, *, ai: int | None = None) -> dict:
        now = self.now_ms()
        return {"e": "ALGO_UPDATE", "T": now, "E": now, "o": {
            "caid": a["cid"], "aid": a["algoId"], "at": "CONDITIONAL", "o": a["type"], "s": a["symbol"], "S": a["side"],
            "ps": "BOTH", "f": "GTE_GTC", "q": str(a["quantity"]), "X": status, "ai": "" if ai is None else str(ai),
            "ap": "0", "aq": "0", "act": "MARKET" if ai else "", "tp": str(a["trigger"]), "p": "0", "V": "NONE",
            "wt": a["working"], "pm": "NONE", "cp": a["close_position"], "pP": False, "R": a["reduce_only"], "tt": 0,
            "gtd": 0, "rm": ""}}

    def _snapshot_algo(self, a: dict) -> dict:
        return {"algoId": a["algoId"], "clientAlgoId": a["cid"], "algoType": "CONDITIONAL", "orderType": a["type"],
                "symbol": a["symbol"], "side": a["side"], "positionSide": "BOTH", "timeInForce": "GTE_GTC",
                "quantity": str(a["quantity"]), "algoStatus": a["status"], "triggerPrice": str(a["trigger"]),
                "price": "0", "workingType": a["working"], "closePosition": a["close_position"], "priceProtect": False,
                "reduceOnly": a["reduce_only"], "createTime": a["createTime"], "updateTime": a["updateTime"],
                "triggerTime": a["triggerTime"], "goodTillDate": 0,
                "actualOrderId": "" if a["actual_order_id"] is None else str(a["actual_order_id"])}

    # ----------------------------------------------------------------- сценарии

    def seed_algo(self, cid: str, symbol="BTCUSDT", side="SELL", type_="STOP_MARKET", trigger=59_000.0, status="NEW",
                  quantity=0.0, close_position=True, reduce_only=False) -> dict:
        self._algo_seq += 1
        a = {"algoId": self._algo_seq, "cid": cid, "symbol": symbol, "side": side, "type": type_, "trigger": trigger,
             "quantity": quantity, "close_position": close_position, "reduce_only": reduce_only, "working": "CONTRACT_PRICE", "status": status,
             "actual_order_id": None, "createTime": self.now_ms(), "updateTime": self.now_ms(), "triggerTime": 0}
        self.algo_orders.append(a)
        return a

    async def trigger_algo(self, caid: str, price: float | None = None, sequence: Sequence[str] = NORMAL_SEQUENCE,
                           child_cid: str | None = None, expire_siblings: bool = True) -> dict:
        """Срабатывание: порождённый MARKET-ордер закрывает позицию. sequence - какие события и в каком порядке уходят
        в приватный поток (REST-снимок при этом всегда показывает итог)."""
        a = self.algo(caid)
        px = price if price is not None else a["trigger"]
        self._order_seq += 1
        position = abs(self.positions[a["symbol"]])
        qty = position if a["close_position"] else min(a["quantity"], position)  # reduceOnly: больше позиции не исполнится
        child = {"orderId": self._order_seq, "cid": child_cid or f"x{self._order_seq}", "symbol": a["symbol"], "side": a["side"],
                 "type": "MARKET", "qty": qty, "price": 0.0, "executed": 0.0, "status": "NEW", "reduce_only": True,
                 "updateTime": self.now_ms()}
        self.orders.append(child)
        trade = self._book_trade(child, qty, px, maker=False)
        child["status"] = "FILLED"
        a.update(status="FINISHED", actual_order_id=child["orderId"], updateTime=self.now_ms(), triggerTime=self.now_ms())
        events = {
            "a_triggering": lambda: self.algo_event(a, "TRIGGERING"),
            "a_triggered": lambda: self.algo_event(a, "TRIGGERED", ai=child["orderId"]),
            "trade": lambda: self.order_event(child, trade, "TRADE"),
            "a_finished": lambda: self.algo_event(a, "FINISHED", ai=child["orderId"]),
        }
        for name in sequence:
            await self.push(events[name]())
        if expire_siblings and abs(self.positions[a["symbol"]]) < 1e-12:
            for s in self.algo_orders:
                if s is not a and s["symbol"] == a["symbol"] and s["status"] == "NEW" and (s["close_position"] or s["reduce_only"]):
                    s.update(status="EXPIRED", updateTime=self.now_ms())
                    await self.push(self.algo_event(s, "EXPIRED"))
        return child

    # ------------------------------------------------------------------ обработчики

    @staticmethod
    def _category(type_: str) -> str:
        return "tp" if type_.startswith("TAKE_PROFIT") else "stop"

    def _validate_algo(self, p: dict) -> web.Response | None:
        if p.get("algoType") != "CONDITIONAL":
            return self._err(400, -1116, "Invalid algoType.")
        if p.get("type") not in ("STOP_MARKET", "TAKE_PROFIT_MARKET") or p.get("side") not in ("BUY", "SELL"):
            return self._err(400, -1116, "Invalid orderType.")
        if not CLIENT_ID_RE.match(p.get("clientAlgoId", "")):
            return self._err(400, -4015, "Client order id is not valid.")
        if any(a["cid"] == p["clientAlgoId"] and a["status"] == "NEW" for a in self.algo_orders):
            return self._err(400, -4116, "ClientAlgoId is duplicated.")
        if "triggerPrice" not in p:
            return self._err(400, -1102, "Mandatory parameter 'triggerPrice' was not sent.")
        close = p.get("closePosition") == "true"
        if close and ("quantity" in p or "reduceOnly" in p):
            return self._err(400, -1106, "Parameter 'quantity'/'reduceOnly' sent when not required.")
        if not close and "quantity" not in p:
            return self._err(400, -1102, "Mandatory parameter 'quantity' was not sent.")
        if not close and float(p["quantity"]) < 0.001:
            return self._err(400, -1013, "Filter failure: LOT_SIZE")
        trigger, side, price = float(p["triggerPrice"]), p["side"], self.market_price
        sell_stop = (p["type"] == "STOP_MARKET") == (side == "SELL")  # стоп-SELL и тейк-BUY срабатывают при падении цены
        if (sell_stop and price <= trigger) or (not sell_stop and price >= trigger):
            return self._err(400, -2021, "Order would immediately trigger.")
        pos = self.positions[p["symbol"]]
        reduce = close or p.get("reduceOnly") == "true"
        if reduce and ((side == "SELL" and pos <= 0) or (side == "BUY" and pos >= 0)):
            return self._err(400, -2022, "ReduceOnly Order is rejected.")
        if self.strict_reduce_sum and not close and p.get("reduceOnly") == "true":
            open_qty = sum(a["quantity"] for a in self.algo_orders
                           if a["symbol"] == p["symbol"] and a["side"] == side and a["status"] == "NEW" and a["reduce_only"])
            if open_qty + float(p["quantity"]) > abs(pos) + 1e-12:
                return self._err(400, -2022, "ReduceOnly Order is rejected.")
        if close and any(a["symbol"] == p["symbol"] and a["side"] == side and a["status"] == "NEW" and a["close_position"]
                         and self._category(a["type"]) == self._category(p["type"]) for a in self.algo_orders):
            return self._err(400, -4130, "An open stop or take profit order with GTE and closePosition in the direction is existing.")
        return None

    async def _algo_post(self, request):
        params, error = self._auth(request)
        if error:
            return error
        self.algo_post_log.append(params)
        self.algo_requests.append(f"POST {params.get('clientAlgoId')}")
        if "pre_algo" in self.hooks:
            self.hooks["pre_algo"](params)
        fault = self._take("algo_post")
        if fault == "http429":
            return self._err(429, -1003, "Too many requests", {"Retry-After": "30"})
        if fault == "drop_before":
            request.transport.abort()
            return web.Response()
        if fault == "http500_before":
            return self._err(500, -1000, "Internal error")
        if fault == "reject":
            return self._err(400, self.algo_reject[0], self.algo_reject[1])
        rejection = self._validate_algo(params)
        if rejection is not None:
            return rejection
        self._algo_seq += 1
        close = params.get("closePosition") == "true"
        a = {"algoId": self._algo_seq, "cid": params["clientAlgoId"], "symbol": params["symbol"], "side": params["side"],
             "type": params["type"], "trigger": float(params["triggerPrice"]), "quantity": float(params.get("quantity", 0.0)),
             "close_position": close, "reduce_only": params.get("reduceOnly") == "true", "working": params.get("workingType", "CONTRACT_PRICE"),
             "status": "NEW", "actual_order_id": None, "createTime": self.now_ms(), "updateTime": self.now_ms(), "triggerTime": 0}
        self.algo_orders.append(a)
        await self.push(self.algo_event(a, "NEW"))
        if fault == "lost_response":
            request.transport.abort()  # ордер создан, ответ потерян
            return web.Response()
        if fault == "http500_after":
            return self._err(503, -1007, "Timeout waiting for response from backend server. Send status unknown; execution status unknown.")
        return web.json_response(self._snapshot_algo(a))

    def _find_algo(self, params: dict) -> dict | None:
        if "clientAlgoId" in params:
            return self.algo(params["clientAlgoId"])
        return next((a for a in self.algo_orders if str(a["algoId"]) == params.get("algoId")), None)

    async def _algo_delete(self, request):
        params, error = self._auth(request)
        if error:
            return error
        self.algo_requests.append(f"DELETE {params.get('clientAlgoId')}")
        fault = self._take("algo_delete")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        if fault == "drop_after":
            a = self._find_algo(params)
            if a and a["status"] == "NEW":
                a.update(status="CANCELED", updateTime=self.now_ms())
            request.transport.abort()  # отменён, ответ потерян
            return web.Response()
        a = self._find_algo(params)
        if a is None or a["status"] != "NEW":
            return self._err(400, self.algo_cancel_unknown_code, "Unknown order sent.")
        if fault == "noop":  # биржа ответила успехом, но ордер остался (проверка бдительности скрипта живой проверки)
            return web.json_response({"algoId": a["algoId"], "clientAlgoId": a["cid"], "code": "200", "msg": "success"})
        a.update(status="CANCELED", updateTime=self.now_ms())
        await self.push(self.algo_event(a, "CANCELED"))
        return web.json_response({"algoId": a["algoId"], "clientAlgoId": a["cid"], "code": "200", "msg": "success"})

    async def _algo_get(self, request):
        params, error = self._auth(request)
        if error:
            return error
        self.algo_requests.append(f"GET {params.get('clientAlgoId')}")
        fault = self._take("algo_get")
        if fault == "drop":
            request.transport.abort()
            return web.Response()
        if fault == "http500":
            return self._err(500, -1000, "Internal error")
        a = self._find_algo(params)
        if a is None:
            return self._err(400, self.algo_get_unknown_code, "Order does not exist.")
        return web.json_response(self._snapshot_algo(a))

    async def _algo_open(self, request):
        params, error = self._auth(request)
        if error:
            return error
        fault = self._take("algo_open")
        if fault == "http500":
            return self._err(500, -1000, "Internal error")
        symbol = params.get("symbol")
        return web.json_response([self._snapshot_algo(a) for a in self.algo_orders
                                  if a["status"] == "NEW" and (symbol is None or a["symbol"] == symbol)])


def dumps(event) -> str:
    return json.dumps(event)


async def settle(predicate, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)
