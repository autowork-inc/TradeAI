"""Имитация сервера cTrader Open API: настоящий TCP-сервер (без TLS), разговаривает кадрами Protobuf по схемам Spotware (`exchange/ctrader_proto.py`).

Нужен, чтобы проверять `CTraderConnector` целиком: авторизация, справочники, трендбары, котировки, ордера, Position Recovery, обрыв связи. Состояние (позиции,
ордера, сделки, свечи) и исходы (`effects`) задаются тестом. Ничего реального: ни сети, ни ключей.
"""

import asyncio
from typing import Any

from exchange import ctrader_proto as P

ASSETS = {1: "USD", 2: "EUR", 3: "XAU", 4: "GBP", 5: "JPY"}
# symbolId: (имя, base, quote, digits, lotSize, minVolume, stepVolume, maxVolume)
SYMBOLS = {
    1: ("EURUSD", 2, 1, 5, 10_000_000, 100_000, 100_000, 5_000_000_000),
    2: ("XAUUSD", 3, 1, 2, 10_000, 100, 100, 5_000_000),
    3: ("GBPUSD", 4, 1, 5, 10_000_000, 100_000, 100_000, 5_000_000_000),
    4: ("USDJPY", 1, 5, 3, 10_000_000, 100_000, 100_000, 5_000_000_000),
}
CTID = 7654321


class FakeCTraderServer:
    def __init__(self, *, client_id="cid", client_secret="sec", access_token="tok-1", refresh_token="ref-1", is_live=False, ctid=CTID, echo_msg_id=True, deal_page=500):
        self.client_id, self.client_secret = client_id, client_secret
        self.valid_token, self.valid_refresh, self.is_live, self.ctid = access_token, refresh_token, is_live, ctid
        self.requests: list[tuple[str, dict]] = []
        self.heartbeats = 0
        self.quotes: dict[int, tuple[float, float]] = {1: (1.10000, 1.10010), 2: (2000.00, 2000.30), 3: (1.30000, 1.30020), 4: (150.000, 150.020)}
        self.bars: dict[int, list[tuple]] = {}  # symbolId -> [(open_ms, o, h, l, c, volume)]
        self.positions: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.deals: list[dict[str, Any]] = []
        self.balance = 10_000_000  # 100 000.00 USD при moneyDigits=2
        self.unrealized = 0  # центы
        self.effects: list[str | None] = []  # очередь исходов торговых запросов: код ошибки или None (успех); "SILENT" - не отвечать
        self.echo_msg_id = echo_msg_id  # ExecutionEvent с clientMsgId запроса (False - только clientOrderId/positionId)
        self.deal_page = deal_page
        self._n = 1000
        self._writers: list[asyncio.StreamWriter] = []
        self._subs: set[int] = set()
        self.server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> "FakeCTraderServer":
        self.server = await asyncio.start_server(self._client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        for w in list(self._writers):
            w.close()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    def drop(self) -> None:
        """Оборвать все соединения (как падение сети)."""
        for w in list(self._writers):
            w.close()

    def seen(self, name: str) -> list[dict]:
        return [m for n, m in self.requests if n == name]

    # ------------------------------------------------------------------------------------------ протокол
    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.append(writer)
        try:
            while True:
                size = int.from_bytes(await reader.readexactly(4), "big")
                name, msg, mid = P.parse(await reader.readexactly(size))
                if name == "ProtoHeartbeatEvent":
                    self.heartbeats += 1
                    continue
                self.requests.append((name, msg))
                await self._handle(writer, name, msg, mid)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if writer in self._writers:
                self._writers.remove(writer)
            writer.close()

    @staticmethod
    async def _send(writer: asyncio.StreamWriter, name: str, values: dict, mid: str | None) -> None:
        writer.write(P.build(name, values, mid))
        await writer.drain()

    async def _error(self, writer, mid, code, desc="") -> None:
        await self._send(writer, "ErrorRes", {"ctidTraderAccountId": self.ctid, "errorCode": code, "description": desc or code}, mid)

    def _next(self) -> int:
        self._n += 1
        return self._n

    def _money_digits(self) -> int:
        return 2

    def _trade_data(self, symbol_id: int, volume: int, side: int, label: str | None = None) -> dict:
        return {"symbolId": symbol_id, "volume": volume, "tradeSide": side, "openTimestamp": 1_700_000_000_000, "label": label}

    def _pos_msg(self, p: dict) -> dict:
        return {"positionId": p["id"], "tradeData": self._trade_data(p["symbolId"], p["volume"], p["side"], p.get("label")), "positionStatus": P.POS_OPEN, "swap": 0,
                "price": p["price"], "stopLoss": p.get("sl"), "takeProfit": p.get("tp"), "usedMargin": p.get("margin", 100_000), "moneyDigits": 2}

    def _order_msg(self, o: dict) -> dict:
        return {"orderId": o["id"], "tradeData": self._trade_data(o["symbolId"], o["volume"], o["side"], o.get("label")), "orderType": o["type"], "orderStatus": 1,
                "limitPrice": o.get("limit"), "stopLoss": o.get("sl"), "takeProfit": o.get("tp"), "clientOrderId": o.get("cid")}

    async def _handle(self, w, name: str, m: dict, mid: str | None) -> None:
        if name == "ApplicationAuthReq":
            if (m["clientId"], m["clientSecret"]) != (self.client_id, self.client_secret):
                return await self._error(w, mid, "CH_CLIENT_AUTH_FAILURE", "bad credentials")
            return await self._send(w, "ApplicationAuthRes", {}, mid)
        if name == "GetAccountListByAccessTokenReq":
            if m["accessToken"] != self.valid_token:
                return await self._error(w, mid, "CH_ACCESS_TOKEN_INVALID", "token invalid")
            return await self._send(w, "GetAccountListByAccessTokenRes", {"accessToken": m["accessToken"], "ctidTraderAccount": [{"ctidTraderAccountId": self.ctid, "isLive": self.is_live}]}, mid)
        if name == "AccountAuthReq":
            if m["accessToken"] != self.valid_token:
                return await self._error(w, mid, "OA_AUTH_TOKEN_EXPIRED", "expired")
            if m["ctidTraderAccountId"] != self.ctid:
                return await self._error(w, mid, "CH_CTID_TRADER_ACCOUNT_NOT_FOUND")
            return await self._send(w, "AccountAuthRes", {"ctidTraderAccountId": self.ctid}, mid)
        if name == "RefreshTokenReq":
            if m["refreshToken"] != self.valid_refresh:
                return await self._error(w, mid, "CH_ACCESS_TOKEN_INVALID", "refresh invalid")
            self.valid_token, self.valid_refresh = f"tok-{self._next()}", f"ref-{self._next()}"
            return await self._send(w, "RefreshTokenRes", {"accessToken": self.valid_token, "tokenType": "bearer", "expiresIn": 2628000, "refreshToken": self.valid_refresh}, mid)
        if name == "AssetListReq":
            return await self._send(w, "AssetListRes", {"ctidTraderAccountId": self.ctid, "asset": [{"assetId": i, "name": n} for i, n in ASSETS.items()]}, mid)
        if name == "SymbolsListReq":
            return await self._send(w, "SymbolsListRes", {"ctidTraderAccountId": self.ctid, "symbol": [
                {"symbolId": i, "symbolName": s[0], "enabled": True, "baseAssetId": s[1], "quoteAssetId": s[2], "description": s[0]} for i, s in SYMBOLS.items()]}, mid)
        if name == "SymbolByIdReq":
            out = []
            for sid in m["symbolId"]:
                s = SYMBOLS[sid]
                out.append({"symbolId": sid, "digits": s[3], "pipPosition": s[3] - 1, "minVolume": s[5], "stepVolume": s[6], "maxVolume": s[7], "lotSize": s[4],
                            "tradingMode": P.TRADING_ENABLED, "slDistance": 0, "distanceSetIn": 1})
            return await self._send(w, "SymbolByIdRes", {"ctidTraderAccountId": self.ctid, "symbol": out}, mid)
        if name == "TraderReq":
            return await self._send(w, "TraderRes", {"ctidTraderAccountId": self.ctid, "trader": {
                "ctidTraderAccountId": self.ctid, "balance": self.balance, "depositAssetId": 1, "leverageInCents": 10_000, "accountType": 0, "brokerName": "FakeBroker",
                "moneyDigits": 2, "traderLogin": 12345, "accessRights": 0}}, mid)
        if name == "ReconcileReq":
            return await self._send(w, "ReconcileRes", {"ctidTraderAccountId": self.ctid, "position": [self._pos_msg(p) for p in self.positions],
                                                        "order": [self._order_msg(o) for o in self.orders]}, mid)
        if name == "GetPositionUnrealizedPnLReq":
            per = self.unrealized // len(self.positions) if self.positions else 0
            return await self._send(w, "GetPositionUnrealizedPnLRes", {"ctidTraderAccountId": self.ctid, "moneyDigits": 2, "positionUnrealizedPnL": [
                {"positionId": p["id"], "grossUnrealizedPnL": per, "netUnrealizedPnL": per} for p in self.positions]}, mid)
        if name == "SubscribeSpotsReq":
            await self._send(w, "SubscribeSpotsRes", {"ctidTraderAccountId": self.ctid}, mid)
            for sid in m["symbolId"]:
                self._subs.add(sid)
                bid, ask = self.quotes[sid]
                await self._send(w, "SpotEvent", {"ctidTraderAccountId": self.ctid, "symbolId": sid, "bid": int(round(bid * 1e5)), "ask": int(round(ask * 1e5)),
                                                  "timestamp": 1_700_000_000_000}, None)
            return None
        if name == "GetTrendbarsReq":
            return await self._trendbars(w, m, mid)
        if name == "DealListReq":
            deals = sorted((d for d in self.deals if m["fromTimestamp"] <= d["executionTimestamp"] <= m["toTimestamp"]), key=lambda d: -d["executionTimestamp"])
            page = deals[:min(m.get("maxRows", 500), self.deal_page)]
            return await self._send(w, "DealListRes", {"ctidTraderAccountId": self.ctid, "deal": page, "hasMore": len(deals) > len(page)}, mid)
        if name in ("NewOrderReq", "AmendPositionSLTPReq", "ClosePositionReq", "CancelOrderReq"):
            return await self._trade(w, name, m, mid)
        return await self._error(w, mid, "UNSUPPORTED_MESSAGE", name)

    async def _trendbars(self, w, m: dict, mid) -> None:
        if m["toTimestamp"] - m["fromTimestamp"] > P.PERIOD_MAX_WINDOW_MS[m["period"]]:
            return await self._error(w, mid, "INCORRECT_BOUNDARIES", "window too wide")
        rows = sorted(b for b in self.bars.get(m["symbolId"], []) if m["fromTimestamp"] <= b[0] <= m["toTimestamp"])
        rows = rows[-m.get("count", len(rows)):] if rows else rows
        bars = []
        for t, o, h, lo, c, v in rows:
            low = int(round(lo * 1e5))
            bars.append({"volume": int(v), "period": m["period"], "low": low, "deltaOpen": int(round(o * 1e5)) - low, "deltaClose": int(round(c * 1e5)) - low,
                         "deltaHigh": int(round(h * 1e5)) - low, "utcTimestampInMinutes": t // 60_000})
        await self._send(w, "GetTrendbarsRes", {"ctidTraderAccountId": self.ctid, "period": m["period"], "trendbar": bars, "symbolId": m["symbolId"], "hasMore": False}, mid)

    # ------------------------------------------------------------------------------------------ торговля
    async def _exec(self, w, mid, etype: int, *, position=None, order=None, deal=None) -> None:
        values = {"ctidTraderAccountId": self.ctid, "executionType": etype, "position": position, "order": order, "deal": deal}
        await self._send(w, "ExecutionEvent", values, mid if self.echo_msg_id else None)

    async def _trade(self, w, name: str, m: dict, mid) -> None:
        effect = self.effects.pop(0) if self.effects else None
        if effect == "SILENT":
            return None
        if effect is not None:
            return await self._error(w, mid, effect)
        if name == "NewOrderReq":
            sid, side, vol = m["symbolId"], m["tradeSide"], m["volume"]
            if m["orderType"] == P.ORDER_MARKET:
                if m.get("stopLoss") is not None or m.get("takeProfit") is not None:
                    return await self._error(w, mid, "TRADING_BAD_STOPS", "absolute SL/TP not supported for MARKET orders")
                bid, ask = self.quotes[sid]
                price = ask if side == P.SIDE_BUY else bid
                sgn = 1 if side == P.SIDE_BUY else -1
                sl = None if m.get("relativeStopLoss") is None else round(price - sgn * m["relativeStopLoss"] / 1e5, 5)
                tp = None if m.get("relativeTakeProfit") is None else round(price + sgn * m["relativeTakeProfit"] / 1e5, 5)
                pos = {"id": self._next(), "symbolId": sid, "volume": vol, "side": side, "price": price, "sl": sl, "tp": tp, "label": m.get("label"), "cid": m.get("clientOrderId")}
                self.positions.append(pos)
                order = {"orderId": self._next(), "tradeData": self._trade_data(sid, vol, side), "orderType": P.ORDER_MARKET, "orderStatus": 2, "clientOrderId": m.get("clientOrderId"),
                         "executionPrice": price}
                await self._exec(w, mid, P.EXEC_ACCEPTED, order=order)
                await self._exec(w, mid, P.EXEC_FILLED, position=self._pos_msg(pos), order=order, deal={
                    "dealId": self._next(), "orderId": order["orderId"], "positionId": pos["id"], "volume": vol, "filledVolume": vol, "symbolId": sid, "createTimestamp": 1,
                    "executionTimestamp": 1, "executionPrice": price, "tradeSide": side, "dealStatus": 2})
                return None
            o = {"id": self._next(), "symbolId": sid, "volume": vol, "side": side, "type": P.ORDER_LIMIT, "limit": m["limitPrice"], "sl": m.get("stopLoss"),
                 "tp": m.get("takeProfit"), "cid": m.get("clientOrderId"), "label": m.get("label")}
            self.orders.append(o)
            return await self._exec(w, mid, P.EXEC_ACCEPTED, order=self._order_msg(o))
        if name == "AmendPositionSLTPReq":
            pos = next((p for p in self.positions if p["id"] == m["positionId"]), None)
            if pos is None:
                return await self._error(w, mid, "POSITION_NOT_FOUND")
            pos["sl"], pos["tp"] = m.get("stopLoss"), m.get("takeProfit")
            return await self._exec(w, mid, P.EXEC_REPLACED, position=self._pos_msg(pos))
        if name == "ClosePositionReq":
            pos = next((p for p in self.positions if p["id"] == m["positionId"]), None)
            if pos is None:
                return await self._error(w, mid, "POSITION_NOT_FOUND")
            pos["volume"] -= m["volume"]
            bid, ask = self.quotes[pos["symbolId"]]
            price = bid if pos["side"] == P.SIDE_BUY else ask
            if pos["volume"] <= 0:
                self.positions.remove(pos)
            return await self._exec(w, mid, P.EXEC_FILLED, position=self._pos_msg(pos), deal={
                "dealId": self._next(), "orderId": self._next(), "positionId": pos["id"], "volume": m["volume"], "filledVolume": m["volume"], "symbolId": pos["symbolId"],
                "createTimestamp": 2, "executionTimestamp": 2, "executionPrice": price, "tradeSide": P.SIDE_SELL if pos["side"] == P.SIDE_BUY else P.SIDE_BUY, "dealStatus": 2})
        if name == "CancelOrderReq":
            o = next((o for o in self.orders if o["id"] == m["orderId"]), None)
            if o is None:
                return await self._error(w, mid, "ORDER_NOT_FOUND")
            self.orders.remove(o)
            return await self._exec(w, mid, P.EXEC_CANCELLED, order=self._order_msg(o))
        return None
