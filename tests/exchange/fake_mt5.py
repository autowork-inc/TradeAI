"""Имитация RPC-соединения MetaApi (MT5) со состоянием: счёт, символы, котировки, позиции, отложенные ордера, история сделок.

Настоящий `MetaApiLiveClient` работает поверх неё (подменено только `_connection`): сборка запросов, проверки сетки и коды возврата - боевые.
Реальных запросов к брокеру нет. Любое отклонение ответа брокера задаётся очередью `effects` (исключение `TradeException` или dict-ответ).
"""

from datetime import datetime, timezone
from typing import Any

from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException

from exchange.mt5_metaapi import RETCODES, MetaApiLiveClient
from exchange.symbol_mapper import SymbolMapper

FULL = "SYMBOL_TRADE_MODE_FULL"


def spec(path: str, *, contract: float, tick: float, digits: int, vmin=0.01, vstep=0.01, vmax=100.0, mode=FULL, stops=0, fills=("SYMBOL_FILLING_FOK",)):
    return {"contractSize": contract, "minVolume": vmin, "maxVolume": vmax, "volumeStep": vstep, "tickSize": tick, "digits": digits, "point": tick,
            "stopsLevel": stops, "freezeLevel": 0, "fillingModes": list(fills), "tradeMode": mode, "profitCurrency": "USD", "marginCurrency": "USD",
            "path": path, "description": path}


SPECS = {
    "EURUSD": spec("Forex\\EURUSD", contract=100_000, tick=0.00001, digits=5, vmax=500),
    "XAUUSD": spec("Metals\\XAUUSD", contract=100, tick=0.01, digits=2, fills=("SYMBOL_FILLING_FOK", "SYMBOL_FILLING_IOC")),
    "XAGUSD": spec("Metals\\XAGUSD", contract=5000, tick=0.001, digits=3),
}
# котировки: (bid, ask, потеря за тик на лот в USD)
QUOTES = {"EURUSD": (1.10000, 1.10010, 1.0), "XAUUSD": (2000.00, 2000.30, 1.0), "XAGUSD": (25.000, 25.018, 5.0)}


def trade_error(code: int) -> TradeException:
    return TradeException(f"retcode {code}", code, f"TRADE_RETCODE_{RETCODES[code][0]}")


class FakeMt5Rpc:
    def __init__(self, *, equity: float = 100_000.0, currency: str = "USD", free_margin: float | None = None):
        self.equity, self.currency = equity, currency
        self.free_margin = equity if free_margin is None else free_margin
        self.specs = {k: dict(v) for k, v in SPECS.items()}
        self.quotes = dict(QUOTES)
        self.positions: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.deals: list[dict[str, Any]] = []
        self.trades: list[tuple] = []  # (метод, аргументы) каждого торгового вызова - в порядке отправки
        self.effects: list = []  # очередь исходов торговых вызовов: исключение, dict-ответ или None
        self.synchronizing = False
        self.on_trade = None  # хук: вызывается перед обработкой торгового запроса (проверка порядка вызовов)
        self._n = 100

    # ------------------------------------------------------------------ чтение
    async def get_account_information(self):
        return {"broker": "Fake", "server": "Fake-Demo", "currency": self.currency, "balance": self.equity, "equity": self.equity, "margin": 0.0,
                "freeMargin": self.free_margin, "leverage": 100, "tradeAllowed": True, "marginMode": "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING",
                "name": "demo", "login": 1}

    async def get_symbols(self):
        return list(self.specs) + ["BRN", "GOLD"]

    async def get_symbol_specification(self, symbol):
        if symbol == "BRN":
            return spec("Nasdaq\\Stock\\BRN", contract=1, tick=0.01, digits=2, vmin=1, vstep=1)
        if symbol == "GOLD":
            return spec("Nasdaq\\Stock\\GOLD", contract=1, tick=0.01, digits=2, vmin=1, vstep=1)
        return dict(self.specs[symbol])

    async def get_symbol_price(self, symbol):
        bid, ask, tv = self.quotes[symbol]
        return {"symbol": symbol, "bid": bid, "ask": ask, "lossTickValue": tv, "profitTickValue": tv, "time": datetime(2026, 1, 1, tzinfo=timezone.utc)}

    async def calculate_margin(self, order):
        return {"margin": order["volume"] * self.specs[order["symbol"]]["contractSize"] * order["openPrice"] / 100}  # плечо 1:100

    async def get_positions(self):
        return [dict(p) for p in self.positions]

    async def get_position(self, pid):
        return next(dict(p) for p in self.positions if p["id"] == pid)

    async def get_orders(self):
        return [dict(o) for o in self.orders]

    async def get_deals_by_time_range(self, start, end, offset=0, limit=1000):
        deals = [d for d in self.deals if start <= d["time"] < end]
        return {"deals": deals[offset:offset + limit], "synchronizing": self.synchronizing}

    # ------------------------------------------------------------------ торговля
    def _outcome(self, name, args):
        self.trades.append((name, *args))
        if self.on_trade is not None:
            self.on_trade(name, args)
        effect = self.effects.pop(0) if self.effects else None
        if isinstance(effect, Exception):
            raise effect
        return effect

    def _next(self) -> str:
        self._n += 1
        return str(self._n)

    @staticmethod
    def _done(order_id=None, position_id=None, code=10009, name="DONE"):
        return {"numericCode": code, "stringCode": f"TRADE_RETCODE_{name}", "message": "ok", "orderId": order_id, "positionId": position_id}

    async def _market(self, side, symbol, volume, sl, tp, options):
        custom = self._outcome(f"market_{side.lower()}", (symbol, volume, sl, tp, options))
        if custom is not None:
            return custom
        bid, ask, _ = self.quotes[symbol]
        pid = self._next()
        self.positions.append({"id": pid, "type": f"POSITION_TYPE_{side}", "symbol": symbol, "volume": volume, "openPrice": ask if side == "BUY" else bid,
                               "stopLoss": sl or 0, "takeProfit": tp or 0, "profit": 0.0, "clientId": (options or {}).get("clientId")})
        return self._done(self._next(), pid)

    async def create_market_buy_order(self, symbol, volume, sl=None, tp=None, options=None):
        return await self._market("BUY", symbol, volume, sl, tp, options)

    async def create_market_sell_order(self, symbol, volume, sl=None, tp=None, options=None):
        return await self._market("SELL", symbol, volume, sl, tp, options)

    async def _limit(self, side, symbol, volume, price, sl, tp, options):
        custom = self._outcome(f"limit_{side.lower()}", (symbol, volume, price, sl, tp, options))
        if custom is not None:
            return custom
        oid = self._next()
        self.orders.append({"id": oid, "type": f"ORDER_TYPE_{side}_LIMIT", "symbol": symbol, "volume": volume, "openPrice": price, "stopLoss": sl or 0,
                            "takeProfit": tp or 0, "clientId": (options or {}).get("clientId")})
        return self._done(oid, None, 10008, "PLACED")

    async def create_limit_buy_order(self, symbol, volume, price, sl=None, tp=None, options=None):
        return await self._limit("BUY", symbol, volume, price, sl, tp, options)

    async def create_limit_sell_order(self, symbol, volume, price, sl=None, tp=None, options=None):
        return await self._limit("SELL", symbol, volume, price, sl, tp, options)

    async def modify_position(self, position_id, sl=None, tp=None):
        custom = self._outcome("modify", (position_id, sl, tp))
        if custom is not None:
            return custom
        for p in self.positions:
            if p["id"] == position_id:
                p["stopLoss"], p["takeProfit"] = sl or 0, tp or 0
        return self._done(None, position_id)

    async def close_position(self, position_id, options=None):
        custom = self._outcome("close", (position_id, options))
        if custom is not None:
            return custom
        self.positions = [p for p in self.positions if p["id"] != position_id]
        return self._done(None, position_id)

    async def close_position_partially(self, position_id, volume, options=None):
        custom = self._outcome("close_part", (position_id, volume, options))
        if custom is not None:
            return custom
        for p in self.positions:
            if p["id"] == position_id:
                p["volume"] = round(p["volume"] - volume, 8)
        return self._done(None, position_id)

    async def cancel_order(self, order_id):
        custom = self._outcome("cancel", (order_id,))
        if custom is not None:
            return custom
        self.orders = [o for o in self.orders if o["id"] != order_id]
        return self._done(order_id)

    # ------------------------------------------------------------------ «события брокера» для тестов
    def fill_pending(self, order_id: str) -> None:
        """Лимитка исполнилась: ордер исчезает, появляется позиция (clientId сохраняется)."""
        o = next(o for o in self.orders if o["id"] == order_id)
        self.orders.remove(o)
        self.positions.append({"id": self._next(), "type": "POSITION_TYPE_" + ("BUY" if "BUY" in o["type"] else "SELL"), "symbol": o["symbol"], "volume": o["volume"],
                               "openPrice": o["openPrice"], "stopLoss": o["stopLoss"], "takeProfit": o["takeProfit"], "profit": 0.0, "clientId": o["clientId"]})

    def broker_closes(self, position_id: str, profit: float, when: datetime | None = None, commission: float = 0.0) -> None:
        """SL/TP на стороне брокера: позиция закрыта, в истории - сделка с результатом."""
        self.positions = [p for p in self.positions if p["id"] != position_id]
        self.add_deal(profit, commission=commission, when=when)

    def add_deal(self, profit: float, *, commission: float = 0.0, swap: float = 0.0, kind: str = "DEAL_TYPE_SELL", when: datetime | None = None) -> None:
        self.deals.append({"type": kind, "profit": profit, "commission": commission, "swap": swap, "time": when or datetime.now(timezone.utc)})


def make_client(rpc: FakeMt5Rpc | None = None) -> tuple[MetaApiLiveClient, FakeMt5Rpc]:
    rpc = rpc or FakeMt5Rpc()
    client = MetaApiLiveClient({"METAAPI_TOKEN": "t", "METAAPI_ACCOUNT_ID": "a"})
    client._connection = rpc
    return client, rpc


async def make_mapper(client: MetaApiLiveClient, symbols=("EURUSD", "XAUUSD", "XAGUSD")) -> SymbolMapper:
    mapper = SymbolMapper(client, overrides={}, allow_disabled=True)
    await mapper.load(symbols)
    return mapper
