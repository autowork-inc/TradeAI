"""Имитация брокера cTrader (площадка MT5 в коде) со состоянием: счёт, символы, котировки, позиции, отложенные ордера, история сделок.

Это подделка ПУБЛИЧНОГО интерфейса `CTraderConnector` (то, чем пользуются ExecutionRouter, сопровождение `mt5_exit` и SymbolMapper), а не протокола: логика роутера и
выходов проверяется здесь, а сам клиент (кодек, авторизация, MARKET с относительными SL/TP, Position Recovery) - против имитации сервера в
`tests/exchange/fake_ctrader_server.py` (`test_ctrader.py`). Любое отклонение ответа брокера задаётся очередью `effects` (исключение `CTraderTradeError` или None).
"""

from datetime import datetime, timezone
from typing import Any

from exchange.ctrader import AccountInfo, CTraderOrder, CTraderPosition, CTraderTradeError, MarketClosedError, Quote, _trade_error
from exchange.symbol_mapper import SymbolMapper

FULL = "SYMBOL_TRADE_MODE_FULL"
# коды прежних тестов (retcode MT5) -> строковые коды cTrader
_CODES = {10018: "MARKET_CLOSED", 10019: "NOT_ENOUGH_MONEY", 10016: "TRADING_BAD_STOPS", 10012: "TIMEOUT_ERROR"}


def spec(path: str, *, contract: float, tick: float, digits: int, vmin=0.01, vstep=0.01, vmax=100.0, mode=FULL, stops=0):
    return {"contractSize": contract, "minVolume": vmin, "maxVolume": vmax, "volumeStep": vstep, "tickSize": tick, "digits": digits, "point": tick,
            "stopsLevel": stops, "freezeLevel": 0, "fillingModes": [], "tradeMode": mode, "profitCurrency": "USD", "marginCurrency": "USD",
            "path": path, "description": path}


SPECS = {
    "EURUSD": spec("Forex\\EURUSD", contract=100_000, tick=0.00001, digits=5, vmax=500),
    "XAUUSD": spec("Metals\\XAUUSD", contract=100, tick=0.01, digits=2),
    "XAGUSD": spec("Metals\\XAGUSD", contract=5000, tick=0.001, digits=3),
}
# котировки: (bid, ask, потеря за тик на лот в USD)
QUOTES = {"EURUSD": (1.10000, 1.10010, 1.0), "XAUUSD": (2000.00, 2000.30, 1.0), "XAGUSD": (25.000, 25.018, 5.0)}


def trade_error(code: int) -> CTraderTradeError:
    """Ошибка брокера по прежнему номеру (10018 - рынок закрыт, 10019 - нет денег, 10016 - плохие стопы, 10012 - таймаут: исход неизвестен)."""
    return _trade_error(f"retcode {code}", _CODES[code])


class FakeMt5Rpc:
    """Брокер-подделка. Имя сохранено с прежних тестов; одновременно играет роль клиента (`make_client` возвращает (rpc, rpc))."""

    def __init__(self, *, equity: float = 100_000.0, currency: str = "USD", free_margin: float | None = None):
        self.equity, self.currency = equity, currency
        self.free_margin = equity if free_margin is None else free_margin
        self.specs = {k: dict(v) for k, v in SPECS.items()}
        self.quotes = dict(QUOTES)
        self.positions: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.deals: list[dict[str, Any]] = []
        self.trades: list[tuple] = []  # (метод, аргументы) каждого торгового вызова - в порядке отправки
        self.effects: list = []  # очередь исходов торговых вызовов: исключение или None
        self.on_trade = None  # хук: вызывается перед обработкой торгового запроса (проверка порядка вызовов)
        self._n = 100
        self.id_prefix = "tb"

    # ------------------------------------------------------------------ подключение / чтение
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get_account_info(self) -> AccountInfo:
        return AccountInfo(broker="Fake", server="Fake-Demo", currency=self.currency, balance=self.equity, equity=self.equity, margin=0.0,
                           free_margin=self.free_margin, leverage=100.0, trade_allowed=True, margin_mode="HEDGED", name="demo", login=1)

    async def get_symbols(self) -> list[str]:
        return list(self.specs) + ["BRN", "GOLD"]

    async def get_symbol_specification(self, symbol: str) -> dict:
        if symbol == "BRN":
            return spec("Nasdaq\\Stock\\BRN", contract=1, tick=0.01, digits=2, vmin=1, vstep=1)
        if symbol == "GOLD":
            return spec("Nasdaq\\Stock\\GOLD", contract=1, tick=0.01, digits=2, vmin=1, vstep=1)
        return dict(self.specs[symbol])

    def ensure_entry_allowed(self, symbol: str) -> None:
        mode = self.specs.get(symbol, {}).get("tradeMode")
        if mode and mode != FULL:
            raise MarketClosedError(f"{symbol}: торговля символом сейчас ограничена брокером (tradeMode {mode}): новые входы запрещены")

    async def get_price(self, symbol: str) -> Quote:
        bid, ask, tv = self.quotes[symbol]
        return Quote(symbol, bid, ask, datetime(2026, 1, 1, tzinfo=timezone.utc), tv, tv)

    async def calculate_margin(self, symbol: str, side: str, volume: float, price: float) -> float:
        return volume * self.specs[symbol]["contractSize"] * price / 100  # плечо 1:100

    async def get_positions(self) -> list[CTraderPosition]:
        return [CTraderPosition(p["id"], p["symbol"], "BUY" if p["type"].endswith("BUY") else "SELL", p["volume"], p["openPrice"], p["stopLoss"] or None,
                                p["takeProfit"] or None, float(p.get("profit") or 0.0), p.get("clientId")) for p in self.positions]

    async def get_orders(self) -> list[dict]:
        return [dict(o) for o in self.orders]

    async def realized_pnl_since(self, start_ms: int, end_ms: int | None = None) -> float:
        start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end = datetime.fromtimestamp((end_ms if end_ms is not None else start_ms + 86_400_000) / 1000, tz=timezone.utc)
        return sum(d["profit"] + d["commission"] + d["swap"] for d in self.deals if start <= d["time"] < end and d["type"] != "DEAL_TYPE_BALANCE")

    # ------------------------------------------------------------------ торговля
    def _outcome(self, name: str, args: tuple) -> None:
        self.trades.append((name, *args))
        if self.on_trade is not None:
            self.on_trade(name, args)
        effect = self.effects.pop(0) if self.effects else None
        if isinstance(effect, Exception):
            raise effect

    def _next(self) -> str:
        self._n += 1
        return str(self._n)

    @staticmethod
    def _done(symbol: str, client_id, order_id=None, position_id=None, state="DONE") -> CTraderOrder:
        return CTraderOrder(symbol, client_id, order_id, position_id, state, "ok", "FILLED" if state == "DONE" else "ORDER_ACCEPTED")

    async def market_order(self, symbol: str, side: str, volume: float, sl: float | None = None, tp: float | None = None, *, client_id: str | None = None) -> CTraderOrder:
        options = {"clientId": client_id}
        self._outcome(f"market_{side.lower()}", (symbol, volume, sl, tp, options))
        bid, ask, _ = self.quotes[symbol]
        pid = self._next()
        self.positions.append({"id": pid, "type": f"POSITION_TYPE_{side}", "symbol": symbol, "volume": volume, "openPrice": ask if side == "BUY" else bid,
                               "stopLoss": sl or 0, "takeProfit": tp or 0, "profit": 0.0, "clientId": client_id})
        return self._done(symbol, client_id, self._next(), pid)

    async def limit_order(self, symbol: str, side: str, volume: float, price: float, sl: float | None = None, tp: float | None = None, *,
                          client_id: str | None = None, expires_at: datetime | None = None) -> CTraderOrder:
        options: dict[str, Any] = {"clientId": client_id}
        if expires_at is not None:
            options["expiration"] = {"type": "ORDER_TIME_SPECIFIED", "time": expires_at}
        self._outcome(f"limit_{side.lower()}", (symbol, volume, price, sl, tp, options))
        oid = self._next()
        self.orders.append({"id": oid, "type": f"ORDER_TYPE_{side}_LIMIT", "symbol": symbol, "volume": volume, "openPrice": price, "stopLoss": sl or 0,
                            "takeProfit": tp or 0, "clientId": client_id})
        return self._done(symbol, client_id, oid, None, "PLACED")

    async def modify_position(self, position_id: str, sl: float | None = None, tp: float | None = None) -> CTraderOrder:
        pos = next(p for p in self.positions if p["id"] == position_id)
        new_sl = sl if sl is not None else (pos["stopLoss"] or None)
        new_tp = tp if tp is not None else (pos["takeProfit"] or None)
        self._outcome("modify", (position_id, new_sl, new_tp))
        pos["stopLoss"], pos["takeProfit"] = new_sl or 0, new_tp or 0
        return self._done(pos["symbol"], pos.get("clientId"), None, position_id)

    async def close_position(self, position_id: str, volume: float | None = None, *, client_id: str | None = None) -> CTraderOrder:
        pos = next(p for p in self.positions if p["id"] == position_id)
        if volume is None or abs(volume - pos["volume"]) <= 1e-9:
            self._outcome("close", (position_id, {"clientId": client_id} if client_id else None))
            self.positions = [p for p in self.positions if p["id"] != position_id]
        else:
            self._outcome("close_part", (position_id, volume, {"clientId": client_id} if client_id else None))
            pos["volume"] = round(pos["volume"] - volume, 8)
        return self._done(pos["symbol"], client_id, None, position_id)

    async def cancel_order(self, order_id: str) -> CTraderOrder:
        self._outcome("cancel", (order_id,))
        self.orders = [o for o in self.orders if o["id"] != order_id]
        return self._done("", None, order_id)

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


def make_client(rpc: FakeMt5Rpc | None = None) -> tuple[FakeMt5Rpc, FakeMt5Rpc]:
    rpc = rpc or FakeMt5Rpc()
    return rpc, rpc


async def make_mapper(client, symbols=("EURUSD", "XAUUSD", "XAGUSD")) -> SymbolMapper:
    mapper = SymbolMapper(client, overrides={}, allow_disabled=True)
    await mapper.load(symbols)
    return mapper
