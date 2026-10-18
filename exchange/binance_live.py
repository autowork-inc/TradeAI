"""Клиент приватного API Binance USDⓈ-M Futures для боевого контура (Live Execution Engine).

Что это. Тонкий, строгий слой «метод -> запрос биржи -> типизированный ответ». Подпись HMAC-SHA256, заголовок ключа,
коррекция часов (-1021), типизированные ошибки (429/418 -> RateLimitError, 4xx -> BinanceAPIError с кодом, сеть/5xx ->
NetworkError/HttpServerError) берутся из `execution.rest.SignedRest`: это тот же проверенный транспорт, что у `LiveBarrier`,
второй копии подписи в проекте нет. Ключи - `BINANCE_API_KEY` / `BINANCE_API_SECRET` из окружения (`.env` загружает config.py);
в repr и логи секрет не попадает.

Чего клиент НЕ делает (сознательно).
  * Не повторяет запросы сам: сетевой сбой при отправке ордера = «исход неизвестен», решение (сверка по clientOrderId)
    принимает вызывающий (`orchestrator/live_engine.py`). Идемпотентность обеспечивают детерминированные client id.
  * Не округляет молча: цена и объём обязаны лежать на сетке биржи (tickSize / stepSize) и проходить minQty / MIN_NOTIONAL,
    иначе `OrderValidationError` ДО отправки (R1: лучше отказ, чем ордер, отличающийся от рассчитанного). Округляют
    `round_qty` / `round_price` явно, до вызова.
  * Не включает торговлю: без вызова `load_filters` ни один ордер не уйдёт (нечем проверить сетку).

Формат запросов (как «на проводе»):
  MARKET/LIMIT  POST /fapi/v1/order      symbol, side, type, quantity, [price, timeInForce], newClientOrderId,
                                         newOrderRespType=RESULT, [reduceOnly=true]
  SL / TP       POST /fapi/v1/algoOrder  algoType=CONDITIONAL, symbol, side, type=STOP_MARKET | TAKE_PROFIT_MARKET,
                                         triggerPrice, workingType, clientAlgoId, и либо quantity + reduceOnly=true
                                         (по умолчанию, решение О18), либо closePosition=true
  Условные ордера USDⓈ-M с 2025-12-09 живут в Algo Service (CLAUDE.md, разд. 4); старый /fapi/v1/order для STOP_MARKET
  отвечает -4120.
❓ Не проверено на живой бирже: путь и поля `/fapi/v3/balance`, принимает ли Algo Service сумму reduceOnly-ордеров (SL + TP)
больше позиции (главная гипотеза О18), код отказа отмены неизвестного algo-ордера. Всё это проверяется на testnet до боя.
"""

import hashlib
import logging
import math
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any

import aiohttp

import config
from execution.exchange_filters import SymbolFilters, fetch_symbol_filters
from execution.reconcile import ExchangeAlgoOrder, parse_algo_orders
from execution.rest import BinanceAPIError, Credentials, SignedRest
from utils.errors import ConfigError, DataError, TradingInfraError

log = logging.getLogger(__name__)

SIDES = ("BUY", "SELL")
ERR_NO_NEED_TO_CHANGE_MARGIN_TYPE = -4046  # маржа уже в нужном режиме: для set_margin_type это успех
ALGO_TERMINAL = frozenset({"FINISHED", "CANCELED", "EXPIRED", "REJECTED", "TRIGGERED"})
INCOME_TYPES = frozenset({"REALIZED_PNL", "COMMISSION", "FUNDING_FEE"})  # то, из чего складывается суточный результат
INCOME_PAGE = 1000  # максимум биржи на страницу /fapi/v1/income (по умолчанию она отдаёт 100)
INCOME_MAX_PAGES = 50
USER_TRADES_PAGE = 1000  # максимум биржи на запрос /fapi/v1/userTrades
USER_TRADES_WINDOW_MS = 7 * 86_400_000  # окно startTime..endTime у userTrades - не больше 7 суток
USER_TRADES_MAX_WINDOWS = 30  # сделка старше ~210 суток при сверке - ошибка, а не бесконечный перебор окон


class OrderValidationError(TradingInfraError):
    """Параметры ордера не прошли локальную проверку (сетка цены/объёма, минимумы, сторона): запрос НЕ отправлялся."""


# ------------------------------------------------------------------------------------------- формат


def fmt_decimal(value: float) -> str:
    """Число в виде, который принимает Binance: без экспоненты и хвостовых нулей (1e-05 -> 0.00001)."""
    if not math.isfinite(value):
        raise OrderValidationError(f"Недопустимое число {value!r}")
    text = format(Decimal(repr(float(value))), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def client_id(prefix: str, role: str, key: str) -> str:
    """Детерминированный client id `<префикс><роль>_<20 hex от key>` (<= 36 символов, формат Binance).

    Один и тот же (key, role) всегда даёт один и тот же id: повторная отправка после неизвестного исхода не создаёт дубль
    (биржа отвергает повтор), а состояние ордера всегда можно спросить по id.
    """
    if not (role.isalnum() and key):
        raise OrderValidationError(f"client_id: role {role!r} должна быть буквенно-цифровой, key не пуст")
    cid = f"{prefix}{role}_{hashlib.sha1(key.encode('utf-8')).hexdigest()[:20]}"
    if len(cid) > 36:
        raise OrderValidationError(f"client id {cid!r} длиннее 36 символов")
    return cid


def _on_grid(value: float, step: float) -> bool:
    return (Decimal(repr(float(value))) / Decimal(repr(float(step)))) % 1 == 0


def _num(data: Mapping[str, Any], key: str, what: str, *, default: float | None = None) -> float:
    try:
        raw = data[key] if default is None else data.get(key, default)
        out = float(raw)
    except (KeyError, TypeError, ValueError) as exc:
        raise DataError(f"{what}: нет числового поля {key!r} в {str(dict(data))[:300]}") from exc
    if not math.isfinite(out):
        raise DataError(f"{what}: поле {key!r} не конечно ({raw!r})")
    return out


def _text(data: Mapping[str, Any], key: str, what: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise DataError(f"{what}: нет строкового поля {key!r} в {str(dict(data))[:300]}")
    return value


# ------------------------------------------------------------------------------------- типы ответов


@dataclass(frozen=True)
class Balance:
    asset: str
    wallet: float  # баланс кошелька
    available: float  # доступно для новой маржи
    unrealized_pnl: float

    @property
    def margin_balance(self) -> float:
        return self.wallet + self.unrealized_pnl


@dataclass(frozen=True)
class PositionInfo:
    symbol: str
    amt: float  # > 0 лонг, < 0 шорт
    entry_price: float


@dataclass(frozen=True)
class OrderResult:
    symbol: str
    client_order_id: str
    order_id: int | None
    status: str  # NEW | PARTIALLY_FILLED | FILLED | CANCELED | EXPIRED | REJECTED
    side: str
    type: str
    orig_qty: float
    executed_qty: float
    avg_price: float  # 0.0, пока нет исполнений
    price: float

    @classmethod
    def parse(cls, data: Any) -> "OrderResult":
        if not isinstance(data, dict):
            raise DataError(f"Ответ по ордеру не объект: {str(data)[:200]}")
        what = "order"
        order_id = data.get("orderId")
        return cls(
            symbol=_text(data, "symbol", what), client_order_id=_text(data, "clientOrderId", what),
            order_id=order_id if isinstance(order_id, int) else None, status=_text(data, "status", what),
            side=_text(data, "side", what), type=_text(data, "type", what), orig_qty=_num(data, "origQty", what),
            executed_qty=_num(data, "executedQty", what), avg_price=_num(data, "avgPrice", what, default=0.0),
            price=_num(data, "price", what, default=0.0),
        )

    @property
    def done(self) -> bool:
        return self.status in ("FILLED", "CANCELED", "EXPIRED", "REJECTED")


@dataclass(frozen=True)
class AlgoResult:
    symbol: str
    client_algo_id: str
    algo_id: int | None
    status: str  # NEW | TRIGGERING | TRIGGERED | FINISHED | CANCELED | EXPIRED | REJECTED
    type: str
    actual_price: float = 0.0  # цена исполнения порождённого ордера (после срабатывания; ❓ поле из документации)
    actual_qty: float = 0.0

    @classmethod
    def parse(cls, data: Any) -> "AlgoResult":
        if not isinstance(data, dict):
            raise DataError(f"Ответ по algo-ордеру не объект: {str(data)[:200]}")
        what = "algoOrder"
        algo_id = data.get("algoId")
        return cls(
            symbol=str(data.get("symbol", "")), client_algo_id=_text(data, "clientAlgoId", what),
            algo_id=algo_id if isinstance(algo_id, int) else None, status=_text(data, "algoStatus", what),
            type=str(data.get("orderType", data.get("type", ""))),
            actual_price=_num(data, "actualPrice", what, default=0.0) if data.get("actualPrice") not in (None, "") else 0.0,
            actual_qty=_num(data, "actualQty", what, default=0.0) if data.get("actualQty") not in (None, "") else 0.0,
        )

    @property
    def terminal(self) -> bool:
        return self.status in ALGO_TERMINAL


@dataclass(frozen=True)
class UserTrade:
    """Исполнение по счёту (`/fapi/v1/userTrades`): фактическая цена и комиссия."""

    symbol: str
    side: str  # BUY | SELL
    qty: float
    price: float
    commission: float
    commission_asset: str
    realized_pnl: float
    time_ms: int

    @classmethod
    def parse(cls, data: Any) -> "UserTrade":
        if not isinstance(data, dict):
            raise DataError(f"userTrades: строка не объект: {str(data)[:200]}")
        what = "userTrades"
        t = data.get("time")
        if isinstance(t, bool) or not isinstance(t, int):
            raise DataError(f"userTrades: нет целого поля time в {str(data)[:200]}")
        return cls(symbol=_text(data, "symbol", what), side=_text(data, "side", what), qty=_num(data, "qty", what), price=_num(data, "price", what),
                   commission=_num(data, "commission", what, default=0.0), commission_asset=str(data.get("commissionAsset", "")),
                   realized_pnl=_num(data, "realizedPnl", what, default=0.0), time_ms=t)


# --------------------------------------------------------------------------------------------- клиент


class BinanceLiveClient:
    def __init__(self, rest: SignedRest, *, id_prefix: str | None = None):
        self._rest = rest
        self.id_prefix = id_prefix if id_prefix is not None else config.CLIENT_ORDER_ID_PREFIX
        self._filters: dict[str, SymbolFilters] = {}

    @classmethod
    def from_env(cls, session: aiohttp.ClientSession, base_url: str | None = None) -> "BinanceLiveClient":
        """Клиент с ключами из окружения (BINANCE_API_KEY / BINANCE_API_SECRET). Нет ключей - ConfigError."""
        creds = Credentials.from_env()
        return cls(SignedRest(session, base_url or config.LIVE_REST_URL, creds))

    @property
    def rest(self) -> SignedRest:
        return self._rest

    # ------------------------------------------------------------------------------- фильтры и сетка

    async def load_filters(self, symbols: Collection[str]) -> None:
        """Фильтры символов с биржи (публичный exchangeInfo): без них ордера не отправляются."""
        self._filters.update(await fetch_symbol_filters(self._rest, symbols))

    def set_filters(self, filters: Mapping[str, SymbolFilters]) -> None:
        self._filters.update(filters)

    def filters(self, symbol: str) -> SymbolFilters:
        f = self._filters.get(symbol)
        if f is None:
            raise ConfigError(f"Фильтры {symbol} не загружены (load_filters): ордер нельзя проверить на сетку и минимумы")
        return f

    def round_qty(self, symbol: str, qty: float) -> float:
        """Объём ВНИЗ до шага лота: риск никогда не больше рассчитанного."""
        step = Decimal(repr(self.filters(symbol).step_size))
        return float((Decimal(repr(float(qty))) / step).to_integral_value(rounding=ROUND_FLOOR) * step)

    def round_price(self, symbol: str, price: float) -> float:
        """Цена к ближайшему шагу цены."""
        tick = Decimal(repr(self.filters(symbol).tick_size))
        return float((Decimal(repr(float(price))) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick)

    def _check_side(self, side: str) -> None:
        if side not in SIDES:
            raise OrderValidationError(f"side должен быть из {SIDES}, получено {side!r}")

    def _check_qty(self, symbol: str, qty: float, *, entry_price: float | None) -> None:
        f = self.filters(symbol)
        if not (math.isfinite(qty) and qty > 0) or not _on_grid(qty, f.step_size):
            raise OrderValidationError(f"{symbol}: объём {qty!r} не на сетке лота {f.step_size}")
        if qty < f.min_qty:
            raise OrderValidationError(f"{symbol}: объём {qty} меньше minQty {f.min_qty}")
        if entry_price is not None and qty * entry_price < f.min_notional:  # reduceOnly-ордера минимальной суммы не требуют
            raise OrderValidationError(f"{symbol}: сумма {qty * entry_price:.4f} меньше MIN_NOTIONAL {f.min_notional}")

    def _check_price(self, symbol: str, price: float, what: str) -> None:
        f = self.filters(symbol)
        if not (math.isfinite(price) and price > 0) or not _on_grid(price, f.tick_size):
            raise OrderValidationError(f"{symbol}: {what} {price!r} не на сетке цены {f.tick_size}")

    # --------------------------------------------------------------------------------- счёт и позиции

    async def get_balance(self, asset: str = "USDT") -> Balance:
        """Баланс актива из `config.BALANCE_PATH` (по умолчанию /fapi/v3/balance; ❓ формат сверить на testnet)."""
        data = await self._rest.request("GET", config.BALANCE_PATH)
        if not isinstance(data, list):
            raise DataError(f"balance: ожидается список, получено {str(data)[:200]}")
        for row in data:
            if isinstance(row, dict) and row.get("asset") == asset:
                return Balance(
                    asset, wallet=_num(row, "balance", "balance"), available=_num(row, "availableBalance", "balance"),
                    unrealized_pnl=_num(row, "crossUnPnl", "balance", default=0.0),
                )
        raise DataError(f"balance: актива {asset} нет в ответе")

    async def get_position(self, symbol: str) -> "PositionInfo":
        """Позиция по символу (One-Way): объём со знаком и средняя цена входа (0.0, пока позиции нет)."""
        data = await self._rest.request("GET", config.POSITION_RISK_PATH, {"symbol": symbol})
        if not isinstance(data, list):
            raise DataError(f"positionRisk: ожидается список, получено {str(data)[:200]}")
        if not data:  # по символу не было позиции - Binance (testnet и mainnet) отвечает пустым списком: это нулевая позиция, не ошибка
            return PositionInfo(symbol, 0.0, 0.0)
        rows = [r for r in data if isinstance(r, dict) and r.get("symbol") == symbol]
        if not rows:
            raise DataError(f"positionRisk: нет строки по {symbol}: {str(data)[:200]}")
        amt = sum(_num(r, "positionAmt", "positionRisk") for r in rows)
        entry = _num(rows[0], "entryPrice", "positionRisk", default=0.0)
        return PositionInfo(symbol, amt, entry)

    async def get_position_amt(self, symbol: str) -> float:
        """Чистая позиция по символу (One-Way): > 0 лонг, < 0 шорт, 0 нет."""
        return (await self.get_position(symbol)).amt

    async def get_mark_price(self, symbol: str) -> float:
        """Mark price биржи (публичный premiumIndex)."""
        data = await self._rest.request("GET", config.PREMIUM_INDEX_PATH, {"symbol": symbol}, signed=False)
        if not isinstance(data, dict):
            raise DataError(f"premiumIndex {symbol}: ожидается объект, получено {str(data)[:200]}")
        price = _num(data, "markPrice", "premiumIndex")
        if price <= 0:
            raise DataError(f"premiumIndex {symbol}: markPrice {price}")
        return price

    async def get_open_algo_orders(self, symbol: str) -> list[ExchangeAlgoOrder]:
        """Живые algo-ордера по символу (❓ путь и поля `openAlgoOrders` сверены только по сводке документации)."""
        raw = await self._rest.request("GET", config.OPEN_ALGO_ORDERS_PATH, {"symbol": symbol})
        return list(parse_algo_orders(raw).values())

    async def realized_pnl_since(self, start_ms: int, end_ms: int | None = None) -> float:
        """Суммарный результат счёта с `start_ms`: REALIZED_PNL + COMMISSION + FUNDING_FEE (USDT, убыток отрицателен).

        Источник правды - журнал начислений биржи (`/fapi/v1/income`), а не локальный учёт: считает и сделки,
        закрытые стопом на бирже, пока бот был выключен. Страницы по 1000 записей; упёрлись в INCOME_MAX_PAGES - ошибка,
        а не недосчитанный (слишком оптимистичный) убыток.
        """
        total, cursor = 0.0, start_ms
        for _ in range(INCOME_MAX_PAGES):
            params: dict[str, Any] = {"startTime": cursor, "endTime": end_ms, "limit": INCOME_PAGE}
            rows = await self._rest.request("GET", config.INCOME_PATH, params)
            if not isinstance(rows, list):
                raise DataError(f"income: ожидается список, получено {str(rows)[:200]}")
            for row in rows:
                if not isinstance(row, dict):
                    raise DataError(f"income: строка не объект: {row!r}")
                if row.get("incomeType") in INCOME_TYPES:
                    total += _num(row, "income", "income")
            if len(rows) < INCOME_PAGE:
                return total
            cursor = max(int(r["time"]) for r in rows) + 1
        raise DataError(f"income: больше {INCOME_MAX_PAGES} страниц с {start_ms}: суточный убыток недосчитан бы")

    async def user_trades(self, symbol: str, start_ms: int, end_ms: int) -> list[UserTrade]:
        """Исполнения по символу за [start_ms, end_ms] по времени (окнами по 7 суток - ограничение биржи). Окно, упёршееся в 1000 строк, - DataError:
        недобранные исполнения дали бы неверную цену выхода при сверке."""
        if end_ms < start_ms:
            raise DataError(f"userTrades {symbol}: конец {end_ms} раньше начала {start_ms}")
        out: list[UserTrade] = []
        cursor = start_ms
        for _ in range(USER_TRADES_MAX_WINDOWS):
            stop = min(cursor + USER_TRADES_WINDOW_MS - 1, end_ms)
            rows = await self._rest.request("GET", config.USER_TRADES_PATH, {"symbol": symbol, "startTime": cursor, "endTime": stop, "limit": USER_TRADES_PAGE})
            if not isinstance(rows, list):
                raise DataError(f"userTrades: ожидается список, получено {str(rows)[:200]}")
            if len(rows) >= USER_TRADES_PAGE:
                raise DataError(f"userTrades {symbol}: в окне {cursor}..{stop} не меньше {USER_TRADES_PAGE} исполнений - выборка неполная")
            out += [UserTrade.parse(r) for r in rows]
            if stop >= end_ms:
                return sorted(out, key=lambda u: u.time_ms)
            cursor = stop + 1
        raise DataError(f"userTrades {symbol}: больше {USER_TRADES_MAX_WINDOWS} окон по 7 суток с {start_ms}")

    # ---------------------------------------------------------------------- плечо и режим маржи

    async def set_leverage(self, symbol: str, leverage: int) -> int:
        if not (isinstance(leverage, int) and 1 <= leverage <= 125):
            raise OrderValidationError(f"Плечо должно быть целым 1..125, получено {leverage!r}")
        data = await self._rest.request("POST", config.LEVERAGE_PATH, {"symbol": symbol, "leverage": leverage})
        applied = data.get("leverage") if isinstance(data, dict) else None
        if applied != leverage:
            raise DataError(f"{symbol}: запрошено плечо {leverage}, биржа применила {applied!r}")
        return leverage

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> None:
        """ISOLATED: потеря ограничена маржей сделки, а не всем счётом. «Уже установлено» (-4046) - успех."""
        if margin_type not in ("ISOLATED", "CROSSED"):
            raise OrderValidationError(f"marginType должен быть ISOLATED или CROSSED, получено {margin_type!r}")
        try:
            await self._rest.request("POST", config.MARGIN_TYPE_PATH, {"symbol": symbol, "marginType": margin_type})
        except BinanceAPIError as exc:
            if exc.code != ERR_NO_NEED_TO_CHANGE_MARGIN_TYPE:
                raise

    # ------------------------------------------------------------------------------------- ордера

    async def market_order(self, symbol: str, side: str, qty: float, *, client_id: str, reduce_only: bool = False,
                           ref_price: float | None = None) -> OrderResult:
        """MARKET. `ref_price` - ориентир цены для проверки MIN_NOTIONAL входа (для reduce_only не нужен)."""
        self._check_side(side)
        self._check_qty(symbol, qty, entry_price=None if reduce_only else _need(ref_price, "ref_price"))
        params = {
            "symbol": symbol, "side": side, "type": "MARKET", "quantity": fmt_decimal(qty), "newClientOrderId": client_id,
            "newOrderRespType": "RESULT", "reduceOnly": True if reduce_only else None,
        }
        return OrderResult.parse(await self._rest.request("POST", config.ORDER_PATH, params))

    async def limit_order(self, symbol: str, side: str, qty: float, price: float, *, client_id: str, post_only: bool = False,
                          reduce_only: bool = False) -> OrderResult:
        """LIMIT GTC (post_only=True -> GTX: ордер, который стал бы тейкером, биржа не принимает)."""
        self._check_side(side)
        self._check_price(symbol, price, "цена лимитки")
        self._check_qty(symbol, qty, entry_price=None if reduce_only else price)
        params = {
            "symbol": symbol, "side": side, "type": "LIMIT", "quantity": fmt_decimal(qty), "price": fmt_decimal(price),
            "timeInForce": "GTX" if post_only else "GTC", "newClientOrderId": client_id, "newOrderRespType": "RESULT",
            "reduceOnly": True if reduce_only else None,
        }
        return OrderResult.parse(await self._rest.request("POST", config.ORDER_PATH, params))

    async def stop_loss(self, symbol: str, side: str, trigger_price: float, *, client_id: str, qty: float | None = None,
                        close_position: bool = False) -> AlgoResult:
        """STOP_MARKET (Algo Service). `side` - сторона ЗАКРЫТИЯ (для лонга SELL). Сработает по `workingType`."""
        return await self._algo(symbol, side, "STOP_MARKET", trigger_price, qty, client_id, close_position)

    async def take_profit(self, symbol: str, side: str, trigger_price: float, *, client_id: str, qty: float | None = None,
                          close_position: bool = False) -> AlgoResult:
        """TAKE_PROFIT_MARKET (Algo Service). `side` - сторона закрытия."""
        return await self._algo(symbol, side, "TAKE_PROFIT_MARKET", trigger_price, qty, client_id, close_position)

    async def _algo(self, symbol: str, side: str, order_type: str, trigger: float, qty: float | None, cid: str,
                    close_position: bool) -> AlgoResult:
        self._check_side(side)
        self._check_price(symbol, trigger, "триггер")
        params: dict[str, Any] = {
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side, "type": order_type, "triggerPrice": fmt_decimal(trigger),
            "workingType": config.ALGO_WORKING_TYPE, "clientAlgoId": cid,
        }
        if close_position:
            if qty is not None:
                raise OrderValidationError("closePosition=true несовместим с quantity")
            params["closePosition"] = True
        else:
            if qty is None:
                raise OrderValidationError("Нужен qty (или close_position=True)")
            self._check_qty(symbol, qty, entry_price=None)
            params["quantity"], params["reduceOnly"] = fmt_decimal(qty), True
        return AlgoResult.parse(await self._rest.request("POST", config.ALGO_ORDER_PATH, params))

    async def cancel_order(self, symbol: str, client_id: str) -> OrderResult:
        return OrderResult.parse(await self._rest.request("DELETE", config.ORDER_PATH, {"symbol": symbol, "origClientOrderId": client_id}))

    async def get_order(self, symbol: str, client_id: str) -> OrderResult:
        return OrderResult.parse(await self._rest.request("GET", config.ORDER_PATH, {"symbol": symbol, "origClientOrderId": client_id}))

    async def cancel_algo_order(self, client_id: str) -> None:
        """Отмена по clientAlgoId. Отказ биржи - BinanceAPIError (что значит код, решает вызывающий: см. get_algo_order)."""
        data = await self._rest.request("DELETE", config.ALGO_ORDER_PATH, {"clientAlgoId": client_id})
        if not isinstance(data, dict) or data.get("clientAlgoId") != client_id:
            raise DataError(f"cancel algoOrder {client_id}: неожиданный ответ {str(data)[:200]}")

    async def get_algo_order(self, client_id: str) -> AlgoResult:
        return AlgoResult.parse(await self._rest.request("GET", config.ALGO_ORDER_PATH, {"clientAlgoId": client_id}))


def _need(value: float | None, name: str) -> float:
    if value is None or not (math.isfinite(value) and value > 0):
        raise OrderValidationError(f"{name} обязателен и положителен для ордера, увеличивающего позицию")
    return value
