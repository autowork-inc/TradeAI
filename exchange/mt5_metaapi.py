"""Асинхронный клиент традиционных рынков (Forex, металлы, CFD на крипту) через облачный шлюз MetaApi -> MetaTrader 5.

Почему MetaApi, а не пакет `MetaTrader5`. Наш VPS - Linux, а `MetaTrader5` работает только в Windows (общается с локальным
терминалом через DLL). MetaApi (metaapi.cloud) держит терминал MT5 у себя и отдаёт WebSocket/REST; мы работаем из Python
на любой ОС через `metaapi-cloud-sdk` (импорт `metaapi_cloud_sdk`). Используется RPC-соединение SDK
(`account.get_rpc_connection()`): запрос-ответ без локального зеркала терминала; это легче потокового режима и не держит
состояние позиций в памяти процесса (источник правды - терминал).

Жизненный цикл `connect()` (каждый шаг ограничен таймаутом; таймаут = `MetaApiError`, а не «продолжаем вслепую», R1):
    MetaApi(token) -> get_account(id) -> [deploy, если не DEPLOYED/DEPLOYING] -> wait_deployed -> wait_connected
    (терминал связан с БРОКЕРОМ: `connection_status == CONNECTED`) -> get_rpc_connection().connect() -> wait_synchronized.
Отдельно `connection_state()` отдаёт состояние аккаунта и связи с брокером без торговых действий.

Соответствие `exchange.binance_live.BinanceLiveClient` и отличия MT5 (их нельзя терять при подключении к LiveExecutionEngine):
    * Объём - в ЛОТАХ (`volumeStep` / `minVolume` / `maxVolume` из спецификации символа), а не в монетах. Параметр здесь назван `qty`
      (как у Binance), но это лоты.
    * SL/TP - поля ПОЗИЦИИ (или ордера), а не отдельные условные ордера: нет Algo Service, нет `reduceOnly`. Перенос стопа
      (`stop_loss`) - атомарная модификация позиции, окна без стопа нет. Частичный тейк (Scale Out) - `close_position(lots=...)`.
    * Нет post-only: лимитка может исполниться как тейкер, если цена ушла.
    * Нет единой mark price: стопы и вход считаются по Bid/Ask своей стороны (шорт закрывается по Ask).
    * Торговые часы: вне сессии (выходные, дневная пауза) брокер отклоняет ордера (`TRADE_RETCODE_MARKET_CLOSED`).
    * Брокер задаёт минимальную дистанцию стопа (`stopsLevel`) и шаг цены (`tickSize`); неверные значения отклоняются брокером.
    * Счёт может быть netting (одна позиция на символ) или hedging (несколько); методы работают с `position_id`.
    * Деньги в валюте депозита счёта (USD, EUR...), не USDT.

Fail-Fast (R1): ключи читаются из окружения в конструкторе; нет `METAAPI_TOKEN` или id аккаунта - мгновенный `ConfigError`
(код выхода 3) до любого подключения. Исключения SDK переводятся в типизированные: `TradeException` -> `MetaApiTradeError`
(с кодами брокера), 401/403 -> `ConfigError`, таймауты/нет связи/нет синхронизации/прочие ошибки API -> `MetaApiError`.
Секрет в `repr`, логи и исключения не попадает.

Переменные окружения:
    METAAPI_TOKEN        токен API MetaApi (https://app.metaapi.cloud/token)
    METAAPI_ACCOUNT_ID   id MetaApi-аккаунта (синоним `ACCOUNT_ID`; если заданы оба - берётся METAAPI_ACCOUNT_ID)

❓ Не проверено на живом брокере: исполнение ордеров (методы торговли покрыты тестами только на имитации SDK), ограничение
длины `clientId`, поведение `POSITION_MODIFY` при частично заданных SL/TP (поэтому `stop_loss`/`take_profit` всегда
передают оба уровня), обработка выходных и requote.
"""

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import config  # noqa: F401  (подгружает .env в окружение процесса, как в exchange.binance_live)
from exchange.binance_live import Balance, PositionInfo
from utils.errors import ConfigError, TradingInfraError

log = logging.getLogger(__name__)

TOKEN_ENV = "METAAPI_TOKEN"
ACCOUNT_ENV = "ACCOUNT_ID"
ACCOUNT_ENV_ALIAS = "METAAPI_ACCOUNT_ID"

SIDES = ("BUY", "SELL")
# (режим в спецификации символа, режим в запросе) в порядке предпочтения для MARKET
_FILLING_PRIORITY = (
    ("SYMBOL_FILLING_FOK", "ORDER_FILLING_FOK"),
    ("SYMBOL_FILLING_IOC", "ORDER_FILLING_IOC"),
    ("SYMBOL_FILLING_RETURN", "ORDER_FILLING_RETURN"),
)
DEFAULT_TIMEOUT_S = 120.0  # на каждый этап подключения (развёртывание, связь с брокером, синхронизация)
REQUEST_TIMEOUT_S = 60.0  # таймаут одного запроса к MetaApi (SDK по умолчанию 60 с)


class MetaApiError(TradingInfraError):
    """Сбой связи с MetaApi/брокером (таймаут, нет синхронизации, ошибка API). Повтор возможен, исход ордера может быть неизвестен."""


# Коды возврата торгового сервера MT5 (TRADE_RETCODE_*, https://www.mql5.com/en/docs/constants/structures/mqltraderesult):
# код -> (имя, пояснение, повтор допустим, исход ордера неизвестен). «Повтор допустим» - запрос заведомо не исполнен и
# условия могут измениться (цена, перегрузка); «исход неизвестен» - ордер мог дойти до брокера: перед повтором сверять по clientId.
RETCODES: dict[int, tuple[str, str, bool, bool]] = {
    10004: ("REQUOTE", "реквота: цена изменилась, запрос отклонён", True, False),
    10006: ("REJECT", "запрос отклонён брокером", False, False),
    10007: ("CANCEL", "запрос отменён трейдером", False, False),
    10011: ("ERROR", "ошибка обработки запроса", False, True),
    10012: ("TIMEOUT", "таймаут запроса на торговом сервере", False, True),
    10013: ("INVALID", "неправильный запрос (параметры ордера)", False, False),
    10014: ("INVALID_VOLUME", "неправильный объём (вне min/max или не кратен шагу)", False, False),
    10015: ("INVALID_PRICE", "неправильная цена", False, False),
    10016: ("INVALID_STOPS", "неправильные стопы (SL/TP ближе допустимого stopsLevel или не по ту сторону цены)", False, False),
    10017: ("TRADE_DISABLED", "торговля запрещена (счёт или символ)", False, False),
    10018: ("MARKET_CLOSED", "рынок закрыт", True, False),
    10019: ("NO_MONEY", "не хватает средств для запроса (маржа)", False, False),
    10020: ("PRICE_CHANGED", "цены изменились", True, False),
    10021: ("PRICE_OFF", "нет котировок для обработки запроса", True, False),
    10022: ("INVALID_EXPIRATION", "неверный срок действия ордера", False, False),
    10023: ("ORDER_CHANGED", "состояние ордера изменилось", False, True),
    10024: ("TOO_MANY_REQUESTS", "слишком частые запросы", True, False),
    10025: ("NO_CHANGES", "в запросе нет изменений", False, False),
    10026: ("SERVER_DISABLES_AT", "автотрейдинг запрещён сервером", False, False),
    10027: ("CLIENT_DISABLES_AT", "автотрейдинг запрещён терминалом клиента", False, False),
    10028: ("LOCKED", "запрос заблокирован для обработки", True, False),
    10029: ("FROZEN", "ордер или позиция заморожены (freezeLevel)", True, False),
    10030: ("INVALID_FILL", "неподдерживаемый тип исполнения (filling mode) для символа", False, False),
    10031: ("CONNECTION", "нет соединения с торговым сервером", True, True),
    10032: ("ONLY_REAL", "операция разрешена только для реальных счетов", False, False),
    10033: ("LIMIT_ORDERS", "достигнут лимит отложенных ордеров", False, False),
    10034: ("LIMIT_VOLUME", "достигнут лимит объёма ордеров и позиций по символу", False, False),
    10035: ("INVALID_ORDER", "неверный или запрещённый тип ордера", False, False),
    10036: ("POSITION_CLOSED", "позиция уже закрыта", False, False),
    10038: ("INVALID_CLOSE_VOLUME", "объём закрытия больше объёма позиции", False, False),
    10039: ("CLOSE_ORDER_EXIST", "ордер на закрытие этой позиции уже существует", False, False),
    10040: ("LIMIT_POSITIONS", "достигнут лимит открытых позиций", False, False),
    10041: ("REJECT_CANCEL", "отложенный ордер отклонён, запрос отменён", False, False),
    10042: ("LONG_ONLY", "разрешены только длинные позиции", False, False),
    10043: ("SHORT_ONLY", "разрешены только короткие позиции", False, False),
    10044: ("CLOSE_ONLY", "разрешено только закрытие позиций", False, False),
    10045: ("FIFO_CLOSE", "закрытие запрещено правилом FIFO (закрывайте старые позиции первыми)", False, False),
    10046: ("HEDGE_PROHIBITED", "встречные позиции по символу запрещены (счёт без хеджирования)", False, False),
}
RETCODES_OK = frozenset({0, 10008, 10009, 10010, 10025})  # успех: 0, PLACED, DONE, DONE_PARTIAL, NO_CHANGES (как в документации MetaApi)
RETCODE_INVALID_FILL = 10030


class MetaApiTradeError(MetaApiError):
    """Брокер/терминал отклонил торговую операцию.

    `numeric_code` / `string_code` - коды MT5 как их вернул MetaApi (10018 / TRADE_RETCODE_MARKET_CLOSED); `reason` - пояснение
    по-русски; `retryable` - запрос заведомо не исполнен, повтор позже имеет смысл (реквота, рынок закрыт, нет котировок);
    `outcome_unknown` - ордер мог дойти до брокера (таймаут, обрыв): перед повтором сверить по clientId.
    """

    def __init__(self, message: str, numeric_code: int | None = None, string_code: str | None = None):
        known = RETCODES.get(numeric_code) if numeric_code is not None else None
        self.numeric_code = numeric_code
        self.string_code = string_code
        self.retcode_name = f"TRADE_RETCODE_{known[0]}" if known else string_code
        self.reason = known[1] if known else "неизвестный код возврата брокера"
        self.retryable = known[2] if known else False
        self.outcome_unknown = known[3] if known else True  # неизвестный код - не считаем ордер «точно не исполненным»
        super().__init__(f"{message} [{numeric_code} {self.retcode_name}: {self.reason}]" if numeric_code is not None else message)


RETCODE_MARKET_CLOSED = 10018
_DEALS_PAGE, _DEALS_MAX = 1000, 20_000
_PNL_DEAL_TYPES = frozenset({"DEAL_TYPE_BUY", "DEAL_TYPE_SELL", "DEAL_TYPE_COMMISSION", "DEAL_TYPE_COMMISSION_DAILY", "DEAL_TYPE_COMMISSION_MONTHLY",
                             "DEAL_TYPE_CHARGE", "DEAL_TYPE_INTEREST"})  # деньги, заработанные/потерянные торговлей; DEAL_TYPE_BALANCE - нет
TRADE_MODE_FULL = "SYMBOL_TRADE_MODE_FULL"


class MarketClosedError(MetaApiTradeError):
    """Рынок/символ сейчас недоступен для НОВЫХ входов: брокер ответил 10018 MARKET_CLOSED либо `tradeMode` символа не FULL
    (выходные, дневная пауза, CLOSEONLY, DISABLED...). Это обычный отказ сигнала, а не сбой: Safe Mode не включается, процесс не падает -
    вызывающий (`exchange.router.ExecutionRouter`) превращает его в отказ `market_closed`."""

    def __init__(self, message: str, numeric_code: int | None = RETCODE_MARKET_CLOSED, string_code: str | None = "TRADE_RETCODE_MARKET_CLOSED"):
        super().__init__(message, numeric_code, string_code)


def _trade_error(message: str, numeric_code: int | None, string_code: str | None) -> MetaApiTradeError:
    """Код 10018 - `MarketClosedError`, остальные - `MetaApiTradeError`."""
    cls = MarketClosedError if numeric_code == RETCODE_MARKET_CLOSED else MetaApiTradeError
    return cls(message, numeric_code, string_code)


@dataclass(frozen=True)
class MetaApiCredentials:
    token: str
    account_id: str

    def __repr__(self) -> str:  # токен не должен попадать в логи и трейсбеки
        return f"MetaApiCredentials(account_id={self.account_id!r}, token=***)"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "MetaApiCredentials":
        """Токен и аккаунт из окружения. Любого нет или пуст - `ConfigError` до какого-либо обращения к сети."""
        src = os.environ if env is None else env
        token = (src.get(TOKEN_ENV) or "").strip()
        account_id = (src.get(ACCOUNT_ENV_ALIAS) or src.get(ACCOUNT_ENV) or "").strip()
        bad = next((i for i, ch in enumerate(token) if not 33 <= ord(ch) <= 126), None)
        if bad is not None:  # токен MetaApi - JWT (только печатный ASCII); иначе httpx упал бы на заголовке с UnicodeEncodeError
            raise ConfigError(f"MetaApi: {TOKEN_ENV} повреждён - посторонний символ на позиции {bad} (токен состоит только из латиницы, цифр и . _ -); скопируйте токен заново")
        missing = [name for name, value in ((TOKEN_ENV, token), (ACCOUNT_ENV_ALIAS, account_id)) if not value]
        if missing:
            raise ConfigError(f"MetaApi: не заданы переменные окружения {', '.join(missing)} (токен и id аккаунта: https://app.metaapi.cloud)")
        return cls(token=token, account_id=account_id)


@dataclass(frozen=True)
class ConnectionState:
    """Состояние аккаунта в облаке и связи терминала с брокером (без торговых действий)."""

    account_id: str
    name: str
    login: str
    server: str
    platform: str  # mt4 | mt5
    account_type: str  # cloud-g1 | cloud-g2 ...
    state: str  # DEPLOYED | DEPLOYING | UNDEPLOYED ...
    connection_status: str  # CONNECTED | DISCONNECTED | DISCONNECTED_FROM_BROKER
    region: str

    @property
    def broker_connected(self) -> bool:
        return self.state == "DEPLOYED" and self.connection_status == "CONNECTED"


@dataclass(frozen=True)
class AccountInfo:
    """Сводка счёта MT5 (валюта депозита)."""

    broker: str
    server: str
    currency: str
    balance: float
    equity: float
    margin: float
    free_margin: float
    leverage: float
    trade_allowed: bool
    margin_mode: str
    name: str
    login: int | None


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: float
    ask: float
    time: datetime | None
    loss_tick_value: float | None = None  # потеря за 1 тик на 1 лот в ВАЛЮТЕ СЧЁТА (для расчёта лота, exchange.mt5_risk)
    profit_tick_value: float | None = None

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(frozen=True)
class MetaApiPosition:
    position_id: str
    symbol: str
    side: str  # BUY | SELL
    lots: float
    open_price: float
    stop_loss: float | None  # None - стопа нет
    take_profit: float | None
    profit: float
    client_id: str | None


@dataclass(frozen=True)
class MetaApiOrder:
    """Результат торговой операции или отложенный ордер."""

    symbol: str
    client_id: str | None
    order_id: str | None  # ticket ордера у брокера
    position_id: str | None
    state: str  # результат: DONE | PLACED | ...; для отложенного ордера - состояние MT5 (ORDER_STATE_PLACED...)
    message: str
    numeric_code: int | None = None
    string_code: str | None = None

    @property
    def partial(self) -> bool:
        """DONE_PARTIAL: исполнена только часть объёма (возможно при IOC; при FOK не бывает) - вызывающий обязан сверить позицию."""
        return self.numeric_code == 10010


# ----------------------------------------------------------------------------------------------- вспомогательное


def _opt(value: Any) -> float | None:
    """В MT5 отсутствие SL/TP - 0 или поле отсутствует."""
    return float(value) if value else None


def _num(data: Mapping[str, Any], key: str, what: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetaApiError(f"{what}: поле {key!r} отсутствует или не число: {value!r}")
    return float(value)


def _side(side: str) -> str:
    if side not in SIDES:
        raise ConfigError(f"side должен быть BUY или SELL, получено {side!r}")
    return side


def _positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        raise ConfigError(f"{name} должен быть положительным числом, получено {value!r}")
    return float(value)


def _position(raw: Mapping[str, Any]) -> MetaApiPosition:
    kind = str(raw.get("type", ""))
    if kind not in ("POSITION_TYPE_BUY", "POSITION_TYPE_SELL"):
        raise MetaApiError(f"позиция {raw.get('id')!r}: неизвестный тип {kind!r}")
    return MetaApiPosition(
        position_id=str(raw["id"]), symbol=str(raw["symbol"]), side="BUY" if kind.endswith("BUY") else "SELL",
        lots=_num(raw, "volume", "position"), open_price=_num(raw, "openPrice", "position"), stop_loss=_opt(raw.get("stopLoss")),
        take_profit=_opt(raw.get("takeProfit")), profit=float(raw.get("profit") or 0.0), client_id=raw.get("clientId"),
    )


def _order_from_trade(symbol: str, client_id: str | None, resp: Mapping[str, Any]) -> MetaApiOrder:
    """Ответ MetaApi на торговый запрос -> `MetaApiOrder`. SDK сам бросает TradeException на отказ; код вне списка успехов
    в нормальном ответе - тоже ошибка (страховка: молча принять отказ за успех нельзя)."""
    code = resp.get("numericCode")
    if code is not None and code not in RETCODES_OK:
        raise _trade_error(f"{symbol}: {resp.get('message', '')}", code, resp.get("stringCode"))
    return MetaApiOrder(
        symbol=symbol, client_id=client_id, order_id=resp.get("orderId"), position_id=resp.get("positionId"),
        state=str(resp.get("stringCode", "")), message=str(resp.get("message", "")), numeric_code=code, string_code=resp.get("stringCode"),
    )


class MetaApiLiveClient:
    """Клиент приватного API MT5 через MetaApi: операции `BinanceLiveClient` в терминах MT5 (лоты, SL/TP позиции).

    Создание объекта НЕ подключается к сети: `__init__` только проверяет ключи (Fail-Fast). Подключение - `connect()`;
    использование - `async with MetaApiLiveClient() as client:` либо `connect()` / `close()` вручную.
    """

    def __init__(self, env: Mapping[str, str] | None = None, *, id_prefix: str | None = None):
        self._creds = MetaApiCredentials.from_env(env)  # ConfigError здесь - раньше любого подключения
        self.id_prefix = id_prefix if id_prefix is not None else config.CLIENT_ORDER_ID_PREFIX
        self._api: Any = None  # metaapi_cloud_sdk.MetaApi (SDK импортируется лениво, чтобы модуль грузился без него)
        self._account: Any = None  # MetatraderAccount
        self._connection: Any = None  # RpcMetaApiConnectionInstance
        self._specs: dict[str, Mapping[str, Any]] = {}  # спецификации символов брокера (после load_symbols)

    def __repr__(self) -> str:
        return f"MetaApiLiveClient(account_id={self._creds.account_id!r}, connected={self._connection is not None})"

    @property
    def account_id(self) -> str:
        return self._creds.account_id

    async def __aenter__(self) -> "MetaApiLiveClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------------------- перевод исключений SDK

    @staticmethod
    async def _call(what: str, awaitable: Any) -> Any:
        """Выполняет вызов SDK и переводит исключения SDK в типизированные ошибки проекта. Чужие исключения (ошибки кода) не глотаются."""
        from metaapi_cloud_sdk.clients.error_handler import ApiException, ForbiddenException, UnauthorizedException
        from metaapi_cloud_sdk.clients.metaapi.not_connected_exception import NotConnectedException
        from metaapi_cloud_sdk.clients.metaapi.not_synchronized_exception import NotSynchronizedException
        from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException
        from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

        try:
            return await awaitable
        except TradeException as exc:
            raise _trade_error(f"{what}: брокер отклонил операцию: {exc}", getattr(exc, "numericCode", None), getattr(exc, "stringCode", None)) from exc
        except (UnauthorizedException, ForbiddenException) as exc:
            raise ConfigError(f"{what}: MetaApi отказал в доступе (токен неверен или не имеет прав на аккаунт): {exc}") from exc
        except TimeoutException as exc:
            raise MetaApiError(f"{what}: таймаут MetaApi: {exc}") from exc
        except (NotConnectedException, NotSynchronizedException) as exc:
            raise MetaApiError(f"{what}: терминал не подключён/не синхронизирован: {exc}") from exc
        except ApiException as exc:
            raise MetaApiError(f"{what}: ошибка API MetaApi: {exc}") from exc

    def _rpc(self) -> Any:
        if self._connection is None:
            raise MetaApiError("MetaApiLiveClient не подключён: сначала await connect()")
        return self._connection

    # ------------------------------------------------------------------------------- подключение

    async def connect(self, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        """Подключиться к аккаунту: развернуть (DEPLOYED), дождаться связи терминала с брокером (CONNECTED) и синхронизации.

        Таймаут любого этапа - `MetaApiError` (R1). Повторный вызов на подключённом клиенте ничего не делает.
        """
        if self._connection is not None:
            return
        from metaapi_cloud_sdk import MetaApi

        self._api = MetaApi(self._creds.token, {"requestTimeout": REQUEST_TIMEOUT_S})
        try:
            account = await self._call("get_account", self._api.metatrader_account_api.get_account(self._creds.account_id))
            if account.state not in ("DEPLOYING", "DEPLOYED"):
                log.info("MetaApi: аккаунт в состоянии %s - разворачиваю", account.state)
                await self._call("deploy", account.deploy())
            await self._call("wait_deployed", account.wait_deployed(timeout_in_seconds=timeout_s))
            await self._call("wait_connected (терминал-брокер)", account.wait_connected(timeout_in_seconds=timeout_s))
            connection = account.get_rpc_connection()
            await self._call("rpc connect", connection.connect())
            await self._call("wait_synchronized", connection.wait_synchronized(timeout_in_seconds=timeout_s))
        except BaseException:
            self._api.close()  # не оставляем фоновых соединений SDK при неудачном подключении
            self._api = None
            raise
        self._account, self._connection = account, connection
        log.info("MetaApi: подключено, аккаунт %s (%s, %s)", account.name, account.server, account.connection_status)

    async def close(self) -> None:
        """Закрыть соединение с MetaApi (позиции и ордера на стороне брокера не затрагиваются)."""
        connection, api = self._connection, self._api
        self._connection = self._account = self._api = None
        try:
            if connection is not None:
                await connection.close()
        finally:
            if api is not None:
                api.close()

    def connection_state(self) -> ConnectionState:
        """Состояние аккаунта и связи с брокером по данным, полученным при подключении (`reload()` обновляет)."""
        if self._account is None:
            raise MetaApiError("MetaApiLiveClient не подключён: сначала await connect()")
        a = self._account
        return ConnectionState(
            account_id=a.id, name=a.name, login=str(a.login), server=a.server, platform=f"mt{a.version}",
            account_type=a.type, state=a.state, connection_status=a.connection_status, region=a.region,
        )

    async def refresh_connection_state(self) -> ConnectionState:
        """Перечитать состояние аккаунта из облака и вернуть его."""
        if self._account is None:
            raise MetaApiError("MetaApiLiveClient не подключён: сначала await connect()")
        await self._call("reload", self._account.reload())
        return self.connection_state()

    # ------------------------------------------------------------------------------- спецификации символов

    async def load_symbols(self, symbols: list[str]) -> None:
        """Загрузить спецификации символов (volumeStep/Min/Max, tickSize, digits, stopsLevel). Нужны для проверки лотов и цен."""
        for symbol in symbols:
            await self.get_symbol_specification(symbol)

    def _spec(self, symbol: str) -> Mapping[str, Any]:
        spec = self._specs.get(symbol)
        if spec is None:
            raise ConfigError(f"Нет спецификации {symbol}: сначала await load_symbols([...])")
        return spec

    def round_lots(self, symbol: str, lots: float) -> float:
        """Объём вниз до `volumeStep` брокера (риск не больше рассчитанного); меньше `minVolume` - `ConfigError`."""
        spec = self._spec(symbol)
        step, vmin, vmax = _num(spec, "volumeStep", symbol), _num(spec, "minVolume", symbol), _num(spec, "maxVolume", symbol)
        rounded = round(int(lots / step + 1e-9) * step, 8)
        if rounded < vmin or rounded > vmax:
            raise ConfigError(f"{symbol}: объём {lots} -> {rounded} вне допустимых [{vmin}, {vmax}] (шаг {step})")
        return rounded

    def round_price(self, symbol: str, price: float) -> float:
        """Цена на сетку `tickSize` символа (до знака `digits`)."""
        spec = self._spec(symbol)
        tick, digits = _num(spec, "tickSize", symbol), int(_num(spec, "digits", symbol))
        return round(round(price / tick) * tick, digits)

    def _check_lots(self, symbol: str, lots: float) -> None:
        _positive(lots, "qty (лоты)")
        if symbol in self._specs:  # спецификация загружена - строгая проверка сетки до отправки (R1)
            spec = self._specs[symbol]
            step, vmin, vmax = _num(spec, "volumeStep", symbol), _num(spec, "minVolume", symbol), _num(spec, "maxVolume", symbol)
            if not (vmin - 1e-12 <= lots <= vmax + 1e-12) or abs(lots / step - round(lots / step)) > 1e-6:
                raise ConfigError(f"{symbol}: объём {lots} не на сетке брокера (шаг {step}, диапазон [{vmin}, {vmax}])")

    # ------------------------------------------------------------------------------- счёт, котировки, позиции

    async def get_account_info(self) -> AccountInfo:
        """Баланс, средства (equity), маржа и параметры счёта MT5."""
        info = await self._call("get_account_information", self._rpc().get_account_information())
        return AccountInfo(
            broker=str(info.get("broker", "")), server=str(info.get("server", "")), currency=str(info.get("currency", "")),
            balance=_num(info, "balance", "account"), equity=_num(info, "equity", "account"), margin=_num(info, "margin", "account"),
            free_margin=_num(info, "freeMargin", "account"), leverage=float(info.get("leverage") or 0.0),
            trade_allowed=bool(info.get("tradeAllowed")), margin_mode=str(info.get("marginMode", "")), name=str(info.get("name", "")),
            login=info.get("login"),
        )

    async def get_balance(self) -> Balance:
        """Баланс в формате `BinanceLiveClient`: wallet = balance, available = free margin, unrealized_pnl = equity - balance."""
        info = await self.get_account_info()
        return Balance(asset=info.currency, wallet=info.balance, available=info.free_margin, unrealized_pnl=info.equity - info.balance)

    async def get_price(self, symbol: str) -> Quote:
        """Текущие Bid/Ask терминала. Вне торговых часов вернётся последняя котировка (брокер ордер всё равно отклонит)."""
        raw = await self._call(f"get_symbol_price {symbol}", self._rpc().get_symbol_price(symbol))
        bid, ask = _num(raw, "bid", f"price {symbol}"), _num(raw, "ask", f"price {symbol}")
        if bid <= 0 or ask <= 0 or ask < bid:
            raise MetaApiError(f"{symbol}: некорректная котировка bid={bid} ask={ask}")
        return Quote(symbol, bid, ask, raw.get("time"), _opt(raw.get("lossTickValue")), _opt(raw.get("profitTickValue")))

    async def get_candles(self, symbol: str, timeframe: str, limit: int, *, start_time: datetime | None = None) -> list[tuple[int, float, float, float, float, float]]:
        """Исторические свечи (REST рыночных данных MetaApi): [(open_time_ms UTC, open, high, low, close, tick_volume), ...] по возрастанию времени.

        `start_time` - самое позднее время (свечи грузятся назад от него); без него - последние `limit` свечей (последняя может быть ещё
        формирующейся: отбрасывает вызывающий). `limit` <= 1000. Объём - тиковый (у Forex реального объёма нет). ❓ формат `time` (UTC) сверен по
        документации MetaApi, живым запросом не проверялся."""
        if not 1 <= limit <= 1000:
            raise ConfigError(f"get_candles: limit в [1, 1000], получено {limit}")
        if self._account is None:
            raise MetaApiError("MetaApiLiveClient не подключён: сначала await connect()")
        raw = await self._call(f"get_historical_candles {symbol} {timeframe}", self._account.get_historical_candles(symbol, timeframe, start_time, limit))
        out: list[tuple[int, float, float, float, float, float]] = []
        for c in raw:
            t = c.get("time")
            if not isinstance(t, datetime):
                raise MetaApiError(f"{symbol}: свеча без времени: {c!r}")
            tv = c.get("tickVolume", c.get("volume"))
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)  # MetaApi отдаёт UTC; наивное время считаем UTC, а не местным
            out.append((int(t.timestamp() * 1000), _num(c, "open", symbol), _num(c, "high", symbol), _num(c, "low", symbol), _num(c, "close", symbol),
                        float(tv or 0.0)))
        out.sort()
        return out

    async def get_positions(self) -> list[MetaApiPosition]:
        """Все открытые позиции счёта."""
        raw = await self._call("get_positions", self._rpc().get_positions())
        return [_position(p) for p in raw]

    async def get_position(self, symbol: str) -> PositionInfo:
        """Чистая позиция по символу: лоты со знаком (> 0 buy, < 0 sell) и средневзвешенная цена входа (0.0, пока позиции нет)."""
        rows = [p for p in await self.get_positions() if p.symbol == symbol]
        signed = sum(p.lots if p.side == "BUY" else -p.lots for p in rows)
        total = sum(p.lots for p in rows)
        entry = sum(p.open_price * p.lots for p in rows) / total if total else 0.0
        return PositionInfo(symbol, round(signed, 8), entry)

    async def get_symbols(self) -> list[str]:
        """Имена всех символов, доступных у брокера (с его суффиксами: BTCUSDm, EURUSD.r...)."""
        return list(await self._call("get_symbols", self._rpc().get_symbols()))

    async def realized_pnl_since(self, start_ms: int, end_ms: int | None = None) -> float:
        """Реализованный результат счёта за интервал в валюте счёта: по истории сделок терминала сумма `profit + commission + swap` сделок
        BUY/SELL, комиссий, сборов и процентов. Пополнения/снятия/кредит/бонус (DEAL_TYPE_BALANCE и др.) НЕ входят. Убыток отрицателен.

        Данные истории ещё синхронизируются (`synchronizing`) - `MetaApiError` (неполную сумму принимать нельзя: лимит убытка решает по ней)."""
        from datetime import timezone

        end = end_ms if end_ms is not None else start_ms + 86_400_000
        start_dt, end_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc), datetime.fromtimestamp(end / 1000, tz=timezone.utc)
        total, offset = 0.0, 0
        while True:
            page = await self._call("get_deals_by_time_range", self._rpc().get_deals_by_time_range(start_dt, end_dt, offset, _DEALS_PAGE))
            if page.get("synchronizing"):
                raise MetaApiError("история сделок MT5 ещё синхронизируется: суточный результат неполон")
            deals = page.get("deals") or []
            for d in deals:
                if str(d.get("type", "")) in _PNL_DEAL_TYPES:
                    total += float(d.get("profit") or 0.0) + float(d.get("commission") or 0.0) + float(d.get("swap") or 0.0)
            if len(deals) < _DEALS_PAGE:
                return total
            offset += _DEALS_PAGE
            if offset > _DEALS_MAX:
                raise MetaApiError(f"слишком много сделок за интервал (> {_DEALS_MAX}): суточный результат не посчитать")

    async def get_orders(self) -> list[Mapping[str, Any]]:
        """Открытые отложенные ордера (сырые словари MetaApi)."""
        return list(await self._call("get_orders", self._rpc().get_orders()))

    async def get_order(self, order_id: str) -> Mapping[str, Any]:
        """Открытый отложенный ордер по ticket. Нет такого (исполнен/отменён) - `MetaApiError`; искать в истории: get_history_order."""
        return await self._call(f"get_order {order_id}", self._rpc().get_order(order_id))

    # ------------------------------------------------------------------------------- спецификации и маржа

    async def get_symbol_specification(self, symbol: str) -> Mapping[str, Any]:
        """Спецификация символа у брокера (сырой словарь MetaApi). Кешируется: ею же пользуются проверки сетки и выбор filling mode."""
        spec = await self._call(f"get_symbol_specification {symbol}", self._rpc().get_symbol_specification(symbol))
        if not isinstance(spec, Mapping):
            raise MetaApiError(f"{symbol}: спецификация не словарь: {spec!r}")
        self._specs[symbol] = spec
        return spec

    async def calculate_margin(self, symbol: str, side: str, volume: float, price: float) -> float:
        """Маржа, которую терминал брокера потребует под ордер (валюта счёта): точнее любой самодельной формулы (режим расчёта, плечо)."""
        order = {"symbol": symbol, "type": "ORDER_TYPE_BUY" if _side(side) == "BUY" else "ORDER_TYPE_SELL",
                 "volume": _positive(volume, "volume"), "openPrice": _positive(price, "price")}
        raw = await self._call(f"calculate_margin {symbol}", self._rpc().calculate_margin(order))
        return _num(raw, "margin", f"margin {symbol}")

    # ------------------------------------------------------------------------------- проверки перед отправкой

    def _check_price(self, symbol: str, price: float, what: str) -> float:
        """Цена на сетке `tickSize` (если спецификация загружена); иначе только положительность. Не на сетке - `ConfigError` (R1)."""
        _positive(price, what)
        spec = self._specs.get(symbol)
        if spec is not None:
            tick = _num(spec, "tickSize", symbol)
            if abs(price / tick - round(price / tick)) > 1e-6:
                raise ConfigError(f"{symbol}: {what} {price} не на сетке брокера (tickSize {tick})")
        return float(price)

    def _check_stops(self, symbol: str, side: str, sl: float | None, tp: float | None) -> None:
        """SL и TP - положительные, на сетке цены, по разные стороны от входа: BUY: SL < TP, SELL: TP < SL."""
        if sl is not None:
            self._check_price(symbol, sl, "SL")
        if tp is not None:
            self._check_price(symbol, tp, "TP")
        if sl is not None and tp is not None and not (sl < tp if side == "BUY" else tp < sl):
            raise ConfigError(f"{symbol}: SL {sl} и TP {tp} расположены неверно для {side} (BUY: SL < TP, SELL: TP < SL)")

    def ensure_entry_allowed(self, symbol: str) -> None:
        """Новые входы разрешены только при `tradeMode = FULL` (по закешированной спецификации): иначе `MarketClosedError`.
        CLOSEONLY / LONGONLY / SHORTONLY / DISABLED - для автоматической торговли равны «рынок закрыт» (закрытие позиций это не касается).
        Спецификации нет или в ней нет `tradeMode` - проверить нельзя, решает брокер (ответом 10018)."""
        mode = (self._specs.get(symbol) or {}).get("tradeMode")
        if mode and mode != TRADE_MODE_FULL:
            raise MarketClosedError(f"{symbol}: торговля символом сейчас ограничена брокером (tradeMode {mode}): новые входы запрещены")

    def filling_candidates(self, symbol: str) -> list[str]:
        """Допустимые для символа режимы исполнения MARKET в порядке предпочтения: FOK (всё или ничего: объём плана не дробится),
        затем IOC, затем RETURN. Пусто - спецификация не загружена или режимов нет: опция не передаётся, выбирает MetaApi (FOK, затем IOC).
        BOC (только пассивные) для рыночных ордеров не годится."""
        spec = self._specs.get(symbol)
        if spec is None:
            return []
        allowed = set(spec.get("fillingModes") or ())
        return [order_mode for symbol_mode, order_mode in _FILLING_PRIORITY if symbol_mode in allowed]

    # ------------------------------------------------------------------------------- торговля

    async def market_order(self, symbol: str, side: str, volume: float, sl: float | None = None, tp: float | None = None, *,
                           client_id: str | None = None) -> MetaApiOrder:
        """MARKET на `volume` ЛОТОВ; `sl`/`tp` прикладываются к позиции в том же запросе (стоп появляется вместе с позицией, R6).

        Режим исполнения выбирается по спецификации символа (FOK -> IOC -> RETURN); если брокер всё же ответил 10030 INVALID_FILL
        (запрос заведомо не исполнен) - пробуется следующий допустимый. Отказ брокера - `MetaApiTradeError` (с кодом и пояснением);
        сетевой сбой после отправки - `MetaApiError`, исход неизвестен: сверять по `client_id` через `get_positions`.
        Ответ с `partial=True` (DONE_PARTIAL) - исполнена часть объёма."""
        _side(side)
        self._check_lots(symbol, _positive(volume, "volume"))
        self._check_stops(symbol, side, sl, tp)
        self.ensure_entry_allowed(symbol)
        conn = self._rpc()
        send = conn.create_market_buy_order if side == "BUY" else conn.create_market_sell_order
        modes: list[str | None] = list(self.filling_candidates(symbol)) or [None]
        for i, mode in enumerate(modes):
            options: dict[str, Any] = {}
            if client_id:
                options["clientId"] = client_id
            if mode:
                options["fillingModes"] = [mode]
            try:
                resp = await self._call(f"market {side} {volume} {symbol}", send(symbol, volume, sl, tp, options or None))
            except MetaApiTradeError as exc:
                if exc.numeric_code == RETCODE_INVALID_FILL and i + 1 < len(modes):
                    log.warning("%s: режим %s отклонён (10030), пробую %s", symbol, mode, modes[i + 1])
                    continue
                raise
            return _order_from_trade(symbol, client_id, resp)
        raise MetaApiError(f"{symbol}: нет допустимого режима исполнения")  # недостижимо: цикл либо вернул, либо поднял

    async def limit_order(self, symbol: str, side: str, volume: float, price: float, sl: float | None = None, tp: float | None = None, *,
                          client_id: str | None = None, expires_at: datetime | None = None) -> MetaApiOrder:
        """Отложенный лимитный ордер (BUY_LIMIT / SELL_LIMIT) на `volume` лотов; при `expires_at` (aware datetime) - со сроком жизни.
        Аналога post-only нет. Режим исполнения для отложенных ордеров выбирает MetaApi/брокер."""
        _side(side)
        self._check_lots(symbol, _positive(volume, "volume"))
        self._check_price(symbol, price, "цена лимитки")
        self._check_stops(symbol, side, sl, tp)
        self.ensure_entry_allowed(symbol)
        options: dict[str, Any] = {}
        if client_id:
            options["clientId"] = client_id
        if expires_at is not None:
            if expires_at.tzinfo is None:
                raise ConfigError("expires_at должен быть aware datetime (UTC)")
            options["expiration"] = {"type": "ORDER_TIME_SPECIFIED", "time": expires_at}
        conn = self._rpc()
        send = conn.create_limit_buy_order if side == "BUY" else conn.create_limit_sell_order
        resp = await self._call(f"limit {side} {volume} {symbol} @ {price}", send(symbol, volume, price, sl, tp, options or None))
        return _order_from_trade(symbol, client_id, resp)

    async def modify_position(self, position_id: str, sl: float | None = None, tp: float | None = None) -> MetaApiOrder:
        """Изменить SL и/или TP открытой позиции (безубыток, трейлинг). `None` - уровень не менять.

        Модификация атомарна: окна без стопа нет. Чтобы не сбросить неуказанный уровень, клиент читает позицию и ВСЕГДА передаёт
        оба уровня (❓ поведение POSITION_MODIFY при одном заданном уровне не проверено). Ответ NO_CHANGES (10025) - успех."""
        if sl is None and tp is None:
            raise ConfigError("modify_position: нужен хотя бы один из sl / tp")
        conn = self._rpc()
        current = _position(await self._call(f"get_position {position_id}", conn.get_position(position_id)))
        new_sl = sl if sl is not None else current.stop_loss
        new_tp = tp if tp is not None else current.take_profit
        self._check_stops(current.symbol, current.side, new_sl, new_tp)
        resp = await self._call(f"modify {position_id}", conn.modify_position(position_id, new_sl, new_tp))
        return _order_from_trade(current.symbol, current.client_id, resp)

    async def stop_loss(self, position_id: str, price: float) -> MetaApiOrder:
        """Выставить/сдвинуть SL позиции (TP не меняется)."""
        return await self.modify_position(position_id, sl=_positive(price, "price"))

    async def take_profit(self, position_id: str, price: float) -> MetaApiOrder:
        """Выставить/сдвинуть TP позиции (SL не меняется)."""
        return await self.modify_position(position_id, tp=_positive(price, "price"))

    async def close_position(self, position_id: str, volume: float | None = None, *, client_id: str | None = None) -> MetaApiOrder:
        """Закрыть позицию целиком (`volume=None` или объём равен позиции) или часть (Scale Out) встречным рыночным ордером.

        Перед частичным закрытием читается позиция: объём больше позиции - `ConfigError`; при загруженной спецификации объём обязан быть на
        сетке `volumeStep`, а остаток - не меньше `minVolume` (иначе брокер ответит 10038/10014 уже после отправки)."""
        conn = self._rpc()
        options = {"clientId": client_id} if client_id else None
        if volume is None:
            resp = await self._call(f"close {position_id}", conn.close_position(position_id, options))
            return _order_from_trade("", client_id, resp)
        _positive(volume, "volume")
        pos = _position(await self._call(f"get_position {position_id}", conn.get_position(position_id)))
        if volume > pos.lots + 1e-9:
            raise ConfigError(f"{pos.symbol}: закрыть {volume} лотов, а в позиции {position_id} только {pos.lots}")
        if abs(volume - pos.lots) <= 1e-9:
            resp = await self._call(f"close {position_id}", conn.close_position(position_id, options))
            return _order_from_trade(pos.symbol, client_id, resp)
        self._check_lots(pos.symbol, volume)
        spec = self._specs.get(pos.symbol)
        if spec is not None and pos.lots - volume < _num(spec, "minVolume", pos.symbol) - 1e-9:
            raise ConfigError(f"{pos.symbol}: после закрытия {volume} в позиции останется {pos.lots - volume:g} лотов - меньше minVolume брокера")
        resp = await self._call(f"close {volume} of {position_id}", conn.close_position_partially(position_id, volume, options))
        return _order_from_trade(pos.symbol, client_id, resp)

    async def cancel_order(self, order_id: str) -> MetaApiOrder:
        """Отменить отложенный ордер по ticket."""
        resp = await self._call(f"cancel {order_id}", self._rpc().cancel_order(order_id))
        return _order_from_trade("", None, resp)
