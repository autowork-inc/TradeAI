"""Асинхронный клиент традиционных рынков (Forex, металлы) через cTrader Open API (Spotware): `CTraderConnector`. Заменил платный шлюз MetaApi (2026-10-23).

Протокол. Protobuf поверх TCP+TLS (`live.ctraderapi.com` / `demo.ctraderapi.com`, порт 5035), кадр = 4 байта длины + `ProtoMessage` (кодек - `exchange/ctrader_proto.py`,
без Twisted и без пакета `ctrader-open-api`: он строится на втором цикле событий). Один читающий цикл разбирает кадры; ответы сопоставляются с запросами по
`clientMsgId`, события (котировки, исполнения) попадают в обработчики. Раз в 10 с уходит heartbeat (сервер закрывает молчащее соединение).

Аутентификация (OAuth 2.0, документация «App and account authentication»): приложение (Client ID + Secret) -> `ProtoOAApplicationAuthReq`; счёт
(Account ID + Access Token) -> `ProtoOAAccountAuthReq`. Access Token живёт ~30 дней: при `OA_AUTH_TOKEN_EXPIRED`/`CH_ACCESS_TOKEN_INVALID` и заданном Refresh Token
коннектор сам обновляет пару токенов (`ProtoOARefreshTokenReq`) и сохраняет её в `DATA_DIR/.ctrader_tokens.json` (файл с секретами: в git и архивы деплоя не попадает).
Первичный токен выдаёт `scripts/get_ctrader_token.py`.

Переменные окружения (только `.env.shadow` на сервере, в репозитории нет):
    CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET   ключи приложения (https://openapi.ctrader.com/apps)
    CTRADER_ACCOUNT_ID                         ctidTraderAccountId торгового счёта
    CTRADER_ACCESS_TOKEN                       токен доступа (scope trading - для ордеров; accounts - только чтение)
    CTRADER_REFRESH_TOKEN                      (необязательно) для автообновления токена
    CTRADER_ENV                                auto (по умолчанию: хост определяется по списку счетов токена) | demo | live

Соответствие прежнему клиенту MetaApi (удалён; чтобы ExecutionRouter, сопровождение и SymbolMapper остались без переделки): объём в ЛОТАХ (`lots = volume / lotSize`), деньги - в валюте
депозита, имена методов те же (`get_price`, `get_candles`, `market_order`, `modify_position`, `close_position`...), а `get_symbol_specification` отдаёт словарь той же
формы (contractSize, minVolume, volumeStep, tickSize, digits, path, tradeMode...). Особенности cTrader:
    * MARKET-ордеру абсолютные SL/TP нельзя (сервер: «Not supported for MARKET orders»): в запросе уходят ОТНОСИТЕЛЬНЫЕ (расстояние от справочной котировки), позиция
      открывается сразу с защитой, затем SL/TP выравниваются на абсолютные цены (`ProtoOAAmendPositionSLTPReq`). Выровнять не удалось - позиция остаётся с относительной защитой.
    * Позиция и ордера - сущности сервера: `Position Recovery` = `ProtoOAReconcileReq` (`reconcile()`), он же питает get_positions / get_orders / get_position.
    * Нет filling mode и retcode MT5; ошибки - строковые коды (`TRADING_BAD_STOPS`...), см. `TRADE_ERRORS`.

❓ Не проверено на живом счёте: ничего из этого файла не работало против реального сервера (в тестах - имитация сервера по схемам Spotware). Лимиты сервера: 50 запросов/с
(не исторических) и 5/с для трендбаров - коннектор держит паузу 0.25 с между запросами трендбаров. Окна трендбаров по периодам - по документации (`PERIOD_MAX_WINDOW_MS`).
"""

import asyncio
import json
import logging
import os
import ssl as ssl_lib
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import config
from exchange import ctrader_proto as P
from exchange.binance_live import Balance, PositionInfo
from utils.errors import ConfigError, DataError, TradingInfraError
from utils.storage import atomic_write_text

log = logging.getLogger(__name__)

CLIENT_ID_ENV, CLIENT_SECRET_ENV, ACCOUNT_ENV, TOKEN_ENV, REFRESH_ENV, ENV_ENV = (
    "CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET", "CTRADER_ACCOUNT_ID", "CTRADER_ACCESS_TOKEN", "CTRADER_REFRESH_TOKEN", "CTRADER_ENV")
HOSTS = {"live": "live.ctraderapi.com", "demo": "demo.ctraderapi.com"}
PORT = 5035
TOKEN_URL = "https://openapi.ctrader.com/apps/token"
SIDES = ("BUY", "SELL")
CONNECT_TIMEOUT_S = 20.0
REQUEST_TIMEOUT_S = 30.0
TRADE_TIMEOUT_S = 20.0
HEARTBEAT_S = 10.0
HISTORY_PAUSE_S = 0.25  # лимит сервера на трендбары: 5 запросов в секунду
QUOTE_WAIT_S = 10.0
FRAME_MAX = 8 * 1024 * 1024
_DEALS_PAGE, _DEALS_MAX = 500, 20_000

TRADE_MODE_FULL = "SYMBOL_TRADE_MODE_FULL"
_METALS = frozenset({"XAU", "XAG", "XPT", "XPD"})
_FIAT = frozenset({"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF", "TRY", "ZAR", "MXN", "SGD", "HKD", "CNH", "CNY", "RUB"})


class CTraderError(TradingInfraError):
    """Сбой связи с cTrader (таймаут, обрыв, ошибка сервера). Повтор возможен, исход ордера может быть неизвестен."""


# Коды ошибок сервера (ProtoOAErrorCode) -> (пояснение, повтор допустим, исход ордера неизвестен). «Повтор допустим» - запрос заведомо не исполнен и условия
# могут измениться (рынок закрыт, нет котировок); «исход неизвестен» - ордер мог дойти до брокера: перед повтором сверять по clientOrderId.
TRADE_ERRORS: dict[str, tuple[str, bool, bool]] = {
    "MARKET_CLOSED": ("рынок закрыт", True, False),
    "SYMBOL_HAS_HOLIDAY": ("по символу выходной/праздник", True, False),
    "NO_QUOTES": ("нет котировок для исполнения", True, False),
    "NOT_ENOUGH_MONEY": ("не хватает средств (маржа)", False, False),
    "MAX_EXPOSURE_REACHED": ("достигнут максимальный объём позиций по символу", False, False),
    "POSITION_NOT_FOUND": ("позиция не найдена (уже закрыта)", False, False),
    "ORDER_NOT_FOUND": ("ордер не найден (исполнен или снят)", False, False),
    "POSITION_NOT_OPEN": ("позиция не открыта", False, False),
    "POSITION_LOCKED": ("позиция заблокирована", True, False),
    "TOO_MANY_POSITIONS": ("слишком много позиций", False, False),
    "TRADING_BAD_VOLUME": ("неверный объём (вне min/max или не кратен шагу)", False, False),
    "TRADING_BAD_STOPS": ("неверные SL/TP (ближе допустимого или не по ту сторону цены)", False, False),
    "TRADING_BAD_PRICES": ("неверная цена", False, False),
    "TRADING_BAD_STAKE": ("неверная ставка", False, False),
    "PROTECTION_IS_TOO_CLOSE_TO_MARKET": ("SL/TP слишком близко к рынку", False, False),
    "TRADING_BAD_EXPIRATION_DATE": ("неверный срок действия ордера", False, False),
    "PENDING_EXECUTION": ("ордер уже исполняется", True, True),
    "TRADING_DISABLED": ("торговля символом запрещена", False, False),
    "TRADING_NOT_ALLOWED": ("торговля счёта запрещена (права токена/режим счёта)", False, False),
    "UNABLE_TO_CANCEL_ORDER": ("ордер не отменить", False, False),
    "UNABLE_TO_AMEND_ORDER": ("ордер не изменить", False, False),
    "SHORT_SELLING_NOT_ALLOWED": ("короткие позиции запрещены", False, False),
    "REQUEST_FREQUENCY_EXCEEDED": ("слишком частые запросы", True, False),
    "CANT_ROUTE_REQUEST": ("запрос не маршрутизирован", True, True),
    "TIMEOUT_ERROR": ("таймаут на стороне сервера", False, True),
}
_AUTH_FATAL = frozenset({"CH_CLIENT_AUTH_FAILURE", "CH_OA_CLIENT_NOT_FOUND", "CH_CTID_TRADER_ACCOUNT_NOT_FOUND", "ACCOUNT_NOT_AUTHORIZED", "RET_NO_SUCH_LOGIN",
                         "RET_ACCOUNT_DISABLED", "CH_CLIENT_NOT_AUTHENTICATED"})
_TOKEN_EXPIRED = frozenset({"OA_AUTH_TOKEN_EXPIRED", "CH_ACCESS_TOKEN_INVALID"})


class CTraderTradeError(CTraderError):
    """Брокер отклонил торговую операцию. `code` - строковый код ProtoOAErrorCode; `retryable` - запрос заведомо не исполнен, повтор позже имеет смысл;
    `outcome_unknown` - ордер мог дойти до брокера: перед повтором сверить по clientOrderId."""

    def __init__(self, message: str, code: str | None = None):
        known = TRADE_ERRORS.get(code or "")
        self.code = code
        self.reason = known[0] if known else "неизвестный код ошибки брокера"
        self.retryable = known[1] if known else False
        self.outcome_unknown = known[2] if known else True  # неизвестный код - не считаем ордер «точно не исполненным»
        super().__init__(f"{message} [{code}: {self.reason}]" if code else message)


class MarketClosedError(CTraderTradeError):
    """Рынок/символ недоступен для НОВЫХ входов (MARKET_CLOSED, выходные, tradeMode ≠ ENABLED). Обычный отказ сигнала, а не сбой: Safe Mode не включается -
    вызывающий (`ExecutionRouter`) превращает его в отказ `market_closed`."""

    def __init__(self, message: str, code: str | None = "MARKET_CLOSED"):
        super().__init__(message, code)


def _trade_error(message: str, code: str | None) -> CTraderTradeError:
    cls = MarketClosedError if code in ("MARKET_CLOSED", "SYMBOL_HAS_HOLIDAY") else CTraderTradeError
    return cls(message, code)


# ------------------------------------------------------------------------------------------------ данные
@dataclass(frozen=True)
class CTraderCredentials:
    client_id: str
    client_secret: str
    account_id: int
    access_token: str
    refresh_token: str = ""

    def __repr__(self) -> str:  # секреты не должны попадать в логи и трейсбеки
        return f"CTraderCredentials(account_id={self.account_id}, client_id={self.client_id[:6]}***, secrets=***)"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "CTraderCredentials":
        """Ключи из окружения. Чего-то нет или пусто - `ConfigError` до обращения к сети."""
        src = os.environ if env is None else env
        vals = {n: (src.get(n) or "").strip() for n in (CLIENT_ID_ENV, CLIENT_SECRET_ENV, ACCOUNT_ENV, TOKEN_ENV)}
        missing = [n for n, v in vals.items() if not v]
        if missing:
            raise ConfigError(f"cTrader: не заданы переменные окружения {', '.join(missing)} (приложение: https://openapi.ctrader.com/apps, токен: scripts/get_ctrader_token.py)")
        if not vals[ACCOUNT_ENV].isdigit():
            raise ConfigError(f"cTrader: {ACCOUNT_ENV} должен быть числом (ctidTraderAccountId), получено {vals[ACCOUNT_ENV]!r}")
        bad = [n for n in (CLIENT_ID_ENV, CLIENT_SECRET_ENV, TOKEN_ENV) if any(not 33 <= ord(ch) <= 126 for ch in vals[n])]
        if bad:
            raise ConfigError(f"cTrader: {', '.join(bad)} содержит посторонние символы (пробел/перенос/нелатиница): скопируйте значение заново")
        return cls(vals[CLIENT_ID_ENV], vals[CLIENT_SECRET_ENV], int(vals[ACCOUNT_ENV]), vals[TOKEN_ENV], (src.get(REFRESH_ENV) or "").strip())


@dataclass(frozen=True)
class AccountInfo:
    """Сводка счёта (валюта депозита)."""

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
    loss_tick_value: float | None = None  # потеря за 1 тик на 1 лот в ВАЛЮТЕ СЧЁТА (для расчёта лота, exchange.mt5_risk); None - курс конвертации неизвестен
    profit_tick_value: float | None = None

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(frozen=True)
class CTraderPosition:
    position_id: str
    symbol: str
    side: str  # BUY | SELL
    lots: float
    open_price: float
    stop_loss: float | None
    take_profit: float | None
    profit: float
    client_id: str | None  # clientOrderId входного ордера (label позиции при MARKET)


@dataclass(frozen=True)
class CTraderOrder:
    """Результат торговой операции или отложенный ордер."""

    symbol: str
    client_id: str | None
    order_id: str | None
    position_id: str | None
    state: str  # DONE | PLACED | ...
    message: str
    code: str | None = None  # FILLED | PARTIAL_FILL | ORDER_ACCEPTED | ORDER_REPLACED | ORDER_CANCELLED
    fill_price: float | None = None

    @property
    def partial(self) -> bool:
        """Исполнена только часть объёма: вызывающий обязан сверить позицию."""
        return self.code == "PARTIAL_FILL"


@dataclass
class _Pending:
    """Ожидающий ответа запрос. `done` - предикат «этот ответ/событие завершает запрос» (для торговых: нужный executionType)."""

    msg_id: str
    expect: tuple[str, ...]
    fut: asyncio.Future
    done: Any = None
    client_order_id: str | None = None
    position_id: int | None = None
    order_id: int | None = None
    events: list = field(default_factory=list)


def _opt(value: Any) -> float | None:
    return float(value) if value else None


def _positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        raise ConfigError(f"{name} должен быть положительным числом, получено {value!r}")
    return float(value)


def _side(side: str) -> str:
    if side not in SIDES:
        raise ConfigError(f"side должен быть BUY или SELL, получено {side!r}")
    return side


def asset_path(name: str, base: str, quote: str) -> str:
    """Путь символа в «дереве» (формат, который читает SymbolMapper): Metals / Forex / иначе CFD. В cTrader классов актива в списке символов нет - определяем по активам."""
    if base in _METALS:
        return f"Metals\\{name}"
    if base in _FIAT and quote in _FIAT:
        return f"Forex\\{name}"
    return f"CFD\\{name}"


class CTraderConnector:
    """Клиент cTrader Open API: данные (свечи, котировки), счёт, торговля и восстановление позиций. Создание объекта НЕ подключается к сети (только проверка ключей)."""

    def __init__(self, env: Mapping[str, str] | None = None, *, token_store: Path | None = None, host: str | None = None, port: int = PORT,
                 use_tls: bool = True, id_prefix: str | None = None, clock: Any = time.time):
        """token_store - файл с обновлёнными токенами (по умолчанию DATA_DIR/.ctrader_tokens.json). host/port/use_tls - для тестов (имитация сервера)."""
        self._creds = CTraderCredentials.from_env(env)
        src = os.environ if env is None else env
        self._env_mode = (src.get(ENV_ENV) or config.CTRADER_ENV).strip().lower()
        if self._env_mode not in ("auto", "demo", "live"):
            raise ConfigError(f"{ENV_ENV} должен быть auto|demo|live, получено {self._env_mode!r}")
        self._host_override, self._port, self._tls = host, port, use_tls
        self._token_store = token_store if token_store is not None else config.DATA_DIR / config.CTRADER_TOKEN_FILE_NAME
        self.id_prefix = id_prefix if id_prefix is not None else config.CLIENT_ORDER_ID_PREFIX
        self._clock = clock
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._tasks: list[asyncio.Task] = []
        self._pending: dict[str, _Pending] = {}
        self._write_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._last_history = 0.0
        self._dead: str | None = None  # причина потери связи (после неё все запросы отказывают сразу)
        self._access_token, self._refresh_token = self._creds.access_token, self._creds.refresh_token
        self.is_live: bool | None = None
        self.host_used: str | None = None
        self._light: dict[str, Mapping[str, Any]] = {}  # имя -> LightSymbol
        self._light_by_id: dict[int, Mapping[str, Any]] = {}
        self._assets: dict[int, str] = {}
        self._sym: dict[int, Mapping[str, Any]] = {}  # symbolId -> полный Symbol
        self._specs: dict[str, Mapping[str, Any]] = {}
        self._trader: Mapping[str, Any] | None = None
        self._quotes: dict[int, dict[str, Any]] = {}
        self._conversion: dict[str, tuple[int, bool] | None] = {}  # валюта прибыли -> (symbolId пары к валюте депозита, True: курс = mid, False: 1/mid)
        self._subscribed: set[int] = set()
        self._quote_events: dict[int, asyncio.Event] = {}
        self.positions: list[CTraderPosition] = []  # снимок Position Recovery при подключении
        self._load_saved_tokens()  # токены, обновлённые при прошлом запуске по Refresh Token (в окружении лежит устаревший access token)

    def __repr__(self) -> str:
        return f"CTraderConnector(account_id={self._creds.account_id}, connected={self._writer is not None and self._dead is None})"

    @property
    def account_id(self) -> int:
        return self._creds.account_id

    async def __aenter__(self) -> "CTraderConnector":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    # ----------------------------------------------------------------------------------------- транспорт

    async def _open(self, host: str) -> None:
        ctx = ssl_lib.create_default_context() if self._tls else None
        try:
            self._reader, self._writer = await asyncio.wait_for(asyncio.open_connection(host, self._port, ssl=ctx), CONNECT_TIMEOUT_S)
        except (OSError, asyncio.TimeoutError, ssl_lib.SSLError) as exc:
            raise CTraderError(f"cTrader: нет соединения с {host}:{self._port}: {exc}") from exc
        self._dead = None
        self.host_used = host
        self._tasks = [asyncio.create_task(self._read_loop(), name="ctrader_reader"), asyncio.create_task(self._heartbeat_loop(), name="ctrader_heartbeat")]

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                head = await self._reader.readexactly(4)
                size = int.from_bytes(head, "big")
                if not 0 < size <= FRAME_MAX:
                    raise CTraderError(f"cTrader: недопустимая длина кадра {size}")
                body = await self._reader.readexactly(size)
                self._dispatch(*P.parse(body))
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ssl_lib.SSLError) as exc:
            self._fail_all(f"соединение потеряно: {exc or 'EOF'}")
        except CTraderError as exc:
            self._fail_all(str(exc))
        except DataError as exc:
            self._fail_all(f"кадр не разобран: {exc}")
        finally:
            self._fail_all("читающий цикл остановлен")  # идемпотентно: что бы ни случилось (в т.ч. ошибка кода), запросы не повисают до таймаута

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_S)
            if self._dead is not None:
                return
            try:
                await self._send("ProtoHeartbeatEvent", {})
            except CTraderError:
                return  # причину уже записал читающий цикл / _send

    def _fail_all(self, reason: str, *, quiet: bool = False) -> None:
        if self._dead is None:
            self._dead = reason
            if not quiet:
                log.warning("cTrader: %s", reason)
        pending, self._pending = self._pending, {}
        for p in pending.values():
            if not p.fut.done():
                p.fut.set_exception(CTraderError(f"cTrader: {reason}"))
        for ev in self._quote_events.values():
            ev.set()

    async def _send(self, name: str, values: Mapping[str, Any], msg_id: str | None = None) -> None:
        if self._dead is not None or self._writer is None:
            raise CTraderError(f"cTrader: нет связи ({self._dead or 'не подключено'})")
        frame = P.build(name, values, msg_id) if name != "ProtoHeartbeatEvent" else P.wrap(P.PT[name], b"", None)
        try:
            async with self._write_lock:
                self._writer.write(frame)
                await self._writer.drain()
        except (ConnectionError, OSError) as exc:
            self._fail_all(f"запись не удалась: {exc}")
            raise CTraderError(f"cTrader: запись не удалась: {exc}") from exc

    async def _request(self, name: str, values: Mapping[str, Any], expect: str | tuple[str, ...], *, timeout: float = REQUEST_TIMEOUT_S) -> dict[str, Any]:
        """Отправить запрос и дождаться ответа указанного вида. Ошибка сервера -> CTraderError / ConfigError (авторизация)."""
        expect_t = (expect,) if isinstance(expect, str) else tuple(expect)
        msg_id = uuid.uuid4().hex[:16]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = _Pending(msg_id, expect_t, fut)
        try:
            await self._send(name, values, msg_id)
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError as exc:
            raise CTraderError(f"cTrader: таймаут {timeout:g} с ({name})") from exc
        finally:
            self._pending.pop(msg_id, None)

    def _dispatch(self, name: str, msg: dict[str, Any], msg_id: str | None) -> None:
        if name == "ProtoHeartbeatEvent":
            return
        if name == "SpotEvent":
            self._on_spot(msg)
            return
        if name in ("AccountsTokenInvalidatedEvent", "ClientDisconnectEvent", "AccountDisconnectEvent"):
            self._fail_all(f"сервер отключил сессию ({name}: {msg.get('reason', '')})")
            return
        pend = self._pending.get(msg_id) if msg_id else None
        if name == "ExecutionEvent" or name == "OrderErrorEvent":
            pend = pend or self._match_trade(name, msg)
            if pend is not None:
                self._on_trade_event(pend, name, msg)
            return
        if pend is None:
            return  # событие, которое мы не моделируем / ответ на запрос, которого уже нет (таймаут)
        if name in ("ErrorRes", "ProtoErrorRes"):
            if not pend.fut.done():
                pend.fut.set_exception(self._server_error(name, msg))
        elif name in pend.expect:
            if not pend.fut.done():
                pend.fut.set_result(msg)
        elif not pend.fut.done():
            pend.fut.set_exception(CTraderError(f"cTrader: на запрос пришёл {name}, ожидался {pend.expect}"))

    def _match_trade(self, name: str, msg: dict[str, Any]) -> _Pending | None:
        order = msg.get("order") or {}
        position = msg.get("position") or {}
        cid, pid, oid = order.get("clientOrderId"), position.get("positionId") or order.get("positionId") or msg.get("positionId"), order.get("orderId") or msg.get("orderId")
        for p in self._pending.values():
            if p.done is None:
                continue
            if cid and p.client_order_id == cid or pid and p.position_id == pid or oid and p.order_id == oid:
                return p
        return None

    def _on_trade_event(self, pend: _Pending, name: str, msg: dict[str, Any]) -> None:
        if pend.fut.done():
            return
        if name == "OrderErrorEvent":
            pend.fut.set_exception(_trade_error("ордер отклонён сервером: " + str(msg.get("description") or ""), msg.get("errorCode")))
            return
        pend.events.append(msg)
        state = pend.done(msg) if pend.done is not None else "done"
        if state == "reject":
            pend.fut.set_exception(_trade_error("ордер отклонён сервером: " + str(msg.get("errorCode") or ""), msg.get("errorCode")))
        elif state == "done":
            pend.fut.set_result(msg)

    @staticmethod
    def _server_error(name: str, msg: Mapping[str, Any]) -> CTraderError:
        code, text = str(msg.get("errorCode") or ""), str(msg.get("description") or "")
        if code in _AUTH_FATAL:
            return ConfigError(f"cTrader отказал в доступе: {code} {text}")  # type: ignore[return-value]
        if code in TRADE_ERRORS:
            return _trade_error(text or code, code)
        return CTraderError(f"cTrader: ошибка сервера {code} {text}".strip())

    # ----------------------------------------------------------------------------------------- подключение

    async def connect(self, *, timeout_s: float = 60.0) -> None:
        """Подключиться: TCP+TLS -> приложение -> список счетов токена (определяет demo/live) -> счёт -> справочники -> Position Recovery.
        Повторный вызов на подключённом клиенте ничего не делает. Любой сбой - исключение (R1); неверные ключи - `ConfigError`."""
        if self._writer is not None and self._dead is None:
            return
        await self.close()
        try:
            await asyncio.wait_for(self._connect_once(), timeout_s)
        except asyncio.TimeoutError as exc:
            await self.close()
            raise CTraderError(f"cTrader: подключение не завершилось за {timeout_s:g} с") from exc
        except BaseException:
            await self.close()
            raise

    def _hosts_to_try(self) -> list[str]:
        if self._host_override:
            return [self._host_override]
        if self._env_mode in HOSTS:
            return [HOSTS[self._env_mode]]
        return [HOSTS["demo"], HOSTS["live"]]

    async def _connect_once(self) -> None:
        tried: list[str] = []
        for host in self._hosts_to_try():
            await self._open(host)
            await self._request("ApplicationAuthReq", {"clientId": self._creds.client_id, "clientSecret": self._creds.client_secret}, "ApplicationAuthRes")
            accounts = await self._account_list()
            mine = next((a for a in accounts if a.get("ctidTraderAccountId") == self._creds.account_id), None)
            if mine is not None:
                self.is_live = bool(mine.get("isLive"))
                right = HOSTS["live" if self.is_live else "demo"]
                if not self._host_override and self._env_mode == "auto" and host != right:
                    await self.close()  # счёт на другом сервере (demo/live): переподключаемся туда, где он живёт
                    await self._open(right)
                    await self._request("ApplicationAuthReq", {"clientId": self._creds.client_id, "clientSecret": self._creds.client_secret}, "ApplicationAuthRes")
                await self._auth_account()
                break
            tried.append(f"{host}: счетов {[a.get('ctidTraderAccountId') for a in accounts]}")
            await self.close()
        else:
            raise ConfigError(f"cTrader: счёта {self._creds.account_id} нет среди счетов токена ({'; '.join(tried)}); выдайте токен на нужный счёт (scripts/get_ctrader_token.py)")
        await self._load_reference()
        await self.reconcile()
        log.info("cTrader: подключено, счёт %s (%s, %s), позиций на сервере: %d", self._creds.account_id, "live" if self.is_live else "demo", self.host_used, len(self.positions))

    async def _account_list(self) -> list[dict[str, Any]]:
        try:
            res = await self._request("GetAccountListByAccessTokenReq", {"accessToken": self._access_token}, "GetAccountListByAccessTokenRes")
        except ConfigError:
            raise
        except CTraderError as exc:
            if not await self._refresh_if_expired(exc):
                raise
            res = await self._request("GetAccountListByAccessTokenReq", {"accessToken": self._access_token}, "GetAccountListByAccessTokenRes")
        return list(res.get("ctidTraderAccount") or [])

    async def _auth_account(self) -> None:
        try:
            await self._request("AccountAuthReq", {"ctidTraderAccountId": self._creds.account_id, "accessToken": self._access_token}, "AccountAuthRes")
        except CTraderError as exc:
            if not await self._refresh_if_expired(exc):
                raise
            await self._request("AccountAuthReq", {"ctidTraderAccountId": self._creds.account_id, "accessToken": self._access_token}, "AccountAuthRes")

    async def _refresh_if_expired(self, exc: BaseException) -> bool:
        """Токен истёк и есть Refresh Token -> новая пара токенов (и сохранение). False - обновлять нечем / ошибка другая."""
        if not any(code in str(exc) for code in _TOKEN_EXPIRED) or not self._refresh_token:
            return False
        res = await self._request("RefreshTokenReq", {"refreshToken": self._refresh_token}, "RefreshTokenRes")
        self._access_token, self._refresh_token = res["accessToken"], res.get("refreshToken") or self._refresh_token
        self._save_tokens()
        log.warning("cTrader: токен доступа обновлён по Refresh Token (срок %s с), пара сохранена в %s", res.get("expiresIn"), self._token_store)
        return True

    def _load_saved_tokens(self) -> None:
        try:
            raw = self._token_store.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        try:
            data = json.loads(raw)
            if data["account_id"] == self._creds.account_id and data["client_id"] == self._creds.client_id and data["access_token"] and data["refresh_token"]:
                self._access_token, self._refresh_token = data["access_token"], data["refresh_token"]
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("cTrader: файл токенов %s не читается (%s) - беру токены из окружения", self._token_store, exc)

    def _save_tokens(self) -> None:
        atomic_write_text(self._token_store, json.dumps({"account_id": self._creds.account_id, "client_id": self._creds.client_id, "access_token": self._access_token,
                                                         "refresh_token": self._refresh_token, "saved_at": self._clock()}))

    async def close(self) -> None:
        """Закрыть соединение (позиции и ордера на сервере брокера не затрагиваются)."""
        writer, tasks = self._writer, self._tasks
        self._writer = self._reader = None
        self._tasks = []
        self._subscribed.clear()
        self._quote_events.clear()
        self._fail_all("соединение закрыто", quiet=True)
        self._dead = "закрыто"
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ssl_lib.SSLError):
                pass  # закрытие уже оборванного соединения: ошибка транспорта не важна

    # ----------------------------------------------------------------------------------------- справочники

    async def _load_reference(self) -> None:
        cid = self._creds.account_id
        assets = await self._request("AssetListReq", {"ctidTraderAccountId": cid}, "AssetListRes")
        self._assets = {int(a["assetId"]): str(a.get("name") or "") for a in assets.get("asset", [])}
        syms = await self._request("SymbolsListReq", {"ctidTraderAccountId": cid, "includeArchivedSymbols": False}, "SymbolsListRes")
        self._light = {str(s["symbolName"]): s for s in syms.get("symbol", []) if s.get("symbolName")}
        self._light_by_id = {int(s["symbolId"]): s for s in self._light.values()}
        trader = await self._request("TraderReq", {"ctidTraderAccountId": cid}, "TraderRes")
        self._trader = trader["trader"]

    def _symbol_id(self, symbol: str) -> int:
        light = self._light.get(symbol)
        if light is None:
            raise ConfigError(f"cTrader: символа {symbol!r} нет у брокера (после connect() доступны get_symbols())")
        return int(light["symbolId"])

    def _require_connected(self) -> None:
        if self._writer is None or self._dead is not None:
            raise CTraderError(f"cTrader: клиент не подключён ({self._dead or 'сначала await connect()'})")

    async def get_symbols(self) -> list[str]:
        """Имена символов, доступных на счёте (с суффиксами брокера, если есть)."""
        self._require_connected()
        return sorted(self._light)

    async def get_symbol_specification(self, symbol: str) -> Mapping[str, Any]:
        """Спецификация символа в форме, которую читает `SymbolMapper.parse_spec` (контракт, лоты, тик, режим торговли, путь). Кешируется."""
        self._require_connected()
        sid = self._symbol_id(symbol)
        res = await self._request("SymbolByIdReq", {"ctidTraderAccountId": self._creds.account_id, "symbolId": [sid]}, "SymbolByIdRes")
        full = next((s for s in res.get("symbol", []) if s.get("symbolId") == sid), None)
        if full is None:
            raise CTraderError(f"{symbol}: сервер не вернул полную спецификацию")
        self._sym[sid] = full
        light = self._light[symbol]
        lot = float(full.get("lotSize") or 0)
        if not lot > 0:
            raise CTraderError(f"{symbol}: в спецификации нет lotSize")
        digits = int(full.get("digits") or 0)
        point = 10.0 ** -digits
        mode = full.get("tradingMode", P.TRADING_ENABLED)
        trade_mode = TRADE_MODE_FULL if mode == P.TRADING_ENABLED else "SYMBOL_TRADE_MODE_CLOSEONLY" if mode == P.TRADING_CLOSE_ONLY else "SYMBOL_TRADE_MODE_DISABLED"
        base, quote = self._assets.get(int(light.get("baseAssetId") or 0), ""), self._assets.get(int(light.get("quoteAssetId") or 0), "")
        stops_pts = float(full.get("slDistance") or 0) if full.get("distanceSetIn", 1) == 1 else 0.0
        spec = {
            "symbol": symbol, "path": asset_path(symbol, base, quote), "description": str(light.get("description") or ""),
            "contractSize": lot / 100.0, "minVolume": float(full.get("minVolume") or 0) / lot, "maxVolume": float(full.get("maxVolume") or 0) / lot,
            "volumeStep": float(full.get("stepVolume") or 0) / lot, "tickSize": point, "point": point, "digits": digits, "stopsLevel": stops_pts, "freezeLevel": 0,
            "fillingModes": [], "tradeMode": trade_mode, "profitCurrency": quote, "marginCurrency": base, "lotSize": lot,
        }
        self._specs[symbol] = spec
        return spec

    async def load_symbols(self, symbols: list[str]) -> None:
        for s in symbols:
            await self.get_symbol_specification(s)

    def _spec(self, symbol: str) -> Mapping[str, Any]:
        spec = self._specs.get(symbol)
        if spec is None:
            raise ConfigError(f"Нет спецификации {symbol}: сначала await load_symbols([...])")
        return spec

    def round_lots(self, symbol: str, lots: float) -> float:
        """Объём вниз до шага брокера; вне [min, max] - `ConfigError`."""
        spec = self._spec(symbol)
        step, vmin, vmax = spec["volumeStep"], spec["minVolume"], spec["maxVolume"]
        rounded = round(int(lots / step + 1e-9) * step, 8)
        if rounded < vmin or rounded > vmax:
            raise ConfigError(f"{symbol}: объём {lots} -> {rounded} вне допустимых [{vmin}, {vmax}] (шаг {step})")
        return rounded

    def round_price(self, symbol: str, price: float) -> float:
        spec = self._spec(symbol)
        return round(round(price / spec["tickSize"]) * spec["tickSize"], int(spec["digits"]))

    def _volume_units(self, symbol: str, lots: float) -> int:
        """Лоты -> объём протокола (сотые доли единицы). Проверка сетки до отправки (R1)."""
        spec = self._spec(symbol)
        step, vmin, vmax = spec["volumeStep"], spec["minVolume"], spec["maxVolume"]
        if not (vmin - 1e-12 <= lots <= vmax + 1e-12) or abs(lots / step - round(lots / step)) > 1e-6:
            raise ConfigError(f"{symbol}: объём {lots} не на сетке брокера (шаг {step}, диапазон [{vmin}, {vmax}])")
        return int(round(lots * spec["lotSize"]))

    def ensure_entry_allowed(self, symbol: str) -> None:
        mode = (self._specs.get(symbol) or {}).get("tradeMode")
        if mode and mode != TRADE_MODE_FULL:
            raise MarketClosedError(f"{symbol}: торговля символом сейчас ограничена брокером (tradeMode {mode}): новые входы запрещены")

    # ----------------------------------------------------------------------------------------- котировки и свечи

    def _on_spot(self, msg: dict[str, Any]) -> None:
        sid = msg.get("symbolId")
        if sid is None:
            return
        q = self._quotes.setdefault(int(sid), {})
        if msg.get("bid"):
            q["bid"] = msg["bid"] / P.PRICE_SCALE
        if msg.get("ask"):
            q["ask"] = msg["ask"] / P.PRICE_SCALE
        q["ts"] = msg.get("timestamp") or int(self._clock() * 1000)
        ev = self._quote_events.get(int(sid))
        if ev is not None and "bid" in q and "ask" in q:
            ev.set()

    async def _ensure_quote(self, sid: int) -> dict[str, Any]:
        """Подписка на котировки символа (один раз за соединение) и ожидание первой."""
        self._require_connected()
        if sid not in self._subscribed:
            self._quote_events.setdefault(sid, asyncio.Event())
            await self._request("SubscribeSpotsReq", {"ctidTraderAccountId": self._creds.account_id, "symbolId": [sid]}, "SubscribeSpotsRes")
            self._subscribed.add(sid)
        q = self._quotes.get(sid, {})
        if "bid" not in q or "ask" not in q:
            ev = self._quote_events.setdefault(sid, asyncio.Event())
            try:
                await asyncio.wait_for(ev.wait(), QUOTE_WAIT_S)
            except asyncio.TimeoutError as exc:
                raise CTraderError(f"cTrader: нет котировки по символу id={sid} за {QUOTE_WAIT_S:g} с (рынок закрыт?)") from exc
            self._require_connected()
            q = self._quotes.get(sid, {})
        if "bid" not in q or "ask" not in q:
            raise CTraderError(f"cTrader: котировка id={sid} неполная: {q}")
        return q

    async def get_price(self, symbol: str) -> Quote:
        """Последние Bid/Ask из потока котировок (вне торговых часов - последние известные). `loss_tick_value` считается в валюте депозита (конвертация по курсу пары)."""
        sid = self._symbol_id(symbol)
        q = await self._ensure_quote(sid)
        bid, ask = float(q["bid"]), float(q["ask"])
        if bid <= 0 or ask <= 0 or ask < bid:
            raise CTraderError(f"{symbol}: некорректная котировка bid={bid} ask={ask}")
        tick = None
        spec = self._specs.get(symbol)
        if spec is not None:
            rate = await self._rate_to_deposit(str(spec["profitCurrency"]))
            tick = None if rate is None else spec["tickSize"] * spec["contractSize"] * rate
        when = datetime.fromtimestamp(q["ts"] / 1000, tz=timezone.utc) if q.get("ts") else None
        return Quote(symbol, bid, ask, when, tick, tick)

    async def _rate_to_deposit(self, quote_ccy: str) -> float | None:
        """Сколько единиц валюты депозита стоит 1 единица `quote_ccy` (для стоимости тика). Нет подходящей пары - None. Пара поиска запоминается (get_price зовут раз в секунду)."""
        assert self._trader is not None
        deposit = self._assets.get(int(self._trader.get("depositAssetId") or 0), "")
        if not quote_ccy or not deposit:
            return None
        if quote_ccy == deposit:
            return 1.0
        if quote_ccy not in self._conversion:
            self._conversion[quote_ccy] = None
            for light in self._light.values():
                base, quote = self._assets.get(int(light.get("baseAssetId") or 0)), self._assets.get(int(light.get("quoteAssetId") or 0))
                if (base, quote) in ((quote_ccy, deposit), (deposit, quote_ccy)) and light.get("enabled", True):
                    self._conversion[quote_ccy] = (int(light["symbolId"]), base == quote_ccy)
                    break
        pair = self._conversion[quote_ccy]
        if pair is None:
            return None
        q = await self._ensure_quote(pair[0])
        mid = (float(q["bid"]) + float(q["ask"])) / 2
        return mid if pair[1] else 1.0 / mid

    async def get_candles(self, symbol: str, timeframe: str, limit: int, *, start_time: datetime | None = None) -> list[tuple[int, float, float, float, float, float]]:
        """Исторические свечи: [(open_time_ms UTC, open, high, low, close, tick_volume), ...] по возрастанию. `start_time` - самое позднее время (свечи грузятся назад от него);
        без него - до текущего момента (последняя может быть ещё формирующейся: отбрасывает вызывающий). Окно запроса - максимум, допустимый периодом
        (для 5m 84 часа), поэтому страница может содержать меньше `limit` свечей (выходные): листать назад должен вызывающий."""
        if not 1 <= limit <= 1000:
            raise ConfigError(f"get_candles: limit в [1, 1000], получено {limit}")
        period = P.PERIOD.get(timeframe)
        if period is None:
            raise ConfigError(f"get_candles: неизвестный таймфрейм {timeframe!r}")
        sid = self._symbol_id(symbol)
        self._require_connected()
        to_ms = int(start_time.timestamp() * 1000) if start_time is not None else int(self._clock() * 1000)
        from_ms = to_ms - P.PERIOD_MAX_WINDOW_MS[period]
        async with self._history_lock:  # 5 запросов трендбаров в секунду
            wait = self._last_history + HISTORY_PAUSE_S - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_history = time.monotonic()
            res = await self._request("GetTrendbarsReq", {"ctidTraderAccountId": self._creds.account_id, "fromTimestamp": from_ms, "toTimestamp": to_ms,
                                                          "period": period, "symbolId": sid, "count": limit}, "GetTrendbarsRes")
        out = []
        for b in res.get("trendbar", []):
            if b.get("low") is None or b.get("utcTimestampInMinutes") is None:
                raise CTraderError(f"{symbol}: трендбар без цены/времени: {b}")
            low = b["low"]
            out.append((int(b["utcTimestampInMinutes"]) * 60_000, (low + b.get("deltaOpen", 0)) / P.PRICE_SCALE, (low + b.get("deltaHigh", 0)) / P.PRICE_SCALE,
                        low / P.PRICE_SCALE, (low + b.get("deltaClose", 0)) / P.PRICE_SCALE, float(b.get("volume") or 0)))
        out.sort()
        return out[-limit:]

    # ----------------------------------------------------------------------------------------- счёт

    def _money(self, raw: int | None, digits: int | None = None) -> float:
        assert self._trader is not None
        d = digits if digits is not None else int(self._trader.get("moneyDigits") or 2)
        return (raw or 0) / 10 ** d

    async def get_account_info(self) -> AccountInfo:
        """Баланс, средства и маржа счёта. Equity = баланс + нереализованный чистый PnL всех позиций (`ProtoOAGetPositionUnrealizedPnLReq`, ответ сервера, без самодельной
        конвертации валют); занятая маржа - сумма usedMargin позиций."""
        self._require_connected()
        res = await self._request("TraderReq", {"ctidTraderAccountId": self._creds.account_id}, "TraderRes")
        self._trader = trader = res["trader"]
        balance = self._money(trader.get("balance"))
        pnl = await self._request("GetPositionUnrealizedPnLReq", {"ctidTraderAccountId": self._creds.account_id}, "GetPositionUnrealizedPnLRes")
        digits = int(pnl.get("moneyDigits") or trader.get("moneyDigits") or 2)
        unrealized = sum(self._money(p.get("netUnrealizedPnL"), digits) for p in pnl.get("positionUnrealizedPnL", []))
        rec = await self._request("ReconcileReq", {"ctidTraderAccountId": self._creds.account_id}, "ReconcileRes")
        used = sum(self._money(p.get("usedMargin"), int(p.get("moneyDigits") or digits)) for p in rec.get("position", []))
        equity = balance + unrealized
        lev = trader.get("leverageInCents")
        return AccountInfo(
            broker=str(trader.get("brokerName") or ""), server=str(self.host_used or ""), currency=self._assets.get(int(trader.get("depositAssetId") or 0), ""),
            balance=balance, equity=equity, margin=used, free_margin=equity - used, leverage=(lev / 100.0 if lev else 0.0),
            trade_allowed=trader.get("accessRights", 0) == 0, margin_mode="HEDGED" if trader.get("accountType", 0) == 0 else "NETTED", name=str(trader.get("brokerName") or ""),
            login=trader.get("traderLogin"))

    async def get_balance(self) -> Balance:
        info = await self.get_account_info()
        return Balance(asset=info.currency, wallet=info.balance, available=info.free_margin, unrealized_pnl=info.equity - info.balance)

    async def calculate_margin(self, symbol: str, side: str, volume: float, price: float) -> float:
        """Маржа под ордер (валюта депозита). cTrader не отдаёт расчёт маржи запросом: оценка = объём в единицах * цена * курс базовой валюты / плечо счёта (верна для Forex;
        для металлов и CFD - грубее: ❓ сверить с терминалом). Запас безопасности - на вызывающем (`mt5_risk.max_margin_use`)."""
        _side(side)
        spec = self._spec(symbol)
        lev = float(((self._trader or {}).get("leverageInCents") or 0)) / 100.0
        if not lev > 0:
            raise CTraderError(f"{symbol}: у счёта нет плеча - маржу не оценить")
        base_rate = await self._rate_to_deposit(str(spec["marginCurrency"]))
        if base_rate is None:
            raise CTraderError(f"{symbol}: нет курса {spec['marginCurrency']} к валюте депозита - маржу не оценить")
        return _positive(volume, "volume") * spec["contractSize"] * base_rate / lev

    async def realized_pnl_since(self, start_ms: int, end_ms: int | None = None) -> float:
        """Реализованный результат за интервал в валюте депозита: по сделкам счёта сумма (grossProfit + swap + commission закрывающих сделок) и комиссий всех сделок. Убыток отрицателен."""
        self._require_connected()
        end = end_ms if end_ms is not None else start_ms + 86_400_000
        total, cursor, seen = 0.0, end, 0
        while True:
            res = await self._request("DealListReq", {"ctidTraderAccountId": self._creds.account_id, "fromTimestamp": start_ms, "toTimestamp": cursor, "maxRows": _DEALS_PAGE},
                                      "DealListRes")
            deals = res.get("deal", [])
            for d in deals:
                digits = int(d.get("moneyDigits") or (self._trader or {}).get("moneyDigits") or 2)
                total += self._money(d.get("commission"), digits)
                close = d.get("closePositionDetail")
                if close:
                    total += self._money(close.get("grossProfit"), digits) + self._money(close.get("swap"), digits)
            seen += len(deals)
            if not res.get("hasMore") or not deals:
                return total
            if seen > _DEALS_MAX:
                raise CTraderError(f"слишком много сделок за интервал (> {_DEALS_MAX}): суточный результат не посчитать")
            cursor = min(int(d["executionTimestamp"]) for d in deals) - 1

    # ----------------------------------------------------------------------------------------- позиции (Position Recovery)

    def _position(self, p: Mapping[str, Any]) -> CTraderPosition:
        td = p.get("tradeData") or {}
        sid = int(td.get("symbolId") or 0)
        light = self._light_by_id.get(sid)
        if light is None:
            raise CTraderError(f"позиция {p.get('positionId')}: неизвестный символ id={sid}")
        symbol = str(light["symbolName"])
        lot = float((self._specs.get(symbol) or self._sym.get(sid) or {}).get("lotSize") or 0)
        if not lot > 0:
            lot = float((self._sym.get(sid) or {}).get("lotSize") or 0)
        lots = int(td.get("volume") or 0) / lot if lot > 0 else float(td.get("volume") or 0)
        return CTraderPosition(str(p["positionId"]), symbol, "BUY" if td.get("tradeSide") == P.SIDE_BUY else "SELL", lots, float(p.get("price") or 0.0),
                               _opt(p.get("stopLoss")), _opt(p.get("takeProfit")), 0.0, td.get("label") or None)

    async def _ensure_lot_sizes(self, sids: set[int]) -> None:
        """lotSize нужен для перевода объёма позиции в лоты; догружаем недостающие символы."""
        need = [s for s in sids if not (self._sym.get(s) or {}).get("lotSize")]
        if need:
            res = await self._request("SymbolByIdReq", {"ctidTraderAccountId": self._creds.account_id, "symbolId": need}, "SymbolByIdRes")
            for s in res.get("symbol", []):
                self._sym[int(s["symbolId"])] = s

    async def reconcile(self) -> dict[str, Any]:
        """Position Recovery: открытые позиции и отложенные ордера счёта прямо с сервера (`ProtoOAReconcileReq`). Результат - снимок в `self.positions`."""
        self._require_connected()
        res = await self._request("ReconcileReq", {"ctidTraderAccountId": self._creds.account_id}, "ReconcileRes")
        raw_positions = [p for p in res.get("position", []) if p.get("positionStatus", P.POS_OPEN) == P.POS_OPEN]
        await self._ensure_lot_sizes({int((p.get("tradeData") or {}).get("symbolId") or 0) for p in raw_positions} | {int((o.get("tradeData") or {}).get("symbolId") or 0) for o in res.get("order", [])})
        self.positions = [self._position(p) for p in raw_positions]
        res["_positions"] = self.positions
        return res

    async def get_positions(self) -> list[CTraderPosition]:
        return list((await self.reconcile())["_positions"])

    async def get_position(self, symbol: str) -> PositionInfo:
        """Чистая позиция по символу: лоты со знаком (> 0 buy, < 0 sell) и средняя цена входа (0.0, пока позиции нет)."""
        rows = [p for p in await self.get_positions() if p.symbol == symbol]
        signed = sum(p.lots if p.side == "BUY" else -p.lots for p in rows)
        total = sum(p.lots for p in rows)
        return PositionInfo(symbol, round(signed, 8), sum(p.open_price * p.lots for p in rows) / total if total else 0.0)

    async def get_orders(self) -> list[Mapping[str, Any]]:
        """Отложенные ордера (словари: id, symbol, clientId, type BUY_LIMIT/SELL_LIMIT..., volume в лотах, openPrice)."""
        res = await self.reconcile()
        out = []
        for o in res.get("order", []):
            if o.get("orderStatus", 1) != 1:
                continue
            td = o.get("tradeData") or {}
            sym = self._light_by_id.get(int(td.get("symbolId") or 0))
            lot = float((self._sym.get(int(td.get("symbolId") or 0)) or {}).get("lotSize") or 0)
            if sym is None:
                continue
            kind = {P.ORDER_LIMIT: "LIMIT", P.ORDER_STOP: "STOP"}.get(o.get("orderType"), "MARKET")
            out.append({"id": str(o["orderId"]), "symbol": str(sym["symbolName"]), "clientId": o.get("clientOrderId"), "type": f"{'BUY' if td.get('tradeSide') == P.SIDE_BUY else 'SELL'}_{kind}",
                        "volume": int(td.get("volume") or 0) / lot if lot > 0 else 0.0, "openPrice": o.get("limitPrice") or o.get("stopPrice"),
                        "stopLoss": o.get("stopLoss"), "takeProfit": o.get("takeProfit")})
        return out

    async def get_order(self, order_id: str) -> Mapping[str, Any]:
        for o in await self.get_orders():
            if o["id"] == str(order_id):
                return o
        raise CTraderError(f"ордер {order_id} не найден среди открытых (исполнен или снят)")

    # ----------------------------------------------------------------------------------------- торговля

    async def _trade(self, name: str, values: Mapping[str, Any], done: Any, *, client_order_id: str | None = None, position_id: int | None = None,
                     order_id: int | None = None) -> dict[str, Any]:
        """Отправить торговый запрос и дождаться ИТОГОВОГО события (done(event) -> 'done' | 'reject' | None). Таймаут - исход неизвестен (сверять по clientOrderId)."""
        msg_id = uuid.uuid4().hex[:16]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        pend = _Pending(msg_id, ("ExecutionEvent",), fut, done, client_order_id, position_id, order_id)
        self._pending[msg_id] = pend
        try:
            await self._send(name, values, msg_id)
            return await asyncio.wait_for(fut, TRADE_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            err = CTraderTradeError(f"cTrader: таймаут {TRADE_TIMEOUT_S:g} с ({name}): исход ордера неизвестен, сверьте позиции по clientOrderId", "TIMEOUT_ERROR")
            raise err from exc
        finally:
            self._pending.pop(msg_id, None)

    @staticmethod
    def _until(*done_types: int):
        def check(event: Mapping[str, Any]) -> str | None:
            et = event.get("executionType")
            if et in done_types:
                return "done"
            if et in (P.EXEC_REJECTED, P.EXEC_CANCEL_REJECTED, P.EXEC_EXPIRED) and et not in done_types:
                return "reject"
            return None
        return check

    def _check_stops(self, symbol: str, side: str, sl: float | None, tp: float | None) -> None:
        for v, what in ((sl, "SL"), (tp, "TP")):
            if v is not None:
                _positive(v, what)
        if sl is not None and tp is not None and not (sl < tp if side == "BUY" else tp < sl):
            raise ConfigError(f"{symbol}: SL {sl} и TP {tp} расположены неверно для {side} (BUY: SL < TP, SELL: TP < SL)")

    def _ids(self, symbol: str) -> tuple[int, Mapping[str, Any]]:
        return self._symbol_id(symbol), self._spec(symbol)

    async def market_order(self, symbol: str, side: str, volume: float, sl: float | None = None, tp: float | None = None, *, client_id: str | None = None) -> CTraderOrder:
        """MARKET на `volume` ЛОТОВ с жёсткими SL/TP: в запросе - ОТНОСИТЕЛЬНЫЕ уровни от справочной котировки (позиция открывается сразу с защитой, R6), после исполнения
        они выравниваются на абсолютные цены. Нет котировки - ордер не отправляется. Отказ брокера - `CTraderTradeError`; таймаут после отправки - исход неизвестен (сверять по `client_id`)."""
        _side(side)
        _positive(volume, "volume")
        self._check_stops(symbol, side, sl, tp)
        self.ensure_entry_allowed(symbol)
        sid, spec = self._ids(symbol)
        units = self._volume_units(symbol, volume)
        q = await self._ensure_quote(sid)
        ref = float(q["ask"] if side == "BUY" else q["bid"])
        rel_sl = rel_tp = None
        if sl is not None:
            if (side == "BUY" and sl >= ref) or (side == "SELL" and sl <= ref):
                raise CTraderTradeError(f"{symbol}: SL {sl} не по ту сторону котировки {ref}", "TRADING_BAD_STOPS")
            rel_sl = max(1, int(round(abs(ref - sl) * P.PRICE_SCALE)))
        if tp is not None:
            if (side == "BUY" and tp <= ref) or (side == "SELL" and tp >= ref):
                raise CTraderTradeError(f"{symbol}: TP {tp} не по ту сторону котировки {ref}", "TRADING_BAD_STOPS")
            rel_tp = max(1, int(round(abs(ref - tp) * P.PRICE_SCALE)))
        cid = (client_id or f"{self.id_prefix}{uuid.uuid4().hex[:12]}")[:50]
        ev = await self._trade("NewOrderReq", {
            "ctidTraderAccountId": self._creds.account_id, "symbolId": sid, "orderType": P.ORDER_MARKET, "tradeSide": P.SIDE_BUY if side == "BUY" else P.SIDE_SELL,
            "volume": units, "relativeStopLoss": rel_sl, "relativeTakeProfit": rel_tp, "clientOrderId": cid, "label": cid},
            self._until(P.EXEC_FILLED, P.EXEC_PARTIAL_FILL), client_order_id=cid)
        pos = ev.get("position") or {}
        position_id = pos.get("positionId")
        fill = float(pos.get("price") or (ev.get("deal") or {}).get("executionPrice") or 0.0) or None
        state, message = "DONE", "исполнено"
        if position_id is not None and (sl is not None or tp is not None):
            try:  # относительные -> абсолютные цены (то, что требует R6/выход Shadow)
                await self._amend(int(position_id), sl, tp)
            except CTraderError as exc:
                if isinstance(exc, ConfigError):
                    raise
                log.warning("%s: абсолютные SL/TP не выставлены (позиция %s держит относительные уровни от цены исполнения): %s", symbol, position_id, exc)
                message = f"относительные SL/TP: {exc}"
        return CTraderOrder(symbol, cid, str(ev.get("order", {}).get("orderId", "")) or None, None if position_id is None else str(position_id), state, message,
                            "PARTIAL_FILL" if ev.get("executionType") == P.EXEC_PARTIAL_FILL else "FILLED", fill)

    async def limit_order(self, symbol: str, side: str, volume: float, price: float, sl: float | None = None, tp: float | None = None, *,
                          client_id: str | None = None, expires_at: datetime | None = None) -> CTraderOrder:
        """Отложенный лимитный ордер на `volume` лотов с абсолютными SL/TP (для LIMIT-ордера сервер их принимает); при `expires_at` (aware datetime) - со сроком."""
        _side(side)
        _positive(volume, "volume")
        _positive(price, "цена лимитки")
        self._check_stops(symbol, side, sl, tp)
        self.ensure_entry_allowed(symbol)
        sid, spec = self._ids(symbol)
        units = self._volume_units(symbol, volume)
        cid = (client_id or f"{self.id_prefix}{uuid.uuid4().hex[:12]}")[:50]
        values: dict[str, Any] = {"ctidTraderAccountId": self._creds.account_id, "symbolId": sid, "orderType": P.ORDER_LIMIT,
                                  "tradeSide": P.SIDE_BUY if side == "BUY" else P.SIDE_SELL, "volume": units, "limitPrice": self.round_price(symbol, price),
                                  "stopLoss": sl, "takeProfit": tp, "clientOrderId": cid, "label": cid}
        if expires_at is not None:
            if expires_at.tzinfo is None:
                raise ConfigError("expires_at должен быть aware datetime (UTC)")
            values.update(timeInForce=P.TIF_GTD, expirationTimestamp=int(expires_at.timestamp() * 1000))
        ev = await self._trade("NewOrderReq", values, self._until(P.EXEC_ACCEPTED, P.EXEC_FILLED), client_order_id=cid)
        order = ev.get("order") or {}
        pos = ev.get("position") or {}
        return CTraderOrder(symbol, cid, str(order["orderId"]) if "orderId" in order else None, str(pos["positionId"]) if "positionId" in pos else None,
                            "PLACED", "ордер принят", "ORDER_ACCEPTED")

    async def _amend(self, position_id: int, sl: float | None, tp: float | None) -> dict[str, Any]:
        return await self._trade("AmendPositionSLTPReq", {"ctidTraderAccountId": self._creds.account_id, "positionId": position_id, "stopLoss": sl, "takeProfit": tp},
                                 self._until(P.EXEC_REPLACED), position_id=position_id)

    async def modify_position(self, position_id: str, sl: float | None = None, tp: float | None = None) -> CTraderOrder:
        """Изменить SL и/или TP открытой позиции. `None` - уровень не менять: клиент читает позицию и ВСЕГДА передаёт оба уровня (запрос без поля мог бы сбросить уровень)."""
        if sl is None and tp is None:
            raise ConfigError("modify_position: нужен хотя бы один из sl / tp")
        current = next((p for p in await self.get_positions() if p.position_id == str(position_id)), None)
        if current is None:
            raise _trade_error(f"позиция {position_id} не найдена", "POSITION_NOT_FOUND")
        new_sl = sl if sl is not None else current.stop_loss
        new_tp = tp if tp is not None else current.take_profit
        self._check_stops(current.symbol, current.side, new_sl, new_tp)
        await self._amend(int(position_id), None if new_sl is None else self.round_price(current.symbol, new_sl), None if new_tp is None else self.round_price(current.symbol, new_tp))
        return CTraderOrder(current.symbol, current.client_id, None, str(position_id), "DONE", "SL/TP изменены", "ORDER_REPLACED")

    async def stop_loss(self, position_id: str, price: float) -> CTraderOrder:
        return await self.modify_position(position_id, sl=_positive(price, "price"))

    async def take_profit(self, position_id: str, price: float) -> CTraderOrder:
        return await self.modify_position(position_id, tp=_positive(price, "price"))

    async def close_position(self, position_id: str, volume: float | None = None, *, client_id: str | None = None) -> CTraderOrder:
        """Закрыть позицию целиком (`volume=None`) или часть (в лотах) встречным рыночным ордером."""
        current = next((p for p in await self.get_positions() if p.position_id == str(position_id)), None)
        if current is None:
            raise _trade_error(f"позиция {position_id} не найдена", "POSITION_NOT_FOUND")
        lots = current.lots if volume is None else _positive(volume, "volume")
        if lots > current.lots + 1e-9:
            raise ConfigError(f"{current.symbol}: закрыть {lots} лотов, а в позиции {position_id} только {current.lots}")
        lot = float(self._spec(current.symbol)["lotSize"]) if current.symbol in self._specs else float((self._sym.get(self._symbol_id(current.symbol)) or {}).get("lotSize") or 0)
        units = int(round(lots * lot))
        if not units > 0:
            raise ConfigError(f"{current.symbol}: объём закрытия {lots} лотов даёт 0")
        ev = await self._trade("ClosePositionReq", {"ctidTraderAccountId": self._creds.account_id, "positionId": int(position_id), "volume": units},
                               self._until(P.EXEC_FILLED, P.EXEC_PARTIAL_FILL), position_id=int(position_id))
        return CTraderOrder(current.symbol, client_id, None, str(position_id), "DONE", "закрыто", "FILLED", float((ev.get("deal") or {}).get("executionPrice") or 0) or None)

    async def cancel_order(self, order_id: str) -> CTraderOrder:
        await self._trade("CancelOrderReq", {"ctidTraderAccountId": self._creds.account_id, "orderId": int(order_id)}, self._until(P.EXEC_CANCELLED), order_id=int(order_id))
        return CTraderOrder("", None, str(order_id), None, "DONE", "ордер снят", "ORDER_CANCELLED")


async def fetch_accounts(client_id: str, client_secret: str, access_token: str, *, hosts: tuple[str, ...] | None = None, port: int = PORT,
                         use_tls: bool = True) -> list[dict[str, Any]]:
    """Счета, доступные токену (`ProtoOAGetAccountListByAccessTokenReq`), с demo- и live-сервера: [{ctidTraderAccountId, isLive, traderLogin, brokerTitleShort, host}].
    Одноразовое подключение без авторизации счёта - для `scripts/get_ctrader_token.py`. Одинаковые счета с разных хостов склеиваются."""
    found: dict[int, dict[str, Any]] = {}
    for host in hosts or tuple(HOSTS.values()):
        c = CTraderConnector({CLIENT_ID_ENV: client_id, CLIENT_SECRET_ENV: client_secret, ACCOUNT_ENV: "1", TOKEN_ENV: access_token}, host=host, port=port, use_tls=use_tls)
        try:
            await c._open(host)
            await c._request("ApplicationAuthReq", {"clientId": client_id, "clientSecret": client_secret}, "ApplicationAuthRes")
            for acc in await c._account_list():
                found.setdefault(int(acc["ctidTraderAccountId"]), {**acc, "host": host})
        finally:
            await c.close()
    return list(found.values())
