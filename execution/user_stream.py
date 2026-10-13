"""Приватный поток Binance USDⓈ-M Futures (User Data Stream): статусы ордеров и позиции без опроса REST.

Маршрут: wss://fstream.binance.com/private/ws?listenKey=<ключ>&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE.
Старые /ws и /stream отключены (см. config.WS_BASE_URL), пользовательские данные идут через /private.
Точный формат параметра events сверен с документацией Binance, но живьём не проверялся:
проверьте на тестовой сети (BINANCE_REST_URL / BINANCE_WS_PRIVATE_URL) до боевого запуска.

listenKey: POST /fapi/v1/listenKey (нужен только X-MBX-APIKEY, подпись не нужна) -> продление PUT каждые
LISTEN_KEY_KEEPALIVE_S (ключ живёт 60 минут) -> DELETE при выходе.

В отличие от сборщика стакана (fail-fast, перезапускает Docker), приватный поток ПЕРЕПОДКЛЮЧАЕТСЯ сам:
пока бот держит позицию, выход из процесса дороже. Но каждый обрыв - это дыра, в которой могли пройти
исполнения. Поэтому после КАЖДОГО подключения (включая первое) вызывается on_connected(reconnect):
вызывающий обязан сверить состояние по REST (LiveBarrier.resync_all). Порядок важен: сначала сокет
открыт (события копятся в буфере), потом сверка - иначе событие между сверкой и открытием потерялось бы.
Дубли после сверки безопасны: барьер дедуплицирует исполнения по id сделки.

Тишина в приватном потоке - норма (нет ордеров), поэтому таймаут молчания, как у стакана, не ставится;
зависшее соединение ловит ping/pong (heartbeat). Битое сообщение - DataError и выход: в торговом
потоке мусор нельзя молча пропускать. Ошибка обработчика тоже выходит наружу.
"""

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiohttp

import config
from backtester.contracts import Side
from execution.rest import ERR_BAD_LISTEN_KEY, BinanceAPIError, SignedRest
from utils.binance_rest import parse_retry_after
from utils.errors import (
    DataError,
    HttpServerError,
    NetworkError,
    RateLimitError,
    ServerShutdown,
    StreamStalled,
    WebSocketClosed,
)

log = logging.getLogger("user_stream")


# ---------------------------------------------------------------------------- события


@dataclass(frozen=True, slots=True)
class OrderUpdate:
    """ORDER_TRADE_UPDATE. Числа - float; commission / commission_asset - None, если исполнения нет."""

    event_time: int
    symbol: str
    client_order_id: str
    order_id: int
    side: Side
    order_type: str  # MARKET / LIMIT / ... как у биржи
    orig_qty: float
    price: float
    exec_type: str  # NEW / TRADE / CANCELED / EXPIRED / AMENDMENT / CALCULATED
    status: str  # NEW / PARTIALLY_FILLED / FILLED / CANCELED / EXPIRED / REJECTED / EXPIRED_IN_MATCH
    last_qty: float  # объём ЭТОГО исполнения (0, если исполнения нет)
    cum_qty: float  # накопленный объём ордера
    last_price: float
    trade_time: int
    trade_id: int  # 0, если исполнения нет
    is_maker: bool
    reduce_only: bool
    commission: float | None
    commission_asset: str | None
    position_side: str

    @property
    def has_trade(self) -> bool:
        return self.exec_type == "TRADE" and self.last_qty > 0


@dataclass(frozen=True, slots=True)
class PositionUpdate:
    symbol: str
    amount: float  # знаковая позиция (pa)
    entry_price: float
    position_side: str


@dataclass(frozen=True, slots=True)
class BalanceUpdate:
    asset: str
    wallet_balance: float
    balance_change: float


@dataclass(frozen=True, slots=True)
class AccountUpdate:
    """ACCOUNT_UPDATE: изменения позиций и баланса. reason - a.m (ORDER, FUNDING_FEE, DEPOSIT, ...)."""

    event_time: int
    reason: str
    balances: tuple[BalanceUpdate, ...]
    positions: tuple[PositionUpdate, ...]


@dataclass(frozen=True, slots=True)
class AlgoUpdate:
    """ALGO_UPDATE: создание и смена статуса условного (algo) ордера (Algo Service, Этап 4).

    Поля события `o` (по документации Binance, Event Algo Order Update): caid - clientAlgoId, aid - algoId, at - тип
    algo (CONDITIONAL), o - тип ордера, s - символ, S - сторона, ps - positionSide, q - объём, X - статус
    (NEW -> TRIGGERING -> TRIGGERED -> FINISHED | CANCELED | REJECTED | EXPIRED), tp - цена триггера,
    wt - workingType, cp - closePosition, R - reduceOnly, rm - причина отказа. ai / ap / aq / act заполнены,
    только когда ордер сработал и попал в матчинг-движок: ai - id ордера в движке (по нему ORDER_TRADE_UPDATE
    опознаётся как исполнение этого algo-ордера: id `c` порождённого ордера в документации не описан).
    """

    event_time: int
    client_algo_id: str
    algo_id: int
    algo_type: str
    order_type: str
    symbol: str
    side: Side
    position_side: str
    qty: float
    status: str
    actual_order_id: int | None  # ai: None, пока не сработал
    avg_price: float
    executed_qty: float
    trigger_price: float
    working_type: str
    close_position: bool
    reduce_only: bool
    reason: str  # rm: причина отказа, пусто в штатных статусах


@dataclass(frozen=True, slots=True)
class RawEvent:
    """Любое другое событие (MARGIN_CALL, ACCOUNT_CONFIG_UPDATE, ...): отдаётся как есть."""

    name: str
    payload: dict


StreamEvent = OrderUpdate | AccountUpdate | AlgoUpdate | RawEvent
ALGO_STATUSES = frozenset({"NEW", "TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "REJECTED", "EXPIRED"})
Handler = Callable[[StreamEvent], Awaitable[None] | None]


class ListenKeyExpired(Exception):
    """Биржа сообщила listenKeyExpired: нужен новый ключ и новое соединение (штатно, не ошибка)."""


def _num(src: dict, key: str, ctx: str) -> float:
    try:
        return float(src[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise DataError(f"{ctx}: нет числового поля {key!r}") from exc


def _int(src: dict, key: str, ctx: str) -> int:
    value = src.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DataError(f"{ctx}: поле {key!r} должно быть целым, получено {value!r}")
    return value


def _str(src: dict, key: str, ctx: str) -> str:
    value = src.get(key)
    if not isinstance(value, str) or not value:
        raise DataError(f"{ctx}: поле {key!r} должно быть непустой строкой, получено {value!r}")
    return value


def parse_order_update(payload: dict) -> OrderUpdate:
    ctx = "ORDER_TRADE_UPDATE"
    o = payload.get("o")
    if not isinstance(o, dict):
        raise DataError(f"{ctx}: нет объекта 'o': {str(payload)[:200]}")
    try:
        side = Side(_str(o, "S", ctx).lower())
    except ValueError as exc:
        raise DataError(f"{ctx}: неизвестная сторона {o.get('S')!r}") from exc
    commission = o.get("n")
    return OrderUpdate(
        event_time=_int(payload, "E", ctx),
        symbol=_str(o, "s", ctx),
        client_order_id=_str(o, "c", ctx),
        order_id=_int(o, "i", ctx),
        side=side,
        order_type=_str(o, "o", ctx),
        orig_qty=_num(o, "q", ctx),
        price=_num(o, "p", ctx),
        exec_type=_str(o, "x", ctx),
        status=_str(o, "X", ctx),
        last_qty=_num(o, "l", ctx),
        cum_qty=_num(o, "z", ctx),
        last_price=_num(o, "L", ctx),
        trade_time=_int(o, "T", ctx),
        trade_id=_int(o, "t", ctx),
        is_maker=bool(o.get("m", False)),
        reduce_only=bool(o.get("R", False)),
        commission=float(commission) if commission is not None else None,
        commission_asset=o.get("N") if commission is not None else None,
        position_side=str(o.get("ps", "BOTH")),
    )


def parse_account_update(payload: dict) -> AccountUpdate:
    ctx = "ACCOUNT_UPDATE"
    a = payload.get("a")
    if not isinstance(a, dict):
        raise DataError(f"{ctx}: нет объекта 'a': {str(payload)[:200]}")
    balances = tuple(
        BalanceUpdate(_str(b, "a", ctx), _num(b, "wb", ctx), _num(b, "bc", ctx)) for b in a.get("B", ())
    )
    positions = tuple(
        PositionUpdate(_str(p, "s", ctx), _num(p, "pa", ctx), _num(p, "ep", ctx), str(p.get("ps", "BOTH")))
        for p in a.get("P", ())
    )
    return AccountUpdate(_int(payload, "E", ctx), str(a.get("m", "")), balances, positions)


def _opt_num(src: dict, key: str, ctx: str) -> float:
    """Числовое поле, которое до срабатывания может быть пустым/нулевым: '' и отсутствие -> 0.0, мусор -> DataError."""
    value = src.get(key)
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise DataError(f"{ctx}: поле {key!r} не число: {value!r}") from exc


def _opt_order_id(src: dict, key: str, ctx: str) -> int | None:
    """ai: '' / 0 / отсутствует -> None (ордер ещё не в движке); число или строка-число -> int; иначе DataError."""
    value = src.get(key)
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, bool):
        raise DataError(f"{ctx}: поле {key!r} не id: {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise DataError(f"{ctx}: поле {key!r} не id: {value!r}") from exc


def parse_algo_update(payload: dict) -> AlgoUpdate:
    ctx = "ALGO_UPDATE"
    o = payload.get("o")
    if not isinstance(o, dict):
        raise DataError(f"{ctx}: нет объекта 'o': {str(payload)[:200]}")
    try:
        side = Side(_str(o, "S", ctx).lower())
    except ValueError as exc:
        raise DataError(f"{ctx}: неизвестная сторона {o.get('S')!r}") from exc
    status = _str(o, "X", ctx)
    if status not in ALGO_STATUSES:
        raise DataError(f"{ctx}: неизвестный статус {status!r} (известны {sorted(ALGO_STATUSES)}): торговый поток не пропускает неизвестное")
    return AlgoUpdate(
        event_time=_int(payload, "E", ctx),
        client_algo_id=_str(o, "caid", ctx),
        algo_id=_int(o, "aid", ctx),
        algo_type=str(o.get("at", "CONDITIONAL")),
        order_type=_str(o, "o", ctx),
        symbol=_str(o, "s", ctx),
        side=side,
        position_side=str(o.get("ps", "BOTH")),
        qty=_opt_num(o, "q", ctx),
        status=status,
        actual_order_id=_opt_order_id(o, "ai", ctx),
        avg_price=_opt_num(o, "ap", ctx),
        executed_qty=_opt_num(o, "aq", ctx),
        trigger_price=_opt_num(o, "tp", ctx),
        working_type=str(o.get("wt", "")),
        close_position=bool(o.get("cp", False)),
        reduce_only=bool(o.get("R", False)),
        reason=str(o.get("rm", "") or ""),
    )


def parse_event(raw: str) -> StreamEvent:
    """Разбор кадра. listenKeyExpired -> ListenKeyExpired, serverShutdown -> ServerShutdown."""
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise DataError(f"Сообщение не JSON: {raw[:200]!r}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"Сообщение не объект: {raw[:200]!r}")
    if isinstance(payload.get("data"), dict) and "stream" in payload:  # режим /stream: конверт
        payload = payload["data"]
    name = payload.get("e")
    if name == "ORDER_TRADE_UPDATE":
        return parse_order_update(payload)
    if name == "ACCOUNT_UPDATE":
        return parse_account_update(payload)
    if name == "ALGO_UPDATE":
        return parse_algo_update(payload)
    if name == "listenKeyExpired":
        raise ListenKeyExpired(raw[:200])
    if name == "serverShutdown":
        raise ServerShutdown(f"Биржа сообщила об отключении: {raw[:200]}")
    if not isinstance(name, str):
        raise DataError(f"Сообщение без поля 'e': {raw[:200]!r}")
    return RawEvent(name, payload)


# ---------------------------------------------------------------------------- поток

_RECONNECTABLE = (WebSocketClosed, NetworkError, HttpServerError, StreamStalled, ServerShutdown, ListenKeyExpired)


class UserDataStream:
    def __init__(
        self,
        rest: SignedRest,
        session: aiohttp.ClientSession,
        handler: Handler,
        *,
        on_connected: Callable[[bool], Awaitable[None]] | None = None,
        ws_base_url: str | None = None,
        events: tuple[str, ...] | None = None,
        keepalive_s: float | None = None,
        heartbeat_s: float | None = None,
        max_reconnects: int | None = None,
        backoff_s: float | None = None,
        backoff_max_s: float | None = None,
        stable_s: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._rest, self._session, self._handler = rest, session, handler
        self._on_connected = on_connected
        self._ws_base = (ws_base_url or config.WS_PRIVATE_URL).rstrip("/")
        self._events = events or config.USER_STREAM_EVENTS
        self._keepalive_s = keepalive_s if keepalive_s is not None else config.LISTEN_KEY_KEEPALIVE_S
        self._heartbeat_s = heartbeat_s if heartbeat_s is not None else config.USER_STREAM_HEARTBEAT_S
        self._max_reconnects = max_reconnects if max_reconnects is not None else config.USER_STREAM_MAX_RECONNECTS
        self._backoff = backoff_s if backoff_s is not None else config.USER_STREAM_BACKOFF_S
        self._backoff_max = backoff_max_s if backoff_max_s is not None else config.USER_STREAM_BACKOFF_MAX_S
        self._stable_s = stable_s
        self._sleep = sleep
        self.connections = 0  # сколько раз соединение было установлено
        self.messages = 0

    # ------------------------------------------------------------------ listenKey

    async def _start_listen_key(self) -> str:
        data = await self._rest.request("POST", config.LISTEN_KEY_PATH, signed=False)
        key = data.get("listenKey") if isinstance(data, dict) else None
        if not isinstance(key, str) or not key:
            raise DataError(f"Неожиданный ответ {config.LISTEN_KEY_PATH}: {str(data)[:200]}")
        return key

    async def _keepalive_loop(self, key: str) -> None:
        while True:
            await asyncio.sleep(self._keepalive_s)  # реальные часы; подменяемый self._sleep - только пауза между переподключениями
            try:
                await self._rest.request("PUT", config.LISTEN_KEY_PATH, {"listenKey": key}, signed=False)
                log.info("listenKey продлён")
            except BinanceAPIError as exc:
                if exc.code == ERR_BAD_LISTEN_KEY:
                    raise ListenKeyExpired(f"listenKey не принят при продлении: {exc}") from exc
                raise

    async def _close_listen_key(self, key: str) -> None:
        try:
            await self._rest.request("DELETE", config.LISTEN_KEY_PATH, {"listenKey": key}, signed=False)
        except Exception as exc:  # best-effort: ключ и так протухнет
            log.warning("listenKey не закрыт: %s", exc)

    def url(self, key: str) -> str:
        return f"{self._ws_base}/ws?listenKey={key}&events={'/'.join(self._events)}"

    # ------------------------------------------------------------------- основной цикл

    async def run(self) -> None:
        """Работает, пока не отменена задача. Выходит исключением, если переподключения не помогают."""
        failures = 0
        while True:
            started = time.monotonic()
            try:
                await self._session_once(reconnect=self.connections > 0)
            except _RECONNECTABLE as exc:
                if time.monotonic() - started >= self._stable_s:
                    failures = 0  # соединение жило долго: это штатный разрыв (24 часа), а не флаппинг
                failures += 1
                if failures > self._max_reconnects:
                    raise WebSocketClosed(
                        f"Приватный поток не восстановился за {self._max_reconnects} попыток: {exc}"
                    ) from exc
                delay = min(self._backoff_max, self._backoff * 2 ** (failures - 1))
                log.warning("Приватный поток: %s: %s. Переподключение %d/%d через %.1f с",
                            type(exc).__name__, exc, failures, self._max_reconnects, delay)
                await self._sleep(delay)

    async def _session_once(self, *, reconnect: bool) -> None:
        key = await self._start_listen_key()
        ws = None
        tasks: list[asyncio.Task] = []
        try:
            ws = await self._connect(self.url(key))
            self.connections += 1
            log.info("Приватный поток подключён (переподключение: %s)", reconnect)
            if self._on_connected is not None:
                await self._on_connected(reconnect)  # сверка по REST - после открытия сокета
            tasks = [
                asyncio.create_task(self._consume(ws), name="user_stream.consume"),
                asyncio.create_task(self._keepalive_loop(key), name="user_stream.keepalive"),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()  # исключение первой завершившейся задачи уходит наружу
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if ws is not None:
                await ws.close()
            await asyncio.shield(self._close_listen_key(key))

    async def _connect(self, url: str) -> aiohttp.ClientWebSocketResponse:
        try:
            return await asyncio.wait_for(
                self._session.ws_connect(url, heartbeat=self._heartbeat_s, autoping=True),
                timeout=config.WS_CONNECT_TIMEOUT_S,
            )
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in (429, 418):
                raise RateLimitError(exc.status, parse_retry_after(exc.headers or {}, exc.status), url) from exc
            raise NetworkError(f"Рукопожатие приватного потока отклонено: HTTP {exc.status} {exc.message}") from exc
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as exc:
            raise NetworkError(f"Не удалось подключиться к приватному потоку: {type(exc).__name__}: {exc}") from exc

    async def _consume(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                self.messages += 1
                event = parse_event(msg.data)
                result = self._handler(event)
                if inspect.isawaitable(result):
                    await result
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                raise WebSocketClosed(f"Приватный поток закрыт (код {ws.close_code})")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise NetworkError(f"Ошибка WebSocket: {ws.exception()!r}")
            else:
                raise DataError(f"Неожиданный тип кадра WebSocket: {msg.type!r}")
