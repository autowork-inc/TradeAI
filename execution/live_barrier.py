"""LiveBarrier - боевой аналог ExecutionBarrier: те же типы (OrderIntent, OrderTicket, Fill, OrderClosed),
вместо локального стакана - подписанные REST-запросы Binance Futures.

Почему не наследник ExecutionBarrier. Базовый класс проверяет симуляцию: MARKET исполняется ровно в
exec_ts = asof_ts + order_latency_ms, комиссию считает сам барьер, движок сопоставления отдаёт RawFill.
На живой бирже время исполнения и комиссия - факты, присланные биржей; проверки базового класса
отвергли бы настоящие исполнения. Поэтому LiveBarrier повторяет поверхность (submit / advance /
position / open_ticket_ids / now_ts) и выпускает те же события, чтобы Strategy.on_event и портфель
работали без изменений, но отличается по двум пунктам: submit() и отмены - async (идёт сеть), а
Fill.ts / Fill.fee - от биржи. Цикл BacktestEngine синхронный, поэтому боевой цикл - отдельный async-цикл.

Идемпотентность (главное правило этого модуля).
  * clientOrderId создаётся ОДИН раз при submit() и используется во всех повторах:
    tb-<мс в base36>-<20 hex от UUID4>, 32 символа (лимит Binance - 36).
  * Запись «до отправки»: ордер сохраняется в LocalState (на диск) раньше POST. Упал процесс -
    reconcile после перезапуска найдёт ордер по clientOrderId.
  * Таймаут / обрыв / 5xx (в т.ч. -1007) значат «исход неизвестен», а не «ордер не создан». Слепой
    повтор опасен: уникальность clientOrderId Binance гарантирует лишь среди ОТКРЫТЫХ ордеров, поэтому
    после мгновенного исполнения MARKET тот же id создал бы второй ордер и удвоил позицию. Порядок:
      1. GET /fapi/v1/order?origClientOrderId=... : найден -> принимаем его, повтора нет;
      2. нет (-2013) -> ждём, пока исходный запрос протухнет (timestamp + recvWindow): после этого биржа
         его исполнить уже не может; проверяем ещё раз; всё ещё нет -> повтор с тем же id;
      3. выяснить не удалось или попытки кончились -> OrderStatusUnknown и Safe Mode.
  * Исполнения дедуплицируются по (symbol, id сделки): поток, REST-ответ и сверка могут прислать одну
    сделку несколько раз - позиция меняется один раз.
  * 4xx с кодом биржи (фильтр лота, margin, и т.п.) - окончательный отказ: OrderClosed(REJECTED), без повторов.
  * 429/418 - RateLimitError наружу (общий runner запишет .ban_lock); ордер при этом либо не отправлялся
    (забывается), либо исход неизвестен (Safe Mode).

Источники исполнений. Основной - ORDER_TRADE_UPDATE из user_stream (on_order_update): реальные цена,
время и комиссия. Запасной - GET /fapi/v1/userTrades по orderId, когда ответ REST / сверка показывают
исполнение, которого поток не принёс. Оба пишут через один дедуплицирующий путь.

Post-only (Этап 8.2). LIMIT-ордера уходят с timeInForce=GTX: биржа принимает их только пассивными (мейкер).
Ордер, который стал бы тейкером, биржа отклоняет (-5022) или закрывает как EXPIRED - оба случая барьер отдаёт
событием OrderClosed(EXPIRED) («не встал»), а не REJECTED. Срок жизни (ttl_ms) исполняет expire_due: DELETE
/fapi/v1/order и OrderClosed с остатком. Перевыставление (чейзинг) - надстройка execution/smart_execution.py:
каждое submit() создаёт НОВЫЙ clientOrderId и тикет, старый id не переиспользуется (после отмены биржа его
освобождает, и повторное использование было бы неотличимо от дубля). add_listener даёт ей события сразу.

Ограничения: объём и цена должны быть уже округлены под LOT_SIZE / PRICE_FILTER биржи (нарушение -> отказ
-1013 -> REJECTED); только one-way режим позиций; фандинг (FundingSettlement) не выпускается - он виден
в ACCOUNT_UPDATE(reason=FUNDING_FEE) и в балансе; комиссия не в USDT пересчитывается по ставкам
ExecutionConfig с предупреждением. CANCELED закрывается как CloseReason.EXPIRED (в контракте нет
отдельной причины, а отменяет бота только срок жизни ордера).
"""

import asyncio
import logging
import re
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, Decimal
from typing import Any

import config
from backtester.contracts import (
    _QTY_EPS,
    BarrierEvent,
    CloseReason,
    ContractViolation,
    ExecutionConfig,
    Fill,
    Liquidity,
    OrderClosed,
    OrderIntent,
    OrderTicket,
    OrderType,
    Side,
)
from execution.rest import (
    ERR_CANCEL_UNKNOWN,
    ERR_ORDER_NOT_FOUND,
    ERR_POST_ONLY_REJECT,
    ERR_WOULD_TRIGGER,
    BinanceAPIError,
    RequestTrace,
    SignedRest,
)
from execution.state import ALGO_TERMINAL, LocalAlgoOrder, LocalOrder, LocalState, Protection, SafeMode, StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate
from utils.errors import (
    ConfigError,
    DataError,
    HttpServerError,
    NetworkError,
    OrderStatusUnknown,
    RateLimitError,
    SafeModeActive,
)

log = logging.getLogger("live_barrier")

CLIENT_ORDER_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")  # формат Binance
TERMINAL_STATUSES = frozenset({"FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"})
# Отказы, при которых «ордер с таким id уже есть» возможен: сначала смотрим на бирже, потом решаем.
_DUPLICATE_CODES = frozenset({-4116, -4015})
_ABSENT = object()  # биржа подтвердила: такого ордера нет
_EXPIRY_MARGIN_MS = 500
_SEEN_TRADES = 20_000
# Algo Service: код отказа GET algoOrder для неизвестного id в документации не указан; считаем «нет» только эти (как у обычных ордеров),
# любой другой отказ - не гадаем, а поднимаем (fail-closed). ❓ проверить на testnet.
_ALGO_ABSENT_CODES = frozenset({ERR_ORDER_NOT_FOUND, ERR_CANCEL_UNKNOWN})
_ALGO_RANK = {"PENDING": 0, "NEW": 1, "TRIGGERING": 2, "TRIGGERED": 3, "FINISHED": 4, "CANCELED": 4, "EXPIRED": 4, "REJECTED": 4}


def _b36(value: int) -> str:
    digits, out = "0123456789abcdefghijklmnopqrstuvwxyz", ""
    while value:
        value, rem = divmod(value, 36)
        out = digits[rem] + out
    return out or "0"


def new_client_order_id(now_ms: int | None = None) -> str:
    """Уникальный идентификатор ордера: префикс + время + UUID4. Создаётся один раз на ордер."""
    ts = now_ms if now_ms is not None else time.time_ns() // 1_000_000
    cid = f"{config.CLIENT_ORDER_ID_PREFIX}-{_b36(ts)}-{uuid.uuid4().hex[:20]}"
    if not CLIENT_ORDER_ID_RE.match(cid):
        raise DataError(f"Сформирован недопустимый clientOrderId {cid!r}")
    return cid


def new_client_algo_id(now_ms: int | None = None) -> str:
    """Уникальный clientAlgoId защитного ордера: <префикс>a-<время>-<UUID4>, <= 36 символов, формат как у clientOrderId."""
    ts = now_ms if now_ms is not None else time.time_ns() // 1_000_000
    cid = f"{config.CLIENT_ORDER_ID_PREFIX}a-{_b36(ts)}-{uuid.uuid4().hex[:20]}"
    if not CLIENT_ORDER_ID_RE.match(cid):
        raise DataError(f"Сформирован недопустимый clientAlgoId {cid!r}")
    return cid


def fmt_decimal(value: float) -> str:
    """Число в виде, который принимает Binance: без экспоненты и хвостовых нулей (1e-05 -> 0.00001)."""
    text = format(Decimal(repr(float(value))), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _priority(event: BarrierEvent) -> tuple[int, int]:
    return event.ts, 0 if isinstance(event, Fill) else 1  # при равном ts исполнение раньше закрытия


class LiveBarrier:
    def __init__(
        self,
        rest: SignedRest,
        exec_config: ExecutionConfig,
        state: LocalState,
        safe_mode: SafeMode,
        *,
        store: StateStore | None = None,
        quote_asset: str | None = None,
        attempts: int | None = None,
        backoff_s: float | None = None,
        drift_grace_ms: int = 3_000,
        qty_tolerance: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        post_only: bool | None = None,
        algo_grace_ms: int | None = None,
    ):
        """post_only: LIMIT-ордера уходят с timeInForce=GTX (по умолчанию config.LIVE_POST_ONLY): биржа не примет
        ордер, который сразу исполнился бы как тейкер (-5022), и он закроется как «не встал» (EXPIRED), а не REJECTED."""
        self._rest, self._cfg, self._state, self._safe, self._store = rest, exec_config, state, safe_mode, store
        self._post_only = config.LIVE_POST_ONLY if post_only is None else bool(post_only)
        self._listeners: list[Callable[[BarrierEvent], None]] = []
        self._quote = quote_asset or config.QUOTE_ASSET
        self._attempts = attempts if attempts is not None else config.ORDER_RETRY_ATTEMPTS
        self._backoff = backoff_s if backoff_s is not None else config.ORDER_RETRY_BACKOFF_S
        self._drift_grace_ms = drift_grace_ms
        self._tol = qty_tolerance if qty_tolerance is not None else config.RECONCILE_QTY_TOLERANCE
        self._sleep = sleep
        self._now = 0
        self._events: list[BarrierEvent] = []
        self._live: dict[str, OrderTicket] = {}  # clientOrderId -> ticket
        self._inflight: set[str] = set()  # POST ещё идёт: сверка такие ордера не трогает
        self._uncertain: set[str] = set()  # исход отправки неизвестен
        self._needs_resync: set[str] = set()
        self._seen_set: set[tuple[str, int]] = set()
        self._seen_order: deque[tuple[str, int]] = deque()
        self._exchange_pos: dict[str, float] = {}
        self._drift_since: dict[str, int] = {}
        self._warned_assets: set[str] = set()
        # Algo API (Этап 4)
        self._pump_lock = asyncio.Lock()
        self._grace_ms = algo_grace_ms if algo_grace_ms is not None else config.ALGO_UNEXPLAINED_GRACE_MS
        self._unexplained: dict[tuple[str, int], tuple[OrderUpdate, int]] = {}  # исполнения раньше ALGO_UPDATE: (symbol, trade_id) -> (событие, срок)
        self._failed_protection: dict[str, str] = {}  # монета -> причина (REJECTED от биржи): исполнит pump
        self._cancel_siblings: set[str] = set()  # монеты, где сработал один защитный ордер
        self._be_requested: set[str] = set()  # монеты, где mark price достигла +1R
        self._retired_algo: deque[str] = deque(maxlen=500)  # clientAlgoId завершённых/отменённых: опоздавшее эхо - не «чужое»
        self._exit_roles: dict[str, str] = {}  # монета -> роль последнего сработавшего algo-ордера (sl | be_sl | tp)
        for cid, o in state.orders.items():  # ордера из прошлого запуска (после reconcile)
            intent = OrderIntent(o.symbol, o.side, o.qty, o.asof_ts, o.order_type, o.limit_price, o.ttl_ms)
            self._live[cid] = OrderTicket(o.ticket_id, intent, o.exec_ts, o.expire_ts)

    # ----------------------------------------------------------------- публичное

    @property
    def now_ts(self) -> int:
        return self._now

    @property
    def config(self) -> ExecutionConfig:
        return self._cfg

    def position(self, symbol: str) -> float:
        return self._state.position(symbol)

    def protection(self, symbol: str) -> Protection | None:
        """Защита монеты, пока она не снята (позиция жива или остатки algo-ордеров ещё не убраны pump'ом)."""
        return self._state.protections.get(symbol)

    def pop_exit_role(self, symbol: str) -> str | None:
        """Роль algo-ордера, закрывшего позицию (sl | be_sl | tp); None - позицию закрыл не он (рыночное закрытие, вручную)."""
        return self._exit_roles.pop(symbol, None)

    def round_price(self, symbol: str, price: float, rounding: str = ROUND_HALF_UP) -> float:
        """Цена, округлённая до шага PRICE_FILTER (config.LIVE_PRICE_TICK)."""
        return self._to_tick(symbol, price, rounding)

    def open_ticket_ids(self) -> tuple[int, ...]:
        return tuple(sorted(t.ticket_id for t in self._live.values()))

    @property
    def next_ticket_id(self) -> int:
        """Номер, который получит ближайший submit(): тикет выдаётся синхронно, до первого await."""
        return self._state.next_ticket_id

    def add_listener(self, listener: Callable[[BarrierEvent], None]) -> None:
        """Подписка на события в момент их возникновения (Fill, OrderClosed), не дожидаясь advance().

        advance() отдаёт события циклу по правилу видимости (ts < now) раз в шаг цикла; надстройке, которая
        реагирует на закрытие ордера (Smart Execution), ждать этого шага нельзя. Слушатель синхронный и
        быстрый; его исключение не должно ломать учёт исполнений, поэтому оно только логируется."""
        self._listeners.append(listener)

    def _emit(self, event: BarrierEvent) -> None:
        self._events.append(event)
        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                log.exception("Слушатель событий барьера упал на %r", event)

    async def submit(self, intent: OrderIntent, *, reduce_only: bool = False) -> OrderTicket:
        """Отправляет решение стратегии. Возвращает тикет; отказ биржи придёт событием OrderClosed(REJECTED).

        reduce_only=True - закрывающий ордер: разрешён и в Safe Mode. Остальные в Safe Mode -> SafeModeActive.
        """
        if not isinstance(intent, OrderIntent):
            raise ContractViolation(f"submit принимает OrderIntent, получено {type(intent).__name__}")
        if not reduce_only:
            self._safe.require_trading()
        cid = new_client_order_id()
        ticket_id = self._state.next_ticket_id
        self._state.next_ticket_id += 1
        exec_ts = self._cfg.exec_ts(intent.asof_ts)
        expire = exec_ts + intent.ttl_ms if intent.order_type is OrderType.LIMIT else None
        ticket = OrderTicket(ticket_id, intent, exec_ts, expire)
        order = LocalOrder(
            cid, ticket_id, intent.symbol, intent.side, intent.order_type, intent.qty, intent.limit_price,
            intent.ttl_ms, intent.asof_ts, exec_ts, expire, reduce_only,
        )
        self._state.orders[cid] = order
        self._live[cid] = ticket
        self._save(strict=True)  # write-ahead: не сохранили - не отправляем
        self._inflight.add(cid)
        try:
            snapshot = await self._place(order, reduce_only)
        except RateLimitError:
            if cid in self._uncertain:
                self._unknown(order, "429/418 при выяснении исхода отправки")
            else:
                self._forget(cid)  # биржа запрос не обработала
            raise
        except SafeModeActive:
            self._forget(cid)  # включился между попытками: подтверждено, что ордера на бирже нет
            raise
        except BinanceAPIError as exc:
            if exc.code == ERR_POST_ONLY_REJECT:
                # Не ошибка ордера: цена за время пути пересекла стакан. Закрываем как «не встал», чтобы
                # надстройка перевыставила его по новой котировке, а не сочла отказом биржи.
                log.info("Ордер %s (post-only) не принят: стал бы тейкером (код %s)", cid, exc.code)
                self._close(cid, CloseReason.EXPIRED, self._rest.now_ms())
                return ticket
            log.warning("Ордер %s отклонён биржей: %s (код %s)", cid, exc.msg, exc.code)
            self._close(cid, CloseReason.REJECTED, self._rest.now_ms())
            return ticket
        finally:
            self._inflight.discard(cid)
        await self._apply_snapshot(cid, snapshot)
        return ticket

    async def close_position(self, symbol: str, asof_ts: int) -> OrderTicket | None:
        """Закрывает позицию рыночным reduce-only ордером (работает и в Safe Mode)."""
        position = self.position(symbol)
        if abs(position) <= _QTY_EPS:
            return None
        side = Side.SELL if position > 0 else Side.BUY
        return await self.submit(OrderIntent(symbol, side, abs(position), asof_ts), reduce_only=True)

    async def cancel(self, ticket_id: int) -> None:
        cid = self._cid_of(ticket_id)
        if cid is None:
            return
        order = self._state.orders[cid]
        try:
            snapshot = await self._rest.request(
                "DELETE", config.ORDER_PATH, {"symbol": order.symbol, "origClientOrderId": cid}
            )
        except BinanceAPIError as exc:
            if exc.code not in (ERR_CANCEL_UNKNOWN, ERR_ORDER_NOT_FOUND):
                raise
            await self._resync_one(cid)  # уже исполнен / закрыт - узнаём итог
            return
        except (NetworkError, HttpServerError) as exc:
            log.warning("Отмена %s: исход неизвестен (%s), сверю позже", cid, exc)
            self._needs_resync.add(cid)  # отмена идемпотентна: повторится через expire_due / resync
            return
        await self._apply_snapshot(cid, snapshot)

    async def expire_due(self, now_ms: int) -> None:
        """Отменяет LIMIT-ордера, чей срок жизни (expire_ts) истёк. Вызывать из боевого цикла."""
        for cid, ticket in list(self._live.items()):
            order = self._state.orders.get(cid)
            if order is None or ticket.expire_ts is None or ticket.expire_ts > now_ms or cid in self._inflight:
                continue
            await self.cancel(ticket.ticket_id)

    def advance(self, now_ts: int) -> tuple[BarrierEvent, ...]:
        """События с ts СТРОГО меньше now_ts (правило видимости контракта), по возрастанию времени."""
        if isinstance(now_ts, bool) or not isinstance(now_ts, int) or now_ts < self._now:
            raise ContractViolation(f"Время идёт назад или не целое: {now_ts!r} < {self._now}")
        self.check_drift()
        ready = [e for e in self._events if e.ts < now_ts]
        self._events = [e for e in self._events if e.ts >= now_ts]
        ready.sort(key=_priority)
        self._now = now_ts
        return tuple(ready)

    # ------------------------------------------------------------ события потока

    def on_order_update(self, u: OrderUpdate) -> None:
        """Обработчик ORDER_TRADE_UPDATE (синхронный: между шагами нет await, состояние не гоняется)."""
        order = self._state.orders.get(u.client_order_id)
        if order is None:
            if self._algo_owns(u):  # исполнение порождённого algo-ордера (стоп/тейк): своё, не чужое
                self._algo_fill(u)
            else:
                self._foreign(u)
            return
        if u.has_trade:
            self._book_trade(
                order, trade_id=u.trade_id, qty=u.last_qty, price=u.last_price, ts=u.trade_time,
                commission=u.commission, asset=u.commission_asset, maker=u.is_maker,
            )
            order = self._state.orders.get(u.client_order_id)
            if order is None:  # полностью исполнен, ордер закрыт самим исполнением
                return
        if u.cum_qty > order.filled_qty + _QTY_EPS:
            self._needs_resync.add(order.client_order_id)  # пропущены сделки: доберём из userTrades
        if u.status in TERMINAL_STATUSES:
            if order.filled_qty + _QTY_EPS < u.cum_qty:
                return  # закроем после того, как доберём сделки (resync_pending)
            self._close(order.client_order_id, _close_reason(order, u.status), u.event_time)
        elif u.status != order.status:
            order.status = u.status
            self._save()

    def on_account_update(self, u: AccountUpdate) -> None:
        """ACCOUNT_UPDATE: запоминает позиции биржи. Расхождение с локальными ловит check_drift."""
        for p in u.positions:
            if p.position_side != "BOTH":
                self._safe.trip("Режим Hedge (positionSide != BOTH) не поддерживается", {"symbol": p.symbol})
                continue
            self._exchange_pos[p.symbol] = p.amount

    def check_drift(self, now_ms: int | None = None) -> list[str]:
        """Позиция биржи (по ACCOUNT_UPDATE) расходится с локальной дольше drift_grace_ms -> Safe Mode.

        Так ловятся стоп, ликвидация, ADL и ручные действия: их исполнения идут под чужим clientOrderId
        и в позицию бота не попадают. Пауза нужна из-за гонки ORDER_TRADE_UPDATE / ACCOUNT_UPDATE.
        """
        now = now_ms if now_ms is not None else self._rest.now_ms()
        drifted = []
        for symbol, exchange_qty in self._exchange_pos.items():
            local = self._state.position(symbol)
            if abs(exchange_qty - local) <= self._tol:
                self._drift_since.pop(symbol, None)
                continue
            since = self._drift_since.setdefault(symbol, now)
            if now - since >= self._drift_grace_ms:
                drifted.append(symbol)
                self._safe.trip(
                    f"Позиция {symbol} на бирже расходится с локальной", {"symbol": symbol, "exchange": exchange_qty, "local": local},
                )
        return drifted

    # ======================================================================== Algo API (Этап 4)
    #
    # Защита позиции. Стоп и тейк - условные ордера Algo Service (POST/DELETE/GET /fapi/v1/algoOrder), типы STOP_MARKET и
    # TAKE_PROFIT_MARKET с quantity + reduceOnly=true (решение О18; closePosition=true не используется): срабатывая, они
    # только уменьшают позицию и не могут её открыть или развернуть.
    #   * arm_protection() - намерение (стоп, тейк, безубыток) пишется на диск ДО входа (write-ahead, как у ордеров);
    #   * pump() - исполнитель-«выравниватель»: для каждой роли (sl / be_sl / tp) сумма объёмов живых algo-ордеров обязана
    #     покрывать объём позиции; не хватает (вход исполнился частями, ордер не ставился) - ставит ДОБАВКУ на разницу.
    #     Он же переносит стоп в безубыток и разбирает исполнения, пришедшие раньше ALGO_UPDATE; вызывать не реже раза в секунду;
    #   * безубыток (D11, О18): СНАЧАЛА POST нового стопа на входе (роль be_sl), и только когда он покрывает позицию - DELETE
    #     старых стопов. Позиция ни на мгновение не остаётся без стопа на бирже (R6);
    #   * реквизит каждого algo-ордера (clientAlgoId) записывается в LocalState до POST; исполнения порождённого ордера
    #     опознаются по ai (id ордера в движке) из ALGO_UPDATE или запроса GET algoOrder и НЕ считаются «чужими».
    # Любой отказ в защите открытой позиции, у которой нет другого стопа, = немедленное reduce-only закрытие по рынку
    # (+ Safe Mode, кроме обычного рыночного случая «стоп уже за ценой», код -2021): позиция без стопа недопустима (R6).
    # ❓ Принимает ли биржа сумму reduceOnly-ордеров БОЛЬШЕ объёма позиции (SL + TP, а при переносе ещё и be_sl - до 3x),
    # неизвестно: это проверяет scripts/test_algo_live.py. Отказ нового be_sl не опасен: исходный стоп остаётся (be_failed).

    def arm_protection(
        self, symbol: str, *, direction: int, entry_price: float, sl_price: float, ticket_id: int,
        tp_price: float | None = None, tp_r: float | None = None, be_r: float | None = None,
    ) -> Protection:
        """Записывает намерение защиты. direction: +1 лонг, -1 шорт. LIMIT-вход: tp_price известен заранее; MARKET-вход:
        tp_r (3.0) - тейк считается от фактической цены входа. be_r (1.0) - перенос стопа на вход при +be_r*R (D11).
        ticket_id - номер тикета входного ордера: `barrier.next_ticket_id` непосредственно перед submit() (между ними не
        должно быть await). Вызывать ДО отправки входа: если процесс упадёт между входом и стопом, намерение уже на диске."""
        if direction not in (1, -1):
            raise DataError(f"direction должен быть +1 или -1, получено {direction!r}")
        if not (entry_price > 0 and sl_price > 0) or not direction * (entry_price - sl_price) > 0:
            raise DataError(f"{symbol}: стоп {sl_price} должен быть по убыточную сторону входа {entry_price} (direction={direction})")
        if tp_price is not None and (tp_r is not None or not direction * (tp_price - entry_price) > 0):
            raise DataError(f"{symbol}: тейк {tp_price} задан неверно (по прибыльную сторону входа, и без tp_r одновременно)")
        if tp_r is not None and not tp_r > 0 or be_r is not None and not be_r > 0:
            raise DataError(f"{symbol}: tp_r и be_r должны быть > 0")
        if symbol in self._state.protections:
            raise DataError(f"{symbol}: защита уже есть (одна на монету, D4): тикет {self._state.protections[symbol].parent}")
        mode = ROUND_DOWN if direction > 0 else ROUND_UP  # стоп - дальше от входа, тейк - ближе к нему (консервативно)
        prot = Protection(
            symbol=symbol, direction=direction, entry_price=self._to_tick(symbol, entry_price, ROUND_HALF_UP),
            sl_price=self._to_tick(symbol, sl_price, mode), parent=str(ticket_id), created_ms=self._rest.now_ms(),
            tp_price=None if tp_price is None else self._to_tick(symbol, tp_price, mode), tp_r=tp_r, be_r=be_r,
        )
        self._state.protections[symbol] = prot
        self._save(strict=True)
        return prot

    def on_mark_price(self, symbol: str, price: float) -> None:
        """Mark price из потока markPrice@1s (D11, О2): достигла +1R -> перенос стопа на вход запрошен; выполнит pump()."""
        prot = self._state.protections.get(symbol)
        if prot is None or prot.be_done or prot.be_failed or symbol in self._be_requested or abs(self._state.position(symbol)) <= self._tol:
            return
        trigger = prot.be_trigger()
        if trigger is not None and prot.direction * (price - trigger) >= 0:
            self._be_requested.add(symbol)
            log.info("%s: mark %s достигла уровня безубытка %s, перенос стопа запрошен", symbol, price, trigger)

    async def pump(self) -> None:
        """Исполнитель защиты и разбора algo-исполнений. Безопасно вызывать часто; ошибки не подавляются."""
        async with self._pump_lock:
            await self._flush_failures()
            await self._resolve_unexplained()
            for symbol in list(self._state.protections):
                await self._maintain(symbol)

    async def move_stop_to_breakeven(self, symbol: str) -> None:
        """Принудительный перенос стопа на вход (обычно его запрашивает on_mark_price, а выполняет pump)."""
        async with self._pump_lock:
            prot = self._state.protections.get(symbol)
            if prot is None:
                raise DataError(f"{symbol}: нет защиты для переноса стопа")
            if not prot.be_done and not prot.be_failed:
                prot.be_done = True
                self._save(strict=True)  # намерение «стоп на входе» - на диск ДО сетевых действий
            if prot.be_done and abs(self._state.position(symbol)) > self._tol:
                await self._cover_with_breakeven(prot, abs(self._state.position(symbol)))

    # ------------------------------------------------------------------ исполнитель

    async def _maintain(self, symbol: str) -> None:
        prot = self._state.protections[symbol]
        pos = self._state.position(symbol)
        live_entry = next((o for o in self._state.orders.values() if str(o.ticket_id) == prot.parent), None)
        if abs(pos) <= self._tol:
            if prot.fill_qty > 0 or live_entry is None:
                # позиция была и закрыта (стоп/тейк/вручную) либо вход не состоялся: снимаем остатки и намерение
                for algo in self._state.algo_for(symbol):
                    if algo.status == "NEW":
                        await self._cancel_algo_confirmed(algo)
                if live_entry is not None:
                    await self.cancel(live_entry.ticket_id)  # стоп сработал, а вход ещё жив: не даём ему открыть позицию заново
                    return
                if not self._state.algo_for(symbol):
                    del self._state.protections[symbol]
                    self._be_requested.discard(symbol)
                    self._save(strict=True)
            return
        if (pos > 0) != (prot.direction > 0):
            self._safe.trip("Позиция направлена против защиты", {"symbol": symbol, "position": pos, "direction": prot.direction})
            return
        if symbol in self._be_requested:
            self._be_requested.discard(symbol)
            if not prot.be_done and not prot.be_failed:
                prot.be_done = True
                self._save(strict=True)  # намерение «стоп на входе» - на диск ДО сетевых действий (упадём - pump достроит после рестарта)
        size = abs(pos)
        if prot.be_done:
            if not await self._cover_with_breakeven(prot, size):
                return  # позиция закрыта по рынку: дальше нечего защищать
        elif not await self._cover(prot, "sl", prot.stop_price, size):
            return  # отказ: позиция закрыта, тейк ставить не на что
        tp = prot.effective_tp()
        if tp is not None:
            await self._cover(prot, "tp", self._to_tick(symbol, tp, ROUND_DOWN if prot.direction > 0 else ROUND_UP), size)

    def _qty_step(self, symbol: str) -> Decimal:
        step = config.LIVE_QTY_STEP.get(symbol)
        if step is None:
            raise ConfigError(f"Нет шага лота в config.LIVE_QTY_STEP для {symbol}: объём защитного ордера нельзя округлить")
        return Decimal(repr(float(step)))

    def _missing_qty(self, symbol: str, role: str, size: float) -> float:
        """Сколько не хватает живым ордерам роли до объёма позиции, в целых шагах лота (0.0 - покрыто). Сумма долей float
        (0.1 + 0.2) не равна объёму позиции в последних знаках: разница округляется до шага, а не сравнивается с допуском."""
        step = self._qty_step(symbol)
        covered = sum((Decimal(repr(float(a.qty))) for a in self._state.algo_for(symbol, role)), Decimal(0))
        missing = ((Decimal(repr(float(size))) - covered) / step).quantize(Decimal(1), rounding=ROUND_HALF_UP) * step
        return float(missing) if missing > 0 else 0.0

    async def _cover(self, prot: Protection, role: str, price: float, size: float) -> bool:
        """Доводит покрытие роли (sl / tp) до объёма позиции. False - биржа отказала и позиция уже закрыта."""
        missing = self._missing_qty(prot.symbol, role, size)
        return missing <= 0 or await self._place_algo(prot, role, price, missing) is not None

    async def _cover_with_breakeven(self, prot: Protection, size: float) -> bool:
        """Стоп на входе (be_sl) покрывает позицию - ТОЛЬКО ПОТОМ снимаются старые стопы (POST -> DELETE, О18).
        Каждый шаг идемпотентен и выводится из состояния на диске: упали между POST и DELETE - следующий pump снимет старые;
        упали до POST - поставит be_sl. False - позиции больше нет (стоп уже за ценой: закрыта по рынку)."""
        symbol = prot.symbol
        missing = self._missing_qty(symbol, "be_sl", size)
        if missing > 0:
            old = self._state.algo_for(symbol, "sl")
            price = self._to_tick(symbol, prot.entry_price, ROUND_HALF_UP)
            try:
                await self._try_place_algo(prot, "be_sl", price, missing, replaces=old[-1].client_algo_id if old else None)
            except BinanceAPIError as exc:
                return await self._breakeven_refused(prot, exc, size)
            except OrderStatusUnknown:
                # исходный стоп держит позицию; что с новым ордером - разберёт сверка. Работать дальше вслепую нельзя.
                self._safe.trip(f"Исход выставления стопа безубытка {symbol} неизвестен", {"symbol": symbol})
                raise
        for algo in list(self._state.algo_for(symbol, "sl")):
            status = await self._cancel_algo_confirmed(algo)
            if status in ("TRIGGERING", "TRIGGERED", "FINISHED"):
                log.warning("%s: старый стоп уже сработал (%s) во время переноса в безубыток", symbol, status)
        return True

    async def _breakeven_refused(self, prot: Protection, exc: BinanceAPIError, size: float) -> bool:
        symbol = prot.symbol
        if exc.code == ERR_WOULD_TRIGGER:
            # цена вернулась к входу: стоп на входе сработал бы немедленно. Безубыток = выход здесь же, по рынку.
            await self._protection_failed(symbol, f"стоп безубытка уже за ценой ({exc.msg}): выход по рынку", trip=False)
            return False
        if self._missing_qty(symbol, "sl", size) > 0:  # другого стопа нет: позиция без защиты
            await self._protection_failed(symbol, f"биржа отклонила стоп безубытка, а исходного стопа нет: {exc.msg} (код {exc.code})", trip=True)
            return False
        log.critical("%s: биржа отказала в стопе безубытка (%s, код %s): остаётся исходный стоп, перенос отменён", symbol, exc.msg, exc.code)
        for algo in list(self._state.algo_for(symbol, "be_sl")):  # добавка, поставленная до отказа, не нужна
            await self._cancel_algo_confirmed(algo)
        prot.be_done, prot.be_failed = False, True
        self._save(strict=True)
        return True

    async def _place_algo(
        self, prot: Protection, role: str, price: float, qty: float, *, replaces: str | None = None,
    ) -> LocalAlgoOrder | None:
        """Выставляет algo-ордер. None - биржа отказала, защита не поставлена: позиция уже закрыта (_protection_failed)."""
        try:
            return await self._try_place_algo(prot, role, price, qty, replaces=replaces)
        except BinanceAPIError as exc:
            await self._protection_failed(
                prot.symbol, f"биржа отклонила algo-ордер {role} {qty} @ {price}: {exc.msg} (код {exc.code})", trip=exc.code != ERR_WOULD_TRIGGER,
            )
            return None
        except OrderStatusUnknown:
            await self._protection_failed(prot.symbol, f"исход выставления algo-ордера {role} неизвестен", trip=True)
            raise

    async def _try_place_algo(
        self, prot: Protection, role: str, price: float, qty: float, *, replaces: str | None = None,
    ) -> LocalAlgoOrder:
        """POST algoOrder без политики отказа: BinanceAPIError (после очистки локальной записи) и OrderStatusUnknown - наружу."""
        algo = LocalAlgoOrder(
            client_algo_id=new_client_algo_id(), symbol=prot.symbol, side=prot.closing_side, role=role,
            order_type="TAKE_PROFIT_MARKET" if role == "tp" else "STOP_MARKET", trigger_price=price,
            created_ms=self._rest.now_ms(), close_position=False, qty=qty, working_type=config.ALGO_WORKING_TYPE,
            parent=prot.parent, replaces=replaces,
        )
        self._state.algo_orders[algo.client_algo_id] = algo
        self._save(strict=True)  # write-ahead: не записали - не отправляем
        try:
            snap = await self._post_algo(algo)
        except BinanceAPIError:
            self._state.algo_orders.pop(algo.client_algo_id, None)
            self._save()
            raise
        self._apply_algo_snapshot(algo, snap)
        return algo

    async def _post_algo(self, algo: LocalAlgoOrder) -> dict:
        """POST algoOrder с тем же clientAlgoId при каждом повторе. Сетевой сбой/5xx = «исход неизвестен»: сначала
        смотрим на бирже по clientAlgoId; повтор - только когда подтверждено, что ордера нет и исходный запрос протух."""
        params = self._algo_params(algo)
        attempt = 0
        while True:
            attempt += 1
            trace = RequestTrace()
            try:
                data = await self._rest.request("POST", config.ALGO_ORDER_PATH, params, trace=trace)
            except (NetworkError, HttpServerError) as exc:
                log.warning("Algo-ордер %s: исход отправки неизвестен (%s: %s)", algo.client_algo_id, type(exc).__name__, exc)
                outcome = await self._resolve_algo_unknown(algo, trace.timestamp_ms or self._rest.now_ms())
                if isinstance(outcome, dict):
                    return outcome
                if outcome is not _ABSENT or attempt >= config.ALGO_PLACE_ATTEMPTS:
                    raise OrderStatusUnknown(f"Algo-ордер {algo.client_algo_id} ({algo.symbol}): исход отправки неизвестен") from exc
                continue
            if not isinstance(data, dict) or data.get("clientAlgoId") != algo.client_algo_id:
                raise DataError(f"Неожиданный ответ {config.ALGO_ORDER_PATH}: {str(data)[:200]}")
            return data

    def _algo_params(self, a: LocalAlgoOrder) -> dict[str, Any]:
        params: dict[str, Any] = {
            "algoType": "CONDITIONAL", "symbol": a.symbol, "side": a.side.value.upper(), "type": a.order_type,
            "triggerPrice": fmt_decimal(a.trigger_price), "workingType": a.working_type, "clientAlgoId": a.client_algo_id,
        }
        if a.close_position:
            params["closePosition"] = True
        else:
            params["quantity"], params["reduceOnly"] = fmt_decimal(a.qty), True
        return params

    async def _resolve_algo_unknown(self, algo: LocalAlgoOrder, sent_ts: int) -> Any:
        found = await self._lookup_algo_retrying(algo)
        if found is not _ABSENT:
            return found  # dict или None (не удалось выяснить)
        delay_ms = sent_ts + self._rest.recv_window_ms + _EXPIRY_MARGIN_MS - self._rest.now_ms()
        if delay_ms > 0:
            await self._sleep(delay_ms / 1000)  # после этого момента биржа исходный запрос уже отвергнет (-1021)
        return await self._lookup_algo_retrying(algo)

    async def _lookup_algo_retrying(self, algo: LocalAlgoOrder) -> Any:
        for attempt in range(1, self._attempts + 1):
            try:
                return await self._query_algo(algo)
            except (NetworkError, HttpServerError, BinanceAPIError) as exc:
                log.warning("Запрос algo-ордера %s не удался: %s", algo.client_algo_id, exc)
                await self._sleep(self._backoff * attempt)
        return None

    async def _query_algo(self, algo: LocalAlgoOrder) -> Any:
        """dict снимка или _ABSENT (биржа не знает такого clientAlgoId); прочие отказы биржи - исключение (не гадаем)."""
        try:
            data = await self._rest.request("GET", config.ALGO_ORDER_PATH, {"clientAlgoId": algo.client_algo_id})
        except BinanceAPIError as exc:
            if exc.code in _ALGO_ABSENT_CODES:
                return _ABSENT
            raise
        if not isinstance(data, dict) or data.get("clientAlgoId") != algo.client_algo_id:
            raise DataError(f"Неожиданный ответ по algo-ордеру {algo.client_algo_id}: {str(data)[:200]}")
        return data

    async def _cancel_algo_confirmed(self, algo: LocalAlgoOrder) -> str:
        """DELETE algoOrder и подтверждённый итог. -> CANCELED | EXPIRED | FINISHED | TRIGGERING | TRIGGERED.
        Любой отказ или сбой DELETE разбирается запросом статуса: по коду отказа не гадаем (в документации его нет)."""
        try:
            await self._rest.request("DELETE", config.ALGO_ORDER_PATH, {"clientAlgoId": algo.client_algo_id})
            self._set_algo_status(algo, "CANCELED")
            return "CANCELED"
        except (BinanceAPIError, NetworkError, HttpServerError) as exc:
            log.warning("Отмена algo-ордера %s не подтверждена (%s): проверяю статус", algo.client_algo_id, exc)
        snap = await self._lookup_algo_retrying(algo)
        if snap is _ABSENT:
            self._set_algo_status(algo, "CANCELED")
            return "CANCELED"
        if snap is None:
            self._safe.trip("Исход отмены algo-ордера неизвестен", {"client_algo_id": algo.client_algo_id, "symbol": algo.symbol})
            raise OrderStatusUnknown(f"Algo-ордер {algo.client_algo_id}: исход отмены неизвестен")
        self._apply_algo_snapshot(algo, snap)
        status = str(snap.get("algoStatus"))
        if status == "NEW":
            raise DataError(f"Algo-ордер {algo.client_algo_id} остался NEW после отмены: {str(snap)[:200]}")
        return status

    async def _protection_failed(self, symbol: str, reason: str, *, trip: bool) -> None:
        """Защитить позицию не удалось: Safe Mode (если не обычный рыночный случай) и немедленное reduce-only закрытие."""
        log.critical("%s: защита не поставлена: %s", symbol, reason)
        if trip:
            self._safe.trip(f"Защита позиции {symbol} не поставлена: {reason}", {"symbol": symbol})
        if abs(self._state.position(symbol)) > self._tol:
            await self.close_position(symbol, self._rest.now_ms())  # reduce-only MARKET: разрешён и в Safe Mode

    async def _flush_failures(self) -> None:
        while self._failed_protection:
            symbol, reason = self._failed_protection.popitem()
            await self._protection_failed(symbol, reason, trip=True)
        for symbol in list(self._cancel_siblings):
            self._cancel_siblings.discard(symbol)
            for algo in self._state.algo_for(symbol):
                if algo.status == "NEW":  # сработал один защитный ордер - второй больше не нужен
                    await self._cancel_algo_confirmed(algo)

    # ------------------------------------------------------------ события ALGO_UPDATE

    def on_algo_update(self, u: AlgoUpdate) -> None:
        """ALGO_UPDATE (синхронный обработчик, как on_order_update). Статусы монотонны: опоздавшее эхо не откатывает."""
        algo = self._state.algo_orders.get(u.client_algo_id)
        if algo is None:
            if u.client_algo_id in self._retired_algo:
                log.info("Опоздавшее событие завершённого algo-ордера %s (%s): пропущено", u.client_algo_id, u.status)
            elif not (self._state.algo_orders or self._state.protections):
                # барьер algo-ордерами не пользуется (ML-контур): чужие условные ордера на счёте - не повод для Safe Mode;
                # при работе с защитой их ловит сверка (UNKNOWN_EXCHANGE_ALGO)
                log.info("ALGO_UPDATE %s (%s) при отсутствии своих algo-ордеров: пропущено", u.client_algo_id, u.status)
            elif u.status in ("NEW", "TRIGGERING", "TRIGGERED"):
                self._safe.trip(
                    "Algo-ордер, которого нет в локальном состоянии (чужой стоп/тейк или потерянное состояние?)",
                    {"client_algo_id": u.client_algo_id, "symbol": u.symbol, "type": u.order_type, "status": u.status},
                )
            else:
                log.info("Событие чужого/завершённого algo-ордера %s (%s): пропущено", u.client_algo_id, u.status)
            return
        if (u.symbol, u.side, u.order_type) != (algo.symbol, algo.side, algo.order_type):
            self._safe.trip("ALGO_UPDATE не совпадает с локальным algo-ордером",
                            {"event": str(u)[:300], "local": algo.to_dict()})
            return
        algo.algo_id = u.algo_id
        if u.actual_order_id is not None:
            self._learn_actual_order(algo, u.actual_order_id)
        self._set_algo_status(algo, u.status)
        if u.status == "REJECTED":
            self._failed_protection[algo.symbol] = f"algo-ордер {algo.role} отклонён биржей: {u.reason or 'без причины'}"

    def _learn_actual_order(self, algo: LocalAlgoOrder, order_id: int) -> None:
        algo.actual_order_id = order_id
        self._state.remember_triggered(order_id, algo.client_algo_id)
        for key, (update, _) in list(self._unexplained.items()):
            if update.order_id == order_id:  # исполнение пришло раньше ALGO_UPDATE - теперь оно объяснено
                del self._unexplained[key]
                self._algo_fill(update)

    def _set_algo_status(self, algo: LocalAlgoOrder, status: str) -> None:
        new = _ALGO_RANK.get(status)
        if new is None:
            raise DataError(f"Неизвестный статус algo-ордера {status!r}")
        if new < _ALGO_RANK.get(algo.status, 0):
            return  # опоздавшее эхо
        algo.status = status
        if status in ("TRIGGERING", "TRIGGERED", "FINISHED"):
            self._cancel_siblings.add(algo.symbol)
        if status in ALGO_TERMINAL:
            self._state.algo_orders.pop(algo.client_algo_id, None)
            self._retired_algo.append(algo.client_algo_id)
        self._save()

    def _apply_algo_snapshot(self, algo: LocalAlgoOrder, snap: dict) -> None:
        """Состояние algo-ордера по REST (ответ POST / GET): algoId, статус, id порождённого ордера."""
        try:
            algo.algo_id = int(snap["algoId"])
            status = str(snap.get("algoStatus", "NEW"))
            actual = snap.get("actualOrderId")
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданный снимок algo-ордера {algo.client_algo_id}: {str(snap)[:200]}") from exc
        if actual not in (None, "", 0, "0"):
            try:
                self._learn_actual_order(algo, int(actual))
            except (TypeError, ValueError) as exc:
                raise DataError(f"Неожиданный actualOrderId {actual!r} у {algo.client_algo_id}") from exc
        self._set_algo_status(algo, status)

    async def resync_algo_all(self) -> None:
        """После (пере)подключения потока: статус каждого живого algo-ордера по REST (ALGO_UPDATE в обрыве мог пропасть)."""
        for algo in list(self._state.algo_orders.values()):
            snap = await self._lookup_algo_retrying(algo)
            if snap is _ABSENT:
                if algo.status != "PENDING":
                    self._safe.trip("Algo-ордер пропал с биржи", {"client_algo_id": algo.client_algo_id, "symbol": algo.symbol})
                continue
            if snap is None:
                self._safe.trip("Статус algo-ордера не удалось выяснить", {"client_algo_id": algo.client_algo_id})
                continue
            self._apply_algo_snapshot(algo, snap)
            if algo.actual_order_id is not None and algo.status in ("TRIGGERING", "TRIGGERED", "FINISHED"):
                await self._book_algo_rest_trades(algo)

    # --------------------------------------------------- исполнения порождённого ордера

    def _algo_owns(self, u: OrderUpdate) -> bool:
        return str(u.order_id) in self._state.triggered_order_ids or u.client_order_id in self._state.algo_orders

    def _algo_fill(self, u: OrderUpdate) -> None:
        caid = self._state.triggered_order_ids.get(str(u.order_id))
        if caid is None and u.client_order_id in self._state.algo_orders:
            caid = u.client_order_id  # если биржа даёт порождённому ордеру clientAlgoId (в документации не описано, ❓)
            self._state.remember_triggered(u.order_id, caid)
        algo = self._state.algo_orders.get(caid) if caid else None
        if u.has_trade:
            self._book_algo_trade(
                u.symbol, u.side, u.trade_id, u.last_qty, u.last_price, u.trade_time, u.commission, u.commission_asset,
                u.is_maker, algo,
            )

    def _book_algo_trade(
        self, symbol: str, side: Side, trade_id: int, qty: float, price: float, ts: int, commission: float | None,
        asset: str | None, maker: bool, algo: LocalAlgoOrder | None,
    ) -> None:
        """Единственная точка учёта исполнений algo-ордеров; дедупликация - общая с _book_trade (symbol, trade_id)."""
        key = (symbol, trade_id)
        if key in self._seen_set:
            return
        self._seen_set.add(key)
        self._seen_order.append(key)
        if len(self._seen_order) > _SEEN_TRADES:
            self._seen_set.discard(self._seen_order.popleft())
        before = self._state.position(symbol)
        after = before + side.sign * qty
        self._state.positions[symbol] = 0.0 if abs(after) <= _QTY_EPS else after
        if algo is not None:
            algo.filled_qty += qty
            self._exit_roles[symbol] = algo.role
        liquidity = Liquidity.MAKER if maker else Liquidity.TAKER
        ticket_id = self._state.next_ticket_id  # исполнение без нашего OrderIntent: синтетический тикет, чтобы Fill был полноценным
        self._state.next_ticket_id += 1
        self._emit(Fill(ticket_id=ticket_id, symbol=symbol, side=side, qty=qty, price=price, liquidity=liquidity, ts=ts,
                        fee=self._fee(qty, price, liquidity, commission, asset)))
        if abs(after) > abs(before) + self._tol:
            self._safe.trip("Algo-исполнение увеличило позицию (стоп/тейк обязан только закрывать)",
                            {"symbol": symbol, "before": before, "after": after, "trade_id": trade_id})
        self._save()

    async def _book_algo_rest_trades(self, algo: LocalAlgoOrder) -> None:
        trades = await self._rest.request("GET", config.USER_TRADES_PATH, {"symbol": algo.symbol, "orderId": algo.actual_order_id})
        if not isinstance(trades, list):
            raise DataError(f"Неожиданный ответ userTrades: {str(trades)[:200]}")
        for t in sorted(trades, key=lambda t: (t.get("time", 0), t.get("id", 0))):
            try:
                self._book_algo_trade(
                    algo.symbol, Side(str(t["side"]).lower()), int(t["id"]), float(t["qty"]), float(t["price"]), int(t["time"]),
                    float(t["commission"]), str(t["commissionAsset"]), bool(t["maker"]), algo,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise DataError(f"Неожиданная сделка в userTrades: {str(t)[:200]}") from exc

    async def _resolve_unexplained(self) -> None:
        """Исполнения, пришедшие раньше ALGO_UPDATE: ищем объяснение по REST (actualOrderId живых algo-ордеров монеты);
        нет объяснения до конца окна ALGO_UNEXPLAINED_GRACE_MS - значит исполнение чужое: Safe Mode."""
        for key, (update, deadline) in list(self._unexplained.items()):
            for algo in self._state.algo_for(update.symbol, active_only=False):
                if key not in self._unexplained:
                    break
                snap = await self._lookup_algo_retrying(algo)
                if isinstance(snap, dict):
                    self._apply_algo_snapshot(algo, snap)
            if key in self._unexplained and self._rest.now_ms() >= deadline:
                del self._unexplained[key]
                self._safe.trip(
                    "Исполнение, которое не объясняет ни один свой ордер или algo-ордер (стоп, ликвидация, ручное действие?)",
                    {"client_order_id": update.client_order_id, "symbol": update.symbol, "qty": update.last_qty,
                     "price": update.last_price, "order_id": update.order_id},
                )

    def _to_tick(self, symbol: str, price: float, rounding: str) -> float:
        tick = config.LIVE_PRICE_TICK.get(symbol)
        if tick is None:
            raise ConfigError(f"Нет шага цены в config.LIVE_PRICE_TICK для {symbol}: стоп нельзя округлить под PRICE_FILTER")
        step = Decimal(repr(float(tick)))
        return float((Decimal(repr(float(price))) / step).quantize(Decimal(1), rounding=rounding) * step)

    # --------------------------------------------------------------------- сверка

    async def resync_all(self) -> None:
        """После (пере)подключения потока: проверяет по REST каждый живой ордер, добирает пропущенное."""
        for cid in list(self._live):
            if cid not in self._inflight:
                await self._resync_one(cid)
        await self.resync_algo_all()

    async def resync_pending(self) -> None:
        """Ордера, у которых поток показал больше исполнений, чем мы учли (или отмена не подтвердилась)."""
        for cid in list(self._needs_resync):
            self._needs_resync.discard(cid)
            if cid in self._live and cid not in self._inflight:
                await self._resync_one(cid)

    async def _resync_one(self, cid: str) -> None:
        order = self._state.orders.get(cid)
        if order is None:
            return
        try:
            snapshot = await self._lookup(order)
        except (NetworkError, HttpServerError) as exc:
            log.warning("Сверка %s не удалась (%s), повторю позже", cid, exc)
            self._needs_resync.add(cid)
            return
        if snapshot is _ABSENT:
            if order.status != "PENDING":  # был на бирже и пропал: так не бывает
                self._safe.trip("Ордер пропал с биржи", {"client_order_id": cid, "symbol": order.symbol})
            return
        await self._apply_snapshot(cid, snapshot)

    # ------------------------------------------------------------------- отправка

    def _order_params(self, order: LocalOrder, reduce_only: bool) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": order.symbol,
            "side": order.side.value.upper(),
            "type": order.order_type.value.upper(),
            "quantity": fmt_decimal(order.qty),
            "newClientOrderId": order.client_order_id,
            "newOrderRespType": "RESULT",  # сразу статус и исполненный объём
            "reduceOnly": True if reduce_only else None,
        }
        if order.order_type is OrderType.LIMIT:
            params["price"] = fmt_decimal(order.limit_price)
            # Срок жизни держим сами (expire_due): GTD требует >= 10 минут. GTX = post-only (только мейкер).
            params["timeInForce"] = "GTX" if self._post_only else "GTC"
        return params

    async def _place(self, order: LocalOrder, reduce_only: bool) -> dict:
        cid = order.client_order_id
        params = self._order_params(order, reduce_only)
        attempt = 0
        while True:
            attempt += 1
            if attempt > 1 and not reduce_only:
                self._safe.require_trading()  # за время ожидания мог включиться Safe Mode
            trace = RequestTrace()
            try:
                snapshot = await self._rest.request("POST", config.ORDER_PATH, params, trace=trace)
            except BinanceAPIError as exc:
                if exc.code in _DUPLICATE_CODES:
                    found = await self._lookup_or_none(order)
                    if isinstance(found, dict):
                        self._uncertain.discard(cid)
                        return found
                raise
            except (NetworkError, HttpServerError) as exc:
                self._uncertain.add(cid)
                log.warning("Ордер %s: исход отправки неизвестен (%s: %s)", cid, type(exc).__name__, exc)
                outcome = await self._resolve_unknown(order, trace.timestamp_ms or self._rest.now_ms())
                if isinstance(outcome, dict):
                    self._uncertain.discard(cid)
                    return outcome
                if outcome is not _ABSENT or attempt >= self._attempts:
                    self._unknown(order, "исход отправки не удалось выяснить")
                    raise OrderStatusUnknown(f"Ордер {cid} ({order.symbol}): исход отправки неизвестен") from exc
                self._uncertain.discard(cid)  # подтверждено: исходный запрос исполниться уже не может
                log.warning("Ордер %s: на бирже нет, повторяю с тем же clientOrderId (попытка %d)", cid, attempt + 1)
                continue
            self._uncertain.discard(cid)
            return snapshot

    async def _resolve_unknown(self, order: LocalOrder, sent_ts: int) -> Any:
        """dict - ордер на бирже; _ABSENT - его точно нет и исходный запрос протух; None - выяснить не удалось."""
        found = await self._lookup_retrying(order)
        if found is not _ABSENT:
            return found  # dict или None
        delay_ms = sent_ts + self._rest.recv_window_ms + _EXPIRY_MARGIN_MS - self._rest.now_ms()
        if delay_ms > 0:
            await self._sleep(delay_ms / 1000)  # после этого момента биржа запрос уже отвергнет (-1021)
        return await self._lookup_retrying(order)

    async def _lookup_retrying(self, order: LocalOrder) -> Any:
        for attempt in range(1, self._attempts + 1):
            found = await self._lookup_or_none(order)
            if found is not None:
                return found
            await self._sleep(self._backoff * attempt)
        return None

    async def _lookup_or_none(self, order: LocalOrder) -> Any:
        """dict / _ABSENT, а при сетевой ошибке и отказе биржи - None (не знаем)."""
        try:
            return await self._lookup(order)
        except (NetworkError, HttpServerError, BinanceAPIError) as exc:
            log.warning("Запрос ордера %s не удался: %s", order.client_order_id, exc)
            return None

    async def _lookup(self, order: LocalOrder) -> Any:
        try:
            data = await self._rest.request(
                "GET", config.ORDER_PATH, {"symbol": order.symbol, "origClientOrderId": order.client_order_id}
            )
        except BinanceAPIError as exc:
            if exc.code == ERR_ORDER_NOT_FOUND:
                return _ABSENT
            raise
        if not isinstance(data, dict) or data.get("clientOrderId") != order.client_order_id:
            raise DataError(f"Неожиданный ответ по ордеру {order.client_order_id}: {str(data)[:200]}")
        return data

    # ------------------------------------------------------------- применение итогов

    async def _apply_snapshot(self, cid: str, snap: dict) -> None:
        """Состояние ордера по REST (ответ POST / DELETE / GET): добирает сделки, закрывает, обновляет статус."""
        order = self._state.orders.get(cid)
        if order is None:
            return  # поток уже всё учёл и закрыл
        try:
            status, executed = str(snap["status"]), float(snap["executedQty"])
            order_id = int(snap["orderId"])
            ts = int(snap.get("updateTime") or self._rest.now_ms())
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданный снимок ордера {cid}: {str(snap)[:200]}") from exc
        if executed > order.filled_qty + _QTY_EPS:
            await self._book_rest_trades(order, order_id)
            order = self._state.orders.get(cid)
            if order is None:
                return
        if status in TERMINAL_STATUSES:
            if order.filled_qty + _QTY_EPS < executed:
                self._needs_resync.add(cid)  # сделок ещё не хватает: закрывать рано
                return
            self._close(cid, _close_reason(order, status), ts)
        elif status != order.status:
            order.status = status
            self._save()

    async def _book_rest_trades(self, order: LocalOrder, order_id: int) -> None:
        try:
            trades = await self._rest.request(
                "GET", config.USER_TRADES_PATH, {"symbol": order.symbol, "orderId": order_id}
            )
        except (NetworkError, HttpServerError) as exc:
            log.warning("userTrades %s: %s; доберу позже", order.client_order_id, exc)
            self._needs_resync.add(order.client_order_id)
            return
        if not isinstance(trades, list):
            raise DataError(f"Неожиданный ответ userTrades: {str(trades)[:200]}")
        for t in sorted(trades, key=lambda t: (t.get("time", 0), t.get("id", 0))):
            try:
                self._book_trade(
                    order, trade_id=int(t["id"]), qty=float(t["qty"]), price=float(t["price"]), ts=int(t["time"]),
                    commission=float(t["commission"]), asset=str(t["commissionAsset"]), maker=bool(t["maker"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise DataError(f"Неожиданная сделка в userTrades: {str(t)[:200]}") from exc
            if order.client_order_id not in self._state.orders:
                return

    def _book_trade(
        self, order: LocalOrder, *, trade_id: int, qty: float, price: float, ts: int,
        commission: float | None, asset: str | None, maker: bool,
    ) -> None:
        """Единственная точка, меняющая позицию. Повторная сделка (symbol, trade_id) игнорируется."""
        key = (order.symbol, trade_id)
        if key in self._seen_set:
            return
        if qty > order.remaining + max(_QTY_EPS, self._tol):
            self._safe.trip(
                "Переисполнение ордера: сделок больше, чем объём",
                {"client_order_id": order.client_order_id, "qty": order.qty, "filled": order.filled_qty, "trade_qty": qty},
            )
            return
        self._seen_set.add(key)
        self._seen_order.append(key)
        if len(self._seen_order) > _SEEN_TRADES:
            self._seen_set.discard(self._seen_order.popleft())
        liquidity = Liquidity.MAKER if maker else Liquidity.TAKER
        order.filled_qty += qty
        prot = self._state.protections.get(order.symbol)
        if prot is not None and prot.parent == str(order.ticket_id):  # фактическая цена входа - основа безубытка и тейка по R
            prot.fill_qty += qty
            prot.fill_cost += qty * price
            prot.entry_price = prot.fill_cost / prot.fill_qty
        if order.remaining <= max(_QTY_EPS, self._tol):
            order.filled_qty = order.qty
        position = self._state.position(order.symbol) + order.side.sign * qty
        self._state.positions[order.symbol] = 0.0 if abs(position) <= _QTY_EPS else position
        self._emit(Fill(
            ticket_id=order.ticket_id, symbol=order.symbol, side=order.side, qty=qty, price=price,
            liquidity=liquidity, ts=ts, fee=self._fee(qty, price, liquidity, commission, asset),
        ))
        if order.remaining <= 0:
            self._state.orders.pop(order.client_order_id, None)
            self._live.pop(order.client_order_id, None)
            self._needs_resync.discard(order.client_order_id)
        else:
            order.status = "PARTIALLY_FILLED"
        self._save()

    def _fee(self, qty: float, price: float, liquidity: Liquidity, commission: float | None, asset: str | None) -> float:
        if commission is not None and asset == self._quote:
            return commission  # факт от биржи; < 0 - ребейт
        if commission is not None and asset not in self._warned_assets:
            self._warned_assets.add(asset)
            log.warning("Комиссия в %s, а не в %s: пересчитываю по ставкам ExecutionConfig", asset, self._quote)
        return self._cfg.fee(liquidity, qty, price)

    def _close(self, cid: str, reason: CloseReason, ts: int) -> None:
        order = self._state.orders.pop(cid, None)
        ticket = self._live.pop(cid, None)
        self._needs_resync.discard(cid)
        self._uncertain.discard(cid)
        if order is None or ticket is None:
            return
        if order.remaining > max(_QTY_EPS, self._tol):
            self._emit(OrderClosed(order.ticket_id, max(1, ts), order.remaining, reason))
        self._save()

    def _forget(self, cid: str) -> None:
        """Ордер на биржу не уходил: убираем из локального состояния без событий."""
        self._state.orders.pop(cid, None)
        self._live.pop(cid, None)
        self._save()

    def _foreign(self, u: OrderUpdate) -> None:
        if u.has_trade and (u.symbol, u.trade_id) in self._seen_set:
            return  # запоздалый дубль уже учтённой сделки закрытого ордера
        if u.has_trade and (self._state.algo_for(u.symbol) or u.symbol in self._state.protections):
            # есть свои algo-ордера: ORDER_TRADE_UPDATE мог прийти раньше ALGO_UPDATE с ai. Ждём объяснения (pump ищет его
            # по REST), по истечении окна - Safe Mode.
            self._unexplained.setdefault((u.symbol, u.trade_id), (u, self._rest.now_ms() + self._grace_ms))
            log.warning("Исполнение %s %s без ордера: жду ALGO_UPDATE (окно %d мс)", u.symbol, u.trade_id, self._grace_ms)
            return
        if u.has_trade:
            self._safe.trip(
                "Исполнение ордера, которого нет в локальном состоянии (стоп, ликвидация, ручное действие?)",
                {"client_order_id": u.client_order_id, "symbol": u.symbol, "qty": u.last_qty, "price": u.last_price},
            )
        else:
            log.info("Событие чужого/закрытого ордера %s (%s): пропущено", u.client_order_id, u.status)

    def _unknown(self, order: LocalOrder, reason: str) -> None:
        self._uncertain.add(order.client_order_id)  # остаётся в состоянии PENDING: разберёт reconcile
        self._safe.trip(
            f"Исход ордера неизвестен: {reason}",
            {"client_order_id": order.client_order_id, "symbol": order.symbol, "side": order.side.value, "qty": order.qty},
        )

    def _cid_of(self, ticket_id: int) -> str | None:
        return next((cid for cid, t in self._live.items() if t.ticket_id == ticket_id), None)

    def _save(self, strict: bool = False) -> None:
        if self._store is None:
            return
        try:
            self._store.save(self._state)
        except Exception:
            if strict:
                raise
            log.critical("Не удалось сохранить состояние на диск", exc_info=True)
            self._safe.trip("Не удалось сохранить локальное состояние на диск")


def _close_reason(order: LocalOrder, status: str) -> CloseReason:
    if status == "REJECTED":
        return CloseReason.REJECTED
    if status in ("EXPIRED", "EXPIRED_IN_MATCH") and order.order_type is OrderType.MARKET:
        return CloseReason.NO_LIQUIDITY
    return CloseReason.EXPIRED  # LIMIT истёк или отменён по сроку жизни
