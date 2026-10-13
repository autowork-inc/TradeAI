"""Контракт симулятора исполнения: ExecutionBarrier. Только типы и правила, без движка сопоставления.

Барьер - единственная дверь между стратегией и смоделированным рынком. Всё, что делает
бэктест оптимистичным (мгновенное исполнение, забытые комиссии, пропущенный фандинг),
закрывается здесь, а не в соглашениях между модулями. Реализация (движок) наследует
ExecutionBarrier и заполняет два метода: _accept и _match. Всё остальное - submit, advance,
комиссии, фандинг, учёт позиции - делает базовый класс и проверяет на каждом вызове.

Правила (каждое проверяется кодом ниже и тестами):

 1. Задержка. Стратегия задаёт только asof_ts решения; момент исполнения считает барьер:
    exec_ts = asof_ts + order_latency_ms, order_latency_ms >= 1. Поля «время исполнения» у
    OrderIntent нет. Ордер нельзя подать задним числом (asof_ts < времени барьера).
 2. Видимость. advance(now_ts) отдаёт события с ts СТРОГО меньше now_ts: событие в ту же
    миллисекунду, что и решение, стратегии ещё не известно (то же правило, что у
    TimelineAligner: источник попадает в строку, только если известен строго раньше asof_ts).
    Рабочий цикл движка: advance(asof_ts) -> решение -> submit(...).
 3. Комиссии. Fill.fee всегда считает барьер: notional * ставка роли исполнения
    (MAKER/TAKER). Движок отдаёт RawFill без комиссии, поэтому «забыть» её нельзя.
    Знак: fee > 0 - списано с аккаунта, fee < 0 - ребейт мейкера.
 4. Фандинг. Списание привязано к funding_time (серверное время записи фандинга) и
    считается по позиции СТРОГО до funding_time: исполнение в ту же миллисекунду в позицию
    не входит (вход ровно в T фандинг не платит, выход ровно в T - платит).
    funding_fee = signed_qty * mark_price * funding_rate; > 0 - списано (лонг при
    положительной ставке), < 0 - начислено. mark_price - значение из записи фандинга.
    Нулевая позиция фандинг не создаёт. Не покрытое данными списание при открытой позиции -
    FundingCoverageError, а не молчаливый пропуск.
    ВАЖНО: ставка в учёте и ставка в признаках - разные вещи. Учёт идёт по серверному
    funding_time (фактическое событие). Признак «последняя известная ставка» строго по
    avail_ts (pipeline/sync): стратегия не видит ставку раньше, чем её опубликовали.
 5. Жизненный цикл. Ордер закрывается ровно один раз: полным исполнением или OrderClosed.
    MARKET исполняется целиком в exec_ts как TAKER (остаток без ликвидности -> OrderClosed);
    LIMIT живёт [exec_ts, exec_ts + ttl_ms), исполняется не хуже лимитной цены, по
    истечении закрывается ровно в exec_ts + ttl_ms. «Висящих» ордеров быть не может.
 6. Время не идёт назад: advance(now_ts) требует now_ts >= прежнего; события движка идут по
    неубыванию ts и лежат в [прежнее время, now_ts).
 7. Конец данных. finalize(end_ts, marks) вызывается один раз после advance(end_ts): все ещё
    открытые ордера закрываются как EXPIRED в end_ts (раньше своего срока - данных больше нет,
    ждать нечего), остатки позиций выкупаются TAKER-ордерами по ценам marks. После finalize
    барьер закрыт: submit/advance/finalize запрещены. Так висящий LIMIT не заставляет движок
    «досчитывать» время за пределы данных (где фандинг-фид честно бросил бы FundingCoverageError).

Правила для реализации, которые код проверить не может:
  * ордер исполняется против первого сообщения стакана с recv_ts >= exec_ts (момент, когда
    ордер дошёл до биржи); сообщение, пришедшее раньше, - уже прошлое состояние книги;
    слишком поздний стакан (дыра в данных) - не повод исполнять по его ценам;
  * никаких данных после ts исполнения, кроме этого сообщения (свеча, закрывающаяся позже,
    не используется);
  * проскальзывание и глубина - из стакана, а не константой.

Метки обучения (pipeline/labels) считаются по тем же шагам сетки: вход и выход сдвигаются на
одну и ту же order_latency_ms, поэтому горизонт метки совпадает с горизонтом удержания.
"""

import math
from abc import ABC, abstractmethod
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from pipeline.types import TimingConfig
from utils.errors import ConfigError, DataError

MAX_FEE_RATE = 0.01  # |ставка| >= 1% - почти наверняка проценты вместо долей (0.05 вместо 0.0005)
_QTY_EPS = 1e-12


class ContractViolation(DataError):
    """Нарушено правило контракта (реализацией барьера или вызывающим кодом)."""


class FundingCoverageError(ContractViolation):
    """Открытая позиция пересекла списание, которого нет в данных."""


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class Liquidity(str, Enum):
    MAKER = "maker"
    TAKER = "taker"


class CloseReason(str, Enum):
    EXPIRED = "expired"  # LIMIT дожил до exec_ts + ttl_ms
    NO_LIQUIDITY = "no_liquidity"  # MARKET: в стакане не хватило глубины для остатка
    REJECTED = "rejected"  # биржа не приняла ордер (до любого исполнения)


# --------------------------------------------------------------------- проверки


def _ms(value: object, name: str, *, minimum: int = 1, error=ConfigError) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise error(f"{name} должен быть целым >= {minimum}, получено {value!r}")
    return value


def _num(value: object, name: str, *, positive: bool = False, error=ConfigError) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise error(f"{name} должен быть конечным числом, получено {value!r}")
    if positive and value <= 0:
        raise error(f"{name} должен быть > 0, получено {value!r}")
    return float(value)


# ---------------------------------------------------------------------- конфиг


@dataclass(frozen=True)
class ExecutionConfig:
    """Допущения исполнения. Ставки комиссий обязательны: это деньги, молчаливых значений нет.

    Ставки - доля от номинала сделки (0.0005 = 0.05%). maker_fee_rate может быть < 0 (ребейт).
    """

    order_latency_ms: int
    maker_fee_rate: float
    taker_fee_rate: float

    def __post_init__(self) -> None:
        _ms(self.order_latency_ms, "order_latency_ms")  # >= 1: исполнение строго позже решения
        maker = _num(self.maker_fee_rate, "maker_fee_rate")
        taker = _num(self.taker_fee_rate, "taker_fee_rate")
        for name, rate in (("maker_fee_rate", maker), ("taker_fee_rate", taker)):
            if abs(rate) >= MAX_FEE_RATE:
                raise ConfigError(f"{name}={rate} выглядит как проценты; ставка - доля номинала (0.0005 = 0.05%)")
        if taker < 0:
            raise ConfigError(f"taker_fee_rate не может быть отрицательной: {taker}")
        if maker > taker:
            raise ConfigError(f"maker_fee_rate={maker} больше taker_fee_rate={taker}: ставки перепутаны?")

    @classmethod
    def from_timing(cls, timing: TimingConfig, *, maker_fee_rate: float, taker_fee_rate: float) -> "ExecutionConfig":
        """Задержка берётся из TimingConfig: единственный источник order_latency_ms."""
        return cls(timing.order_latency_ms, maker_fee_rate, taker_fee_rate)

    def exec_ts(self, asof_ts: int) -> int:
        return asof_ts + self.order_latency_ms

    def fee_rate(self, liquidity: Liquidity) -> float:
        return self.maker_fee_rate if Liquidity(liquidity) is Liquidity.MAKER else self.taker_fee_rate

    def fee(self, liquidity: Liquidity, qty: float, price: float) -> float:
        """Комиссия сделки: > 0 списано, < 0 ребейт."""
        return qty * price * self.fee_rate(liquidity)


def funding_fee(signed_qty: float, mark_price: float, funding_rate: float) -> float:
    """Фандинг позиции в момент списания: > 0 списано, < 0 начислено.

    signed_qty > 0 - лонг. Лонг платит при положительной ставке, шорт получает.
    """
    return signed_qty * mark_price * funding_rate


# ------------------------------------------------------------------ ордера, сделки


@dataclass(frozen=True)
class OrderIntent:
    """Намерение стратегии. Время исполнения здесь задать нельзя - его выставляет барьер."""

    symbol: str
    side: Side
    qty: float  # в базовом активе, > 0 (направление - в side)
    asof_ts: int  # момент решения (шкала знания)
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None  # только LIMIT
    ttl_ms: int | None = None  # только LIMIT: срок жизни после exec_ts

    def __post_init__(self) -> None:
        err = ContractViolation
        if not isinstance(self.symbol, str) or not self.symbol:
            raise err(f"symbol должен быть непустой строкой, получено {self.symbol!r}")
        object.__setattr__(self, "side", Side(self.side))
        object.__setattr__(self, "order_type", OrderType(self.order_type))
        _num(self.qty, "qty", positive=True, error=err)
        _ms(self.asof_ts, "asof_ts", error=err)
        if self.order_type is OrderType.MARKET:
            if self.limit_price is not None or self.ttl_ms is not None:
                raise err("MARKET не принимает limit_price и ttl_ms")
        else:
            if self.limit_price is None or self.ttl_ms is None:
                raise err("LIMIT требует limit_price и ttl_ms (ордер без срока жизни запрещён)")
            _num(self.limit_price, "limit_price", positive=True, error=err)
            _ms(self.ttl_ms, "ttl_ms", error=err)


@dataclass(frozen=True)
class OrderTicket:
    """Принятый барьером ордер. Создаётся только в ExecutionBarrier.submit."""

    ticket_id: int
    intent: OrderIntent
    exec_ts: int  # = asof_ts + order_latency_ms
    expire_ts: int | None  # LIMIT: exec_ts + ttl_ms; MARKET: None


@dataclass(frozen=True)
class RawFill:
    """Исполнение от движка. Комиссии нет: её добавляет барьер."""

    ticket_id: int
    ts: int
    qty: float
    price: float
    liquidity: Liquidity

    def __post_init__(self) -> None:
        _ms(self.ts, "ts", error=ContractViolation)
        _num(self.qty, "qty", positive=True, error=ContractViolation)
        _num(self.price, "price", positive=True, error=ContractViolation)
        object.__setattr__(self, "liquidity", Liquidity(self.liquidity))


@dataclass(frozen=True)
class Fill:
    ticket_id: int
    symbol: str
    side: Side
    qty: float
    price: float
    liquidity: Liquidity
    ts: int
    fee: float  # > 0 списано, < 0 ребейт

    @property
    def notional(self) -> float:
        return self.qty * self.price

    @property
    def signed_qty(self) -> float:
        return self.side.sign * self.qty


@dataclass(frozen=True)
class OrderClosed:
    """Ордер закрыт без (полного) исполнения. remaining_qty - неисполненный остаток."""

    ticket_id: int
    ts: int
    remaining_qty: float
    reason: CloseReason

    def __post_init__(self) -> None:
        _ms(self.ts, "ts", error=ContractViolation)
        _num(self.remaining_qty, "remaining_qty", error=ContractViolation)
        object.__setattr__(self, "reason", CloseReason(self.reason))


@dataclass(frozen=True)
class FundingRecord:
    """Закрытая ставка фандинга (строка FUNDING_SCHEMA): данные учёта, не признак."""

    symbol: str
    funding_time: int  # серверное время списания, мс UTC
    funding_rate: float
    mark_price: float  # обязателен: без него номинал списания не определён

    def __post_init__(self) -> None:
        err = ContractViolation
        if not isinstance(self.symbol, str) or not self.symbol:
            raise err(f"symbol должен быть непустой строкой, получено {self.symbol!r}")
        _ms(self.funding_time, "funding_time", error=err)
        if abs(_num(self.funding_rate, "funding_rate", error=err)) >= 1.0:
            raise err(f"funding_rate вне допустимого диапазона: {self.funding_rate}")
        _num(self.mark_price, "mark_price (null в записи фандинга: учёт невозможен)", positive=True, error=err)


@dataclass(frozen=True)
class FundingSettlement:
    symbol: str
    funding_time: int
    position_qty: float  # знаковая позиция строго до funding_time
    mark_price: float
    funding_rate: float
    funding_fee: float  # > 0 списано, < 0 начислено

    @property
    def ts(self) -> int:
        return self.funding_time


BarrierEvent = Fill | OrderClosed | FundingSettlement


@dataclass(frozen=True)
class EndOfData:
    """Итог finalize: события в порядке «закрытия ордеров, затем выкуп позиций»."""

    events: tuple[BarrierEvent, ...]
    liquidation_tickets: tuple[OrderTicket, ...]  # ордера, созданные барьером для выкупа позиций


class FundingFeed(Protocol):
    """Источник списаний для учёта. Реализация оборачивает FundingLoader (учитывая gap_before)."""

    def settlements(self, symbol: str, from_ts: int, to_ts: int) -> Sequence[FundingRecord]:
        """ВСЕ списания с funding_time в [from_ts, to_ts), по возрастанию времени.

        Если известно, что окно покрыто не полностью (пропущено списание, данные кончились
        раньше to_ts), реализация обязана бросить FundingCoverageError.
        """
        ...


# ------------------------------------------------------------------------ барьер


@dataclass
class _TicketState:
    ticket: OrderTicket
    remaining: float
    n_fills: int = 0
    closed: bool = False


class ExecutionBarrier(ABC):
    """Проверяемая граница между стратегией и рынком. Подкласс = движок сопоставления."""

    def __init__(self, config: ExecutionConfig, funding: FundingFeed, start_ts: int):
        self._config = config
        self._funding = funding
        self._now = _ms(start_ts, "start_ts", minimum=0)
        self._last_asof = self._now
        self._next_id = 1
        self._tickets: dict[int, _TicketState] = {}
        # Журнал позиции: по символу - времена исполнений и накопленная знаковая позиция после них.
        self._ledger_ts: dict[str, list[int]] = {}
        self._ledger_pos: dict[str, list[float]] = {}
        self._finalized = False

    # --------------------------------------------------------------- публичное

    @property
    def config(self) -> ExecutionConfig:
        return self._config

    @property
    def now_ts(self) -> int:
        return self._now

    def position(self, symbol: str) -> float:
        """Знаковая позиция по уже выпущенным исполнениям."""
        return self._position_before(symbol, 2**62)

    def open_ticket_ids(self) -> tuple[int, ...]:
        return tuple(i for i, s in self._tickets.items() if not s.closed)

    def submit(self, intent: OrderIntent) -> OrderTicket:
        """Принимает решение стратегии. exec_ts выставляет барьер."""
        if not isinstance(intent, OrderIntent):
            raise ContractViolation(f"submit принимает OrderIntent, получено {type(intent).__name__}")
        self._require_open("submit")
        floor = max(self._now, self._last_asof)
        if intent.asof_ts < floor:
            raise ContractViolation(
                f"Ордер задним числом: asof_ts={intent.asof_ts} < {floor} "
                "(время барьера или предыдущее решение)"
            )
        exec_ts = self._config.exec_ts(intent.asof_ts)
        expire = exec_ts + intent.ttl_ms if intent.order_type is OrderType.LIMIT else None
        ticket = OrderTicket(self._next_id, intent, exec_ts, expire)
        self._accept(ticket)
        self._tickets[ticket.ticket_id] = _TicketState(ticket, intent.qty)
        self._next_id += 1
        self._last_asof = intent.asof_ts
        return ticket

    def advance(self, now_ts: int) -> tuple[BarrierEvent, ...]:
        """Выпускает события с ts в [прежнее время, now_ts), по возрастанию времени.

        Исполнения и закрытия приходят от движка (_match) и проверяются; комиссии и
        списания фандинга добавляет сам барьер. При равном ts порядок: фандинг, исполнения,
        закрытия.
        """
        self._require_open("advance")
        _ms(now_ts, "now_ts", minimum=0, error=ContractViolation)
        if now_ts < self._now:
            raise ContractViolation(f"Время идёт назад: {now_ts} < {self._now}")
        lo, hi = self._now, now_ts

        events: list[BarrierEvent] = []
        last_ts = lo
        for item in self._match(lo, hi):
            ts = getattr(item, "ts", None)
            if not isinstance(ts, int) or not lo <= ts < hi:
                raise ContractViolation(f"Событие движка вне окна [{lo}, {hi}): {item!r}")
            if ts < last_ts:
                raise ContractViolation(f"События движка не упорядочены по времени: {item!r}")
            last_ts = ts
            if isinstance(item, RawFill):
                events.append(self._admit_fill(item))
            elif isinstance(item, OrderClosed):
                self._admit_close(item)
                events.append(item)
            else:
                raise ContractViolation(f"Движок вернул {type(item).__name__}; допустимы RawFill и OrderClosed")

        events.extend(self._settle_funding(lo, hi))
        self._now = hi
        self._require_resolved(hi)
        events.sort(key=_event_order)
        return tuple(events)

    def finalize(self, end_ts: int, marks: Mapping[str, float]) -> EndOfData:
        """Конец данных (правило 7): EXPIRED для всех открытых ордеров, выкуп позиций по marks.

        Вызывается один раз, сразу после advance(end_ts): end_ts == now_ts. Выкуп - TAKER-ордер
        на |позиция| по цене marks[symbol] (последняя известная цена; проскальзывания нет, так
        как стакана за пределами данных нет), комиссию считает барьер как для любого исполнения.
        Фандинг после end_ts не начисляется: позиции больше нет.
        """
        self._require_open("finalize")
        end_ts = _ms(end_ts, "end_ts", error=ContractViolation)
        if end_ts != self._now:
            raise ContractViolation(f"finalize({end_ts}) вызывается после advance({end_ts}); время барьера {self._now}")

        events: list[BarrierEvent] = []
        for tid in sorted(self._tickets):
            state = self._tickets[tid]
            if not state.closed:
                state.closed = True
                events.append(OrderClosed(tid, end_ts, state.remaining, CloseReason.EXPIRED))

        tickets: list[OrderTicket] = []
        for symbol in sorted(self._ledger_ts):
            qty = self.position(symbol)
            if abs(qty) <= _QTY_EPS:
                continue
            price = marks.get(symbol)
            if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
                raise ContractViolation(f"Нет цены для выкупа позиции {symbol} (qty={qty}): marks[{symbol!r}]={price!r}")
            intent = OrderIntent(symbol, Side.SELL if qty > 0 else Side.BUY, abs(qty), end_ts)
            ticket = OrderTicket(self._next_id, intent, exec_ts=end_ts, expire_ts=None)
            self._next_id += 1
            self._tickets[ticket.ticket_id] = _TicketState(ticket, intent.qty)
            tickets.append(ticket)
            events.append(self._admit_fill(RawFill(ticket.ticket_id, end_ts, intent.qty, float(price), Liquidity.TAKER)))

        self._finalized = True
        return EndOfData(tuple(events), tuple(tickets))

    # ----------------------------------------------------- для реализации (движка)

    @abstractmethod
    def _accept(self, ticket: OrderTicket) -> None:
        """Принять ордер в очередь движка. Исполнение возможно только начиная с ticket.exec_ts."""

    @abstractmethod
    def _match(self, from_ts: int, to_ts: int) -> Sequence[RawFill | OrderClosed]:
        """Все исполнения и закрытия с ts в [from_ts, to_ts), по неубыванию ts, каждое ровно один раз."""

    # ---------------------------------------------------------------- проверки

    def _require_open(self, what: str) -> None:
        if self._finalized:
            raise ContractViolation(f"{what}: барьер закрыт после finalize")

    def _state(self, ticket_id: int, what: str) -> _TicketState:
        state = self._tickets.get(ticket_id)
        if state is None or state.closed:
            raise ContractViolation(f"{what}: ордер {ticket_id} неизвестен или уже закрыт")
        return state

    def _admit_fill(self, raw: RawFill) -> Fill:
        state = self._state(raw.ticket_id, "Исполнение")
        t, intent = state.ticket, state.ticket.intent
        if raw.ts < t.exec_ts:
            raise ContractViolation(f"Исполнение раньше прихода ордера: ts={raw.ts} < exec_ts={t.exec_ts}")
        if intent.order_type is OrderType.MARKET:
            if raw.ts != t.exec_ts:
                raise ContractViolation(f"MARKET исполняется ровно в exec_ts={t.exec_ts}, получено ts={raw.ts}")
            if raw.liquidity is not Liquidity.TAKER:
                raise ContractViolation("MARKET исполняется только как TAKER")
        else:
            if raw.ts >= t.expire_ts:
                raise ContractViolation(f"LIMIT исполнен после срока жизни: ts={raw.ts} >= {t.expire_ts}")
            worse = raw.price > intent.limit_price if intent.side is Side.BUY else raw.price < intent.limit_price
            if worse:
                raise ContractViolation(f"Цена {raw.price} хуже лимитной {intent.limit_price}")
        if raw.qty > state.remaining + _QTY_EPS:
            raise ContractViolation(f"Переисполнение: qty={raw.qty} > остатка {state.remaining}")

        state.remaining -= raw.qty
        state.n_fills += 1
        if state.remaining <= _QTY_EPS:
            state.remaining, state.closed = 0.0, True
        fill = Fill(
            ticket_id=raw.ticket_id, symbol=intent.symbol, side=intent.side, qty=raw.qty, price=raw.price,
            liquidity=raw.liquidity, ts=raw.ts, fee=self._config.fee(raw.liquidity, raw.qty, raw.price),
        )
        times = self._ledger_ts.setdefault(intent.symbol, [])
        cums = self._ledger_pos.setdefault(intent.symbol, [])
        times.append(fill.ts)
        cums.append((cums[-1] if cums else 0.0) + fill.signed_qty)
        return fill

    def _admit_close(self, closed: OrderClosed) -> None:
        state = self._state(closed.ticket_id, "Закрытие")
        t = state.ticket
        if abs(closed.remaining_qty - state.remaining) > _QTY_EPS:
            raise ContractViolation(
                f"Остаток в OrderClosed ({closed.remaining_qty}) не равен учтённому ({state.remaining})"
            )
        if closed.reason is CloseReason.EXPIRED:
            if t.expire_ts is None or closed.ts != t.expire_ts:
                raise ContractViolation(f"EXPIRED допустим только для LIMIT и ровно в expire_ts={t.expire_ts}")
        elif closed.ts != t.exec_ts:
            raise ContractViolation(f"{closed.reason.value} фиксируется ровно в exec_ts={t.exec_ts}, ts={closed.ts}")
        if closed.reason is CloseReason.NO_LIQUIDITY and t.intent.order_type is not OrderType.MARKET:
            raise ContractViolation("NO_LIQUIDITY относится только к MARKET")
        if closed.reason is CloseReason.REJECTED and state.n_fills:
            raise ContractViolation("REJECTED невозможен после исполнения")
        state.closed = True

    def _require_resolved(self, now_ts: int) -> None:
        """Нет «висящих» ордеров: MARKET разрешён к exec_ts, LIMIT - к expire_ts."""
        for tid, s in self._tickets.items():
            deadline = s.ticket.exec_ts if s.ticket.expire_ts is None else s.ticket.expire_ts
            if not s.closed and deadline < now_ts:
                raise ContractViolation(f"Ордер {tid} не закрыт к сроку {deadline} (now={now_ts})")

    # ------------------------------------------------------------------ фандинг

    def _position_before(self, symbol: str, ts: int) -> float:
        times = self._ledger_ts.get(symbol)
        if not times:
            return 0.0
        i = bisect_left(times, ts)  # исполнения с ts' < ts
        return self._ledger_pos[symbol][i - 1] if i else 0.0

    def _settle_funding(self, lo: int, hi: int) -> list[FundingSettlement]:
        out: list[FundingSettlement] = []
        for symbol in sorted(self._ledger_ts):
            times = self._ledger_ts[symbol]
            traded_in_window = bisect_left(times, hi) > bisect_left(times, lo)
            if abs(self._position_before(symbol, lo)) <= _QTY_EPS and not traded_in_window:
                continue  # плоский символ: дыры в данных фандинга его не касаются
            records = self._funding.settlements(symbol, lo, hi)
            prev: int | None = None
            for rec in records:
                ok = (
                    isinstance(rec, FundingRecord) and rec.symbol == symbol and lo <= rec.funding_time < hi
                    and (prev is None or rec.funding_time > prev)  # по возрастанию, без дублей
                )
                if not ok:
                    raise ContractViolation(f"Фандинг-фид вернул некорректную запись для {symbol} в [{lo}, {hi}): {rec!r}")
                prev = rec.funding_time
                qty = self._position_before(symbol, rec.funding_time)
                if abs(qty) <= _QTY_EPS:
                    continue
                out.append(FundingSettlement(
                    symbol, rec.funding_time, qty, rec.mark_price, rec.funding_rate,
                    funding_fee(qty, rec.mark_price, rec.funding_rate),
                ))
        return out


_PRIORITY = {FundingSettlement: 0, Fill: 1, OrderClosed: 2}


def _event_order(event: BarrierEvent) -> tuple[int, int]:
    return event.ts, _PRIORITY[type(event)]
