"""OrderBookBarrier - реализация ExecutionBarrier: матчинг против стакана top-N.

Без констант проскальзывания: цена исполнения - то, что реально лежит в книге.

Синхронизация. Ордер доходит до биржи в exec_ts = asof_ts + order_latency_ms (считает базовый
класс). Исполняется он против ПЕРВОГО сообщения стакана с recv_ts >= exec_ts. Сообщение до
exec_ts - уже прошлое состояние книги, оно не используется никогда.
  * MARKET: одно сообщение, один проход. Сообщение дальше max_book_delay_ms от exec_ts (дыра
    в данных) - не повод исполнять по его ценам: ордер закрывается NO_LIQUIDITY.
  * LIMIT: живёт [exec_ts, exec_ts + ttl_ms) и сверяется с каждым сообщением recv_ts из этого окна.

MARKET - «идём по стакану» (walking the book), IOC. Покупка выкупает ask_qty_0 по ask_px_0,
затем ask_qty_1 по ask_px_1 и т.д., продажа идёт по бидам. Цена исполнения - VWAP собранной
ликвидности: sum(px_i * qty_i) / sum(qty_i). Если глубины top-N не хватило, исполняется
собранное (ОДНА сделка по VWAP; комиссия линейна по номиналу, поэтому равна сумме по уровням),
остаток закрывается OrderClosed(NO_LIQUIDITY) - в книге он не повисает.

LIMIT - пессимистично. Сторона ордера на исполнение НЕ влияет: бид, дошедший до нашей цены,
значит лишь то, что нас «догнали» в очереди, а не то, что нас купили. Нужен проход встречной
стороны:
  * BUY исполняется, только если ask_px_0 <= limit_price; SELL - если bid_px_0 >= limit_price;
  * объём ограничен глубиной встречных уровней, пересёкших лимитку; одна и та же стоящая
    ликвидность не продаётся нам дважды (повторное сообщение с теми же уровнями объёма не
    добавляет; когда пересечение пропало, счётчик сбрасывается);
  * ордер, который пересёк книгу уже при приходе (первое сообщение, вовремя), - marketable:
    идёт по стакану как TAKER (VWAP уровней не хуже лимитки), остаток встаёт в книгу;
  * ордер, ждавший в книге, исполняется как MAKER ровно по limit_price (улучшения цены нет, даже
    если встречная цена глубже) в момент recv_ts сообщения, пересёкшего его.
Ограничение: очередь внутри уровня не моделируется; собственное влияние на книгу тоже (следующее
сообщение приходит из исторических данных).
"""

import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

import polars as pl

from backtester.contracts import (
    _QTY_EPS,
    CloseReason,
    ExecutionBarrier,
    ExecutionConfig,
    FundingFeed,
    Liquidity,
    OrderClosed,
    OrderTicket,
    OrderType,
    RawFill,
    Side,
    _ms,
)
from utils.errors import ConfigError, DataError

Level = tuple[float, float]  # (цена, объём в базовом активе)


# ---------------------------------------------------------------------- стакан


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Сообщение стакана в шкале знания (recv_ts). Уровни - от лучшей цены вглубь, объём > 0."""

    recv_ts: int
    bids: tuple[Level, ...]  # убывание цены
    asks: tuple[Level, ...]  # возрастание цены

    def opposing(self, side: Side) -> tuple[Level, ...]:
        """Ликвидность, с которой торгует ордер: покупка - аски, продажа - биды."""
        return self.asks if side is Side.BUY else self.bids


class BookFeed(Protocol):
    """Источник сообщений стакана. Все выборки - по recv_ts, по возрастанию."""

    def has_symbol(self, symbol: str) -> bool: ...

    def first_at_or_after(self, symbol: str, ts: int) -> BookSnapshot | None:
        """Первое сообщение с recv_ts >= ts (None - данные кончились)."""
        ...

    def messages(self, symbol: str, from_ts: int, to_ts: int) -> Iterator[BookSnapshot]:
        """Сообщения с recv_ts в [from_ts, to_ts)."""
        ...


def _level(px: object, qty: object) -> Level | None:
    """Уровень или None, если его нет (null за пределами глубины) или он нулевой/битый."""
    if px is None or qty is None or not math.isfinite(px) or not math.isfinite(qty) or px <= 0 or qty <= 0:
        return None
    return float(px), float(qty)


class FrameBookFeed:
    """Стакан из таблицы нормализованных сообщений (pipeline.loaders.schemas.depth_schema).

    Нужны колонки recv_ts, symbol и bid_px_i / bid_qty_i / ask_px_i / ask_qty_i для i = 0..N-1.
    Таблица целиком живёт в памяти: на длинные периоды подавайте окно (DepthLoader.scan(...).collect()).
    Порядок строк с одинаковым recv_ts сохраняется (ожидается порядок (recv_ts, u), как у загрузчика).
    """

    def __init__(self, frame: pl.DataFrame):
        if not isinstance(frame, pl.DataFrame):
            raise ConfigError("FrameBookFeed принимает pl.DataFrame")
        levels = 0
        while all(f"{s}_{k}_{levels}" in frame.columns for s in ("bid", "ask") for k in ("px", "qty")):
            levels += 1
        if levels == 0 or not {"recv_ts", "symbol"} <= set(frame.columns):
            raise ConfigError("В стакане нет колонок recv_ts, symbol, bid_px_0, bid_qty_0, ask_px_0, ask_qty_0")
        if frame["recv_ts"].null_count() or frame["symbol"].null_count():
            raise DataError("recv_ts и symbol в стакане не могут быть null")
        self._levels = levels
        cols = ["recv_ts"] + [f"{s}_{k}_{i}" for i in range(levels) for s in ("bid", "ask") for k in ("px", "qty")]
        # порядок внутри уровня: bid_px, bid_qty, ask_px, ask_qty (см. _snapshot)
        self._parts: dict[str, pl.DataFrame] = {}
        for key, part in frame.select("symbol", *cols).partition_by("symbol", as_dict=True).items():
            self._parts[key[0]] = part.drop("symbol").sort("recv_ts", maintain_order=True)

    def has_symbol(self, symbol: str) -> bool:
        return symbol in self._parts

    def _snapshot(self, row: tuple) -> BookSnapshot:
        bids, asks = [], []
        for i in range(self._levels):
            b_px, b_qty, a_px, a_qty = row[1 + 4 * i: 5 + 4 * i]
            if (b := _level(b_px, b_qty)) is not None:
                bids.append(b)
            if (a := _level(a_px, a_qty)) is not None:
                asks.append(a)
        bids.sort(key=lambda lv: -lv[0])  # идти по книге можно только от лучшей цены
        asks.sort(key=lambda lv: lv[0])
        return BookSnapshot(int(row[0]), tuple(bids), tuple(asks))

    def first_at_or_after(self, symbol: str, ts: int) -> BookSnapshot | None:
        part = self._parts[symbol]
        i = part["recv_ts"].search_sorted(ts, side="left")
        return self._snapshot(part.row(i)) if i < part.height else None

    def messages(self, symbol: str, from_ts: int, to_ts: int) -> Iterator[BookSnapshot]:
        part = self._parts[symbol]
        ts = part["recv_ts"]
        i, j = ts.search_sorted(from_ts, side="left"), ts.search_sorted(to_ts, side="left")
        for row in part.slice(i, j - i).iter_rows():
            yield self._snapshot(row)


# ----------------------------------------------------------------- «ходьба по книге»


@dataclass(frozen=True)
class Sweep:
    """Результат прохода по уровням."""

    qty: float  # исполненный объём (при complete - ровно запрошенный)
    vwap: float | None  # None, если собрать нечего
    levels: int  # сколько уровней затронуто
    complete: bool  # запрошенный объём собран целиком


def walk_book(levels: Sequence[Level], qty: float) -> Sweep:
    """Выкупает qty по уровням от лучшего и возвращает VWAP = sum(px_i * q_i) / sum(q_i).

    levels - уже встречная сторона от лучшей цены вглубь. Суммы считаются через fsum: результат не
    зависит от порядка накопления и не копит ошибку на глубоких книгах.
    """
    need = qty
    notional: list[float] = []
    taken: list[float] = []
    for px, level_qty in levels:
        take = level_qty if level_qty < need else need
        notional.append(px * take)
        taken.append(take)
        need -= take
        if need <= _QTY_EPS:
            break
    if not taken:
        return Sweep(0.0, None, 0, False)
    collected = math.fsum(taken)
    complete = need <= _QTY_EPS
    return Sweep(qty if complete else collected, math.fsum(notional) / collected, len(taken), complete)


def crossing_levels(levels: Sequence[Level], side: Side, limit_price: float) -> tuple[Level, ...]:
    """Встречные уровни, пересёкшие лимитку: аск <= цены у BUY, бид >= цены у SELL (префикс книги)."""
    out = []
    for px, qty in levels:
        if (px > limit_price) if side is Side.BUY else (px < limit_price):
            break
        out.append((px, qty))
    return tuple(out)


# ---------------------------------------------------------------------- барьер


@dataclass
class _Live:
    ticket: OrderTicket
    remaining: float
    arrived: bool = False  # первое сообщение с recv_ts >= exec_ts уже обработано
    cross_used: float = 0.0  # объём пересекающей ликвидности, уже проданный нам при текущем пересечении


class OrderBookBarrier(ExecutionBarrier):
    """Матчинг MARKET/LIMIT против стакана. Правила - в докстринге модуля."""

    def __init__(
        self,
        config: ExecutionConfig,
        funding: FundingFeed,
        start_ts: int,
        books: BookFeed | pl.DataFrame,
        *,
        max_book_delay_ms: int = 2_000,
    ):
        super().__init__(config, funding, start_ts)
        self._books: BookFeed = FrameBookFeed(books) if isinstance(books, pl.DataFrame) else books
        # Как далеко от exec_ts может быть сообщение стакана. По умолчанию - book_max_staleness_ms из TimingConfig.
        self._max_delay = _ms(max_book_delay_ms, "max_book_delay_ms")
        self._live: dict[int, _Live] = {}

    # ------------------------------------------------------------ контракт

    def _accept(self, ticket: OrderTicket) -> None:
        symbol = ticket.intent.symbol
        if not self._books.has_symbol(symbol):
            raise ConfigError(f"Нет данных стакана для {symbol}")
        self._live[ticket.ticket_id] = _Live(ticket, ticket.intent.qty)

    def _match(self, from_ts: int, to_ts: int) -> Sequence[RawFill | OrderClosed]:
        out: list[tuple[int, int, int, RawFill | OrderClosed]] = []  # (ts, ticket_id, 0 сделка / 1 закрытие, событие)
        for tid in sorted(self._live):
            live = self._live[tid]
            if live.ticket.intent.order_type is OrderType.MARKET:
                if from_ts <= live.ticket.exec_ts < to_ts:
                    self._market(live, out)
                    del self._live[tid]
            elif self._limit(live, from_ts, to_ts, out):
                del self._live[tid]
        out.sort(key=lambda e: e[:3])  # стабильно: ни одна сделка не обгонит своё закрытие
        return [e[3] for e in out]

    # --------------------------------------------------------------- MARKET

    def _market(self, live: _Live, out: list) -> None:
        t, intent = live.ticket, live.ticket.intent
        snap = self._books.first_at_or_after(intent.symbol, t.exec_ts)
        if snap is not None and snap.recv_ts - t.exec_ts <= self._max_delay:
            sweep = walk_book(snap.opposing(intent.side), live.remaining)
            if sweep.qty > 0:
                self._fill(live, out, t.exec_ts, sweep.qty, sweep.vwap, Liquidity.TAKER)
        if live.remaining > 0:  # IOC: что не нашло ликвидности, не остаётся
            out.append((t.exec_ts, t.ticket_id, 1, OrderClosed(t.ticket_id, t.exec_ts, live.remaining, CloseReason.NO_LIQUIDITY)))

    # ---------------------------------------------------------------- LIMIT

    def _limit(self, live: _Live, from_ts: int, to_ts: int, out: list) -> bool:
        """Обрабатывает окно [from_ts, to_ts). True - ордер закрыт (исполнен или истёк)."""
        t, intent = live.ticket, live.ticket.intent
        start, end = max(from_ts, t.exec_ts), min(to_ts, t.expire_ts)
        if start < end:
            for snap in self._books.messages(intent.symbol, start, end):
                self._limit_step(live, snap, out)
                if live.remaining <= 0:
                    return True
        if from_ts <= t.expire_ts < to_ts:
            out.append((t.expire_ts, t.ticket_id, 1, OrderClosed(t.ticket_id, t.expire_ts, live.remaining, CloseReason.EXPIRED)))
            return True
        return False

    def _limit_step(self, live: _Live, snap: BookSnapshot, out: list) -> None:
        t, intent = live.ticket, live.ticket.intent
        limit = intent.limit_price
        first, live.arrived = not live.arrived, True
        cross = crossing_levels(snap.opposing(intent.side), intent.side, limit)
        if not cross:
            live.cross_used = 0.0  # пересечение кончилось: следующее - новая ликвидность
            return
        if first and snap.recv_ts - t.exec_ts <= self._max_delay:
            # marketable: ордер пересёк книгу при приходе - берём ликвидность как TAKER, по VWAP, не хуже лимитки
            sweep = walk_book(cross, live.remaining)
            price = min(sweep.vwap, limit) if intent.side is Side.BUY else max(sweep.vwap, limit)  # округление fsum
            qty, liquidity = sweep.qty, Liquidity.TAKER
        else:
            # ждал в книге: нас исполнил встречный поток, по нашей цене, в пределах ещё не проданной глубины
            available = math.fsum(q for _, q in cross) - live.cross_used
            if available <= _QTY_EPS:
                return
            qty, price, liquidity = min(live.remaining, available), limit, Liquidity.MAKER
        live.cross_used += qty
        self._fill(live, out, snap.recv_ts, qty, price, liquidity)

    # ------------------------------------------------------------------ общее

    @staticmethod
    def _fill(live: _Live, out: list, ts: int, qty: float, price: float, liquidity: Liquidity) -> None:
        out.append((ts, live.ticket.ticket_id, 0, RawFill(live.ticket.ticket_id, ts, qty, price, liquidity)))
        live.remaining -= qty  # та же арифметика, что у базового класса: остаток в OrderClosed совпадает до бита
        if live.remaining <= _QTY_EPS:
            live.remaining = 0.0
