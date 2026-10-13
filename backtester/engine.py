"""BacktestEngine - событийный симулятор поверх ExecutionBarrier.

Движок не сопоставляет ордера (это делает барьер) и не принимает решений (это делает
стратегия). Он ведёт время, соединяет стратегию, риск и барьер и считает деньги.

Шаг по времени (для каждого asof_ts таблицы решений; строки с одинаковым asof_ts - один шаг):

    1. barrier.advance(asof_ts)     события СТРОГО до asof_ts: исполнения, закрытия, списания фандинга;
                                    движок применяет их к портфелю (позиция, комиссии, фандинг);
    2. цены строк шага -> mark      оценка позиций по ценам, известным в asof_ts;
    3. точка кривой капитала        equity = баланс + нереализованный PnL; guard.on_equity(...);
    4. strategy.decide(ctx)         ctx - FeatureContext (backtester/strategy): ТОЛЬКО текущая строка и снимок портфеля;
    5. каждый запрос -> гуарды -> barrier.submit(intent)   asof_ts проставляет движок, exec_ts - барьер.

Следствия строгости:
  * стратегия не получает кадр целиком и не может заглянуть вперёд; кадр с колонками меток
    (префикс y_) движок не принимает вовсе, кадр без происхождения (src_max_ts < asof_ts) -
    тоже (ProvenanceGuard);
  * ордер, поданный в asof_ts, не может повлиять на эквити в этот же момент: исполнение в
    asof_ts + order_latency_ms становится видно со следующего шага;
  * отклонения риска (гуарды) и отклонения биржи (OrderClosed: REJECTED / NO_LIQUIDITY /
    EXPIRED) фиксируются в журналах; отклонённый остаток немедленно перестаёт считаться
    «ожидающим» и не занимает лимиты.

Деньги (маржинальный USDT-счёт без модели маржи и ликвидации):

    баланс = initial_equity + реализованный PnL - комиссии - фандинг
    equity = баланс + sum(позиция * (mark - средняя цена входа))

Позиция ведётся по средней цене входа; закрытие и разворот реализуют PnL по средней цене.
Комиссии и фандинг берутся ТОЛЬКО из событий барьера (Fill.fee, FundingSettlement.funding_fee),
движок их не пересчитывает.

Конец данных. После последней строки решений движок доводит время только до исполнения уже
поданных ордеров (последний exec_ts + 1 мс), а не до сроков жизни LIMIT: данных за последней
строкой нет, и ожидание «чудесного» исполнения или фандинг-фид за границей данных испортили бы
результат (FundingCoverageError). Затем barrier.finalize: все ещё живые ордера закрываются со
статусом expired (в orders.status), позиции выкупаются TAKER-ордерами по последней известной
цене (последняя цена price_col символа; такие ордера помечены orders.forced = True, комиссия
взята как за обычное исполнение). Кривая капитала заканчивается точкой после выкупа.

Ограничения: нет маржи и ликвидации (плечо ограничивает MaxExposureGuard); mark - цена
колонки price_col строки решения (по умолчанию close свечи, известная в asof_ts).
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import groupby
from types import MappingProxyType

import polars as pl

from backtester.contracts import (
    BarrierEvent,
    ExecutionBarrier,
    Fill,
    FundingSettlement,
    OrderClosed,
    OrderTicket,
)
from backtester.risk import QTY_EPS, OrderRequest, RiskContext, RiskGuard
from backtester.strategy import DecisionContext, FeatureContext, Strategy  # noqa: F401  (реэкспорт прежних имён)
from pipeline.guards.provenance import ProvenanceGuard
from pipeline.types import LABEL_PREFIX
from utils.errors import ConfigError, DataError

_SYMBOL, _ASOF, _SRC = "symbol", "asof_ts", "src_max_ts"


class BacktestError(DataError):
    """Прогон невозможен или внутренний учёт разошёлся с барьером."""


# ------------------------------------------------------------------- портфель


class Portfolio:
    """Позиции и деньги. Меняется только событиями барьера и ценами."""

    def __init__(self, initial_equity: float):
        self.initial = initial_equity
        self.realized = 0.0
        self.fees = 0.0
        self.funding = 0.0
        self._pos: dict[str, float] = {}
        self._avg: dict[str, float] = {}
        self._mark: dict[str, float] = {}

    def position(self, symbol: str) -> float:
        return self._pos.get(symbol, 0.0)

    def positions(self) -> dict[str, float]:
        return {s: q for s, q in self._pos.items() if q != 0.0}

    def mark(self, symbol: str) -> float | None:
        return self._mark.get(symbol)

    def marks(self) -> dict[str, float]:
        return dict(self._mark)

    def set_mark(self, symbol: str, price: float) -> None:
        self._mark[symbol] = price

    @property
    def balance(self) -> float:
        return self.initial + self.realized - self.fees - self.funding

    @property
    def unrealized(self) -> float:
        return sum(q * (self._mark[s] - self._avg[s]) for s, q in self._pos.items() if q != 0.0)

    @property
    def equity(self) -> float:
        return self.balance + self.unrealized

    @property
    def gross_exposure(self) -> float:
        return sum(abs(q) * self._mark[s] for s, q in self._pos.items() if q != 0.0)

    def apply_fill(self, fill: Fill) -> None:
        pos, avg, signed = self.position(fill.symbol), self._avg.get(fill.symbol, 0.0), fill.signed_qty
        new = pos + signed
        if pos == 0.0 or pos * signed > 0:  # открытие или добавление: средняя цена
            avg = (abs(pos) * avg + fill.qty * fill.price) / abs(new)
        else:  # уменьшение, закрытие или разворот
            closing = min(fill.qty, abs(pos))
            self.realized += closing * (fill.price - avg) * (1 if pos > 0 else -1)
            if abs(new) <= QTY_EPS:
                new, avg = 0.0, 0.0
            elif new * pos < 0:
                avg = fill.price  # остаток после разворота открыт по цене исполнения
        self.fees += fill.fee
        self._pos[fill.symbol], self._avg[fill.symbol] = new, avg

    def apply_funding(self, settlement: FundingSettlement) -> None:
        self.funding += settlement.funding_fee


# ------------------------------------------------------------------ результат

_EQUITY_SCHEMA = {
    "ts": pl.Int64, "equity": pl.Float64, "balance": pl.Float64, "realized_pnl": pl.Float64,
    "unrealized_pnl": pl.Float64, "fees": pl.Float64, "funding": pl.Float64, "gross_exposure": pl.Float64,
}
_FILLS_SCHEMA = {
    "ts": pl.Int64, "ticket_id": pl.Int64, "symbol": pl.String, "side": pl.String, "qty": pl.Float64,
    "price": pl.Float64, "liquidity": pl.String, "fee": pl.Float64,
}
_FUNDING_SCHEMA = {
    "funding_time": pl.Int64, "symbol": pl.String, "position_qty": pl.Float64, "mark_price": pl.Float64,
    "funding_rate": pl.Float64, "funding_fee": pl.Float64,
}
_ORDERS_SCHEMA = {
    "ticket_id": pl.Int64, "symbol": pl.String, "side": pl.String, "order_type": pl.String, "qty": pl.Float64,
    "asof_ts": pl.Int64, "exec_ts": pl.Int64, "expire_ts": pl.Int64, "filled_qty": pl.Float64, "status": pl.String,
    "forced": pl.Boolean,
}
_RISK_SCHEMA = {
    "ts": pl.Int64, "guard": pl.String, "symbol": pl.String, "requested_qty": pl.Float64,
    "allowed_qty": pl.Float64, "reason": pl.String,
}


@dataclass(frozen=True)
class BacktestResult:
    initial_equity: float
    equity_curve: pl.DataFrame
    fills: pl.DataFrame
    funding: pl.DataFrame
    orders: pl.DataFrame  # ордера, дошедшие до барьера; status: filled | expired | no_liquidity | rejected;
    # forced - ордер создан барьером для выкупа позиции в конце данных (не стратегией)
    risk_events: pl.DataFrame  # урезания и блоки гуардов (и «нет цены»), до барьера не дошедшие

    @property
    def final_equity(self) -> float:
        return float(self.equity_curve["equity"][-1])

    @property
    def total_pnl(self) -> float:
        return self.final_equity - self.initial_equity

    @property
    def total_fees(self) -> float:
        return float(self.equity_curve["fees"][-1])

    @property
    def total_funding(self) -> float:
        return float(self.equity_curve["funding"][-1])

    @property
    def max_drawdown(self) -> float:
        """Наибольшая просадка кривой капитала от пика (доля)."""
        eq = self.equity_curve["equity"]
        peak = eq.cum_max()
        return float(((peak - eq) / peak).max() or 0.0)


@dataclass
class _Order:
    ticket: OrderTicket
    filled: float = 0.0
    status: str = "open"
    forced: bool = False

    @property
    def remaining(self) -> float:
        return self.ticket.intent.qty - self.filled


# --------------------------------------------------------------------- движок


class BacktestEngine:
    """Одноразовый: барьер хранит состояние, поэтому второй run() на том же объекте запрещён."""

    def __init__(
        self,
        barrier: ExecutionBarrier,
        strategy: Strategy,
        guards: Sequence[RiskGuard] = (),
        *,
        initial_equity: float,
        price_col: str = "close",
        verify_provenance: bool = True,
    ):
        if isinstance(initial_equity, bool) or not isinstance(initial_equity, (int, float)) \
                or not math.isfinite(initial_equity) or initial_equity <= 0:
            raise ConfigError(f"initial_equity должен быть > 0, получено {initial_equity!r}")
        if not isinstance(strategy, Strategy):
            raise ConfigError("strategy должен быть наследником Strategy")
        if not all(isinstance(g, RiskGuard) for g in guards):
            raise ConfigError("guards должен содержать только RiskGuard")
        if not isinstance(price_col, str) or not price_col or price_col.startswith(LABEL_PREFIX):
            raise ConfigError(f"Недопустимая колонка цены {price_col!r}")
        self._barrier, self._strategy, self._guards = barrier, strategy, tuple(guards)
        self._initial = float(initial_equity)
        self._price_col = price_col
        self._verify = verify_provenance
        self._used = False

    # ----------------------------------------------------------------- прогон

    def run(self, decisions: pl.DataFrame) -> BacktestResult:
        if self._used:
            raise BacktestError("BacktestEngine одноразовый: создайте новый барьер и движок")
        self._used = True
        frame = self._validate(decisions)

        self._portfolio = Portfolio(self._initial)
        self._open: dict[int, _Order] = {}
        self._orders: list[_Order] = []
        self._equity_rows: list[dict] = []
        self._fill_rows: list[dict] = []
        self._funding_rows: list[dict] = []
        self._risk_rows: list[dict] = []
        for g in self._guards:
            g.reset()

        if frame.height:
            first_ts = int(frame[_ASOF][0])
            if self._barrier.now_ts > first_ts:
                raise BacktestError(f"Время барьера {self._barrier.now_ts} позже первого решения {first_ts}")

        last_ts = None
        rows = frame.iter_rows(named=True)
        for ts, group in groupby(rows, key=lambda r: r[_ASOF]):
            batch = list(group)
            self._apply(self._barrier.advance(ts))
            for r in batch:
                price = r[self._price_col]
                if price is not None and math.isfinite(price) and price > 0:
                    self._portfolio.set_mark(r[_SYMBOL], float(price))
            self._record_equity(ts)
            equity = self._portfolio.equity
            for g in self._guards:
                g.on_equity(ts, equity)
            for r in batch:
                self._decide(ts, r)
            last_ts = ts

        if last_ts is not None:
            self._finish(last_ts)
        return self._result()

    # ------------------------------------------------------------- подготовка

    def _validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if not isinstance(df, pl.DataFrame):
            raise BacktestError("decisions должен быть pl.DataFrame")
        missing = sorted({_SYMBOL, _ASOF, self._price_col} - set(df.columns))
        if missing:
            raise BacktestError(f"В таблице решений нет колонок {missing}")
        labels = sorted(c for c in df.columns if c.startswith(LABEL_PREFIX))
        if labels:
            raise BacktestError(f"В таблице решений есть метки {labels}: они смотрят в будущее, утечка в стратегию")
        if df[_SYMBOL].null_count() or df[_ASOF].null_count():
            raise BacktestError("symbol и asof_ts не могут быть null")
        if self._verify:
            extra = [c for c in df.columns if c.endswith("_ts") and c not in (_ASOF, _SRC)]
            ProvenanceGuard(extra_ts_cols=extra).check(df.sort(_SYMBOL, _ASOF))  # LookaheadViolation
        return df.sort(_ASOF, _SYMBOL)

    # ------------------------------------------------------------------ события

    def _apply(self, events: Sequence[BarrierEvent]) -> None:
        for e in events:
            if isinstance(e, Fill):
                self._portfolio.apply_fill(e)
                order = self._open[e.ticket_id]
                order.filled += e.qty
                if order.remaining <= QTY_EPS:
                    order.status = "filled"
                    del self._open[e.ticket_id]
                self._fill_rows.append({
                    "ts": e.ts, "ticket_id": e.ticket_id, "symbol": e.symbol, "side": e.side.value,
                    "qty": e.qty, "price": e.price, "liquidity": e.liquidity.value, "fee": e.fee,
                })
            elif isinstance(e, OrderClosed):
                order = self._open.pop(e.ticket_id)  # остаток больше не «ожидает»
                order.status = e.reason.value
            elif isinstance(e, FundingSettlement):
                self._portfolio.apply_funding(e)
                self._funding_rows.append({
                    "funding_time": e.funding_time, "symbol": e.symbol, "position_qty": e.position_qty,
                    "mark_price": e.mark_price, "funding_rate": e.funding_rate, "funding_fee": e.funding_fee,
                })
            self._strategy.on_event(e)

    def _record_equity(self, ts: int) -> None:
        p = self._portfolio
        self._equity_rows.append({
            "ts": ts, "equity": p.equity, "balance": p.balance, "realized_pnl": p.realized,
            "unrealized_pnl": p.unrealized, "fees": p.fees, "funding": p.funding, "gross_exposure": p.gross_exposure,
        })

    # ------------------------------------------------------------------ решения

    def _pending(self, symbol: str) -> float:
        return sum(o.ticket.intent.side.sign * o.remaining for o in self._open.values()
                   if o.ticket.intent.symbol == symbol)

    def _decide(self, ts: int, row: dict) -> None:
        p = self._portfolio
        ctx = FeatureContext(
            ts=ts, symbol=row[_SYMBOL], row=MappingProxyType(dict(row)), equity=p.equity,
            positions=MappingProxyType(p.positions()),
            pending=MappingProxyType({s: self._pending(s) for s in {o.ticket.intent.symbol for o in self._open.values()}}),
            marks=MappingProxyType(p.marks()),
        )
        for request in self._strategy.decide(ctx) or ():
            if not isinstance(request, OrderRequest):
                raise BacktestError(f"Стратегия вернула {type(request).__name__}; ожидается OrderRequest")
            self._submit(ts, request)

    def _log_risk(self, ts: int, guard: str, request: OrderRequest, requested: float, allowed: float, reason: str | None):
        self._risk_rows.append({
            "ts": ts, "guard": guard, "symbol": request.symbol, "requested_qty": requested,
            "allowed_qty": allowed, "reason": reason,
        })

    def _submit(self, ts: int, request: OrderRequest) -> None:
        p = self._portfolio
        if p.mark(request.symbol) is None:
            self._log_risk(ts, "engine", request, request.qty, 0.0, "нет цены символа: ордер не оценить")
            return
        qty = request.qty
        for g in self._guards:
            ctx = RiskContext(
                ts=ts, symbol=request.symbol, equity=p.equity, price=p.mark(request.symbol),
                position=p.position(request.symbol), pending=self._pending(request.symbol),
            )
            decision = g.check(request.with_qty(qty), ctx)
            allowed = decision.qty
            if not isinstance(allowed, (int, float)) or not math.isfinite(allowed) or not 0 <= allowed <= qty + QTY_EPS:
                raise BacktestError(f"{g.name}: недопустимое решение {decision!r} для запроса qty={qty}")
            allowed = min(allowed, qty)
            if allowed < qty - QTY_EPS:
                self._log_risk(ts, g.name, request, qty, allowed if allowed > QTY_EPS else 0.0, decision.reason)
            qty = allowed
            if qty <= QTY_EPS:
                return
        ticket = self._barrier.submit(request.with_qty(qty).to_intent(ts))
        order = _Order(ticket)
        self._orders.append(order)
        self._open[ticket.ticket_id] = order

    # -------------------------------------------------------------- завершение

    def _finish(self, last_ts: int) -> None:
        """Конец данных: доводит приход ордеров, закрывает остатки и выкупает позиции (см. докстринг модуля)."""
        end = max([last_ts, *(o.ticket.exec_ts for o in self._open.values())]) + 1
        self._apply(self._barrier.advance(end))
        closing = self._barrier.finalize(end, self._portfolio.marks())
        for ticket in closing.liquidation_tickets:
            order = _Order(ticket, forced=True)
            self._orders.append(order)
            self._open[ticket.ticket_id] = order
        self._apply(closing.events)
        self._record_equity(end)
        if self._open:
            raise BacktestError(f"После завершения остались открытые ордера: {sorted(self._open)}")
        for symbol, qty in self._portfolio.positions().items():
            if abs(qty - self._barrier.position(symbol)) > 1e-9:
                raise BacktestError(f"Позиция {symbol}: движок {qty}, барьер {self._barrier.position(symbol)}")

    def _result(self) -> BacktestResult:
        orders = [{
            "ticket_id": o.ticket.ticket_id, "symbol": o.ticket.intent.symbol, "side": o.ticket.intent.side.value,
            "order_type": o.ticket.intent.order_type.value, "qty": o.ticket.intent.qty, "asof_ts": o.ticket.intent.asof_ts,
            "exec_ts": o.ticket.exec_ts, "expire_ts": o.ticket.expire_ts, "filled_qty": o.filled, "status": o.status,
            "forced": o.forced,
        } for o in self._orders]
        return BacktestResult(
            initial_equity=self._initial,
            equity_curve=pl.DataFrame(self._equity_rows, schema=_EQUITY_SCHEMA),
            fills=pl.DataFrame(self._fill_rows, schema=_FILLS_SCHEMA),
            funding=pl.DataFrame(self._funding_rows, schema=_FUNDING_SCHEMA),
            orders=pl.DataFrame(orders, schema=_ORDERS_SCHEMA),
            risk_events=pl.DataFrame(self._risk_rows, schema=_RISK_SCHEMA),
        )
