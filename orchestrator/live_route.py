"""LIVE-маршрут оркестратора: `LiveBarrierSink` связывает сигналы стратегий с LiveBarrier (Этап 4, О20).

Что делает приёмник для одного сигнала (порядок важен, он выведен из R6 и R7):
  1. `submit()` (синхронно, вызывается Оркестратором): проверки (Safe Mode, монета свободна, свежий эквити), объём из 1% риска
     (`RiskSizer`, потолок плеча О8/О12), округление вниз до шага лота, минимальные объём и сумма ордера. Отказ - значение
     `Submission(False, причина)`: Оркестратор запишет `skipped_risk` и снимет блокировку. Принят - в журнал уходит
     `live_intent` с геометрией и расчётом (замена снапшота БД до Этапа 5, R7), сделка пишется на диск, задание - в очередь.
  2. `step()` (асинхронно): `arm_protection` -> вход (LIMIT post-only GTX с сроком жизни 10 баров, либо MARKET) БЕЗ await между
     ними (тикет входа известен заранее). Намерение защиты уже на диске, поэтому упавший процесс не оставит позицию без стопа:
     `pump()` барьера ставит стоп/тейк по факту исполнения (R6), в том числе частичного (добавкой на недостающий объём).
  3. События барьера (Fill / OrderClosed) ведут конечный автомат сделки: queued -> entry -> open -> ending -> (закрыта).
     Монета освобождается (Оркестратор снимает блокировку D4) только когда позиции нет, ордер входа не жив, а защита
     снята `pump()`: новая сделка по монете не успеет увидеть старую защиту (`arm_protection` на занятой монете - ошибка).
  4. Тики: `markPrice@1s` -> `LiveBarrier.on_mark_price` (безубыток на +1R, D11); цена сделки -> отмена LIMIT, если цена
     дошла до TP раньше входа. Закрытие свечи: отмена LIMIT при закрытии за хаем свипа (`inval`), принудительное закрытие
     позиции через 192 свечи (О13). Те же правила, что у VirtualTracker (R8: LIVE и Shadow ведут себя одинаково).

Отличия от Shadow, которые нельзя скрыть: исполнение настоящее (очередь, проскальзывание, комиссии биржи, частичные
исполнения), MARKET-вход считает R по цене закрытия сигнальной свечи, а стоп остаётся на рассчитанном уровне, поэтому
фактический риск MARKET-сделки отличается от 1% на величину проскальзывания.

Fail-Fast (R1): ошибки не подавляются. Safe Mode при приёме сигнала - обычный отказ (`safe_mode`), при отправке уже принятого -
сделка закрывается без ордера. Состояние сделок хранится в JSON рядом с состоянием барьера: после перезапуска
`recover()` возвращает сделки с живой защитой/позицией и снова берёт блокировку монеты у Оркестратора.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import config
from backtester.contracts import Fill, OrderClosed, OrderIntent, OrderType, Side
from execution.exchange_filters import round_qty
from execution.live_barrier import LiveBarrier
from execution.state import SafeMode
from features import defaults as D
from orchestrator.journal import EventLog
from orchestrator.risk import RiskSizer, SizingRejected
from orchestrator.tracker import Submission
from orchestrator.types import Direction, Event, Kind, Signal, Tick
from utils.errors import ConfigError, DataError, SafeModeActive
from utils.storage import atomic_write_text

log = logging.getLogger("orchestrator.live")

QUEUED, ENTRY, OPEN, ENDING = "queued", "entry", "open", "ending"
_EPS = 1e-9
_EXIT_ROLE_REASON = {"sl": "sl", "be_sl": "be", "tp": "tp"}


@dataclass
class LiveTrade:
    """Сделка LIVE-маршрута. trade_id = signal_id (по нему Оркестратор держит блокировку монеты)."""

    trade_id: str
    strategy: str
    symbol: str
    direction: Direction
    kind: Kind
    bar_open_time: int
    entry: float  # цена LIMIT на шаге цены; у MARKET - справочная (закрытие сигнальной свечи)
    sl: float
    tp: float | None
    inval: float | None
    qty: float
    risk_amount: float  # qty * |entry - sl|: потеря при сработавшем SL без комиссий; знаменатель R
    created_ms: int
    status: str = QUEUED
    ticket_id: int | None = None
    bars_closed: int = 0
    fill_bar_no: int | None = None
    filled_qty: float = 0.0
    cash: float = 0.0  # сумма -side * qty * price по всем исполнениям сделки (USDT, без комиссий)
    fees: float = 0.0
    cancel_reason: str | None = None  # почему отменяется неисполненный вход (invalidated | tp_before_fill | ...)
    exit_hint: str | None = None  # почему позицию закрывает сам бот (timeout)
    be_logged: bool = False

    @property
    def sgn(self) -> float:
        return self.direction.sgn  # +1 шорт, -1 лонг: цены * sgn дают «шортовый мир»

    def to_record(self) -> dict[str, Any]:
        d = asdict(self)
        d["direction"], d["kind"] = self.direction.value, self.kind.value
        return d

    @classmethod
    def from_record(cls, d: dict[str, Any]) -> "LiveTrade":
        try:
            return cls(**{**d, "direction": Direction(d["direction"]), "kind": Kind(d["kind"])})
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Повреждённая запись LIVE-сделки: {exc}; {str(d)[:200]}") from exc


class LiveTradeStore:
    """Файл состояния сделок приёмника (атомарная запись). Нет файла - пусто; битый - DataError (R1)."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self) -> list[LiveTrade]:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        try:
            data = json.loads(raw)
            if data["version"] != 1:
                raise ValueError(f"версия {data['version']!r}")
            return [LiveTrade.from_record(r) for r in data["trades"]]
        except DataError:
            raise
        except (ValueError, KeyError, TypeError) as exc:
            raise DataError(f"Файл сделок {self.path} повреждён ({exc}); начало: {raw[:200]!r}") from exc

    def save(self, trades: Iterable[LiveTrade]) -> None:
        payload = {"version": 1, "trades": [t.to_record() for t in trades]}
        atomic_write_text(self.path, json.dumps(payload, ensure_ascii=False, indent=1) + "\n")


class LiveBarrierSink:
    def __init__(
        self,
        barrier: LiveBarrier,
        safe: SafeMode,
        journal: EventLog,
        store: LiveTradeStore,
        equity_source: Callable[[], Awaitable[float]],
        now_ms: Callable[[], int],
        *,
        interval_ms: int,
        sizer: RiskSizer | None = None,
        symbols: Iterable[str] | None = None,
        equity_refresh_s: float = 30.0,
        equity_max_age_s: float = 120.0,
        max_hold_bars: int = 192,
        limit_ttl_bars: int = D.LIMIT_TTL_BARS,
        tp_r: float = D.TP_R,
        be_r: float = D.BE_TRIGGER_R,
        loop_interval_s: float = 1.0,
    ):
        if not (interval_ms > 0 and max_hold_bars >= 1 and limit_ttl_bars >= 1 and tp_r > 0 and be_r > 0 and loop_interval_s > 0):
            raise ConfigError("LiveBarrierSink: interval_ms > 0, сроки >= 1, tp_r > 0, be_r > 0, loop_interval_s > 0")
        self._symbols = tuple(symbols if symbols is not None else config.SYMBOLS)
        for table in ("LIVE_PRICE_TICK", "LIVE_QTY_STEP", "LIVE_MIN_QTY", "LIVE_MIN_NOTIONAL"):
            missing = [s for s in self._symbols if s not in getattr(config, table)]
            if missing:
                raise ConfigError(f"Нет {missing} в config.{table}: объём и цены LIVE-ордеров нельзя округлить/проверить")
        self._barrier, self._safe, self._journal, self._store = barrier, safe, journal, store
        self._equity_source, self._now = equity_source, now_ms
        self._interval_ms = interval_ms
        self._sizer = sizer or RiskSizer()
        self._equity_refresh_s, self._equity_max_age_ms = equity_refresh_s, int(equity_max_age_s * 1000)
        self._max_hold, self._ttl_bars, self._tp_r, self._be_r = max_hold_bars, limit_ttl_bars, tp_r, be_r
        self._loop_interval = loop_interval_s
        self._equity: float | None = None
        self._equity_at_ms = 0
        self._next_equity_refresh = 0.0
        self._trades: dict[str, LiveTrade] = {}  # монета -> сделка (одна на монету: блокировка D4)
        self._ids: set[str] = set()
        self._jobs: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._wake = asyncio.Event()
        self.lock = asyncio.Lock()  # шаг исполнителя, синхронизация после (пере)подключения потока и сверка друг друга не пересекают
        self._listeners: list[Callable[[LiveTrade], None]] = []
        barrier.add_listener(self._on_barrier_event)

    # ------------------------------------------------------------------------ интерфейс SignalSink

    def add_close_listener(self, listener: Callable[[LiveTrade], None]) -> None:
        self._listeners.append(listener)

    @property
    def trades(self) -> dict[str, LiveTrade]:
        return dict(self._trades)

    def orphans(self) -> list[str]:
        """Монеты, где у барьера есть позиция или защита, а приёмник о сделке не знает (потерянный файл сделок): так стартовать нельзя."""
        return [s for s in self._symbols if s not in self._trades
                and (self._barrier.protection(s) is not None or abs(self._barrier.position(s)) > _EPS)]

    def submit(self, signal: Signal, now_ms: int) -> Submission:
        if signal.symbol not in self._symbols:
            raise ConfigError(f"{signal.signal_id}: монета {signal.symbol} не настроена для LIVE")
        if signal.signal_id in self._ids:
            raise DataError(f"Сигнал {signal.signal_id} уже принят LIVE-приёмником")
        reject = self._precheck(signal)
        if reject is not None:
            return self._rejected(signal, reject)
        equity = self._fresh_equity()
        if equity is None:
            return self._rejected(signal, "equity_unavailable")
        entry = self._barrier.round_price(signal.symbol, signal.entry) if signal.kind is Kind.LIMIT else signal.entry
        sizing = self._sizer.size(equity, entry, signal.sl)
        if isinstance(sizing, SizingRejected):
            return self._rejected(signal, sizing.reason)
        qty = round_qty(config.LIVE_QTY_STEP[signal.symbol], sizing.qty)  # вниз: риск никогда не больше 1%
        if qty < config.LIVE_MIN_QTY[signal.symbol] - _EPS or qty * entry < config.LIVE_MIN_NOTIONAL[signal.symbol]:
            return self._rejected(signal, "below_min_size")
        trade = LiveTrade(
            trade_id=signal.signal_id, strategy=signal.strategy, symbol=signal.symbol, direction=signal.direction, kind=signal.kind,
            bar_open_time=signal.bar_open_time, entry=entry, sl=signal.sl, tp=signal.tp, inval=signal.inval, qty=qty,
            risk_amount=qty * abs(entry - signal.sl), created_ms=now_ms,
        )
        # R7: причина входа записана ДО любого ордера
        self._emit(now_ms, "live_intent", trade, {
            "kind": signal.kind.value, "entry": entry, "signal_entry": signal.entry, "sl": signal.sl, "tp": signal.tp, "inval": signal.inval,
            "qty": qty, "equity": equity, "sizing_qty": sizing.qty, "exposure": qty * entry / equity, "risk_amount": trade.risk_amount,
            "geometry": list(signal.geometry), "meta": signal.meta,
        })
        self._trades[signal.symbol] = trade
        self._ids.add(trade.trade_id)
        self._save()
        self._jobs.put_nowait(("start", trade.trade_id))
        self._wake.set()
        return Submission(True)

    def on_bar_closed(self, symbol: str, bar_open_time: int, close: float, now_ms: int, bar: Any = None) -> None:
        t = self._trades.get(symbol)
        if t is None:
            return
        if bar_open_time <= t.bar_open_time:
            raise DataError(f"{t.trade_id}: закрытие свечи {bar_open_time} не позже сигнальной {t.bar_open_time}")
        t.bars_closed += 1
        if t.kind is Kind.LIMIT and t.status != ENDING and t.inval is not None and t.sgn * close > t.sgn * t.inval:
            self._request_cancel(t, "invalidated")  # закрытие за хаем свипа: сетап мёртв, остаток лимитки не нужен
        if t.status == OPEN and t.fill_bar_no is not None and t.bars_closed - t.fill_bar_no >= self._max_hold and t.exit_hint is None:
            t.exit_hint = "timeout"
            self._jobs.put_nowait(("close", t.trade_id))
            self._wake.set()

    def on_tick(self, tick: Tick) -> None:
        if tick.kind == "mark":
            self._barrier.on_mark_price(tick.symbol, tick.price)  # безубыток на +1R (D11); без защиты или позиции - пусто
            return
        t = self._trades.get(tick.symbol)
        if (t is not None and t.kind is Kind.LIMIT and t.filled_qty == 0 and t.status in (QUEUED, ENTRY)
                and t.tp is not None and t.sgn * tick.price <= t.sgn * t.tp):
            self._request_cancel(t, "tp_before_fill")

    # ------------------------------------------------------------------------------ события барьера

    def _on_barrier_event(self, event: Any) -> None:
        if isinstance(event, Fill):
            self._on_fill(event)
        elif isinstance(event, OrderClosed):
            self._on_order_closed(event)

    def _on_fill(self, f: Fill) -> None:
        t = self._trades.get(f.symbol)
        if t is None:
            return
        t.cash -= f.side.sign * f.qty * f.price
        t.fees += f.fee
        if f.ticket_id == t.ticket_id:  # исполнение входа
            t.filled_qty += f.qty
            if t.status in (QUEUED, ENTRY):
                t.status = OPEN
            if t.fill_bar_no is None:
                t.fill_bar_no = t.bars_closed + 1
            self._emit(f.ts, "live_entry_fill", t, {"price": f.price, "qty": f.qty, "filled_qty": t.filled_qty,
                                                    "maker": f.liquidity.value == "maker", "fee": f.fee})
        elif t.filled_qty > 0:
            self._emit(f.ts, "live_exit_fill", t, {"price": f.price, "qty": f.qty, "side": f.side.value, "fee": f.fee})
        if t.filled_qty > 0 and abs(self._barrier.position(f.symbol)) <= _EPS:
            t.status = ENDING  # позиция списана: ждём, пока pump снимет защиту
        self._save()

    def _on_order_closed(self, e: OrderClosed) -> None:
        t = next((x for x in self._trades.values() if x.ticket_id == e.ticket_id), None)
        if t is None:
            return  # закрывающий ордер барьера или чужой тикет
        self._emit(e.ts, "live_entry_closed", t, {"remaining": e.remaining_qty, "reason": e.reason.value, "filled_qty": t.filled_qty})
        if t.filled_qty == 0:
            t.cancel_reason = t.cancel_reason or e.reason.value  # expired (TTL / не встал post-only) | rejected | no_liquidity
            t.status = ENDING
            self._save()

    # ----------------------------------------------------------------------------------- исполнитель

    async def run(self) -> None:
        """Цикл исполнителя: задания Оркестратора сразу, обслуживание барьера (pump, сроки, дренаж событий) не реже loop_interval_s."""
        await self.refresh_equity()
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._loop_interval)
            except TimeoutError:
                pass
            self._wake.clear()
            await self.step()

    async def step(self) -> None:
        """Один проход (вынесен для тестов): все накопленные задания, затем обслуживание."""
        async with self.lock:
            while not self._jobs.empty():
                action, trade_id = self._jobs.get_nowait()
                t = next((x for x in self._trades.values() if x.trade_id == trade_id), None)
                if t is None:
                    continue  # сделка уже завершена (отмена догнала закрытие)
                if action == "start":
                    await self._start(t)
                elif action == "cancel":
                    if t.ticket_id is not None and t.ticket_id in self._barrier.open_ticket_ids():
                        await self._barrier.cancel(t.ticket_id)
                    elif t.status == QUEUED:  # отмена обогнала отправку входа
                        self._end_without_order(t)
                elif action == "close":
                    await self._close_position(t)
            await self._maintain()

    async def _maintain(self) -> None:
        now = max(self._now(), self._barrier.now_ts)
        self._barrier.advance(now)  # дренаж событий барьера и проверка расхождения позиции с биржей (Safe Mode)
        await self._barrier.pump()
        await self._barrier.expire_due(now)
        await self._barrier.resync_pending()
        self._poll(now)
        loop_now = asyncio.get_running_loop().time()
        if loop_now >= self._next_equity_refresh:
            await self.refresh_equity()

    async def refresh_equity(self) -> None:
        self._equity = await self._equity_source()
        self._equity_at_ms = self._now()
        self._next_equity_refresh = asyncio.get_running_loop().time() + self._equity_refresh_s

    def _fresh_equity(self) -> float | None:
        if self._equity is None or self._now() - self._equity_at_ms > self._equity_max_age_ms or not self._equity > 0:
            return None
        return self._equity

    async def _start(self, t: LiveTrade) -> None:
        if t.cancel_reason is not None or self._safe.active:
            t.cancel_reason = t.cancel_reason or "safe_mode"
            self._end_without_order(t)
            return
        sgn = 1 if t.direction is Direction.LONG else -1
        ticket_id = self._barrier.next_ticket_id  # между этой строкой и submit() нет await: номер тикета совпадёт
        t.ticket_id = ticket_id
        tp = {"tp_price": t.tp} if t.kind is Kind.LIMIT else {"tp_r": self._tp_r}
        self._barrier.arm_protection(t.symbol, direction=sgn, entry_price=t.entry, sl_price=t.sl, ticket_id=ticket_id, be_r=self._be_r, **tp)
        t.status = ENTRY
        self._save()
        side = Side.BUY if sgn > 0 else Side.SELL
        asof = t.bar_open_time + self._interval_ms  # решение принято на закрытии сигнальной свечи
        if t.kind is Kind.LIMIT:
            intent = OrderIntent(t.symbol, side, t.qty, asof, OrderType.LIMIT, t.entry, self._ttl_bars * self._interval_ms)
        else:
            intent = OrderIntent(t.symbol, side, t.qty, asof)
        try:
            await self._barrier.submit(intent)
        except SafeModeActive:
            t.cancel_reason = "safe_mode"  # Safe Mode включился между приёмом и отправкой: ордер не ушёл
            t.status = ENDING
            self._save()
            return
        self._emit(self._now(), "live_entry_sent", t, {"ticket_id": ticket_id, "type": t.kind.value, "qty": t.qty, "price": t.entry})

    async def _close_position(self, t: LiveTrade) -> None:
        self._emit(self._now(), "live_force_close", t, {"reason": t.exit_hint})
        if t.ticket_id is not None and t.ticket_id in self._barrier.open_ticket_ids():
            await self._barrier.cancel(t.ticket_id)  # остаток входа не должен открыть позицию заново
        await self._barrier.close_position(t.symbol, self._now())

    def _request_cancel(self, t: LiveTrade, reason: str) -> None:
        if t.cancel_reason is not None:
            return
        t.cancel_reason = reason
        self._emit(self._now(), "live_cancel_requested", t, {"reason": reason})
        self._jobs.put_nowait(("cancel", t.trade_id))
        self._wake.set()

    def _end_without_order(self, t: LiveTrade) -> None:
        t.status = ENDING
        self._save()

    # --------------------------------------------------------------------------------- завершение

    def _poll(self, now_ms: int) -> None:
        for t in list(self._trades.values()):
            prot = self._barrier.protection(t.symbol)
            if prot is not None and prot.be_done and not t.be_logged and t.filled_qty > 0:
                t.be_logged = True
                self._emit(now_ms, "be_armed", t, {"be_failed": prot.be_failed, "entry_price": prot.entry_price})
            if t.status == ENDING and prot is None and (t.ticket_id is None or t.ticket_id not in self._barrier.open_ticket_ids()):
                self._finish(t, now_ms)

    def _finish(self, t: LiveTrade, now_ms: int) -> None:
        role = self._barrier.pop_exit_role(t.symbol)
        if t.filled_qty > 0:
            reason = (_EXIT_ROLE_REASON.get(role or "") or t.exit_hint or ("safe_mode_flatten" if self._safe.active else "market_close"))
            pnl = t.cash - t.fees
            self._emit(now_ms, "live_trade_closed", t, {"reason": reason, "pnl_usdt": pnl, "net_r": pnl / t.risk_amount,
                                                        "filled_qty": t.filled_qty, "fees": t.fees, "bars_held": t.bars_closed})
        else:
            self._emit(now_ms, "live_trade_cancelled", t, {"reason": t.cancel_reason or "unfilled"})
        del self._trades[t.symbol]
        self._save()
        for listener in self._listeners:
            listener(t)

    # -------------------------------------------------------------------------------- перезапуск

    def recover(self, orchestrator: Any) -> list[str]:
        """После перезапуска: сделки, чья позиция/защита/ордер входа ещё живы, возвращаются в работу и снова берут блокировку
        монеты у Оркестратора; остальные закрылись, пока процесса не было (в журнал: *_offline). -> trade_id живых."""
        alive: list[str] = []
        for t in self._store.load():
            live = (self._barrier.protection(t.symbol) is not None or abs(self._barrier.position(t.symbol)) > _EPS
                    or (t.ticket_id is not None and t.ticket_id in self._barrier.open_ticket_ids()))
            if live:
                self._trades[t.symbol] = t
                self._ids.add(t.trade_id)
                orchestrator.adopt_held(t.strategy, t.symbol, t.trade_id)
                alive.append(t.trade_id)
                self._emit(self._now(), "live_trade_recovered", t, {"status": t.status, "filled_qty": t.filled_qty})
            else:
                self._emit(self._now(), "live_trade_closed_offline" if t.filled_qty > 0 else "live_trade_cancelled_offline", t,
                           {"status": t.status, "filled_qty": t.filled_qty})
        self._save()
        return alive

    # --------------------------------------------------------------------------------- внутреннее

    def _precheck(self, signal: Signal) -> str | None:
        if self._safe.active:
            return "safe_mode"
        sym = signal.symbol
        if sym in self._trades or self._barrier.protection(sym) is not None or abs(self._barrier.position(sym)) > _EPS:
            return "symbol_busy"  # страховка: Оркестратор такой сигнал отсекает блокировкой монеты раньше
        return None

    def _rejected(self, signal: Signal, reason: str) -> Submission:
        log.info("LIVE %s %s: отказ приёма сигнала: %s", signal.symbol, signal.signal_id, reason)
        return Submission(False, reason)

    def _emit(self, ts: int, kind: str, t: LiveTrade, detail: dict[str, Any]) -> None:
        self._journal.emit(Event(ts, kind, t.symbol, t.strategy, t.trade_id, detail))

    def _save(self) -> None:
        self._store.save(self._trades.values())
