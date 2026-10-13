"""Live Execution Engine: исполнитель боевых ордеров за Live Risk Manager (ВЫКЛЮЧЕН: TRADE_MODE=shadow по умолчанию).

```
 Orchestrator ─► Route(SHADOW) ─► LiveMirrorSink.submit
                                     ├─► VirtualTracker.submit        ВСЕГДА, как раньше (Shadow-статистика не зависит от live)
                                     └─► (принят трекером И стратегия ∈ APPROVED_STRATEGIES) ─► LiveExecutionEngine.enqueue
 execute(signal): Safe Mode? ─► суточный PnL ─► баланс ─► LiveRiskManager.evaluate ─► отклонение цены (MARKET) ─► состояние на диск
                  ─► журнал `live_intent` + flush БД (R7) ─► ISOLATED + плечо ─► вход MARKET | LIMIT
 после исполнения входа (_protect): SL на весь объём + по TAKE_PROFIT_MARKET на каждый уровень Scale Out плана стратегии
 on_tick / on_bar_closed / poll(): сопровождение позиции ТЕМ ЖЕ планом, что у VirtualTracker (разд. 3.16 CLAUDE.md):
     +1R по mark price (D11) или исполнение первого уровня -> стоп на вход (POST нового, затем DELETE старого);
     уровни плана (50% на +1R, 40% на +3R) - биржевые TAKE_PROFIT_MARKET; остаток-runner - трейлинг по закрытой свече
     (ATR или тени), стоп только подтягивается; тайм-аут позиции 192 свечи - закрытие по рынку
```

Принципы.
  * Выход ИДЕНТИЧЕН Shadow: те же уровни/доли/безубыток/трейлинг из `ScaleOutPlan` стратегии (`plans_for`), та же математика
    (уровни от ФАКТИЧЕСКОЙ цены входа и R = |вход − SL|, лучшая цена по тикам сделок, BE по mark price, стоп только к прибыли).
    Различия физические, не логические: цены уровней округляются до шага цены; доли - до шага лота (план, который на сетке
    лота воспроизвести нельзя, получает отказ `plan_not_representable`); срабатывание уровней и стопа делает биржа, а не тик.
    Тест паритета (`tests/orchestrator/test_live_parity.py`) прогоняет случайные траектории цены через VirtualTracker и
    через движок на имитации биржи и сравнивает каждое закрытие (причина, доля, цена).
  * Состояние переживает перезапуск: файл `live_engine_state.json` (атомарная запись, write-ahead: сделка и её id ордеров
    записываются ДО отправки) + сверка с биржей при старте (`recover`): позиции, ордера входа, algo-ордера. Позиция без
    записи, но со стопом на бирже, подтягивается (`live_adopted`); без стопа - Safe Mode (чужую позицию не трогаем).
  * Fail-Fast (R1): ошибка биржи, которую нельзя трактовать однозначно, роняет процесс (`LiveExecutionError`).
  * Позиция без стопа недопустима (R6): не удалось поставить/заменить защиту -> Safe Mode, reduce-only закрытие, падение.
    «Стоп уже за ценой» при переносе (-2021) - обычный исход: выход по рынку без Safe Mode (как BE-выход в Shadow).
  * Идемпотентность: client id детерминированы от `signal_id` (роль + SHA-1); неизвестный исход запроса выясняется по `GET`.

Ограничения (честно): ❓ гипотеза О18 (биржа принимает SL на весь объём и TP-уровни одновременно, сумма reduceOnly > позиции)
не проверена на живой бирже - её проверяет `scripts/test_live_order.py` на testnet; best price для трейлинга и mark price для
безубытка берутся из тех же тиков, что и у Shadow (`on_tick`), поэтому на testnet с данными mainnet они неприменимы.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

import config
from exchange.binance_live import BinanceLiveClient, OrderResult, client_id
from exchange.router import ExecutionRouter, Mt5Prepared, Mt5UnprotectedPosition, RouteRejected, Venue, build_router
from execution.rest import ERR_WOULD_TRIGGER, BinanceAPIError
from execution.state import SafeMode, SafeModeActive
from features import defaults as D
from orchestrator.journal import EventLog
from orchestrator.live_risk import LiveOrderPlan, LiveRejected, LiveRiskManager, split_levels, utc_day_start_ms
from orchestrator.tracker import DEFAULT_PLAN, ScaleOutPlan, Submission
from orchestrator.types import BarInfo, Direction, Event, Kind, Signal, Tick
from utils.errors import ConfigError, DataError, HttpServerError, NetworkError, TradingInfraError
from utils.storage import atomic_write_text

log = logging.getLogger("live_engine")

_EPS = 1e-9
ENTRY, OPEN, CLOSED, CANCELLED = "entry", "open", "closed", "cancelled"
AUTH_ERROR_CODES = frozenset({-1022, -2014, -2015, -1002})  # подпись / ключ / права / IP: повторять и пропускать сигналы бессмысленно
ERR_UNKNOWN_ORDER = (-2011, -2013)  # ордера нет / уже не активен
UNKNOWN_OUTCOME = (NetworkError, HttpServerError)  # запрос мог дойти до биржи
TRIGGERED = ("TRIGGERING", "TRIGGERED", "FINISHED")  # algo-ордер сработал (порождённый ордер исполняется/исполнен)
STATE_VERSION = 1


class LiveExecutionError(TradingInfraError):
    """Боевой контур остановлен: состояние биржи и бота нельзя считать согласованным (Safe Mode включён)."""


# ------------------------------------------------------------------------------------------ состояние


@dataclass
class Level:
    """Уровень Scale Out: биржевой TAKE_PROFIT_MARKET на `qty` по цене `price` (r - уровень в R, для имён закрытий)."""

    r: float
    qty: float
    price: float
    cid: str
    done: bool = False


@dataclass
class LiveTrade:
    plan: LiveOrderPlan
    created_ms: int
    status: str = ENTRY
    entry_cid: str = ""
    entry_sent: bool = False  # с этого момента ордер входа может существовать на бирже (записывается ДО отправки)
    filled_qty: float = 0.0
    fill_price: float = 0.0
    risk: float = 0.0  # R = |фактический вход − SL| в единицах цены
    remaining: float = 0.0  # ожидаемый объём позиции
    protected: bool = False  # защитные ордера выставлены
    stop_price: float | None = None  # действующий стоп: SL -> безубыток -> трейлинг (только к прибыли)
    stop_kind: str = "sl"  # sl | be | trail - причина выхода, если стоп сработает
    stop_cid: str | None = None
    stop_qty: float = 0.0
    stop_seq: int = 0
    stale_stops: list[str] = field(default_factory=list)  # заменённые стопы, ещё не подтверждённо снятые
    levels: list[Level] = field(default_factory=list)
    levels_done: int = 0
    be_armed: bool = False
    be_pending: bool = False  # безубыток запрошен (mark price +1R / первый уровень), стоп ещё не перенесён
    trail_target: float | None = None  # новая цена стопа runner'а, рассчитанная по закрытой свече
    timeout_pending: bool = False
    best_q: float | None = None  # лучшая цена с момента входа в «шортовом мире» (sgn * цена): ниже - лучше
    bars_closed: int = 0
    fill_bar_no: int | None = None
    recent_bars: list[list[float]] = field(default_factory=list)  # [high, low] последних закрытых свечей (трейлинг по теням)
    cancel_reason: str | None = None  # запрошена отмена лимитки: invalidated | tp_before_fill | ttl | risk_blocked
    adopted: bool = False

    @property
    def trade_id(self) -> str:
        return self.plan.signal_id

    @property
    def sgn(self) -> float:
        return self.plan.direction.sgn

    @property
    def runner_active(self) -> bool:
        p = self.plan.exit_plan
        return self.status == OPEN and p.runner_frac > _EPS and bool(self.levels) and self.levels_done == len(self.levels)

    def to_record(self) -> dict[str, Any]:
        p = self.plan
        plan = {k: getattr(p, k) for k in ("signal_id", "strategy", "symbol", "side", "qty", "entry", "sl", "tp", "inval", "leverage",
                                          "margin_usdt", "notional_usdt", "max_loss_usdt")}
        plan.update(direction=p.direction.value, kind=p.kind.value, exit_plan=p.exit_plan.describe())
        out = {k: v for k, v in self.__dict__.items() if k not in ("plan", "levels")}
        out["plan"], out["levels"] = plan, [lvl.__dict__.copy() for lvl in self.levels]
        return out

    @classmethod
    def from_record(cls, rec: Mapping[str, Any]) -> "LiveTrade":
        try:
            pr = dict(rec["plan"])
            ep = pr.pop("exit_plan")
            exit_plan = ScaleOutPlan(tuple((float(r), float(f)) for r, f in ep["levels"]), bool(ep["be_after_first"]), ep["trail_mode"],
                                     float(ep["trail_atr_mult"]), int(ep["trail_wick_bars"]))
            plan = LiveOrderPlan(direction=Direction(pr.pop("direction")), kind=Kind(pr.pop("kind")), exit_plan=exit_plan, **pr)
            data = {k: v for k, v in rec.items() if k not in ("plan", "levels")}
            return cls(plan=plan, levels=[Level(**lvl) for lvl in rec["levels"]], **data)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Запись боевой сделки не читается ({exc!r}): {str(rec)[:300]}") from exc


class LiveStateStore:
    """Файл состояния боевых сделок (атомарная запись). Нет файла - пусто; битый - DataError (R1)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._last = ""

    def load(self) -> list[dict[str, Any]]:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        try:
            data = json.loads(raw)
            if data["version"] != STATE_VERSION:
                raise ValueError(f"версия {data['version']!r}")
            self._last = raw
            return list(data["trades"])
        except (ValueError, KeyError, TypeError) as exc:
            raise DataError(f"Файл состояния боевого контура {self.path} повреждён ({exc}); начало: {raw[:200]!r}") from exc

    def save(self, trades: Collection[LiveTrade]) -> None:
        text = json.dumps({"version": STATE_VERSION, "trades": [t.to_record() for t in trades]}, ensure_ascii=False, indent=1) + "\n"
        if text != self._last:  # не пишем на диск, если ничего не изменилось
            atomic_write_text(self.path, text)
            self._last = text


# ------------------------------------------------------------------------------------------- движок


class LiveExecutionEngine:
    def __init__(
        self,
        client: BinanceLiveClient,
        risk: LiveRiskManager,
        journal: EventLog,
        safe: SafeMode,
        *,
        now_ms: Callable[[], int],
        interval_ms: int,
        limit_ttl_bars: int = D.LIMIT_TTL_BARS,
        poll_interval_s: float = 1.0,
        pnl_refresh_s: float = 30.0,
        post_only: bool | None = None,
        flush: Callable[[], Awaitable[None]] | None = None,
        store: LiveStateStore | None = None,
        max_hold_bars: int = config.LIVE_MAX_HOLD_BARS,
        be_trigger_r: float = D.BE_TRIGGER_R,
        max_entry_deviation: float | None = None,
        router: ExecutionRouter | None = None,
    ):
        if not (interval_ms > 0 and limit_ttl_bars >= 1 and poll_interval_s > 0 and pnl_refresh_s > 0 and max_hold_bars >= 1 and be_trigger_r > 0):
            raise ConfigError("LiveExecutionEngine: interval_ms > 0, limit_ttl_bars >= 1, poll_interval_s > 0, pnl_refresh_s > 0, "
                              "max_hold_bars >= 1, be_trigger_r > 0")
        self.client, self.risk, self._journal, self._safe = client, risk, journal, safe
        self.router = router  # мульти-биржевая маршрутизация (exchange/router.py); None - прежний однобиржевой контур (только Binance)
        self._now, self._interval_ms, self._ttl_bars = now_ms, interval_ms, limit_ttl_bars
        self._poll_s, self._pnl_refresh_ms = poll_interval_s, int(pnl_refresh_s * 1000)
        self._post_only = config.LIVE_POST_ONLY if post_only is None else post_only
        self._flush, self._store = flush, store
        self._max_hold, self._be_r = max_hold_bars, be_trigger_r
        self._max_dev = config.LIVE_MAX_ENTRY_DEVIATION if max_entry_deviation is None else max_entry_deviation
        self._trades: dict[str, LiveTrade] = {}  # монета -> сделка (одна на монету: LiveRiskManager.busy)
        self._prepared: set[str] = set()  # монеты, где уже выставлены ISOLATED и плечо
        self._queue: asyncio.Queue[tuple[Signal, int]] = asyncio.Queue()
        self._pnl_at_ms = 0
        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------------- вход из Оркестратора

    @property
    def trades(self) -> dict[str, LiveTrade]:
        return dict(self._trades)

    def enqueue(self, signal: Signal, now_ms: int) -> None:
        """Синхронно (из обработчика свечи): сигнал в очередь исполнителя. Сеть здесь запрещена."""
        self._queue.put_nowait((signal, now_ms))

    def on_tick(self, tick: Tick) -> None:
        """Те же тики, что получает VirtualTracker: лучшая цена (трейлинг) - по сделкам, безубыток +1R - по mark price (D11)."""
        t = self._trades.get(tick.symbol)
        if t is None or t.status != OPEN or t.best_q is None:
            return
        q = t.sgn * tick.price
        if tick.kind == "mark":
            if not t.be_armed and not t.be_pending and q <= t.sgn * t.fill_price - self._be_r * t.risk:
                t.be_pending = True
        else:
            t.best_q = min(t.best_q, q)

    def on_bar_closed(self, symbol: str, bar_open_time: int, close: float, now_ms: int, bar: BarInfo | None = None) -> None:
        """Закрытие свечи: отмены лимитки (те же правила, что у VirtualTracker, R8), тайм-аут позиции, трейлинг runner'а."""
        t = self._trades.get(symbol)
        if t is None:
            return
        t.bars_closed += 1
        sgn = t.sgn
        if t.status == ENTRY:
            if t.cancel_reason is None and t.plan.kind is Kind.LIMIT:
                if t.plan.inval is not None and sgn * close > sgn * t.plan.inval:
                    t.cancel_reason = "invalidated"
                elif t.plan.tp is not None and sgn * close <= sgn * t.plan.tp:
                    t.cancel_reason = "tp_before_fill"
        elif t.status == OPEN:
            if t.fill_bar_no is not None and t.bars_closed - t.fill_bar_no >= self._max_hold:
                t.timeout_pending = True
            else:
                self._trail(t, bar)

    def _trail(self, t: LiveTrade, bar: BarInfo | None) -> None:
        """Подтягивание стопа runner'а по закрытой свече (как VirtualTracker._trail); исполняется следующим опросом."""
        plan = t.plan.exit_plan
        if bar is not None and bar.high is not None and bar.low is not None:
            t.recent_bars = (t.recent_bars + [[bar.high, bar.low]])[-plan.trail_wick_bars:]
        if not t.runner_active or t.stop_price is None:
            return
        if bar is None:
            raise LiveExecutionError(f"{t.trade_id}: runner без данных закрытой свечи - трейлинг невозможен")
        sgn = t.sgn
        if plan.trail_mode == "atr":
            if bar.atr is None or not 0 < bar.atr < float("inf"):
                raise LiveExecutionError(f"{t.trade_id}: нет ATR для трейлинга ({bar.atr!r})")
            candidate = t.best_q + plan.trail_atr_mult * bar.atr  # лучшая цена с входа -+ k * ATR (в «шортовом мире»)
        else:
            if not t.recent_bars:
                return
            candidate = max(max(sgn * h, sgn * lo) for h, lo in t.recent_bars)  # худший экстремум за N свечей
        if candidate < sgn * t.stop_price - 1e-12:
            t.trail_target = self.client.round_price(t.plan.symbol, sgn * candidate)  # sgn = ±1: обратный переход к реальной цене

    # ----------------------------------------------------------------------------------- жизненный цикл

    def start(self) -> None:
        if self._task is not None:
            raise ConfigError("LiveExecutionEngine уже запущен")
        self._task = asyncio.create_task(self.run(), name="live_engine")

    def check(self) -> None:
        """Из основного цикла: упавшая задача исполнителя роняет процесс (R1)."""
        if self._task is not None and self._task.done():
            self._task.result()  # исключение задачи (или CancelledError) поднимается здесь
            raise LiveExecutionError("Задача Live Execution Engine завершилась без ошибки: так не должно быть")

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)  # причина падения уже поднята через check()

    async def run(self) -> None:
        while True:
            await self.step()
            await asyncio.sleep(self._poll_s)

    async def step(self) -> None:
        while not self._queue.empty():
            signal, ts = self._queue.get_nowait()
            await self.execute(signal, ts)
        await self.poll()

    # --------------------------------------------------------------------------------------- исполнение

    async def refresh_pnl(self, now_ms: int) -> None:
        """Суточный результат ВСЕХ площадок (Binance, MT5) -> риск-менеджер (сквозной лимит: Total = Binance + MT5). Лимит достигнут ->
        новых входов нет на обеих площадках, неисполненные входы (Binance и MT5) снимаются, открытые позиции не трогаем."""
        start = utc_day_start_ms(now_ms)
        if self.router is None:
            by_venue = {Venue.BINANCE.value: await self.client.realized_pnl_since(start)}
        else:
            by_venue = await self.router.realized_pnl(start)
        self._pnl_at_ms = now_ms
        tripped = False
        for venue, pnl in by_venue.items():
            tripped = self.risk.update_venue_pnl(venue, pnl, now_ms) or tripped
        if tripped:
            self._emit(now_ms, "live_daily_limit", "", "", None, {"pnl_usdt": self.risk.daily_pnl, "pnl_by_venue": self.risk.venue_pnl,
                                                                  "limit_usdt": self.risk.daily_loss_limit, "blocked_until_ms": self.risk.blocked_until_ms})
        if self.risk.blocked(now_ms):
            for t in self._trades.values():
                if t.status == ENTRY and t.cancel_reason is None:
                    t.cancel_reason = "risk_blocked"
            if self.router is not None:
                for mt in await self.router.cancel_pending_mt5():
                    self.risk.release(mt.symbol, mt.signal_id)
                    self._emit(now_ms, "live_trade_cancelled", mt.symbol, mt.strategy, mt.signal_id, {"venue": "mt5", "reason": "risk_blocked"})

    async def execute(self, signal: Signal, now_ms: int) -> None:
        if self._safe.active:
            self._emit_signal(now_ms, "live_rejected", signal, {"reason": "safe_mode", "detail": self._safe.reason})
            return
        if self.router is not None:
            rejected = self.router.reject_unsupported(signal.symbol)
            if rejected is not None:
                self._emit_signal(now_ms, "live_rejected", signal, {**(rejected.detail or {}), "reason": rejected.reason})
                return
            if self.router.venue_of(signal.symbol) is Venue.MT5:
                await self._execute_mt5(signal, now_ms)
                return
        await self.refresh_pnl(now_ms)
        balance = await self.client.get_balance()
        if balance.available < self.risk.margin_usdt:
            self._emit_signal(now_ms, "live_rejected", signal, {"reason": "insufficient_balance", "available": balance.available,
                                                                "margin": self.risk.margin_usdt})
            return
        plan = self.risk.evaluate(signal, now_ms)
        if isinstance(plan, LiveRejected):
            self._emit_signal(now_ms, "live_rejected", signal, {"reason": plan.reason, **(plan.detail or {})})
            return
        trade = LiveTrade(plan, now_ms, entry_cid=client_id(self.client.id_prefix, "E", plan.signal_id))
        self._trades[plan.symbol] = trade
        res: OrderResult | None = None
        ok = False
        try:
            if plan.kind is Kind.MARKET and not await self._price_ok(signal, now_ms):
                return  # finally снимет резерв: ордера не было
            self._save()  # write-ahead: сделка и её client id на диске ДО любого запроса на ордер
            self._emit(now_ms, "live_intent", plan.symbol, plan.strategy, plan.signal_id, {  # R7: до первого запроса на ордер
                "side": plan.side, "kind": plan.kind.value, "qty": plan.qty, "entry": plan.entry, "sl": plan.sl, "tp": plan.tp,
                "leverage": plan.leverage, "margin_usdt": plan.margin_usdt, "notional_usdt": plan.notional_usdt,
                "max_loss_usdt": plan.max_loss_usdt, "daily_pnl_usdt": self.risk.daily_pnl, "balance_available": balance.available,
                "exit_plan": plan.exit_plan.describe(), "geometry": list(signal.geometry), "meta": signal.meta})
            if self._flush is not None:
                await self._flush()
            await self._prepare_symbol(plan)
            res = await self._send_entry(trade)
            ok = True
        finally:
            if not ok and trade.status == ENTRY and not trade.entry_sent:
                self._finish(trade, CANCELLED, "aborted", now_ms)  # до отправки входа что-то упало/отказано: ордера нет, монета свободна
        if res is not None:
            await self._apply_entry(trade, res)

    async def _execute_mt5(self, signal: Signal, now_ms: int) -> None:
        """Сигнал по MT5-символу: общие ворота риска (одобрение, сквозной суточный лимит, свежий PnL ВСЕХ площадок, монета свободна) ->
        маршрутизатор считает лоты и проверяет рынок -> намерение в журнал (R7) -> ордер со стопом. Отказы (в т.ч. `market_closed`) - значения:
        Safe Mode не включается, процесс не падает; неизвестный исход отправки - исключение (R1)."""
        assert self.router is not None
        await self.refresh_pnl(now_ms)
        gate = self.risk.gate(signal, now_ms)
        if gate is not None:
            self._emit_signal(now_ms, "live_rejected", signal, {**(gate.detail or {}), "reason": gate.reason, "venue": "mt5"})
            return
        self.risk.reserve(signal.symbol, signal.signal_id)
        ok = False
        try:
            prepared = await self.router.prepare_mt5(signal, now_ms)
            if isinstance(prepared, RouteRejected):
                self._emit_signal(now_ms, "live_rejected", signal, {**(prepared.detail or {}), "reason": prepared.reason, "venue": "mt5"})
                return
            self._emit_mt5_intent(signal, prepared, now_ms)
            if self._flush is not None:
                await self._flush()
            sent = await self.router.send_mt5(prepared)
            if isinstance(sent, RouteRejected):
                self._emit_signal(now_ms, "live_rejected", signal, {**(sent.detail or {}), "reason": sent.reason, "venue": "mt5"})
                return
            self._emit(now_ms, "live_entry_placed" if sent.status == "pending" else "live_entry_filled", sent.symbol, sent.strategy, sent.signal_id,
                       {"venue": "mt5", "client_id": sent.client_id, "order_id": sent.order_id, "position_id": sent.position_id, "lots": sent.lots})
            ok = True
        finally:
            if not ok:
                self.risk.release(signal.symbol, signal.signal_id)

    def _emit_mt5_intent(self, signal: Signal, p: Mt5Prepared, now_ms: int) -> None:
        plan = p.plan
        self._emit(now_ms, "live_intent", plan.symbol, signal.strategy, signal.signal_id, {  # R7: до первого запроса на ордер
            "venue": "mt5", "broker_symbol": plan.broker_symbol, "side": plan.side, "kind": p.kind.value, "lots": plan.lots, "entry": p.price,
            "sl": plan.sl, "tp": plan.tp, "risk_amount": plan.risk_amount, "risk_target": plan.risk_target, "loss_per_lot": plan.loss_per_lot,
            "margin_required": plan.margin_required, "equity": plan.equity, "daily_pnl_usdt": self.risk.daily_pnl,
            "pnl_by_venue": self.risk.venue_pnl, "geometry": list(signal.geometry), "meta": signal.meta})

    async def _price_ok(self, signal: Signal, now_ms: int) -> bool:
        """MARKET: mark price биржи не должна уйти от цены сигнала больше допустимого (иначе R и уровни плана уже не те)."""
        mark = await self.client.get_mark_price(signal.symbol)
        dev = abs(mark - signal.entry) / signal.entry
        if dev <= self._max_dev:
            return True
        self._emit_signal(now_ms, "live_rejected", signal, {"reason": "price_deviation", "mark": mark, "signal_entry": signal.entry,
                                                            "deviation": dev, "max": self._max_dev})
        return False

    async def _prepare_symbol(self, plan: LiveOrderPlan) -> None:
        if plan.symbol in self._prepared:
            return
        await self.client.set_margin_type(plan.symbol, "ISOLATED")
        await self.client.set_leverage(plan.symbol, plan.leverage)
        self._prepared.add(plan.symbol)

    async def _send_entry(self, trade: LiveTrade) -> OrderResult | None:
        p = trade.plan
        trade.entry_sent = True
        self._save()  # «ордер мог дойти до биржи» - на диске раньше запроса
        try:
            if p.kind is Kind.MARKET:
                return await self.client.market_order(p.symbol, p.side, p.qty, client_id=trade.entry_cid, ref_price=p.entry)
            return await self.client.limit_order(p.symbol, p.side, p.qty, p.entry, client_id=trade.entry_cid, post_only=self._post_only)
        except UNKNOWN_OUTCOME as exc:
            log.error("%s: исход отправки входа неизвестен (%s), сверяю по clientOrderId", p.signal_id, exc)
            try:
                return await self.client.get_order(p.symbol, trade.entry_cid)
            except BinanceAPIError as lookup:
                if lookup.code not in ERR_UNKNOWN_ORDER:
                    raise
                trade.entry_sent = False
                self._emit_trade(trade, "live_entry_not_placed", {"reason": "network_unknown_then_absent", "error": str(exc)})
                self._finish(trade, CANCELLED, "entry_not_placed", self._now())
                return None
        except BinanceAPIError as exc:
            if exc.code in AUTH_ERROR_CODES:
                self._trip(f"Binance отклонил ключ/подпись ({exc.code}): {exc.msg}", {"signal_id": p.signal_id})
                raise LiveExecutionError(f"Аутентификация: {exc}") from exc
            trade.entry_sent = False
            self._emit_trade(trade, "live_entry_rejected", {"code": exc.code, "msg": exc.msg})  # биржа ответила: ордера нет
            self._finish(trade, CANCELLED, "entry_rejected", self._now())
            return None

    async def _apply_entry(self, trade: LiveTrade, res: OrderResult) -> None:
        now = self._now()
        if res.status == "NEW":
            self._emit_trade(trade, "live_entry_placed", {"status": res.status, "client_order_id": trade.entry_cid})
            return  # ждёт исполнения: poll()
        if res.status == "PARTIALLY_FILLED":
            await self._cancel_entry(trade)  # остаток не нужен: план делится на ИСПОЛНЕННЫЙ объём
            res = await self.client.get_order(trade.plan.symbol, trade.entry_cid)
        if res.executed_qty > _EPS:
            trade.filled_qty, trade.fill_price = res.executed_qty, res.avg_price
            self._emit_trade(trade, "live_entry_filled", {"qty": res.executed_qty, "avg_price": res.avg_price, "status": res.status})
            await self._protect(trade)
        elif res.done:
            self._finish(trade, CANCELLED, f"entry_{res.status.lower()}", now)
        else:
            raise LiveExecutionError(f"{trade.trade_id}: неожиданный статус входа {res.status} при исполненном объёме 0")

    async def _cancel_entry(self, trade: LiveTrade) -> None:
        try:
            await self.client.cancel_order(trade.plan.symbol, trade.entry_cid)
        except BinanceAPIError as exc:
            if exc.code not in ERR_UNKNOWN_ORDER:  # «неизвестен» = уже исполнен/отменён: итог покажет get_order
                raise

    # ------------------------------------------------------------------------------------ защита и план

    async def _protect(self, trade: LiveTrade) -> None:
        """Вход исполнен: считаем уровни плана от ФАКТИЧЕСКОЙ цены (как VirtualTracker), пишем на диск, ставим SL и TP-уровни."""
        p, qty, fill = trade.plan, trade.filled_qty, trade.fill_price
        sgn = trade.sgn
        if not sgn * p.sl > sgn * fill:  # вход исполнен уже за стопом (гэп): стопа «за ценой» не бывает
            await self._emergency(trade, DataError(f"цена входа {fill} уже за стопом {p.sl}"), "fill_beyond_stop")
        split = split_levels(p.exit_plan, qty, self.client.filters(p.symbol))
        if split is None:  # частичное исполнение дало объём, который план не делит: закрываем без Safe Mode
            self._emit_trade(trade, "live_plan_not_representable", {"qty": qty})
            self._start_trade(trade, [])
            await self._close_market(trade, "plan_not_representable")
            return
        risk = abs(fill - p.sl)
        levels: list[Level] = []
        for i, (r, lvl_qty) in enumerate(split[0]):
            price = self.client.round_price(p.symbol, fill - sgn * r * risk)
            if not sgn * price < sgn * fill:
                await self._emergency(trade, DataError(f"уровень {r}R: цена {price} не по ту сторону входа {fill}"), "bad_level")
            levels.append(Level(r, lvl_qty, price, client_id(self.client.id_prefix, f"T{i}", p.signal_id)))
        trade.risk = risk
        self._start_trade(trade, levels)
        await self._place_orders(trade)
        self._emit_trade(trade, "live_protected", {"qty": qty, "fill_price": fill, "sl": p.sl, "levels": [[v.r, v.qty, v.price] for v in levels],
                                                   "exit_plan": p.exit_plan.describe()})

    def _start_trade(self, trade: LiveTrade, levels: list[Level]) -> None:
        p = trade.plan
        trade.status, trade.remaining = OPEN, trade.filled_qty
        trade.stop_price, trade.stop_kind, trade.stop_qty, trade.stop_seq = p.sl, "sl", trade.filled_qty, 0
        trade.stop_cid = client_id(self.client.id_prefix, "S0", trade.trade_id)
        trade.levels, trade.levels_done = levels, 0
        trade.best_q = trade.sgn * trade.fill_price  # mfe = 0 в момент входа
        trade.fill_bar_no = trade.bars_closed + 1  # как VirtualTracker
        self._save()  # write-ahead: id защитных ордеров на диске ДО их выставления

    async def _place_orders(self, trade: LiveTrade, *, check_first: bool = False) -> None:
        """SL на `stop_qty` и TAKE_PROFIT_MARKET на каждый невыполненный уровень (quantity + reduceOnly, О18). check_first - после
        перезапуска: сначала спросить биржу по client id (повтор POST с тем же id отвергается)."""
        p = trade.plan

        async def missing(cid: str) -> bool:
            if not check_first:
                return True
            try:
                await self.client.get_algo_order(cid)
                return False
            except BinanceAPIError as exc:
                if exc.code not in ERR_UNKNOWN_ORDER:
                    raise
                return True

        try:
            if trade.stop_cid is not None and await missing(trade.stop_cid):
                await self.client.stop_loss(p.symbol, p.close_side, trade.stop_price, client_id=trade.stop_cid, qty=trade.stop_qty)
            for lvl in trade.levels:
                if not lvl.done and await missing(lvl.cid):
                    await self.client.take_profit(p.symbol, p.close_side, lvl.price, client_id=lvl.cid, qty=lvl.qty)
        except (BinanceAPIError, *UNKNOWN_OUTCOME) as exc:
            await self._emergency(trade, exc, "protection_failed")
        trade.protected = True
        self._save()

    async def _emergency(self, trade: LiveTrade, cause: BaseException, why: str) -> None:
        """Позиция без подтверждённой защиты: Safe Mode, остаток входа и позиция - в ноль, снять защиту, упасть."""
        p = trade.plan
        self._trip(f"{p.symbol}: {why}: {cause}", {"signal_id": p.signal_id})
        self._emit_trade(trade, "live_emergency", {"why": why, "cause": str(cause)})
        try:
            await self._cancel_entry(trade)  # остаток лимитки не должен исполниться уже после закрытия
        except (BinanceAPIError, *UNKNOWN_OUTCOME) as exc:
            log.critical("%s: остаток входа не отменён (%s); закрываю позицию", p.signal_id, exc)
        pos = await self.client.get_position_amt(p.symbol)
        if abs(pos) > _EPS:
            close_side = "SELL" if pos > 0 else "BUY"
            await self.client.market_order(p.symbol, close_side, self.client.round_qty(p.symbol, abs(pos)), reduce_only=True,
                                           client_id=client_id(self.client.id_prefix, "X", p.signal_id))
        for cid in self._all_cids(trade):  # позиция уже закрыта: чистка вторична, Safe Mode включён
            try:
                await self._cancel_algo(cid)
            except (BinanceAPIError, LiveExecutionError) as exc:
                log.critical("%s: защитный ордер %s не снят (%s): проверьте openAlgoOrders вручную", p.signal_id, cid, exc)
        self._finish(trade, CLOSED, f"emergency_{why}", self._now())
        raise LiveExecutionError(f"{p.symbol}: {why}: {cause}") from (cause if isinstance(cause, Exception) else None)

    @staticmethod
    def _all_cids(trade: LiveTrade) -> list[str]:
        cids = [trade.stop_cid, *(lvl.cid for lvl in trade.levels if not lvl.done), *trade.stale_stops]
        return [c for c in dict.fromkeys(cids) if c is not None]

    async def _cancel_algo(self, cid: str) -> None:
        """Снять algo-ордер. Отказ допустим, только если ордер уже терминален или биржа его не знает (проверяем запросом)."""
        try:
            await self.client.cancel_algo_order(cid)
        except BinanceAPIError as exc:
            try:
                state = await self.client.get_algo_order(cid)
            except BinanceAPIError as lookup:
                if lookup.code in ERR_UNKNOWN_ORDER:
                    return  # биржа такого ордера не знает (не выставлялся или давно удалён)
                raise
            if not state.terminal:
                raise LiveExecutionError(f"algo-ордер {cid} не снимается ({exc.code}: {exc.msg}), статус {state.status}") from exc

    # ---------------------------------------------------------------------------------------- опрос

    async def poll(self) -> None:
        now = self._now()
        if now - self._pnl_at_ms >= self._pnl_refresh_ms:
            await self.refresh_pnl(now)
        for trade in list(self._trades.values()):
            if trade.status == ENTRY and trade.entry_sent:
                await self._poll_entry(trade, now)
            elif trade.status == OPEN:
                await self._manage(trade, now)
        if self.router is not None:
            for ev in await self.router.poll_mt5():
                mt = ev.trade
                kind = {"opened": "live_entry_filled", "closed": "live_closed", "cancelled": "live_trade_cancelled"}[ev.kind]
                self._emit(now, kind, mt.symbol, mt.strategy, mt.signal_id, {"venue": "mt5", **ev.detail})
                if ev.kind in ("closed", "cancelled"):
                    self.risk.release(mt.symbol, mt.signal_id)
        self._save()

    async def _poll_entry(self, trade: LiveTrade, now: int) -> None:
        if trade.cancel_reason is None and now - trade.created_ms >= self._ttl_bars * self._interval_ms:
            trade.cancel_reason = "ttl"
        try:
            res = await self.client.get_order(trade.plan.symbol, trade.entry_cid)
        except BinanceAPIError as exc:
            if exc.code not in ERR_UNKNOWN_ORDER:
                raise
            self._finish(trade, CANCELLED, "entry_not_found", now)  # записан как отправленный, биржа его не знает (после рестарта)
            return
        if res.executed_qty <= _EPS and trade.cancel_reason is not None and not res.done:
            await self._cancel_entry(trade)
            res = await self.client.get_order(trade.plan.symbol, trade.entry_cid)
            if res.executed_qty <= _EPS and not res.done:
                raise LiveExecutionError(f"{trade.trade_id}: ордер входа не отменяется (статус {res.status})")
        if res.executed_qty <= _EPS and res.done:
            self._finish(trade, CANCELLED, trade.cancel_reason or f"entry_{res.status.lower()}", now)
        elif res.executed_qty > _EPS:
            await self._apply_entry(trade, res)

    async def _manage(self, trade: LiveTrade, now: int) -> None:
        """Сопровождение открытой позиции: что сработало на бирже -> безубыток/трейлинг/размер стопа -> тайм-аут."""
        p = trade.plan
        for cid in list(trade.stale_stops):  # заменённые стопы: снять (идемпотентно)
            await self._cancel_algo(cid)
            trade.stale_stops.remove(cid)
        pos = await self.client.get_position_amt(p.symbol)
        amt = abs(pos)
        if amt > _EPS and pos * trade.sgn > 0:  # знак позиции противоположен направлению сделки
            self._trip(f"{p.symbol}: позиция {pos} противоположна сделке {p.direction.value}", {"signal_id": p.signal_id})
            raise LiveExecutionError(f"{p.symbol}: позиция {pos} не соответствует сделке {trade.trade_id}")
        if amt < trade.remaining - _EPS:  # позиция уменьшилась: выясняем, что сработало (в т.ч. за время простоя после перезапуска)
            stop_hit = await self._sync_exits(trade, amt)
            trade.remaining = amt
            if amt <= _EPS:
                await self._finish_closed(trade, self._exit_reason(trade, stop_hit), now)
                return
        if trade.levels_done >= 1 and p.exit_plan.be_after_first and not trade.be_armed:
            trade.be_pending = True
        if trade.timeout_pending:
            await self._close_market(trade, "timeout")
            return
        await self._apply_stop(trade, amt)

    async def _sync_exits(self, trade: LiveTrade, amt: float) -> bool:
        """Какие algo-ордера сработали с прошлого опроса: уровни помечаются выполненными (+ журнал), возвращает «сработал стоп»."""
        plan = trade.plan.exit_plan
        stop_hit = False
        if trade.stop_cid is not None:
            stop_hit = (await self.client.get_algo_order(trade.stop_cid)).status in TRIGGERED
        last = len(trade.levels) - 1
        fired = False
        for i, lvl in enumerate(trade.levels):
            if lvl.done:
                continue
            state = await self.client.get_algo_order(lvl.cid)
            if state.status in TRIGGERED:
                fired = lvl.done = True
                trade.levels_done += 1
                reason = "tp" if i == last and plan.runner_frac <= _EPS else f"scale_{lvl.r:g}r"
                self._emit_trade(trade, "live_partial_close", {"reason": reason, "qty": lvl.qty, "price": state.actual_price or lvl.price,
                                                               "remaining": amt})
        if amt > _EPS and not stop_hit and not fired and amt < trade.remaining - _EPS:
            self._emit_trade(trade, "live_position_changed", {"expected": trade.remaining, "actual": amt})  # закрыли часть вне бота
        return stop_hit

    def _exit_reason(self, trade: LiveTrade, stop_hit: bool) -> str:
        if stop_hit:
            return trade.stop_kind
        if trade.levels and trade.levels[-1].done and trade.plan.exit_plan.runner_frac <= _EPS:
            return "tp"
        return "unknown"  # позицию закрыли вне бота (вручную) или биржа сняла ордера: PnL всё равно придёт в income

    async def _apply_stop(self, trade: LiveTrade, amt: float) -> None:
        """Переводит стоп на бирже: безубыток (стоп на вход), трейлинг, размер = остаток позиции. Только в сторону прибыли."""
        sgn, f = trade.sgn, self.client.filters(trade.plan.symbol)
        price, kind = trade.stop_price, trade.stop_kind
        be_done = trade.be_pending
        if be_done:
            be_price = self.client.round_price(trade.plan.symbol, trade.fill_price)
            if sgn * be_price < sgn * price:  # стоп переносится только в сторону прибыли
                price, kind = be_price, "be"
        trail = trade.trail_target
        if trail is not None and sgn * trail < sgn * price - 1e-12:
            price, kind = trail, "trail"
        if price == trade.stop_price and abs(trade.stop_qty - amt) <= f.step_size / 2:
            trade.be_armed = trade.be_armed or be_done
            trade.be_pending, trade.trail_target = False, None
            return
        if not await self._replace_stop(trade, price, kind, amt):
            return  # замена не состоялась (позиция закрыта по рынку или неизвестный исход): прежний стоп/нет позиции
        if be_done:
            trade.be_armed, trade.be_pending = True, False
            if kind == "be":
                self._emit_trade(trade, "live_be_armed", {"stop": trade.stop_price})
        if trail is not None:
            trade.trail_target = None
            if kind == "trail":
                self._emit_trade(trade, "live_trail_moved", {"stop": trade.stop_price, "mode": trade.plan.exit_plan.trail_mode})

    async def _replace_stop(self, trade: LiveTrade, price: float, kind: str, qty: float) -> bool:
        """POST нового стопа, затем DELETE старого: позиция ни на мгновение не остаётся без стопа (О18). False - замена не состоялась."""
        p = trade.plan
        new_seq = trade.stop_seq + 1
        new_cid = client_id(self.client.id_prefix, f"S{new_seq}", trade.trade_id)
        try:
            await self.client.stop_loss(p.symbol, p.close_side, price, client_id=new_cid, qty=qty)
        except BinanceAPIError as exc:
            if exc.code != ERR_WOULD_TRIGGER:
                await self._emergency(trade, exc, "stop_replace_failed")
            await self._close_market(trade, kind)  # цена уже за новым стопом: выход по рынку (BE/трейлинг-выход), не авария
            return False
        except UNKNOWN_OUTCOME:
            try:
                await self.client.get_algo_order(new_cid)  # новый стоп дошёл до биржи?
            except BinanceAPIError as lookup:
                if lookup.code not in ERR_UNKNOWN_ORDER:
                    raise
                return False  # не дошёл: прежний стоп на месте, повтор в следующий опрос
        old = trade.stop_cid
        trade.stop_seq, trade.stop_cid, trade.stop_price, trade.stop_kind, trade.stop_qty = new_seq, new_cid, price, kind, qty
        if old is not None:
            trade.stale_stops.append(old)
        self._save()  # новый стоп (и старый в списке на снятие) на диске ДО снятия старого
        if old is not None:
            await self._cancel_algo(old)
            trade.stale_stops.remove(old)
        return True

    async def _close_market(self, trade: LiveTrade, reason: str) -> None:
        """Закрыть остаток по рынку (reduce-only): тайм-аут, выход при «стоп за ценой», план не делится."""
        p = trade.plan
        pos = await self.client.get_position_amt(p.symbol)
        if abs(pos) > _EPS:
            await self.client.market_order(p.symbol, p.close_side, self.client.round_qty(p.symbol, abs(pos)), reduce_only=True,
                                           client_id=client_id(self.client.id_prefix, "X", trade.trade_id))
        await self._finish_closed(trade, reason, self._now())

    async def _finish_closed(self, trade: LiveTrade, reason: str, now: int) -> None:
        for cid in self._all_cids(trade):  # без позиции парные защитные ордера не нужны и опасны для следующей сделки по монете
            await self._cancel_algo(cid)
        self._emit_trade(trade, "live_closed", {"reason": reason, "filled_qty": trade.filled_qty, "fill_price": trade.fill_price,
                                                "levels_done": trade.levels_done, "daily_pnl_usdt": self.risk.daily_pnl})
        self._finish(trade, CLOSED, reason, now)

    # ------------------------------------------------------------------------- перезапуск и сверка

    async def recover(self, symbols: Collection[str]) -> None:
        """Старт: поднять сделки из файла состояния и сверить их с биржей; найти чужие позиции и висячие ордера на монетах пула."""
        now = self._now()
        for rec in self._store.load() if self._store is not None else []:
            t = LiveTrade.from_record(rec)
            if t.plan.symbol in self._trades:
                raise DataError(f"В файле состояния две сделки по {t.plan.symbol}")
            self.risk.adopt_busy(t.plan.symbol, t.trade_id)
            self._trades[t.plan.symbol] = t
        await self.refresh_pnl(now)
        for t in list(self._trades.values()):
            self._emit_trade(t, "live_recovered", {"status": t.status, "levels_done": t.levels_done, "remaining": t.remaining})
            if t.status == ENTRY:
                if t.entry_sent:
                    await self._poll_entry(t, now)
                else:
                    self._finish(t, CANCELLED, "never_sent", now)  # упали до отправки входа
            elif t.status == OPEN:
                await self._place_orders(t, check_first=True)  # план записан, но защиту могли не успеть выставить
                await self._manage(t, now)
        for symbol in symbols:
            if symbol not in self._trades:
                await self._adopt_or_clean(symbol, now)
        if self.router is not None and self.router.mt5 is not None:
            try:
                adopted = await self.router.recover_mt5()
            except Mt5UnprotectedPosition as exc:
                self._trip(str(exc), {})
                raise LiveExecutionError(str(exc)) from exc
            for mt in adopted:
                self.risk.adopt_busy(mt.symbol, mt.signal_id)
                self._emit(now, "live_adopted", mt.symbol, mt.strategy, mt.signal_id, {"venue": "mt5", "status": mt.status, "side": mt.side, "lots": mt.lots,
                                                                                   "position_id": mt.position_id, "order_id": mt.order_id})
        self._save()

    async def _adopt_or_clean(self, symbol: str, now: int) -> None:
        """Монета без записи сделки: позиция со стопом на бирже подтягивается, без стопа - Safe Mode; висячие наши algo-ордера снимаются."""
        pos = await self.client.get_position(symbol)
        algos = await self.client.get_open_algo_orders(symbol)
        if abs(pos.amt) <= _EPS:
            for a in (a for a in algos if a.client_algo_id.startswith(self.client.id_prefix)):
                await self._cancel_algo(a.client_algo_id)  # без позиции reduceOnly-ордер не нужен и опасен для следующей сделки
                self._emit(now, "live_orphan_algo_cancelled", symbol, "", None, {"client_algo_id": a.client_algo_id, "type": a.order_type})
            return
        direction = Direction.LONG if pos.amt > 0 else Direction.SHORT
        sgn, amt = direction.sgn, abs(pos.amt)
        close_side = "SELL" if pos.amt > 0 else "BUY"
        mine = [a for a in algos if a.side.value.upper() == close_side]
        stops = [a for a in mine if a.order_type in ("STOP_MARKET", "STOP")]
        if not stops or not pos.entry_price > 0:
            self._trip(f"{symbol}: позиция {pos.amt} без стопа/цены входа и без записи сделки", {"symbol": symbol})
            raise LiveExecutionError(f"{symbol}: на бирже позиция {pos.amt} без защитного стопа и без записи сделки: Safe Mode, разберите вручную")
        stop = min(stops, key=lambda a: sgn * a.trigger_price)  # самый тугой стоп (ближе к прибыли)
        be_like = sgn * stop.trigger_price <= sgn * pos.entry_price  # стоп на входе или за ним: безубыток уже взведён
        risk = 0.0 if be_like else abs(pos.entry_price - stop.trigger_price)
        tps = sorted((a for a in mine if a.order_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")), key=lambda a: -sgn * a.trigger_price)
        levels = [Level((abs(a.trigger_price - pos.entry_price) / risk) if risk else float(i + 1), a.qty or amt, a.trigger_price,
                        a.client_algo_id) for i, a in enumerate(tps)]
        fracs = tuple((lvl.r, min(lvl.qty / amt, 1.0)) for lvl in levels)
        rest = 1.0 - sum(f for _, f in fracs)
        exit_plan = ScaleOutPlan(fracs, True, "atr" if rest > _EPS else None) if fracs else DEFAULT_PLAN
        plan = LiveOrderPlan(
            signal_id=f"adopted_{symbol}_{now}", strategy="adopted", symbol=symbol, direction=direction, kind=Kind.MARKET,
            side="BUY" if pos.amt > 0 else "SELL", qty=amt, entry=pos.entry_price, sl=stop.trigger_price, tp=None, inval=None,
            leverage=self.risk.leverage, margin_usdt=amt * pos.entry_price / self.risk.leverage, notional_usdt=amt * pos.entry_price,
            max_loss_usdt=amt * abs(pos.entry_price - stop.trigger_price), exit_plan=exit_plan)
        t = LiveTrade(plan, now, status=OPEN, entry_sent=True, filled_qty=amt, fill_price=pos.entry_price, risk=risk, remaining=amt,
                      protected=True, stop_price=stop.trigger_price, stop_kind="be" if be_like else "sl", stop_cid=stop.client_algo_id,
                      stop_qty=stop.qty or amt, levels=levels, best_q=sgn * pos.entry_price, be_armed=be_like, adopted=True, fill_bar_no=0)
        self.risk.adopt_busy(symbol, plan.signal_id)
        self._trades[symbol] = t
        self._emit_trade(t, "live_adopted", {"amt": pos.amt, "entry": pos.entry_price, "stop": stop.trigger_price,
                                             "levels": [[v.r, v.qty, v.price] for v in levels]})

    # ------------------------------------------------------------------------------------------ прочее

    def _finish(self, trade: LiveTrade, status: str, reason: str, now: int) -> None:
        trade.status = status
        self._trades.pop(trade.plan.symbol, None)
        self.risk.release(trade.plan.symbol, trade.plan.signal_id)
        self._save()
        if status == CANCELLED:
            self._emit_trade(trade, "live_trade_cancelled", {"reason": reason})

    def _save(self) -> None:
        if self._store is not None:
            self._store.save(list(self._trades.values()))

    def _trip(self, reason: str, details: dict[str, Any]) -> None:
        self._safe.trip(f"live: {reason}", details)

    def _emit(self, ts: int, kind: str, symbol: str, strategy: str, signal_id: str | None, detail: dict[str, Any]) -> None:
        self._journal.emit(Event(ts, kind, symbol, strategy, signal_id, detail))

    def _emit_signal(self, ts: int, kind: str, s: Signal, detail: dict[str, Any]) -> None:
        self._emit(ts, kind, s.symbol, s.strategy, s.signal_id, detail)

    def _emit_trade(self, t: LiveTrade, kind: str, detail: dict[str, Any]) -> None:
        self._emit(self._now(), kind, t.plan.symbol, t.plan.strategy, t.trade_id, detail)


class LiveMirrorSink:
    """Приёмник маршрута SHADOW с боевым «зеркалом»: Shadow-трекер получает ВСЁ, как и без live; одобренные стратегии
    дополнительно уходят в движок. Отказ или сбой live не влияет на Shadow-учёт (трекер вызывается первым и независим)."""

    def __init__(self, primary: Any, engine: LiveExecutionEngine):
        self._primary, self._engine = primary, engine

    def submit(self, signal: Signal, now_ms: int) -> Submission:
        sub = self._primary.submit(signal, now_ms)
        if sub.accepted and self._engine.risk.is_approved(signal.strategy):
            self._engine.enqueue(signal, now_ms)
        return sub

    def on_bar_closed(self, symbol: str, bar_open_time: int, close: float, now_ms: int, bar: Any = None) -> None:
        self._primary.on_bar_closed(symbol, bar_open_time, close, now_ms, bar)
        self._engine.on_bar_closed(symbol, bar_open_time, close, now_ms, bar)

    def add_close_listener(self, listener: Callable[[Any], None]) -> None:
        self._primary.add_close_listener(listener)


async def build_live_engine(
    session: aiohttp.ClientSession, journal: EventLog, *, symbols: Collection[str], strategy_names: Collection[str], interval_ms: int,
    flush: Callable[[], Awaitable[None]] | None = None, now_ms: Callable[[], int] | None = None,
    plans: Mapping[str, ScaleOutPlan] | None = None,
) -> LiveExecutionEngine:
    """Собирает боевой контур при TRADE_MODE=live. Любое сомнение (режим, ключи, mainnet без подтверждения, Safe Mode, неизвестная
    стратегия в APPROVED_STRATEGIES, позиция без стопа) - ConfigError / LiveExecutionError ДО первого нового ордера.
    Позиции и ордера прошлого запуска подтягиваются из файла состояния и сверяются с биржей (`recover`)."""
    if config.TRADE_MODE != "live":
        raise ConfigError(f"Live Execution Engine собирается только при TRADE_MODE=live (сейчас {config.TRADE_MODE!r})")
    if not config.USE_TESTNET and not config.LIVE_ALLOW_MAINNET:
        raise ConfigError("TRADE_MODE=live на mainnet = реальные деньги: подтвердите явно LIVE_ALLOW_MAINNET=true "
                          "(или USE_TESTNET=true для ордеров на testnet)")
    unknown = sorted(set(config.APPROVED_STRATEGIES) - set(strategy_names))
    if unknown:
        raise ConfigError(f"APPROVED_STRATEGIES содержит неизвестные стратегии {unknown}; запущены {sorted(strategy_names)}")
    if plans is None:
        from orchestrator.registry import plans_for

        plans = plans_for(list(strategy_names))
    safe = SafeMode()
    try:
        safe.require_trading()
    except SafeModeActive as exc:
        raise ConfigError(f"Live не стартует: {exc}. Разберите причину и снимите замок вручную") from exc
    client = BinanceLiveClient.from_env(session)
    await client.rest.sync_time()
    await client.load_filters(symbols)
    router = await build_router(client, with_mt5=config.LIVE_ENABLE_MT5)  # MT5 - только по LIVE_ENABLE_MT5=true (ключи MetaApi обязательны)
    risk = LiveRiskManager(
        {s: client.filters(s) for s in symbols}, approved_strategies=config.APPROVED_STRATEGIES, margin_usdt=config.LIVE_MARGIN_USDT,
        leverage=config.LIVE_LEVERAGE, daily_loss_limit_usdt=config.LIVE_DAILY_LOSS_LIMIT_USDT,
        state_path=config.DATA_DIR / config.LIVE_RISK_STATE_NAME, plans=plans, venues=router.venues,
    )
    clock = now_ms or (lambda: int(time.time() * 1000))
    engine = LiveExecutionEngine(client, risk, journal, safe, now_ms=clock, interval_ms=interval_ms, flush=flush,
                                 store=LiveStateStore(config.DATA_DIR / config.LIVE_STATE_NAME), router=router)
    await engine.recover(symbols)
    return engine
