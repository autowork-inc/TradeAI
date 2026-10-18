"""VirtualTracker: виртуальное исполнение сигналов для Shadow Mode (D5). Состояние - в памяти; все сделки, закрытия и снапшоты
пишутся ещё и в PostgreSQL через `TradeRecorder` (Этап 5).

Исполнение симулируется по тикам WebSocket, а не по свечам (в отличие от бэктеста strategies/simulate.py):
  * `trade` (aggTrade) - цена последней сделки: заполнение LIMIT, срабатывание стопа, частичные закрытия по уровням;
  * `mark` (markPrice@1s) - mark price: ТОЛЬКО триггер переноса SL в безубыток при +1R (D11, О2).
  * закрытие свечи (`on_bar_closed`) - срок заявки, отмена по закрытию за хаем свипа, тайм-аут позиции, подтягивание трейлинга.

Управление позицией - `ScaleOutPlan` (решение 2026-10-10, вместо «100% на 3R» из D9). По умолчанию:
  * +1R: закрыть 50% ИСХОДНОГО объёма и перенести SL в безубыток (вход);
  * +3R: закрыть ещё 40%;
  * остаток 10% (runner): трейлинг-стоп - по ATR (стоп = лучшая цена -+ 3*ATR последней закрытой свечи) либо по теням последних
    свечей; стоп только подтягивается в сторону прибыли и никогда не лежит дальше безубытка. Тейка у runner'а нет.
`LEGACY_PLAN` - прежняя схема (100% на 3R, BE только по mark price): для старых тестов и сравнения.

Правила (всё в «шортовом мире»: цена * sgn, sgn = +1 шорт / -1 лонг):
  * LIMIT исполняется, когда цена сделки ПРОШЛА уровень (q > entry), касание не считается; цена исполнения - сам
    уровень (пассивная заявка, комиссия maker). Если до исполнения цена дошла до TP сигнала - заявка отменяется.
  * MARKET исполняется по первому тику сделки после сигнала по цене тика (taker); R и плечо считаются по факту.
    Не исполнился до закрытия следующей свечи - значит, поток тиков встал: TrackerError (R1).
  * Стоп (SL / безубыток / трейлинг) - стоп-маркет: срабатывает по цене сделки и закрывает остаток по цене тика (со скольжением).
    Частичные закрытия по уровням - тоже по цене тика. На одном тике сначала проверяется стоп (худший сценарий), потом уровни.
  * Безубыток: (а) mark price достигла entry -+ 1R (D11) - со следующего тика; (б) после первого частичного закрытия плана.
  * Комиссии: вход LIMIT - maker 0.02%, остальное - taker 0.05% (config.LIVE_*_FEE_RATE); у каждого закрытия - своя доля комиссии входа.
Изоляция позиций (2026-10-11, мульти-стратегийный Shadow). Позиция идентифицируется СОСТАВНЫМ КЛЮЧОМ (символ, strategy_id), строкой
`<символ>_<strategy_id>` (SOLUSDT_SMC_Pro_Trend): по одной монете параллельно живут независимые позиции разных стратегий, в том числе
разнонаправленные - у каждой свой стоп, уровни Scale Out, трейлинг, счётчик свечей и запись в БД; тик монеты обслуживает каждую по
отдельности. Вторая активная позиция под тем же ключом - TrackerError (протокол нарушен: блокировка монеты Оркестратора её не пропустила
бы). План управления позицией задаётся на стратегию (`plans`): у контртренда это скальп 1R, у остальных - Scale Out.
Размер позиции - из виртуального эквити (без реинвестирования: каждая сделка рискует 1% от одной и той же суммы),
чтобы R-статистика и PnL в USDT не зависели от порядка сделок. net R сделки = сумма net R её закрытий (доля * (вход - выход) / R).

Любое нарушение протокола (дубль сигнала, свеча раньше сигнальной, MARKET без тиков, runner без данных свечи) - TrackerError,
процесс падает и перезапускается (R1). Подавления исключений здесь нет.
"""

import math
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

import config
from features import defaults as D
from orchestrator.journal import EventLog
from orchestrator.risk import RiskSizer, Sizing, SizingRejected
from orchestrator.types import BarInfo, Event, Kind, Signal, Tick
from persistence.recorder import NullRecorder, TradeRecorder
from utils.errors import ConfigError, TradingInfraError

PENDING_LIMIT, PENDING_MARKET, OPEN, CLOSED, CANCELLED = "pending_limit", "pending_market", "open", "closed", "cancelled"
ACTIVE = (PENDING_LIMIT, PENDING_MARKET, OPEN)
_EPS = 1e-9


class TrackerError(TradingInfraError):
    """Нарушен протокол трекера (баг вызывающего кода или остановившийся поток тиков)."""


@dataclass(frozen=True)
class Submission:
    accepted: bool
    reason: str | None = None  # при отказе: 'exposure' | 'bad_risk'


class SignalSink(Protocol):
    """Приёмник сигналов маршрута: VirtualTracker (SHADOW) или LiveBarrier-адаптер (LIVE, Этап 4)."""

    def submit(self, signal: Signal, now_ms: int) -> Submission: ...
    def on_bar_closed(self, symbol: str, bar_open_time: int, close: float, now_ms: int, bar: BarInfo | None = None, interval: str | None = None) -> None: ...
    def add_close_listener(self, listener: Callable[[Any], None]) -> None: ...  # слушателю достаточно .trade_id (VirtualTrade | LiveTrade)


@dataclass(frozen=True)
class ScaleOutPlan:
    """levels: ((уровень в R, доля ИСХОДНОГО объёма), ...) по возрастанию; остаток - runner (нужен trail_mode)."""

    levels: tuple[tuple[float, float], ...]
    be_after_first: bool = False
    trail_mode: str | None = None  # "atr" | "wick" | None (если остатка нет)
    trail_atr_mult: float = D.TRAIL_ATR_MULT
    trail_wick_bars: int = D.TRAIL_WICK_BARS
    # От чего считаются цены уровней: "fill" - от фактической цены входа и R = |вход - SL| (как раньше); "signal" (2026-10-18, SMC_Pro_Trend) - от цен
    # СИГНАЛА: входа (50% FVG) и SL за экстремумом свипа, то есть от структуры графика, а не от цены, сдвинутой проскальзыванием. Уровень, оказавшийся
    # не в прибыли относительно фактического входа, считается от входа (иначе тейк «за ценой»). R-статистика сделки всегда - от фактического входа.
    tp_anchor: str = "fill"

    def __post_init__(self) -> None:
        if self.tp_anchor not in ("fill", "signal"):
            raise ConfigError(f"ScaleOutPlan: tp_anchor 'fill' или 'signal', получено {self.tp_anchor!r}")
        if not self.levels:
            raise ConfigError("ScaleOutPlan: нужен хотя бы один уровень")
        prev = 0.0
        for r, frac in self.levels:
            if not (r > prev and 0 < frac <= 1):
                raise ConfigError(f"ScaleOutPlan: уровни R строго возрастают и > 0, доли в (0, 1]: {self.levels}")
            prev = r
        if sum(f for _, f in self.levels) > 1 + _EPS:
            raise ConfigError(f"ScaleOutPlan: сумма долей {sum(f for _, f in self.levels)} больше 1")
        if self.runner_frac > _EPS and self.trail_mode not in ("atr", "wick"):
            raise ConfigError("ScaleOutPlan: у остатка (runner) должен быть trail_mode 'atr' или 'wick'")
        if self.runner_frac <= _EPS and self.trail_mode is not None:
            raise ConfigError("ScaleOutPlan: остатка нет, trail_mode должен быть None")
        if not (self.trail_atr_mult > 0 and self.trail_wick_bars >= 1):
            raise ConfigError("ScaleOutPlan: trail_atr_mult > 0, trail_wick_bars >= 1")

    @property
    def runner_frac(self) -> float:
        rest = 1.0 - sum(f for _, f in self.levels)
        return 0.0 if rest <= _EPS else rest

    @property
    def last_r(self) -> float:
        return self.levels[-1][0]

    def describe(self) -> dict[str, Any]:
        return {"levels": [list(x) for x in self.levels], "be_after_first": self.be_after_first, "runner_frac": self.runner_frac,
                "trail_mode": self.trail_mode, "trail_atr_mult": self.trail_atr_mult, "trail_wick_bars": self.trail_wick_bars, "tp_anchor": self.tp_anchor}

    def level_price(self, r: float, *, side_sgn: float, fill: float, sl: float, signal_entry: float | None) -> float:
        """Цена уровня `r` (реальная, не q-мир). side_sgn - Direction.sgn (+1 шорт, -1 лонг). Якорь "signal": от входа и SL сигнала; уровень не в прибыли
        относительно фактического входа (проскальзывание за уровень) - от фактического входа, как "fill". Одна функция для VirtualTracker и боевого движка (R8)."""
        by_fill = fill - side_sgn * r * abs(fill - sl)
        if self.tp_anchor != "signal" or signal_entry is None:
            return by_fill
        anchored = signal_entry - side_sgn * r * abs(signal_entry - sl)
        return anchored if side_sgn * anchored < side_sgn * fill else by_fill


def impulse_hits_limit(bar: BarInfo | None, *, entry: float, sl: float, mult: float = D.IMPULSE_BAR_ATR) -> bool:
    """Импульсная свеча у ждущей лимитки: диапазон закрытой свечи > mult * ATR(14)[i-1] И свеча дошла до зоны заявки - ближе 1R (|вход - SL|)
    к цене лимитки со стороны рынка (шорт: high >= вход - R; лонг: low <= вход + R). Такой «нож» обычно пробивает и вход, и стоп: заявку снимаем.
    Нет данных свечи / ATR - False (правило не срабатывает; обычные отмены работают). Одна функция для VirtualTracker и боевого движка (R8)."""
    if bar is None or bar.high is None or bar.low is None or bar.atr_prev is None or not bar.atr_prev > 0:
        return False
    if not bar.high - bar.low > mult * bar.atr_prev:
        return False
    risk = abs(entry - sl)
    return bar.high >= entry - risk if sl > entry else bar.low <= entry + risk


DEFAULT_PLAN = ScaleOutPlan(D.SCALE_OUT_LEVELS, D.SCALE_OUT_BE_AFTER_FIRST, D.RUNNER_TRAIL_MODE, D.TRAIL_ATR_MULT, D.TRAIL_WICK_BARS)
LEGACY_PLAN = ScaleOutPlan(((D.TP_R, 1.0),), False, None)


@dataclass(frozen=True)
class Leg:
    """Одно закрытие (частичное или итоговое). Все R - вклад ЭТОГО закрытия в сделку (с учётом доли)."""

    seq: int
    ts: int
    reason: str  # scale_1r | scale_3r | tp (последний уровень без runner'а) | sl | be | trail | timeout
    frac: float
    price: float
    gross_r: float
    fees_r: float

    @property
    def net_r(self) -> float:
        return self.gross_r - self.fees_r


@dataclass
class VirtualTrade:
    signal: Signal
    sgn: float
    equity: float
    status: str
    registered_ms: int
    plan: ScaleOutPlan = field(default_factory=lambda: DEFAULT_PLAN)
    entry_m: float | None = None  # цена входа в «шортовом мире»: сначала цена заявки, потом факт
    sl_m: float = 0.0
    tp_m: float | None = None  # TP сигнала (3R): для отмены LIMIT «цена дошла до TP до исполнения»
    inval_m: float | None = None
    stop_m: float = 0.0  # действующий стоп (q >= stop_m -> выход остатка): SL -> безубыток -> трейлинг; только вниз (в q-мире)
    stop_kind: str = "sl"  # sl | be | trail - причина выхода, если стоп сработает
    bars_closed: int = 0  # свечей закрылось после сигнальной
    fill_bar_no: int | None = None
    fill_price: float | None = None
    fill_ts: int | None = None
    sizing: Sizing | None = None
    risk: float | None = None  # |sl - вход| в единицах цены
    be_armed: bool = False
    be_armed_ts: int | None = None
    mfe_r: float = 0.0  # максимальное благоприятное / неблагоприятное движение в R (по тикам сделок)
    mae_r: float = 0.0
    remaining: float = 1.0  # доля исходного объёма, ещё открытая
    levels_done: int = 0
    legs: list[Leg] = field(default_factory=list)
    recent_bars: deque = field(default_factory=deque)  # последние закрытые свечи (режим трейлинга "wick")
    exit_price: float | None = None  # средневзвешенная цена всех закрытий
    exit_ts: int | None = None
    exit_reason: str | None = None  # причина ПОСЛЕДНЕГО закрытия (итоговая)
    gross_r: float | None = None
    net_r: float | None = None
    pnl_usdt: float | None = None
    gap_affected: bool = False  # во время сделки был обрыв потока: результат ненадёжен

    @property
    def trade_id(self) -> str:
        return self.signal.signal_id

    @property
    def realized_gross_r(self) -> float:
        return sum(leg.gross_r for leg in self.legs)

    @property
    def realized_net_r(self) -> float:
        return sum(leg.net_r for leg in self.legs)

    @property
    def runner_active(self) -> bool:
        return self.status == OPEN and self.plan.runner_frac > _EPS and self.levels_done == len(self.plan.levels)

    def plan_tp_price(self) -> float | None:
        """Цена последнего уровня плана (у runner'а это условная цель, не ордер)."""
        if self.entry_m is None or self.risk is None:
            return None
        return self.level_q(self.plan.last_r) * self.sgn

    def level_q(self, r: float) -> float:
        """Уровень `r` в «шортовом мире» (ScaleOutPlan.level_price: якорь - факт входа или цены сигнала)."""
        s = self.signal
        return self.sgn * self.plan.level_price(r, side_sgn=self.sgn, fill=self.entry_m * self.sgn, sl=s.sl, signal_entry=s.entry)

    def record(self) -> dict[str, Any]:
        s = self.signal
        return {
            "trade_id": self.trade_id, "strategy": s.strategy, "strategy_id": s.strategy_id, "position_key": s.position_key, "symbol": s.symbol, "direction": s.direction.value,
            "kind": s.kind.value, "status": self.status, "signal_bar": s.bar_open_time, "fill_price": self.fill_price,
            "fill_ts": self.fill_ts, "sl": s.sl, "tp": self.plan_tp_price(),
            "exit_price": self.exit_price, "exit_ts": self.exit_ts, "exit_reason": self.exit_reason,
            "gross_r": self.gross_r, "net_r": self.net_r, "pnl_usdt": self.pnl_usdt, "mfe_r": self.mfe_r, "mae_r": self.mae_r,
            "be_armed": self.be_armed, "gap_affected": self.gap_affected,
            "qty": None if self.sizing is None else self.sizing.qty, "remaining": self.remaining,
            "legs": [{"seq": g.seq, "reason": g.reason, "frac": g.frac, "price": g.price, "net_r": g.net_r} for g in self.legs],
        }


class VirtualTracker:
    def __init__(
        self, log: EventLog, *, equity: float = 10_000.0, sizer: RiskSizer | None = None, limit_ttl_bars: int = D.LIMIT_TTL_BARS,
        max_hold_bars: int = 192, be_trigger_r: float = D.BE_TRIGGER_R, plan: ScaleOutPlan = DEFAULT_PLAN,
        maker_fee: float = 0.0002, taker_fee: float = 0.0005, recorder: TradeRecorder | None = None, mode: str = "shadow",
        plans: Mapping[str, ScaleOutPlan] | None = None,
    ):
        if not (equity > 0 and limit_ttl_bars >= 1 and max_hold_bars >= 1 and be_trigger_r > 0):
            raise ConfigError("VirtualTracker: equity > 0, сроки >= 1, be_trigger_r > 0")
        self._log, self._equity = log, equity
        self._sizer = sizer or RiskSizer()
        self._limit_ttl, self._max_hold, self._be_r, self._plan = limit_ttl_bars, max_hold_bars, be_trigger_r, plan
        self._plans = dict(plans or {})  # strategy_id -> план управления позицией (нет в словаре - общий `plan`)
        self._maker, self._taker = maker_fee, taker_fee
        self._rec: TradeRecorder = recorder or NullRecorder()
        self._mode = mode
        self.trades: list[VirtualTrade] = []
        self._active: dict[str, dict[str, VirtualTrade]] = defaultdict(dict)  # символ -> {strategy_id -> активная позиция}
        self._ids: set[str] = set()
        self._listeners: list[Callable[[VirtualTrade], None]] = []

    # ------------------------------------------------------------------------------ интерфейс SignalSink

    def add_close_listener(self, listener: Callable[[VirtualTrade], None]) -> None:
        self._listeners.append(listener)

    def active(self, symbol: str | None = None, strategy: str | None = None) -> list[VirtualTrade]:
        """Активные (заявка/позиция) сделки; фильтры по монете и/или strategy_id."""
        out = [t for sym, by_strategy in self._active.items() if symbol is None or sym == symbol for t in by_strategy.values()]
        return out if strategy is None else [t for t in out if t.signal.strategy == strategy]

    def plan_for(self, strategy: str) -> ScaleOutPlan:
        return self._plans.get(strategy, self._plan)

    def submit(self, signal: Signal, now_ms: int) -> Submission:
        if signal.signal_id in self._ids:
            raise TrackerError(f"Сигнал {signal.signal_id} уже зарегистрирован")
        busy = self._active[signal.symbol].get(signal.strategy)
        if busy is not None:
            raise TrackerError(f"Ключ позиции {signal.position_key} занят сделкой {busy.trade_id}: второй сигнал {signal.signal_id} "
                               "не должен был пройти блокировку монеты")
        sgn = signal.direction.sgn
        plan = self.plan_for(signal.strategy)
        trade = VirtualTrade(signal, sgn, self._equity, PENDING_MARKET if signal.kind is Kind.MARKET else PENDING_LIMIT, now_ms, plan,
                             recent_bars=deque(maxlen=plan.trail_wick_bars))
        trade.sl_m = trade.stop_m = sgn * signal.sl
        if signal.kind is Kind.LIMIT:
            sizing = self._sizer.size(self._equity, signal.entry, signal.sl)
            if isinstance(sizing, SizingRejected):
                self._emit(now_ms, "skipped_risk", trade, {"reason": sizing.reason, "exposure": sizing.exposure})
                return Submission(False, sizing.reason)
            trade.sizing, trade.entry_m = sizing, sgn * signal.entry
            trade.tp_m, trade.inval_m = sgn * signal.tp, sgn * signal.inval
            trade.risk = abs(signal.sl - signal.entry)
        self._ids.add(signal.signal_id)
        self.trades.append(trade)
        self._active[signal.symbol][signal.strategy] = trade
        self._persist(trade)
        self._emit(now_ms, "trade_registered", trade, {"kind": signal.kind.value, "entry": signal.entry, "sl": signal.sl,
                                                       "tp": signal.tp, "geometry": list(signal.geometry), "meta": signal.meta,
                                                       "plan": plan.describe()})
        return Submission(True)

    # --------------------------------------------------------------------------------------------- тики

    def on_tick(self, tick: Tick) -> None:
        by_strategy = self._active.get(tick.symbol)
        if not by_strategy:
            return
        for t in list(by_strategy.values()):  # каждая позиция монеты обслуживается отдельно, порядок не важен: состояния не пересекаются
            if tick.kind == "mark":
                self._on_mark(t, tick)
            else:
                self._on_trade(t, tick)

    def _on_mark(self, t: VirtualTrade, tick: Tick) -> None:
        if t.status != OPEN or t.be_armed:
            return
        if t.sgn * tick.price <= t.entry_m - self._be_r * t.risk:  # mark дошла до +1R: стоп на вход, с этого тика (D11)
            self._arm_be(t, tick.ts_ms, {"mark": tick.price})

    def _on_trade(self, t: VirtualTrade, tick: Tick) -> None:
        q = t.sgn * tick.price
        if t.status == PENDING_LIMIT:
            if q > t.entry_m:  # цена прошла уровень заявки
                self._fill(t, t.entry_m * t.sgn, tick.ts_ms)
            elif q <= t.tp_m:
                self._cancel(t, "tp_before_fill", tick.ts_ms)
                return
            else:
                return
        elif t.status == PENDING_MARKET:
            risk = t.sl_m - q
            if not risk > 0:
                self._cancel(t, "bad_risk", tick.ts_ms)
                return
            sizing = self._sizer.size(self._equity, tick.price, t.signal.sl)
            if isinstance(sizing, SizingRejected):
                self._cancel(t, f"rejected_{sizing.reason}", tick.ts_ms)
                return
            t.sizing, t.entry_m, t.risk = sizing, q, risk
            t.tp_m = t.level_q(t.plan.last_r)
            self._fill(t, tick.price, tick.ts_ms)
        if t.status != OPEN:
            return
        t.mfe_r = max(t.mfe_r, (t.entry_m - q) / t.risk)
        t.mae_r = max(t.mae_r, (q - t.entry_m) / t.risk)
        if q >= t.stop_m:  # худший сценарий: на одном тике сначала стоп, потом уровни
            self._close_leg(t, t.stop_kind, t.remaining, tick.price, tick.ts_ms)
            return
        self._scale_out(t, q, tick)

    def _scale_out(self, t: VirtualTrade, q: float, tick: Tick) -> None:
        plan = t.plan
        while t.status == OPEN and t.levels_done < len(plan.levels):
            r, frac = plan.levels[t.levels_done]
            if q > t.level_q(r):
                return
            t.levels_done += 1
            frac = min(frac, t.remaining)
            finishing = t.levels_done == len(plan.levels) and plan.runner_frac <= _EPS
            self._close_leg(t, "tp" if finishing else f"scale_{r:g}r", frac, tick.price, tick.ts_ms)
            if t.status == OPEN and t.levels_done == 1 and plan.be_after_first:
                self._arm_be(t, tick.ts_ms, {"source": "scale_out"})

    def _arm_be(self, t: VirtualTrade, ts: int, detail: dict[str, Any]) -> None:
        if t.be_armed:
            return
        t.be_armed, t.be_armed_ts = True, ts
        if t.entry_m < t.stop_m:  # стоп переносится только в сторону прибыли
            t.stop_m, t.stop_kind = t.entry_m, "be"
        self._persist(t)
        self._emit(ts, "be_armed", t, detail)

    # ------------------------------------------------------------------------------------------ свечи

    def on_bar_closed(self, symbol: str, bar_open_time: int, close: float, now_ms: int, bar: BarInfo | None = None, interval: str | None = None) -> None:
        """Закрытие свечи. `interval` - её таймфрейм: сделка живёт по свечам СВОЕГО таймфрейма (сигнала), чужие (15m при сделке на 5m) пропускаются; None - любой (старые вызовы)."""
        for t in list(self._active.get(symbol, {}).values()):
            if interval is not None and t.signal.interval != interval:
                continue
            if bar_open_time <= t.signal.bar_open_time:
                raise TrackerError(f"{t.trade_id}: закрытие свечи {bar_open_time} не позже сигнальной {t.signal.bar_open_time}")
            t.bars_closed += 1
            q = t.sgn * close
            if t.status == PENDING_MARKET:
                raise TrackerError(f"{t.trade_id}: MARKET не исполнен за свечу - поток тиков {symbol} остановился?")
            scale = self._bars_scale(t)
            if t.status == PENDING_LIMIT:
                if q > t.inval_m:
                    self._cancel(t, "invalidated", now_ms)
                elif impulse_hits_limit(bar, entry=t.entry_m * t.sgn, sl=t.signal.sl):
                    self._cancel(t, "impulse_bar", now_ms)
                elif t.bars_closed >= self._limit_ttl * scale:
                    self._cancel(t, "ttl", now_ms)
            elif t.bars_closed - t.fill_bar_no >= self._max_hold * scale:
                self._close_leg(t, "timeout", t.remaining, close, now_ms)
            else:
                self._trail(t, bar, now_ms)

    @staticmethod
    def _bars_scale(t: VirtualTrade) -> int:
        """Сроки (`limit_ttl_bars`, `max_hold_bars`) заданы в барах M15; на 5m умножаются на 3 - календарное время то же (features.defaults.bars_scale)."""
        ms = config.INTERVAL_MS.get(t.signal.interval)
        if ms is None:
            raise ConfigError(f"{t.trade_id}: неизвестный таймфрейм сигнала {t.signal.interval!r}")
        return D.bars_scale(ms)

    def _trail(self, t: VirtualTrade, bar: BarInfo | None, now_ms: int) -> None:
        """Подтягивание стопа runner'а по закрытой свече СВОЕГО таймфрейма (вступает в силу со следующего тика: без заглядывания вперёд)."""
        if bar is not None and bar.high is not None and bar.low is not None:
            t.recent_bars.append(bar)
        if not t.runner_active:
            return
        if bar is None:
            raise TrackerError(f"{t.trade_id}: runner без данных закрытой свечи - трейлинг невозможен")
        if t.plan.trail_mode == "atr":
            if bar.atr is None or not math.isfinite(bar.atr) or bar.atr <= 0:
                raise TrackerError(f"{t.trade_id}: нет ATR для трейлинга ({bar.atr!r})")
            best_m = t.entry_m - t.mfe_r * t.risk  # лучшая (наименьшая) цена в q-мире с момента входа
            candidate = best_m + t.plan.trail_atr_mult * bar.atr
        else:
            if len(t.recent_bars) == 0:
                return
            candidate = max(max(t.sgn * b.high, t.sgn * b.low) for b in t.recent_bars)  # худший экстремум за N свечей
        if candidate < t.stop_m - 1e-12:
            t.stop_m, t.stop_kind = candidate, "trail"
            self._persist(t)
            self._emit(now_ms, "trail_moved", t, {"stop": t.stop_m * t.sgn, "mode": t.plan.trail_mode})

    def mark_gap(self, symbol: str, now_ms: int) -> None:
        """Обрыв потока тиков: тики за обрыв потеряны, результат активных сделок по этой монете ненадёжен."""
        for t in list(self._active.get(symbol, {}).values()):
            t.gap_affected = True
            self._persist(t)
            self._emit(now_ms, "data_gap", t, {})

    # ----------------------------------------------------------------------------------- переходы

    def _fill(self, t: VirtualTrade, price: float, ts: int) -> None:
        t.status, t.fill_price, t.fill_ts, t.fill_bar_no = OPEN, price, ts, t.bars_closed + 1
        self._persist(t)
        self._emit(ts, "limit_filled" if t.signal.kind is Kind.LIMIT else "market_filled", t,
                   {"price": price, "qty": t.sizing.qty, "exposure": t.sizing.exposure})

    def _close_leg(self, t: VirtualTrade, reason: str, frac: float, price: float, ts: int) -> None:
        """Закрывает долю frac исходного объёма. Остаток <= 0 - сделка закрыта целиком."""
        frac = min(frac, t.remaining)
        fee_rate_in = self._maker if t.signal.kind is Kind.LIMIT else self._taker
        gross = frac * (t.entry_m - t.sgn * price) / t.risk
        fees_r = frac * (fee_rate_in * t.fill_price + self._taker * price) / t.risk  # доля комиссии входа + комиссия этого закрытия
        leg = Leg(len(t.legs) + 1, ts, reason, frac, price, gross, fees_r)
        t.legs.append(leg)
        t.remaining -= frac
        final = t.remaining <= _EPS
        if final:
            t.remaining = 0.0
            t.status, t.exit_ts, t.exit_reason = CLOSED, ts, reason
            t.exit_price = sum(g.frac * g.price for g in t.legs) / sum(g.frac for g in t.legs)
            t.gross_r, t.net_r = t.realized_gross_r, t.realized_net_r
            t.pnl_usdt = t.net_r * t.sizing.risk_amount
        self._rec.close_leg(self._leg_row(t, leg))
        if final:
            self._finish(t, "trade_closed", ts, {"reason": reason, "price": price, "net_r": t.net_r, "gross_r": t.gross_r,
                                                 "legs": len(t.legs)})
        else:
            self._persist(t)
            self._emit(ts, "partial_close", t, {"reason": reason, "frac": frac, "price": price, "remaining": t.remaining,
                                                "net_r_leg": leg.net_r})

    def _cancel(self, t: VirtualTrade, reason: str, ts: int) -> None:
        t.status, t.exit_reason = CANCELLED, reason
        self._finish(t, "trade_cancelled", ts, {"reason": reason})

    def _finish(self, t: VirtualTrade, kind: str, ts: int, detail: dict[str, Any]) -> None:
        if self._active[t.signal.symbol].pop(t.signal.strategy, None) is not t:
            raise TrackerError(f"{t.trade_id}: сделка не числится активной под ключом {t.signal.position_key}")
        self._persist(t)
        self._emit(ts, kind, t, {**detail, "record": t.record()})
        for listener in self._listeners:
            listener(t)

    def _emit(self, ts: int, kind: str, t: VirtualTrade, detail: dict[str, Any]) -> None:
        s = t.signal
        self._log.emit(Event(ts, kind, s.symbol, s.strategy, s.signal_id, detail))

    # ------------------------------------------------------------------------------------- запись в БД

    def _persist(self, t: VirtualTrade) -> None:
        s, ra = t.signal, (t.sizing.risk_amount if t.sizing else None)
        done = bool(t.legs)
        self._rec.trade({
            "id": t.trade_id, "strategy_name": s.strategy, "strategy_id": s.strategy_id, "coin": s.symbol, "side": s.direction.value, "kind": s.kind.value,
            "mode": self._mode, "status": t.status, "qty": None if t.sizing is None else t.sizing.qty,
            "remaining_frac": t.remaining if t.status in (OPEN, CLOSED) else None, "signal_time": s.bar_close_ms,
            "entry_time": t.fill_ts, "exit_time": t.exit_ts, "planned_entry": s.entry if s.kind is Kind.LIMIT else None,
            "entry_price": t.fill_price, "exit_price": t.exit_price, "sl": s.sl,
            "tp": t.plan_tp_price() if t.fill_price is not None else s.tp,
            "stop_price": t.stop_m * t.sgn if t.status == OPEN else None, "risk": t.risk, "risk_amount": ra,
            "pnl": t.realized_net_r * ra if done and ra else None,
            "fees": sum(g.fees_r for g in t.legs) * ra if done and ra else None,
            "r_multiple": t.realized_net_r if done else None, "gross_r": t.realized_gross_r if done else None,
            "mfe_r": t.mfe_r if t.fill_price is not None else None, "mae_r": t.mae_r if t.fill_price is not None else None,
            "exit_reason": t.exit_reason, "be_armed": t.be_armed, "gap_affected": t.gap_affected,
            "client_order_id": None, "plan": t.plan.describe(),
        })

    @staticmethod
    def _leg_row(t: VirtualTrade, g: Leg) -> dict[str, Any]:
        ra, qty = t.sizing.risk_amount, t.sizing.qty
        return {"trade_id": t.trade_id, "seq": g.seq, "ts": g.ts, "reason": g.reason, "frac": g.frac, "qty": qty * g.frac,
                "price": g.price, "gross_r": g.gross_r, "fees_r": g.fees_r, "net_r": g.net_r, "pnl": g.net_r * ra, "fees": g.fees_r * ra}

    # ------------------------------------------------------------------------------------- статистика

    def summary(self) -> dict[str, dict[str, Any]]:
        """По (стратегия, направление): сколько заявок, исполнено, исходы, сумма net R (только закрытые, без gap_affected)."""
        out: dict[str, dict[str, Any]] = {}
        for t in self.trades:
            row = out.setdefault(f"{t.signal.strategy}/{t.signal.direction.value}", {
                "signals": 0, "cancelled": 0, "open": 0, "pending": 0, "closed": 0, "tp": 0, "sl": 0, "be": 0, "trail": 0, "timeout": 0,
                "partials": 0, "gap_affected": 0, "sum_net_r": 0.0,
            })
            row["signals"] += 1
            row["gap_affected"] += int(t.gap_affected)
            if t.status == CANCELLED:
                row["cancelled"] += 1
            elif t.status == OPEN:
                row["open"] += 1
            elif t.status in (PENDING_LIMIT, PENDING_MARKET):
                row["pending"] += 1
            else:
                row["closed"] += 1
                row[t.exit_reason] += 1
                row["partials"] += int(len(t.legs) > 1)
                if not t.gap_affected:
                    row["sum_net_r"] += t.net_r
        return out
