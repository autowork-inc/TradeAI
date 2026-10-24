"""Orchestrator: FeatureContext -> стратегии -> блокировка монеты -> маршрут (SHADOW | LIVE).

Поток на каждую закрытую свечу (строго в таком порядке):
  1. проверка порядка: свеча монеты обязана идти ровно за предыдущей В СВОЁМ ТАЙМФРЕЙМЕ (ключ (монета, interval): 5m и 15m одной монеты - две независимые
     цепочки; feed гарантирует непрерывность, иначе баг -> ошибка);
  2. приёмники сообщают о закрытии свечи с её таймфреймом (сроки заявок, отмены, тайм-ауты; могут освободить монету): приёмник учитывает только сделки
     этого таймфрейма (бар 15m не «стареет» заявку, поданную на 5m);
  3. каждая стратегия СВОЕГО таймфрейма (`strategy.interval`; None - любого) считает `detect(ctx)`; ошибка стратегии НЕ перехватывается: процесс падает,
     Docker перезапускает (R1). Закрытия 1h/1d сюда не попадают вовсе: они обновляют состояние старших таймфреймов (orchestrator/mtf.py);
  4. каждый сигнал: «устарел» (свеча закрылась давно: replay после обрыва) -> skipped_stale; фильтр сигналов (`signal_gates`: импульс на 1m для MARKET,
     новости High Impact, выходные) отказал -> skipped_filter (причина и имя фильтра в detail); монета занята -> skipped_due_to_lock (D4); приёмник
     отказал по риску -> skipped_risk, блокировка снимается; иначе -> signal_accepted. Фильтры касаются только НОВЫХ сигналов: сопровождение и закрытие
     открытых сделок идёт через приёмники и от них не зависит.

Маршрут задаётся на стратегию: `Route(Mode.SHADOW, VirtualTracker)` или `Route(Mode.LIVE, <LiveBarrier-адаптер, Этап 4>)`.
Область блокировки: все LIVE-стратегии делят ОДНУ блокировку (на бирже одна позиция на монету, One-Way); у каждой
SHADOW-стратегии своя (3.2: «у каждой стратегии свой виртуальный трекер»), чтобы статистика одной не искажалась другой.
Блокировку снимает закрытие сделки или отмена заявки (слушатель приёмника).
"""

import logging
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, Sequence

from orchestrator.journal import EventLog
from orchestrator.lock import CoinOwnershipLock, Owner
from orchestrator.strategies import Strategy
from orchestrator.tracker import SignalSink, VirtualTrade
from orchestrator.types import Event, FeatureContext, Mode, Signal
from persistence.recorder import NullRecorder, TradeRecorder
from utils.errors import ConfigError, TradingInfraError

log = logging.getLogger("orchestrator")

DEFAULT_MAX_SIGNAL_AGE_MS = 30_000  # событие закрытия свечи приходит за секунды; позже - это replay после обрыва


class OrchestratorError(TradingInfraError):
    """Нарушен порядок свечей или протокол маршрутизации: баг, а не рыночная ситуация (R1)."""


class SignalGate(Protocol):
    """Фильтр новых сигналов: причина отказа (строка) или None - сигнал проходит дальше."""

    name: str

    def check(self, signal: Signal, now_ms: int) -> str | None: ...


@dataclass(frozen=True)
class Route:
    mode: Mode
    sink: SignalSink


class Orchestrator:
    def __init__(
        self, strategies: Sequence[Strategy], routes: Mapping[str, Route], log_: EventLog, *,
        max_signal_age_ms: int = DEFAULT_MAX_SIGNAL_AGE_MS, recorder: TradeRecorder | None = None, snapshot_bars: int = 100,
        symbol_filter: Callable[[str, str], bool] | None = None, signal_gates: Sequence[SignalGate] = (),
    ):
        names = [s.name for s in strategies]
        if len(set(names)) != len(names):
            raise ConfigError(f"Имена стратегий не уникальны: {names}")
        missing = sorted(set(names) - set(routes))
        if missing:
            raise ConfigError(f"Нет маршрута для стратегий {missing}")
        if max_signal_age_ms <= 0:
            raise ConfigError("max_signal_age_ms должен быть > 0")
        self._strategies, self._routes, self._log = tuple(strategies), dict(routes), log_
        self._max_age = max_signal_age_ms
        self._rec: TradeRecorder = recorder or NullRecorder()
        self._snapshot_bars = snapshot_bars
        self._symbol_filter = symbol_filter  # (стратегия, символ) -> можно ли; None - всё разрешено
        self._gates = tuple(signal_gates)
        self._setup_states: dict[tuple[str, str, str], int] = {}  # (стратегия, монета, сторона) -> последнее состояние автомата
        self._locks: dict[str, CoinOwnershipLock] = {}
        self._held: dict[str, tuple[CoinOwnershipLock, str, Owner]] = {}  # signal_id -> (блокировка, монета, владелец)
        self._last_bar: dict[tuple[str, str], int] = {}
        self._seen: set[str] = set()
        for sink in {id(r.sink): r.sink for r in self._routes.values()}.values():
            sink.add_close_listener(self._on_trade_finished)

    # ------------------------------------------------------------------------------------- публичное

    def lock_for(self, strategy: str) -> CoinOwnershipLock:
        """Блокировка, в области которой работает стратегия (см. докстринг модуля)."""
        route = self._routes[strategy]
        key = "live" if route.mode is Mode.LIVE else f"shadow:{strategy}"
        return self._locks.setdefault(key, CoinOwnershipLock(key))

    def on_context(self, ctx: FeatureContext) -> tuple[Event, ...]:
        key = (ctx.symbol, ctx.interval)
        last = self._last_bar.get(key)
        if last is not None and ctx.bar_open_time != last + ctx.interval_ms:
            raise OrchestratorError(
                f"{ctx.symbol} {ctx.interval}: свеча {ctx.bar_open_time} идёт не следом за {last} (ожидалась {last + ctx.interval_ms})"
            )
        self._last_bar[key] = ctx.bar_open_time
        bar = ctx.bar
        for sink in {id(r.sink): r.sink for r in self._routes.values()}.values():
            sink.on_bar_closed(ctx.symbol, ctx.bar_open_time, ctx.last_close, ctx.received_ms, bar, interval=ctx.interval)  # приёмник считает бары только своих сделок
        age = ctx.received_ms - ctx.bar_close_ms
        events: list[Event] = []
        for strategy in self._strategies:
            want = getattr(strategy, "interval", None)
            if want is not None and want != ctx.interval:
                continue  # у стратегии свой рабочий таймфрейм (SMC_Pro_Trend - 5m, остальные - 15m): на чужой свече она не считается (гонка 1h/1d против 5m)
            if self._symbol_filter is not None and not self._symbol_filter(strategy.name, ctx.symbol):
                continue  # стратегия на этом символе запрещена (напр. OBI_Breakout без L2-стакана): даже не считается
            analyze = getattr(strategy, "analyze", None)  # стратегия с автоматом состояний отдаёт и сигналы, и состояния за один расчёт
            signals, states = analyze(ctx) if analyze is not None else (strategy.detect(ctx), {})
            self._log_setup_states(strategy.name, ctx, states)
            for signal in signals:
                events.append(self._route(signal, ctx, stale=age > self._max_age, age=age))
        return tuple(events)

    def _log_setup_states(self, strategy: str, ctx: FeatureContext, states: dict[str, dict]) -> None:
        """Переходы автомата сетапа (S0..S4) по сторонам - в журнал (и в state_events БД). Первое наблюдение - из состояния 0."""
        for side, info in states.items():
            key = (strategy, ctx.symbol, side)
            prev, cur = self._setup_states.get(key, 0), int(info["state"])
            self._setup_states[key] = cur
            if cur != prev:
                detail = {"side": side, "from": prev, "to": cur, **({"reject": info["reject"]} if info.get("reject") else {})}
                self._log.emit(Event(ctx.received_ms, "setup_state", ctx.symbol, strategy, None, detail))

    def adopt_held(self, strategy: str, symbol: str, trade_id: str) -> None:
        """После перезапуска: сделка пережила процесс (позиция/защита на бирже) - вернуть её блокировку монеты (D4)."""
        lock, owner = self.lock_for(strategy), Owner(strategy, trade_id)
        if not lock.try_acquire(symbol, owner).acquired:
            raise OrchestratorError(f"{symbol}: монета занята {lock.owner(symbol)}, сделку {trade_id} вернуть нельзя")
        self._held[trade_id] = (lock, symbol, owner)
        self._seen.add(trade_id)

    def held(self) -> dict[str, tuple[str, str]]:
        """signal_id -> (монета, имя блокировки): что удерживается сейчас (для диагностики и тестов)."""
        return {sid: (sym, lock.name) for sid, (lock, sym, _) in self._held.items()}

    # ------------------------------------------------------------------------------------- внутреннее

    def _route(self, signal: Signal, ctx: FeatureContext, *, stale: bool, age: int) -> Event:
        if signal.signal_id in self._seen:
            raise OrchestratorError(f"Сигнал {signal.signal_id} получен повторно")
        self._seen.add(signal.signal_id)
        route = self._routes[signal.strategy]
        base = {"mode": route.mode.value, "kind": signal.kind.value, "direction": signal.direction.value}
        if stale:
            return self._event(ctx, "skipped_stale", signal, {**base, "age_ms": age})
        for gate in self._gates:
            reason = gate.check(signal, ctx.received_ms)
            if reason is not None and getattr(gate, "tag_only", False):  # фильтр-метка (новости, 2026-10-24): сигнал идёт дальше с пометкой в meta
                signal.meta[gate.name] = reason
                self._event(ctx, f"tagged_{gate.name}", signal, {**base, "reason": reason})
            elif reason is not None:
                return self._event(ctx, "skipped_filter", signal, {**base, "filter": gate.name, "reason": reason})
        lock = self.lock_for(signal.strategy)
        owner = Owner(signal.strategy, signal.signal_id)
        acquisition = lock.try_acquire(signal.symbol, owner)
        if not acquisition.acquired:
            held = acquisition.held_by
            return self._event(ctx, "skipped_due_to_lock", signal,
                               {**base, "lock": lock.name, "held_by": {"strategy": held.strategy, "trade_id": held.trade_id}})
        submission = route.sink.submit(signal, ctx.received_ms)
        if not submission.accepted:
            lock.release(signal.symbol, owner)
            return self._event(ctx, "skipped_risk", signal, {**base, "reason": submission.reason})
        self._held[signal.signal_id] = (lock, signal.symbol, owner)
        # снапшот входа (R7): свечи + геометрия сетапа; строка сделки уже поставлена в очередь записи приёмником (внешний ключ)
        self._rec.snapshot({"trade_id": signal.signal_id, "strategy_id": signal.strategy_id, "created_at": ctx.received_ms, "candles": ctx.snapshot_candles(self._snapshot_bars),
                            "geometry": list(signal.geometry), "meta": {**signal.meta, "entry": signal.entry, "sl": signal.sl, "tp": signal.tp,
                                                                         "inval": signal.inval, "interval": signal.interval}})
        return self._event(ctx, "signal_accepted", signal, {**base, "entry": signal.entry, "sl": signal.sl, "tp": signal.tp, "lock": lock.name})

    def _on_trade_finished(self, trade: VirtualTrade) -> None:
        held = self._held.pop(trade.trade_id, None)
        if held is None:
            raise OrchestratorError(f"Закрыта сделка {trade.trade_id}, которой оркестратор не владел")
        lock, symbol, owner = held
        lock.release(symbol, owner)

    def _event(self, ctx: FeatureContext, kind: str, signal: Signal, detail: dict) -> Event:
        event = Event(ctx.received_ms, kind, signal.symbol, signal.strategy, signal.signal_id, detail)
        self._log.emit(event)
        if kind != "signal_accepted":
            log.info("%s %s %s: %s", signal.symbol, signal.strategy, kind, detail)
        return event
