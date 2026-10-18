"""Live Risk Manager: единственные ворота между сигналом Оркестратора и биржей.

Сигнал проходит через `evaluate()` и либо превращается в `LiveOrderPlan` (всё уже рассчитано и проверено), либо получает
`LiveRejected(reason)`. Отказ - обычный исход, поэтому значение, а не исключение; неверные входные данные и поломка файла
защёлки - исключение (R1). Менеджер синхронный и чистый (время и PnL ему передают): всё, что требует сети (баланс,
суточный результат с биржи, плечо), делает `LiveExecutionEngine` до вызова `evaluate`.

Правила (все жёсткие, без «мягких» режимов):
  1. Фильтр стратегий `approved_strategies`: чужие сигналы получают `strategy_not_approved` (Shadow-трекер их по-прежнему
     пишет - это решает `LiveMirrorSink`, менеджер лишь страхует).
  2. Фиксированный объём: позиция = `margin_usdt * leverage` в USDT (по умолчанию 10 USDT маржи с плечом 10x = 100 USDT),
     объём - вниз до шага лота. Риск сделки = объём * |вход - SL| (не «1% от депозита», как в Shadow): расстояние до стопа
     разное, поэтому проверяется потолок, см. п. 4.
  3. Суточный лимит убытка (UTC): результат счёта с 00:00 UTC (реализованный PnL + комиссии + фандинг, данные биржи)
     <= -`daily_loss_limit_usdt` -> торговля полностью блокируется до 00:00 UTC следующих суток. Блокировка - защёлка в файле:
     рестарт процесса её не снимает. Нет свежего значения PnL (старше `pnl_max_age_s` или не получено) - новых сделок нет
     (fail-closed). Уже открытые позиции лимит не закрывает: у них есть стоп на бирже, а рыночное закрытие по панике
     фиксирует убыток без нужды (решение, см. CLAUDE.md разд. 3.23).
  4. Стоп ближе ликвидации: расстояние до SL < `liq_safety / leverage` от входа (при 10x и 0.8 - меньше 8%). Дальний стоп
     означает, что позицию ликвидирует раньше, чем он сработает.
  5. Одна позиция на монету (One-Way Mode: две позиции разных стратегий по одной монете биржа сольёт в одну чистую).
  6. Сквозной суточный лимит (мульти-биржевой контур, 2026-10-13): `Total Daily PnL = Binance PnL + MT5 PnL` (каждый - реализованный результат
     счёта с 00:00 UTC, в USD/USDT 1:1). Лимит одинаков для всей системы: суммарный убыток >= лимита блокирует новые входы на ВСЕХ площадках
     сразу (защёлка одна). Результат каждой площадки обязан быть свежим (`pnl_max_age_s`), иначе новых сделок нет на ВСЕХ площадках
     (`pnl_unavailable`, fail-closed: без одного из слагаемых суммы нет). Набор площадок задаёт `venues` (по умолчанию только Binance).
"""

import json
import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import config
from execution.exchange_filters import SymbolFilters
from orchestrator.tracker import DEFAULT_PLAN, ScaleOutPlan
from orchestrator.types import Direction, Kind, Signal
from utils.errors import ConfigError, DataError
from utils.storage import atomic_write_text

log = logging.getLogger("live_risk")

DAY_MS = 86_400_000
_EPS = 1e-9
MAX_FRAC_ERROR = Decimal("0.02")  # доля уровня на сетке лота может отличаться от доли плана не больше чем на 2% позиции


def utc_day_start_ms(ts_ms: int) -> int:
    """Начало суток UTC (00:00:00.000), в которые попадает `ts_ms`."""
    return ts_ms - ts_ms % DAY_MS


@dataclass(frozen=True)
class LiveOrderPlan:
    """Всё, что нужно исполнителю: проверено менеджером, пересчитывать нечего."""

    signal_id: str
    strategy: str
    symbol: str
    direction: Direction
    kind: Kind
    side: str  # BUY | SELL: сторона входа
    qty: float  # на сетке лота
    entry: float  # LIMIT: цена заявки (на сетке); MARKET: справочная цена сигнала
    sl: float  # на сетке цены
    tp: float | None  # LIMIT: из сигнала (на сетке); MARKET: считается от фактической цены входа
    inval: float | None
    leverage: int
    margin_usdt: float  # маржа, которую займёт позиция (= notional / leverage)
    notional_usdt: float
    max_loss_usdt: float  # потеря при срабатывании SL, без комиссий и проскальзывания
    exit_plan: ScaleOutPlan  # ТОТ ЖЕ план управления позицией, что у VirtualTracker для этой стратегии (Scale Out + безубыток + runner)

    @property
    def close_side(self) -> str:
        return "SELL" if self.side == "BUY" else "BUY"


@dataclass(frozen=True)
class LiveRejected:
    reason: str  # trading_blocked | pnl_unavailable | strategy_not_approved | symbol_busy | below_min_size | plan_not_representable | sl_too_far | bad_risk
    detail: dict[str, Any] | None = None


class LiveRiskManager:
    def __init__(
        self,
        filters: Mapping[str, SymbolFilters],
        *,
        approved_strategies: Collection[str],
        margin_usdt: float,
        leverage: int,
        daily_loss_limit_usdt: float,
        state_path: Path,
        liq_safety: float = config.LIVE_LIQ_SAFETY,
        pnl_max_age_s: float = config.LIVE_PNL_MAX_AGE_S,
        plans: Mapping[str, ScaleOutPlan] | None = None,
        venues: Collection[str] = ("binance",),
        max_open_positions: int = config.LIVE_MAX_OPEN_POSITIONS,
    ):
        if not (margin_usdt > 0 and isinstance(leverage, int) and 1 <= leverage <= 125 and daily_loss_limit_usdt > 0):
            raise ConfigError("LiveRiskManager: margin_usdt > 0, leverage целое 1..125, daily_loss_limit_usdt > 0")
        if not (0 < liq_safety <= 1 and pnl_max_age_s > 0):
            raise ConfigError("LiveRiskManager: liq_safety в (0, 1], pnl_max_age_s > 0")
        approved = tuple(approved_strategies)
        if not approved or len(set(approved)) != len(approved):
            raise ConfigError(f"LiveRiskManager: approved_strategies пуст или с дублями: {approved}")
        shadow_only = sorted(set(approved) & set(config.SHADOW_ONLY_STRATEGIES))
        if shadow_only:
            raise ConfigError(f"LiveRiskManager: стратегии {shadow_only} работают только в Shadow (config.SHADOW_ONLY_STRATEGIES)")
        if not (isinstance(max_open_positions, int) and max_open_positions >= 1):
            raise ConfigError(f"LiveRiskManager: max_open_positions - целое >= 1, сейчас {max_open_positions!r}")
        self.max_open_positions = max_open_positions
        self._filters = dict(filters)
        self.approved = frozenset(approved)
        self.margin_usdt, self.leverage, self.daily_loss_limit = float(margin_usdt), leverage, float(daily_loss_limit_usdt)
        self._liq_safety, self._pnl_max_age_ms = liq_safety, int(pnl_max_age_s * 1000)
        self._plans = dict(plans or {})  # strategy_id -> план выхода (нет в словаре - DEFAULT_PLAN, как у VirtualTracker)
        self._state_path = Path(state_path)
        self._busy: dict[str, str] = {}  # монета -> signal_id (резерв с момента одобрения до release)
        self._venues = tuple(venues)
        if not self._venues or len(set(self._venues)) != len(self._venues):
            raise ConfigError(f"LiveRiskManager: venues пуст или с дублями: {self._venues}")
        self._venue_pnl: dict[str, tuple[float, int]] = {}  # площадка -> (результат за сутки UTC, момент получения)
        self._blocked_until_ms = self._load_latch()

    def add_filters(self, filters: Mapping[str, SymbolFilters]) -> None:
        """Фильтры биржи для монет, вошедших в live-уровень на лету (автоселектор): до включения монеты в набор live, иначе `evaluate` падал бы с ConfigError."""
        self._filters.update(filters)

    # ------------------------------------------------------------------------------------- защёлка

    def _load_latch(self) -> int:
        try:
            raw = self._state_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return 0
        try:
            return int(json.loads(raw)["blocked_until_ms"])
        except (ValueError, KeyError, TypeError) as exc:
            raise DataError(f"Файл защёлки риска {self._state_path} повреждён ({exc}); начало: {raw[:200]!r}") from exc

    def _trip(self, now_ms: int, pnl: float) -> None:
        self._blocked_until_ms = utc_day_start_ms(now_ms) + DAY_MS
        by_venue = {v: p for v, (p, _) in self._venue_pnl.items()}
        payload = {"blocked_until_ms": self._blocked_until_ms, "tripped_at_ms": now_ms, "pnl_usdt": pnl,
                   "limit_usdt": self.daily_loss_limit, "pnl_by_venue": by_venue}
        atomic_write_text(self._state_path, json.dumps(payload) + "\n")
        log.critical("СУТОЧНЫЙ ЛИМИТ УБЫТКА: %.2f USDT суммарно по площадкам %s (лимит %.2f). Торговля заблокирована на ВСЕХ площадках до %d (00:00 UTC)",
                     pnl, by_venue, self.daily_loss_limit, self._blocked_until_ms)

    def blocked(self, now_ms: int) -> bool:
        return now_ms < self._blocked_until_ms

    @property
    def blocked_until_ms(self) -> int:
        return self._blocked_until_ms

    @property
    def venues(self) -> tuple[str, ...]:
        return self._venues

    @property
    def venue_pnl(self) -> dict[str, float]:
        """Последний известный суточный результат по площадкам (то, что уже получено)."""
        return {v: p for v, (p, _) in self._venue_pnl.items()}

    @property
    def daily_pnl(self) -> float | None:
        """Total Daily PnL = сумма по ВСЕМ площадкам; None, пока какая-то площадка ни разу не сообщила результат."""
        if any(v not in self._venue_pnl for v in self._venues):
            return None
        return sum(p for p, _ in self._venue_pnl.values())

    def stale_venues(self, now_ms: int) -> list[str]:
        """Площадки без свежего результата (не сообщали или старше `pnl_max_age_s`)."""
        return [v for v in self._venues if v not in self._venue_pnl or now_ms - self._venue_pnl[v][1] > self._pnl_max_age_ms]

    def update_venue_pnl(self, venue: str, pnl_usdt: float, now_ms: int) -> bool:
        """Результат одной площадки с 00:00 UTC (убыток отрицателен). True - суммарный лимит достигнут ИМЕННО сейчас (блокировка включена).

        Решение о блокировке принимается по сумме последних известных результатов ВСЕХ площадок, как только все они сообщили хотя бы раз.
        Граница: суммарный убыток >= лимита блокирует (лимит - допустимая потеря, а не «ещё одна сделка сверху»)."""
        if venue not in self._venues:
            raise ConfigError(f"Площадка {venue!r} не зарегистрирована в LiveRiskManager (venues={self._venues})")
        if pnl_usdt != pnl_usdt:  # NaN
            raise DataError(f"Суточный PnL площадки {venue} не число (NaN)")
        self._venue_pnl[venue] = (float(pnl_usdt), now_ms)
        total = self.daily_pnl
        if total is not None and -total >= self.daily_loss_limit - _EPS and not self.blocked(now_ms):
            self._trip(now_ms, total)
            return True
        return False

    def update_daily_pnl(self, pnl_usdt: float, now_ms: int) -> bool:
        """Совместимость с однобиржевым контуром: результат площадки `binance`."""
        return self.update_venue_pnl("binance", pnl_usdt, now_ms)

    # ---------------------------------------------------------------------------------------- решение

    def is_approved(self, strategy: str) -> bool:
        return strategy in self.approved

    def gate(self, signal: Signal, now_ms: int) -> LiveRejected | None:
        """Общие для всех площадок ворота: стратегия одобрена, не больше `max_open_positions` одновременных позиций (остальные сигналы остаются виртуальными в Shadow), нет блокировки по суточному лимиту (ОДНА на все площадки), свежий PnL ВСЕХ
        площадок, монета свободна. Резерв не ставит. None - можно считать размер (на своей площадке)."""
        if not self.is_approved(signal.strategy):
            return LiveRejected("strategy_not_approved")
        if self.blocked(now_ms):
            return LiveRejected("trading_blocked", {"blocked_until_ms": self._blocked_until_ms, "pnl_by_venue": self.venue_pnl})
        stale = self.stale_venues(now_ms)
        if stale:
            return LiveRejected("pnl_unavailable", {"stale_venues": stale, "pnl_age_ms": {
                v: (None if v not in self._venue_pnl else now_ms - self._venue_pnl[v][1]) for v in stale}})
        if signal.symbol in self._busy:
            return LiveRejected("symbol_busy", {"held_by": self._busy[signal.symbol]})
        if len(self._busy) >= self.max_open_positions:  # занятые монеты = открытые позиции + заявки до исполнения, по всем площадкам
            return LiveRejected("max_open_positions", {"open": len(self._busy), "max": self.max_open_positions})
        return None

    def reserve(self, symbol: str, signal_id: str) -> None:
        """Резерв монеты для площадки со своим расчётом размера (MT5): до `release`."""
        if symbol in self._busy:
            raise DataError(f"{symbol}: уже удерживается {self._busy[symbol]}")
        self._busy[symbol] = signal_id

    def evaluate(self, signal: Signal, now_ms: int) -> LiveOrderPlan | LiveRejected:
        """Одобряет сигнал (и резервирует монету до `release`) или отказывает. Порядок проверок - от дешёвых и общих к частным."""
        rejected = self.gate(signal, now_ms)
        if rejected is not None:
            return rejected
        f = self._filters.get(signal.symbol)
        if f is None:
            raise ConfigError(f"{signal.symbol}: нет фильтров биржи, боевой ордер рассчитать нельзя")
        sgn = signal.direction.sgn
        entry = _to_grid(signal.entry, f.tick_size) if signal.kind is Kind.LIMIT else signal.entry
        sl = _to_grid(signal.sl, f.tick_size)
        if not sgn * sl > sgn * entry:  # округление свело стоп со входом
            return LiveRejected("bad_risk", {"entry": entry, "sl": sl})
        distance = abs(sl - entry) / entry
        if distance >= self._liq_safety / self.leverage:
            return LiveRejected("sl_too_far", {"distance": distance, "max": self._liq_safety / self.leverage})
        notional_target = self.margin_usdt * self.leverage
        qty = _floor_to_step(notional_target / entry, f.step_size)
        if qty < f.min_qty - _EPS or qty * entry < f.min_notional:
            return LiveRejected("below_min_size", {"qty": qty, "notional": qty * entry, "min_notional": f.min_notional})
        tp = _to_grid(signal.tp, f.tick_size) if signal.tp is not None else None
        exit_plan = self._plans.get(signal.strategy, DEFAULT_PLAN)
        if split_levels(exit_plan, qty, f) is None:  # Scale Out не делится на сетку лота: торговать по другой математике нельзя
            return LiveRejected("plan_not_representable", {"qty": qty, "step": f.step_size, "min_qty": f.min_qty,
                                                           "levels": [list(x) for x in exit_plan.levels], "runner_frac": exit_plan.runner_frac})
        notional = qty * entry
        plan = LiveOrderPlan(
            signal_id=signal.signal_id, strategy=signal.strategy, symbol=signal.symbol, direction=signal.direction, kind=signal.kind,
            side="SELL" if signal.direction is Direction.SHORT else "BUY", qty=qty, entry=entry, sl=sl, tp=tp, inval=signal.inval,
            leverage=self.leverage, margin_usdt=notional / self.leverage, notional_usdt=notional,
            max_loss_usdt=qty * abs(entry - sl), exit_plan=exit_plan,
        )
        self._busy[signal.symbol] = signal.signal_id
        return plan

    def release(self, symbol: str, signal_id: str) -> None:
        """Монета свободна (позиции нет, ордера сняты). Чужой или повторный release - ошибка протокола (R1)."""
        if self._busy.get(symbol) != signal_id:
            raise DataError(f"release({symbol}, {signal_id}): монета удерживается {self._busy.get(symbol)!r}")
        del self._busy[symbol]

    def adopt_busy(self, symbol: str, signal_id: str) -> None:
        """После перезапуска: на бирже есть позиция/ордера прошлого запуска - монета занята."""
        if symbol in self._busy:
            raise DataError(f"{symbol}: уже удерживается {self._busy[symbol]}")
        self._busy[symbol] = signal_id

    def busy(self) -> dict[str, str]:
        return dict(self._busy)


def _to_grid(price: float, tick: float) -> float:
    t = Decimal(repr(float(tick)))
    return float((Decimal(repr(float(price))) / t).to_integral_value(rounding=ROUND_HALF_UP) * t)


def _floor_to_step(qty: float, step: float) -> float:
    s = Decimal(repr(float(step)))
    return float((Decimal(repr(float(qty))) / s).to_integral_value(rounding=ROUND_FLOOR) * s)


def split_levels(plan: ScaleOutPlan, qty: float, f: SymbolFilters) -> tuple[tuple[tuple[float, float], ...], float] | None:
    """Делит объём позиции по плану Scale Out на сетке лота: ((уровень в R, объём), ...) и объём runner'а.

    Доли плана - от ИСХОДНОГО объёма (как у VirtualTracker); объём каждого уровня - вниз до шага лота, остаток (runner или, если
    его нет, хвост последнего уровня) - разница, поэтому сумма точно равна позиции. None - какая-то часть меньше minQty или её доля отличается от плана больше чем на MAX_FRAC_ERROR (план
    на таком объёме не воспроизводится, и менеджер отказывает вместо того, чтобы молча исполнить другую математику).
    """
    step, min_qty = Decimal(repr(float(f.step_size))), Decimal(repr(float(f.min_qty)))
    total = Decimal(repr(float(qty)))
    used = Decimal(0)
    planned: list[tuple[Decimal, Decimal]] = []  # (доля плана, объём на сетке лота)
    last = len(plan.levels) - 1
    for i, (_, frac) in enumerate(plan.levels):
        if i == last and plan.runner_frac <= _EPS:
            part = total - used  # последний уровень без runner'а забирает хвост
        else:
            part = (total * Decimal(repr(float(frac))) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        planned.append((Decimal(repr(float(frac))), part))
        used += part
    runner = total - used  # без runner'а хвост уже в последнем уровне, остаток = 0
    if plan.runner_frac > _EPS:
        planned.append((Decimal(repr(plan.runner_frac)), runner))
    if any(part < min_qty - Decimal("1e-12") or abs(part / total - frac) > MAX_FRAC_ERROR for frac, part in planned):
        return None
    return tuple((r, float(part)) for (r, _), (_, part) in zip(plan.levels, planned)), float(runner)
