"""Сопровождение открытых MT5-позиций ПЛАНОМ ВЫХОДА стратегии (`ScaleOutPlan`) - та же математика, что у `VirtualTracker` (Shadow).

На Binance уровни Scale Out и стоп стоят на бирже отдельными algo-ордерами. У MT5 SL/TP - поля позиции, отдельных ордеров на частичные
тейки нет, поэтому уровни исполняет бот: цена сделки дошла до уровня -> `close_position(position_id, volume)` (доля в лотах на сетке
`volumeStep`), стоп переносится `modify_position`. Стоп брокера стоит на позиции всё время (R6): сработал - позиция исчезла.

Правила (зеркало VirtualTracker, R8):
  * R = |фактическая цена входа - SL|; уровни = вход -+ r * R, от ФАКТИЧЕСКОЙ цены входа (MARKET - по факту, LIMIT - по цене заявки).
  * Уровень достигнут, когда цена сделки (тот же поток тиков, что у Shadow) дошла до него; несколько уровней за раз (гэп) закрываются подряд.
  * Безубыток: mark price коснулась +1R (D11) ИЛИ исполнен первый уровень (`be_after_first`): `modify_position(sl=вход, tp=...)`, только к прибыли.
  * Runner (остаток): трейлинг по закрытой свече (`atr`: лучшая цена -+ k*ATR; `wick`: экстремум последних N свечей), вступает в силу
    со следующего опроса; стоп только подтягивается и не лежит дальше безубытка.
  * Тайм-аут: `max_hold_bars` свечей после входа -> закрытие остатка по рынку.
  * Брокерский TP: при плане БЕЗ runner'а - цена последнего уровня (брокер закроет остаток и при выключенном боте); при runner'е TP нет
    (иначе брокер закрыл бы и runner на последнем уровне, а Shadow ведёт его трейлингом).
Устойчивость к сбоям: состояние берётся у брокера. Выполненные уровни выводятся из ФАКТИЧЕСКИХ лотов позиции (`lots <= остаток после уровня k`),
а объём закрытия считается как «лоты сейчас - остаток после уровня», поэтому повтор после неизвестного исхода или рестарта не закроет лишнего.

Закрытый рынок (retcode 10018 / `MarketClosedError`) - не авария и не Safe Mode: действие остаётся ожидающим, в журнал один раз
пишется `deferred`, при следующем опросе оно повторяется, при успехе пишется `replayed`. Неизвестный исход запроса (таймаут, обрыв) -
повтор в следующий опрос (все операции идемпотентны по лотам/цене). «Стоп уже за ценой» при переносе (INVALID_STOPS 10016) - выход по рынку
с причиной `be`/`trail` без Safe Mode (аналог -2021 на Binance). Любая другая ошибка брокера - исключение (R1).
"""

import logging
import math
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Mapping

from exchange.mt5_metaapi import MarketClosedError, MetaApiLiveClient, MetaApiPosition, MetaApiTradeError
from exchange.symbol_mapper import Mt5SymbolSpec
from features import defaults as D
from orchestrator.live_risk import split_levels
from orchestrator.tracker import ScaleOutPlan
from orchestrator.types import BarInfo, Tick
from utils.errors import DataError, TradingInfraError

log = logging.getLogger("mt5_exit")

_EPS = 1e-9
RETCODE_INVALID_STOPS = 10016


class Mt5ExitError(TradingInfraError):
    """Нарушена согласованность сопровождения MT5-позиции (R1): данные для трейлинга, объёмы, состояние брокера."""


@dataclass(frozen=True)
class Mt5ExitEvent:
    kind: str  # partial_close | be_armed | trail_moved | closed | deferred | replayed | plan_not_representable
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Mt5Exit:
    """Состояние сопровождения одной MT5-позиции (пишется на диск вместе со сделкой)."""

    plan: ScaleOutPlan
    sgn: float  # +1 для шорта, -1 для лонга («шортовый мир», как в VirtualTracker)
    entry: float  # фактическая цена входа (позиция у брокера)
    sl0: float
    risk: float
    initial_lots: float
    levels: list[list[float]]  # [[r, цена, лоты], ...]
    runner_lots: float
    stop_price: float
    stop_kind: str = "sl"  # sl | be | trail - причина выхода, если стоп брокера сработает
    tp_price: float | None = None  # брокерский TP (только у плана без runner'а)
    levels_done: int = 0
    be_armed: bool = False
    be_pending: bool = False
    trail_target: float | None = None
    best_q: float = 0.0  # лучшая цена с входа в «шортовом мире» (sgn * цена): ниже - лучше
    last_q: float | None = None  # последняя цена сделки в «шортовом мире»
    bars_closed: int = 0
    fill_bar_no: int = 1
    recent_bars: list[list[float]] = field(default_factory=list)  # [high, low] последних закрытых свечей
    forced_reason: str | None = None  # ожидает закрытия остатка по рынку: timeout | be | trail | fill_beyond_stop | plan_not_representable
    deferred: list[str] = field(default_factory=list)  # действия, отложенные из-за закрытого рынка (уже записаны в журнал)

    def expected_after(self, k: int) -> float:
        """Лоты позиции после исполнения уровня k (0-based); у плана без runner'а после последнего уровня - 0."""
        return round(self.initial_lots - sum(lvl[2] for lvl in self.levels[: k + 1]), 8)

    def to_record(self) -> dict[str, Any]:
        out = {k: v for k, v in self.__dict__.items() if k != "plan"}
        out["plan"] = self.plan.describe()
        return out

    @classmethod
    def from_record(cls, rec: Mapping[str, Any]) -> "Mt5Exit":
        try:
            ep = rec["plan"]
            plan = ScaleOutPlan(tuple((float(r), float(f)) for r, f in ep["levels"]), bool(ep["be_after_first"]), ep["trail_mode"],
                                float(ep["trail_atr_mult"]), int(ep["trail_wick_bars"]))
            data = {k: v for k, v in rec.items() if k != "plan"}
            return cls(plan=plan, **data)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Запись сопровождения MT5-позиции не читается ({exc!r}): {str(rec)[:300]}") from exc


def lot_filters(spec: Mt5SymbolSpec) -> Any:
    """Сетка лота брокера в виде, который понимает `split_levels` (step_size / min_qty)."""
    return SimpleNamespace(step_size=spec.volume_step, min_qty=spec.min_volume)


def level_price(entry: float, sgn: float, r: float, risk: float, spec: Mt5SymbolSpec) -> float:
    return _snap(entry - sgn * r * risk, spec)


def _snap(price: float, spec: Mt5SymbolSpec) -> float:
    return round(round(price / spec.tick_size) * spec.tick_size, spec.digits)


def broker_tp(plan: ScaleOutPlan, entry: float, sl: float, sgn: float, spec: Mt5SymbolSpec) -> float | None:
    """Брокерский TP: цена последнего уровня, если у плана нет runner'а; иначе None (runner не должен закрываться брокером)."""
    if plan.runner_frac > _EPS:
        return None
    return level_price(entry, sgn, plan.last_r, abs(entry - sl), spec)


def build_exit(plan: ScaleOutPlan, pos: MetaApiPosition, sl: float, spec: Mt5SymbolSpec) -> Mt5Exit:
    """Состояние сопровождения для только что открытой позиции. Если закрыть надо сразу (вход за стопом, план не делится) - `forced_reason`."""
    sgn = -1.0 if pos.side == "BUY" else 1.0
    entry = pos.open_price
    risk = abs(entry - sl)
    stop = pos.stop_loss if pos.stop_loss is not None else sl
    split = split_levels(plan, pos.lots, lot_filters(spec))
    base = dict(plan=plan, sgn=sgn, entry=entry, sl0=sl, risk=risk, initial_lots=pos.lots, runner_lots=0.0, stop_price=stop, best_q=sgn * entry)
    if not sgn * sl > sgn * entry:
        return Mt5Exit(levels=[], **base, forced_reason="fill_beyond_stop")  # вход исполнен уже за стопом (гэп)
    if split is None:
        return Mt5Exit(levels=[], **base, forced_reason="plan_not_representable")
    levels = [[r, level_price(entry, sgn, r, risk, spec), lots] for r, lots in split[0]]
    base["runner_lots"] = split[1]
    return Mt5Exit(levels=levels, **base, tp_price=broker_tp(plan, entry, sl, sgn, spec))


# ------------------------------------------------------------------------------------------------ тики и свечи


def on_tick(ex: Mt5Exit, tick: Tick, be_trigger_r: float = D.BE_TRIGGER_R) -> None:
    """Те же тики, что получает VirtualTracker: лучшая и последняя цена - по сделкам, безубыток +1R - по mark price (D11)."""
    q = ex.sgn * tick.price
    if tick.kind == "mark":
        if not ex.be_armed and not ex.be_pending and q <= ex.sgn * ex.entry - be_trigger_r * ex.risk:
            ex.be_pending = True
    else:
        ex.best_q = min(ex.best_q, q)
        ex.last_q = q


def on_bar_closed(ex: Mt5Exit, bar: BarInfo | None, spec: Mt5SymbolSpec, max_hold_bars: int) -> None:
    """Закрытие свечи: тайм-аут, трейлинг runner'а (как VirtualTracker._trail; исполняется следующим опросом)."""
    ex.bars_closed += 1
    if ex.bars_closed - ex.fill_bar_no >= max_hold_bars:
        ex.forced_reason = ex.forced_reason or "timeout"
        return
    plan = ex.plan
    if bar is not None and bar.high is not None and bar.low is not None:
        ex.recent_bars = (ex.recent_bars + [[bar.high, bar.low]])[-plan.trail_wick_bars:]
    if not runner_active(ex):
        return
    if bar is None:
        raise Mt5ExitError("runner без данных закрытой свечи - трейлинг невозможен")
    if plan.trail_mode == "atr":
        if bar.atr is None or not math.isfinite(bar.atr) or bar.atr <= 0:
            raise Mt5ExitError(f"нет ATR для трейлинга ({bar.atr!r})")
        candidate = ex.best_q + plan.trail_atr_mult * bar.atr
    else:
        if not ex.recent_bars:
            return
        candidate = max(max(ex.sgn * h, ex.sgn * lo) for h, lo in ex.recent_bars)
    if candidate < ex.sgn * ex.stop_price - 1e-12:
        ex.trail_target = _snap(ex.sgn * candidate, spec)


def runner_active(ex: Mt5Exit) -> bool:
    return ex.plan.runner_frac > _EPS and bool(ex.levels) and ex.levels_done == len(ex.levels)


# ------------------------------------------------------------------------------------------------ шаг сопровождения


def _market_closed(exc: MetaApiTradeError) -> bool:
    return isinstance(exc, MarketClosedError)


async def step(ex: Mt5Exit, client: MetaApiLiveClient, pos: MetaApiPosition, spec: Mt5SymbolSpec) -> list[Mt5ExitEvent]:
    """Один опрос открытой позиции: выполненные уровни (по лотам брокера) -> уровни по цене -> безубыток/трейлинг -> тайм-аут/принудительный выход.

    `pos` - свежая позиция у брокера. Возвращает события; полное закрытие - событие `closed` (позиции у брокера больше нет)."""
    events: list[Mt5ExitEvent] = []
    lots = pos.lots
    # 1) что исполнено у брокера (в т.ч. за время простоя / после неизвестного исхода): выводим из лотов
    while ex.levels_done < len(ex.levels) and lots <= ex.expected_after(ex.levels_done) + _EPS:
        k = ex.levels_done
        ex.levels_done += 1
        events.append(Mt5ExitEvent("partial_close", {"reason": _level_reason(ex, k), "lots": ex.levels[k][2], "remaining": lots, "source": "broker_lots"}))
    if ex.levels_done >= 1 and ex.plan.be_after_first and not ex.be_armed:
        ex.be_pending = True
    # 2) принудительный выход (тайм-аут, «стоп уже за ценой», вход за стопом, план не делится)
    if ex.forced_reason is not None:
        return events + await _close_all(ex, client, pos, ex.forced_reason, "forced")
    # 3) уровни по цене сделки (как VirtualTracker._scale_out: подряд, пока цена за уровнем)
    while ex.levels_done < len(ex.levels) and ex.last_q is not None:
        k = ex.levels_done
        r, price, lvl_lots = ex.levels[k]
        if ex.last_q > ex.sgn * ex.entry - r * ex.risk:
            break
        target = ex.expected_after(k)
        if target <= _EPS:  # последний уровень без runner'а: остаток целиком
            events += await _close_all(ex, client, pos, "tp", f"level_{k}")
            return events
        volume = round(lots - target, 8)
        try:
            await client.close_position(pos.position_id, volume)
        except MetaApiTradeError as exc:
            if _market_closed(exc):
                events += _defer(ex, f"level_{k}", exc)
                break
            if exc.outcome_unknown:
                log.warning("%s: исход частичного закрытия неизвестен (%s): сверка по лотам в следующем опросе", pos.symbol, exc)
                break
            raise
        events += _replayed(ex, f"level_{k}")
        ex.levels_done += 1
        lots = target
        events.append(Mt5ExitEvent("partial_close", {"reason": _level_reason(ex, k), "lots": volume, "price": price, "remaining": lots, "source": "bot"}))
        if ex.levels_done == 1 and ex.plan.be_after_first:
            ex.be_pending = True
    # 4) безубыток и трейлинг: стоп переносится только к прибыли
    events += await _apply_stop(ex, client, pos, spec)
    return events


def _level_reason(ex: Mt5Exit, k: int) -> str:
    last = len(ex.levels) - 1
    return "tp" if k == last and ex.plan.runner_frac <= _EPS else f"scale_{ex.levels[k][0]:g}r"


async def _apply_stop(ex: Mt5Exit, client: MetaApiLiveClient, pos: MetaApiPosition, spec: Mt5SymbolSpec) -> list[Mt5ExitEvent]:
    sgn = ex.sgn
    price, kind = ex.stop_price, ex.stop_kind
    be_requested = ex.be_pending
    if be_requested:
        be_price = _snap(ex.entry, spec)
        if sgn * be_price < sgn * price:
            price, kind = be_price, "be"
    trail = ex.trail_target
    if trail is not None and sgn * trail < sgn * price - 1e-12:
        price, kind = trail, "trail"
    if price == ex.stop_price:
        ex.be_armed = ex.be_armed or be_requested
        ex.be_pending, ex.trail_target = False, None
        return []
    key = "be" if kind == "be" else "trail"
    try:
        await client.modify_position(pos.position_id, sl=price, tp=ex.tp_price)
    except MetaApiTradeError as exc:
        if _market_closed(exc):
            return _defer(ex, key, exc)
        if exc.numeric_code == RETCODE_INVALID_STOPS:  # цена уже за новым стопом: выход по рынку (BE/трейлинг-выход), не авария
            ex.forced_reason = kind
            return await _close_all(ex, client, pos, kind, "stop_too_close")
        if exc.outcome_unknown:
            log.warning("%s: исход переноса стопа неизвестен (%s): повтор в следующем опросе", pos.symbol, exc)
            return []
        raise
    events = _replayed(ex, key)
    ex.stop_price, ex.stop_kind = price, kind
    if be_requested:
        ex.be_armed, ex.be_pending = True, False
        if kind == "be":
            events.append(Mt5ExitEvent("be_armed", {"stop": price}))
    if ex.trail_target is not None:
        ex.trail_target = None
        if kind == "trail":
            events.append(Mt5ExitEvent("trail_moved", {"stop": price, "mode": ex.plan.trail_mode}))
    return events


async def _close_all(ex: Mt5Exit, client: MetaApiLiveClient, pos: MetaApiPosition, reason: str, key: str) -> list[Mt5ExitEvent]:
    """Закрыть остаток по рынку. Закрытый рынок - отложено (ex.forced_reason остаётся, повтор при следующем опросе)."""
    try:
        await client.close_position(pos.position_id)
    except MetaApiTradeError as exc:
        if reason not in ("tp",):
            ex.forced_reason = reason  # повторить, пока рынок не откроется
        if _market_closed(exc):
            return _defer(ex, f"close_{key}", exc)
        if exc.outcome_unknown:
            log.warning("%s: исход закрытия неизвестен (%s): сверка в следующем опросе", pos.symbol, exc)
            return []
        raise
    events = _replayed(ex, f"close_{key}")
    events.append(Mt5ExitEvent("closed", {"reason": reason, "lots": pos.lots, "position_id": pos.position_id, "source": "bot"}))
    return events


def _defer(ex: Mt5Exit, key: str, exc: MetaApiTradeError) -> list[Mt5ExitEvent]:
    """Рынок закрыт: команда не потеряна (состояние не продвинуто), в журнал - один раз на действие."""
    if key in ex.deferred:
        return []
    ex.deferred.append(key)
    return [Mt5ExitEvent("deferred", {"action": key, "code": exc.numeric_code, "why": "market_closed"})]


def _replayed(ex: Mt5Exit, key: str) -> list[Mt5ExitEvent]:
    if key in ex.deferred:
        ex.deferred.remove(key)
        return [Mt5ExitEvent("replayed", {"action": key})]
    return []


def vanished_reason(ex: Mt5Exit) -> str:
    """Позиция исчезла у брокера без нашего закрытия: сработал SL брокера (sl / be / trail) либо брокерский TP последнего уровня."""
    if ex.forced_reason is not None:
        return ex.forced_reason
    if ex.tp_price is not None and ex.levels_done >= len(ex.levels) - 1 and ex.last_q is not None and ex.last_q <= ex.sgn * ex.tp_price + 1e-12:
        return "tp"
    return ex.stop_kind
