"""Сопровождение открытых MT5-позиций ПЛАНОМ ВЫХОДА стратегии (`ScaleOutPlan`) - та же математика, что у `VirtualTracker` (Shadow).

На Binance уровни Scale Out и стоп стоят на бирже отдельными algo-ордерами. У MT5 SL/TP - поля позиции, отдельных ордеров на частичные
тейки нет, поэтому уровни исполняет бот: цена сделки дошла до уровня -> `close_position(position_id, volume)` (доля в лотах на сетке
`volumeStep`), стоп переносится `modify_position`. Стоп брокера стоит на позиции всё время (R6): сработал - позиция исчезла.

Правила (зеркало VirtualTracker, R8):
  * R = |фактическая цена входа - SL|; уровни = вход -+ r * R, от ФАКТИЧЕСКОЙ цены входа (MARKET - по факту, LIMIT - по цене заявки).
  * Уровень достигнут, когда цена сделки (тот же поток тиков, что у Shadow) дошла до него; несколько уровней за раз (гэп) закрываются подряд.
  * Безубытка, трейлинга и остатка-runner нет: стоп брокера статичен всю сделку, последний уровень закрывает остаток.
  * Тайм-аут: `max_hold_bars` свечей после входа -> закрытие остатка по рынку.
  * Брокерский TP: цена последнего уровня (брокер закроет остаток и при выключенном боте).
Устойчивость к сбоям: состояние берётся у брокера. Выполненные уровни выводятся из ФАКТИЧЕСКИХ лотов позиции (`lots <= остаток после уровня k`),
а объём закрытия считается как «лоты сейчас - остаток после уровня», поэтому повтор после неизвестного исхода или рестарта не закроет лишнего.

Закрытый рынок (retcode 10018 / `MarketClosedError`) - не авария и не Safe Mode: действие остаётся ожидающим, в журнал один раз
пишется `deferred`, при следующем опросе оно повторяется, при успехе пишется `replayed`. Неизвестный исход запроса (таймаут, обрыв) -
повтор в следующий опрос (все операции идемпотентны по лотам/цене). «Стоп уже за ценой» при переносе (INVALID_STOPS 10016) - выход по рынку
без Safe Mode. Любая другая ошибка брокера - исключение (R1).
"""

import logging
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
    kind: str  # partial_close | closed | deferred | replayed | plan_not_representable
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
    stop_price: float
    stop_kind: str = "sl"  # sl - причина выхода, если стоп брокера сработает
    tp_price: float | None = None  # брокерский TP (цена последнего уровня)
    levels_done: int = 0
    best_q: float = 0.0  # лучшая цена с входа в «шортовом мире» (sgn * цена): ниже - лучше
    last_q: float | None = None  # последняя цена сделки в «шортовом мире»
    bars_closed: int = 0
    fill_bar_no: int = 1
    forced_reason: str | None = None  # ожидает закрытия остатка по рынку: timeout | fill_beyond_stop | plan_not_representable
    deferred: list[str] = field(default_factory=list)  # действия, отложенные из-за закрытого рынка (уже записаны в журнал)

    def expected_after(self, k: int) -> float:
        """Лоты позиции после исполнения уровня k (0-based); после последнего уровня - 0."""
        return round(self.initial_lots - sum(lvl[2] for lvl in self.levels[: k + 1]), 8)

    def to_record(self) -> dict[str, Any]:
        out = {k: v for k, v in self.__dict__.items() if k != "plan"}
        out["plan"] = self.plan.describe()
        return out

    @classmethod
    def from_record(cls, rec: Mapping[str, Any]) -> "Mt5Exit":
        try:
            ep = rec["plan"]
            plan = ScaleOutPlan.from_record(ep["levels"])
            data = {k: v for k, v in rec.items() if k not in ("plan", "be_armed", "be_pending", "trail_target", "recent_bars", "runner_lots")}  # поля файлов до отмены безубытка и трейлинга
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
    """Брокерский TP: цена последнего уровня плана."""
    return level_price(entry, sgn, plan.last_r, abs(entry - sl), spec)


def build_exit(plan: ScaleOutPlan, pos: MetaApiPosition, sl: float, spec: Mt5SymbolSpec) -> Mt5Exit:
    """Состояние сопровождения для только что открытой позиции. Если закрыть надо сразу (вход за стопом, план не делится) - `forced_reason`."""
    sgn = -1.0 if pos.side == "BUY" else 1.0
    entry = pos.open_price
    risk = abs(entry - sl)
    stop = pos.stop_loss if pos.stop_loss is not None else sl
    split = split_levels(plan, pos.lots, lot_filters(spec))
    base = dict(plan=plan, sgn=sgn, entry=entry, sl0=sl, risk=risk, initial_lots=pos.lots, stop_price=stop, best_q=sgn * entry)
    if not sgn * sl > sgn * entry:
        return Mt5Exit(levels=[], **base, forced_reason="fill_beyond_stop")  # вход исполнен уже за стопом (гэп)
    if split is None:
        return Mt5Exit(levels=[], **base, forced_reason="plan_not_representable")
    levels = [[r, level_price(entry, sgn, r, risk, spec), lots] for r, lots in split]
    return Mt5Exit(levels=levels, **base, tp_price=broker_tp(plan, entry, sl, sgn, spec))


# ------------------------------------------------------------------------------------------------ тики и свечи


def on_tick(ex: Mt5Exit, tick: Tick) -> None:
    """Те же тики, что получает VirtualTracker: лучшая и последняя цена - по сделкам (mark price стоп не двигает)."""
    q = ex.sgn * tick.price
    if tick.kind != "mark":
        ex.best_q = min(ex.best_q, q)
        ex.last_q = q


def on_bar_closed(ex: Mt5Exit, bar: BarInfo | None, spec: Mt5SymbolSpec, max_hold_bars: int) -> None:
    """Закрытие свечи: тайм-аут (исполняется следующим опросом)."""
    ex.bars_closed += 1
    if ex.bars_closed - ex.fill_bar_no >= max_hold_bars:
        ex.forced_reason = ex.forced_reason or "timeout"


# ------------------------------------------------------------------------------------------------ шаг сопровождения


def _market_closed(exc: MetaApiTradeError) -> bool:
    return isinstance(exc, MarketClosedError)


async def step(ex: Mt5Exit, client: MetaApiLiveClient, pos: MetaApiPosition, spec: Mt5SymbolSpec) -> list[Mt5ExitEvent]:
    """Один опрос открытой позиции: выполненные уровни (по лотам брокера) -> уровни по цене -> тайм-аут/принудительный выход.

    `pos` - свежая позиция у брокера. Возвращает события; полное закрытие - событие `closed` (позиции у брокера больше нет)."""
    events: list[Mt5ExitEvent] = []
    lots = pos.lots
    # 1) что исполнено у брокера (в т.ч. за время простоя / после неизвестного исхода): выводим из лотов
    while ex.levels_done < len(ex.levels) and lots <= ex.expected_after(ex.levels_done) + _EPS:
        k = ex.levels_done
        ex.levels_done += 1
        events.append(Mt5ExitEvent("partial_close", {"reason": _level_reason(ex, k), "lots": ex.levels[k][2], "remaining": lots, "source": "broker_lots"}))
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
    return events


def _level_reason(ex: Mt5Exit, k: int) -> str:
    last = len(ex.levels) - 1
    return "tp" if k == last else f"scale_{ex.levels[k][0]:g}r"


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
    """Позиция исчезла у брокера без нашего закрытия: сработал SL брокера (sl) либо брокерский TP последнего уровня."""
    if ex.forced_reason is not None:
        return ex.forced_reason
    if ex.tp_price is not None and ex.levels_done >= len(ex.levels) - 1 and ex.last_q is not None and ex.last_q <= ex.sgn * ex.tp_price + 1e-12:
        return "tp"
    return ex.stop_kind
