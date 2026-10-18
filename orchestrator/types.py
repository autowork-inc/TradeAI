"""Общие типы оркестратора: Signal, Tick, FeatureContext, события журнала.

Все типы неизменяемы (frozen) и проверяют себя при создании: неверный сигнал - ошибка в момент создания (R1),
а не «странная сделка» через час. Геометрия сигнала - в едином формате разд. 3.3 CLAUDE.md:
    {"type": "zone" | "line" | "marker", "t1": мс, "t2": мс, "p1": цена, "p2": цена, "label": "..."}
Клиент мониторинга (Этап 6) рисует её, ничего не зная о стратегиях.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import polars as pl

from utils.errors import DataError


class Direction(str, Enum):
    SHORT = "short"
    LONG = "long"

    @property
    def sgn(self) -> float:
        """+1 для шорта, -1 для лонга: цены * sgn дают «шортовый мир» (как в strategies/simulate.py)."""
        return 1.0 if self is Direction.SHORT else -1.0


class Kind(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class Mode(str, Enum):
    SHADOW = "shadow"
    LIVE = "live"


def position_key(symbol: str, strategy_id: str) -> str:
    return f"{symbol}_{strategy_id}"


GEOMETRY_TYPES = frozenset({"zone", "line", "marker"})
GEOMETRY_KEYS = ("type", "t1", "t2", "p1", "p2", "label")


def geometry(kind: str, t1: int, t2: int, p1: float, p2: float, label: str) -> dict[str, Any]:
    if kind not in GEOMETRY_TYPES:
        raise DataError(f"Неизвестный тип геометрии {kind!r}")
    return {"type": kind, "t1": int(t1), "t2": int(t2), "p1": float(p1), "p2": float(p2), "label": label}


@dataclass(frozen=True)
class Signal:
    """Решение стратегии на закрытии свечи `bar_open_time`.

    LIMIT: entry - цена заявки, tp задан, inval - уровень отмены (close за ним до исполнения отменяет заявку).
    MARKET: entry - только справочная цена (close сигнального бара); вход по первому тику после сигнала, R и TP
    считаются по факту входа (tp = None).
    """

    signal_id: str
    strategy: str
    symbol: str
    interval: str
    direction: Direction
    kind: Kind
    bar_open_time: int
    bar_close_ms: int
    entry: float
    sl: float
    tp: float | None
    inval: float | None
    geometry: tuple[dict[str, Any], ...] = ()
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        sgn = self.direction.sgn
        if not (self.entry > 0 and self.sl > 0):
            raise DataError(f"{self.signal_id}: entry и sl должны быть положительными ({self.entry}, {self.sl})")
        if not sgn * self.sl > sgn * self.entry:
            raise DataError(f"{self.signal_id}: SL {self.sl} не за входом {self.entry} для {self.direction.value}")
        if self.kind is Kind.LIMIT:
            if self.tp is None or self.inval is None:
                raise DataError(f"{self.signal_id}: у LIMIT-сигнала обязательны tp и inval")
            if not sgn * self.tp < sgn * self.entry:
                raise DataError(f"{self.signal_id}: TP {self.tp} не по ту сторону входа {self.entry}")
        for g in self.geometry:
            if tuple(g) != GEOMETRY_KEYS:
                raise DataError(f"{self.signal_id}: геометрия не в едином формате: {g}")

    @property
    def strategy_id(self) -> str:
        """Идентификатор стратегии (то же, что `strategy`): пишется в колонку strategy_id всех таблиц БД."""
        return self.strategy

    @property
    def position_key(self) -> str:
        """Составной ключ позиции `<символ>_<стратегия>` (например SOLUSDT_SMC_Pro_Trend): одна позиция на ключ."""
        return position_key(self.symbol, self.strategy)


@dataclass(frozen=True)
class Tick:
    """Тик биржи: kind='trade' - цена сделки (aggTrade), kind='mark' - mark price (markPrice@1s).

    У сделки (aggTrade) есть ещё объём `qty` и сторона агрессора: `buyer_maker=True` - покупатель был мейкером, т.е. агрессор
    ПРОДАЛ (по рынку в bid). Нужны ленте микроструктуры (дельта объёма, стратегия OBI_Breakout); у mark price они None.
    """

    symbol: str
    price: float
    ts_ms: int
    kind: str
    qty: float | None = None
    buyer_maker: bool | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("trade", "mark"):
            raise DataError(f"Неизвестный вид тика {self.kind!r}")
        if not (self.price > 0 and self.price < float("inf")):
            raise DataError(f"Недопустимая цена тика {self.price!r} ({self.symbol})")
        if self.qty is not None and not (self.qty >= 0 and self.qty < float("inf")):
            raise DataError(f"Недопустимый объём тика {self.qty!r} ({self.symbol})")


@dataclass(frozen=True)
class BarInfo:
    """Только что закрытая свеча для трейлинга runner'а: экстремумы и ATR (None - колонки нет в кадре)."""

    high: float | None
    low: float | None
    atr: float | None
    atr_prev: float | None = None  # ATR(14) до этой свечи (ATR[i-1]): порог импульсной свечи не зависит от неё самой


SNAPSHOT_COLUMNS = ("open_time", "open", "high", "low", "close", "volume", "atr")


@dataclass(frozen=True)
class FeatureContext:
    """Закрытые свечи одной монеты + признаки (features.smc.add_features) на момент закрытия последней.

    frame: непрерывная сетка, последняя строка - только что закрытая свеча (R5). received_ms - время получения
    события закрытия по часам, синхронизированным с биржей (для проверки «свежести» сигнала).
    """

    symbol: str
    interval: str
    interval_ms: int
    frame: pl.DataFrame
    received_ms: int

    def __post_init__(self) -> None:
        if self.frame.height == 0:
            raise DataError(f"Пустой FeatureContext {self.symbol} {self.interval}")

    @property
    def bar_open_time(self) -> int:
        return int(self.frame["open_time"][-1])

    @property
    def bar_close_ms(self) -> int:
        return self.bar_open_time + self.interval_ms

    @property
    def last_close(self) -> float:
        return float(self.frame["close"][-1])

    @property
    def bar(self) -> BarInfo:
        def last(name: str) -> float | None:
            if name not in self.frame.columns:
                return None
            v = self.frame[name][-1]
            return None if v is None or v != v else float(v)  # null и NaN -> None

        prev = None
        if "atr" in self.frame.columns and self.frame.height >= 2:
            v = self.frame["atr"][-2]
            prev = None if v is None or v != v else float(v)
        return BarInfo(last("high"), last("low"), last("atr"), prev)

    def snapshot_candles(self, n: int = 100) -> list[dict[str, Any]]:
        """Последние n свечей для снапшота сделки (JSONB): OHLCV и ATR, где есть. null/NaN -> None."""
        cols = [c for c in SNAPSHOT_COLUMNS if c in self.frame.columns]
        rows = self.frame.tail(n).select(cols).to_dicts()  # выгрузка данных для хранения, не математика паттернов
        return [{k: (None if isinstance(v, float) and v != v else v) for k, v in r.items()} for r in rows]


@dataclass(frozen=True)
class Event:
    """Запись журнала решений оркестратора и трекера. kind: signal_accepted | skipped_due_to_lock | skipped_stale |
    skipped_risk | trade_registered | limit_filled | be_armed | trade_closed | data_gap | ..."""

    ts_ms: int
    kind: str
    symbol: str
    strategy: str
    signal_id: str | None
    detail: dict[str, Any] = field(default_factory=dict)
