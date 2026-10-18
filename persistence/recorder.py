"""Интерфейс записи сделок (Этап 5) без привязки к БД: трекер и Оркестратор знают только `TradeRecorder`.

Строки - обычные словари с «сырыми» Python-значениями (время - мс с эпохи UTC, числа - float, JSON-поля - dict/list); конвертацию
в типы БД (TIMESTAMPTZ, NUMERIC, JSONB) делает `persistence.pg.PgRecorder`. Поэтому трекер тестируется без БД
(`MemoryRecorder`), а при пустом DATABASE_URL работает `NullRecorder` (остаётся только JSONL-журнал).

Методы синхронные и быстрые (кладут запись в очередь): их зовут из обработчика тиков WebSocket, ждать сеть там нельзя.
Порядок записей сохраняется (FIFO): строка сделки раньше её закрытий и снапшота - иначе внешний ключ не сойдётся.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from orchestrator.journal import EventLog
from orchestrator.types import Event
from utils.errors import DataError

Row = dict[str, Any]


@dataclass(frozen=True)
class DbOpenTrade:
    """Открытая боевая сделка из БД (mode='live', status='open') для сверки с биржей при старте (`LiveExecutionEngine.recover`).

    `row` - полная строка trades в «сырых» Python-типах (мс, float, dict), с теми же ключами, что TRADE_COLUMNS: после сверки движок
    пишет её обратно целиком (upsert). `closes` - уже записанные закрытия {seq, ts, reason, qty, price, pnl, fees, gross_r} по порядку."""

    row: Row
    closes: tuple[Row, ...] = field(default_factory=tuple)


class TradeRecorder(Protocol):
    def trade(self, row: Row) -> None: ...
    def close_leg(self, row: Row) -> None: ...
    def snapshot(self, row: Row) -> None: ...
    def snapshot_copy(self, trade_id: str, source_trade_id: str, *, required: bool = True) -> None:
        """Снапшот боевой сделки `trade_id` = копия снапшота теневой `source_trade_id` (свечи, геометрия, meta + пометка live).
        required=True: источника нет -> ошибка (Fail-Fast); required=False (восстановление после рестарта): источника нет -> предупреждение, без падения."""
        ...
    def state_event(self, row: Row) -> None: ...


class NullRecorder:
    def trade(self, row: Row) -> None:
        pass

    def close_leg(self, row: Row) -> None:
        pass

    def snapshot(self, row: Row) -> None:
        pass

    def snapshot_copy(self, trade_id: str, source_trade_id: str, *, required: bool = True) -> None:
        pass

    def state_event(self, row: Row) -> None:
        pass


class MemoryRecorder:
    """Для тестов и как эталон формата: хранит последние состояния сделок и все остальные записи."""

    def __init__(self) -> None:
        self.trades: dict[str, Row] = {}
        self.trade_history: list[Row] = []
        self.closes: list[Row] = []
        self.snapshots: list[Row] = []
        self.missing_snapshots: list[str] = []  # trade_id боевых сделок, у которых источника снапшота нет (required=False)
        self.state_events: list[Row] = []

    def trade(self, row: Row) -> None:
        self.trades[row["id"]] = dict(row)
        self.trade_history.append(dict(row))

    def close_leg(self, row: Row) -> None:
        self.closes.append(dict(row))

    def snapshot(self, row: Row) -> None:
        self.snapshots.append(dict(row))

    def snapshot_copy(self, trade_id: str, source_trade_id: str, *, required: bool = True) -> None:
        """Эталон поведения PgRecorder: копия снапшота источника под новым id (повтор - без дубля); источника нет -> ошибка или пометка."""
        if any(s["trade_id"] == trade_id for s in self.snapshots):
            return
        source = next((s for s in self.snapshots if s["trade_id"] == source_trade_id), None)
        if source is None:
            if required:
                raise DataError(f"Снапшот-источник {source_trade_id} для боевой сделки {trade_id} не найден")
            self.missing_snapshots.append(trade_id)
            return
        self.snapshots.append({**source, "trade_id": trade_id, "meta": {**(source.get("meta") or {}), "live": True, "source_trade_id": source_trade_id}})

    def state_event(self, row: Row) -> None:
        self.state_events.append(dict(row))

    async def replace_closes(self, trade_row: Row, remove_seqs: Sequence[int], add: Sequence[Row]) -> None:
        """Эталон `PgResyncStore` (ретро-сверка): удалить закрытия по seq, добавить новые, записать строку сделки."""
        tid = trade_row["id"]
        self.closes = [c for c in self.closes if not (c["trade_id"] == tid and int(c["seq"]) in set(remove_seqs))]
        self.closes += [dict(a) for a in add]
        self.trade(trade_row)

    async def mark_attempt(self, trade_row: Row) -> None:
        self.trade(trade_row)


# ----------------------------------------------------------------------------- журнал событий -> state_events

_TRANSITIONS = {  # kind события -> (из состояния, в состояние)
    "signal_accepted": ("signal", "accepted"),
    "trade_registered": ("accepted", "pending"),
    "limit_filled": ("pending", "open"),
    "market_filled": ("pending", "open"),
    "partial_close": ("open", "open"),
    "be_armed": ("open", "open"),
    "trail_moved": ("open", "open"),
    "trade_closed": ("open", "closed"),
    "trade_cancelled": ("pending", "cancelled"),
}
SYSTEM_SOURCES = frozenset({"htf"})  # источники событий, которые не стратегии: у них strategy_id = NULL
_HEAVY_KEYS = ("geometry", "record")  # геометрия лежит в снапшоте, запись сделки - в trades: в state_events они только раздували бы таблицу


def state_event_row(event: Event) -> Row:
    """Event Оркестратора/трекера -> строка state_events. Переход берётся из таблицы выше; setup_state несёт свой."""
    detail = {k: v for k, v in event.detail.items() if k not in _HEAVY_KEYS}
    if event.kind == "setup_state":
        frm, to = f"{detail.get('side')}:S{detail['from']}", f"{detail.get('side')}:S{detail['to']}"
    else:
        frm, to = _TRANSITIONS.get(event.kind, (None, None))
    reason = detail.get("reason")
    return {
        "ts": event.ts_ms, "coin": event.symbol, "strategy": event.strategy,
        "strategy_id": None if event.strategy in SYSTEM_SOURCES else event.strategy, "signal_id": event.signal_id, "from_state": frm,
        "to_state": to, "kind": event.kind, "reason": None if reason is None else str(reason), "detail": detail,
    }


class RecordingEventLog:
    """EventLog, который пишет событие во внутренний журнал (JSONL/память) и в state_events через рекордер."""

    def __init__(self, inner: EventLog, recorder: TradeRecorder):
        self._inner, self._recorder = inner, recorder

    def emit(self, event: Event) -> None:
        self._inner.emit(event)
        self._recorder.state_event(state_event_row(event))
