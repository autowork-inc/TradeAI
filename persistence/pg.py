"""PgRecorder: асинхронная запись сделок в PostgreSQL через очередь (FIFO) и одну задачу-писатель.

Почему очередь. Трекер вызывается из обработчика тиков WebSocket (десятки сообщений в секунду): ждать сеть БД там нельзя. Методы
рекордера кладут строку в очередь и возвращаются; писатель выполняет записи строго по порядку, каждую в своей транзакции.

Fail-Fast (R1). Ошибка записи (БД упала, строка не подходит под схему, NaN в JSON) завершает задачу-писателя исключением; основной
цикл зовёт `check()` каждую секунду и падает вместе с ней, Docker перезапускает процесс. Переполнение очереди (БД «зависла») - тоже
ошибка, а не молчаливая потеря. Что НЕ защищено: записи, лежавшие в очереди в момент падения процесса (доли секунды при нормальной
работе); JSONL-журнал остаётся вторым источником. Для боевого LIVE (R7) перед отправкой ордера нужен `await flush()`.

Теневые сделки при перезапуске продолжаются (2026-10-23, отмена О17): `PgRecorder(restore_open=True)` отдаёт ждущие лимитки и открытые позиции в `restorable`,
а трекер поднимает их (`VirtualTracker.restore`) с пометкой gap_affected. Остальные «висящие» сделки (рыночная заявка без исполнения, нет снапшота) и все
сделки при `restore_open=False` `start()` помечает `aborted`/`process_restart` - в БД не остаётся вечно открытых виртуальных позиций.
Это касается ТОЛЬКО режима рекордера (`mode`, по умолчанию 'shadow'): боевые строки (mode='live') не трогаются - их состояние живёт в файле
`live_engine_state.json` и после рестарта восстанавливается движком (upsert строки, закрытий и снапшота).
"""

import asyncio
import json
import logging
import math
from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from persistence import schema
from persistence.db import Database
from persistence.recorder import DbOpenTrade, DbShadowTrade, Row
from utils.errors import DataError, TradingInfraError

log = logging.getLogger("persistence.pg")

_TABLES = {
    "trade": (schema.UPSERT_TRADE, schema.TRADE_COLUMNS),
    "close": (schema.INSERT_CLOSE, schema.CLOSE_COLUMNS),
    "snapshot": (schema.INSERT_SNAPSHOT, schema.SNAPSHOT_COLUMNS),
    "state": (schema.INSERT_STATE_EVENT, schema.STATE_EVENT_COLUMNS),
}


def convert(kind: str, value: Any, name: str = "?") -> Any:
    """Значение строки -> тип asyncpg. None остаётся None (NULL). Нечисловое/нефинитное в числе, NaN в JSON - DataError."""
    if value is None:
        return None
    if kind == "ts":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DataError(f"{name}: время должно быть числом (мс), получено {value!r}")
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if kind == "num":
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)) or not math.isfinite(value):
            raise DataError(f"{name}: число ожидается конечным, получено {value!r}")
        return value if isinstance(value, Decimal) else Decimal(repr(value))
    if kind == "bool":
        if not isinstance(value, bool):
            raise DataError(f"{name}: ожидается bool, получено {value!r}")
        return value
    if kind == "json":
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise DataError(f"{name}: значение не записывается в JSON ({exc})") from exc
    if kind == "text":
        if not isinstance(value, str):
            raise DataError(f"{name}: ожидается строка, получено {value!r}")
        return value
    raise DataError(f"Неизвестный тип колонки {kind!r}")


def from_db(kind: str, value: Any) -> Any:
    """Обратное `convert`: значение asyncpg -> «сырой» Python-тип строки (мс, float, dict/list)."""
    if value is None:
        return None
    if kind == "ts":
        return int(value.timestamp() * 1000)
    if kind == "num":
        return float(value)
    if kind == "json":
        return json.loads(value) if isinstance(value, str) else value
    return value


async def fetch_open_live(db: Database) -> list[DbOpenTrade]:
    """Открытые боевые сделки (mode='live', status='open') с их закрытиями - для сверки с биржей при старте (`LiveExecutionEngine.recover`)."""
    rows = [{name: from_db(kind, r[name]) for name, kind in schema.TRADE_COLUMNS.items()} for r in await db.fetch(schema.SELECT_OPEN_LIVE)]
    closes: dict[str, list[Row]] = {}
    if rows:
        kinds = {**schema.CLOSE_COLUMNS, "trade_id": "text"}
        for c in await db.fetch(schema.SELECT_CLOSES_OF, [r["id"] for r in rows]):
            closes.setdefault(c["trade_id"], []).append({k: from_db(kinds[k], c[k]) for k in c.keys()})
    return [DbOpenTrade(r, tuple(closes.get(r["id"], ()))) for r in rows]


async def fetch_open_shadow(db: Database, mode: str = "shadow") -> list[DbShadowTrade]:
    """Незавершённые теневые сделки (ждущая лимитка и открытая позиция) со снапшотом и закрытиями - для продолжения после рестарта.
    Сделка без снапшота (процесс упал между записью строки и снапшота) не восстанавливается: уйдёт в aborted вместе с остальными сиротами."""
    rows = [{name: from_db(kind, r[name]) for name, kind in schema.TRADE_COLUMNS.items()} for r in await db.fetch(schema.SELECT_OPEN_SHADOW, mode)]
    if not rows:
        return []
    ids = [r["id"] for r in rows]
    snaps = {s["trade_id"]: s for s in await db.fetch(schema.SELECT_SNAPSHOT_META, ids)}
    kinds = {**schema.CLOSE_COLUMNS, "trade_id": "text"}
    closes: dict[str, list[Row]] = {}
    for c in await db.fetch(schema.SELECT_CLOSES_FULL, ids):
        closes.setdefault(c["trade_id"], []).append({k: from_db(kinds[k], c[k]) for k in c.keys()})
    out: list[DbShadowTrade] = []
    for r in rows:
        snap = snaps.get(r["id"])
        if snap is None:
            log.warning("Теневая сделка %s без снапшота: восстановить нельзя", r["id"])
            continue
        out.append(DbShadowTrade(r, tuple(closes.get(r["id"], ())), from_db("json", snap["meta"]) or {}, tuple(from_db("json", snap["geometry"]) or ())))
    return out


async def fetch_resync_candidates(db: Database) -> list[DbOpenTrade]:
    """Закрытые боевые сделки с `exit_reason='unknown'` или `gap_affected` (и без прежней попытки сверки) вместе с записанными закрытиями."""
    rows = [{name: from_db(kind, r[name]) for name, kind in schema.TRADE_COLUMNS.items()} for r in await db.fetch(schema.SELECT_RESYNC)]
    closes: dict[str, list[Row]] = {}
    if rows:
        kinds = {**schema.CLOSE_COLUMNS, "trade_id": "text"}
        for c in await db.fetch(schema.SELECT_CLOSES_OF, [r["id"] for r in rows]):
            closes.setdefault(c["trade_id"], []).append({k: from_db(kinds[k], c[k]) for k in c.keys()})
    return [DbOpenTrade(r, tuple(closes.get(r["id"], ()))) for r in rows]


class PgResyncStore:
    """Запись результатов ретро-сверки: удаление неточных закрытий, новые закрытия и итоговая строка сделки - ОДНОЙ транзакцией."""

    def __init__(self, db: Database):
        self._db = db

    async def replace_closes(self, trade_row: Row, remove_seqs: Sequence[int], add: Sequence[Row]) -> None:
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                if remove_seqs:
                    await conn.execute(schema.DELETE_CLOSES, trade_row["id"], list(remove_seqs))
                for leg in add:
                    await conn.execute(schema.INSERT_CLOSE, *to_args(leg, schema.CLOSE_COLUMNS))
                await conn.execute(schema.UPSERT_TRADE, *to_args(trade_row, schema.TRADE_COLUMNS))

    async def mark_attempt(self, trade_row: Row) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(schema.UPSERT_TRADE, *to_args(trade_row, schema.TRADE_COLUMNS))


def to_args(row: Row, columns: dict[str, str]) -> list[Any]:
    extra = set(row) - set(columns)
    if extra:
        raise DataError(f"Лишние поля в строке БД: {sorted(extra)}")
    return [convert(kind, row.get(name), name) for name, kind in columns.items()]


class PgRecorder:
    def __init__(self, db: Database, *, mode: str = "shadow", max_queue: int = 20_000, restore_open: bool = False):
        """restore_open=True (shadow_run): висящие теневые сделки НЕ уходят в aborted, а отдаются в `self.restorable` для продолжения (`VirtualTracker.restore`)."""
        self._db, self._mode, self._restore_open = db, mode, restore_open
        self.restorable: list[DbShadowTrade] = []
        self._queue: asyncio.Queue[tuple[str, Row]] = asyncio.Queue(maxsize=max_queue)
        self._task: asyncio.Task | None = None
        self.written = 0

    async def start(self) -> list[str]:
        """Миграции, пометка висящих сделок прошлого запуска, запуск писателя. -> id помеченных сделок."""
        await self._db.migrate()
        self.restorable = await fetch_open_shadow(self._db, self._mode) if self._restore_open else []
        orphans = await self._db.fetch(schema.ABORT_ORPHANS, self._mode, [t.row["id"] for t in self.restorable])
        for r in orphans:
            log.warning("Сделка %s (%s) прошлого запуска помечена aborted: восстановить её нельзя (нет снапшота / рыночная заявка без исполнения)", r["id"], f"{r['coin']}_{r['strategy_id']}")
        for t in self.restorable:
            log.info("Теневая сделка %s (%s_%s, %s) будет продолжена после рестарта", t.row["id"], t.row["coin"], t.row["strategy_id"], t.row["status"])
        self._task = asyncio.create_task(self._writer(), name="pg_recorder")
        return [r["id"] for r in orphans]

    # ------------------------------------------------------------------------------ интерфейс TradeRecorder

    def trade(self, row: Row) -> None:
        self._put("trade", row)

    def close_leg(self, row: Row) -> None:
        self._put("close", row)

    def snapshot(self, row: Row) -> None:
        self._put("snapshot", row)

    def snapshot_copy(self, trade_id: str, source_trade_id: str, *, required: bool = True) -> None:
        """Снапшот боевой сделки = копия снапшота теневой (одним INSERT ... SELECT внутри БД). Строка боевой сделки должна быть поставлена в очередь РАНЬШЕ
        (внешний ключ); очередь FIFO это гарантирует. required=False - только для восстановления после рестарта (см. `_copy_snapshot`)."""
        self._put("snapshot_copy", {"trade_id": trade_id, "source_trade_id": source_trade_id, "required": required})

    def state_event(self, row: Row) -> None:
        self._put("state", row)

    def _put(self, kind: str, row: Row) -> None:
        if self._task is None:
            raise TradingInfraError("PgRecorder не запущен (вызовите start())")
        self.check()
        try:
            self._queue.put_nowait((kind, row))
        except asyncio.QueueFull as exc:
            raise TradingInfraError(f"Очередь записи в БД переполнена ({self._queue.maxsize}): БД не успевает или зависла") from exc

    # ---------------------------------------------------------------------------------------- управление

    def check(self) -> None:
        """Поднимает исключение упавшего писателя (вызывать из основного цикла)."""
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise TradingInfraError("Задача записи в БД отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise TradingInfraError("Задача записи в БД завершилась без ошибки: так быть не должно")

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    async def flush(self, timeout: float = 30.0) -> None:
        """Дождаться, пока всё поставленное в очередь записано (или писатель упал)."""
        assert self._task is not None
        joiner = asyncio.create_task(self._queue.join())
        try:
            done, _ = await asyncio.wait({joiner, self._task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            joiner.cancel()
        self.check()
        if not done:
            raise TradingInfraError(f"Очередь записи в БД не опустела за {timeout} с (осталось {self.pending})")

    async def aclose(self, timeout: float = 10.0) -> None:
        """Штатное завершение: дописать очередь, остановить писателя, закрыть пул. Ошибки писателя поднимаются."""
        try:
            if self._task is not None and not self._task.done():
                await self.flush(timeout)
            elif self._task is not None:
                self.check()  # писатель уже умер: поднимаем его ошибку, а не прячем её за штатным закрытием
        finally:
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            await self._db.close()

    # ------------------------------------------------------------------------------------------- писатель

    async def _writer(self) -> None:
        while True:
            kind, row = await self._queue.get()
            try:
                if kind == "snapshot_copy":
                    await self._copy_snapshot(row)
                else:
                    sql, columns = _TABLES[kind]
                    args = to_args(row, columns)
                    async with self._db.pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.execute(sql, *args)
                self.written += 1
            finally:
                self._queue.task_done()

    async def _copy_snapshot(self, row: Row) -> None:
        """INSERT ... SELECT снапшота теневой сделки под id боевой. Ноль скопированных строк - не исключение БД, а значение, которое проверяем явно:
        снапшот боевой сделки уже есть (повторная запись при восстановлении) - нормально; источника нет и копия не существует -
        при required=True это DataError (Fail-Fast, писатель падает, процесс перезапускается), при required=False (восстановление
        открытой позиции после рестарта) - предупреждение в лог: живую позицию из-за пустого графика ронять нельзя. Ошибки самой БД
        (обрыв, нарушение ключа) поднимаются всегда."""
        trade_id = convert("text", row["trade_id"], "trade_id")
        source = convert("text", row["source_trade_id"], "source_trade_id")
        required = convert("bool", row["required"], "required")
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.execute(schema.COPY_SNAPSHOT, trade_id, source)
                copied = int(status.rsplit(" ", 1)[-1])  # ответ вида "INSERT 0 1"
                exists = copied > 0 or await conn.fetchval(schema.SNAPSHOT_EXISTS, trade_id) is not None
        if exists:
            return
        if required:
            raise DataError(f"Снапшот-источник {source} для боевой сделки {trade_id} не найден: у боевой сделки не будет графика")
        log.warning("Снапшот-источник %s для боевой сделки %s не найден (восстановление): сделка продолжается без графика", source, trade_id)
