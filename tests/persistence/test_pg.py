"""PostgreSQL (Этап 5): конвертация значений, миграции, запись сделок/закрытий/снапшотов/состояний через очередь, отказоустойчивость.

Тесты с БД идут, только если задан TEST_DATABASE_URL (например postgresql://tradeai:pw@127.0.0.1:54330/tradeai_test); имя БД
обязано заканчиваться на `_test`: перед каждым тестом схема public ПЕРЕСОЗДАЁТСЯ, на боевую базу это не должно попасть.
Остальное (конвертация, формат строк трекера, DSN) проверяется без БД.
"""

import asyncio
import json
import os
from decimal import Decimal
from urllib.parse import urlsplit

import polars as pl
import pytest

from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import VirtualTracker
from orchestrator.types import BarInfo, Event, FeatureContext, Mode
from persistence import schema
from persistence.db import Database, DatabaseError, check_dsn, redact_dsn
from persistence.pg import PgRecorder, convert, to_args
from persistence.recorder import MemoryRecorder, RecordingEventLog, state_event_row
from tests.orchestrator.helpers import IV, Scripted, mk, tick
from utils.errors import ConfigError, DataError, TradingInfraError

TEST_URL = os.environ.get("TEST_DATABASE_URL", "")
needs_db = pytest.mark.skipif(not TEST_URL, reason="нужен TEST_DATABASE_URL (БД *_test)")


def run(coro):
    return asyncio.run(coro)


async def fresh_db() -> Database:
    assert urlsplit(TEST_URL).path.strip("/").endswith("_test"), "TEST_DATABASE_URL должен указывать на БД *_test"
    db = await Database.connect(TEST_URL, attempts=3, delay_s=0.2)
    async with db.pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    return db


# ================================================================================== без БД


def test_convert_types_and_rejections():
    assert convert("ts", 1_700_000_000_123).isoformat() == "2023-11-14T22:13:20.123000+00:00"
    assert convert("num", 0.1) == Decimal("0.1") and convert("num", 5) == Decimal(5) and convert("num", None) is None
    assert convert("json", {"a": [1, None]}) == '{"a":[1,null]}' and convert("bool", True) is True
    for kind, bad in (("num", float("nan")), ("num", float("inf")), ("num", "1"), ("num", True), ("ts", "x"), ("bool", 1),
                      ("json", {"x": float("nan")}), ("json", {"x": object()}), ("text", 5)):
        with pytest.raises(DataError):
            convert(kind, bad, "поле")


def test_tracker_rows_fit_the_schema_exactly():
    rec = MemoryRecorder()
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec)
    tr.submit(mk(), 1000)
    tr.on_tick(tick(100.5, 10))
    tr.on_tick(tick(94.0, 20))
    tr.on_bar_closed("BTCUSDT", IV, 90.0, 1, BarInfo(91.0, 88.0, 2.0))
    tr.on_tick(tick(79.5, 30))  # единственный тейк 1:4
    for row in rec.trade_history:
        to_args(row, schema.TRADE_COLUMNS)  # лишнее поле или неверный тип - DataError
    for row in rec.closes:
        to_args(row, schema.CLOSE_COLUMNS)
    assert len(rec.closes) == 1 and rec.trade_history[-1]["status"] == "closed"
    with pytest.raises(DataError, match="Лишние"):
        to_args({**rec.trade_history[0], "bogus": 1}, schema.TRADE_COLUMNS)


def test_state_event_rows_from_journal_events():
    row = state_event_row(Event(5, "setup_state", "BTCUSDT", "smc", None, {"side": "short", "from": 1, "to": 2}))
    assert row["strategy_id"] == "smc"
    assert state_event_row(Event(5, "htf_bias", "BTCUSDT", "htf", None, {"bias": "Bullish"}))["strategy_id"] is None  # не стратегия
    assert (row["from_state"], row["to_state"], row["kind"], row["signal_id"]) == ("short:S1", "short:S2", "setup_state", None)
    row = state_event_row(Event(6, "trade_registered", "BTCUSDT", "smc", "sid", {"geometry": [1], "record": {}, "kind": "limit"}))
    assert (row["from_state"], row["to_state"]) == ("accepted", "pending") and "geometry" not in row["detail"] and row["detail"]["kind"] == "limit"
    assert state_event_row(Event(7, "trade_cancelled", "BTCUSDT", "smc", "sid", {"reason": "ttl"}))["reason"] == "ttl"
    to_args(row, schema.STATE_EVENT_COLUMNS)


def test_dsn_redaction_and_validation():
    assert redact_dsn("postgresql://u:secret@db:5432/x") == "postgresql://u:***@db:5432/x" and "secret" not in redact_dsn("postgres://u:secret@h/x")
    check_dsn("postgresql://u:p@db:5432/x")
    for bad in ("", "mysql://u:p@h/x", "postgresql://u:p@/x", "postgresql://u:p@h"):
        with pytest.raises(ConfigError):
            check_dsn(bad)


def test_unreachable_database_fails_after_bounded_attempts_without_leaking_the_password():
    async def body():
        with pytest.raises(DatabaseError) as exc:
            await Database.connect("postgresql://u:topsecret@127.0.0.1:1/x_test", attempts=2, delay_s=0.01)
        return str(exc.value)

    assert "topsecret" not in run(body())


def test_schema_sql_is_consistent():
    assert schema.SCHEMA_VERSION == 3 and [v for v, _ in schema.MIGRATIONS] == [1, 2, 3]
    assert schema.UPSERT_TRADE.count("$") == len(schema.TRADE_COLUMNS) and "updated_at = now()" in schema.UPSERT_TRADE
    assert "::jsonb" in schema.INSERT_SNAPSHOT and "ON CONFLICT (trade_id, seq) DO NOTHING" in schema.INSERT_CLOSE


# ===================================================================================== с БД


@needs_db
def test_migrations_create_the_schema_and_are_idempotent():
    async def body():
        db = await fresh_db()
        try:
            v1 = await db.migrate()
            v2 = await db.migrate()
            tables = {r["tablename"] for r in await db.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}
            cols = {r["column_name"]: r["data_type"] for r in await db.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'trades'")}
            versions = [r["version"] for r in await db.fetch("SELECT version FROM schema_migrations")]
            return v1, v2, tables, cols, versions
        finally:
            await db.close()

    v1, v2, tables, cols, versions = run(body())
    assert v1 == v2 == 3 and versions == [1, 2, 3]
    assert {"trades", "trade_closes", "trade_snapshots", "state_events", "favorites", "schema_migrations"} <= tables
    assert set(cols) >= set(schema.TRADE_COLUMNS)
    assert cols["strategy_id"] == "text"
    assert cols["entry_price"] == "numeric" and cols["signal_time"] == "timestamp with time zone" and cols["plan"] == "jsonb"


@needs_db
def test_database_with_newer_schema_than_the_program_is_refused():
    async def body():
        db = await fresh_db()
        try:
            await db.migrate()
            async with db.pool.acquire() as conn:
                await conn.execute("INSERT INTO schema_migrations (version) VALUES (99)")
            with pytest.raises(DatabaseError, match="новее"):
                await db.migrate()
        finally:
            await db.close()

    run(body())


def frame(n=30) -> pl.DataFrame:
    return pl.DataFrame({
        "open_time": [i * IV for i in range(n)], "open": [100.0 + i for i in range(n)], "high": [101.0 + i for i in range(n)],
        "low": [99.0 + i for i in range(n)], "close": [100.5 + i for i in range(n)], "volume": [10.0] * n, "atr": [None] + [2.0] * (n - 1),
    })


@needs_db
def test_full_trade_lifecycle_lands_in_all_four_tables():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        journal = RecordingEventLog(MemoryEventLog(), rec)
        tracker = VirtualTracker(journal, equity=10_000.0, recorder=rec)
        sig = mk(bar=3)
        orch = Orchestrator([Scripted("smc", {3: [sig]})], {"smc": Route(Mode.SHADOW, tracker)}, journal, recorder=rec, snapshot_bars=10)
        f = frame(4)
        for i in range(4):
            orch.on_context(FeatureContext("BTCUSDT", "15m", IV, f.head(i + 1), (i + 1) * IV + 1000))
        tracker.on_tick(tick(100.5, 10_000))
        tracker.on_tick(tick(94.0, 20_000))
        tracker.on_tick(tick(84.0, 21_000))
        tracker.on_bar_closed("BTCUSDT", 4 * IV, 90.0, 22_000, BarInfo(91.0, 88.0, 2.0))
        tracker.on_tick(tick(90.5, 30_000))
        await rec.flush()
        trade = dict((await db.fetch("SELECT * FROM trades"))[0])
        closes = [dict(r) for r in await db.fetch("SELECT * FROM trade_closes ORDER BY seq")]
        snap = dict((await db.fetch("SELECT * FROM trade_snapshots"))[0])
        states = [dict(r) for r in await db.fetch("SELECT * FROM state_events ORDER BY id")]
        t = tracker.trades[0]
        await rec.aclose()
        return trade, closes, snap, states, t

    trade, closes, snap, states, t = run(body())
    assert trade["id"] == t.trade_id and trade["status"] == "closed" and trade["exit_reason"] == "trail" and trade["mode"] == "shadow"
    assert trade["side"] == "short" and trade["coin"] == "BTCUSDT" and isinstance(trade["entry_price"], Decimal) and trade["entry_price"] == Decimal("100.0")
    assert float(trade["r_multiple"]) == pytest.approx(t.net_r) and float(trade["pnl"]) == pytest.approx(t.pnl_usdt) and float(trade["remaining_frac"]) == 0
    assert trade["entry_time"].tzinfo is not None and trade["exit_time"] > trade["entry_time"] and trade["be_armed"] is True
    plan = json.loads(trade["plan"])
    assert plan["levels"] == [[1.0, 0.5], [3.0, 0.4]] and plan["trail_mode"] == "atr"
    assert [(c["seq"], c["reason"]) for c in closes] == [(1, "scale_1r"), (2, "scale_3r"), (3, "trail")]
    assert sum(float(c["frac"]) for c in closes) == pytest.approx(1.0) and sum(float(c["net_r"]) for c in closes) == pytest.approx(t.net_r)
    candles = json.loads(snap["candles"])
    assert len(candles) == 4 and candles[-1]["close"] == pytest.approx(103.5) and candles[0]["atr"] is None  # null ATR прогрева -> JSON null
    assert trade["strategy_id"] == "smc" and snap["strategy_id"] == "smc" and all(s["strategy_id"] == "smc" for s in states)  # strategy_id во всех таблицах
    assert snap["trade_id"] == t.trade_id and json.loads(snap["geometry"]) == [] and json.loads(snap["meta"])["sl"] == 105.0
    kinds = [s["kind"] for s in states]
    assert kinds == ["trade_registered", "signal_accepted", "limit_filled", "partial_close", "be_armed", "partial_close", "trail_moved", "trade_closed"]
    assert states[0]["from_state"] == "accepted" and states[-1]["to_state"] == "closed" and all(s["signal_id"] == t.trade_id for s in states)


@needs_db
def test_restart_marks_dangling_trades_aborted():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec)
        tracker.submit(mk(bar=1), 1000)  # станет открытой позицией
        tracker.submit(mk(symbol="ETHUSDT", bar=1), 1000)  # висит заявкой
        tracker.on_tick(tick(100.5, 10))
        tracker.submit(mk(bar=5, symbol="DOGEUSDT"), 2000)  # висит заявкой
        await rec.flush()
        await rec.aclose()
        db2 = await Database.connect(TEST_URL, attempts=2, delay_s=0.1)
        rec2 = PgRecorder(db2)
        aborted = await rec2.start()
        rows = {r["id"]: (r["status"], r["exit_reason"]) for r in await db2.fetch("SELECT id, status, exit_reason FROM trades")}
        await rec2.aclose()
        return aborted, rows

    aborted, rows = run(body())
    assert len(aborted) == 3 and len(rows) == 3 and all(v == ("aborted", "process_restart") for v in rows.values())


@needs_db
def test_restart_after_pool_change_aborts_old_coins_and_does_not_crash_on_new_ones():
    """2026-10-11: пул Shadow заменён (BTC/ETH/DOGE -> SOL/XRP/...). Висящие сделки старых монет -> aborted, новые монеты пишутся как обычно."""
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec)
        tracker.submit(mk(bar=1, symbol="BTCUSDT"), 1000)  # станет открытой позицией
        tracker.on_tick(tick(100.5, 10, symbol="BTCUSDT"))
        tracker.submit(mk(bar=2, symbol="ETHUSDT"), 1000)  # висит заявкой
        tracker.submit(mk(bar=3, symbol="DOGEUSDT"), 1000)
        rec.state_event({"ts": 1000, "coin": "DOGEUSDT", "strategy": "smc", "signal_id": None, "from_state": "S0", "to_state": "S1", "kind": "setup_state",
                         "reason": None, "detail": {"side": "short"}})
        await rec.flush()
        await rec.aclose()
        db2 = await Database.connect(TEST_URL, attempts=2, delay_s=0.1)
        rec2 = PgRecorder(db2)
        aborted = await rec2.start()  # новый процесс уже с другим config.SHADOW_SYMBOLS: список монет рекордеру не нужен
        tracker2 = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec2)
        tracker2.submit(mk(bar=9, symbol="SOLUSDT"), 5000)
        await rec2.flush()
        rows = {r["coin"]: (r["status"], r["exit_reason"]) for r in await db2.fetch("SELECT coin, status, exit_reason FROM trades")}
        await rec2.aclose()
        return aborted, rows

    aborted, rows = run(body())
    assert len(aborted) == 3
    assert {c: rows[c] for c in ("BTCUSDT", "ETHUSDT", "DOGEUSDT")} == {c: ("aborted", "process_restart") for c in ("BTCUSDT", "ETHUSDT", "DOGEUSDT")}
    assert rows["SOLUSDT"][0] == "pending_limit" and rows["SOLUSDT"][1] is None


@needs_db
def test_closed_trades_survive_a_restart_untouched():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0, recorder=rec)
        tracker.submit(mk(), 1000)
        tracker.on_tick(tick(100.5, 10))
        tracker.on_tick(tick(105.5, 11))  # стоп: сделка закрыта
        await rec.flush()
        await rec.aclose()
        db2 = await Database.connect(TEST_URL, attempts=2, delay_s=0.1)
        rec2 = PgRecorder(db2)
        aborted = await rec2.start()
        status = (await db2.fetch("SELECT status, exit_reason FROM trades"))[0]
        await rec2.aclose()
        return aborted, dict(status)

    aborted, status = run(body())
    assert aborted == [] and status == {"status": "closed", "exit_reason": "sl"}


@needs_db
def test_writer_failure_surfaces_through_check_r1():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        rec.trade({"id": "x", "strategy_name": "smc", "strategy_id": "smc", "coin": "BTCUSDT", "side": "sideways", "kind": "limit", "mode": "shadow",
                   "status": "open", "signal_time": 1000, "sl": 1.0})  # нарушает CHECK (side)
        with pytest.raises(Exception, match="check|side"):
            for _ in range(100):
                await asyncio.sleep(0.05)
                rec.check()
        with pytest.raises(Exception):
            rec.trade({"id": "y"})  # писатель мёртв: новые записи не принимаются молча
        with pytest.raises(Exception):
            await rec.aclose()

    run(body())


@needs_db
def test_invalid_row_kills_the_writer_instead_of_being_skipped():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        rec.state_event({"ts": 1000, "coin": "BTCUSDT", "strategy": "smc", "kind": "x", "detail": {"nan": float("nan")}})
        with pytest.raises(DataError):
            await rec.flush(timeout=5)
        await db.close()

    run(body())


@needs_db
def test_queue_overflow_is_an_error_not_silent_loss():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db, max_queue=2)
        await rec.start()
        rec._task.cancel()  # писатель стоит: очередь копится
        await asyncio.gather(rec._task, return_exceptions=True)
        rec._task = asyncio.create_task(asyncio.sleep(3600))
        row = {"ts": 1, "coin": "BTCUSDT", "strategy": "smc", "kind": "k"}
        rec.state_event(row)
        rec.state_event(row)
        with pytest.raises(TradingInfraError, match="переполнена"):
            rec.state_event(row)
        rec._task.cancel()
        await db.close()

    run(body())


@needs_db
def test_upsert_keeps_one_row_per_trade_and_ignores_duplicate_legs():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        base = {"id": "t1", "strategy_name": "smc", "strategy_id": "smc", "coin": "BTCUSDT", "side": "short", "kind": "limit", "mode": "shadow",
                "status": "pending_limit", "signal_time": 1000, "sl": 105.0, "be_armed": False, "gap_affected": False}
        rec.trade(base)
        rec.trade({**base, "status": "open", "entry_price": 100.0, "entry_time": 2000})
        leg = {"trade_id": "t1", "seq": 1, "ts": 3000, "reason": "scale_1r", "frac": 0.5, "price": 95.0, "gross_r": 0.5, "fees_r": 0.01, "net_r": 0.49}
        rec.close_leg(leg)
        rec.close_leg(leg)  # повтор той же записи (seq) - не дубль
        await rec.flush()
        trades = await db.fetch("SELECT status, entry_price, created_at, updated_at FROM trades")
        legs = await db.fetch("SELECT * FROM trade_closes")
        await rec.aclose()
        return trades, legs

    trades, legs = run(body())
    assert len(trades) == 1 and trades[0]["status"] == "open" and trades[0]["entry_price"] == Decimal("100.0") and len(legs) == 1
    assert trades[0]["updated_at"] >= trades[0]["created_at"]


# ============================================================== strategy_id: миграция v2 и изоляция позиций в БД


async def _apply_v1_only(db: Database) -> None:
    """БД в состоянии ДО мульти-стратегийного релиза: только миграция 1."""
    async with db.pool.acquire() as conn:
        await conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        for ddl in schema.MIGRATIONS[0][1]:
            await conn.execute(ddl)
        await conn.execute("INSERT INTO schema_migrations (version) VALUES (1)")


@needs_db
def test_migration_v2_keeps_old_data_and_backfills_strategy_id():
    async def body():
        db = await fresh_db()
        await _apply_v1_only(db)
        ins = ("INSERT INTO trades (id, strategy_name, coin, side, kind, mode, status, signal_time, sl) "
               "VALUES ($1, $2, $3, $4, 'limit', 'shadow', $5, now(), 100)")
        async with db.pool.acquire() as conn:
            await conn.execute(ins, "smc:BTCUSDT:1", "smc", "BTCUSDT", "short", "closed")
            await conn.execute(ins, "breakout:ETHUSDT:1", "breakout", "ETHUSDT", "long", "closed")
            await conn.execute(ins, "smc:DOGEUSDT:1", "smc", "DOGEUSDT", "short", "open")  # висящая сделка прошлого запуска
            await conn.execute("INSERT INTO trade_snapshots (trade_id, created_at, candles, geometry) VALUES ('smc:BTCUSDT:1', now(), '[]', '[]')")
            await conn.execute("INSERT INTO state_events (ts, coin, strategy, kind) VALUES (now(), 'BTCUSDT', 'smc', 'setup_state'), (now(), 'BTCUSDT', 'htf', 'htf_bias')")
        version = await db.migrate()
        trades = {r["id"]: dict(r) for r in await db.fetch("SELECT id, strategy_name, strategy_id, status, exit_reason FROM trades")}
        snaps = [dict(r) for r in await db.fetch("SELECT trade_id, strategy_id FROM trade_snapshots")]
        events = {r["strategy"]: r["strategy_id"] for r in await db.fetch("SELECT strategy, strategy_id FROM state_events")}
        nullable = {r["table_name"]: r["is_nullable"] for r in await db.fetch(
            "SELECT table_name, is_nullable FROM information_schema.columns WHERE column_name = 'strategy_id'")}
        await db.close()
        return version, trades, snaps, events, nullable

    version, trades, snaps, events, nullable = run(body())
    assert version == 3
    assert trades["smc:BTCUSDT:1"]["strategy_id"] == "smc" and trades["breakout:ETHUSDT:1"]["strategy_id"] == "breakout"  # старые имена сохранены
    assert trades["smc:BTCUSDT:1"]["status"] == "closed"  # закрытые сделки не тронуты
    assert trades["smc:DOGEUSDT:1"]["status"] == "aborted" and trades["smc:DOGEUSDT:1"]["exit_reason"] == "process_restart"
    assert snaps == [{"trade_id": "smc:BTCUSDT:1", "strategy_id": "smc"}]
    assert events == {"smc": "smc", "htf": None}  # системное событие - не стратегия
    assert nullable == {"trades": "NO", "trade_snapshots": "NO", "state_events": "YES"}


@needs_db
def test_one_active_position_per_coin_and_strategy_but_strategies_do_not_collide():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        base = {"coin": "SOLUSDT", "kind": "market", "mode": "shadow", "status": "open", "signal_time": 1000, "sl": 105.0, "be_armed": False, "gap_affected": False}
        rec.trade({**base, "id": "a", "strategy_name": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "side": "short"})
        rec.trade({**base, "id": "b", "strategy_name": "VWAP_Reversion", "strategy_id": "VWAP_Reversion", "side": "long"})  # та же монета, другая сторона и стратегия
        await rec.flush()
        rows = await db.fetch("SELECT id, side, strategy_id FROM trades ORDER BY id")
        rec.trade({**base, "id": "c", "strategy_name": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "side": "long"})  # вторая активная под тем же ключом
        failure = None
        try:
            await rec.flush()
        except Exception as exc:  # писатель умер: ошибка схемы не подавляется (R1)
            failure = exc
        await db.close()
        return rows, failure

    rows, failure = run(body())
    assert [(r["id"], r["side"], r["strategy_id"]) for r in rows] == [("a", "short", "SMC_Pro_Trend"), ("b", "long", "VWAP_Reversion")]
    assert failure is not None and "trades_one_active_per_key" in str(failure)


@needs_db
def test_old_position_is_closed_before_the_same_key_can_open_again():
    async def body():
        db = await fresh_db()
        rec = PgRecorder(db)
        await rec.start()
        base = {"coin": "SOLUSDT", "kind": "market", "mode": "shadow", "signal_time": 1000, "sl": 105.0, "be_armed": False, "gap_affected": False,
                "strategy_name": "ICT_Killzones", "strategy_id": "ICT_Killzones", "side": "short"}
        rec.trade({**base, "id": "a", "status": "open"})
        rec.trade({**base, "id": "a", "status": "closed", "exit_reason": "sl"})
        rec.trade({**base, "id": "b", "status": "open"})  # ключ освободился - можно
        await rec.flush()
        rows = await db.fetch("SELECT id, status FROM trades ORDER BY id")
        await rec.aclose()
        return rows

    assert [(r["id"], r["status"]) for r in run(body())] == [("a", "closed"), ("b", "open")]
