"""История прогонов Playground: SQLite (WAL) - прогон = стратегии + монета + период; сделки прогона со снапшотом, закрытиями и хронологией.

Модуль без зависимостей от кода бота: его копирует и образ API (создание прогона в очередь, чтение истории).
Пишут несколько процессов (фоновые прогоны, воркер сервиса): короткие транзакции, busy_timeout. Сделки прогона записываются одной транзакцией в конце.
"""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:  # хранилище читает и API (лёгкий образ без кода стратегий): движок не импортируется
    from playground.engine import RunResult

# стратегии Playground (engine.PLAYGROUND_STRATEGIES; равенство проверяет тест): все Shadow, кроме OBI_Breakout (нет истории стакана)
AVAILABLE_STRATEGIES = ("SMC_Pro_Trend", "SMC_Counter_Trend", "ICT_Killzones", "VWAP_Reversion", "Horizontal_Breakout", "Trendline_Breakout",
                        "Horizontal_Breakout_CHR", "Trendline_Breakout_CHR")
CHR_STRATEGIES = ("Trendline_Breakout_CHR", "Horizontal_Breakout_CHR")


class DataError(Exception):
    """Прогон или сделка не найдены."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY, created_ms INTEGER NOT NULL, status TEXT NOT NULL, symbol TEXT NOT NULL, strategies TEXT NOT NULL,
    start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL, tp_r TEXT NOT NULL DEFAULT '{}', label TEXT, profile TEXT, batch TEXT,
    progress REAL NOT NULL DEFAULT 0, stage TEXT, error TEXT, summary TEXT, bars INTEGER, seconds REAL, finished_ms INTEGER, code_version TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, trade_id TEXT NOT NULL, strategy_id TEXT NOT NULL, status TEXT NOT NULL,
    signal_time INTEGER, row TEXT NOT NULL, closes TEXT NOT NULL, snapshot TEXT, events TEXT NOT NULL, PRIMARY KEY (run_id, trade_id)
);
CREATE INDEX IF NOT EXISTS trades_run ON trades(run_id, signal_time);
CREATE TABLE IF NOT EXISTS skipped (run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE, rows TEXT NOT NULL, PRIMARY KEY (run_id));
"""
STATUSES = ("queued", "running", "done", "failed")


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self._db() as db:
            db.executescript(SCHEMA)

    def _db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=60, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=60000")
        return db

    # ------------------------------------------------------------------------------------------ прогоны

    def create(self, symbol: str, strategies: Sequence[str], start_ms: int, end_ms: int, *, tp_r: dict[str, float] | None = None, label: str | None = None,
               profile: str | None = None, batch: str | None = None, status: str = "queued", code_version: str | None = None) -> str:
        run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
        with self._db() as db:
            db.execute("INSERT INTO runs (id, created_ms, status, symbol, strategies, start_ms, end_ms, tp_r, label, profile, batch, code_version) "
                       "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                       (run_id, int(time.time() * 1000), status, symbol, json.dumps(list(strategies)), start_ms, end_ms,
                        json.dumps(dict(tp_r or {})), label, profile, batch, code_version))
        return run_id

    def progress(self, run_id: str, progress: float, stage: str, status: str = "running") -> None:
        with self._db() as db:
            db.execute("UPDATE runs SET progress=?, stage=?, status=? WHERE id=?", (progress, stage, status, run_id))

    def fail(self, run_id: str, error: str) -> None:
        with self._db() as db:
            db.execute("UPDATE runs SET status='failed', error=?, finished_ms=? WHERE id=?", (error[:2000], int(time.time() * 1000), run_id))

    def next_queued(self) -> str | None:
        """Взять следующий прогон из очереди (атомарно: queued -> running)."""
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT id FROM runs WHERE status='queued' ORDER BY created_ms LIMIT 1").fetchone()
            if row is None:
                db.execute("COMMIT")
                return None
            db.execute("UPDATE runs SET status='running', stage='старт' WHERE id=?", (row["id"],))
            db.execute("COMMIT")
            return row["id"]

    def orphans_failed(self) -> int:
        """После рестарта сервиса: «running» без процесса -> failed."""
        with self._db() as db:
            return db.execute("UPDATE runs SET status='failed', error='прогон прерван перезапуском сервиса' WHERE status='running'").rowcount

    def save_result(self, run_id: str, result: "RunResult", summary: dict[str, Any]) -> dict[str, Any]:
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM trades WHERE run_id=?", (run_id,))
            db.executemany(
                "INSERT INTO trades (run_id, trade_id, strategy_id, status, signal_time, row, closes, snapshot, events) VALUES (?,?,?,?,?,?,?,?,?)",
                [(run_id, t["id"], t["strategy_id"], t["status"], t.get("signal_time"), json.dumps(t, default=str),
                  json.dumps(result.closes.get(t["id"], []), default=str), json.dumps(result.snapshots.get(t["id"]), default=str),
                  json.dumps(result.events.get(t["id"], []), default=str)) for t in result.trades])
            db.execute("INSERT OR REPLACE INTO skipped (run_id, rows) VALUES (?,?)", (run_id, json.dumps(result.skipped, default=str)))
            db.execute("UPDATE runs SET status='done', progress=1, stage='готово', summary=?, bars=?, seconds=?, finished_ms=? WHERE id=?",
                       (json.dumps(summary), result.bars, result.seconds, int(time.time() * 1000), run_id))
            db.execute("COMMIT")
        return summary

    def delete(self, run_id: str) -> bool:
        with self._db() as db:
            return db.execute("DELETE FROM runs WHERE id=? AND status IN ('done','failed','queued')", (run_id,)).rowcount > 0

    # ------------------------------------------------------------------------------------------ чтение

    @staticmethod
    def _run_row(r: sqlite3.Row, with_summary: bool = True) -> dict[str, Any]:
        d = dict(r)
        d["strategies"] = json.loads(d["strategies"])
        d["tp_r"] = json.loads(d["tp_r"] or "{}")
        summary = json.loads(d["summary"]) if d.get("summary") else None
        if summary is not None and not with_summary:  # список: без кривых доходности (они тяжёлые)
            for s in (summary["overall"], *summary["by_strategy"].values()):
                s.pop("curve", None)
        d["summary"] = summary
        return d

    def runs(self, strategies_subset: Sequence[str] | None = None, limit: int = 200) -> list[dict[str, Any]]:
        """Прогоны, новые сверху. strategies_subset: только прогоны, все стратегии которых входят в набор (профиль CHR)."""
        with self._db() as db:
            rows = db.execute("SELECT * FROM runs ORDER BY created_ms DESC LIMIT ?", (limit,)).fetchall()
        out = [self._run_row(r, with_summary=False) for r in rows]
        if strategies_subset is not None:
            allowed = set(strategies_subset)
            out = [r for r in out if set(r["strategies"]) <= allowed]
        return out

    def run(self, run_id: str) -> dict[str, Any]:
        with self._db() as db:
            r = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if r is None:
            raise DataError(f"Прогон {run_id} не найден")
        return self._run_row(r)

    def trades(self, run_id: str, strategy: str | None = None) -> list[dict[str, Any]]:
        sql, args = "SELECT row FROM trades WHERE run_id=?", [run_id]
        if strategy:
            sql += " AND strategy_id=?"
            args.append(strategy)
        with self._db() as db:
            rows = db.execute(sql + " ORDER BY signal_time", args).fetchall()
        return [json.loads(r["row"]) for r in rows]

    def trade(self, run_id: str, trade_id: str) -> dict[str, Any]:
        with self._db() as db:
            r = db.execute("SELECT * FROM trades WHERE run_id=? AND trade_id=?", (run_id, trade_id)).fetchone()
        if r is None:
            raise DataError(f"Сделка {trade_id} прогона {run_id} не найдена")
        return {"trade": json.loads(r["row"]), "closes": json.loads(r["closes"]), "snapshot": json.loads(r["snapshot"]) if r["snapshot"] else None,
                "events": json.loads(r["events"])}

    def skipped(self, run_id: str) -> list[dict[str, Any]]:
        with self._db() as db:
            r = db.execute("SELECT rows FROM skipped WHERE run_id=?", (run_id,)).fetchone()
        return json.loads(r["rows"]) if r else []

    # ------------------------------------------------------------------------------------------ перенос

    def export_runs(self, run_ids: Sequence[str], target: "Store") -> int:
        """Копия готовых прогонов (с их сделками) в другое хранилище - локальные фоновые прогоны -> хранилище сервера."""
        n = 0
        with self._db() as src, target._db() as dst:
            for rid in run_ids:
                run = src.execute("SELECT * FROM runs WHERE id=? AND status='done'", (rid,)).fetchone()
                if run is None:
                    continue
                dst.execute("BEGIN IMMEDIATE")
                cols = run.keys()
                dst.execute(f"INSERT OR REPLACE INTO runs ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})", tuple(run))
                dst.execute("DELETE FROM trades WHERE run_id=?", (rid,))
                for t in src.execute("SELECT * FROM trades WHERE run_id=?", (rid,)).fetchall():
                    dst.execute(f"INSERT INTO trades ({','.join(t.keys())}) VALUES ({','.join('?' * len(t.keys()))})", tuple(t))
                sk = src.execute("SELECT * FROM skipped WHERE run_id=?", (rid,)).fetchone()
                if sk is not None:
                    dst.execute("INSERT OR REPLACE INTO skipped (run_id, rows) VALUES (?,?)", tuple(sk))
                dst.execute("COMMIT")
                n += 1
        return n
