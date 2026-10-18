"""Хранилище анализатора - SQLite в изолированном томе контейнера (решение пользователя): задания, отчёты, версии стратегий, календарь новостей.
Основная БД бота здесь не участвует. Доступ из двух процессов (API и воркер) - WAL + короткие транзакции."""

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from analyzer.errors import Conflict, NotFound
from analyzer.metrics import epoch_ms
from analyzer.versions import VersionDef

SCHEMA_VERSION = 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, created_at REAL NOT NULL, started_at REAL, finished_at REAL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed')),
    progress REAL NOT NULL DEFAULT 0, stage TEXT NOT NULL DEFAULT '', params TEXT NOT NULL, error TEXT
);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status, created_at);
CREATE TABLE IF NOT EXISTS reports (job_id TEXT PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE, created_at REAL NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS versions (name TEXT PRIMARY KEY, body TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS calendar (ts INTEGER NOT NULL, currency TEXT NOT NULL, impact TEXT NOT NULL, event TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS calendar_ts_idx ON calendar (ts);
CREATE UNIQUE INDEX IF NOT EXISTS calendar_uniq_idx ON calendar (ts, currency, event);
CREATE TABLE IF NOT EXISTS calendar_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class Store:
    def __init__(self, data_dir: Path):
        data_dir.mkdir(parents=True, exist_ok=True)
        self.path = data_dir / "analyzer.sqlite"
        conn = sqlite3.connect(self.path, timeout=30)
        try:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"SQLite анализатора новее программы (v{version} > v{SCHEMA_VERSION})")
            conn.executescript(SCHEMA)  # executescript сам управляет транзакцией, поэтому не через _tx
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        finally:
            conn.close()

    @contextmanager
    def _tx(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------------------------ задания

    def create_job(self, params: dict[str, Any], max_queue: int) -> str:
        with self._tx(immediate=True) as c:
            waiting = c.execute("SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')").fetchone()[0]
            if waiting >= max_queue:
                raise Conflict(f"Очередь заполнена ({waiting}): дождитесь завершения текущих заданий")
            job_id = uuid.uuid4().hex[:12]
            c.execute("INSERT INTO jobs (id, created_at, status, params) VALUES (?, ?, 'queued', ?)", (job_id, time.time(), json.dumps(params, default=str)))
        return job_id

    def claim_next(self) -> dict[str, Any] | None:
        with self._tx(immediate=True) as c:
            row = c.execute("SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1").fetchone()
            if row is None:
                return None
            c.execute("UPDATE jobs SET status = 'running', started_at = ?, stage = 'start' WHERE id = ?", (time.time(), row["id"]))
        return self._job(row) | {"status": "running"}

    def set_progress(self, job_id: str, stage: str, progress: float) -> None:
        with self._tx() as c:
            c.execute("UPDATE jobs SET stage = ?, progress = ? WHERE id = ? AND status = 'running'", (stage, max(0.0, min(1.0, progress)), job_id))

    def finish(self, job_id: str, report: dict[str, Any], keep: int) -> None:
        body = json.dumps(report, ensure_ascii=False, allow_nan=False)
        with self._tx(immediate=True) as c:
            c.execute("INSERT OR REPLACE INTO reports (job_id, created_at, body) VALUES (?, ?, ?)", (job_id, time.time(), body))
            c.execute("UPDATE jobs SET status = 'done', progress = 1, stage = 'done', finished_at = ? WHERE id = ?", (time.time(), job_id))
            old = c.execute("SELECT id FROM jobs WHERE status IN ('done', 'failed') ORDER BY finished_at DESC LIMIT -1 OFFSET ?", (keep,)).fetchall()
            c.executemany("DELETE FROM jobs WHERE id = ?", [(r["id"],) for r in old])

    def fail(self, job_id: str, error: str) -> None:
        with self._tx() as c:
            c.execute("UPDATE jobs SET status = 'failed', error = ?, finished_at = ? WHERE id = ?", (error[:2000], time.time(), job_id))

    def fail_orphans(self) -> int:
        """Задания, которые были 'running' при остановке контейнера/воркера, не продолжаются: помечаются failed."""
        with self._tx(immediate=True) as c:
            return c.execute("UPDATE jobs SET status = 'failed', error = 'worker_restart', finished_at = ? WHERE status = 'running'", (time.time(),)).rowcount

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        return {"id": row["id"], "status": row["status"], "progress": row["progress"], "stage": row["stage"], "created_at": row["created_at"], "started_at": row["started_at"],
                "finished_at": row["finished_at"], "error": row["error"], "params": json.loads(row["params"])}

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._tx() as c:
            row = c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFound("job_not_found")
        return self._job(row)

    def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._tx() as c:
            rows = c.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [self._job(r) for r in rows]

    def queue_depth(self) -> dict[str, int]:
        with self._tx() as c:
            return {r["status"]: r["n"] for r in c.execute("SELECT status, count(*) AS n FROM jobs GROUP BY status")}

    def get_report(self, job_id: str) -> dict[str, Any]:
        with self._tx() as c:
            row = c.execute("SELECT body FROM reports WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFound("report_not_found")
        return json.loads(row["body"])

    # ------------------------------------------------------------------------------------ версии

    def put_version(self, v: VersionDef, *, overwrite: bool) -> None:
        with self._tx(immediate=True) as c:
            exists = c.execute("SELECT 1 FROM versions WHERE name = ?", (v.name,)).fetchone()
            if exists and not overwrite:
                raise Conflict(f"Версия {v.name!r} уже есть (overwrite=true заменит)")
            c.execute("INSERT OR REPLACE INTO versions (name, body, created_at) VALUES (?, ?, ?)", (v.name, json.dumps(v.to_json()), time.time()))

    def list_versions(self) -> list[VersionDef]:
        with self._tx() as c:
            rows = c.execute("SELECT body FROM versions ORDER BY name").fetchall()
        return [VersionDef.from_json(json.loads(r["body"])) for r in rows]

    def delete_version(self, name: str) -> None:
        with self._tx() as c:
            if c.execute("DELETE FROM versions WHERE name = ?", (name,)).rowcount == 0:
                raise NotFound("version_not_found")

    # ------------------------------------------------------------------------------------ календарь

    def replace_calendar(self, events: pd.DataFrame, source: str) -> None:
        ms = epoch_ms(events["ts"]).tolist()
        rows = list(zip(ms, events["currency"], events["impact"], events["event"]))
        with self._tx(immediate=True) as c:
            c.execute("DELETE FROM calendar")
            c.executemany("INSERT INTO calendar (ts, currency, impact, event) VALUES (?, ?, ?, ?)", rows)
            c.execute("INSERT OR REPLACE INTO calendar_meta (key, value) VALUES ('source', ?), ('loaded_at', ?)", (source, str(time.time())))

    def merge_calendar(self, events: pd.DataFrame, source: str) -> int:
        """Добавляет события к накопленным (календарь бота хранит только текущую неделю - история копится здесь). Возвращает число НОВЫХ событий."""
        ms = epoch_ms(events["ts"]).tolist()
        rows = list(zip(ms, events["currency"], events["impact"], events["event"]))
        with self._tx(immediate=True) as c:
            before = c.execute("SELECT count(*) AS n FROM calendar").fetchone()["n"]
            c.executemany("INSERT OR IGNORE INTO calendar (ts, currency, impact, event) VALUES (?, ?, ?, ?)", rows)
            after = c.execute("SELECT count(*) AS n FROM calendar").fetchone()["n"]
            c.execute("INSERT OR REPLACE INTO calendar_meta (key, value) VALUES ('source', ?), ('loaded_at', ?)", (source, str(time.time())))
        return after - before

    def clear_calendar(self) -> None:
        with self._tx() as c:
            c.execute("DELETE FROM calendar")
            c.execute("DELETE FROM calendar_meta")

    def load_calendar(self) -> pd.DataFrame:
        with self._tx() as c:
            rows = c.execute("SELECT ts, currency, impact, event FROM calendar ORDER BY ts").fetchall()
        return pd.DataFrame({"ts": pd.to_datetime([r["ts"] for r in rows], unit="ms", utc=True), "currency": [r["currency"] for r in rows],
                             "impact": [r["impact"] for r in rows], "event": [r["event"] for r in rows]})

    def calendar_info(self) -> dict[str, Any]:
        with self._tx() as c:
            r = c.execute("SELECT count(*) AS n, min(ts) AS lo, max(ts) AS hi FROM calendar").fetchone()
            meta = {x["key"]: x["value"] for x in c.execute("SELECT key, value FROM calendar_meta")}
            by_impact = {x["impact"]: x["n"] for x in c.execute("SELECT impact, count(*) AS n FROM calendar GROUP BY impact")}
        iso = lambda ms: None if ms is None else pd.Timestamp(ms, unit="ms", tz="UTC").isoformat()  # noqa: E731
        return {"events": r["n"], "from": iso(r["lo"]), "to": iso(r["hi"]), "by_impact": by_impact, "source": meta.get("source"),
                "loaded_at": float(meta["loaded_at"]) if "loaded_at" in meta else None}
