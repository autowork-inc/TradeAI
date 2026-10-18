"""Интеграционный тест на настоящем PostgreSQL: реальная схема бота (persistence/schema.py) + SQL-роль analyzer_ro + загрузчик анализатора.

Запускается, только если задан ANALYZER_TEST_ADMIN_URL - DSN суперпользователя на БД `*_test` (схема public ПЕРЕСОЗДАЁТСЯ; на боевую базу это не должно попасть),
например: postgresql://postgres:pw@127.0.0.1:54331/tradeai_test. Проверяет то, чего не видят юнит-тесты: что SELECT загрузчика совпадает со схемой бота,
что роль читает ровно четыре таблицы и физически не может писать/менять/читать schema_migrations.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import psycopg
import pytest

from analyzer.config import Settings
from analyzer.db import LoadQuery, load_trades

ADMIN = os.environ.get("ANALYZER_TEST_ADMIN_URL", "")
pytestmark = pytest.mark.skipif(not ADMIN, reason="нужен ANALYZER_TEST_ADMIN_URL (БД *_test)")
ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "ro_pw_1234567890"
SQL_FILE = ROOT / "analyzer" / "sql" / "001_analyzer_ro.sql"


def bot_schema():
    """persistence/schema.py бота: чистый модуль без зависимостей, подгружается по пути (пакет analyzer остаётся изолированным)."""
    spec = importlib.util.spec_from_file_location("bot_schema", ROOT / "persistence" / "schema.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bot_schema"] = mod
    spec.loader.exec_module(mod)
    return mod


def ro_dsn() -> str:
    u = urlsplit(ADMIN)
    return urlunsplit((u.scheme, f"analyzer_ro:{PASSWORD}@{u.hostname}:{u.port}", u.path, "", ""))


def candles(n=30, last_volume=40.0):
    base = 1_790_000_000_000
    cs = [{"open_time": base + i * 300_000, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10.0, "atr": 1.0} for i in range(n - 1)]
    return cs + [{"open_time": base + n * 300_000, "open": 100, "high": 103, "low": 100, "close": 102, "volume": last_volume, "atr": 1.0}]


@pytest.fixture(scope="module")
def db():
    assert urlsplit(ADMIN).path.strip("/").endswith("_test"), "ANALYZER_TEST_ADMIN_URL должен указывать на БД *_test"
    schema = bot_schema()
    dbname = urlsplit(ADMIN).path.strip("/")
    with psycopg.connect(ADMIN, autocommit=True) as c:
        c.execute("DROP SCHEMA public CASCADE")
        c.execute("CREATE SCHEMA public")
        c.execute("DROP OWNED BY analyzer_ro") if c.execute("SELECT 1 FROM pg_roles WHERE rolname = 'analyzer_ro'").fetchone() else None
        c.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        for version, statements in schema.MIGRATIONS:
            for stmt in statements:
                c.execute(stmt)
            c.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
        t0 = pd.Timestamp("2026-10-01 10:00", tz="UTC")
        plan = json.dumps({"levels": [[1.0, 0.5], [3.0, 0.4]], "be_after_first": True, "runner_frac": 0.1, "trail_mode": "atr", "trail_atr_mult": 3.0, "trail_wick_bars": 3, "tp_anchor": "signal"})
        specs = [("w1", "closed", False, 1.95, "shadow"), ("l1", "closed", False, -1.05, "shadow"), ("gap", "closed", True, 0.5, "shadow"), ("open1", "open", False, None, "shadow"),
                 ("ab1", "aborted", False, None, "shadow"), ("live1", "closed", False, 0.7, "live")]
        for i, (tid, status, gap, r, mode) in enumerate(specs):
            sig = t0 + pd.Timedelta(hours=i)
            c.execute("""INSERT INTO trades (id, strategy_name, strategy_id, coin, side, kind, mode, status, signal_time, entry_time, exit_time, sl, pnl, fees, r_multiple, gross_r,
                                             mfe_r, mae_r, exit_reason, gap_affected, plan)
                         VALUES (%s, 'S', 'SMC_Pro_Trend', %s, 'long', 'limit', %s, %s, %s, %s, %s, 99, %s, 1.5, %s, %s, 2.4, 0.3, 'tp', %s, %s::jsonb)""",
                      (tid, f"C{i}USDT", mode, status, sig, sig, sig + pd.Timedelta(hours=1) if status == "closed" else None,
                       None if r is None else r * 100, r, None if r is None else r + 0.05, gap, plan))
            c.execute("INSERT INTO trade_snapshots (trade_id, strategy_id, created_at, candles, geometry, meta) VALUES (%s, 'SMC_Pro_Trend', %s, %s::jsonb, '[]'::jsonb, %s::jsonb)",
                      (tid, sig, json.dumps(candles()), json.dumps({"interval": "5m", "entry": 100.0, "require_buildup": i % 2 == 0})))
        c.execute("INSERT INTO trade_closes (trade_id, seq, ts, reason, frac, price, gross_r, fees_r, net_r) VALUES ('w1', 1, %s, 'tp', 1, 101, 2, 0.05, 1.95)", (t0,))
        c.execute("INSERT INTO state_events (ts, coin, strategy, strategy_id, kind) VALUES (%s, 'C0USDT', 'S', 'SMC_Pro_Trend', 'x')", (t0,))
        script = SQL_FILE.read_text(encoding="utf-8").replace(":'analyzer_password'", f"'{PASSWORD}'").replace(':"DBNAME"', f'"{dbname}"')
        c.execute(script)  # тот же файл, что применяет deploy/shadow.sh (psql-переменные подставлены вручную)
    yield
    with psycopg.connect(ADMIN, autocommit=True) as c:
        c.execute("DROP OWNED BY analyzer_ro")
        c.execute("DROP ROLE analyzer_ro")


def settings(tmp_path):
    return Settings(database_url=ro_dsn(), token="t" * 40, data_dir=tmp_path)


def test_loader_reads_closed_non_gap_trades_and_builds_features(db, tmp_path):
    df = load_trades(settings(tmp_path), LoadQuery(mode="shadow"))
    assert sorted(df["id"]) == ["l1", "w1"]  # open/aborted/gap_affected и live исключены
    w1 = df[df["id"] == "w1"].iloc[0]
    assert w1["r"] == pytest.approx(1.95) and w1["vol_ratio"] == pytest.approx(4.0) and w1["range_atr"] == pytest.approx(3.0)
    assert w1["meta"]["require_buildup"] is True and w1["plan"]["be_after_first"] is True and w1["plan_hash"] != "noplan"
    assert str(df["exit_time"].dtype).startswith("datetime64") and df["pnl"].notna().all()
    assert list(load_trades(settings(tmp_path), LoadQuery(mode="live"))["id"]) == ["live1"]
    assert len(load_trades(settings(tmp_path), LoadQuery(mode="all"))) == 3
    assert list(load_trades(settings(tmp_path), LoadQuery(mode="all", coins=("C0USDT",)))["id"]) == ["w1"]
    assert list(load_trades(settings(tmp_path), LoadQuery(mode="all", since=pd.Timestamp("2026-10-01 12:30", tz="UTC").to_pydatetime()))["id"]) == ["live1"]


def test_loader_refuses_to_truncate_silently(db, tmp_path):
    from analyzer.errors import DataError
    s = Settings(database_url=ro_dsn(), token="t" * 40, data_dir=tmp_path, max_trades=1)
    with pytest.raises(DataError, match="больше предела"):
        load_trades(s, LoadQuery(mode="all"))


def test_role_can_only_select_the_four_trade_tables(db):
    with psycopg.connect(ro_dsn(), autocommit=True) as c:
        for table in ("trades", "trade_closes", "trade_snapshots", "state_events"):
            assert c.execute(f"SELECT count(*) FROM {table}").fetchone()[0] >= 1
        assert c.execute("SHOW default_transaction_read_only").fetchone()[0] == "on"
        assert c.execute("SHOW default_transaction_isolation").fetchone()[0] == "repeatable read"
        for sql in ("INSERT INTO trades (id) VALUES ('x')", "UPDATE trades SET pnl = 0", "DELETE FROM trades", "TRUNCATE trades", "DROP TABLE trades", "CREATE TABLE evil (x int)",
                    "SELECT * FROM schema_migrations", "ALTER TABLE trades ADD COLUMN z int", "CREATE ROLE evil"):
            with pytest.raises(psycopg.Error):
                c.execute(sql)
        # даже если попросить запись явно: сессия read-only
        c.execute("SET default_transaction_read_only = off")
        with pytest.raises(psycopg.Error):
            c.execute("UPDATE trades SET pnl = 0")


def test_role_script_is_idempotent_and_updates_the_password(db):
    dbname = urlsplit(ADMIN).path.strip("/")
    script = SQL_FILE.read_text(encoding="utf-8").replace(":'analyzer_password'", "'second_pw_1234567'").replace(':"DBNAME"', f'"{dbname}"')
    with psycopg.connect(ADMIN, autocommit=True) as c:
        c.execute(script)
        c.execute(script)
    u = urlsplit(ADMIN)
    with psycopg.connect(urlunsplit((u.scheme, f"analyzer_ro:second_pw_1234567@{u.hostname}:{u.port}", u.path, "", ""))) as c:
        assert c.execute("SELECT count(*) FROM trades").fetchone()[0] >= 1
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(ro_dsn())  # прежний пароль больше не подходит
    with psycopg.connect(ADMIN, autocommit=True) as c:  # вернуть пароль для остальных тестов модуля
        c.execute(SQL_FILE.read_text(encoding="utf-8").replace(":'analyzer_password'", f"'{PASSWORD}'").replace(':"DBNAME"', f'"{dbname}"'))
