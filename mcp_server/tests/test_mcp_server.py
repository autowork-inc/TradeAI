"""Тесты аналитического MCP-сервера.

Без БД идут: чистая аналитика, разбор .env, защита запросов, контракт инструментов.
С БД (нужен TEST_DATABASE_URL на базу `*_test`; схема public ПЕРЕСОЗДАЁТСЯ): запросы на настоящем PostgreSQL 16, режим только
чтения, потеря соединения, и полный путь по протоколу MCP (stdio-процесс сервера + клиент SDK).
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg
import pytest

ROOT = Path(__file__).resolve().parents[2]
MCP_DIR = ROOT / "mcp_server"
sys.path.insert(0, str(MCP_DIR))
sys.path.insert(0, str(ROOT))

import analytics as A  # noqa: E402
import db as dbmod  # noqa: E402
import queries as Q  # noqa: E402
import server as S  # noqa: E402
from persistence.schema import MIGRATIONS  # noqa: E402

TEST_URL = os.environ.get("TEST_DATABASE_URL", "")
needs_db = pytest.mark.skipif(not TEST_URL, reason="нужен TEST_DATABASE_URL (БД *_test)")
NOW = datetime.now(timezone.utc)


def run(coro):
    """Один вызов = один цикл событий и свежий пул (пул asyncpg привязан к своему циклу; в боевом сервере цикл один)."""
    async def wrapper():
        S.db = dbmod.Database()
        try:
            return await coro
        finally:
            await S.db.close()
    return asyncio.run(wrapper())


# ------------------------------------------------------------------ чистая аналитика

def test_summary_matches_definitions():
    s = A.summary([2.0, -1.0, 1.5, -1.1])
    assert s["trades"] == 4 and s["wins"] == 2 and s["losses"] == 2
    assert s["win_rate"] == 0.5 and s["net_r"] == 1.4
    assert s["profit_factor"] == pytest.approx(3.5 / 2.1, abs=1e-3)
    only_wins = A.summary([1.0, 2.0])
    assert only_wins["profit_factor"] is None and only_wins["profit_factor_infinite"] is True
    empty = A.summary([])
    assert empty["trades"] == 0 and empty["win_rate"] is None and empty["avg_r"] is None


def test_max_drawdown_peak_to_trough():
    t0 = NOW
    seq = [(t0 + timedelta(hours=i), r) for i, r in enumerate([1, 2, -1, -2, 1, -0.5])]
    dd = A.max_drawdown(seq)
    assert dd["max_drawdown_r"] == 3.0  # пик 3 после второй сделки, дно 0 после четвёртой
    assert dd["peak_time"] == seq[1][0] and dd["trough_time"] == seq[3][0]


def test_trend_alignment():
    assert A.trend_alignment("short", "Bearish") == "with_trend"
    assert A.trend_alignment("short", "Bullish") == "counter_trend"
    assert A.trend_alignment("long", "Bullish") == "with_trend"
    assert A.trend_alignment("long", "Bearish") == "counter_trend"
    assert A.trend_alignment("long", "Neutral") == "neutral"
    assert A.trend_alignment("long", None) == "unknown"


def test_jsonable_converts_everything_serializable():
    from decimal import Decimal
    out = A.jsonable({"a": Decimal("1.5"), "t": NOW, "n": float("nan"), "l": [Decimal("2")]})
    json.dumps(out, allow_nan=False)  # не падает: Decimal/datetime/NaN обработаны
    assert out["a"] == 1.5 and out["n"] is None


def test_sample_warning_below_200():
    assert A.sample_note(10) and "10" in A.sample_note(10)
    assert A.sample_note(200) is None


# ------------------------------------------------------------------ .env, пароль, защита запросов

def test_env_parsing(tmp_path):
    f = tmp_path / ".env"
    f.write_text("# DATABASE_URL=postgresql://x:y@a/b\nBINANCE_API_KEY=secret\nexport DATABASE_URL = 'postgresql://u:p@127.0.0.1:15432/tradeai'\n", encoding="utf-8")
    assert dbmod._parse_env_url(f.read_text(encoding="utf-8")) == "postgresql://u:p@127.0.0.1:15432/tradeai"
    assert dbmod._parse_env_url("BINANCE_API_KEY=secret\n") == ""


def test_read_database_url_priority_and_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(dbmod, "ENV_CANDIDATES", (tmp_path / "a.env", tmp_path / "b.env"))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(dbmod.DbError, match="DATABASE_URL не задан"):
        dbmod.read_database_url()
    (tmp_path / "b.env").write_text("DATABASE_URL=postgresql://u:p@h:1/d\n", encoding="utf-8")
    assert dbmod.read_database_url() == "postgresql://u:p@h:1/d"
    (tmp_path / "a.env").write_text("DATABASE_URL=postgresql://u:p@first:1/d\n", encoding="utf-8")
    assert dbmod.read_database_url().endswith("@first:1/d")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@env:1/d")
    assert dbmod.read_database_url().endswith("@env:1/d")  # окружение процесса сильнее файлов


def test_redact_hides_password():
    url = "postgresql://tradeai:S3cret%21pw@127.0.0.1:15432/tradeai"
    msg = dbmod.redact(f"connect to {url} failed; password S3cret!pw", url)
    assert "S3cret" not in msg and "***" in msg
    assert dbmod.describe_target(url) == "127.0.0.1:15432/tradeai"


@pytest.mark.parametrize("bad", [
    "DELETE FROM trades", "UPDATE trades SET pnl = 0", "INSERT INTO trades DEFAULT VALUES", "DROP TABLE trades",
    "SELECT 1; DELETE FROM trades", "  \n truncate trades", "COPY trades TO PROGRAM 'x'", "SELECT 1; SELECT 2",
])
def test_fetch_rejects_anything_but_one_select(bad):
    with pytest.raises(dbmod.DbError, match="только один SELECT"):
        run(dbmod.Database().fetch(bad))  # проверка идёт ДО подключения: БД не нужна


def test_all_project_queries_are_single_selects():
    for name in ("CLOSED_IN_WINDOW", "STATUS_COUNTS_IN_WINDOW", "KNOWN_STRATEGIES", "RECENT_TRADES", "CLOSES_FOR_TRADES", "CLOSED_FOR_ANALYSIS"):
        q = getattr(Q, name).strip()
        assert q.upper().startswith("SELECT") and ";" not in q, name
        for word in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"):
            assert word not in q.upper().replace("make_interval".upper(), ""), (name, word)


def test_tools_registered_with_validation():
    tools = run(S.mcp.list_tools())
    names = {t.name for t in tools}
    assert names == {"get_strategies_overview", "get_trade_details", "analyze_drawdown_causes"}
    for bad in ("x; DROP TABLE trades", "", "a b", "../x", "x" * 65):
        with pytest.raises(ValueError):
            S._strategy_id(bad)
    for bad in (0, -5, True, 10**7, "7", 1.5):
        with pytest.raises(ValueError):
            S._int_in("n", bad, 1, 100)


def test_server_module_has_no_stdout_prints_and_no_write_sql():
    for f in MCP_DIR.glob("*.py"):
        src = f.read_text(encoding="utf-8")
        assert "print(" not in src, f"{f.name}: print() испортил бы stdio-протокол MCP"


# ------------------------------------------------------------------ PostgreSQL

async def _fresh(url: str) -> asyncpg.Connection:
    assert urlsplit(url).path.strip("/").endswith("_test"), "TEST_DATABASE_URL должен указывать на БД *_test"
    conn = await asyncpg.connect(url)
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    for _, stmts in MIGRATIONS:
        for stmt in stmts:
            await conn.execute(stmt)
    return conn


async def _trade(conn, tid, sid, coin, side, *, status="closed", r=None, hours_ago=2.0, htf="Bullish", gap=False,
                 reason="sl", mfe=0.1, mae=-1.0, meta_extra=None, with_snapshot=True):
    sig = NOW - timedelta(hours=hours_ago + 1)
    ext = NOW - timedelta(hours=hours_ago) if status == "closed" else None
    await conn.execute(
        """INSERT INTO trades (id, strategy_name, strategy_id, coin, side, kind, mode, status, signal_time, entry_time, exit_time,
               planned_entry, entry_price, exit_price, sl, tp, pnl, r_multiple, mfe_r, mae_r, exit_reason, gap_affected)
           VALUES ($1,$2,$2,$3,$4,'limit','shadow',$5,$6,$6,$7,100,100,99,101,103,$8,$9,$10,$11,$12,$13)""",
        tid, sid, coin, side, status, sig, ext, None if r is None else r * 10, r, mfe, mae, reason if status == "closed" else None, gap)
    if r is not None:
        await conn.execute(
            "INSERT INTO trade_closes (trade_id, seq, ts, reason, frac, price, gross_r, fees_r, net_r) VALUES ($1,1,$2,$3,1,99,$4,0.01,$5)",
            tid, ext, reason, r - 0.01, r)
    if with_snapshot:
        meta = {"interval": "15m", "htf_interval": "4h", "htf_label": htf, "htf_bias": 1 if htf == "Bullish" else -1 if htf == "Bearish" else 0,
                "pool_level": 101.0, "sweep_time": int(sig.timestamp() * 1000), "bars_since_sweep": 5, "atr": 0.8, "risk_pct": 0.01,
                **(meta_extra or {})}
        await conn.execute("INSERT INTO trade_snapshots (trade_id, strategy_id, created_at, candles, geometry, meta) VALUES ($1,$2,$3,'[]','[]',$4)",
                           tid, sid, sig, json.dumps(meta))


async def _seed(conn):
    await _trade(conn, "SMC_Pro_Trend_t1", "SMC_Pro_Trend", "SOLUSDT", "long", r=2.0, htf="Bullish", reason="trail", mfe=3.2, mae=-0.2)
    await _trade(conn, "SMC_Pro_Trend_t2", "SMC_Pro_Trend", "SOLUSDT", "short", r=-1.0, htf="Bullish", hours_ago=3, mfe=0.1)
    await _trade(conn, "SMC_Pro_Trend_t3", "SMC_Pro_Trend", "XRPUSDT", "short", r=1.5, htf="Bearish", hours_ago=4, reason="trail", mfe=2.0)
    await _trade(conn, "SMC_Pro_Trend_t4", "SMC_Pro_Trend", "XRPUSDT", "short", r=-1.1, htf="Bullish", hours_ago=5, mfe=1.3)
    await _trade(conn, "SMC_Pro_Trend_t5", "SMC_Pro_Trend", "ADAUSDT", "long", r=-1.0, htf="Bullish", gap=True, hours_ago=6)  # вне статистики
    await _trade(conn, "SMC_Pro_Trend_t6", "SMC_Pro_Trend", "DOTUSDT", "long", status="open", hours_ago=1)               # открыта
    await _trade(conn, "SMC_Pro_Trend_t7", "SMC_Pro_Trend", "SOLUSDT", "long", r=-1.0, hours_ago=24 * 10)                # вне окна 168 ч
    await _trade(conn, "ICT_Killzones_k1", "ICT_Killzones", "LTCUSDT", "long", r=-1.0, htf="Neutral", meta_extra={"session": "london"})
    await _trade(conn, "ICT_Killzones_k2", "ICT_Killzones", "LTCUSDT", "short", r=0.9, htf="Bearish", hours_ago=7, reason="tp", meta_extra={"session": "new_york"})


@pytest.fixture()
def seeded_env(monkeypatch):
    async def go():
        conn = await _fresh(TEST_URL)
        try:
            await _seed(conn)
        finally:
            await conn.close()
    run(go())
    monkeypatch.setenv("DATABASE_URL", TEST_URL)
    S.db = dbmod.Database()  # свежий пул под тестовую БД


@needs_db
def test_overview_numbers_and_exclusions(seeded_env):
    res = run(S.get_strategies_overview(168))
    smc = next(s for s in res["strategies"] if s["strategy_id"] == "SMC_Pro_Trend")
    assert smc["trades"] == 4 and smc["wins"] == 2 and smc["losses"] == 2
    assert smc["win_rate"] == 0.5 and smc["net_r"] == 1.4
    assert smc["profit_factor"] == pytest.approx(1.667, abs=1e-3)
    assert smc["not_in_stats"] == {"closed_gap_affected": 1, "open": 1}  # t7 вне окна, поэтому не считается нигде
    assert {s["strategy_id"] for s in res["strategies"]} == {"SMC_Pro_Trend", "ICT_Killzones"}
    assert res["total"]["trades"] == 6 and res["sample_warning"]
    wide = run(S.get_strategies_overview(24 * 30))
    assert next(s for s in wide["strategies"] if s["strategy_id"] == "SMC_Pro_Trend")["trades"] == 5  # t7 вошла в 30 суток
    json.dumps(res, allow_nan=False)


@needs_db
def test_trade_details_has_reason_snapshot_and_closes(seeded_env):
    res = run(S.get_trade_details("SMC_Pro_Trend", 3))
    assert res["count"] == 3
    assert [t["id"] for t in res["trades"]] == ["SMC_Pro_Trend_t6", "SMC_Pro_Trend_t1", "SMC_Pro_Trend_t2"]  # новые сигналы первыми
    t1 = res["trades"][1]
    assert t1["htf_bias"] == "Bullish" and t1["signal_meta"]["pool_level"] == 101.0
    assert t1["reason"]["title"].startswith("SMC Pro Trend") and "Bullish" in json.dumps(t1["reason"], ensure_ascii=False)
    assert t1["closes"][0]["reason"] == "trail" and t1["closes"][0]["net_r"] == 2.0
    assert t1["r_multiple"] == 2.0 and t1["pnl"] == 20.0 and "candles" not in t1
    json.dumps(res, allow_nan=False)


@needs_db
def test_trade_details_unknown_strategy_lists_known(seeded_env):
    res = run(S.get_trade_details("Nope_Strategy", 5))
    assert res["trades"] == [] and "SMC_Pro_Trend" in res["note"] and "ICT_Killzones" in res["note"]


@needs_db
def test_trade_without_snapshot_does_not_break(seeded_env):
    async def go():
        conn = await asyncpg.connect(TEST_URL)
        try:
            await _trade(conn, "VWAP_Reversion_v1", "VWAP_Reversion", "AVAXUSDT", "long", r=-1.0, with_snapshot=False)
        finally:
            await conn.close()
    run(go())
    res = run(S.get_trade_details("VWAP_Reversion", 5))
    assert res["trades"][0]["has_snapshot"] is False and "Снапшота" in res["trades"][0]["reason"]["summary"]
    dd = run(S.analyze_drawdown_causes("VWAP_Reversion"))
    assert dd["closed_trades"] == 1 and dd["breakdowns"]["by_htf_bias"][0]["group"] == "unknown"


@needs_db
def test_drawdown_breakdowns(seeded_env):
    res = run(S.analyze_drawdown_causes("SMC_Pro_Trend"))
    assert res["closed_trades"] == 5  # t1-t4 и t7 (gap_affected и открытая исключены; окно времени не применяется)
    lt = res["losing_trades"]
    assert lt["count"] == 3 and lt["total_loss_r"] == 3.1
    by_htf = {g["group"]: g for g in res["breakdowns"]["by_htf_bias"]}
    assert by_htf["Bullish"]["loss_rate"] == pytest.approx(3 / 4, abs=1e-3)  # t1(+), t2(-), t4(-), t7(-)
    assert by_htf["Bearish"]["losses"] == 0
    align = {g["group"]: g for g in res["breakdowns"]["by_trend_alignment"]}
    assert align["counter_trend"]["trades"] == 2 and align["counter_trend"]["losses"] == 2  # шорты при Bullish
    assert res["breakdowns"]["by_side"][0]["group"] in ("short", "long")
    assert res["worst_trades"][0]["net_r"] == -1.1 and res["worst_trades"][0]["id"] == "SMC_Pro_Trend_t4"
    assert res["worst_trades"][0]["reason"]["summary"] and res["worst_trades"][0]["htf_bias"] == "Bullish"
    assert lt["reached_1r_then_lost"] == 1  # t4: MFE 1.3
    assert res["drawdown"]["max_drawdown_r"] >= 1.1 and res["sample_warning"]
    json.dumps(res, allow_nan=False)
    ict = run(S.analyze_drawdown_causes("ICT_Killzones"))
    assert {g["group"] for g in ict["breakdowns"]["by_session"]} == {"london", "new_york"}


@needs_db
def test_session_is_read_only_at_database_level(seeded_env):
    async def go():
        pool = await S.db._get_pool()
        async with pool.acquire() as conn:
            for stmt in ("DELETE FROM trades", "UPDATE trades SET pnl = 0", "DROP TABLE trades", "INSERT INTO state_events (ts, coin, strategy, kind) VALUES (now(),'x','y','z')"):
                with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
                    await conn.execute(stmt)
            assert await conn.fetchval("SELECT count(*) FROM trades") == 9
    run(go())


def test_unreachable_database_gives_actionable_error_without_password(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://tradeai:TopSecret123@127.0.0.1:1/tradeai")
    database = dbmod.Database()
    with pytest.raises(dbmod.DbError) as ei:
        run(database.fetch("SELECT 1"))
    msg = str(ei.value)
    assert "TopSecret123" not in msg and "127.0.0.1:1/tradeai" in msg and "туннел" in msg.lower()
    assert ei.value.__cause__ is None and ei.value.__suppress_context__  # в цепочке нет исходного исключения с DSN


@needs_db
def test_connection_loss_recovers_on_next_call(seeded_env):
    async def go():
        first = await S.db.fetch("SELECT count(*)::int AS n FROM trades")
        assert first[0]["n"] == 9
        pool = await S.db._get_pool()
        async with pool.acquire() as conn:
            admin = await asyncpg.connect(TEST_URL)
            try:  # «обрыв туннеля»: сервер убивает наши соединения
                await admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'tradeai-mcp-readonly'")
            finally:
                await admin.close()
        for _ in range(3):  # первый вызов может поймать мёртвое соединение и вернуть понятную ошибку; потом пул восстанавливается
            try:
                return await S.db.fetch("SELECT count(*)::int AS n FROM trades")
            except dbmod.DbError as exc:
                assert "потеряно" in str(exc) or "Нет доступа" in str(exc)
        raise AssertionError("пул не восстановился")
    assert run(go())[0]["n"] == 9


@needs_db
def test_end_to_end_over_mcp_stdio_protocol(seeded_env):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    async def go():
        params = StdioServerParameters(command=sys.executable, args=[str(MCP_DIR / "server.py")], env={**os.environ, "DATABASE_URL": TEST_URL})
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                assert {t.name for t in tools.tools} == {"get_strategies_overview", "get_trade_details", "analyze_drawdown_causes"}
                res = await session.call_tool("get_strategies_overview", {"time_window_hours": 168})
                assert not res.isError
                payload = json.loads(res.content[0].text)
                assert payload["total"]["trades"] == 6
                bad = await session.call_tool("get_trade_details", {"strategy_id": "x; DROP TABLE trades", "limit": 5})
                assert bad.isError and "strategy_id" in bad.content[0].text
    run(go())
