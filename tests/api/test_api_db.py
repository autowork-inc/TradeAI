"""API мониторинга на настоящем PostgreSQL (как tests/persistence/test_pg.py: нужен TEST_DATABASE_URL на БД *_test, схема public пересоздаётся).

Данные пишутся НАСТОЯЩИМ кодом записи (Оркестратор -> VirtualTracker -> PgRecorder), а не вручную: если формат снапшота или
состояний изменится, тест API упадёт, а не продолжит проверять устаревшую выдумку.
"""

import asyncio
import dataclasses
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from api.main import connect_pool, create_app
from api.settings import Settings
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.tracker import VirtualTracker
from orchestrator.types import BarInfo, Direction, FeatureContext, Mode, geometry
from persistence import schema
from persistence.db import Database
from persistence.pg import PgRecorder, to_args
from persistence.recorder import RecordingEventLog
from tests.orchestrator.helpers import IV, Scripted, mk, tick

TEST_URL = os.environ.get("TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not TEST_URL, reason="нужен TEST_DATABASE_URL (БД *_test)")
KEY = "test-api-key-" + "x" * 30
H = {"X-API-Key": KEY}
T0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)  # относительно «сейчас»: окно состояний в БД считается от now()
T0_MS = int(T0.timestamp() * 1000)


def run(coro):
    return asyncio.run(coro)


def trade_row(tid, coin, r, minutes, *, status="closed", strategy="SMC_Pro_Trend", side="short", reason="tp", gap=False):
    ts = T0_MS + minutes * 60_000
    return {"id": tid, "strategy_name": strategy, "strategy_id": strategy, "coin": coin, "side": side, "kind": "limit", "mode": "shadow", "status": status,
            "signal_time": ts - 600_000, "sl": 105.0, "tp": 85.0, "planned_entry": 100.0, "be_armed": False, "gap_affected": gap,
            "r_multiple": r if status == "closed" else None, "exit_time": ts if status == "closed" else None,
            "exit_reason": reason if status == "closed" else None, "entry_price": 100.0, "entry_time": ts - 300_000}


def candles(n=30):
    f = pl.DataFrame({"open_time": [i * IV for i in range(n)], "open": [100.0 + i for i in range(n)], "high": [101.0 + i for i in range(n)],
                      "low": [99.0 + i for i in range(n)], "close": [100.5 + i for i in range(n)], "volume": [10.0] * n,
                      "atr": [None] + [2.0] * (n - 1)})
    return f


async def seed(url: str) -> dict:
    """Чистая БД: один полный жизненный цикл настоящим кодом + набор «ручных» закрытых сделок для статистики."""
    db = await Database.connect(url, attempts=3, delay_s=0.2)
    async with db.pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    rec = PgRecorder(db)
    await rec.start()
    journal = RecordingEventLog(MemoryEventLog(), rec)
    tracker = VirtualTracker(journal, equity=10_000.0, recorder=rec)
    sig = dataclasses.replace(
        mk("SMC_Pro_Trend", bar=3, direction=Direction.SHORT),
        geometry=(geometry("zone", 0, 9 * IV, 102.0, 98.0, "FVG"), geometry("line", IV, 2 * IV, 105.0, 105.0, "SL")),
        meta={"pool_level": 104.0, "sweep_time": 2 * IV, "bars_since_sweep": 1, "risk_pct": 0.05, "atr": 2.0})
    orch = Orchestrator([Scripted("SMC_Pro_Trend", {3: [sig]})], {"SMC_Pro_Trend": Route(Mode.SHADOW, tracker)}, journal, recorder=rec, snapshot_bars=10)
    f = candles(4)
    for i in range(4):
        orch.on_context(FeatureContext("BTCUSDT", "15m", IV, f.head(i + 1), (i + 1) * IV + 1000))
    tracker.on_tick(tick(100.5, 10_000))
    tracker.on_tick(tick(94.0, 20_000))
    tracker.on_tick(tick(84.0, 21_000))
    tracker.on_bar_closed("BTCUSDT", 4 * IV, 90.0, 22_000, BarInfo(91.0, 88.0, 2.0))
    tracker.on_tick(tick(90.5, 30_000))
    await rec.flush()
    full_id = tracker.trades[0].trade_id
    # ручные сделки (в будущем относительно настоящей, чтобы порядок по времени был известен)
    rows = [trade_row("m1", "BTCUSDT", 2.0, 10), trade_row("m2", "BTCUSDT", -1.0, 20, reason="sl"), trade_row("m3", "ETHUSDT", -1.0, 30, reason="sl"),
            trade_row("m4", "ETHUSDT", -1.0, 40, reason="sl"), trade_row("m5", "BTCUSDT", 0.5, 50, reason="trail"),
            trade_row("m6", "DOGEUSDT", 3.0, 60, strategy="breakout", side="long"),
            trade_row("g1", "BTCUSDT", 10.0, 70, gap=True),                       # закрыта с обрывом потока: в статистику не входит
            trade_row("a1", "BTCUSDT", None, 80, status="aborted"), trade_row("o1", "ETHUSDT", None, 90, status="open")]
    async with db.pool.acquire() as conn:
        for row in rows:
            await conn.execute(schema.UPSERT_TRADE, *to_args(row, schema.TRADE_COLUMNS))
        for ev in ({"ts": T0_MS, "coin": "BTCUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": "short:S0", "to_state": "short:S1",
                    "kind": "setup_state", "reason": None, "detail": {"side": "short", "from": 0, "to": 1}},
                   {"ts": T0_MS + 1000, "coin": "BTCUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": "short:S1", "to_state": "short:S2",
                    "kind": "setup_state", "reason": None, "detail": {"side": "short", "from": 1, "to": 2}},
                   {"ts": T0_MS + 2000, "coin": "ETHUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": "long:S2", "to_state": "long:S3",
                    "kind": "setup_state", "reason": None, "detail": {"side": "long", "from": 2, "to": 3, "reject": "exposure"}}):
            await conn.execute(schema.INSERT_STATE_EVENT, *to_args(ev, schema.STATE_EVENT_COLUMNS))
        # событие давно, а не в окне - не должно влиять на текущее состояние
        old = {"ts": T0_MS - 30 * 86_400_000, "coin": "DOGEUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": "long:S0", "to_state": "long:S4",
               "kind": "setup_state", "reason": None, "detail": {}}
        await conn.execute(schema.INSERT_STATE_EVENT, *to_args(old, schema.STATE_EVENT_COLUMNS))
    await rec.aclose()
    return {"full_id": full_id}


def make_settings(tmp_path: Path, **kw) -> Settings:
    base = dict(database_url=TEST_URL, api_key=KEY, shadow_data_dir=tmp_path, poll_interval_s=0.2, keepalive_s=1.0)
    return Settings(**{**base, "coins": ("BTCUSDT", "ETHUSDT", "DOGEUSDT", "1000PEPEUSDT"), **kw})  # фикстуры - прежние монеты, логика от пула не зависит


@pytest.fixture()
def seeded():
    assert urlsplit(TEST_URL).path.strip("/").endswith("_test"), "TEST_DATABASE_URL должен указывать на БД *_test"
    return run(seed(TEST_URL))


@pytest.fixture()
def client(seeded, tmp_path):
    (tmp_path / ".shadow_heartbeat").write_text("1")
    with TestClient(create_app(make_settings(tmp_path))) as c:
        c.seeded = seeded
        yield c


# ================================================================================== защита


def test_every_data_endpoint_requires_the_key_but_health_does_not(client):
    assert client.get("/api/health").json() == {"status": "ok"}
    for path in ("/api/info", "/api/stats", "/api/trades", "/api/trades/x/snapshot"):
        r = client.get(path)
        assert r.status_code == 401 and r.json() == {"detail": "invalid_api_key"}
        assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/api/info", headers=H).status_code == 200


def test_key_in_query_string_is_not_accepted(client):
    assert client.get(f"/api/info?api_key={KEY}").status_code == 401 and client.get(f"/api/info?X-API-Key={KEY}").status_code == 401


def test_openapi_and_docs_are_not_exposed(client):
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404 and client.get(path, headers=H).status_code == 404


def test_brute_force_is_throttled_even_for_the_correct_key(seeded, tmp_path):
    with TestClient(create_app(make_settings(tmp_path, auth_max_failures=3))) as c:
        for _ in range(3):
            assert c.get("/api/info", headers={"X-API-Key": "bad"}).status_code == 401
        r = c.get("/api/info", headers=H)
        assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1  # окно блокировки действует и на верный ключ


def test_database_session_is_read_only(seeded):
    async def body():
        pool = await connect_pool(TEST_URL, attempts=2, delay_s=0.1)
        try:
            async with pool.acquire() as conn:
                with pytest.raises(Exception, match="read-only"):
                    await conn.execute("DELETE FROM trades")
                with pytest.raises(Exception, match="read-only"):
                    await conn.execute("UPDATE trades SET sl = 1")
                return await conn.fetchval("SELECT count(*) FROM trades")
        finally:
            await pool.close()

    assert run(body()) == 10  # 1 настоящая + 9 ручных: ничего не удалено


# ================================================================================== /api/info и /api/stats


def test_info_reports_schema_version_counts_and_heartbeat(client):
    r = client.get("/api/info", headers=H).json()
    assert r["schema_version"] == schema.SCHEMA_VERSION and r["coins"][-1] == "1000PEPEUSDT"
    assert r["counts"] == {"closed": 7, "closed_gap_affected": 1, "open": 1, "pending": 0, "cancelled": 0, "aborted": 1}
    assert r["shadow"]["alive"] is True and r["shadow"]["heartbeat_age_s"] < 30


def test_stats_aggregate_only_clean_closed_trades(client):
    s = client.get("/api/stats", headers=H).json()
    o = s["overall"]
    full = next(c for c in s["by_coin"] if c["coin"] == "BTCUSDT")
    # настоящая сделка (Scale Out: 1R + 3R + трейлинг) входит как одна закрытая; r у неё положительный
    assert o["trades"] == 7 and o["wins"] >= 4 and s["counts"]["closed_gap_affected"] == 1
    assert full["trades"] == 4  # m1, m2, m5 + настоящая; g1 (gap_affected) не считается
    assert [c["coin"] for c in s["by_coin"]][-1] == "1000PEPEUSDT" and s["by_coin"][-1]["trades"] == 0
    assert s["best_coin"] in {"BTCUSDT", "DOGEUSDT"} and s["worst_coin"] == "ETHUSDT"
    assert s["significance"]["enough"] is False and s["equity_curve"][-1]["cum_r"] == pytest.approx(o["net_r"])


def test_stats_filters(client):
    only = client.get("/api/stats", headers=H, params={"coin": "ETHUSDT"}).json()
    assert only["overall"]["trades"] == 2 and only["overall"]["net_r"] == pytest.approx(-2.0) and only["filters"]["coin"] == ["ETHUSDT"]
    longs = client.get("/api/stats", headers=H, params={"side": "long"}).json()
    assert longs["overall"]["trades"] == 1 and longs["best_coin"] == "DOGEUSDT"
    since = (T0 + timedelta(minutes=45)).isoformat()
    late = client.get("/api/stats", headers=H, params={"since": since}).json()
    assert late["overall"]["trades"] == 2 and {c["coin"] for c in late["by_coin"] if c["trades"]} == {"BTCUSDT", "DOGEUSDT"}
    assert client.get("/api/stats", headers=H, params={"coin": "BTC;DROP"}).status_code == 422
    assert client.get("/api/stats", headers=H, params={"mode": "paper"}).status_code == 422
    live = client.get("/api/stats", headers=H, params={"mode": "live"}).json()
    assert live["overall"]["trades"] == 0 and live["best_coin"] is None


# ================================================================================== /api/trades


def test_trades_pagination_and_ordering(client):
    page1 = client.get("/api/trades", headers=H, params={"limit": 4}).json()
    assert page1["total"] == 10 and len(page1["items"]) == 4 and page1["limit"] == 4 and page1["offset"] == 0
    times = [t["signal_time"] for t in page1["items"]]
    assert times == sorted(times, reverse=True)  # новые сверху
    page3 = client.get("/api/trades", headers=H, params={"limit": 4, "offset": 8}).json()
    assert len(page3["items"]) == 2
    ids = {t["id"] for t in page1["items"]} | {t["id"] for t in client.get("/api/trades", headers=H, params={"limit": 4, "offset": 4}).json()["items"]} \
        | {t["id"] for t in page3["items"]}
    assert len(ids) == 10  # страницы не пересекаются и не теряют строки
    first = page1["items"][0]
    assert first["signal_time"].endswith("Z") and isinstance(first["sl"], float)


def test_trades_filters_by_status_and_coin(client):
    closed = client.get("/api/trades", headers=H, params={"status": "closed"}).json()
    assert closed["total"] == 8 and all(t["status"] == "closed" for t in closed["items"])
    both = client.get("/api/trades", headers=H, params=[("status", "open"), ("status", "aborted")]).json()
    assert {t["status"] for t in both["items"]} == {"open", "aborted"} and both["total"] == 2
    eth = client.get("/api/trades", headers=H, params=[("coin", "ETHUSDT"), ("status", "closed")]).json()
    assert eth["total"] == 2 and {t["coin"] for t in eth["items"]} == {"ETHUSDT"}
    multi = client.get("/api/trades", headers=H, params=[("coin", "ETHUSDT"), ("coin", "DOGEUSDT")]).json()
    assert multi["total"] == 4 and client.get("/api/trades", headers=H, params={"strategy": "breakout"}).json()["total"] == 1


def test_trades_reject_bad_parameters(client):
    for params in ({"limit": 0}, {"limit": 501}, {"offset": -1}, {"status": "nonsense"}, {"coin": "a b"}, {"side": "up"}):
        assert client.get("/api/trades", headers=H, params=params).status_code == 422, params


def test_trades_flag_which_rows_have_a_snapshot(client):
    items = {t["id"]: t for t in client.get("/api/trades", headers=H, params={"limit": 50}).json()["items"]}
    assert items[client.seeded["full_id"]]["has_snapshot"] is True and items["m1"]["has_snapshot"] is False


# ================================================================================== /api/trades/{id}/snapshot


def test_snapshot_returns_candles_geometry_reason_closes_and_timeline(client):
    tid = client.seeded["full_id"]
    r = client.get(f"/api/trades/{tid}/snapshot", headers=H)
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["trade"]["id"] == tid and d["trade"]["status"] == "closed" and d["trade"]["exit_reason"] == "trail"
    assert len(d["candles"]) == 4 and d["candles"][-1]["close"] == pytest.approx(103.5) and d["candles"][0]["atr"] is None
    assert {g["label"] for g in d["geometry"]} == {"FVG", "SL"} and d["geometry"][0]["type"] == "zone"
    assert d["meta"]["interval"] == "15m" and d["meta"]["sl"] == 105.0
    assert d["reason"]["title"] == "SMC Pro Trend · шорт · BTCUSDT" and "Swing High 104" in d["reason"]["summary"]
    assert [c["reason"] for c in d["closes"]] == ["scale_1r", "scale_3r", "trail"]
    assert sum(c["frac"] for c in d["closes"]) == pytest.approx(1.0) and d["closes"][0]["ts"].endswith("Z")
    assert [e["kind"] for e in d["events"]][0] == "trade_registered" and d["events"][-1]["kind"] == "trade_closed"
    assert all(e["kind"] == "setup_state" for e in d["setup_path"])


def test_snapshot_setup_path_lists_automaton_transitions_before_the_signal(seeded, tmp_path):
    async def add_path():
        db = await Database.connect(TEST_URL, attempts=2, delay_s=0.1)
        async with db.pool.acquire() as conn:
            sig_ms = int((await conn.fetchval("SELECT signal_time FROM trades WHERE id = $1", seeded["full_id"])).timestamp() * 1000)
            for i, (a, b) in enumerate((("short:S0", "short:S1"), ("short:S1", "short:S2"), ("short:S2", "short:S4"))):
                ev = {"ts": sig_ms - (3 - i) * 1000, "coin": "BTCUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": a, "to_state": b,
                      "kind": "setup_state", "reason": None, "detail": {}}
                await conn.execute(schema.INSERT_STATE_EVENT, *to_args(ev, schema.STATE_EVENT_COLUMNS))
            late = {"ts": sig_ms + 3_600_000, "coin": "BTCUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None, "from_state": "short:S4", "to_state": "short:S0",
                    "kind": "setup_state", "reason": None, "detail": {}}
            await conn.execute(schema.INSERT_STATE_EVENT, *to_args(late, schema.STATE_EVENT_COLUMNS))
        await db.close()

    run(add_path())
    with TestClient(create_app(make_settings(tmp_path))) as c:
        path = c.get(f"/api/trades/{seeded['full_id']}/snapshot", headers=H).json()["setup_path"]
    assert [p["to_state"] for p in path][-3:] == ["short:S1", "short:S2", "short:S4"]  # по возрастанию времени, без событий после сигнала


def test_snapshot_not_found_cases(client):
    assert client.get("/api/trades/nope/snapshot", headers=H).json() == {"detail": "trade_not_found"}
    r = client.get("/api/trades/m1/snapshot", headers=H)  # сделка есть, снапшота нет
    assert r.status_code == 404 and r.json() == {"detail": "snapshot_not_found"}
    assert client.get("/api/trades/a%20b/snapshot", headers=H).status_code == 422  # недопустимые символы в id
    assert client.get("/api/trades/' OR 1=1 --/snapshot", headers=H).status_code in (404, 422)


# ================================================================================== WebSocket


def test_websocket_rejects_missing_or_wrong_key(client):
    for headers in ({}, {"X-API-Key": "bad"}):
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/live_state", headers=headers):
                pass


def test_websocket_sends_full_state_then_pushes_changes(client):
    with client.websocket_connect("/ws/live_state", headers=H) as ws:
        first = ws.receive_json()
        assert first["type"] == "state" and [c["coin"] for c in first["coins"]] == ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "1000PEPEUSDT"]
        by = {c["coin"]: {s["side"]: s for s in c["setups"]} for c in first["coins"]}
        assert by["BTCUSDT"]["short"]["state"] == 2 and by["BTCUSDT"]["short"]["label"] == "свип + FVG сформирован"
        assert by["ETHUSDT"]["long"]["state"] == 3 and by["ETHUSDT"]["long"]["reject"] == "exposure"
        assert by["DOGEUSDT"]["long"]["state"] == 0  # событие 30-дневной давности вне окна
        assert by["BTCUSDT"]["long"]["since"] is None
        eth_pos = next(c for c in first["coins"] if c["coin"] == "ETHUSDT")["positions"]
        assert [p["id"] for p in eth_pos] == ["o1"] and eth_pos[0]["status"] == "open"
        assert first["shadow"]["alive"] is True and first["events"]

        # изменение в БД, появившееся ПОСЛЕ подключения клиента, должно прийти без запроса
        async def fresh_event():
            db = await Database.connect(TEST_URL, attempts=2, delay_s=0.1)
            async with db.pool.acquire() as conn:
                ev = {"ts": int(datetime.now(timezone.utc).timestamp() * 1000), "coin": "DOGEUSDT", "strategy": "SMC_Pro_Trend", "strategy_id": "SMC_Pro_Trend", "signal_id": None,
                      "from_state": "long:S0", "to_state": "long:S1", "kind": "setup_state", "reason": None, "detail": {}}
                await conn.execute(schema.INSERT_STATE_EVENT, *to_args(ev, schema.STATE_EVENT_COLUMNS))
            await db.close()

        run(fresh_event())
        got = None
        for _ in range(20):  # опрос 0.2 с, keepalive 1 с: изменение придёт в течение пары секунд
            msg = ws.receive_json()
            doge = next(c for c in msg["coins"] if c["coin"] == "DOGEUSDT")
            if doge["setups"][1]["state"] == 1:
                got = doge
                break
        assert got is not None and got["setups"][1]["label"] == "свип ликвидности" and got["setups"][1]["since"].endswith("Z")


def test_websocket_reports_missing_heartbeat_as_unknown(seeded, tmp_path):
    with TestClient(create_app(make_settings(tmp_path))) as c, c.websocket_connect("/ws/live_state", headers=H) as ws:
        assert ws.receive_json()["shadow"] == {"heartbeat_age_s": None, "alive": None}


def test_health_is_degraded_when_database_goes_away(seeded, tmp_path):
    with TestClient(create_app(make_settings(tmp_path, poll_interval_s=30.0, keepalive_s=30.0))) as c:  # фоновый опрос не мешает
        assert c.get("/api/health").status_code == 200
        hub = c.app.state.hub_state["hub"]
        hub.healthy = False  # состояние, в которое хаб переходит при ошибке опроса (проверяется ниже отдельно)
        assert c.get("/api/health").status_code == 503 and c.get("/api/health").json() == {"status": "degraded"}


def test_hub_publishes_error_message_when_polling_fails(seeded, tmp_path):
    with TestClient(create_app(make_settings(tmp_path))) as c:
        hub = c.app.state.hub_state["hub"]

        async def broken():
            raise OSError("db gone")

        hub.fetch_state = broken
        with c.websocket_connect("/ws/live_state", headers=H) as ws:
            for _ in range(20):
                msg = ws.receive_json()
                if msg["type"] == "error":
                    assert msg["detail"] == "database_unavailable"
                    break
            else:
                pytest.fail("клиент не получил error")
        assert c.get("/api/health").status_code == 503


# ================================================================================== контракт с клиентом (client/tests/fixtures)

FIXTURES = Path(__file__).resolve().parents[2] / "client" / "tests" / "fixtures"
FREEFORM = {"meta", "detail", "plan"}  # поля со свободным набором ключей (зависят от стратегии)


def same_shape(expected, actual, path="$"):
    """Ключи словарей совпадают на всех уровнях (списки - по первому элементу, свободные поля - не сравниваются)."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        assert set(expected) == set(actual), f"{path}: ключи {sorted(set(expected) ^ set(actual))} различаются"
        for k in expected:
            if k not in FREEFORM:
                same_shape(expected[k], actual[k], f"{path}.{k}")
    elif isinstance(expected, list) and isinstance(actual, list) and expected and actual:
        same_shape(expected[0], actual[0], f"{path}[0]")
    else:
        assert isinstance(expected, dict) == isinstance(actual, dict) and isinstance(expected, list) == isinstance(actual, list), f"{path}: разные типы"


def test_client_fixtures_match_the_api_contract(client):
    """Клиент тестируется на снимках ответов API (client/tests/fixtures). Если API изменил форму ответа, а снимки не обновлены - падаем здесь."""
    import json

    load = lambda name: json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    same_shape(load("info.json"), client.get("/api/info", headers=H).json(), "info")
    same_shape(load("stats.json"), client.get("/api/stats", headers=H).json(), "stats")
    trades = client.get("/api/trades", headers=H, params={"limit": 50}).json()
    same_shape(load("trades.json"), trades, "trades")
    snap = client.get(f"/api/trades/{client.seeded['full_id']}/snapshot", headers=H).json()
    for name in ("snapshot_smc_short.json", "snapshot_breakout_long.json", "snapshot_pending.json", "snapshot_open.json"):
        same_shape({k: v for k, v in load(name).items() if k not in ("closes", "setup_path", "events")},
                   {k: v for k, v in snap.items() if k not in ("closes", "setup_path", "events")}, name)
    same_shape(load("snapshot_smc_short.json")["closes"], snap["closes"], "closes")
    with client.websocket_connect("/ws/live_state", headers=H) as ws:
        same_shape(load("live_state.json"), ws.receive_json(), "live_state")
