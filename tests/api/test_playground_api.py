"""API Playground (api/playground.py): очередь прогонов, история по профилям, сделки прогона в формате живых сделок. Хранилище - SQLite во временном каталоге."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import playground as pg
from playground.store import Store

T = 1_761_264_000_000  # 2025-10-24 00:00 UTC


class _Res:
    def __init__(self):
        sid = "SMC_Counter_Trend:BTCUSDT:15m:1:long"
        self.trades = [{"id": sid, "strategy_name": "SMC_Counter_Trend", "strategy_id": "SMC_Counter_Trend", "coin": "BTCUSDT", "side": "long", "kind": "market",
                        "mode": "shadow", "status": "closed", "signal_time": T, "entry_time": T, "exit_time": T + 3_600_000, "entry_price": 100.0, "sl": 99.0,
                        "tp": 104.0, "exit_price": 99.0, "exit_reason": "sl", "r_multiple": -1.1, "pnl": -5.5, "plan": {"levels": [[1.0, 0.5], [4.0, 0.5]]},
                        "client_order_id": None}]
        self.closes = {sid: [{"trade_id": sid, "seq": 1, "ts": T + 3_600_000, "reason": "sl", "frac": 1.0, "price": 99.0, "net_r": -1.1}]}
        self.snapshots = {sid: {"trade_id": sid, "created_at": T, "candles": [{"open_time": T - 900_000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}],
                                "geometry": [], "meta": {"interval": "15m", "entry": 100.0, "sl": 99.0}}}
        self.events = {sid: [{"ts": T, "kind": "trade_registered", "detail": {}}, {"ts": T - 60_000, "kind": "setup_state", "detail": {}}]}
        self.skipped, self.bars, self.seconds = [], 10, 1.0


def app_with(tmp_path):
    app = FastAPI()
    pg.install(app, lambda: None, tmp_path)
    return TestClient(app), Store(tmp_path / "playground.sqlite")


def test_create_validates_and_queues_and_chr_scope(tmp_path):
    c, store = app_with(tmp_path)
    ok = c.post("/api/playground/runs", json={"symbol": "btcusdt", "strategies": ["SMC_Counter_Trend"], "start": "2025-10-24", "end": "2025-11-24"})
    assert ok.status_code == 200
    run = store.run(ok.json()["id"])
    assert run["status"] == "queued" and run["symbol"] == "BTCUSDT" and run["end_ms"] - run["start_ms"] == 32 * 86_400_000  # «по дату» включительно
    assert c.post("/api/playground/runs", json={"symbol": "EURUSD", "strategies": ["SMC_Counter_Trend"], "start": "2025-10-24", "end": "2025-11-24"}).status_code == 422
    assert c.post("/api/playground/runs", json={"symbol": "BTCUSDT", "strategies": ["OBI_Breakout"], "start": "2025-10-24", "end": "2025-11-24"}).status_code == 422
    assert c.post("/api/playground/runs", json={"symbol": "BTCUSDT", "strategies": ["SMC_Counter_Trend"], "start": "2024-01-01", "end": "2025-11-24"}).status_code == 422
    assert c.post("/api/playground/runs", json={"symbol": "BTCUSDT", "strategies": ["SMC_Counter_Trend"], "start": "2025-10-24", "end": "2025-11-24",
                                               "profile": "CHR"}).status_code == 403  # CHR запускает только свои стратегии
    chr_run = c.post("/api/playground/runs", json={"symbol": "ETHUSDT", "strategies": ["Trendline_Breakout_CHR"], "start": "2025-10-24", "end": "2025-11-24",
                                                   "profile": "CHR"})
    assert chr_run.status_code == 200
    assert [r["id"] for r in c.get("/api/playground/runs?scope=chr").json()["items"]] == [chr_run.json()["id"]]
    assert len(c.get("/api/playground/runs").json()["items"]) == 2  # SANTA видит всё


def test_trades_and_snapshot_in_live_format(tmp_path):
    c, store = app_with(tmp_path)
    rid = store.create("BTCUSDT", ["SMC_Counter_Trend"], T, T + 86_400_000, status="running")
    res = _Res()
    store.save_result(rid, res, {"overall": {"closed": 1}, "by_strategy": {}})
    items = c.get(f"/api/playground/runs/{rid}/trades").json()["items"]
    assert items[0]["entry_time"] == "2025-10-24T00:00:00.000Z" and items[0]["mode"] == "playground" and items[0]["has_snapshot"] and "client_order_id" not in items[0]
    tid = items[0]["id"]
    snap = c.get(f"/api/playground/runs/{rid}/trades/{tid}/snapshot").json()
    assert snap["trade"]["id"] == tid and snap["candles"] and snap["meta"]["interval"] == "15m" and snap["reason"]["title"]
    assert [e["kind"] for e in snap["events"]] == ["trade_registered"] and [e["kind"] for e in snap["setup_path"]] == ["setup_state"]
    assert snap["closes"][0]["ts"].endswith("Z")
    assert c.get(f"/api/playground/runs/{rid}/trades/nope/snapshot").status_code == 404
    assert c.delete(f"/api/playground/runs/{rid}").status_code == 200 and c.get(f"/api/playground/runs/{rid}").status_code == 404


def test_not_configured_is_503(tmp_path):
    app = FastAPI()
    pg.install(app, lambda: None, None)
    assert TestClient(app).get("/api/playground/runs").status_code == 503
