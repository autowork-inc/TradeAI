"""MFE Simulator (api/mfe_sim.py): все сделки с входом, доигрывание по 1m-свечам (пессимистично), тейки, сквизы, отступ стопа, HTTP-задание.
Binance подменён httpx.MockTransport, БД - подставным пулом; сети и PostgreSQL нет."""

import asyncio
from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import mfe_sim as m

T0 = int(datetime(2026, 10, 20, 10, tzinfo=timezone.utc).timestamp() * 1000)
NOW = T0 + 5 * 86_400_000
MIN = m.MIN_MS


def row(**kw):
    base = {"id": "s1", "strategy": "ICT_Killzones", "coin": "BTCUSDT", "side": "long", "kind": "market", "mode": "shadow", "status": "aborted",
            "entry_price": 100.0, "entry_time": datetime.fromtimestamp((T0 + 1000) / 1000, tz=timezone.utc), "exit_time": None, "sl": 99.0,
            "risk_amount": 5.0, "r_multiple": None, "gap_affected": False, "exit_reason": "process_restart", "meta_atr": 0.5, "candle_atr": None}
    return {**base, **kw}


def path(*hl, close=None):
    """Свечи с минуты после входа: (high, low) по порядку."""
    return [(T0 + (i + 1) * MIN, h, lo, close if close is not None else (h + lo) / 2) for i, (h, lo) in enumerate(hl)]


def trade(**kw):
    (t,), _ = m.select_trades([row(**kw)])
    return t


FEE = 0.0005 * 100  # вход MARKET, R = 1


def test_every_status_with_entry_and_live_copies_are_taken_bad_stops_are_counted():
    rows = [row(id="a", status="aborted"), row(id="b", status="closed"), row(id="c", status="open"),
            row(id="a:live", mode="live"), row(id="x:live", mode="live"), row(id="bad", sl=101.0), row(id="chr", strategy="Trendline_Breakout_CHR")]
    trades, counts = m.select_trades(rows)
    assert [t.id for t in trades] == ["a", "b", "c", "a:live", "x:live"]  # 1.6.0: live-копии тоже считаются
    assert counts == {"rows": 6, "live_copies": 1, "bad_stop": 1, "other_profile": 1}
    chr_trades, chr_counts = m.select_trades(rows, "chr")  # профиль CHR - только свои стратегии
    assert [t.id for t in chr_trades] == ["chr"] and chr_counts["other_profile"] == 6


def test_take_profit_before_stop_and_pessimism_inside_one_candle():
    t = trade()
    r = m.simulate_path(t, path((101.6, 99.5), (100.5, 98.5)), t.sl, NOW)
    assert r.by_tp[1.5] == pytest.approx(1.5 - FEE - 0.0005 * 101.5) and r.by_tp[2.0] == pytest.approx(-1 - FEE - 0.0005 * 99)
    assert r.mfe_r == pytest.approx(1.6) and r.stop_hit_ms == T0 + 2 * MIN
    # Scale Out бота: 50% на +1R взято, остаток на 2R не дошёл - стоп
    assert r.outcome(2.0, scale=True) == pytest.approx(0.5 - 0.5 - FEE - 0.5 * 0.0005 * 101 - 0.5 * 0.0005 * 99)
    both = m.simulate_path(t, path((102.5, 98.9)), t.sl, NOW)  # одна свеча задела и тейк 2R, и стоп -> стоп
    assert both.by_tp[2.0] == pytest.approx(-1 - FEE - 0.0005 * 99) and both.mfe_r == 0.0


def test_open_trade_is_counted_at_the_current_price_and_horizon_closes_at_last_close():
    t = trade()
    live = m.simulate_path(t, path((100.5, 99.5), close=100.3), t.sl, now_ms=T0 + 10 * MIN)
    assert live.open_now and live.by_tp[1.0] == pytest.approx(0.3 - FEE - 0.0005 * 100.3)  # открытая: по текущей цене
    done = m.simulate_path(t, path((100.5, 99.5), close=100.4), t.sl, now_ms=NOW)
    assert done.by_tp[4.0] == pytest.approx(0.4 - FEE - 0.0005 * 100.4)


def test_squeeze_after_the_stop_and_padding_suggestion():
    """Стоп 99 выбит тенью 98.9, потом цена ушла на +3R: сквиз; с отступом +0.5 ATR (0.25) стоп 98.75 переживает тень и берёт тейк."""
    rows = [row(id=f"t{i}", entry_time=datetime.fromtimestamp((T0 + 1000) / 1000, tz=timezone.utc)) for i in range(3)]
    bars = path((100.2, 98.9), (101.0, 99.8), (103.5, 100.9))
    trades, counts = m.select_trades(rows)
    paths, hunts = {}, {}
    for t in trades:
        paths[t.id] = {k: m.simulate_path(t, bars, t.sl - t.dir * k * t.atr, NOW) for k in m.PAD_GRID}
        hunts[t.id] = m.squeeze_after_stop(t, bars, paths[t.id][0.0].stop_hit_ms)
    rep = m.build_report(trades, paths, hunts, counts, datetime(2026, 10, 20, tzinfo=timezone.utc), NOW)
    assert rep["squeeze"]["stopped"] == 3 and rep["squeeze"]["reach1"] == 3 and rep["squeeze"]["rate"] == 1.0
    assert rep["padding"]["suggest"] is not None and rep["padding"]["suggest"]["pad_atr"] > 0
    assert rep["verdict"]["n"] == 3 and "Сделок меньше" in rep["verdict"]["text"]
    assert rep["counts"]["statuses"] == {"aborted": 3}


def klines_handler(bars):
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if req.url.params["symbol"] != "BTCUSDT":
            return httpx.Response(400, json={"code": -1121})
        start = int(req.url.params["startTime"])
        return httpx.Response(200, json=[[b[0], "0", str(b[1]), str(b[2]), str(b[3]), "0"] for b in bars if b[0] >= start][:1000])
    return handler, calls


class FakePool:
    def __init__(self, rows):
        self.rows = rows

    def acquire(self):
        pool = self

        class Ctx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def fetch(self, sql, since, until):
                return pool.rows
        return Ctx()


def test_http_job_runs_in_background_and_reports(monkeypatch):
    monkeypatch.setattr(m, "REQUEST_PAUSE_S", 0.0)
    bars = path((101.2, 99.6), (102.3, 100.8), (104.2, 101.5))
    handler, calls = klines_handler(bars)
    rows = [row(id="a"), row(id="b", coin="EURUSD"), row(id="a:live", mode="live")]
    app = FastAPI()
    job = m.install(app, lambda: None, lambda: FakePool(rows), httpx.MockTransport(handler), now_ms=lambda: NOW)
    with TestClient(app) as c:
        assert c.get("/api/mfe/status").json()["status"] == "idle"
        assert c.post("/api/mfe/run?since=2026-10-20").json()["status"] == "running"
        for _ in range(200):
            st = c.get("/api/mfe/status").json()
            if st["status"] != "running":
                break
            asyncio.run(asyncio.sleep(0.01))
        assert st["status"] == "done", st
        rep = st["report"]
        assert rep["counts"]["with_entry"] == 3 and rep["counts"]["simulated"] == 2 and rep["counts"]["live_copies"] == 1
        assert rep["counts"]["no_candles_symbols"] == ["EURUSD"] and rep["scale_matrix"] and rep["verdict"]["best_plain"]
        assert rep["verdict"]["best_tp"] == 4.0 and [r["tp"] for r in rep["tp_matrix"]] == list(m.TP_GRID)
        assert c.get("/api/mfe/status?scope=chr").json()["status"] == "idle"  # у профиля CHR своё задание
        assert c.post("/api/mfe/run?since=2026-10-22&until=2026-10-21").status_code == 422
        assert {"symbol": "BTCUSDT", "interval": "1m"}.items() <= dict(calls[0].url.params).items()
        assert c.post("/api/mfe/run?since=bad").status_code == 422


def test_binance_ban_stops_the_job_with_a_clear_message(monkeypatch):
    monkeypatch.setattr(m, "REQUEST_PAUSE_S", 0.0)
    app = FastAPI()
    m.install(app, lambda: None, lambda: FakePool([row()]), httpx.MockTransport(lambda req: httpx.Response(429)), now_ms=lambda: NOW)
    with TestClient(app) as c:
        c.post("/api/mfe/run")
        for _ in range(200):
            st = c.get("/api/mfe/status").json()
            if st["status"] != "running":
                break
            asyncio.run(asyncio.sleep(0.01))
        assert st["status"] == "failed" and "ограничил" in st["error"]


def test_strategy_state_reads_auto_tp_overrides_and_templates_by_profile(tmp_path):
    import json

    data = tmp_path / "live"
    data.mkdir()
    (data / ".auto_tp.json").write_text(json.dumps({"updated_ms": 5, "since": "2026-10-22", "strategies": {
        "ICT_Killzones": {"tp_r": 6.5, "n": 7}, "Trendline_Breakout_CHR": {"tp_r": 2.0, "n": 5}}}), encoding="utf-8")
    lab = tmp_path / "lab"
    (lab / "proposals").mkdir(parents=True)
    (lab / "strategy_overrides.json").write_text(json.dumps({"version": 1, "strategies": {"ICT_Killzones": {"params": {"x": 1}}}}), encoding="utf-8")
    (lab / "proposals" / "p.json").write_text(json.dumps({"id": "p", "strategy": "ICT_Killzones", "title": "t", "status": "template"}), encoding="utf-8")
    santa = m.read_strategy_state([data], lab, "santa")
    assert santa["auto_tp"]["strategies"] == {"ICT_Killzones": {"tp_r": 6.5, "n": 7}} and santa["proposals"][0]["status"] == "template"
    assert "ICT_Killzones" in santa["overrides"]
    chr_ = m.read_strategy_state([data], lab, "chr")
    assert list(chr_["auto_tp"]["strategies"]) == ["Trendline_Breakout_CHR"] and chr_["proposals"] == [] and chr_["overrides"] == {}


def test_news_endpoint_reads_the_freshest_bot_calendar(tmp_path):
    import json

    from api.main import read_news

    old, new = tmp_path / "shadow", tmp_path / "live"
    old.mkdir()
    new.mkdir()
    (old / ".news_calendar.json").write_text(json.dumps({"loaded_ms": 1, "events": [[1792413000000, "USD", "старое"]]}), encoding="utf-8")
    (new / ".news_calendar.json").write_text(json.dumps({"loaded_ms": 5, "events": [[1792499400000, "USD", "Federal Funds Rate"],
                                                                                    [1792413000000, "CAD", "CPI m/m"]]}), encoding="utf-8")
    data = read_news([old, new])
    assert data["loaded_ms"] == 5 and [e["title"] for e in data["events"]] == ["CPI m/m", "Federal Funds Rate"] and data["events"][0]["ts"].endswith("Z")
    assert read_news([tmp_path / "none"]) == {"loaded_ms": None, "events": []}
