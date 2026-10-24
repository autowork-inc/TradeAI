"""Live-Dashboard: «взгляд стратегии» (playground/watch.py), сервис watch (кэш свечей и взглядов, meta) и прокси API. Без сети: свечи синтетические."""

import asyncio
import json
from pathlib import Path

import httpx
import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import config
from api import watch_proxy
from api.settings import Settings
from orchestrator.registry import SHADOW_STRATEGIES
from playground import watch
from playground.watch_server import WatchService
from strategies.chr_horizontal import detect_chr_horizontal
from strategies.chr_trendline import detect_chr_trendline
from strategies.trendline_breakout import detect_trendline_breakout
from tests.playground.test_engine import minutes

NOW = None


def klines(df: pl.DataFrame) -> list[list]:
    return [[r["open_time"], str(r["open"]), str(r["high"]), str(r["low"]), str(r["close"]), str(r["volume"]), r["open_time"] + 1, "0", 0, "0", "0", "0"]
            for r in df.iter_rows(named=True)]


class FakeApi:
    """Публичный REST Binance на синтетических минутках: klines любого таймфрейма собираются из них (последние limit, включая формирующуюся)."""

    def __init__(self, m: pl.DataFrame, now_ms: int):
        from playground.history import resample
        self.m, self.now, self.calls, self._resample = m, now_ms, [], resample

    async def get(self, path, params=None, weight=1):
        self.calls.append((params["symbol"], params["interval"], params["limit"]))
        ms = config.INTERVAL_MS[params["interval"]]
        bars = self._resample(self.m.filter(pl.col("open_time") < self.now - self.now % ms), ms)
        part = self.m.filter(pl.col("open_time") >= self.now - self.now % ms).filter(pl.col("open_time") < self.now)
        if part.height:
            bars = pl.concat([bars, pl.DataFrame({"open_time": [self.now - self.now % ms], "open": [part["open"][0]], "high": [part["high"].max()],
                                                  "low": [part["low"].min()], "close": [part["close"][-1]], "volume": [part["volume"].sum()]})])
        return klines(bars.tail(params["limit"]))


@pytest.fixture(scope="module")
def data():
    return minutes(days_after_start=0)


def service(data, extra_ms=7 * 60_000, dirs=()):
    now = int(data["open_time"][-1]) - 3 * 86_400_000 + extra_ms
    api = FakeApi(data, now)
    svc = WatchService(api, clock=lambda: api.now / 1000, dirs=list(dirs))
    return svc, api


def test_every_strategy_view_is_well_formed(data):
    svc, _ = service(data)
    for name in SHADOW_STRATEGIES:
        v = asyncio.run(svc.view(name, "TESTUSDT"))
        assert v["strategy"] == name and set(v["sides"]) == {"long", "short"} and v["candles"] and v["summary"]
        for side in v["sides"].values():
            assert side["stage"] in (0, 1, 2, 3) and side["text"] and all({"label", "ok", "value"} <= set(c) for c in side["checks"])
        for g in v["geometry"]:
            assert g["type"] in ("line", "zone", "marker", "series")
        last_closed = v["candles"][-1]["open_time"] + config.INTERVAL_MS[v["interval"]]
        assert last_closed <= svc.now_ms()  # стратегия видит только закрытые свечи
        assert v["forming"] is None or v["forming"]["open_time"] == last_closed  # формирующаяся - отдельно
        json.dumps(v)


def test_views_and_candles_are_cached_until_next_bar(data):
    svc, api = service(data)
    asyncio.run(svc.view("ICT_Killzones", "TESTUSDT"))
    n = len(api.calls)
    asyncio.run(svc.view("ICT_Killzones", "TESTUSDT"))
    assert len(api.calls) == n  # та же закрытая свеча: ни закрытых свечей, ни формирующейся (10 с) заново
    api.now += 15 * 60_000  # закрылась новая 15m
    svc._clock = lambda: api.now / 1000
    v = asyncio.run(svc.view("ICT_Killzones", "TESTUSDT"))
    assert len(api.calls) > n and v["candles"][-1]["open_time"] + 900_000 <= api.now


def test_meta_reads_bot_universe_and_obi_only_live_tier(data, tmp_path):
    (tmp_path / ".symbol_universe.json").write_text(json.dumps({"updated_ms": 1, "live": ["BTCUSDT", "ETHUSDT", "SOLUSDT"], "test": ["ZECUSDT", "EURUSD"]}))
    svc, _ = service(data, dirs=[tmp_path])
    m = svc.meta()
    assert m["live_strategies"] == ["SMC_Pro_Trend"]
    assert m["strategies"]["OBI_Breakout"]["coins"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"] and "ZECUSDT" in m["strategies"]["ICT_Killzones"]["coins"]
    assert "EURUSD" not in m["strategies"]["ICT_Killzones"]["coins"]  # Forex/металлы - без минуток Binance
    assert m["strategies"]["SMC_Pro_Trend"]["interval"] == "5m"


def test_keep_internal_does_not_change_the_trading_output(data):
    from features.smc import SmcParams
    from orchestrator.feature_engine import FeatureEngine
    from playground.history import resample

    f = FeatureEngine(SmcParams()).build("TESTUSDT", "15m", 900_000, resample(data, 900_000).tail(300), 0).frame.with_columns(pl.lit(-1).alias("htf_bias"))
    for fn in (lambda **k: detect_chr_horizontal(f, **k), lambda **k: detect_chr_trendline(f, 900_000, **k), lambda **k: detect_trendline_breakout(f, 900_000, **k)):
        plain, full = fn(), fn(keep_internal=True)
        assert full.select(plain.columns).equals(plain) and len(full.columns) > len(plain.columns)


def test_api_proxy_forwards_with_secret_and_maps_failures():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["token"], seen["url"] = request.headers.get("X-Watch-Token"), str(request.url)
        return httpx.Response(200, json={"ok": 1})

    def app(transport):
        a = FastAPI()
        watch_proxy.install(a, Settings(database_url="postgresql://u:p@db:5432/x", api_key="k" * 32, shadow_data_dir=Path("."), watch_url="http://watch:8000"), lambda: None, transport)
        return TestClient(a)

    c = app(httpx.MockTransport(handler))
    assert c.get("/api/watch/view", params={"strategy": "ICT_Killzones", "symbol": "BTCUSDT"}).json() == {"ok": 1}
    assert seen["token"] == "k" * 32 and seen["url"] == "http://watch:8000/view?strategy=ICT_Killzones&symbol=BTCUSDT"
    assert c.get("/api/watch/view", params={"strategy": "ICT_Killzones", "symbol": "EURUSD"}).status_code == 422
    assert c.get("/api/watch/view", params={"strategy": "ICT_Killzones", "symbol": "QUSDT"}).status_code == 200  # однобуквенный тикер (Q) - валиден
    assert c.get("/api/watch/coins", params={"strategy": "ICT_Killzones"}).status_code == 200 and seen["url"].endswith("/coins?strategy=ICT_Killzones")
    down = app(httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("no"))))
    assert down.get("/api/watch/meta").status_code == 503


def test_coin_filter_keeps_coins_with_two_ok_checks_and_raises_threshold_over_15(data):
    svc, _ = service(data)

    def fake_view(oks_long, oks_short, stage=1):
        side = lambda n: {"stage": stage, "text": "", "checks": [{"label": str(i), "ok": i < n, "value": ""} for i in range(5)]}  # noqa: E731
        return {"sides": {"long": side(oks_long), "short": side(oks_short)}}

    coins = [f"C{i}USDT" for i in range(20)]
    svc.meta = lambda: {"strategies": {"ICT_Killzones": {"coins": coins}}, "live_strategies": []}
    for i, c in enumerate(coins[:5]):
        svc._views[("ICT_Killzones", c)] = (0, fake_view(i % 3, 1))  # оценки 0,1,2,0,1 (лонг) / 1 (шорт)
    r = svc.coins("ICT_Killzones")
    assert [x["coin"] for x in r["coins"]] == ["C2USDT"] and r["threshold"] == 2 and r["hidden"] == 4 and len(r["pending"]) == 15
    svc._views[("ICT_Killzones", "C0USDT")] = (0, fake_view(0, 0, stage=3))  # сигнал виден всегда
    assert "C0USDT" in [x["coin"] for x in svc.coins("ICT_Killzones")["coins"]]
    for i, c in enumerate(coins):
        svc._views[("ICT_Killzones", c)] = (0, fake_view(3 if i < 4 else 2, 0))
    r = svc.coins("ICT_Killzones")
    assert r["threshold"] == 3 and [x["coin"] for x in r["coins"]] == coins[:4] and all(x["score"] == 3 and x["side"] == "long" for x in r["coins"])
