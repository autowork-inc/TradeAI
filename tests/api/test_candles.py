"""Свечи Forex/металлов для графика: кэш бота -> /api/candles (сборка таймфреймов из M5, формирующаяся свеча, плоские свечи) и запись кэша ботом."""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api import candles
from api.main import create_app
from api.settings import Settings
from orchestrator.chart_cache import DIR_NAME, ChartCache

M5 = 300_000
KEY = "k" * 40
H = {"X-API-Key": KEY}
T0 = 1_790_000_000_000 - 1_790_000_000_000 % 14_400_000  # начало 4-часовой корзины


def m5_rows(start, n, price=100.0, volume=10.0):
    return [[start + i * M5, price + i, price + i + 2, price + i - 1, price + i + 1, volume] for i in range(n)]


def test_resample_builds_utc_grid_candles_and_skips_incomplete_edges():
    rows = m5_rows(T0 + M5, 10)  # со второго слота: первая 15-минутная корзина неполная
    out = candles.resample(rows, 900_000)
    assert [r[0] for r in out] == [T0 + 900_000, T0 + 1_800_000, T0 + 2_700_000][:len(out)] and out[0][0] == T0 + 900_000
    first = out[0]
    assert first[1] == rows[2][1] and first[4] == rows[4][4] and first[2] == max(r[2] for r in rows[2:5]) and first[3] == min(r[3] for r in rows[2:5]) and first[5] == 30.0
    assert candles.resample(rows, M5) == [r for r in rows]  # 5m - как есть


def test_flat_weekend_candles_are_not_served_and_live_candle_completes_the_last_bucket():
    flat = [[T0 + i * M5, 1.0, 1.0, 1.0, 1.0, 0.0] for i in range(6)]
    assert candles.resample(flat, 900_000) == []
    closed = m5_rows(T0, 2)  # две закрытые M5 корзины 15m [T0, T0+15m)
    live = [T0 + 2 * M5, 150.0, 160.0, 90.0, 155.0]  # третья (формирующаяся) M5 той же корзины
    (bar,) = candles.resample(closed, 900_000, live=live)
    assert bar[0] == T0 and bar[1] == closed[0][1] and bar[4] == 155.0 and bar[2] == 160.0 and bar[3] == 90.0
    assert candles.resample(closed, 900_000) == []  # без формирующейся свечи корзина неполная и не выдаётся
    (only_live,) = candles.resample([], M5, live=live)
    assert only_live[0] == T0 + 2 * M5 and only_live[4] == 155.0


def test_chart_cache_is_written_by_the_bot_and_read_by_the_api(tmp_path):
    cache = ChartCache(tmp_path / DIR_NAME)
    store = {T0 + i * M5: (T0 + i * M5, 100.0 + i, 102.0 + i, 99.0 + i, 101.0 + i, 5.0, 0, 0, 0, 0.0, 0.0) for i in range(24)}
    cache.store_closed("EURUSD", store, 1)
    cache.store_closed("EURUSD", store, 2)  # новой свечи нет: файл не переписывается
    now = T0 + 24 * M5 + 30_000
    for k in range(5):
        cache.on_tick("EURUSD", now + k * 1000, 125.0 + k)
    files = sorted(p.name for p in (tmp_path / DIR_NAME).iterdir())
    assert files == ["EURUSD.json", "EURUSD.live.json"]
    out = candles.build([tmp_path], "EURUSD", "15m", 100, None, now_ms=now + 5000)
    assert out["live"] is True and out["items"][-1][0] == T0 + 24 * M5 - (24 * M5) % 900_000
    last = out["items"][-1]
    assert last[4] == 129.0 and last[2] >= 129.0  # закрытие - последняя котировка
    stale = candles.build([tmp_path], "EURUSD", "5m", 1000, None, now_ms=now + 10 * 60_000)
    assert stale["live"] is False and stale["items"][-1][0] == T0 + 23 * M5  # котировка старше двух минут: формирующуюся свечу не рисуем
    with pytest.raises(candles.NoCandles):
        candles.build([tmp_path], "GBPUSD", "5m", 10, None)
    older = candles.build([tmp_path], "EURUSD", "5m", 5, T0 + 10 * M5, now_ms=now)
    assert [r[0] for r in older["items"]] == [T0 + i * M5 for i in range(6, 11)]


def test_chart_cache_write_errors_do_not_raise(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    cache = ChartCache(blocker / DIR_NAME)  # каталог создать нельзя: торговый процесс не должен упасть
    cache.store_closed("EURUSD", {T0: (T0, 1, 1, 1, 1, 1, 0, 0, 0, 0.0, 0.0)}, 1)
    cache.on_tick("EURUSD", T0, 1.0)


def test_candles_endpoint_needs_key_and_serves_cache(tmp_path):
    s = Settings("postgresql://tradeai:pw@postgres:5432/tradeai", KEY, shadow_data_dir=tmp_path)
    c = TestClient(create_app(s))
    assert c.get("/api/candles?coin=EURUSD").status_code == 401
    assert c.get("/api/candles?coin=EURUSD", headers=H).json() == {"detail": "no_candles"}
    (tmp_path / DIR_NAME).mkdir()
    (tmp_path / DIR_NAME / "EURUSD.json").write_text(json.dumps({"symbol": "EURUSD", "interval_ms": M5, "candles": m5_rows(T0, 48)}), encoding="utf-8")
    r = c.get("/api/candles?coin=EURUSD&interval=1h&limit=3", headers=H).json()
    assert r["coin"] == "EURUSD" and len(r["items"]) == 3 and r["items"][0][0] % 3_600_000 == 0
    assert c.get("/api/candles?coin=EURUSD&interval=2h", headers=H).status_code == 422
    assert c.get("/api/candles?coin=../x", headers=H).status_code == 422
