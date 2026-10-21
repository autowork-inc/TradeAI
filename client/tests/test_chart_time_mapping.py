"""Рисунки на графике привязаны к абсолютной оси времени (Unix-секунды UTC) и цене (2026-10-21, v1.3): при смене таймфрейма точка остаётся на том же моменте.
Функции пересчёта время <-> логический индекс берутся из chart.js (блок <time-mapping>) и исполняются в node; нет node - тест пропускается."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tradeai_client import drawings_store

JS = Path(__file__).resolve().parents[1] / "tradeai_client" / "chart_web" / "chart.js"
NODE = shutil.which("node")


def _run(script: str) -> dict:
    src = JS.read_text(encoding="utf-8")
    block = re.search(r"// <time-mapping>(.*?)// </time-mapping>", src, re.S).group(1)
    head = ("const IV={'5m':300,'15m':900,'1h':3600,'4h':14400};"
            "function make(tf,t0,n){const iv=IV[tf];const bars=[];for(let i=0;i<n;i++)bars.push({time:t0+i*iv});return {bars,barsIv:iv};}"
            "function api(S){const CFG={interval:'15m'};const ivSec=()=>S.barsIv;" + block +
            "return {X:(t)=>timeToLogical(t),T:(L)=>logicalToTime(L)};}")
    out = subprocess.run([NODE, "-e", head + script], capture_output=True, text=True, timeout=30, check=True).stdout
    return json.loads(out)


@pytest.mark.skipif(NODE is None, reason="node не установлен")
def test_point_keeps_its_unix_time_across_timeframe_switches():
    res = _run("""
      const t0 = 1700006400, a = {};
      const tfs = ['5m','15m','1h','4h'], maps = {};
      for (const tf of tfs) maps[tf] = api(make(tf, t0, Math.ceil(30 * 86400 / IV[tf])));
      const t = t0 + 10 * 86400 + 7 * 3600 + 421;  // произвольный момент
      const out = {};
      for (const tf of tfs) { const L = maps[tf].X(t); out[tf] = maps[tf].T(L); }
      console.log(JSON.stringify(out));""")
    assert all(abs(v - (1700006400 + 10 * 86400 + 7 * 3600 + 421)) < 1e-6 for v in res.values()), res


@pytest.mark.skipif(NODE is None, reason="node не установлен")
def test_point_sits_on_the_same_candle_centre_on_every_timeframe():
    """Точка, поставленная в центре свечи 4h, на 15m лежит в центре соответствующего времени (середина интервала), а не на индексе 4h-свечи."""
    res = _run("""
      const t0 = 1700006400, m4 = api(make('4h', t0, 100)), m15 = api(make('15m', t0, 1600));
      const t = m4.T(50);  // середина 51-й 4h-свечи (логический индекс 50 -> время центра)
      console.log(JSON.stringify({t, centre4h: t0 + 50 * 14400 + 7200, l15: m15.X(t)}));""")
    assert res["t"] == res["centre4h"] and abs(res["l15"] - (50 * 16 + 8 - 0.5)) < 1e-6, res


def test_store_normalizes_to_integer_unix_seconds_and_drops_candle_indices():
    good = {"id": "a", "type": "trend", "color": "#ffb300", "width": 2, "opacity": 0.9}
    items = [
        {**good, "id": "sec", "p1": {"t": 1700000000.4, "p": 1.0}, "p2": {"t": 1700000900, "p": 2.0}},
        {**good, "id": "ms", "p1": {"t": 1700000000000, "p": 1.0}, "p2": {"t": 1700000900000, "p": 2.0}},  # миллисекунды -> секунды
        {**good, "id": "idx", "p1": {"t": 812, "p": 1.0}, "p2": {"t": 830, "p": 2.0}},  # номер свечи - не время: отбрасывается
        {**good, "id": "str", "p1": {"t": "2026-10-20", "p": 1.0}, "p2": {"t": 1700000900, "p": 2.0}},  # строковая дата - не принимается
    ]
    out = drawings_store.clean(items)
    assert [d["id"] for d in out] == ["sec", "ms"]
    assert out[0]["p1"]["t"] == 1700000000 and isinstance(out[0]["p1"]["t"], int)
    assert out[1]["p1"] == {"t": 1700000000, "p": 1.0} and out[1]["p2"]["t"] == 1700000900
