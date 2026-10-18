"""Хвост графика без пропусков (постраничная догрузка + разрывы), реальные исполнения биржи на графике, время без зашитых подписей пояса."""

import asyncio
import re
from datetime import datetime, timezone

import pytest

from tradeai_client import fmt
from tradeai_client.binance import MAX_LIMIT, fetch_klines_filled, missing_bars
from tradeai_client.chart_spec import build_chart_spec

IV = 300_000  # 5m
T0 = 1_790_000_000_000 - 1_790_000_000_000 % IV


def row(t):
    return [t, "1", "2", "0.5", "1.5", "10"]


class FakeSession:
    """Имитация aiohttp-сессии: /klines отдаёт свечи сетки в [startTime, endTime] не больше `limit` (и не больше `page_cap`); `holes` - пропуски у биржи."""

    def __init__(self, first, last, *, page_cap=MAX_LIMIT, holes=()):
        self.grid = [t for t in range(first, last + 1, IV) if t not in set(holes)]
        self.cap, self.calls = page_cap, []

    def get(self, url, params, timeout):
        lo, hi = int(params["startTime"]), int(params["endTime"])
        self.calls.append((lo, hi))
        rows = [row(t) for t in self.grid if lo <= t <= hi][: min(int(params["limit"]), self.cap)]

        class Resp:
            status = 200

            async def json(self, content_type=None):
                return rows

        class Ctx:
            async def __aenter__(self):
                return Resp()

            async def __aexit__(self, *a):
                return False

        return Ctx()


def run(coro):
    return asyncio.run(coro)


def test_tail_is_loaded_page_by_page_up_to_now():
    now = T0 + 400 * IV
    s = FakeSession(T0, now, page_cap=100)  # биржа отдаёт по 100 свечей: одной страницы мало
    kl, left = run(fetch_klines_filled(s, "http://x", "BTCUSDT", "5m", T0, now, interval_ms=IV, now_ms=now))
    assert left == 0 and [k["t"] for k in kl] == list(range(T0, now - IV + 1, IV)) and len(s.calls) >= 4  # последняя свеча ещё не закрыта (R5)


def test_hole_inside_the_range_is_refetched_once_and_reported_if_it_stays():
    now = T0 + 50 * IV
    s = FakeSession(T0, now, holes=[T0 + 10 * IV, T0 + 11 * IV, T0 + 30 * IV])
    kl, left = run(fetch_klines_filled(s, "http://x", "BTCUSDT", "5m", T0, now, interval_ms=IV, now_ms=now))
    assert left == 3 and T0 + 10 * IV not in {k["t"] for k in kl}
    assert (T0 + 10 * IV, T0 + 12 * IV - 1) in s.calls and (T0 + 30 * IV, T0 + 31 * IV - 1) in s.calls  # каждый отрезок разрыва запрошен отдельно


def test_missing_bars_counts_only_closed_candles():
    have = [{"t": T0 + i * IV} for i in (0, 1, 3)]
    assert missing_bars(have, T0, T0 + 5 * IV, IV, T0 + 5 * IV + 1) == [T0 + 2 * IV, T0 + 4 * IV]  # свеча T0+5*IV ещё формируется
    assert missing_bars(have, T0, T0 + 5 * IV, IV, T0 + 6 * IV) == [T0 + 2 * IV, T0 + 4 * IV, T0 + 5 * IV]


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def snap_with_fills(fills):
    candles = [{"open_time": T0 + i * IV, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 5.0, "atr": 1.0} for i in range(60)]
    sig = T0 + 59 * IV
    trade = {"id": "x:live", "strategy_name": "SMC_Pro_Trend", "coin": "SOLUSDT", "side": "long", "kind": "market", "mode": "live", "status": "closed",
             "signal_time": iso(sig), "entry_time": iso(sig + IV), "exit_time": iso(sig + 4 * IV), "planned_entry": None, "entry_price": 100.0,
             "exit_price": 101.0, "sl": 98.0, "tp": 106.0, "r_multiple": 0.4, "pnl": 0.8, "exit_reason": "manual_binance", "gap_affected": False, "plan": {}}
    closes = [{"seq": 1, "ts": trade["exit_time"], "reason": "manual_binance", "frac": 1.0, "price": 101.0, "net_r": 0.4}]
    return {"trade": trade, "closes": closes, "candles": candles, "geometry": [], "meta": {"interval": "5m"}, "fills": fills,
            "reason": {"title": "t", "summary": "", "facts": []}}, sig


def test_real_fills_become_markers_and_fact_lines():
    _, sig = snap_with_fills([])
    fills = [{"ts": sig + IV + 1000, "side": "BUY", "qty": 1.0, "price": 100.2, "role": "entry"},
             {"ts": sig + 4 * IV + 5000, "side": "SELL", "qty": 0.4, "price": 101.0, "role": "exit"},
             {"ts": sig + 4 * IV + 9000, "side": "SELL", "qty": 0.6, "price": 102.0, "role": "exit"}]
    snap, sig = snap_with_fills(fills)
    sp = build_chart_spec(snap, None, now_ms=sig + 10 * IV)
    marks = [m for m in sp["trade_markers"] if m["text"] == "И"]
    assert len(marks) == 2  # два выходных исполнения одной свечи - один маркер
    exit_mark = next(m for m in marks if "выход" in m["full"])
    assert exit_mark["price"] == pytest.approx(0.4 * 101.0 + 0.6 * 102.0) and exit_mark["ts"] == sig + 4 * IV + 9000
    lines = {h["title"]: h for h in sp["hlines"]}
    assert lines["ВХОД"]["price"] == pytest.approx(100.2) and lines["ВЫХОД"]["price"] == pytest.approx(101.6)
    assert any("Исполнение на бирже" in e["text"] for e in sp["panel"]["timeline"])


def test_no_fills_changes_nothing():
    snap, sig = snap_with_fills([])
    sp = build_chart_spec(snap, None, now_ms=sig + 10 * IV)
    assert not [m for m in sp["trade_markers"] if m["text"] == "И"] and "ВЫХОД" not in {h["title"] for h in sp["hlines"]}


def test_server_time_tokens_are_localized_without_a_zone_label():
    ms = 1_790_000_000_000
    out = fmt.localize_utc_text("Свип в {ts:%d}. Азия ({hm:0}-{hm:420})" % ms)
    assert fmt.fmt_ms_local(ms, "%d.%m.%Y %H:%M") in out and "UTC" not in out and "{" not in out
    assert re.search(r"Азия \(\d\d:\d\d-\d\d:\d\d\)", out)
    legacy = fmt.localize_utc_text("в 2026-09-21 10:00 UTC")
    assert "UTC" not in legacy and "2026" in legacy


def test_panel_times_have_no_zone_label(fx):
    snap = fx("snapshot_smc_short.json")
    sig = fmt.to_ms(snap["trade"]["signal_time"])
    sp = build_chart_spec(snap, None, now_ms=sig + 300 * 900_000)
    assert all("UTC" not in e["time"] for e in sp["panel"]["timeline"]) and "tz_label" not in sp
    assert next(e for e in sp["panel"]["timeline"] if e["text"] == "Сигнал стратегии")["time"] == fmt.fmt_ms_local(sig)
