"""Блок 4 (2026-10-18): календарь ForexFactory (High Impact, +-15 минут, обновление раз в час, таймаут 2 с, сбой сети не блокирует бота)
и запрет НОВЫХ сигналов на выходных (сб 00:00 - вс 23:59 UTC) при обычном сопровождении открытых сделок."""

import asyncio
import json
from pathlib import Path
from datetime import datetime, timezone

import aiohttp
import pytest

import config
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.news_calendar import (NewsCalendar, NewsEvent, NewsFetchError, WeekendGate, fetch_calendar, weekend_gate, is_weekend, parse_calendar,
                                        symbol_currencies)
from orchestrator.tracker import CLOSED, OPEN, VirtualTracker
from orchestrator.types import Direction, Kind, Mode
from tests.orchestrator.helpers import IV, Scripted, ctx, mk, tick

MIN = 60_000


def ms(y, mo, d, h=0, mi=0) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


NFP = ms(2026, 10, 16, 12, 30)  # пятница, 08:30 по Нью-Йорку (UTC-4)
RAW = [
    {"title": "Non-Farm Employment Change", "country": "USD", "date": "2026-10-16T08:30:00-04:00", "impact": "High"},
    {"title": "German ZEW", "country": "EUR", "date": "2026-10-13T09:00:00+00:00", "impact": "High"},
    {"title": "Retail Sales m/m", "country": "USD", "date": "2026-10-15T08:30:00-04:00", "impact": "Medium"},
    {"title": "Bank Holiday", "country": "JPY", "date": "2026-10-12T00:00:00-04:00", "impact": "Holiday"},
]


def calendar(events=None, now=NFP, **kw) -> NewsCalendar:
    async def fetch():
        return events if events is not None else parse_calendar(RAW)

    cal = NewsCalendar(fetch, wall_ms=lambda: now, **kw)
    asyncio.run(cal.refresh())
    return cal


def test_parse_keeps_only_high_impact_and_converts_time_zones_to_utc():
    events = parse_calendar(RAW)
    assert [(e.currency, e.title) for e in events] == [("EUR", "German ZEW"), ("USD", "Non-Farm Employment Change")]
    assert events[1].ts_ms == NFP


@pytest.mark.parametrize("raw", [{"a": 1}, [1], [{"impact": "High", "country": "USD", "date": "завтра"}],
                                 [{"impact": "High", "country": "USD", "date": "2026-10-16T08:30:00"}], [{"impact": "High", "date": "2026-10-16T08:30:00Z"}]])
def test_broken_calendar_is_rejected_whole(raw):
    with pytest.raises(NewsFetchError):
        parse_calendar(raw)


def test_symbol_currencies():
    assert symbol_currencies("BTCUSDT") == {"USD"} and symbol_currencies("XAUUSD") == {"USD"} and symbol_currencies("EURUSD") == {"EUR", "USD"}


@pytest.mark.parametrize("offset_min, blocked", [(-6, False), (-5, True), (0, True), (5, True), (6, False)])
def test_news_window_is_five_minutes_before_and_after(offset_min, blocked):  # 1.6.0: было 15/15
    cal = calendar()
    reason = cal.check(mk(symbol="BTCUSDT"), NFP + offset_min * MIN)
    assert (reason is not None) is blocked and (reason is None or "Non-Farm" in reason)


def test_news_of_another_currency_does_not_block():
    cal = calendar()
    zew = ms(2026, 10, 13, 9)
    assert cal.check(mk(symbol="BTCUSDT"), zew) is None and cal.check(mk(symbol="EURUSD"), zew) is not None


def test_never_loaded_or_stale_calendar_does_not_block_trading():
    async def fetch():
        raise NewsFetchError("нет сети")

    cal = NewsCalendar(fetch)
    assert cal.check(mk(symbol="BTCUSDT"), NFP) is None  # не загружен - fail-open
    stale = calendar(now=NFP - config.NEWS_MAX_AGE_MS - 1)
    assert stale.check(mk(symbol="BTCUSDT"), NFP) is None


def test_background_loop_survives_network_errors_and_refreshes_hourly():
    sleeps, calls = [], []

    async def fetch():
        calls.append(1)
        if len(calls) < 3:
            raise NewsFetchError("таймаут 2 с")
        return parse_calendar(RAW)

    async def sleep(s):
        sleeps.append(s)
        if len(sleeps) >= 3:
            raise asyncio.CancelledError

    cal = NewsCalendar(fetch, sleep=sleep, wall_ms=lambda: NFP)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cal.run())
    assert sleeps == [3600.0] * 3 and len(calls) == 3 and cal.loaded_ms == NFP and cal.failures == 0


def test_code_errors_in_the_background_task_are_fail_fast():
    async def fetch():
        raise KeyError("баг")

    async def scenario():
        cal = NewsCalendar(fetch)
        cal.start()
        await asyncio.sleep(0.05)
        with pytest.raises(KeyError):
            cal.check_task()

    asyncio.run(scenario())


def test_calendar_is_cached_on_disk_and_reloaded_after_restart(tmp_path):
    path = tmp_path / "news.json"
    calendar(cache_path=path)
    assert json.loads(path.read_text(encoding="utf-8"))["loaded_ms"] == NFP

    async def fetch():
        raise NewsFetchError("нет сети")

    reborn = NewsCalendar(fetch, cache_path=path)
    assert reborn.loaded_ms == NFP and reborn.check(mk(symbol="BTCUSDT"), NFP) is not None
    path.write_text("{битый", encoding="utf-8")
    assert NewsCalendar(fetch, cache_path=path).loaded_ms is None  # битый кеш - промах, ждём загрузку


def test_fetch_has_a_hard_two_second_timeout_and_raises_immediately():
    class Resp:
        status = 200

        async def __aenter__(self):
            await asyncio.sleep(10)

        async def __aexit__(self, *a):
            return False

    class Session:
        def __init__(self):
            self.kw = None

        def get(self, url, **kw):
            self.kw = kw
            return Resp()

    async def scenario():
        s = Session()
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(NewsFetchError):
            await fetch_calendar(s, "http://x", 0.05)  # сервер «завис» на 10 с: выходим по таймауту, а не ждём
        return loop.time() - t0, s.kw["timeout"].total

    took, total = asyncio.run(scenario())
    assert took < 1.0 and total == 0.05
    assert config.NEWS_TIMEOUT_S == 2.0 and config.NEWS_REFRESH_S == 3600.0


def test_http_and_connection_errors_become_news_fetch_error():
    class Resp:
        def __init__(self, status=200, exc=None):
            self.status, self.exc = status, exc

        async def __aenter__(self):
            if self.exc:
                raise self.exc
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self, content_type=None):
            return RAW

    class Session:
        def __init__(self, resp):
            self.resp, self.timeout = resp, None

        def get(self, url, timeout):
            self.timeout = timeout
            return self.resp

    s = Session(Resp())
    assert len(asyncio.run(fetch_calendar(s, "http://x", 2.0))) == 2 and s.timeout.total == 2.0
    for resp in (Resp(status=503), Resp(exc=aiohttp.ClientConnectionError("reset")), Resp(exc=asyncio.TimeoutError())):
        with pytest.raises(NewsFetchError):
            asyncio.run(fetch_calendar(Session(resp), "http://x", 2.0))


# -------------------------------------------------------------------------------------------- выходные


@pytest.mark.parametrize("ts, weekend", [
    (ms(2026, 10, 16, 23, 59), False),  # пятница 23:59
    (ms(2026, 10, 17, 0, 0), True),  # суббота 00:00
    (ms(2026, 10, 18, 23, 59), True),  # воскресенье 23:59
    (ms(2026, 10, 19, 0, 0), False),  # понедельник 00:00
])
def test_weekend_window_is_saturday_to_sunday_utc(ts, weekend):
    assert is_weekend(ts) is weekend
    assert (WeekendGate().check(mk(), ts) == "weekend") is weekend


def test_weekend_blocks_new_signals_but_open_trades_are_managed_as_usual():
    log = MemoryEventLog()
    tr = VirtualTracker(log, equity=10_000.0)
    friday = ms(2026, 10, 16, 20)
    saturday_bar = (ms(2026, 10, 17, 10) // IV)
    first = mk(kind=Kind.MARKET, direction=Direction.SHORT, entry=100.0, bar=friday // IV)
    second = mk(kind=Kind.MARKET, direction=Direction.SHORT, entry=100.0, bar=saturday_bar, symbol="ETHUSDT")
    orch = Orchestrator([Scripted("smc", {friday // IV: [first], saturday_bar: [second]})], {"smc": Route(Mode.SHADOW, tr)}, log, signal_gates=[WeekendGate()])
    (ev,) = orch.on_context(ctx(idx=friday // IV))
    assert ev.kind == "signal_accepted"
    tr.on_tick(tick(100.0, friday + IV + 1))
    assert tr.trades[0].status == OPEN
    (ev2,) = orch.on_context(ctx(symbol="ETHUSDT", idx=saturday_bar))
    assert ev2.kind == "skipped_filter" and ev2.detail["reason"] == "weekend"
    tr.on_tick(tick(105.5, ms(2026, 10, 17, 12)))  # суббота: стоп открытой пятничной сделки срабатывает как обычно
    assert tr.trades[0].status == CLOSED and tr.trades[0].exit_reason == "sl" and len(tr.trades) == 1


def test_news_gate_in_the_orchestrator():
    cal = calendar()
    log = MemoryEventLog()
    tr = VirtualTracker(log, equity=10_000.0)
    bar = (NFP - IV) // IV  # свеча закрывается в момент NFP
    sig = mk(kind=Kind.MARKET, bar=bar)
    orch = Orchestrator([Scripted("smc", {bar: [sig]})], {"smc": Route(Mode.SHADOW, tr)}, log, signal_gates=[cal])
    (ev,) = orch.on_context(ctx(idx=bar, received=NFP + 1000))
    assert ev.kind == "skipped_filter" and ev.detail["filter"] == "news" and "Non-Farm" in ev.detail["reason"] and tr.trades == []
    assert isinstance(cal.events[0], NewsEvent)


def test_weekend_filter_applies_only_in_live_mode():
    assert isinstance(weekend_gate("live", True), WeekendGate)
    assert weekend_gate("shadow", True) is None  # Shadow ищет сигналы и симулирует сделки и в сб-вс
    assert weekend_gate("live", False) is None  # выключатель WEEKEND_FILTER_ENABLED


def test_shadow_run_wires_weekend_gate_through_trade_mode():
    src = (Path(__file__).resolve().parents[2] / "scripts" / "shadow_run.py").read_text(encoding="utf-8")
    assert "weekend_gate(config.TRADE_MODE, config.WEEKEND_FILTER_ENABLED)" in src and "WeekendGate()" not in src
