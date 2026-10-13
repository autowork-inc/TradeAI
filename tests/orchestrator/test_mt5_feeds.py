"""Данные MT5-символов для Shadow: свечи M15 + производные H4 из MetaApi, заполнение выходных плоскими свечами, тики из котировок.

Источник - поддельный (`FakeSource`): отдаёт историю и «открывает» новые закрытые свечи по команде теста; настоящих запросов нет. Сквозной прогон:
фиды -> HtfBias -> микроструктура -> Оркестратор с ПЯТЬЮ настоящими стратегиями через выходные: ни одного исключения (в т.ч. `HtfBias.attach` не видит «отставшего» H4),
а на плоских свечах и после дыры торговых сигналов принято не было."""

import asyncio
import random
from datetime import datetime

import pytest

from orchestrator.core import Orchestrator, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.htf import HtfBias
from orchestrator.journal import MemoryEventLog
from orchestrator.microstructure import MicroStructure
from orchestrator.mt5_feeds import H4_MS, STALE_AFTER_MS, Mt5KlineFeed, Mt5TickFeed, MultiFeed, derive_h4, _row
from orchestrator.registry import SHADOW_STRATEGIES, build_strategies, plans_for
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Mode
from features.smc import SmcParams
from utils.errors import DataError

M15 = 900_000
SYM = "XAUUSD"
MON = 1785715200000  # 2026-08-03 00:00 UTC (понедельник)


def make_bars(days: int, seed: int = 7) -> list[tuple]:
    """M15-бары пн-пт (с 00:00 до 22:00 UTC пятницы) и с воскресенья 22:00 UTC; случайное блуждание около 2000."""
    rnd = random.Random(seed)
    bars, price = [], 2000.0
    for k in range(days * 96):
        t = MON + k * M15
        day = (t - MON) // 86_400_000 % 7  # 0 = понедельник ... 4 = пятница, 5 = суббота, 6 = воскресенье
        minutes = (t - MON) // 60_000 % 1440
        if day in (5,) or (day == 4 and minutes >= 22 * 60) or (day == 6 and minutes < 22 * 60):
            continue  # рынок закрыт
        o = price
        c = round(o + rnd.gauss(0, 1.2), 2)
        h = round(max(o, c) + abs(rnd.gauss(0, 0.6)), 2)
        lo = round(min(o, c) - abs(rnd.gauss(0, 0.6)), 2)
        bars.append((t, o, h, lo, c, float(rnd.randint(200, 2000))))
        price = c
    return bars


class FakeSource:
    def __init__(self, bars: list[tuple], visible: int):
        self.bars, self.visible = bars, visible
        self.calls = 0
        self.quote = (2000.0, 2000.4)

    async def get_candles(self, symbol, timeframe, limit, *, start_time: datetime | None = None):
        self.calls += 1
        assert timeframe == "15m" and symbol == "XAUUSD.r"
        pool = self.bars[: self.visible]
        if start_time is not None:
            pool = [b for b in pool if b[0] <= int(start_time.timestamp() * 1000)]
        return pool[-limit:]

    async def get_price(self, symbol):
        class Q:
            bid, ask = self.quote
        return Q


class Clock:
    def __init__(self, t: int):
        self.t = t

    def __call__(self) -> int:
        return self.t


def make_feed(source, clock, **kw):
    feed = Mt5KlineFeed(source, FeatureEngine(SmcParams()), broker_name=lambda s: s + ".r", symbols=(SYM,), clock_ms=clock, **kw)
    return feed, feed.htf_view(FeatureEngine(SmcParams()))


def weekend_index(bars: list[tuple], after: int) -> int:
    """Индекс первой свечи после дыры, не раньше `after`."""
    return next(i for i in range(max(after, 1), len(bars)) if bars[i][0] - bars[i - 1][0] > M15)


# ----------------------------------------------------------------------------------------------------- чистые части


def test_derive_h4_uses_only_complete_utc_buckets():
    rows = {MON + k * M15: _row(MON + k * M15, 10 + k, 20 + k, 5 + k, 12 + k, 1.0, M15) for k in range(16 + 8)}  # 1.5 корзины: вторая неполная
    h4 = derive_h4(rows, M15)
    assert list(h4) == [MON] and h4[MON][1] == 10 and h4[MON][4] == 12 + 15 and h4[MON][2] == 20 + 15 and h4[MON][3] == 5 and h4[MON][5] == 16.0


# ----------------------------------------------------------------------------------------------------- сквозной прогон


def test_full_pipeline_through_a_weekend_never_trades_on_flat_or_post_gap_bars():
    bars = make_bars(60)
    wk = weekend_index(bars, 2150)  # первая реальная свеча после выходных (индекс в списке баров)
    start = wk - 60  # старт за ~15 часов до закрытия рынка
    source = FakeSource(bars, visible=start + 1)  # +1: последняя свеча - ещё формирующаяся
    clock = Clock(bars[start][0] + 3 * M15)
    feed, htf_view = make_feed(source, clock, poll_s=0.01)
    htf = HtfBias("4h", H4_MS, MemoryEventLog())
    micro = MicroStructure(("BTCUSDT",), M15)
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0, plans=plans_for(SHADOW_STRATEGIES))
    strategies = build_strategies(SHADOW_STRATEGIES)
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log)
    stale_bars: set[int] = set()
    emitted: list[int] = []

    async def cycle() -> int:
        for h in await htf_view.poll():
            htf.update(h)
        n = 0
        for ctx in await feed.poll():
            if ctx.received_ms - ctx.bar_close_ms >= STALE_AFTER_MS:
                stale_bars.add(ctx.bar_open_time)
            emitted.append(ctx.bar_open_time)
            orch.on_context(micro.attach(htf.attach(ctx)))
            n += 1
        return n

    async def body():
        feed.start()
        for _ in range(200):  # фон загрузил историю
            await asyncio.sleep(0.01)
            if feed._g.ready:
                break
        assert feed._g.ready
        assert await cycle() == 0  # на старте история не эмитится; H4 отдал одну последнюю закрытую свечу (htf.update выше)
        assert htf.has(SYM) and htf.current(SYM) is not None
        # новые закрытые свечи по одной до закрытия рынка: все эмитятся по порядку
        while source.visible < wk:
            source.visible += 1
            clock.t = bars[source.visible - 1][0] + M15 + 5_000 if source.visible - 1 >= 0 else clock.t
            await asyncio.sleep(0.02)  # фон успевает обновить хранилище
            feed._g.merge(SYM, await source.get_candles("XAUUSD.r", "15m", 8))
            await cycle()
        assert emitted and all(b - a == M15 for a, b in zip(emitted, emitted[1:]))
        before_weekend = len(emitted)
        # выходные: новых свечей нет, часы идут - контекстов нет
        clock.t = bars[wk][0] - 3600_000
        assert await cycle() == 0 and len(emitted) == before_weekend
        # открытие рынка: приходит реальная свеча после дыры -> плоский backlog + эта свеча, подряд
        source.visible = wk + 2
        clock.t = bars[wk][0] + M15 + 5_000
        feed._g.merge(SYM, await source.get_candles("XAUUSD.r", "15m", 8))
        n = await cycle()
        gap_bars = (bars[wk][0] - bars[wk - 1][0]) // M15 - 1
        assert n == gap_bars + 1 and all(b - a == M15 for a, b in zip(emitted, emitted[1:]))
        await feed.aclose()

    asyncio.run(body())
    # плоские свечи и разогрев после дыры помечены «опоздавшими»: Оркестратор пропускает сигналы по ним (skipped_stale), торговать нельзя
    wk_open, gap_bars = bars[wk][0], (bars[wk][0] - bars[wk - 1][0]) // M15 - 1
    assert len(stale_bars) >= gap_bars + 1 and min(stale_bars) > bars[wk - 1][0] and all(b < wk_open + 20 * M15 for b in stale_bars)
    assert not [e for e in log.events if e.kind == "signal_accepted" and e.ts in {b + M15 + STALE_AFTER_MS for b in stale_bars}]


def test_stale_marking_covers_flat_bars_and_warmup_after_the_gap():
    bars = make_bars(60)
    wk = weekend_index(bars, 2150)
    source = FakeSource(bars, visible=wk + 1)
    clock = Clock(bars[wk][0] + 3 * M15)
    feed, _ = make_feed(source, clock, poll_s=0.01)

    async def body():
        feed.start()
        for _ in range(300):
            await asyncio.sleep(0.01)
            if feed._g.ready:
                break
        stale = feed._g.stale_until[SYM]
        flat = [t for t in feed._g.store[SYM] if bars[wk - 1][0] < t < bars[wk][0]]
        assert flat and all(stale[t] for t in flat)  # плоские - все
        assert all(stale.get(bars[wk][0] + k * M15) for k in range(20))  # 20 свечей разогрева после дыры
        assert not stale.get(bars[wk][0] + 20 * M15)
        assert all(feed._g.store[SYM][t][1] == feed._g.store[SYM][t][4] == bars[wk - 1][4] and feed._g.store[SYM][t][5] == 0.0 for t in flat)  # плоская: цена = прошлый close, объём 0
        await feed.aclose()

    asyncio.run(body())


def test_forming_candle_is_never_used_and_off_grid_candle_is_an_error():
    bars = make_bars(60)
    source = FakeSource(bars, visible=2200)
    feed, _ = make_feed(source, Clock(bars[2198][0] + M15 + 5_000))
    g = feed._g
    asyncio.run(g.bootstrap())
    assert max(g.store[SYM]) == bars[2198][0]  # bars[2199] формируется
    with pytest.raises(DataError, match="не на сетке"):
        g.merge(SYM, [(bars[2199][0] + 1, 1.0, 1.0, 1.0, 1.0, 1.0)])


def test_transient_metaapi_failures_are_retried_then_fail_fast():
    from exchange.mt5_metaapi import MetaApiError

    class Flaky(FakeSource):
        def __init__(self, *a, fail=0):
            super().__init__(*a)
            self.fail = fail

        async def get_candles(self, *a, **kw):
            if self.fail > 0:
                self.fail -= 1
                raise MetaApiError("таймаут")
            return await super().get_candles(*a, **kw)

    bars = make_bars(60)
    ok = Flaky(bars, 2200, fail=2)
    feed, _ = make_feed(ok, Clock(bars[2198][0] + M15 + 5_000), poll_s=0.001, max_failures=5)
    asyncio.run(feed._g._guarded(feed._g.bootstrap))
    assert ok.fail == 0  # два сбоя повторены, затем история загружена
    assert feed._g.store[SYM]
    bad = Flaky(bars, 2200, fail=99)
    feed2, _ = make_feed(bad, Clock(bars[2198][0] + M15 + 5_000), poll_s=0.001, max_failures=3)
    with pytest.raises(MetaApiError):
        asyncio.run(feed2._g._guarded(feed2._g.bootstrap))


def test_tick_feed_emits_mid_price_as_trade_and_mark_and_marks_gaps():
    from exchange.mt5_metaapi import MetaApiError

    ticks, gaps = [], []
    src = FakeSource([], 0)
    feed = Mt5TickFeed(src, ticks.append, gaps.append, broker_name=lambda s: s + ".r", symbols=(SYM,), clock_ms=lambda: 5)
    asyncio.run(feed.poll_once())
    assert [(t.symbol, t.price, t.kind, t.ts_ms) for t in ticks] == [(SYM, 2000.2, "trade", 5), (SYM, 2000.2, "mark", 5)]

    class Down(FakeSource):
        async def get_price(self, symbol):
            raise MetaApiError("нет связи")

    async def run_down():
        f = Mt5TickFeed(Down([], 0), ticks.append, gaps.append, broker_name=lambda s: s, symbols=(SYM,), clock_ms=lambda: 9, poll_s=0.001, max_failures=3)
        f.start()
        await asyncio.sleep(0.2)
        with pytest.raises(MetaApiError):
            f.check()

    asyncio.run(run_down())
    assert gaps == [9, 9]  # два сбоя - две отметки gap, третий роняет процесс


def test_multi_feed_concatenates_polls_and_forwards_lifecycle():
    class F:
        def __init__(self, items):
            self.items, self.started, self.closed, self.checked = items, 0, 0, 0

        def start(self):
            self.started += 1

        async def aclose(self):
            self.closed += 1

        def check(self):
            self.checked += 1

        async def poll(self):
            return self.items

    a, b = F([1, 2]), F([3])
    m = MultiFeed([a, b])
    m.start(), m.check()
    assert asyncio.run(m.poll()) == [1, 2, 3]
    asyncio.run(m.aclose())
    assert (a.started, b.started, a.closed, b.closed, a.checked, b.checked) == (1, 1, 1, 1, 1, 1)


def test_shadow_data_path_never_calls_trade_methods():
    """Shadow берёт у MetaApi только данные: торговых вызовов нет ни в фидах, ни в точке входа (даже если токен случайно с торговыми правами)."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for rel in ("orchestrator/mt5_feeds.py", "scripts/shadow_run.py"):
        src = (root / rel).read_text(encoding="utf-8")
        assert not re.search(r"market_order|limit_order|close_position|modify_position|cancel_order|stop_loss\(|take_profit\(", src), rel
