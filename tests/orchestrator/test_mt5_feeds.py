"""Данные MT5-символов для Shadow: свечи M5 (единственный запрос к MetaApi) + производные 15m / 1h / 4h / 1d, заполнение выходных плоскими свечами, тики из котировок.

Источник - поддельный (`FakeSource`): отдаёт историю и «открывает» новые закрытые свечи по команде теста; настоящих запросов нет, а любой таймфрейм кроме "5m"
- ошибка теста. Сквозной прогон: фиды -> MtfState -> микроструктура -> Оркестратор с ПЯТЬЮ настоящими стратегиями (SMC_Pro_Trend на 5m с 1h/1d, остальные на 15m) через
закрытие рынка: ни одного исключения (в т.ч. `MtfState.attach` не видит «отставшего» старшего таймфрейма), а на плоских свечах и после дыры сигналов принято не было."""

import asyncio
import random
from datetime import datetime

import pytest

from features.smc import SmcParams
from orchestrator.core import Orchestrator, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.journal import MemoryEventLog
from orchestrator.kline_feed import MultiFeed
from orchestrator.microstructure import MicroStructure
from orchestrator.mt5_feeds import (D1_MS, H1_MS, H4_MS, M5_MS, M15_MS, STALE_AFTER_MS, Mt5KlineFeed, Mt5TickFeed, _row, derive_h4, derive_tf)
from orchestrator.mtf import MtfState
from orchestrator.registry import SHADOW_STRATEGIES, build_strategies, plans_for
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Mode
from utils.errors import ConfigError, DataError

SYM = "XAUUSD"
MON = 1785715200000  # 2026-08-03 00:00 UTC (понедельник)
D1_ENGINE_MIN = 60
AFTER = 19_000  # индекс бара (~66 суток от старта): хватает на историю 1d (62 суток) и на дыру закрытия рынка после неё


def make_bars(days: int, seed: int = 7) -> list[tuple]:
    """M5-бары пн-пт (с 00:00 до 22:00 UTC пятницы) и с субботы 04:00 UTC (рынок закрыт с пт 22:00 до сб 04:00); случайное блуждание около 2000."""
    rnd = random.Random(seed)
    bars, price = [], 2000.0
    for k in range(days * 288):
        t = MON + k * M5_MS
        day = (t - MON) // 86_400_000 % 7  # 0 = понедельник ... 4 = пятница, 5 = суббота, 6 = воскресенье
        minutes = (t - MON) // 60_000 % 1440
        if (day == 4 and minutes >= 22 * 60) or (day == 5 and minutes < 4 * 60):
            continue  # рынок закрыт
        o = price
        c = round(o + rnd.gauss(0, 0.7), 2)
        h = round(max(o, c) + abs(rnd.gauss(0, 0.35)), 2)
        lo = round(min(o, c) - abs(rnd.gauss(0, 0.35)), 2)
        bars.append((t, o, h, lo, c, float(rnd.randint(200, 2000))))
        price = c
    return bars


class FakeSource:
    def __init__(self, bars: list[tuple], visible: int):
        self.bars, self.visible = bars, visible
        self.calls = 0
        self.timeframes: set[str] = set()
        self.quote = (2000.0, 2000.4)

    async def get_candles(self, symbol, timeframe, limit, *, start_time: datetime | None = None):
        self.calls += 1
        self.timeframes.add(timeframe)
        assert timeframe == "5m" and symbol == "XAUUSD.r"  # единственный запрос к брокеру - M5
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


SMALL = dict(d1_history_bars=62, htf_history_bars=110, h1_history_bars=300)  # 1d - не 90 суток истории: тесты быстрее


def make_feed(source, clock, **kw):
    feed = Mt5KlineFeed(source, FeatureEngine(SmcParams()), broker_name=lambda s: s + ".r", symbols=(SYM,), clock_ms=clock, **{**SMALL, **kw})
    return feed


def views(feed):
    """(фид состояния 1h/4h/1d, рабочий фид 15m + первичный 5m) в том порядке, в котором их опрашивает ShadowRun."""
    state = MultiFeed([feed.h1_view(FeatureEngine(SmcParams())), feed.htf_view(FeatureEngine(SmcParams())),
                       feed.d1_view(FeatureEngine(SmcParams(), min_bars=D1_ENGINE_MIN))])
    return state, MultiFeed([feed.m15_view(FeatureEngine(SmcParams())), feed])


def weekend_index(bars: list[tuple], after: int) -> int:
    """Индекс первой свечи после дыры, не раньше `after`."""
    return next(i for i in range(max(after, 1), len(bars)) if bars[i][0] - bars[i - 1][0] > M5_MS)


# ----------------------------------------------------------------------------------------------------- чистые части


def grid(start: int, n: int, step: int = M5_MS) -> dict[int, tuple]:
    return {start + k * step: _row(start + k * step, 10 + k, 20 + k, 5 + k, 12 + k, 1.0, step) for k in range(n)}


def test_derive_tf_uses_only_complete_utc_buckets_for_every_target():
    rows = grid(MON, 48 * 2 // 2 + 24)  # 1.5 корзины 4h на M5 (48 + 24 слота): вторая неполная
    h4 = derive_tf(rows, M5_MS, H4_MS)
    assert list(h4) == [MON] and h4[MON][1] == 10 and h4[MON][4] == 12 + 47 and h4[MON][2] == 20 + 47 and h4[MON][3] == 5 and h4[MON][5] == 48.0
    h1 = derive_tf(rows, M5_MS, H1_MS)
    assert list(h1) == [MON + k * H1_MS for k in range(6)]  # 72 слота = 6 полных часов
    assert h1[MON + H1_MS][1] == 10 + 12 and h1[MON + H1_MS][4] == 12 + 23 and h1[MON + H1_MS][5] == 12.0
    assert derive_tf(grid(MON, 288), M5_MS, D1_MS).keys() == {MON} and not derive_tf(grid(MON, 287), M5_MS, D1_MS)  # 287 слотов - сутки неполные
    assert derive_tf(grid(MON, 3, M15_MS), M15_MS, H1_MS) == {}  # меньше корзины


def test_derive_tf_from_a_partial_start_skips_the_leading_incomplete_bucket_and_since_scans_only_the_tail():
    rows = grid(MON + 5 * M5_MS, 12 * 3)  # начало внутри часа
    full = derive_tf(rows, M5_MS, H1_MS)
    assert list(full) == [MON + H1_MS, MON + 2 * H1_MS] or MON + H1_MS in full  # первый (неполный) час не взят
    assert MON not in full
    tail = derive_tf(rows, M5_MS, H1_MS, since=MON + 2 * H1_MS)
    assert set(tail) <= set(full) and all(tail[t] == full[t] for t in tail) and MON + H1_MS not in tail


def test_derive_tf_validates_the_interval_ratio_and_derive_h4_keeps_its_old_contract():
    for bad in (M5_MS, 7 * M5_MS // 2, M5_MS // 2):
        with pytest.raises(ConfigError):
            derive_tf(grid(MON, 5), M5_MS, bad)
    rows = {MON + k * M15_MS: _row(MON + k * M15_MS, 10 + k, 20 + k, 5 + k, 12 + k, 1.0, M15_MS) for k in range(16 + 8)}  # 1.5 корзины: вторая неполная
    h4 = derive_h4(rows, M15_MS)
    assert list(h4) == [MON] and h4[MON][1] == 10 and h4[MON][4] == 12 + 15 and h4[MON][2] == 20 + 15 and h4[MON][3] == 5 and h4[MON][5] == 16.0


# ----------------------------------------------------------------------------------------------------- сквозной прогон


def test_full_pipeline_through_a_market_closure_never_trades_on_flat_or_post_gap_bars():
    bars = make_bars(110)
    wk = weekend_index(bars, AFTER)  # первая реальная свеча после закрытия рынка (индекс в списке баров)
    start = wk - 150  # старт за ~12.5 часов до закрытия рынка
    source = FakeSource(bars, visible=start + 1)  # +1: последняя свеча - ещё формирующаяся
    clock = Clock(bars[start][0] + 3 * M5_MS)
    feed = make_feed(source, clock, poll_s=0.01)
    state, main = views(feed)
    mtf = MtfState({"5m": M5_MS, "15m": M15_MS, "1h": H1_MS, "4h": H4_MS, "1d": D1_MS}, MemoryEventLog())
    micro = MicroStructure(("BTCUSDT",), M15_MS)
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0, plans=plans_for(SHADOW_STRATEGIES))
    strategies = build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log)
    stale: dict[str, set[int]] = {"5m": set(), "15m": set()}
    emitted: dict[str, list[int]] = {"5m": [], "15m": []}
    states: list[str] = []

    async def cycle() -> int:
        for h in await state.poll():  # сначала ВСЕ закрытия старших таймфреймов, потом рабочие свечи (как ShadowRun.tick)
            states.append(h.interval)
            mtf.update(h)
        n = 0
        for ctx in await main.poll():
            assert ctx.interval in emitted  # стратегиям идут только рабочие таймфреймы
            if ctx.received_ms - ctx.bar_close_ms >= STALE_AFTER_MS:
                stale[ctx.interval].add(ctx.bar_open_time)
            emitted[ctx.interval].append(ctx.bar_open_time)
            ctx = mtf.attach(ctx)
            if ctx.interval_ms == micro.interval_ms:
                ctx = micro.attach(ctx)
            orch.on_context(ctx)
            n += 1
        return n

    async def body():
        feed.start()
        for _ in range(600):  # фон загрузил историю
            await asyncio.sleep(0.01)
            if feed._g.ready:
                break
        assert feed._g.ready
        assert await cycle() == 0  # на старте история не эмитится; старшие виды отдали по одной последней закрытой свече (mtf.update выше)
        assert sorted(states) == ["1d", "1h", "4h"] and mtf.has(SYM) and mtf.current(SYM) is not None
        # новые закрытые свечи по одной до закрытия рынка: все эмитятся по порядку
        while source.visible < wk:
            source.visible += 1
            clock.t = bars[source.visible - 1][0] + M5_MS + 5_000
            await asyncio.sleep(0.02)  # фон успевает обновить хранилище
            feed._g.merge(SYM, await source.get_candles("XAUUSD.r", "5m", 8))
            await cycle()
        assert emitted["5m"] and all(b - a == M5_MS for a, b in zip(emitted["5m"], emitted["5m"][1:]))
        assert emitted["15m"] and all(b - a == M15_MS for a, b in zip(emitted["15m"], emitted["15m"][1:]))
        assert "1h" in states[3:] and states[3:].count("1h") >= 5  # 1h-контексты приходят по мере закрытия часов
        before = {k: len(v) for k, v in emitted.items()}
        # рынок закрыт: новых свечей нет, часы идут - контекстов нет
        clock.t = bars[wk][0] - 3600_000
        assert await cycle() == 0 and {k: len(v) for k, v in emitted.items()} == before
        # открытие рынка: приходит реальная свеча после дыры -> плоский backlog + эта свеча, подряд, по обоим рабочим таймфреймам
        source.visible = wk + 2
        clock.t = bars[wk][0] + 3 * M5_MS + 5_000
        feed._g.merge(SYM, await source.get_candles("XAUUSD.r", "5m", 8))
        await cycle()
        await feed.aclose()

    asyncio.run(body())
    assert source.timeframes == {"5m"}  # ни одного запроса другого таймфрейма
    assert all(b - a == M5_MS for a, b in zip(emitted["5m"], emitted["5m"][1:])) and all(b - a == M15_MS for a, b in zip(emitted["15m"], emitted["15m"][1:]))
    gap_bars = (bars[wk][0] - bars[wk - 1][0]) // M5_MS - 1
    assert len([t for t in emitted["5m"] if bars[wk - 1][0] < t <= bars[wk][0]]) == gap_bars + 1  # плоский backlog + первая реальная свеча
    # плоские свечи и разогрев после дыры помечены «опоздавшими»: Оркестратор пропускает сигналы по ним (skipped_stale), торговать нельзя
    wk_open = bars[wk][0]
    assert len(stale["5m"]) >= gap_bars + 1 and min(stale["5m"]) > bars[wk - 1][0] and all(b < wk_open + 60 * M5_MS for b in stale["5m"])
    assert stale["15m"] and all(b > bars[wk - 1][0] - M15_MS and b < wk_open + 21 * M15_MS for b in stale["15m"])
    late_ts = {b + M5_MS + STALE_AFTER_MS for b in stale["5m"]} | {b + M15_MS + STALE_AFTER_MS for b in stale["15m"]}
    assert not [e for e in log.events if e.kind == "signal_accepted" and e.ts in late_ts]


def test_state_views_share_the_snapshot_with_primaries_until_every_primary_took_it():
    bars = make_bars(110)
    wk = weekend_index(bars, AFTER)
    source = FakeSource(bars, visible=wk - 100)
    clock = Clock(bars[wk - 100][0] + 3 * M5_MS)
    feed = make_feed(source, clock, poll_s=3600)  # фоновое обновление не мешает: опрос раз в час
    state, main = views(feed)
    g = feed._g

    async def body():
        await g._guarded(g.bootstrap)
        g.ready = True
        await state.poll()
        snap = g.frozen
        assert snap is not None  # старшие виды сняли снимок
        g.merge(SYM, [bars[wk - 100]])  # «новая» свеча приходит между опросами
        await main.poll()  # 15m и 5m обрабатывают ТОТ ЖЕ снимок (не видят новую свечу) ...
        assert g.frozen is None  # ... и после последнего первичного потребителя снимок сброшен
        await state.poll()
        assert g.frozen is not snap and max(g.frozen[0][SYM]) > max(snap[0][SYM])  # следующий цикл - новый снимок с новой свечой

    asyncio.run(body())
    with pytest.raises(DataError, match="не зарегистрирован"):
        g.take_snapshot("nope")


def test_every_derived_view_covers_its_history_and_history_bootstrap_stays_within_page_budget():
    bars = make_bars(110)
    wk = weekend_index(bars, AFTER)
    source = FakeSource(bars, visible=wk - 100)
    feed = make_feed(source, Clock(bars[wk - 100][0] + 3 * M5_MS), poll_s=3600)
    state, _ = views(feed)

    async def body():
        await feed._g._guarded(feed._g.bootstrap)
        feed._g.ready = True
        return await state.poll()

    ctxs = {c.interval: c for c in asyncio.run(body())}
    assert set(ctxs) == {"1h", "4h", "1d"}
    assert ctxs["1h"].frame.height == 300 and ctxs["4h"].frame.height == 110 and 55 <= ctxs["1d"].frame.height <= 62  # 1d: все полные сутки истории в пределах окна
    need = max(300, 62 * 288) + 2 * 288
    assert source.calls <= need // 1000 + 3  # страницы по 1000 свечей, тот же цикл, что был у M15; других запросов нет


def test_derived_views_validate_their_configuration():
    source = FakeSource([], 0)
    feed = make_feed(source, Clock(MON))
    with pytest.raises(ConfigError, match="min_bars"):
        feed.d1_view(FeatureEngine(SmcParams()))  # 62 суток < min_bars=100 у стандартного движка
    with pytest.raises(ConfigError, match="только M5"):
        Mt5KlineFeed(source, FeatureEngine(SmcParams()), broker_name=lambda s: s, symbols=(SYM,), interval="15m")


# ----------------------------------------------------------------------------------------------------- хранилище и сбои


def test_stale_marking_covers_flat_bars_and_warmup_after_the_gap():
    bars = make_bars(110)
    wk = weekend_index(bars, AFTER)
    source = FakeSource(bars, visible=wk + 1)
    clock = Clock(bars[wk][0] + 3 * M5_MS)
    feed = make_feed(source, clock, poll_s=0.01)

    async def body():
        feed.start()
        for _ in range(900):
            await asyncio.sleep(0.01)
            if feed._g.ready:
                break
        stale = feed._g.stale_until[SYM]
        flat = [t for t in feed._g.store[SYM] if bars[wk - 1][0] < t < bars[wk][0]]
        assert flat and all(stale[t] for t in flat)  # плоские - все
        assert all(stale.get(bars[wk][0] + k * M5_MS) for k in range(60))  # 60 M5-свечей (= 20 M15) разогрева после дыры
        assert not stale.get(bars[wk][0] + 60 * M5_MS)
        assert all(feed._g.store[SYM][t][1] == feed._g.store[SYM][t][4] == bars[wk - 1][4] and feed._g.store[SYM][t][5] == 0.0 for t in flat)  # плоская: цена = прошлый close, объём 0
        await feed.aclose()

    asyncio.run(body())


def test_forming_candle_is_never_used_and_off_grid_candle_is_an_error():
    bars = make_bars(110)
    source = FakeSource(bars, visible=AFTER)
    feed = make_feed(source, Clock(bars[AFTER - 2][0] + M5_MS + 5_000))
    g = feed._g
    asyncio.run(g.bootstrap())
    assert max(g.store[SYM]) == bars[AFTER - 2][0]  # bars[14999] формируется
    with pytest.raises(DataError, match="не на сетке"):
        g.merge(SYM, [(bars[AFTER - 1][0] + 1, 1.0, 1.0, 1.0, 1.0, 1.0)])


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

    bars = make_bars(110)
    ok = Flaky(bars, AFTER, fail=2)
    feed = make_feed(ok, Clock(bars[AFTER - 2][0] + M5_MS + 5_000), poll_s=0.001, max_failures=5)
    asyncio.run(feed._g._guarded(feed._g.bootstrap))
    assert ok.fail == 0  # два сбоя повторены, затем история загружена
    assert feed._g.store[SYM]
    bad = Flaky(bars, AFTER, fail=99)
    feed2 = make_feed(bad, Clock(bars[AFTER - 2][0] + M5_MS + 5_000), poll_s=0.001, max_failures=3)
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


def test_multi_feed_is_importable_from_the_old_place():
    from orchestrator import mt5_feeds

    assert mt5_feeds.MultiFeed is MultiFeed


def test_shadow_data_path_never_calls_trade_methods():
    """Shadow берёт у MetaApi только данные: торговых вызовов нет ни в фидах, ни в точке входа (даже если токен случайно с торговыми правами)."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for rel in ("orchestrator/mt5_feeds.py", "scripts/shadow_run.py"):
        src = (root / rel).read_text(encoding="utf-8")
        assert not re.search(r"market_order|limit_order|close_position|modify_position|cancel_order|stop_loss\(|take_profit\(", src), rel


def test_only_the_five_minute_timeframe_is_ever_requested_from_the_broker():
    """«Никаких новых REST-запросов»: в коде фидов единственный таймфрейм у MetaApi - BASE_TIMEFRAME ("5m"); 15m/1h/4h/1d нигде не запрашиваются."""
    import re
    from pathlib import Path

    src = (Path(__file__).resolve().parents[2] / "orchestrator" / "mt5_feeds.py").read_text(encoding="utf-8")
    calls = re.findall(r"self\.source\.get_candles\(.*", src)  # весь остаток строки вызова
    assert len(calls) == 2 and all("BASE_TIMEFRAME" in c for c in calls)  # история при старте и обновление
    assert not re.search(r"get_candles\([^)]*[\"'](15m|1h|4h|1d)[\"']", src)
