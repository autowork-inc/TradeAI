"""Данные Forex/металлов для Shadow Mode (EURUSD, XAUUSD, XAGUSD): свечи M15 + производные H4 и тики из котировок MetaApi.

Binance WebSocket этих символов не отдаёт (`kline_15m` и `aggTrade` - только крипта), поэтому для MT5-символов пул Shadow питают отдельные фиды с тем же
контрактом, что у `SmcKlineFeed` / `TickFeed` (R5: только закрытые свечи; Оркестратору - по одному `FeatureContext` на свечу, без пропусков):

  * `Mt5KlineFeed` - M15: история и новые закрытые свечи из REST рыночных данных MetaApi (`get_candles`), опрос раз в `poll_s`. Свеча закрыта, когда
    `open_time + interval + settle <= сейчас` (последнюю формирующуюся не берём).
  * Дыры в сетке - ЗАКРЫТЫЙ РЫНОК (выходные, пауза, праздники): у MT5 свечи без тиков не бывает, а пайплайн признаков и Оркестратор требуют непрерывную сетку.
    Пропуски между известными закрытыми свечами заполняются ПЛОСКИМИ свечами (open = high = low = close = прошлый close, объём 0). Заполнение идёт только
    «назад» - когда уже пришла следующая реальная свеча, поэтому во время закрытого рынка контекстов нет, а при открытии они приходят подряд (replay).
  * Контексты плоских свечей и `warmup_bars` свечей после дыры в `stale_gap_bars`+ свечей получают `received_ms` = закрытие + 1 ч: Оркестратор
    помечает их `skipped_stale` и НЕ торгует (ATR и средний объём после выходных занижены плоскими свечами; сигналы на них были бы артефактом). Трекер и сроки
    позиций при этом получают закрытия свечей как обычно.
  * H4 для HTF-фильтра ПРОИЗВОДНЫЙ: собирается из M15 (границы UTC 00/04/08/12/16/20) того же хранилища, а не берётся у брокера: у брокера границы H4 привязаны к
    серверному времени, а гарантия «H4 не отстаёт от M15» (HtfBias.attach) выполнима, только если обе шкалы из одних данных. `Mt5HtfView.poll()` снимает снимок
    хранилища, `Mt5KlineFeed.poll()` затем обрабатывает ТОТ ЖЕ снимок (иначе новая свеча между опросами дала бы M15 без своего H4 -> DataError).
  * `Mt5TickFeed` - тики из котировок (раз в секунду): цена = середина bid/ask, и как `trade`, и как `mark`. Это грубее настоящей ленты aggTrade: лимитные заявки Shadow
    исполняются по проходу цены середины, а не по сделкам (❓ у Forex нет единой ленты сделок; допущение Shadow). Объёма и стороны агрессора нет (OBI_Breakout по
    этим символам сигналов не даёт: нет стакана/ленты - значения `ms_*` = null, fail-closed).

Fail-Fast (R1): временные сбои MetaApi (таймаут, терминал не подключён) повторяются до `max_failures` подряд (иначе падение одной площадки роняло бы и крипту
из того же процесса); дальше - исключение, процесс падает, Docker перезапускает. Ошибка данных (кривая свеча, свеча из будущего) - сразу.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Protocol

import polars as pl

from exchange.mt5_metaapi import MetaApiError
from orchestrator.feature_engine import FeatureEngine
from orchestrator.types import FeatureContext, Tick
from orchestrator.ws import wall_clock_ms
from pipeline.loaders.schemas import KLINE_SCHEMA
from utils.errors import ConfigError, DataError

log = logging.getLogger("orchestrator.mt5_feeds")

H4_MS = 4 * 3_600_000
STALE_AFTER_MS = 3_600_000  # на сколько «опоздавшим» объявляется контекст, по которому торговать нельзя
CANDLES_PAGE = 1000

Row = tuple  # строка KLINE_SCHEMA


class CandleSource(Protocol):
    async def get_candles(self, symbol: str, timeframe: str, limit: int, *, start_time: datetime | None = None) -> list[tuple[int, float, float, float, float, float]]: ...
    async def get_price(self, symbol: str) -> Any: ...


def _row(open_time: int, o: float, h: float, lo: float, c: float, v: float, interval_ms: int) -> Row:
    return (open_time, o, h, lo, c, v, open_time + interval_ms - 1, v * c, 0, 0.0, 0.0)


def _flat(open_time: int, close: float, interval_ms: int) -> Row:
    return _row(open_time, close, close, close, close, 0.0, interval_ms)


def derive_h4(rows: dict[int, Row], m15_ms: int) -> dict[int, Row]:
    """Полные 4-часовые свечи (UTC-сетка) из непрерывных M15-строк: open первой, high/low экстремумы, close последней, volume суммы. Неполный край - не берётся."""
    per = H4_MS // m15_ms
    out: dict[int, Row] = {}
    for start in sorted({t - t % H4_MS for t in rows}):
        slots = [start + k * m15_ms for k in range(per)]
        if all(s in rows for s in slots):
            part = [rows[s] for s in slots]
            out[start] = _row(start, part[0][1], max(r[2] for r in part), min(r[3] for r in part), part[-1][4], sum(r[5] for r in part), H4_MS)
    return out


class _Group:
    """Общее состояние одного набора MT5-символов: M15-хранилище, снимок на цикл опроса, фоновая задача."""

    def __init__(self, source: CandleSource, broker_name: Callable[[str], str], symbols: tuple[str, ...], interval_ms: int, history_bars: int, htf_bars: int,
                 clock_ms: Callable[[], int], poll_s: float, settle_s: float, max_failures: int, stale_gap_bars: int, warmup_bars: int):
        self.source, self.broker_name, self.symbols = source, broker_name, symbols
        self.interval_ms, self.history, self.htf_bars = interval_ms, history_bars, htf_bars
        self.clock, self.poll_s, self.settle_ms = clock_ms, poll_s, int(settle_s * 1000)
        self.max_failures, self.stale_gap_bars, self.warmup_bars = max_failures, stale_gap_bars, warmup_bars
        self.store: dict[str, dict[int, Row]] = {s: {} for s in symbols}
        self.stale_until: dict[str, dict[int, bool]] = {s: {} for s in symbols}  # open_time -> «торговать нельзя»
        self.frozen: tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]] | None = None
        self.ready = False
        self.task: asyncio.Task | None = None
        self.failures = 0

    def closed_limit(self) -> int:
        """open_time самой поздней свечи, которая уже закрыта (с запасом settle)."""
        now = self.clock() - self.settle_ms
        return now - now % self.interval_ms - self.interval_ms

    async def bootstrap(self) -> None:
        need = max(self.history, self.htf_bars * (H4_MS // self.interval_ms)) + 2 * (H4_MS // self.interval_ms)  # запас на неполный край и дыры
        for symbol in self.symbols:
            broker = self.broker_name(symbol)
            rows: list[tuple] = []
            start: datetime | None = None
            while len(rows) < need:
                page = await self.source.get_candles(broker, "15m", CANDLES_PAGE, start_time=start)
                fresh = [r for r in page if not rows or r[0] < min(x[0] for x in rows)]
                if not fresh:
                    break  # история брокера кончилась
                rows += fresh
                start = datetime.fromtimestamp(min(r[0] for r in rows) / 1000, tz=timezone.utc)
            self.merge(symbol, rows)
        self.ready = True
        log.info("История MT5 M15 загружена: %s", {s: len(c) for s, c in self.store.items()})

    async def refresh(self) -> None:
        for symbol in self.symbols:
            page = await self.source.get_candles(self.broker_name(symbol), "15m", 8)
            self.merge(symbol, page)

    def merge(self, symbol: str, candles: list[tuple]) -> None:
        """Закрытые свечи -> хранилище; пропуски между известными закрытыми свечами - плоские свечи (рынок был закрыт)."""
        store, stale = self.store[symbol], self.stale_until[symbol]
        limit = self.closed_limit()
        for t, o, h, lo, c, v in candles:
            if t % self.interval_ms:
                raise DataError(f"{symbol}: свеча {t} не на сетке {self.interval_ms} мс (часовой пояс брокера?)")
            if t > limit or t in store:
                continue  # формирующаяся или уже известная (закрытая свеча не меняется)
            store[t] = _row(t, o, h, lo, c, v, self.interval_ms)
        times = sorted(store)
        for a, b in zip(times, times[1:]):
            gap = (b - a) // self.interval_ms - 1
            if gap <= 0:
                continue
            for k in range(1, gap + 1):
                t = a + k * self.interval_ms
                if t not in store:
                    store[t] = _flat(t, store[a][4], self.interval_ms)
                    stale[t] = True
            if gap >= self.stale_gap_bars:
                for k in range(self.warmup_bars):
                    stale[b + k * self.interval_ms] = True
        for t in sorted(store)[: max(0, len(store) - (self.htf_bars * (H4_MS // self.interval_ms) + 3 * (H4_MS // self.interval_ms) + 50))]:
            del store[t]
            stale.pop(t, None)

    def freeze(self) -> tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]]:
        self.frozen = ({s: dict(r) for s, r in self.store.items()}, {s: dict(r) for s, r in self.stale_until.items()})
        return self.frozen

    def take_snapshot(self) -> tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]]:
        snap, self.frozen = (self.frozen or self.freeze()), None
        return snap

    async def run(self) -> None:
        await self._guarded(self.bootstrap)
        while True:
            await asyncio.sleep(self.poll_s)
            await self._guarded(self.refresh)

    async def _guarded(self, op: Callable[[], Awaitable[None]]) -> None:
        while True:
            try:
                await op()
                self.failures = 0
                return
            except MetaApiError as exc:
                self.failures += 1
                if self.failures >= self.max_failures:
                    raise
                log.warning("MetaApi: сбой данных (%d/%d): %s", self.failures, self.max_failures, exc)
                await asyncio.sleep(self.poll_s)


class _FeedBase:
    def __init__(self, group: _Group, name: str):
        self._g, self._name = group, name

    def start(self) -> None:
        if self._g.task is None:
            self._g.task = asyncio.create_task(self._g.run(), name="mt5_kline_feed")

    async def aclose(self) -> None:
        task, self._g.task = self._g.task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _check_task(self) -> None:
        t = self._g.task
        if t is not None and t.done():
            if t.cancelled():
                raise DataError("Задача фида MT5 отменена")
            exc = t.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача фида MT5 завершилась без ошибки: так быть не должно")


class Mt5KlineFeed(_FeedBase):
    """Свечи M15 MT5-символов (контракт SmcKlineFeed.poll). Пара к нему - `htf_view()` (H4)."""

    def __init__(self, source: CandleSource, engine: FeatureEngine, *, broker_name: Callable[[str], str], symbols: tuple[str, ...], interval: str = "15m",
                 history_bars: int = 300, htf_history_bars: int = 110, clock_ms: Callable[[], int] = wall_clock_ms, poll_s: float = 5.0, settle_s: float = 3.0,
                 max_failures: int = 20, stale_gap_bars: int = 4, warmup_bars: int = 20):
        if interval != "15m":
            raise ConfigError("Mt5KlineFeed: поддержан только M15 (H4 собирается из него)")
        if history_bars < engine.min_bars or htf_history_bars < engine.min_bars:
            raise ConfigError(f"history_bars/htf_history_bars меньше min_bars={engine.min_bars}")
        interval_ms = 15 * 60_000
        group = _Group(source, broker_name, tuple(symbols), interval_ms, history_bars, htf_history_bars, clock_ms, poll_s, settle_s, max_failures,
                       stale_gap_bars, warmup_bars)
        super().__init__(group, "m15")
        self._engine, self._clock, self.interval, self.interval_ms = engine, clock_ms, interval, interval_ms
        self._last: dict[str, int | None] = {s: None for s in symbols}

    def htf_view(self, engine: FeatureEngine | None = None) -> "Mt5HtfView":
        return Mt5HtfView(self._g, engine or self._engine)

    def check(self) -> None:
        self._check_task()

    async def poll(self) -> list[FeatureContext]:
        self._check_task()
        if not self._g.ready:
            return []
        store, stale = self._g.take_snapshot()
        out: list[FeatureContext] = []
        for symbol in self._g.symbols:
            rows = store[symbol]
            if not rows:
                continue
            last = self._last[symbol]
            if last is None:  # история на старте не эмитится: последняя закрытая свеча - точка отсчёта
                self._last[symbol] = last = max(rows)
                continue
            for t in sorted(x for x in rows if x > last):
                if t != last + self.interval_ms:
                    raise DataError(f"{symbol} {self.interval}: дыра в свечах между {last} и {t}")
                out.append(await self._context(symbol, t, rows, stale[symbol].get(t, False)))
                last = t
            self._last[symbol] = last
        return out

    async def _context(self, symbol: str, open_time: int, rows: dict[int, Row], stale: bool) -> FeatureContext:
        times = sorted(t for t in rows if t <= open_time)[-self._g.history:]
        candles = pl.DataFrame([rows[t] for t in times], schema=KLINE_SCHEMA, orient="row")
        close_ms = open_time + self.interval_ms
        received = close_ms + STALE_AFTER_MS if stale else max(self._clock(), close_ms)
        return await asyncio.to_thread(self._engine.build, symbol, self.interval, self.interval_ms, candles, received)


class Mt5HtfView(_FeedBase):
    """H4 из тех же M15-данных (контракт SmcKlineFeed.poll с `emit_latest_on_start`): на старте - последняя закрытая H4, затем каждая новая."""

    interval = "4h"

    def __init__(self, group: _Group, engine: FeatureEngine):
        super().__init__(group, "h4")
        self._engine = engine
        self.interval_ms = H4_MS
        self._last: dict[str, int | None] = {s: None for s in group.symbols}

    def start(self) -> None:  # фоновую задачу владеет M15-фид
        pass

    async def aclose(self) -> None:
        pass

    def check(self) -> None:
        self._check_task()

    async def poll(self) -> list[FeatureContext]:
        self._check_task()
        if not self._g.ready:
            return []
        store, stale = self._g.freeze()  # снимок на цикл: M15-фид обработает тот же
        out: list[FeatureContext] = []
        for symbol in self._g.symbols:
            h4 = derive_h4(store[symbol], self._g.interval_ms)
            if not h4:
                continue
            last = self._last[symbol]
            times = sorted(h4)
            if last is None:  # старт: последняя закрытая H4 - контекст с историей (как emit_latest_on_start)
                self._last[symbol] = times[-1]
                out.append(await self._context(symbol, times[-1], h4, received_now=True))
                continue
            for t in times:
                if t <= last:
                    continue
                if t != last + H4_MS:
                    raise DataError(f"{symbol} 4h: дыра в производных свечах между {last} и {t}")
                out.append(await self._context(symbol, t, h4, received_now=False))
                last = t
            self._last[symbol] = last
        return out

    async def _context(self, symbol: str, open_time: int, h4: dict[int, Row], *, received_now: bool) -> FeatureContext:
        times = sorted(t for t in h4 if t <= open_time)[-self._g.htf_bars:]
        # непрерывность сетки H4 гарантирована заполнением M15: берём только непрерывный хвост
        run = [times[-1]]
        for t in reversed(times[:-1]):
            if run[-1] - t != H4_MS:
                break
            run.append(t)
        times = sorted(run)
        candles = pl.DataFrame([h4[t] for t in times], schema=KLINE_SCHEMA, orient="row")
        received = max(self._g.clock(), open_time + H4_MS) if received_now else open_time + H4_MS + STALE_AFTER_MS
        return await asyncio.to_thread(self._engine.build, symbol, "4h", H4_MS, candles, received)


class Mt5TickFeed:
    """Тики MT5-символов: котировка раз в `poll_s` -> два тика (trade и mark) по середине bid/ask."""

    def __init__(self, source: CandleSource, on_tick: Callable[[Tick], None], on_gap: Callable[[int], None], *, broker_name: Callable[[str], str],
                 symbols: tuple[str, ...], clock_ms: Callable[[], int] = wall_clock_ms, poll_s: float = 1.0, max_failures: int = 20):
        self._source, self._on_tick, self._on_gap = source, on_tick, on_gap
        self._broker, self._symbols = broker_name, tuple(symbols)
        self._clock, self._poll_s, self._max = clock_ms, poll_s, max_failures
        self._task: asyncio.Task | None = None
        self.last_tick_ms: dict[str, int] = {}

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="mt5_tick_feed")

    async def aclose(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def check(self) -> None:
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise DataError("Задача тиков MT5 отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача тиков MT5 завершилась без ошибки: так быть не должно")

    async def poll_once(self) -> None:
        ts = self._clock()
        for symbol in self._symbols:
            q = await self._source.get_price(self._broker(symbol))
            mid = (q.bid + q.ask) / 2
            self.last_tick_ms[symbol] = ts
            self._on_tick(Tick(symbol, mid, ts, "trade"))
            self._on_tick(Tick(symbol, mid, ts, "mark"))

    async def _run(self) -> None:
        failures = 0
        while True:
            try:
                await self.poll_once()
                failures = 0
            except MetaApiError as exc:
                failures += 1
                if failures >= self._max:
                    raise
                log.warning("MetaApi: сбой котировок (%d/%d): %s", failures, self._max, exc)
                self._on_gap(self._clock())  # тики за сбой потеряны: активные сделки помечаются gap_affected
            await asyncio.sleep(self._poll_s)


class MultiFeed:
    """Несколько фидов с одним контрактом (Binance + MT5) как один: poll склеивает контексты, start/aclose/check - всем."""

    def __init__(self, feeds: list[Any]):
        self._feeds = feeds

    def start(self) -> None:
        for f in self._feeds:
            f.start()

    async def aclose(self) -> None:
        for f in self._feeds:
            await f.aclose()

    def check(self) -> None:
        for f in self._feeds:
            f.check()

    async def poll(self) -> list[FeatureContext]:
        out: list[FeatureContext] = []
        for f in self._feeds:
            out += await f.poll()
        return out


async def connect_mt5_data(symbols: tuple[str, ...]):
    """Подключение к MetaApi для ДАННЫХ MT5-символов: (клиент, SymbolMapper) или None, если ключей MetaApi в окружении нет (тогда MT5-символы пропускаются).
    Ошибка подключения при заданных ключах - исключение (R1): молча торговать без данных нельзя."""
    from exchange.mt5_metaapi import MetaApiCredentials, MetaApiLiveClient
    from exchange.symbol_mapper import SymbolMapper

    try:
        MetaApiCredentials.from_env()
    except ConfigError as exc:
        log.warning("MT5-символы %s пропущены: нет ключей MetaApi (%s). Задайте METAAPI_TOKEN и METAAPI_ACCOUNT_ID в .env.shadow на сервере", symbols, exc)
        return None
    client = MetaApiLiveClient()
    await client.connect()
    mapper = SymbolMapper(client, allow_disabled=True)  # закрытый рынок при старте не должен ронять сбор данных
    await mapper.load(symbols)
    return client, mapper
