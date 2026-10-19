"""Данные Forex/металлов для Shadow Mode (EURUSD, XAUUSD, XAGUSD): свечи M5 + производные 15m / 1h / 4h / 1d и тики из котировок MetaApi.

Binance WebSocket этих символов не отдаёт (`kline_*` и `aggTrade` - только крипта), поэтому для MT5-символов пул Shadow питают отдельные фиды с тем же
контрактом, что у `SmcKlineFeed` / `TickFeed` (R5: только закрытые свечи; Оркестратору - по одному `FeatureContext` на свечу, без пропусков):

  * ЕДИНСТВЕННЫЙ источник - M5 из REST рыночных данных MetaApi (`get_candles(..., "5m", ...)`): история при старте и новые закрытые свечи раз в `poll_s`.
    Других таймфреймов у брокера НЕ запрашивается (2026-10-14: «никаких новых REST-запросов»): число запросов растёт только за счёт глубины истории M5 при
    старте (страницы по 1000 свечей, тот же цикл, что был у M15). Свеча закрыта, когда `open_time + interval + settle <= сейчас` (формирующуюся не берём).
  * Все старшие таймфреймы ПРОИЗВОДНЫЕ (`derive_tf`): 15m (рабочий для четырёх стратегий), 1h (структура и свип SMC_Pro_Trend), 4h (HTF Bias для 15m-стратегий),
    1d (HTF Bias SMC_Pro_Trend) собираются из M5 того же хранилища на UTC-сетке: open первой, high/low - экстремумы, close последней, volume - сумма. У брокера
    границы старших свечей привязаны к серверному времени, а гарантия «старшая свеча не отстаёт от рабочей» (HtfBias / MtfState.attach) выполнима, только
    если все шкалы собраны из одних данных. Неполный край (формирующийся час/сутки) не берётся.
  * Снимок. `Mt5HtfView.poll()` (и любой другой производный вид) снимает снимок хранилища; первичные фиды (M5 и производный 15m) в том же цикле обрабатывают ТОТ ЖЕ
    снимок (иначе новая свеча между опросами дала бы рабочую без своей старшей -> DataError). Снимок сбрасывается, когда его забрали все первичные фиды.
  * Дыры в сетке - ЗАКРЫТЫЙ РЫНОК (выходные, пауза, праздники): у MT5 свечи без тиков не бывает, а пайплайн признаков и Оркестратор требуют непрерывную сетку.
    Пропуски между известными закрытыми свечами заполняются ПЛОСКИМИ свечами (open = high = low = close = прошлый close, объём 0). Заполнение идёт только
    «назад» - когда уже пришла следующая реальная свеча, поэтому во время закрытого рынка контекстов нет, а при открытии они приходят подряд (replay).
  * Контексты плоских свечей и `warmup_bars` свечей после дыры в `stale_gap_bars`+ свечей получают `received_ms` = закрытие + 1 ч: Оркестратор
    помечает их `skipped_stale` и НЕ торгует (ATR и средний объём после выходных занижены плоскими свечами; сигналы на них были бы артефактом). Пороги заданы
    в M5-барах (12 и 60 = прежние 4 и 20 M15). Производная 15m-свеча «опоздавшая», если опоздала любая из трёх её M5. Трекер и сроки позиций получают закрытия
    свечей как обычно.
  * `Mt5TickFeed` - тики из котировок (раз в секунду): цена = середина bid/ask, и как `trade`, и как `mark`. Это грубее настоящей ленты aggTrade: лимитные заявки Shadow
    исполняются по проходу цены середины, а не по сделкам (❓ у Forex нет единой ленты сделок; допущение Shadow). Объёма и стороны агрессора нет (OBI_Breakout по
    этим символам сигналов не даёт: нет стакана/ленты - значения `ms_*` = null, fail-closed).

Сбои связи. С `link` (Mt5Link, orchestrator/mt5_link.py; так работает shadow_run с 2026-10-18) все запросы идут через менеджер подключения: сбой
MetaApi -> пауза 5 минут -> переподключение, бесконечно; процесс (и крипта в нём) не падает, MT5-символы на время простоя молчат. После простоя
`refresh` догружает ВСЕ пропущенные M5 (а не последние 8): иначе реальные свечи времени простоя были бы приняты за закрытый рынок (плоские).
Без `link` (прежний режим, тесты): временные сбои MetaApi повторяются до `max_failures` подряд, дальше - исключение.
Fail-Fast (R1) в обоих режимах: ошибка данных (кривая свеча, свеча из будущего) и конфигурации - сразу исключение.
"""

import asyncio
import logging
from bisect import bisect_left
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any, Protocol

import polars as pl

from exchange.mt5_metaapi import MetaApiError
from features import defaults as D
from orchestrator.feature_engine import FeatureEngine
from orchestrator.kline_feed import MultiFeed  # noqa: F401  (прежнее место класса; чисто крипто-запуск этот модуль не импортирует)
from orchestrator.types import FeatureContext, Tick
from orchestrator.ws import wall_clock_ms
from pipeline.loaders.schemas import KLINE_SCHEMA
from utils.errors import ConfigError, DataError

log = logging.getLogger("orchestrator.mt5_feeds")

M5_MS = 5 * 60_000
M15_MS = 15 * 60_000
H1_MS = 3_600_000
H4_MS = 4 * 3_600_000
D1_MS = 86_400_000
STALE_AFTER_MS = 3_600_000  # на сколько «опоздавшим» объявляется контекст, по которому торговать нельзя
CANDLES_PAGE = 1000
BASE_TIMEFRAME = "5m"  # единственный таймфрейм, который запрашивается у MetaApi

Row = tuple  # строка KLINE_SCHEMA


class CandleSource(Protocol):
    async def get_candles(self, symbol: str, timeframe: str, limit: int, *, start_time: datetime | None = None) -> list[tuple[int, float, float, float, float, float]]: ...
    async def get_price(self, symbol: str) -> Any: ...


def _row(open_time: int, o: float, h: float, lo: float, c: float, v: float, interval_ms: int) -> Row:
    return (open_time, o, h, lo, c, v, open_time + interval_ms - 1, v * c, 0, 0.0, 0.0)


def _flat(open_time: int, close: float, interval_ms: int) -> Row:
    return _row(open_time, close, close, close, close, 0.0, interval_ms)


def derive_tf(rows: Mapping[int, Row], base_ms: int, target_ms: int, *, since: int | None = None) -> dict[int, Row]:
    """Полные свечи `target_ms` (UTC-сетка) из непрерывных строк `base_ms`: open первой, high/low - экстремумы, close последней, volume - суммы.
    Неполный край (не все слоты есть) не берётся. `since` - с какого open_time искать (по умолчанию - с начала): производные виды досчитывают только хвост.
    Чистая функция: запросов нет, закрытая свеча старшего таймфрейма после появления не меняется."""
    if base_ms <= 0 or target_ms <= base_ms or target_ms % base_ms:
        raise ConfigError(f"derive_tf: целевой интервал {target_ms} должен быть кратным и большим базового {base_ms}")
    if not rows:
        return {}
    per = target_ms // base_ms
    lo = min(rows) if since is None else since
    first = lo - lo % target_ms
    out: dict[int, Row] = {}
    for start in range(first, max(rows) + 1, target_ms):
        slots = [start + k * base_ms for k in range(per)]
        if all(s in rows for s in slots):
            part = [rows[s] for s in slots]
            out[start] = _row(start, part[0][1], max(r[2] for r in part), min(r[3] for r in part), part[-1][4], sum(r[5] for r in part), target_ms)
    return out


def derive_h4(rows: Mapping[int, Row], m15_ms: int) -> dict[int, Row]:
    """4-часовые свечи из строк `m15_ms` (имя и сигнатура прежние: любая кратная база - M15, M5)."""
    return derive_tf(rows, m15_ms, H4_MS)


class _Group:
    """Общее состояние одного набора MT5-символов: M5-хранилище, снимок на цикл опроса, фоновая задача."""

    def __init__(self, source: CandleSource, broker_name: Callable[[str], str], symbols: tuple[str, ...], interval_ms: int, history_bars: int,
                 windows: Mapping[int, int], clock_ms: Callable[[], int], poll_s: float, settle_s: float, max_failures: int, stale_gap_bars: int, warmup_bars: int,
                 link: Any = None):
        """windows: интервал производного вида (мс) -> сколько его свечей нужно в истории; из них считается глубина M5-хранилища."""
        self.source, self.broker_name, self.symbols = source, broker_name, symbols
        self.interval_ms, self.history = interval_ms, history_bars
        for target in windows:
            if target <= interval_ms or target % interval_ms:
                raise ConfigError(f"производный интервал {target} не кратен базовому {interval_ms}")
        self.windows = dict(windows)
        self.max_per = max([t // interval_ms for t in self.windows] or [1])
        self.depth = max([history_bars, *(bars * (t // interval_ms) for t, bars in self.windows.items())])  # сколько базовых свечей нужно держать
        self.clock, self.poll_s, self.settle_ms = clock_ms, poll_s, int(settle_s * 1000)
        self.max_failures, self.stale_gap_bars, self.warmup_bars = max_failures, stale_gap_bars, warmup_bars
        self.store: dict[str, dict[int, Row]] = {s: {} for s in symbols}
        self.stale_until: dict[str, dict[int, bool]] = {s: {} for s in symbols}  # open_time -> «торговать нельзя»
        self.frozen: tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]] | None = None
        self.primaries: set[str] = {"m5"}  # первичные потребители снимка; он сбрасывается, когда его забрали все
        self._taken: set[str] = set()
        self._gaps_checked: dict[str, int] = {}  # символ -> до какого open_time пропуски уже заполнены
        self.ready = False
        self.task: asyncio.Task | None = None
        self.failures = 0
        self.link = link  # Mt5Link: сбои связи -> пауза и переподключение вместо падения процесса
        self.on_store: Callable[[str, Mapping[int, Row]], None] | None = None  # слушатель обновлений M5-хранилища (кэш свечей для графика клиента: orchestrator/chart_cache.py)

    def closed_limit(self) -> int:
        """open_time самой поздней свечи, которая уже закрыта (с запасом settle)."""
        now = self.clock() - self.settle_ms
        return now - now % self.interval_ms - self.interval_ms

    async def bootstrap(self) -> None:
        need = self.depth + 2 * self.max_per  # запас на неполный край и дыры
        for symbol in self.symbols:
            self.merge(symbol, await self._back_pages(self.broker_name(symbol), lambda rows: len(rows) >= need))
        self.ready = True
        log.info("История MT5 %s загружена: %s", BASE_TIMEFRAME, {s: len(c) for s, c in self.store.items()})

    async def refresh(self) -> None:
        """Новые закрытые M5. Обычно - последние 8; после простоя связи - все пропущенные с последней известной свечи (страницами назад)."""
        limit = self.closed_limit()
        for symbol in self.symbols:
            broker = self.broker_name(symbol)
            store = self.store[symbol]
            last = max(store) if store else None
            missing = 8 if last is None else max(8, (limit - last) // self.interval_ms + 2)
            if missing <= CANDLES_PAGE:
                self.merge(symbol, await self.source.get_candles(broker, BASE_TIMEFRAME, missing))
                continue
            self.merge(symbol, sorted(await self._back_pages(broker, lambda rows: bool(rows) and min(r[0] for r in rows) <= last)))

    async def _back_pages(self, broker: str, enough: Callable[[list[tuple]], bool]) -> list[tuple]:
        """M5 страницами по CANDLES_PAGE назад от текущего момента, пока `enough(rows)` не скажет «хватит» или история брокера не кончится."""
        rows: list[tuple] = []
        start: datetime | None = None
        while not enough(rows):
            page = await self.source.get_candles(broker, BASE_TIMEFRAME, CANDLES_PAGE, start_time=start)
            fresh = [r for r in page if not rows or r[0] < min(x[0] for x in rows)]
            if not fresh:
                break  # история брокера кончилась
            rows += fresh
            start = datetime.fromtimestamp(min(r[0] for r in rows) / 1000, tz=timezone.utc)
        return rows

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
        checked = self._gaps_checked.get(symbol)
        begin = 0 if checked is None else max(0, bisect_left(times, checked) - 1)  # пропуски левее уже заполнены: проверяем только хвост
        for a, b in zip(times[begin:], times[begin + 1:]):
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
        if times:
            self._gaps_checked[symbol] = times[-1]
        keep = self.depth + 3 * self.max_per + 50
        for t in sorted(store)[: max(0, len(store) - keep)]:
            del store[t]
            stale.pop(t, None)
        if self.on_store is not None:
            self.on_store(symbol, store)

    def freeze(self) -> tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]]:
        self.frozen = ({s: dict(r) for s, r in self.store.items()}, {s: dict(r) for s, r in self.stale_until.items()})
        self._taken = set()
        return self.frozen

    def snapshot(self) -> tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]]:
        """Снимок цикла для производных видов (создаётся первым, кто спросил; не сбрасывается)."""
        return self.frozen or self.freeze()

    def take_snapshot(self, consumer: str = "m5") -> tuple[dict[str, dict[int, Row]], dict[str, dict[int, bool]]]:
        """Снимок цикла для первичного фида `consumer`; когда его забрали все первичные фиды, снимок сбрасывается (следующий цикл - новый снимок)."""
        if consumer not in self.primaries:
            raise DataError(f"take_snapshot: {consumer!r} не зарегистрирован первичным потребителем ({sorted(self.primaries)})")
        if self.frozen is not None and consumer in self._taken:
            self.freeze()  # потребитель пришёл за вторым снимком, не дождавшись остальных: новый цикл
        snap = self.snapshot()
        self._taken.add(consumer)
        if self._taken >= self.primaries:
            self.frozen, self._taken = None, set()
        return snap

    async def run(self) -> None:
        await self._guarded(self.bootstrap)
        while True:
            await asyncio.sleep(self.poll_s)
            await self._guarded(self.refresh)

    async def _guarded(self, op: Callable[[], Awaitable[None]]) -> None:
        if self.link is not None:
            await self.link.call("свечи M5", op)
            return
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
    """Свечи M5 MT5-символов (контракт SmcKlineFeed.poll) - рабочий таймфрейм SMC_Pro_Trend. Производные виды: `m15_view()` (рабочий для остальных
    стратегий), `h1_view()`, `htf_view()` (4h), `d1_view()` (старшие, их контексты идут в `MtfState`)."""

    def __init__(self, source: CandleSource, engine: FeatureEngine, *, broker_name: Callable[[str], str], symbols: tuple[str, ...], interval: str = BASE_TIMEFRAME,
                 history_bars: int = 300, htf_history_bars: int = 110, m15_history_bars: int = 300, h1_history_bars: int = D.MTF_STRUCT_HISTORY_BARS,
                 d1_history_bars: int = D.MTF_BIAS_HISTORY_BARS, clock_ms: Callable[[], int] = wall_clock_ms, poll_s: float = 5.0, settle_s: float = 3.0,
                 max_failures: int = 20, stale_gap_bars: int = 12, warmup_bars: int = 60, emit: bool = True, link: Any = None):
        """emit=False: M5 не отдаётся стратегиям (мульти-таймфреймовый режим выключен), но фид по-прежнему участвует в цикле снимков: без него снимок хранилища не сбрасывался бы."""
        if interval != BASE_TIMEFRAME:
            raise ConfigError("Mt5KlineFeed: поддержан только M5 (15m, 1h, 4h и 1d собираются из него)")
        if history_bars < engine.min_bars:
            raise ConfigError(f"history_bars меньше min_bars={engine.min_bars}")
        interval_ms = M5_MS
        windows = {M15_MS: m15_history_bars, H1_MS: h1_history_bars, H4_MS: htf_history_bars, D1_MS: d1_history_bars}
        group = _Group(source, broker_name, tuple(symbols), interval_ms, history_bars, windows, clock_ms, poll_s, settle_s, max_failures, stale_gap_bars, warmup_bars,
                       link=link)
        super().__init__(group, "m5")
        self._engine, self._clock, self.interval, self.interval_ms, self.emit = engine, clock_ms, interval, interval_ms, emit
        self._bars = {"15m": m15_history_bars, "1h": h1_history_bars, "4h": htf_history_bars, "1d": d1_history_bars}
        self._last: dict[str, int | None] = {s: None for s in symbols}

    def set_store_listener(self, listener: Callable[[str, Mapping[int, Row]], None] | None) -> None:
        """Слушатель обновлений M5-хранилища (после каждого `merge`). Только чтение данных: исключения слушателя - его забота (кэш графика ловит OSError сам)."""
        self._g.on_store = listener

    def m15_view(self, engine: FeatureEngine | None = None) -> "Mt5M15View":
        return Mt5M15View(self._g, engine or self._engine, self._bars["15m"])

    def h1_view(self, engine: FeatureEngine | None = None) -> "Mt5H1View":
        return Mt5H1View(self._g, engine or self._engine, self._bars["1h"])

    def htf_view(self, engine: FeatureEngine | None = None) -> "Mt5HtfView":
        return Mt5HtfView(self._g, engine or self._engine, self._bars["4h"])

    def d1_view(self, engine: FeatureEngine | None = None) -> "Mt5D1View":
        return Mt5D1View(self._g, engine or self._engine, self._bars["1d"])

    def check(self) -> None:
        self._check_task()

    async def poll(self) -> list[FeatureContext]:
        self._check_task()
        if not self._g.ready:
            return []
        store, stale = self._g.take_snapshot("m5")
        out: list[FeatureContext] = []
        for symbol in self._g.symbols:
            rows = store[symbol]
            if not rows:
                continue
            last = self._last[symbol]
            if last is None:  # история на старте не эмитится: последняя закрытая свеча - точка отсчёта
                self._last[symbol] = last = max(rows)
                continue
            for t in range(last + self.interval_ms, max(rows) + 1, self.interval_ms):  # сетка непрерывна; не обходим всё хранилище (оно в десятки тысяч свечей)
                if t not in rows:
                    raise DataError(f"{symbol} {self.interval}: дыра в свечах после {last} (нет {t})")
                if self.emit:
                    out.append(await self._context(symbol, t, rows, stale[symbol].get(t, False)))
                last = t
            self._last[symbol] = last
        return out

    async def _context(self, symbol: str, open_time: int, rows: dict[int, Row], stale: bool) -> FeatureContext:
        iv = self.interval_ms
        times = [t for t in range(open_time - (self._g.history - 1) * iv, open_time + 1, iv) if t in rows]  # последние `history` свечей непрерывной сетки
        candles = pl.DataFrame([rows[t] for t in times], schema=KLINE_SCHEMA, orient="row")
        close_ms = open_time + self.interval_ms
        received = close_ms + STALE_AFTER_MS if stale else max(self._clock(), close_ms)
        return await asyncio.to_thread(self._engine.build, symbol, self.interval, self.interval_ms, candles, received)


class Mt5DerivedView(_FeedBase):
    """Свечи интервала `interval` (кратного M5), собранные из ТОГО ЖЕ снимка M5-хранилища (контракт SmcKlineFeed.poll).

    `emit_latest=True` (старшие виды: 1h, 4h, 1d): на старте - последняя закрытая свеча с историей (состояние, как `emit_latest_on_start`), затем каждая новая.
    `emit_latest=False` (первичный 15m): история на старте не эмитится, затем каждая новая свеча подряд; «опоздавшая» (stale) - если опоздала любая её M5.
    Производные свечи кэшируются: закрытая старшая свеча не меняется, поэтому каждый цикл досчитывается только хвост (а не вся история)."""

    def __init__(self, group: _Group, engine: FeatureEngine, interval: str, interval_ms: int, history_bars: int, *, emit_latest: bool):
        super().__init__(group, interval)
        if history_bars < engine.min_bars:
            raise ConfigError(f"{interval}: history_bars={history_bars} меньше min_bars={engine.min_bars}")
        if interval_ms not in group.windows or history_bars > group.windows[interval_ms]:
            raise ConfigError(f"{interval}: глубина {history_bars} больше заложенной в хранилище ({group.windows.get(interval_ms)}): M5-история не покроет")
        self._engine, self.interval, self.interval_ms, self._bars, self._emit_latest = engine, interval, interval_ms, history_bars, emit_latest
        self._per = interval_ms // group.interval_ms
        self._last: dict[str, int | None] = {s: None for s in group.symbols}
        self._cache: dict[str, dict[int, Row]] = {s: {} for s in group.symbols}
        if not emit_latest:
            group.primaries.add(interval)

    def start(self) -> None:  # фоновую задачу владеет M5-фид
        pass

    async def aclose(self) -> None:
        pass

    def check(self) -> None:
        self._check_task()

    def _derived(self, symbol: str, rows: dict[int, Row]) -> dict[int, Row]:
        cache = self._cache[symbol]
        since = max(cache) + self.interval_ms if cache else None
        cache.update(derive_tf(rows, self._g.interval_ms, self.interval_ms, since=since))
        for t in sorted(cache)[: max(0, len(cache) - self._bars - 5)]:
            del cache[t]
        return cache

    async def poll(self) -> list[FeatureContext]:
        self._check_task()
        if not self._g.ready:
            return []
        store, stale = self._g.take_snapshot(self.interval) if not self._emit_latest else self._g.snapshot()
        out: list[FeatureContext] = []
        for symbol in self._g.symbols:
            derived = self._derived(symbol, store[symbol])
            if not derived:
                continue
            last, times = self._last[symbol], sorted(derived)
            if last is None:
                self._last[symbol] = times[-1]
                if self._emit_latest:  # старт: последняя закрытая свеча - контекст с историей
                    out.append(await self._context(symbol, times[-1], derived, stale[symbol], received_now=True))
                continue
            for t in times:
                if t <= last:
                    continue
                if t != last + self.interval_ms:
                    raise DataError(f"{symbol} {self.interval}: дыра в производных свечах между {last} и {t}")
                out.append(await self._context(symbol, t, derived, stale[symbol], received_now=False))
                last = t
            self._last[symbol] = last
        return out

    async def _context(self, symbol: str, open_time: int, derived: dict[int, Row], stale: dict[int, bool], *, received_now: bool) -> FeatureContext:
        times = sorted(t for t in derived if t <= open_time)[-self._bars:]
        # непрерывность сетки гарантирована заполнением M5: берём только непрерывный хвост
        run = [times[-1]]
        for t in reversed(times[:-1]):
            if run[-1] - t != self.interval_ms:
                break
            run.append(t)
        times = sorted(run)
        candles = pl.DataFrame([derived[t] for t in times], schema=KLINE_SCHEMA, orient="row")
        close_ms = open_time + self.interval_ms
        if self._emit_latest:
            received = max(self._g.clock(), close_ms) if received_now else close_ms + STALE_AFTER_MS
        else:  # первичный 15m: как M5 - «опоздавшая» (плоские свечи, разогрев после дыры) не торгуется
            late = any(stale.get(open_time + k * self._g.interval_ms, False) for k in range(self._per))
            received = close_ms + STALE_AFTER_MS if late else max(self._g.clock(), close_ms)
        return await asyncio.to_thread(self._engine.build, symbol, self.interval, self.interval_ms, candles, received)


class Mt5M15View(Mt5DerivedView):
    """15m (рабочий таймфрейм SMC_Counter_Trend, OBI_Breakout, ICT_Killzones, VWAP_Reversion) - первичный, обязан опрашиваться ПОСЛЕ старших видов."""

    def __init__(self, group: _Group, engine: FeatureEngine, history_bars: int = 300):
        super().__init__(group, engine, "15m", M15_MS, history_bars, emit_latest=False)


class Mt5H1View(Mt5DerivedView):
    """1h: структура и пул ликвидности SMC_Pro_Trend (контекст идёт в MtfState: свип считается на 1h)."""

    def __init__(self, group: _Group, engine: FeatureEngine, history_bars: int = D.MTF_STRUCT_HISTORY_BARS):
        super().__init__(group, engine, "1h", H1_MS, history_bars, emit_latest=True)


class Mt5HtfView(Mt5DerivedView):
    """4h (HTF Bias для 15m-стратегий): на старте - последняя закрытая 4h, затем каждая новая."""

    def __init__(self, group: _Group, engine: FeatureEngine, history_bars: int = 110):
        super().__init__(group, engine, "4h", H4_MS, history_bars, emit_latest=True)


class Mt5D1View(Mt5DerivedView):
    """1d (HTF Bias SMC_Pro_Trend)."""

    def __init__(self, group: _Group, engine: FeatureEngine, history_bars: int = D.MTF_BIAS_HISTORY_BARS):
        super().__init__(group, engine, "1d", D1_MS, history_bars, emit_latest=True)


class Mt5TickFeed:
    """Тики MT5-символов: котировка раз в `poll_s` -> два тика (trade и mark) по середине bid/ask."""

    def __init__(self, source: CandleSource, on_tick: Callable[[Tick], None], on_gap: Callable[[int], None], *, broker_name: Callable[[str], str],
                 symbols: tuple[str, ...], clock_ms: Callable[[], int] = wall_clock_ms, poll_s: float = 1.0, max_failures: int = 20, link: Any = None):
        """link (Mt5Link): сбой связи -> пауза и переподключение (тики за простой потеряны: `on_gap` вызывает сам менеджер при падении связи)."""
        self._source, self._on_tick, self._on_gap = source, on_tick, on_gap
        self._link = link
        if link is not None:
            link.on_down(on_gap)
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
        if self._link is not None:
            while True:
                await self._link.call("котировки", self.poll_once)
                await asyncio.sleep(self._poll_s)
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


async def connect_mt5_data(symbols: tuple[str, ...]):
    """Менеджер подключения MetaApi для ДАННЫХ MT5-символов (`Mt5Link`) или None, если ключей MetaApi в окружении нет (тогда MT5-символы пропускаются).

    Подключения здесь НЕТ (2026-10-18): его делает `Mt5Link` в фоновых задачах фидов MT5, поэтому недоступный MetaApi не задерживает и не роняет
    старт процесса с криптой; сбой связи - пауза 5 минут и переподключение. Ошибки конфигурации (нет ключей - выше, 401/403, символ не найден у брокера) -
    по-прежнему исключение (R1). Список символов брокера кешируется на 24 часа в DATA_DIR."""
    import config
    from exchange.mt5_metaapi import MetaApiCredentials, MetaApiLiveClient
    from exchange.symbol_mapper import SymbolMapper
    from orchestrator.mt5_link import Mt5Link

    try:
        MetaApiCredentials.from_env()
    except ConfigError as exc:
        log.warning("MT5-символы %s пропущены: нет ключей MetaApi (%s). Задайте METAAPI_TOKEN и METAAPI_ACCOUNT_ID в .env.shadow на сервере", symbols, exc)
        return None
    client = MetaApiLiveClient(symbols_cache=config.DATA_DIR / config.METAAPI_SYMBOLS_CACHE_NAME)
    mapper = SymbolMapper(client, allow_disabled=True)  # закрытый рынок при старте не должен ронять сбор данных
    return Mt5Link(client, mapper, symbols)
