"""Автоселектор монет Binance Futures по суточному объёму (решение пользователя 2026-10-18).

Правило (проверка раз в `config.UNIVERSE_REFRESH_S` = 6 часов, публичные эндпоинты exchangeInfo и ticker/24hr, ключи не нужны):
  * BTC и ETH закреплены (`config.UNIVERSE_PINNED`): всегда live-уровень, как бы ни шёл объём;
  * остальные USDT-перпетуалы по `quoteVolume` за 24 ч:  >= 400M -> live-уровень (`config.LIVE_BINANCE_SYMBOLS`, перевод в live автоматический);
    100M..400M -> скоуп Binance Test (`config.BINANCE_TEST_SYMBOLS`, только Shadow); < 100M -> не отслеживается.

Close-Only. Монета, упавшая из live-уровня, исчезает из `LIVE_BINANCE_SYMBOLS`: `ExecutionRouter` отвечает на новые входы `unsupported_symbol`, а уже открытая
боевая позиция (и ждущая заявка) продолжает вестись движком до закрытия по TP/SL/BE/трейлингу/тайм-ауту - ведение сделки не зависит от состава набора. Монета
ниже 100M перестаёт давать новые сигналы (`Universe.tracked`), но её потоки остаются подписанными: открытые сделки (Shadow и live) получают тики и свечи до конца.
Потоки не отписываются до рестарта процесса (рестарт строит набор заново). Монета, перешедшая из Binance Test в live, получает фильтры биржи (`prepare_live`)
и поток стакана для OBI_Breakout (`add_book`) ДО попадания в набор live.

Новые монеты подключаются в работающий процесс отдельной группой потоков (`add_group`): существующие WebSocket-соединения не затрагиваются. Группа сначала
поднимает фиды старших таймфреймов (1h/4h/1d), ждёт, пока их состояние попадёт в `MtfState`, и только потом запускает рабочие свечи и тики: рабочая свеча новой монеты
не может прийти раньше своего bias.

Сбои: временная недоступность биржи (сеть, 5xx) при опросе - повтор через `UNIVERSE_RETRY_S`, после `UNIVERSE_FETCH_FAILURES_MAX` подряд - падение (R1); бан/лимит
(429/418) и мусорные ответы - сразу исключение. Монеты моложе `UNIVERSE_MIN_LISTING_DAYS` не берутся (для 1d-bias нужно 60+ свечей).
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import config
from utils.errors import ConfigError, DataError, HttpServerError, NetworkError
from utils.storage import atomic_write_text
from utils.symbolset import SymbolSet

log = logging.getLogger("orchestrator.universe")

EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
TICKER_24H_PATH = "/fapi/v1/ticker/24hr"
DAY_MS = 86_400_000
GROUP_BATCH = 40  # символов в одной группе потоков (URL комбинированного потока и первичная загрузка истории остаются умеренными)
STATE_VERSION = 1


@dataclass(frozen=True)
class Tiers:
    live: tuple[str, ...]  # закреплённые первыми, затем по убыванию объёма
    test: tuple[str, ...]  # по убыванию объёма
    volumes: Mapping[str, float] = field(default_factory=dict)  # quoteVolume за 24 ч, USDT

    def all(self) -> tuple[str, ...]:
        return self.live + self.test


@dataclass(frozen=True)
class Diff:
    promoted: tuple[str, ...]  # стали live (в т.ч. новые монеты сразу в live)
    demoted: tuple[str, ...]  # были live, объём упал: Close-Only (новые входы закрыты, открытые сделки ведутся)
    added_test: tuple[str, ...]  # новые в Binance Test
    dropped: tuple[str, ...]  # перестали отслеживаться (< 100M): новых сигналов нет

    def empty(self) -> bool:
        return not (self.promoted or self.demoted or self.added_test or self.dropped)


def _symbol_ok(symbol: str) -> bool:
    return symbol.isascii() and symbol.isalnum() and symbol == symbol.upper()


def parse_listings(info: Any, *, min_listing_days: int) -> set[str]:
    """exchangeInfo -> допустимые символы: USDT-перпетуалы в статусе TRADING, торгуемые не моложе `min_listing_days`. Мусор в ответе - DataError."""
    if not isinstance(info, dict) or not isinstance(info.get("symbols"), list) or not isinstance(info.get("serverTime"), int):
        raise DataError(f"Неожиданный ответ {EXCHANGE_INFO_PATH}: {str(info)[:200]}")
    oldest = info["serverTime"] - min_listing_days * DAY_MS
    out: set[str] = set()
    for item in info["symbols"]:
        try:
            symbol, status, kind, quote = item["symbol"], item["status"], item["contractType"], item["quoteAsset"]
            onboard = int(item["onboardDate"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданная запись exchangeInfo: {str(item)[:200]}") from exc
        if status == "TRADING" and kind == "PERPETUAL" and quote == "USDT" and _symbol_ok(symbol) and onboard <= oldest:
            out.add(symbol)
    return out


def parse_volumes(tickers: Any) -> dict[str, float]:
    """ticker/24hr -> {символ: quoteVolume}. Мусор - DataError."""
    if not isinstance(tickers, list):
        raise DataError(f"Неожиданный ответ {TICKER_24H_PATH}: {str(tickers)[:200]}")
    out: dict[str, float] = {}
    for item in tickers:
        try:
            volume = float(item["quoteVolume"])
            symbol = item["symbol"]
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданная запись ticker/24hr: {str(item)[:200]}") from exc
        if volume != volume or volume < 0:
            raise DataError(f"{symbol}: некорректный quoteVolume {item['quoteVolume']!r}")
        out[symbol] = volume
    return out


def classify(volumes: Mapping[str, float], eligible: Collection[str], *, pinned: Sequence[str] = config.UNIVERSE_PINNED,
             live_min: float = config.UNIVERSE_LIVE_MIN_USDT, test_min: float = config.UNIVERSE_TEST_MIN_USDT,
             max_symbols: int = config.UNIVERSE_MAX_SYMBOLS) -> Tiers:
    """Чистая функция: объёмы -> уровни. Закреплённые монеты обязаны быть в данных биржи (иначе DataError: ответ неполный)."""
    if not (0 < test_min < live_min):
        raise ConfigError(f"classify: нужно 0 < test_min < live_min, получено {test_min}, {live_min}")
    absent = [s for s in pinned if s not in volumes]
    if absent:
        raise DataError(f"В ticker/24hr нет закреплённых монет {absent}: ответ биржи неполный")
    ranked = sorted(((volumes[s], s) for s in eligible if s in volumes and s not in pinned), key=lambda x: (-x[0], x[1]))
    live_alts = [s for v, s in ranked if v >= live_min]
    test = [s for v, s in ranked if test_min <= v < live_min]
    room = max_symbols - len(pinned)
    if len(live_alts) + len(test) > room:  # предохранитель: остаются самые ликвидные (live раньше test)
        log.warning("Кандидатов %d больше предела %d: остаются самые ликвидные", len(live_alts) + len(test), room)
        live_alts = live_alts[:room]
        test = test[: room - len(live_alts)]
    return Tiers(tuple(pinned) + tuple(live_alts), tuple(test), {s: volumes[s] for s in (*pinned, *live_alts, *test)})


class Universe:
    """Текущий состав: живые `SymbolSet` (их читают маршрутизатор live, фильтр стратегий, валидация) + MT5-символы, подписанные потоки и снимок на диск."""

    def __init__(self, *, live: SymbolSet = config.LIVE_BINANCE_SYMBOLS, test: SymbolSet = config.BINANCE_TEST_SYMBOLS,
                 pinned: Sequence[str] = config.UNIVERSE_PINNED, mt5: Sequence[str] = config.LIVE_MT5_SYMBOLS, state_path: Path | None = None,
                 clock_ms: Callable[[], int] = lambda: int(time.time() * 1000)):
        self.live, self.test, self.pinned, self.mt5 = live, test, tuple(pinned), tuple(mt5)
        self.live.replace([*self.pinned, *(s for s in self.live if s not in self.pinned)])
        self.subscribed: set[str] = set(self.pinned)  # символы с запущенными потоками (закреплённые строит shadow_run при старте)
        self.book: set[str] = set()  # символы, по которым идёт стакан (OBI_Breakout)
        self._path, self._clock = state_path, clock_ms
        self._volumes: dict[str, float] = {}
        self._check_disjoint()

    def _check_disjoint(self) -> None:
        both = sorted(set(self.live) & set(self.test))
        if both:
            raise ConfigError(f"Тикеры скоупа Binance Test нельзя исполнять в live (только Shadow): {both}")
        pinned_test = sorted(set(self.pinned) & set(self.test))
        if pinned_test:
            raise ConfigError(f"Закреплённые монеты не могут быть в Binance Test: {pinned_test}")

    # ------------------------------------------------------------------------------------------ запросы

    def tracked(self, symbol: str) -> bool:
        """Можно ли считать стратегии по символу (новые сигналы): MT5, live-уровень или Binance Test. Прочее (в т.ч. упавшее ниже 100M) - нет."""
        return symbol in self.mt5 or symbol in self.live or symbol in self.test

    def tracked_crypto(self) -> tuple[str, ...]:
        return (*self.live, *self.test)

    # ------------------------------------------------------------------------------------------ обновление

    def apply(self, tiers: Tiers) -> Diff:
        """Применить уровни (все символы уже подписаны). Наборы меняются на месте; нарушение «test ∩ live = ∅» невозможно по построению и проверяется (R1)."""
        unsubscribed = [s for s in tiers.all() if s not in self.subscribed]
        if unsubscribed:
            raise ConfigError(f"Монеты без потоков нельзя включить в уровни: {unsubscribed}")
        old_live, old_test = set(self.live), set(self.test)
        new_live, new_test = set(tiers.live), set(tiers.test)
        diff = Diff(
            promoted=tuple(s for s in tiers.live if s not in old_live),
            demoted=tuple(sorted(old_live - new_live)),  # включая упавших ниже 100M: для них одновременно верно и `dropped`
            added_test=tuple(s for s in tiers.test if s not in old_test and s not in old_live),
            dropped=tuple(sorted((old_live | old_test) - new_live - new_test)),
        )
        self.test.replace(tiers.test)
        self.live.replace(tiers.live)
        self._check_disjoint()
        self._volumes = dict(tiers.volumes)
        return diff

    # ------------------------------------------------------------------------------------------ диск

    def snapshot(self) -> dict[str, Any]:
        return {"version": STATE_VERSION, "updated_ms": self._clock(), "pinned": list(self.pinned), "live": list(self.live), "test": list(self.test),
                "mt5": list(self.mt5), "subscribed": sorted(self.subscribed),
                "volume_musd": {s: round(v / 1e6, 1) for s, v in self._volumes.items()}}

    def save(self) -> None:
        if self._path is not None:
            atomic_write_text(self._path, json.dumps(self.snapshot(), ensure_ascii=False, indent=1) + "\n")

    def load_saved(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """(live, test) из снимка прошлого запуска; нет файла - пусто, битый - DataError. Применять к наборам нельзя (потоков ещё нет): нужно для фильтров и сверки live."""
        if self._path is None:
            return (), ()
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return (), ()
        try:
            data = json.loads(raw)
            if data["version"] != STATE_VERSION:
                raise ValueError(f"версия {data['version']!r}")
            live, test = tuple(data["live"]), tuple(data["test"])
            if not all(isinstance(s, str) and _symbol_ok(s) for s in (*live, *test)):
                raise ValueError("некорректный тикер")
        except (ValueError, KeyError, TypeError) as exc:
            raise DataError(f"Снимок автоселектора {self._path} повреждён ({exc}); начало: {raw[:200]!r}") from exc
        return live, test


class GroupConnector:
    """Подключение группы монет к работающим фидам (используется как `Hooks.add_group`). Порядок важен: состояние старших таймфреймов (1h/4h/1d) -> ожидание, пока
    ShadowRun занесёт его в `MtfState` (`warmed()`) и ещё один цикл опроса -> рабочие свечи и тики. Рабочая свеча новой монеты не может прийти раньше её bias (иначе
    `MtfState.attach` упал бы с DataError). Существующие соединения не затрагиваются: у группы свои WebSocket."""

    def __init__(self, *, make_feeds: Callable[[tuple[str, ...]], tuple[list[Any], list[Any]]], work: Any, state: Any | None, ticks: Any,
                 make_tick_feed: Callable[[tuple[str, ...]], Any], warmup_timeout_s: float = 600.0, settle_s: float = 2.0,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self._make_feeds, self._work, self._state, self._ticks, self._make_tick_feed = make_feeds, work, state, ticks, make_tick_feed
        self._timeout, self._settle, self._clock, self._sleep = warmup_timeout_s, settle_s, clock, sleep

    async def __call__(self, batch: tuple[str, ...]) -> None:
        work_g, state_g = self._make_feeds(batch)
        if state_g and self._state is None:
            raise ConfigError("GroupConnector: у группы есть фиды старших таймфреймов, а набор состояния не задан")
        for f in state_g:
            self._state.add(f)
        deadline = self._clock() + self._timeout
        while not all(f.warmed() for f in state_g):
            if self._clock() > deadline:
                raise DataError(f"Монеты {batch}: история старших таймфреймов не загрузилась за {self._timeout:.0f} с")
            await self._sleep(1.0)
        await self._sleep(self._settle)  # состояние последнего опроса точно занесено в MtfState до рабочих свечей
        for f in work_g:
            self._work.add(f)
        self._ticks.add(self._make_tick_feed(batch))
        log.info("Подключена группа монет (%d): %s", len(batch), ",".join(batch))


@dataclass
class Hooks:
    """Что менеджер умеет делать в процессе; реализует shadow_run (в тестах - подделки)."""

    add_group: Callable[[tuple[str, ...]], Awaitable[None]]  # поднять потоки новых монет и дождаться готовности
    prepare_live: Callable[[Sequence[str]], Awaitable[None]] | None = None  # фильтры биржи для монет, входящих в live (есть только в TRADE_MODE=live)
    add_book: Callable[[tuple[str, ...]], None] | None = None  # стакан для монет live-уровня (OBI_Breakout)
    open_symbols: Callable[[], Collection[str]] | None = None  # монеты с открытой боевой сделкой: потоки нужны, даже если монета вышла из уровней (Close-Only)


class UniverseManager:
    def __init__(self, api: Any, universe: Universe, hooks: Hooks, *, refresh_s: float = config.UNIVERSE_REFRESH_S, retry_s: float = config.UNIVERSE_RETRY_S,
                 max_failures: int = config.UNIVERSE_FETCH_FAILURES_MAX, min_listing_days: int = config.UNIVERSE_MIN_LISTING_DAYS):
        self._api, self.universe, self._hooks = api, universe, hooks
        self._refresh_s, self._retry_s, self._max_failures, self._min_days = refresh_s, retry_s, max_failures, min_listing_days
        self._task: asyncio.Task | None = None
        self.refreshes = 0
        self.last_diff: Diff | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="universe_manager")

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def check(self) -> None:
        """Упавшая задача менеджера роняет процесс (R1), как и задачи фидов."""
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise DataError("Задача автоселектора монет отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача автоселектора монет завершилась без ошибки: так быть не должно")

    async def run(self) -> None:
        failures = 0
        while True:
            try:
                await self.refresh()
                failures, delay = 0, self._refresh_s
            except (NetworkError, HttpServerError) as exc:  # ожидаемый временный сбой биржи; бан/лимит (RateLimitError) и мусорные ответы сюда не попадают
                failures += 1
                if failures >= self._max_failures:
                    raise
                log.warning("Автоселектор: биржа недоступна (%s: %s), попытка %d/%d, повтор через %.0f с; прежний состав сохранён",
                            type(exc).__name__, exc, failures, self._max_failures, self._retry_s)
                delay = self._retry_s
            await asyncio.sleep(delay)

    async def fetch_tiers(self) -> Tiers:
        info = await self._api.get(EXCHANGE_INFO_PATH, None, weight=1)
        eligible = parse_listings(info, min_listing_days=self._min_days)
        volumes = parse_volumes(await self._api.get(TICKER_24H_PATH, None, weight=40))
        return classify(volumes, eligible)

    async def refresh(self) -> Diff:
        tiers = await self.fetch_tiers()
        u = self.universe
        new = [s for s in tiers.all() if s not in u.subscribed]
        if self._hooks.open_symbols is not None:  # позиция пережила рестарт, а монета уже вне уровней: тики и свечи нужны до закрытия (новых сигналов по ней не будет)
            new += [s for s in sorted(self._hooks.open_symbols()) if s not in u.subscribed and s not in new and s not in config.LIVE_MT5_SYMBOLS]
        for i in range(0, len(new), GROUP_BATCH):
            batch = tuple(new[i:i + GROUP_BATCH])
            await self._hooks.add_group(batch)
            u.subscribed.update(batch)
        promote = [s for s in tiers.live if s not in u.live]
        if promote and self._hooks.prepare_live is not None:
            await self._hooks.prepare_live(promote)  # фильтры биржи загружены ДО того, как монета станет live: сигнал не придёт к менеджеру риска без них
        book = tuple(s for s in tiers.live if s not in u.book and s not in config.LIVE_MT5_SYMBOLS)
        if book and self._hooks.add_book is not None:
            self._hooks.add_book(book)
            u.book.update(book)
        diff = u.apply(tiers)
        u.save()
        self.refreshes += 1
        self.last_diff = diff
        log.info("Автоселектор #%d: live %d (%s), Binance Test %d, новые потоки %d%s", self.refreshes, len(tiers.live), ",".join(tiers.live[:12]) + ("…" if len(tiers.live) > 12 else ""),
                 len(tiers.test), len(new), "" if diff.empty() else
                 f"; в live {list(diff.promoted)}, Close-Only (новые входы закрыты, открытые ведутся) {list(diff.demoted)}, новые в Test {list(diff.added_test)}, "
                 f"не отслеживаются {list(diff.dropped)}")
        return diff
