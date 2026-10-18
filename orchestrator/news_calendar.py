"""Календарь новостей ForexFactory и фильтр выходных для НОВЫХ сигналов (Блок 4, 2026-10-18).

Новости. Раз в час (`NEWS_REFRESH_S`) скачивается недельный календарь ForexFactory в JSON (`NEWS_CALENDAR_URL`, публичная выгрузка
faireconomy.media: title, country, date с часовым поясом, impact). Берутся только события `impact == "High"`. Новый сигнал по символу, валюта
которого затронута событием, пропускается за 15 минут ДО и 15 минут ПОСЛЕ события (`skipped_filter`, причина `news`): спред, проскальзывание
и пилы на релизе делают сигналы стратегий шумом. Валюты символа: крипта (`...USDT`) и металлы (`XAU/XAG...`) - USD; валютная пара - обе валюты.

Сеть. Жёсткий таймаут запроса 2 с (`NEWS_TIMEOUT_S`). Любая ошибка сети, HTTP или формата - `NewsFetchError` сразу, без повторов внутри запроса;
фоновая задача ловит ИМЕННО этот тип (не «любое исключение»), пишет предупреждение и работает дальше с последним удачным календарём: бот не ждёт
ForexFactory и не падает из-за него (R1 допускает осознанную обработку ожидаемого внешнего отказа). Ошибки кода в задаче - падение процесса.
Последний удачный календарь лежит на диске (`DATA_DIR/.news_calendar.json`): после рестарта фильтр работает сразу, а ForexFactory, который
ограничивает частоту запросов, не дёргается лишний раз. Календарь ещё ни разу не загружен или старше `NEWS_MAX_AGE_S` - фильтр новостей
НЕ блокирует сигналы (fail-open; иначе недоступный сайт новостей остановил бы торговлю) и раз в час пишет об этом предупреждение.

Выходные. С 00:00 UTC субботы до 23:59:59 UTC воскресенья новые сигналы не принимаются ни по одному символу (`weekend`). Это фильтр сигналов
Оркестратора: сопровождение и закрытие открытых сделок (тики, свечи, стопы, уровни, тайм-ауты) работает в обычном режиме.
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

import config
from orchestrator.types import Signal
from utils.errors import TradingInfraError
from utils.storage import atomic_write_text

log = logging.getLogger("orchestrator.news")

DAY_MS = 86_400_000


class NewsFetchError(TradingInfraError):
    """Календарь не получен (сеть, таймаут, HTTP, формат): бот продолжает работу с прежним календарём."""


@dataclass(frozen=True)
class NewsEvent:
    ts_ms: int
    currency: str
    title: str


def parse_calendar(raw: Any) -> list[NewsEvent]:
    """JSON ForexFactory -> события High Impact. Кривая запись - NewsFetchError (весь ответ отвергается: частичный календарь хуже прежнего)."""
    if not isinstance(raw, list):
        raise NewsFetchError(f"календарь: ожидается список, получено {str(raw)[:200]}")
    out: list[NewsEvent] = []
    for row in raw:
        if not isinstance(row, dict):
            raise NewsFetchError(f"календарь: запись не объект: {str(row)[:200]}")
        if str(row.get("impact", "")).strip().lower() != "high":
            continue
        try:
            dt = datetime.fromisoformat(str(row["date"]))
        except (KeyError, ValueError) as exc:
            raise NewsFetchError(f"календарь: нет даты или она не ISO: {str(row)[:200]}") from exc
        if dt.tzinfo is None:
            raise NewsFetchError(f"календарь: дата без часового пояса: {row['date']!r}")
        currency = str(row.get("country", "")).strip().upper()
        if not currency:
            raise NewsFetchError(f"календарь: нет валюты (country): {str(row)[:200]}")
        out.append(NewsEvent(int(dt.timestamp() * 1000), currency, str(row.get("title", "")).strip()))
    return sorted(out, key=lambda e: e.ts_ms)


def symbol_currencies(symbol: str) -> frozenset[str]:
    """Валюты, чьи новости двигают символ: крипта (`...USDT`, `...USDC`) и металлы - USD; валютная пара из 6 букв - обе валюты."""
    s = symbol.upper()
    if s.endswith(("USDT", "USDC")) or s.startswith(("XAU", "XAG", "XPT", "XPD")):
        return frozenset({"USD"})
    if len(s) == 6 and s.isalpha():
        return frozenset({s[:3], s[3:]})
    return frozenset({"USD"})


async def fetch_calendar(session: aiohttp.ClientSession, url: str, timeout_s: float) -> list[NewsEvent]:
    """Один запрос с жёстким таймаутом. Любой сбой - NewsFetchError немедленно (повтор - через час, в фоновой задаче).
    Таймаут двойной: `ClientTimeout(total)` у aiohttp и `asyncio.wait_for` на весь запрос с чтением тела - зависший сервер не держит дольше `timeout_s`."""

    async def get() -> Any:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                raise NewsFetchError(f"календарь: HTTP {resp.status}")
            return await resp.json(content_type=None)

    try:
        raw = await asyncio.wait_for(get(), timeout_s)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        raise NewsFetchError(f"календарь не получен за {timeout_s} с: {type(exc).__name__}: {exc}") from exc
    return parse_calendar(raw)


class NewsCalendar:
    """События High Impact + фоновое обновление. `check(signal, now_ms)` - фильтр сигналов Оркестратора (`signal_gates`)."""

    name = "news"

    def __init__(self, fetch: Callable[[], Awaitable[list[NewsEvent]]], *, cache_path: Path | None = None,
                 refresh_s: float = config.NEWS_REFRESH_S, before_ms: int = config.NEWS_BLACKOUT_BEFORE_MS, after_ms: int = config.NEWS_BLACKOUT_AFTER_MS,
                 max_age_ms: int = config.NEWS_MAX_AGE_MS, wall_ms: Callable[[], int] = lambda: int(time.time() * 1000),
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self._fetch, self._cache = fetch, cache_path
        self._refresh_s, self._before, self._after, self._max_age = refresh_s, before_ms, after_ms, max_age_ms
        self._wall, self._sleep = wall_ms, sleep
        self.events: list[NewsEvent] = []
        self.loaded_ms: int | None = None
        self.failures = 0
        self._warned_ms = 0
        self._task: asyncio.Task | None = None
        self._load_cache()

    # ------------------------------------------------------------------------------------- фильтр

    def check(self, signal: Signal, now_ms: int) -> str | None:
        if self.loaded_ms is None or now_ms - self.loaded_ms > self._max_age:
            if now_ms - self._warned_ms >= 3_600_000:
                self._warned_ms = now_ms
                log.warning("Новости: календарь %s - фильтр новостей не блокирует сигналы",
                            "ещё не загружен" if self.loaded_ms is None else f"устарел ({(now_ms - self.loaded_ms) / 3_600_000:.1f} ч)")
            return None
        wanted = symbol_currencies(signal.symbol)
        for e in self.events:
            if e.ts_ms - self._before <= now_ms <= e.ts_ms + self._after and e.currency in wanted:
                return f"news: {e.currency} {e.title}"
        return None

    # ---------------------------------------------------------------------------------- обновление

    async def refresh(self) -> None:
        """Скачать календарь; NewsFetchError поднимается наверх (решает вызывающий: фоновая задача - лог и работа дальше)."""
        events = await self._fetch()
        self.events, self.loaded_ms, self.failures = events, self._wall(), 0
        if self._cache is not None:
            atomic_write_text(self._cache, json.dumps({"loaded_ms": self.loaded_ms, "events": [[e.ts_ms, e.currency, e.title] for e in events]},
                                                      ensure_ascii=False))
        log.info("Новости: календарь обновлён, событий High Impact %d", len(events))

    async def run(self) -> None:
        while True:
            try:
                await self.refresh()
            except NewsFetchError as exc:
                self.failures += 1
                log.warning("Новости: %s (сбоев подряд %d); работаю с прежним календарём, следующая попытка через %.0f с", exc, self.failures, self._refresh_s)
            await self._sleep(self._refresh_s)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="news_calendar")

    def check_task(self) -> None:
        """Упавшая фоновая задача (ошибка кода, не сети) роняет процесс (R1)."""
        t = self._task
        if t is not None and t.done():
            if t.cancelled():
                raise TradingInfraError("Задача календаря новостей отменена")
            exc = t.exception()
            raise exc if exc is not None else TradingInfraError("Задача календаря новостей завершилась без ошибки: так быть не должно")

    async def aclose(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _load_cache(self) -> None:
        if self._cache is None:
            return
        try:
            raw = self._cache.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        try:
            data = json.loads(raw)
            events = [NewsEvent(int(t), str(c), str(title)) for t, c, title in data["events"]]
            loaded = int(data["loaded_ms"])
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("Новости: кеш календаря %s не читается (%s) - жду загрузку", self._cache, exc)
            return
        self.events, self.loaded_ms = events, loaded


def is_weekend(ts_ms: int) -> bool:
    """Суббота 00:00 UTC ... воскресенье 23:59:59.999 UTC."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).weekday() >= 5


class WeekendGate:
    """Запрет НОВЫХ сигналов на выходных (UTC). Открытые сделки сопровождаются как обычно: фильтр стоит только на пути нового сигнала."""

    name = "weekend"

    def check(self, signal: Signal, now_ms: int) -> str | None:
        return "weekend" if is_weekend(now_ms) else None
