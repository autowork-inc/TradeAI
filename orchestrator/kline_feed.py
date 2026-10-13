"""SmcKlineFeed: закрытые свечи 15m/1h по WebSocket -> FeatureContext для Оркестратора (R5: только закрытые, k.x = true).

Источник: `<symbol>@kline_<interval>` на маршруте config.WS_MARKET_URL (/market/stream; проверено на mainnet 2026-10-08,
в /public свечи не приходят). История для признаков - REST /fapi/v1/klines (history_bars + формирующаяся) на старте и
после КАЖДОГО (пере)подключения: сначала подписка, потом REST, чтобы между историей и потоком не было дыры.

Контракт с Оркестратором: `poll()` отдаёт по FeatureContext на КАЖДУЮ закрывшуюся свечу строго по порядку и без
пропусков. Свечи, закрывшиеся за время обрыва, отдаются вдогонку (replay) с received_ms = «сейчас»: Оркестратор видит
их возраст, не торгует по ним (skipped_stale), но трекер получает закрытие и считает сроки заявок верно.
Дыра в сетке, которую даже REST не закрыл, - DataError (R1), а не молчаливая склейка.

Часы: событие закрытия и «возраст» свечи меряются по часам биржи (смещение берётся из /fapi/v1/time на каждом
подключении): на этом хосте локальные часы уходят от биржевых на минуту.
"""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import aiohttp
import polars as pl

import config
from execution.live_features import parse_klines
from orchestrator.feature_engine import FeatureEngine
from orchestrator.types import FeatureContext
from orchestrator.ws import ReconnectingStream, decode_combined, wall_clock_ms
from pipeline.loaders.schemas import KLINE_SCHEMA
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import ConfigError, DataError, WebSocketClosed

log = logging.getLogger("orchestrator.kline_feed")


class SmcKlineFeed:
    def __init__(
        self, api: BinanceRest | None, session: aiohttp.ClientSession | None, engine: FeatureEngine, *,
        symbols: tuple[str, ...] | None = None, interval: str = "15m", history_bars: int = 300,
        clock_ms: Callable[[], int] = wall_clock_ms, market_url: str | None = None, max_reconnects: int | None = None,
        backoff_s: float | None = None, backoff_max_s: float | None = None, emit_latest_on_start: bool = False,
    ):
        if interval not in config.INTERVAL_MS:
            raise ConfigError(f"Неизвестный таймфрейм {interval!r}")
        if history_bars < engine.min_bars:
            raise ConfigError(f"history_bars={history_bars} меньше min_bars={engine.min_bars}")
        self._api, self._session, self._engine = api, session, engine
        self._symbols = tuple(symbols if symbols is not None else config.SYMBOLS)
        self.interval, self._interval_ms, self._history = interval, config.INTERVAL_MS[interval], history_bars
        self._clock = clock_ms
        self._emit_latest = emit_latest_on_start  # H4-контекст нужен сразу после старта (история = состояние), а не через 4 часа
        self._url = f"{market_url or config.WS_MARKET_URL}?streams=" + "/".join(f"{s.lower()}@kline_{interval}" for s in self._symbols)
        self._expected = {f"{s.lower()}@kline_{interval}": s for s in self._symbols}
        self._store: dict[str, dict[int, tuple]] = {s: {} for s in self._symbols}  # open_time -> строка KLINE_SCHEMA
        self._last_emitted: dict[str, int | None] = {s: None for s in self._symbols}
        self._recv: dict[tuple[str, int], int] = {}  # (символ, open_time) -> время получения закрытия, часы биржи
        self._offset_ms = 0
        self._stream = ReconnectingStream(
            f"kline_{interval}", self._url, session, self._on_frame, on_connected=self._bootstrap, clock_ms=clock_ms,  # type: ignore[arg-type]
            max_reconnects=max_reconnects, backoff_s=backoff_s, backoff_max_s=backoff_max_s,
        )
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------------------ жизненный цикл

    def start(self) -> None:
        if self._task is None:
            if self._api is None or self._session is None:
                raise ConfigError("SmcKlineFeed: для запуска потока нужны api и session")
            self._task = asyncio.create_task(self._stream.run(), name="smc_kline_feed")

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def exchange_now_ms(self) -> int:
        return self._clock() + self._offset_ms

    def stats(self) -> dict[str, Any]:
        return {"received": self._stream.received, "reconnects": self._stream.reconnects,
                "candles": {s: len(c) for s, c in self._store.items()}, "last_emitted": dict(self._last_emitted)}

    # ----------------------------------------------------------------------------------- REST / WS

    async def _bootstrap(self) -> None:
        server_ms = await self._api.server_time_ms()
        self._offset_ms = server_ms - self._clock()
        limit = self._history + 1  # + формирующаяся свеча: parse_klines её отбросит
        for symbol in self._symbols:
            raw = await self._api.get(
                config.KLINES_PATH, {"symbol": symbol, "interval": self.interval, "limit": limit}, weight=klines_weight(limit),
            )
            self._store_frame(symbol, parse_klines(raw, symbol, server_ms))
            if self._last_emitted[symbol] is None:
                newest = max(self._store[symbol])
                # история на старте не эмитится; с emit_latest_on_start отдаётся одна последняя закрытая свеча (контекст с историей)
                self._last_emitted[symbol] = newest - self._interval_ms if self._emit_latest else newest
            else:
                missed = [t for t in self._store[symbol] if t > self._last_emitted[symbol]]
                if missed:
                    log.warning("%s: к отправке готово %d закрытых свечей (после обрыва - вдогонку, replay: торговать по устаревшим нельзя)", symbol, len(missed))
        log.info("История %s загружена: %s, смещение часов биржи %+d мс", self.interval, {s: len(c) for s, c in self._store.items()}, self._offset_ms)

    def _on_frame(self, raw: str, recv_ts: int) -> None:
        _, symbol, data = decode_combined(raw, self._expected)
        self.on_kline_message(symbol, data, recv_ts + self._offset_ms)

    def on_kline_message(self, symbol: str, data: dict[str, Any], recv_ms: int) -> bool:
        """Сообщение @kline. True - принята закрытая свеча. recv_ms - время получения по часам биржи."""
        k = data.get("k")
        if not isinstance(k, dict) or k.get("s") != symbol or k.get("i") != self.interval:
            raise DataError(f"Неожиданное сообщение свечи {symbol}: {str(data)[:200]}")
        if k.get("x") is not True:
            return False
        try:
            raw = [k["t"], k["o"], k["h"], k["l"], k["c"], k["v"], k["T"], k["q"], k["n"], k["V"], k["Q"]]
            frame = parse_klines([raw], symbol, int(k["T"]) + 1)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданная закрытая свеча {symbol}: {str(k)[:200]}") from exc
        self._store_frame(symbol, frame)
        self._recv.setdefault((symbol, int(k["t"])), recv_ms)
        return True

    def _store_frame(self, symbol: str, frame: pl.DataFrame) -> None:
        store = self._store[symbol]
        for row in frame.select(KLINE_SCHEMA.names()).iter_rows():  # загрузка данных биржи в буфер, не математика паттернов
            store[row[0]] = row
        for open_time in sorted(store)[: max(0, len(store) - self._history - 5)]:
            del store[open_time]

    # ------------------------------------------------------------------------------------------ poll

    async def poll(self) -> list[FeatureContext]:
        """Контексты по свечам, закрывшимся с прошлого вызова (по порядку, без пропусков)."""
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise WebSocketClosed("Задача фида свечей отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача фида свечей завершилась без ошибки: так быть не должно")
        out: list[FeatureContext] = []
        for symbol in self._symbols:
            last = self._last_emitted[symbol]
            if last is None:
                continue  # история ещё не загружена
            for open_time in sorted(t for t in self._store[symbol] if t > last):
                if open_time != last + self._interval_ms:
                    raise DataError(f"{symbol} {self.interval}: дыра в свечах между {last} и {open_time}")
                out.append(await self._context(symbol, open_time))
                last = open_time
            self._last_emitted[symbol] = last
        return out

    async def _context(self, symbol: str, open_time: int) -> FeatureContext:
        store = self._store[symbol]
        times = sorted(t for t in store if t <= open_time)[-self._history:]
        candles = pl.DataFrame([store[t] for t in times], schema=KLINE_SCHEMA, orient="row")
        received = self._recv.pop((symbol, open_time), None)
        if received is None:  # свеча пришла по REST (replay после обрыва): время получения - «сейчас»
            received = self.exchange_now_ms()
        return await asyncio.to_thread(self._engine.build, symbol, self.interval, self._interval_ms, candles, received)
