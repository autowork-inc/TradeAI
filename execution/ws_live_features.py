"""WsLiveFeatureFeed: боевой источник решений на WebSocket-потоках Binance. Ликвидация Train-Serve Skew.

Проблема REST-фида (execution/live_features.py): стакан снимался раз в LIVE_LOOP_INTERVAL_S (5 с), а обучающий
стакан - поток сообщений @depth10@500ms, записанный сборщиком (collectors/depth_stream.py). obi/spread/microprice
- функции последнего состояния и расхождения не имели, но ofi_sum суммирует приращения по сообщениям: при снимках
раз в 5 с он считался в другом масштабе, а gap_break_ms приходилось расширять. Модель получала признаки не из того
распределения, на котором училась.

Решение: фид слушает ТЕ ЖЕ потоки, что сборщик обучающих данных, и кладёт в буфер ТЕ ЖЕ сообщения:

    <symbol>@depth10@500ms   (маршрут config.WS_BASE_URL, /public)   -> строка depth_schema(levels) на сообщение
    <symbol>@kline_1m        (маршрут config.WS_MARKET_URL, /market) -> закрытая свеча (k.x = true) в KLINE_SCHEMA

Два маршрута, потому что /public/stream свечи не отдаёт: подписка на kline там молча игнорируется (проверено
вживую). Сообщение стакана нормализуется тем же parse_book_snapshot, что и снимок REST-фида, а он повторяет
правила DepthLoader (пустая сторона, цена <= 0, объём < 0, пересечённый стакан -> брак; повтор u -> дубль);
recv_ts - локальные часы в момент получения, как у сборщика (time.time_ns() // 1_000_000).

Конвейер признаков - канонический, весь код общий с REST-фидом и обучением (LiveFeatureFeed._row_from_candles):

    буфер -> DataQualityGate -> WatermarkClock -> TimelineAligner(decisions, attach_book)
          -> compile_features -> apply_features -> ПОСЛЕДНЯЯ строка

TimingConfig - ТРЕНИРОВОЧНЫЙ (gap_break_ms = 5 с и т.д.), а не расширенный default_live_timing(): при потоке раз в
500 мс доказательство закрытия свечи приходит через сотни миллисекунд, широкого допуска больше не нужно.
Решение по свече принимается, когда пришло событие её закрытия (kline x=true) и строка «созрела»
(asof_ts <= сейчас); poll() вызывается таймером боевого цикла, сеть он не трогает - только буферы.

Что остаётся REST: история свечей на старте и после каждого (пере)подключения потока свечей (один запрос на
символ + серверное время для проверки часов) - без неё признакам с длинным окном (120 баров) пришлось бы ждать
два часа. Свечи до начала буфера стакана принимаются как assume_pre_stream, как и в REST-фиде.

Прогрев после КАЖДОГО запуска не убрать: ofi_norm_15 требует 15 подряд идущих минутных корзин по >= 2 сообщения,
стакан задним числом получить нельзя (REST его не отдаёт) - ~15 минут признаки потока ордеров null, стратегия
в это время не торгует (штатно, как у REST-фида). Предикты по остальным признакам не отдаются: модель обучена на
полном наборе.

Отказоустойчивость. Каждый поток переподключается сам (backoff, LIVE_WS_MAX_RECONNECTS подряд неудач -> выход
исключением, его поднимает следующий poll). Дыра в стакане на время обрыва видна конвейеру (устаревший стакан,
разрыв сегмента): признаки в ней null, а не выдуманные. Мусорное сообщение - DataError и выход (fail-fast, как у
сборщика); 429/418 при подключении - RateLimitError (код 2, бан-замок ставит общий runner).
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable, Sequence
from typing import Any

import aiohttp
import polars as pl

import config
from collectors.depth_stream import connect
from execution.live_features import INTERVAL, LiveFeatureFeed, parse_book_snapshot, parse_klines
from pipeline.features.base import Feature
from pipeline.loaders.schemas import KLINE_SCHEMA
from pipeline.types import TimingConfig
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import (
    ConfigError,
    DataError,
    HttpServerError,
    NetworkError,
    ServerShutdown,
    StreamStalled,
    TradingInfraError,
    WebSocketClosed,
)

log = logging.getLogger("ws_live_features")

_RECONNECTABLE = (WebSocketClosed, NetworkError, HttpServerError, StreamStalled, ServerShutdown)
_PARTIAL_DEPTH_LEVELS = (5, 10, 20)  # допустимые N у @depth<N>@<speed> (partial book depth)


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def decode_frame(raw: str, expected: dict[str, str]) -> tuple[str, dict[str, Any]]:
    """Кадр комбинированного потока -> (символ, data). expected: имя потока -> символ."""
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise DataError(f"Сообщение не JSON: {raw[:200]!r}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"Сообщение не объект: {raw[:200]!r}")
    data = payload.get("data")
    if payload.get("e") == "serverShutdown" or (isinstance(data, dict) and data.get("e") == "serverShutdown"):
        raise ServerShutdown(f"Биржа сообщила об отключении: {raw[:200]}")
    stream = payload.get("stream")
    if stream not in expected or not isinstance(data, dict):
        raise DataError(f"Неожиданное сообщение (stream={stream!r}): {raw[:200]!r}")
    return expected[stream], data


class WsLiveFeatureFeed(LiveFeatureFeed):
    """DecisionFeed на WebSocket. См. докстринг модуля. Жизненный цикл: первый poll() запускает потоки, aclose() их гасит."""

    def __init__(
        self,
        api: BinanceRest,
        session: aiohttp.ClientSession | None,
        symbols: Sequence[str] | None = None,
        *,
        features: Sequence[Feature] | None = None,
        levels: int | None = None,
        timing: TimingConfig | None = None,
        extra_bars: int = 3,
        required_features: Sequence[str] = (),
        clock_ms: Callable[[], int] = _wall_clock_ms,
        depth_url: str | None = None,
        market_url: str | None = None,
        max_reconnects: int | None = None,
        backoff_s: float | None = None,
        backoff_max_s: float | None = None,
        autostart: bool = True,
    ):
        """api/session нужны только на старте потоков (конструктор сети не трогает: live_run проверяет модель
        заранее с api=None). timing по умолчанию - тренировочный TimingConfig(). clock_ms - локальные часы для
        recv_ts и проверки зрелости строки (подменяются в тестах); остальное - адреса и параметры переподключения.
        autostart=False - режим воспроизведения: poll() потоки не запускает, сообщения подаются вручную через
        on_depth_message / on_kline_message (паритет-тест с офлайн-конвейером).
        """
        super().__init__(
            api, symbols, features=features, levels=levels, timing=timing or TimingConfig(),
            extra_bars=extra_bars, required_features=required_features,
        )
        if self._levels not in _PARTIAL_DEPTH_LEVELS:
            raise ConfigError(f"Поток @depth<N> существует только для N из {_PARTIAL_DEPTH_LEVELS}, получено {self._levels}")
        self._session = session
        self._autostart = autostart
        self._clock_ms = clock_ms
        self._depth_url = depth_url or config.WS_BASE_URL
        self._market_url = market_url or config.WS_MARKET_URL
        self._max_reconnects = max_reconnects if max_reconnects is not None else config.LIVE_WS_MAX_RECONNECTS
        self._backoff = backoff_s if backoff_s is not None else config.LIVE_WS_BACKOFF_S
        self._backoff_max = backoff_max_s if backoff_max_s is not None else config.LIVE_WS_BACKOFF_MAX_S

        self._depth_streams = {
            f"{s.lower()}@depth{self._levels}@{config.DEPTH_UPDATE_SPEED}": s for s in self._symbols
        }
        self._kline_streams = {f"{s.lower()}@kline_{INTERVAL}": s for s in self._symbols}
        self._candles: dict[str, dict[int, tuple]] = {s: {} for s in self._symbols}  # open_time -> строка KLINE_SCHEMA
        self._candle_cap = self._history + 2
        self._closed_event: dict[str, tuple[int, int]] = {}  # символ -> (open_time, локальное recv_ts) закрытия по WS
        self._tasks: list[asyncio.Task] = []
        self._received = {"depth": 0, "market": 0}
        self._reconnects = {"depth": 0, "market": 0}
        self._counts = {"depth_ok": 0, "depth_rejected": 0, "depth_duplicate": 0, "klines_closed": 0}
        self.clock_offset_ms: int | None = None  # серверное время - локальное на последнем старте (диагностика)

    # ---------------------------------------------------------------------------- жизненный цикл

    def _stream_url(self, base: str, names: Sequence[str]) -> str:
        return f"{base}?streams={'/'.join(names)}"

    def start(self) -> None:
        """Запускает читатели потоков (идемпотентно). Нужен работающий цикл событий."""
        if self._tasks:
            return
        if self._session is None or self._api is None:
            raise ConfigError("WsLiveFeatureFeed: для запуска потоков нужны api и session")
        self._tasks = [
            asyncio.create_task(
                self._stream_loop(
                    "depth", self._stream_url(self._depth_url, list(self._depth_streams)), self._depth_streams,
                    self._on_depth_frame,
                ),
                name="ws_feed.depth",
            ),
            asyncio.create_task(
                self._stream_loop(
                    "market", self._stream_url(self._market_url, list(self._kline_streams)), self._kline_streams,
                    self._on_market_frame, on_connected=self._bootstrap_candles,
                ),
                name="ws_feed.market",
            ),
        ]

    async def aclose(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    def stats(self) -> dict[str, Any]:
        return {
            **self._counts, "received": dict(self._received), "reconnects": dict(self._reconnects),
            "book_buffer": {s: len(b) for s, b in self._books.items()},
            "candles": {s: len(c) for s, c in self._candles.items()},
        }

    def best_quote(self, symbol: str, max_age_ms: int | None = None) -> tuple[float, float] | None:
        """(best bid, best ask) по последнему принятому сообщению стакана или None: сообщений нет либо оно старше
        max_age_ms (по локальным часам, как recv_ts). Нужен Smart Execution: лимитка ставится по ЖИВОЙ котировке,
        а при обрыве потока стакан устаревает - по устаревшей цене пассивный ордер ставить нельзя."""
        buf = self._books.get(symbol)
        if not buf:
            return None
        last = buf[-1]
        limit = config.LIVE_QUOTE_MAX_AGE_MS if max_age_ms is None else max_age_ms
        if self._clock_ms() - last["recv_ts"] > limit:
            return None
        bid, ask = last["bid_px_0"], last["ask_px_0"]
        return (bid, ask) if bid is not None and ask is not None else None

    def _raise_if_failed(self) -> None:
        for t in self._tasks:
            if t.done():
                if t.cancelled():
                    raise WebSocketClosed(f"Задача {t.get_name()} отменена")
                exc = t.exception()
                if exc is not None:
                    raise exc
                raise TradingInfraError(f"Задача {t.get_name()} завершилась без ошибки: так быть не должно")

    # ----------------------------------------------------------------------------- читатели

    async def _stream_loop(
        self,
        label: str,
        url: str,
        expected: dict[str, str],
        handler: Callable[[str, int, dict[str, str]], None],
        *,
        on_connected: Callable[[], Any] | None = None,
    ) -> None:
        failures = 0
        while True:
            before = self._received[label]
            try:
                assert self._session is not None
                ws = await connect(self._session, url)
                try:
                    log.info("WS %s подключён: %s", label, url)
                    if on_connected is not None:
                        await on_connected()
                    await self._consume(label, ws, handler, expected)
                finally:
                    await ws.close()
            except _RECONNECTABLE as exc:
                if self._received[label] > before:
                    failures = 0  # соединение жило и отдавало данные: это новый обрыв, а не продолжение серии
                failures += 1
                if failures > self._max_reconnects:
                    raise WebSocketClosed(
                        f"Поток {label} не восстановился за {self._max_reconnects} попыток: {exc}"
                    ) from exc
                delay = min(self._backoff_max, self._backoff * 2 ** (failures - 1))
                log.warning("WS %s: %s: %s; переподключение %d/%d через %.1f с (в буфере дыра: признаки на ней null)",
                            label, type(exc).__name__, exc, failures, self._max_reconnects, delay)
                self._reconnects[label] += 1
                await asyncio.sleep(delay)

    async def _consume(
        self, label: str, ws: aiohttp.ClientWebSocketResponse,
        handler: Callable[[str, int, dict[str, str]], None], expected: dict[str, str],
    ) -> None:
        while True:
            try:
                msg = await ws.receive(timeout=config.WS_STALL_TIMEOUT_S)
            except asyncio.TimeoutError as exc:
                raise StreamStalled(f"Нет данных {config.WS_STALL_TIMEOUT_S} с при открытом соединении") from exc
            recv_ts = self._clock_ms()
            if msg.type == aiohttp.WSMsgType.TEXT:
                handler(msg.data, recv_ts, expected)
                self._received[label] += 1
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                raise WebSocketClosed(f"Соединение {label} закрыто (код {ws.close_code}), принято {self._received[label]}")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise NetworkError(f"Ошибка WebSocket {label}: {ws.exception()!r}")
            else:
                raise DataError(f"Неожиданный тип кадра WebSocket {label}: {msg.type!r}")

    # ------------------------------------------------------------------------------ стакан

    def _on_depth_frame(self, raw: str, recv_ts: int, expected: dict[str, str]) -> None:
        symbol, data = decode_frame(raw, expected)
        self.on_depth_message(symbol, data, recv_ts)

    def on_depth_message(self, symbol: str, data: dict[str, Any], recv_ts: int) -> bool:
        """Сообщение @depth (поле data кадра, как его пишет сборщик) -> буфер символа. True - принято.

        Структурный мусор - DataError; брак по смыслу и повтор u - отбрасываются со счётчиком (как карантин
        и duplicate_u у DepthLoader).
        """
        missing = [k for k in ("s", "E", "u", "b", "a") if k not in data]
        if missing or not isinstance(data["b"], list) or not isinstance(data["a"], list):
            raise DataError(f"В сообщении стакана {symbol} нет полей {missing} или b/a не списки: {str(data)[:200]}")
        if data["s"] != symbol:
            raise DataError(f"Символ {data['s']!r} не соответствует потоку {symbol}")
        unreadable = False
        for side in (data["b"], data["a"]):
            for level in side[: self._levels]:
                if not isinstance(level, (list, tuple)) or len(level) < 2:
                    raise DataError(f"Уровень стакана {symbol} не пара [цена, объём]: {str(level)[:100]}")
                try:
                    float(level[0]), float(level[1])
                except (TypeError, ValueError):
                    unreadable = True  # DepthLoader: нечисловой уровень -> карантин bad_level, не фатальная ошибка
        row = None if unreadable else parse_book_snapshot(
            {"bids": data["b"], "asks": data["a"], "E": data["E"], "T": data.get("T"), "lastUpdateId": data["u"]},
            symbol, recv_ts, self._levels,
        )
        if row is None:
            self._counts["depth_rejected"] += 1
            if self._counts["depth_rejected"] in (1, 10, 100) or self._counts["depth_rejected"] % 1000 == 0:
                log.warning("%s: сообщение стакана отбраковано (пусто/пересечено/неверные уровни), всего %d",
                            symbol, self._counts["depth_rejected"])
            return False
        buf = self._books[symbol]
        if buf and row["u"] <= buf[-1]["u"]:
            self._counts["depth_duplicate"] += 1  # повтор u: DepthLoader оставляет первое по recv_ts
            return False
        pu = data.get("pu")
        row["pu"] = pu if isinstance(pu, int) and not isinstance(pu, bool) else None
        if not buf:
            log.info("%s: первое сообщение стакана (E=%d), буфер копится", symbol, row["E"])
        buf.append(row)
        self._counts["depth_ok"] += 1
        while buf and buf[0]["recv_ts"] < recv_ts - self._retention_ms:
            buf.popleft()
        return True

    # ------------------------------------------------------------------------------ свечи

    def _on_market_frame(self, raw: str, recv_ts: int, expected: dict[str, str]) -> None:
        symbol, data = decode_frame(raw, expected)
        self.on_kline_message(symbol, data, recv_ts)

    def on_kline_message(self, symbol: str, data: dict[str, Any], recv_ts: int | None = None) -> bool:
        """Сообщение @kline_1m. Закрытая свеча (k.x) -> буфер свечей и триггер решения; формирующаяся игнорируется.

        recv_ts - локальное время получения (по умолчанию - сейчас). True - свеча принята."""
        k = data.get("k")
        if not isinstance(k, dict) or k.get("s") != symbol or k.get("i") != INTERVAL:
            raise DataError(f"Неожиданное сообщение свечи {symbol}: {str(data)[:200]}")
        if k.get("x") is not True:
            return False
        try:
            raw = [k["t"], k["o"], k["h"], k["l"], k["c"], k["v"], k["T"], k["q"], k["n"], k["V"], k["Q"]]
            frame = parse_klines([raw], symbol, int(k["T"]) + 1)  # те же проверки, что у REST-свечей
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданная закрытая свеча {symbol}: {str(k)[:200]}") from exc
        self._store_candles(symbol, frame)
        self._counts["klines_closed"] += 1
        open_time = int(k["t"])
        previous = self._closed_event.get(symbol)
        if previous is not None and previous[0] < open_time and self._done.get(symbol, -1) < previous[0]:
            log.warning("%s: решение по свече open_time=%d пропущено (строка не созрела до закрытия следующей)",
                        symbol, previous[0])
        if previous is None or open_time > previous[0]:
            self._closed_event[symbol] = (open_time, self._clock_ms() if recv_ts is None else recv_ts)
        return True

    def _store_candles(self, symbol: str, frame: pl.DataFrame) -> None:
        store = self._candles[symbol]
        for row in frame.select(KLINE_SCHEMA.names()).iter_rows():
            store[row[0]] = row  # та же свеча из REST и из потока - одна запись (по open_time)
        for open_time in sorted(store)[: max(0, len(store) - self._candle_cap)]:
            del store[open_time]

    def _candle_frame(self, symbol: str) -> pl.DataFrame:
        store = self._candles[symbol]
        rows = [store[t] for t in sorted(store)]
        return pl.DataFrame(rows, schema=KLINE_SCHEMA, orient="row").select(
            pl.lit(symbol).alias("symbol"), *KLINE_SCHEMA.names()
        )

    async def _bootstrap_candles(self) -> None:
        """История закрытых свечей по REST: на старте и после каждого переподключения потока свечей."""
        server_ms = await self._api.server_time_ms()  # границу «закрыта» берём по часам биржи, не хоста
        self.clock_offset_ms = server_ms - self._clock_ms()
        if abs(self.clock_offset_ms) > config.LIVE_WS_CLOCK_WARN_MS:
            log.warning("Часы хоста расходятся с биржей на %d мс: recv_ts (локальные часы, как у сборщика) и "
                        "серверные E будут сдвинуты; DataQualityGate терпит постоянный сдвиг, но не скачки",
                        self.clock_offset_ms)
        limit = self._history + 1  # + формирующаяся свеча, её parse_klines отбросит
        for symbol in self._symbols:
            raw = await self._api.get(
                config.KLINES_PATH, {"symbol": symbol, "interval": INTERVAL, "limit": limit},
                weight=klines_weight(limit),
            )
            self._store_candles(symbol, parse_klines(raw, symbol, server_ms))
        log.info("История свечей загружена по REST: %s", {s: len(c) for s, c in self._candles.items()})

    # ------------------------------------------------------------------------------- poll

    async def poll(self, now_ms: int) -> list[dict[str, Any]]:
        """Строки признаков по свечам, закрытым с прошлого вызова. now_ms (часы бота, синхронизированные с биржей)
        не используется: recv_ts - локальные часы, как у сборщика, зрелость строки меряется по ним же."""
        self._raise_if_failed()
        if self._autostart:
            self.start()
        local_now = self._clock_ms()
        rows = []
        for symbol in self._symbols:
            row = await self._row_ws(symbol, local_now)
            if row is not None:
                rows.append(row)
        return rows

    async def _row_ws(self, symbol: str, now_ms: int) -> dict[str, Any] | None:
        """Решение запускает СОБЫТИЕ закрытия свечи по потоку, а не часы хоста: часы могут идти иначе, чем у биржи
        (на testnet замерено расхождение в минуту), и «ожидаемая свеча по локальной минуте» оказалась бы чужой."""
        event = self._closed_event.get(symbol)
        if event is None:
            return None  # свеча по потоку ещё не закрывалась (история с REST - только для прогрева признаков)
        expected_open, seen_ts = event
        if self._done.get(symbol, -1) >= expected_open:
            return None
        t = self._timing
        if now_ms < seen_ts + t.candle_publish_lag_ms + t.decision_compute_ms:
            return None  # asof_ts не может быть раньше: доказательство закрытия пришло не раньше самой свечи
        candles, messages = self._candle_frame(symbol), list(self._books[symbol])  # снимок в потоке цикла событий
        # Конвейер тяжёлый (десятки мс): в потоке, чтобы не задерживать pong'и и приём сообщений.
        row = await asyncio.to_thread(self._row_from_candles, symbol, candles, messages, now_ms, expected_open)
        if row is not None:
            log.info("%s: WS-фид: свеча open_time=%d, asof_ts=%d, сообщений стакана в буфере %d, в корзине %s",
                     symbol, expected_open, row["asof_ts"], len(messages), row.get("book_n_msgs"))
        return row
