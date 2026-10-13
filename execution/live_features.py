"""LiveFeatureFeed: источник решений для MLModelStrategy с ТЕМ ЖЕ конвейером признаков, что у бэктеста (REST).

ВНИМАНИЕ: боевой путь (scripts/live_run.py --model-path) использует НЕ этот REST-фид, а его наследника
WsLiveFeatureFeed (execution/ws_live_features.py): стакан и свечи по WebSocket, 500 мс, как у обучающих данных.
Этот класс остаётся общей базой (конвейер, буфер, _row_from_candles) и REST-реализацией для проверок: снимки
раз в шаг цикла дают Train-Serve Skew по ofi_sum (см. «Известные расхождения» ниже).

Принцип «No Train-Serve Skew»: признаки в бою считаются не вручную, а прогоном сырья через код обучения:

    свечи + стакан -> DataQualityGate -> WatermarkClock -> TimelineAligner(decisions, attach_book)
                   -> compile_features -> apply_features -> ПОСЛЕДНЯЯ строка

Стратегия получает только эту строку (срез актуальных признаков), история ей не отдаётся.

Что делает poll(now_ms):
  1. Для каждого символа снимает REST-снимок стакана и кладёт его в скользящий буфер как «сообщение»
     (recv_ts - серверное время получения, E/T - из ответа биржи, u = lastUpdateId). Буфер нужен потому, что
     часть признаков стакана не берётся из одного снимка: ofi_sum - накопленный поток по сообщениям корзины
     (минимум два), ofi_norm_5/15 - окна по 5/15 барам сетки. Без накопления они всегда null, и модель
     никогда бы не торговала.
  2. Когда последняя закрытая свеча символа новая, скачивает lookback_bars + 1 + запас закрытых свечей,
     прогоняет конвейер и отдаёт последнюю строку.

Строка отдаётся, только если она СООТВЕТСТВУЕТ ожидаемой свече и уже «созрела»: asof_ts <= now_ms. Решение
бэктеста принимается в asof_ts = (recv_ts доказательства закрытия свечи) + candle_publish_lag + decision_compute,
и корзина стакана - это сообщения строго до asof_ts. Выдать строку раньше значило бы посчитать признаки по
неполной корзине, чего в обучении не было. Свеча, которую конвейер отбросил (поток «слеп», нет доказательства
закрытия), строкой не заменяется: устаревшую строку вместо неё не отдаём, решения по этой свече нет.

Прогрев (null-признаки - стратегия на них не торгует, это штатно):
  * свечи: lookback_bars (у самого длинного признака) + 1 баров приходят из REST сразу. Свечи до начала нашего
    буфера стакана не имеют доказательства закрытия и принимаются как assume_pre_stream (avail = закрытие +
    лаг; часы бота синхронизированы с биржей, а свеча заведомо закрыта). Значения признаков строки при этом
    те же, что у бэктеста: строка зависит только от прошлых строк;
  * стакан: obi/spread/microprice берутся из одного снимка последней корзины, а ofi_norm_{w} нужны w подряд
    идущих корзин по >= 2 снимка, то есть ~15 минут работы фида после каждого перезапуска (стакан по REST задним
    числом не получить, буфер в памяти).

Известные расхождения с обучением, которые конвейер убрать не может:
  * обучающий стакан - поток сообщений раз в ~500 мс, боевой - снимки раз в шаг цикла (LIVE_LOOP_INTERVAL_S,
    5 с). obi_*, spread_bps, microprice_dev_bps - функции последнего состояния и расхождения не имеют; ofi_sum
    суммирует приращения по сообщениям и при редких снимках имеет другой масштаб. Для паритета стакан нужно
    брать из того же WebSocket-потока, что у сборщика (collectors/depth_stream.py), а не из REST;
  * решение принимается позже, чем допускает бэктест: строка созревает через ~lag + compute после ПЕРВОГО снимка,
    увидевшего закрытие свечи, а снимки идут раз в шаг цикла. gap_break_ms поэтому шире тренировочного (см.
    default_live_timing): он решает лишь, принять свечу или отбросить, на значения признаков не влияет.
"""

import logging
import math
import time
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any

import polars as pl

import config
from pipeline.features.base import CompiledFeature, Feature, apply_features, compile_features, source_columns
from pipeline.features.microstructure import book_aggregations, book_levels, default_features
from pipeline.loaders.schemas import KLINE_SCHEMA, depth_schema
from pipeline.quality.gate import DataQualityGate
from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import WatermarkClock
from pipeline.types import TimingConfig
from utils.binance_rest import BinanceRest, depth_weight, klines_weight
from utils.errors import ConfigError, DataError

log = logging.getLogger("live_features")

INTERVAL = "1m"  # сетка решений обучающего конвейера (scripts/train_walkforward.INTERVAL)
MAX_KLINES_LIMIT = 1500  # потолок параметра limit у /fapi/v1/klines
_DEPTH_LIMITS = (5, 10, 20, 50, 100, 500, 1000)  # допустимые limit у /fapi/v1/depth


def default_live_timing() -> TimingConfig:
    """Тренировочные допущения, кроме gap_break_ms.

    Свечу «доказывает» первый снимок стакана с E >= конца свечи. В бою снимки идут раз в LIVE_LOOP_INTERVAL_S,
    поэтому E первого снимка отстаёт от закрытия на шаг цикла и сетевые задержки: допуск тренировочных 5 с
    отбрасывал бы свечи без нужды. Допуск обязан быть меньше интервала свечи.
    """
    gap = int(config.LIVE_LOOP_INTERVAL_S * 1000) + 10_000
    return TimingConfig(gap_break_ms=min(gap, config.INTERVAL_MS[INTERVAL] - 1))


# ---------------------------------------------------------------------------- разбор ответов


def parse_klines(raw: Any, symbol: str, now_ms: int) -> pl.DataFrame:
    """Ответ /klines -> кадр (symbol + KLINE_SCHEMA) только ЗАКРЫТЫХ свечей (close_time < now_ms), по возрастанию."""
    if not isinstance(raw, list):
        raise DataError(f"klines: ожидается список, получено {str(raw)[:200]}")
    rows: list[tuple] = []
    for k in raw:
        try:
            row = (
                int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]),
                int(k[6]), float(k[7]), int(k[8]), float(k[9]), float(k[10]),
            )
        except (TypeError, ValueError, IndexError, KeyError) as exc:
            raise DataError(f"klines: неожиданная запись {str(k)[:200]}") from exc
        prices = (row[1], row[2], row[3], row[4])
        if not all(math.isfinite(x) for x in (*prices, row[5], row[7], row[9], row[10])) or row[4] <= 0:
            raise DataError(f"klines: недопустимые значения свечи {str(k)[:200]}")
        if row[6] < now_ms:
            rows.append(row)
    if any(b[0] <= a[0] for a, b in zip(rows, rows[1:])):
        raise DataError("klines: open_time не растёт")
    frame = pl.DataFrame(rows, schema=KLINE_SCHEMA, orient="row")
    return frame.select(pl.lit(symbol).alias("symbol"), *KLINE_SCHEMA.names())


def parse_book_snapshot(raw: Any, symbol: str, recv_ts: int, levels: int) -> dict[str, Any] | None:
    """REST-снимок стакана -> строка схемы depth_schema(levels) (без pu). None - брак.

    Мусор по структуре (не объект, нет E/lastUpdateId, уровень не пара) - DataError. Брак по смыслу -
    None, как карантин DepthLoader: пустая сторона, цена <= 0, объём < 0, пересечённый стакан.
    """
    try:
        bids, asks = raw["bids"], raw["asks"]
        event_time, update_id = raw["E"], raw["lastUpdateId"]
        if (not isinstance(bids, list) or not isinstance(asks, list)
                or any(isinstance(v, bool) or not isinstance(v, int) for v in (event_time, update_id))):
            raise TypeError
        parsed: dict[str, float] = {}
        bad_level = False
        for side, book in (("bid", bids), ("ask", asks)):
            for i, level in enumerate(book[:levels]):
                px, qty = float(level[0]), float(level[1])
                parsed[f"{side}_px_{i}"], parsed[f"{side}_qty_{i}"] = px, qty
                bad_level |= not (math.isfinite(px) and math.isfinite(qty) and px > 0 and qty >= 0)
    except (TypeError, ValueError, KeyError, IndexError) as exc:
        raise DataError(f"depth: неожиданный ответ {str(raw)[:200]}") from exc
    if not bids or not asks or bad_level or parsed["bid_px_0"] >= parsed["ask_px_0"]:
        return None
    trade_time = raw.get("T")
    row: dict[str, Any] = {
        "recv_ts": recv_ts, "symbol": symbol, "E": event_time,
        "T": trade_time if isinstance(trade_time, int) and not isinstance(trade_time, bool) else None,
        "u": update_id, "pu": None,
    }
    for i in range(levels):
        for name in (f"bid_px_{i}", f"bid_qty_{i}", f"ask_px_{i}", f"ask_qty_{i}"):
            row[name] = parsed.get(name)
    return row


def _depth_limit(levels: int) -> int:
    for limit in _DEPTH_LIMITS:
        if limit >= levels:
            return limit
    raise ConfigError(f"Глубина стакана {levels} больше максимального limit {_DEPTH_LIMITS[-1]}")


# ------------------------------------------------------------------------------------ фид


class LiveFeatureFeed:
    """DecisionFeed: по строке канонических признаков на каждую новую закрытую свечу символа. См. докстринг модуля."""

    def __init__(
        self,
        api: BinanceRest,
        symbols: Sequence[str] | None = None,
        *,
        features: Sequence[Feature] | None = None,
        levels: int | None = None,
        timing: TimingConfig | None = None,
        extra_bars: int = 3,
        required_features: Sequence[str] = (),
        monotonic: Callable[[], float] = time.monotonic,
    ):
        """features - набор признаков обучения (по умолчанию default_features(taker_flow=True), как у
        train_walkforward без --no-taker-flow); required_features - что обязана уметь модель: нет такого
        выхода у набора - ConfigError при создании, а не KeyError на боевом шаге. extra_bars - запас закрытых
        свечей сверх lookback_bars + 1. monotonic - часы для замера времени внутри poll (подменяются в тестах).
        """
        self._api = api
        self._symbols = tuple(symbols if symbols is not None else config.SYMBOLS)
        if not self._symbols:
            raise ConfigError("Нужен хотя бы один символ")
        if config.LIVE_FEED_INTERVAL != INTERVAL:
            raise ConfigError(
                f"LIVE_FEED_INTERVAL={config.LIVE_FEED_INTERVAL!r}: модели обучены на сетке {INTERVAL}, "
                "канонический фид работает только с ней"
            )
        self._interval_ms = config.INTERVAL_MS[INTERVAL]
        self._levels = levels if levels is not None else config.DEPTH_LEVELS
        if isinstance(self._levels, bool) or not isinstance(self._levels, int) or self._levels < 1:
            raise ConfigError(f"levels должен быть целым >= 1, получено {self._levels!r}")
        self._depth_limit = _depth_limit(self._levels)
        self._timing = timing or default_live_timing()
        if isinstance(extra_bars, bool) or not isinstance(extra_bars, int) or extra_bars < 0:
            raise ConfigError(f"extra_bars должен быть целым >= 0, получено {extra_bars!r}")
        self._mono = monotonic
        self._clock = WatermarkClock(self._timing)
        self._aligner = TimelineAligner(self._timing)
        self._gate = DataQualityGate(self._timing)

        feats = list(features) if features is not None else default_features(taker_flow=True)
        self._compiled: tuple[CompiledFeature, ...] = self._compile(feats)
        self.feature_names: tuple[str, ...] = tuple(n for c in self._compiled for n in c.exprs)
        self.lookback_bars: int = max(c.lookback.bars_1m for c in self._compiled)
        missing = sorted(set(required_features) - set(self.feature_names))
        if missing:
            raise ConfigError(f"Фид не считает признаки, нужные модели: {missing}; считает: {list(self.feature_names)}")
        self._required = tuple(required_features) or self.feature_names

        # N закрытых свечей > lookback_bars: строка k полна, когда перед ней есть lookback_bars баров без разрывов.
        self._history = self.lookback_bars + 1 + extra_bars
        if self._history + 1 > MAX_KLINES_LIMIT:
            raise ConfigError(
                f"Глубина истории {self._history + 1} свечей больше лимита биржи {MAX_KLINES_LIMIT}: "
                "признаки с такой памятью (lookback) в бою не поддерживаются"
            )
        # Буфер стакана покрывает всю загруженную историю свечей (+ бар на границу окна первой корзины).
        self._retention_ms = (self._history + 1) * self._interval_ms
        self._books: dict[str, deque[dict[str, Any]]] = {s: deque() for s in self._symbols}
        self._done: dict[str, int] = {}  # open_time последней свечи, по которой решение уже отдано
        self._waiting: dict[str, int] = {}  # свеча, которую пробовали, но строка не созрела

    # ----------------------------------------------------------------------- конвейер

    def _decisions(self, candles: pl.DataFrame, book: pl.DataFrame) -> pl.LazyFrame:
        """Сырьё -> таблица решений со стаканом (ТЕ ЖЕ шаги, что у scripts/train_walkforward.build_dataset)."""
        lazy_book = self._gate.apply(book.lazy()).clean  # предусловие WatermarkClock, как в load_market
        # assume_pre_stream: история свечей REST старше начала буфера стакана - см. докстринг модуля.
        avail = self._clock.candle_availability(
            candles.lazy(), self._interval_ms, self._clock.watermark(lazy_book), assume_pre_stream=True
        )
        decisions = self._aligner.decisions(avail, self._interval_ms)
        return self._aligner.attach_book(
            decisions, lazy_book, book_aggregations(book_levels(lazy_book.collect_schema().names()))
        )

    def _compile(self, features: Sequence[Feature]) -> tuple[CompiledFeature, ...]:
        """Компиляция признаков по схеме реальных данных (на пустых кадрах: данные не нужны, только колонки)."""
        empty_candles = pl.DataFrame(schema={"symbol": pl.String, **KLINE_SCHEMA})
        empty_book = pl.DataFrame(schema=depth_schema(self._levels))
        decisions = self._decisions(empty_candles, empty_book)
        return compile_features(features, source_columns(decisions))

    def compute_features(self, candles: pl.DataFrame, book: pl.DataFrame) -> pl.DataFrame:
        """Таблица решений с признаками по свечам и сообщениям стакана ОДНОГО символа (по asof_ts)."""
        return apply_features(self._decisions(candles, book), self._compiled).collect()

    # ------------------------------------------------------------------------- poll

    async def poll(self, now_ms: int) -> list[dict[str, Any]]:
        started = self._mono()
        for symbol in self._symbols:
            await self._sample_book(symbol, now_ms, started)
        rows = []
        for symbol in self._symbols:
            row = await self._row(symbol, now_ms)
            if row is not None:
                rows.append(row)
        return rows

    async def _sample_book(self, symbol: str, now_ms: int, started: float) -> None:
        raw = await self._api.get(
            config.DEPTH_SNAPSHOT_PATH, {"symbol": symbol, "limit": self._depth_limit},
            weight=depth_weight(self._depth_limit),
        )
        # now_ms взят в начале шага: время получения = он + сколько уже прошло (не раньше реального получения).
        recv_ts = now_ms + int((self._mono() - started) * 1000)
        message = parse_book_snapshot(raw, symbol, recv_ts, self._levels)
        if message is None:
            log.warning("%s: стакан отбракован (пуст/пересечён/неверные уровни), снимок пропущен", symbol)
            return
        buf = self._books[symbol]
        if buf and message["u"] <= buf[-1]["u"]:
            return  # стакан не менялся с прошлого снимка (тот же u) - дубль, как duplicate_u у DepthLoader
        message["pu"] = buf[-1]["u"] if buf else None
        buf.append(message)
        while buf and buf[0]["recv_ts"] < now_ms - self._retention_ms:
            buf.popleft()

    def _due_open(self, symbol: str, now_ms: int) -> int | None:
        """open_time свечи, по которой пора пробовать отдать строку, или None (уже отдана / рано)."""
        boundary = now_ms // self._interval_ms * self._interval_ms  # открытие формирующейся = закрытие последней закрытой
        expected_open = boundary - self._interval_ms
        if self._done.get(symbol, -1) >= expected_open:
            return None
        waited = self._waiting.get(symbol)
        if waited is not None and waited != expected_open:
            log.warning("%s: решение по свече open_time=%d пропущено (строка не созрела до следующей свечи)",
                        symbol, waited)
            del self._waiting[symbol]
        t = self._timing
        if now_ms < boundary + t.candle_publish_lag_ms + t.decision_compute_ms:
            return None  # раньше этого момента строка созреть не может: данные не дёргаем
        return expected_open

    async def _row(self, symbol: str, now_ms: int) -> dict[str, Any] | None:
        expected_open = self._due_open(symbol, now_ms)
        if expected_open is None:
            return None

        limit = self._history + 1  # + формирующаяся свеча, её parse_klines отбросит
        raw = await self._api.get(
            config.KLINES_PATH, {"symbol": symbol, "interval": INTERVAL, "limit": limit}, weight=klines_weight(limit),
        )
        return self._row_from_candles(
            symbol, parse_klines(raw, symbol, now_ms), list(self._books[symbol]), now_ms, expected_open
        )

    def _row_from_candles(
        self, symbol: str, candles: pl.DataFrame, messages: list[dict[str, Any]], now_ms: int, expected_open: int
    ) -> dict[str, Any] | None:
        """Закрытые свечи + сообщения стакана (копия буфера) -> строка признаков ожидаемой свечи (None - ещё нет).

        Общий хвост _row для REST-фида и WS-фида (execution/ws_live_features.py): конвейер один. Не трогает буфер
        стакана, поэтому WS-фид вызывает его в потоке, пока читатели сокета продолжают класть сообщения."""
        if candles.height == 0 or int(candles["open_time"][-1]) != expected_open:
            self._waiting[symbol] = expected_open  # биржа ещё не опубликовала свечу
            return None
        if candles.height < self.lookback_bars + 1:
            log.warning("%s: закрытых свечей %d < %d (lookback_bars + 1): признаки с длинным окном будут null",
                        symbol, candles.height, self.lookback_bars + 1)

        book = pl.DataFrame(messages, schema=depth_schema(self._levels))
        frame = self.compute_features(candles, book)
        if frame.height == 0 or int(frame["open_time"][-1]) != expected_open:
            log.info("%s: свеча open_time=%d пока не принята конвейером (нет доказательства закрытия)",
                     symbol, expected_open)
            self._waiting[symbol] = expected_open
            return None
        row = frame.row(-1, named=True)
        if row["asof_ts"] > now_ms:
            self._waiting[symbol] = expected_open  # корзина стакана ещё не закрыта: решение бэктеста принимается позже
            return None

        self._done[symbol] = expected_open
        self._waiting.pop(symbol, None)
        empty = sorted(n for n in self._required if row.get(n) is None)
        if empty:
            log.info("%s: признаки без значения (прогрев/устаревший стакан): %s", symbol, ", ".join(empty))
        return row
