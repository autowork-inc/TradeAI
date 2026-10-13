"""Источник решений для боевого цикла: по строке на каждую новую закрытую свечу символа.

В бэктесте стратегия получает строки готовой таблицы признаков; в бою такой таблицы нет (признаки
Этапа 2 считаются пакетно по записанному стакану). Поэтому здесь - минимальный каркасный источник
для BaselineStrategy: из REST считает ma_fast / ma_slow (SMA закрытий) и obi (дисбаланс топ-N стакана:
(bid - ask) / (bid + ask), как obi_k в pipeline/features/microstructure.py). Это каркас для проверки
боевого контура на тестовой сети, а НЕ торговая идея. Для MLModelStrategy - execution/live_features.py.

Протокол DecisionFeed: `await poll(now_ms)` -> список строк (dict с symbol, asof_ts, close и признаками);
пустой список - нового решения нет. Запросы к бирже - только когда новая свеча уже должна была
закрыться, а не на каждом шаге цикла. Неожиданный формат ответа -> DataError (мусор молча не пропускаем).
"""

import logging
import math
from collections.abc import Sequence
from typing import Any, Protocol

import config
from utils.binance_rest import BinanceRest, depth_weight, klines_weight
from utils.errors import ConfigError, DataError

log = logging.getLogger("live_feed")


class DecisionFeed(Protocol):
    async def poll(self, now_ms: int) -> Sequence[dict[str, Any]]: ...


def parse_closed_klines(raw: Any, now_ms: int) -> list[tuple[int, int, float]]:
    """-> [(open_time, close_time, close)] только ЗАКРЫТЫХ свечей (close_time < now_ms), по возрастанию."""
    if not isinstance(raw, list):
        raise DataError(f"klines: ожидается список, получено {str(raw)[:200]}")
    out: list[tuple[int, int, float]] = []
    for k in raw:
        try:
            open_time, close_time, close = int(k[0]), int(k[6]), float(k[4])
        except (TypeError, ValueError, IndexError, KeyError) as exc:
            raise DataError(f"klines: неожиданная запись {str(k)[:200]}") from exc
        if not math.isfinite(close) or close <= 0:
            raise DataError(f"klines: недопустимая цена закрытия {close!r}")
        if close_time < now_ms:
            out.append((open_time, close_time, close))
    if any(b[0] <= a[0] for a, b in zip(out, out[1:])):
        raise DataError("klines: open_time не растёт")
    return out


def order_book_imbalance(raw: Any, levels: int) -> float | None:
    """(bid - ask) / (bid + ask) по топ-levels уровням. None - стакан пуст (нет сигнала)."""
    try:
        bid = sum(float(q) for _, q in raw["bids"][:levels])
        ask = sum(float(q) for _, q in raw["asks"][:levels])
    except (TypeError, ValueError, KeyError, IndexError) as exc:
        raise DataError(f"depth: неожиданный ответ {str(raw)[:200]}") from exc
    total = bid + ask
    if not math.isfinite(total) or total <= 0:
        return None
    return (bid - ask) / total


class KlineObiFeed:
    def __init__(
        self,
        api: BinanceRest,
        symbols: Sequence[str] | None = None,
        *,
        interval: str | None = None,
        fast: int | None = None,
        slow: int | None = None,
        obi_levels: int | None = None,
        delay_s: float | None = None,
    ):
        self._api = api
        self._symbols = tuple(symbols if symbols is not None else config.SYMBOLS)
        self._interval = interval or config.LIVE_FEED_INTERVAL
        if self._interval not in config.INTERVAL_MS:
            raise ConfigError(f"Неизвестный таймфрейм {self._interval!r}")
        self._interval_ms = config.INTERVAL_MS[self._interval]
        self._fast = fast if fast is not None else config.LIVE_MA_FAST
        self._slow = slow if slow is not None else config.LIVE_MA_SLOW
        if not 1 <= self._fast < self._slow:
            raise ConfigError(f"Нужно 1 <= fast < slow, получено fast={self._fast}, slow={self._slow}")
        self._levels = obi_levels if obi_levels is not None else config.LIVE_OBI_LEVELS
        self._delay_ms = int((delay_s if delay_s is not None else config.LIVE_FEED_DELAY_S) * 1000)
        self._last_open: dict[str, int] = {}

    async def poll(self, now_ms: int) -> list[dict[str, Any]]:
        rows = []
        for symbol in self._symbols:
            last = self._last_open.get(symbol)
            # Следующая свеча закроется не раньше last_open + 2 интервала: раньше спрашивать незачем.
            if last is not None and now_ms < last + 2 * self._interval_ms + self._delay_ms:
                continue
            row = await self._row(symbol, now_ms)
            if row is not None:
                rows.append(row)
        return rows

    async def _row(self, symbol: str, now_ms: int) -> dict[str, Any] | None:
        limit = self._slow + 2
        raw = await self._api.get(
            config.KLINES_PATH, {"symbol": symbol, "interval": self._interval, "limit": limit},
            weight=klines_weight(limit),
        )
        closed = parse_closed_klines(raw, now_ms)
        if len(closed) < self._slow:
            log.warning("%s: закрытых свечей %d < %d (прогрев ma_slow), решения нет", symbol, len(closed), self._slow)
            return None
        open_time, close_time, close = closed[-1]
        if open_time == self._last_open.get(symbol):
            return None  # новая свеча ещё не закрылась (опоздали с запасом) - спросим на следующем шаге
        closes = [c for _, _, c in closed]
        book = await self._api.get(
            config.DEPTH_SNAPSHOT_PATH, {"symbol": symbol, "limit": self._levels}, weight=depth_weight(self._levels),
        )
        obi = order_book_imbalance(book, self._levels)
        self._last_open[symbol] = open_time
        return {
            "symbol": symbol,
            "asof_ts": close_time + 1,
            "close": close,
            "ma_fast": sum(closes[-self._fast:]) / self._fast,
            "ma_slow": sum(closes[-self._slow:]) / self._slow,
            "obi": obi,
        }
