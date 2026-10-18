"""Микроструктура рынка для OBI_Breakout: стакан `depth10@500ms` и лента `aggTrade` -> агрегаты по закрытой свече (2026-10-11).

Поток данных (все WebSocket-соединения общие, лишних подключений нет):
  * лента: те же сообщения aggTrade, что получает TickFeed (цена -> трекер); `MicroStructure.on_tick` берёт из тика объём и сторону
    агрессора (`Tick.qty`, `Tick.buyer_maker`);
  * стакан: новый `DepthFeed` (публичный маршрут config.WS_BASE_URL, `<symbol>@depth10@500ms`, полное состояние топ-10 в каждом сообщении).
`MicroAggregator` складывает события по свечам (ключ - open_time свечи по времени биржи) и по запросу отдаёт `MicroBar`:
  * obi   - среднее (bid_qty - ask_qty) / (bid_qty + ask_qty) по топ-10 уровням за последние obi_window_s секунд свечи (от -1 до +1);
  * delta - (объём покупок агрессором - объём продаж) / общий объём ленты за ВСЮ свечу (от -1 до +1);
  * valid - данные свечи полные: поток был подключён к началу свечи (с допуском grace) и не обрывался. Неполные данные - None
    (подтверждения нет, сигнала OBI_Breakout нет): частичную свечу выдавать за полную нельзя.
`MicroStructure.attach(ctx)` добавляет к кадру контекста колонки ms_obi и ms_delta: значения только на ПОСЛЕДНЕЙ строке (у истории
нет ни стакана, ни ленты), остальное null. Читать сводку свечи безопасно, только когда свеча закрыта; события, пришедшие после чтения
по другому соединению (гонка WS), в сводку не попадают - дельта слегка занижена, это принято (знак и порог устойчивы).
"""

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

import aiohttp
import polars as pl

import config
from orchestrator.types import FeatureContext, Tick
from orchestrator.ws import ReconnectingStream, decode_combined, wall_clock_ms
from strategies.obi_breakout import DELTA_COLUMN, OBI_COLUMN
from utils.errors import ConfigError, DataError, WebSocketClosed

log = logging.getLogger("orchestrator.microstructure")

KINDS = ("trade", "depth")
DEPTH_LEVELS = 10


@dataclass(frozen=True)
class MicroBar:
    obi: float | None
    delta: float | None
    trades: int
    obi_samples: int
    valid: bool


class _Bar:
    __slots__ = ("buy", "sell", "trades", "obi")

    def __init__(self) -> None:
        self.buy = self.sell = 0.0
        self.trades = 0
        self.obi: deque[tuple[int, float]] = deque()  # (время, obi): только хвост окна


class MicroAggregator:
    def __init__(self, symbols: tuple[str, ...], interval_ms: int, *, obi_window_s: float = 120.0, grace_s: float = 10.0, keep: int = 4):
        if interval_ms <= 0 or obi_window_s <= 0 or grace_s < 0 or keep < 2:
            raise ConfigError("MicroAggregator: interval_ms > 0, obi_window_s > 0, grace_s >= 0, keep >= 2")
        self._symbols, self._iv = tuple(symbols), interval_ms
        self._window_ms, self._grace_ms, self._keep = int(obi_window_s * 1000), int(grace_s * 1000), keep
        self._bars: dict[str, dict[int, _Bar]] = {s: {} for s in self._symbols}
        self._first_ms: dict[str, int | None] = {k: None for k in KINDS}  # время первого сообщения текущего соединения
        self._added_ms: dict[str, int] = {}  # монеты, добавленные в работающий процесс (автоселектор): сводки свечей, начатых до добавления, неполные

    def add_symbols(self, symbols: tuple[str, ...], now_ms: int) -> None:
        """Монеты, подключённые на лету: окна свечей, открывшихся раньше `now_ms`, для них неполные (valid=False, ms_obi / ms_delta = null)."""
        for s in symbols:
            if s not in self._bars:
                self._symbols += (s,)
                self._bars[s] = {}
                self._added_ms[s] = now_ms

    # ------------------------------------------------------------------------------------------ события

    def _bar(self, symbol: str, ts_ms: int) -> _Bar | None:
        bars = self._bars.get(symbol)
        if bars is None:
            raise DataError(f"MicroAggregator: неизвестная монета {symbol!r}")
        open_time = ts_ms - ts_ms % self._iv
        bar = bars.get(open_time)
        if bar is None:
            if bars and open_time < min(bars):
                return None  # событие старше хранимых свечей (сильное опоздание): мимо
            bar = bars[open_time] = _Bar()
            for old in sorted(bars)[: max(0, len(bars) - self._keep)]:
                del bars[old]
        return bar

    def on_trade(self, symbol: str, ts_ms: int, qty: float, buyer_maker: bool) -> None:
        if self._first_ms["trade"] is None:
            self._first_ms["trade"] = ts_ms
        bar = self._bar(symbol, ts_ms)
        if bar is None:
            return
        if buyer_maker:
            bar.sell += qty  # покупатель - мейкер: агрессор продал
        else:
            bar.buy += qty
        bar.trades += 1

    def on_depth(self, symbol: str, ts_ms: int, bids: list, asks: list) -> None:
        if self._first_ms["depth"] is None:
            self._first_ms["depth"] = ts_ms
        bar = self._bar(symbol, ts_ms)
        if bar is None:
            return
        bid_q, ask_q = _sum_qty(bids), _sum_qty(asks)
        total = bid_q + ask_q
        if total <= 0:
            raise DataError(f"{symbol}: пустой стакан (bid {bid_q}, ask {ask_q})")
        bar.obi.append((ts_ms, (bid_q - ask_q) / total))
        while bar.obi and bar.obi[0][0] < ts_ms - self._window_ms:
            bar.obi.popleft()

    def mark_gap(self, kind: str) -> None:
        """Обрыв потока: данные свечей, в которых он случился, неполные (первое сообщение нового соединения придёт позже начала свечи)."""
        if kind not in KINDS:
            raise ConfigError(f"Неизвестный поток микроструктуры {kind!r}")
        self._first_ms[kind] = None

    # ------------------------------------------------------------------------------------------ сводка

    def summary(self, symbol: str, bar_open_time: int) -> MicroBar:
        """Сводка закрытой свечи. valid=False - данные неполные: obi и delta тогда None."""
        bar = self._bars.get(symbol, {}).get(bar_open_time)
        firsts = (self._first_ms["trade"], self._first_ms["depth"])
        valid = bar is not None and all(f is not None and f <= bar_open_time + self._grace_ms for f in firsts)
        added = self._added_ms.get(symbol)
        if added is not None and added > bar_open_time + self._grace_ms:
            valid = False
        if not valid:
            return MicroBar(None, None, 0 if bar is None else bar.trades, 0 if bar is None else len(bar.obi), False)
        total = bar.buy + bar.sell
        delta = (bar.buy - bar.sell) / total if total > 0 else None
        obi = sum(v for _, v in bar.obi) / len(bar.obi) if bar.obi else None
        return MicroBar(obi, delta, bar.trades, len(bar.obi), True)


def _sum_qty(levels: list) -> float:
    if not isinstance(levels, list) or not levels:
        raise DataError(f"Стакан без уровней: {str(levels)[:100]}")
    try:
        return sum(float(q) for _, q in levels[:DEPTH_LEVELS])
    except (TypeError, ValueError) as exc:
        raise DataError(f"Неожиданный уровень стакана: {str(levels)[:200]}") from exc


class MicroStructure:
    """Агрегатор + присоединение сводки к контексту M15. `on_tick` - приёмник тиков ленты (из TickFeed)."""

    def __init__(self, symbols: tuple[str, ...], interval_ms: int, **kwargs: Any):
        self.interval_ms = interval_ms  # свечи ЭТОГО таймфрейма получают ms_obi / ms_delta (окна агрегатора кратны ему); другие (5m) ShadowRun не присоединяет
        self.aggregator = MicroAggregator(symbols, interval_ms, **kwargs)

    def add_symbols(self, symbols: tuple[str, ...], now_ms: int) -> None:
        self.aggregator.add_symbols(symbols, now_ms)

    def on_tick(self, tick: Tick) -> None:
        if tick.kind != "trade":
            return
        if tick.qty is None or tick.buyer_maker is None:
            raise DataError(f"{tick.symbol}: у тика ленты нет объёма/стороны агрессора (qty={tick.qty!r}, buyer_maker={tick.buyer_maker!r})")
        self.aggregator.on_trade(tick.symbol, tick.ts_ms, tick.qty, tick.buyer_maker)

    def attach(self, ctx: FeatureContext) -> FeatureContext:
        """Контекст -> тот же контекст с колонками ms_obi и ms_delta (значения только на последней строке)."""
        clash = [c for c in (OBI_COLUMN, DELTA_COLUMN) if c in ctx.frame.columns]
        if clash:
            raise DataError(f"Микроструктура: колонки {clash} уже есть в кадре")
        micro = self.aggregator.summary(ctx.symbol, ctx.bar_open_time)
        h = ctx.frame.height
        cols = [pl.Series(name, [None] * (h - 1) + [value], dtype=pl.Float64) for name, value in ((OBI_COLUMN, micro.obi), (DELTA_COLUMN, micro.delta))]
        return replace(ctx, frame=ctx.frame.with_columns(cols))


class DepthFeed:
    """Поток `<symbol>@depth10@500ms` (публичный маршрут) -> MicroAggregator.on_depth. Обрыв -> mark_gap('depth')."""

    def __init__(
        self, session: aiohttp.ClientSession | None, aggregator: MicroAggregator, *, symbols: tuple[str, ...] | None = None,
        clock_ms: Callable[[], int] = wall_clock_ms, url: str | None = None, max_reconnects: int | None = None,
        backoff_s: float | None = None, backoff_max_s: float | None = None,
    ):
        self._symbols = tuple(symbols if symbols is not None else config.SHADOW_SYMBOLS)
        self._agg, self._session = aggregator, session
        self._expected = {f"{s.lower()}@depth{DEPTH_LEVELS}@{config.DEPTH_UPDATE_SPEED}": s for s in self._symbols}
        self._url = f"{url or config.WS_BASE_URL}?streams=" + "/".join(self._expected)
        self._stream = ReconnectingStream(
            "depth", self._url, session, self._on_frame, on_gap=lambda ts: aggregator.mark_gap("depth"), clock_ms=clock_ms,  # type: ignore[arg-type]
            max_reconnects=max_reconnects, backoff_s=backoff_s, backoff_max_s=backoff_max_s,
        )
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None:
            if self._session is None:
                raise ConfigError("DepthFeed: для запуска потока нужна session")
            self._task = asyncio.create_task(self._stream.run(), name="depth_feed")

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def check(self) -> None:
        """Поднимает исключение упавшей задачи потока (вызывать из основного цикла)."""
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise WebSocketClosed("Задача фида стакана отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача фида стакана завершилась без ошибки: так быть не должно")

    def stats(self) -> dict[str, Any]:
        return {"received": self._stream.received, "reconnects": self._stream.reconnects}

    def _on_frame(self, raw: str, recv_ts: int) -> None:
        _, symbol, data = decode_combined(raw, self._expected)
        if data.get("s") != symbol or "b" not in data or "a" not in data:
            raise DataError(f"Неожиданное сообщение стакана {symbol}: {str(data)[:200]}")
        try:
            ts = int(data.get("T") or data["E"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Сообщение стакана {symbol} без времени: {str(data)[:200]}") from exc
        self._agg.on_depth(symbol, ts, data["b"], data["a"])
