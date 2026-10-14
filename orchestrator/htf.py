"""HtfBias: тренд H4 по монетам для фильтра M15 (features/htf.py). Состояние - только последняя таблица bias на монету.

Поток: контекст H4 (SmcKlineFeed interval=4h, FeatureEngine) -> `update` пересчитывает таблицу [htf_avail_ms, htf_bias] из
кадра (300 H4-свечей, чистая функция features.htf.htf_bias_table); контекст M15 -> `attach` добавляет колонки htf_bias и
htf_avail_ms к его кадру (join_asof, без будущего). Отсутствие или устаревание H4-данных - DataError (R1): торговать
вслепую без фильтра нельзя.
"""

import logging
from dataclasses import replace

import polars as pl

from features.htf import AVAIL_COLUMN, BIAS_COLUMN, BIAS_LABELS, attach_htf_bias, htf_bias_table
from orchestrator.journal import EventLog
from orchestrator.types import Event, FeatureContext
from utils.errors import ConfigError, DataError

log = logging.getLogger("orchestrator.htf")


class HtfBias:
    def __init__(self, interval: str, interval_ms: int, events: EventLog | None = None, *, bias_column: str = BIAS_COLUMN, avail_column: str = AVAIL_COLUMN):
        """bias_column / avail_column - имена колонок, которые `attach` добавит к кадру (мульти-таймфреймовый режим: `1d_bias` / `1d_avail_ms`)."""
        if interval_ms <= 0:
            raise ConfigError(f"HtfBias: interval_ms должен быть > 0, получено {interval_ms}")
        self.interval, self.interval_ms, self._events = interval, interval_ms, events
        self.bias_column, self.avail_column = bias_column, avail_column
        self._tables: dict[str, pl.DataFrame] = {}
        self._last: dict[str, int | None] = {}

    def update(self, ctx: FeatureContext) -> None:
        """Контекст закрытой старшей свечи (H4 / 1d) -> новая таблица bias монеты. Смена bias пишется в журнал (и в state_events БД)."""
        if ctx.interval != self.interval or ctx.interval_ms != self.interval_ms:
            raise DataError(f"HtfBias ждёт {self.interval}, получен контекст {ctx.symbol} {ctx.interval}")
        table = htf_bias_table(ctx.frame, ctx.interval_ms, avail_column=self.avail_column, bias_column=self.bias_column)
        self._tables[ctx.symbol] = table
        raw = table[self.bias_column][-1]
        bias = None if raw is None else int(raw)
        previous = self._last.get(ctx.symbol, "unset")
        self._last[ctx.symbol] = bias
        if previous != bias:
            label = "n/a" if bias is None else BIAS_LABELS[bias]
            log.info("%s: HTF (%s) bias %s", ctx.symbol, self.interval, label)
            if self._events is not None:
                self._events.emit(Event(ctx.received_ms, "htf_bias", ctx.symbol, "htf", None,
                                        {"interval": self.interval, "bias": label, "from": None if previous == "unset" else previous, "to": bias}))

    def has(self, symbol: str) -> bool:
        return symbol in self._tables

    def current(self, symbol: str) -> int | None:
        """Последний известный bias монеты (+1 / 0 / -1) или None (прогрев/нет данных)."""
        return self._last.get(symbol)

    def attach(self, ctx: FeatureContext) -> FeatureContext:
        """Контекст рабочего таймфрейма -> тот же контекст с колонками bias и avail. Нет таблицы или старший таймфрейм отстаёт -> DataError."""
        tag = f"H{self.interval[:-1]}" if self.interval.endswith("h") else self.interval.upper()  # 4h -> H4, 1d -> 1D
        table = self._tables.get(ctx.symbol)
        if table is None:
            raise DataError(f"{ctx.symbol}: нет данных {tag} для HTF-фильтра (фид {self.interval} не отдал ни одной свечи)")
        frame = attach_htf_bias(ctx.frame, table, avail_column=self.avail_column, bias_column=self.bias_column)
        avail = frame[self.avail_column][-1]
        if avail is None:
            raise DataError(f"{ctx.symbol}: для свечи {ctx.interval} {ctx.bar_open_time} нет закрытой {tag}-свечи (история {tag} короче истории рабочего таймфрейма)")
        age = ctx.bar_open_time - int(avail)
        if age >= self.interval_ms:
            raise DataError(f"{ctx.symbol}: {tag}-данные отстают - последняя доступная свеча закрылась {age} мс назад "
                            f"(допустимо меньше {self.interval_ms}): {tag}-фид не получил свечу вовремя")
        return replace(ctx, frame=frame)
