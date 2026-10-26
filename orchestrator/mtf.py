"""MtfState: состояние старших таймфреймов для Shadow, один объект на все рабочие таймфреймы (2026-10-14).

Зачем один объект. В процессе одновременно работают две группы стратегий:
  * `SMC_Pro_Trend` - рабочий 5m, структура и свип на 1h, bias на 1d (features/mtf.py);
  * остальные четыре (и `SMC_Counter_Trend`, который читает bias) - рабочий 15m, bias на 4h (orchestrator/htf.py), как раньше.
Фиды старших таймфреймов (1h, 4h, 1d) отдают контексты в один список; `ShadowRun` вызывает `update` на каждый из них, затем `attach` на каждый рабочий
контекст (duck-typing с `HtfBias`: у обоих `update/attach/has/current`). Гонка «1h-свип против 5m-свечи» устранена порядком цикла: все контексты старших
таймфреймов тика обрабатываются ДО рабочих (scripts/shadow_run.py), а присоединение по времени (join_asof по `open_time`) в любом случае не даёт 5m-свече
увидеть то, что закрылось позже её открытия.

Закрытие 1h или 1d НЕ запускает стратегии: оно только пересчитывает таблицу состояния монеты (1h: свипы, уровни, свинги; 1d: bias). Стратегии считаются
только на закрытии своего рабочего таймфрейма (`Orchestrator.on_context`, атрибут `interval` стратегии).

Fail-Fast (R1): рабочий контекст без состояния старшего таймфрейма или с отставшим состоянием (последняя доступная 1h-свеча закрылась >= 1 ч назад, 1d - >= 1 суток)
- DataError: торговать вслепую без фильтра нельзя.
"""

import logging
from dataclasses import replace

import polars as pl

from features.htf import AVAIL_COLUMN, BIAS_COLUMN, H4_AVAIL, H4_BIAS
from features.mtf import D1_AVAIL, D1_BIAS, H1_AVAIL, attach_table, d1_bias_table, h1_table
from orchestrator.htf import HtfBias
from orchestrator.journal import EventLog
from orchestrator.types import FeatureContext
from utils.errors import ConfigError, DataError

log = logging.getLogger("orchestrator.mtf")


class MtfState:
    def __init__(self, interval_ms: dict[str, int], events: EventLog | None = None, *, base: str | None = "5m", struct: str = "1h", bias: str = "1d",
                 legacy: str | None = "15m", legacy_bias: str = "4h"):
        """interval_ms - длительности используемых таймфреймов. base=None выключает MTF (остаётся только legacy), legacy=None выключает 4h-bias."""
        needed = [x for x in (base, struct if base else None, bias, legacy, legacy_bias if legacy else None) if x]
        unknown = [x for x in needed if x not in interval_ms]
        if unknown:
            raise ConfigError(f"MtfState: нет длительности для таймфреймов {unknown}")
        if base is None and legacy is None:
            raise ConfigError("MtfState: нужен хотя бы один рабочий таймфрейм (base или legacy)")
        self.base, self.struct, self.bias, self.legacy, self.legacy_bias = base, struct, bias, legacy, legacy_bias
        self._ms = dict(interval_ms)
        self._h1: dict[str, pl.DataFrame] = {}
        # 2026-10-26: дневной тренд нужен ВСЕМ рабочим таймфреймам (5m и 15m) - фильтр «только по тренду дневки»; 4h-структура - только SMC_Counter_Trend
        self._d1 = HtfBias(bias, interval_ms[bias], events, bias_column=D1_BIAS, avail_column=D1_AVAIL)
        self._h4 = HtfBias(legacy_bias, interval_ms[legacy_bias], events, bias_column=H4_BIAS, avail_column=H4_AVAIL) if legacy else None

    @property
    def state_intervals(self) -> tuple[str, ...]:
        """Таймфреймы, контексты которых обновляют состояние (а не идут стратегиям)."""
        return tuple(dict.fromkeys(x for x in ((self.struct,) if self.base else ()) + (self.bias,) + ((self.legacy_bias,) if self.legacy else ())))

    # ------------------------------------------------------------------------------------------ обновление

    def update(self, ctx: FeatureContext) -> None:
        """Контекст закрытой 1h / 1d / 4h-свечи -> состояние монеты. Другой таймфрейм - DataError (фид собран неверно)."""
        if self.base and ctx.interval == self.struct:
            if ctx.interval_ms != self._ms[self.struct]:
                raise DataError(f"MtfState: {ctx.symbol} {ctx.interval}: interval_ms={ctx.interval_ms}, ожидалось {self._ms[self.struct]}")
            self._h1[ctx.symbol] = h1_table(ctx.frame, ctx.interval_ms)
        elif ctx.interval == self.bias:
            self._d1.update(ctx)
        elif self.legacy and ctx.interval == self.legacy_bias:
            self._h4.update(ctx)
        else:
            raise DataError(f"MtfState: контекст {ctx.symbol} {ctx.interval} не относится к старшим таймфреймам {self.state_intervals}")

    def has(self, symbol: str) -> bool:
        """Есть состояние всех старших таймфреймов монеты (для рабочих таймфреймов, которые включены)."""
        ok = self._d1.has(symbol)
        if self.base:
            ok = ok and symbol in self._h1
        if self.legacy:
            ok = ok and self._h4.has(symbol)
        return ok

    def current(self, symbol: str) -> int | None:
        """Последний известный дневной тренд монеты (+1 / 0 / -1 / None)."""
        return self._d1.current(symbol)

    # --------------------------------------------------------------------------------------- присоединение

    def attach(self, ctx: FeatureContext) -> FeatureContext:
        """Рабочий контекст -> с колонками старших таймфреймов: 5m: `1h_*` и `1d_*`; 15m: `htf_bias` (4h). Иначе - ошибка конфигурации."""
        if self.base and ctx.interval == self.base:
            return self._attach_mtf(ctx)
        if self.legacy and ctx.interval == self.legacy:
            return self._attach_legacy(ctx)
        raise DataError(f"MtfState: рабочий таймфрейм {ctx.interval} не настроен (base={self.base}, legacy={self.legacy})")

    def _attach_legacy(self, ctx: FeatureContext) -> FeatureContext:
        """15m: дневной тренд (`1d_bias`; он же `htf_bias` - его читают фильтры тренда стратегий) + 4h-структура (`h4_bias`, только SMC_Counter_Trend).
        Обе проверки «старший таймфрейм не отстаёт» - как раньше (HtfBias.attach)."""
        with_d1 = self._d1.attach(ctx)
        frame = with_d1.frame.with_columns(pl.col(D1_BIAS).alias(BIAS_COLUMN), pl.col(D1_AVAIL).alias(AVAIL_COLUMN))
        return self._h4.attach(replace(ctx, frame=frame))

    def _attach_mtf(self, ctx: FeatureContext) -> FeatureContext:
        table = self._h1.get(ctx.symbol)
        if table is None:
            raise DataError(f"{ctx.symbol}: нет данных {self.struct} для мульти-таймфреймового автомата (фид {self.struct} не отдал ни одной свечи)")
        with_d1 = self._d1.attach(ctx)  # 1d: колонки `1d_bias` и `1d_avail_ms` + проверка «не отстаёт»
        frame = attach_table(with_d1.frame, table, H1_AVAIL)
        avail = frame[H1_AVAIL][-1]
        if avail is None:
            raise DataError(f"{ctx.symbol}: для свечи {ctx.interval} {ctx.bar_open_time} нет закрытой {self.struct}-свечи (история {self.struct} короче истории {ctx.interval})")
        age = ctx.bar_open_time - int(avail)
        if age >= self._ms[self.struct]:
            raise DataError(f"{ctx.symbol}: {self.struct}-данные отстают - последняя доступная свеча закрылась {age} мс назад "
                            f"(допустимо меньше {self._ms[self.struct]}): {self.struct}-фид не получил свечу вовремя")
        return replace(ctx, frame=frame)
