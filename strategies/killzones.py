"""Стратегия D `ICT_Killzones`: ложный пробой (свип) границ Азиатской сессии в окна открытия Лондона и Нью-Йорка (2026-10-11).

Векторный Polars (R2), без заглядывания в будущее (R3/R4), одна функция для бэктеста и live (R8).

Время - только UTC (open_time свечи - мс UTC), без перехода на летнее время (в крипте сессии привязывают к фиксированным часам;
зимой/летом реальное открытие Лондона/Нью-Йорка сдвигается на час - принято, см. CLAUDE.md разд. 3.21):
  * Азиатская сессия: 00:00-07:00 UTC; диапазон = max(high) и min(low) её свечей ЗА ТЕКУЩИЕ сутки (якорь - начало суток UTC);
  * London killzone: 07:00-10:00 UTC; New York killzone: 12:00-15:00 UTC (по времени ОТКРЫТИЯ сигнальной свечи).
Диапазон «известен» только после закрытия всех свечей сессии (asia_n == ожидаемому числу): если кадр начинается внутри сессии (окно live
короче суток) - сигналов в эти сутки нет. Бегущий максимум/минимум берётся только по свечам сессии (остальные - заменитель ±inf), поэтому
значение в строке j не зависит от строк после j.
Сигнал на закрытой свече j внутри killzone:
  * шорт: high[j] > Asia High, close[j] < Asia High (ложный пробой вверх), close[j] < open[j], уровень не пробивался ранее в эти сутки;
  * лонг: зеркало по Asia Low;
  * свеча, пробившая обе границы сразу, пропускается (направление неоднозначно).
Вход MARKET, SL = экстремум прокола + 0.2*ATR (за хай/лоу свечи свипа), управление - Scale Out по умолчанию.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from utils.errors import ConfigError, DataError

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
_REQUIRED = ("open_time", "open", "high", "low", "close", "atr")
INF = float("inf")


@dataclass(frozen=True)
class KillzoneParams:
    asia_start_h: int = 0
    asia_end_h: int = 7
    london: tuple[int, int] = (7, 10)
    new_york: tuple[int, int] = (12, 15)
    sl_buffer_atr: float = D.SL_BUFFER_ATR

    def __post_init__(self) -> None:
        for name, (a, b) in (("asia", (self.asia_start_h, self.asia_end_h)), ("london", self.london), ("new_york", self.new_york)):
            if not (0 <= a < b <= 24):
                raise ConfigError(f"KillzoneParams.{name}: нужны часы 0 <= начало < конец <= 24, получено ({a}, {b})")
        if self.asia_end_h > self.london[0] or self.london[1] > self.new_york[0]:
            raise ConfigError("KillzoneParams: порядок Азия -> Лондон -> Нью-Йорк, окна не пересекаются (диапазон Азии должен закрыться до killzone)")
        if not (isinstance(self.sl_buffer_atr, (int, float)) and self.sl_buffer_atr > 0):
            raise ConfigError(f"KillzoneParams.sl_buffer_atr должен быть положительным, получено {self.sl_buffer_atr!r}")


def signal_columns() -> tuple[str, ...]:
    return ("kz_asia_high", "kz_asia_low", "kz_session", "kz_short_signal", "kz_long_signal", "kz_short_sl", "kz_long_sl")


def detect_killzones(df: pl.DataFrame, interval_ms: int, params: KillzoneParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (нужны open_time, OHLC, atr) -> тот же кадр + kz_* колонки (signal_columns)."""
    p = params or KillzoneParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: сначала features.smc.add_features")
    clash = [c for c in signal_columns() if c in df.columns]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    if isinstance(interval_ms, bool) or not isinstance(interval_ms, int) or interval_ms <= 0 or HOUR_MS % interval_ms != 0:
        raise ConfigError(f"interval_ms должен делить час нацело, получено {interval_ms!r}")
    col = pl.col
    a0, a1 = p.asia_start_h * HOUR_MS, p.asia_end_h * HOUR_MS
    expected_asia = (a1 - a0) // interval_ms
    w = df.with_columns(
        (col("open_time") // DAY_MS).alias("_day"),
        (col("open_time") % DAY_MS).alias("_tod"),
    ).with_columns(
        ((col("_tod") >= a0) & (col("_tod") < a1)).alias("_asia"),
    ).with_columns(
        col("_asia").cast(pl.Int64).cum_sum().over("_day").alias("_asia_n"),
        pl.when(col("_asia")).then(col("high")).otherwise(-INF).cum_max().over("_day").alias("_asia_hi_run"),
        pl.when(col("_asia")).then(col("low")).otherwise(INF).cum_min().over("_day").alias("_asia_lo_run"),
        col("high").cum_max().over("_day").alias("_day_hi"),
        col("low").cum_min().over("_day").alias("_day_lo"),
    ).with_columns(
        col("_day_hi").shift(1).over("_day").alias("_day_hi_prev"),
        col("_day_lo").shift(1).over("_day").alias("_day_lo_prev"),
    )
    complete = col("_asia_n") == expected_asia
    w = w.with_columns(
        pl.when(complete).then(col("_asia_hi_run")).alias("_ahi"),
        pl.when(complete).then(col("_asia_lo_run")).alias("_alo"),
    )
    london = (col("_tod") >= p.london[0] * HOUR_MS) & (col("_tod") < p.london[1] * HOUR_MS)
    ny = (col("_tod") >= p.new_york[0] * HOUR_MS) & (col("_tod") < p.new_york[1] * HOUR_MS)
    in_kz = (london | ny) & ~col("_asia")
    taken_hi = (col("_day_hi_prev") > col("_ahi")).fill_null(False)  # уровень уже пробивали в эти сутки
    taken_lo = (col("_day_lo_prev") < col("_alo")).fill_null(False)
    both = ((col("high") > col("_ahi")) & (col("low") < col("_alo"))).fill_null(False)
    short = (in_kz & (col("high") > col("_ahi")) & (col("close") < col("_ahi")) & (col("close") < col("open")) & ~taken_hi & ~both).fill_null(False)
    long = (in_kz & (col("low") < col("_alo")) & (col("close") > col("_alo")) & (col("close") > col("open")) & ~taken_lo & ~both).fill_null(False)
    w = w.with_columns(
        col("_ahi").alias("kz_asia_high"), col("_alo").alias("kz_asia_low"),
        pl.when(london).then(pl.lit("london")).when(ny).then(pl.lit("new_york")).alias("kz_session"),
        short.alias("kz_short_signal"), long.alias("kz_long_signal"),
        pl.when(short).then(col("high") + p.sl_buffer_atr * col("atr")).alias("kz_short_sl"),
        pl.when(long).then(col("low") - p.sl_buffer_atr * col("atr")).alias("kz_long_sl"),
    )
    return w.drop([c for c in w.columns if c.startswith("_")])
