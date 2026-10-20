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
Фильтры качества (2026-10-20, все пороги - в KillzoneParams для калибровки):
  * тренд H4 жёсткий (features.htf.side_allowed): шорт (свип Asia High) только при Bearish, лонг (свип Asia Low) только при Bullish;
    Neutral и нет bias - запрет; выключатель require_htf_trend нужен только для калибровки и бэктеста;
  * ширина Азии: (Asia High - Asia Low) >= min_asia_range_atr * ATR[j-1] - узкий диапазон ликвидности не накопил;
  * глубина прокола: (high[j] - Asia High) >= min_sweep_depth_atr * ATR[j-1] (лонг - зеркало);
  * отвержение: верхняя тень шорт-свечи (high - max(open, close)) >= min_wick_frac от размаха high - low (лонг - нижняя тень).
Вход MARKET, SL = экстремум прокола + 0.2*ATR (за хай/лоу свечи свипа). Размер позиции - фиксированные 5 USDT риска (orchestrator/risk.py),
выход - один тейк на весь объём строго 1:4 (registry.STRICT_PLAN), без безубытка, частичных фиксаций и трейлинга; стратегия плана не задаёт.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN, side_allowed
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
    # ------------------------------------------------------------------ калибровка (стартовые допущения 2026-10-20)
    require_htf_trend: bool = True  # жёсткий тренд H4 (колонка htf_bias); False - только для калибровки
    min_asia_range_atr: float = 2.0  # ширина Азии >= этой доли ATR[j-1] (0 - выключено)
    min_sweep_depth_atr: float = 0.15  # глубина прокола уровня >= этой доли ATR[j-1] (0 - выключено)
    min_wick_frac: float = 0.55  # тень в сторону прокола >= этой доли размаха свечи (0 - выключено)

    def __post_init__(self) -> None:
        for name, (a, b) in (("asia", (self.asia_start_h, self.asia_end_h)), ("london", self.london), ("new_york", self.new_york)):
            if not (0 <= a < b <= 24):
                raise ConfigError(f"KillzoneParams.{name}: нужны часы 0 <= начало < конец <= 24, получено ({a}, {b})")
        if self.asia_end_h > self.london[0] or self.london[1] > self.new_york[0]:
            raise ConfigError("KillzoneParams: порядок Азия -> Лондон -> Нью-Йорк, окна не пересекаются (диапазон Азии должен закрыться до killzone)")
        if not (isinstance(self.sl_buffer_atr, (int, float)) and self.sl_buffer_atr > 0):
            raise ConfigError(f"KillzoneParams.sl_buffer_atr должен быть положительным, получено {self.sl_buffer_atr!r}")
        if not isinstance(self.require_htf_trend, bool):
            raise ConfigError(f"KillzoneParams.require_htf_trend должен быть bool, получено {self.require_htf_trend!r}")
        for name in ("min_asia_range_atr", "min_sweep_depth_atr", "min_wick_frac"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ConfigError(f"KillzoneParams.{name} должен быть числом >= 0, получено {v!r}")
        if self.min_wick_frac > 1:
            raise ConfigError(f"KillzoneParams.min_wick_frac - доля размаха свечи, 0..1, получено {self.min_wick_frac!r}")


def signal_columns() -> tuple[str, ...]:
    return ("kz_asia_high", "kz_asia_low", "kz_session", "kz_short_signal", "kz_long_signal", "kz_short_sl", "kz_long_sl",
            "kz_range_atr", "kz_short_depth_atr", "kz_long_depth_atr", "kz_short_wick", "kz_long_wick")


def detect_killzones(df: pl.DataFrame, interval_ms: int, params: KillzoneParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (нужны open_time, OHLC, atr) -> тот же кадр + kz_* колонки (signal_columns)."""
    p = params or KillzoneParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: сначала features.smc.add_features")
    if p.require_htf_trend and BIAS_COLUMN not in df.columns:
        raise DataError(f"Нет колонки {BIAS_COLUMN}: ICT_Killzones читает тренд H4 (HtfBias.attach)")
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
    atr_prev = col("atr").shift(1)  # ATR[j-1]: без заглядывания в свечу j (D3)
    span = col("high") - col("low")
    range_atr = (col("_ahi") - col("_alo")) / atr_prev
    depth_hi = (col("high") - col("_ahi")) / atr_prev
    depth_lo = (col("_alo") - col("low")) / atr_prev
    wick_hi = pl.when(span > 0).then((col("high") - pl.max_horizontal("open", "close")) / span).otherwise(0.0)
    wick_lo = pl.when(span > 0).then((pl.min_horizontal("open", "close") - col("low")) / span).otherwise(0.0)
    wide = (range_atr >= p.min_asia_range_atr).fill_null(False)
    trend_short = side_allowed("short") if p.require_htf_trend else pl.lit(True)
    trend_long = side_allowed("long") if p.require_htf_trend else pl.lit(True)
    short = (in_kz & (col("high") > col("_ahi")) & (col("close") < col("_ahi")) & (col("close") < col("open")) & ~taken_hi & ~both
             & wide & trend_short & (depth_hi >= p.min_sweep_depth_atr).fill_null(False) & (wick_hi >= p.min_wick_frac)).fill_null(False)
    long = (in_kz & (col("low") < col("_alo")) & (col("close") > col("_alo")) & (col("close") > col("open")) & ~taken_lo & ~both
            & wide & trend_long & (depth_lo >= p.min_sweep_depth_atr).fill_null(False) & (wick_lo >= p.min_wick_frac)).fill_null(False)
    w = w.with_columns(
        col("_ahi").alias("kz_asia_high"), col("_alo").alias("kz_asia_low"),
        pl.when(london).then(pl.lit("london")).when(ny).then(pl.lit("new_york")).alias("kz_session"),
        short.alias("kz_short_signal"), long.alias("kz_long_signal"),
        pl.when(short).then(col("high") + p.sl_buffer_atr * col("atr")).alias("kz_short_sl"),
        pl.when(long).then(col("low") - p.sl_buffer_atr * col("atr")).alias("kz_long_sl"),
        range_atr.alias("kz_range_atr"), depth_hi.alias("kz_short_depth_atr"), depth_lo.alias("kz_long_depth_atr"),
        wick_hi.alias("kz_short_wick"), wick_lo.alias("kz_long_wick"),
    )
    return w.drop([c for c in w.columns if c.startswith("_")])
