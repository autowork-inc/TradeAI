"""Стратегия E `VWAP_Reversion`: возврат к среднему от 3-го стандартного отклонения внутридневного Anchored VWAP (2026-10-11).

Векторный Polars (R2), без заглядывания в будущее (R3/R4), одна функция для бэктеста и live (R8).

Рабочий таймфрейм - M15 (исходный; эксперимент с M5 откатан 2026-10-15); сама математика от таймфрейма не зависит, меняются только окна в барах.
VWAP якорный: накопление с начала суток UTC (open_time // сутки), всё через cum_sum().over(сутки), поэтому значение в строке j зависит
только от свечей этих суток до j включительно. Цена свечи - типичная (high + low + close) / 3. Взвешенное по объёму стандартное
отклонение: sd = sqrt(max(Σ(v·p²)/Σv - vwap², 0)); полосы vwap ± 3·sd.
Суточный якорь требует, чтобы в кадре были ВСЕ свечи суток с 00:00 (иначе VWAP live по окну 300 баров разошёлся бы с бэктестом, R8): первая
свеча суток в кадре обязана иметь open_time = начало суток; частичные сутки (начало окна) сигналов не дают. Кроме того, нужно минимум
min_day_bars свечей суток (4 часа: 16 баров M15): на первых свечах sd вырожден, и «касание 3 sd» ничего не значит.
Сигнал на закрытой свече j (шорт; лонг - зеркало):
  1. касание верхней полосы: high >= vwap + 3·sd на j или на одной из touch_lookback - 1 предыдущих свечей (полоса - на той свече; 3 бара M15 = 45 минут);
  2. разворотный свечной паттерн на j: медвежье поглощение (бычья свеча j-1, медвежья j, тело j перекрывает тело j-1) или «падающая звезда»
     (верхняя тень >= 60% диапазона, тело <= 30% диапазона);
  3. close[j] ниже верхней полосы (цена вернулась внутрь).
  4. (2026-10-20) близость к экстремуму: (high[j] - close[j]) <= max_entry_to_ext_atr * ATR[j-1] (лонг - (close[j] - low[j])): вход не «посередине
     ничего», а рядом с хвостом свечи, т.е. рядом с зоной риска; закрылась далеко от хвоста - сигнал бракуется;
  5. (2026-10-20) тренд H4 жёсткий (features.htf.side_allowed, require_htf_trend): шорт только при Bearish, лонг только при Bullish;
     Neutral, встречный тренд и нет bias - запрет.
Вход MARKET, SL структурный: экстремум high (лонг - low) за последние swing_lookback свечей (включая j и окно касания) + 0.2*ATR, а не только
хвост сигнальной свечи. Размер позиции - 5 USDT риска, выход - один тейк 1:4 без безубытка (orchestrator/risk.py, registry.VWAP_PLAN);
значения vwap, sd и диагностика входа (близость к экстремуму, размер стопа, расстояния до полосы и VWAP, в ATR[j-1]) пишутся в meta сигнала.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN, side_allowed
from strategies.killzones import DAY_MS
from utils.errors import ConfigError, DataError

_REQUIRED = ("open_time", "open", "high", "low", "close", "volume", "atr")
SIDES = ("short", "long")


DAY_MARGIN_BARS = 12  # запас сверх одних суток при проверке глубины истории на старте


def min_history_bars(interval_ms: int) -> int:
    """Минимальная глубина истории кадра (`--history`), при которой суточный якорь всегда в кадре: сутки + запас (на M15: 96 + 12 = 108, на M5: 288 + 12 = 300).
    Короче - первая свеча суток выпадает из кадра, `_anchored` ложно, и сигналов нет молча."""
    return DAY_MS // interval_ms + DAY_MARGIN_BARS


@dataclass(frozen=True)
class VwapParams:
    """Окна заданы в БАРАХ рабочего таймфрейма. Значения по умолчанию - исходные, утверждённые 2026-10-11 для M15 (16 баров = 4 часа, 3 бара = 45 минут).
    Для другого таймфрейма с теми же календарными интервалами - `VwapParams.for_interval(interval_ms)` (M5: 48 и 9; эксперимент 2026-10-15 показал на 5m шум
    и убытки на комиссиях, стратегия возвращена на M15)."""

    sd_mult: float = 3.0
    min_day_bars: int = 16  # 4 часа накопления суток на M15
    touch_lookback: int = 3  # касание полосы за последние 45 минут на M15
    star_wick_frac: float = 0.6  # верхняя тень >= 60% диапазона свечи
    star_body_frac: float = 0.3  # тело <= 30% диапазона
    sl_buffer_atr: float = D.SL_BUFFER_ATR
    # ------------------------------------------------------------------ калибровка (стартовые допущения 2026-10-20)
    max_entry_to_ext_atr: float = 1.0  # вход (close) не дальше этой доли ATR[j-1] от хвоста свечи j (high для шорта, low для лонга)
    swing_lookback: int = 12  # структурный стоп: экстремум за последние N свечей (12 на M15 = 3 часа)
    require_htf_trend: bool = True  # жёсткий тренд H4 (колонка htf_bias); False - только для калибровки

    @classmethod
    def for_interval(cls, interval_ms: int, **overrides: float) -> "VwapParams":
        """Параметры с окнами в барах таймфрейма `interval_ms`: календарные интервалы те же, что у M15 (4 часа и 45 минут), остальное - по умолчанию или `overrides`."""
        scale = D.bars_scale(interval_ms)
        return cls(min_day_bars=cls.min_day_bars * scale, touch_lookback=cls.touch_lookback * scale,
                   swing_lookback=cls.swing_lookback * scale, **overrides)  # type: ignore[arg-type]

    def __post_init__(self) -> None:
        for name in ("min_day_bars", "touch_lookback", "swing_lookback"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigError(f"VwapParams.{name} должен быть целым >= 1, получено {value!r}")
        for name in ("sd_mult", "star_wick_frac", "star_body_frac", "sl_buffer_atr", "max_entry_to_ext_atr"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ConfigError(f"VwapParams.{name} должен быть положительным числом, получено {value!r}")
        if self.swing_lookback < self.touch_lookback:
            raise ConfigError(f"VwapParams.swing_lookback ({self.swing_lookback}) не может быть меньше touch_lookback ({self.touch_lookback}): стоп обязан закрывать окно касания")
        if not isinstance(self.require_htf_trend, bool):
            raise ConfigError(f"VwapParams.require_htf_trend должен быть bool, получено {self.require_htf_trend!r}")


def signal_columns() -> tuple[str, ...]:
    return ("vw_vwap", "vw_sd", "vw_upper", "vw_lower", "vw_short_signal", "vw_long_signal", "vw_short_sl", "vw_long_sl",
            "vw_short_ext_atr", "vw_long_ext_atr", "vw_short_stop_atr", "vw_long_stop_atr",
            "vw_short_band_atr", "vw_long_band_atr", "vw_short_vwap_atr", "vw_long_vwap_atr")


def detect_vwap_reversion(df: pl.DataFrame, params: VwapParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (нужны open_time, OHLC, volume, atr) -> тот же кадр + vw_* колонки (signal_columns)."""
    p = params or VwapParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: сначала features.smc.add_features")
    if p.require_htf_trend and BIAS_COLUMN not in df.columns:
        raise DataError(f"Нет колонки {BIAS_COLUMN}: VWAP_Reversion читает тренд H4 (HtfBias.attach)")
    clash = [c for c in signal_columns() if c in df.columns]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    col = pl.col
    typical = (col("high") + col("low") + col("close")) / 3
    w = df.with_columns(
        (col("open_time") // DAY_MS).alias("_day"), typical.alias("_tp"),
    ).with_columns(
        col("volume").cum_sum().over("_day").alias("_cv"),
        (col("_tp") * col("volume")).cum_sum().over("_day").alias("_cpv"),
        (col("_tp") * col("_tp") * col("volume")).cum_sum().over("_day").alias("_cpv2"),
        pl.int_range(pl.len(), dtype=pl.Int64).over("_day").alias("_n"),  # номер свечи в сутках с нуля
        (col("open_time").first().over("_day") == col("_day") * DAY_MS).alias("_anchored"),  # кадр содержит начало суток
    ).with_columns(
        pl.when(col("_cv") > 0).then(col("_cpv") / col("_cv")).alias("vw_vwap"),
    ).with_columns(
        pl.when(col("_cv") > 0).then(((col("_cpv2") / col("_cv")) - col("vw_vwap") ** 2).clip(lower_bound=0.0).sqrt()).alias("vw_sd"),
    )
    w = w.with_columns(
        (col("vw_vwap") + p.sd_mult * col("vw_sd")).alias("vw_upper"),
        (col("vw_vwap") - p.sd_mult * col("vw_sd")).alias("vw_lower"),
    )
    usable = col("_anchored") & (col("_n") + 1 >= p.min_day_bars) & (col("vw_sd") > 0)
    rng = col("high") - col("low")
    body = (col("close") - col("open")).abs()
    upper_wick = col("high") - pl.max_horizontal("open", "close")
    lower_wick = pl.min_horizontal("open", "close") - col("low")
    o1, c1 = col("open").shift(1), col("close").shift(1)
    bear_engulf = ((col("close") < col("open")) & (c1 > o1) & (col("open") >= c1) & (col("close") <= o1)).fill_null(False)
    bull_engulf = ((col("close") > col("open")) & (c1 < o1) & (col("open") <= c1) & (col("close") >= o1)).fill_null(False)
    star = ((rng > 0) & (upper_wick >= p.star_wick_frac * rng) & (body <= p.star_body_frac * rng)).fill_null(False)
    hammer = ((rng > 0) & (lower_wick >= p.star_wick_frac * rng) & (body <= p.star_body_frac * rng)).fill_null(False)
    # касание полосы на этой или предыдущих touch_lookback - 1 свечах (флаг считается свечой со своей полосой)
    touch_up = (usable & (col("high") >= col("vw_upper"))).fill_null(False).cast(pl.Int64)
    touch_dn = (usable & (col("low") <= col("vw_lower"))).fill_null(False).cast(pl.Int64)
    w = w.with_columns(
        (touch_up.rolling_sum(window_size=p.touch_lookback, min_samples=1) > 0).alias("_touch_up"),
        (touch_dn.rolling_sum(window_size=p.touch_lookback, min_samples=1) > 0).alias("_touch_dn"),
        col("high").rolling_max(window_size=p.touch_lookback, min_samples=1).alias("_hi_win"),
        col("low").rolling_min(window_size=p.touch_lookback, min_samples=1).alias("_lo_win"),
        col("high").rolling_max(window_size=p.swing_lookback, min_samples=1).alias("_swing_hi"),
        col("low").rolling_min(window_size=p.swing_lookback, min_samples=1).alias("_swing_lo"),
    )
    atr_prev = col("atr").shift(1)  # ATR[j-1]: без заглядывания в свечу j (D3)
    ext_short = (col("high") - col("close")) / atr_prev  # вход (close) - хвост свечи
    ext_long = (col("close") - col("low")) / atr_prev
    near_short = (ext_short <= p.max_entry_to_ext_atr).fill_null(False)
    near_long = (ext_long <= p.max_entry_to_ext_atr).fill_null(False)
    trend_short = side_allowed("short") if p.require_htf_trend else pl.lit(True)
    trend_long = side_allowed("long") if p.require_htf_trend else pl.lit(True)
    short = (usable & col("_touch_up") & (bear_engulf | star) & (col("close") < col("vw_upper")) & near_short & trend_short).fill_null(False)
    long = (usable & col("_touch_dn") & (bull_engulf | hammer) & (col("close") > col("vw_lower")) & near_long & trend_long).fill_null(False)
    sl_short = col("_swing_hi") + p.sl_buffer_atr * col("atr")
    sl_long = col("_swing_lo") - p.sl_buffer_atr * col("atr")
    w = w.with_columns(
        short.alias("vw_short_signal"), long.alias("vw_long_signal"),
        pl.when(short).then(sl_short).alias("vw_short_sl"),
        pl.when(long).then(sl_long).alias("vw_long_sl"),
        ext_short.alias("vw_short_ext_atr"), ext_long.alias("vw_long_ext_atr"),
        ((sl_short - col("close")) / atr_prev).alias("vw_short_stop_atr"), ((col("close") - sl_long) / atr_prev).alias("vw_long_stop_atr"),
        ((col("vw_upper") - col("close")) / atr_prev).alias("vw_short_band_atr"), ((col("close") - col("vw_lower")) / atr_prev).alias("vw_long_band_atr"),
        ((col("close") - col("vw_vwap")) / atr_prev).alias("vw_short_vwap_atr"), ((col("vw_vwap") - col("close")) / atr_prev).alias("vw_long_vwap_atr"),
    )
    return w.drop([c for c in w.columns if c.startswith("_")])
