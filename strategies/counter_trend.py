"""Стратегия B `SMC_Counter_Trend`: контртрендовый отскок после ГЛУБОКОГО свипа ликвидности на аномальном объёме (2026-10-11).

Векторный Polars (R2); значение в строке j зависит только от строк <= j (R3/R4), одна функция для бэктеста и live (R8).

Условия на закрытой свече j (для шорта; лонг - зеркало цен, как в strategies/smc_setups.py):
  1. пул ликвидности - последний подтверждённый Swing High (sh1), ещё не пробитый (та же проверка, что у S0/S1 автомата SMC);
  2. свип: high[j] > пул, close[j] < пул (проколол и закрылся обратно под уровнем);
  3. ГЛУБОКИЙ: (high[j] - пул) / ATR[j-1] >= min_sweep_atr (1.5; изначально 2.5-3 ATR - на истории почти не срабатывало, смягчено 2026-10-11). ATR предыдущего бара - как в D3: волатильность
     замеряется до самой свечи прокола, иначе её собственный размах раздувал бы ATR и занижал глубину;
  4. аномальный объём разворотной свечи: volume[j] > volume_mult (1.5; было 2.5) * SMA20(volume) по ПРЕДЫДУЩИМ 20 барам (vol_sma_prev, без j);
  5. разворотная свеча: close[j] < open[j] (для лонга - наоборот);
  6. СТРОГО ПРОТИВ тренда H4: шорт - только при Bullish (htf_bias = +1), лонг - только при Bearish (-1); Neutral и отсутствие bias
     (null) - запрет, в отличие от Pro_Trend, где Neutral разрешён.
Вход MARKET по первому тику после закрытия свечи (Оркестратор), SL = экстремум прокола + 0.2*ATR, тейк - скальп 1R (план управления
позицией задаёт реестр стратегий: orchestrator/registry.py). Плечо проверяет RiskSizer трекера (потолок 5.0), отказ пишется в журнал.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from features.htf import BEARISH, BIAS_COLUMN, BULLISH
from strategies.smc_setups import SIDES, _mirrored
from utils.errors import ConfigError, DataError

_REQUIRED = ("open", "high", "low", "close", "volume", "atr", "vol_sma_prev", "swing_high", "swing_low", "sh1_price", "sh1_time",
             "sl1_price", "sl1_time", BIAS_COLUMN)


@dataclass(frozen=True)
class CounterTrendParams:
    min_sweep_atr: float = 1.5  # глубина прокола пула в ATR[j-1]; было 2.5 (на истории 1 сигнал за 6 мес. на 8 монетах), смягчено 2026-10-11 по решению пользователя
    volume_mult: float = 1.5  # объём разворотной свечи > volume_mult * SMA20 предыдущих баров; было 2.5
    sl_buffer_atr: float = D.SL_BUFFER_ATR

    def __post_init__(self) -> None:
        for name in ("min_sweep_atr", "volume_mult", "sl_buffer_atr"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ConfigError(f"CounterTrendParams.{name} должен быть положительным числом, получено {value!r}")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"ct_{side}_{n}" for n in ("signal", "pool", "extreme", "depth_atr", "vol_ratio", "sl"))


def _detect_side(df: pl.DataFrame, side: str, p: CounterTrendParams) -> pl.DataFrame:
    sgn = 1.0 if side == "short" else -1.0
    m = _mirrored(side)
    col = pl.col
    w = df.select(
        m["o"].alias("o"), m["h"].alias("h"), m["c"].alias("c"), col("atr"), col("volume"), col("vol_sma_prev"),
        m["pool_flag"].cast(pl.Int64).cum_sum().alias("grp"), m["pool_price"].shift(1).alias("L"), col(BIAS_COLUMN).alias("bias"),
    )
    w = w.with_columns(col("h").cum_max().over("grp").alias("grp_max"))
    taken_prev = (col("grp_max").shift(1) > col("L")).fill_null(True)  # пул уже пробит до бара j
    sweep = ((col("h") > col("L")) & (col("c") < col("L")) & ~taken_prev).fill_null(False)
    depth = (col("h") - col("L")) / col("atr").shift(1)  # в ATR предыдущего бара
    volume_ok = ((col("vol_sma_prev") > 0) & (col("volume") > p.volume_mult * col("vol_sma_prev"))).fill_null(False)
    against_trend = (col("bias") == (BULLISH if side == "short" else BEARISH)).fill_null(False)  # строго против H4
    signal = sweep & (depth >= p.min_sweep_atr).fill_null(False) & volume_ok & (col("c") < col("o")) & against_trend
    sl_m = col("h") + p.sl_buffer_atr * col("atr")
    return w.select(
        signal.alias(f"ct_{side}_signal"),
        pl.when(signal).then(col("L") * sgn).alias(f"ct_{side}_pool"),
        pl.when(signal).then(col("h") * sgn).alias(f"ct_{side}_extreme"),
        pl.when(signal).then(depth).alias(f"ct_{side}_depth_atr"),
        pl.when(signal).then(col("volume") / col("vol_sma_prev")).alias(f"ct_{side}_vol_ratio"),
        pl.when(signal).then(sl_m * sgn).alias(f"ct_{side}_sl"),
    )


def detect_counter_trend(df: pl.DataFrame, params: CounterTrendParams | None = None) -> pl.DataFrame:
    """Кадр с признаками и колонкой htf_bias -> тот же кадр + ct_short_* / ct_long_* (signal_columns)."""
    p = params or CounterTrendParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: нужны признаки features.smc.add_features и htf_bias (HtfBias.attach)")
    clash = [c for side in SIDES for c in signal_columns(side) if c in df.columns]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    out = df
    for side in SIDES:  # две стороны, не строки
        out = out.hstack(_detect_side(df, side, p).get_columns())
    return out
