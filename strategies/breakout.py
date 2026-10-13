"""Стратегия 2 (Breakout Buildup, MARKET, только лонг): триггер пробоя поджатия. Векторный Polars (R2).

Поджатие (features.smc: `buildup`, `buildup_level`, `buildup_low3`) должно быть известно ДО бара пробоя:
берутся значения строки j-1 (иначе свинг, подтверждённый на самой пробойной свече, подсматривал бы её).

Триггер на баре j закрытой свечи (R5):
  * buildup[j-1] и close[j] > buildup_level[j-1] - пробой максимума трёх равных хаёв;
  * close[j-1] <= buildup_level[j-1] - это ПЕРЕСЕЧЕНИЕ уровня; без него сигнал повторялся бы на каждой свече над
    уровнем (О11, пояснение к разд. 3.6: «триггер» - событие, а не состояние);
  * volume[j] > 2 * SMA20(volume) по 20 предыдущим барам (vol_ratio > 2, пробойный бар в среднее не входит).
SL = buildup_low3[j-1] - 0.2*ATR[j] (под последним свинг-лоу). Вход MARKET по open следующего бара: R, TP = 3R и
проверка плеча (О8) считаются симулятором по фактической цене входа. «Пробой до подтверждения 3-го хая
пропускается» (разд. 3.6 п.7) выполняется автоматически: до подтверждения buildup ложен.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from utils.errors import ConfigError, DataError


@dataclass(frozen=True)
class BreakoutParams:
    volume_mult: float = 2.0  # разд. 3.6 п.4
    sl_buffer_atr: float = D.SL_BUFFER_ATR  # О5

    def __post_init__(self) -> None:
        if not (self.volume_mult > 0 and self.sl_buffer_atr >= 0):
            raise ConfigError("volume_mult > 0 и sl_buffer_atr >= 0")


BREAKOUT_COLUMNS = ("breakout_signal", "breakout_level", "breakout_sl")


def detect_breakout(df: pl.DataFrame, params: BreakoutParams | None = None) -> pl.DataFrame:
    p = params or BreakoutParams()
    need = ("close", "volume", "atr", "buildup", "buildup_level", "buildup_low3", "vol_ratio")
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок признаков {missing}: сначала features.smc.add_features")
    clash = [c for c in BREAKOUT_COLUMNS if c in df.columns]
    if clash:
        raise DataError(f"Колонки Breakout уже есть в кадре: {clash}")
    level = pl.col("buildup_level").shift(1)
    armed = pl.col("buildup").shift(1)
    cross = (pl.col("close") > level) & (pl.col("close").shift(1) <= level)
    trigger = (armed & cross & (pl.col("vol_ratio") > p.volume_mult)).fill_null(False)
    sl = pl.col("buildup_low3").shift(1) - p.sl_buffer_atr * pl.col("atr")
    return df.with_columns(
        trigger.alias("breakout_signal"),
        pl.when(trigger).then(level).alias("breakout_level"),
        pl.when(trigger).then(sl).alias("breakout_sl"),
    )
