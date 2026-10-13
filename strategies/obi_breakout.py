"""Стратегия C `OBI_Breakout`: импульсный пробой локального экстремума (BOS), подтверждённый микроструктурой (2026-10-11).

Откаты и свипы игнорируются. Векторный Polars (R2), без заглядывания в будущее (R3/R4).

Структурная часть (бар j, лонг; шорт - зеркало):
  * BOS: close[j] > последнего подтверждённого Swing High, известного ДО бара j (sh1_price[j-1]), а close[j-1] <= этого уровня (событие
    пересечения, а не состояние - как О11 у Breakout Buildup);
  * импульс: тело свечи (close - open) > impulse_atr_mult (1.0, как порог импульса FVG, О12) * ATR[j-1].
Подтверждение потоками реального времени (колонки добавляет orchestrator/microstructure.py::MicroStructure.attach, только на ПОСЛЕДНЕЙ
строке - у истории нет ни стакана, ни ленты; значение в остальных строках null):
  * `ms_obi` - Order Book Imbalance по топ-10 уровням (depth10@500ms): среднее по снимкам за последние obi_window_s секунд свечи
    (bid_qty - ask_qty) / (bid_qty + ask_qty), от -1 до +1; лонг требует ms_obi >= obi_min (0.25), шорт - <= -obi_min;
  * `ms_delta` - дельта ленты (aggTrade) за свечу: (объём покупок агрессором - объём продаж) / общий объём; «всплеск» - |ms_delta| >=
    delta_min (0.30) при объёме свечи >= delta_volume_mult (1.5) * SMA20.
  Достаточно одного из двух (OBI ИЛИ дельта). Нет данных микроструктуры (null) - подтверждения нет, сигнала нет.
Стратегию нельзя проверить на истории свечей: стакана и ленты в истории нет, поэтому она набирает статистику только в Shadow.
Вход MARKET, SL = экстремум пробойной свечи -+ 0.2*ATR, управление - Scale Out по умолчанию.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from utils.errors import ConfigError, DataError

OBI_COLUMN = "ms_obi"
DELTA_COLUMN = "ms_delta"
MICRO_COLUMNS = (OBI_COLUMN, DELTA_COLUMN)
SIDES = ("long", "short")
_REQUIRED = ("open", "high", "low", "close", "volume", "atr", "vol_ratio", "sh1_price", "sl1_price") + MICRO_COLUMNS


@dataclass(frozen=True)
class ObiParams:
    impulse_atr_mult: float = D.IMPULSE_ATR_MULT
    obi_min: float = 0.25
    delta_min: float = 0.30
    delta_volume_mult: float = 1.5
    sl_buffer_atr: float = D.SL_BUFFER_ATR

    def __post_init__(self) -> None:
        for name in ("impulse_atr_mult", "obi_min", "delta_min", "delta_volume_mult", "sl_buffer_atr"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ConfigError(f"ObiParams.{name} должен быть положительным числом, получено {value!r}")
        if self.obi_min >= 1 or self.delta_min >= 1:
            raise ConfigError("ObiParams: obi_min и delta_min - доли в (0, 1)")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"obi_{side}_{n}" for n in ("signal", "level", "sl", "via_obi", "via_delta"))


def _detect_side(df: pl.DataFrame, side: str, p: ObiParams) -> pl.DataFrame:
    col = pl.col
    if side == "long":
        level = col("sh1_price").shift(1)
        crossed = (col("close") > level) & (col("close").shift(1) <= level)
        impulse = (col("close") - col("open")) > p.impulse_atr_mult * col("atr").shift(1)
        obi_ok = col(OBI_COLUMN) >= p.obi_min
        delta_ok = col(DELTA_COLUMN) >= p.delta_min
        sl = col("low") - p.sl_buffer_atr * col("atr")
    else:
        level = col("sl1_price").shift(1)
        crossed = (col("close") < level) & (col("close").shift(1) >= level)
        impulse = (col("open") - col("close")) > p.impulse_atr_mult * col("atr").shift(1)
        obi_ok = col(OBI_COLUMN) <= -p.obi_min
        delta_ok = col(DELTA_COLUMN) <= -p.delta_min
        sl = col("high") + p.sl_buffer_atr * col("atr")
    delta_spike = delta_ok & (col("vol_ratio") >= p.delta_volume_mult)
    bos = (crossed & impulse).fill_null(False)
    via_obi, via_delta = obi_ok.fill_null(False), delta_spike.fill_null(False)
    signal = bos & (via_obi | via_delta)
    return df.select(
        signal.alias(f"obi_{side}_signal"),
        pl.when(signal).then(level).alias(f"obi_{side}_level"),
        pl.when(signal).then(sl).alias(f"obi_{side}_sl"),
        (signal & via_obi).alias(f"obi_{side}_via_obi"),
        (signal & via_delta).alias(f"obi_{side}_via_delta"),
    )


def detect_obi_breakout(df: pl.DataFrame, params: ObiParams | None = None) -> pl.DataFrame:
    """Кадр с признаками и колонками ms_obi / ms_delta -> тот же кадр + obi_long_* / obi_short_* (signal_columns)."""
    p = params or ObiParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: нужны признаки features.smc.add_features и микроструктура (MicroStructure.attach)")
    clash = [c for side in SIDES for c in signal_columns(side) if c in df.columns]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    out = df
    for side in SIDES:
        out = out.hstack(_detect_side(df, side, p).get_columns())
    return out
