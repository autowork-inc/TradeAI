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

Защита от входа «на дне / пике» импульса (2026-10-15). MARKET по закрытию аномально большой свечи (дамп на 4%+) - это вход в конце движения: стоп за экстремум свечи
огромный, +1R недостижим. Поэтому пробой отклоняется, если:
  * перерастянут: |close - уровень BOS| > max_bos_distance_atr (1.5) * ATR[j-1] - цена уже далеко от пробитого уровня, поздно входить;
  * свеча истощения: тело > max_body_atr (2.5) * ATR[j-1].
ATR берётся ДО пробойной свечи (как у порога импульса): ATR[j] включает истинный диапазон самой аномальной свечи и ослаблял бы собственный фильтр.
Опционально (`require_buildup`, по умолчанию выключено - меняет частоту сигналов) пробою должно предшествовать поджатие у уровня: хотя бы buildup_touches (2) бара из последних
buildup_lookback (16) баров с экстремумом в пределах buildup_touch_atr (0.3) * ATR[j-1] от уровня (сам свинг-бар считается касанием) ИЛИ сжатие волатильности
ATR[j-1] <= buildup_atr_ratio (0.9) * ATR[j-1-buildup_lookback]. Окно касаний - сумма сдвигов на константные смещения (как скользящие средние в features/smc.py), не итерация по строкам.
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
    max_bos_distance_atr: float = 1.5  # Max BOS Distance Guard: закрытие дальше 1.5 ATR от пробитого уровня - пробой перерастянут
    max_body_atr: float = 2.5  # Max Candle Body Guard: тело пробойной свечи больше 2.5 ATR - свеча истощения
    require_buildup: bool = False  # опциональная проверка поджатия перед пробоем (см. докстринг модуля)
    buildup_lookback: int = 16
    buildup_touches: int = 2
    buildup_touch_atr: float = 0.3
    buildup_atr_ratio: float = 0.9

    def __post_init__(self) -> None:
        for name in ("impulse_atr_mult", "obi_min", "delta_min", "delta_volume_mult", "sl_buffer_atr", "max_bos_distance_atr", "max_body_atr",
                     "buildup_touch_atr", "buildup_atr_ratio"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ConfigError(f"ObiParams.{name} должен быть положительным числом, получено {value!r}")
        if self.obi_min >= 1 or self.delta_min >= 1:
            raise ConfigError("ObiParams: obi_min и delta_min - доли в (0, 1)")
        if not isinstance(self.require_buildup, bool):
            raise ConfigError(f"ObiParams.require_buildup должен быть bool, получено {self.require_buildup!r}")
        for name, low in (("buildup_lookback", 2), ("buildup_touches", 1)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < low:
                raise ConfigError(f"ObiParams.{name} должен быть целым >= {low}, получено {value!r}")
        if self.buildup_touches > self.buildup_lookback:
            raise ConfigError("ObiParams: buildup_touches не может превышать buildup_lookback")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"obi_{side}_{n}" for n in ("signal", "level", "sl", "via_obi", "via_delta"))


def _touches(level: pl.Expr, extreme: str, atr_before: pl.Expr, p: ObiParams) -> pl.Expr:
    """Сколько из последних buildup_lookback баров ДО пробойного имели экстремум (`high` для лонга, `low` для шорта) в пределах buildup_touch_atr * ATR от уровня.
    Сумма по константным смещениям 1..lookback (не по строкам): значение в строке j зависит только от строк < j (R3)."""
    tol = p.buildup_touch_atr * atr_before
    total = pl.lit(0, dtype=pl.Int64)
    for s in range(1, p.buildup_lookback + 1):
        total = total + ((pl.col(extreme).shift(s) - level).abs() <= tol).fill_null(False).cast(pl.Int64)
    return total


def _detect_side(df: pl.DataFrame, side: str, p: ObiParams) -> pl.DataFrame:
    col = pl.col
    atr_before = col("atr").shift(1)
    if side == "long":
        level = col("sh1_price").shift(1)
        crossed = (col("close") > level) & (col("close").shift(1) <= level)
        body = col("close") - col("open")
        obi_ok = col(OBI_COLUMN) >= p.obi_min
        delta_ok = col(DELTA_COLUMN) >= p.delta_min
        sl = col("low") - p.sl_buffer_atr * col("atr")
        extreme = "high"
    else:
        level = col("sl1_price").shift(1)
        crossed = (col("close") < level) & (col("close").shift(1) >= level)
        body = col("open") - col("close")
        obi_ok = col(OBI_COLUMN) <= -p.obi_min
        delta_ok = col(DELTA_COLUMN) <= -p.delta_min
        sl = col("high") + p.sl_buffer_atr * col("atr")
        extreme = "low"
    impulse = body > p.impulse_atr_mult * atr_before
    not_overstretched = (col("close") - level).abs() <= p.max_bos_distance_atr * atr_before  # Max BOS Distance Guard
    not_exhausted = body <= p.max_body_atr * atr_before  # Max Candle Body Guard
    delta_spike = delta_ok & (col("vol_ratio") >= p.delta_volume_mult)
    bos = (crossed & impulse & not_overstretched & not_exhausted).fill_null(False)
    if p.require_buildup:
        squeezed = atr_before <= p.buildup_atr_ratio * col("atr").shift(1 + p.buildup_lookback)
        bos = bos & ((_touches(level, extreme, atr_before, p) >= p.buildup_touches) | squeezed).fill_null(False)
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
