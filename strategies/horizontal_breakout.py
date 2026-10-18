"""Стратегия `Horizontal_Breakout`: пробой плотной горизонтальной консолидации по старшему тренду (2026-10-18, только Shadow).

Источник гипотезы - разметка 866 графиков нейросетью (`results.jsonl`, scripts/analyze_better.py): самый частый тип - horizontal_breakout
(405 из 866): консолидация перед пробоем в 98%, медиана 25 баров (IQR 14-45), закрытие за уровнем в 87%, импульсная свеча в 62%,
многократный тест уровня в 21%, всплеск объёма поднимает оценку разметки (6.66 против 5.55), шорт в медвежьем контексте - 200 из 237,
стоп за локальным свингом / границей консолидации (360 из 405), дистанция стопа 0.5-1.0% (медиана 0.76%). Исходов сделок в разметке нет -
это гипотеза, проверяемая статистикой Shadow, а не доказанное преимущество.

Векторный Polars (R2), без заглядывания в будущее (R3/R4): все величины коробки берутся по барам j-W .. j-1 (до пробойной свечи), ATR -
ATR[j-1] (ATR[j] уже содержит размах самой пробойной свечи и ослаблял бы фильтры). Окна - сумма сдвигов на константные смещения или
rolling_max/min (как в features/smc.py), не итерация по строкам.

Коробка (окно W = window баров до пробойной свечи j), лонг (шорт - зеркало):
  box_hi = max high[j-W..j-1], box_lo = min low[j-W..j-1], высота H = box_hi - box_lo.
Фильтры плотности консолидации (все обязательны):
  * размер:      min_box_atr <= H / ATR[j-1] <= max_box_atr - не «точка» и не тренд (у случайного блуждания за 24 бара H ~ 5-6 ATR);
  * боковик:     эффективность Кауфмана ER = |close[j-1] - close[j-1-W]| / sum |close[i] - close[i-1]| <= max_efficiency
                 (1 - прямая, ~1/sqrt(W) - случайное блуждание, меньше - возврат к середине);
  * уровень:     касаний верхней границы (high в пределах touch_atr * ATR[j-1] от box_hi) >= min_touches, и между первым и последним
                 касанием >= min_touch_span баров - уровень протестирован в разное время, а не одной широкой свечой.
Триггер на закрытой свече j:
  * закрытие за уровнем: close[j] >= box_hi + min_break_atr * ATR[j-1]; прошлая свеча закрылась внутри прошлой коробки (событие, О11);
  * импульс без истощения: min_body_atr <= тело / ATR[j-1] <= max_body_atr, close - box_hi <= max_extension_atr * ATR[j-1];
  * закрытие у экстремума: CLV = (close - low) / (high - low) >= min_clv (для шорта (high - close) / (high - low));
  * объём: vol_ratio (объём / SMA20 предыдущих) >= volume_mult;
  * старший тренд (htf_filter): лонг не при Bearish H4, шорт не при Bullish (features/htf.py), нет bias - запрет (fail-closed).
Стоп: экстремум последних stop_lookback баров до пробоя и самой пробойной свечи (локальный свинг перед пробоем) -+ sl_buffer_atr * ATR[j-1].
Риск в ATR вне [min_risk_atr, max_risk_atr] - сигнал отклоняется (стоп не подгоняется, как О8 с плечом).
Качество (0..1, среднее семи компонент, пишется в meta для разбора статистики Shadow; порог min_quality, по умолчанию 0 - не фильтрует):
  касания, боковик (1 - ER / max_efficiency), плотность (средний размах бара / высота коробки), объём, закрытие (CLV), сжатие
  волатильности (ATR[j-1] / ATR[j-1-W]) и согласие с трендом (1 - по тренду, 0.5 - Neutral).
Вход MARKET; план выхода - orchestrator/registry.py (LEVEL_BREAKOUT_PLAN).
"""

from dataclasses import dataclass, fields

import polars as pl

from features import defaults as D
from features.htf import BEARISH, BIAS_COLUMN, BULLISH, side_allowed
from utils.errors import ConfigError, DataError

SIDES = ("long", "short")
_REQUIRED = ("open_time", "open", "high", "low", "close", "atr", "vol_ratio")
BOX_COLUMNS = ("hb_box_hi", "hb_box_lo", "hb_box_t1", "hb_box_atr", "hb_efficiency", "hb_density")


@dataclass(frozen=True)
class HorizontalParams:
    window: int = 24  # баров консолидации (разметка: медиана 25, IQR 14-45)
    min_box_atr: float = 1.5
    max_box_atr: float = 5.0
    max_efficiency: float = 0.4
    touch_atr: float = 0.25
    min_touches: int = 3
    min_touch_span: int = 6
    min_break_atr: float = 0.05
    max_extension_atr: float = 1.2
    min_body_atr: float = 0.5
    max_body_atr: float = 2.5
    min_clv: float = 0.6
    volume_mult: float = 1.8
    htf_filter: bool = True
    stop_lookback: int = 6
    sl_buffer_atr: float = D.SL_BUFFER_ATR
    min_risk_atr: float = 0.5
    max_risk_atr: float = 3.0
    min_quality: float = 0.0

    def __post_init__(self) -> None:
        for f in fields(self):
            v = getattr(self, f.name)
            if f.type in (int, "int"):
                if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                    raise ConfigError(f"HorizontalParams.{f.name} должен быть целым >= 1, получено {v!r}")
            elif f.type in (bool, "bool"):
                if not isinstance(v, bool):
                    raise ConfigError(f"HorizontalParams.{f.name} должен быть bool, получено {v!r}")
            elif isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ConfigError(f"HorizontalParams.{f.name} должен быть числом >= 0, получено {v!r}")
        if self.window < 4 or self.min_touches > self.window or self.min_touch_span >= self.window or self.stop_lookback > self.window:
            raise ConfigError("HorizontalParams: window >= 4, min_touches <= window, min_touch_span < window, stop_lookback <= window")
        if not (self.min_box_atr < self.max_box_atr and self.min_body_atr < self.max_body_atr and self.min_risk_atr < self.max_risk_atr):
            raise ConfigError("HorizontalParams: нижние границы должны быть меньше верхних (box / body / risk)")
        if not (0 < self.max_efficiency <= 1 and 0 <= self.min_clv < 1 and 0 <= self.min_quality <= 1 and self.volume_mult > 0):
            raise ConfigError("HorizontalParams: max_efficiency в (0, 1], min_clv в [0, 1), min_quality в [0, 1], volume_mult > 0")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"hb_{side}_{n}" for n in ("signal", "level", "sl", "quality", "touches", "touch_span", "clv", "risk_atr"))


def _shift_sum(expr_at, n: int, start: int = 1) -> pl.Expr:
    """sum_{s=start}^{start+n-1} expr_at(s): фиксированный порядок сложения - результат не зависит от длины кадра (R4)."""
    total = expr_at(start)
    for s in range(start + 1, start + n):
        total = total + expr_at(s)
    return total

_T = "_hb_"  # промежуточные колонки: расчёт этапами (материализация), иначе дерево выражений дублируется в каждом выходе и планирование дороже расчёта


def _c(name: str) -> pl.Expr:
    return pl.col(_T + name)


def _stage_box(p: HorizontalParams) -> list[pl.Expr]:
    col, W = pl.col, p.window
    box_hi = col("high").shift(1).rolling_max(window_size=W, min_samples=W)
    box_lo = col("low").shift(1).rolling_min(window_size=W, min_samples=W)
    path = _shift_sum(lambda s: (col("close").shift(s) - col("close").shift(s + 1)).abs(), W)
    return [
        col("atr").shift(1).alias(_T + "atr0"), box_hi.alias("hb_box_hi"), box_lo.alias("hb_box_lo"), col("open_time").shift(W).alias("hb_box_t1"),
        pl.when(path > 0).then((col("close").shift(1) - col("close").shift(W + 1)).abs() / path).alias("hb_efficiency"),
        _shift_sum(lambda s: col("high").shift(s) - col("low").shift(s), W).alias(_T + "range_sum"),
    ]


def _stage_touches(p: HorizontalParams) -> list[pl.Expr]:
    """Касания границ коробки барами j-W .. j-1: число, самое старое и самое свежее смещение (константные сдвиги, не строки)."""
    col, W = pl.col, p.window
    out = [((col("hb_box_hi") - col("hb_box_lo")) / _c("atr0")).alias("hb_box_atr"),
           pl.when(col("hb_box_hi") > col("hb_box_lo")).then(_c("range_sum") / W / (col("hb_box_hi") - col("hb_box_lo"))).alias("hb_density")]
    for side, level, extreme in (("long", "hb_box_hi", "high"), ("short", "hb_box_lo", "low")):
        touch = [((col(level) - col(extreme).shift(s)).abs() <= p.touch_atr * _c("atr0")).fill_null(False).cast(pl.Int32) for s in range(1, W + 1)]
        touches = pl.sum_horizontal(touch)
        oldest = pl.max_horizontal([t * s for s, t in zip(range(1, W + 1), touch)])  # самое старое смещение с касанием (0 - касаний нет)
        newest = (W + 1) - pl.max_horizontal([t * (W + 1 - s) for s, t in zip(range(1, W + 1), touch)])  # самое свежее
        out += [touches.alias(f"{_T}{side}_touches"), pl.when(touches > 0).then(oldest - newest).alias(f"{_T}{side}_touch_span")]
    return out


def _stage_side(p: HorizontalParams, side: str, has_bias: bool) -> list[pl.Expr]:
    col, W, atr0 = pl.col, p.window, _c("atr0")
    rng = col("high") - col("low")
    if side == "long":
        level = col("hb_box_hi")
        crossed = (col("close") >= level + p.min_break_atr * atr0) & (col("close").shift(1) <= level.shift(1))
        body, extension = col("close") - col("open"), col("close") - level
        clv = pl.when(rng > 0).then((col("close") - col("low")) / rng)
        sl = col("low").rolling_min(window_size=p.stop_lookback + 1, min_samples=p.stop_lookback + 1) - p.sl_buffer_atr * atr0
        risk, aligned = col("close") - sl, BULLISH
    else:
        level = col("hb_box_lo")
        crossed = (col("close") <= level - p.min_break_atr * atr0) & (col("close").shift(1) >= level.shift(1))
        body, extension = col("open") - col("close"), level - col("close")
        clv = pl.when(rng > 0).then((col("high") - col("close")) / rng)
        sl = col("high").rolling_max(window_size=p.stop_lookback + 1, min_samples=p.stop_lookback + 1) + p.sl_buffer_atr * atr0
        risk, aligned = sl - col("close"), BEARISH
    touches, span = _c(f"{side}_touches"), _c(f"{side}_touch_span")
    consolidation = (
        (col("hb_box_atr") >= p.min_box_atr) & (col("hb_box_atr") <= p.max_box_atr) & (col("hb_efficiency") <= p.max_efficiency)
        & (touches >= p.min_touches) & (span >= p.min_touch_span)
    )
    trigger = (
        crossed & (body >= p.min_body_atr * atr0) & (body <= p.max_body_atr * atr0) & (extension <= p.max_extension_atr * atr0)
        & (clv >= p.min_clv) & (col("vol_ratio") >= p.volume_mult)
    )
    q_trend = pl.when(col(BIAS_COLUMN) == aligned).then(1.0).when(col(BIAS_COLUMN) == 0).then(0.5).otherwise(0.0) if has_bias else pl.lit(0.5)
    quality = pl.mean_horizontal(
        (touches / (2 * p.min_touches)).clip(0.0, 1.0),
        (1.0 - col("hb_efficiency") / p.max_efficiency).clip(0.0, 1.0),
        (col("hb_density") / 0.4).clip(0.0, 1.0),
        (col("vol_ratio") / (2 * p.volume_mult)).clip(0.0, 1.0),
        clv.clip(0.0, 1.0),
        (1.5 - atr0 / col("atr").shift(1 + W)).clip(0.0, 1.0),  # сжатие волатильности за время консолидации
        q_trend,
    )
    pre = f"{_T}{side}_"
    return [(consolidation & trigger).fill_null(False).alias(pre + "setup"), level.alias(pre + "level"), sl.alias(pre + "sl"),
            quality.alias(pre + "quality"), clv.alias(pre + "clv"), (risk / atr0).alias(pre + "risk_atr")]


def detect_horizontal_breakout(df: pl.DataFrame, params: HorizontalParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (features.smc.add_features [+ htf_bias]) -> тот же кадр + hb_box_* и hb_long_* / hb_short_* (signal_columns)."""
    p = params or HorizontalParams()
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: сначала features.smc.add_features")
    if p.htf_filter and BIAS_COLUMN not in df.columns:
        raise DataError(f"Horizontal_Breakout с htf_filter: нет колонки {BIAS_COLUMN} (оркестратор присоединяет H4 bias)")
    clash = [c for c in BOX_COLUMNS + signal_columns("long") + signal_columns("short") if c in df.columns] + [c for c in df.columns if c.startswith(_T)]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    has_bias = BIAS_COLUMN in df.columns
    work = df.with_columns(_stage_box(p)).with_columns(_stage_touches(p)).with_columns([*_stage_side(p, "long", has_bias), *_stage_side(p, "short", has_bias)])
    signals = []
    for side in SIDES:
        pre = f"{_T}{side}_"
        risk = pl.col(pre + "risk_atr")
        trend_ok = side_allowed(side) if p.htf_filter else pl.lit(True)
        signals.append((pl.col(pre + "setup") & trend_ok & (risk >= p.min_risk_atr) & (risk <= p.max_risk_atr)
                        & (pl.col(pre + "quality") >= p.min_quality)).fill_null(False).alias(f"hb_{side}_signal"))
    work = work.with_columns(signals)
    exprs = []
    for side in SIDES:
        signal = pl.col(f"hb_{side}_signal")
        for name in signal_columns(side)[1:]:
            exprs.append(pl.when(signal).then(_c(name.removeprefix("hb_"))).alias(name))
    return work.with_columns(exprs).drop([c for c in work.columns if c.startswith(_T)])
