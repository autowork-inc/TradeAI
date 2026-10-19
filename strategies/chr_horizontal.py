"""Стратегия `Horizontal_Breakout_CHR` (в клиенте - «Horizontal Breakout (CHR)»): пробой горизонтального уровня после поджатия
по жёсткому тренду H4 (2026-10-19, только Shadow). Общие правила CHR - strategies/chr_common.py.

Векторный Polars (R2), без заглядывания в будущее (R3/R4): уровень, касания и поджатие берутся по барам до пробойной свечи j.
Лонг (шорт - зеркало):
  * уровень сопротивления L = max high[j-W .. j-1] (W = window);
  * касания: подтверждённые локальные максимумы (пивоты с окном pivot_k, бары j-W .. j-1-pivot_k) с high >= L - touch_atr * ATR[j-1];
    их >= min_touches;
  * поджатие: последние squeeze_points подтверждённых локальных минимумов строго повышаются (higher lows) и все лежат в окне W;
  * пробой: close[j] > L (строго за уровнем) полнотелой бычьей свечой (тело >= min_body_frac размаха); прошлое закрытие - под уровнем
    (L - максимум окна, в которое входит бар j-1, поэтому пробой - всегда событие, а не состояние);
  * тренд: htf_bias == Bullish (Neutral, Bearish и нет bias - запрет), без выключателя;
  * стоп: последний минимум поджатия - sl_buffer_atr * ATR[j-1] (D.SL_BUFFER_ATR, О5); стоп статичен. Размер позиции - фиксированные 5 USDT риска
    (orchestrator/risk.py), выход - единственный тейк на весь объём строго 1:4 (D.TP_R, registry.CHR_PLAN); частичных фиксаций, безубытка и трейлинга нет.
Вход MARKET по первому тику после закрытия свечи j.

Пороги для калибровки - константы CHR_H_* ниже (они же значения по умолчанию ChrHorizontalParams и атрибуты класса StrategyHorizontalBreakoutChr).
"""

from dataclasses import dataclass, fields

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN
from strategies.chr_common import SIDES, full_body, pivot_chain, squeeze, trend_strict
from utils.errors import ConfigError, DataError

# ------------------------------------------------------------------------------------------------ КАЛИБРОВКА (стартовые допущения 2026-10-19)
CHR_H_WINDOW = 24  # баров до пробоя, в которых ищутся уровень, касания и поджатие
CHR_H_PIVOT_K = 2  # окно локального экстремума (D2): бар выше / ниже k соседей с каждой стороны
CHR_H_MIN_TOUCHES = 2  # правило CHR: минимум 2 подтверждённых касания
CHR_H_TOUCH_ATR = 0.25  # касание - экстремум пивота не дальше этой доли ATR[j-1] от уровня
CHR_H_SQUEEZE_POINTS = 3  # строго повышающихся минимумов (лонг) / понижающихся максимумов (шорт): 3 точки = 2 шага
CHR_H_MIN_BODY_FRAC = 0.6  # полнотелая свеча: тело >= этой доли размаха
CHR_H_SL_BUFFER_ATR = D.SL_BUFFER_ATR  # запас стопа за последней точкой поджатия, в ATR

_REQUIRED = ("open_time", "open", "high", "low", "close", "atr")
_SIGNAL_FIELDS = ("signal", "level", "sl", "touches", "squeeze_1", "squeeze_2", "squeeze_3", "squeeze_start_t", "squeeze_last_t", "risk_atr", "body_frac")
_T = "_chh_"  # промежуточные колонки (материализация этапами)


@dataclass(frozen=True)
class ChrHorizontalParams:
    window: int = CHR_H_WINDOW
    pivot_k: int = CHR_H_PIVOT_K
    min_touches: int = CHR_H_MIN_TOUCHES
    touch_atr: float = CHR_H_TOUCH_ATR
    squeeze_points: int = CHR_H_SQUEEZE_POINTS
    min_body_frac: float = CHR_H_MIN_BODY_FRAC
    sl_buffer_atr: float = CHR_H_SL_BUFFER_ATR

    def __post_init__(self) -> None:
        for f in fields(self):
            v = getattr(self, f.name)
            if f.type in (int, "int"):
                if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                    raise ConfigError(f"ChrHorizontalParams.{f.name} должен быть целым >= 1, получено {v!r}")
            elif isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ConfigError(f"ChrHorizontalParams.{f.name} должен быть числом >= 0, получено {v!r}")
        if self.min_touches < 2 or self.squeeze_points < 2:
            raise ConfigError("ChrHorizontalParams: min_touches >= 2, squeeze_points >= 2 (правила CHR)")
        if self.window <= self.pivot_k * 2 or not 0 < self.min_body_frac <= 1:
            raise ConfigError("ChrHorizontalParams: window > 2 * pivot_k, min_body_frac в (0, 1]")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"chh_{side}_{n}" for n in _SIGNAL_FIELDS)


def _c(name: str) -> pl.Expr:
    return pl.col(_T + name)


# сторона -> (экстремум уровня/касаний, экстремум поджатия); цепочки пивотов: "hi" - максимумы, "lo" - минимумы
_SIDE = {"long": ("high", "low"), "short": ("low", "high")}
_CHAIN = {"high": "hi", "low": "lo"}


def _stage_level(p: ChrHorizontalParams) -> list[pl.Expr]:
    col, W = pl.col, p.window
    out = [col("atr").shift(1).alias(_T + "atr0"), col("open_time").shift(W).alias(_T + "t_start"),
           col("high").shift(1).rolling_max(window_size=W, min_samples=W).alias(_T + "lvl_long"),
           col("low").shift(1).rolling_min(window_size=W, min_samples=W).alias(_T + "lvl_short")]
    for chain in _CHAIN.values():  # цепочки пивотов, известные до бара j
        for n in range(1, p.squeeze_points + 1):
            out += [_c(f"{chain}{n}_p").shift(1).alias(f"{_T}{chain}{n}_pj"), _c(f"{chain}{n}_t").shift(1).alias(f"{_T}{chain}{n}_tj")]
    return out


def _stage_side(p: ChrHorizontalParams, side: str) -> list[pl.Expr]:
    col, k, atr0 = pl.col, p.pivot_k, _c("atr0")
    level_kind, sq_kind = _SIDE[side]
    level, flag = _c(f"lvl_{side}"), _c(f"{_CHAIN[level_kind]}flag")
    tol = p.touch_atr * atr0
    # пивот бара j-s-k подтверждён в строке j-s (s >= 1: до пробойной свечи); бар пивота не старше j-W
    near = [(flag.shift(s) & ((col(level_kind).shift(s + k) >= level - tol) if side == "long" else (col(level_kind).shift(s + k) <= level + tol)))
            .fill_null(False).cast(pl.Int32) for s in range(1, p.window - k + 1)]
    touches = pl.sum_horizontal(near)
    sq = _CHAIN[sq_kind]
    in_window = (_c(f"{sq}{p.squeeze_points}_tj") >= _c("t_start")).fill_null(False)
    squeezed = squeeze(side, f"{_T}{sq}", p.squeeze_points, suffix="_pj") & in_window
    last_sq = _c(f"{sq}1_pj")
    if side == "long":
        beyond, sl, risk = col("close") > level, last_sq - p.sl_buffer_atr * atr0, col("close") - (last_sq - p.sl_buffer_atr * atr0)
    else:
        beyond, sl, risk = col("close") < level, last_sq + p.sl_buffer_atr * atr0, (last_sq + p.sl_buffer_atr * atr0) - col("close")
    rng = col("high") - col("low")
    body = (col("close") - col("open")).abs()
    signal = (touches >= p.min_touches) & squeezed & beyond & full_body(side, p.min_body_frac) & trend_strict(side) & (risk > 0) & (atr0 > 0)
    pre = f"{_T}{side}_"
    return [signal.fill_null(False).alias(pre + "signal"), level.alias(pre + "level"), sl.alias(pre + "sl"), touches.alias(pre + "touches"),
            *[_c(f"{sq}{n}_pj").alias(f"{pre}squeeze_{n}") for n in (1, 2, 3) if n <= p.squeeze_points],
            _c(f"{sq}{p.squeeze_points}_tj").alias(pre + "squeeze_start_t"), _c(f"{sq}1_tj").alias(pre + "squeeze_last_t"), (risk / atr0).alias(pre + "risk_atr"),
            pl.when(rng > 0).then(body / rng).alias(pre + "body_frac")]


def detect_chr_horizontal(df: pl.DataFrame, params: ChrHorizontalParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (features.smc.add_features + htf_bias) -> тот же кадр + chh_long_* / chh_short_* (signal_columns; значения - только в строке сигнала)."""
    p = params or ChrHorizontalParams()
    missing = [c for c in (*_REQUIRED, BIAS_COLUMN) if c not in df.columns]
    if missing:
        raise DataError(f"Horizontal_Breakout_CHR: нет колонок {missing} (features.smc.add_features + H4 bias оркестратора)")
    clash = [c for s in SIDES for c in signal_columns(s) if c in df.columns] + [c for c in df.columns if c.startswith(_T)]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    work = df
    for kind, chain in _CHAIN.items():
        for stage in pivot_chain(kind, p.pivot_k, p.squeeze_points, f"{_T}{chain}"):
            work = work.with_columns(stage)
    work = work.with_columns(_stage_level(p))
    work = work.with_columns([*_stage_side(p, "long"), *_stage_side(p, "short")])
    exprs = []
    for side in SIDES:
        signal = _c(f"{side}_signal")
        for name in _SIGNAL_FIELDS:
            src = _c(f"{side}_{name}")
            if name == "squeeze_3" and p.squeeze_points < 3:
                src = pl.lit(None, dtype=pl.Float64)
            exprs.append((signal if name == "signal" else pl.when(signal).then(src)).alias(f"chh_{side}_{name}"))
    return work.with_columns(exprs).drop([c for c in work.columns if c.startswith(_T)])
