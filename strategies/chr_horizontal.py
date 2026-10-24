"""Стратегия `Horizontal_Breakout_CHR` (в клиенте - «Пробой (CHR)»): пробой горизонтального уровня после поджатия
по жёсткому тренду H4 (2026-10-19, только Shadow). Общие правила CHR - strategies/chr_common.py.

Векторный Polars (R2), без заглядывания в будущее (R3/R4): уровень, касания и поджатие берутся по барам до пробойной свечи j.
Лонг (шорт - зеркало):
  * уровень сопротивления L = max high[j-W .. j-1] (W = window);
  * касания: подтверждённые локальные максимумы (пивоты с окном pivot_k, бары j-W .. j-1-pivot_k) с high >= L - touch_atr * ATR[j-1];
    их >= min_touches;
  * поджатие (с 2026-10-23): локальная компрессия `chr_common.compression` на comp_bars (5-10) барах НЕПОСРЕДСТВЕННО перед пробойной свечой - волатильность затухает,
    минимумы подтягиваются к уровню, high "трутся" об уровень, база узкая; higher lows на всём окне поджатием больше не считаются;
  * актуальность: последнее касание не старше max_touch_age баров; глубина поиска не больше 80 баров (MAX_LOOKBACK);
  * пробой: размах свечи <= max_break_range_atr * ATR[j-1] (больше - вход MARKET запрещён); close[j] > L (строго за уровнем) полнотелой бычьей свечой (тело >= min_body_frac размаха); прошлое закрытие - под уровнем
    (L - максимум окна, в которое входит бар j-1, поэтому пробой - всегда событие, а не состояние);
  * тренд: htf_bias == Bullish (Neutral, Bearish и нет bias - запрет), без выключателя;
  * стоп (с 2026-10-23, `chr_common.local_stop`): ближайший локальный свинг среди последних 3-8 баров перед пробоем - sl_buffer_atr * ATR[j-1] (0.2 ATR), никакого
    глобального минимума окна; риск не больше max_risk_atr * ATR[j-1], иначе сигнала нет; стоп статичен. Размер позиции - фиксированные 5 USDT риска
    (orchestrator/risk.py), выход - единственный тейк на весь объём строго 1:4 (D.TP_R, registry.CHR_PLAN); частичных фиксаций, безубытка и трейлинга нет.
Вход MARKET по первому тику после закрытия свечи j.

Пороги для калибровки - константы CHR_H_* ниже (они же значения по умолчанию ChrHorizontalParams и атрибуты класса StrategyHorizontalBreakoutChr).
"""

from dataclasses import dataclass, fields

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN
from strategies import chr_common as K
from strategies.chr_common import SIDES, compression, full_body, local_stop, pivot_chain, trend_strict
from utils.errors import ConfigError, DataError

# ------------------------------------------------------------------------------------------------ КАЛИБРОВКА (стартовые допущения 2026-10-19)
CHR_H_WINDOW = 30  # баров до пробоя, в которых ищутся уровень, касания и поджатие (жёсткий потолок - MAX_LOOKBACK)
CHR_H_PIVOT_K = 3  # окно локального экстремума (D2): бар выше / ниже k соседей с каждой стороны (явный свинг, а не случайная тень)
CHR_H_MIN_TOUCHES = 2  # правило CHR: минимум 2 подтверждённых касания
CHR_H_TOUCH_ATR = 0.25  # касание - экстремум пивота не дальше этой доли ATR[j-1] от уровня
CHR_H_MAX_TOUCH_AGE = 20  # последнее касание уровня не старше этого числа баров до пробойной свечи
CHR_H_MAX_BREAK_RANGE_ATR = 1.5  # размах пробойной свечи больше этого числа ATR[j-1] - вход MARKET запрещён (не покупаем на хаях)
CHR_H_COMP_BARS = K.COMP_BARS  # окно локальной компрессии перед пробоем (5-10 баров)
CHR_H_COMP_DECAY = K.COMP_DECAY  # свежая половина базы по размаху <= этой доли старой (волатильность затухает)
CHR_H_COMP_MAX_RANGE_ATR = K.COMP_MAX_RANGE_ATR  # свежая половина базы в среднем уже этого числа ATR
CHR_H_COMP_HEIGHT_ATR = K.COMP_HEIGHT_ATR  # высота всей базы не больше этого числа ATR
CHR_H_COMP_RUB_ATR = K.COMP_RUB_ATR  # high в среднем не дальше этого числа ATR от уровня ("трутся" об него)
CHR_H_SL_SWING_BARS = K.SL_SWING_BARS  # стоп ищется за свингом среди последних 3-8 баров
CHR_H_MAX_RISK_ATR = K.SL_MAX_RISK_ATR  # стоп дальше этого числа ATR от входа - сигнала нет
CHR_H_MIN_BODY_FRAC = 0.6  # полнотелая свеча: тело >= этой доли размаха
CHR_H_SL_BUFFER_ATR = D.SL_BUFFER_ATR  # отступ стопа за локальным свингом, в ATR (0.2)

MAX_LOOKBACK = 80  # глубина поиска структуры не больше 80 баров: только актуальная локальная структура
_REQUIRED = ("open_time", "open", "high", "low", "close", "atr")
_SIGNAL_FIELDS = ("signal", "level", "sl", "touches", "squeeze_1", "squeeze_2", "squeeze_3", "squeeze_start_t", "squeeze_last_t", "risk_atr", "body_frac",
                  "comp_decay", "comp_rub_atr", "comp_height_atr", "sl_extreme")
_T = "_chh_"  # промежуточные колонки (материализация этапами)


@dataclass(frozen=True)
class ChrHorizontalParams:
    window: int = CHR_H_WINDOW
    pivot_k: int = CHR_H_PIVOT_K
    min_touches: int = CHR_H_MIN_TOUCHES
    touch_atr: float = CHR_H_TOUCH_ATR
    max_touch_age: int = CHR_H_MAX_TOUCH_AGE
    max_break_range_atr: float = CHR_H_MAX_BREAK_RANGE_ATR
    comp_bars: int = CHR_H_COMP_BARS
    comp_decay: float = CHR_H_COMP_DECAY
    comp_max_range_atr: float = CHR_H_COMP_MAX_RANGE_ATR
    comp_height_atr: float = CHR_H_COMP_HEIGHT_ATR
    comp_rub_atr: float = CHR_H_COMP_RUB_ATR
    sl_swing_bars: int = CHR_H_SL_SWING_BARS
    max_risk_atr: float = CHR_H_MAX_RISK_ATR
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
        if self.min_touches < 2 or not 5 <= self.comp_bars <= 10 or not 3 <= self.sl_swing_bars <= 8:
            raise ConfigError("ChrHorizontalParams: min_touches >= 2, comp_bars 5-10, sl_swing_bars 3-8 (правила CHR)")
        if self.comp_bars >= self.window:
            raise ConfigError("ChrHorizontalParams: окно компрессии должно быть короче окна уровня (comp_bars < window)")
        if self.window <= self.pivot_k * 2 or not 0 < self.min_body_frac <= 1:
            raise ConfigError("ChrHorizontalParams: window > 2 * pivot_k, min_body_frac в (0, 1]")
        if self.window > MAX_LOOKBACK or self.max_touch_age > MAX_LOOKBACK or self.max_break_range_atr <= 0:
            raise ConfigError(f"ChrHorizontalParams: window и max_touch_age <= {MAX_LOOKBACK} (только локальная структура), max_break_range_atr > 0")


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
    return out


def _stage_side(p: ChrHorizontalParams, side: str) -> list[pl.Expr]:
    col, k, atr0 = pl.col, p.pivot_k, _c("atr0")
    level_kind, _ = _SIDE[side]
    level, flag = _c(f"lvl_{side}"), _c(f"{_CHAIN[level_kind]}flag")
    tol = p.touch_atr * atr0
    # пивот бара j-s-k подтверждён в строке j-s (s >= 1: до пробойной свечи); бар пивота не старше j-W
    near = [(flag.shift(s) & ((col(level_kind).shift(s + k) >= level - tol) if side == "long" else (col(level_kind).shift(s + k) <= level + tol)))
            .fill_null(False).cast(pl.Int32) for s in range(1, p.window - k + 1)]
    touches = pl.sum_horizontal(near)
    recent = pl.sum_horizontal(near[:max(0, p.max_touch_age - k)]) if p.max_touch_age > k else pl.lit(0)  # касание свежее: пивот не старше max_touch_age баров
    comp = compression(side, lambda _s: level, atr0, p.comp_bars, p.comp_decay, p.comp_max_range_atr, p.comp_height_atr, p.comp_rub_atr)
    sl, extreme = local_stop(side, atr0, p.sl_swing_bars, p.sl_buffer_atr)
    beyond = (col("close") > level) if side == "long" else (col("close") < level)
    risk = (col("close") - sl) if side == "long" else (sl - col("close"))
    rng = col("high") - col("low")
    body = (col("close") - col("open")).abs()
    calm = (rng <= p.max_break_range_atr * atr0).fill_null(False)  # аномально большая пробойная свеча - не входим
    signal = (touches >= p.min_touches) & (recent >= 1) & calm & comp["ok"] & beyond & full_body(side, p.min_body_frac) & trend_strict(side) & (risk > 0) & (risk <= p.max_risk_atr * atr0) & (atr0 > 0)
    pre = f"{_T}{side}_"
    return [signal.fill_null(False).alias(pre + "signal"), level.alias(pre + "level"), sl.alias(pre + "sl"), touches.alias(pre + "touches"),
            comp["floor_new"].alias(pre + "squeeze_1"), comp["floor_old"].alias(pre + "squeeze_2"), pl.lit(None, dtype=pl.Float64).alias(pre + "squeeze_3"),
            col("open_time").shift(p.comp_bars).alias(pre + "squeeze_start_t"), col("open_time").shift(1).alias(pre + "squeeze_last_t"), (risk / atr0).alias(pre + "risk_atr"),
            pl.when(rng > 0).then(body / rng).alias(pre + "body_frac"), comp["decay_ratio"].alias(pre + "comp_decay"),
            comp["rub_atr"].alias(pre + "comp_rub_atr"), comp["height_atr"].alias(pre + "comp_height_atr"), extreme.alias(pre + "sl_extreme")]


def detect_chr_horizontal(df: pl.DataFrame, params: ChrHorizontalParams | None = None, *, keep_internal: bool = False) -> pl.DataFrame:
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
        for stage in pivot_chain(kind, p.pivot_k, 1, f"{_T}{chain}"):
            work = work.with_columns(stage)
    work = work.with_columns(_stage_level(p))
    work = work.with_columns([*_stage_side(p, "long"), *_stage_side(p, "short")])
    exprs = []
    for side in SIDES:
        signal = _c(f"{side}_signal")
        for name in _SIGNAL_FIELDS:
            src = _c(f"{side}_{name}")
            exprs.append((signal if name == "signal" else pl.when(signal).then(src)).alias(f"chh_{side}_{name}"))
    out = work.with_columns(exprs)
    # keep_internal: промежуточные колонки (линия, касания, поджатие до сигнала) - только для показа «что видит стратегия» (Live-Dashboard); бот их не читает
    return out if keep_internal else out.drop([c for c in work.columns if c.startswith(_T)])
