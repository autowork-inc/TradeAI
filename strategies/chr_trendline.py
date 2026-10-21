"""Стратегия `Trendline_Breakout_CHR` (в клиенте - «Trendline Breakout (CHR)»): пробой наклонной линии после поджатия по жёсткому
тренду H4 (2026-10-19, только Shadow). Общие правила CHR - strategies/chr_common.py.

Векторный Polars (R2), без заглядывания в будущее (R3/R4): опоры линии, касания и поджатие - пивоты, подтверждённые до пробойной свечи j.
Лонг (шорт - зеркало: поддержка по минимумам, восходящая, поджатие lower highs, пробой вниз):
  * линия сопротивления - через два последних подтверждённых локальных максимума A2 (старше) -> A1, нисходящая (A1 < A2), наклон
    |цена за бар| >= min_slope_atr * ATR[j-1] (иначе это горизонтальный уровень); A2 не старше окна W;
  * чистый воздух: от A2 до бара j-1 ни один high не выше линии больше чем на cross_atr * ATR[j-1], ни одно закрытие не за линией;
  * актуальность: A1 не старше max_touch_age баров, глубина поиска не больше 80 баров (MAX_LOOKBACK);
  * размах пробойной свечи <= max_break_range_atr * ATR[j-1], иначе вход MARKET запрещён (не покупаем на хаях);
  * касания: две опоры + более старые пивоты-максимумы цепочки (до touch_depth), лежащие на продолжении линии в пределах touch_atr * ATR[j-1]
    и в окне W; касаний >= min_touches;
  * поджатие: последние squeeze_points подтверждённых локальных минимумов строго повышаются (higher lows) и лежат в окне W;
  * пробой: close[j] > линия в момент j (строго за линией) полнотелой бычьей свечой (тело >= min_body_frac размаха);
  * тренд: htf_bias == Bullish (Neutral, Bearish и нет bias - запрет), без выключателя;
  * стоп: последний минимум поджатия - sl_buffer_atr * ATR[j-1] (D.SL_BUFFER_ATR, О5); стоп статичен. Размер позиции - фиксированные 5 USDT риска
    (orchestrator/risk.py), выход - единственный тейк на весь объём строго 1:4 (D.TP_R, registry.CHR_PLAN); частичных фиксаций, безубытка и трейлинга нет.
Вход MARKET по первому тику после закрытия свечи j.

Пороги для калибровки - константы CHR_T_* ниже (они же значения по умолчанию ChrTrendlineParams и атрибуты класса StrategyTrendlineBreakoutChr).
"""

from dataclasses import dataclass, fields

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN
from strategies.chr_common import SIDES, full_body, pivot_chain, squeeze, trend_strict
from utils.errors import ConfigError, DataError

# ------------------------------------------------------------------------------------------------ КАЛИБРОВКА (стартовые допущения 2026-10-19)
CHR_T_WINDOW = 60  # баров до пробоя: в них лежат первая опора линии, касания и поджатие (жёсткий потолок - MAX_LOOKBACK)
CHR_T_PIVOT_K = 3  # окно локального экстремума (D2): явный свинг, 3 бара слева и справа
CHR_T_MIN_TOUCHES = 2  # правило CHR: минимум 2 подтверждённых касания (две опоры линии - уже два касания)
CHR_T_TOUCH_DEPTH = 4  # сколько последних пивотов цепочки проверяется на касание линии (две опоры + более старые)
CHR_T_TOUCH_ATR = 0.25  # допуск касания линии экстремумом пивота, в ATR[j-1]
CHR_T_CROSS_ATR = 0.05  # «чистый воздух»: между опорами и до пробоя high/low не выходят за линию больше этого допуска, а закрытия - ни на тик
CHR_T_MAX_TOUCH_AGE = 24  # вторая (свежая) опора линии не старше этого числа баров до пробойной свечи
CHR_T_MAX_BREAK_RANGE_ATR = 1.5  # размах пробойной свечи больше этого числа ATR[j-1] - вход MARKET запрещён (не покупаем на хаях)
CHR_T_MIN_SLOPE_ATR = 0.02  # наклон линии, ATR за бар: положе - это горизонтальный уровень (Horizontal_Breakout_CHR)
CHR_T_SQUEEZE_POINTS = 3  # строго повышающихся минимумов (лонг) / понижающихся максимумов (шорт)
CHR_T_MIN_BODY_FRAC = 0.6  # полнотелая свеча: тело >= этой доли размаха
CHR_T_SL_BUFFER_ATR = D.SL_BUFFER_ATR  # запас стопа за последней точкой поджатия, в ATR

MAX_LOOKBACK = 80  # глубина поиска структуры не больше 80 баров: только актуальная локальная структура
_REQUIRED = ("open_time", "open", "high", "low", "close", "atr")
_SIGNAL_FIELDS = ("signal", "level", "sl", "touches", "slope_atr", "anchor1_time", "anchor1_price", "anchor2_time", "anchor2_price",
                  "squeeze_1", "squeeze_2", "squeeze_3", "squeeze_start_t", "squeeze_last_t", "risk_atr", "body_frac")
_T = "_cht_"


@dataclass(frozen=True)
class ChrTrendlineParams:
    window: int = CHR_T_WINDOW
    pivot_k: int = CHR_T_PIVOT_K
    min_touches: int = CHR_T_MIN_TOUCHES
    touch_depth: int = CHR_T_TOUCH_DEPTH
    touch_atr: float = CHR_T_TOUCH_ATR
    cross_atr: float = CHR_T_CROSS_ATR
    max_touch_age: int = CHR_T_MAX_TOUCH_AGE
    max_break_range_atr: float = CHR_T_MAX_BREAK_RANGE_ATR
    min_slope_atr: float = CHR_T_MIN_SLOPE_ATR
    squeeze_points: int = CHR_T_SQUEEZE_POINTS
    min_body_frac: float = CHR_T_MIN_BODY_FRAC
    sl_buffer_atr: float = CHR_T_SL_BUFFER_ATR

    def __post_init__(self) -> None:
        for f in fields(self):
            v = getattr(self, f.name)
            if f.type in (int, "int"):
                if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                    raise ConfigError(f"ChrTrendlineParams.{f.name} должен быть целым >= 1, получено {v!r}")
            elif isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ConfigError(f"ChrTrendlineParams.{f.name} должен быть числом >= 0, получено {v!r}")
        if self.min_touches < 2 or self.squeeze_points < 2 or self.touch_depth < 2:
            raise ConfigError("ChrTrendlineParams: min_touches >= 2, squeeze_points >= 2, touch_depth >= 2 (правила CHR)")
        if self.window <= self.pivot_k * 2 or not 0 < self.min_body_frac <= 1 or self.min_slope_atr <= 0:
            raise ConfigError("ChrTrendlineParams: window > 2 * pivot_k, min_body_frac в (0, 1], min_slope_atr > 0")
        if self.window > MAX_LOOKBACK or self.max_touch_age > MAX_LOOKBACK or self.max_break_range_atr <= 0:
            raise ConfigError(f"ChrTrendlineParams: window и max_touch_age <= {MAX_LOOKBACK} (только локальная структура), max_break_range_atr > 0")


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"cht_{side}_{n}" for n in _SIGNAL_FIELDS)


def _c(name: str) -> pl.Expr:
    return pl.col(_T + name)


# сторона -> (цепочка опор линии, цепочка поджатия); "hi" - пивоты-максимумы, "lo" - минимумы
_SIDE = {"long": ("hi", "lo"), "short": ("lo", "hi")}
_KIND = {"hi": "high", "lo": "low"}


def _stage_known(p: ChrTrendlineParams) -> list[pl.Expr]:
    """Цепочки пивотов, известные до бара j (строка j-1)."""
    depth = max(p.touch_depth, p.squeeze_points)
    out = [pl.col("atr").shift(1).alias(_T + "atr0"), pl.col("open_time").shift(p.window).alias(_T + "t_start")]
    for chain in _KIND:
        for n in range(1, depth + 1):
            out += [_c(f"{chain}{n}_p").shift(1).alias(f"{_T}{chain}{n}_pj"), _c(f"{chain}{n}_t").shift(1).alias(f"{_T}{chain}{n}_tj")]
    return out


def _stage_line(side: str, iv: int) -> list[pl.Expr]:
    a = _SIDE[side][0]
    p1, t1, p2, t2 = _c(f"{a}1_pj"), _c(f"{a}1_tj"), _c(f"{a}2_pj"), _c(f"{a}2_tj")
    span = (t1 - t2) / iv
    pre = f"{_T}{side}_"
    return [pl.when(span > 0).then((p1 - p2) / span).alias(pre + "slope")]


def _line_at(side: str, t: pl.Expr, iv: int) -> pl.Expr:
    a = _SIDE[side][0]
    return _c(f"{a}1_pj") + _c(f"{side}_slope") * (t - _c(f"{a}1_tj")) / iv


def _stage_side(p: ChrTrendlineParams, side: str, iv: int) -> list[pl.Expr]:
    col, atr0 = pl.col, _c("atr0")
    a, sq = _SIDE[side]
    tol = p.touch_atr * atr0
    sgn = 1.0 if side == "long" else -1.0  # «за линией» для лонга - выше
    slope = _c(f"{side}_slope")
    direction_ok = (slope < 0) if side == "long" else (slope > 0)  # сопротивление нисходящее, поддержка восходящая
    t2 = _c(f"{a}2_tj")
    line_ok = direction_ok & (slope.abs() >= p.min_slope_atr * atr0) & (t2 >= _c("t_start"))
    # «чистый воздух»: от первой (старшей) опоры до бара j-1 (между опорами и от второй опоры до пробоя) линия не проходит сквозь свечи:
    # экстремумы не дальше cross_atr за линией, закрытия - не за линией вовсе
    extreme = _KIND[a]
    cross = p.cross_atr * atr0
    respected = pl.lit(True)
    for s in range(1, p.window + 1):
        ts = col("open_time").shift(s)
        line_s = _line_at(side, ts, iv)
        ok = (ts < t2) | ((sgn * (col(extreme).shift(s) - line_s) <= cross) & (sgn * (col("close").shift(s) - line_s) <= 0))
        respected = respected & ok.fill_null(False)
    fresh = ((col("open_time") - _c(f"{a}1_tj")) <= p.max_touch_age * iv).fill_null(False)  # вторая опора - недавняя
    prev_inside = sgn * (col("close").shift(1) - _line_at(side, col("open_time").shift(1), iv)) <= 0
    extra = [((_c(f"{a}{n}_tj") >= _c("t_start")) & ((_c(f"{a}{n}_pj") - _line_at(side, _c(f"{a}{n}_tj"), iv)).abs() <= tol))
             .fill_null(False).cast(pl.Int32) for n in range(3, p.touch_depth + 1)]
    touches = pl.lit(2, dtype=pl.Int32) + (pl.sum_horizontal(extra) if extra else pl.lit(0, dtype=pl.Int32))
    in_window = (_c(f"{sq}{p.squeeze_points}_tj") >= _c("t_start")).fill_null(False)
    squeezed = squeeze(side, f"{_T}{sq}", p.squeeze_points, suffix="_pj") & in_window
    level = _line_at(side, col("open_time"), iv)
    last_sq = _c(f"{sq}1_pj")
    sl = last_sq - sgn * p.sl_buffer_atr * atr0
    risk = sgn * (col("close") - sl)
    beyond = sgn * (col("close") - level) > 0
    rng = col("high") - col("low")
    calm = (rng <= p.max_break_range_atr * atr0).fill_null(False)  # аномально большая пробойная свеча - не входим
    signal = (line_ok & respected & fresh & calm & prev_inside & (touches >= p.min_touches) & squeezed & beyond & full_body(side, p.min_body_frac)
              & trend_strict(side) & (risk > 0) & (atr0 > 0))
    pre = f"{_T}{side}_"
    return [signal.fill_null(False).alias(pre + "signal"), level.alias(pre + "level"), sl.alias(pre + "sl"), touches.alias(pre + "touches"),
            (slope / atr0).alias(pre + "slope_atr"), t2.alias(pre + "anchor1_time"), _c(f"{a}2_pj").alias(pre + "anchor1_price"),
            _c(f"{a}1_tj").alias(pre + "anchor2_time"), _c(f"{a}1_pj").alias(pre + "anchor2_price"),
            *[_c(f"{sq}{n}_pj").alias(f"{pre}squeeze_{n}") for n in (1, 2, 3) if n <= p.squeeze_points],
            _c(f"{sq}{p.squeeze_points}_tj").alias(pre + "squeeze_start_t"), _c(f"{sq}1_tj").alias(pre + "squeeze_last_t"), (risk / atr0).alias(pre + "risk_atr"),
            pl.when(rng > 0).then((col("close") - col("open")).abs() / rng).alias(pre + "body_frac")]


def detect_chr_trendline(df: pl.DataFrame, interval_ms: int, params: ChrTrendlineParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (features.smc.add_features + htf_bias) -> тот же кадр + cht_long_* / cht_short_* (signal_columns; значения - только в строке сигнала)."""
    p = params or ChrTrendlineParams()
    if isinstance(interval_ms, bool) or not isinstance(interval_ms, int) or interval_ms <= 0:
        raise ConfigError(f"interval_ms должен быть положительным целым, получено {interval_ms!r}")
    missing = [c for c in (*_REQUIRED, BIAS_COLUMN) if c not in df.columns]
    if missing:
        raise DataError(f"Trendline_Breakout_CHR: нет колонок {missing} (features.smc.add_features + H4 bias оркестратора)")
    clash = [c for s in SIDES for c in signal_columns(s) if c in df.columns] + [c for c in df.columns if c.startswith(_T)]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    work = df
    depth = max(p.touch_depth, p.squeeze_points)
    for chain, kind in _KIND.items():
        for stage in pivot_chain(kind, p.pivot_k, depth, f"{_T}{chain}"):
            work = work.with_columns(stage)
    work = work.with_columns(_stage_known(p))
    work = work.with_columns([*_stage_line("long", interval_ms), *_stage_line("short", interval_ms)])
    work = work.with_columns([*_stage_side(p, "long", interval_ms), *_stage_side(p, "short", interval_ms)])
    exprs = []
    for side in SIDES:
        signal = _c(f"{side}_signal")
        for name in _SIGNAL_FIELDS:
            src = pl.lit(None, dtype=pl.Float64) if name == "squeeze_3" and p.squeeze_points < 3 else _c(f"{side}_{name}")
            exprs.append((signal if name == "signal" else pl.when(signal).then(src)).alias(f"cht_{side}_{name}"))
    return work.with_columns(exprs).drop([c for c in work.columns if c.startswith(_T)])
