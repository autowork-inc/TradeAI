"""Стратегия `Trendline_Breakout`: пробой или свип наклонной трендовой линии по двум подтверждённым свингам (2026-10-18, только Shadow).

Источник гипотезы - разметка 866 графиков (`results.jsonl`, scripts/analyze_better.py): trendline - второй по частоте тип (293 из 866):
консолидация 94%, закрытие за линией 91%, импульс 56%, стоп за локальным свингом в 231 из 293, дистанция стопа медиана 0.75%;
ложный пробой (sweep) - 11%. Исходов сделок в разметке нет: это гипотеза для статистики Shadow.

Линии (векторный Polars, R2; все опоры известны ДО бара j - берутся со строки j-1, R3/R4):
  * сопротивление R - через два последних подтверждённых Swing High (sh2 -> sh1, features/smc.py), только нисходящее (sh1 < sh2);
  * поддержка S - через два последних Swing Low (sl2 -> sl1), только восходящая (sl1 > sl2).
  Цена линии в момент t: p1 + slope * (t - t1) / interval, slope - изменение цены за бар.
Геометрия линии (обязательно):
  * наклон |slope| / ATR[j-1] в [min_slope_atr, max_slope_atr] - не горизонталь (это Horizontal_Breakout) и не обвал «стенкой»;
  * пролёт между опорами min_span_bars..max_span_bars, со второй опоры прошло не больше max_age_bars (линия свежая);
  * линия «уважается»: ни одного закрытия за линией (дальше violation_atr * ATR[j-1]) от первой опоры до бара j-1 включительно -
    иначе линия уже сломана и пробоя нет. Проверка - пары (j, i) через int_ranges + explode + group_by на глубину max_span_bars + max_age_bars (не по строкам);
  * дополнительные касания (экстремум бара в пределах touch_atr * ATR[j-1] от линии, не ближе 2 баров к опорам) >= min_extra_touches.
Режим breakout (лонг от R; шорт от S - зеркало): close[j] >= линия + min_break_atr * ATR[j-1], импульсное тело min_body_atr..max_body_atr ATR,
  close - линия <= max_extension_atr ATR, CLV >= min_clv, vol_ratio >= volume_mult. Стоп - минимум последних stop_lookback баров и пробойной
  свечи - sl_buffer_atr * ATR[j-1] (локальный свинг перед пробоем).
Режим sweep (шорт от R; лонг от S - зеркало): хай прокалывает линию на min_sweep_atr..max_sweep_atr ATR, закрытие возвращается под линию,
  верхняя тень >= min_wick_frac размаха свечи, vol_ratio >= sweep_volume_mult - ложный пробой: сделка по направлению линии. Стоп - за
  экстремумом свипа + sl_buffer_atr * ATR[j-1].
Общие условия: старший тренд (htf_filter: лонг не при Bearish H4, шорт не при Bullish; нет bias - запрет), риск в ATR в
  [min_risk_atr, max_risk_atr] (стоп не подгоняется). На стороне breakout приоритетнее sweep; лонг и шорт на одном баре (линии
  пересеклись, треугольник за вершиной) - неоднозначность, сигналов нет.
Качество (0..1, среднее шести компонент, в meta): касания, пролёт, пологость наклона, объём, закрытие (CLV / тень), согласие с трендом.
"""

from dataclasses import dataclass, fields

import polars as pl

from features import defaults as D
from features.htf import BEARISH, BIAS_COLUMN, BULLISH, side_allowed
from utils.errors import ConfigError, DataError

SIDES = ("long", "short")
BREAKOUT, SWEEP = "breakout", "sweep"
_REQUIRED = ("open_time", "open", "high", "low", "close", "atr", "vol_ratio",
             "sh1_price", "sh1_time", "sh2_price", "sh2_time", "sl1_price", "sl1_time", "sl2_price", "sl2_time")
_SIGNAL_FIELDS = ("signal", "mode", "level", "sl", "quality", "slope_atr", "span_bars", "age_bars", "touches", "risk_atr",
                  "anchor1_time", "anchor1_price", "anchor2_time", "anchor2_price")


@dataclass(frozen=True)
class TrendlineParams:
    min_span_bars: int = 10
    max_span_bars: int = 96
    max_age_bars: int = 48
    min_slope_atr: float = 0.02
    max_slope_atr: float = 0.6
    violation_atr: float = 0.1
    touch_atr: float = 0.25
    min_extra_touches: int = 0
    min_break_atr: float = 0.05
    max_extension_atr: float = 1.2
    min_body_atr: float = 0.5
    max_body_atr: float = 2.5
    min_clv: float = 0.6
    volume_mult: float = 1.5
    enable_sweep: bool = True
    min_sweep_atr: float = 0.1
    max_sweep_atr: float = 1.5
    min_wick_frac: float = 0.5
    sweep_volume_mult: float = 1.5
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
                low = 0 if f.name == "min_extra_touches" else 1
                if isinstance(v, bool) or not isinstance(v, int) or v < low:
                    raise ConfigError(f"TrendlineParams.{f.name} должен быть целым >= {low}, получено {v!r}")
            elif f.type in (bool, "bool"):
                if not isinstance(v, bool):
                    raise ConfigError(f"TrendlineParams.{f.name} должен быть bool, получено {v!r}")
            elif isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ConfigError(f"TrendlineParams.{f.name} должен быть числом >= 0, получено {v!r}")
        pairs = (("min_span_bars", "max_span_bars"), ("min_slope_atr", "max_slope_atr"), ("min_body_atr", "max_body_atr"),
                 ("min_sweep_atr", "max_sweep_atr"), ("min_risk_atr", "max_risk_atr"))
        bad = [f"{a} < {b}" for a, b in pairs if not getattr(self, a) < getattr(self, b)]
        if bad:
            raise ConfigError(f"TrendlineParams: нужно {', '.join(bad)}")
        if not (0 <= self.min_clv < 1 and 0 < self.min_wick_frac < 1 and 0 <= self.min_quality <= 1 and self.min_slope_atr > 0):
            raise ConfigError("TrendlineParams: min_clv в [0, 1), min_wick_frac в (0, 1), min_quality в [0, 1], min_slope_atr > 0")

    @property
    def lookback_bars(self) -> int:
        """Глубина проверки «линия не сломана»: от первой опоры до бара j-1 - не больше пролёта + возраста."""
        return self.max_span_bars + self.max_age_bars


def signal_columns(side: str) -> tuple[str, ...]:
    return tuple(f"tl_{side}_{n}" for n in _SIGNAL_FIELDS)

_T = "_tl_"  # префикс промежуточных колонок: расчёт идёт этапами (материализация), иначе дерево из сотен сдвигов дублируется в каждом выражении
LINES = {"r": "sh", "s": "sl"}  # r - сопротивление по Swing High, s - поддержка по Swing Low
# (сторона, режим) -> линия: лонг - пробой сопротивления или свип поддержки; шорт - пробой поддержки или свип сопротивления
CANDIDATES = {("long", BREAKOUT): "r", ("long", SWEEP): "s", ("short", BREAKOUT): "s", ("short", SWEEP): "r"}
_PER_LINE = {"level": "now", "slope_atr": "slope_atr", "span_bars": "span", "age_bars": "age", "touches": "touches",
             "anchor1_time": "t2", "anchor1_price": "p2", "anchor2_time": "t1", "anchor2_price": "p1"}


def _c(name: str) -> pl.Expr:
    return pl.col(_T + name)


def _line_value(key: str, t: pl.Expr, iv: int) -> pl.Expr:
    return _c(f"{key}_p1") + _c(f"{key}_slope") * (t - _c(f"{key}_t1")) / iv


def _stage_anchors(iv: int) -> list[pl.Expr]:
    """Опоры линий, известные до бара j (строка j-1), пролёт в барах и наклон (цена за бар)."""
    col = pl.col
    out = [col("atr").shift(1).alias(_T + "atr0")]
    for key, prefix in LINES.items():
        p1, t1 = col(f"{prefix}1_price").shift(1), col(f"{prefix}1_time").shift(1)
        p2, t2 = col(f"{prefix}2_price").shift(1), col(f"{prefix}2_time").shift(1)
        span = (t1 - t2) / iv
        out += [p1.alias(f"{_T}{key}_p1"), t1.alias(f"{_T}{key}_t1"), p2.alias(f"{_T}{key}_p2"), t2.alias(f"{_T}{key}_t2"),
                span.alias(f"{_T}{key}_span"), pl.when(span > 0).then((p1 - p2) / span).alias(f"{_T}{key}_slope")]
    return out


def _with_history(work: pl.DataFrame, p: TrendlineParams, iv: int) -> pl.DataFrame:
    """Нарушения линии закрытием и дополнительные касания на барах i = j-lookback_bars .. j-1 для каждого бара j.

    Векторно (R2): пары (j, i) строятся int_ranges + explode, цены бара i присоединяются join'ом, счётчики - group_by по j.
    Это те же суммы, что и по константным сдвигам 1..lookback_bars, но без дерева из сотен выражений (планирование было дороже расчёта).
    Значение в строке j зависит только от строк < j (R3/R4): пары смотрят назад, опоры линии взяты со строки j-1."""
    j, i = _T + "j", _T + "i"
    base = work.with_columns(pl.int_range(pl.len(), dtype=pl.Int64).alias(j))
    anchors = [f"{_T}{k}_{n}" for k in LINES for n in ("p1", "t1", "t2", "slope")] + [_T + "atr0"]
    bars = base.select(pl.col(j).alias(i), pl.col("open_time").alias(_T + "ts"), pl.col("close").alias(_T + "cs"),
                       pl.col("high").alias(_T + "hs"), pl.col("low").alias(_T + "ls"))
    pairs = (
        base.select(j, *anchors)
        .with_columns(pl.int_ranges(pl.col(j) - p.lookback_bars, pl.col(j), dtype=pl.Int64).alias(i))
        .explode(i, empty_as_null=False)  # диапазон на строку никогда не пуст (lookback_bars >= 1)
        .filter(pl.col(i) >= 0)
        .join(bars, on=i, how="inner")
    )
    t_s, atr0 = _c("ts"), _c("atr0")
    counts = []
    for key in LINES:
        sign, extreme = (1.0, "hs") if key == "r" else (-1.0, "ls")
        after_first = (t_s > _c(f"{key}_t2")).fill_null(False)
        line_s = _line_value(key, t_s, iv)
        viol = (after_first & (sign * (_c("cs") - line_s) > p.violation_atr * atr0)).fill_null(False).cast(pl.Int32)
        far = ((t_s - _c(f"{key}_t1")).abs() > 2 * iv) & ((t_s - _c(f"{key}_t2")).abs() > 2 * iv)
        touch = (after_first & far & ((_c(extreme) - line_s).abs() <= p.touch_atr * atr0)).fill_null(False).cast(pl.Int32)
        counts += [viol.sum().alias(f"{_T}{key}_viol"), touch.sum().alias(f"{_T}{key}_touches")]
    agg = pairs.group_by(j).agg(counts)
    names = [f"{_T}{key}_{n}" for key in LINES for n in ("viol", "touches")]
    return base.join(agg, on=j, how="left").with_columns(pl.col(n).fill_null(0) for n in names).drop(j)


def _stage_lines(p: TrendlineParams, iv: int) -> list[pl.Expr]:
    """Годность линии на баре j и её цена в момент j."""
    out = []
    for key in LINES:
        slope_atr = _c(f"{key}_slope") / _c("atr0")
        direction_ok = (_c(f"{key}_p1") < _c(f"{key}_p2")) if key == "r" else (_c(f"{key}_p1") > _c(f"{key}_p2"))
        age = (pl.col("open_time") - _c(f"{key}_t1")) / iv
        span = _c(f"{key}_span")
        valid = (
            direction_ok & (slope_atr.abs() >= p.min_slope_atr) & (slope_atr.abs() <= p.max_slope_atr)
            & (span >= p.min_span_bars) & (span <= p.max_span_bars) & (age <= p.max_age_bars)
            & (_c(f"{key}_viol") == 0) & (_c(f"{key}_touches") >= p.min_extra_touches)
        )
        out += [valid.fill_null(False).alias(f"{_T}{key}_valid"), _line_value(key, pl.col("open_time"), iv).alias(f"{_T}{key}_now"),
                slope_atr.alias(f"{_T}{key}_slope_atr"), age.alias(f"{_T}{key}_age")]
    return out


def _stage_candidates(p: TrendlineParams, has_bias: bool) -> list[pl.Expr]:
    """Четыре кандидата (сторона, режим): условие, стоп, риск в ATR и качество."""
    col, atr0 = pl.col, _c("atr0")
    rng = col("high") - col("low")
    clv_long = pl.when(rng > 0).then((col("close") - col("low")) / rng)
    clv_short = pl.when(rng > 0).then((col("high") - col("close")) / rng)
    upper_wick = pl.when(rng > 0).then((col("high") - pl.max_horizontal("open", "close")) / rng)
    lower_wick = pl.when(rng > 0).then((pl.min_horizontal("open", "close") - col("low")) / rng)
    sl_below = col("low").rolling_min(window_size=p.stop_lookback + 1, min_samples=p.stop_lookback + 1) - p.sl_buffer_atr * atr0
    sl_above = col("high").rolling_max(window_size=p.stop_lookback + 1, min_samples=p.stop_lookback + 1) + p.sl_buffer_atr * atr0
    out = []
    for (side, mode), key in CANDIDATES.items():
        up, now, valid = side == "long", _c(f"{key}_now"), _c(f"{key}_valid")
        if mode == BREAKOUT:
            body = (col("close") - col("open")) if up else (col("open") - col("close"))
            beyond = (col("close") - now) if up else (now - col("close"))
            close_q = clv_long if up else clv_short
            cond = (
                valid & (beyond >= p.min_break_atr * atr0) & (beyond <= p.max_extension_atr * atr0)
                & (body >= p.min_body_atr * atr0) & (body <= p.max_body_atr * atr0) & (close_q >= p.min_clv) & (col("vol_ratio") >= p.volume_mult)
            )
            sl, vmult = (sl_below if up else sl_above), p.volume_mult
        else:  # лонг: лой прокалывает поддержку, закрытие над линией; шорт: хай прокалывает сопротивление, закрытие под линией
            depth = (now - col("low")) if up else (col("high") - now)
            back = (col("close") > now) if up else (col("close") < now)
            close_q = lower_wick if up else upper_wick
            cond = (
                pl.lit(p.enable_sweep) & valid & (depth >= p.min_sweep_atr * atr0) & (depth <= p.max_sweep_atr * atr0) & back
                & (close_q >= p.min_wick_frac) & (col("vol_ratio") >= p.sweep_volume_mult)
            )
            sl = (col("low") - p.sl_buffer_atr * atr0) if up else (col("high") + p.sl_buffer_atr * atr0)
            vmult = p.sweep_volume_mult
        risk = (col("close") - sl) if up else (sl - col("close"))
        aligned = BULLISH if up else BEARISH
        q_trend = pl.when(col(BIAS_COLUMN) == aligned).then(1.0).when(col(BIAS_COLUMN) == 0).then(0.5).otherwise(0.0) if has_bias else pl.lit(0.5)
        quality = pl.mean_horizontal(
            (_c(f"{key}_touches") / 2).clip(0.0, 1.0),
            (_c(f"{key}_span") / (3 * p.min_span_bars)).clip(0.0, 1.0),
            (1.0 - _c(f"{key}_slope_atr").abs() / p.max_slope_atr).clip(0.0, 1.0),
            (col("vol_ratio") / (2 * vmult)).clip(0.0, 1.0),
            close_q.clip(0.0, 1.0),
            q_trend,
        )
        name = f"{_T}{side}_{mode}"
        out += [cond.fill_null(False).alias(name + "_cond"), sl.alias(name + "_sl"), (risk / atr0).alias(name + "_risk_atr"),
                quality.alias(name + "_quality")]
    return out


def detect_trendline_breakout(df: pl.DataFrame, interval_ms: int, params: TrendlineParams | None = None) -> pl.DataFrame:
    """Кадр с признаками (features.smc.add_features [+ htf_bias]) -> тот же кадр + tl_long_* / tl_short_* (signal_columns)."""
    p = params or TrendlineParams()
    if isinstance(interval_ms, bool) or not isinstance(interval_ms, int) or interval_ms <= 0:
        raise ConfigError(f"interval_ms должен быть положительным целым, получено {interval_ms!r}")
    missing = [c for c in _REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок {missing}: сначала features.smc.add_features")
    if p.htf_filter and BIAS_COLUMN not in df.columns:
        raise DataError(f"Trendline_Breakout с htf_filter: нет колонки {BIAS_COLUMN} (оркестратор присоединяет H4 bias)")
    clash = [c for side in SIDES for c in signal_columns(side) if c in df.columns] + [c for c in df.columns if c.startswith(_T)]
    if clash:
        raise DataError(f"Колонки стратегии уже есть в кадре: {clash}")
    work = _with_history(df.with_columns(_stage_anchors(interval_ms)), p, interval_ms)
    work = work.with_columns(_stage_lines(p, interval_ms)).with_columns(_stage_candidates(p, BIAS_COLUMN in df.columns))
    ok = []
    for side, mode in CANDIDATES:
        name = f"{side}_{mode}"
        trend_ok = side_allowed(side) if p.htf_filter else pl.lit(True)
        ok.append((_c(name + "_cond") & trend_ok & (_c(name + "_risk_atr") >= p.min_risk_atr) & (_c(name + "_risk_atr") <= p.max_risk_atr)
                   & (_c(name + "_quality") >= p.min_quality)).fill_null(False).alias(f"{_T}{name}_ok"))
    work = work.with_columns(ok)
    raw = {side: _c(f"{side}_{BREAKOUT}_ok") | _c(f"{side}_{SWEEP}_ok") for side in SIDES}
    work = work.with_columns([(raw[s] & ~(raw["long"] & raw["short"])).alias(f"{_T}{s}_signal") for s in SIDES])
    exprs = []
    for side in SIDES:
        signal, b_ok = _c(f"{side}_signal"), _c(f"{side}_{BREAKOUT}_ok")
        kb, ks = CANDIDATES[(side, BREAKOUT)], CANDIDATES[(side, SWEEP)]

        def pick(b: pl.Expr, s: pl.Expr, signal=signal, b_ok=b_ok) -> pl.Expr:
            return pl.when(signal & b_ok).then(b).when(signal).then(s)

        values = {
            "signal": signal,
            "mode": pick(pl.lit(BREAKOUT), pl.lit(SWEEP)),
            **{k: pick(_c(f"{side}_{BREAKOUT}_{k}"), _c(f"{side}_{SWEEP}_{k}")) for k in ("sl", "quality", "risk_atr")},
            **{k: pick(_c(f"{kb}_{v}"), _c(f"{ks}_{v}")) for k, v in _PER_LINE.items()},
        }
        exprs += [values[n].alias(f"tl_{side}_{n}") for n in _SIGNAL_FIELDS]
    return work.with_columns(exprs).drop([c for c in work.columns if c.startswith(_T)])
