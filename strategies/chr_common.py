"""Общие кирпичи стратегий CHR (`Horizontal_Breakout_CHR`, `Trendline_Breakout_CHR`, 2026-10-19, только Shadow).

Правила CHR (задание пользователя, по разметке 51 графика из charts.xlsx, out/digitized/raw_responses.jsonl):
  * тренд старшего ТФ - фильтр со 100% жёсткостью: лонг только при Bullish H4, шорт только при Bearish; Neutral и нет bias - запрет;
  * >= 2 подтверждённых касаний уровня / линии до пробоя;
  * поджатие (с 2026-10-23, `compression`): ЛОКАЛЬНАЯ база в 5-10 барах непосредственно перед пробоем - волатильность затухает, минимумы (лонг) / максимумы (шорт)
    подтягиваются к уровню, экстремумы "трутся" об уровень, вся база узкая; прежние higher lows на 50 барах поджатием больше не считаются;
  * пробой - закрытие строго за уровнем / линией полнотелой свечой по направлению сделки;
  * стоп один раз при входе за БЛИЖАЙШИМ локальным свингом последних 3-8 баров (`local_stop`) - 0.2 ATR, не за глобальным экстремумом окна; статичный (без безубытка и трейлинга); размер - фиксированные 5 USDT риска, выход - единственный тейк на весь объём 1:4 (registry.CHR_PLAN).

Локальный экстремум (пивот) бара i - D2 с окном k: high[i-k..i-1] < high[i] >= high[i+1..i+k] (для минимума зеркально). Он подтверждён только
на баре i+k, поэтому колонки пивотов пишутся в строку подтверждения (только положительные сдвиги - R3), а детекторы читают их со сдвигом 1
(знание до пробойной свечи j: бары <= j-1, R4).
"""

import polars as pl

from features.htf import BEARISH, BIAS_COLUMN, BULLISH
from utils.errors import DataError

SIDES = ("long", "short")
CHR_TREND = {"long": BULLISH, "short": BEARISH}


def pivot_confirmed(kind: str, k: int) -> pl.Expr:
    """True в строке c, если бар c-k - локальный максимум (kind='high') / минимум ('low') с окном k (D2), подтверждённый закрытием бара c."""
    col = pl.col(kind)
    cand = col.shift(k)
    if kind == "high":
        left = col.shift(k + 1).rolling_max(window_size=k, min_samples=k)
        right = col.rolling_max(window_size=k, min_samples=k)
        return ((left < cand) & (cand >= right)).fill_null(False)
    if kind == "low":
        left = col.shift(k + 1).rolling_min(window_size=k, min_samples=k)
        right = col.rolling_min(window_size=k, min_samples=k)
        return ((left > cand) & (cand <= right)).fill_null(False)
    raise DataError(f"pivot_confirmed: kind должен быть 'high' или 'low', получено {kind!r}")


def pivot_chain(kind: str, k: int, depth: int, prefix: str) -> list[list[pl.Expr]]:
    """Этапы (список списков выражений; каждый этап - отдельный with_columns) для последних `depth` пивотов, подтверждённых к строке:
    `{prefix}{n}_p` (цена) и `{prefix}{n}_t` (open_time бара пивота), n = 1 - самый свежий. Цепочка - forward_fill по строкам подтверждения
    (значение в строке зависит только от прошлых строк). Пивот, подтверждённый в самой строке, уже входит в неё: детектор читает .shift(1)."""
    flag = f"{prefix}flag"
    stages: list[list[pl.Expr]] = [[pivot_confirmed(kind, k).alias(flag)]]
    is_new = pl.col(flag)
    stages.append([pl.when(is_new).then(pl.col(kind).shift(k)).forward_fill().alias(f"{prefix}1_p"),
                   pl.when(is_new).then(pl.col("open_time").shift(k)).forward_fill().alias(f"{prefix}1_t")])
    for n in range(2, depth + 1):  # в строке нового пивота предыдущий n-1-й становится n-м
        stages.append([pl.when(is_new).then(pl.col(f"{prefix}{n - 1}_{f}").shift(1)).otherwise(None).forward_fill().alias(f"{prefix}{n}_{f}")
                       for f in ("p", "t")])
    return stages


def squeeze(side: str, prefix: str, points: int, suffix: str = "_p") -> pl.Expr:
    """Поджатие на последних `points` пивотах (колонки `{prefix}{n}{suffix}`, n = 1 - самый свежий; детектор передаёт цепочку, сдвинутую
    на строку j-1): лонг - каждый следующий минимум СТРОГО выше предыдущего (higher lows); шорт - каждый следующий максимум строго ниже (lower highs)."""
    ok = pl.lit(True)
    for n in range(1, points):  # n - более свежий, n+1 - более старый
        newer, older = pl.col(f"{prefix}{n}{suffix}"), pl.col(f"{prefix}{n + 1}{suffix}")
        ok = ok & ((newer > older) if side == "long" else (newer < older))
    return ok.fill_null(False)


def full_body(side: str, min_body_frac: float) -> pl.Expr:
    """Полнотелая свеча по направлению сделки: тело >= min_body_frac размаха, бычья для лонга / медвежья для шорта."""
    col = pl.col
    rng = col("high") - col("low")
    body = (col("close") - col("open")) if side == "long" else (col("open") - col("close"))
    return ((rng > 0) & (body > 0) & (body >= min_body_frac * rng)).fill_null(False)


def trend_strict(side: str) -> pl.Expr:
    """Жёсткий фильтр тренда CHR, независимо от общего features.htf.side_allowed: лонг ТОЛЬКО при Bullish, шорт ТОЛЬКО при Bearish;
    Neutral, встречный тренд и null (нет bias) - запрет."""
    return (pl.col(BIAS_COLUMN) == CHR_TREND[side]).fill_null(False)


# ------------------------------------------------------------------------------------------------ локальная компрессия и локальный стоп (2026-10-23)
COMP_BARS = 8  # окно компрессии непосредственно перед пробойной свечой, баров (допустимо 5-10)
COMP_DECAY = 0.85  # средний размах свечей свежей половины окна <= этой доли среднего размаха старой половины (волатильность затухает)
COMP_MAX_RANGE_ATR = 0.9  # и свежая половина в среднем уже 0.9 ATR[j-1]: база не из размашистых свечей
COMP_HEIGHT_ATR = 2.5  # вся база (max high - min low окна) не выше 2.5 ATR[j-1]: узкая проторговка, а не волны
COMP_RUB_ATR = 0.6  # экстремумы "трутся" об уровень: среднее расстояние high (лонг) / low (шорт) до уровня <= 0.6 ATR[j-1] (по всему окну и по свежей половине)
SL_SWING_BARS = 8  # стоп ищется за свингом среди последних 3-8 баров перед пробойной (допустимо 3-8)
SL_MAX_RISK_ATR = 2.5  # стоп дальше 2.5 ATR от входа - сигнала нет: стоп обязан быть коротким


def _mean(exprs: list[pl.Expr]) -> pl.Expr:
    return pl.sum_horizontal(exprs) / len(exprs)


def compression(side: str, level_at, atr0: pl.Expr, bars: int = COMP_BARS, decay: float = COMP_DECAY, max_range_atr: float = COMP_MAX_RANGE_ATR,
                height_atr: float = COMP_HEIGHT_ATR, rub_atr: float = COMP_RUB_ATR) -> dict[str, pl.Expr]:
    """Строгая модель локального поджатия под уровнем (лонг; шорт - зеркало) на `bars` барах j-bars .. j-1 (j - пробойная свеча, в окно не входит; R3/R4).
    Окно делится на старую и свежую половины (при нечётном числе баров средний пропускается). `level_at(s)` - выражение уровня / линии на баре j-s.
    Условия (все обязательны):
      1. затухание: средний размах (high-low) свежей половины <= decay * среднего размаха старой;
      2. свежая половина в среднем не шире max_range_atr * ATR[j-1];
      3. узкая база: max high - min low всего окна <= height_atr * ATR[j-1];
      4. трение об уровень: среднее расстояние high до уровня <= rub_atr * ATR[j-1] по всему окну И по свежей половине;
      5. минимумы подтягиваются: среднее расстояние low до уровня у свежей половины меньше, чем у старой, и самый низкий low свежей половины выше самого низкого старой.
    -> {"ok", "floor_new", "floor_old", "decay_ratio", "rub_atr", "height_atr"} (floor_* - low-экстремумы половин: значения поджатия для графика / отчёта)."""
    if not 5 <= bars <= 10:
        raise DataError(f"compression: окно поджатия 5-10 баров, получено {bars}")
    long = side == "long"
    sgn = 1.0 if long else -1.0
    hi, lo = pl.col("high"), pl.col("low")
    h = bars // 2
    fresh, old, every = range(1, h + 1), range(bars - h + 1, bars + 1), range(1, bars + 1)
    rng = lambda s: hi.shift(s) - lo.shift(s)  # noqa: E731
    mean_new, mean_old = _mean([rng(s) for s in fresh]), _mean([rng(s) for s in old])
    ext, floor = (hi, lo) if long else (lo, hi)
    rub = lambda s: sgn * (level_at(s) - ext.shift(s))  # noqa: E731
    base = lambda s: sgn * (level_at(s) - floor.shift(s))  # noqa: E731
    height = pl.max_horizontal([hi.shift(s) for s in every]) - pl.min_horizontal([lo.shift(s) for s in every])
    best = (lambda ss: pl.min_horizontal([lo.shift(s) for s in ss])) if long else (lambda ss: pl.max_horizontal([hi.shift(s) for s in ss]))
    floor_new, floor_old = best(fresh), best(old)
    rub_all, rub_new = _mean([rub(s) for s in every]), _mean([rub(s) for s in fresh])
    ok = ((mean_old > 0) & (mean_new <= decay * mean_old) & (mean_new <= max_range_atr * atr0) & (height <= height_atr * atr0)
          & (rub_all <= rub_atr * atr0) & (rub_new <= rub_atr * atr0)
          & (_mean([base(s) for s in fresh]) < _mean([base(s) for s in old])) & (sgn * floor_new > sgn * floor_old))
    return {"ok": ok.fill_null(False), "floor_new": floor_new, "floor_old": floor_old, "decay_ratio": pl.when(mean_old > 0).then(mean_new / mean_old),
            "rub_atr": rub_all / atr0, "height_atr": height / atr0}


def local_stop(side: str, atr0: pl.Expr, swing_bars: int = SL_SWING_BARS, buffer_atr: float = 0.2) -> tuple[pl.Expr, pl.Expr]:
    """(стоп, экстремум): стоп СТРОГО за ближайшим локальным свингом среди баров j-2 .. j-swing_bars (j - пробойная свеча) - buffer_atr * ATR[j-1].
    Свинг лонга - low бара i строго ниже low соседа слева (i-1) и не выше low соседа справа (i+1); шорт - зеркально. Берётся САМЫЙ БЛИЖАЙШИЙ к пробою свинг
    (перебор от j-2 назад). Свинга в окне нет -> экстремум 3 последних баров. Если low (high) самой пробойной свечи глубже свинга - экстремум берётся по ней
    (стоп не внутри свечи пробоя). Глобального минимума окна поиска здесь нет."""
    if not 3 <= swing_bars <= 8:
        raise DataError(f"local_stop: окно свинга 3-8 баров, получено {swing_bars}")
    long = side == "long"
    col = pl.col
    ext = col("low") if long else col("high")
    swings = []
    for s in range(2, swing_bars + 1):
        v, left, right = ext.shift(s), ext.shift(s + 1), ext.shift(s - 1)
        is_swing = ((v < left) & (v <= right)) if long else ((v > left) & (v >= right))
        swings.append(pl.when(is_swing.fill_null(False)).then(v))
    tail = [ext.shift(s) for s in (1, 2, 3)]
    fallback = pl.min_horizontal(tail) if long else pl.max_horizontal(tail)
    extreme = pl.coalesce(*swings, fallback)
    extreme = pl.min_horizontal(extreme, col("low")) if long else pl.max_horizontal(extreme, col("high"))
    sgn = 1.0 if long else -1.0
    return extreme - sgn * buffer_atr * atr0, extreme
