"""Общие кирпичи стратегий CHR (`Horizontal_Breakout_CHR`, `Trendline_Breakout_CHR`, 2026-10-19, только Shadow).

Правила CHR (задание пользователя, по разметке 51 графика из charts.xlsx, out/digitized/raw_responses.jsonl):
  * тренд старшего ТФ - фильтр со 100% жёсткостью: лонг только при Bullish H4, шорт только при Bearish; Neutral и нет bias - запрет;
  * >= 2 подтверждённых касаний уровня / линии до пробоя;
  * поджатие: лонг - последовательно повышающиеся локальные минимумы (higher lows) под уровнем; шорт - понижающиеся максимумы (lower highs);
  * пробой - закрытие строго за уровнем / линией полнотелой свечой по направлению сделки;
  * стоп один раз при входе за последним экстремумом поджатия, статичный (без безубытка и трейлинга); размер - фиксированные 5 USDT риска, выход - единственный тейк на весь объём 1:4 (registry.CHR_PLAN).

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
