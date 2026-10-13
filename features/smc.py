"""Признаки SMC Swing / Breakout Buildup: ATR, свинги, FVG, поджатие. Только векторный Polars (R2).

Контракт входа. Кадр ОДНОЙ монеты и ОДНОГО таймфрейма из ЗАКРЫТЫХ свечей (R5: формирующуюся свечу вызывающий
не передаёт) с колонками open_time (мс, Int64), open, high, low, close, volume. Строки идут по возрастанию
open_time ровно с шагом interval_ms: пропуск свечи - DataError, а не «тихая» склейка (вызывающий сам режет
историю на непрерывные сегменты). Нулей (null/NaN) и невозможных свечей (high < low и т.п.) быть не может.

Контракт выхода. Входные колонки + признаки. Значение в строке j зависит ТОЛЬКО от строк <= j (R3), поэтому
оно совпадает при расчёте по всей истории и по обрезанной на j (тест обрезки R4, tests/features/test_smc.py).
Из этого же следует паритет бэктеста и live (R8): в бою считается последнее окно ~300 баров, и после прогрева
значения те же (память признаков конечна: ATR и SMA - простые скользящие, не рекурсивные EWM).

Время подтверждения. Свинг на баре i подтверждается на баре j = i + k (D10: k = 5): флаг ставится на строку j,
цена и время самого свинга лежат в *_price / *_time. Раньше j флага нет - иначе заглядывание в будущее.

Что решено в CLAUDE.md (раздел 6):
  D2  свинг-хай: хай кандидата строго больше k левых баров и не меньше k правых (`left_max < cand >= right_max`);
      свинг-лоу зеркально. Из двух равных хаёв свингом становится ПЕРВЫЙ.
  D3  амплитуда: (хай кандидата - минимум low окна [i-k, i+k]) > 1.5 * ATR[i-1]; для лоу зеркально.
  О1  ATR[i-1] - ATR бара, предшествующего кандидату; период 14.
  D7  середина FVG (fvg_*_mid) - цена лимитки.
ATR здесь - простое скользящее среднее истинного диапазона (SMA-ATR), а не рекурсивное сглаживание Уайлдера:
у рекурсивного память бесконечна, и значение зависело бы от того, с какого бара начато окно (нарушение R8).
Скользящие средние собраны сложением сдвигов (а не rolling_mean): порядок сложения зафиксирован, и результат
побитово не зависит от длины кадра по построению. Замер на rolling_mean расхождений не нашёл (98 точек,
Polars 1.44.2), но такой гарантии нет в документации; тест обрезки требует точного равенства, поэтому
берём вариант с гарантией.

Не здесь: свип, CHoCH, триггеры входа и выхода, жизненный цикл сетапа - это детекторы Этапа 2, они читают
эти колонки.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from utils.errors import ConfigError, DataError

REQUIRED_COLUMNS = ("open_time", "open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class SmcParams:
    k: int = 5  # D10: окно свинга 2k + 1 = 11 баров
    atr_period: int = 14  # О1/О5
    swing_atr_mult: float = 1.5  # D3: X
    impulse_atr_mult: float = D.IMPULSE_ATR_MULT  # разд. 3.5 п.3, О12: тело импульсной свечи FVG (1.0)
    eq_tolerance: float = 0.001  # разд. 3.6 п.1: (max - min) / min <= 0.1%
    volume_sma: int = 20  # разд. 3.6 п.4

    def __post_init__(self) -> None:
        for name in ("k", "atr_period", "volume_sma"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ConfigError(f"SmcParams.{name} должен быть целым >= 1, получено {value!r}")
        for name in ("swing_atr_mult", "impulse_atr_mult", "eq_tolerance"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value >= 0:
                raise ConfigError(f"SmcParams.{name} должен быть числом >= 0, получено {value!r}")

    @property
    def warmup_bars(self) -> int:
        """Сколько баров нужно, чтобы ATR[i-1] первого возможного свинга i существовал (+ окно 2k + 1)."""
        return max(self.atr_period + 1 + self.k + 1, 2 * self.k + 1)


# Все колонки, которые добавляет add_features (тесты и детекторы Этапа 2 опираются на этот список).
def feature_columns() -> tuple[str, ...]:
    cols = ["atr", "vol_sma_prev", "vol_ratio", "swing_high", "swing_low"]
    for prefix in ("sh", "sl"):
        for n in (1, 2, 3):
            cols += [f"{prefix}{n}_price", f"{prefix}{n}_time"]
    cols += ["eqh_spread", "eqh_ok", "hl_ok", "buildup", "buildup_level", "buildup_low3"]
    for side in ("bear", "bull"):
        cols += [f"fvg_{side}", f"fvg_{side}_top", f"fvg_{side}_bottom", f"fvg_{side}_mid", f"fvg_{side}_t1"]
    return tuple(cols)


# ------------------------------------------------------------------------------------ проверка входа


def validate_candles(df: pl.DataFrame, interval_ms: int) -> None:
    """Fail-fast (R1): кадр нарушает контракт входа -> DataError. Это проверка данных, а не расчёт паттернов."""
    if isinstance(interval_ms, bool) or not isinstance(interval_ms, int) or interval_ms <= 0:
        raise ConfigError(f"interval_ms должен быть положительным целым, получено {interval_ms!r}")
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise DataError(f"В кадре свечей нет колонок {missing}")
    if "symbol" in df.columns and df["symbol"].n_unique() > 1:
        raise DataError("В кадре несколько символов: признаки считаются по одной монете за вызов")
    if df.height == 0:
        return
    if df["open_time"].dtype != pl.Int64:
        raise DataError(f"open_time должен быть Int64 (мс), получено {df['open_time'].dtype}")
    for c in REQUIRED_COLUMNS[1:]:
        if not df[c].dtype.is_float() and not df[c].dtype.is_integer():
            raise DataError(f"Колонка {c} не числовая: {df[c].dtype}")
    checks = df.select(
        nulls=pl.any_horizontal([pl.col(c).is_null() for c in REQUIRED_COLUMNS]).any(),
        nonfinite=pl.any_horizontal([pl.col(c).cast(pl.Float64).is_finite().not_() for c in REQUIRED_COLUMNS[1:]]).any(),
        bad_step=(pl.col("open_time").diff().drop_nulls() != interval_ms).any(),
        bad_ohlc=(
            (pl.col("high") < pl.max_horizontal("open", "close", "low"))
            | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
            | (pl.col("low") <= 0)
            | (pl.col("volume") < 0)
        ).any(),
    ).row(0, named=True)
    if checks["nulls"]:
        raise DataError("В свечах есть null")
    if checks["nonfinite"]:
        raise DataError("В свечах есть NaN или inf")
    if checks["bad_step"]:
        raise DataError(f"open_time не идёт строго с шагом {interval_ms} мс (пропуск, дубль или неверный порядок)")
    if checks["bad_ohlc"]:
        raise DataError("Невозможная свеча: high < max(open, close, low), low > min(...), low <= 0 или volume < 0")


# --------------------------------------------------------------------------------------- строительные блоки


def _sma(expr: pl.Expr, n: int) -> pl.Expr:
    """Среднее по n последним строкам, включая текущую; null, пока нет n значений. Сумма сдвигов в фиксированном
    порядке: результат побитово не зависит от длины кадра. Цикл идёт по смещениям окна (константа), не по строкам."""
    acc = expr
    for s in range(1, n):
        acc = acc + expr.shift(s)
    return acc / n


def _true_range() -> pl.Expr:
    h, low, pc = pl.col("high"), pl.col("low"), pl.col("close").shift(1)
    tr = pl.max_horizontal(h - low, (h - pc).abs(), (low - pc).abs())
    # max_horizontal пропускает null, поэтому у первого бара (нет прошлого close) явно ставим null.
    return pl.when(pc.is_null()).then(pl.lit(None, dtype=pl.Float64)).otherwise(tr)


def _swing_columns(p: SmcParams) -> list[pl.Expr]:
    k, w = p.k, 2 * p.k + 1
    high, low = pl.col("high"), pl.col("low")
    atr_before = pl.col("atr").shift(k + 1)  # ATR[i-1], где i = j - k (О1)

    cand_h = high.shift(k)
    left_max = high.shift(k + 1).rolling_max(window_size=k, min_samples=k)  # бары i-k .. i-1
    right_max = high.rolling_max(window_size=k, min_samples=k)  # бары i+1 .. j
    window_low = low.rolling_min(window_size=w, min_samples=w)  # бары i-k .. i+k
    swing_high = (
        (cand_h > left_max) & (cand_h >= right_max) & ((cand_h - window_low) > p.swing_atr_mult * atr_before)
    ).fill_null(False)

    cand_l = low.shift(k)
    left_min = low.shift(k + 1).rolling_min(window_size=k, min_samples=k)
    right_min = low.rolling_min(window_size=k, min_samples=k)
    window_high = high.rolling_max(window_size=w, min_samples=w)
    swing_low = (
        (cand_l < left_min) & (cand_l <= right_min) & ((window_high - cand_l) > p.swing_atr_mult * atr_before)
    ).fill_null(False)

    t = pl.col("open_time").shift(k)
    return [
        swing_high.alias("swing_high"),
        swing_low.alias("swing_low"),
        pl.when(swing_high).then(cand_h).alias("_sh_price"),
        pl.when(swing_high).then(t).alias("_sh_time"),
        pl.when(swing_low).then(cand_l).alias("_sl_price"),
        pl.when(swing_low).then(t).alias("_sl_time"),
    ]


def _fvg_columns(p: SmcParams) -> list[pl.Expr]:
    """FVG из трёх свечей i-2, i-1 (импульс), i = текущая строка j. Импульс: тело > 1.5 * ATR[i-2] и направление."""
    o1, c1 = pl.col("open").shift(1), pl.col("close").shift(1)
    body = (c1 - o1).abs()
    impulse = body > p.impulse_atr_mult * pl.col("atr").shift(2)
    low2, high2 = pl.col("low").shift(2), pl.col("high").shift(2)
    high0, low0 = pl.col("high"), pl.col("low")
    t1 = pl.col("open_time").shift(2)

    bear = ((low2 > high0) & impulse & (c1 < o1)).fill_null(False)
    bull = ((high2 < low0) & impulse & (c1 > o1)).fill_null(False)
    return [
        bear.alias("fvg_bear"),
        pl.when(bear).then(low2).alias("fvg_bear_top"),
        pl.when(bear).then(high0).alias("fvg_bear_bottom"),
        pl.when(bear).then((low2 + high0) / 2).alias("fvg_bear_mid"),
        pl.when(bear).then(t1).alias("fvg_bear_t1"),
        bull.alias("fvg_bull"),
        pl.when(bull).then(low0).alias("fvg_bull_top"),
        pl.when(bull).then(high2).alias("fvg_bull_bottom"),
        pl.when(bull).then((high2 + low0) / 2).alias("fvg_bull_mid"),
        pl.when(bull).then(t1).alias("fvg_bull_t1"),
    ]


# ------------------------------------------------------------------------------------------- публичное


def add_features(df: pl.DataFrame, params: SmcParams | None = None, *, interval_ms: int) -> pl.DataFrame:
    """Свечи одной монеты -> те же свечи + признаки (feature_columns()). См. докстринг модуля."""
    p = params or SmcParams()
    validate_candles(df, interval_ms)
    base = df.with_columns(
        _sma(_true_range(), p.atr_period).alias("atr"),
        _sma(pl.col("volume").shift(1), p.volume_sma).alias("vol_sma_prev"),  # без текущего бара: пробойный объём не входит
    ).with_columns(
        pl.when(pl.col("vol_sma_prev") > 0).then(pl.col("volume") / pl.col("vol_sma_prev")).alias("vol_ratio"),
        *_swing_columns(p),
        *_fvg_columns(p),
    )

    # Три последних подтверждённых свинга (1 - самый свежий). На флаговой строке «предыдущий» берётся со строки j-1.
    for prefix, flag, price, time in (("sh", "swing_high", "_sh_price", "_sh_time"), ("sl", "swing_low", "_sl_price", "_sl_time")):
        f = pl.col(flag)
        base = base.with_columns(
            pl.col(price).forward_fill().alias(f"{prefix}1_price"),
            pl.col(time).forward_fill().alias(f"{prefix}1_time"),
        )
        for n in (2, 3):
            base = base.with_columns(
                pl.when(f).then(pl.col(f"{prefix}{n - 1}_price").shift(1)).forward_fill().alias(f"{prefix}{n}_price"),
                pl.when(f).then(pl.col(f"{prefix}{n - 1}_time").shift(1)).forward_fill().alias(f"{prefix}{n}_time"),
            )

    hi = [pl.col(f"sh{n}_price") for n in (1, 2, 3)]
    lo = [pl.col(f"sl{n}_price") for n in (1, 2, 3)]
    h_max, h_min = pl.max_horizontal(hi), pl.min_horizontal(hi)  # max/min_horizontal пропускают null: проверяем все три явно
    highs_known = pl.all_horizontal([e.is_not_null() for e in hi])
    all_known = highs_known & pl.all_horizontal([e.is_not_null() for e in lo])
    return (
        base.with_columns(
            pl.when(highs_known & (h_min > 0)).then((h_max - h_min) / h_min).alias("eqh_spread"),
            pl.when(highs_known).then(h_max).alias("buildup_level"),
            pl.col("sl1_price").alias("buildup_low3"),
        )
        .with_columns(
            (pl.col("eqh_spread") <= p.eq_tolerance).fill_null(False).alias("eqh_ok"),
            ((lo[0] > lo[1]) & (lo[1] > lo[2])).fill_null(False).alias("hl_ok"),
        )
        .with_columns(
            # (предложено, О6) все три лоу образовались после самого старого из трёх хаёв
            (pl.col("eqh_ok") & pl.col("hl_ok") & all_known & (pl.col("sl3_time") > pl.col("sh3_time")))
            .fill_null(False)
            .alias("buildup")
        )
        .drop("_sh_price", "_sh_time", "_sl_price", "_sl_time")
        .select(*df.columns, *feature_columns())
    )

