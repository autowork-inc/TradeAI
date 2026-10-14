"""HTF Bias: тренд старшего таймфрейма (H4) по рыночной структуре - фильтр для сетапов M15 (2026-10-11).

Определение (на закрытой H4-свече j, по подтверждённым свингам features.smc.add_features):
  * Bullish (+1): последний подтверждённый Swing High выше предыдущего (HH) И последний Swing Low выше предыдущего (HL);
  * Bearish (-1): последний Swing High ниже предыдущего (LH) И последний Swing Low ниже предыдущего (LL);
  * Neutral ( 0): структура смешанная или свингов ещё меньше двух с каждой стороны - торговать можно в обе стороны;
  * null: у H4-свечи ещё нет ATR (прогрев) - фильтр считает такое состояние запретом (fail-closed).
Структура, а не EMA 50/200: рекурсивная EMA зависит от глубины истории (live ~300 баров != бэктест, нарушение R8), а свинги -
нет (та же причина, что у ATR-SMA, О7). Цена: свинг на H4 подтверждается через k = 5 баров (20 часов), поэтому смена
тренда видна с задержкой - так же, как у любого подтверждённого свинга.

Без заглядывания в будущее (R3/R4). Значение H4-свечи с open_time = T доступно для M15 только с её ЗАКРЫТИЯ: M15-свеча с
open_time >= T + 4h видит этот bias, более ранние - предыдущий. Привязка по open_time M15 (а не по close_time) намеренно:
M15-свеча, закрывающаяся в тот же момент, что и H4, получила бы bias, который в живом потоке может прийти позже неё
(гонка двух WebSocket-событий), и live разошёлся бы с бэктестом (R8). Теперь запас - 15 минут.

Все функции векторные (Polars, R2): rolling/when-then/join_asof, без итераций по строкам.
"""

import polars as pl

from utils.errors import DataError

BULLISH, BEARISH, NEUTRAL = 1, -1, 0
BIAS_LABELS = {BULLISH: "Bullish", BEARISH: "Bearish", NEUTRAL: "Neutral"}
BIAS_COLUMN = "htf_bias"
AVAIL_COLUMN = "htf_avail_ms"

_NEEDED = ("open_time", "atr", "sh1_price", "sh2_price", "sl1_price", "sl2_price")


def htf_bias_table(h4: pl.DataFrame, interval_ms: int, *, avail_column: str = AVAIL_COLUMN, bias_column: str = BIAS_COLUMN) -> pl.DataFrame:
    """H4-кадр с признаками (features.smc.add_features) -> таблица [htf_avail_ms, htf_bias] по возрастанию времени.
    Имена колонок задаются (мульти-таймфреймовый режим: `1d_avail_ms` / `1d_bias`); определение bias от таймфрейма не зависит."""
    missing = [c for c in _NEEDED if c not in h4.columns]
    if missing:
        raise DataError(f"HTF: в H4-кадре нет колонок {missing} (сначала features.smc.add_features)")
    if h4.height == 0:
        raise DataError("HTF: пустой H4-кадр")
    hh, hl = pl.col("sh1_price") > pl.col("sh2_price"), pl.col("sl1_price") > pl.col("sl2_price")
    lh, ll = pl.col("sh1_price") < pl.col("sh2_price"), pl.col("sl1_price") < pl.col("sl2_price")
    bias = (
        pl.when(pl.col("atr").is_null()).then(None)
        .when((hh & hl).fill_null(False)).then(BULLISH)
        .when((lh & ll).fill_null(False)).then(BEARISH)
        .otherwise(NEUTRAL)
        .cast(pl.Int8)
    )
    return h4.select((pl.col("open_time") + interval_ms).alias(avail_column), bias.alias(bias_column))


def attach_htf_bias(frame: pl.DataFrame, table: pl.DataFrame, *, avail_column: str = AVAIL_COLUMN, bias_column: str = BIAS_COLUMN) -> pl.DataFrame:
    """Добавить к кадру младшего таймфрейма колонки htf_bias и htf_avail_ms: последняя старшая свеча, закрывшаяся не позже open_time его свечи."""
    clash = [c for c in (bias_column, avail_column) if c in frame.columns]
    if clash:
        raise DataError(f"HTF: колонки {clash} уже есть в кадре")
    if "open_time" not in frame.columns:
        raise DataError("HTF: в кадре нет open_time")
    return frame.join_asof(table, left_on="open_time", right_on=avail_column, strategy="backward")


def side_allowed(side: str, column: str = BIAS_COLUMN) -> pl.Expr:
    """Сторона сетапа разрешена структурой старшего таймфрейма: шорт - не при Bullish, лонг - не при Bearish; null (нет bias) - запрет."""
    if side == "short":
        return (pl.col(column) <= NEUTRAL).fill_null(False)
    if side == "long":
        return (pl.col(column) >= NEUTRAL).fill_null(False)
    raise DataError(f"HTF: неизвестная сторона {side!r}")
