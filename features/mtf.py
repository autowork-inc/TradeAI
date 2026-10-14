"""Мульти-таймфреймовые признаки SMC: 1d (HTF Bias) -> 1h (пул ликвидности и свип) -> 5m (FVG, CHoCH, вход). Только векторный Polars (R2).

Идея. Автомат сетапа (strategies/smc_setups.py) считается на рабочем 5m-кадре, но «что считать свипом» решает старший таймфрейм:
  * 1h-кадр (после features.smc.add_features) -> `h1_table`: на каждую закрытую 1h-свечу - время доступности, её OHLC/ATR/свинги и, по сторонам,
    флаг СВИПА пула ликвидности 1h (`1h_short_sweep` / `1h_long_sweep`), уровень пула, его время и экстремум свипающей 1h-свечи;
  * 1d-кадр -> `d1_bias_table`: тренд (HH+HL / LH+LL, features/htf.py) с именами колонок `1d_avail_ms` / `1d_bias`;
  * `attach_mtf(frame_5m, h1, d1)` присоединяет обе таблицы к 5m-кадру через join_asof (backward по времени ДОСТУПНОСТИ): строка 5m видит последнюю
    1h/1d-свечу, закрывшуюся не позже её open_time (forward-fill без заглядывания в будущее).

Свип - событие 1h-свечи. Он «наступает» для 5m-автомата на ПЕРВОЙ 5m-свече после закрытия 1h-свечи (`open_time == 1h_avail_ms`; на остальных 11 строках часа
значения те же, но событием они не являются). Поэтому FVG (S2) и CHoCH (S3) на 5m строго ПОСЛЕ свипа: ни одна 5m-свеча внутри самой свипающей 1h-свечи в сетап
не входит. Привязка по open_time 5m (а не по её закрытию) намеренно, как у HTF Bias: 1h-событие и закрытие 5m-свечи приходят разными WebSocket-потоками,
и live разошёлся бы с бэктестом (R8); запас - одна 5m-свеча.

Определение свипа ОДНО на оба случая (R8): `sweep_raw` - то же выражение, по которому одно-таймфреймовый автомат считает свип на своём кадре
(пул - последний подтверждённый свинг-хай, ещё не пробитый после подтверждения; high > пула, close < пула). Лонг - зеркало (цены * -1, `mirrored_columns`).

Колонки 1h/1d приходят из ВНЕШНИХ кадров, поэтому свойство R4 здесь формулируется так: таблица по 1h-кадру, обрезанному на свече j, совпадает со строкой j
таблицы по всей истории (тест tests/features/test_mtf.py), а присоединение по времени не использует ничего позже open_time 5m-строки.
"""

import polars as pl

from features.htf import htf_bias_table
from utils.errors import DataError

H1_PREFIX = "1h_"
D1_PREFIX = "1d_"
H1_AVAIL = "1h_avail_ms"
H1_OPEN = "1h_open_time"
D1_AVAIL = "1d_avail_ms"
D1_BIAS = "1d_bias"
SIDES = ("short", "long")

_H1_NEEDED = ("open_time", "open", "high", "low", "close", "atr", "swing_high", "swing_low", "sh1_price", "sh1_time", "sl1_price", "sl1_time",
              "fvg_bear", "fvg_bull")
_H1_PASSTHROUGH = ("high", "low", "close", "atr", "sh1_price", "sh1_time", "sl1_price", "sl1_time", "fvg_bear", "fvg_bull")


def h1_sweep_columns(side: str) -> tuple[str, ...]:
    return tuple(f"{H1_PREFIX}{side}_{n}" for n in ("sweep", "pool", "pool_time", "extreme"))


def h1_columns() -> tuple[str, ...]:
    """Все колонки, которые `h1_table` отдаёт 5m-кадру (кроме ключа соединения `1h_avail_ms`, он тоже присоединяется)."""
    return (H1_AVAIL, H1_OPEN, *(f"{H1_PREFIX}{c}" for c in _H1_PASSTHROUGH), *(c for s in SIDES for c in h1_sweep_columns(s)))


def mirrored_columns(side: str) -> dict[str, pl.Expr]:
    """Колонки кадра в «шортовом» мире (лонг - зеркало: цены умножены на -1). Одни и те же выражения использует и 5m-автомат, и 1h-свип."""
    c = pl.col
    if side == "short":
        return {
            "o": c("open"), "h": c("high"), "l": c("low"), "c": c("close"),
            "pool_flag": c("swing_high"), "pool_price": c("sh1_price"), "pool_time": c("sh1_time"),
            "ref_price": c("sl1_price"), "ref_time": c("sl1_time"),
            "fvg": c("fvg_bear"), "fvg_mid": c("fvg_bear_mid"), "fvg_far": c("fvg_bear_top"), "fvg_near": c("fvg_bear_bottom"),
            "fvg_t1": c("fvg_bear_t1"),
        }
    if side == "long":
        return {
            "o": -c("open"), "h": -c("low"), "l": -c("high"), "c": -c("close"),
            "pool_flag": c("swing_low"), "pool_price": -c("sl1_price"), "pool_time": c("sl1_time"),
            "ref_price": -c("sh1_price"), "ref_time": c("sh1_time"),
            "fvg": c("fvg_bull"), "fvg_mid": -c("fvg_bull_mid"), "fvg_far": -c("fvg_bull_bottom"), "fvg_near": -c("fvg_bull_top"),
            "fvg_t1": c("fvg_bull_t1"),
        }
    raise DataError(f"Неизвестная сторона {side!r}")


def sweep_raw(w: pl.DataFrame) -> pl.DataFrame:
    """S0/S1 в зеркальном мире. Вход: колонки h, c, grp (номер группы свинга: cum_sum флага свинга), L (пул ДО бара: sh1.shift(1)).
    Выход: тот же кадр + `grp_max` и `sweep_raw`. Пул не пробит (cum_max(high) по группе до бара j-1 не выше уровня), на баре j: high > пула, close < пула."""
    w = w.with_columns(pl.col("h").cum_max().over("grp").alias("grp_max"))
    taken_prev = (pl.col("grp_max").shift(1) > pl.col("L")).fill_null(True)
    sweep = (pl.col("h") > pl.col("L")) & (pl.col("c") < pl.col("L")) & ~taken_prev
    return w.with_columns(sweep.fill_null(False).alias("sweep_raw"))


def _h1_side(h1: pl.DataFrame, side: str) -> pl.DataFrame:
    sgn = 1.0 if side == "short" else -1.0
    m = mirrored_columns(side)
    w = h1.select(m["h"].alias("h"), m["c"].alias("c"), m["pool_flag"].cast(pl.Int64).cum_sum().alias("grp"),
                  m["pool_price"].shift(1).alias("L"), m["pool_time"].shift(1).alias("Lt"))
    w = sweep_raw(w)
    sw = pl.col("sweep_raw")
    names = h1_sweep_columns(side)
    return w.select(
        sw.alias(names[0]),
        pl.when(sw).then(pl.col("L") * sgn).alias(names[1]),  # обратно из зеркального мира: цена пула
        pl.when(sw).then(pl.col("Lt")).alias(names[2]),
        pl.when(sw).then(pl.col("h") * sgn).alias(names[3]),  # экстремум свипающей 1h-свечи: хай (шорт) / лоу (лонг)
    )


def h1_table(h1: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """1h-кадр с признаками (features.smc.add_features) -> таблица по закрытым 1h-свечам для присоединения к 5m-кадру (см. докстринг модуля)."""
    missing = [c for c in _H1_NEEDED if c not in h1.columns]
    if missing:
        raise DataError(f"MTF: в 1h-кадре нет колонок {missing} (сначала features.smc.add_features)")
    if h1.height == 0:
        raise DataError("MTF: пустой 1h-кадр")
    base = h1.select(
        (pl.col("open_time") + interval_ms).alias(H1_AVAIL),
        pl.col("open_time").alias(H1_OPEN),
        *(pl.col(c).alias(f"{H1_PREFIX}{c}") for c in _H1_PASSTHROUGH),
    )
    return base.hstack([col for side in SIDES for col in _h1_side(h1, side).get_columns()])


def d1_bias_table(d1: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """1d-кадр с признаками -> таблица [1d_avail_ms, 1d_bias]; определение bias то же, что у H4 (features/htf.py)."""
    return htf_bias_table(d1, interval_ms, avail_column=D1_AVAIL, bias_column=D1_BIAS)


def attach_table(frame: pl.DataFrame, table: pl.DataFrame, avail_column: str) -> pl.DataFrame:
    """Присоединить таблицу старшего таймфрейма: каждая строка получает последнюю свечу, закрывшуюся не позже её open_time."""
    clash = [c for c in table.columns if c in frame.columns]
    if clash:
        raise DataError(f"MTF: колонки {clash} уже есть в кадре")
    if "open_time" not in frame.columns:
        raise DataError("MTF: в кадре нет open_time")
    return frame.join_asof(table, left_on="open_time", right_on=avail_column, strategy="backward")


def attach_mtf(frame: pl.DataFrame, h1: pl.DataFrame, d1: pl.DataFrame) -> pl.DataFrame:
    """5m-кадр + таблицы `h1_table` и `d1_bias_table` -> кадр с колонками `1h_*` и `1d_*` (forward-fill по времени доступности). Чистая функция."""
    return attach_table(attach_table(frame, h1, H1_AVAIL), d1, D1_AVAIL)


def new_h1_event() -> pl.Expr:
    """Строка 5m - первая после закрытия очередной 1h-свечи (в этой строке 1h-свип становится известен). Каузально: зависит только от самой строки."""
    return (pl.col("open_time") == pl.col(H1_AVAIL)).fill_null(False)


__all__ = ["D1_AVAIL", "D1_BIAS", "D1_PREFIX", "H1_AVAIL", "H1_OPEN", "H1_PREFIX", "SIDES", "attach_mtf", "attach_table", "d1_bias_table",
           "h1_columns", "h1_sweep_columns", "h1_table", "mirrored_columns", "new_h1_event", "sweep_raw"]
