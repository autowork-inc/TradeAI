"""Стратегия 1 (SMC Swing): конечный автомат сетапа S0 -> S1 -> S2 -> S3 -> S4, целиком векторный Polars (R2).

Вход: кадр после features.smc.add_features. Выход: тот же кадр + колонки `short_*` и `long_*`.
Значение в строке j зависит только от строк <= j (R3/R4): тест обрезки в tests/strategies/test_smc_setups.py.

Как автомат записан без циклов. Один сетап = один свип. Номер сетапа `sid = cum_sum(свип)`: новый свип начинает
новый сетап, а все «текущие» величины сетапа берутся как forward_fill / cum_max / cum_sum внутри `.over(sid)`.
Если свип случился при ещё живом старом сетапе, побеждает новый (структура изменилась).

  S0  пул ликвидности: последний ПОДТВЕРЖДЁННЫЙ свинг-хай (sh1) и он ещё не был пробит ни одним high после
      подтверждения (cum_max(high) внутри группы свинга <= уровня). Пул известен ДО бара j (sh1.shift(1)).
  S1  свип: high[j] > пул, close[j] < пул. Хай свипа = running max(high) с бара свипа (растёт, пока сетап жив).
      Сетап «убит» (dead) закрытием выше хая свипа; «просрочен» через setup_ttl_bars баров после свипа (D8/О12: 30).
  S2  падение + FVG: медвежий FVG (features.smc), первая свеча которого (i-2) не раньше бара свипа. Берётся
      последний такой FVG сетапа (его mid - цена лимитки, D7).
  S3  CHoCH: ПЕРВОЕ закрытие ниже последнего подтверждённого свинг-лоу (динамического sl1), который образовался
      ПОСЛЕ свипнутого хая (О10). Если первое такое закрытие произошло без FVG, сетап мёртв: слом без имбаланса
      не берём (порядок состояний S2 -> S3 обязателен).
  S4  заявка: LIMIT на fvg_mid, SL = хай свипа + 0.2*ATR, TP = 3R. Сигнал отклоняется, если лимитка не пассивна
      (mid не выше close - тогда она исполнилась бы как тейкер, а у нас post-only GTX) или плечо > max_exposure
      (О8: риск 1% при узком стопе не урезаем, а пропускаем и пишем причину).

Лонг - зеркало: цены умножаются на -1 (high' = -low, low' = -high), и та же функция считает «шорт» в зеркальном
мире; на выходе цены возвращаются обратно. Одна функция детекции для обеих сторон (R8).

HTF-фильтр (`htf_filter=True`): сетап против структуры H4 не начинается вовсе - свип считается только если
side_allowed(side) (features/htf.py: шорт не при Bullish, лонг не при Bearish, нет bias - запрет); дальше автомат прежний.

Состояние `*_state` в строке j: 0 нет живого сетапа, 1 свип, 2 свип + FVG, 3 CHoCH отклонён, 4 сигнал (заявка).
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN, side_allowed
from utils.errors import ConfigError, DataError

SIDES = ("short", "long")


@dataclass(frozen=True)
class SmcSetupParams:
    setup_ttl_bars: int = D.SETUP_TTL_BARS  # D8
    sl_buffer_atr: float = D.SL_BUFFER_ATR  # О5
    tp_r: float = D.TP_R  # D9
    risk_per_trade: float = D.RISK_PER_TRADE  # D10
    max_exposure: float = D.MAX_EXPOSURE  # О8
    choch_ref_after_pool: bool = True  # О10: опорный свинг-лоу для CHoCH образовался после свипнутого хая
    htf_filter: bool = False  # 2026-10-11: свип (S1) возможен только по тренду H4 (колонка htf_bias, features/htf.py); Shadow включает

    def __post_init__(self) -> None:
        if not isinstance(self.htf_filter, bool):
            raise ConfigError(f"htf_filter должен быть bool, получено {self.htf_filter!r}")
        if isinstance(self.setup_ttl_bars, bool) or not isinstance(self.setup_ttl_bars, int) or self.setup_ttl_bars < 1:
            raise ConfigError(f"setup_ttl_bars должен быть целым >= 1, получено {self.setup_ttl_bars!r}")
        for name in ("sl_buffer_atr", "tp_r", "risk_per_trade", "max_exposure"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
                raise ConfigError(f"{name} должен быть положительным числом, получено {value!r}")


_REQUIRED = (
    "open", "high", "low", "close", "atr", "swing_high", "swing_low", "sh1_price", "sh1_time", "sl1_price", "sl1_time",
    "fvg_bear", "fvg_bear_mid", "fvg_bear_top", "fvg_bear_bottom", "fvg_bear_t1", "fvg_bull", "fvg_bull_mid", "fvg_bull_top",
    "fvg_bull_bottom", "fvg_bull_t1",
)


def setup_columns(side: str) -> tuple[str, ...]:
    names = ("sweep", "sid", "sweep_idx", "pool_level", "pool_time", "sweep_extreme", "dead", "nofvg_break", "state",
             "choch", "signal", "reject", "entry", "sl", "tp", "risk_pct", "fvg_hi", "fvg_lo", "fvg_t1")
    return tuple(f"{side}_{n}" for n in names)


def all_setup_columns() -> tuple[str, ...]:
    return tuple(c for side in SIDES for c in setup_columns(side))


def _mirrored(side: str) -> dict[str, pl.Expr]:
    """Колонки в «шортовом» мире. sgn = +1 для шорта, -1 для лонга (цены умножаются на sgn)."""
    c = pl.col
    if side == "short":
        return {
            "o": c("open"), "h": c("high"), "l": c("low"), "c": c("close"),
            "pool_flag": c("swing_high"), "pool_price": c("sh1_price"), "pool_time": c("sh1_time"),
            "ref_price": c("sl1_price"), "ref_time": c("sl1_time"),
            "fvg": c("fvg_bear"), "fvg_mid": c("fvg_bear_mid"), "fvg_far": c("fvg_bear_top"), "fvg_near": c("fvg_bear_bottom"),
            "fvg_t1": c("fvg_bear_t1"),
        }
    return {
        "o": -c("open"), "h": -c("low"), "l": -c("high"), "c": -c("close"),
        "pool_flag": c("swing_low"), "pool_price": -c("sl1_price"), "pool_time": c("sl1_time"),
        "ref_price": -c("sh1_price"), "ref_time": c("sh1_time"),
        "fvg": c("fvg_bull"), "fvg_mid": -c("fvg_bull_mid"), "fvg_far": -c("fvg_bull_bottom"), "fvg_near": -c("fvg_bull_top"),
        "fvg_t1": c("fvg_bull_t1"),
    }


def _detect_side(df: pl.DataFrame, side: str, p: SmcSetupParams) -> pl.DataFrame:
    sgn = 1.0 if side == "short" else -1.0
    m = _mirrored(side)
    col = pl.col
    idx = pl.int_range(pl.len(), dtype=pl.Int64)
    w = df.select(
        idx.alias("idx"), col("atr"), m["o"].alias("o"), m["h"].alias("h"), m["l"].alias("l"), m["c"].alias("c"),
        m["pool_flag"].cast(pl.Int64).cum_sum().alias("grp"), m["pool_price"].shift(1).alias("L"),
        m["pool_time"].shift(1).alias("Lt"), m["ref_price"].alias("ref"), m["ref_time"].alias("ref_t"),
        m["fvg"].alias("fvg"), m["fvg_mid"].alias("fvg_mid"), m["fvg_far"].alias("fvg_far"), m["fvg_near"].alias("fvg_near"),
        m["fvg_t1"].alias("fvg_t1"),
        *([col(BIAS_COLUMN)] if p.htf_filter else []),
    )

    # S0/S1: пул не пробит (cum_max(high) по группе свинга до бара j-1 не выше уровня) и на баре j свип.
    w = w.with_columns(col("h").cum_max().over("grp").alias("grp_max"))
    taken_prev = (col("grp_max").shift(1) > col("L")).fill_null(True)
    sweep = (col("h") > col("L")) & (col("c") < col("L")) & ~taken_prev
    if p.htf_filter:  # HTF Bias: против структуры H4 сетап не начинается (S0 -> S1 запрещён); bias на баре j известен без будущего
        sweep = sweep & side_allowed(side)
    w = w.with_columns(sweep.fill_null(False).alias("sweep"))

    # Один сетап = один свип; «текущие» величины сетапа протягиваются forward_fill.
    w = w.with_columns(
        col("sweep").cast(pl.Int64).cum_sum().alias("sid"),
        pl.when(col("sweep")).then(col("idx")).forward_fill().alias("s_idx"),
        pl.when(col("sweep")).then(col("L")).forward_fill().alias("pool_level"),
        pl.when(col("sweep")).then(col("Lt")).forward_fill().alias("pool_time"),
    )
    w = w.with_columns(
        (col("idx") - col("s_idx")).alias("bars"),
        col("h").cum_max().over("sid").alias("extreme"),  # хай свипа: running max с бара свипа
    )
    invalid = ((col("c") > col("extreme").shift(1)) & (col("bars") >= 1)).fill_null(False)
    w = w.with_columns((invalid.cast(pl.Int64).cum_sum().over("sid") > 0).alias("dead"))
    alive = col("s_idx").is_not_null() & ~col("dead") & (col("bars") <= p.setup_ttl_bars)
    w = w.with_columns(alive.alias("alive"))

    # S2: последний FVG сетапа (первая свеча FVG, i-2, не раньше бара свипа).
    fvg_ev = col("fvg") & (col("idx") - 2 >= col("s_idx")) & col("alive")
    w = w.with_columns(
        pl.when(fvg_ev).then(col("fvg_mid")).forward_fill().over("sid").alias("fm"),
        pl.when(fvg_ev).then(col("fvg_far")).forward_fill().over("sid").alias("ffar"),
        pl.when(fvg_ev).then(col("fvg_near")).forward_fill().over("sid").alias("fnear"),
        pl.when(fvg_ev).then(col("fvg_t1")).forward_fill().over("sid").alias("ft1"),
    )
    w = w.with_columns(col("fm").is_not_null().alias("fvg_seen"))

    # S3: первое закрытие ниже последнего подтверждённого свинг-лоу, образованного после свипнутого хая.
    ref_ok = (col("ref_t") > col("pool_time")) if p.choch_ref_after_pool else col("ref_t").is_not_null()
    brk = (col("alive") & (col("c") < col("ref")) & ref_ok & (col("bars") >= 1)).fill_null(False)
    first_brk = brk & (brk.cast(pl.Int64).cum_sum().over("sid") == 1)
    w = w.with_columns(
        (first_brk & col("fvg_seen")).alias("choch"),
        (first_brk & ~col("fvg_seen")).alias("nofvg_break"),
    )

    # S4: заявка. Цены в зеркальном мире: шорт sell-limit выше рынка, SL выше входа.
    sl = col("extreme") + p.sl_buffer_atr * col("atr")
    risk = sl - col("fm")
    exposure = p.risk_per_trade * col("fm").abs() / risk
    passive = col("fm") > col("c")
    risk_ok = (risk > 0) & (exposure <= p.max_exposure)
    w = w.with_columns(
        sl.alias("sl_m"), risk.alias("risk_m"),
        (col("choch") & passive & risk_ok).fill_null(False).alias("signal"),
        pl.when(col("choch") & ~passive).then(pl.lit("not_passive"))
        .when(col("choch") & passive & ~(risk > 0)).then(pl.lit("bad_risk"))
        .when(col("choch") & passive & (risk > 0) & ~(exposure <= p.max_exposure)).then(pl.lit("exposure"))
        .alias("reject"),
    )
    # После CHoCH сетап отработан (заявка выставлена или сигнал отклонён): строго позже CHoCH состояние 0.
    consumed = (col("choch").cast(pl.Int64).cum_sum().over("sid") - col("choch").cast(pl.Int64)) > 0
    live = col("alive") & ~consumed
    state = (
        pl.when(col("signal")).then(4).when(col("choch")).then(3).when(live & col("fvg_seen")).then(2)
        .when(live).then(1).otherwise(0).cast(pl.Int8)
    )
    on_signal = col("signal")
    entry = col("fm")
    tp = entry - p.tp_r * col("risk_m")
    px = lambda e: e * sgn  # noqa: E731  (возврат из зеркального мира в реальные цены)
    out = w.select(
        col("sweep").alias(f"{side}_sweep"),
        col("sid").alias(f"{side}_sid"),
        col("s_idx").alias(f"{side}_sweep_idx"),
        px(col("pool_level")).alias(f"{side}_pool_level"),
        col("pool_time").alias(f"{side}_pool_time"),
        px(col("extreme")).alias(f"{side}_sweep_extreme"),
        (col("dead") & col("s_idx").is_not_null()).alias(f"{side}_dead"),
        col("nofvg_break").alias(f"{side}_nofvg_break"),
        state.alias(f"{side}_state"),
        col("choch").alias(f"{side}_choch"),
        on_signal.alias(f"{side}_signal"),
        col("reject").alias(f"{side}_reject"),
        pl.when(on_signal).then(px(entry)).alias(f"{side}_entry"),
        pl.when(on_signal).then(px(col("sl_m"))).alias(f"{side}_sl"),
        pl.when(on_signal).then(px(tp)).alias(f"{side}_tp"),
        pl.when(col("choch")).then(col("risk_m") / col("fm").abs()).alias(f"{side}_risk_pct"),
        pl.max_horizontal(px(col("ffar")), px(col("fnear"))).alias(f"{side}_fvg_hi"),
        pl.min_horizontal(px(col("ffar")), px(col("fnear"))).alias(f"{side}_fvg_lo"),
        col("ft1").alias(f"{side}_fvg_t1"),
    )
    return out


def detect_smc(df: pl.DataFrame, params: SmcSetupParams | None = None) -> pl.DataFrame:
    """Кадр с признаками -> тот же кадр + short_*/long_* колонки автомата (см. setup_columns)."""
    p = params or SmcSetupParams()
    required = _REQUIRED + ((BIAS_COLUMN,) if p.htf_filter else ())
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataError(f"Нет колонок признаков {missing}: сначала features.smc.add_features")
    clash = [c for c in all_setup_columns() if c in df.columns]
    if clash:
        raise DataError(f"Колонки автомата уже есть в кадре: {clash}")
    out = df
    for side in SIDES:  # две стороны, не строки
        out = out.hstack(_detect_side(df, side, p).get_columns())
    return out
