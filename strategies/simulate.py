"""Пессимистичный бэктест исходов сделок по сигналам детекторов. Векторный Polars: сигналы x смещения баров.

Это оценка ИСХОДА (смотрит вперёд от сигнала - так и должен работать бэктест), а не признак: R4 к нему не
применяется. Жизненный цикл заявки - обработчик событий (D1): блокировку «владелец монеты» (D4) делает
`apply_ownership_lock` обычным циклом по маленькой таблице сделок (сотни строк), а не по свечам.

Допущения (всегда худший сценарий исполнения, О3):
  * LIMIT исполняется только при ПРОХОДЕ цены через уровень: для шорта high > entry строго (касание не считается);
    на баре исполнения благоприятные движения не засчитываются, неблагоприятные (high >= SL) - засчитываются;
  * MARKET исполняется по open следующего бара; тот же принцип на баре входа;
  * если в одном баре достижимы и SL, и безубыток, и TP - приоритет SL > BE > TP;
  * безубыток: +1R достигнут на баре b (low <= entry - 1R) -> стоп на входе действует с бара b (худший случай:
    возврат к входу в том же баре выбивает);
  * заявка LIMIT отменяется, если до исполнения close закрылся выше хая свипа, цена дошла до TP или прошло
    limit_ttl баров; сделка без выхода за max_hold баров закрывается по close;
  * комиссии: вход LIMIT - maker, вход MARKET и все выходы - taker (стоп/тейк - условные MARKET-ордера);
    проскальзывание стопа НЕ моделируется (❓ оптимистичная сторона).
Все расчёты в «шортовом» мире (цены * sgn, sgn = +1 шорт, -1 лонг), как в smc_setups.
"""

from dataclasses import dataclass

import polars as pl

from features import defaults as D
from utils.errors import ConfigError, DataError

INF = 10**9
SIGNAL_COLUMNS = ("sig_id", "idx", "sgn", "kind", "entry", "sl", "tp", "inval")


@dataclass(frozen=True)
class SimParams:
    limit_ttl: int = D.LIMIT_TTL_BARS  # D8
    max_hold: int = 192  # 2 суток на 15m; не из утверждённых параметров, защита от вечных сделок
    tp_r: float = D.TP_R
    be_trigger_r: float = D.BE_TRIGGER_R
    maker_fee: float = 0.0002  # config.LIVE_MAKER_FEE_RATE
    taker_fee: float = 0.0005  # config.LIVE_TAKER_FEE_RATE
    risk_per_trade: float = D.RISK_PER_TRADE
    max_exposure: float = D.MAX_EXPOSURE

    def __post_init__(self) -> None:
        if self.limit_ttl < 1 or self.max_hold < 1:
            raise ConfigError("limit_ttl и max_hold должны быть >= 1")
        if not (self.tp_r > 0 and self.be_trigger_r > 0 and self.max_exposure > 0 and self.risk_per_trade > 0):
            raise ConfigError("tp_r, be_trigger_r, max_exposure, risk_per_trade должны быть > 0")


def _first(cond: pl.Expr) -> pl.Expr:
    """Минимальное смещение, на котором выполнено условие (null, если нигде)."""
    return pl.col("off").filter(cond).min()


def simulate(bars: pl.DataFrame, signals: pl.DataFrame, p: SimParams | None = None) -> pl.DataFrame:
    """bars: непрерывные свечи ОДНОЙ монеты (open_time, open, high, low, close), индекс строки = idx сигнала.
    signals: SIGNAL_COLUMNS; kind 'limit' | 'market'; для market entry/tp/inval игнорируются (entry = open следующего
    бара, tp = entry -+ tp_r*R). Возвращает по строке на сигнал с исходом и R."""
    p = p or SimParams()
    missing = [c for c in SIGNAL_COLUMNS if c not in signals.columns]
    if missing:
        raise DataError(f"В сигналах нет колонок {missing}")
    if signals.height == 0:
        return _empty_result()
    if signals["sig_id"].n_unique() != signals.height:
        raise DataError("sig_id должен быть уникальным")
    parts = []
    for sgn in (1.0, -1.0):
        sub = signals.filter(pl.col("sgn") == sgn)
        if sub.height:
            parts.append(_simulate_side(bars, sub, sgn, p))
    return pl.concat(parts).sort("sig_id")


def _empty_result() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "sig_id": pl.Int64, "outcome": pl.String, "entry_px": pl.Float64, "exit_px": pl.Float64, "fill_off": pl.Int64,
        "exit_off": pl.Int64, "risk": pl.Float64, "gross_r": pl.Float64, "net_r": pl.Float64,
    })


def _simulate_side(bars: pl.DataFrame, sig: pl.DataFrame, sgn: float, p: SimParams) -> pl.DataFrame:
    col = pl.col
    # Бары в «шортовом» мире: Ho - high, Lo - low, Co - close, Oo - open (для лонга mirror: high' = -low).
    o = bars.select(
        pl.int_range(pl.len(), dtype=pl.Int64).alias("bar_idx"),
        (col("open") * sgn).alias("Oo"), (col("close") * sgn).alias("Co"),
        (col("high") if sgn > 0 else -col("low")).alias("Ho"),
        (col("low") if sgn > 0 else -col("high")).alias("Lo"),
    )
    s = sig.select(
        "sig_id", "idx", "kind", (col("sl") * sgn).alias("sl_o"),
        (col("entry") * sgn).alias("lim_o"), (col("tp") * sgn).alias("lim_tp_o"), (col("inval") * sgn).alias("inval_o"),
    )
    # Вход MARKET - open следующего бара; R и TP считаем по фактическому входу.
    nxt = o.select((col("bar_idx") - 1).alias("idx"), col("Oo").alias("next_open"))
    s = s.join(nxt, on="idx", how="left")
    market = col("kind") == "market"
    s = s.with_columns(
        pl.when(market).then(col("next_open")).otherwise(col("lim_o")).alias("e"),
    ).with_columns(
        (col("sl_o") - col("e")).alias("risk"),
    ).with_columns(
        pl.when(market).then(col("e") - p.tp_r * col("risk")).otherwise(col("lim_tp_o")).alias("tp_o"),
    )
    # Отсев MARKET без данных/с нулевым риском/с превышением плеча (О8); LIMIT уже прошёл эти проверки в детекторе.
    exposure = p.risk_per_trade * col("e").abs() / col("risk")
    bad = market & (col("e").is_null() | ~(col("risk") > 0) | ~(exposure <= p.max_exposure))
    rejected = s.filter(bad).with_columns(
        pl.when(col("e").is_null()).then(pl.lit("no_data")).when(~(col("risk") > 0)).then(pl.lit("bad_risk"))
        .otherwise(pl.lit("rejected_exposure")).alias("outcome")
    )
    s = s.filter(~bad)

    horizon = p.limit_ttl + p.max_hold + 1
    x = s.join(pl.DataFrame({"off": pl.int_range(1, horizon + 1, eager=True)}), how="cross").with_columns(
        (col("idx") + col("off")).alias("bar_idx")
    ).join(o, on="bar_idx", how="left")

    limit = col("kind") == "limit"
    # A. Исполнение. LIMIT: первый проход через уровень в пределах limit_ttl; отмена - close выше хая свипа или TP
    # раньше исполнения (то же смещение = исполнено).
    a = x.group_by("sig_id").agg(
        pl.when(limit.first()).then(_first((col("Ho") > col("e")) & (col("off") <= p.limit_ttl)))
        .otherwise(pl.lit(1, dtype=pl.Int64)).alias("f0"),
        _first(col("Co") > col("inval_o")).alias("cancel_close"),
        _first(col("Lo") <= col("tp_o")).alias("cancel_tp"),
        col("Ho").is_not_null().sum().alias("avail"),
    )
    s = s.join(a, on="sig_id", how="left").with_columns(
        pl.when(market)
        .then(pl.when(col("avail") > 0).then(1))
        .when(
            col("f0").is_not_null()
            & (col("f0") <= col("cancel_close").fill_null(INF)) & (col("f0") <= col("cancel_tp").fill_null(INF))
        ).then(col("f0").cast(pl.Int64))
        .alias("fill")
    ).drop("f0", "cancel_close", "cancel_tp", "avail")
    x = x.join(s.select("sig_id", "fill"), on="sig_id", how="left")

    # B. События после исполнения.
    arm_level = col("e") - p.be_trigger_r * col("risk")
    b = x.filter(col("fill").is_not_null()).group_by("sig_id").agg(
        _first((col("off") >= col("fill")) & (col("Ho") >= col("sl_o"))).alias("t_sl"),
        _first((col("off") > col("fill")) & (col("Lo") <= arm_level)).alias("t_arm"),
        _first((col("off") > col("fill")) & (col("Lo") <= col("tp_o"))).alias("t_tp"),
    )
    s = s.join(b, on="sig_id", how="left")
    x = x.join(s.select("sig_id", "t_arm"), on="sig_id", how="left")
    c = x.filter(col("t_arm").is_not_null()).group_by("sig_id").agg(
        _first((col("off") >= col("t_arm")) & (col("Ho") >= col("e"))).alias("t_be"),
    )
    s = s.join(c, on="sig_id", how="left")
    timeout_off = col("fill") + p.max_hold
    close_at_timeout = x.filter(col("off") == (col("fill") + p.max_hold)).select("sig_id", col("Co").alias("c_timeout"))
    s = s.join(close_at_timeout, on="sig_id", how="left")

    sl_t, be_t, tp_t = (col(n).cast(pl.Int64).fill_null(INF) for n in ("t_sl", "t_be", "t_tp"))
    first_ev = pl.min_horizontal(sl_t, be_t, tp_t)
    outcome = (
        pl.when(col("fill").is_null()).then(pl.lit("unfilled"))
        .when(first_ev <= timeout_off).then(
            pl.when(sl_t == first_ev).then(pl.lit("sl")).when(be_t == first_ev).then(pl.lit("be")).otherwise(pl.lit("tp"))
        )
        .when(col("c_timeout").is_not_null()).then(pl.lit("timeout"))
        .otherwise(pl.lit("unresolved"))
    )
    s = s.with_columns(outcome.alias("outcome"), first_ev.alias("first_ev"))
    exit_o = (
        pl.when(col("outcome") == "sl").then(col("sl_o")).when(col("outcome") == "be").then(col("e"))
        .when(col("outcome") == "tp").then(col("tp_o")).when(col("outcome") == "timeout").then(col("c_timeout"))
    )
    exit_off = (
        pl.when(col("outcome").is_in(["sl", "be", "tp"])).then(col("first_ev")).when(col("outcome") == "timeout")
        .then(timeout_off)
    )
    filled = col("outcome").is_in(["sl", "be", "tp", "timeout"])
    gross = (col("e") - exit_o) / col("risk")
    fee_in = pl.when(market).then(p.taker_fee).otherwise(p.maker_fee) * col("e").abs()
    fee_out = p.taker_fee * exit_o.abs()
    s = s.with_columns(
        (col("e") * sgn).alias("entry_px"), (exit_o * sgn).alias("exit_px"), exit_off.cast(pl.Int64).alias("exit_off"),
        pl.when(filled).then(gross).alias("gross_r"),
        pl.when(filled).then(gross - (fee_in + fee_out) / col("risk")).alias("net_r"),
    )
    res = s.select(
        "sig_id", "outcome", "entry_px", "exit_px", col("fill").alias("fill_off"), "exit_off", col("risk"), "gross_r", "net_r",
    )
    rej = rejected.select(
        "sig_id", "outcome", pl.lit(None, dtype=pl.Float64).alias("entry_px"), pl.lit(None, dtype=pl.Float64).alias("exit_px"),
        pl.lit(None, dtype=pl.Int64).alias("fill_off"), pl.lit(None, dtype=pl.Int64).alias("exit_off"),
        pl.lit(None, dtype=pl.Float64).alias("risk"), pl.lit(None, dtype=pl.Float64).alias("gross_r"),
        pl.lit(None, dtype=pl.Float64).alias("net_r"),
    )
    return pl.concat([res.cast(rej.schema), rej])


def apply_ownership_lock(trades: pl.DataFrame, *, idx_col: str = "idx", end_col: str = "end_idx") -> pl.DataFrame:
    """D4: пока по монете есть заявка/позиция, новые сигналы пропускаются (skipped_due_to_lock). trades отсортирован по
    idx; end_idx - последний бар, на котором заявка/позиция живы. Цикл - по таблице сделок (события), не по свечам."""
    if trades.height == 0:
        return trades.with_columns(pl.lit(False).alias("skipped_due_to_lock"))
    ordered = trades.sort(idx_col)
    busy_until = -1
    flags = []
    for start, end in zip(ordered[idx_col].to_list(), ordered[end_col].to_list()):
        if start <= busy_until:
            flags.append(True)
        else:
            flags.append(False)
            busy_until = end
    return ordered.with_columns(pl.Series("skipped_due_to_lock", flags))
