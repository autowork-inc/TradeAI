"""Сборка прогона: свечи -> признаки -> детекторы -> сигналы -> пессимистичная симуляция -> блокировка монеты -> статистика.

Это обвязка бэктеста (Этап 2). Паттерны считают features/ и strategies/{smc_setups,breakout}.py (векторно);
здесь только склейка таблиц и подсчёт итогов.
"""

from dataclasses import dataclass

import polars as pl

from features.smc import SmcParams, add_features
from pipeline.loaders.klines_loader import KlinesLoader
from strategies.breakout import BreakoutParams, detect_breakout
from strategies.simulate import SimParams, apply_ownership_lock, simulate
from strategies.smc_setups import SIDES, SmcSetupParams, detect_smc

CANDLE_COLUMNS = ("open_time", "open", "high", "low", "close", "volume")


def load_segments(symbol: str, interval: str, start_ms: int, end_ms: int, interval_ms: int) -> list[pl.DataFrame]:
    """Непрерывные куски истории (пропуск свечи начинает новый кусок): add_features требует сетку без дыр."""
    raw = KlinesLoader().scan(symbol, interval, start_ms, end_ms).collect()
    seg = raw.with_columns(pl.col("gap_before").cast(pl.Int64).cum_sum().alias("_seg"))
    return [part.select(CANDLE_COLUMNS) for _, part in seg.partition_by("_seg", as_dict=True, maintain_order=True).items()]


def smc_signals(det: pl.DataFrame) -> pl.DataFrame:
    parts = []
    for side, sgn in (("short", 1.0), ("long", -1.0)):
        parts.append(
            det.with_row_index("idx").filter(pl.col(f"{side}_signal")).select(
                pl.col("idx").cast(pl.Int64), pl.lit(sgn).alias("sgn"), pl.lit("limit").alias("kind"),
                pl.lit("smc_" + side).alias("strategy"), pl.col("open_time"),
                pl.col(f"{side}_entry").alias("entry"), pl.col(f"{side}_sl").alias("sl"), pl.col(f"{side}_tp").alias("tp"),
                pl.col(f"{side}_sweep_extreme").alias("inval"), pl.col(f"{side}_sweep_idx").cast(pl.Int64).alias("sweep_idx"),
            )
        )
    return pl.concat(parts).sort("idx")


def breakout_signals(det: pl.DataFrame) -> pl.DataFrame:
    return det.with_row_index("idx").filter(pl.col("breakout_signal")).select(
        pl.col("idx").cast(pl.Int64), pl.lit(-1.0).alias("sgn"), pl.lit("market").alias("kind"),
        pl.lit("breakout").alias("strategy"), pl.col("open_time"), pl.col("close").alias("entry"),
        pl.col("breakout_sl").alias("sl"), pl.lit(None, dtype=pl.Float64).alias("tp"),
        pl.lit(None, dtype=pl.Float64).alias("inval"), pl.lit(None, dtype=pl.Int64).alias("sweep_idx"),
    )


@dataclass(frozen=True)
class RunConfig:
    smc_features: SmcParams = SmcParams()
    smc: SmcSetupParams = SmcSetupParams()
    breakout: BreakoutParams = BreakoutParams()
    sim: SimParams = SimParams()
    ownership_lock: bool = True


def run_symbol(symbol: str, segments: list[pl.DataFrame], interval_ms: int, cfg: RunConfig = RunConfig()):
    """Возвращает (кадры детекции по сегментам, таблица сделок по обеим стратегиям). Цены/R - после симуляции."""
    detections, trades = [], []
    for n, seg in enumerate(segments):
        feats = add_features(seg, cfg.smc_features, interval_ms=interval_ms)
        det = detect_breakout(detect_smc(feats, cfg.smc), cfg.breakout).with_columns(pl.lit(symbol).alias("symbol"))
        detections.append(det)
        sigs = pl.concat([smc_signals(det), breakout_signals(det)]).with_row_index("sig_id").with_columns(
            pl.col("sig_id").cast(pl.Int64)
        )
        if sigs.height == 0:
            continue
        res = simulate(seg, sigs, cfg.sim)
        t = sigs.join(res, on="sig_id").with_columns(pl.lit(symbol).alias("symbol"), pl.lit(n).alias("segment"))
        trades.append(t)
    if not trades:
        return detections, pl.DataFrame()
    t = pl.concat(trades)
    # Конец жизни заявки/позиции для блокировки монеты: исполнена - бар выхода; не исполнена - срок заявки (консервативно).
    end = (
        pl.when(pl.col("exit_off").is_not_null()).then(pl.col("idx") + pl.col("exit_off"))
        .when(pl.col("outcome") == "unfilled").then(pl.col("idx") + cfg.sim.limit_ttl)
        .otherwise(pl.col("idx"))
    )
    t = t.with_columns(end.alias("end_idx"))
    if cfg.ownership_lock:
        parts = [apply_ownership_lock(g.sort("idx")) for g in t.partition_by(["segment"], maintain_order=True)]
        t = pl.concat(parts)
    else:
        t = t.with_columns(pl.lit(False).alias("skipped_due_to_lock"))
    return detections, t


def summarize(trades: pl.DataFrame) -> dict:
    """Итоги по ИСПОЛНЕННЫМ сделкам (без пропущенных блокировкой). R - net (с комиссиями) и gross."""
    done = trades.filter(~pl.col("skipped_due_to_lock") & pl.col("outcome").is_in(["sl", "be", "tp", "timeout"])).sort("idx")
    n = done.height
    out = {"signals": trades.height, "locked": int(trades["skipped_due_to_lock"].sum()) if trades.height else 0,
           "unfilled": trades.filter(~pl.col("skipped_due_to_lock") & (pl.col("outcome") == "unfilled")).height,
           "rejected": trades.filter(~pl.col("skipped_due_to_lock") & pl.col("outcome").str.starts_with("rejected")).height,
           "trades": n}
    if n == 0:
        return out
    net, gross = done["net_r"], done["gross_r"]
    cum = net.cum_sum()
    wins, losses = net.filter(net > 0).sum(), -net.filter(net < 0).sum()
    out |= {
        "tp": int((done["outcome"] == "tp").sum()), "sl": int((done["outcome"] == "sl").sum()),
        "be": int((done["outcome"] == "be").sum()), "timeout": int((done["outcome"] == "timeout").sum()),
        "win_rate_net": float((net > 0).mean()), "sum_gross_r": float(gross.sum()), "sum_net_r": float(net.sum()),
        "expectancy_net_r": float(net.mean()), "profit_factor": float(wins / losses) if losses > 0 else float("inf"),
        "max_dd_r": float((cum.cum_max() - cum).max()), "avg_hold_bars": float((done["exit_off"] - done["fill_off"]).mean()),
        "avg_risk_pct": float((done["risk"] / done["entry_px"].abs()).mean()),
    }
    return out
