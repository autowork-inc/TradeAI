"""Выборка сделок -> DataFrame для расчётов. Здесь же - признаки, извлекаемые из снапшота входа (VSA): всё, что можно узнать о сделке
ДО её исхода (объём сигнальной свечи против среднего за 20 баров, размах свечи в ATR), и «отпечаток» плана выхода (версия по умолчанию)."""

import hashlib
import json
from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd

from analyzer.errors import DataError

VOLUME_BASE_BARS = 20  # среднее объёма за 20 баров ДО сигнальной свечи (ТЗ, модуль 3)
# Ключи плана выхода, которые описывают КОНФИГУРАЦИЮ стратегии (остальное в `plan` - факты исполнения: fills, exit_price_source, fees_estimated)
PLAN_CONFIG_KEYS = ("levels", "be_after_first", "runner_frac", "trail_mode", "trail_atr_mult", "trail_wick_bars", "tp_anchor")
REQUIRED = ("id", "strategy_id", "coin", "side", "signal_time", "exit_time", "r_multiple")
TIME_COLUMNS = ("signal_time", "entry_time", "exit_time")
NUMERIC_COLUMNS = ("r_multiple", "gross_r", "pnl", "fees", "mfe_r", "mae_r", "planned_entry", "entry_price", "exit_price", "sl", "risk_amount", "qty")


def candle_features(candles: Any) -> tuple[float | None, float | None]:
    """(vol_ratio, range_atr) сигнальной (последней) свечи снапшота. vol_ratio = объём / среднее объёма 20 предыдущих баров; range_atr = (high-low)/atr.
    Недостаточно баров / нулевой знаменатель / нет поля - None (признак не выдумывается)."""
    if not isinstance(candles, list) or not candles:
        return None, None
    last = candles[-1]
    vol_ratio = range_atr = None
    if len(candles) > VOLUME_BASE_BARS:
        try:
            base = float(np.mean([float(c["volume"]) for c in candles[-VOLUME_BASE_BARS - 1:-1]]))
            vol_ratio = float(last["volume"]) / base if base > 0 else None
        except (KeyError, TypeError, ValueError):
            vol_ratio = None
    try:
        atr = float(last.get("atr") or 0.0)
        range_atr = (float(last["high"]) - float(last["low"])) / atr if atr > 0 else None
    except (KeyError, TypeError, ValueError):
        range_atr = None
    return vol_ratio, range_atr


def plan_hash(plan: Any) -> str:
    """Короткий отпечаток конфигурации плана выхода: две сделки с разной схемой выхода получают разные отпечатки. Нет плана - 'noplan'."""
    if not isinstance(plan, dict):
        return "noplan"
    cfg = {k: plan.get(k) for k in PLAN_CONFIG_KEYS if k in plan}
    if not cfg:
        return "noplan"
    return hashlib.sha1(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest()[:8]


def build_frame(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Строки загрузчика -> DataFrame с нормализованными типами. Колонки: все поля trades + `r` (чистый R), `entry_ts` (вход, иначе сигнал),
    `fee_r` (gross_r - r), `meta`/`plan` (dict), `plan_hash`, `vol_ratio`, `range_atr`."""
    rows = list(rows)
    if not rows:
        return pd.DataFrame({"id": pd.Series(dtype=str), "r": pd.Series(dtype=float), "exit_time": pd.Series(dtype="datetime64[ns, UTC]")})
    df = pd.DataFrame(rows)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise DataError(f"В выборке нет колонок: {', '.join(missing)}")
    for c in TIME_COLUMNS:
        df[c] = pd.to_datetime(df[c], utc=True) if c in df else pd.NaT
    for c in NUMERIC_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
    if df["r_multiple"].isna().any():
        raise DataError("У закрытых сделок есть пустой r_multiple: выборка повреждена (закрытая сделка без итога)")
    if df["exit_time"].isna().any():
        raise DataError("У закрытых сделок есть пустой exit_time")
    df["r"] = df["r_multiple"].astype(float)
    df["entry_ts"] = df["entry_time"].fillna(df["signal_time"])
    df["fee_r"] = (df["gross_r"] - df["r"]).clip(lower=0.0)
    df["plan"] = df["plan"].map(lambda p: p if isinstance(p, dict) else {}) if "plan" in df else [{}] * len(df)
    df["meta"] = df["meta"].map(lambda m: m if isinstance(m, dict) else {}) if "meta" in df else [{}] * len(df)
    for c in ("vol_ratio", "range_atr"):
        df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else np.nan
    df["plan_hash"] = df["plan"].map(plan_hash)
    return df.sort_values(["exit_time", "id"], kind="stable").reset_index(drop=True)
