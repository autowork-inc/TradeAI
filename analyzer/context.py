"""Профилирование контекста (модуль 3): торговые сессии и VSA-признаки сигнальной свечи.

Сессии (UTC, без перехода на летнее время - как в ICT_Killzones бота), по времени ВХОДА (иначе сигнала):
  Asia 00:00-07:00, London 07:00-12:00, New York 12:00-21:00, Off-hours 21:00-24:00.
VSA: `vol_ratio` = объём сигнальной свечи / среднее объёма 20 предыдущих баров; `range_atr` = размах свечи / ATR. Берутся из снапшота входа.
Профиль успешных: победители (R > 0) против остальных по каждому признаку - медианы, размер эффекта (d Коэна), перестановочная p-оценка.
"""

from typing import Any

import numpy as np
import pandas as pd

from analyzer.metrics import clean, permutation_p
from analyzer.slices import brief, feature_columns

SESSIONS = (("Asia", 0, 7), ("London", 7, 12), ("New York", 12, 21))
OFF_HOURS = "Off-hours"
VOL_BINS = (0.0, 0.8, 1.2, 1.5, 2.0, 3.0, float("inf"))


def session_of(ts: pd.Series) -> pd.Series:
    hours = pd.DatetimeIndex(ts).tz_convert("UTC").hour
    out = np.full(len(hours), OFF_HOURS, dtype=object)
    for name, lo, hi in SESSIONS:
        out[(hours >= lo) & (hours < hi)] = name
    return pd.Series(out, index=ts.index)


def sessions_report(df: pd.DataFrame, cfg: Any) -> dict[str, Any]:
    """Метрики по сессиям и лучшая сессия по матожиданию (среди сессий, где достаточно сделок)."""
    if len(df) == 0:
        return {"rows": [], "best": None}
    sess = session_of(df["entry_ts"])
    rows = []
    for name in [s[0] for s in SESSIONS] + [OFF_HOURS]:
        g = df[(sess == name).to_numpy()]
        if len(g):
            rows.append({"session": name, "hours_utc": next((f"{lo:02d}:00-{hi:02d}:00" for n, lo, hi in SESSIONS if n == name), "21:00-24:00"),
                         **brief(g, cfg, with_ci=True), "enough": len(g) >= cfg.min_group_trades})
    ranked = [r for r in rows if r["enough"] and r["expectancy_r"] is not None]
    best = max(ranked, key=lambda r: r["expectancy_r"])["session"] if ranked else None
    return clean({"rows": rows, "best": best})


def vsa_bins(df: pd.DataFrame, cfg: Any) -> list[dict[str, Any]]:
    v = df["vol_ratio"] if "vol_ratio" in df else pd.Series(dtype=float)
    valid = v.notna()
    if valid.sum() == 0:
        return []
    cat = pd.cut(v[valid], bins=list(VOL_BINS), right=False)
    rows = []
    for iv, g in df[valid].groupby(cat, observed=True):
        hi = "inf" if np.isinf(iv.right) else f"{iv.right:g}"
        rows.append({"label": f"{iv.left:g}..{hi}", **brief(g, cfg, with_ci=False)})
    return clean(rows)


def cohens_d(a: np.ndarray, b: np.ndarray) -> float | None:
    if len(a) < 2 or len(b) < 2:
        return None
    pooled = np.sqrt(((len(a) - 1) * a.var(ddof=1) + (len(b) - 1) * b.var(ddof=1)) / (len(a) + len(b) - 2))
    return float((a.mean() - b.mean()) / pooled) if pooled > 0 else None


def success_profile(df: pd.DataFrame, cfg: Any) -> dict[str, Any]:
    """Общие характеристики победителей (R > 0) против остальных. Только признаки, известные до исхода (feature_columns)."""
    if len(df) == 0:
        return {"winners": 0, "others": 0, "features": []}
    win = (df["r"] > 0).to_numpy()
    rows = []
    for name, s in feature_columns(df).items():
        if not pd.api.types.is_numeric_dtype(s):
            continue
        x = s.to_numpy(dtype=float)
        a, b = x[win & ~np.isnan(x)], x[~win & ~np.isnan(x)]
        if len(a) < 5 or len(b) < 5:
            continue
        rows.append({"feature": name, "winners_n": len(a), "others_n": len(b), "winners_median": float(np.median(a)), "others_median": float(np.median(b)),
                     "winners_mean": float(a.mean()), "others_mean": float(b.mean()), "effect_d": cohens_d(a, b), "p_value": permutation_p(a, b)})
    rows.sort(key=lambda r: (r["p_value"] is None, r["p_value"] if r["p_value"] is not None else 1.0))
    return clean({"winners": int(win.sum()), "others": int((~win).sum()), "features": rows,
                  "note": "p-оценки без поправки на множественные сравнения: признак с малой p - повод проверить гипотезу на новых данных, а не доказательство."})
