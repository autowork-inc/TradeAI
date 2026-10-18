"""Срезы выборки и честные оценки «что если» БЕЗ симулятора и переисполнения истории (решение пользователя).

Что считается честно по записанным в БД полям:
  * Срезы по значению признака: сторона, тип входа, сессия, `vol_ratio`/`range_atr`, любые числовые/булевы/короткие строковые поля `meta` снапшота
    (ATR-буфер, `require_buildup`, доля FVG - если стратегия записала их в meta). Это ОПИСАНИЕ того, что было, а не прогноз: срез с лучшим результатом -
    гипотеза, её надо проверить на новых данных (поэтому в рекомендациях требуется устойчивость в обеих половинах периода).
  * «Подтянуть стоп до s·R» по MAE: сделка, чья максимальная неблагоприятная экскурсия достигла s·R, была бы выбита стопом на -s·R. Точно при допущениях:
    тот же объём позиции (R - прежний), исполнение стопа ровно по цене (без проскальзывания), и только у сделок с переносом в безубыток после +1R
    (до безубытка частичных выходов не бывает, поэтому порядок событий известен). Расширять стоп оценить нельзя - исход после прежнего стопа неизвестен.
  * «Единственная цель T·R» по MFE: сделка, достигшая T·R (MFE), закрылась бы на T·R. Оценка СНИЗУ для T > уровня безубытка: сделки, закрытые по безубытку,
    могли бы дойти дальше, но их MFE оборван выходом.
Чего здесь НЕТ: изменения входа (50% -> 25% FVG), нового ATR-буфера у входа - сделок, которые стратегия не открыла бы, в БД нет. Это не оценивается.
"""

import math
from typing import Any

import numpy as np
import pandas as pd

from analyzer.metrics import compute_metrics

EXCLUDED_META_KEYS = {"entry", "sl", "tp", "inval", "interval"}  # цены сигнала и служебное: срез по ним бессмыслен
PRICE_LIKE = ("price", "entry", "sl_", "tp_", "inval", "high", "low", "mid", "time", "_ts", "_ms", "level", "open", "close", "vwap", "zone", "line", "slope")
STOP_GRID = (0.9, 0.8, 0.7, 0.6, 0.5)
TARGET_GRID = (1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0)
STRING_LEVELS_MAX = 8
STRING_LEN_MAX = 32


def brief(df: pd.DataFrame, cfg: Any, *, with_ci: bool = False) -> dict[str, Any]:
    """Короткий набор метрик для срезов."""
    m = compute_metrics(df, initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n, with_ci=with_ci)
    return {k: m.get(k) for k in ("n", "winrate", "profit_factor", "profit_factor_infinite", "expectancy_r", "expectancy_r_ci", "net_r", "max_drawdown_r", "sharpe_trade")}


def group_metrics(df: pd.DataFrame, keys: list[str], cfg: Any, *, with_ci: bool = True) -> list[dict[str, Any]]:
    """Полные метрики по группам (strategy_id, coin ...). Группы без сделок не создаются."""
    out = []
    for key, g in df.groupby(keys, sort=True, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        m = compute_metrics(g, initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n, with_ci=with_ci)
        out.append({**dict(zip(keys, key)), **m})
    return out


def _arr(df: pd.DataFrame, col: str) -> np.ndarray:
    return df[col].to_numpy(dtype=float)


def _what_if_row(df: pd.DataFrame, new_r: np.ndarray, changed: np.ndarray, cfg: Any) -> dict[str, Any]:
    alt = df.assign(r=new_r)
    base, after = brief(df, cfg), brief(alt, cfg)
    return {"n": len(df), "n_changed": int(changed.sum()), "winners_cut": int((changed & (df["r"].to_numpy() > 0)).sum()),
            "expectancy_r": after["expectancy_r"], "profit_factor": after["profit_factor"], "profit_factor_infinite": after["profit_factor_infinite"],
            "winrate": after["winrate"], "net_r": after["net_r"], "max_drawdown_r": after["max_drawdown_r"],
            "delta_expectancy_r": None if base["expectancy_r"] is None or after["expectancy_r"] is None else after["expectancy_r"] - base["expectancy_r"]}


def stop_tightening(df: pd.DataFrame, cfg: Any, grid: tuple[float, ...] = STOP_GRID) -> dict[str, Any]:
    """Оценка «стоп = s·R вместо 1R» по MAE (см. допущения в docstring модуля)."""
    if len(df) == 0:
        return {"baseline": brief(df, cfg), "rows": [], "eligible": 0}
    eligible = (df["mae_r"].notna() & df["plan"].map(lambda p: bool(p.get("be_after_first")))).to_numpy()
    mae = df["mae_r"].fillna(0.0).to_numpy(dtype=float)
    fee = df["fee_r"].fillna(0.0).to_numpy(dtype=float)
    r = _arr(df, "r")
    rows = []
    for s in grid:
        hit = eligible & (mae >= s)
        rows.append({"stop_r": s, **_what_if_row(df, np.where(hit, -s - fee, r), hit, cfg)})
    return {"baseline": brief(df, cfg), "rows": rows, "eligible": int(eligible.sum()),
            "assumptions": "Тот же объём позиции, стоп исполняется ровно по цене, только сделки с переносом в безубыток; расширить стоп оценить нельзя."}


def target_reach(df: pd.DataFrame, cfg: Any, grid: tuple[float, ...] = TARGET_GRID) -> dict[str, Any]:
    """Оценка «одна цель T·R на весь объём» по MFE (оценка снизу, см. docstring модуля)."""
    if len(df) == 0:
        return {"baseline": brief(df, cfg), "rows": []}
    has = df["mfe_r"].notna().to_numpy()
    mfe = df["mfe_r"].fillna(0.0).to_numpy(dtype=float)
    fee = df["fee_r"].fillna(0.0).to_numpy(dtype=float)
    r = _arr(df, "r")
    rows = []
    for t in grid:
        hit = has & (mfe >= t)
        rows.append({"target_r": t, "reach_rate": float(hit.sum() / max(has.sum(), 1)), **_what_if_row(df, np.where(hit, t - fee, r), hit, cfg)})
    return {"baseline": brief(df, cfg), "rows": rows, "bias": "pessimistic",
            "assumptions": "Цель на весь объём; сделки, закрытые по безубытку, могли бы дойти дальше, поэтому оценка занижена."}


def feature_columns(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Признаки для срезов, известные ДО исхода: сторона, тип входа, час UTC, vol_ratio, range_atr и скалярные поля meta снапшота."""
    cols: dict[str, pd.Series] = {}
    for c in ("vol_ratio", "range_atr"):
        if c in df and df[c].notna().sum():
            cols[c] = df[c].astype(float)
    cols["hour_utc"] = pd.Series(pd.DatetimeIndex(df["entry_ts"]).tz_convert("UTC").hour.astype(float), index=df.index)
    for c in ("side", "kind"):
        if c in df:
            cols[c] = df[c].astype(object)
    keys: dict[str, int] = {}
    for m in df["meta"]:
        for k in m:
            keys[k] = keys.get(k, 0) + 1
    for k in sorted(keys):
        low = k.lower()
        if k in EXCLUDED_META_KEYS or any(p in low for p in PRICE_LIKE):
            continue
        raw = df["meta"].map(lambda m, k=k: m.get(k))
        vals = raw.dropna()
        if vals.empty:
            continue
        if all(isinstance(v, bool) for v in vals):
            cols[f"meta.{k}"] = raw.map(lambda v: None if v is None or (isinstance(v, float) and math.isnan(v)) else str(bool(v)).lower()).astype(object)
        elif all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
            if vals.nunique() >= 2:
                cols[f"meta.{k}"] = pd.to_numeric(raw, errors="coerce")
        elif all(isinstance(v, str) and len(v) <= STRING_LEN_MAX for v in vals) and 2 <= vals.nunique() <= STRING_LEVELS_MAX:
            cols[f"meta.{k}"] = raw.astype(object)
    return cols


def feature_slices(df: pd.DataFrame, cfg: Any, *, n_bins: int = 4) -> list[dict[str, Any]]:
    """Метрики по значениям каждого признака: числовые - по квантильным корзинам, категориальные - по значениям. Корзины с n < min_group_trades помечаются, но показываются."""
    out = []
    for name, series in feature_columns(df).items():
        valid = series.notna()
        if valid.sum() < 2 * cfg.min_group_trades:
            continue
        sub, s = df[valid], series[valid]
        bins: list[dict[str, Any]] = []
        if pd.api.types.is_numeric_dtype(s):
            try:
                cat = pd.qcut(s, q=min(n_bins, max(2, int(valid.sum() // cfg.min_group_trades))), duplicates="drop")
            except ValueError:
                continue
            for iv, g in sub.groupby(cat, observed=True):
                bins.append({"label": f"{iv.left:.3g}..{iv.right:.3g}", "lo": float(iv.left), "hi": float(iv.right), **brief(g, cfg, with_ci=True)})
            kind = "numeric"
        else:
            for val, g in sub.groupby(s, observed=True):
                bins.append({"label": str(val), "value": str(val), **brief(g, cfg, with_ci=True)})
            kind = "categorical"
        if len(bins) >= 2:
            out.append({"feature": name, "kind": kind, "coverage": float(valid.mean()), "bins": bins})
    return out
