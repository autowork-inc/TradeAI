"""Математическое ядро: метрики по выборке закрытых сделок. Чистые функции над pandas/numpy, без БД и без сети (тестируются на синтетике).

Единицы: основная - R-мультипл ЧИСТЫЙ (`r`, после комиссий: `trades.r_multiple`); доллары - `pnl` (виртуальный эквити Shadow, риск 1%).
Никакой «вероятности после серии убытков»: сделки считаются независимыми наблюдениями, неопределённость выражается доверительными
интервалами (bootstrap) и порогом «достаточно данных».

Определения:
  Profit Factor        = сумма прибыльных R / модуль суммы убыточных R (без убыточных - не определён: `profit_factor=None`, `profit_factor_infinite`);
  Матожидание          = среднее R на сделку (и среднее pnl в USD);
  Sharpe (на сделку)   = mean(R) / std(R, ddof=1) - без годового масштаба;
  Sharpe (дневной)     = mean/std дневных сумм R (дни без сделок = 0, от первой до последней сделки) * sqrt(365): рынок круглосуточный;
  Sortino              = mean / нижнее отклонение sqrt(mean(min(x, 0)^2)) - цель 0, та же схема (на сделку и дневной);
  Max Drawdown         = наибольшее падение накопленной кривой от пика: в R, в USD и в % от эквити (стартовый эквити - из настроек).
"""

import math
from typing import Any

import numpy as np
import pandas as pd

ANNUAL_DAYS = 365
BOOT_CELLS = 4_000_000  # потолок ячеек матрицы ресэмплов за раз (память воркера)
MIN_CI_TRADES = 8


def clean(obj: Any) -> Any:
    """Значение -> JSON-совместимое: numpy -> python, NaN/inf -> None, Timestamp -> ISO UTC, рекурсивно по dict/list/tuple."""
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [clean(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [clean(v) for v in obj.tolist()]
    if isinstance(obj, pd.Timestamp):
        return None if pd.isna(obj) else obj.tz_convert("UTC").isoformat() if obj.tzinfo else obj.isoformat()
    if obj is pd.NaT:
        return None
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, np.bool_):
        return bool(obj)
    return str(obj)


def epoch_ms(ts: Any) -> np.ndarray:
    """Моменты времени (tz-aware) -> epoch мс UTC. as_unit('ms') обязателен: у pandas 2.x единица хранения (s/ms/us/ns) зависит от источника, и asi8 без неё врёт."""
    return pd.DatetimeIndex(ts).tz_convert("UTC").as_unit("ms").asi8


def _num(x: float | None) -> float | None:
    if x is None:
        return None
    f = float(x)
    return f if math.isfinite(f) else None


def bootstrap_means(x: np.ndarray, n_boot: int, seed: int = 7) -> np.ndarray:
    """Средние n_boot ресэмплов x (с возвращением). Порциями, чтобы матрица индексов не раздувала память."""
    n = len(x)
    rng = np.random.default_rng(seed)
    out = np.empty(n_boot)
    step = max(1, BOOT_CELLS // max(n, 1))
    for s in range(0, n_boot, step):
        k = min(step, n_boot - s)
        out[s:s + k] = x[rng.integers(0, n, (k, n))].mean(axis=1)
    return out


def bootstrap_mean_ci(x: np.ndarray | list[float], n_boot: int = 1000, alpha: float = 0.05, seed: int = 7) -> list[float] | None:
    arr = np.asarray(x, dtype=float)
    if len(arr) < MIN_CI_TRADES:
        return None
    lo, hi = np.quantile(bootstrap_means(arr, n_boot, seed), [alpha / 2, 1 - alpha / 2])
    return [float(lo), float(hi)]


def bootstrap_diff(a: np.ndarray | list[float], b: np.ndarray | list[float], n_boot: int = 1000, alpha: float = 0.05, seed: int = 11) -> dict[str, Any] | None:
    """Разность средних mean(b) - mean(a): точечная оценка, интервал и доля ресэмплов с разностью <= 0 (односторонняя «p» без поправок)."""
    xa, xb = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    if len(xa) < MIN_CI_TRADES or len(xb) < MIN_CI_TRADES:
        return None
    d = bootstrap_means(xb, n_boot, seed) - bootstrap_means(xa, n_boot, seed + 1)
    lo, hi = np.quantile(d, [alpha / 2, 1 - alpha / 2])
    return {"diff": float(xb.mean() - xa.mean()), "ci": [float(lo), float(hi)], "p_not_better": float((d <= 0).mean()), "p_not_worse": float((d >= 0).mean())}


def profit_factor(r: np.ndarray) -> tuple[float | None, bool]:
    gp = float(r[r > 0].sum())
    gl = float(-r[r < 0].sum())
    if gl <= 0:
        return None, gp > 0
    return gp / gl, False


def max_drawdown(curve_with_start: np.ndarray) -> float:
    """Наибольшее падение от пика. Кривая уже включает стартовое значение."""
    if len(curve_with_start) == 0:
        return 0.0
    return float((np.maximum.accumulate(curve_with_start) - curve_with_start).max())


def _ratio(mean: float, dev: float) -> float | None:
    return mean / dev if dev > 0 and math.isfinite(dev) else None


def _risk_ratios(x: np.ndarray) -> tuple[float | None, float | None]:
    """(Sharpe, Sortino) без годового масштаба."""
    if len(x) < 2:
        return None, None
    mean = float(x.mean())
    sharpe = _ratio(mean, float(x.std(ddof=1)))
    sortino = _ratio(mean, math.sqrt(float(np.mean(np.minimum(x, 0.0) ** 2))))
    return sharpe, sortino


def daily_returns(r: np.ndarray, exit_time: pd.Series) -> np.ndarray:
    """Дневные суммы R по дню закрытия (UTC), включая пустые дни между первой и последней сделкой."""
    if len(r) == 0:
        return np.array([])
    days = pd.Series(r, index=pd.DatetimeIndex(exit_time).tz_convert("UTC").floor("D")).groupby(level=0).sum()
    full = pd.date_range(days.index.min(), days.index.max(), freq="D", tz="UTC")
    return days.reindex(full, fill_value=0.0).to_numpy(dtype=float)


def compute_metrics(df: pd.DataFrame, *, initial_equity: float = 10_000.0, min_conclusions: int = 200, bootstrap_n: int = 1000, with_ci: bool = True) -> dict[str, Any]:
    """Метрики выборки. df: `r` (чистый R), `exit_time` (tz-aware), опционально `pnl` (USD). Пустая выборка - n=0 и пустые значения."""
    n = len(df)
    note = None if n >= min_conclusions else (f"Сделок {n} (для выводов нужно порядка {min_conclusions}+): метрики описательные, статистически значимых выводов делать нельзя.")
    base: dict[str, Any] = {"n": n, "significance": {"enough": n >= min_conclusions, "min_trades": min_conclusions, "note": note}}
    if n == 0:
        return {**base, "wins": 0, "losses": 0, "winrate": None, "profit_factor": None, "profit_factor_infinite": False, "net_r": 0.0, "expectancy_r": None,
                "expectancy_r_ci": None, "expectancy_usd": None, "net_usd": None, "sharpe_trade": None, "sortino_trade": None, "sharpe_daily": None,
                "sortino_daily": None, "max_drawdown_r": None, "max_drawdown_usd": None, "max_drawdown_pct": None}
    d = df.sort_values("exit_time", kind="stable")
    r = d["r"].to_numpy(dtype=float)
    wins, losses = int((r > 0).sum()), int((r < 0).sum())
    pf, pf_inf = profit_factor(r)
    sharpe_t, sortino_t = _risk_ratios(r)
    daily = daily_returns(r, d["exit_time"])
    sharpe_d, sortino_d = _risk_ratios(daily)
    ann = math.sqrt(ANNUAL_DAYS)
    expectancy = float(r.mean())
    std = float(r.std(ddof=1)) if n > 1 else 0.0
    out: dict[str, Any] = {
        **base, "wins": wins, "losses": losses, "breakeven": n - wins - losses, "winrate": wins / n,
        "profit_factor": pf, "profit_factor_infinite": pf_inf, "gross_profit_r": float(r[r > 0].sum()), "gross_loss_r": float(-r[r < 0].sum()),
        "net_r": float(r.sum()), "expectancy_r": expectancy,
        "expectancy_r_ci": bootstrap_mean_ci(r, bootstrap_n) if with_ci else None,
        "avg_win_r": float(r[r > 0].mean()) if wins else None, "avg_loss_r": float(r[r < 0].mean()) if losses else None,
        "t_stat": expectancy / (std / math.sqrt(n)) if std > 0 else None,
        "sharpe_trade": sharpe_t, "sortino_trade": sortino_t,
        "sharpe_daily": None if sharpe_d is None else sharpe_d * ann, "sortino_daily": None if sortino_d is None else sortino_d * ann, "days": int(len(daily)),
        "max_drawdown_r": max_drawdown(np.concatenate([[0.0], np.cumsum(r)])),
    }
    out["payoff"] = (out["avg_win_r"] / abs(out["avg_loss_r"])) if out["avg_win_r"] and out["avg_loss_r"] else None
    pnl = d["pnl"].to_numpy(dtype=float) if "pnl" in d else np.full(n, np.nan)
    if np.isfinite(pnl).all():
        eq = initial_equity + np.concatenate([[0.0], np.cumsum(pnl)])
        peak = np.maximum.accumulate(eq)
        out.update(expectancy_usd=float(pnl.mean()), net_usd=float(pnl.sum()), max_drawdown_usd=float((peak - eq).max()), max_drawdown_pct=float(((peak - eq) / peak).max() * 100.0))
    else:
        out.update(expectancy_usd=None, net_usd=None, max_drawdown_usd=None, max_drawdown_pct=None)
    return clean(out)


def downsample_idx(n: int, max_points: int) -> np.ndarray:
    if n <= max_points:
        return np.arange(n)
    return np.unique(np.linspace(0, n - 1, max_points).round().astype(int))


def equity_curve(df: pd.DataFrame, max_points: int = 500) -> dict[str, list]:
    """Кривая накопленного R по времени закрытия. Колонки-массивы: `t` (мс UTC), `i` (номер сделки), `cum_r`, `cum_usd` (если есть pnl)."""
    if len(df) == 0:
        return {"t": [], "i": [], "cum_r": [], "cum_usd": []}
    d = df.sort_values("exit_time", kind="stable")
    cum_r = np.cumsum(d["r"].to_numpy(dtype=float))
    pnl = d["pnl"].to_numpy(dtype=float) if "pnl" in d else None
    t = epoch_ms(d["exit_time"])
    idx = downsample_idx(len(d), max_points)
    return clean({"t": t[idx], "i": idx + 1, "cum_r": cum_r[idx], "cum_usd": np.cumsum(pnl)[idx] if pnl is not None and np.isfinite(pnl).all() else [None] * len(idx)})


def permutation_p(a: np.ndarray, b: np.ndarray, n_perm: int = 2000, seed: int = 5) -> float | None:
    """Двусторонняя перестановочная p-оценка различия средних a и b."""
    if len(a) < MIN_CI_TRADES or len(b) < MIN_CI_TRADES:
        return None
    pool = np.concatenate([a, b])
    obs = abs(a.mean() - b.mean())
    rng = np.random.default_rng(seed)
    hits = 0
    for _ in range(n_perm):
        rng.shuffle(pool)
        hits += abs(pool[:len(a)].mean() - pool[len(a):].mean()) >= obs
    return float((hits + 1) / (n_perm + 1))
