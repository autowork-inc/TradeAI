"""Версионирование стратегий и A/B-сравнение (модуль 4).

Версия - это именованный срез сделок одной стратегии: `strategy_id` + период по времени ВХОДА [from, to) + (необязательно) отпечаток плана выхода `plan_hash`
+ точные значения полей meta. Примеры: «SMC v1.0» = SMC_Pro_Trend до 2026-10-18; «SMC v1.1 strict buildup» = после, с meta.require_buildup = true.
Версии заводит человек (API `/versions`); дополнительно анализатор находит АВТОВЕРСИИ - периоды с разной конфигурацией плана выхода (`plan_hash`).
Изменения порогов входа в БД не записываются (в плане/мета их нет), поэтому такие версии задаются периодом: «с даты деплоя».

Сравнение A (базовая) и B (кандидат): метрики обеих, наложенные кривые накопленного R (по номеру сделки и по времени), разность матожиданий с bootstrap-интервалом
и вердикт деградации:
  degraded     - матожидание B ниже A и верхняя граница 95% интервала разности < 0 (значимо хуже);
  suspected    - B ниже A, интервал захватывает 0, но P(B не хуже A) < 10% либо PF упал более чем на 20%;
  improved     - B выше A, нижняя граница интервала > 0;
  inconclusive - иначе; insufficient_data - в одной из версий меньше min_group_trades сделок.
"""

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from analyzer.errors import DataError
from analyzer.metrics import bootstrap_diff, clean, compute_metrics, equity_curve, profit_factor

PF_DROP_WARN = 0.20
SUSPECT_P = 0.10


@dataclass(frozen=True)
class VersionDef:
    name: str
    strategy_id: str
    ts_from: pd.Timestamp | None = None  # включительно, UTC
    ts_to: pd.Timestamp | None = None  # исключительно
    plan_hash: str | None = None
    meta_equals: dict[str, Any] = field(default_factory=dict)
    note: str = ""

    def to_json(self) -> dict[str, Any]:
        return clean({"name": self.name, "strategy_id": self.strategy_id, "ts_from": self.ts_from, "ts_to": self.ts_to, "plan_hash": self.plan_hash,
                      "meta_equals": self.meta_equals, "note": self.note})

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "VersionDef":
        def ts(v: Any) -> pd.Timestamp | None:
            if v in (None, ""):
                return None
            t = pd.Timestamp(v)
            return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
        return cls(name=d["name"], strategy_id=d["strategy_id"], ts_from=ts(d.get("ts_from")), ts_to=ts(d.get("ts_to")), plan_hash=d.get("plan_hash") or None,
                   meta_equals=dict(d.get("meta_equals") or {}), note=d.get("note") or "")


def select(df: pd.DataFrame, v: VersionDef) -> pd.DataFrame:
    """Сделки версии (по времени входа)."""
    mask = (df["strategy_id"] == v.strategy_id).to_numpy()
    if v.ts_from is not None:
        mask &= (df["entry_ts"] >= v.ts_from).to_numpy()
    if v.ts_to is not None:
        mask &= (df["entry_ts"] < v.ts_to).to_numpy()
    if v.plan_hash:
        mask &= (df["plan_hash"] == v.plan_hash).to_numpy()
    for k, want in v.meta_equals.items():
        mask &= df["meta"].map(lambda m, k=k, want=want: m.get(k) == want).to_numpy()
    return df[mask]


def auto_versions(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Периоды с разной конфигурацией плана выхода внутри каждой стратегии: имя `<strategy>@<plan_hash>`, первая/последняя сделка, число сделок."""
    out = []
    for (sid, ph), g in df.groupby(["strategy_id", "plan_hash"], sort=True):
        out.append({"name": f"{sid}@{ph}", "strategy_id": sid, "plan_hash": ph, "n": int(len(g)), "first_entry": g["entry_ts"].min(), "last_entry": g["entry_ts"].max()})
    return clean(out)


def degradation(base_r: np.ndarray, cand_r: np.ndarray, *, min_n: int, n_boot: int = 1000) -> dict[str, Any]:
    """Вердикт деградации кандидата относительно базовой версии (см. docstring модуля)."""
    if len(base_r) < min_n or len(cand_r) < min_n:
        return {"verdict": "insufficient_data", "message": f"Нужно не меньше {min_n} сделок в каждой версии (A: {len(base_r)}, B: {len(cand_r)})", "diff": None}
    d = bootstrap_diff(base_r, cand_r, n_boot)
    if d is None:
        return {"verdict": "insufficient_data", "message": "Слишком мало сделок для интервала", "diff": None}
    pf_a, _ = profit_factor(base_r)
    pf_b, _ = profit_factor(cand_r)
    pf_drop = (pf_a - pf_b) / pf_a if pf_a and pf_b is not None and pf_a > 0 else None
    lo, hi = d["ci"]
    if d["diff"] < 0 and hi < 0:
        verdict, msg = "degraded", "Матожидание новой версии значимо ниже базовой."
    elif d["diff"] < 0 and (d["p_not_worse"] < SUSPECT_P or (pf_drop is not None and pf_drop > PF_DROP_WARN)):
        verdict, msg = "suspected", "Новая версия, похоже, хуже, но данных пока не хватает для уверенного вывода."
    elif d["diff"] > 0 and lo > 0:
        verdict, msg = "improved", "Матожидание новой версии значимо выше базовой."
    else:
        verdict, msg = "inconclusive", "Различие неотличимо от шума."
    return {"verdict": verdict, "message": msg, "diff": d, "pf_drop": pf_drop}


def compare_versions(df: pd.DataFrame, a: VersionDef, b: VersionDef, cfg: Any, *, align: str = "overlap") -> dict[str, Any]:
    """A/B. align='overlap': сделки обеих версий обрезаются по общему отрезку времени (если версии не пересекаются - остаётся 'relative' с пометкой);
    align='relative': версии целиком, кривые по номеру сделки. Кривые отдаются в обоих видах (`i` и `t`)."""
    if align not in ("overlap", "relative"):
        raise DataError("align: overlap | relative")
    sa, sb = select(df, a), select(df, b)
    note = None
    used = "relative"
    if align == "overlap" and len(sa) and len(sb):
        lo, hi = max(sa["entry_ts"].min(), sb["entry_ts"].min()), min(sa["exit_time"].max(), sb["exit_time"].max())
        if lo < hi:
            ca, cb = sa[(sa["entry_ts"] >= lo) & (sa["exit_time"] <= hi)], sb[(sb["entry_ts"] >= lo) & (sb["exit_time"] <= hi)]
            if len(ca) and len(cb):
                sa, sb, used = ca, cb, "overlap"
        if used != "overlap":
            note = "Версии не пересекаются по времени: сравнение по номеру сделки, а не по общему отрезку (рынок мог измениться между периодами)."
    elif align == "overlap":
        note = "В одной из версий нет сделок."
    kw = dict(initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n)
    ma, mb = compute_metrics(sa, **kw), compute_metrics(sb, **kw)
    verdict = degradation(sa["r"].to_numpy(dtype=float), sb["r"].to_numpy(dtype=float), min_n=cfg.min_group_trades, n_boot=cfg.bootstrap_n)
    return clean({"baseline": {"version": a.to_json(), "metrics": ma, "curve": equity_curve(sa)}, "candidate": {"version": b.to_json(), "metrics": mb, "curve": equity_curve(sb)},
                  "align": used, "note": note, "verdict": verdict,
                  "window": {"from": max(x for x in (sa["entry_ts"].min(), sb["entry_ts"].min()) if pd.notna(x)) if len(sa) and len(sb) else None,
                             "to": min(x for x in (sa["exit_time"].max(), sb["exit_time"].max()) if pd.notna(x)) if len(sa) and len(sb) else None}})
