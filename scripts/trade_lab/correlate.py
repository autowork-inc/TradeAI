"""Корреляции признаков сделки с исходом - по каждой стратегии отдельно.

Признаки: числовые и категориальные поля meta (у каждой стратегии свои), геометрия стопа (stops.py), сторона, тип входа, монета, сессия и час UTC.
Цели: hit1 - сделка прошла +1R до стопа/выхода (не зависит от плана выхода); r - чистый R по выбранному плану (exits.simulate).
Метод: ранговая корреляция Спирмена + квартили для чисел, группы для категорий. Порог - MIN_GROUP сделок в группе.
Это ГИПОТЕЗЫ: поправки на множественность сравнений нет, выборка мала.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .data import Trade
from .exits import Plan, simulate
from .stops import risk_bucket

MIN_GROUP = 15
SKIP_META = {"entry", "sl", "tp", "inval", "interval", "geometry"}
MAX_CATEGORIES = 8


def session(hour: int) -> str:
    return "Asia" if hour < 7 else "London" if hour < 12 else "NewYork" if hour < 21 else "Off"


def features(t: Trade) -> dict[str, Any]:
    dt = datetime.fromtimestamp(t.signal_ms / 1000, tz=timezone.utc)
    f: dict[str, Any] = {"side": t.side, "kind": t.kind, "coin": t.coin, "session": session(dt.hour), "hour_utc": float(dt.hour),
                         "weekday": dt.strftime("%a")}
    for k in ("risk_atr", "beyond_anchor_atr", "pierce_20", "near_struct_atr"):
        v = t.extra.get(k)
        if isinstance(v, (int, float)):
            f[f"stop.{k}"] = float(v)
    if "stop_class" in t.extra:
        f["stop.class"] = t.extra["stop_class"]
        f["stop.risk_bucket"] = risk_bucket(t.extra.get("risk_atr"))
    for k, v in t.meta.items():
        if k in SKIP_META or v is None:
            continue
        if isinstance(v, bool):
            f[f"meta.{k}"] = float(v)
        elif isinstance(v, (int, float)):
            f[f"meta.{k}"] = float(v)
        elif isinstance(v, str) and len(v) <= 40:
            f[f"meta.{k}"] = v
    return f


def ranks(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    out = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2 + 1
        for m in range(i, j + 1):
            out[order[m]] = avg
        i = j + 1
    return out


def spearman(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    rx, ry = ranks(xs), ranks(ys)
    mx, my = sum(rx) / len(rx), sum(ry) / len(ry)
    sxy = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    sxx, syy = sum((a - mx) ** 2 for a in rx), sum((b - my) ** 2 for b in ry)
    return None if sxx == 0 or syy == 0 else sxy / (sxx * syy) ** 0.5


@dataclass(frozen=True)
class Finding:
    feature: str
    kind: str  # num | cat
    n: int
    rho_hit1: float | None
    rho_r: float | None
    groups: list[tuple[str, int, float, float | None]]  # (группа, n, доля +1R, средний R)


def _group_row(label: str, items: list[tuple[float, float | None]]) -> tuple[str, int, float, float | None]:
    rs = [r for _, r in items if r is not None]
    return label, len(items), sum(h for h, _ in items) / len(items), (sum(rs) / len(rs) if rs else None)


def analyse(trades: list[Trade], plan: Plan) -> list[Finding]:
    rows = []
    for t in trades:
        hit1 = 1.0 if (t.mfe or 0.0) >= 1.0 else 0.0
        rows.append((features(t), hit1, simulate(t, plan).net_r))
    names = sorted({k for f, _, _ in rows for k in f})
    out: list[Finding] = []
    for name in names:
        vals = [(f[name], h, r) for f, h, r in rows if name in f]
        if len(vals) < 2 * MIN_GROUP:
            continue
        if all(isinstance(v, float) for v, _, _ in vals):
            xs = [v for v, _, _ in vals]
            if len(set(xs)) < 2:
                continue
            rho_h = spearman(xs, [h for _, h, _ in vals])
            known = [(v, r) for v, _, r in vals if r is not None]
            rho_r = spearman([v for v, _ in known], [r for _, r in known]) if len(known) >= 2 * MIN_GROUP else None
            ordered = sorted(vals, key=lambda x: x[0])
            q = max(1, len(ordered) // 4)
            groups = []
            for i in range(4):
                chunk = ordered[i * q:(i + 1) * q] if i < 3 else ordered[3 * q:]
                if chunk:
                    groups.append(_group_row(f"Q{i + 1} [{chunk[0][0]:.3g}..{chunk[-1][0]:.3g}]", [(h, r) for _, h, r in chunk]))
            out.append(Finding(name, "num", len(vals), rho_h, rho_r, groups))
        else:
            cats: dict[str, list[tuple[float, float | None]]] = {}
            for v, h, r in vals:
                cats.setdefault(str(v), []).append((h, r))
            big = {k: v for k, v in cats.items() if len(v) >= MIN_GROUP}
            if len(big) < 2 or len(cats) > MAX_CATEGORIES * 4:
                continue
            groups = sorted((_group_row(k, v) for k, v in big.items()), key=lambda g: -g[2])
            out.append(Finding(name, "cat", len(vals), None, None, groups))
    return sorted(out, key=lambda f: -_strength(f))


def _strength(f: Finding) -> float:
    if f.kind == "num":
        return max(abs(f.rho_hit1 or 0.0), abs(f.rho_r or 0.0))
    hits = [g[2] for g in f.groups]
    return (max(hits) - min(hits)) / 2 if hits else 0.0
