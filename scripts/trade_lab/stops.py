"""Геометрия стопа по свечам снапшота (100 свечей рабочего ТФ до сигнала включительно): где стоит SL относительно структуры.

Метрики (лонг; шорт - зеркально):
  risk_atr          |вход - SL| / ATR(14, SMA true range) сигнальной свечи;
  anchor            «якорь» - самая глубокая структура, за которой прячется стоп: подтверждённые пивоты-минимумы (k=2) за 30 свечей
                    и минимум 3 последних свечей, лежащие между SL и входом; нет таких - якоря нет;
  beyond_anchor_atr насколько SL дальше якоря (ожидаемо ~0.2 ATR - буфер стратегий);
  near_struct_atr   расстояние от SL до ближайшей структуры между SL и входом (для «в воздухе»);
  pierce_20         сколько из 20 последних свечей уже заходили за уровень SL (стоп в шуме).
Классы (первое сработавшее): no_data, tight (< 0.5 ATR), noise (pierce_20 >= 1), air (нет структуры ближе 1.5 ATR), sag (провис: за якорем
> 0.6 ATR), wide (> 2.5 ATR), ok.
"""

from __future__ import annotations

from typing import Any

from .data import Trade

ATR_N = 14
PIVOT_K = 2
ANCHOR_BARS = 30
PIERCE_BARS = 20
TIGHT_ATR, WIDE_ATR, SAG_ATR, AIR_ATR = 0.5, 2.5, 0.6, 1.5
CLASS_RU = {"no_data": "нет свечей", "tight": "узкий", "noise": "в шуме", "air": "в воздухе", "sag": "провис", "wide": "широкий", "ok": "норма"}
META_STOP_KEYS = ("sl_extreme", "sweep_extreme", "stop_atr", "risk_atr")


def atr_sma(candles: list[dict[str, Any]], n: int = ATR_N) -> float | None:
    if len(candles) < n + 1:
        return None
    trs = []
    for prev, cur in zip(candles[-n - 1:-1], candles[-n:]):
        h, low, pc = float(cur["high"]), float(cur["low"]), float(prev["close"])
        trs.append(max(h - low, abs(h - pc), abs(low - pc)))
    atr = sum(trs) / n
    return atr if atr > 0 else None


def pivots(candles: list[dict[str, Any]], side: str, k: int = PIVOT_K, bars: int = ANCHOR_BARS) -> list[float]:
    """Подтверждённые пивоты на стороне стопа (минимумы для лонга, максимумы для шорта) за последние `bars` свечей."""
    key = "low" if side == "long" else "high"
    vals = [float(c[key]) for c in candles]
    sgn = 1 if side == "long" else -1  # лонг: пивот = строго ниже k слева и не выше k справа
    out = []
    for i in range(max(k, len(vals) - bars), len(vals) - k):
        v = sgn * vals[i]
        if all(v < sgn * vals[j] for j in range(i - k, i)) and all(v <= sgn * vals[j] for j in range(i + 1, i + k + 1)):
            out.append(vals[i])
    return out


def geometry(t: Trade) -> dict[str, Any]:
    c = t.candles
    entry = t.entry if t.entry is not None else (t.meta.get("entry") if isinstance(t.meta.get("entry"), (int, float)) else None)
    atr = atr_sma(c) if c else None
    if entry is None or atr is None:
        return {"stop_class": "no_data"}
    d = 1 if t.side == "long" else -1  # «ниже» для лонга
    dist = abs(entry - t.sl)
    key = "low" if t.side == "long" else "high"
    structs = pivots(c, t.side) + [min(float(x["low"]) for x in c[-3:]) if t.side == "long" else max(float(x["high"]) for x in c[-3:])]
    behind = [s for s in structs if d * (s - t.sl) >= 0 and d * (entry - s) > 0]  # между SL и входом
    above = [d * (s - t.sl) for s in behind]
    anchor = (min(behind) if t.side == "long" else max(behind)) if behind else None
    pierce = sum(1 for x in c[-PIERCE_BARS:] if d * (float(x[key]) - t.sl) < 0)
    g: dict[str, Any] = {
        "atr": atr, "risk_atr": dist / atr, "anchor": anchor,
        "beyond_anchor_atr": None if anchor is None else d * (anchor - t.sl) / atr,
        "near_struct_atr": min(above) / atr if above else None, "pierce_20": pierce,
    }
    for k in META_STOP_KEYS:
        v = t.meta.get(k)
        if isinstance(v, (int, float)):
            g[f"meta_{k}"] = float(v)
    if isinstance(t.meta.get("sl_extreme"), (int, float)):
        g["meta_extreme_vs_anchor_atr"] = None if anchor is None else d * (anchor - float(t.meta["sl_extreme"])) / atr
    g["stop_class"] = classify(g)
    return g


def classify(g: dict[str, Any]) -> str:
    if g.get("risk_atr") is None:
        return "no_data"
    if g["risk_atr"] < TIGHT_ATR:
        return "tight"
    if g["pierce_20"] >= 1:
        return "noise"
    if g["near_struct_atr"] is None or g["near_struct_atr"] > AIR_ATR:
        return "air"
    if g["beyond_anchor_atr"] is not None and g["beyond_anchor_atr"] > SAG_ATR:
        return "sag"
    if g["risk_atr"] > WIDE_ATR:
        return "wide"
    return "ok"


def risk_bucket(risk_atr: float | None) -> str:
    if risk_atr is None:
        return "?"
    for hi, label in ((0.5, "<0.5"), (1.0, "0.5-1"), (1.5, "1-1.5"), (2.5, "1.5-2.5")):
        if risk_atr < hi:
            return label
    return ">=2.5"


def annotate(trades: list[Trade]) -> None:
    for t in trades:
        t.extra.update(geometry(t))
