"""Сравнение разметки трейдера и бота: совпало / пропущено ботом / лишнее у бота - по каждому понятию.

Рисунок трейдера: {"id", "tool": trend|hline|ray|rect|point|long|short, "term": id понятия, "note", "p1": {"t","p"}, "p2": {"t","p"}}.
Допуски - в ATR (по цене) и в свечах (по времени), не «копейка в копейку»: человек ставит линию на глаз.
Лишнее у бота считается только по понятиям, которые трейдер в этом уроке размечал («размечено полностью»), и только в окне, которое он смотрел."""

from typing import Any

from tradeai_client.lab.detect import atr_series

PRICE_TOL_ATR = 0.5
TIME_TOL_BARS = 2


def user_geometry(d: dict, end_t: int) -> dict[str, Any] | None:
    """Рисунок трейдера -> геометрия того же вида, что объекты бота (kind, t1, t2, p1, p2)."""
    tool, p1, p2 = d.get("tool"), d.get("p1"), d.get("p2")
    if not p1:
        return None
    if tool == "hline":
        return {"kind": "hline", "t1": p1["t"], "t2": end_t, "p1": p1["p"], "p2": p1["p"]}
    if tool == "point":
        return {"kind": "point", "t1": p1["t"], "t2": p1["t"], "p1": p1["p"], "p2": p1["p"]}
    if not p2:
        return None
    a, b = (p1, p2) if p1["t"] <= p2["t"] else (p2, p1)
    if tool == "trend":
        return {"kind": "line", "t1": a["t"], "t2": b["t"], "p1": a["p"], "p2": b["p"]}
    if tool == "ray":
        if b["t"] == a["t"]:
            return None
        k = (b["p"] - a["p"]) / (b["t"] - a["t"])
        return {"kind": "line", "t1": a["t"], "t2": end_t, "p1": a["p"], "p2": a["p"] + k * (end_t - a["t"])}
    if tool == "rect":
        return {"kind": "box", "t1": a["t"], "t2": b["t"], "p1": min(p1["p"], p2["p"]), "p2": max(p1["p"], p2["p"])}
    return None  # сделка (long/short) - не разметка понятия


def _price_at(g: dict, t: float) -> float:
    if g["kind"] != "line" or g["t2"] == g["t1"]:
        return g["p1"]
    return g["p1"] + (g["p2"] - g["p1"]) * (t - g["t1"]) / (g["t2"] - g["t1"])


def same(u: dict, b: dict, atr: float, iv: int) -> bool:
    """Совпадают ли рисунок трейдера u и объект бота b (оба - геометрия)."""
    pt, tt = PRICE_TOL_ATR * atr, TIME_TOL_BARS * iv
    ku, kb = u["kind"], b["kind"]
    if ku == "point" or kb == "point":
        if ku != kb:
            return False
        return abs(u["t1"] - b["t1"]) <= tt and abs(u["p1"] - b["p1"]) <= pt
    overlap = min(u["t2"], b["t2"]) - max(u["t1"], b["t1"])
    if overlap < -tt:
        return False
    if ku in ("hline", "box") and kb in ("hline", "box"):
        ulo, uhi = min(u["p1"], u["p2"]), max(u["p1"], u["p2"])
        blo, bhi = min(b["p1"], b["p2"]), max(b["p1"], b["p2"])
        if ku == "box" and kb == "box":  # коридоры: перекрытие по цене и по времени не меньше трети меньшего
            p_ov = min(uhi, bhi) - max(ulo, blo)
            t_need = 0.33 * min(u["t2"] - u["t1"], b["t2"] - b["t1"])
            return p_ov >= -pt and overlap >= t_need - tt
        return min(uhi, bhi) - max(ulo, blo) >= -pt
    if ku == "line" and kb == "line":
        lo, hi = max(u["t1"], b["t1"]), min(u["t2"], b["t2"])
        span = u["t2"] - u["t1"]
        if hi - lo < 0.5 * span - tt:
            return False
        return abs(_price_at(u, lo) - _price_at(b, lo)) <= pt * 1.2 and abs(_price_at(u, hi) - _price_at(b, hi)) <= pt * 1.2
    return False


def compare(lesson: dict, bot_objects: list[dict]) -> dict[str, Any]:
    """-> {"by_type": {term: {tp, fn, fp, precision, recall, f1}}, "overall": {...}, "marks": [{id|bot_index, status}]}."""
    bars, iv = lesson["bars"], int(lesson["iv"])
    atr = (atr_series(bars)[-1] if bars else 0) or 1e-9
    view = lesson.get("view") or {"from_t": bars[0]["t"], "to_t": bars[-1]["t"]}
    end_t = bars[-1]["t"]
    users = []
    for d in lesson.get("drawings", []):
        g = user_geometry(d, end_t)
        if g is not None and d.get("term"):
            users.append((d, {**g, "term": d["term"]}))
    complete = set(lesson.get("complete_types") or {u[1]["term"] for u in users})
    in_view = lambda o: o["t2"] >= view["from_t"] and o["t1"] <= view["to_t"]  # noqa: E731
    bots = [(i, o) for i, o in enumerate(bot_objects) if o["type"] in complete and o["kind"] in ("hline", "box", "line", "point") and in_view(o)]
    used: set[int] = set()
    stats: dict[str, dict[str, int]] = {}
    marks: list[dict] = []
    for d, g in users:
        st = stats.setdefault(g["term"], {"tp": 0, "fn": 0, "fp": 0})
        hit = next((i for i, o in bots if i not in used and o["type"] == g["term"] and same(g, o, atr, iv)), None)
        if hit is None:
            st["fn"] += 1
            marks.append({"user_id": d.get("id"), "status": "missed"})
        else:
            used.add(hit)
            st["tp"] += 1
            marks.append({"user_id": d.get("id"), "bot_index": hit, "status": "matched"})
    for i, o in bots:
        if i not in used:
            stats.setdefault(o["type"], {"tp": 0, "fn": 0, "fp": 0})["fp"] += 1
            marks.append({"bot_index": i, "status": "extra"})
    return {"by_type": {k: _f1(v) for k, v in stats.items()}, "overall": _f1({k: sum(v[k] for v in stats.values()) for k in ("tp", "fn", "fp")}), "marks": marks}


def _f1(s: dict[str, int]) -> dict[str, Any]:
    tp, fn, fp = s["tp"], s["fn"], s["fp"]
    p = tp / (tp + fp) if tp + fp else None
    r = tp / (tp + fn) if tp + fn else None
    f1 = (2 * p * r / (p + r)) if p and r else (0.0 if (tp + fn + fp) else None)
    return {"tp": tp, "fn": fn, "fp": fp, "precision": p, "recall": r, "f1": f1}
