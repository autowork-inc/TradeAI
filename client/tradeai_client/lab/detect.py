"""Детекторы понятий словаря: что бот «видит» на графике. Чистый Python по списку закрытых свечей.

Свеча: {"t": open_time в секундах UTC, "o","h","l","c","v", "bv": объём агрессивных покупок (taker buy base) или None}.
Объект разметки: {"type": id понятия, "kind": hline|box|line|point, "t1","t2" (секунды), "p1","p2" (цены), "label", "side": long|short|None, "score"}.

Каждый детектор - функция (Ctx, params) -> [объекты]; параметры - числа (их подбирает tune.py под уроки трейдера). Детекторы смотрят только на переданные
свечи: в реплее это свечи до курсора, поэтому сигнал на последней свече не знает будущего. На истории разметка рисуется по всему окну (как трейдер видит
прошлое целиком), сигналы бота в реплее - только по последней закрытой свече.
"""

from dataclasses import dataclass, field
from typing import Any, Callable

ATR_N = 14

DEFAULT_PARAMS: dict[str, dict[str, float]] = {
    "high": {"k": 3},
    "low": {"k": 3},
    "trend": {"k": 5},
    "level": {"k": 3, "tol_atr": 0.3, "min_touches": 3, "lookback": 1500, "max_items": 10},
    "zone": {"k": 3, "tol_atr": 0.6, "min_touches": 2, "react_atr": 1.5, "lookback": 300, "max_items": 5},
    "liquidity": {"k": 3, "eq_atr": 0.15, "lookback": 1500},
    "gap": {"min_atr": 0.3, "lookback": 300},
    "range": {"min_bars": 16, "max_height_atr": 4.0, "max_er": 0.35},
    "consolidation": {"min_bars": 8, "max_height_atr": 2.0, "decay": 0.9},
    "trendline": {"k": 4, "min_span": 8, "tol_atr": 0.25, "min_touches": 2, "lookback": 300, "max_items": 6},
    "breakout": {"min_break_atr": 0.1, "min_body_atr": 0.4},
    "zone_exit": {"hold_bars": 3},
    "reversal": {"wick_frac": 0.55, "near_atr": 0.4},
    "volume": {"spike": 2.5, "window": 6, "buy_max": 0.42},
}


def obj(type_: str, kind: str, t1: float, t2: float, p1: float, p2: float, label: str, side: str | None = None, score: float = 1.0, **meta: Any) -> dict[str, Any]:
    o = {"type": type_, "kind": kind, "t1": int(t1), "t2": int(t2), "p1": float(p1), "p2": float(p2), "label": label, "side": side, "score": round(float(score), 3)}
    if meta:
        o["meta"] = meta
    return o


# ------------------------------------------------------------------------------------------ основа


def atr_series(bars: list[dict], n: int = ATR_N) -> list[float]:
    """SMA-ATR (как у бота): истинный диапазон, среднее за n; в начале - среднее по имеющимся."""
    out, trs = [], []
    for i, b in enumerate(bars):
        pc = bars[i - 1]["c"] if i else b["c"]
        trs.append(max(b["h"] - b["l"], abs(b["h"] - pc), abs(b["l"] - pc)))
        w = trs[-n:]
        out.append(sum(w) / len(w))
    return out


def pivots(bars: list[dict], k: int) -> tuple[list[int], list[int]]:
    """Подтверждённые экстремумы: high[i] выше k свечей слева (строго) и не ниже k справа; справа k свечей уже есть (без будущего)."""
    k = max(1, int(k))
    n = len(bars)
    hi, lo = [], []
    for i in range(k, n - k):
        h, l = bars[i]["h"], bars[i]["l"]
        if h > max(b["h"] for b in bars[i - k:i]) and h >= max(b["h"] for b in bars[i + 1:i + k + 1]):
            hi.append(i)
        if l < min(b["l"] for b in bars[i - k:i]) and l <= min(b["l"] for b in bars[i + 1:i + k + 1]):
            lo.append(i)
    return hi, lo


@dataclass
class Ctx:
    bars: list[dict]
    iv: int
    atr: list[float] = field(default_factory=list)
    piv: dict[int, tuple[list[int], list[int]]] = field(default_factory=dict)
    out: dict[str, list[dict]] = field(default_factory=dict)  # результаты уже посчитанных детекторов (зависимости)

    def __post_init__(self) -> None:
        self.atr = atr_series(self.bars)

    @property
    def n(self) -> int:
        return len(self.bars)

    def t(self, i: int) -> int:
        return self.bars[i]["t"]

    def end_t(self) -> int:
        return self.bars[-1]["t"]

    def pivots(self, k: float) -> tuple[list[int], list[int]]:
        k = int(k)
        if k not in self.piv:
            self.piv[k] = pivots(self.bars, k)
        return self.piv[k]

    def since(self, lookback: float) -> int:
        return max(0, self.n - int(lookback))


# ------------------------------------------------------------------------------------------ экстремумы и тренд


def det_high(c: Ctx, p: dict) -> list[dict]:
    hi, _ = c.pivots(p["k"])
    return [obj("high", "point", c.t(i), c.t(i), c.bars[i]["h"], c.bars[i]["h"], "H") for i in hi]


def det_low(c: Ctx, p: dict) -> list[dict]:
    _, lo = c.pivots(p["k"])
    return [obj("low", "point", c.t(i), c.t(i), c.bars[i]["l"], c.bars[i]["l"], "L") for i in lo]


def zigzag(c: Ctx, k: float) -> list[tuple[int, float, str]]:
    """Чередующиеся экстремумы (H/L): подряд идущие одного типа сливаются в самый крайний."""
    hi, lo = c.pivots(k)
    pts = sorted([(i, c.bars[i]["h"], "H") for i in hi] + [(i, c.bars[i]["l"], "L") for i in lo])
    out: list[tuple[int, float, str]] = []
    for pt in pts:
        if out and out[-1][2] == pt[2]:
            if (pt[2] == "H" and pt[1] >= out[-1][1]) or (pt[2] == "L" and pt[1] <= out[-1][1]):
                out[-1] = pt
            continue
        out.append(pt)
    return out


def trend_state(c: Ctx, k: float) -> int:
    """+1 вверх (HH и HL), -1 вниз (LH и LL), 0 боковик - по последним подтверждённым экстремумам."""
    zz = zigzag(c, k)
    highs = [p for p in zz if p[2] == "H"][-2:]
    lows = [p for p in zz if p[2] == "L"][-2:]
    if len(highs) < 2 or len(lows) < 2:
        return 0
    if highs[1][1] > highs[0][1] and lows[1][1] > lows[0][1]:
        return 1
    if highs[1][1] < highs[0][1] and lows[1][1] < lows[0][1]:
        return -1
    return 0


def det_trend(c: Ctx, p: dict) -> list[dict]:
    zz = zigzag(c, p["k"])
    out, last = [], {"H": None, "L": None}
    for a, b in zip(zz, zz[1:]):
        up = b[1] > a[1]
        out.append(obj("trend", "line", c.t(a[0]), c.t(b[0]), a[1], b[1], "", "long" if up else "short", 1.0))
    for i, price, kind in zz:
        prev = last[kind]
        if prev is not None:
            tag = ("HH" if price > prev else "LH") if kind == "H" else ("HL" if price > prev else "LL")
            out.append(obj("trend", "point", c.t(i), c.t(i), price, price, tag, None, 1.0))
        last[kind] = price
    st = trend_state(c, p["k"])
    label = {1: "тренд вверх", -1: "тренд вниз", 0: "боковик"}[st]
    out.append(obj("trend", "label", c.end_t(), c.end_t(), c.bars[-1]["c"], c.bars[-1]["c"], label, {1: "long", -1: "short", 0: None}[st], 1.0, state=st))
    return out


# ------------------------------------------------------------------------------------------ уровни, зоны, ликвидность


def _clusters(points: list[tuple[int, float]], tol: float) -> list[list[tuple[int, float]]]:
    pts = sorted(points, key=lambda x: x[1])
    groups: list[list[tuple[int, float]]] = []
    for pt in pts:
        if groups and pt[1] - sum(x[1] for x in groups[-1]) / len(groups[-1]) <= tol:
            groups[-1].append(pt)
        else:
            groups.append([pt])
    return groups


def det_level(c: Ctx, p: dict) -> list[dict]:
    hi, lo = c.pivots(p["k"])
    s = c.since(p["lookback"])
    pts = [(i, c.bars[i]["h"]) for i in hi if i >= s] + [(i, c.bars[i]["l"]) for i in lo if i >= s]
    atr = c.atr[-1] or 1e-9
    out = []
    for g in _clusters(pts, p["tol_atr"] * atr):
        if len(g) < p["min_touches"]:
            continue
        prices = sorted(x[1] for x in g)
        lvl = prices[len(prices) // 2]
        out.append(obj("level", "hline", c.t(min(x[0] for x in g)), c.end_t(), lvl, lvl, f"уровень ×{len(g)}", None, len(g)))
    close = c.bars[-1]["c"]
    out.sort(key=lambda o: (-o["score"], abs(o["p1"] - close)))
    return out[:int(p["max_items"])]


def det_zone(c: Ctx, p: dict) -> list[dict]:
    hi, lo = c.pivots(p["k"])
    s = c.since(p["lookback"])
    b = c.bars
    reacted: list[tuple[int, float]] = []
    for i in hi:
        if i >= s and min(x["l"] for x in b[i + 1:i + 13] or [b[i]]) <= b[i]["h"] - p["react_atr"] * c.atr[i]:
            reacted.append((i, b[i]["h"]))
    for i in lo:
        if i >= s and max(x["h"] for x in b[i + 1:i + 13] or [b[i]]) >= b[i]["l"] + p["react_atr"] * c.atr[i]:
            reacted.append((i, b[i]["l"]))
    atr = c.atr[-1] or 1e-9
    zones = []
    for g in _clusters(reacted, p["tol_atr"] * atr):
        if len(g) < p["min_touches"]:
            continue
        lo_p, hi_p = min(x[1] for x in g), max(x[1] for x in g)
        if hi_p - lo_p < 0.25 * atr:
            mid = (hi_p + lo_p) / 2
            lo_p, hi_p = mid - 0.125 * atr, mid + 0.125 * atr
        zones.append([min(x[0] for x in g), lo_p, hi_p, len(g)])
    zones.sort(key=lambda z: z[1])
    merged: list[list] = []
    for z in zones:  # пересекающиеся зоны - одна
        if merged and z[1] <= merged[-1][2]:
            m = merged[-1]
            m[0], m[2], m[3] = min(m[0], z[0]), max(m[2], z[2]), m[3] + z[3]
        else:
            merged.append(z)
    close = c.bars[-1]["c"]
    out = [obj("zone", "box", c.t(z[0]), c.end_t(), z[1], z[2], f"зона ×{z[3]}", None, z[3]) for z in merged]
    out.sort(key=lambda o: (-o["score"], abs((o["p1"] + o["p2"]) / 2 - close)))
    return out[:int(p["max_items"])]


def det_liquidity(c: Ctx, p: dict) -> list[dict]:
    hi, lo = c.pivots(p["k"])
    s = c.since(p["lookback"])
    b, out, seen = c.bars, [], []
    for idx, key, up in ((hi, "h", True), (lo, "l", False)):
        idx = [i for i in idx if i >= s]
        for a, z in zip(idx, idx[1:]):
            tol = p["eq_atr"] * c.atr[z]
            if abs(b[a][key] - b[z][key]) > tol:
                continue
            lvl = max(b[a][key], b[z][key]) if up else min(b[a][key], b[z][key])
            if any(abs(lvl - x) <= tol for x in seen):
                continue
            seen.append(lvl)
            swept = next((j for j in range(z + 1, c.n) if (b[j]["h"] > lvl if up else b[j]["l"] < lvl)), None)
            side_txt = "равные хаи" if up else "равные лои"
            if swept is None:
                out.append(obj("liquidity", "hline", c.t(a), c.end_t(), lvl, lvl, f"ликвидность ({side_txt})", "short" if up else "long", 2))
            else:
                back = b[swept]["c"] < lvl if up else b[swept]["c"] > lvl
                out.append(obj("liquidity", "hline", c.t(a), c.t(swept), lvl, lvl, f"ликвидность ({side_txt})", "short" if up else "long", 2))
                out.append(obj("liquidity", "point", c.t(swept), c.t(swept), lvl, lvl, "свип ликвидности" if back else "ликвидность снята",
                               "short" if up else "long", 2 if back else 1))
    return out


# ------------------------------------------------------------------------------------------ гэпы, проторговки


def det_gap(c: Ctx, p: dict) -> list[dict]:
    b, out = c.bars, []
    for i in range(max(2, c.since(p["lookback"])), c.n):
        for up in (True, False):
            lo_p, hi_p = (b[i - 2]["h"], b[i]["l"]) if up else (b[i]["h"], b[i - 2]["l"])
            if hi_p - lo_p < p["min_atr"] * c.atr[i - 1]:
                continue
            filled = next((j for j in range(i + 1, c.n) if (b[j]["l"] <= lo_p if up else b[j]["h"] >= hi_p)), None)
            label = "гэп заполнен" if filled is not None else "гэп"
            out.append(obj("gap", "box", c.t(i - 1), c.t(filled) if filled is not None else c.end_t(), lo_p, hi_p, label, "long" if up else "short",
                           (hi_p - lo_p) / (c.atr[i - 1] or 1e-9), filled=filled is not None))
    return out


def find_boxes(c: Ctx, min_bars: float, max_h_atr: float, max_er: float | None = None, decay: float | None = None) -> list[dict]:
    """Коридоры: окно растёт, пока высота <= max_h_atr * ATR; вышла за предел и окно длинное (и боковое / сжимающееся) - коридор.
    Свеча, разорвавшая высоту, - кандидат на пробой (`break_i`). Последний коридор может быть ещё открыт (`open=True`)."""
    b, n, boxes = c.bars, c.n, []
    s = 0
    while s < n - 1:
        hi, lo, e = b[s]["h"], b[s]["l"], s + 1
        while e < n:
            nh, nl = max(hi, b[e]["h"]), min(lo, b[e]["l"])
            if nh - nl > max_h_atr * c.atr[e]:
                break
            hi, lo, e = nh, nl, e + 1
        length = e - s
        ok = length >= min_bars
        if ok and max_er is not None:
            path = sum(abs(b[j]["c"] - b[j - 1]["c"]) for j in range(s + 1, e)) or 1e-9
            ok = abs(b[e - 1]["c"] - b[s]["c"]) / path <= max_er
        if ok and decay is not None:
            half = (s + e) // 2
            r1 = sum(b[j]["h"] - b[j]["l"] for j in range(s, half)) / max(1, half - s)
            r2 = sum(b[j]["h"] - b[j]["l"] for j in range(half, e)) / max(1, e - half)
            ok = r2 <= decay * r1
        if ok:
            boxes.append({"s": s, "e": e - 1, "lo": lo, "hi": hi, "open": e >= n, "break_i": e if e < n else None})
            s = e
        else:
            s += 1
    return boxes


def _box_objs(c: Ctx, type_: str, boxes: list[dict], name: str) -> list[dict]:
    return [obj(type_, "box", c.t(x["s"]), c.t(x["e"]), x["lo"], x["hi"], name + (" (идёт)" if x["open"] else ""), None, x["e"] - x["s"] + 1,
                open=x["open"], break_i=x["break_i"]) for x in boxes]


def det_range(c: Ctx, p: dict) -> list[dict]:
    return _box_objs(c, "range", find_boxes(c, p["min_bars"], p["max_height_atr"], max_er=p["max_er"]), "проторговка")


def det_consolidation(c: Ctx, p: dict) -> list[dict]:
    return _box_objs(c, "consolidation", find_boxes(c, p["min_bars"], p["max_height_atr"], decay=p["decay"]), "консолидация")


# ------------------------------------------------------------------------------------------ наклонки, пробои


def det_trendline(c: Ctx, p: dict) -> list[dict]:
    hi, lo = c.pivots(p["k"])
    s = c.since(p["lookback"])
    b, out = c.bars, []
    for idx, key, falling in ((hi, "h", True), (lo, "l", False)):
        idx = [i for i in idx if i >= s]
        for a, z in zip(idx, idx[1:]):
            if z - a < p["min_span"]:
                continue
            pa, pz = b[a][key], b[z][key]
            if (falling and pz >= pa) or (not falling and pz <= pa):
                continue
            slope = (pz - pa) / (z - a)
            line = lambda j: pa + slope * (j - a)  # noqa: E731
            sgn = 1 if falling else -1  # за линией: для сопротивления - выше, для поддержки - ниже
            if any(sgn * (b[j]["c"] - line(j)) > p["tol_atr"] * c.atr[j] for j in range(a + 1, z)):
                continue  # между опорами закрытия за линией - линия «грязная»
            end, touches = c.n - 1, 2
            for j in range(z + 1, c.n):
                if sgn * (b[j]["c"] - line(j)) > p["tol_atr"] * c.atr[j]:
                    end = j
                    break
                if abs(b[j][key] - line(j)) <= p["tol_atr"] * c.atr[j] and j - z > 2:
                    touches += 1
            if touches < p["min_touches"] or end - z < p["min_span"] // 2:
                continue
            broken = end < c.n - 1 or sgn * (b[-1]["c"] - line(c.n - 1)) > p["tol_atr"] * c.atr[-1]
            label = ("сопротивление" if falling else "поддержка") + (" (пробита)" if broken else "")
            out.append(obj("trendline", "line", c.t(a), c.t(end), pa, line(end), label, "short" if falling else "long", touches,
                           slope_per_bar=slope, a=a, broken_i=end if broken else None))
    out.sort(key=lambda o: -o["t2"])
    return out[:int(p["max_items"])]


def det_breakout(c: Ctx, p: dict) -> list[dict]:
    b, out, used = c.bars, [], set()

    def add(j: int, up: bool, lvl: float, what: str) -> None:
        if j in used:
            return
        body = abs(b[j]["c"] - b[j]["o"])
        if body < p["min_body_atr"] * c.atr[j]:
            return
        used.add(j)
        out.append(obj("breakout", "point", c.t(j), c.t(j), b[j]["c"], b[j]["c"], f"пробой {what} {'вверх' if up else 'вниз'}", "long" if up else "short", 1,
                       level=lvl, bar=j))

    for box in c.out.get("range", []) + c.out.get("consolidation", []):
        bi = (box.get("meta") or {}).get("break_i")
        if bi is None:
            continue
        for j in range(bi, min(c.n, bi + 4)):  # закрытие за границей - на свече разрыва или следом
            if b[j]["c"] > box["p2"] + p["min_break_atr"] * c.atr[j]:
                add(j, True, box["p2"], "проторговки" if box["type"] == "range" else "консолидации")
                break
            if b[j]["c"] < box["p1"] - p["min_break_atr"] * c.atr[j]:
                add(j, False, box["p1"], "проторговки" if box["type"] == "range" else "консолидации")
                break
    for z in c.out.get("zone", []) + c.out.get("level", []):
        lo_p, hi_p = (z["p1"], z["p2"]) if z["kind"] == "box" else (z["p1"], z["p1"])
        start = max(1, _index_at(c, z["t1"]) + 1)
        for j in range(c.n - 1, start - 1, -1):  # последнее пересечение
            tol = p["min_break_atr"] * c.atr[j]
            if b[j - 1]["c"] <= hi_p < b[j]["c"] - tol:
                add(j, True, hi_p, "зоны" if z["kind"] == "box" else "уровня")
                break
            if b[j - 1]["c"] >= lo_p > b[j]["c"] + tol:
                add(j, False, lo_p, "зоны" if z["kind"] == "box" else "уровня")
                break
    for tl in c.out.get("trendline", []):
        j = (tl.get("meta") or {}).get("broken_i")
        if j is not None and j < c.n:
            add(j, tl["side"] == "short", tl["p2"], "наклонки")
    return out


def det_zone_exit(c: Ctx, p: dict) -> list[dict]:
    b, out, hold = c.bars, [], int(p["hold_bars"])
    for br in c.out.get("breakout", []):
        j, lvl, up = br["meta"]["bar"], br["meta"]["level"], br["side"] == "long"
        if j + hold >= c.n:
            continue
        if all((b[k]["c"] > lvl) if up else (b[k]["c"] < lvl) for k in range(j + 1, j + hold + 1)):
            k = j + hold
            out.append(obj("zone_exit", "point", c.t(k), c.t(k), b[k]["c"], b[k]["c"], "выход из зоны " + ("вверх" if up else "вниз"), br["side"], 1))
    return out


def _index_at(c: Ctx, t: int) -> int:
    lo, hi = 0, c.n - 1
    while lo < hi:
        m = (lo + hi + 1) // 2
        if c.bars[m]["t"] <= t:
            lo = m
        else:
            hi = m - 1
    return lo


# ------------------------------------------------------------------------------------------ разворот, объём


def det_reversal(c: Ctx, p: dict) -> list[dict]:
    b, out = c.bars, []
    supports, resists = [], []
    for z in c.out.get("zone", []) + c.out.get("level", []) + [x for x in c.out.get("liquidity", []) if x["kind"] == "hline"]:
        lo_p, hi_p = (z["p1"], z["p2"]) if z["kind"] == "box" else (z["p1"], z["p1"])
        supports.append((z["t1"], lo_p, hi_p))
        resists.append((z["t1"], lo_p, hi_p))
    for j in range(1, c.n):
        x, rng = b[j], b[j]["h"] - b[j]["l"]
        if rng <= 0:
            continue
        near = p["near_atr"] * c.atr[j]
        lower, upper = min(x["o"], x["c"]) - x["l"], x["h"] - max(x["o"], x["c"])
        prev = b[j - 1]
        bull = (lower >= p["wick_frac"] * rng and x["c"] >= x["l"] + 0.5 * rng) or \
               (x["c"] > x["o"] and prev["c"] < prev["o"] and x["c"] >= prev["o"] and x["o"] <= prev["c"])
        bear = (upper >= p["wick_frac"] * rng and x["c"] <= x["h"] - 0.5 * rng) or \
               (x["c"] < x["o"] and prev["c"] > prev["o"] and x["c"] <= prev["o"] and x["o"] >= prev["c"])
        if bull and any(t1 < x["t"] and lo_p - near <= x["l"] <= hi_p + near for t1, lo_p, hi_p in supports):
            out.append(obj("reversal", "point", x["t"], x["t"], x["l"], x["l"], "разворот вверх", "long", 1))
        elif bear and any(t1 < x["t"] and lo_p - near <= x["h"] <= hi_p + near for t1, lo_p, hi_p in resists):
            out.append(obj("reversal", "point", x["t"], x["t"], x["h"], x["h"], "разворот вниз", "short", 1))
    return out


def det_volume(c: Ctx, p: dict) -> list[dict]:
    b, out = c.bars, []
    for j in range(20, c.n):
        sma = sum(x["v"] for x in b[j - 20:j]) / 20
        if sma > 0 and b[j]["v"] >= p["spike"] * sma:
            up = b[j]["c"] >= b[j]["o"]
            out.append(obj("volume", "point", b[j]["t"], b[j]["t"], b[j]["h"] if up else b[j]["l"], b[j]["h"] if up else b[j]["l"],
                           f"всплеск объёма ×{b[j]['v'] / sma:.1f}", "long" if up else "short", b[j]["v"] / sma))
    w = max(2, int(p["window"]))
    if all(x.get("bv") is not None for x in b):
        for s in range(0, c.n - w + 1, w):
            win = b[s:s + w]
            vol = sum(x["v"] for x in win)
            if vol <= 0:
                continue
            share = sum(x["bv"] for x in win) / vol
            e = s + w - 1
            if share <= p["buy_max"] and b[e]["c"] < b[s]["o"]:
                out.append(obj("volume", "point", b[e]["t"], b[e]["t"], b[e]["l"], b[e]["l"], f"нет покупок ({share:.0%})", "short", 1 - share))
            elif share >= 1 - p["buy_max"] and b[e]["c"] > b[s]["o"]:
                out.append(obj("volume", "point", b[e]["t"], b[e]["t"], b[e]["h"], b[e]["h"], f"нет продаж ({1 - share:.0%})", "long", share))
    return out


# ------------------------------------------------------------------------------------------ запуск

DETECTORS: dict[str, Callable[[Ctx, dict], list[dict]]] = {  # порядок важен: пробой опирается на коридоры, зоны и наклонки
    "high": det_high, "low": det_low, "trend": det_trend, "level": det_level, "zone": det_zone, "liquidity": det_liquidity, "gap": det_gap,
    "range": det_range, "consolidation": det_consolidation, "trendline": det_trendline, "breakout": det_breakout, "zone_exit": det_zone_exit,
    "reversal": det_reversal, "volume": det_volume,
}


def params_for(overrides: dict[str, dict] | None) -> dict[str, dict[str, float]]:
    """Параметры всех детекторов: умолчания + переопределения стратегии (неизвестные ключи отбрасываются)."""
    out = {k: dict(v) for k, v in DEFAULT_PARAMS.items()}
    for t, ps in (overrides or {}).items():
        if t in out and isinstance(ps, dict):
            out[t].update({k: float(v) for k, v in ps.items() if k in out[t] and isinstance(v, (int, float)) and not isinstance(v, bool)})
    return out


def run(bars: list[dict], iv: int, enabled: list[str] | None = None, overrides: dict[str, dict] | None = None) -> tuple[list[dict], Ctx]:
    """Все детекторы по свечам; вернуть объекты только включённых понятий (зависимости считаются всегда). -> (объекты, контекст)."""
    if len(bars) < 30:
        return [], Ctx(bars, iv)
    c = Ctx(bars, iv)
    ps = params_for(overrides)
    for name, fn in DETECTORS.items():
        c.out[name] = fn(c, ps[name])
    want = set(enabled) if enabled is not None else set(DETECTORS)
    return [o for name, objs in c.out.items() if name in want for o in objs], c
