"""Свечной график на canvas Flet (Live-Dashboard, 1.8.0): свечи + формирующаяся свеча, разметка стратегии (уровни, линии, зоны, полосы, маркеры)
и уровни позиции бота (вход / SL / TP). Без внешних пакетов и WebView - работает и в .exe, и в веб-версии."""

from typing import Any

import flet as ft
import flet.canvas as cv

from tradeai_client import fmt
from tradeai_client.ui import widgets as w

SIDE_COLOR = {"long": w.GREEN, "short": w.RED, None: w.ACCENT}
SIGNAL_COLOR = "#f2c94c"
RIGHT, BOTTOM, TOP, LEFT = 74.0, 20.0, 8.0, 4.0


def _paint(color: str, width: float = 1.0, *, fill: bool = False, opacity: float = 1.0) -> ft.Paint:
    c = ft.Colors.with_opacity(opacity, color) if opacity < 1 else color
    return ft.Paint(color=c, stroke_width=width, style=ft.PaintingStyle.FILL if fill else ft.PaintingStyle.STROKE)


def candle_chart(candles: list[dict], *, interval_ms: int, geometry: list[dict] = (), levels: list[dict] = (), forming: dict | None = None,
                 width: float = 900, height: float = 320) -> ft.Control:
    """candles - закрытые свечи (open_time, open, high, low, close); forming - текущая незакрытая (рисуется бледнее);
    geometry - разметка стратегии (playground/watch.py: line | zone | marker | series); levels - горизонтальные уровни позиций [{price, label, color}]."""
    bars = list(candles) + ([forming] if forming else [])
    if len(bars) < 2:
        return ft.Container(content=w.label("Нет свечей"), width=width, height=height, alignment=ft.Alignment.CENTER)
    t0, t_last = bars[0]["open_time"], bars[-1]["open_time"]
    n = len(bars) + 4  # запас справа под подписи
    pw, ph = width - LEFT - RIGHT, height - TOP - BOTTOM
    step = pw / n
    lo = min(b["low"] for b in bars)
    hi = max(b["high"] for b in bars)
    span = hi - lo or hi * 0.001 or 1.0
    for lv in levels:  # уровни позиции бота видны всегда
        lo, hi = min(lo, lv["price"]), max(hi, lv["price"])
    span = hi - lo or span
    for g in geometry:  # разметка стратегии рядом с ценой расширяет шкалу; далёкая (больше половины диапазона) - нет, чтобы свечи не сплющило: она подписывается у края
        for p in (g.get("p1"), g.get("p2")) if g.get("type") != "series" else ():
            if p is not None and lo - span * 0.5 <= p <= hi + span * 0.5:
                lo, hi = min(lo, p), max(hi, p)
    pad = (hi - lo) * 0.04 or 1.0
    lo, hi = lo - pad, hi + pad

    def x(t: float) -> float:
        return LEFT + ((t - t0) / interval_ms + 0.5) * step

    def y(p: float) -> float:
        return TOP + (hi - p) / (hi - lo) * ph

    x_min, x_max = LEFT, LEFT + pw
    shapes: list = []
    grid = _paint(w.BORDER, 1)
    tick = ft.TextStyle(size=10, color=w.MUTED)
    for i in range(5):
        p = lo + (hi - lo) * i / 4
        shapes += [cv.Line(LEFT, y(p), x_max, y(p), paint=_paint(w.BORDER, 1, opacity=0.5)), cv.Text(x_max + 6, y(p) - 7, fmt.fmt_price(p), style=tick)]
    for i in range(0, len(bars), max(1, len(bars) // 5)):
        shapes.append(cv.Text(x(bars[i]["open_time"]) - 14, height - BOTTOM + 4, fmt.fmt_ms_local(bars[i]["open_time"], "%H:%M"), style=tick))
    shapes.append(cv.Line(x_max, TOP, x_max, TOP + ph, paint=grid))

    for g in geometry:  # зоны - под свечами
        if g.get("type") == "zone":
            a, b = max(x(g["t1"]), x_min), min(x(g["t2"]), x_max)
            if b > a:
                col = SIDE_COLOR.get(g.get("side"), w.ACCENT)
                top, bot = y(max(g["p1"], g["p2"])), y(min(g["p1"], g["p2"]))
                shapes += [cv.Rect(a, top, b - a, max(1.0, bot - top), paint=_paint(col, fill=True, opacity=0.12)),
                           cv.Text(a + 3, top + 2, g.get("label", ""), style=ft.TextStyle(size=10, color=col))]

    body = max(1.0, step * 0.62)
    for i, b in enumerate(bars):
        live = forming is not None and i == len(bars) - 1
        up = b["close"] >= b["open"]
        col = w.GREEN if up else w.RED
        cx = x(b["open_time"])
        op = 0.45 if live else 1.0
        shapes.append(cv.Line(cx, y(b["high"]), cx, y(b["low"]), paint=_paint(col, 1, opacity=op)))
        top, bot = y(max(b["open"], b["close"])), y(min(b["open"], b["close"]))
        shapes.append(cv.Rect(cx - body / 2, top, body, max(1.0, bot - top), paint=_paint(col, fill=True, opacity=op)))

    off = {"up": 0, "down": 0}
    for g in geometry:
        kind, col = g.get("type"), (SIGNAL_COLOR if g.get("signal") else SIDE_COLOR.get(g.get("side"), w.ACCENT))
        if kind == "line":
            t1, t2, p1, p2 = g["t1"], g["t2"], g["p1"], g["p2"]
            if t2 == t1:
                continue
            k = (p2 - p1) / (t2 - t1)
            a, b = max(t1, t0), min(t2, t_last + interval_ms)
            if b <= a:
                continue
            pa, pb = p1 + k * (a - t1), p1 + k * (b - t1)
            if not (lo <= pb <= hi):  # за краем шкалы: подпись со стрелкой у верхнего / нижнего края (несколько - друг под другом)
                up = pb > hi
                k_off = off["up" if up else "down"]
                off["up" if up else "down"] += 1
                shapes.append(cv.Text(LEFT + 4, TOP + 2 + 13 * k_off if up else TOP + ph - 14 - 13 * k_off, f"{'↑' if up else '↓'} {g.get('label', '')} {fmt.fmt_price(pb)}",
                                      style=ft.TextStyle(size=10, color=col)))
                continue
            shapes += [cv.Line(x(a), y(pa), x(b), y(pb), paint=_paint(col, 2 if g.get("signal") else 1.5)),
                       cv.Text(min(x(b), x_max - 90) + 3, y(pb) - 13, g.get("label", ""), style=ft.TextStyle(size=10, color=col))]
        elif kind == "series":
            pts = [(x(t), y(p)) for t, p in g.get("points", []) if t >= t0]
            if len(pts) >= 2:
                shapes.append(cv.Path([cv.Path.MoveTo(*pts[0]), *(cv.Path.LineTo(*p) for p in pts[1:])], paint=_paint(w.AMBER if g["label"] == "VWAP" else "#b07cf0", 1.5)))
                shapes.append(cv.Text(min(pts[-1][0], x_max - 40) + 3, pts[-1][1] - 13, g["label"], style=ft.TextStyle(size=10, color=w.AMBER)))
        elif kind == "marker" and g["t1"] >= t0:
            cx, cy = x(g["t1"]), y(g["p1"])
            shapes += [cv.Circle(cx, cy, 4, paint=_paint(col, fill=True)), cv.Text(cx + 5, cy - 14, g.get("label", ""), style=ft.TextStyle(size=10, color=col))]

    for lv in levels:  # позиция бота: вход / SL / TP через весь график, подпись на шкале
        yy = y(lv["price"])
        if TOP <= yy <= TOP + ph:
            shapes += [cv.Line(LEFT, yy, x_max, yy, paint=_paint(lv["color"], 1.5)),
                       cv.Rect(x_max + 1, yy - 8, RIGHT - 2, 16, paint=_paint(lv["color"], fill=True)),
                       cv.Text(x_max + 4, yy - 7, f"{lv['label']} {fmt.fmt_price(lv['price'])}", style=ft.TextStyle(size=10, color="#0e1117", weight=ft.FontWeight.W_700))]
    last = bars[-1]["close"]
    shapes += [cv.Line(LEFT, y(last), x_max, y(last), paint=_paint(w.TEXT, 1, opacity=0.35)),
               cv.Rect(x_max + 1, y(last) - 8, RIGHT - 2, 16, paint=_paint(w.TEXT, fill=True)),
               cv.Text(x_max + 4, y(last) - 7, fmt.fmt_price(last), style=ft.TextStyle(size=10, color="#0e1117", weight=ft.FontWeight.W_700))]
    return cv.Canvas(shapes, width=width, height=height)


def position_levels(positions: list[dict[str, Any]]) -> list[dict]:
    """Позиции бота (live_state) -> горизонтальные уровни графика."""
    out = []
    for p in positions:
        entry = p.get("entry_price") if p.get("entry_price") is not None else p.get("planned_entry")
        for price, label, color in ((entry, "вход", w.ACCENT), (p.get("sl"), "SL", w.RED), (p.get("tp"), "TP", w.GREEN)):
            if price is not None:
                out.append({"price": float(price), "label": label, "color": color})
    return out
