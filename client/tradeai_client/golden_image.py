"""Картинка графика сделки (PNG) для «Базы эталонных сетапов»: рисуется Pillow из того же `spec`, что и окно графика, БЕЗ окна и WebView.
Её клиент отправляет на сервер вместе со звёздочкой (api/favorites.py кладёт в golden_set/<id>.png) - позже такие картинки пойдут в few-shot примеры ИИ.
Содержимое: свечи (в т.ч. после сигнала), зоны (FVG), горизонтальные уровни (вход / SL / TP / ...) с подписями, маркеры событий, заголовок сделки."""

import io
import re
from typing import Any

from PIL import Image, ImageDraw, ImageFont

W, H = 1400, 800
LEFT, RIGHT, TOP, BOTTOM = 16, 96, 44, 36
BG, GRID, TEXT, MUTED = (14, 17, 23), (28, 33, 41), (230, 233, 239), (139, 147, 163)
UP, DOWN = (38, 166, 154), (239, 83, 80)
_RGBA = re.compile(r"rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)")


def parse_color(value: str, default: tuple[int, ...] = (150, 150, 150)) -> tuple[int, int, int, int]:
    """'#rrggbb' / '#rgb' / 'rgba(r,g,b,a)' -> (r, g, b, a 0..255). Незнакомое значение - серый (картинка не должна падать из-за цвета)."""
    v = (value or "").strip()
    m = _RGBA.match(v)
    if m:
        a = 1.0 if m.group(4) is None else float(m.group(4))
        return int(m.group(1)), int(m.group(2)), int(m.group(3)), int(max(0.0, min(1.0, a)) * 255)
    if v.startswith("#") and len(v) in (4, 7):
        h = v[1:] if len(v) == 7 else "".join(c * 2 for c in v[1:])
        try:
            return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 255
        except ValueError:
            pass
    return (*default[:3], 255)


def _font(size: int) -> ImageFont.ImageFont | ImageFont.FreeTypeFont:
    for name in ("segoeui.ttf", "arial.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default(size)


def render_png(spec: dict[str, Any], *, width: int = W, height: int = H) -> bytes:
    """spec (chart_spec.build_chart_spec) -> PNG. Видимое окно - spec['visible'] (мс UTC); свечи вне окна не рисуются."""
    t0, t1 = spec["visible"]
    bars = [b for b in spec["bars"] if t0 <= b["t"] <= t1] or spec["bars"]
    if not bars:
        raise ValueError("нет свечей для картинки")
    iv = max(1, int(spec["interval_ms"]))
    x_min, x_max = bars[0]["t"], max(bars[-1]["t"], t1)
    levels = [h["price"] for h in spec["hlines"]]
    lo = min([b["l"] for b in bars] + [p for p in levels if p is not None])
    hi = max([b["h"] for b in bars] + [p for p in levels if p is not None])
    pad = (hi - lo) * 0.06 or max(abs(hi) * 0.001, 1e-9)
    lo, hi = lo - pad, hi + pad
    pw, ph = width - LEFT - RIGHT, height - TOP - BOTTOM
    px = lambda t: LEFT + (t - x_min) / max(1, (x_max - x_min + iv)) * pw  # noqa: E731
    py = lambda p: TOP + (hi - p) / (hi - lo) * ph  # noqa: E731
    digits = int(spec.get("digits", 2))

    img = Image.new("RGBA", (width, height), BG + (255,))
    over = Image.new("RGBA", (width, height), (0, 0, 0, 0))  # полупрозрачные зоны рисуются отдельным слоем
    d, od = ImageDraw.Draw(img), ImageDraw.Draw(over)
    f_small, f_mid, f_title = _font(13), _font(14), _font(20)

    for i in range(1, 8):  # сетка и шкала цены
        price = lo + (hi - lo) * i / 8
        y = py(price)
        d.line([(LEFT, y), (width - RIGHT, y)], fill=GRID, width=1)
        d.text((width - RIGHT + 8, y - 8), f"{price:.{digits}f}", fill=MUTED, font=f_small)

    for box in spec["boxes"]:
        x1, x2 = sorted((px(box["t1"]), px(box["t2"])))
        y1, y2 = sorted((py(box["p1"]), py(box["p2"])))
        od.rectangle([x1, y1, max(x2, x1 + 2), max(y2, y1 + 1)], fill=parse_color(box["fill"], (200, 200, 80)), outline=parse_color(box["color"]), width=1)

    cw = max(1.0, min(14.0, pw / max(1, len(bars)) * 0.7))
    for b in bars:
        x, color = px(b["t"] + iv / 2), UP if b["c"] >= b["o"] else DOWN
        d.line([(x, py(b["h"])), (x, py(b["l"]))], fill=color, width=1)
        y_open, y_close = py(b["o"]), py(b["c"])
        d.rectangle([x - cw / 2, min(y_open, y_close), x + cw / 2, max(max(y_open, y_close), min(y_open, y_close) + 1)], fill=color)

    for h in spec["hlines"]:
        y, color = py(h["price"]), parse_color(h["color"])
        step = 7 if h.get("style") == "dashed" else 3 if h.get("style") == "dotted" else None
        if step:
            x = LEFT
            while x < width - RIGHT:
                d.line([(x, y), (min(x + step, width - RIGHT), y)], fill=color, width=2)
                x += step * 2
        else:
            d.line([(LEFT, y), (width - RIGHT, y)], fill=color, width=2)
        label = f"{h['title']} {h['price']:.{digits}f}"
        tw = d.textlength(label, font=f_small)
        d.rectangle([width - RIGHT + 2, y - 9, width - RIGHT + 8 + tw, y + 9], fill=color)
        d.text((width - RIGHT + 5, y - 8), label, fill=(10, 12, 16), font=f_small)

    for m in spec["trade_markers"] + spec["setup_markers"]:
        if not (x_min <= m["t"] <= x_max):
            continue
        bar = min(bars, key=lambda b: abs(b["t"] - m["t"]))
        above = m["position"] == "above"
        y = py(bar["h"]) - 12 if above else py(bar["l"]) + 12
        x, color = px(bar["t"] + iv / 2), parse_color(m["color"])
        d.ellipse([x - 5, y - 5, x + 5, y + 5], fill=color)
        d.text((x + 8, y - 8), str(m["text"]), fill=color, font=f_mid)

    for v in spec["vlines"]:
        if x_min <= v["t"] <= x_max:
            x = px(v["t"])
            d.line([(x, TOP), (x, height - BOTTOM)], fill=parse_color(v["color"]), width=1)
            d.text((x + 3, TOP + 2), str(v["text"]), fill=parse_color(v["color"]), font=f_mid)

    img = Image.alpha_composite(img, over)
    d = ImageDraw.Draw(img)
    d.text((LEFT, 10), str(spec["title"]), fill=TEXT, font=f_title)
    d.rectangle([LEFT, TOP, width - RIGHT, height - BOTTOM], outline=GRID, width=1)
    out = io.BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
