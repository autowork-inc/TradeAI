"""Рисунки пользователя на графике: локальный JSON `drawings.json` рядом с .exe (как settings.json), ключ - тикер. Переживают перезапуск и смену таймфрейма:
координаты - время (секунды UTC) и цена, не пиксели и не номера свечей. Файл правит и человек: битые записи отбрасываются, а не роняют график."""

import json
import logging
import math
import os
import re
from pathlib import Path
from typing import Any

log = logging.getLogger("tradeai_client.drawings")
FILE_NAME = "drawings.json"
TYPES = ("trend", "hline", "ray", "rect")
MAX_PER_SYMBOL = 500
_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,40}$")


class DrawingsError(Exception):
    """Файл рисунков нельзя прочитать или записать (текст показывается пользователю)."""


def _point(v: Any) -> dict[str, float] | None:
    if not isinstance(v, dict):
        return None
    t, p = v.get("t"), v.get("p")
    if isinstance(t, bool) or isinstance(p, bool) or not isinstance(t, (int, float)) or not isinstance(p, (int, float)):
        return None
    if not (math.isfinite(t) and math.isfinite(p)) or t <= 0:
        return None
    return {"t": float(t), "p": float(p)}


def clean(items: Any) -> list[dict[str, Any]]:
    """Список рисунков -> только корректные записи (тип, точки, цвет #rrggbb, толщина 1..12, прозрачность 0.05..1); лишние поля отбрасываются."""
    if not isinstance(items, list):
        return []
    out: list[dict[str, Any]] = []
    for d in items[:MAX_PER_SYMBOL]:
        if not isinstance(d, dict) or d.get("type") not in TYPES or not isinstance(d.get("id"), str) or not _ID.match(d["id"]):
            continue
        p1, p2 = _point(d.get("p1")), _point(d.get("p2"))
        if p1 is None or (d["type"] != "hline" and p2 is None):
            continue
        color, width, opacity = d.get("color"), d.get("width"), d.get("opacity")
        if not isinstance(color, str) or not _COLOR.match(color) or isinstance(width, bool) or not isinstance(width, (int, float)) or not 1 <= width <= 12:
            continue
        if isinstance(opacity, bool) or not isinstance(opacity, (int, float)) or not 0.05 <= opacity <= 1:
            continue
        item = {"id": d["id"], "type": d["type"], "color": color, "width": int(width), "opacity": float(opacity), "p1": p1}
        if d["type"] != "hline":
            item["p2"] = p2
        out.append(item)
    return out


def _read_all(path: Path) -> dict[str, list]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DrawingsError(f"Файл рисунков {path} нечитаем ({exc}). Переименуйте или удалите его.") from exc
    if not isinstance(data, dict):
        raise DrawingsError(f"Файл рисунков {path} имеет неверный формат")
    return data


def load(path: Path, symbol: str) -> list[dict[str, Any]]:
    return clean(_read_all(path).get(symbol))


def save(path: Path, symbol: str, items: Any) -> int:
    """Записать рисунки монеты (атомарно: оборванная запись не оставит битый файл), остальные монеты не трогаются. -> сколько рисунков сохранено."""
    good = clean(items)
    data = _read_all(path)
    if good:
        data[symbol] = good
    else:
        data.pop(symbol, None)
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as exc:
        raise DrawingsError(f"Рисунки не записаны в {path}: {exc}") from exc
    return len(good)
