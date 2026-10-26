"""Хранилище полигона на ПК: уроки, стратегии (с историей версий для отката), свои понятия словаря.

Папка - %APPDATA%\\TradeAI-Monitor\\lab (переживает обновление .exe). Файлы JSON; запись атомарная (через временный файл).
Уроки дополнительно отправляются на сервер (remote.py), чтобы их видели разработчик и другие трейдеры; локальная копия - всегда."""

import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

from tradeai_client.lab import detect, setups, vocab

LESSON_MAX_BARS = 2500
_SLUG = re.compile(r"[^0-9A-Za-zА-Яа-яЁё _-]")


class StoreError(Exception):
    """Запись/чтение не удалось (текст показывается пользователю)."""


def lab_dir() -> Path:
    from tradeai_client.settings import user_dir
    d = Path(os.environ.get("TRADEAI_LAB_DIR") or (user_dir() / "lab"))
    for sub in ("lessons", "strategies"):
        (d / sub).mkdir(parents=True, exist_ok=True)
    return d


def _write(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as exc:
        raise StoreError(f"Не удалось сохранить {path.name}: {exc}") from exc


def _read(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except (OSError, json.JSONDecodeError) as exc:
        raise StoreError(f"Файл {path.name} не читается: {exc}") from exc


# ------------------------------------------------------------------------------------------ словарь


def load_vocab(root: Path | None = None) -> list[vocab.Term]:
    return vocab.merge(_read((root or lab_dir()) / "vocab.json", []) or [])


def add_term(name: str, kind: str, color: str = "#c9d1d9", hint: str = "", root: Path | None = None) -> vocab.Term:
    root = root or lab_dir()
    term = vocab.custom_term(name, kind, color, hint)
    custom = _read(root / "vocab.json", []) or []
    if any(c.get("id") == term.id for c in custom) or term.id in vocab.BUILTIN_IDS:
        raise vocab.VocabError(f"Понятие «{name}» уже есть")
    custom.append(term.to_dict())
    _write(root / "vocab.json", custom)
    return term


# ------------------------------------------------------------------------------------------ уроки


def new_id() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:6]


def clean_lesson(raw: dict) -> dict[str, Any]:
    """Проверка и нормализация урока из окна полигона. Свечи - массивы [t,o,h,l,c,v,bv] или словари; лишнее отбрасывается."""
    if not isinstance(raw, dict):
        raise StoreError("Урок: ожидается объект")
    sym, tf = str(raw.get("symbol") or "").upper(), str(raw.get("tf") or "")
    if not re.fullmatch(r"[A-Z0-9]{2,24}", sym) or tf not in detect_iv():
        raise StoreError("Урок: неверная монета или таймфрейм")
    bars = [to_bar(b) for b in (raw.get("bars") or [])][-LESSON_MAX_BARS:]
    if len(bars) < 30:
        raise StoreError("Урок: мало свечей (нужно хотя бы 30)")
    drawings = []
    for d in raw.get("drawings") or []:
        if not isinstance(d, dict) or d.get("tool") not in ("trend", "hline", "ray", "rect", "point", "long", "short") or not isinstance(d.get("p1"), dict):
            continue
        keep = {k: d[k] for k in ("id", "tool", "term", "note", "p1", "p2", "color", "width", "opacity", "sl", "tp", "entry") if k in d}
        keep["note"] = str(keep.get("note") or "")[:1000]
        drawings.append(keep)
    return {
        "id": str(raw.get("id") or new_id()), "version": 1, "created": raw.get("created") or int(time.time()), "updated": int(time.time()),
        "author": str(raw.get("author") or "")[:40], "title": str(raw.get("title") or f"{sym} {tf}")[:120], "symbol": sym, "tf": tf,
        "iv": detect_iv()[tf], "bars": bars, "drawings": drawings, "notes": str(raw.get("notes") or "")[:5000],
        "view": raw.get("view") if isinstance(raw.get("view"), dict) else {"from_t": bars[0]["t"], "to_t": bars[-1]["t"]},
        "complete_types": [str(x) for x in (raw.get("complete_types") or [])],
        "trades": raw.get("trades") if isinstance(raw.get("trades"), list) else [], "strategy": str(raw.get("strategy") or ""),
    }


def detect_iv() -> dict[str, int]:
    from tradeai_client.lab.market import IV
    return IV


def to_bar(b: Any) -> dict[str, Any]:
    if isinstance(b, dict):
        return {"t": int(b["t"]), "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]), "c": float(b["c"]), "v": float(b.get("v") or 0),
                "bv": None if b.get("bv") is None else float(b["bv"])}
    return {"t": int(b[0]), "o": float(b[1]), "h": float(b[2]), "l": float(b[3]), "c": float(b[4]), "v": float(b[5]), "bv": float(b[6]) if len(b) > 6 and b[6] is not None else None}


def save_lesson(raw: dict, root: Path | None = None) -> dict[str, Any]:
    lesson = clean_lesson(raw)
    _write((root or lab_dir()) / "lessons" / f"{lesson['id']}.json", lesson)
    return lesson


def list_lessons(root: Path | None = None) -> list[dict[str, Any]]:
    out = []
    for f in sorted(((root or lab_dir()) / "lessons").glob("*.json"), reverse=True):
        try:
            ls = _read(f, None)
        except StoreError:
            continue
        if not ls:
            continue
        terms: dict[str, int] = {}
        for d in ls.get("drawings", []):
            if d.get("term"):
                terms[d["term"]] = terms.get(d["term"], 0) + 1
        out.append({"id": ls["id"], "title": ls.get("title"), "symbol": ls["symbol"], "tf": ls["tf"], "author": ls.get("author"), "updated": ls.get("updated"),
                    "from_t": ls["view"]["from_t"], "to_t": ls["view"]["to_t"], "drawings": len(ls.get("drawings", [])), "terms": terms,
                    "notes": (ls.get("notes") or "")[:200]})
    return out


def load_lesson(lesson_id: str, root: Path | None = None) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9A-Za-z_-]{6,64}", lesson_id or ""):
        raise StoreError("неверный id урока")
    ls = _read((root or lab_dir()) / "lessons" / f"{lesson_id}.json", None)
    if ls is None:
        raise StoreError(f"урок {lesson_id} не найден")
    return ls


def load_lessons(root: Path | None = None) -> list[dict[str, Any]]:
    return [load_lesson(x["id"], root) for x in list_lessons(root)]


def delete_lesson(lesson_id: str, root: Path | None = None) -> None:
    load_lesson(lesson_id, root)
    ((root or lab_dir()) / "lessons" / f"{lesson_id}.json").unlink(missing_ok=True)


# ------------------------------------------------------------------------------------------ стратегии

DEFAULT_ENABLED = [t.id for t in vocab.BUILTIN if t.detector and t.id not in ("high", "low")]


def default_strategy(name: str = "Моя стратегия") -> dict[str, Any]:
    return {"name": name, "created": int(time.time()), "updated": int(time.time()), "enabled": list(DEFAULT_ENABLED), "params": {},
            "setup": dict(setups.DEFAULT_SETUP), "setups": list(setups.SETUPS), "notes": "", "history": []}


def _slug(name: str) -> str:
    s = _SLUG.sub("", (name or "").strip())[:60].strip()
    if not s:
        raise StoreError("Название стратегии: буквы, цифры, пробел, - и _")
    return s


def list_strategies(root: Path | None = None) -> list[dict[str, Any]]:
    root = root or lab_dir()
    items = [_read(f, None) for f in sorted((root / "strategies").glob("*.json"))]
    items = [x for x in items if x]
    if not items:
        st = default_strategy()
        save_strategy(st, "создана по умолчанию", root)
        items = [st]
    return items


def get_strategy(name: str, root: Path | None = None) -> dict[str, Any]:
    st = _read((root or lab_dir()) / "strategies" / f"{_slug(name)}.json", None)
    if st is None:
        raise StoreError(f"стратегия «{name}» не найдена")
    return st


def save_strategy(st: dict[str, Any], why: str = "", root: Path | None = None) -> dict[str, Any]:
    """Сохранить стратегию; прежнее состояние параметров/понятий уходит в history (откат)."""
    root = root or lab_dir()
    name = _slug(st.get("name", ""))
    path = root / "strategies" / f"{name}.json"
    old = _read(path, None)
    clean = {**default_strategy(name), **{k: st[k] for k in ("enabled", "params", "setup", "setups", "notes", "created") if k in st}}
    clean["name"], clean["updated"] = name, int(time.time())
    clean["enabled"] = [t for t in clean["enabled"] if isinstance(t, str)]
    clean["params"] = {t: v for t, v in (clean["params"] or {}).items() if t in detect.DEFAULT_PARAMS and isinstance(v, dict)}
    hist = list((old or {}).get("history") or [])
    if old and (old.get("params") != clean["params"] or old.get("enabled") != clean["enabled"] or old.get("setup") != clean["setup"]):
        hist.append({"at": int(time.time()), "why": why or "изменено", "enabled": old.get("enabled"), "params": old.get("params"), "setup": old.get("setup")})
    clean["history"] = hist[-50:]
    _write(path, clean)
    return clean


def rollback_strategy(name: str, steps: int = 1, root: Path | None = None) -> dict[str, Any]:
    st = get_strategy(name, root)
    hist = st.get("history") or []
    if len(hist) < steps:
        raise StoreError("Откатывать некуда: истории изменений меньше")
    prev = hist[-steps]
    st = {**st, "enabled": prev["enabled"], "params": prev["params"], "setup": prev.get("setup") or st["setup"], "history": hist[:-steps]}
    _write((root or lab_dir()) / "strategies" / f"{_slug(name)}.json", {**st, "updated": int(time.time())})
    return st


def active_name(root: Path | None = None) -> str:
    got = _read((root or lab_dir()) / "active.json", {}) or {}
    names = [s["name"] for s in list_strategies(root)]
    return got.get("name") if got.get("name") in names else names[0]


def set_active(name: str, root: Path | None = None) -> None:
    get_strategy(name, root)
    _write((root or lab_dir()) / "active.json", {"name": _slug(name)})
