"""Хранилище полигона на ПК: уроки, стратегии (с историей версий для отката), свои понятия словаря.

Папка - %APPDATA%\\TradeAI-Monitor\\lab (переживает обновление .exe). Файлы JSON; запись атомарная (через временный файл).
1.9.1 (решение пользователя): у каждого профиля СВОЁ пространство - lab/santa и lab/chr (уроки, словарь, стратегии не пересекаются: видение рынка
SANTA и CHR разное). Поэтому каталог (`root`) - обязательный аргумент всех функций: забыть профиль и записать «в общее» нельзя.
Уроки дополнительно отправляются на сервер (remote.py) в пространство того же профиля; локальная копия - всегда."""

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


def space_of(profile: str) -> str:
    """Имя пространства профиля: SANTA -> santa, CHR -> chr (тот же список и умолчание, что у профилей клиента)."""
    from tradeai_client import profiles
    return profiles.normalize_profile(profile).lower()


def base_dir() -> Path:
    from tradeai_client.settings import user_dir
    return Path(os.environ.get("TRADEAI_LAB_DIR") or (user_dir() / "lab"))


def lab_dir(profile: str) -> Path:
    """Каталог пространства профиля (создаётся при первом обращении)."""
    base = base_dir()
    _migrate_legacy(base)
    d = base / space_of(profile)
    for sub in ("lessons", "strategies"):
        (d / sub).mkdir(parents=True, exist_ok=True)
    return d


def _migrate_legacy(base: Path) -> None:
    """1.9.0 хранил всё в общем lab/: уроки уходят в пространство своего автора (CHR -> chr, иначе santa), стратегии и словарь - в santa."""
    legacy = [p for p in (base / "lessons", base / "strategies", base / "vocab.json", base / "active.json") if p.exists()]
    if not legacy:
        return
    for f in sorted((base / "lessons").glob("*.json")) if (base / "lessons").is_dir() else []:
        try:
            author = (_read(f, {}) or {}).get("author") or ""
        except StoreError:
            author = ""
        dest = base / space_of(author) / "lessons"
        dest.mkdir(parents=True, exist_ok=True)
        os.replace(f, dest / f.name)
    santa = base / space_of("SANTA")
    for f in sorted((base / "strategies").glob("*.json")) if (base / "strategies").is_dir() else []:
        (santa / "strategies").mkdir(parents=True, exist_ok=True)
        os.replace(f, santa / "strategies" / f.name)
    for name in ("vocab.json", "active.json"):
        if (base / name).exists():
            santa.mkdir(parents=True, exist_ok=True)
            os.replace(base / name, santa / name)
    for sub in ("lessons", "strategies"):
        if (base / sub).is_dir() and not any((base / sub).iterdir()):
            (base / sub).rmdir()


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


def load_vocab(root: Path) -> list[vocab.Term]:
    return vocab.merge(_read(root / "vocab.json", []) or [])


def add_term(name: str, kind: str, color: str = "#c9d1d9", hint: str = "", *, root: Path) -> vocab.Term:
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


def save_lesson(raw: dict, root: Path) -> dict[str, Any]:
    lesson = clean_lesson(raw)
    _write(root / "lessons" / f"{lesson['id']}.json", lesson)
    return lesson


def list_lessons(root: Path) -> list[dict[str, Any]]:
    out = []
    for f in sorted((root / "lessons").glob("*.json"), reverse=True):
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


def load_lesson(lesson_id: str, root: Path) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9A-Za-z_-]{6,64}", lesson_id or ""):
        raise StoreError("неверный id урока")
    ls = _read(root / "lessons" / f"{lesson_id}.json", None)
    if ls is None:
        raise StoreError(f"урок {lesson_id} не найден")
    return ls


def load_lessons(root: Path) -> list[dict[str, Any]]:
    return [load_lesson(x["id"], root) for x in list_lessons(root)]


def delete_lesson(lesson_id: str, root: Path) -> None:
    load_lesson(lesson_id, root)
    (root / "lessons" / f"{lesson_id}.json").unlink(missing_ok=True)


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


def list_strategies(root: Path) -> list[dict[str, Any]]:
    items = [_read(f, None) for f in sorted((root / "strategies").glob("*.json"))]
    items = [x for x in items if x]
    if not items:
        st = default_strategy()
        save_strategy(st, "создана по умолчанию", root=root)
        items = [st]
    return items


def get_strategy(name: str, root: Path) -> dict[str, Any]:
    st = _read(root / "strategies" / f"{_slug(name)}.json", None)
    if st is None:
        raise StoreError(f"стратегия «{name}» не найдена")
    return st


def save_strategy(st: dict[str, Any], why: str = "", *, root: Path) -> dict[str, Any]:
    """Сохранить стратегию; прежнее состояние параметров/понятий уходит в history (откат)."""
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


def rollback_strategy(name: str, steps: int = 1, *, root: Path) -> dict[str, Any]:
    st = get_strategy(name, root)
    hist = st.get("history") or []
    if len(hist) < steps:
        raise StoreError("Откатывать некуда: истории изменений меньше")
    prev = hist[-steps]
    st = {**st, "enabled": prev["enabled"], "params": prev["params"], "setup": prev.get("setup") or st["setup"], "history": hist[:-steps]}
    _write(root / "strategies" / f"{_slug(name)}.json", {**st, "updated": int(time.time())})
    return st


def active_name(root: Path) -> str:
    got = _read(root / "active.json", {}) or {}
    names = [s["name"] for s in list_strategies(root)]
    return got.get("name") if got.get("name") in names else names[0]


def set_active(name: str, root: Path) -> None:
    get_strategy(name, root)
    _write(root / "active.json", {"name": _slug(name)})
