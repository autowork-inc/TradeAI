"""`/api/lab/*` - обмен уроками Create Strategy (1.9.0): уроки, свои понятия словаря и стратегии трейдеров хранятся на сервере, чтобы их видели
разработчик (по урокам пишутся новые детекторы) и другие ПК того же профиля. Файлы JSON в LAB_DIR/<santa|chr> (./data/lab, пишет только API);
пространство в пути запроса, профили друг друга не видят.

Это единственное место, кроме избранного, куда API пишет: только в свой каталог, с проверкой формата и размера; торговлю и БД бота не трогает."""

import asyncio
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Annotated, Any, Callable

from fastapi import Body, Depends, FastAPI, HTTPException, Path as PathParam

LESSON_ID = r"^[0-9A-Za-z_-]{6,64}$"
MAX_LESSON_BYTES = 3_000_000
MAX_BARS = 3000
MAX_DRAWINGS = 500
NAME = re.compile(r"^[0-9A-Za-zА-Яа-яЁё _-]{1,60}$")
# 1.9.1 (решение пользователя): у профилей SANTA и CHR своё видение рынка - уроки, словарь и стратегии лежат в LAB_DIR/santa и LAB_DIR/chr и не пересекаются
SPACES = ("santa", "chr")
Space = Annotated[str, PathParam(pattern=r"^(santa|chr)$")]


def _write(path: Path, data: Any) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def check_lesson(lesson: dict, lesson_id: str) -> dict:
    if not isinstance(lesson, dict) or lesson.get("id") != lesson_id:
        raise HTTPException(status_code=422, detail="lesson_id_mismatch")
    if not re.fullmatch(r"[A-Z0-9]{2,24}", str(lesson.get("symbol") or "")) or lesson.get("tf") not in ("1m", "5m", "15m", "1h", "4h", "1d"):
        raise HTTPException(status_code=422, detail="bad_symbol_or_tf")
    bars, drawings = lesson.get("bars"), lesson.get("drawings")
    if not isinstance(bars, list) or not 30 <= len(bars) <= MAX_BARS or not isinstance(drawings, list) or len(drawings) > MAX_DRAWINGS:
        raise HTTPException(status_code=422, detail="bad_bars_or_drawings")
    if len(json.dumps(lesson, ensure_ascii=False).encode("utf-8")) > MAX_LESSON_BYTES:
        raise HTTPException(status_code=413, detail="lesson_too_large")
    return lesson


def meta(lesson: dict) -> dict:
    terms: dict[str, int] = {}
    for d in lesson.get("drawings") or []:
        if isinstance(d, dict) and d.get("term"):
            terms[str(d["term"])] = terms.get(str(d["term"]), 0) + 1
    view = lesson.get("view") or {}
    return {"id": lesson["id"], "title": lesson.get("title"), "symbol": lesson["symbol"], "tf": lesson["tf"], "author": lesson.get("author"),
            "updated": lesson.get("updated"), "from_t": view.get("from_t"), "to_t": view.get("to_t"), "drawings": len(lesson.get("drawings") or []), "terms": terms,
            "notes": str(lesson.get("notes") or "")[:200]}


def install(app: FastAPI, auth: Callable, directory: Path | None) -> None:
    def root(space: str) -> Path:
        if directory is None:
            raise HTTPException(status_code=503, detail="lab_not_configured")
        d = directory / space  # space проверен шаблоном пути: только santa | chr
        for sub in ("lessons", "strategies"):
            (d / sub).mkdir(parents=True, exist_ok=True)
        return d

    @app.get("/api/lab/{space}/lessons", dependencies=[Depends(auth)])
    async def lessons(space: Space) -> dict[str, Any]:
        def scan() -> list[dict]:
            out = []
            for f in sorted((root(space) / "lessons").glob("*.json"), reverse=True):
                try:
                    out.append(meta(_read(f)))
                except (OSError, ValueError, KeyError):
                    continue
            return out
        return {"items": await asyncio.to_thread(scan)}

    @app.get("/api/lab/{space}/lessons/{lesson_id}", dependencies=[Depends(auth)])
    async def lesson(space: Space, lesson_id: Annotated[str, PathParam(pattern=LESSON_ID)]) -> dict[str, Any]:
        path = root(space) / "lessons" / f"{lesson_id}.json"
        if not path.exists():
            raise HTTPException(status_code=404, detail="not_found")
        return await asyncio.to_thread(_read, path)

    @app.put("/api/lab/{space}/lessons/{lesson_id}", dependencies=[Depends(auth)])
    async def put_lesson(space: Space, lesson_id: Annotated[str, PathParam(pattern=LESSON_ID)], body: Annotated[dict, Body()]) -> dict[str, Any]:
        lesson = check_lesson(body, lesson_id)
        lesson["received_at"] = int(time.time())
        await asyncio.to_thread(_write, root(space) / "lessons" / f"{lesson_id}.json", lesson)
        return {"saved": lesson_id}

    @app.delete("/api/lab/{space}/lessons/{lesson_id}", dependencies=[Depends(auth)])
    async def delete_lesson(space: Space, lesson_id: Annotated[str, PathParam(pattern=LESSON_ID)]) -> dict[str, Any]:
        path = root(space) / "lessons" / f"{lesson_id}.json"
        if not path.exists():
            raise HTTPException(status_code=404, detail="not_found")
        path.unlink()
        return {"deleted": lesson_id}

    @app.get("/api/lab/{space}/vocab", dependencies=[Depends(auth)])
    async def vocab(space: Space) -> dict[str, Any]:
        path = root(space) / "vocab.json"
        return {"items": _read(path) if path.exists() else []}

    @app.put("/api/lab/{space}/vocab", dependencies=[Depends(auth)])
    async def put_vocab(space: Space, body: Annotated[dict, Body()]) -> dict[str, Any]:
        items = body.get("items")
        if not isinstance(items, list) or len(items) > 200 or not all(isinstance(x, dict) and NAME.match(str(x.get("name") or "")) for x in items):
            raise HTTPException(status_code=422, detail="bad_vocab")
        await asyncio.to_thread(_write, root(space) / "vocab.json", items)
        return {"saved": len(items)}

    @app.get("/api/lab/{space}/strategies", dependencies=[Depends(auth)])
    async def strategies(space: Space) -> dict[str, Any]:
        def scan() -> list[dict]:
            out = []
            for f in sorted((root(space) / "strategies").glob("*.json")):
                try:
                    out.append(_read(f))
                except (OSError, ValueError):
                    continue
            return out
        return {"items": await asyncio.to_thread(scan)}

    @app.put("/api/lab/{space}/strategies/{name}", dependencies=[Depends(auth)])
    async def put_strategy(space: Space, name: str, body: Annotated[dict, Body()]) -> dict[str, Any]:
        if not NAME.match(name) or body.get("name") != name or len(json.dumps(body, ensure_ascii=False)) > 500_000:
            raise HTTPException(status_code=422, detail="bad_strategy")
        fname = hashlib.sha1(name.encode("utf-8")).hexdigest()[:16]  # имя файла - хэш: кириллица и пробелы в имени стратегии не доходят до ФС
        await asyncio.to_thread(_write, root(space) / "strategies" / f"{fname}.json", {**body, "received_at": int(time.time())})
        return {"saved": name}
