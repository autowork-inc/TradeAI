"""Избранное («База эталонных сетапов», 2026-10-19): звёздочка на сделке + выгрузка в каталог golden_set для будущих few-shot примеров ИИ.

Единственное место, где API ПИШЕТ в БД. Остальное API по-прежнему читает через пул с `default_transaction_read_only = on`; здесь - отдельный
крошечный пул, а SQL ниже трогает только таблицу `favorites` (фиксированные операторы, имена из кода, значения - параметрами). Сделки, свечи и
снапшоты через него изменить нельзя: в коде нет ни одного оператора над другими таблицами (проверяет tests/api/test_favorites.py).

Флаг общий для всех профилей клиента (SANTA и CHR): `created_by` - только подпись. На диск (settings.golden_set_dir) по звёздочке пишутся
`<id>.json` (сделка, закрытия, свечи, геометрия, meta, причина входа) и - если клиент прислал - `<id>.png` (картинка графика, её рисует клиент).
Снятие звёздочки удаляет и файлы. Ошибка диска -> 500, строка в БД в этом случае не создаётся (звёздочка не должна врать про файлы).
"""

import asyncio
import base64
import binascii
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Path as PathParam
from pydantic import BaseModel, StringConstraints

from api.reason import build_reason
from api.serialize import close_out, iso, trade_out

log = logging.getLogger("api.favorites")

TradeId = Annotated[str, PathParam(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9:._-]+$")]  # то же, что api.main.TradeId (без циклического импорта)
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
MAX_PNG_BYTES = 4_000_000
PROFILES = ("SANTA", "CHR")

ADD_SQL = "INSERT INTO favorites (trade_id, created_by) VALUES ($1, $2) ON CONFLICT (trade_id) DO NOTHING"
REMOVE_SQL = "DELETE FROM favorites WHERE trade_id = $1"
EXISTS_SQL = "SELECT 1 FROM favorites WHERE trade_id = $1"
TRADE_SQL = "SELECT * FROM trades WHERE id = $1"
SNAPSHOT_SQL = "SELECT created_at, candles, geometry, meta FROM trade_snapshots WHERE trade_id = $1"
CLOSES_SQL = "SELECT seq, ts, reason, frac, qty, price, gross_r, fees_r, net_r, pnl, fees FROM trade_closes WHERE trade_id = $1 ORDER BY seq"


class FavoriteBody(BaseModel):
    profile: Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,20}$")] = "SANTA"
    image_png_b64: str | None = None  # картинка графика (PNG, base64); без неё сохраняется только JSON


async def connect_write_pool(dsn: str) -> asyncpg.Pool:
    """Пул для звёздочек: 1-2 соединения, обычная (не read-only) транзакция. Ленивый min_size=0: пока никто не ставит звёздочек, соединений нет."""
    return await asyncpg.create_pool(dsn, min_size=0, max_size=2, command_timeout=15, server_settings={"application_name": "tradeai-api-favorites"})


def file_stem(trade_id: str) -> str:
    """Имя файла из id сделки (`<signal_id>:live` и т. п.): только безопасные символы, без путей."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", trade_id).lstrip(".")[:180] or "trade"


def decode_png(data: str) -> bytes:
    try:
        raw = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=422, detail="bad_image") from exc
    if len(raw) > MAX_PNG_BYTES or not raw.startswith(PNG_MAGIC):
        raise HTTPException(status_code=422, detail="bad_image")
    return raw


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def write_golden(directory: Path, trade_id: str, payload: dict[str, Any], png: bytes | None) -> list[str]:
    """Файлы эталона. -> имена записанных файлов. Каталог создаётся при необходимости; ошибка диска - OSError (вызывающий отдаёт 500)."""
    directory.mkdir(parents=True, exist_ok=True)
    stem = file_stem(trade_id)
    names = []
    if png is not None:
        _write_atomic(directory / f"{stem}.png", png)
        names.append(f"{stem}.png")
    has_png = png is not None or (directory / f"{stem}.png").exists()  # повторная звёздочка без картинки не должна терять прежнюю
    payload = {**payload, "image": f"{stem}.png" if has_png else None}
    _write_atomic(directory / f"{stem}.json", json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8"))
    names.append(f"{stem}.json")
    return names


def remove_golden(directory: Path, trade_id: str) -> None:
    stem = file_stem(trade_id)
    for ext in ("json", "png"):
        (directory / f"{stem}.{ext}").unlink(missing_ok=True)


def install(app: FastAPI, settings: Any, auth: Any, read_pool: Any, write_pool: Any) -> None:
    """Подключает PUT/DELETE /api/favorites/{trade_id}. read_pool()/write_pool() - функции, возвращающие пулы (они создаются в lifespan)."""
    @app.put("/api/favorites/{trade_id}", dependencies=[Depends(auth)])
    async def add_favorite(trade_id: TradeId, body: FavoriteBody) -> dict[str, Any]:
        png = decode_png(body.image_png_b64) if body.image_png_b64 else None
        async with read_pool().acquire() as conn:
            async with conn.transaction(readonly=True, isolation="repeatable_read"):
                trade = await conn.fetchrow(TRADE_SQL, trade_id)
                if trade is None:
                    raise HTTPException(status_code=404, detail="trade_not_found")
                snap = await conn.fetchrow(SNAPSHOT_SQL, trade_id)
                closes = await conn.fetch(CLOSES_SQL, trade_id)
        files: list[str] = []
        if settings.golden_set_dir is not None:
            t = {**trade_out(trade), "plan": trade["plan"]}
            meta = (snap["meta"] or {}) if snap else {}
            geometry = snap["geometry"] if snap else []
            payload = {"trade": t, "closes": [close_out(c) for c in closes], "meta": meta, "geometry": geometry,
                       "candles": snap["candles"] if snap else [], "reason": build_reason(t, meta, geometry),
                       "favorited_by": body.profile, "favorited_at": iso(datetime.now(timezone.utc))}
            try:
                files = await asyncio.to_thread(write_golden, settings.golden_set_dir, trade_id, payload, png)
            except OSError as exc:
                log.error("golden_set: не записан %s: %s", trade_id, exc)
                raise HTTPException(status_code=500, detail="golden_set_write_failed") from exc
        async with write_pool().acquire() as conn:
            await conn.execute(ADD_SQL, trade_id, body.profile)
        log.info("Избранное: %s (%s), файлы: %s", trade_id, body.profile, ",".join(files) or "нет")
        return {"trade_id": trade_id, "is_favorite": True, "files": files}

    @app.delete("/api/favorites/{trade_id}", dependencies=[Depends(auth)])
    async def remove_favorite(trade_id: TradeId) -> dict[str, Any]:
        async with write_pool().acquire() as conn:
            await conn.execute(REMOVE_SQL, trade_id)
        if settings.golden_set_dir is not None:
            try:
                await asyncio.to_thread(remove_golden, settings.golden_set_dir, trade_id)
            except OSError as exc:
                log.error("golden_set: не удалены файлы %s: %s", trade_id, exc)  # строка из БД уже снята; хвост на диске не мешает
        return {"trade_id": trade_id, "is_favorite": False}
