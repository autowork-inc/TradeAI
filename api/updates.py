"""Раздача обновлений клиента (2026-10-19): `version.json` и `TradeAI-Monitor.exe` из каталога settings.update_dir (на сервере ./data/updates).
Файлы кладёт скрипт сборки клиента (`client/build_exe.ps1 -Publish`). Оба адреса требуют ключ API: собранный .exe может содержать зашитый ключ."""

import json
import re
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse

EXE_NAME = "TradeAI-Monitor.exe"
VERSION_RE = re.compile(r"^\d{1,4}(\.\d{1,4}){1,3}$")


def read_version(directory: Path) -> dict[str, Any]:
    path = directory / "version.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="no_update_published")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="version_json_invalid") from exc
    if not isinstance(data, dict) or not VERSION_RE.match(str(data.get("version", ""))):
        raise HTTPException(status_code=500, detail="version_json_invalid")
    return data


def install(app: FastAPI, update_dir: Path | None, auth: Any) -> None:
    def directory() -> Path:
        if update_dir is None:
            raise HTTPException(status_code=404, detail="updates_not_configured")
        return update_dir

    @app.get("/api/update/version.json", dependencies=[Depends(auth)])
    async def version() -> dict[str, Any]:
        return read_version(directory())

    @app.get("/api/update/" + EXE_NAME, dependencies=[Depends(auth)])
    async def exe() -> FileResponse:
        path = directory() / EXE_NAME
        if not path.is_file():
            raise HTTPException(status_code=404, detail="no_update_published")
        return FileResponse(path, media_type="application/octet-stream", filename=EXE_NAME)
