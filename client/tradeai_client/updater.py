"""Автообновление клиента (2026-10-19). Сервер (api/updates.py) отдаёт `version.json` и `TradeAI-Monitor.exe`; их кладёт `build_exe.ps1 -Publish`.

Порядок: при старте быстрый запрос version.json -> версия сервера новее локальной -> диалог -> загрузка нового .exe рядом с текущим (`*.new`),
проверка размера и sha256 -> `.bat`-помощник ждёт, пока текущий .exe освободится, подменяет файл и запускает новый. Подмена возможна только у
собранного .exe (из исходников обновлять нечего: показывается подсказка). Проблемы сети при проверке - не ошибка: клиент просто работает дальше.
"""

import asyncio
import hashlib
import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from tradeai_client.api import ApiClient, ApiError

log = logging.getLogger("tradeai_client.updater")
EXE_NAME = "TradeAI-Monitor.exe"
VERSION_RE = re.compile(r"^\d{1,4}(\.\d{1,4}){1,3}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class UpdateError(Exception):
    """Загрузка или установка обновления не удалась (показывается пользователю как есть)."""


@dataclass(frozen=True)
class UpdateInfo:
    version: str
    sha256: str
    size: int
    notes: str = ""
    changelog: tuple = ()  # релизы из version.json сервера (changelog.Release, новые первыми): что нового в версии - ещё до загрузки


def parse_version(text: str) -> tuple[int, ...]:
    if not VERSION_RE.match(text or ""):
        raise ValueError(f"неверная версия {text!r}")
    return tuple(int(x) for x in text.split("."))


def is_newer(remote: str, local: str) -> bool:
    a, b = parse_version(remote), parse_version(local)
    n = max(len(a), len(b))
    return a + (0,) * (n - len(a)) > b + (0,) * (n - len(b))


def parse_info(raw: dict) -> UpdateInfo:
    """version.json сервера -> UpdateInfo. Неполный или кривой файл - UpdateError: ставить по нему нельзя (нечем проверить загрузку)."""
    try:
        version, sha, size = str(raw["version"]), str(raw["sha256"]).lower(), int(raw["size"])
        parse_version(version)
    except (KeyError, ValueError, TypeError) as exc:
        raise UpdateError(f"version.json на сервере неполный или повреждён: {exc}") from exc
    if not SHA_RE.match(sha) or size <= 0:
        raise UpdateError("version.json: нужны sha256 (64 hex-символа) и size > 0")
    releases: tuple = ()
    if raw.get("changelog"):
        from tradeai_client import changelog  # локально: changelog импортирует этот модуль

        try:
            releases = tuple(changelog.parse(raw["changelog"]))
        except changelog.ChangelogError as exc:
            log.warning("История версий на сервере повреждена, показываем только заметки: %s", exc)
    return UpdateInfo(version, sha, size, str(raw.get("notes") or ""), releases)


async def check_for_update(client: ApiClient, local_version: str) -> UpdateInfo | None:
    """Новее ли версия на сервере. Ни публикации, ни сети, ни старого сервера - None (молча: это не повод мешать работе)."""
    try:
        raw = await client.update_version()
    except ApiError as exc:
        log.info("Проверка обновлений пропущена: %s", exc.kind)
        return None
    if raw is None:
        return None
    try:
        info = parse_info(raw)
    except UpdateError as exc:
        log.warning("%s", exc)
        return None
    return info if is_newer(info.version, local_version) else None


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def exe_path() -> Path:
    return Path(sys.executable).resolve()


async def download(client: ApiClient, info: UpdateInfo, target: Path, progress: Callable[[float], None] | None = None) -> Path:
    """Скачать новый .exe в `target` (временное имя рядом с текущим), проверить размер и sha256. Битый файл удаляется."""
    tmp = target.with_name(target.name + ".part")
    digest, done = hashlib.sha256(), 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in client.download_update():
                f.write(chunk)
                digest.update(chunk)
                done += len(chunk)
                if progress:
                    progress(min(1.0, done / info.size))
        if done != info.size or digest.hexdigest() != info.sha256:
            raise UpdateError(f"Файл обновления повреждён при загрузке (получено {done} из {info.size} байт, контрольная сумма не совпала)")
        os.replace(tmp, target)
    except (OSError, aiohttp.ClientError, asyncio.TimeoutError, ApiError) as exc:
        raise UpdateError(f"Не удалось скачать обновление: {exc}") from exc
    finally:
        tmp.unlink(missing_ok=True)
    return target


BAT = """@echo off
chcp 65001 >nul
set "NEW={new}"
set "EXE={exe}"
set /a N=0
:retry
move /Y "%NEW%" "%EXE%" >nul 2>&1
if not errorlevel 1 goto done
set /a N+=1
if %N% GEQ 90 goto fail
timeout /t 1 /nobreak >nul
goto retry
:done
set "_MEIPASS2="
set "PYINSTALLER_RESET_ENVIRONMENT=1"
start "" "%EXE%"
del "%~f0"
exit /b 0
:fail
del "%~f0"
exit /b 1
"""


def write_swap_script(new: Path, exe: Path, directory: Path) -> Path:
    for p in (new, exe):
        if '"' in str(p) or "%" in str(p):
            raise UpdateError(f"Путь {p} содержит недопустимые для обновления символы (кавычка или %): переместите программу")
    script = directory / "tradeai_update.bat"
    script.write_text(BAT.format(new=new, exe=exe), encoding="utf-8", newline="\r\n")
    return script


def clean_launch_env(env: dict[str, str]) -> dict[str, str]:
    """Окружение для запуска НОВОГО .exe из работающего старого (однофайловая сборка PyInstaller).

    Старый процесс держит в переменных окружения путь к своей временной папке `_MEIxxxx` (`_MEIPASS2`, `_PYI_*`); новый .exe, унаследовав их, ищет
    python312.dll в чужой папке, которую старый процесс при выходе уже удалил: «Failed to load Python DLL ... python312.dll. LoadLibrary: Не найден
    указанный модуль». Поэтому эти переменные убираем, а `PYINSTALLER_RESET_ENVIRONMENT=1` велит загрузчику считать запуск чистым (1.9.2)."""
    clean = {k: v for k, v in env.items() if not (k.upper().startswith("_PYI") or k.upper() == "_MEIPASS2")}
    clean["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
    return clean


def launch_swap(script: Path) -> None:
    """Запустить помощника так, чтобы он пережил завершение клиента: клиент при выходе убивает своё дерево процессов (taskkill /T), поэтому помощник
    стартует через PowerShell Start-Process - у него не остаётся родителя, и дерево до него не дотягивается."""
    ps = f"Start-Process -WindowStyle Hidden -FilePath cmd.exe -ArgumentList '/c','\"{script}\"'"
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        subprocess.run(["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", ps], check=True, timeout=30, creationflags=flags,
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       env=clean_launch_env(dict(os.environ)))
    except (OSError, subprocess.SubprocessError) as exc:
        raise UpdateError(f"Не удалось запустить установку обновления: {exc}") from exc
