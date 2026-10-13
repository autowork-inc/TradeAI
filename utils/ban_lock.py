"""Персистентный замок бана биржи: <DATA_DIR>/.ban_lock.

При HTTP 429/418 время окончания бана записывается на диск. Любой запуск любого скрипта
проверяет замок первым делом и, пока бан не истёк, завершается с кодом 2 без единого
обращения к бирже. Таймер поэтому переживает смерть процесса и контейнера.

Параллельная запись (оба скрипта поймали 429 одновременно): запись атомарна
(tmp + os.replace), срок бана только продлевается (берётся максимум со старым значением).
Блокировки между процессами нет намеренно: оба получают один и тот же бан IP, расхождение
Retry-After - секунды, и его перекрывает BAN_SAFETY_MARGIN_S. Истёкший замок не удаляется
(удаление создавало бы гонку с новой записью), он просто игнорируется и перезаписывается.
"""

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

import config
from utils.errors import BanActive, DataError
from utils.storage import atomic_write_text


def lock_path() -> Path:
    return config.DATA_DIR / config.BAN_LOCK_NAME


def _read(path: Path) -> dict | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise DataError(f"Не удалось прочитать {path}: {exc}") from exc
    try:
        data = json.loads(raw)
        until = float(data["banned_until"])
        if not math.isfinite(until):
            raise ValueError("banned_until не число")
    except (ValueError, KeyError, TypeError) as exc:
        # Не знаем срок бана - не рискуем и не игнорируем. Удалять вручную, только убедившись, что бана нет.
        raise DataError(f"Замок {path} повреждён ({exc}); содержимое: {raw[:200]!r}") from exc
    data["banned_until"] = until
    return data


def ensure_not_banned() -> None:
    """Вызывается в начале main() каждого скрипта. Бан действует -> BanActive (код 2)."""
    data = _read(lock_path())
    if data is None:
        return
    remaining = data["banned_until"] - time.time()
    if remaining > 0:
        detail = (
            f"до {data.get('banned_until_utc', data['banned_until'])}, "
            f"HTTP {data.get('status', '?')}, {data.get('source', '?')}"
        )
        raise BanActive(data["banned_until"], remaining, detail)


def record_ban(retry_after: int, status: int, source: str) -> float:
    """Записывает окончание бана (epoch-секунды). Возвращает записанное значение."""
    path = lock_path()
    now = time.time()
    until = now + retry_after + config.BAN_SAFETY_MARGIN_S
    try:
        previous = _read(path)
    except DataError:
        previous = None  # повреждённый замок заменяем корректным, это строго безопаснее
    if previous is not None:
        until = max(until, previous["banned_until"])
    payload = {
        "banned_until": until,
        "banned_until_utc": datetime.fromtimestamp(until, tz=timezone.utc).isoformat(timespec="seconds"),
        "status": status,
        "source": source,
        "written_at": now,
    }
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False) + "\n")
    return until
