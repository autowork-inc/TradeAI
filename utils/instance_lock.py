"""Защита от второго экземпляра скрипта: эксклюзивный неблокирующий замок на файл.

Замок берётся на файловый дескриптор средствами ОС (только стандартная библиотека):
    Linux/macOS - fcntl.flock(LOCK_EX | LOCK_NB)
    Windows     - msvcrt.locking(LK_NBLCK) на байте 0
ОС снимает замок сама, когда дескриптор закрывается, в том числе при падении или kill -9
процесса. Мёртвых замков не бывает, чистить ничего не нужно.

Файл замка <DATA_DIR>/.instance_lock.<имя скрипта> НИКОГДА не удаляется: удаление создаёт
гонку (A удалил файл, B залочил старый inode, C создал новый файл и залочил его - два
владельца). Содержимое файла (pid, хост, время старта) - только подсказка для диагноза;
истиной является сам замок. Лок по одному файлу на скрипт: стрим стакана и разовая
докачка свечей должны работать одновременно, а два экземпляра одного скрипта - нет.

Границы: замок действует между процессами, видящими один и тот же файл через одну ОС.
Это покрывает два контейнера на одном хосте и ручной запуск рядом с контейнером на Linux.
Ручной запуск на Windows-хосте рядом с контейнером Docker Desktop НЕ покрывается: хост
берёт замок msvcrt, контейнер - flock, и друг друга они не видят.
"""

import errno
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import config
from utils.errors import DataError, InstanceAlreadyRunning

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

# Коды ошибок, означающие "замок занят" (а не "нет прав / нет каталога").
_BUSY_ERRNOS = {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK}
if hasattr(errno, "EDEADLOCK"):
    _BUSY_ERRNOS.add(errno.EDEADLOCK)

# Подсказка о владельце пишется со смещения 1: на Windows байт 0 заблокирован и другим
# процессам не читается.
_INFO_OFFSET = 1


def _try_lock(fd: int) -> None:
    """Берёт замок без ожидания. Занято -> OSError с errno из _BUSY_ERRNOS."""
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(fd: int) -> None:
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


def _read_holder(fd: int) -> str:
    try:
        os.lseek(fd, _INFO_OFFSET, os.SEEK_SET)
        return os.read(fd, 512).decode("utf-8", "replace").strip() or "владелец неизвестен"
    except OSError:
        return "владелец неизвестен"


class InstanceLock:
    """Контекстный менеджер: `with InstanceLock("depth_stream"): ...`.

    Занято другим процессом -> InstanceAlreadyRunning (код выхода 1), без ожидания.
    """

    def __init__(self, name: str):
        self.name = name
        self.path: Path = config.DATA_DIR / f"{config.INSTANCE_LOCK_NAME}.{name}"
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeError(f"замок {self.path} уже взят этим объектом")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as exc:
            raise DataError(f"Не удалось открыть файл замка {self.path}: {exc}") from exc
        try:
            _try_lock(fd)
        except OSError as exc:
            holder = _read_holder(fd) if exc.errno in _BUSY_ERRNOS else ""
            os.close(fd)
            if exc.errno in _BUSY_ERRNOS:
                raise InstanceAlreadyRunning(
                    f"{self.name}: замок {self.path} занят другим процессом ({holder})"
                ) from exc
            raise DataError(f"Не удалось заблокировать {self.path}: {exc}") from exc

        try:
            info = (
                f"pid={os.getpid()} host={socket.gethostname()} "
                f"started={datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"
            )
            os.ftruncate(fd, _INFO_OFFSET)
            os.lseek(fd, _INFO_OFFSET, os.SEEK_SET)
            os.write(fd, info.encode("utf-8"))
        except OSError:
            pass  # подсказка не обязательна, замок уже взят
        self._fd = fd

    def release(self) -> None:
        """Явное снятие (штатный выход). При смерти процесса то же делает ОС."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            _unlock(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> "InstanceLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info) -> None:
        self.release()
