"""Безопасная запись файлов сырого слоя."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import IO

import polars as pl


def _fsync_dir(directory: Path) -> None:
    """Фиксирует переименование на диске. На Windows каталоги не открываются - пропуск."""
    if os.name == "nt":
        return
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_text(path: Path, text: str) -> None:
    """Текстовый аналог atomic_write_parquet: tmp -> fsync -> os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)


def tmp_path_for(path: Path) -> Path:
    """2024-01.parquet -> 2024-01.tmp.parquet (в том же каталоге, чтобы rename был атомарным)."""
    return path.with_name(f"{path.stem}.tmp.parquet")


def atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Пишет во временный файл, сбрасывает на диск, затем атомарно подменяет целевой.

    При падении в любой момент `path` остаётся либо старой полной версией, либо новой
    полной версией. Недописанный файл может остаться только как *.tmp.parquet.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tmp_path_for(path)
    try:
        df.write_parquet(tmp)
        with open(tmp, "rb+") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)


def cleanup_stale_tmp(directory: Path) -> list[Path]:
    """Удаляет *.tmp.parquet - остатки прерванной записи (наши собственные временные файлы)."""
    removed = []
    for tmp in directory.glob("*.tmp.parquet"):
        tmp.unlink()
        removed.append(tmp)
    return removed


class HourlyJsonlWriter:
    """Пишет строки в <root>/<SYMBOL>/<YYYY-MM-DD>/<HH>.jsonl; час определяется по recv_ts (UTC).

    Каждая строка заканчивается flush(): при падении процесса данные уже в ОС, и
    оборваться может только строка, записываемая в момент падения. Читатель обязан
    пропускать нечитаемую последнюю строку файла.
    """

    def __init__(self, root: Path):
        self._root = root
        self._open: dict[str, tuple[str, IO[str]]] = {}

    def write(self, symbol: str, recv_ts_ms: int, line: str) -> None:
        moment = datetime.fromtimestamp(recv_ts_ms / 1000, tz=timezone.utc)
        key = moment.strftime("%Y-%m-%d/%H")
        current = self._open.get(symbol)
        if current is None or current[0] != key:
            if current is not None:
                current[1].close()
            current = (key, self._open_file(symbol, key))
            self._open[symbol] = current
        fh = current[1]
        fh.write(line + "\n")
        fh.flush()

    def _open_file(self, symbol: str, key: str) -> IO[str]:
        day, hour = key.split("/")
        path = self._root / symbol / day / f"{hour}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        # Если прошлый запуск упал посреди строки, файл заканчивается без "\n".
        # Закрываем обрубок переводом строки, иначе первая новая запись склеится с ним.
        needs_newline = False
        if path.exists() and path.stat().st_size > 0:
            with open(path, "rb") as probe:
                probe.seek(-1, os.SEEK_END)
                needs_newline = probe.read(1) != b"\n"
        fh = open(path, "a", encoding="utf-8", newline="\n")
        if needs_newline:
            fh.write("\n")
            fh.flush()
        return fh

    def close(self) -> None:
        for _, fh in self._open.values():
            fh.close()
        self._open.clear()
