"""Журнал событий оркестратора и трекера: в памяти и (необязательно) в append-only JSONL на диске.

БД появится на Этапе 5. Пока JSONL нужен, чтобы статистика Shadow Mode не пропадала при каждом перезапуске
процесса (Fail-Fast: контейнер перезапускается при любой ошибке). Запись строго: ошибка диска - исключение (R1),
значение, которое нельзя записать в JSON, - TypeError, а не `default=str`, прячущий баг.
"""

import json
import logging
from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from orchestrator.types import Event

log = logging.getLogger("journal")


class EventLog(Protocol):
    def emit(self, event: Event) -> None: ...


def _default(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Нельзя записать в журнал: {type(obj).__name__}")


class MemoryEventLog:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def of_kind(self, kind: str) -> list[Event]:
        return [e for e in self.events if e.kind == kind]


class JsonlEventLog(MemoryEventLog):
    """Память + файл. Каждая запись - одна строка JSON, flush сразу: при падении теряется не более одной записи."""

    def __init__(self, path: Path):
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def emit(self, event: Event) -> None:
        line = json.dumps(asdict(event), ensure_ascii=False, separators=(",", ":"), allow_nan=False, default=_default)
        self._fh.write(line + "\n")
        self._fh.flush()
        super().emit(event)

    def close(self) -> None:
        self._fh.close()
