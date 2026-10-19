"""История версий клиента (Changelog / Release Notes, Блок 5, 2026-10-19).

Источник - `changelog.json` рядом с этим модулем (в собранном .exe он вложен: работает без сети и без сервера). Формат:
  {"versions": [{"version": "1.0", "date": "2026-10-19", "title": "...", "changes": [{"type": "new|fix|improve", "text": "..."} | "строка"]}]}
Новая версия = новая запись В НАЧАЛЕ списка (порядок не важен: сортировка по номеру). `build_exe.ps1 -Publish` отказывается публиковать сборку, если запись
для `__version__` отсутствует, и кладёт всю историю в `version.json` на сервере - окно обновления показывает, что нового в версии, ещё до загрузки.
Номера версий сравниваются по `updater.parse_version` (1.0 == 1.0.0)."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tradeai_client import updater

FILE = Path(__file__).resolve().parent / "changelog.json"
TYPES = {"new": "Новое", "improve": "Улучшено", "fix": "Исправлено"}


class ChangelogError(ValueError):
    """Файл или данные истории версий повреждены."""


@dataclass(frozen=True)
class Change:
    type: str
    text: str


@dataclass(frozen=True)
class Release:
    version: str
    date: str
    title: str
    changes: tuple[Change, ...]


def key(version: str) -> tuple[int, ...]:
    """Канонический ключ версии: 1.0 == 1.0.0 == 1.0.0.0 (дополняется нулями до четырёх чисел)."""
    v = updater.parse_version(version)
    return v + (0,) * (4 - len(v))


def parse(raw: Any) -> list[Release]:
    """Список записей (из файла или из version.json сервера) -> релизы, новые первыми. Некорректная запись - ChangelogError (молча терять историю нельзя)."""
    items = raw.get("versions") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        raise ChangelogError("история версий: ожидался список versions")
    out: list[Release] = []
    for e in items:
        try:
            version = str(e["version"])
            updater.parse_version(version)
            changes = tuple(Change("new" if isinstance(c, str) else str(c.get("type", "new")), c if isinstance(c, str) else str(c["text"])) for c in e["changes"])
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ChangelogError(f"история версий: неверная запись {e!r}: {exc}") from exc
        if any(c.type not in TYPES or not c.text.strip() for c in changes):
            raise ChangelogError(f"история версий {version}: тип изменения {sorted(TYPES)} и непустой текст")
        out.append(Release(version, str(e.get("date", "")), str(e.get("title", "")), changes))
    if len({key(r.version) for r in out}) != len(out):
        raise ChangelogError("история версий: версия встречается дважды")
    return sorted(out, key=lambda r: key(r.version), reverse=True)


def load(path: Path | None = None) -> list[Release]:
    try:
        return parse(json.loads((path or FILE).read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChangelogError(f"история версий нечитаема ({exc})") from exc


def release_for(releases: list[Release], version: str) -> Release | None:
    want = key(version)
    return next((r for r in releases if key(r.version) == want), None)


def missed(releases: list[Release], seen: str, current: str) -> list[Release]:
    """Релизы новее `seen` и не новее `current` (новые первыми): что человек пропустил. seen пусто - показывать нечего, кроме текущего (решает вызывающий)."""
    return [r for r in releases if not updater.is_newer(r.version, current) and (not seen or updater.is_newer(r.version, seen))]


def should_show_whats_new(seen: str, current: str) -> bool:
    """Окно «Приложение обновлено»: текущая версия строго новее последней виденной (пусто = ни одной не видел)."""
    return updater.is_newer(current, seen or "0.0")
