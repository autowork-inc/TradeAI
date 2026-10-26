"""Обмен уроками и стратегиями с сервером мониторинга (`/api/lab/*`, ключ X-API-Key) в пространстве профиля (santa | chr):
уроки, словарь и стратегии одного профиля другой профиль не получает; разработчик читает оба каталога на сервере.

Сервер - необязательная часть: нет связи - всё работает на локальных файлах (store.py), отправка повторяется при следующем сохранении/синхронизации."""

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

TIMEOUT_S = 20


class RemoteError(Exception):
    """Сервер недоступен или отказал (текст показывается пользователю)."""


SPACES = ("santa", "chr")


def _sp(space: str) -> str:
    """Пространство профиля на сервере (1.9.1): уроки SANTA и CHR лежат раздельно; неизвестное имя - ошибка, а не «общая» папка."""
    if space not in SPACES:
        raise RemoteError(f"неизвестный профиль уроков: {space!r}")
    return space


def _req(base: str, key: str, method: str, path: str, body: Any = None) -> Any:
    if not base or not key:
        raise RemoteError("сервер не настроен (адрес и ключ - в настройках приложения)")
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(f"{base.rstrip('/')}{path}", data=data, method=method,
                                 headers={"X-API-Key": key, **({"Content-Type": "application/json"} if data is not None else {})})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise RemoteError("на сервере нет раздела уроков (обновите сервер)") from exc
        raise RemoteError(f"сервер ответил HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        raise RemoteError(f"сервер недоступен: {type(exc).__name__}") from exc


def push_lesson(base: str, key: str, space: str, lesson: dict) -> None:
    _req(base, key, "PUT", f"/api/lab/{_sp(space)}/lessons/{urllib.parse.quote(lesson['id'])}", lesson)


def list_lessons(base: str, key: str, space: str) -> list[dict]:
    return _req(base, key, "GET", f"/api/lab/{_sp(space)}/lessons")["items"]


def get_lesson(base: str, key: str, space: str, lesson_id: str) -> dict:
    return _req(base, key, "GET", f"/api/lab/{_sp(space)}/lessons/{urllib.parse.quote(lesson_id)}")


def push_strategy(base: str, key: str, space: str, st: dict) -> None:
    _req(base, key, "PUT", f"/api/lab/{_sp(space)}/strategies/{urllib.parse.quote(st['name'])}", st)


def list_strategies(base: str, key: str, space: str) -> list[dict]:
    return _req(base, key, "GET", f"/api/lab/{_sp(space)}/strategies")["items"]


def push_vocab(base: str, key: str, space: str, custom: list[dict]) -> None:
    _req(base, key, "PUT", f"/api/lab/{_sp(space)}/vocab", {"items": custom})


def get_vocab(base: str, key: str, space: str) -> list[dict]:
    return _req(base, key, "GET", f"/api/lab/{_sp(space)}/vocab")["items"]
