"""Обмен уроками и стратегиями с сервером мониторинга (`/api/lab/*`, ключ X-API-Key): уроки одного трейдера видят другой трейдер и разработчик.

Сервер - необязательная часть: нет связи - всё работает на локальных файлах (store.py), отправка повторяется при следующем сохранении/синхронизации."""

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

TIMEOUT_S = 20


class RemoteError(Exception):
    """Сервер недоступен или отказал (текст показывается пользователю)."""


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


def push_lesson(base: str, key: str, lesson: dict) -> None:
    _req(base, key, "PUT", f"/api/lab/lessons/{urllib.parse.quote(lesson['id'])}", lesson)


def list_lessons(base: str, key: str) -> list[dict]:
    return _req(base, key, "GET", "/api/lab/lessons")["items"]


def get_lesson(base: str, key: str, lesson_id: str) -> dict:
    return _req(base, key, "GET", f"/api/lab/lessons/{urllib.parse.quote(lesson_id)}")


def push_strategy(base: str, key: str, st: dict) -> None:
    _req(base, key, "PUT", f"/api/lab/strategies/{urllib.parse.quote(st['name'])}", st)


def list_strategies(base: str, key: str) -> list[dict]:
    return _req(base, key, "GET", "/api/lab/strategies")["items"]


def push_vocab(base: str, key: str, custom: list[dict]) -> None:
    _req(base, key, "PUT", "/api/lab/vocab", {"items": custom})


def get_vocab(base: str, key: str) -> list[dict]:
    return _req(base, key, "GET", "/api/lab/vocab")["items"]
