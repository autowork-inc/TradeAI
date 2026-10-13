"""Подключение к PostgreSQL (tradeai) для аналитического MCP-сервера: ТОЛЬКО ЧТЕНИЕ.

Слои защиты от записи (достаточно любого одного, они независимы):
  1. сессия БД открывается с `default_transaction_read_only = on` - сервер сам отклонит INSERT/UPDATE/DELETE/DDL;
  2. `fetch()` принимает только один оператор, начинающийся с SELECT / WITH, и не содержит `;` внутри;
  3. все запросы в `queries.py` - готовые строки с параметрами `$1..`, текст от LLM в SQL не подставляется.

Пароль не попадает ни в сообщения об ошибках, ни в логи (`redact`).
"""

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import asyncpg

HERE = Path(__file__).resolve().parent
ENV_CANDIDATES = (HERE / ".env", HERE.parent / ".env")  # сначала свой файл, затем корневой .env проекта
CONNECT_TIMEOUT_S = 10.0
QUERY_TIMEOUT_S = 30.0
STATEMENT_TIMEOUT_MS = 25_000


class DbError(RuntimeError):
    """Ошибка доступа к БД с понятным человеку пояснением (текст безопасен: без пароля)."""


def read_database_url() -> str:
    """DATABASE_URL: переменная окружения процесса, иначе строка `DATABASE_URL=` из mcp_server/.env, иначе из корневого .env.

    Из .env читается ТОЛЬКО эта строка: ключи Binance из корневого .env сюда не попадают.
    """
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        for path in ENV_CANDIDATES:
            if path.is_file():
                url = _parse_env_url(path.read_text(encoding="utf-8-sig"))
                if url:
                    break
    if not url:
        raise DbError(
            "DATABASE_URL не задан. Создайте mcp_server/.env (шаблон - mcp_server/.env.example) со строкой "
            "DATABASE_URL=postgresql://tradeai:<пароль>@127.0.0.1:15432/tradeai или передайте её в env блока MCP-конфигурации."
        )
    return url


def _parse_env_url(text: str) -> str:
    for line in text.splitlines():
        m = re.match(r"^\s*(?:export\s+)?DATABASE_URL\s*=\s*(.*?)\s*$", line)
        if m and not line.lstrip().startswith("#"):
            value = m.group(1)
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            return value.strip()
    return ""


def redact(text: str, url: str = "") -> str:
    """Убирает пароль из текста (из заданного URL и из любых `scheme://user:пароль@`)."""
    if url:
        password = urlsplit(url).password
        if password:
            text = text.replace(password, "***").replace(unquote(password), "***")
    return re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1***@", text)


def describe_target(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.hostname or '?'}:{parts.port or 5432}/{(parts.path or '/').lstrip('/') or '?'}"


class Database:
    """Ленивый пул: сервер стартует и без БД (туннель можно поднять позже); подключение пробуется при каждом вызове инструмента."""

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()
        self._url = ""

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        """JSONB <-> объекты Python (по умолчанию asyncpg отдаёт JSONB строкой)."""
        await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")

    async def _get_pool(self) -> asyncpg.Pool:
        async with self._lock:
            if self._pool is not None:
                return self._pool
            self._url = read_database_url()
            try:
                self._pool = await asyncpg.create_pool(
                    self._url, min_size=1, max_size=3, timeout=CONNECT_TIMEOUT_S, init=self._init_connection, command_timeout=QUERY_TIMEOUT_S,
                    server_settings={
                        "default_transaction_read_only": "on",  # слой 1: запись невозможна на уровне сессии
                        "statement_timeout": str(STATEMENT_TIMEOUT_MS),
                        "application_name": "tradeai-mcp-readonly",
                    })
            except (OSError, asyncio.TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError, ValueError) as exc:
                raise DbError(self._connect_hint(exc)) from None  # from None: в цепочке исключений мог остаться DSN с паролем
            return self._pool

    def _connect_hint(self, exc: BaseException) -> str:
        target = describe_target(self._url)
        detail = redact(f"{type(exc).__name__}: {exc}", self._url)
        if isinstance(exc, asyncpg.InvalidPasswordError):
            why = "сервер БД отклонил логин/пароль (проверьте POSTGRES_PASSWORD в .env.shadow на VPS и DATABASE_URL; спецсимволы пароля кодируются %XX)"
        elif isinstance(exc, asyncpg.InvalidCatalogNameError):
            why = "такой базы нет (ожидается имя `tradeai`)"
        elif isinstance(exc, (asyncio.TimeoutError, TimeoutError, ConnectionRefusedError, OSError)):
            why = ("нет соединения: SSH-туннель не поднят или оборвался, либо порт/адрес неверны. "
                   "Запустите mcp_server/tunnel-hidden.vbs (или install_autostart.ps1 для автозапуска), лог - mcp_server/tunnel.log (см. README, разд. 1)")
        else:
            why = "не удалось подключиться"
        return f"Нет доступа к БД {target}: {why}. [{detail}]"

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        stripped = query.strip().rstrip(";").strip()
        if not re.match(r"(?is)^(select|with)\b", stripped) or ";" in stripped:  # слой 2
            raise DbError("Внутренняя ошибка: разрешён только один SELECT-запрос.")
        pool = await self._get_pool()
        try:
            async with pool.acquire() as conn:
                return [dict(r) for r in await conn.fetch(stripped, *args)]
        except asyncpg.ReadOnlySQLTransactionError:
            raise DbError("БД отклонила запись (сессия только для чтения) - это ошибка в коде запроса.") from None
        except asyncpg.UndefinedTableError:
            raise DbError("Таблицы нет: схема БД не создана или старше v2. Запустите shadow-run на VPS, он применяет миграции.") from None
        except asyncpg.QueryCanceledError:
            raise DbError(f"Запрос отменён по таймауту {STATEMENT_TIMEOUT_MS // 1000} с: сузьте окно времени или лимит.") from None
        except (OSError, asyncio.TimeoutError, asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError) as exc:
            self._pool, dead = None, self._pool  # соединение потеряно (туннель оборвался): пул пересоздастся при следующем вызове
            if dead is not None:
                dead.terminate()
            raise DbError("Соединение с БД потеряно (вероятно, оборвался SSH-туннель). Поднимите туннель и повторите вызов. "
                          f"[{redact(type(exc).__name__ + ': ' + str(exc), self._url)}]") from None
        except asyncpg.PostgresError as exc:
            raise DbError(redact(f"Ошибка БД: {type(exc).__name__}: {exc}", self._url)) from None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
