"""Подключение к PostgreSQL (asyncpg): пул, ожидание готовности БД, миграции. Пароль в логи и исключения не попадает."""

import asyncio
import logging
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from persistence import schema
from utils.errors import ConfigError, TradingInfraError

log = logging.getLogger("persistence.db")


class DatabaseError(TradingInfraError):
    """БД недоступна или схема не сходится: торговать/писать статистику вслепую нельзя (R1)."""


def redact_dsn(dsn: str) -> str:
    """postgresql://user:***@host:5432/db - для логов."""
    parts = urlsplit(dsn)
    if parts.password is None:
        return dsn
    host = parts.hostname or ""
    netloc = f"{parts.username}:***@{host}" + (f":{parts.port}" if parts.port else "")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def check_dsn(dsn: str) -> None:
    parts = urlsplit(dsn)
    if parts.scheme not in ("postgresql", "postgres") or not parts.hostname or not parts.path.strip("/"):
        raise ConfigError("DATABASE_URL должен иметь вид postgresql://user:password@host:5432/dbname")


class Database:
    def __init__(self, pool: asyncpg.Pool, dsn: str):
        self.pool, self._dsn = pool, dsn

    @classmethod
    async def connect(cls, dsn: str, *, attempts: int = 10, delay_s: float = 2.0, max_size: int = 3) -> "Database":
        """Пул соединений. БД может подниматься параллельно с процессом (compose): ограниченное число попыток, затем ошибка."""
        check_dsn(dsn)
        last: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                pool = await asyncpg.create_pool(dsn, min_size=1, max_size=max_size, command_timeout=30)
                log.info("PostgreSQL: подключено %s", redact_dsn(dsn))
                return cls(pool, dsn)
            except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as exc:
                last = exc
                log.warning("PostgreSQL %s недоступен (попытка %d/%d): %s", redact_dsn(dsn), attempt, attempts, type(exc).__name__)
                if attempt < attempts:
                    await asyncio.sleep(delay_s)
        raise DatabaseError(f"PostgreSQL {redact_dsn(dsn)} недоступен после {attempts} попыток: {type(last).__name__}") from last

    async def close(self) -> None:
        await self.pool.close()

    async def migrate(self) -> int:
        """Применяет недостающие миграции (по одной транзакции на версию, под advisory-блокировкой). -> текущая версия схемы."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock($1)", schema.MIGRATION_LOCK_KEY)
                await conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
                applied = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
                unknown = applied - {v for v, _ in schema.MIGRATIONS}
                if unknown:
                    raise DatabaseError(f"В БД есть миграции {sorted(unknown)}, которых нет в коде: схема новее программы, откат версии программы запрещён")
                for version, statements in schema.MIGRATIONS:
                    if version in applied:
                        continue
                    for ddl in statements:
                        await conn.execute(ddl)
                    await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", version)
                    log.info("PostgreSQL: применена миграция %d", version)
        return schema.SCHEMA_VERSION

    async def fetch(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(sql, *args)
