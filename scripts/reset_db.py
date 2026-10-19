"""Полная очистка статистики Shadow в PostgreSQL: python -m scripts.reset_db [--yes] [--dsn DSN].

Выполняет `TRUNCATE trades, trade_closes, trade_snapshots, state_events RESTART IDENTITY CASCADE`.
Схема и `schema_migrations` не трогаются. Без `--yes` только показывает, сколько строк будет удалено.

Откуда берётся DSN (первое найденное): `--dsn` > окружение `DATABASE_URL` > строка `DATABASE_URL=` из `mcp_server/.env`
(туннель `127.0.0.1:15432`, см. `mcp_server/tunnel.ps1`) > из корневого `.env`. Читается ТОЛЬКО эта строка, остальные ключи не трогаются.
Пароль в вывод не попадает. Это ЛОКАЛЬНЫЙ инструмент: на сервер он не нужен (`deploy_pack` берёт только файлы из COPY в Dockerfile).

Перед очисткой остановите `shadow-run` (иначе процесс сразу допишет новые строки):
    ssh root@<VPS> "cd ~/trading && docker compose --env-file .env.shadow stop shadow-run"
"""

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

import asyncpg

from persistence.db import check_dsn, redact_dsn
from utils.errors import ConfigError, TradingInfraError

ROOT = Path(__file__).resolve().parent.parent
ENV_FILES = (ROOT / "mcp_server" / ".env", ROOT / ".env")
TABLES = ("trades", "trade_closes", "trade_snapshots", "state_events", "favorites")  # порядок для отчёта; TRUNCATE одним оператором + CASCADE
_LINE = re.compile(r"""^\s*(?:export\s+)?DATABASE_URL\s*=\s*(?P<v>.*?)\s*$""")


def _parse_env_url(text: str) -> str:
    """Значение DATABASE_URL из текста .env ('' если строки нет). Кавычки снимаются; закомментированная строка не считается."""
    for raw in text.splitlines():
        m = _LINE.match(raw)
        if m:
            v = m.group("v")
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                v = v[1:-1]
            return v
    return ""


def resolve_dsn(cli_dsn: str | None) -> str:
    if cli_dsn:
        return cli_dsn
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    for path in ENV_FILES:
        if path.is_file():
            url = _parse_env_url(path.read_text(encoding="utf-8"))
            if url:
                return url
    raise ConfigError("DATABASE_URL не найден: задайте --dsn, переменную окружения или строку DATABASE_URL в mcp_server/.env")


async def _counts(conn: asyncpg.Connection) -> dict[str, int]:
    return {t: await conn.fetchval(f"SELECT count(*) FROM {t}") for t in TABLES}  # имена из константы, не из ввода


async def reset(dsn: str, *, apply: bool) -> int:
    check_dsn(dsn)
    print(f"БД: {redact_dsn(dsn)}")
    conn = await asyncpg.connect(dsn, timeout=15, command_timeout=60)
    try:
        before = await _counts(conn)
        for t, n in before.items():
            print(f"  до:    {t:<16}{n:>8}")
        if not apply:
            print("Ничего не удалено. Добавьте --yes, чтобы выполнить TRUNCATE.")
            return 0
        async with conn.transaction():
            await conn.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
        after = await _counts(conn)
        for t, n in after.items():
            print(f"  после: {t:<16}{n:>8}")
        left = {t: n for t, n in after.items() if n}
        if left:
            raise TradingInfraError(f"после TRUNCATE остались строки: {left} (пишет ли ещё shadow-run?)")
        print("Готово: таблицы пусты.")
        return 0
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Очистка trades / trade_closes / trade_snapshots / state_events (TRUNCATE ... CASCADE)")
    ap.add_argument("--yes", action="store_true", help="выполнить очистку (без флага - только показать счётчики)")
    ap.add_argument("--dsn", default=None, help="postgresql://user:password@host:port/db (иначе окружение / mcp_server/.env)")
    a = ap.parse_args(argv)
    try:
        return asyncio.run(reset(resolve_dsn(a.dsn), apply=a.yes))
    except (ConfigError, TradingInfraError) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
