"""Общая обвязка запуска: логи, сигналы, превращение исключений в коды выхода."""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, NoReturn

import config
from utils import ban_lock
from utils.errors import (
    EXIT_GENERAL,
    EXIT_OK,
    EXIT_RATE_LIMIT,
    InstanceAlreadyRunning,
    RateLimitError,
    TradingInfraError,
)
from utils.logger import setup_logging

_HEARTBEAT_S = 300


async def sleep_with_heartbeat(seconds: int, log: logging.Logger) -> None:
    """Запасной вариант (если .ban_lock не записался): сон с записями в лог, чтобы пауза не выглядела зависанием."""
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(min(_HEARTBEAT_S, remaining))
        remaining = deadline - time.monotonic()
        if remaining > 0:
            log.warning("Пауза Retry-After: осталось ~%d с", remaining)


async def _supervise(main: Callable[[], Awaitable[None]], log: logging.Logger) -> int:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, task.cancel)
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # Windows: сигналы обработает сам asyncio.run

    try:
        await main()
        return EXIT_OK
    except asyncio.CancelledError:
        log.warning("Остановлено сигналом (SIGTERM/SIGINT)")
        return EXIT_OK
    except RateLimitError as exc:
        # Срок бана сохраняется на диск, процесс выходит сразу: следующий запуск (любой
        # скрипт, любой контейнер) увидит .ban_lock и завершится кодом 2 без запросов.
        try:
            until = ban_lock.record_ban(exc.retry_after, exc.status, str(exc))
        except Exception:
            # Замок не записан - перезапуск не будет знать о бане. Единственный безопасный
            # вариант: выдержать Retry-After в этом процессе.
            log.critical(
                "АЛЕРТ: биржа ограничила IP (%s), НО .ban_lock записать не удалось. "
                "Выдерживаю Retry-After=%d с в процессе.", exc, exc.retry_after, exc_info=True,
            )
            try:
                await sleep_with_heartbeat(exc.retry_after + config.BAN_SAFETY_MARGIN_S, log)
            except asyncio.CancelledError:
                log.warning("Пауза Retry-After прервана сигналом")
        else:
            log.critical(
                "АЛЕРТ: биржа ограничила IP (%s). Бан записан в %s до %s UTC. "
                "Завершаюсь с кодом %d.",
                exc, ban_lock.lock_path(),
                datetime.fromtimestamp(until, tz=timezone.utc).isoformat(timespec="seconds"),
                EXIT_RATE_LIMIT,
            )
        return EXIT_RATE_LIMIT
    except InstanceAlreadyRunning as exc:
        log.critical("Экземпляр уже запущен: %s (код выхода %d)", exc, exc.exit_code)
        return exc.exit_code
    except TradingInfraError as exc:
        log.error("%s: %s (код выхода %d)", type(exc).__name__, exc, exc.exit_code)
        return exc.exit_code
    except Exception:
        log.exception("Непредвиденная ошибка")
        return EXIT_GENERAL


def run(main: Callable[[], Awaitable[None]], name: str) -> NoReturn:
    """Запускает main() и завершает процесс с кодом, соответствующим исходу."""
    log = setup_logging(name)
    log.info("Старт %s", name)
    try:
        code = asyncio.run(_supervise(main, log))
    except KeyboardInterrupt:
        code = EXIT_OK
    log.info("Завершение %s, код выхода %d", name, code)
    logging.shutdown()
    sys.exit(code)
