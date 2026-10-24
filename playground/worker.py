"""Воркер сервиса Playground (контейнер playground): берёт прогоны из очереди (их ставит API по кнопке клиента) и считает по одному.

    python -m playground.worker

Низкий приоритет (nice 19): сервер общий с ботом. Сбой данных/сети (нет истории, бан Binance) - прогон failed с причиной, воркер работает дальше;
ошибка кода - падение процесса (R1), Docker перезапускает, прогон помечается failed при старте.
"""

import logging
import os
import time

from playground import paths
from playground.cli import execute
from playground.store import Store
from utils.errors import TradingInfraError

log = logging.getLogger("playground.worker")
IDLE_S = 3


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if hasattr(os, "nice"):
        os.nice(19)
    os.umask(0)  # SQLite пишет и API (другой пользователь контейнера): файлы базы и WAL должны быть доступны на запись обоим
    store = Store(paths.db_path())
    n = store.orphans_failed()
    if n:
        log.warning("Прогонов, прерванных перезапуском: %d (помечены failed)", n)
    log.info("Playground: жду прогоны (%s)", paths.db_path())
    while True:
        run_id = store.next_queued()
        if run_id is None:
            time.sleep(IDLE_S)
            continue
        log.info("Прогон %s: старт", run_id)
        try:
            summary = execute(store, run_id)
        except TradingInfraError as exc:
            log.warning("Прогон %s не выполнен: %s", run_id, exc)
            store.fail(run_id, f"{type(exc).__name__}: {exc}")
            continue
        o = summary["overall"]
        log.info("Прогон %s: готово, сделок %d, сумма R %.2f, PnL %.2f", run_id, o["closed"], o["sum_r"], o["pnl"])


if __name__ == "__main__":
    main()
