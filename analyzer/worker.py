"""Воркер: отдельный процесс (multiprocessing), берёт задания из SQLite по одному, читает сделки из PostgreSQL, считает отчёт.

Изоляция от бота: низкий приоритет (nice 19), один процесс, лимит времени на задание, контейнер ограничен compose (1 CPU / 1 ГБ).
Падение расчёта НЕ роняет воркер: ошибка записывается в задание (status=failed, error) и логируется с трассировкой - это граница между
заданиями, а не «слепой try/except»: ни одно задание не может остаться «вечно running».
"""

import logging
import os
import time
from collections.abc import Callable
from datetime import timezone

import pandas as pd

from analyzer.botcalendar import sync_bot_calendar
from analyzer.config import Settings
from analyzer.db import LoadQuery, Loader, load_trades
from analyzer.engine import run_analysis, timed
from analyzer.errors import JobTimeout
from analyzer.recommend import load_manifest
from analyzer.store import Store

log = logging.getLogger("analyzer.worker")
POLL_S = 1.0


def _utc(ts):
    if ts is None:
        return None
    t = pd.Timestamp(ts)
    return t.tz_localize(timezone.utc) if t.tzinfo is None else t.tz_convert("UTC")


def query_from(params: dict) -> LoadQuery:
    return LoadQuery(mode=params.get("mode", "shadow"), strategies=tuple(params["strategies"]) if params.get("strategies") else None,
                     coins=tuple(params["coins"]) if params.get("coins") else None,
                     since=_utc(params["since"]).to_pydatetime() if params.get("since") else None, until=_utc(params["until"]).to_pydatetime() if params.get("until") else None)


def process_one(settings: Settings, store: Store, loader: Loader = load_trades, clock: Callable[[], float] = time.monotonic) -> bool:
    """Берёт одно ждущее задание и выполняет. True - задание было. Ошибка расчёта фиксируется в задании."""
    job = store.claim_next()
    if job is None:
        return False
    started = clock()

    def progress(stage: str, p: float) -> None:
        if clock() - started > settings.job_timeout_s:
            raise JobTimeout(f"Превышен лимит {settings.job_timeout_s:.0f} с")
        store.set_progress(job["id"], stage, p)

    try:
        params = job["params"]
        progress("load", 0.02)
        df = loader(settings, query_from(params), lambda f: progress("load", 0.02 + 0.25 * f))
        sync_bot_calendar(store, settings.bot_data_dirs)  # календарь ForexFactory из хранилища бота (read-only том)
        events = store.load_calendar()
        manifest = load_manifest(settings.manifest_path)
        report = timed(lambda: run_analysis(df, params, settings, manifest, events=events, user_versions=store.list_versions(), progress=progress))
        store.finish(job["id"], report, settings.keep_reports)
        log.info("Задание %s выполнено: %d сделок за %.1f с", job["id"], len(df), report.get("elapsed_s") or 0.0)
    except Exception as exc:  # noqa: BLE001 - граница задания: любая причина фиксируется в задании и в логе (с трассировкой), воркер живёт дальше
        log.exception("Задание %s не выполнено", job["id"])
        store.fail(job["id"], f"{type(exc).__name__}: {exc}")
    return True


def run_forever(settings: Settings) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if hasattr(os, "nice"):
        os.nice(19)  # самый низкий приоритет: боту CPU нужнее
    store = Store(settings.data_dir)
    orphans = store.fail_orphans()  # единственный потребитель очереди: задание 'running' на старте - осиротевшее (воркер перезапущен)
    if orphans:
        log.warning("Осиротевших заданий помечено failed: %d", orphans)
    log.info("Воркер анализатора запущен (pid %d)", os.getpid())
    while True:
        if not process_one(settings, store):
            time.sleep(POLL_S)
