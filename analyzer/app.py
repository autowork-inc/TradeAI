"""REST анализатора (FastAPI). Наружу НЕ публикуется: его вызывает только основной API бота (прокси `/api/analysis/*`) по внутренней сети compose.
Защита второго уровня - заголовок X-Analyzer-Token (общий секрет с основным API). Тяжёлые расчёты здесь не выполняются: POST /runs ставит задание в очередь SQLite,
считает отдельный процесс-воркер. /docs и OpenAPI отключены.
"""

import asyncio
import hmac
import logging
import multiprocessing
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Annotated, Any

import pandas as pd
import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

from analyzer import __version__
from analyzer.config import Settings
from analyzer.db import check_db
from analyzer.errors import CalendarError, Conflict, DataError, NotFound
from analyzer.models import DryRunRequest, RunRequest, VersionBody
from analyzer.botcalendar import sync_bot_calendar
from analyzer.recommend import dry_run, load_manifest
from analyzer.store import Store
from analyzer.versions import VersionDef
from analyzer.worker import run_forever

log = logging.getLogger("analyzer.app")
JobId = Annotated[str, Path(pattern=r"^[0-9a-f]{12}$")]
VersionPath = Annotated[str, Path(pattern=r"^[A-Za-z0-9 _.@+()-]{1,60}$")]
SUPERVISE_S = 5.0


class WorkerSupervisor:
    """Держит процесс-воркер живым: упал - перезапуск (его падение не касается API и бота)."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._ctx = multiprocessing.get_context("spawn")
        self._proc: multiprocessing.process.BaseProcess | None = None

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.is_alive()

    def ensure(self) -> None:
        if not self.alive:
            if self._proc is not None:
                log.error("Воркер остановился (код %s): перезапуск", self._proc.exitcode)
            self._proc = self._ctx.Process(target=run_forever, args=(self._settings,), name="analyzer-worker", daemon=True)
            self._proc.start()

    def stop(self) -> None:
        if self._proc is not None and self._proc.is_alive():
            self._proc.terminate()
            self._proc.join(5)


def create_app(settings: Settings, *, start_worker: bool = True, db_check: Callable[[Settings], None] = check_db) -> FastAPI:
    store = Store(settings.data_dir)
    supervisor = WorkerSupervisor(settings) if start_worker else None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        task = None
        if supervisor is not None:
            supervisor.ensure()

            async def watch() -> None:
                while True:
                    await asyncio.sleep(SUPERVISE_S)
                    supervisor.ensure()
            task = asyncio.create_task(watch())
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if supervisor is not None:
                supervisor.stop()

    app = FastAPI(title="TradeAI analyzer", version=__version__, docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.store = store

    async def auth(request: Request) -> None:
        provided = request.headers.get("X-Analyzer-Token", "")
        if not provided or not hmac.compare_digest(provided.encode(), settings.token.encode()):
            raise HTTPException(status_code=401, detail="invalid_token")

    @app.exception_handler(NotFound)
    async def _nf(_: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(Conflict)
    async def _cf(_: Request, exc: Conflict) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(CalendarError)
    @app.exception_handler(DataError)
    async def _bad(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=422)

    @app.get("/health")
    async def health() -> JSONResponse:
        ok = supervisor is None or supervisor.alive
        return JSONResponse({"status": "ok" if ok else "degraded"}, status_code=200 if ok else 503)

    @app.get("/ready")
    async def ready() -> JSONResponse:
        """Готовность = воркер жив И БД бота доступна под ролью analyzer_ro. Это проверяет healthcheck контейнера (а не только ответ HTTP)."""
        if supervisor is not None and not supervisor.alive:
            return JSONResponse({"status": "degraded", "reason": "worker_down"}, status_code=503)
        try:
            await asyncio.to_thread(db_check, settings)
        except psycopg.Error as exc:
            log.error("Проверка БД не пройдена: %s", exc)
            return JSONResponse({"status": "degraded", "reason": "db_unavailable", "detail": str(exc).splitlines()[0][:200]}, status_code=503)
        return JSONResponse({"status": "ok"})

    @app.get("/status", dependencies=[Depends(auth)])
    async def status() -> dict[str, Any]:
        sync_bot_calendar(store, settings.bot_data_dirs)
        return {"version": __version__, "worker_alive": None if supervisor is None else supervisor.alive, "queue": store.queue_depth(),
                "calendar": store.calendar_info(), "versions": len(store.list_versions()), "min_conclusions": settings.min_conclusions}

    # ------------------------------------------------------------------------------------ задания

    @app.post("/runs", status_code=202, dependencies=[Depends(auth)])
    async def start_run(body: RunRequest) -> dict[str, Any]:
        if body.since and body.until and body.since >= body.until:
            raise HTTPException(status_code=422, detail="since должен быть раньше until")
        job_id = store.create_job(body.model_dump(mode="json"), settings.max_queue)
        return {"job_id": job_id, "status": "queued"}

    @app.get("/runs", dependencies=[Depends(auth)])
    async def list_runs(limit: Annotated[int, Query(ge=1, le=100)] = 20) -> dict[str, Any]:
        return {"items": store.list_jobs(limit)}

    @app.get("/runs/{job_id}", dependencies=[Depends(auth)])
    async def run_status(job_id: JobId) -> dict[str, Any]:
        return store.get_job(job_id)

    @app.get("/runs/{job_id}/report", dependencies=[Depends(auth)])
    async def run_report(job_id: JobId) -> JSONResponse:
        job = store.get_job(job_id)
        if job["status"] != "done":
            return JSONResponse({"detail": "report_not_ready", "status": job["status"], "error": job["error"]}, status_code=409)
        return JSONResponse({"job": job, "report": store.get_report(job_id)})

    # ------------------------------------------------------------------------------------ версии

    @app.get("/versions", dependencies=[Depends(auth)])
    async def versions() -> dict[str, Any]:
        return {"items": [v.to_json() for v in store.list_versions()]}

    @app.post("/versions", status_code=201, dependencies=[Depends(auth)])
    async def put_version(body: VersionBody) -> dict[str, Any]:
        def ts(v):
            return None if v is None else (pd.Timestamp(v).tz_localize("UTC") if pd.Timestamp(v).tzinfo is None else pd.Timestamp(v).tz_convert("UTC"))
        v = VersionDef(name=body.name, strategy_id=body.strategy_id, ts_from=ts(body.ts_from), ts_to=ts(body.ts_to), plan_hash=body.plan_hash,
                       meta_equals=body.meta_equals, note=body.note)
        if v.ts_from is not None and v.ts_to is not None and v.ts_from >= v.ts_to:
            raise HTTPException(status_code=422, detail="ts_from должен быть раньше ts_to")
        store.put_version(v, overwrite=body.overwrite)
        return v.to_json()

    @app.delete("/versions/{name}", dependencies=[Depends(auth)])
    async def delete_version(name: VersionPath) -> dict[str, Any]:
        store.delete_version(name)
        return {"deleted": name}

    # ------------------------------------------------------------------------------------ календарь

    @app.get("/calendar", dependencies=[Depends(auth)])
    async def calendar_info() -> dict[str, Any]:
        sync_bot_calendar(store, settings.bot_data_dirs)
        return store.calendar_info()

    # ------------------------------------------------------------------------------------ dry run

    @app.post("/dry-run", dependencies=[Depends(auth)])
    async def dry(body: DryRunRequest) -> dict[str, Any]:
        job = store.get_job(body.run_id)
        if job["status"] != "done":
            raise HTTPException(status_code=409, detail="report_not_ready")
        recs = store.get_report(body.run_id).get("recommendations") or []
        if body.recommendation_ids is not None:
            wanted = set(body.recommendation_ids)
            unknown = wanted - {r["id"] for r in recs}
            if unknown:
                raise HTTPException(status_code=422, detail=f"unknown_recommendations: {sorted(unknown)}")
            recs = [r for r in recs if r["id"] in wanted]
        return {"run_id": body.run_id, **dry_run(recs, load_manifest(settings.manifest_path))}

    return app
