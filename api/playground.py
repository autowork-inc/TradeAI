"""`/api/playground/*` - Playground (2026-10-24): прогоны стратегий бота по истории. API ставит прогон в очередь и читает историю из SQLite
сервиса playground (общий каталог ./data/playground); считает воркер (контейнер playground, playground/worker.py) - в процессе API тяжёлого кода нет.

Сделки прогона отдаются в том же формате, что живые (`/api/trades`, `/api/trades/{id}/snapshot`): клиент открывает их тем же окном графика
с «Разбором сделки» и хронологией. Профиль CHR видит только прогоны своих стратегий (scope=chr).
"""

import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Callable, Literal

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from api.reason import build_reason
from playground.store import AVAILABLE_STRATEGIES, CHR_STRATEGIES, DataError, Store

RUN_ID = Annotated[str, Field(pattern=r"^[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")]
SYMBOL = re.compile(r"^[A-Z0-9]{2,20}USDT$")
MAX_DAYS = 400
DAY_MS = 86_400_000
TS_FIELDS = ("signal_time", "entry_time", "exit_time")


class RunIn(BaseModel):
    symbol: str = Field(min_length=3, max_length=24)
    strategies: list[str] = Field(min_length=1, max_length=len(AVAILABLE_STRATEGIES))
    start: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    profile: Literal["SANTA", "CHR"] = "SANTA"


def ms_iso(v: Any) -> str | None:
    if v is None:
        return None
    return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _day(text: str) -> int:
    try:
        return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        raise HTTPException(status_code=422, detail="bad_date") from None


def trade_json(row: dict[str, Any]) -> dict[str, Any]:
    """Строка сделки прогона (мс) -> формат /api/trades (ISO-время, mode=playground)."""
    out = {**row, **{k: ms_iso(row.get(k)) for k in TS_FIELDS}, "mode": "playground", "has_snapshot": True, "is_favorite": False}
    out.pop("client_order_id", None)
    return out


def event_json(e: dict[str, Any]) -> dict[str, Any]:
    return {**e, "ts": ms_iso(e["ts"])}


def install(app: FastAPI, auth: Callable, directory: Path | None) -> None:
    holder: dict[str, Store] = {}

    def store() -> Store:
        if directory is None:
            raise HTTPException(status_code=503, detail="playground_not_configured")
        if "s" not in holder:
            holder["s"] = Store(directory / "playground.sqlite")
        return holder["s"]

    async def call(fn, *args, **kw):
        try:
            return await asyncio.to_thread(fn, *args, **kw)
        except DataError:
            raise HTTPException(status_code=404, detail="not_found") from None

    @app.get("/api/playground/meta", dependencies=[Depends(auth)])
    async def meta() -> dict[str, Any]:
        return {"strategies": list(AVAILABLE_STRATEGIES), "chr_strategies": list(CHR_STRATEGIES), "max_days": MAX_DAYS}

    @app.get("/api/playground/runs", dependencies=[Depends(auth)])
    async def runs(scope: Literal["santa", "chr"] = "santa", limit: Annotated[int, Query(ge=1, le=500)] = 200) -> dict[str, Any]:
        subset = CHR_STRATEGIES if scope == "chr" else None
        return {"items": await call(store().runs, subset, limit)}

    @app.post("/api/playground/runs", dependencies=[Depends(auth)])
    async def create(body: RunIn) -> dict[str, Any]:
        symbol = body.symbol.strip().upper()
        if not SYMBOL.match(symbol):
            raise HTTPException(status_code=422, detail="bad_symbol")
        names = list(dict.fromkeys(body.strategies))
        if any(n not in AVAILABLE_STRATEGIES for n in names):
            raise HTTPException(status_code=422, detail="bad_strategy")
        if body.profile == "CHR" and any(n not in CHR_STRATEGIES for n in names):
            raise HTTPException(status_code=403, detail="strategy_not_in_profile")
        start, end = _day(body.start), _day(body.end) + DAY_MS  # «по дату» включительно
        now = int(datetime.now(timezone.utc).timestamp() * 1000) // 60_000 * 60_000
        end = min(end, now)
        if not end > start or (end - start) > MAX_DAYS * DAY_MS:
            raise HTTPException(status_code=422, detail="bad_period")
        run_id = await call(store().create, symbol, names, start, end, profile=body.profile, status="queued")
        return {"id": run_id}

    @app.get("/api/playground/runs/{run_id}", dependencies=[Depends(auth)])
    async def run(run_id: RUN_ID) -> dict[str, Any]:
        return await call(store().run, run_id)

    @app.delete("/api/playground/runs/{run_id}", dependencies=[Depends(auth)])
    async def delete(run_id: RUN_ID) -> dict[str, Any]:
        if not await call(store().delete, run_id):
            raise HTTPException(status_code=409, detail="run_busy_or_missing")
        return {"deleted": run_id}

    @app.get("/api/playground/runs/{run_id}/trades", dependencies=[Depends(auth)])
    async def trades(run_id: RUN_ID, strategy: Annotated[str | None, Query(max_length=60)] = None) -> dict[str, Any]:
        rows = await call(store().trades, run_id, strategy)
        items = [trade_json(r) for r in rows]
        return {"items": items, "total": len(items)}

    @app.get("/api/playground/runs/{run_id}/trades/{trade_id}/snapshot", dependencies=[Depends(auth)])
    async def snapshot(run_id: RUN_ID, trade_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9:._-]{1,200}$")]) -> dict[str, Any]:
        one = await call(store().trade, run_id, trade_id)
        snap = one["snapshot"] or {}
        t = {**trade_json(one["trade"]), "plan": one["trade"].get("plan")}
        geometry, meta_ = snap.get("geometry") or [], snap.get("meta") or {}
        events = [event_json(e) for e in one["events"]]
        return {
            "trade": t, "closes": [{**c, "ts": ms_iso(c["ts"])} for c in one["closes"]], "snapshot_created_at": ms_iso(snap.get("created_at")),
            "candles": snap.get("candles") or [], "geometry": geometry, "meta": meta_, "reason": build_reason(t, meta_, geometry),
            "events": [e for e in events if e["kind"] != "setup_state"], "fills": [],
            "setup_path": [e for e in events if e["kind"] == "setup_state"],  # по возрастанию времени, как у /api/trades/{id}/snapshot
        }
