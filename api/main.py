"""FastAPI-сервер мониторинга Shadow Mode (Этап 6, CLAUDE.md разд. 3.19). Запуск: `python -m api.main` (в контейнере - `api` из docker-compose.yml).

Принципы:
  * ТОЛЬКО ЧТЕНИЕ (единственное исключение - звёздочки избранного, api/favorites.py: отдельный пул, SQL только над таблицей favorites). Все соединения с БД открываются с `default_transaction_read_only = on`: даже ошибка в коде API не сможет записать
    или удалить сделку. Миграции делает shadow-run, не API.
  * Всё, кроме /api/health, требует заголовок X-API-Key (см. api/security.py). /docs, /redoc и схема OpenAPI отключены.
  * Fail-Fast там, где это уместно: без ключа/DATABASE_URL процесс не стартует (код 3). Недоступная БД во время работы - 503 и
    `error` в WebSocket (а не падение: читающему API незачем перезапускаться вместе с БД), нечитаемые данные - 500/`data_integrity`.
"""

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal

import asyncpg
import httpx
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import StringConstraints

from api import analysis_proxy, candles, favorites, updates
from api.livestate import SETUP_STRATEGIES, LiveStateHub, current_coins, freshest_heartbeat_age_s, read_account
from api.reason import build_reason
from api.security import FailureThrottle, check_key
from api.serialize import DataIntegrityError, close_out, event_out, iso, trade_out
from api.settings import DEFAULT_STRATEGIES, ApiConfigError, Settings, load_settings
from api.stats import compute_stats, counts_from_rows

log = logging.getLogger("api")

Mode = Literal["live", "shadow", "all"]  # all - оба контура вместе
Status = Literal["pending_limit", "pending_market", "open", "closed", "cancelled", "aborted"]
Side = Literal["long", "short"]
Token = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.-]{1,40}$")]
TradeId = Annotated[str, Path(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9:._-]+$")]
MAX_WS_CLIENTS = 20
_ONE_MINUTE = timedelta(minutes=1)  # события автомата пишутся по часам получения свечи, чуть позже signal_time

TRADE_LIST_SQL = """
SELECT t.id, t.strategy_name, t.strategy_id, t.coin, t.side, t.kind, t.mode, t.status, t.qty, t.remaining_frac, t.signal_time, t.entry_time,
       t.exit_time, t.planned_entry, t.entry_price, t.exit_price, t.sl, t.tp, t.stop_price, t.risk, t.risk_amount, t.pnl, t.fees,
       t.r_multiple, t.gross_r, t.mfe_r, t.mae_r, t.exit_reason, t.be_armed, t.gap_affected, (s.trade_id IS NOT NULL) AS has_snapshot,
       (f.trade_id IS NOT NULL) AS is_favorite
  FROM trades t LEFT JOIN trade_snapshots s ON s.trade_id = t.id LEFT JOIN favorites f ON f.trade_id = t.id
 WHERE {where}
 ORDER BY t.signal_time DESC, t.id
 LIMIT ${limit} OFFSET ${offset}
"""
TRADE_ONE_SQL = "SELECT * FROM trades WHERE id = $1"
CLOSES_SQL = "SELECT seq, ts, reason, frac, qty, price, gross_r, fees_r, net_r, pnl, fees FROM trade_closes WHERE trade_id = $1 ORDER BY seq"
SNAPSHOT_SQL = "SELECT created_at, candles, geometry, meta FROM trade_snapshots WHERE trade_id = $1"
TRADE_EVENTS_SQL = "SELECT ts, coin, strategy, strategy_id, signal_id, from_state, to_state, kind, reason, detail FROM state_events WHERE signal_id = $1 ORDER BY id"
SETUP_PATH_SQL = """
SELECT ts, coin, strategy, strategy_id, signal_id, from_state, to_state, kind, reason, detail FROM state_events
 WHERE kind = 'setup_state' AND coin = $1 AND strategy = $2 AND to_state LIKE $3 AND ts <= $4
 ORDER BY ts DESC, id DESC LIMIT 12
"""
STATS_SQL = """
SELECT id, coin, strategy_id, side, exit_reason, r_multiple, pnl, exit_time FROM trades
 WHERE status = 'closed' AND NOT gap_affected AND {where} ORDER BY exit_time, id
"""
COUNTS_SQL = "SELECT status, gap_affected, count(*) AS n FROM trades WHERE (mode = $1 OR $1 = 'all') GROUP BY status, gap_affected"
STRATEGIES_SQL = "SELECT DISTINCT strategy_id FROM trades WHERE (mode = $1 OR $1 = 'all') ORDER BY strategy_id"


async def _init_connection(conn: asyncpg.Connection) -> None:
    # asyncpg по умолчанию отдаёт JSONB строкой; нужны готовые объекты (свечи, геометрия, detail)
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def connect_pool(dsn: str, *, attempts: int = 10, delay_s: float = 2.0) -> asyncpg.Pool:
    """Пул только для чтения. БД может подниматься параллельно с API (compose): ограниченное число попыток, затем ошибка."""
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(
                dsn, min_size=1, max_size=5, command_timeout=15, init=_init_connection,
                server_settings={"default_transaction_read_only": "on", "application_name": "tradeai-api"})
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as exc:
            last = exc
            log.warning("PostgreSQL недоступен (попытка %d/%d): %s", attempt, attempts, type(exc).__name__)
            if attempt < attempts:
                await asyncio.sleep(delay_s)
    raise ConnectionError(f"PostgreSQL недоступен после {attempts} попыток: {type(last).__name__}") from last


def where_clause(prefix: str, mode: str, statuses=None, coins=None, strategies=None, sides=None, since: datetime | None = None,
                 since_column: str = "signal_time") -> tuple[str, list[Any]]:
    """WHERE по общим фильтрам. Значения - только параметрами ($n), в текст SQL попадают лишь имена колонок из кода."""
    args: list[Any] = [mode]
    parts = [f"({prefix}mode = $1 OR $1 = 'all')"]
    for column, values in (("status", statuses), ("coin", coins), ("strategy_id", strategies), ("side", sides)):
        if values:
            args.append(list(values))
            parts.append(f"{prefix}{column} = ANY(${len(args)}::text[])")
    if since is not None:
        args.append(since if since.tzinfo else since.replace(tzinfo=timezone.utc))
        parts.append(f"{prefix}{since_column} >= ${len(args)}")
    return " AND ".join(parts), args


def create_app(settings: Settings, *, analyzer_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    throttle = FailureThrottle(settings.auth_max_failures, settings.auth_window_s)
    state: dict[str, Any] = {"pool": None, "write_pool": None, "hub": None, "ws": 0}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        pool = await connect_pool(settings.database_url)
        write_pool = await favorites.connect_write_pool(settings.database_url)
        hub = LiveStateHub(pool, settings)
        await hub.start()
        state["pool"], state["write_pool"], state["hub"] = pool, write_pool, hub
        log.info("API запущен: монеты %s, опрос БД каждые %.1f с", ",".join(settings.coins), settings.poll_interval_s)
        try:
            yield
        finally:
            await hub.stop()
            await pool.close()
            await write_pool.close()

    app = FastAPI(title="TradeAI monitor", version="1", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.hub_state = state  # для тестов

    async def auth(request: Request) -> None:
        check_key(request, settings.api_key, throttle)

    def pool() -> asyncpg.Pool:
        return state["pool"]

    favorites.install(app, settings, auth, pool, lambda: state["write_pool"])  # PUT/DELETE /api/favorites/{id}: единственная запись API в БД
    updates.install(app, settings.update_dir, auth)  # /api/update/version.json, /api/update/TradeAI-Monitor.exe
    analysis_proxy.install(app, settings, auth, analyzer_transport)  # /api/analysis/* -> Strategy Quant Analyzer (отдельный контейнер, наружу не публикуется)

    # ---------------------------------------------------------------------------------- ошибки

    @app.exception_handler(asyncpg.PostgresError)
    @app.exception_handler(asyncpg.InterfaceError)
    @app.exception_handler(asyncio.TimeoutError)
    @app.exception_handler(OSError)
    async def database_error(request: Request, exc: Exception) -> JSONResponse:
        log.error("%s %s: ошибка БД %s", request.method, request.url.path, type(exc).__name__)
        return JSONResponse({"detail": "database_error"}, status_code=503)

    @app.exception_handler(DataIntegrityError)
    async def integrity_error(request: Request, exc: DataIntegrityError) -> JSONResponse:
        log.error("%s %s: данные в БД нечитаемы: %s", request.method, request.url.path, exc)
        return JSONResponse({"detail": "data_integrity"}, status_code=500)

    async def _is_favorite(trade_id: str) -> bool:
        async with pool().acquire() as conn:
            return await conn.fetchval(favorites.EXISTS_SQL, trade_id) is not None

    # ---------------------------------------------------------------------------------- REST

    @app.get("/api/health")
    async def health() -> JSONResponse:
        """Без ключа и без подробностей: для healthcheck контейнера. 503, если БД недоступна или хаб не получает данные."""
        hub: LiveStateHub | None = state["hub"]
        ok = hub is not None and hub.healthy
        return JSONResponse({"status": "ok" if ok else "degraded"}, status_code=200 if ok else 503)

    @app.get("/api/info", dependencies=[Depends(auth)])
    async def info(mode: Mode = "shadow") -> dict[str, Any]:
        async with pool().acquire() as conn:
            version = await conn.fetchval("SELECT max(version) FROM schema_migrations")
            rows = await conn.fetch(COUNTS_SQL, mode)
            seen = [r["strategy_id"] for r in await conn.fetch(STRATEGIES_SQL, mode)]
        hb = freshest_heartbeat_age_s([settings.shadow_data_dir, settings.live_data_dir])
        return {"server_time": iso(datetime.now(timezone.utc)), "mode": mode, "schema_version": version, "coins": list(current_coins(settings)),
                "strategies": sorted({*DEFAULT_STRATEGIES, *seen}),  # настроенные + все, по которым есть сделки (в т.ч. прежние smc / breakout)
                "counts": counts_from_rows(rows), "account": read_account([settings.live_data_dir]),
                "shadow": {"heartbeat_age_s": hb, "alive": None if hb is None else hb < 120.0}}

    @app.get("/api/stats", dependencies=[Depends(auth)])
    async def stats(mode: Mode = "shadow", coin: Annotated[list[Token] | None, Query()] = None,
                    strategy: Annotated[list[Token] | None, Query()] = None, side: Annotated[list[Side] | None, Query()] = None,
                    since: datetime | None = None) -> dict[str, Any]:
        where, args = where_clause("", mode, None, coin, strategy, side, since, since_column="exit_time")
        async with pool().acquire() as conn:
            async with conn.transaction(readonly=True, isolation="repeatable_read"):
                rows = await conn.fetch(STATS_SQL.format(where=where), *args)
                counts = await conn.fetch(COUNTS_SQL, mode)
        out = compute_stats([dict(r) for r in rows], settings.coins, counts_from_rows(counts))
        out["filters"] = {"mode": mode, "coin": coin, "strategy": strategy, "side": side, "since": iso(since) if since else None}
        out["server_time"] = iso(datetime.now(timezone.utc))
        return out

    @app.get("/api/trades", dependencies=[Depends(auth)])
    async def trades(limit: Annotated[int, Query(ge=1, le=500)] = 50, offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0,
                     mode: Mode = "shadow", status: Annotated[list[Status] | None, Query()] = None,
                     coin: Annotated[list[Token] | None, Query()] = None, strategy: Annotated[list[Token] | None, Query()] = None,
                     side: Annotated[list[Side] | None, Query()] = None, favorite: bool = False) -> dict[str, Any]:
        where, args = where_clause("t.", mode, status, coin, strategy, side)
        if favorite:
            where += " AND f.trade_id IS NOT NULL"  # вкладка «Избранное»: только отмеченные звёздочкой (общие для всех профилей)
        sql = TRADE_LIST_SQL.format(where=where, limit=len(args) + 1, offset=len(args) + 2)
        async with pool().acquire() as conn:
            async with conn.transaction(readonly=True, isolation="repeatable_read"):
                rows = await conn.fetch(sql, *args, limit, offset)
                total = await conn.fetchval(f"SELECT count(*) FROM trades t LEFT JOIN favorites f ON f.trade_id = t.id WHERE {where}", *args)
        items = [{**trade_out(r), "has_snapshot": r["has_snapshot"], "is_favorite": r["is_favorite"]} for r in rows]
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    @app.get("/api/candles", dependencies=[Depends(auth)])
    async def candles_endpoint(coin: Token, interval: Literal["5m", "15m", "1h", "4h"] = "15m", limit: Annotated[int, Query(ge=1, le=candles.MAX_LIMIT)] = 1000,
                               end: Annotated[int | None, Query(ge=0)] = None) -> dict[str, Any]:
        """Свечи Forex/металлов (кэш бота из MetaApi) для графика клиента; крипту клиент берёт у Binance сам. end - open_time последней нужной свечи, мс (прокрутка истории)."""
        dirs = [settings.shadow_data_dir] + ([settings.live_data_dir] if settings.live_data_dir else [])
        try:
            return await asyncio.to_thread(candles.build, dirs, coin, interval, limit, end)
        except candles.NoCandles:
            raise HTTPException(status_code=404, detail="no_candles") from None

    @app.get("/api/trades/{trade_id}/snapshot", dependencies=[Depends(auth)])
    async def trade_snapshot(trade_id: TradeId) -> dict[str, Any]:
        async with pool().acquire() as conn:
            async with conn.transaction(readonly=True, isolation="repeatable_read"):
                trade = await conn.fetchrow(TRADE_ONE_SQL, trade_id)
                if trade is None:
                    raise HTTPException(status_code=404, detail="trade_not_found")
                snap = await conn.fetchrow(SNAPSHOT_SQL, trade_id)
                closes = await conn.fetch(CLOSES_SQL, trade_id)
                events = await conn.fetch(TRADE_EVENTS_SQL, trade_id)
                path = []
                if trade["strategy_id"] in SETUP_STRATEGIES:
                    sig = trade["signal_time"]
                    path = await conn.fetch(SETUP_PATH_SQL, trade["coin"], trade["strategy_id"], f"{trade['side']}:%",
                                            sig + _ONE_MINUTE)
        if snap is None:
            raise HTTPException(status_code=404, detail="snapshot_not_found")  # снапшот пишется следом за строкой сделки: возможна короткая гонка
        fav = await _is_favorite(trade_id)
        t = {**trade_out(trade), "plan": trade["plan"], "is_favorite": fav}  # план Scale Out ({levels: [[R, доля], ...], trail_mode, ...}) нужен клиенту для уровней на графике
        geometry, meta = snap["geometry"], snap["meta"] or {}
        return {
            "trade": t, "closes": [close_out(c) for c in closes], "snapshot_created_at": iso(snap["created_at"]),
            "candles": snap["candles"], "geometry": geometry, "meta": meta,
            "reason": build_reason(t, meta, geometry), "events": [event_out(e) for e in events],
            "fills": (t.get("plan") or {}).get("fills") or [],  # реальные исполнения биржи (userTrades, мс UTC): боевые сделки, записаны при закрытии
            "setup_path": [event_out(e) for e in reversed(path)],
        }

    # ---------------------------------------------------------------------------------- WebSocket

    @app.websocket("/ws/live_state")
    async def live_state(ws: WebSocket) -> None:
        try:
            check_key(ws, settings.api_key, throttle)
        except HTTPException as exc:
            await ws.close(code=1008, reason=str(exc.detail))  # до accept: клиент получит отказ рукопожатия (403)
            return
        if state["ws"] >= MAX_WS_CLIENTS:
            await ws.close(code=1013, reason="too_many_clients")
            return
        await ws.accept()
        state["ws"] += 1
        hub: LiveStateHub = state["hub"]
        version = 0

        async def drain() -> None:  # заметить закрытие со стороны клиента; входящие сообщения игнорируются
            while (await ws.receive())["type"] != "websocket.disconnect":
                pass

        receiver = asyncio.create_task(drain())
        try:
            while True:
                if hub.version > version and hub.latest is not None:
                    version = hub.version
                    await ws.send_json(hub.latest)
                waiter = asyncio.create_task(hub.wait_newer(version, 30.0))
                done, _ = await asyncio.wait({receiver, waiter}, return_when=asyncio.FIRST_COMPLETED)
                if receiver in done:
                    waiter.cancel()
                    break
                if not waiter.result():
                    await ws.send_json({"type": "ping", "server_time": iso(datetime.now(timezone.utc))})
        except WebSocketDisconnect:
            pass  # штатное отключение клиента, не ошибка
        finally:
            state["ws"] -= 1
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)

    return app



def main() -> int:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        settings = load_settings()
    except ApiConfigError as exc:
        print(f"ОШИБКА НАСТРОЙКИ API: {exc}", file=sys.stderr)
        return 3
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_level="info", proxy_headers=False, ws_max_size=4096)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
