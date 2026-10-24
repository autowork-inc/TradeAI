"""Прокси `/api/watch/{meta|view}` -> сервис watch (Live-Dashboard, 1.8.0; playground/watch_server.py). Сервис наружу не публикуется: клиент ходит только
в основной API с X-API-Key, API пересылает GET с общим секретом X-Watch-Token (= API_KEY). Сервиса нет / таймаут - 503 / 504, мониторинг не страдает."""

from collections.abc import Callable
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Query
from fastapi.responses import JSONResponse, Response

from api.settings import Settings

TIMEOUT = httpx.Timeout(30.0, connect=3.0)  # первый расчёт взгляда (история 4 таймфреймов с Binance) - до нескольких секунд


def install(app: FastAPI, settings: Settings, auth: Callable, transport: httpx.AsyncBaseTransport | None = None) -> None:
    async def forward(path: str, params: dict) -> Response:
        if not settings.watch_url:
            return JSONResponse({"detail": "watch_not_configured"}, status_code=503)
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT, transport=transport) as http:
                resp = await http.get(f"{settings.watch_url.rstrip('/')}/{path}", params=params, headers={"X-Watch-Token": settings.api_key})
        except httpx.TimeoutException:
            return JSONResponse({"detail": "watch_timeout"}, status_code=504)
        except httpx.TransportError:
            return JSONResponse({"detail": "watch_unavailable"}, status_code=503)
        if resp.status_code == 401:
            return JSONResponse({"detail": "watch_auth_failed"}, status_code=502)
        if "application/json" not in resp.headers.get("content-type", ""):
            return JSONResponse({"detail": "watch_bad_response"}, status_code=502)
        return Response(content=resp.content, status_code=resp.status_code, media_type="application/json")

    @app.get("/api/watch/meta", dependencies=[Depends(auth)])
    async def meta() -> Response:
        return await forward("meta", {})

    @app.get("/api/watch/coins", dependencies=[Depends(auth)])
    async def coins(strategy: Annotated[str, Query(pattern=r"^[A-Za-z_]{3,40}$")]) -> Response:
        return await forward("coins", {"strategy": strategy})

    @app.get("/api/watch/view", dependencies=[Depends(auth)])
    async def view(strategy: Annotated[str, Query(pattern=r"^[A-Za-z_]{3,40}$")], symbol: Annotated[str, Query(pattern=r"^[A-Z0-9]{1,20}USDT$")]) -> Response:
        return await forward("view", {"strategy": strategy, "symbol": symbol})

