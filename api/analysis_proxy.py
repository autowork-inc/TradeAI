"""Прокси `/api/analysis/*` -> Strategy Quant Analyzer (CLAUDE.md разд. 3.17). Анализатор наружу не публикуется: Monitor ходит ТОЛЬКО в основной API
(тот же адрес и ключ X-API-Key), а API по внутренней сети compose пересылает запрос анализатору с общим секретом X-Analyzer-Token.

Принципы:
  * белый список путей и методов (status, runs, versions, calendar, dry-run); любые другие пути - 404, `..` и странные символы - 404;
  * ключ клиента анализатору НЕ пересылается; ответ 401 от анализатора (рассинхрон секретов) превращается в 502 `analyzer_auth_failed`, чтобы клиент не принял
    его за «неверный API-ключ» и не открыл окно настроек;
  * анализатор не запущен / тайм-аут - 503 `analyzer_unavailable` / 504 `analyzer_timeout`: мониторинг бота от этого не страдает.
"""

import re
from collections.abc import Callable

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from api.settings import Settings

ALLOWED: dict[str, set[str]] = {"status": {"GET"}, "runs": {"GET", "POST"}, "versions": {"GET", "POST", "DELETE"}, "calendar": {"GET"}, "dry-run": {"POST"}}
SEGMENT = re.compile(r"^[A-Za-z0-9 _.@+()-]{1,60}$")
MAX_BODY = 5_000_000  # календарь новостей - самый большой запрос
TIMEOUT = httpx.Timeout(20.0, connect=3.0)
FORWARD_QUERY_MAX = 500


def _validate(path: str, method: str) -> None:
    parts = path.split("/")
    if not 1 <= len(parts) <= 3 or any(p in ("", ".", "..") or not SEGMENT.match(p) for p in parts) or parts[0] not in ALLOWED:
        raise HTTPException(status_code=404, detail="not_found")
    if method not in ALLOWED[parts[0]]:
        raise HTTPException(status_code=405, detail="method_not_allowed")


def install(app: FastAPI, settings: Settings, auth: Callable, transport: httpx.AsyncBaseTransport | None = None) -> None:
    @app.api_route("/api/analysis/{path:path}", methods=["GET", "POST", "DELETE"], dependencies=[Depends(auth)])
    async def analysis(path: str, request: Request) -> Response:
        _validate(path, request.method)
        if not settings.analyzer_url:
            return JSONResponse({"detail": "analyzer_not_configured"}, status_code=503)
        body = b""
        if request.method in ("POST", "DELETE"):
            if int(request.headers.get("content-length") or 0) > MAX_BODY:
                raise HTTPException(status_code=413, detail="body_too_large")
            body = await request.body()
            if len(body) > MAX_BODY:
                raise HTTPException(status_code=413, detail="body_too_large")
        query = request.url.query
        if len(query) > FORWARD_QUERY_MAX:
            raise HTTPException(status_code=414, detail="query_too_long")
        url = f"{settings.analyzer_url.rstrip('/')}/{path}" + (f"?{query}" if query else "")
        headers = {"X-Analyzer-Token": settings.analyzer_token or settings.api_key}
        if body and request.headers.get("content-type"):
            headers["Content-Type"] = request.headers["content-type"]
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT, transport=transport) as http:
                resp = await http.request(request.method, url, content=body or None, headers=headers)
        except httpx.TimeoutException:
            return JSONResponse({"detail": "analyzer_timeout"}, status_code=504)
        except httpx.TransportError:
            return JSONResponse({"detail": "analyzer_unavailable"}, status_code=503)
        if resp.status_code == 401:
            return JSONResponse({"detail": "analyzer_auth_failed"}, status_code=502)
        if "application/json" not in resp.headers.get("content-type", ""):
            return JSONResponse({"detail": "analyzer_bad_response"}, status_code=502)
        return Response(content=resp.content, status_code=resp.status_code, media_type="application/json")
