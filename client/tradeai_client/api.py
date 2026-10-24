"""Клиент API мониторинга: REST (aiohttp) и WebSocket живого состояния с переподключением.

Ключ ходит только в заголовке X-API-Key (не в адресе). Ошибки сервера превращаются в `ApiError` с понятным видом (`kind`), чтобы
интерфейс показывал причину («неверный ключ», «сервер недоступен»), а не трассировку.
"""

import asyncio
import base64
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import quote

import aiohttp

from tradeai_client.settings import ClientSettings

log = logging.getLogger("tradeai_client.api")
HEADER = "X-API-Key"
RECONNECT_DELAYS = (1, 2, 4, 8, 15)  # секунды; после последней задержка остаётся 15 с


class ApiError(Exception):
    """kind: auth (401) | throttled (429) | not_found (404) | conflict (409) | server (5xx/503) | analyzer (анализатор недоступен/отказал) | network | protocol | bad_request (422)."""

    def __init__(self, kind: str, message: str, status: int | None = None):
        super().__init__(message)
        self.kind, self.status = kind, status


_HINTS = {
    "auth": "Неверный API-ключ. Его показывает на сервере: ./deploy/shadow.sh apikey",
    "throttled": "Слишком много неверных попыток: сервер временно блокирует вход. Подождите минуту.",
    "network": "Сервер недоступен. Проверьте адрес и порт (по умолчанию http://193.233.75.254:8001), доступ к порту в файрволе сервера и что контейнер api запущен (если задан адрес 127.0.0.1 - что запущен SSH-туннель).",
    "server": "Сервер API получил ошибку базы данных (shadow-run/postgres недоступны?). Смотрите ./deploy/shadow.sh logs api",
}


_ANALYZER_HINTS = {
    "analyzer_unavailable": "Анализатор не запущен. На сервере: ./deploy/shadow.sh analyzer-up (бот и мониторинг работают как раньше).",
    "analyzer_not_configured": "API не знает адрес анализатора (ANALYZER_URL): обновите docker-compose.yml на сервере и перезапустите api.",
    "analyzer_timeout": "Анализатор не ответил вовремя. Смотрите ./deploy/shadow.sh logs analyzer",
    "analyzer_auth_failed": "Анализатор отклонил секрет API (ANALYZER_TOKEN расходится с API_KEY): пересоздайте контейнеры api и analyzer.",
    "analyzer_bad_response": "Анализатор вернул не JSON: смотрите ./deploy/shadow.sh logs analyzer",
}


def _detail(body: Any) -> str:
    return str(body.get("detail")) if isinstance(body, dict) and "detail" in body else ""


class ApiClient:
    def __init__(self, settings: ClientSettings, session: aiohttp.ClientSession | None = None, timeout_s: float = 15.0):
        self._s = settings.validate()
        self._owns = session is None
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)  # ключ добавляется к каждому запросу отдельно (_get)
        return self._session

    async def close(self) -> None:
        if self._owns and self._session is not None:
            await self._session.close()

    async def _get(self, path: str, params: list[tuple[str, str]] | None = None) -> Any:
        return await self._request("GET", path, params)

    async def _request(self, method: str, path: str, params: list[tuple[str, str]] | None = None, *, json_body: Any = None, data: bytes | None = None,
                       content_type: str | None = None) -> Any:
        url = self._s.normalized_url + path
        headers = {HEADER: self._s.api_key}
        if content_type:
            headers["Content-Type"] = content_type
        try:
            async with self.session.request(method, url, params=params, headers=headers, json=json_body, data=data) as resp:
                try:
                    body = await resp.json(content_type=None)
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as exc:
                    raise ApiError("protocol", f"Сервер вернул не JSON (HTTP {resp.status}): адрес указывает на API?", resp.status) from exc
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
            raise ApiError("network", _HINTS["network"]) from exc
        if resp.status in (200, 201, 202):
            return body
        detail = _detail(body)
        if detail in _ANALYZER_HINTS:  # отказ анализатора через прокси API: это не «неверный ключ» и не «ошибка БД»
            raise ApiError("analyzer", _ANALYZER_HINTS[detail], resp.status)
        if resp.status == 401:
            raise ApiError("auth", _HINTS["auth"], 401)
        if resp.status == 429:
            raise ApiError("throttled", _HINTS["throttled"], 429)
        if resp.status == 404:
            raise ApiError("not_found", f"Не найдено: {detail or path}", 404)
        if resp.status == 409:
            raise ApiError("conflict", detail or "Конфликт состояния (повторите позже)", 409)
        if resp.status == 422:
            raise ApiError("bad_request", f"Сервер отклонил параметры запроса: {detail}", 422)
        raise ApiError("server", f"{_HINTS['server']} (HTTP {resp.status}: {detail})", resp.status)

    # ------------------------------------------------------------------------------------------ методы

    async def info(self) -> dict[str, Any]:
        return await self._get("/api/info")

    async def stats(self, *, coins: list[str] | None = None, strategies: list[str] | None = None, sides: list[str] | None = None,
                    since: str | None = None, mode: str = "shadow") -> dict[str, Any]:
        params = [("mode", mode)]
        params += [("coin", c) for c in coins or []] + [("strategy", s) for s in strategies or []] + [("side", s) for s in sides or []]
        if since:
            params.append(("since", since))
        return await self._get("/api/stats", params)

    async def trades(self, *, limit: int = 50, offset: int = 0, statuses: list[str] | None = None, coins: list[str] | None = None,
                     strategies: list[str] | None = None, sides: list[str] | None = None, mode: str = "shadow", favorite: bool = False) -> dict[str, Any]:
        params = [("limit", str(limit)), ("offset", str(offset)), ("mode", mode)]
        if favorite:
            params.append(("favorite", "true"))
        params += [("status", s) for s in statuses or []] + [("coin", c) for c in coins or []]
        params += [("strategy", s) for s in strategies or []] + [("side", s) for s in sides or []]
        return await self._get("/api/trades", params)

    async def snapshot(self, trade_id: str) -> dict[str, Any]:
        return await self._get(f"/api/trades/{quote(trade_id, safe=':')}/snapshot")

    # ---- Избранное (Golden Set): звёздочка хранится на сервере и общая для профилей

    async def favorite_add(self, trade_id: str, profile: str, png: bytes | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"profile": profile}
        if png:
            body["image_png_b64"] = base64.b64encode(png).decode("ascii")
        return await self._request("PUT", f"/api/favorites/{quote(trade_id, safe=':')}", json_body=body)

    async def favorite_remove(self, trade_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/api/favorites/{quote(trade_id, safe=':')}")

    # ---- автообновление клиента

    async def update_version(self) -> dict[str, Any] | None:
        """version.json с сервера; ничего не опубликовано (404) - None."""
        try:
            return await self._get("/api/update/version.json")
        except ApiError as exc:
            if exc.kind == "not_found":
                return None
            raise

    async def download_update(self):
        """Асинхронный итератор кусков TradeAI-Monitor.exe. Общего таймаута нет (файл ~100 МБ): только на паузу в потоке."""
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=60)
        try:
            async with self.session.get(self._s.normalized_url + "/api/update/TradeAI-Monitor.exe", headers={HEADER: self._s.api_key}, timeout=timeout) as resp:
                if resp.status != 200:
                    raise ApiError("not_found" if resp.status == 404 else "server", f"Сервер не отдал файл обновления (HTTP {resp.status})", resp.status)
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    yield chunk
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
            raise ApiError("network", _HINTS["network"]) from exc

    # ---- MFE Simulator (api/mfe_sim.py): доигрывание всех сделок с входом по 1m-свечам Binance на сервере

    async def mfe_run(self, since: str) -> dict[str, Any]:
        return await self._request("POST", "/api/mfe/run", [("since", since)])

    async def mfe_status(self) -> dict[str, Any]:
        return await self._get("/api/mfe/status")

    # ---- Strategy Quant Analyzer (через прокси основного API: /api/analysis/*)

    async def analysis_status(self) -> dict[str, Any]:
        return await self._get("/api/analysis/status")

    async def analysis_runs(self, limit: int = 10) -> dict[str, Any]:
        return await self._get("/api/analysis/runs", [("limit", str(limit))])

    async def analysis_start(self, body: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/api/analysis/runs", json_body=body)

    async def analysis_job(self, job_id: str) -> dict[str, Any]:
        return await self._get(f"/api/analysis/runs/{quote(job_id, safe='')}")

    async def analysis_report(self, job_id: str) -> dict[str, Any]:
        return await self._get(f"/api/analysis/runs/{quote(job_id, safe='')}/report")

    async def analysis_dry_run(self, run_id: str, recommendation_ids: list[str] | None = None) -> dict[str, Any]:
        return await self._request("POST", "/api/analysis/dry-run", json_body={"run_id": run_id, "recommendation_ids": recommendation_ids})


class LiveStateStream:
    """WebSocket /ws/live_state: сообщения -> `on_message(dict)`, состояние соединения -> `on_status(kind, text)`.

    Обрыв - переподключение с нарастающей паузой. Неверный ключ (отказ рукопожатия 401/403/429) - остановка без бесконечных попыток:
    иначе клиент сам исчерпал бы лимит неверных попыток на сервере. `run()` завершается при `stop()` или отказе по ключу.
    """

    def __init__(self, settings: ClientSettings, on_message: Callable[[dict], Awaitable[None] | None],
                 on_status: Callable[[str, str], Awaitable[None] | None], session: aiohttp.ClientSession | None = None):
        self._s = settings.validate()
        self._on_message, self._on_status = on_message, on_status
        self._session, self._owns = session, session is None
        self._stop = asyncio.Event()
        self._ws: aiohttp.ClientWebSocketResponse | None = None

    async def _emit(self, fn: Callable, *args: Any) -> None:
        res = fn(*args)
        if asyncio.iscoroutine(res):
            await res

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def run(self) -> None:
        session = self._session or aiohttp.ClientSession()
        failures = 0
        try:
            while not self._stop.is_set():
                try:
                    await self._emit(self._on_status, "connecting", "Подключение…")
                    async with session.ws_connect(self._s.ws_url, headers={HEADER: self._s.api_key}, heartbeat=20.0, max_msg_size=2_000_000) as ws:
                        self._ws, failures = ws, 0
                        await self._emit(self._on_status, "connected", "Подключено")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._emit(self._on_message, json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                                break
                except aiohttp.WSServerHandshakeError as exc:
                    if exc.status in (401, 403, 429):
                        kind = "throttled" if exc.status == 429 else "auth"
                        await self._emit(self._on_status, kind, _HINTS[kind])
                        return
                    await self._emit(self._on_status, "error", f"Сервер отклонил соединение (HTTP {exc.status})")
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                    log.warning("WebSocket оборван: %s", type(exc).__name__)
                if self._stop.is_set():
                    return
                delay = RECONNECT_DELAYS[min(failures, len(RECONNECT_DELAYS) - 1)]
                failures += 1
                await self._emit(self._on_status, "reconnecting", f"Связь потеряна, повтор через {delay} с")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
        finally:
            if self._owns:
                await session.close()
