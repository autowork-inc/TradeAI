"""Общий читатель WebSocket для фидов оркестратора: переподключение с backoff, Fail-Fast на всём остальном.

Логика повторяет WsLiveFeatureFeed._stream_loop (execution/ws_live_features.py), вынесенную в отдельный класс, чтобы её
разделяли фид свечей и фид тиков. Переподключаемся ТОЛЬКО на ожидаемые сетевые сбои (_RECONNECTABLE); мусорное
сообщение (DataError), 429/418 (RateLimitError) и исчерпание попыток - исключение наружу, процесс падает (R1).
"""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import aiohttp

import config
from collectors.depth_stream import connect
from utils.errors import (
    DataError,
    HttpServerError,
    NetworkError,
    ServerShutdown,
    StreamStalled,
    WebSocketClosed,
)

log = logging.getLogger("orchestrator.ws")

RECONNECTABLE = (WebSocketClosed, NetworkError, HttpServerError, StreamStalled, ServerShutdown)
T = TypeVar("T")


def wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def decode_combined(raw: str, expected: dict[str, T]) -> tuple[str, T, dict[str, Any]]:
    """Кадр комбинированного потока -> (имя потока, значение expected[имя], data). Неожиданное - DataError."""
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise DataError(f"Сообщение не JSON: {raw[:200]!r}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"Сообщение не объект: {raw[:200]!r}")
    data = payload.get("data")
    if payload.get("e") == "serverShutdown" or (isinstance(data, dict) and data.get("e") == "serverShutdown"):
        raise ServerShutdown(f"Биржа сообщила об отключении: {raw[:200]}")
    stream = payload.get("stream")
    if stream not in expected or not isinstance(data, dict):
        raise DataError(f"Неожиданное сообщение (stream={stream!r}): {raw[:200]!r}")
    return stream, expected[stream], data


class ReconnectingStream:
    def __init__(
        self, label: str, url: str, session: aiohttp.ClientSession, handler: Callable[[str, int], None], *,
        on_connected: Callable[[], Awaitable[None]] | None = None, on_gap: Callable[[int], None] | None = None,
        clock_ms: Callable[[], int] = wall_clock_ms, max_reconnects: int | None = None,
        backoff_s: float | None = None, backoff_max_s: float | None = None,
    ):
        self.label, self._url, self._session, self._handler = label, url, session, handler
        self._on_connected, self._on_gap, self._clock = on_connected, on_gap, clock_ms
        self._max = max_reconnects if max_reconnects is not None else config.LIVE_WS_MAX_RECONNECTS
        self._backoff = backoff_s if backoff_s is not None else config.LIVE_WS_BACKOFF_S
        self._backoff_max = backoff_max_s if backoff_max_s is not None else config.LIVE_WS_BACKOFF_MAX_S
        self.received = 0
        self.reconnects = 0

    async def run(self) -> None:
        failures = 0
        while True:
            before = self.received
            try:
                ws = await connect(self._session, self._url)
                try:
                    log.info("WS %s подключён: %s", self.label, self._url)
                    if self._on_connected is not None:
                        await self._on_connected()
                    await self._consume(ws)
                finally:
                    await ws.close()
            except RECONNECTABLE as exc:
                if self.received > before:
                    failures = 0  # соединение жило и отдавало данные: это новый обрыв, а не продолжение серии
                failures += 1
                if self._on_gap is not None:
                    self._on_gap(self._clock())
                if failures > self._max:
                    raise WebSocketClosed(f"Поток {self.label} не восстановился за {self._max} попыток: {exc}") from exc
                delay = min(self._backoff_max, self._backoff * 2 ** (failures - 1))
                log.warning("WS %s: %s: %s; переподключение %d/%d через %.1f с", self.label, type(exc).__name__, exc,
                            failures, self._max, delay)
                self.reconnects += 1
                await asyncio.sleep(delay)

    async def _consume(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            try:
                msg = await ws.receive(timeout=config.WS_STALL_TIMEOUT_S)
            except asyncio.TimeoutError as exc:
                raise StreamStalled(f"Нет данных {config.WS_STALL_TIMEOUT_S} с при открытом соединении") from exc
            recv_ts = self._clock()
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handler(msg.data, recv_ts)
                self.received += 1
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                raise WebSocketClosed(f"Соединение {self.label} закрыто (код {ws.close_code}), принято {self.received}")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise NetworkError(f"Ошибка WebSocket {self.label}: {ws.exception()!r}")
            else:
                raise DataError(f"Неожиданный тип кадра WebSocket {self.label}: {msg.type!r}")
