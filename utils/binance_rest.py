"""Минимальный REST-клиент Binance Futures: статусы -> исключения, контроль веса запросов."""

import asyncio
import json
import logging
import time
from typing import Any, Mapping

import aiohttp

import config
from utils.errors import (
    DataError,
    HttpClientError,
    HttpServerError,
    NetworkError,
    RateLimitError,
)

log = logging.getLogger(__name__)

_WEIGHT_HEADER = "X-MBX-USED-WEIGHT-1M"


def klines_weight(limit: int) -> int:
    """Вес запроса /fapi/v1/klines по таблице Binance: [1,100)->1, [100,500)->2, [500,1000]->5, >1000->10."""
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def depth_weight(limit: int) -> int:
    """Вес /fapi/v1/depth (проверено вживую): 5..50 -> 2, 100 -> 5, 500 -> 10, 1000 -> 20."""
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20


def parse_retry_after(headers: Mapping[str, str], status: int) -> int:
    default = (
        config.RETRY_AFTER_DEFAULT_418_S if status == 418 else config.RETRY_AFTER_DEFAULT_429_S
    )
    raw = headers.get("Retry-After")
    if raw is None:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        return default
    return max(1, min(value, config.RETRY_AFTER_MAX_S))


class BinanceRest:
    def __init__(self, session: aiohttp.ClientSession, base_url: str):
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._used_weight = 0
        self._weight_minute = int(time.time() // 60)

    async def _throttle(self, cost: int) -> None:
        """Не доводим до 429: у порога ждём начала следующей минуты, когда вес обнуляется."""
        now = time.time()
        minute = int(now // 60)
        if minute != self._weight_minute:
            self._weight_minute = minute
            self._used_weight = 0
        if self._used_weight + cost > config.WEIGHT_SOFT_LIMIT_1M:
            wait = 60 - (now % 60) + 1
            log.info(
                "Вес запросов %d/%d, жду %.0f с до следующей минуты",
                self._used_weight, config.WEIGHT_LIMIT_1M, wait,
            )
            await asyncio.sleep(wait)
            self._weight_minute = int(time.time() // 60)
            self._used_weight = 0

    def _record_weight(self, headers: Mapping[str, str], cost: int) -> None:
        self._weight_minute = int(time.time() // 60)
        raw = headers.get(_WEIGHT_HEADER)
        try:
            self._used_weight = int(raw) if raw is not None else self._used_weight + cost
        except ValueError:
            self._used_weight += cost

    async def get(self, path: str, params: Mapping[str, Any] | None = None, weight: int = 1) -> Any:
        """GET с разбором статуса. Любой неуспех - исключение, без повторов."""
        await self._throttle(weight)
        url = self._base_url + path
        try:
            async with self._session.get(url, params=params) as resp:
                self._record_weight(resp.headers, weight)
                body = await resp.text()
                status = resp.status
                headers = resp.headers
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as exc:
            raise NetworkError(f"{type(exc).__name__} при запросе {url}: {exc}") from exc

        if status == 200:
            try:
                return json.loads(body)
            except ValueError as exc:
                raise DataError(f"Ответ {url} не является JSON: {body[:200]!r}") from exc
        if status in (429, 418):
            raise RateLimitError(status, parse_retry_after(headers, status), url)
        if 400 <= status < 500:
            raise HttpClientError(status, url, body[:300])
        raise HttpServerError(status, url, body[:300])

    async def server_time_ms(self) -> int:
        data = await self.get(config.SERVER_TIME_PATH)
        if not isinstance(data, dict) or not isinstance(data.get("serverTime"), int):
            raise DataError(f"Неожиданный ответ {config.SERVER_TIME_PATH}: {str(data)[:200]}")
        return data["serverTime"]
