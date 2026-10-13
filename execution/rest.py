"""Подписанный REST-клиент Binance Futures (USER_TRADE / USER_DATA / USER_STREAM).

Отличия от utils.binance_rest.BinanceRest (публичный GET без повторов):
  * HMAC-SHA256 подпись; строка запроса подписывается и уходит ровно в том виде, в каком подписана
    (yarl.URL(encoded=True): aiohttp ничего не перекодирует, иначе подпись не сойдётся);
  * методы POST / PUT / DELETE; ключ API - в заголовке X-MBX-APIKEY;
  * ошибки биржи с телом {"code": -2013, "msg": ...} -> BinanceAPIError(.code) - по коду решает вызывающий;
  * -1021 (часы разошлись): один раз пересчитывается смещение по /fapi/v1/time и запрос повторяется.
    Это безопасно: при -1021 биржа запрос не исполняла.

Что клиент НЕ делает: повторов сетевых ошибок. Для ордеров повтор небезопасен без сверки по
clientOrderId, поэтому решение принимает live_barrier. 429/418 -> RateLimitError (его превращает в
.ban_lock общий runner), 5xx и сетевые сбои -> HttpServerError / NetworkError: «исход неизвестен».
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import aiohttp
from yarl import URL

import config
from utils.binance_rest import parse_retry_after
from utils.errors import (
    ConfigError,
    DataError,
    HttpClientError,
    HttpServerError,
    NetworkError,
    RateLimitError,
)

log = logging.getLogger(__name__)

# Коды ошибок Binance, на которые опирается код (остальные - обычный отказ).
ERR_TIMESTAMP = -1021  # timestamp вне recvWindow: запрос не исполнялся
ERR_ORDER_NOT_FOUND = -2013  # ордера нет (в т.ч. «ещё не дошёл»)
ERR_CANCEL_UNKNOWN = -2011  # отмена: ордер неизвестен / уже закрыт
ERR_BAD_LISTEN_KEY = -1125  # listenKey не существует / истёк
ERR_POST_ONLY_REJECT = -5022  # GTX: ордер стал бы тейкером и не принят (на книге его не было)
# Algo Service. Коды сверены с документацией только частично: -2021 назван на странице New Algo Order (TRAILING_STOP_MARKET:
# «Order would immediately trigger»); -4130 - из практики classic futures (второй closePosition-стоп в том же направлении),
# для Algo Service ❓ не проверено. Код отказа отмены неизвестного algo-ордера в документации не указан: решение по нему
# принимается по запросу статуса (GET algoOrder), а не по коду.
ERR_WOULD_TRIGGER = -2021
ERR_CLOSE_POSITION_EXISTS = -4130


@dataclass(frozen=True)
class Credentials:
    api_key: str
    api_secret: str = field(repr=False)  # секрет не попадает ни в repr, ни в логи

    def __post_init__(self) -> None:
        if not self.api_key or not self.api_secret:
            raise ConfigError("Пустой ключ или секрет Binance API")

    @classmethod
    def from_env(cls) -> "Credentials":
        key, secret = os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", "")
        if not key or not secret:
            raise ConfigError("Задайте переменные окружения BINANCE_API_KEY и BINANCE_API_SECRET")
        return cls(key, secret)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


class BinanceAPIError(HttpClientError):
    """HTTP 4xx с телом {"code", "msg"}: биржа ответила и отказала. Запрос НЕ исполнен."""

    def __init__(self, status: int, url: str, body: str, code: int | None, msg: str):
        super().__init__(status, url, body)
        self.code = code
        self.msg = msg


@dataclass
class RequestTrace:
    """Заполняется клиентом: timestamp, с которым подписан последний отправленный вариант запроса."""

    timestamp_ms: int | None = None


class SignedRest:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        credentials: Credentials,
        *,
        recv_window_ms: int | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._creds = credentials
        self.recv_window_ms = recv_window_ms if recv_window_ms is not None else config.RECV_WINDOW_MS
        self._clock = clock
        self._offset_ms = 0  # серверное время - локальное
        self.used_weight_1m = 0
        self.order_count_10s = 0

    def now_ms(self) -> int:
        """Время биржи (локальные часы + измеренное смещение)."""
        return int(self._clock() * 1000) + self._offset_ms

    async def sync_time(self) -> int:
        """Измеряет смещение часов по /fapi/v1/time (середина интервала запроса). Возвращает смещение, мс."""
        t0 = self._clock()
        data = await self.request("GET", config.SERVER_TIME_PATH, signed=False)
        t1 = self._clock()
        if not isinstance(data, dict) or not isinstance(data.get("serverTime"), int):
            raise DataError(f"Неожиданный ответ {config.SERVER_TIME_PATH}: {str(data)[:200]}")
        self._offset_ms = data["serverTime"] - int((t0 + t1) / 2 * 1000)
        return self._offset_ms

    async def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        signed: bool = True,
        trace: RequestTrace | None = None,
    ) -> Any:
        try:
            return await self._request_once(method, path, params, signed, trace)
        except BinanceAPIError as exc:
            if exc.code != ERR_TIMESTAMP or not signed:
                raise
        # -1021: запрос отвергнут до исполнения, часы разошлись - меряем смещение и пробуем один раз
        log.warning("%s %s: -1021, пересчитываю смещение часов", method, path)
        await self.sync_time()
        return await self._request_once(method, path, params, signed, trace)

    async def _request_once(self, method, path, params, signed, trace) -> Any:
        items = [(k, _wire(v)) for k, v in (params or {}).items() if v is not None]
        if signed:
            ts = self.now_ms()
            if trace is not None:
                trace.timestamp_ms = ts
            items += [("recvWindow", str(self.recv_window_ms)), ("timestamp", str(ts))]
        query = urlencode(items)
        if signed:
            query += "&signature=" + sign(query, self._creds.api_secret)
        url = f"{self._base_url}{path}"
        headers = {"X-MBX-APIKEY": self._creds.api_key}
        try:
            async with self._session.request(
                method, URL(f"{url}?{query}" if query else url, encoded=True), headers=headers
            ) as resp:
                status, body, rheaders = resp.status, await resp.text(), resp.headers
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as exc:
            # Запрос мог дойти до биржи: «исход неизвестен» - см. live_barrier
            raise NetworkError(f"{type(exc).__name__} при {method} {url}: {exc}") from exc

        self._record_limits(rheaders)
        if status == 200:
            try:
                return json.loads(body)
            except ValueError as exc:
                raise DataError(f"Ответ {url} не является JSON: {body[:200]!r}") from exc
        if status in (429, 418):
            raise RateLimitError(status, parse_retry_after(rheaders, status), url)
        if 400 <= status < 500:
            code, msg = _error_body(body)
            raise BinanceAPIError(status, url, body[:300], code, msg)
        raise HttpServerError(status, url, body[:300])

    def _record_limits(self, headers: Mapping[str, str]) -> None:
        for header, attr in (("X-MBX-USED-WEIGHT-1M", "used_weight_1m"), ("X-MBX-ORDER-COUNT-10S", "order_count_10s")):
            try:
                if (raw := headers.get(header)) is not None:
                    setattr(self, attr, int(raw))
            except ValueError:
                pass
        if self.used_weight_1m > config.WEIGHT_SOFT_LIMIT_1M:
            log.warning("Вес запросов %d/%d: близко к 429", self.used_weight_1m, config.WEIGHT_LIMIT_1M)


def _wire(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _error_body(body: str) -> tuple[int | None, str]:
    try:
        data = json.loads(body)
        code = data.get("code")
        return (code if isinstance(code, int) else None), str(data.get("msg", ""))
    except (ValueError, AttributeError):
        return None, body[:200]
