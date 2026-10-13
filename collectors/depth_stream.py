"""Запись стакана Binance USDⓈ-M Futures в реальном времени -> data/raw/depth/<SYMBOL>/<YYYY-MM-DD>/<HH>.jsonl.

Запуск (из корня проекта):  python -m collectors.depth_stream

Порядок работы:
  1. проверка .ban_lock (бан не истёк -> код 2, без запросов);
  2. замок .instance_lock.depth_stream (второй экземпляр -> код 1);
  3. REST-снапшот /fapi/v1/depth по каждому тикеру -> первая запись в jsonl (stream="snapshot");
  4. одно комбинированное WebSocket-соединение на все тикеры -> записи stream="<имя потока>".

Формат строки:
    {"recv_ts": <мс UTC, часы этой машины>, "symbol": "BTCUSDT",
     "stream": "snapshot" | "btcusdt@depth10@500ms", "data": {...ответ биржи как есть...}}
У снапшота в data поля lastUpdateId/E/T/bids/asks, у сообщения потока - E/T/s/U/u/pu/b/a.

Примечание: поток @depth<N>@<speed> - partial depth, каждое сообщение уже содержит полное
состояние топ-N уровней (не дельты), так что снапшот здесь - опорная точка и глубокий срез
(до 1000 уровней), а не обязательное условие восстановления книги. Если когда-нибудь
перейти на дифф-поток @depth, снапшот придётся брать ПОСЛЕ открытия сокета и буферизации
событий (U <= lastUpdateId+1 <= u), иначе между ними теряются события.

Fail-Fast: закрытие соединения (включая плановое на 24 часах), тишина дольше
WS_STALL_TIMEOUT_S, serverShutdown, неожиданный формат - исключение и выход.
Перезапускает процесс менеджер (Docker).
"""

import asyncio
import json
import logging
import time

import aiohttp

import config
from utils import ban_lock
from utils.binance_rest import BinanceRest, depth_weight, parse_retry_after
from utils.errors import (
    DataError,
    NetworkError,
    RateLimitError,
    ServerShutdown,
    StreamStalled,
    WebSocketClosed,
)
from utils.instance_lock import InstanceLock
from utils.runner import run
from utils.storage import HourlyJsonlWriter

log = logging.getLogger("depth_stream")

SNAPSHOT_STREAM = "snapshot"
_REQUIRED_DATA_KEYS = ("s", "E", "u", "b", "a")


def stream_names() -> list[str]:
    return [
        f"{symbol.lower()}@depth{config.DEPTH_LEVELS}@{config.DEPTH_UPDATE_SPEED}"
        for symbol in config.SYMBOLS
    ]


def stream_url(names: list[str]) -> str:
    return f"{config.WS_BASE_URL}?streams={'/'.join(names)}"


def parse_depth_message(raw: str, expected: dict[str, str]) -> tuple[str, str, dict]:
    """Разбор сообщения комбинированного потока.

    expected: имя потока -> символ. Возвращает (stream, symbol, data).
    """
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise DataError(f"Сообщение не JSON: {raw[:200]!r}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"Сообщение не объект: {raw[:200]!r}")

    data = payload.get("data")
    if payload.get("e") == "serverShutdown" or (
        isinstance(data, dict) and data.get("e") == "serverShutdown"
    ):
        raise ServerShutdown(f"Биржа сообщила об отключении: {raw[:200]}")

    stream = payload.get("stream")
    if stream not in expected or not isinstance(data, dict):
        raise DataError(f"Неожиданное сообщение (stream={stream!r}): {raw[:200]!r}")
    missing = [k for k in _REQUIRED_DATA_KEYS if k not in data]
    if missing or not isinstance(data["b"], list) or not isinstance(data["a"], list):
        raise DataError(f"В сообщении {stream} нет полей {missing} или b/a не списки: {raw[:200]!r}")
    if data["s"] != expected[stream]:
        raise DataError(f"Символ {data['s']!r} не соответствует потоку {stream}")
    return stream, expected[stream], data


def validate_snapshot(symbol: str, data: object) -> dict:
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("lastUpdateId"), int)
        or not isinstance(data.get("bids"), list)
        or not isinstance(data.get("asks"), list)
        or not data["bids"]
        or not data["asks"]
    ):
        raise DataError(f"Неожиданный снапшот {symbol}: {str(data)[:200]}")
    return data


async def write_snapshots(writer: HourlyJsonlWriter) -> None:
    """REST-снапшот каждого тикера -> первые строки в jsonl. Любая ошибка - исключение, сокет не открывается."""
    timeout = aiohttp.ClientTimeout(
        total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S
    )
    weight = depth_weight(config.DEPTH_SNAPSHOT_LIMIT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        api = BinanceRest(session, config.REST_BASE_URL)
        for symbol in config.SYMBOLS:
            data = await api.get(
                config.DEPTH_SNAPSHOT_PATH,
                {"symbol": symbol, "limit": config.DEPTH_SNAPSHOT_LIMIT},
                weight=weight,
            )
            recv_ts = time.time_ns() // 1_000_000
            validate_snapshot(symbol, data)
            line = json.dumps(
                {"recv_ts": recv_ts, "symbol": symbol, "stream": SNAPSHOT_STREAM, "data": data},
                separators=(",", ":"),
            )
            writer.write(symbol, recv_ts, line)
            log.info("%s: снапшот записан, lastUpdateId=%d, уровней bid/ask %d/%d",
                     symbol, data["lastUpdateId"], len(data["bids"]), len(data["asks"]))


async def connect(session: aiohttp.ClientSession, url: str) -> aiohttp.ClientWebSocketResponse:
    try:
        return await asyncio.wait_for(
            session.ws_connect(url, autoping=True), timeout=config.WS_CONNECT_TIMEOUT_S
        )
    except aiohttp.WSServerHandshakeError as exc:
        if exc.status in (429, 418):
            raise RateLimitError(
                exc.status, parse_retry_after(exc.headers or {}, exc.status), url
            ) from exc
        raise NetworkError(f"Рукопожатие WebSocket отклонено: HTTP {exc.status} {exc.message}") from exc
    except (asyncio.TimeoutError, aiohttp.ClientError, OSError) as exc:
        raise NetworkError(f"Не удалось подключиться к {url}: {type(exc).__name__}: {exc}") from exc


async def consume(ws: aiohttp.ClientWebSocketResponse, writer: HourlyJsonlWriter,
                  expected: dict[str, str]) -> None:
    count = 0
    while True:
        try:
            msg = await ws.receive(timeout=config.WS_STALL_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            raise StreamStalled(
                f"Нет данных {config.WS_STALL_TIMEOUT_S} с при открытом соединении"
            ) from exc
        recv_ts = time.time_ns() // 1_000_000

        if msg.type == aiohttp.WSMsgType.TEXT:
            stream, symbol, data = parse_depth_message(msg.data, expected)
            line = json.dumps(
                {"recv_ts": recv_ts, "symbol": symbol, "stream": stream, "data": data},
                separators=(",", ":"),
            )
            writer.write(symbol, recv_ts, line)
            count += 1
            if count == 1:
                log.info("Получено первое сообщение, запись идёт")
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
            raise WebSocketClosed(
                f"Соединение закрыто (код {ws.close_code}), принято сообщений: {count}"
            )
        elif msg.type == aiohttp.WSMsgType.ERROR:
            raise NetworkError(f"Ошибка WebSocket: {ws.exception()!r}")
        else:
            raise DataError(f"Неожиданный тип кадра WebSocket: {msg.type!r}")


async def main() -> None:
    config.validate()
    ban_lock.ensure_not_banned()  # бан не истёк -> BanActive, код 2, без обращения к бирже
    with InstanceLock("depth_stream"):  # второй экземпляр -> код 1, замок держится до выхода
        names = stream_names()
        expected = {name: symbol for name, symbol in zip(names, config.SYMBOLS)}
        url = stream_url(names)
        writer = HourlyJsonlWriter(config.DEPTH_DIR)
        try:
            await write_snapshots(writer)  # строго до открытия сокета
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, connect=config.WS_CONNECT_TIMEOUT_S)
            ) as session:
                ws = await connect(session, url)
                log.info("Подключено: %s", url)
                try:
                    await consume(ws, writer, expected)
                finally:
                    await ws.close()
        finally:
            writer.close()


if __name__ == "__main__":
    run(main, "depth_stream")
