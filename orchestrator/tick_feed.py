"""TickFeed: тики для VirtualTracker. `<symbol>@aggTrade` (цена сделки) и `<symbol>@markPrice@1s` (mark price).

Оба потока идут по маршруту config.WS_MARKET_URL (/market/stream; проверено на mainnet 2026-10-08: aggTrade ~50 сообщений
в секунду на BTCUSDT, markPrice раз в секунду). Из aggTrade берутся также объём (q) и сторона агрессора (m) - для ленты микроструктуры (orchestrator/microstructure.py).
Обрыв потока = потерянные тики: приёмнику вызывается on_gap (трекер
помечает активные сделки gap_affected), затем переподключение. Мусор в сообщении - DataError (R1).
"""

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import aiohttp

import config
from orchestrator.types import Tick
from orchestrator.ws import ReconnectingStream, decode_combined, wall_clock_ms
from utils.errors import ConfigError, DataError, WebSocketClosed

log = logging.getLogger("orchestrator.tick_feed")

_EVENT = {"trade": "aggTrade", "mark": "markPriceUpdate"}
_TS_FIELD = {"trade": "T", "mark": "E"}


class TickFeed:
    def __init__(
        self, session: aiohttp.ClientSession | None, on_tick: Callable[[Tick], None], on_gap: Callable[[int], None], *,
        symbols: tuple[str, ...] | None = None, clock_ms: Callable[[], int] = wall_clock_ms, market_url: str | None = None,
        max_reconnects: int | None = None, backoff_s: float | None = None, backoff_max_s: float | None = None,
    ):
        self._symbols = tuple(symbols if symbols is not None else config.SYMBOLS)
        self._session, self._on_tick = session, on_tick
        names: dict[str, tuple[str, str]] = {}
        for s in self._symbols:
            names[f"{s.lower()}@aggTrade"] = (s, "trade")
            names[f"{s.lower()}@markPrice@1s"] = (s, "mark")
        self._expected = names
        self._url = f"{market_url or config.WS_MARKET_URL}?streams=" + "/".join(names)
        self._stream = ReconnectingStream(
            "ticks", self._url, session, self._on_frame, on_gap=on_gap, clock_ms=clock_ms,  # type: ignore[arg-type]
            max_reconnects=max_reconnects, backoff_s=backoff_s, backoff_max_s=backoff_max_s,
        )
        self._task: asyncio.Task | None = None
        self.last_tick_ms: dict[str, int] = {}

    def start(self) -> None:
        if self._task is None:
            if self._session is None:
                raise ConfigError("TickFeed: для запуска потока нужна session")
            self._task = asyncio.create_task(self._stream.run(), name="tick_feed")

    async def aclose(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def check(self) -> None:
        """Поднимает исключение упавшей задачи потока (вызывать из основного цикла)."""
        if self._task is not None and self._task.done():
            if self._task.cancelled():
                raise WebSocketClosed("Задача фида тиков отменена")
            exc = self._task.exception()
            if exc is not None:
                raise exc
            raise DataError("Задача фида тиков завершилась без ошибки: так быть не должно")

    def stats(self) -> dict[str, Any]:
        return {"received": self._stream.received, "reconnects": self._stream.reconnects}

    def _on_frame(self, raw: str, recv_ts: int) -> None:
        _, (symbol, kind), data = decode_combined(raw, self._expected)
        if data.get("e") != _EVENT[kind] or data.get("s") != symbol:
            raise DataError(f"Неожиданное сообщение {kind} {symbol}: {str(data)[:200]}")
        try:
            if kind == "trade":  # лента: объём и сторона агрессора нужны микроструктуре (m=True: покупатель - мейкер, т.е. агрессор продал)
                if not isinstance(data["m"], bool):
                    raise ValueError(f"поле m не bool: {data['m']!r}")
                tick = Tick(symbol, float(data["p"]), int(data[_TS_FIELD[kind]]), kind, float(data["q"]), data["m"])
            else:
                tick = Tick(symbol, float(data["p"]), int(data[_TS_FIELD[kind]]), kind)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Неожиданный тик {kind} {symbol}: {str(data)[:200]}") from exc
        self.last_tick_ms[symbol] = tick.ts_ms
        self._on_tick(tick)
