"""Свечи ПОСЛЕ сигнала из публичного REST Binance (без ключей).

Зачем. Снапшот сделки хранит только то, что бот видел НА МОМЕНТ сигнала (последние ~100 закрытых свечей): вход, частичные закрытия
и выход случаются позже, и без свечей после сигнала маркеры не на чем рисовать. Эти свечи - публичные данные того же рынка и того же
таймфрейма, по которым работает Shadow (mainnet USDⓈ-M, `GET /fapi/v1/klines`). Они НЕ сохраняются в БД и не влияют на статистику.

Если Binance недоступен с вашего ПК (гео-блок, сеть) - `BinanceError`, график строится по снапшоту, маркеры привязываются к сетке времени.
"""

import asyncio
from typing import Any

import aiohttp

MAX_LIMIT = 1500  # максимум свечей за запрос у Binance


class BinanceError(Exception):
    """Публичные свечи получить не удалось (текст показывается пользователю)."""


def parse_klines(rows: Any, *, now_ms: int, interval_ms: int) -> list[dict[str, float]]:
    """Ответ /klines -> список закрытых свечей {t,o,h,l,c,v}. Незакрытая (текущая) свеча отбрасывается (R5: только закрытые)."""
    if not isinstance(rows, list):
        raise BinanceError("Binance вернул неожиданный ответ (не список свечей)")
    out: list[dict[str, float]] = []
    for row in rows:
        try:
            t, o, h, low, c, v = int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
        except (TypeError, ValueError, IndexError) as exc:
            raise BinanceError(f"Binance вернул свечу неожиданного формата: {row!r}") from exc
        if t + interval_ms > now_ms:
            continue  # свеча ещё формируется
        out.append({"t": t, "o": o, "h": h, "l": low, "c": c, "v": v})
    return out


async def fetch_klines(session: aiohttp.ClientSession, base_url: str, symbol: str, interval: str, start_ms: int, end_ms: int, *,
                       interval_ms: int, now_ms: int, timeout_s: float = 10.0) -> list[dict[str, float]]:
    params = {"symbol": symbol, "interval": interval, "startTime": str(start_ms), "endTime": str(end_ms), "limit": str(MAX_LIMIT)}
    try:
        async with session.get(f"{base_url.rstrip('/')}/fapi/v1/klines", params=params, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                hint = " (возможна блокировка региона)" if resp.status in (403, 418, 429, 451) else ""
                raise BinanceError(f"Binance ответил HTTP {resp.status}{hint}")
            rows = await resp.json(content_type=None)
    except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
        raise BinanceError("Binance недоступен с этого ПК") from exc
    return parse_klines(rows, now_ms=now_ms, interval_ms=interval_ms)
