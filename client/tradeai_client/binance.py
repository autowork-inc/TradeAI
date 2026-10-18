"""Свечи ПОСЛЕ сигнала из публичного REST Binance (без ключей).

Зачем. Снапшот сделки хранит только то, что бот видел НА МОМЕНТ сигнала (последние ~100 закрытых свечей): вход, частичные закрытия
и выход случаются позже, и без свечей после сигнала маркеры не на чем рисовать. Эти свечи - публичные данные того же рынка и того же
таймфрейма, по которым работает Shadow (mainnet USDⓈ-M, `GET /fapi/v1/klines`). Они НЕ сохраняются в БД и не влияют на статистику.

Если Binance недоступен с вашего ПК (гео-блок, сеть) - `BinanceError`, график строится по снапшоту, маркеры привязываются к сетке времени.
Ответ 400/404 (нет такого символа: Forex/металлы идут из MT5) - `BinanceNoData`: без сообщения об ошибке, только снимок из БД.
"""

import asyncio
from typing import Any

import aiohttp

MAX_LIMIT = 1500  # максимум свечей за запрос у Binance


class BinanceError(Exception):
    """Публичные свечи получить не удалось (текст показывается пользователю)."""


class BinanceNoData(BinanceError):
    """Binance ответил 400/404: символа или диапазона у него нет (например EURUSD / XAUUSD с MT5). Это не сбой: график строится по снимку из БД."""


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
            if resp.status in (400, 404):
                raise BinanceNoData(f"Binance ответил HTTP {resp.status}")
            if resp.status != 200:
                hint = " (возможна блокировка региона)" if resp.status in (403, 418, 429, 451) else ""
                raise BinanceError(f"Binance ответил HTTP {resp.status}{hint}")
            rows = await resp.json(content_type=None)
    except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
        raise BinanceError("Binance недоступен с этого ПК") from exc
    return parse_klines(rows, now_ms=now_ms, interval_ms=interval_ms)


MAX_PAGES = 12  # потолок страниц по 1500 свечей за один вызов (M1 за 12 суток; для M5/M15 - месяцы)


def missing_bars(candles: list[dict[str, float]], start_ms: int, end_ms: int, interval_ms: int, now_ms: int) -> list[int]:
    """Открытия закрытых свечей сетки таймфрейма в [start, end], которых нет в `candles` (разрыв: обрыв потока, рестарт бота, пропуск у биржи)."""
    have = {int(c["t"]) for c in candles}
    first = start_ms + (-start_ms) % interval_ms  # начало первой свечи сетки не раньше start
    last = min(end_ms, now_ms - interval_ms)  # свеча закрыта, когда open + интервал <= now
    return [t for t in range(first, last + 1, interval_ms) if t not in have]


async def fetch_klines_filled(session: aiohttp.ClientSession, base_url: str, symbol: str, interval: str, start_ms: int, end_ms: int, *,
                              interval_ms: int, now_ms: int, timeout_s: float = 10.0) -> tuple[list[dict[str, float]], int]:
    """Свечи [start, end] без пропусков до текущего момента: страницы по 1500 (хвост графика не обрывается на первой странице), затем разрывы внутри
    диапазона догружаются отдельными запросами (один раз). Возвращает (свечи по возрастанию времени, сколько закрытых свечей так и не нашлось).
    Только закрытые свечи (R5); ничего не придумывается - недостающее считается и возвращается числом."""
    end = min(end_ms, now_ms)
    got: dict[int, dict[str, float]] = {}

    async def load(lo: int, hi: int) -> None:
        cursor = lo
        for _ in range(MAX_PAGES):
            page = await fetch_klines(session, base_url, symbol, interval, cursor, hi, interval_ms=interval_ms, now_ms=now_ms, timeout_s=timeout_s)
            fresh = [c for c in page if int(c["t"]) >= cursor]
            for c in fresh:
                got[int(c["t"])] = c
            if not fresh or int(fresh[-1]["t"]) + interval_ms > hi:
                return
            cursor = int(fresh[-1]["t"]) + interval_ms

    await load(start_ms, end)
    holes = missing_bars(list(got.values()), start_ms, end, interval_ms, now_ms)
    first_hole = None
    for t in holes:  # разрыв внутри диапазона: непрерывные отрезки пропусков запрашиваются заново
        if first_hole is None:
            first_hole, prev = t, t
        elif t == prev + interval_ms:
            prev = t
        else:
            await load(first_hole, prev + interval_ms - 1)
            first_hole, prev = t, t
    if first_hole is not None:
        await load(first_hole, prev + interval_ms - 1)
    left = missing_bars(list(got.values()), start_ms, end, interval_ms, now_ms)
    return [got[t] for t in sorted(got)], len(left)
