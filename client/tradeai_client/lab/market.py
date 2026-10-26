"""Свечи и монеты для полигона: публичный REST Binance USDⓈ-M (без ключей), синхронно (мост окна pywebview вызывается в своём потоке).

Свеча - массив [open_time_сек, o, h, l, c, v, bv], где bv - объём агрессивных покупок (taker buy base): из него детектор объёма видит «нет покупателей»."""

import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

REST = "https://fapi.binance.com"
TIMEOUT_S = 15
MAX_LIMIT = 1500
IV = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
STABLE = ("USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", "USDPUSDT", "DAIUSDT")
_CACHE: dict[str, tuple[float, Any]] = {}


class MarketError(Exception):
    """Binance не ответил / ответил ошибкой (текст показывается пользователю)."""


def _get(path: str, params: dict | None = None, base: str = REST) -> Any:
    url = f"{base.rstrip('/')}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "TradeAI-Lab"}), timeout=TIMEOUT_S) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        hint = " (лимит запросов или блокировка региона)" if exc.code in (403, 418, 429, 451) else ""
        raise MarketError(f"Binance ответил HTTP {exc.code}{hint}") from exc
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        raise MarketError(f"Binance недоступен: {type(exc).__name__}") from exc


def _row(r: list) -> list[float]:
    return [int(r[0]) // 1000, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[9])]


def klines(symbol: str, interval: str, *, end_ms: int | None = None, start_ms: int | None = None, limit: int = 1000) -> list[list[float]]:
    """Закрытые свечи по возрастанию времени: до end_ms (последние limit штук) или от start_ms (первые limit). Незакрытая свеча отбрасывается."""
    if interval not in IV:
        raise MarketError(f"неизвестный таймфрейм {interval}")
    q: dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": max(1, min(int(limit), MAX_LIMIT))}
    if end_ms is not None:
        q["endTime"] = int(end_ms)
    if start_ms is not None:
        q["startTime"] = int(start_ms)
    rows = _get("/fapi/v1/klines", q)
    if not isinstance(rows, list):
        raise MarketError("Binance вернул не список свечей")
    now = time.time()
    return [_row(r) for r in rows if int(r[0]) / 1000 + IV[interval] <= now]


def history(symbol: str, interval: str, end_s: int, bars: int) -> list[list[float]]:
    """bars закрытых свечей, последняя открылась не позже end_s - IV (закрыта к end_s). Несколько страниц при необходимости."""
    out: list[list[float]] = []
    end_ms = (end_s - IV[interval]) * 1000 + 999
    while len(out) < bars:
        page = klines(symbol, interval, end_ms=end_ms, limit=min(MAX_LIMIT, bars - len(out)))
        page = [r for r in page if not out or r[0] < out[0][0]]
        if not page:
            break
        out = page + out
        end_ms = page[0][0] * 1000 - 1
    return out[-bars:]


def future(symbol: str, interval: str, after_s: int, bars: int = 1000) -> list[list[float]]:
    """Свечи ПОСЛЕ момента after_s (открытые не раньше него) - то, что реплей покажет дальше."""
    return klines(symbol, interval, start_ms=after_s * 1000, limit=bars)


def top_coins(n: int = 15) -> list[dict[str, Any]]:
    """Самые ликвидные USDT-перпетуалы по суточному обороту (кэш 30 мин): [{symbol, volume_usdt, onboard_s}]."""
    got = _CACHE.get("top")
    if got and time.time() - got[0] < 1800 and len(got[1]) >= n:
        return got[1][:n]
    info = _get("/fapi/v1/exchangeInfo")
    ok = {s["symbol"]: int(s.get("onboardDate") or 0) // 1000 for s in info["symbols"]
          if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING" and s["symbol"].isascii()}
    tick = _get("/fapi/v1/ticker/24hr")
    rows = sorted(({"symbol": t["symbol"], "volume_usdt": float(t["quoteVolume"]), "onboard_s": ok[t["symbol"]]} for t in tick
                   if t["symbol"] in ok and t["symbol"] not in STABLE), key=lambda r: -r["volume_usdt"])
    _CACHE["top"] = (time.time(), rows[:40])
    return rows[:n]


def random_spot(coins: list[dict[str, Any]], *, rng: random.Random | None = None, now_s: int | None = None,
                timeframes: tuple[str, ...] = ("5m", "15m", "1h", "4h"), max_days: int = 365) -> dict[str, Any]:
    """Случайная монета из списка, случайный таймфрейм и случайный момент за последний год (с запасом истории до и свечей после для реплея)."""
    rng = rng or random.Random()
    now_s = int(now_s if now_s is not None else time.time())
    coin = rng.choice(coins)
    tf = rng.choice(timeframes)
    iv = IV[tf]
    lo = max(now_s - max_days * 86400, int(coin.get("onboard_s") or 0) + 400 * iv)
    hi = now_s - 300 * iv  # впереди должно остаться хотя бы 300 свечей на реплей
    if hi <= lo:
        lo, hi = now_s - 30 * 86400, now_s - 300 * iv
    t = rng.randint(lo, max(lo + 1, hi))
    return {"symbol": coin["symbol"], "tf": tf, "at_s": t - t % iv}
