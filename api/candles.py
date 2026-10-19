"""Свечи Forex/металлов для графика клиента: GET /api/candles (2026-10-19). Источник - справочный кэш бота (orchestrator/chart_cache.py): закрытые M5 из MetaApi и формирующаяся M5
по котировкам. API MetaApi не касается. Любой таймфрейм (5m / 15m / 1h / 4h) собирается из M5 на UTC-сетке, как это делает сам бот (derive_tf); плоские свечи закрытого рынка
(O=H=L=C, объём 0) на график не выдаются. Крипту клиент берёт напрямую у Binance - здесь её нет."""

import json
import time
from pathlib import Path
from typing import Any

M5_MS = 300_000
INTERVALS = {"5m": M5_MS, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
CACHE_DIR = ".chart_cache"
LIVE_MAX_AGE_MS = 120_000  # формирующаяся свеча показывается, только если котировка свежая (рынок открыт и бот жив)
MAX_LIMIT = 1500


class NoCandles(Exception):
    """Для монеты нет кэша свечей (не MT5-символ, бот ещё не записал, MetaApi выключен)."""


_cache: dict[str, tuple[float, Any]] = {}


def _read(path: Path) -> Any:
    """JSON-файл с кэшированием по времени изменения (файл с 26 000 свечей разбирается один раз на запись бота)."""
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    hit = _cache.get(str(path))
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None  # файл переписывается прямо сейчас (атомарная подмена редка) или повреждён: следующий запрос прочитает заново
    _cache[str(path)] = (mtime, data)
    return data


def _freshest(dirs: list[Path], name: str) -> Any:
    best: tuple[float, Any] | None = None
    for d in dirs:
        p = d / CACHE_DIR / name
        try:
            m = p.stat().st_mtime
        except OSError:
            continue
        if best is None or m > best[0]:
            data = _read(p)
            if data is not None:
                best = (m, data)
    return None if best is None else best[1]


def _is_flat(r: list[float]) -> bool:
    return r[5] == 0 and r[1] == r[2] == r[3] == r[4]


def resample(rows: list[list[float]], target_ms: int, *, live: list[float] | None = None) -> list[list[float]]:
    """M5 (open_time, o, h, l, c, v; возрастание) -> свечи target_ms на UTC-сетке. Полные корзины; последняя может быть неполной, если в неё входит формирующаяся M5 `live`
    (тогда в корзине - закрытые M5 плюс она). Корзина целиком из плоских свечей пропускается."""
    items = list(rows)
    if live is not None and (not items or live[0] > items[-1][0]):
        items.append([live[0], live[1], live[2], live[3], live[4], 0.0])
    per = target_ms // M5_MS
    buckets: dict[int, list[list[float]]] = {}
    for r in items:
        buckets.setdefault(int(r[0]) - int(r[0]) % target_ms, []).append(r)
    live_bucket = None if live is None else int(live[0]) - int(live[0]) % target_ms
    out = []
    for start in sorted(buckets):
        part = buckets[start]
        full = len({int(r[0]) for r in part}) == per
        if not full and start != live_bucket:
            continue  # неполный край истории или дыра: такую свечу не выдаём
        if all(_is_flat(r) for r in part):
            continue
        out.append([start, part[0][1], max(r[2] for r in part), min(r[3] for r in part), part[-1][4], sum(r[5] for r in part)])
    return out


def build(dirs: list[Path], coin: str, interval: str, limit: int, end_ms: int | None, now_ms: int | None = None) -> dict[str, Any]:
    if interval not in INTERVALS:
        raise ValueError(f"interval: {sorted(INTERVALS)}")
    closed = _freshest(dirs, f"{coin}.json")
    if closed is None or not closed.get("candles"):
        raise NoCandles(coin)
    now = int(time.time() * 1000) if now_ms is None else now_ms
    live_doc = _freshest(dirs, f"{coin}.live.json")
    live = None
    if live_doc and now - int(live_doc["ts"]) <= LIVE_MAX_AGE_MS and (end_ms is None or end_ms >= live_doc["candle"][0]):
        live = [float(x) for x in live_doc["candle"]] + [0.0]
    rows = resample(closed["candles"], INTERVALS[interval], live=live)
    if end_ms is not None:
        rows = [r for r in rows if r[0] <= end_ms]
    rows = rows[-max(1, min(limit, MAX_LIMIT)):]
    return {"items": rows, "coin": coin, "interval": interval, "source": "bot_cache", "live": live is not None}
