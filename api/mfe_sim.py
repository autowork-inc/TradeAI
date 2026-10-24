"""MFE Simulator (вкладка клиента «MFE Simulator»; 1.5.0 2026-10-24, 1.6.0): доигрывание ВСЕХ сделок с фактом входа по 1m-свечам Binance.

Сделки: любой статус (закрыта / прервана рестартом / gap / ещё открыта / отменена после входа), у которых есть entry_price и entry_time, Shadow и Live
(1.6.0: live-копии теневых сигналов тоже считаются, их число показывается отдельно). Окно - с даты по дату (по времени входа), набор стратегий -
по профилю пользователя: CHR - только стратегии *_CHR, SANTA - все остальные.

Метод (пессимистичный, как в бэктесте проекта): путь цены - 1m-свечи публичного Binance Futures с минуты ПОСЛЕ входа (минута входа могла быть до
исполнения), горизонт 48 ч (тайм-аут бота). Внутри одной свечи стоп раньше тейка. Путь сводится к трём числам: пик хода в нашу сторону до стопа (R),
задет ли стоп, где цена на последней свече (R). Отсюда считается любой тейк без потолка. Открытая сделка (путь не закончился) считается по ТЕКУЩЕЙ
цене (MFE - от входа до сейчас), её число показывается отдельно.
Два плана: «весь объём на X» и Scale Out бота (1.6.0): 50% на +1R, остаток на X. Лучший X Scale Out ищется так же, как авто-TP бота
(orchestrator/auto_tp.py): от 1R до самого дальнего хода цены, шаг 0.25.
Риск сделки фиксированный ($5, risk_amount): USD = R × risk. Комиссии как у трекера: вход 0.02% (LIMIT) / 0.05%, выход 0.05%.
Стоп-сквиз: стоп задет, а за следующие 6 ч цена вернулась ко входу и дошла до +1R. Отступ: стоп отодвигается ещё на k·ATR сигнальной свечи
(риск тот же $5 - объём меньше), для каждого k считается та же сетка тейков.
Только крипта (*USDT): у Forex/металлов публичных минутных свечей нет - они перечислены отдельно.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import asyncpg
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query

log = logging.getLogger("api.mfe_sim")

BINANCE_URL = os.environ.get("MFE_BINANCE_URL", "https://fapi.binance.com")
MIN_MS = 60_000
HORIZON_MS = 48 * 3_600_000
HUNT_MS = 6 * 3_600_000
TP_GRID = (1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0)
SCAN_STEP = 0.25
FIRST_TAKE_R, FIRST_TAKE_FRAC = 1.0, 0.5  # Scale Out бота: 50% на +1R
PAD_GRID = (0.0, 0.25, 0.5, 0.75, 1.0)
MAKER, TAKER = 0.0002, 0.0005
DEFAULT_RISK_USD = 5.0
REQUEST_PAUSE_S = 0.5  # бережно к лимиту веса Binance: тот же IP использует бот (limit=1000 -> вес 5, ~600 веса/мин)
KLINES_LIMIT = 1000
MIN_TRADES_FOR_VERDICT = 30

TRADES_SQL = """
SELECT t.id, COALESCE(t.strategy_id, t.strategy_name) AS strategy, t.coin, t.side, t.kind, t.mode, t.status, t.entry_price, t.entry_time,
       t.exit_time, t.sl, t.risk_amount, t.r_multiple, t.gap_affected, t.exit_reason,
       s.meta -> 'atr' AS meta_atr, s.candles -> -1 -> 'atr' AS candle_atr
  FROM trades t LEFT JOIN trade_snapshots s ON s.trade_id = t.id
 WHERE t.entry_price IS NOT NULL AND t.entry_time IS NOT NULL AND t.entry_time >= $1 AND t.entry_time < $2
 ORDER BY t.entry_time, t.id
"""

Bar = tuple[int, float, float, float]  # open_ms, high, low, close


class SimError(Exception):
    """Сбой симуляции с понятным текстом (бан/лимит Binance, нет сети)."""


SCOPES = ("santa", "chr")


def in_scope(strategy: str, scope: str) -> bool:
    """CHR - только стратегии *_CHR (профиль CHR); SANTA - все остальные."""
    return strategy.endswith("_CHR") == (scope == "chr")


@dataclass
class SimTrade:
    id: str
    strategy: str
    coin: str
    side: str
    kind: str
    mode: str
    status: str
    entry: float
    entry_ms: int
    sl: float
    risk_usd: float
    atr: float | None
    fact_r: float | None

    @property
    def dir(self) -> int:
        return 1 if self.side == "long" else -1

    @property
    def fee_in(self) -> float:
        return MAKER if self.kind == "limit" else TAKER


def _num(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x and x > 0 else None


def select_trades(rows: list[dict[str, Any]], scope: str = "santa") -> tuple[list[SimTrade], dict[str, int]]:
    """Строки БД -> сделки для симуляции + счётчики (каждая строка учтена ровно один раз). Live-копии теневых сигналов считаются (1.6.0)."""
    shadow_ids = {r["id"] for r in rows if r["mode"] == "shadow"}
    counts = {"rows": 0, "live_copies": 0, "bad_stop": 0, "other_profile": 0}
    out: list[SimTrade] = []
    for r in rows:
        if not in_scope(r["strategy"], scope):
            counts["other_profile"] += 1
            continue
        counts["rows"] += 1
        if r["mode"] == "live" and r["id"].endswith(":live") and r["id"][: -len(":live")] in shadow_ids:
            counts["live_copies"] += 1
        entry, sl = float(r["entry_price"]), float(r["sl"])
        d = 1 if r["side"] == "long" else -1
        if not d * (entry - sl) > 0:
            counts["bad_stop"] += 1
            continue
        et = r["entry_time"]
        out.append(SimTrade(id=r["id"], strategy=r["strategy"], coin=r["coin"], side=r["side"], kind=r["kind"], mode=r["mode"], status=r["status"],
                            entry=entry, entry_ms=int(et.timestamp() * 1000) if isinstance(et, datetime) else int(et), sl=sl,
                            risk_usd=_num(r["risk_amount"]) or DEFAULT_RISK_USD, atr=_num(r.get("meta_atr")) or _num(r.get("candle_atr")),
                            fact_r=None if r["r_multiple"] is None else float(r["r_multiple"])))
    return out, counts


# ------------------------------------------------------------------------------------------- свечи

class KlineSource:
    """1m-свечи Binance Futures (публичный REST), кэш в памяти на время одного запуска."""

    def __init__(self, client: httpx.AsyncClient, now_ms: int):
        self.client, self.now_ms = client, now_ms
        self.requests = 0
        self._cache: dict[str, list[Bar]] = {}

    async def load(self, symbol: str, start: int, end: int) -> list[Bar]:
        end = min(end, self.now_ms)
        out: list[Bar] = []
        cur = start - start % MIN_MS
        while cur < end:
            if self.requests:
                await asyncio.sleep(REQUEST_PAUSE_S)
            self.requests += 1
            try:
                resp = await self.client.get(f"{BINANCE_URL}/fapi/v1/klines", params={
                    "symbol": symbol, "interval": "1m", "startTime": cur, "endTime": end - 1, "limit": KLINES_LIMIT}, timeout=20)
            except httpx.HTTPError as exc:
                raise SimError(f"нет связи с Binance ({type(exc).__name__}): повторите запуск позже") from None
            if resp.status_code in (418, 429):
                raise SimError(f"Binance ограничил запросы (HTTP {resp.status_code}): запуск остановлен, чтобы не задеть бота; повторите через несколько минут")
            if resp.status_code == 400:
                return []  # символа нет на Binance Futures
            if resp.status_code != 200:
                raise SimError(f"Binance ответил HTTP {resp.status_code} на свечи {symbol}")
            rows = resp.json()
            if not rows:
                break
            out += [(int(x[0]), float(x[2]), float(x[3]), float(x[4])) for x in rows]
            cur = int(rows[-1][0]) + MIN_MS
        return out

    async def bars_for(self, symbol: str, trades: list[SimTrade]) -> list[Bar]:
        start = min(t.entry_ms for t in trades)
        end = max(t.entry_ms for t in trades) + HORIZON_MS + HUNT_MS
        bars = await self.load(symbol, start, end)
        self._cache[symbol] = bars
        return bars


# ------------------------------------------------------------------------------------------- симуляция

@dataclass
class PathResult:
    peak_r: float  # максимум хода в нашу сторону ДО стопа (R этого стопа); у открытой - до текущей минуты
    stopped: bool
    close_r: float  # где цена на последней свече пути (R): итог не задетой части при тайм-ауте / текущая цена открытой
    stop_hit_ms: int | None
    mae_r: float
    open_now: bool  # 48 ч не прошли и стоп не задет: сделка ещё идёт, считается по текущей цене
    entry: float
    risk: float
    sgn: int
    fee_in: float

    @property
    def mfe_r(self) -> float:
        return self.peak_r

    def outcome(self, x: float, scale: bool = False) -> float:
        """Чистый R по плану: весь объём на x R, или (scale) 50% на +1R + остаток на x R. Не взятая часть - стоп (-1R) или close пути."""
        rest = -1.0 if self.stopped else self.close_r
        levels = ((x, 1.0),) if not scale or x <= FIRST_TAKE_R else ((FIRST_TAKE_R, FIRST_TAKE_FRAC), (x, 1.0 - FIRST_TAKE_FRAC))
        gross, fees = 0.0, self.fee_in * self.entry / self.risk
        for r, frac in levels:
            got = r if self.peak_r >= r else rest
            gross += frac * got
            fees += frac * TAKER * (self.entry + self.sgn * got * self.risk) / self.risk
        return gross - fees

    @property
    def by_tp(self) -> dict[float, float]:
        return {x: self.outcome(x) for x in TP_GRID}


def simulate_path(t: SimTrade, bars: list[Bar], sl: float, now_ms: int) -> PathResult | None:
    """Один проход по пути (стоп раньше тейка внутри свечи). None - нет свечей после входа."""
    d = abs(t.entry - sl)
    first = t.entry_ms - t.entry_ms % MIN_MS + MIN_MS
    path = [b for b in bars if first <= b[0] < t.entry_ms + HORIZON_MS]
    if not path or not d > 0:
        return None
    peak = mae = 0.0
    for open_ms, hi, lo, _c in path:
        if (lo <= sl) if t.dir == 1 else (hi >= sl):
            return PathResult(peak, True, -1.0, open_ms, 1.0, False, t.entry, d, t.dir, t.fee_in)
        peak = max(peak, t.dir * ((hi if t.dir == 1 else lo) - t.entry) / d)
        mae = max(mae, t.dir * (t.entry - (lo if t.dir == 1 else hi)) / d)
    return PathResult(peak, False, t.dir * (path[-1][3] - t.entry) / d, None, mae, t.entry_ms + HORIZON_MS > now_ms, t.entry, d, t.dir, t.fee_in)


def scan_best(prs: list[PathResult], scale: bool) -> tuple[float, float] | None:
    """Лучший X (по сумме R) от 1R до самого дальнего хода цены, шаг 0.25 - без потолка (как авто-TP бота). (X, сумма R)."""
    if not prs:
        return None
    top = max(max(p.peak_r for p in prs), FIRST_TAKE_R)
    best: tuple[float, float] | None = None
    k = int(FIRST_TAKE_R / SCAN_STEP)
    while k * SCAN_STEP <= top + 1e-9:
        x = round(k * SCAN_STEP, 4)
        total = sum(p.outcome(x, scale) for p in prs)
        if best is None or total > best[1] + 1e-12:
            best = (x, total)
        k += 1
    return best


def squeeze_after_stop(t: SimTrade, bars: list[Bar], stop_ms: int) -> dict[str, Any]:
    """После стопа (6 ч): вернулась ли цена ко входу и дошла ли до +1R / +2R; насколько тень зашла за стоп в первые 30 мин (в ATR)."""
    d = abs(t.entry - t.sl)
    after = [b for b in bars if stop_ms < b[0] <= stop_ms + HUNT_MS]
    best = max((t.dir * ((hi if t.dir == 1 else lo) - t.entry) / d for _, hi, lo, _c in after), default=None)
    near = [b for b in bars if stop_ms <= b[0] <= stop_ms + 30 * MIN_MS]
    worst = max((t.dir * (t.sl - (lo if t.dir == 1 else hi)) for _, hi, lo, _c in near), default=0.0)
    return {"reach1": best is not None and best >= 1.0, "reach2": best is not None and best >= 2.0, "back_to_entry": best is not None and best >= 0,
            "overshoot_atr": None if not t.atr else max(worst, 0.0) / t.atr}


def _agg(items: list[tuple[SimTrade, float]]) -> dict[str, Any]:
    rs = [r for _, r in items]
    n = len(rs)
    wins, losses = sum(r for r in rs if r > 0), -sum(r for r in rs if r < 0)
    return {"n": n, "wins": sum(1 for r in rs if r > 0), "win_rate": (sum(1 for r in rs if r > 0) / n) if n else None,
            "sum_r": round(sum(rs), 3), "mean_r": round(sum(rs) / n, 4) if n else None, "usd": round(sum(r * t.risk_usd for t, r in items), 2),
            "pf": round(wins / losses, 3) if losses > 0 else None}


def build_report(trades: list[SimTrade], paths: dict[str, dict[float, PathResult | None]], hunts: dict[str, dict[str, Any]], counts: dict[str, int],
                 since: datetime, now_ms: int, until: datetime | None = None, scope: str = "santa") -> dict[str, Any]:
    sim = [t for t in trades if paths.get(t.id, {}).get(0.0) is not None]
    base = {t.id: paths[t.id][0.0] for t in sim}

    def matrix(group: list[SimTrade], pad: float = 0.0, scale: bool = False) -> list[dict[str, Any]]:
        rows = []
        for x in TP_GRID:
            items = [(t, paths[t.id][pad].outcome(x, scale)) for t in group if paths[t.id].get(pad) is not None]
            rows.append({"tp": x, **_agg(items)})
        return rows

    def best_of(group: list[SimTrade], scale: bool) -> dict[str, Any] | None:
        b = scan_best([base[t.id] for t in group], scale)
        if b is None:
            return None
        x, total = b
        return {"tp": x, "sum_r": round(total, 3), "usd": round(sum(base[t.id].outcome(x, scale) * t.risk_usd for t in group), 2),
                "mean_r": round(total / len(group), 4)}

    tp_rows, scale_rows = matrix(sim), matrix(sim, scale=True)
    best_plain, best_scale = best_of(sim, False), best_of(sim, True)
    current = next((r for r in scale_rows if r["tp"] == 4.0), None)
    verdict: dict[str, Any] = {"n": len(sim), "best_tp": None, "text": "Нет сделок с путём цены для симуляции."}
    if best_scale is not None and best_plain is not None:
        reliable = len(sim) >= MIN_TRADES_FOR_VERDICT
        verdict = {"n": len(sim), "best_tp": best_scale["tp"], "best_usd": best_scale["usd"], "best_mean_r": best_scale["mean_r"],
                   "best_plain": best_plain, "current_usd": current["usd"] if current else None, "reliable": reliable,
                   "text": (f"На основе {len(sim)} сделок оптимальный математический Take-Profit для текущего рынка составляет 1:{best_scale['tp']:g} "
                            f"при плане бота (50% на +1R, остаток на тейке): {best_scale['usd']:+.2f} $, {best_scale['mean_r']:+.3f} R на сделку. "
                            f"Без частичной фиксации лучший тейк 1:{best_plain['tp']:g}: {best_plain['usd']:+.2f} $."
                            + ("" if reliable else f" Сделок меньше {MIN_TRADES_FOR_VERDICT} - это ориентир, не вывод."))}

    stopped = [t for t in sim if base[t.id].stopped]
    sq = [hunts[t.id] for t in stopped if t.id in hunts]
    pad_rows = []
    with_atr = [t for t in sim if t.atr]
    for k in PAD_GRID:
        cells = {}
        for x in TP_GRID:
            items = [(t, paths[t.id][k].outcome(x, True)) for t in with_atr if paths[t.id].get(k) is not None]
            cells[str(x)] = _agg(items)
        pad_rows.append({"pad_atr": k, "cells": cells})
    best_cell = max(((row["pad_atr"], float(x), c) for row in pad_rows for x, c in row["cells"].items() if c["n"]), key=lambda z: z[2]["usd"], default=None)
    base_best = max((c for c in pad_rows[0]["cells"].values() if c["n"]), key=lambda c: c["usd"], default=None) if pad_rows else None
    pad_text = "Недостаточно сделок с ATR для оценки отступа."
    pad_suggest = None
    if best_cell is not None and base_best is not None:
        k, x, c = best_cell
        neighbours = [row for row in pad_rows if abs(row["pad_atr"] - k) == 0.25]
        stable = any(row["cells"][str(x)]["n"] and row["cells"][str(x)]["usd"] > base_best["usd"] for row in neighbours) if k > 0 else True
        if k > 0 and c["usd"] > base_best["usd"] and stable:
            pad_suggest = {"pad_atr": k, "tp": x, "usd": c["usd"], "mean_r": c["mean_r"]}
            pad_text = (f"Рекомендуется отодвинуть стоп ещё на {k:g} ATR (риск тот же $5): с тейком 1:{x:g} - {c['usd']:+.2f} $ "
                        f"против {base_best['usd']:+.2f} $ с текущим стопом.")
        else:
            pad_text = "Корректировка отступа стопа не требуется: дополнительный отступ не улучшает результат устойчиво."
    squeeze = {"stopped": len(stopped), "checked": len(sq), "back_to_entry": sum(h["back_to_entry"] for h in sq), "reach1": sum(h["reach1"] for h in sq),
               "reach2": sum(h["reach2"] for h in sq), "rate": (sum(h["reach1"] for h in sq) / len(sq)) if sq else None,
               "overshoot_atr_median": _median([h["overshoot_atr"] for h in sq if h["overshoot_atr"] is not None])}

    by_strategy = []
    groups: dict[str, list[SimTrade]] = {}
    for t in sim:
        groups.setdefault(t.strategy, []).append(t)
    for name, group in sorted(groups.items()):
        b = best_of(group, True)
        cur = sum(base[t.id].outcome(4.0, True) * t.risk_usd for t in group)
        st = [t for t in group if base[t.id].stopped]
        hs = [hunts[t.id] for t in st if t.id in hunts]
        by_strategy.append({"strategy": name, "n": len(group), "rows": matrix(group, scale=True), "best_tp": b["tp"] if b else None,
                            "best_usd": b["usd"] if b else None, "usd_at_4": round(cur, 2),
                            "squeeze": {"stopped": len(st), "reach1": sum(h["reach1"] for h in hs)},
                            "mfe_median": _median([base[t.id].peak_r for t in group])})

    statuses: dict[str, int] = {}
    for t in trades:
        statuses[t.status] = statuses.get(t.status, 0) + 1
    sim_ids = {t.id for t in sim}
    no_data = [t for t in trades if t.id not in sim_ids]
    return {"since": since.date().isoformat(), "until": until.date().isoformat() if until else None, "scope": scope,
            "generated_at": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds"),
            "counts": {**counts, "with_entry": len(trades), "simulated": len(sim), "no_candles": len(no_data),
                       "no_candles_symbols": sorted({t.coin for t in no_data}), "statuses": statuses,
                       "open_now": sum(1 for t in sim if base[t.id].open_now),
                       "modes": {m: sum(1 for t in sim if t.mode == m) for m in ("shadow", "live")}},
            "tp_matrix": tp_rows, "scale_matrix": scale_rows, "verdict": verdict, "squeeze": squeeze,
            "padding": {"rows": pad_rows, "suggest": pad_suggest, "text": pad_text, "with_atr": len(with_atr)},
            "by_strategy": by_strategy,
            "mfe": {"median": _median([base[t.id].peak_r for t in sim]), "reach1": sum(1 for t in sim if base[t.id].peak_r >= 1.0),
                    "mae_median": _median([base[t.id].mae_r for t in sim])}}


def _median(xs: list[float]) -> float | None:
    xs = sorted(xs)
    if not xs:
        return None
    m = len(xs) // 2
    return round(xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2, 3)


async def run_simulation(rows: list[dict[str, Any]], since: datetime, client: httpx.AsyncClient, progress: dict[str, Any],
                         now_ms: int | None = None, until: datetime | None = None, scope: str = "santa") -> dict[str, Any]:
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    trades, counts = select_trades(rows, scope)
    src = KlineSource(client, now_ms)
    by_symbol: dict[str, list[SimTrade]] = {}
    for t in trades:
        if t.coin.endswith("USDT"):
            by_symbol.setdefault(t.coin, []).append(t)
    progress.update(total=len(by_symbol), done=0, stage="свечи Binance")
    paths: dict[str, dict[float, PathResult | None]] = {}
    hunts: dict[str, dict[str, Any]] = {}
    for symbol, group in by_symbol.items():
        bars = await src.bars_for(symbol, group)
        for t in group:
            paths[t.id] = {}
            for k in PAD_GRID:
                if k and not t.atr:
                    continue
                paths[t.id][k] = simulate_path(t, bars, t.sl - t.dir * k * (t.atr or 0.0), now_ms)
            base = paths[t.id].get(0.0)
            if base is not None and base.stopped and base.stop_hit_ms is not None:
                hunts[t.id] = squeeze_after_stop(t, bars, base.stop_hit_ms)
        progress["done"] += 1
        await asyncio.sleep(0)
    progress["stage"] = "отчёт"
    report = build_report(trades, paths, hunts, counts, since, now_ms, until, scope)
    report["counts"]["binance_requests"] = src.requests
    return report


# ------------------------------------------------------------------------------------------- HTTP

@dataclass
class Job:
    status: str = "idle"  # idle | running | done | failed
    since: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    report: dict[str, Any] | None = None
    task: asyncio.Task | None = None

    def out(self) -> dict[str, Any]:
        return {"status": self.status, "since": self.since, "started_at": self.started_at, "finished_at": self.finished_at,
                "progress": self.progress, "error": self.error, "report": self.report}


def install(app: FastAPI, auth, pool, transport: httpx.AsyncBaseTransport | None = None, now_ms=None,
            data_dirs: Sequence[Path] = (), lab_dir: Path | None = None) -> dict[str, Job]:
    """POST /api/mfe/run?since&until&scope - запуск в фоне (по одному на профиль); GET /api/mfe/status?scope - прогресс и отчёт последнего запуска
    профиля; GET /api/strategy/state?scope - текущие тейки авто-TP бота, одобренные изменения и шаблоны предложений (только чтение)."""
    jobs = {scope: Job() for scope in SCOPES}

    async def worker(job: Job, since: datetime, until: datetime, scope: str) -> None:
        try:
            async with pool().acquire() as conn:
                rows = [dict(r) for r in await conn.fetch(TRADES_SQL, since, until)]
            async with httpx.AsyncClient(transport=transport) as client:
                job.report = await run_simulation(rows, since, client, job.progress, now_ms() if now_ms else None, until=until, scope=scope)
            job.status = "done"
        except SimError as exc:
            job.status, job.error = "failed", str(exc)
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, asyncio.TimeoutError) as exc:
            job.status, job.error = "failed", f"база данных недоступна ({type(exc).__name__}): повторите позже"
        finally:
            job.finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def finished_for(job: Job):
        def finished(task: asyncio.Task) -> None:
            """Ошибка в коде симулятора (не сеть/БД): не глотается - пишется в лог с трассировкой и показывается пользователю в статусе."""
            if task.cancelled() or task.exception() is None:
                return
            exc = task.exception()
            log.error("MFE Simulator: сбой", exc_info=exc)
            job.status, job.error = "failed", f"внутренняя ошибка симулятора: {type(exc).__name__}: {exc}"
        return finished

    def day(text: str, name: str) -> datetime:
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(422, f"{name}: дата YYYY-MM-DD") from None

    @app.post("/api/mfe/run", dependencies=[Depends(auth)])
    async def mfe_run(since: str = Query("2026-10-20", pattern=r"^\d{4}-\d{2}-\d{2}$"), until: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
                      scope: str = Query("santa", pattern=r"^(santa|chr)$")) -> dict[str, Any]:
        job = jobs[scope]
        if job.status == "running":
            raise HTTPException(409, "симуляция уже идёт")
        start = day(since, "since")
        end = day(until, "until") + timedelta(days=1) if until else datetime.now(timezone.utc) + timedelta(days=1)  # «по дату» - включительно
        if end <= start:
            raise HTTPException(422, "until раньше since")
        job.status, job.since, job.error, job.report, job.finished_at = "running", since, None, None, None
        job.started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        job.progress = {"done": 0, "total": 0, "stage": "сделки из БД"}
        job.task = asyncio.create_task(worker(job, start, end, scope))
        job.task.add_done_callback(finished_for(job))
        return job.out()

    @app.get("/api/mfe/status", dependencies=[Depends(auth)])
    async def mfe_status(scope: str = Query("santa", pattern=r"^(santa|chr)$")) -> dict[str, Any]:
        return jobs[scope].out()

    @app.get("/api/strategy/state", dependencies=[Depends(auth)])
    async def strategy_state(scope: str = Query("santa", pattern=r"^(santa|chr)$")) -> dict[str, Any]:
        return read_strategy_state(data_dirs, lab_dir, scope)

    return jobs


def read_strategy_state(data_dirs: Sequence[Path], lab_dir: Path | None, scope: str) -> dict[str, Any]:
    """Тейки авто-TP (`.auto_tp.json` бота, свежий из каталогов данных), одобренные изменения (strategy_overrides.json) и предложения/шаблоны
    (strategy_lab/proposals) - отфильтрованные по профилю."""
    auto: dict[str, Any] = {}
    updated = None
    for d in data_dirs:
        f = Path(d) / ".auto_tp.json"
        if f.is_file():
            data = json.loads(f.read_text(encoding="utf-8"))
            if updated is None or data.get("updated_ms", 0) > updated:
                updated, auto = data.get("updated_ms", 0), data
    strategies = {k: v for k, v in (auto.get("strategies") or {}).items() if in_scope(k, scope)}
    overrides: dict[str, Any] = {}
    proposals: list[dict[str, Any]] = []
    if lab_dir is not None and lab_dir.is_dir():
        ov = lab_dir / "strategy_overrides.json"
        if ov.is_file():
            overrides = {k: v for k, v in (json.loads(ov.read_text(encoding="utf-8")).get("strategies") or {}).items() if in_scope(k, scope)}
        for f in sorted((lab_dir / "proposals").glob("*.json")):
            d = json.loads(f.read_text(encoding="utf-8"))
            if in_scope(d.get("strategy", ""), scope):
                proposals.append({k: d.get(k) for k in ("id", "strategy", "title", "status", "kind", "created_at", "applied_at", "rationale", "change",
                                                        "how_to_apply", "risks")})
    return {"auto_tp": {"updated_ms": auto.get("updated_ms"), "since": auto.get("since"), "strategies": strategies},
            "overrides": overrides, "proposals": proposals,
            "how_to": "Откат и применение шаблонов: python -X utf8 -m scripts.manage_snapshots (список снимков и откат по номеру), "
                      "python -X utf8 -m scripts.strategy_proposals apply <id> (применить шаблон), затем выкатка."}
