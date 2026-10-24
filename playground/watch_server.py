"""Сервис watch (Live-Dashboard, 1.8.0): по запросу клиента считает «взгляд стратегии» (playground/watch.py) на свежих свечах Binance.

    python -m playground.watch_server        (контейнер watch, профиль compose playground; наружу не публикуется - его зовёт API: /api/watch/*)

GET /meta                         -> live-стратегии (APPROVED_STRATEGIES), все стратегии с их таймфреймом и монетами (автоселектор бота, только крипта)
GET /view?strategy=..&symbol=..   -> взгляд стратегии + формирующаяся свеча
GET /coins?strategy=..            -> монеты, где выполнено >= 2 условий (больше 15 таких - >= 3); фоновый прогрев держит взгляды всех монет свежими

Свечи: публичный REST Binance, кэш по (монета, таймфрейм): закрытые перекачиваются только после закрытия новой свечи, формирующаяся - не чаще раза в 10 с.
Контекст строится как в боте: окно последних 300/90 закрытых свечей -> FeatureEngine -> MtfState (4h; у SMC_Pro_Trend 1h + 1d). Взгляд кэшируется до
следующей закрытой свечи. Сбой Binance - 503 с причиной (процесс живёт); ошибка кода - 500 в ответе и трассировка в логе aiohttp.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl
from aiohttp import web

import config
from collectors.klines_downloader import parse_klines
from features import defaults as D
from features.smc import SmcParams
from orchestrator.feature_engine import FeatureEngine
from orchestrator.mtf import MtfState
from orchestrator.multi_strategies import OBI_BREAKOUT
from orchestrator.registry import SHADOW_STRATEGIES, build_strategies
from orchestrator.strategies import SMC_PRO_TREND
from playground import watch
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import DataError, TradingInfraError

log = logging.getLogger("playground.watch_server")

HISTORY = {"15m": 300, "5m": 300, "1h": D.MTF_STRUCT_HISTORY_BARS, "4h": D.HTF_HISTORY_BARS, "1d": D.MTF_BIAS_HISTORY_BARS}
FORMING_TTL_S = 10.0
COINS_MIN_OK = 2  # монета видна в выборе, если по какой-то стороне выполнено столько условий чек-листа (решение пользователя 2026-10-24)
COINS_MAX = 15  # таких больше - порог +1
UNIVERSE_FILE = ".symbol_universe.json"
OHLCV = ["open_time", "open", "high", "low", "close", "volume"]


def bot_dirs() -> list[Path]:
    return [Path(p) for p in os.environ.get("WATCH_BOT_DIRS", "").split(",") if p.strip()]


def universe(dirs: list[Path]) -> tuple[list[str], list[str]]:
    """(live-уровень, Binance Test) из самого свежего снимка автоселектора бота; нет файла - закреплённые монеты."""
    best = None
    for d in dirs:
        path = d / UNIVERSE_FILE
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            item = (float(data["updated_ms"]), list(data["live"]), list(data["test"]))
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if best is None or item[0] > best[0]:
            best = item
    if best is None:
        return list(config.UNIVERSE_PINNED), []
    return best[1], best[2]


class WatchService:
    def __init__(self, api: BinanceRest, *, clock=time.time, dirs: list[Path] | None = None):
        self._api, self._clock = api, clock
        self._dirs = bot_dirs() if dirs is None else dirs
        self.strategies = {s.name: s for s in build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")}
        self._closed: dict[tuple[str, str], pl.DataFrame] = {}
        self._forming: dict[tuple[str, str], tuple[float, dict | None]] = {}
        self._views: dict[tuple[str, str], tuple[int, dict]] = {}
        self._locks: dict[tuple, asyncio.Lock] = {}
        self._engines = {tf: FeatureEngine(SmcParams(), min_bars=60) if tf == "1d" else FeatureEngine(SmcParams()) for tf in HISTORY}

    def _lock(self, key: tuple) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def now_ms(self) -> int:
        return int(self._clock() * 1000)

    # ------------------------------------------------------------------------------------------ данные

    def meta(self) -> dict[str, Any]:
        live, test = universe(self._dirs)
        crypto = [c for c in dict.fromkeys([*config.UNIVERSE_PINNED, *live, *test]) if c.endswith("USDT")]
        approved = [s.strip() for s in os.environ.get("APPROVED_STRATEGIES", ",".join(config.APPROVED_STRATEGIES)).split(",") if s.strip()]
        out = {}
        for name, s in self.strategies.items():
            coins = [c for c in dict.fromkeys([*config.UNIVERSE_PINNED, *live]) if c.endswith("USDT")] if name == OBI_BREAKOUT else crypto
            out[name] = {"interval": s.interval, "coins": coins}
        return {"live_strategies": [a for a in approved if a in out], "strategies": out, "live_coins": [c for c in dict.fromkeys([*config.UNIVERSE_PINNED, *live])]}

    async def _fetch(self, symbol: str, tf: str, limit: int) -> pl.DataFrame:
        rows = await self._api.get(config.KLINES_PATH, {"symbol": symbol, "interval": tf, "limit": limit}, weight=klines_weight(limit))
        return parse_klines(rows).select(OHLCV)

    async def closed(self, symbol: str, tf: str) -> pl.DataFrame:
        """Закрытые свечи (последние HISTORY[tf]); перекачка - только когда закрылась новая."""
        key, ms = (symbol, tf), config.INTERVAL_MS[tf]
        async with self._lock(("k", symbol, tf)):
            have = self._closed.get(key)
            now = self.now_ms()
            if have is not None and int(have["open_time"][-1]) + 2 * ms > now:
                return have
            df = await self._fetch(symbol, tf, HISTORY[tf] + 1)
            df = df.filter(pl.col("open_time") + ms <= now)
            if df.height < 60:
                raise DataError(f"{symbol} {tf}: у Binance всего {df.height} закрытых свечей (монета слишком новая)")
            self._closed[key] = df
            return df

    async def forming(self, symbol: str, tf: str) -> dict | None:
        key = (symbol, tf)
        got = self._forming.get(key)
        if got is not None and self._clock() - got[0] < FORMING_TTL_S:
            return got[1]
        df = await self._fetch(symbol, tf, 1)
        row = df.to_dicts()[-1] if df.height else None
        if row is not None and row["open_time"] + config.INTERVAL_MS[tf] <= self.now_ms():
            row = None  # последняя свеча уже закрыта (граница минуты): формирующейся пока нет
        self._forming[key] = (self._clock(), row)
        return row

    # ------------------------------------------------------------------------------------------ взгляд

    async def view(self, name: str, symbol: str) -> dict[str, Any]:
        out = dict(await self.look(name, symbol))
        out["forming"] = await self.forming(symbol, self.strategies[name].interval)
        out["server_ms"] = self.now_ms()
        return out

    async def look(self, name: str, symbol: str) -> dict[str, Any]:
        """Взгляд стратегии на последней закрытой свече (кэш до следующей), без формирующейся свечи."""
        s = self.strategies.get(name)
        if s is None:
            raise DataError(f"неизвестная стратегия {name}")
        tf = s.interval
        work = await self.closed(symbol, tf)
        bar = int(work["open_time"][-1])
        cached = self._views.get((name, symbol))
        if cached is None or cached[0] != bar:
            async with self._lock(("v", name, symbol)):
                cached = self._views.get((name, symbol))
                if cached is None or cached[0] != bar:
                    mtf = name == SMC_PRO_TREND
                    htf = MtfState(config.INTERVAL_MS, None, base=D.MTF_BASE_INTERVAL if mtf else None, struct=D.MTF_STRUCT_INTERVAL,
                                   bias=D.MTF_BIAS_INTERVAL, legacy=None if mtf else "15m", legacy_bias=D.HTF_INTERVAL)
                    states = {st: await self.closed(symbol, st) for st in htf.state_intervals}
                    v = await asyncio.to_thread(self._compute, s, symbol, tf, work, states, htf)
                    cached = (bar, v)
                    self._views[(name, symbol)] = cached
        return cached[1]

    # ------------------------------------------------------------------------------------------ фильтр монет

    def coins(self, name: str) -> dict[str, Any]:
        """Монеты стратегии, где по какой-либо стороне выполнено >= 2 условий чек-листа (если таких больше COINS_MAX - >= 3), лучшие сверху.
        Считается по уже посчитанным взглядам (их держит в актуальном виде фоновый прогрев `warm`); ещё не посчитанные - в `pending`."""
        if name not in self.strategies:
            raise DataError(f"неизвестная стратегия {name}")
        all_coins = self.meta()["strategies"][name]["coins"]
        rows, pending = [], []
        for c in all_coins:
            got = self._views.get((name, c))
            if got is None:
                pending.append(c)
                continue
            best = max(got[1]["sides"].items(), key=lambda kv: (sum(1 for x in kv[1]["checks"] if x["ok"] is True), kv[1]["stage"]))
            rows.append({"coin": c, "score": sum(1 for x in best[1]["checks"] if x["ok"] is True), "side": best[0], "stage": max(v["stage"] for v in got[1]["sides"].values())})
        rows.sort(key=lambda r: (r["stage"] != 3, -r["score"], -r["stage"]))  # сигналы первыми, дальше - больше выполненных условий
        threshold = COINS_MIN_OK
        if sum(1 for r in rows if r["score"] >= threshold) > COINS_MAX:
            threshold += 1
        keep = [r for r in rows if r["score"] >= threshold or r["stage"] == 3]  # сигнал показывается всегда
        return {"strategy": name, "threshold": threshold, "coins": keep, "hidden": len(rows) - len(keep), "pending": pending, "total": len(all_coins)}

    async def warm(self, pause_s: float = 2.0) -> None:
        """Фон: держать взгляды всех стратегий по всем монетам свежими (для фильтра монет). Пересчёт - только когда закрылась новая свеча (кэш look)."""
        while True:
            meta = self.meta()
            for name, info in meta["strategies"].items():
                for coin in info["coins"]:
                    try:
                        await self.look(name, coin)
                    except DataError as exc:  # монета слишком новая / нет истории - пропускаем
                        log.info("прогрев %s %s: %s", name, coin, exc)
                    except TradingInfraError as exc:  # Binance недоступен / бан - пауза и следующий круг
                        log.warning("прогрев остановлен: %s", exc)
                        await asyncio.sleep(60)
                        break
                    await asyncio.sleep(0.05)
            await asyncio.sleep(pause_s)

    def _compute(self, s, symbol: str, tf: str, work: pl.DataFrame, states: dict[str, pl.DataFrame], htf: MtfState) -> dict:
        received = self.now_ms()
        for st, df in states.items():
            htf.update(self._engines[st].build(symbol, st, config.INTERVAL_MS[st], df, received))
        ctx = htf.attach(self._engines[tf].build(symbol, tf, config.INTERVAL_MS[tf], work, received))
        return watch.view(s, ctx)


def make_app(service: WatchService, token: str) -> web.Application:
    @web.middleware
    async def auth(request: web.Request, handler):
        if request.path != "/health" and (not token or request.headers.get("X-Watch-Token") != token):
            return web.json_response({"detail": "unauthorized"}, status=401)
        return await handler(request)

    app = web.Application(middlewares=[auth])

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def meta(_: web.Request) -> web.Response:
        return web.json_response(service.meta())

    async def view(request: web.Request) -> web.Response:
        name, symbol = request.query.get("strategy", ""), request.query.get("symbol", "").upper()
        if not symbol.isalnum() or not symbol.endswith("USDT") or len(symbol) > 24:
            return web.json_response({"detail": "bad_symbol"}, status=422)
        try:
            return web.json_response(await service.view(name, symbol))
        except DataError as exc:
            return web.json_response({"detail": f"data: {exc}"}, status=422)
        except TradingInfraError as exc:  # сеть / Binance (бан, 5xx, таймаут): сервис живёт дальше
            log.warning("watch %s %s: %s", name, symbol, exc)
            return web.json_response({"detail": f"binance: {exc}"}, status=503)

    app.router.add_get("/health", health)
    app.router.add_get("/meta", meta)
    async def coins(request: web.Request) -> web.Response:
        try:
            return web.json_response(service.coins(request.query.get("strategy", "")))
        except DataError as exc:
            return web.json_response({"detail": f"data: {exc}"}, status=422)

    app.router.add_get("/view", view)
    app.router.add_get("/coins", coins)
    return app


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if hasattr(os, "nice"):
        os.nice(10)
    token = os.environ.get("WATCH_TOKEN", "")
    if len(token) < 32:
        raise SystemExit("WATCH_TOKEN не задан (нужен общий секрет с API, >= 32 символов)")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        service = WatchService(BinanceRest(session, config.REST_BASE_URL))
        runner = web.AppRunner(make_app(service, token))
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8000).start()
        log.info("watch: слушаю :8000, стратегий %d", len(service.strategies))
        await service.warm()  # фоновый прогрев взглядов для фильтра монет; ошибка кода - падение процесса (R1), Docker перезапускает


if __name__ == "__main__":
    asyncio.run(main())
