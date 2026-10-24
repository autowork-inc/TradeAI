"""Выбор монет и загрузка истории Playground.

    python -X utf8 -m playground.download --top 10 --from 2025-10-24 [--to 2026-10-24] [--symbols BTCUSDT,ETHUSDT]

--top N: самые ликвидные USDT-перпетуалы Binance по суточному quoteVolume, торгующиеся с запасом до начала периода (прогрев 1d-bias = 90 суток).
Печатает выбранные символы (последняя строка - через запятую) и докачивает их минутки в кэш (playground.history).
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

import config
from playground import paths
from playground.engine import WARMUP_MS
from playground.history import ensure_minutes
from utils.binance_rest import BinanceRest

log = logging.getLogger("playground.download")


def parse_day(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


async def top_symbols(api: BinanceRest, n: int, listed_before_ms: int) -> list[str]:
    info = await api.get("/fapi/v1/exchangeInfo", weight=1)
    ok = {s["symbol"] for s in info["symbols"] if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
          and s.get("status") == "TRADING" and int(s.get("onboardDate") or 0) <= listed_before_ms and s["symbol"].isascii()}
    tickers = await api.get("/fapi/v1/ticker/24hr", weight=40)
    ranked = sorted((t for t in tickers if t["symbol"] in ok), key=lambda t: float(t["quoteVolume"]), reverse=True)
    return [t["symbol"] for t in ranked[:n]]


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=0)
    p.add_argument("--symbols", default="")
    p.add_argument("--from", dest="start", required=True)
    p.add_argument("--to", dest="end", default="")
    a = p.parse_args()
    start = parse_day(a.start)
    end = parse_day(a.end) if a.end else int(datetime.now(timezone.utc).timestamp() * 1000) // 60_000 * 60_000
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        api = BinanceRest(session, config.REST_BASE_URL)
        symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()]
        if a.top:
            symbols += [s for s in await top_symbols(api, a.top, start - WARMUP_MS) if s not in symbols]
        print("SYMBOLS", ",".join(symbols), flush=True)
        for s in symbols:
            await ensure_minutes(api, paths.klines_root(), s, start - WARMUP_MS, end)
            print("DONE", s, flush=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
