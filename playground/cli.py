"""Прогон Playground из командной строки (фоновые прогоны) и выполнение прогона из очереди (воркер сервиса).

    python -X utf8 -m playground.cli run --symbol BTCUSDT --from 2025-10-24 [--to 2026-10-24] [--strategies A,B | --group santa|chr] [--batch top10]
    python -X utf8 -m playground.cli list
    python -X utf8 -m playground.cli export --to <путь к другому playground.sqlite> [--batch top10]
    python -X utf8 -m playground.cli import --from <файл playground.sqlite>   (на сервере: прогоны, посчитанные на ПК)

Минутки должны быть в кэше (playground.download); при нехватке run докачивает их сам.
"""

import argparse
import asyncio
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

import config
from orchestrator.registry import HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR
from playground import paths
from playground.engine import PLAYGROUND_STRATEGIES, WARMUP_MS, RunSpec, simulate, summarize
from playground.history import ensure_minutes, load_minutes
from playground.store import Store
from utils.binance_rest import BinanceRest
from utils.errors import TradingInfraError

log = logging.getLogger("playground.cli")

CHR_GROUP = (TRENDLINE_BREAKOUT_CHR, HORIZONTAL_BREAKOUT_CHR)
GROUPS = {"chr": CHR_GROUP, "santa": tuple(s for s in PLAYGROUND_STRATEGIES if s not in CHR_GROUP), "all": PLAYGROUND_STRATEGIES}


def day_ms(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def code_version() -> str | None:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip() or None
    except OSError:
        return None


async def fetch_history(spec: RunSpec, on_progress=None) -> None:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        await ensure_minutes(BinanceRest(session, config.REST_BASE_URL), paths.klines_root(), spec.symbol, spec.start_ms - WARMUP_MS, spec.end_ms, on_progress)


def execute(store: Store, run_id: str) -> dict:
    """Выполнить прогон: история -> симуляция -> запись. Сбой данных/сети - прогон failed (ошибка кода - исключение наружу)."""
    r = store.run(run_id)
    spec = RunSpec(r["symbol"], tuple(r["strategies"]), r["start_ms"], r["end_ms"], r["tp_r"])
    store.progress(run_id, 0.0, "загрузка истории")
    asyncio.run(fetch_history(spec, lambda p: store.progress(run_id, 0.1 * p, "загрузка истории")))
    minutes = load_minutes(paths.klines_root(), spec.symbol, spec.start_ms - WARMUP_MS, spec.end_ms)
    last = [0.0]

    def progress(p: float) -> None:
        if p - last[0] >= 0.01:
            last[0] = p
            store.progress(run_id, 0.1 + 0.9 * p, "прогон по истории")

    result = simulate(spec, minutes, progress)
    return store.save_result(run_id, result, summarize(result.trades, result.skipped))


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--symbol", required=True)
    r.add_argument("--from", dest="start", required=True)
    r.add_argument("--to", dest="end", default="")
    r.add_argument("--strategies", default="")
    r.add_argument("--group", choices=sorted(GROUPS), default="all")
    r.add_argument("--batch", default=None)
    r.add_argument("--label", default=None)
    sub.add_parser("list")
    e = sub.add_parser("export")
    e.add_argument("--to", dest="target", required=True)
    e.add_argument("--batch", default=None)
    i = sub.add_parser("import")
    i.add_argument("--from", dest="source", required=True)
    a = p.parse_args()
    store = Store(paths.db_path())
    if a.cmd == "list":
        for run in store.runs():
            o = (run["summary"] or {}).get("overall") or {}
            print(run["id"], run["status"], run["symbol"], ",".join(run["strategies"]), f"{run['progress']:.0%}",
                  f"closed={o.get('closed')} sumR={o.get('sum_r')} pnl={o.get('pnl')}", run.get("error") or "")
        return
    if a.cmd == "export":
        ids = [x["id"] for x in store.runs(limit=10_000) if x["status"] == "done" and (a.batch is None or x["batch"] == a.batch)]
        print("exported", store.export_runs(ids, Store(Path(a.target))))
        return
    if a.cmd == "import":  # готовые прогоны из файла (локальные фоновые прогоны) -> хранилище этого Playground (на сервере)
        src = Store(Path(a.source))
        print("imported", src.export_runs([x["id"] for x in src.runs(limit=10_000) if x["status"] == "done"], store))
        return
    strategies = tuple(s.strip() for s in a.strategies.split(",") if s.strip()) or GROUPS[a.group]
    end = day_ms(a.end) if a.end else int(datetime.now(timezone.utc).timestamp() * 1000) // 86_400_000 * 86_400_000
    spec = RunSpec(a.symbol.upper(), strategies, day_ms(a.start), end)
    profile = "CHR" if set(strategies) <= set(CHR_GROUP) else "SANTA"
    run_id = store.create(spec.symbol, spec.strategies, spec.start_ms, spec.end_ms, label=a.label, profile=profile, batch=a.batch, status="running", code_version=code_version())
    print("RUN", run_id, flush=True)
    try:
        summary = execute(store, run_id)
    except TradingInfraError as exc:  # данные / сеть: прогон помечается failed, процесс завершается с ошибкой
        store.fail(run_id, f"{type(exc).__name__}: {exc}")
        raise
    o = summary["overall"]
    print("DONE", run_id, f"closed={o['closed']} winrate={o['winrate']} sumR={o['sum_r']:.2f} pnl={o['pnl']:.2f}", flush=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
