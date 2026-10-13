"""СИНТЕТИЧЕСКИЕ демо-данные для разработки и показа клиента (Этап 6): сделки, снапшоты, закрытия, состояния автомата.

Это НЕ реальные сделки и НЕ результат стратегий: цены - случайное блуждание, исходы заданы вручную. Нужны, чтобы увидеть Dashboard,
Statistics, History и график, не дожидаясь накопления данных в боевом Shadow. Защита: имя БД обязано оканчиваться на `_demo` или
`_test` (схема public пересоздаётся) - на боевую базу `tradeai` скрипт не запустится.

    python -m scripts.demo_api_data --database-url postgresql://u:p@127.0.0.1:54331/tradeai_test --heartbeat-dir out/demo_shadow
"""

import argparse
import asyncio
import random
import time
from pathlib import Path
from urllib.parse import urlsplit

from orchestrator.types import geometry
from persistence import schema
from persistence.db import Database
from persistence.pg import to_args
from utils.errors import ConfigError

IV = 900_000  # 15m
COINS = {"BTCUSDT": 97_000.0, "ETHUSDT": 3_900.0, "DOGEUSDT": 0.185, "1000PEPEUSDT": 0.0112}
# (монета, сторона, стратегия, исход: [(причина, доля, R_чистая)], статус)
SCENARIOS = [
    ("BTCUSDT", "short", "smc", [("scale_1r", 0.5, 0.49), ("scale_3r", 0.4, 1.19), ("trail", 0.1, 0.46)], "closed"),
    ("BTCUSDT", "long", "smc", [("sl", 1.0, -1.04)], "closed"),
    ("ETHUSDT", "short", "smc", [("scale_1r", 0.5, 0.49), ("be", 0.5, -0.02)], "closed"),
    ("ETHUSDT", "long", "smc", [("sl", 1.0, -1.04)], "closed"),
    ("DOGEUSDT", "long", "breakout", [("scale_1r", 0.5, 0.48), ("scale_3r", 0.4, 1.17), ("trail", 0.1, 0.9)], "closed"),
    ("DOGEUSDT", "short", "smc", [("sl", 1.0, -1.05)], "closed"),
    ("1000PEPEUSDT", "short", "smc", [("scale_1r", 0.5, 0.48), ("be", 0.5, -0.03)], "closed"),
    ("1000PEPEUSDT", "long", "breakout", [("sl", 1.0, -1.06)], "closed"),
    ("BTCUSDT", "short", "smc", [("timeout", 1.0, 0.31)], "closed"),
    ("ETHUSDT", "short", "smc", [], "cancelled"),
    ("DOGEUSDT", "short", "smc", [("scale_1r", 0.5, 0.49)], "open"),
    ("BTCUSDT", "long", "smc", [], "pending_limit"),
]


def make_candles(rng: random.Random, start_price: float, n: int, t_last: int) -> list[dict]:
    price, out = start_price, []
    for i in range(n):
        t = t_last - (n - 1 - i) * IV
        o = price
        c = o * (1 + rng.gauss(0, 0.0035))
        hi = max(o, c) * (1 + abs(rng.gauss(0, 0.0018)))
        lo = min(o, c) * (1 - abs(rng.gauss(0, 0.0018)))
        out.append({"open_time": t, "open": o, "high": hi, "low": lo, "close": c, "volume": rng.uniform(80, 400),
                    "atr": None if i < 14 else (hi - lo) * 1.3})
        price = c
    return out


def build_trade(rng: random.Random, i: int, scenario: tuple, signal_bar_open: int) -> dict:
    coin, side, strategy, closes, status = scenario
    candles = make_candles(rng, COINS[coin], 100, signal_bar_open)
    short = side == "short"
    sgn = 1 if short else -1
    last = candles[-1]
    recent = candles[-25:-12]
    pool = max(c["high"] for c in recent) if short else min(c["low"] for c in recent)
    sweep = candles[-12]
    extreme = max(sweep["high"], pool * 1.001) if short else min(sweep["low"], pool * 0.999)
    f1, f3 = candles[-9], candles[-7]
    zone_hi, zone_lo = (f1["low"], f3["high"]) if short else (f3["low"], f1["high"])
    zone_hi, zone_lo = max(zone_hi, zone_lo), min(zone_hi, zone_lo)
    if zone_hi - zone_lo < last["close"] * 0.0008:
        zone_hi, zone_lo = last["close"] * 1.0015, last["close"] * 0.9995
    entry = (zone_hi + zone_lo) / 2
    atr = last["close"] * 0.003
    sl = max(extreme + 0.2 * atr, entry + 0.4 * atr) if short else min(extreme - 0.2 * atr, entry - 0.4 * atr)
    risk = abs(entry - sl)
    tp = entry - sgn * 3 * risk
    signal_ms = signal_bar_open + IV + 1200
    tid = f"{strategy}:{coin}:15m:{signal_bar_open}:{side}"
    t_entry = signal_bar_open + 3 * IV + 40_000
    t_exit = t_entry + (len(closes) + 2) * 2 * IV
    exit_price = entry - sgn * risk * sum(c[2] for c in closes) if closes else None
    r_total = sum(c[2] for c in closes) if closes else None
    row = {
        "id": tid, "strategy_name": strategy, "coin": coin, "side": side, "kind": "limit" if strategy == "smc" else "market", "mode": "shadow",
        "status": status, "qty": round(100.0 / risk, 6), "remaining_frac": 0.0 if status == "closed" else (0.5 if status == "open" else 1.0),
        "signal_time": signal_ms, "entry_time": t_entry if status in ("closed", "open") else None,
        "exit_time": t_exit if status == "closed" else None, "planned_entry": entry,
        "entry_price": entry if status in ("closed", "open") else None, "exit_price": exit_price if status == "closed" else None,
        "sl": sl, "tp": tp, "stop_price": entry if status == "open" else sl, "risk": risk, "risk_amount": 100.0,
        "pnl": round(r_total * 100.0, 2) if status == "closed" and r_total is not None else None, "fees": 1.2 if closes else None,
        "r_multiple": r_total if status == "closed" else (sum(c[2] for c in closes) if closes else None),
        "gross_r": (r_total + 0.05) if status == "closed" and r_total is not None else None,
        "mfe_r": 3.2 if closes and closes[0][0].startswith("scale") else (0.4 if closes else None), "mae_r": -0.6 if closes else None,
        "exit_reason": closes[-1][0] if status == "closed" and closes else ("process_restart" if status == "aborted" else
                                                                         ("ttl" if status == "cancelled" else None)),
        "be_armed": any(c[0] in ("scale_1r", "be") for c in closes), "gap_affected": False, "client_order_id": None,
        "plan": {"levels": [[1.0, 0.5], [3.0, 0.4]], "trail_mode": "atr", "trail_atr_mult": 3.0, "runner_frac": 0.1},
    }
    geo = [geometry("zone", candles[-9]["open_time"], signal_ms + 10 * IV, zone_hi, zone_lo, "FVG"),
           geometry("line", candles[-25]["open_time"], sweep["open_time"], pool, pool, "пул ликвидности"),
           geometry("marker", sweep["open_time"], sweep["open_time"], extreme, extreme, "свип"),
           geometry("marker", signal_bar_open, signal_bar_open, last["close"], last["close"], "CHoCH"),
           geometry("line", signal_ms, signal_ms + 10 * IV, entry, entry, "вход (50% FVG)"),
           geometry("line", signal_ms, signal_ms + 10 * IV, sl, sl, "SL"), geometry("line", signal_ms, signal_ms + 10 * IV, tp, tp, "TP 3R")]
    if strategy == "breakout":
        geo = [geometry("line", candles[-30]["open_time"], signal_bar_open, pool, pool, "уровень равных хаёв"),
               geometry("marker", signal_bar_open, signal_bar_open, last["close"], last["close"], "пробой")]
        meta = {"level": pool, "vol_ratio": 2.4, "eqh_spread": 0.0007, "atr": atr}
    else:
        meta = {"pool_level": pool, "sweep_time": sweep["open_time"], "bars_since_sweep": 11, "risk_pct": risk / entry, "atr": atr}
    meta.update({"entry": entry, "sl": sl, "tp": tp, "inval": extreme, "interval": "15m"})
    close_rows = []
    for seq, (reason, frac, net) in enumerate(closes, start=1):
        px = entry - sgn * risk * (1.0 if reason == "scale_1r" else 3.0 if reason == "scale_3r" else (0.0 if reason == "be" else -1.0 if reason == "sl" else 3.6))
        close_rows.append({"trade_id": tid, "seq": seq, "ts": t_entry + seq * 3 * IV, "reason": reason, "frac": frac, "qty": row["qty"] * frac,
                           "price": px, "gross_r": net + 0.02, "fees_r": 0.02, "net_r": net, "pnl": net * 100.0 * frac, "fees": 0.2})
    return {"trade": row, "closes": close_rows, "snapshot": {"trade_id": tid, "created_at": signal_ms, "candles": candles, "geometry": geo, "meta": meta}}


async def seed(url: str, heartbeat_dir: Path | None, seed_value: int = 7) -> int:
    name = urlsplit(url).path.strip("/")
    if not name.endswith(("_demo", "_test")):
        raise ConfigError(f"Демо-данные только в БД *_demo / *_test (получено {name!r}): схема public будет пересоздана")
    db = await Database.connect(url, attempts=3, delay_s=0.5)
    async with db.pool.acquire() as conn:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    await db.migrate()
    rng = random.Random(seed_value)
    now_ms = int(time.time() * 1000)
    newest_bar = (now_ms - 2 * 3_600_000) // IV * IV
    async with db.pool.acquire() as conn:
        for i, sc in enumerate(SCENARIOS):
            d = build_trade(rng, i, sc, newest_bar - (len(SCENARIOS) - 1 - i) * 12 * IV)  # сделки раз в 3 часа
            await conn.execute(schema.UPSERT_TRADE, *to_args(d["trade"], schema.TRADE_COLUMNS))
            for c in d["closes"]:
                await conn.execute(schema.INSERT_CLOSE, *to_args(c, schema.CLOSE_COLUMNS))
            await conn.execute(schema.INSERT_SNAPSHOT, *to_args(d["snapshot"], schema.SNAPSHOT_COLUMNS))
        t = now_ms - 90_000
        for k, (coin, side, a, b, detail) in enumerate((("BTCUSDT", "short", 1, 2, {}), ("ETHUSDT", "long", 0, 1, {}),
                                                         ("DOGEUSDT", "short", 2, 3, {"reject": "exposure"}))):
            ev = {"ts": t + k * 1000, "coin": coin, "strategy": "smc", "signal_id": None, "from_state": f"{side}:S{a}", "to_state": f"{side}:S{b}",
                  "kind": "setup_state", "reason": None, "detail": {"side": side, "from": a, "to": b, **detail}}
            await conn.execute(schema.INSERT_STATE_EVENT, *to_args(ev, schema.STATE_EVENT_COLUMNS))
    await db.close()
    if heartbeat_dir is not None:
        heartbeat_dir.mkdir(parents=True, exist_ok=True)
        (heartbeat_dir / ".shadow_heartbeat").write_text(str(time.time()))
    return len(SCENARIOS)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--database-url", required=True)
    ap.add_argument("--heartbeat-dir", type=Path, default=None, help="каталог, куда положить свежий .shadow_heartbeat (имитация живого shadow-run)")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    n = asyncio.run(seed(a.database_url, a.heartbeat_dir, a.seed))
    print(f"Демо-данные записаны: {n} сделок (синтетика, не реальные результаты)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
