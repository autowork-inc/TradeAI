"""Анализ MFE, тейк-профита, стопа и корреляций по стратегиям на реальных сделках (ТОЛЬКО ЧТЕНИЕ БД; бот не трогается).

Запуск (нужен SSH-туннель к БД, как у mcp_server; DATABASE_URL - окружение или mcp_server/.env):
  python -X utf8 -m scripts.analyze_tp                      # с 2026-10-20, все стратегии кроме CHR, Shadow + история с 13.10
  python -X utf8 -m scripts.analyze_tp --klines             # + проверка и что-если по 1m-свечам Binance (кэш out/trade_lab/klines)
  python -X utf8 -m scripts.analyze_tp --mode live          # боевые копии отдельно (с Shadow не складываются)
  python -X utf8 -m scripts.analyze_tp --strategy VWAP_Reversion --ref-plan 2
Результат: консоль + out/trade_lab/report.md + out/trade_lab/trades.csv (разделитель «;», открывается в Excel).
Подробно о методе - scripts/trade_lab/exits.py и CLAUDE.md, разд. 3.26.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

from scripts.trade_lab import exits, klines, report, stops
from scripts.trade_lab.data import CHR_STRATEGIES, ROOT, LabError, Ledger, Trade, analysable, build, fetch_rows, ledger_of, read_database_url

OUT = ROOT / "out" / "trade_lab"
STOP_MULTS = (0.5, 0.75, 1.0, 1.25, 1.5)
KL_TPS = (1.0, 1.5, 2.0, 3.0, 4.0)


def parse_day(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MFE / тейк / стоп / корреляции по сделкам из БД (только чтение)")
    p.add_argument("--since", default="2026-10-20", help="основной анализ: сделки с сигналом с этой даты UTC (по умолчанию 2026-10-20)")
    p.add_argument("--history-from", default="2026-10-13", help="база для сравнения версий «до/после» (13.10 - очистка БД)")
    p.add_argument("--mode", choices=("shadow", "live"), default="shadow", help="shadow = все сигналы; live - боевые копии (отдельно)")
    p.add_argument("--include-chr", action="store_true", help="не исключать CHR-стратегии")
    p.add_argument("--strategy", action="append", help="только эти стратегии (можно несколько раз)")
    p.add_argument("--ref-plan", type=float, default=None, help="тейк (R) для блоков стопа/корреляций; по умолчанию - лучший по выборке")
    p.add_argument("--klines", action="store_true", help="1m-свечи Binance: сверка MFE, что-если по стопу, «выбило и ушло»")
    p.add_argument("--out", default=str(OUT), help="каталог отчёта")
    return p.parse_args(argv)


def kline_analysis(trades: list[Trade], cache: Path) -> dict:
    store = klines.KlineStore(cache)
    res: dict = {"n": 0, "skipped_symbol": 0, "unfinished": 0, "grid": {}, "hunt": [], "sanity": None, "sanity_fact": None}
    sanity, sanity_fact, mfe_pairs = [], [], []
    for i, t in enumerate(sorted(trades, key=lambda x: x.signal_ms), 1):
        if t.entry is None or t.entry_ms is None or not t.risk:
            continue
        end = max(t.entry_ms + klines.HORIZON_MS, (t.exit_ms or 0) + klines.HUNT_MS)
        try:
            bars = store.range(t.coin, t.entry_ms - klines.MIN_MS, end)
        except klines.Unavailable:
            res["skipped_symbol"] += 1
            continue
        if i % 25 == 0:
            print(f"  1m-свечи: {i}/{len(trades)} (запросов к Binance: {store.requests})", file=sys.stderr)
        base = klines.path_sim(t, bars, t.sl, 4.0, store.now_ms)
        if base is None:
            res["unfinished"] += 1
            continue
        res["n"] += 1
        if not t.old_plan and t.actual_r is not None:
            sanity.append(base)
            sanity_fact.append(t.actual_r)
        km = klines.kline_mfe(t, bars)
        if km is not None and t.mfe is not None:
            mfe_pairs.append((t, km))
        variants = [(f"SL ×{m:g}", t.entry - t.dir * m * t.risk) for m in STOP_MULTS]
        anchor, atr = t.extra.get("anchor"), t.extra.get("atr")
        if anchor is not None and atr:
            variants.append(("структура + 0.2 ATR", anchor - t.dir * 0.2 * atr))
        for label, sl in variants:
            for x in KL_TPS:
                r = klines.path_sim(t, bars, sl, x, store.now_ms)
                if r is not None:
                    res["grid"].setdefault((label, x), []).append(r)
        if exits.exit_class(t) == "stop" and t.exit_ms is not None:
            res["hunt"].append({**klines.stop_hunt(t, bars, atr), "strategy": t.strategy})
    if sanity:
        res["sanity"], res["sanity_fact"] = sum(sanity) / len(sanity), sum(sanity_fact) / len(sanity_fact)
    if mfe_pairs:
        diffs = [abs(km - (t.mfe or 0)) for t, km in mfe_pairs]
        restored = [(t, km) for t, km in mfe_pairs if t.restored]
        res["mfe_check"] = {"n": len(diffs), "med_abs": median(diffs), "big": sum(d > 0.3 for d in diffs), "restored_n": len(restored),
                            "restored_higher": sum(km > (t.mfe or 0) + 0.1 for t, km in restored)}
    return res


async def load(args: argparse.Namespace, since: datetime) -> tuple[list[Trade], Ledger]:
    exclude = [] if args.include_chr else list(CHR_STRATEGIES)
    rows = await fetch_rows(read_database_url(), since, args.mode, exclude)
    trades, ledger = build(rows)
    if args.strategy:
        keep = set(args.strategy)
        trades = [t for t in trades if t.strategy in keep]
    return trades, ledger


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    since, hist_from = parse_day(args.since), parse_day(args.history_from)
    start = min(since, hist_from)
    try:
        all_trades, ledger = asyncio.run(load(args, start))
    except LabError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 3
    since_ms = int(since.timestamp() * 1000)
    hist_ms = int(hist_from.timestamp() * 1000)
    stops.annotate(all_trades)
    main_set = [t for t in analysable(all_trades) if t.signal_ms >= since_ms]
    clean_set = [t for t in analysable(all_trades, clean_only=True) if t.signal_ms >= since_ms]
    hist_set = [t for t in analysable(all_trades) if t.signal_ms >= hist_ms]
    main_rows_ledger = ledger_of([t for t in all_trades if t.signal_ms >= since_ms])

    if args.ref_plan is not None:
        ref = exits.tp(args.ref_plan)
    else:
        best = exits.best_tp(main_set, exits.TP_GRID)
        ref = exits.tp(best[0]) if best else exits.CURRENT

    excl = "все стратегии" if args.include_chr else f"все стратегии, кроме {', '.join(CHR_STRATEGIES)}"
    parts = [f"# Лаборатория сделок: тейк, стоп, корреляции\n\nСформировано {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC. Режим `{args.mode}`, {excl}"
             + (f", только {', '.join(args.strategy)}" if args.strategy else "") + f". Основной период — сигналы с {args.since}; история — с {args.history_from}. "
             "Риск на сделку $5 → USD = R × risk_amount.",
             "## Сверка: куда ушла каждая сделка",
             report.ledger_section(main_rows_ledger, f"Период с {args.since}"),
             report.ledger_section(ledger, f"Вся выгрузка с {start:%Y-%m-%d} (включая историю)"),
             report.flags_section(main_set),
             "## Блок 1. MFE и фиксированный тейк",
             report.mfe_section(main_set),
             report.tp_section(main_set, f"Все сделки с {args.since} (включая восстановленные и gap_affected)"),
             report.tp_section(clean_set, "Только чистые (как официальная статистика: без gap_affected)")]
    for name, items in exits.by(main_set, lambda t: t.strategy).items():
        parts.append(report.tp_section(items, f"Стратегия {name}"))
    parts += [report.plans_section(main_set), report.stops_section(main_set, ref), report.correlation_section(main_set, ref),
              report.history_section(hist_set)]
    if args.klines:
        print("Загрузка 1m-свечей Binance (первый раз долго, потом из кэша)…", file=sys.stderr)
        try:
            parts.append(report.klines_section(kline_analysis(main_set, Path(args.out) / "klines")))
        except LabError as exc:
            parts.append(f"## Блок 6. 1m-свечи\n\nНе выполнено: {exc}")
    parts.append(report.CAVEATS)
    text = "\n\n".join(parts) + "\n"

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.md").write_text(text, encoding="utf-8")
    report.write_csv(out / "trades.csv", [t for t in all_trades if t.signal_ms >= hist_ms], ref)
    print(text)
    print(f"\nОтчёт: {out / 'report.md'}\nСделки: {out / 'trades.csv'}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
