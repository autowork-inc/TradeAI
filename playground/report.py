"""Сводный отчёт пакета прогонов Playground: стратегии на всех монетах, по монетам, по кварталам, причины выхода и сравнение планов.

    python -X utf8 -m playground.report --batch top10 [--out out/playground/top10.md]
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from playground import paths
from playground.engine import _stats
from playground.store import Store


def quarter(ms: int) -> str:
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{d.year}-Q{(d.month - 1) // 3 + 1}"


def fr(x, nd=2, unit=""):
    return "—" if x is None else f"{x:+.{nd}f}{unit}"


def pct(x):
    return "—" if x is None else f"{100 * x:.0f}%"


def table(rows: list[list[str]], head: list[str]) -> str:
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--batch", required=True)
    p.add_argument("--out", default="")
    a = p.parse_args()
    store = Store(paths.db_path())
    runs = [r for r in store.runs(limit=10_000) if r["batch"] == a.batch and r["status"] == "done"]
    trades = []
    for r in runs:
        for t in store.trades(r["id"]):
            trades.append({**t, "_run": r["id"]})
    closed = [t for t in trades if t["status"] == "closed" and t.get("r_multiple") is not None]
    period = (min(r["start_ms"] for r in runs), max(r["end_ms"] for r in runs)) if runs else (0, 0)
    lines = [f"# Playground: пакет «{a.batch}»", "",
             f"Прогонов: {len(runs)}; монет: {len({r['symbol'] for r in runs})}; период {datetime.fromtimestamp(period[0] / 1000, tz=timezone.utc):%Y-%m-%d} → "
             f"{datetime.fromtimestamp(period[1] / 1000, tz=timezone.utc):%Y-%m-%d}; закрытых сделок: {len(closed)}. Риск $5 на сделку.", ""]

    by_s: dict[str, list] = defaultdict(list)
    for t in trades:
        by_s[t["strategy_id"]].append(t)
    rows = []
    for name, ts in sorted(by_s.items(), key=lambda kv: -_stats(kv[1])["sum_r"]):
        s = _stats(ts)
        rows.append([name, str(s["closed"]), pct(s["winrate"]), fr(s["avg_r"], 3), fr(s["sum_r"]), fr(s["pnl"], 0, " $"),
                     "—" if s["profit_factor"] is None else f"{s['profit_factor']:.2f}", f"{s['max_dd_r']:.1f}", str(s["cancelled"])])
    lines += ["## Стратегии (все монеты)", "", table(rows, ["Стратегия", "Сделок", "Винрейт", "Ср. R", "Σ R", "PnL", "PF", "Макс. просадка R", "Отменено"]), ""]

    coins = sorted({t["coin"] for t in trades})
    rows = []
    for name in sorted(by_s):
        rows.append([name] + [fr(_stats([t for t in by_s[name] if t["coin"] == c])["sum_r"], 1) for c in coins])
    lines += ["## Σ R по монетам", "", table(rows, ["Стратегия"] + [c.replace("USDT", "") for c in coins]), ""]

    qs = sorted({quarter(t["signal_time"]) for t in closed})
    rows = []
    for name in sorted(by_s):
        cells = []
        for q in qs:
            s = _stats([t for t in by_s[name] if quarter(t["signal_time"]) == q])
            cells.append(f"{fr(s['sum_r'], 1)} ({s['closed']})")
        rows.append([name] + cells)
    lines += ["## Σ R по кварталам (в скобках - сделок): устойчивость во времени", "", table(rows, ["Стратегия"] + qs), ""]

    rows = []
    for name in sorted(by_s):
        cs = [t for t in closed if t["strategy_id"] == name]
        reasons = defaultdict(list)
        for t in cs:
            reasons[t.get("exit_reason")].append(t["r_multiple"])
        mfe = sorted(t.get("mfe_r") or 0 for t in cs)
        med = mfe[len(mfe) // 2] if mfe else None
        reach1 = sum(1 for x in mfe if x >= 1) / len(mfe) if mfe else None
        reach4 = sum(1 for x in mfe if x >= 4) / len(mfe) if mfe else None
        rows.append([name, ", ".join(f"{k} {len(v)}" for k, v in sorted(reasons.items(), key=lambda kv: -len(kv[1]))), fr(med, 2), pct(reach1), pct(reach4),
                     fr(sum(x for x in [t.get("mae_r") for t in cs] if x) / len(cs) if cs else None, 2)])
    lines += ["## Выходы и ход цены (MFE - лучший ход до выхода, в R)", "",
              table(rows, ["Стратегия", "Причины выхода", "Медиана MFE", "Дошли до +1R", "Дошли до +4R", "Средний MAE"]), ""]

    side_rows = []
    for name in sorted(by_s):
        for side in ("long", "short"):
            s = _stats([t for t in by_s[name] if t["side"] == side])
            side_rows.append([name, side, str(s["closed"]), pct(s["winrate"]), fr(s["sum_r"], 1)])
    lines += ["## Лонг / шорт", "", table(side_rows, ["Стратегия", "Сторона", "Сделок", "Винрейт", "Σ R"]), ""]

    out = Path(a.out) if a.out else Path("out/playground") / f"{a.batch}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
