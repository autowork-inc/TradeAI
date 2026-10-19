"""Агрегированная статистика по закрытым сделкам - чистые функции над списком строк (тестируются без БД).

Что считается сделкой для статистики (так же, как `deploy/shadow.sh stats`): status = 'closed' и НЕ gap_affected (обрыв потока
тиков во время сделки искажает исход). aborted/cancelled/открытые в статистику не входят, но их количество отдаётся в `counts`.

Win Rate - доля сделок с net R > 0. Profit Factor - сумма положительных R / модуль суммы отрицательных; без убыточных сделок он
не определён (`profit_factor = null`, `profit_factor_infinite = true`, если есть прибыльные). Max drawdown - наибольшее падение
накопленного R от пика (в R), кривая считается по времени закрытия сделки.
"""

import math
from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from api.serialize import DataIntegrityError, iso

MIN_TRADES_FOR_CONCLUSIONS = 200  # CLAUDE.md: «сотни сделок»; ниже - в ответе стоит предупреждение, а не вывод
EQUITY_POINTS_MAX = 2000


def _summary(rs: list[float]) -> dict[str, Any]:
    n = len(rs)
    wins, losses = [r for r in rs if r > 0], [r for r in rs if r < 0]
    gross_profit, gross_loss = sum(wins), -sum(losses)
    mean = sum(rs) / n if n else None
    stderr = None
    if n >= 2:
        var = sum((r - mean) ** 2 for r in rs) / (n - 1)
        stderr = math.sqrt(var / n)
    return {
        "trades": n, "wins": len(wins), "losses": len(losses), "flat": n - len(wins) - len(losses),
        "win_rate": len(wins) / n if n else None,
        "net_r": sum(rs), "avg_r": mean, "avg_r_stderr": stderr,
        "gross_profit_r": gross_profit, "gross_loss_r": gross_loss,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
        "profit_factor_infinite": gross_loss == 0 and gross_profit > 0,
        "best_r": max(rs) if rs else None, "worst_r": min(rs) if rs else None,
    }


def _drawdown(rs_in_time_order: list[float]) -> float:
    peak = equity = worst = 0.0
    for r in rs_in_time_order:
        equity += r
        peak = max(peak, equity)
        worst = max(worst, peak - equity)
    return worst


def _equity_curve(points: list[tuple[datetime, float, float]]) -> list[dict[str, Any]]:
    cum, cum_pnl, out = 0.0, 0.0, []
    for ts, r, pnl in points:
        cum += r
        cum_pnl += pnl
        out.append({"ts": iso(ts), "cum_r": cum, "cum_pnl": cum_pnl})
    if len(out) > EQUITY_POINTS_MAX:  # прореживаем равномерно, последняя точка остаётся
        step = math.ceil(len(out) / EQUITY_POINTS_MAX)
        out = out[::step] + ([out[-1]] if (len(out) - 1) % step else [])
    return out


def compute_stats(rows: Iterable[dict[str, Any]], coins: Iterable[str], status_counts: dict[str, int] | None = None) -> dict[str, Any]:
    """rows: закрытые не-gap сделки с ключами coin, strategy_id, side, exit_reason, r_multiple, exit_time (datetime), в любом порядке."""
    trades: list[dict[str, Any]] = []
    for row in rows:
        r = row["r_multiple"]
        if r is None or row["exit_time"] is None:
            raise DataIntegrityError(f"Закрытая сделка {row.get('id', '?')} без r_multiple/exit_time")
        trades.append({**row, "r_multiple": float(r), "pnl": float(row.get("pnl") or 0.0)})  # pnl нужен профилю CHR (баланс от нуля в USDT); нет значения - 0
    trades.sort(key=lambda t: t["exit_time"])
    all_r = [t["r_multiple"] for t in trades]

    def group(key: str, include: Iterable[str] = ()) -> list[dict[str, Any]]:
        buckets: dict[str, list[float]] = {name: [] for name in include}
        for t in trades:
            buckets.setdefault(t[key], []).append(t["r_multiple"])
        items = [{key if key != "strategy_id" else "strategy": name, **_summary(rs)} for name, rs in buckets.items()]
        items.sort(key=lambda d: (d["trades"] == 0, -d["net_r"], d.get("coin") or d.get("strategy") or d.get("side") or ""))  # без сделок - в конец
        return items

    by_coin = group("coin", coins)
    for rank, item in enumerate(by_coin, start=1):
        item["rank"] = rank if item["trades"] else None
    traded = [c for c in by_coin if c["trades"]]
    reasons = Counter(t["exit_reason"] or "unknown" for t in trades)
    by_reason = [{"reason": k, "trades": v, "net_r": sum(t["r_multiple"] for t in trades if (t["exit_reason"] or "unknown") == k)}
                 for k, v in reasons.most_common()]
    # по каждой монете - по сторонам и стратегиям, чтобы клиент мог показать «где именно работает»
    by_coin_strategy: dict[tuple[str, str], list[float]] = {}
    for t in trades:
        by_coin_strategy.setdefault((t["coin"], t["strategy_id"]), []).append(t["r_multiple"])
    return {
        "overall": {**_summary(all_r), "max_drawdown_r": _drawdown(all_r), "net_pnl": sum(t["pnl"] for t in trades)},
        "by_coin": by_coin,
        "by_strategy": group("strategy_id"),
        "by_side": group("side"),
        "by_coin_strategy": [{"coin": c, "strategy": s, **_summary(rs)} for (c, s), rs in sorted(by_coin_strategy.items())],
        "by_exit_reason": by_reason,
        "equity_curve": _equity_curve([(t["exit_time"], t["r_multiple"], t["pnl"]) for t in trades]),
        "best_coin": traded[0]["coin"] if traded else None,
        "worst_coin": traded[-1]["coin"] if len(traded) > 1 else None,
        "counts": status_counts or {},
        "significance": {
            "min_trades": MIN_TRADES_FOR_CONCLUSIONS, "enough": len(trades) >= MIN_TRADES_FOR_CONCLUSIONS,
            "note": None if len(trades) >= MIN_TRADES_FOR_CONCLUSIONS else
            f"Закрыто {len(trades)} сделок (нужно порядка {MIN_TRADES_FOR_CONCLUSIONS}+): знак Net R пока статистически не отличим от нуля, выводов о прибыльности делать нельзя.",
        },
    }


def counts_from_rows(rows: Iterable[Any]) -> dict[str, int]:
    """Строки (status, gap_affected, n) -> счётчики для клиента. Закрытые с обрывом потока считаются отдельно: в статистику они не входят."""
    out = {"closed": 0, "closed_gap_affected": 0, "open": 0, "pending": 0, "cancelled": 0, "aborted": 0}
    for r in rows:
        status, n = r["status"], int(r["n"])
        if status == "closed":
            out["closed_gap_affected" if r["gap_affected"] else "closed"] += n
        elif status in ("pending_limit", "pending_market"):
            out["pending"] += n
        elif status in out:
            out[status] += n
        else:
            raise DataIntegrityError(f"Неизвестный статус сделки {status!r}")
    return out
