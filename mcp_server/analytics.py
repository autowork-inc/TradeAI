"""Чистая аналитика над строками БД (без сети и MCP): тестируется отдельно, не зависит от asyncpg.

Метрики определены так же, как в `api/stats.py` (CLAUDE.md, О31): Win Rate - доля сделок с net R > 0; Profit Factor - сумма
прибыльных R / модуль убыточных (нет убыточных - `null` + `profit_factor_infinite`); просадка - падение накопленного R от пика.
"""

import math
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

MIN_TRADES_FOR_CONCLUSIONS = 200  # CLAUDE.md: «сотни сделок»; ниже - предупреждение в ответе, а не вывод
HOURS_MAX = 24 * 365


def jsonable(v: Any) -> Any:
    """Decimal -> float, datetime -> ISO UTC, рекурсивно. NaN/inf -> None (JSON их не знает)."""
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    return v


def summary(rs: list[float]) -> dict[str, Any]:
    n = len(rs)
    wins, losses = [r for r in rs if r > 0], [r for r in rs if r < 0]
    gross_profit, gross_loss = sum(wins), -sum(losses)
    mean = sum(rs) / n if n else None
    stderr = None
    if n >= 2:
        stderr = math.sqrt(sum((r - mean) ** 2 for r in rs) / (n - 1) / n)
    return {
        "trades": n, "wins": len(wins), "losses": len(losses), "breakeven": n - len(wins) - len(losses),
        "win_rate": round(len(wins) / n, 4) if n else None,
        "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else None,
        "profit_factor_infinite": gross_loss == 0 and gross_profit > 0,
        "net_r": round(sum(rs), 3),
        "avg_r": round(mean, 4) if mean is not None else None,
        "avg_r_stderr": round(stderr, 4) if stderr is not None else None,
        "gross_profit_r": round(gross_profit, 3), "gross_loss_r": round(gross_loss, 3),
    }


def sample_note(n: int) -> str | None:
    if n >= MIN_TRADES_FOR_CONCLUSIONS:
        return None
    return (f"Выборка {n} сделок < {MIN_TRADES_FOR_CONCLUSIONS}: статистически этого мало, чтобы судить о прибыльности "
            "(разброс одной сделки ≈ 1.4 R). Это описание того, что произошло, а не вывод о стратегии.")


def max_drawdown(trades_in_time_order: list[tuple[datetime, float]]) -> dict[str, Any]:
    """Наибольшая просадка накопленного R от пика (в R) и момент пика/дна."""
    equity = peak = worst = 0.0
    peak_ts = worst_peak_ts = worst_trough_ts = None
    for ts, r in trades_in_time_order:
        equity += r
        if equity > peak:
            peak, peak_ts = equity, ts
        if peak - equity > worst:
            worst, worst_peak_ts, worst_trough_ts = peak - equity, peak_ts, ts
    return {"max_drawdown_r": round(worst, 3), "peak_time": worst_peak_ts, "trough_time": worst_trough_ts, "final_cum_r": round(equity, 3)}


# ---------------------------------------------------------------- get_strategies_overview

def build_overview(closed: list[dict[str, Any]], status_rows: list[dict[str, Any]], window_hours: int) -> dict[str, Any]:
    by_strategy: dict[str, list[float]] = defaultdict(list)
    by_strategy_coin: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for t in closed:
        r = float(t["r_multiple"])
        by_strategy[t["strategy_id"]].append(r)
        by_strategy_coin[t["strategy_id"]][t["coin"]].append(r)

    other: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))  # сделки окна, не вошедшие в статистику
    for row in status_rows:
        sid, status, gap, n = row["strategy_id"], row["status"], row["gap_affected"], int(row["n"])
        if status == "closed" and gap:
            other[sid]["closed_gap_affected"] += n
        elif status in ("pending_limit", "pending_market"):
            other[sid]["pending"] += n
        elif status != "closed":
            other[sid][status] += n  # open / cancelled / aborted

    items = []
    for sid in sorted(set(by_strategy) | set(other)):
        rs = by_strategy.get(sid, [])
        coins = sorted(((c, summary(v)) for c, v in by_strategy_coin.get(sid, {}).items()), key=lambda cv: -cv[1]["net_r"])
        item = {"strategy_id": sid, **summary(rs), "not_in_stats": dict(other.get(sid, {})),
                "best_coin": coins[0][0] if coins else None, "worst_coin": coins[-1][0] if len(coins) > 1 else None}
        items.append(item)
    items.sort(key=lambda d: (d["trades"] == 0, -d["net_r"]))
    all_rs = [float(t["r_multiple"]) for t in closed]
    return {
        "window_hours": window_hours,
        "definition": ("Сделки: status=closed и не gap_affected (режим shadow), окно по времени закрытия. Net R - сумма r_multiple "
                       "(с комиссиями, со всеми частичными закрытиями). Win Rate = доля Net R > 0. Profit Factor = прибыль R / убыток R."),
        "strategies": items,
        "total": summary(all_rs),
        "sample_warning": sample_note(len(all_rs)),
    }


# ---------------------------------------------------------------- get_trade_details

def build_trade_view(row: dict[str, Any], closes: list[dict[str, Any]], reason: dict[str, Any] | None) -> dict[str, Any]:
    meta = row.get("snapshot_meta") or {}
    trade = {k: v for k, v in row.items() if k not in ("snapshot_meta", "snapshot_geometry")}
    return jsonable({
        **trade,
        "reason": reason,
        "htf_bias": meta.get("htf_label"),
        "signal_meta": meta,  # сырые параметры сигнала из trade_snapshots.meta (то, на чём построен текст причины)
        "closes": [{k: v for k, v in c.items() if k != "trade_id"} for c in closes],
        "has_snapshot": row.get("snapshot_meta") is not None or row.get("snapshot_geometry") is not None,
    })


# ---------------------------------------------------------------- analyze_drawdown_causes

def trend_alignment(side: str, htf_label: str | None) -> str:
    """Сделка по тренду H4 / против / нейтрально / нет данных."""
    if htf_label not in ("Bullish", "Bearish", "Neutral"):
        return "unknown"
    if htf_label == "Neutral":
        return "neutral"
    return "with_trend" if (side == "long") == (htf_label == "Bullish") else "counter_trend"


def _bucket(trades: list[dict[str, Any]], key, total_loss: float) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        groups[str(key(t))].append(t)
    out = []
    for name, ts in groups.items():
        rs = [t["r"] for t in ts]
        loss_r = -sum(r for r in rs if r < 0)
        out.append({
            "group": name, "trades": len(ts), "losses": sum(1 for r in rs if r < 0),
            "loss_rate": round(sum(1 for r in rs if r < 0) / len(ts), 3),
            "win_rate": round(sum(1 for r in rs if r > 0) / len(ts), 3),
            "net_r": round(sum(rs), 3), "loss_r": round(loss_r, 3),
            "share_of_total_loss": round(loss_r / total_loss, 3) if total_loss > 0 else None,
        })
    out.sort(key=lambda d: -d["loss_r"])
    return out


def build_drawdown_report(strategy_id: str, rows: list[dict[str, Any]], worst_limit: int, truncated: bool) -> dict[str, Any]:
    """rows: закрытые не-gap сделки стратегии (CLOSED_FOR_ANALYSIS)."""
    trades = []
    for row in rows:
        meta = row.get("snapshot_meta") or {}
        htf = meta.get("htf_label")
        sig = row["signal_time"]
        trades.append({
            "id": row["id"], "r": float(row["r_multiple"]), "coin": row["coin"], "side": row["side"],
            "exit_reason": row["exit_reason"] or "unknown", "htf": htf or "unknown",
            "alignment": trend_alignment(row["side"], htf),
            "hour_utc": sig.hour if isinstance(sig, datetime) else None, "session": meta.get("session"),
            "mfe": None if row["mfe_r"] is None else float(row["mfe_r"]), "mae": None if row["mae_r"] is None else float(row["mae_r"]),
            "be_armed": bool(row["be_armed"]), "exit_time": row["exit_time"], "row": row, "meta": meta,
        })
    all_rs = [t["r"] for t in trades]
    losers = [t for t in trades if t["r"] < 0]
    total_loss = -sum(t["r"] for t in losers)
    if not trades:
        return jsonable({"strategy_id": strategy_id, "closed_trades": 0, "note": "Закрытых сделок (без gap_affected) у стратегии нет."})

    in_time = sorted(((t["exit_time"], t["r"]) for t in trades), key=lambda x: x[0])
    dims: dict[str, list[dict[str, Any]]] = {
        "by_htf_bias": _bucket(trades, lambda t: t["htf"], total_loss),
        "by_trend_alignment": _bucket(trades, lambda t: t["alignment"], total_loss),
        "by_side": _bucket(trades, lambda t: t["side"], total_loss),
        "by_coin": _bucket(trades, lambda t: t["coin"], total_loss),
        "by_exit_reason": _bucket(trades, lambda t: t["exit_reason"], total_loss),
        "by_hour_utc": sorted(_bucket(trades, lambda t: t["hour_utc"], total_loss), key=lambda d: int(d["group"]) if d["group"].isdigit() else 99),
    }
    if any(t["session"] for t in trades):
        dims["by_session"] = _bucket(trades, lambda t: t["session"] or "unknown", total_loss)
    if any("via_obi" in t["meta"] for t in trades):  # OBI_Breakout: чем подтверждён пробой
        dims["by_confirmation"] = _bucket(
            trades, lambda t: "obi+delta" if t["meta"].get("via_obi") and t["meta"].get("via_delta")
            else "obi" if t["meta"].get("via_obi") else "delta" if t["meta"].get("via_delta") else "unknown", total_loss)

    mfes = [t["mfe"] for t in losers if t["mfe"] is not None]
    maes = [t["mae"] for t in losers if t["mae"] is not None]
    loser_profile = {
        "count": len(losers), "total_loss_r": round(total_loss, 3),
        "avg_loss_r": round(-sum(t["r"] for t in losers) / len(losers), 3) if losers else None,
        "worst_loss_r": round(min((t["r"] for t in losers), default=0.0), 3) if losers else None,
        "never_in_profit": sum(1 for m in mfes if m < 0.25),   # MFE < 0.25R: цена почти сразу пошла против входа
        "reached_1r_then_lost": sum(1 for m in mfes if m >= 1.0),  # дошли до +1R и всё равно ушли в минус (после частичного закрытия/BE это редкость)
        "avg_mfe_r": round(sum(mfes) / len(mfes), 3) if mfes else None,
        "avg_mae_r": round(sum(maes) / len(maes), 3) if maes else None,
        "mfe_known": len(mfes),
    }
    worst = sorted(losers, key=lambda t: t["r"])[:worst_limit]
    worst_view = []
    for t in worst:
        row, meta = t["row"], t["meta"]
        worst_view.append({
            "id": t["id"], "coin": t["coin"], "side": t["side"], "net_r": round(t["r"], 3), "exit_reason": t["exit_reason"],
            "htf_bias": t["htf"], "trend_alignment": t["alignment"], "mfe_r": t["mfe"], "mae_r": t["mae"],
            "signal_time": row["signal_time"], "exit_time": row["exit_time"],
            "entry_price": row["entry_price"], "sl": row["sl"], "tp": row["tp"],
            "reason": reason_for(row, row.get("snapshot_meta"), None), "signal_meta": meta,
        })
    return jsonable({
        "strategy_id": strategy_id,
        "closed_trades": len(trades), "overall": summary(all_rs), "drawdown": max_drawdown(in_time),
        "losing_trades": loser_profile, "breakdowns": dims, "worst_trades": worst_view,
        "truncated_to_latest": truncated,
        "how_to_read": ("Группы отсортированы по вкладу в убыток (loss_r). Сравнивайте loss_rate группы с общей долей убытков: "
                        "группа с заметно большей долей и достаточным числом сделок - кандидат в причину. На малых группах (<20 сделок) "
                        "разница - шум. trend_alignment: with_trend / counter_trend считается по htf_label из снапшота (сделки до "
                        "2026-10-11 без H4 - unknown)."),
        "sample_warning": sample_note(len(trades)),
    })


def ensure_repo_importable() -> None:
    """Корень репозитория в sys.path, чтобы использовать `api.reason` (текст причины входа - единый источник правды с клиентом)."""
    root = Path(__file__).resolve().parent.parent
    if (root / "api" / "reason.py").is_file() and str(root) not in sys.path:
        sys.path.insert(0, str(root))


def reason_for(row: dict[str, Any], meta: dict[str, Any] | None, geometry: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """Текст причины входа из `api.reason.build_reason`. Нет папки api/ рядом (сервер скопирован отдельно) - None, останется signal_meta."""
    ensure_repo_importable()
    try:
        from api.reason import build_reason
    except ImportError:
        return None
    if meta is None:
        return {"title": f"{row['strategy_id']} · {row['side']} · {row['coin']}", "summary": "Снапшота сделки нет - причина входа не записана.", "facts": [], "geometry_labels": []}
    trade = {"strategy_id": row["strategy_id"], "strategy_name": row["strategy_name"], "side": row["side"], "coin": row["coin"],
             "planned_entry": row.get("planned_entry"), "sl": row.get("sl"), "tp": row.get("tp"), "entry_price": row.get("entry_price")}
    return plain_time_tokens(build_reason(jsonable(trade), meta, geometry or []))


_TS_TOKEN = re.compile(r"\{ts:(-?\d+)\}")
_HM_TOKEN = re.compile(r"\{hm:(\d{1,4})\}")


def plain_time_tokens(value: Any) -> Any:
    """Метки времени `{ts:мс}` / `{hm:минуты}` из `api.reason` (сервер пояса не пишет) -> явный UTC для ИИ-аналитика: он не видит часов ПК пользователя."""
    if isinstance(value, str):
        value = _TS_TOKEN.sub(lambda m: datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), value)
        return _HM_TOKEN.sub(lambda m: f"{int(m.group(1)) // 60:02d}:{int(m.group(1)) % 60:02d} UTC", value)
    if isinstance(value, list):
        return [plain_time_tokens(v) for v in value]
    if isinstance(value, dict):
        return {k: plain_time_tokens(v) for k, v in value.items()}
    return value
