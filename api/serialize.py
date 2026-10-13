"""Строки БД -> JSON. NUMERIC (Decimal) -> float, TIMESTAMPTZ -> ISO-8601 UTC с «Z»; свечи и геометрия снапшота отдаются как хранятся (мс)."""

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

TRADE_FIELDS = (
    "id", "strategy_name", "strategy_id", "coin", "side", "kind", "mode", "status", "qty", "remaining_frac", "signal_time", "entry_time", "exit_time",
    "planned_entry", "entry_price", "exit_price", "sl", "tp", "stop_price", "risk", "risk_amount", "pnl", "fees", "r_multiple",
    "gross_r", "mfe_r", "mae_r", "exit_reason", "be_armed", "gap_affected",
)
_TS = {"signal_time", "entry_time", "exit_time", "ts", "created_at", "updated_at"}


class DataIntegrityError(Exception):
    """В БД лежит то, чего по схеме и по коду записи быть не должно (закрытая сделка без R, нечитаемое состояние автомата и т. п.)."""


def num(v: Any) -> float | None:
    if v is None:
        return None
    f = float(v) if isinstance(v, (Decimal, int, float)) and not isinstance(v, bool) else math.nan
    if not math.isfinite(f):
        raise DataIntegrityError(f"Нефинитное число в БД: {v!r}")
    return f


def iso(v: datetime | None) -> str | None:
    if v is None:
        return None
    if v.tzinfo is None:
        raise DataIntegrityError("Время без часового пояса: в схеме все времена TIMESTAMPTZ")
    return v.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def to_json_value(name: str, v: Any) -> Any:
    if name in _TS:
        return iso(v)
    if isinstance(v, Decimal):
        return num(v)
    return v


def trade_out(row: Any, fields: tuple[str, ...] = TRADE_FIELDS) -> dict[str, Any]:
    return {f: to_json_value(f, row[f]) for f in fields}


def close_out(row: Any) -> dict[str, Any]:
    return {k: to_json_value(k, row[k]) for k in ("seq", "ts", "reason", "frac", "qty", "price", "gross_r", "fees_r", "net_r", "pnl", "fees")}


def event_out(row: Any) -> dict[str, Any]:
    return {"ts": iso(row["ts"]), "coin": row["coin"], "strategy": row["strategy"], "strategy_id": row["strategy_id"], "signal_id": row["signal_id"], "kind": row["kind"],
            "from_state": row["from_state"], "to_state": row["to_state"], "reason": row["reason"], "detail": row["detail"] or {}}
