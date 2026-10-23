"""Выгрузка сделок из PostgreSQL (сессия только для чтения) и сверка: каждая строка попадает ровно в одну категорию.

Единицы: R = |фактический вход - SL| (как у трекера: mfe_r/mae_r/r_multiple считаются от фактического входа).
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ENV_FILES = (ROOT / "mcp_server" / ".env", ROOT / ".env")
CHR_STRATEGIES = ("Horizontal_Breakout_CHR", "Trendline_Breakout_CHR")
MAKER, TAKER = 0.0002, 0.0005  # ставки VirtualTracker (orchestrator/tracker.py)
DEFAULT_RISK_USD = 5.0  # D.RISK_USDT с 2026-10-19

# Категории сверки: (код, подпись, входит ли в анализ выхода)
CATEGORIES = (
    ("closed_clean", "закрыта, чистая", True),
    ("closed_gap", "закрыта, gap_affected (рестарт / обрыв тиков / неизвестный выход)", True),
    ("closed_no_data", "закрыта, нет входа / R / MFE", False),
    ("active", "ещё активна (open / pending)", False),
    ("cancelled", "отменена, входа не было", False),
    ("aborted_filled", "прервана после входа (исход неизвестен)", False),
    ("aborted_unfilled", "прервана до входа", False),
)
IN_ANALYSIS = {code for code, _, used in CATEGORIES if used}

QUERY = r"""
SELECT t.id, COALESCE(t.strategy_id, t.strategy_name) AS strategy, t.coin, t.side, t.kind, t.mode, t.status,
       t.signal_time, t.entry_time, t.exit_time, t.entry_price, t.exit_price, t.sl, t.risk, t.risk_amount,
       t.r_multiple, t.gross_r, t.mfe_r, t.mae_r, t.exit_reason, t.be_armed, t.gap_affected, t.plan,
       s.candles, s.meta,
       COALESCE((SELECT json_agg(json_build_object('seq', c.seq, 'ts', (extract(epoch FROM c.ts) * 1000)::bigint, 'reason', c.reason,
                                                    'frac', c.frac, 'price', c.price, 'gross_r', c.gross_r, 'net_r', c.net_r) ORDER BY c.seq)
                 FROM trade_closes c WHERE c.trade_id = t.id), '[]'::json) AS closes,
       EXISTS (SELECT 1 FROM state_events e WHERE e.signal_id = regexp_replace(t.id, ':live$', '') AND e.kind = 'trade_restored') AS restored,
       EXISTS (SELECT 1 FROM state_events e WHERE e.signal_id = regexp_replace(t.id, ':live$', '') AND e.kind = 'data_gap') AS tick_gap
FROM trades t
LEFT JOIN trade_snapshots s ON s.trade_id = t.id
WHERE t.signal_time >= $1 AND t.mode = $2 AND NOT (COALESCE(t.strategy_id, t.strategy_name) = ANY($3::text[]))
ORDER BY t.signal_time, t.id
"""


class LabError(RuntimeError):
    """Ошибка аналитики с понятным текстом (без пароля БД)."""


@dataclass
class Trade:
    id: str
    strategy: str
    coin: str
    side: str  # long | short
    kind: str  # limit | market
    mode: str
    status: str
    category: str
    signal_ms: int
    entry_ms: int | None
    exit_ms: int | None
    entry: float | None
    sl: float
    risk: float | None  # цена: |вход - SL|
    risk_usd: float
    actual_r: float | None  # чистый R (r_multiple)
    mfe: float | None
    mae: float | None
    exit_reason: str | None
    be_armed: bool
    gap: bool
    restored: bool
    tick_gap: bool
    fee_in: float
    fee_out: float
    plan_levels: list[tuple[float, float]]
    closes: list[dict[str, Any]]
    candles: list[dict[str, Any]]
    meta: dict[str, Any]
    extra: dict[str, Any] = field(default_factory=dict)  # производные метрики (стоп, свечи) - заполняют другие модули

    @property
    def dir(self) -> int:
        """+1 лонг, -1 шорт: прибыль = dir * (цена - вход)."""
        return 1 if self.side == "long" else -1

    def r_at(self, price: float) -> float:
        assert self.entry is not None and self.risk
        return self.dir * (price - self.entry) / self.risk

    @property
    def final_close(self) -> dict[str, Any] | None:
        return self.closes[-1] if self.closes else None

    @property
    def final_r_gross(self) -> float | None:
        """Цена последнего закрытия в R (а не доля PnL): где была цена, когда сделка закрылась целиком."""
        c = self.final_close
        return None if c is None or self.entry is None or not self.risk else self.r_at(float(c["price"]))

    @property
    def old_plan(self) -> bool:
        """План до 2026-10-19: Scale Out (несколько уровней или доля < 1) / безубыток."""
        return len(self.plan_levels) > 1 or any(f < 0.999 for _, f in self.plan_levels) or self.be_armed


# ------------------------------------------------------------------------------------------- подключение

def read_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    for path in ENV_FILES:
        if url:
            break
        if path.is_file():
            for line in path.read_text(encoding="utf-8-sig").splitlines():
                m = re.match(r"^\s*(?:export\s+)?DATABASE_URL\s*=\s*(.*?)\s*$", line)
                if m and not line.lstrip().startswith("#"):
                    url = m.group(1).strip().strip("\"'")
                    break
    if not url:
        raise LabError("DATABASE_URL не задан: переменная окружения или строка в mcp_server/.env "
                       "(postgresql://tradeai:<пароль>@127.0.0.1:15432/tradeai через SSH-туннель)")
    return url


def redact(text: str) -> str:
    return re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1***@", text)


async def fetch_rows(url: str, since: datetime, mode: str, exclude: list[str]) -> list[dict[str, Any]]:
    import asyncpg  # только здесь: тесты модулей расчёта не требуют драйвера

    try:
        conn = await asyncpg.connect(url, timeout=15, server_settings={
            "default_transaction_read_only": "on", "statement_timeout": "120000", "application_name": "tradeai-trade-lab"})
    except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError, ValueError) as exc:
        raise LabError(f"нет подключения к БД (туннель 127.0.0.1:15432 поднят?): {redact(f'{type(exc).__name__}: {exc}')}") from None
    try:
        for typ in ("jsonb", "json"):
            await conn.set_type_codec(typ, encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
        return [dict(r) for r in await conn.fetch(QUERY, since, mode, exclude)]
    finally:
        await conn.close()


# --------------------------------------------------------------------------------------------- разбор

def _ms(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    return int(v)


def _num(v: Any) -> float | None:
    return None if v is None else float(v)


def classify(r: dict[str, Any]) -> str:
    st = r["status"]
    if st == "closed":
        risk = _num(r["risk"])
        if r["entry_price"] is None or risk is None or not risk > 0 or r["r_multiple"] is None or r["mfe_r"] is None:
            return "closed_no_data"
        return "closed_gap" if r["gap_affected"] else "closed_clean"
    if st in ("open", "pending_limit", "pending_market"):
        return "active"
    if st == "cancelled":
        return "cancelled"
    if st == "aborted":
        return "aborted_filled" if r["entry_price"] is not None else "aborted_unfilled"
    raise LabError(f"незнакомый статус сделки {st!r} у {r['id']}: сверка не может его отнести ни к одной категории")


def fee_rates(kind: str, plan: Any) -> tuple[float, float]:
    rates = plan.get("fee_rates") if isinstance(plan, dict) else None
    if rates:
        return float(rates["entry"]), float(rates["exit"])
    return (MAKER if kind == "limit" else TAKER), TAKER


def plan_levels(plan: Any) -> list[tuple[float, float]]:
    levels = plan.get("levels") if isinstance(plan, dict) else None
    return [(float(r), float(f)) for r, f in levels] if levels else []


def to_trade(r: dict[str, Any]) -> Trade:
    fee_in, fee_out = fee_rates(r["kind"], r["plan"])
    meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
    candles = r.get("candles") if isinstance(r.get("candles"), list) else []
    return Trade(
        id=r["id"], strategy=r["strategy"], coin=r["coin"], side=r["side"], kind=r["kind"], mode=r["mode"], status=r["status"],
        category=classify(r), signal_ms=_ms(r["signal_time"]) or 0, entry_ms=_ms(r["entry_time"]), exit_ms=_ms(r["exit_time"]),
        entry=_num(r["entry_price"]), sl=float(r["sl"]), risk=_num(r["risk"]), risk_usd=_num(r["risk_amount"]) or DEFAULT_RISK_USD,
        actual_r=_num(r["r_multiple"]), mfe=_num(r["mfe_r"]), mae=_num(r["mae_r"]), exit_reason=r["exit_reason"],
        be_armed=bool(r["be_armed"]), gap=bool(r["gap_affected"]), restored=bool(r.get("restored")), tick_gap=bool(r.get("tick_gap")),
        fee_in=fee_in, fee_out=fee_out, plan_levels=plan_levels(r["plan"]), closes=list(r.get("closes") or []), candles=candles, meta=meta)


@dataclass
class Ledger:
    """Сверка: сколько строк выгружено и куда каждая ушла. Сумма категорий обязана равняться числу строк (Fail-Fast)."""

    total: int
    counts: Counter
    excluded_ids: dict[str, list[str]]

    def check(self) -> None:
        if sum(self.counts.values()) != self.total:
            raise LabError(f"сверка не сошлась: строк {self.total}, по категориям {sum(self.counts.values())}")


def ledger_of(trades: list[Trade], total: int | None = None) -> Ledger:
    """total - число выгруженных строк (по умолчанию len(trades)): так сверка ловит строку, потерянную при разборе."""
    excluded: dict[str, list[str]] = {}
    for t in trades:
        if t.category not in IN_ANALYSIS:
            excluded.setdefault(t.category, []).append(t.id)
    ledger = Ledger(len(trades) if total is None else total, Counter(t.category for t in trades), excluded)
    ledger.check()
    return ledger


def build(rows: list[dict[str, Any]]) -> tuple[list[Trade], Ledger]:
    trades = [to_trade(r) for r in rows]
    return trades, ledger_of(trades, total=len(rows))


def analysable(trades: list[Trade], clean_only: bool = False) -> list[Trade]:
    ok = ("closed_clean",) if clean_only else tuple(IN_ANALYSIS)
    return [t for t in trades if t.category in ok]
