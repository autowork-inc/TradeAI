"""Чтение сделок из PostgreSQL бота. ТОЛЬКО SELECT, роль analyzer_ro, одна транзакция REPEATABLE READ + READ ONLY (согласованный снимок), синхронно
(воркер - отдельный процесс). Тяжёлые свечи снапшота читаются серверным курсором порциями и сразу сворачиваются в признаки (VSA): в память
попадают числа, а не сотни свечей на сделку."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from analyzer.config import Settings
from analyzer.errors import DataError
from analyzer.frame import build_frame, candle_features

TRADE_COLUMNS = ("id", "strategy_id", "coin", "side", "kind", "mode", "signal_time", "entry_time", "exit_time", "planned_entry", "entry_price", "exit_price",
                 "sl", "qty", "risk_amount", "pnl", "fees", "r_multiple", "gross_r", "mfe_r", "mae_r", "exit_reason", "be_armed", "plan")
LOAD_SQL = f"""
SELECT {', '.join('t.' + c for c in TRADE_COLUMNS)}, s.meta AS meta, s.candles AS candles
  FROM trades t LEFT JOIN trade_snapshots s ON s.trade_id = t.id
 WHERE t.status = 'closed' AND NOT t.gap_affected
   AND (t.mode = %(mode)s OR %(mode)s = 'all')
   AND (%(strategies)s::text[] IS NULL OR t.strategy_id = ANY(%(strategies)s::text[]))
   AND (%(coins)s::text[] IS NULL OR t.coin = ANY(%(coins)s::text[]))
   AND (%(since)s::timestamptz IS NULL OR t.exit_time >= %(since)s::timestamptz)
   AND (%(until)s::timestamptz IS NULL OR t.exit_time < %(until)s::timestamptz)
 ORDER BY t.exit_time, t.id
 LIMIT %(limit)s
"""
FETCH_SIZE = 200


@dataclass(frozen=True)
class LoadQuery:
    mode: str = "shadow"  # shadow | live | all
    strategies: tuple[str, ...] | None = None
    coins: tuple[str, ...] | None = None
    since: datetime | None = None
    until: datetime | None = None


Loader = Callable[[Settings, LoadQuery, Callable[[float], None] | None], pd.DataFrame]


def row_to_record(row: dict[str, Any]) -> dict[str, Any]:
    """Строка БД -> запись для build_frame: свечи сворачиваются в (vol_ratio, range_atr) и выбрасываются."""
    rec = {k: v for k, v in row.items() if k != "candles"}
    rec["vol_ratio"], rec["range_atr"] = candle_features(row.get("candles"))
    return rec


def load_trades(settings: Settings, query: LoadQuery, progress: Callable[[float], None] | None = None) -> pd.DataFrame:
    """Закрытые не-gap сделки по фильтру. Больше `max_trades` - DataError (не усечение молча): сузьте окно/фильтр."""
    params = {"mode": query.mode, "strategies": list(query.strategies) if query.strategies else None, "coins": list(query.coins) if query.coins else None,
              "since": query.since, "until": query.until, "limit": settings.max_trades + 1}
    records: list[dict[str, Any]] = []
    with psycopg.connect(settings.database_url, connect_timeout=10, row_factory=dict_row, autocommit=False,
                         options=f"-c default_transaction_read_only=on -c statement_timeout={settings.statement_timeout_s * 1000} -c application_name=tradeai-analyzer") as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        total = conn.execute("SELECT count(*) AS n FROM trades t WHERE t.status = 'closed' AND NOT t.gap_affected AND (t.mode = %(mode)s OR %(mode)s = 'all')",
                             {"mode": query.mode}).fetchone()["n"]
        with conn.cursor(name="analyzer_trades") as cur:
            cur.itersize = FETCH_SIZE
            cur.execute(LOAD_SQL, params)
            for row in cur:
                records.append(row_to_record(row))
                if len(records) > settings.max_trades:
                    raise DataError(f"Сделок больше предела {settings.max_trades}: сузьте период (since/until), стратегии или монеты")
                if progress and total and len(records) % FETCH_SIZE == 0:
                    progress(min(1.0, len(records) / total))
    return build_frame(records)
