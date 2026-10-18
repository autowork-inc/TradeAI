"""Схема PostgreSQL (Этап 5, CLAUDE.md разд. 3.3). Версионируется миграциями: каждая применяется один раз, в транзакции, под
advisory-блокировкой (два процесса не мигрируют одновременно). Меняя схему, ДОБАВЛЯЙТЕ новую миграцию, старые не правьте.

Таблицы:
  trades          - одна строка на сделку (обновляется по ходу жизни: заявка -> позиция -> частичные закрытия -> закрыта);
  trade_closes    - КАЖДОЕ закрытие (частичное или итоговое): +1R 50%, +3R 40%, стоп, безубыток, трейлинг, таймаут;
  trade_snapshots - снапшот входа: свечи (JSONB, последние ~100) и геометрия сетапа в едином формате {type,t1,t2,p1,p2,label};
  state_events    - журнал переходов состояний: стейт-машина сетапа SMC (S0..S4 по шорту/лонгу) и жизненный цикл сделок.
Цены и объёмы - NUMERIC, время - TIMESTAMPTZ (UTC). Первичный ключ сделки - signal_id (id = trade_id Оркестратора), поэтому
повторная запись той же сделки - upsert, а не дубль. `mode` отделяет shadow от live в одной БД.
Боевая сделка пишется под id `<signal_id>:live` (иначе upsert затёр бы теневую строку с тем же signal_id); её снапшот - копия теневого (COPY_SNAPSHOT).
"""

from typing import Literal

Kind = Literal["ts", "num", "text", "bool", "json"]

# колонки trades в порядке параметров INSERT; значение - тип для конвертации Python -> asyncpg
TRADE_COLUMNS: dict[str, Kind] = {
    "id": "text", "strategy_name": "text", "strategy_id": "text", "coin": "text", "side": "text", "kind": "text", "mode": "text", "status": "text",
    "qty": "num", "remaining_frac": "num", "signal_time": "ts", "entry_time": "ts", "exit_time": "ts",
    "planned_entry": "num", "entry_price": "num", "exit_price": "num", "sl": "num", "tp": "num", "stop_price": "num",
    "risk": "num", "risk_amount": "num", "pnl": "num", "fees": "num", "r_multiple": "num", "gross_r": "num",
    "mfe_r": "num", "mae_r": "num", "exit_reason": "text", "be_armed": "bool", "gap_affected": "bool",
    "client_order_id": "text", "plan": "json",
}
CLOSE_COLUMNS: dict[str, Kind] = {
    "trade_id": "text", "seq": "num", "ts": "ts", "reason": "text", "frac": "num", "qty": "num", "price": "num",
    "gross_r": "num", "fees_r": "num", "net_r": "num", "pnl": "num", "fees": "num",
}
SNAPSHOT_COLUMNS: dict[str, Kind] = {"trade_id": "text", "strategy_id": "text", "created_at": "ts", "candles": "json", "geometry": "json", "meta": "json"}
STATE_EVENT_COLUMNS: dict[str, Kind] = {
    "ts": "ts", "coin": "text", "strategy": "text", "strategy_id": "text", "signal_id": "text", "from_state": "text", "to_state": "text",
    "kind": "text", "reason": "text", "detail": "json",
}

_V1 = [
    """
    CREATE TABLE trades (
        id              TEXT PRIMARY KEY,
        strategy_name   TEXT NOT NULL,
        coin            TEXT NOT NULL,
        side            TEXT NOT NULL CHECK (side IN ('long', 'short')),
        kind            TEXT NOT NULL CHECK (kind IN ('limit', 'market')),
        mode            TEXT NOT NULL CHECK (mode IN ('live', 'shadow')),
        status          TEXT NOT NULL CHECK (status IN ('pending_limit', 'pending_market', 'open', 'closed', 'cancelled', 'aborted')),
        qty             NUMERIC,
        remaining_frac  NUMERIC,
        signal_time     TIMESTAMPTZ NOT NULL,
        entry_time      TIMESTAMPTZ,
        exit_time       TIMESTAMPTZ,
        planned_entry   NUMERIC,
        entry_price     NUMERIC,
        exit_price      NUMERIC,
        sl              NUMERIC NOT NULL,
        tp              NUMERIC,
        stop_price      NUMERIC,
        risk            NUMERIC,
        risk_amount     NUMERIC,
        pnl             NUMERIC,
        fees            NUMERIC,
        r_multiple      NUMERIC,
        gross_r         NUMERIC,
        mfe_r           NUMERIC,
        mae_r           NUMERIC,
        exit_reason     TEXT,
        be_armed        BOOLEAN NOT NULL DEFAULT false,
        gap_affected    BOOLEAN NOT NULL DEFAULT false,
        client_order_id TEXT,
        plan            JSONB,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX trades_coin_signal_time_idx ON trades (coin, signal_time DESC)",
    "CREATE INDEX trades_strategy_status_idx ON trades (strategy_name, status)",
    """
    CREATE TABLE trade_closes (
        id        BIGSERIAL PRIMARY KEY,
        trade_id  TEXT NOT NULL REFERENCES trades (id) ON DELETE CASCADE,
        seq       INTEGER NOT NULL,
        ts        TIMESTAMPTZ NOT NULL,
        reason    TEXT NOT NULL,
        frac      NUMERIC NOT NULL CHECK (frac > 0),
        qty       NUMERIC,
        price     NUMERIC NOT NULL,
        gross_r   NUMERIC NOT NULL,
        fees_r    NUMERIC NOT NULL,
        net_r     NUMERIC NOT NULL,
        pnl       NUMERIC,
        fees      NUMERIC,
        UNIQUE (trade_id, seq)
    )
    """,
    """
    CREATE TABLE trade_snapshots (
        trade_id   TEXT PRIMARY KEY REFERENCES trades (id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL,
        candles    JSONB NOT NULL,
        geometry   JSONB NOT NULL,
        meta       JSONB
    )
    """,
    """
    CREATE TABLE state_events (
        id         BIGSERIAL PRIMARY KEY,
        ts         TIMESTAMPTZ NOT NULL,
        coin       TEXT NOT NULL,
        strategy   TEXT NOT NULL,
        signal_id  TEXT,
        from_state TEXT,
        to_state   TEXT,
        kind       TEXT NOT NULL,
        reason     TEXT,
        detail     JSONB
    )
    """,
    "CREATE INDEX state_events_coin_ts_idx ON state_events (coin, ts DESC)",
    "CREATE INDEX state_events_signal_idx ON state_events (signal_id)",
]

# Версия 2 (2026-10-11, мульти-стратегийный Shadow): колонка strategy_id в trades, trade_snapshots, state_events.
# Старые данные сохраняются: у сделок и снапшотов strategy_id = прежнее имя стратегии ('smc', 'breakout' - как были записаны,
# без переименования), у событий - прежнее `strategy`; системные события ('htf', не стратегия) остаются с NULL.
# Висящие сделки (заявка/позиция) перед созданием уникального индекса закрываются как aborted/process_restart: миграция идёт
# при старте shadow-run, а состояние Shadow в памяти при перезапуске не восстанавливается (О17) - они всё равно сирота.
# Уникальный индекс - защита на уровне БД: по ключу (монета, strategy_id, режим) не может быть двух активных сделок.
_V2 = [
    "UPDATE trades SET status = 'aborted', exit_reason = 'process_restart', updated_at = now() "
    "WHERE status IN ('pending_limit', 'pending_market', 'open')",
    "ALTER TABLE trades ADD COLUMN strategy_id TEXT",
    "UPDATE trades SET strategy_id = strategy_name",
    "ALTER TABLE trades ALTER COLUMN strategy_id SET NOT NULL",
    "ALTER TABLE trade_snapshots ADD COLUMN strategy_id TEXT",
    "UPDATE trade_snapshots s SET strategy_id = t.strategy_id FROM trades t WHERE t.id = s.trade_id",
    "ALTER TABLE trade_snapshots ALTER COLUMN strategy_id SET NOT NULL",
    "ALTER TABLE state_events ADD COLUMN strategy_id TEXT",
    "UPDATE state_events SET strategy_id = strategy WHERE strategy <> 'htf'",
    "CREATE INDEX trades_coin_strategy_status_idx ON trades (coin, strategy_id, status)",
    "CREATE INDEX trades_strategy_id_signal_time_idx ON trades (strategy_id, signal_time DESC)",
    "CREATE INDEX state_events_coin_strategy_ts_idx ON state_events (coin, strategy_id, ts DESC)",
    "CREATE UNIQUE INDEX trades_one_active_per_key ON trades (coin, strategy_id, mode) "
    "WHERE status IN ('pending_limit', 'pending_market', 'open')",
]

# (версия, [DDL...]); версии идут подряд с 1
MIGRATIONS: list[tuple[int, list[str]]] = [(1, _V1), (2, _V2)]
SCHEMA_VERSION = MIGRATIONS[-1][0]
MIGRATION_LOCK_KEY = 7_340_211  # произвольная константа: ключ pg_advisory_xact_lock


def upsert_sql(table: str, columns: dict[str, Kind], key: str) -> str:
    """INSERT ... ON CONFLICT (key) DO UPDATE для всех колонок, кроме ключа; updated_at обновляется."""
    names = list(columns)
    placeholders = ", ".join(f"${i + 1}{'::jsonb' if kind == 'json' else ''}" for i, kind in enumerate(columns.values()))
    updates = ", ".join(f"{n} = EXCLUDED.{n}" for n in names if n != key)
    return f"INSERT INTO {table} ({', '.join(names)}) VALUES ({placeholders}) ON CONFLICT ({key}) DO UPDATE SET {updates}, updated_at = now()"


def insert_sql(table: str, columns: dict[str, Kind], conflict: str = "") -> str:
    placeholders = ", ".join(f"${i + 1}{'::jsonb' if kind == 'json' else ''}" for i, kind in enumerate(columns.values()))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) {conflict}".strip()


UPSERT_TRADE = upsert_sql("trades", TRADE_COLUMNS, "id")
INSERT_CLOSE = insert_sql("trade_closes", CLOSE_COLUMNS, "ON CONFLICT (trade_id, seq) DO NOTHING")
INSERT_SNAPSHOT = insert_sql("trade_snapshots", SNAPSHOT_COLUMNS, "ON CONFLICT (trade_id) DO NOTHING")
INSERT_STATE_EVENT = insert_sql("state_events", STATE_EVENT_COLUMNS)
# Боевая сделка (id = `<signal_id>:live`, mode = 'live') получает СВОЙ снапшот: копию снапшота теневой сделки ($2) с пометкой в meta.
# Делает это сама БД одним запросом (свечи не гуляют через память процесса); повторный вызов безопасен (ON CONFLICT DO NOTHING).
COPY_SNAPSHOT = (
    "INSERT INTO trade_snapshots (trade_id, strategy_id, created_at, candles, geometry, meta) "
    "SELECT $1::text, strategy_id, created_at, candles, geometry, "
    "COALESCE(meta, '{}'::jsonb) || jsonb_build_object('live', true, 'source_trade_id', $2::text) "
    "FROM trade_snapshots WHERE trade_id = $2::text ON CONFLICT (trade_id) DO NOTHING"
)
SNAPSHOT_EXISTS = "SELECT 1 FROM trade_snapshots WHERE trade_id = $1"
# Сверка при старте боевого контура (Блок 1, 2026-10-18): открытые боевые сделки и их уже записанные закрытия
SELECT_OPEN_LIVE = f"SELECT {', '.join(TRADE_COLUMNS)} FROM trades WHERE mode = 'live' AND status = 'open' ORDER BY entry_time"
# Ретро-сверка закрытых боевых сделок с Binance (live_resync): неизвестный выход или флаг gap_affected, попытка ещё не делалась (plan.resync пуст)
SELECT_RESYNC = (f"SELECT {', '.join(TRADE_COLUMNS)} FROM trades WHERE mode = 'live' AND status = 'closed' "
                 "AND (exit_reason = 'unknown' OR gap_affected) AND (plan IS NULL OR plan->'resync' IS NULL) ORDER BY entry_time")
DELETE_CLOSES = "DELETE FROM trade_closes WHERE trade_id = $1 AND seq = ANY($2::int[])"
SELECT_CLOSES_OF = ("SELECT trade_id, seq, ts, reason, qty, price, pnl, fees, gross_r FROM trade_closes "
                    "WHERE trade_id = ANY($1::text[]) ORDER BY trade_id, seq")
ABORT_ORPHANS = (
    "UPDATE trades SET status = 'aborted', exit_reason = 'process_restart', updated_at = now() "
    "WHERE mode = $1 AND status IN ('pending_limit', 'pending_market', 'open') RETURNING id, coin, strategy_id"
)
