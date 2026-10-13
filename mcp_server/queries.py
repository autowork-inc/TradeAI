"""SQL-запросы аналитического контура. Только SELECT, только параметры `$n` (никакой подстановки строк от LLM в текст запроса).

Что считается сделкой для статистики - так же, как в `api/stats.py` и `deploy/shadow.sh stats`: status = 'closed' и НЕ gap_affected
(обрыв потока тиков во время сделки искажает исход). Режим - shadow (боевое исполнение заморожено, CLAUDE.md разд. 1).
Net R = trades.r_multiple (сумма net R всех частичных закрытий с комиссиями).
"""

MODE = "shadow"

# $1 mode, $2 окно в часах (по времени закрытия)
CLOSED_IN_WINDOW = """
SELECT strategy_id, coin, side, r_multiple, exit_time
FROM trades
WHERE mode = $1 AND status = 'closed' AND NOT gap_affected AND r_multiple IS NOT NULL
  AND exit_time >= now() - make_interval(hours => $2::int)
ORDER BY exit_time, id
"""

# $1 mode, $2 окно (по времени сигнала): сколько сделок вне статистики и почему
STATUS_COUNTS_IN_WINDOW = """
SELECT strategy_id, status, gap_affected, count(*)::int AS n
FROM trades
WHERE mode = $1 AND signal_time >= now() - make_interval(hours => $2::int)
GROUP BY strategy_id, status, gap_affected
"""

KNOWN_STRATEGIES = "SELECT strategy_id, count(*)::int AS n FROM trades WHERE mode = $1 GROUP BY strategy_id ORDER BY strategy_id"

# $1 strategy_id, $2 limit. LEFT JOIN: у сделки без снапшота (не должно быть, но не падаем) поля meta/geometry = NULL.
# Свечи снапшота (candles) НЕ выбираются: ~100 свечей на сделку - десятки КБ, LLM они не нужны.
RECENT_TRADES = """
SELECT t.id, t.strategy_id, t.strategy_name, t.coin, t.side, t.kind, t.mode, t.status,
       t.signal_time, t.entry_time, t.exit_time,
       t.planned_entry, t.entry_price, t.exit_price, t.sl, t.tp, t.stop_price,
       t.qty, t.risk_amount, t.pnl, t.fees, t.r_multiple, t.gross_r, t.mfe_r, t.mae_r,
       t.exit_reason, t.be_armed, t.gap_affected, t.plan,
       s.meta AS snapshot_meta, s.geometry AS snapshot_geometry
FROM trades t
LEFT JOIN trade_snapshots s ON s.trade_id = t.id
WHERE t.strategy_id = $1
ORDER BY t.signal_time DESC, t.id DESC
LIMIT $2
"""

# $1 список id сделок
CLOSES_FOR_TRADES = """
SELECT trade_id, seq, ts, reason, frac, price, gross_r, fees_r, net_r, pnl
FROM trade_closes
WHERE trade_id = ANY($1::text[])
ORDER BY trade_id, seq
"""

# $1 mode, $2 strategy_id, $3 потолок строк. Все закрытые сделки стратегии (и прибыльные: доля убытков считается внутри группы).
CLOSED_FOR_ANALYSIS = """
SELECT t.id, t.strategy_id, t.strategy_name, t.coin, t.side, t.exit_reason, t.r_multiple, t.mfe_r, t.mae_r, t.be_armed,
       t.signal_time, t.entry_time, t.exit_time, t.planned_entry, t.entry_price, t.exit_price, t.sl, t.tp,
       s.meta AS snapshot_meta
FROM trades t
LEFT JOIN trade_snapshots s ON s.trade_id = t.id
WHERE t.mode = $1 AND t.strategy_id = $2 AND t.status = 'closed' AND NOT t.gap_affected AND t.r_multiple IS NOT NULL
ORDER BY t.exit_time DESC, t.id DESC
LIMIT $3
"""

# $1 trade ids: закрытия убыточных сделок (в каком порядке и по какой причине уходила позиция)
LOSER_CLOSES = CLOSES_FOR_TRADES
