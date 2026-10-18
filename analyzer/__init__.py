"""Strategy Quant Analyzer - отдельный сервис (CLAUDE.md разд. 3.17).

ИЗОЛЯЦИЯ: пакет НЕ импортирует код бота (config, orchestrator, strategies, exchange, persistence) и не импортируется им. Единственная связь с ботом -
чтение PostgreSQL ролью analyzer_ro (только SELECT, REPEATABLE READ, read-only) и HTTP через основной API (прокси `/api/analysis/*`).
Анализатор ничего не пишет в БД бота и в его конфигурацию: рекомендации показываются как diff, применяет их человек (правка кода и push в main).
"""

__version__ = "1.0"
