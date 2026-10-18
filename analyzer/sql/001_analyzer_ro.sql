-- Роль analyzer_ro для Strategy Quant Analyzer (CLAUDE.md разд. 3.17): ТОЛЬКО ЧТЕНИЕ четырёх таблиц сделок.
-- Применение (на сервере, из каталога проекта; пароль не хранится в файле):
--   ./deploy/shadow.sh analyzer-init            (создаёт ANALYZER_DB_PASSWORD в .env.shadow и применяет этот скрипт)
-- вручную:
--   docker compose --profile live exec -T postgres psql -U tradeai -d tradeai -v ON_ERROR_STOP=1 -v analyzer_password='<пароль>' -f - < analyzer/sql/001_analyzer_ro.sql
-- Скрипт идемпотентен: повторный запуск обновляет пароль и права, данных не трогает.
--
-- Что роли НЕЛЬЗЯ (и поэтому анализатор физически не может повредить бота): записывать/менять/удалять строки, создавать объекты, читать schema_migrations,
-- быть суперпользователем. Соединение всегда read-only, снимок REPEATABLE READ, запросы ограничены по времени.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyzer_ro') THEN
        CREATE ROLE analyzer_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

ALTER ROLE analyzer_ro WITH LOGIN PASSWORD :'analyzer_password' CONNECTION LIMIT 3;
ALTER ROLE analyzer_ro SET default_transaction_read_only = on;
ALTER ROLE analyzer_ro SET default_transaction_isolation = 'repeatable read';
ALTER ROLE analyzer_ro SET statement_timeout = '120s';
ALTER ROLE analyzer_ro SET lock_timeout = '5s';
ALTER ROLE analyzer_ro SET idle_in_transaction_session_timeout = '120s';

-- чистый лист: снять всё, что могло быть выдано раньше, затем выдать минимум
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM analyzer_ro;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM analyzer_ro;
REVOKE CREATE ON SCHEMA public FROM analyzer_ro;

GRANT CONNECT ON DATABASE :"DBNAME" TO analyzer_ro;
GRANT USAGE ON SCHEMA public TO analyzer_ro;
GRANT SELECT ON trades, trade_closes, trade_snapshots, state_events TO analyzer_ro;
