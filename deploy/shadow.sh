#!/bin/sh
# Управление Shadow Mode на сервере (PostgreSQL + shadow-run). Запускать из каталога проекта (где docker-compose.yml):
#
#   ./deploy/shadow.sh up        # проверки, пароль БД (один раз), сборка образа, старт postgres + shadow-run
#   ./deploy/shadow.sh status    # контейнеры, свежесть «пульса», число сделок в БД, последние строки лога
#   ./deploy/shadow.sh logs      # лог shadow-run в реальном времени (Ctrl+C - выйти, контейнер не остановится)
#   ./deploy/shadow.sh logs api  # то же для API мониторинга (или postgres)
#   ./deploy/shadow.sh apikey    # показать ключ API для клиента (X-API-Key) и команду SSH-туннеля
#   ./deploy/shadow.sh stats     # сводка из БД: сделки по стратегии/монете/причине выхода, сумма R
#   ./deploy/shadow.sh backup    # pg_dump в ./backups/tradeai-<время>.sql.gz
#   ./deploy/shadow.sh psql      # консоль SQL
#   ./deploy/shadow.sh up-live   # БОЕВОЙ режим (shadow-live вместо shadow-run): реальные ордера на Binance, см. docker-compose.yml и .env.shadow.example
#   ./deploy/shadow.sh api       # поднять API мониторинга, если он не работает (то же делают up, up-live и status)
#   ./deploy/shadow.sh analyzer-init  # Strategy Quant Analyzer: пароль роли analyzer_ro (.env.shadow) + SQL-роль в PostgreSQL (идемпотентно)
#   ./deploy/shadow.sh analyzer-up    # собрать и запустить анализатор (профиль analytics; бот не затрагивается); analyzer-down - остановить
#   ./deploy/shadow.sh down      # остановить (данные БД и журнал СОХРАНЯЮТСЯ)
#
# Что скрипт НЕ делает: не трогает live-run и ключи биржи (в архиве Shadow их нет и быть не должно), не публикует порт БД наружу,
# не удаляет том с данными. Пароль БД и ключ API мониторинга создаются один раз в .env.shadow (chmod 600) - сохраните этот файл
# вместе с бэкапами. Порт API по умолчанию опубликован на 0.0.0.0:8001 (порт 8000 на сервере занят): клиент подключается напрямую по IP (SSH-туннель не нужен).
set -eu

cd "$(dirname "$0")/.."
ENV_FILE=".env.shadow"
SERVICES="postgres shadow-run api"
DC="docker compose --env-file $ENV_FILE"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ОШИБКА: не найден '$1'. $2" >&2; exit 1; }; }

check_docker() {
    need docker "Установите Docker: https://docs.docker.com/engine/install/ (на Ubuntu: apt install docker.io docker-compose-v2)"
    docker compose version >/dev/null 2>&1 || { echo "ОШИБКА: нет плагина 'docker compose' (v2). На Ubuntu: apt install docker-compose-v2" >&2; exit 1; }
    docker info >/dev/null 2>&1 || { echo "ОШИБКА: демон Docker недоступен (запущен ли он? есть ли права: sudo или группа docker)." >&2; exit 1; }
}

# Самолечение склейки 2026-10-18: `...VWAP_ReversionANALYZER_DB_PASSWORD=...` -> две строки. Только для уникального имени ANALYZER_DB_PASSWORD
# (API_KEY нельзя: он суффикс BINANCE_API_KEY).
repair_env() {
    [ -f "$ENV_FILE" ] || return 0
    if grep -q '[A-Za-z0-9_,.]ANALYZER_DB_PASSWORD=' "$ENV_FILE"; then
        sed -i 's/\([A-Za-z0-9_,.]\)\(ANALYZER_DB_PASSWORD=\)/\1\n\2/' "$ENV_FILE"
        echo "В $ENV_FILE исправлена склеенная строка ANALYZER_DB_PASSWORD."
    fi
    # Несколько строк ANALYZER_DB_PASSWORD (по одной на каждый запуск analyzer-up при склеенной строке): роль получала склейку всех значений, а compose - последнее.
    # Оставляем ОДНУ - последнюю (её читает compose).
    if [ "$(grep -c '^ANALYZER_DB_PASSWORD=' "$ENV_FILE")" -gt 1 ]; then
        last="$(grep '^ANALYZER_DB_PASSWORD=' "$ENV_FILE" | tail -n1)"
        sed -i '/^ANALYZER_DB_PASSWORD=/d' "$ENV_FILE"
        env_append "$last"
        echo "В $ENV_FILE оставлена одна строка ANALYZER_DB_PASSWORD (последняя)."
    fi
}

ensure_env() {
    repair_env
    ensure_password
    ensure_api_key
}

# Автозапуск API мониторинга (2026-10-18): в docker-compose.yml у api restart: unless-stopped - Docker поднимает его после падения и после
# перезагрузки сервера (если служба docker включена в автозагрузку). Здесь - страховка: api не работает (остановлен/упал/не создан) -> запустить.
ensure_api() {
    if ! $DC --profile live ps --status running --services 2>/dev/null | grep -qx api; then
        echo "== API мониторинга не запущен - запускаю (restart: unless-stopped)"
        $DC --profile live up -d api
    fi
    if command -v systemctl >/dev/null 2>&1 && [ "$(systemctl is-enabled docker 2>/dev/null || true)" = "disabled" ]; then
        echo "ВНИМАНИЕ: служба docker не в автозагрузке - после перезагрузки сервера API не поднимется. Включить: systemctl enable docker" >&2
    fi
}

rand_hex() {
    if command -v openssl >/dev/null 2>&1; then
        out="$(openssl rand -hex "$1")"
    else
        out="$(head -c 256 /dev/urandom | od -An -tx1 | tr -d ' \n' | cut -c1-$(($1 * 2)))"
    fi
    [ "${#out}" -ge $(($1 * 2)) ] || { echo "ОШИБКА: не удалось сгенерировать случайное значение" >&2; exit 1; }
    printf '%s' "$out"
}

# Дописать строку в .env.shadow: если файл не кончается переводом строки, сначала дописывается он - иначе новая переменная склеивается с последней
# (2026-10-18: `APPROVED_STRATEGIES=...VWAP_ReversionANALYZER_DB_PASSWORD=...` уронил shadow-live).
env_append() {
    [ ! -s "$ENV_FILE" ] || [ -z "$(tail -c1 "$ENV_FILE")" ] || echo >> "$ENV_FILE"
    printf '%s\n' "$1" >> "$ENV_FILE"
}

ensure_password() {
    if [ -s "$ENV_FILE" ] && grep -q '^POSTGRES_PASSWORD=.\{16,\}' "$ENV_FILE"; then
        return
    fi
    if [ -e "$ENV_FILE" ] && [ -s "$ENV_FILE" ] && grep -q '^POSTGRES_PASSWORD=' "$ENV_FILE" && ! grep -q '^POSTGRES_PASSWORD=$' "$ENV_FILE"; then
        echo "ОШИБКА: $ENV_FILE есть, но POSTGRES_PASSWORD в нём короче 16 символов. Исправьте вручную (только латиница и цифры)." >&2
        exit 1
    fi
    [ -f "$ENV_FILE" ] && sed -i '/^POSTGRES_PASSWORD=$/d' "$ENV_FILE"  # пустая заготовка из примера
    pw="$(rand_hex 24)"
    umask 077
    env_append "POSTGRES_PASSWORD=$pw"
    chmod 600 "$ENV_FILE"
    echo "В $ENV_FILE создан пароль БД (chmod 600). Сохраните файл: без пароля данные тома pgdata не открыть."
}

ensure_api_key() {
    if grep -q '^API_KEY=.\{32,\}' "$ENV_FILE"; then
        return
    fi
    if grep -q '^API_KEY=.' "$ENV_FILE"; then
        echo "ОШИБКА: API_KEY в $ENV_FILE короче 32 символов. Удалите строку (создам новый) или замените вручную." >&2
        exit 1
    fi
    sed -i '/^API_KEY=$/d' "$ENV_FILE"
    key="$(rand_hex 32)"
    umask 077
    env_append "API_KEY=$key"
    chmod 600 "$ENV_FILE"
    echo "В $ENV_FILE создан ключ API мониторинга (показать: ./deploy/shadow.sh apikey)."
}

# Strategy Quant Analyzer (analyzer/, профиль analytics): пароль роли только на чтение и сама роль. Бот и его контейнеры не затрагиваются.
ensure_analyzer_password() {
    if grep -q '^ANALYZER_DB_PASSWORD=.\{16,\}' "$ENV_FILE"; then
        return
    fi
    sed -i '/^ANALYZER_DB_PASSWORD=$/d' "$ENV_FILE"
    pw="$(rand_hex 24)"
    umask 077
    env_append "ANALYZER_DB_PASSWORD=$pw"
    chmod 600 "$ENV_FILE"
    echo "В $ENV_FILE создан пароль роли analyzer_ro (только чтение БД)."
}

apply_analyzer_role() {
    [ -f analyzer/sql/001_analyzer_ro.sql ] || { echo "ОШИБКА: нет analyzer/sql/001_analyzer_ro.sql (архив собран без анализатора?)" >&2; exit 1; }
    $DC --profile live ps --status running --services 2>/dev/null | grep -qx postgres || { echo "ОШИБКА: postgres не запущен (./deploy/shadow.sh up)." >&2; exit 1; }
    pw="$(grep '^ANALYZER_DB_PASSWORD=' "$ENV_FILE" | tail -n1 | cut -d= -f2-)"
    $DC --profile live exec -T postgres psql -U tradeai -d tradeai -v ON_ERROR_STOP=1 -v analyzer_password="$pw" -f - < analyzer/sql/001_analyzer_ro.sql >/dev/null
    # проверка: под этим паролем роль входит по сети (-h postgres: у 127.0.0.1 в образе postgres метод trust, он пароль не проверяет), иначе контейнер анализатора не подключится
    $DC --profile live exec -T -e PGPASSWORD="$pw" postgres psql -h postgres -U analyzer_ro -d tradeai -tAc 'SELECT 1' >/dev/null \
        || { echo "ОШИБКА: роль analyzer_ro не входит с паролем из $ENV_FILE (проверьте, что строка ANALYZER_DB_PASSWORD в файле одна)." >&2; exit 1; }
    echo "Роль analyzer_ro применена и проверена входом (SELECT на trades, trade_closes, trade_snapshots, state_events; read-only, REPEATABLE READ)."
}

cmd="${1:-up}"
case "$cmd" in
    up)
        check_docker
        [ -f docker-compose.yml ] && [ -f Dockerfile ] || { echo "ОШИБКА: запускайте из каталога проекта (нет docker-compose.yml / Dockerfile)." >&2; exit 1; }
        ensure_env
        mkdir -p data/shadow logs/shadow data/live logs/live data/updates data/golden_set data/playground data/lab backups; chmod 777 data/golden_set data/playground data/lab  # golden_set, playground, lab: пишет API (uid 10001)
        # shadow-run и shadow-live не должны работать вместе (общая БД): режим Shadow сначала гасит боевой контур
        # (открытые позиции остаются на бирже под защитой стоп-ордеров, но без сопровождения - безубыток/трейлинг не двигаются)
        if $DC --profile live ps --status running --services 2>/dev/null | grep -qx shadow-live; then
            echo "ВНИМАНИЕ: останавливаю боевой shadow-live (открытые позиции останутся на бирже под защитой стопов, без сопровождения)." >&2
            $DC --profile live stop shadow-live
        fi
        echo "== Сборка и запуск: $SERVICES"
        $DC up -d --build $SERVICES
        echo "== Жду готовности (до 3 минут)"
        i=0
        while [ "$i" -lt 36 ]; do
            state="$($DC ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null || true)"
            echo "$state" | grep -q '^postgres running healthy' && echo "$state" | grep -q '^shadow-run running' && echo "$state" | grep -q '^api running' && break
            echo "$state" | grep -q '^shadow-run exited' && { echo "ОШИБКА: shadow-run остановился:" >&2; $DC logs --tail 40 shadow-run >&2; exit 1; }
            echo "$state" | grep -q '^api exited' && { echo "ОШИБКА: api остановился:" >&2; $DC logs --tail 40 api >&2; exit 1; }
            i=$((i + 1)); sleep 5
        done
        ensure_api
        $DC ps
        echo "== Последние строки лога shadow-run"
        $DC logs --tail 15 shadow-run
        echo
        echo "Готово. Дальше: ./deploy/shadow.sh status | logs | stats | backup | apikey"
        ;;
    up-live)
        check_docker
        [ -f docker-compose.yml ] && [ -f Dockerfile ] || { echo "ОШИБКА: запускайте из каталога проекта (нет docker-compose.yml / Dockerfile)." >&2; exit 1; }
        ensure_env
        grep -q '^BINANCE_API_KEY=.\{10,\}' "$ENV_FILE" && grep -q '^BINANCE_API_SECRET=.\{10,\}' "$ENV_FILE"             || { echo "ОШИБКА: в $ENV_FILE нет BINANCE_API_KEY / BINANCE_API_SECRET." >&2; exit 1; }
        if grep -q '^USE_TESTNET=true' "$ENV_FILE"; then
            echo "Режим: TESTNET (USE_TESTNET=true): ордера на testnet.binancefuture.com."
        else
            grep -q '^LIVE_ALLOW_MAINNET=true' "$ENV_FILE"                 || { echo "ОШИБКА: боевой режим на mainnet = реальные деньги. Добавьте в $ENV_FILE строку LIVE_ALLOW_MAINNET=true (или USE_TESTNET=true для testnet)." >&2; exit 1; }
            echo "Режим: MAINNET, РЕАЛЬНЫЕ ДЕНЬГИ. Маржа/плечо/лимиты - из $ENV_FILE (по умолчанию 10 x 10, убыток за сутки 50 USDT, не более 4 позиций)."
            if [ "${2:-}" != "--yes" ]; then
                printf 'Введите REAL чтобы продолжить: '; read -r answer
                [ "$answer" = "REAL" ] || { echo "Отменено." >&2; exit 1; }
            fi
        fi
        mkdir -p data/shadow logs/shadow data/live logs/live data/updates data/golden_set data/playground data/lab backups; chmod 777 data/golden_set data/playground data/lab  # golden_set, playground, lab: пишет API (uid 10001)
        echo "== Останавливаю shadow-run (shadow-live заменяет его: общая БД, двух процессов быть не должно)"
        $DC stop shadow-run 2>/dev/null || true
        echo "== Сборка и запуск: postgres api shadow-live"
        $DC --profile live up -d --build postgres api shadow-live
        i=0
        while [ "$i" -lt 36 ]; do
            state="$($DC --profile live ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null || true)"
            echo "$state" | grep -q '^shadow-live running' && break
            echo "$state" | grep -q '^shadow-live exited' && { echo "ОШИБКА: shadow-live остановился:" >&2; $DC --profile live logs --tail 40 shadow-live >&2; exit 1; }
            i=$((i + 1)); sleep 5
        done
        ensure_api
        $DC --profile live ps
        $DC --profile live logs --tail 25 shadow-live
        echo "Готово. Лог: ./deploy/shadow.sh logs shadow-live. Вернуться в Shadow: ./deploy/shadow.sh up"
        ;;
    status)
        check_docker; ensure_env
        ensure_api
        $DC ps
        echo "== Пульс (метка времени основного цикла; свежее 120 с = жив)"
        $DC exec -T shadow-run python -c "import os,time; print(round(time.time()-os.path.getmtime('/app/data/shadow/.shadow_heartbeat')), 'с назад')" || true
        echo "== Сделки в БД"
        $DC exec -T postgres psql -U tradeai -d tradeai -c "SELECT status, count(*) FROM trades GROUP BY status ORDER BY status" || true
        echo "== Лог"
        $DC logs --tail 20 shadow-run
        ;;
    logs)
        check_docker; ensure_env
        svc="${2:-shadow-run}"
        case "$svc" in
            shadow-run|shadow-live|api|postgres|analyzer|web|nginx|playground|watch) ;;
            *) echo "ОШИБКА: неизвестный сервис '$svc' (shadow-run | shadow-live | api | postgres | analyzer | web | nginx | playground)" >&2; exit 2 ;;
        esac
        $DC --profile live --profile analytics --profile web --profile playground logs -f --tail 50 "$svc"
        ;;
    apikey)
        check_docker; ensure_env
        echo "Ключ API (вставьте в клиент в поле «API-ключ»):"
        grep '^API_KEY=' "$ENV_FILE" | cut -d= -f2-
        echo
        echo "API опубликован на ${API_BIND:-0.0.0.0}:${API_PORT:-8001} (прямой доступ по IP сервера). Адрес сервера в клиенте:"
        echo "  http://<IP сервера>:${API_PORT:-8001}"
        echo "Порт должен быть открыт в файрволе сервера/провайдера (например: ufw allow ${API_PORT:-8001}/tcp)."
        ;;
    stats)
        check_docker; ensure_env
        $DC exec -T postgres psql -U tradeai -d tradeai -c "
            SELECT strategy_id, coin, side, count(*) AS закрыто,
                   count(*) FILTER (WHERE exit_reason = 'sl')      AS sl,
                   count(*) FILTER (WHERE exit_reason = 'be')      AS be,
                   count(*) FILTER (WHERE exit_reason = 'trail')   AS trail,
                   count(*) FILTER (WHERE exit_reason = 'tp')      AS tp,
                   count(*) FILTER (WHERE exit_reason = 'timeout') AS timeout,
                   round(sum(r_multiple)::numeric, 2) AS net_r,
                   round(avg(r_multiple)::numeric, 3) AS r_на_сделку
              FROM trades
             WHERE status = 'closed' AND NOT gap_affected
             GROUP BY strategy_id, coin, side ORDER BY strategy_id, coin, side"
        ;;
    backup)
        check_docker; ensure_env
        mkdir -p backups
        out="backups/tradeai-$(date -u +%Y%m%d-%H%M%S).sql.gz"
        $DC exec -T postgres pg_dump -U tradeai -d tradeai | gzip > "$out"
        [ -s "$out" ] || { rm -f "$out"; echo "ОШИБКА: бэкап пуст" >&2; exit 1; }
        echo "Бэкап: $out ($(wc -c < "$out") байт)"
        ;;
    psql)
        check_docker; ensure_env
        exec $DC exec postgres psql -U tradeai -d tradeai
        ;;
    api)
        check_docker; ensure_env
        ensure_api
        $DC --profile live ps api
        ;;
    analyzer-init)
        check_docker; ensure_env
        ensure_analyzer_password
        apply_analyzer_role
        ;;
    analyzer-up)
        check_docker; ensure_env
        ensure_analyzer_password
        apply_analyzer_role
        echo "== Сборка и запуск analyzer (бот не перезапускается)"
        $DC --profile live --profile analytics up -d --build --force-recreate --no-deps analyzer
        i=0
        while [ "$i" -lt 24 ]; do
            $DC --profile live --profile analytics ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | grep -q '^analyzer running healthy' && break
            $DC --profile live --profile analytics ps --format '{{.Service}} {{.State}}' 2>/dev/null | grep -q '^analyzer exited' && { echo "ОШИБКА: analyzer остановился:" >&2; $DC --profile live --profile analytics logs --tail 40 analyzer >&2; exit 1; }
            i=$((i + 1)); sleep 5
        done
        $DC --profile live --profile analytics ps analyzer
        echo "Готово. Лог: ./deploy/shadow.sh logs analyzer. API бота проксирует /api/analysis/* к анализатору (Monitor -> вкладка Analysis)."
        ;;
    analyzer-down)
        check_docker; ensure_env
        $DC --profile live --profile analytics stop analyzer
        echo "Анализатор остановлен (том analyzer_data с отчётами сохранён)."
        ;;
    web-up)
        check_docker; ensure_env
        for k in WEB_LOGIN WEB_PASSWORD; do
            grep -q "^$k=." "$ENV_FILE" || { echo "ОШИБКА: в $ENV_FILE не задан $k (вход в веб-версию не может быть открытым)." >&2; exit 1; }
        done
        echo "== Сборка и запуск веб-версии (web + nginx; бот не перезапускается)"
        $DC --profile web up -d --build --force-recreate --no-deps web nginx
        i=0
        while [ "$i" -lt 24 ]; do
            $DC --profile web ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | grep -q '^web running healthy' && break
            $DC --profile web ps --format '{{.Service}} {{.State}}' 2>/dev/null | grep -q '^web exited' && { echo "ОШИБКА: web остановился:" >&2; $DC --profile web logs --tail 40 web >&2; exit 1; }
            i=$((i + 1)); sleep 5
        done
        $DC --profile web ps web nginx
        port="$(grep '^WEB_PUBLIC_PORT=' "$ENV_FILE" | tail -n1 | cut -d= -f2)"
        echo "Готово: веб-версия на порту ${port:-8080} сервера (http://<IP сервера>:${port:-8080}). Лог: ./deploy/shadow.sh logs web"
        ;;
    playground-up)
        check_docker; ensure_env
        mkdir -p data/playground; chmod 777 data/playground  # SQLite пишут и воркер (root), и API (uid 10001)
        echo "== Сборка и запуск Playground (воркер прогонов по истории; бот не перезапускается)"
        $DC --profile playground up -d --build --force-recreate --no-deps playground watch
        sleep 5
        for svc in playground watch; do
            $DC --profile playground ps --format '{{.Service}} {{.State}}' 2>/dev/null | grep -q "^$svc running" || { echo "ОШИБКА: $svc не работает:" >&2; $DC --profile playground logs --tail 40 "$svc" >&2; exit 1; }
        done
        $DC --profile playground ps playground watch
        echo "Готово. Monitor -> вкладка Playground. Лог: ./deploy/shadow.sh logs playground"
        ;;
    playground-down)
        check_docker; ensure_env
        $DC --profile playground stop playground watch
        echo "Playground остановлен (история прогонов в ./data/playground сохранена)."
        ;;
    web-down)
        check_docker; ensure_env
        $DC --profile web stop web nginx
        echo "Веб-версия остановлена."
        ;;
    down)
        check_docker; ensure_env
        $DC --profile live stop api shadow-run shadow-live postgres
        echo "Остановлено. Данные БД (том pgdata) и журнал ./data/shadow сохранены. Запуск: ./deploy/shadow.sh up"
        ;;
    *)
        echo "Использование: $0 {up|up-live|status|api|logs [сервис]|stats|backup|psql|apikey|analyzer-init|analyzer-up|analyzer-down|web-up|web-down|playground-up|playground-down|down}" >&2
        exit 2
        ;;
esac
