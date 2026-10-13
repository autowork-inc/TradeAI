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

ensure_env() {
    ensure_password
    ensure_api_key
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
    printf 'POSTGRES_PASSWORD=%s\n' "$pw" >> "$ENV_FILE"
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
    printf 'API_KEY=%s\n' "$key" >> "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "В $ENV_FILE создан ключ API мониторинга (показать: ./deploy/shadow.sh apikey)."
}

cmd="${1:-up}"
case "$cmd" in
    up)
        check_docker
        [ -f docker-compose.yml ] && [ -f Dockerfile ] || { echo "ОШИБКА: запускайте из каталога проекта (нет docker-compose.yml / Dockerfile)." >&2; exit 1; }
        ensure_env
        mkdir -p data/shadow logs/shadow backups
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
        $DC ps
        echo "== Последние строки лога shadow-run"
        $DC logs --tail 15 shadow-run
        echo
        echo "Готово. Дальше: ./deploy/shadow.sh status | logs | stats | backup | apikey"
        ;;
    status)
        check_docker; ensure_env
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
            shadow-run|api|postgres) ;;
            *) echo "ОШИБКА: неизвестный сервис '$svc' (shadow-run | api | postgres)" >&2; exit 2 ;;
        esac
        $DC logs -f --tail 50 "$svc"
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
    down)
        check_docker; ensure_env
        $DC stop api shadow-run postgres
        echo "Остановлено. Данные БД (том pgdata) и журнал ./data/shadow сохранены. Запуск: ./deploy/shadow.sh up"
        ;;
    *)
        echo "Использование: $0 {up|status|logs [сервис]|stats|backup|psql|apikey|down}" >&2
        exit 2
        ;;
esac
