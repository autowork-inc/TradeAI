#!/usr/bin/env bash
# Включение HTTPS для веб-версии и API (Let's Encrypt, домен nip.io). Запускать на сервере ОДИН раз:   cd ~/trading && ./deploy/init-ssl.sh --email you@example.com
#
#   ./deploy/init-ssl.sh [--email АДРЕС] [--staging] [--mode auto|nginx|caddy] [--apply-caddy] [--close-plain] [--domain ДОМЕН]
#
# Что делает (режим выбирается сам по тому, кто держит порты 80/443 на сервере; --mode задаёт его принудительно):
#   nginx  - порты 80 и 443 свободны (или уже наши): nginx контейнера слушает 80 (только acme-challenge + 301 на https://ДОМЕН$request_uri) и 443 (TLS, прокси на web и api);
#            первый сертификат запрашивается у Let's Encrypt через webroot (`certbot certonly --webroot`), дальше контейнер certbot продлевает его сам;
#   caddy  - порты 80/443 держит Caddy (на нашем VPS - для другого сайта): нашу nginx он не вытесняется. Сертификат получает и продлевает САМ Caddy; нужен блок
#            deploy/caddy-tradeai.caddy в /etc/caddy/Caddyfile (без --apply-caddy скрипт только показывает блок и команду, файл Caddy не трогает).
# --staging - тестовый сертификат Let's Encrypt (без лимитов, браузер ему не доверяет): проверить цепочку до боевого запроса.
# --close-plain - после успешной проверки закрыть прежний HTTP-вход веб-версии снаружи (порт 8080 -> только localhost). Порт API 8001 остаётся открытым, пока старые
#            клиенты не перейдут на https (они переходят сами, когда видят рабочий https-адрес); закрыть его: API_BIND=127.0.0.1 в .env.shadow и `./deploy/shadow.sh api`.
# Идемпотентен: повторный запуск ничего не ломает (сертификат уже есть - заново не запрашивается).
set -euo pipefail

DOMAIN="${SSL_DOMAIN:-193.233.75.254.nip.io}"
EMAIL="${SSL_EMAIL:-}"
MODE="auto"
STAGING=0
APPLY_CADDY=0
CLOSE_PLAIN=0
ENV_FILE=".env.shadow"
CADDYFILE="${CADDYFILE:-/etc/caddy/Caddyfile}"

while [ $# -gt 0 ]; do
    case "$1" in
        --email) EMAIL="${2:?--email требует адрес}"; shift ;;
        --staging) STAGING=1 ;;
        --mode) MODE="${2:?--mode: auto|nginx|caddy}"; shift ;;
        --apply-caddy) APPLY_CADDY=1 ;;
        --close-plain) CLOSE_PLAIN=1 ;;
        --domain) DOMAIN="${2:?--domain требует домен}"; shift ;;
        -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
        *) echo "Неизвестный параметр: $1 (см. --help)" >&2; exit 2 ;;
    esac
    shift
done

cd "$(dirname "$0")/.."
log() { printf '%s init-ssl: %s\n' "$(date -u +%FT%TZ)" "$*"; }
die() { log "ОШИБКА: $*" >&2; exit 1; }

[ -f "$ENV_FILE" ] || die "нет $ENV_FILE (запускайте из ~/trading после первого деплоя)"
command -v docker >/dev/null || die "нет docker"
command -v curl >/dev/null || die "нет curl"
case "$MODE" in auto|nginx|caddy) ;; *) die "--mode: auto|nginx|caddy" ;; esac
case "$DOMAIN" in *[!A-Za-z0-9.-]*|"") die "некорректный домен: $DOMAIN" ;; esac

DC="docker compose --env-file $ENV_FILE --profile web --profile ssl"

set_env() {  # set_env КЛЮЧ ЗНАЧЕНИЕ: заменить/дописать строку в .env.shadow (права 600 сохраняются)
    local key="$1" val="$2"
    touch "$ENV_FILE"
    if grep -q "^${key}=" "$ENV_FILE"; then sed -i "s|^${key}=.*|${key}=${val}|" "$ENV_FILE"; else
        { [ -z "$(tail -c1 "$ENV_FILE")" ] || echo; echo "${key}=${val}"; } >> "$ENV_FILE"
    fi
    chmod 600 "$ENV_FILE"
}

port_owner() {  # кто слушает порт $1: free | caddy | ours | other:<процесс>
    local line
    line="$(ss -ltnpH "( sport = :$1 )" 2>/dev/null | head -1 || true)"
    if [ -z "$line" ]; then echo free; return; fi
    case "$line" in
        *caddy*) echo caddy ;;
        *docker-proxy*)
            if docker ps --format '{{.Names}} {{.Ports}}' | grep -q "nginx.*:$1->"; then echo ours; else echo "other:docker-proxy"; fi ;;
        *) echo "other:$(printf '%s' "$line" | sed -E 's/.*users:\(\("([^"]+)".*/\1/')" ;;
    esac
}

wait_https() {  # ждать рабочий https-адрес до $1 секунд
    local deadline=$(( $(date +%s) + $1 )) code=""
    while [ "$(date +%s)" -lt "$deadline" ]; do
        code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 8 "https://${DOMAIN}/api/health" 2>/dev/null || true)"
        [ "$code" = "200" ] && return 0
        sleep 3
    done
    return 1
}

o80="$(port_owner 80)"; o443="$(port_owner 443)"
if [ "$MODE" = auto ]; then
    if { [ "$o80" = free ] || [ "$o80" = ours ]; } && { [ "$o443" = free ] || [ "$o443" = ours ]; }; then MODE=nginx
    elif [ "$o80" = caddy ] || [ "$o443" = caddy ]; then MODE=caddy
    else die "порты заняты чужими программами: 80 -> $o80, 443 -> $o443; освободите их или настройте прокси вручную (блок для Caddy: deploy/caddy-tradeai.caddy)"; fi
fi
log "домен $DOMAIN, режим $MODE (порт 80: $o80, порт 443: $o443)"

# ---------------------------------------------------------------------------------------------------------------- режим caddy
if [ "$MODE" = caddy ]; then
    snippet="deploy/caddy-tradeai.caddy"
    [ -f "$snippet" ] || die "нет $snippet"
    $DC up -d nginx >/dev/null
    if grep -q "^${DOMAIN}" "$CADDYFILE" 2>/dev/null; then
        log "блок $DOMAIN уже есть в $CADDYFILE"
    elif [ "$APPLY_CADDY" = 1 ]; then
        [ -w "$CADDYFILE" ] || die "нет доступа на запись в $CADDYFILE (запускайте от root)"
        cp -a "$CADDYFILE" "${CADDYFILE}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
        { printf '\n'; cat "$snippet"; } >> "$CADDYFILE"
        if command -v caddy >/dev/null && ! caddy validate --config "$CADDYFILE" --adapter caddyfile >/dev/null 2>&1; then
            cp -a "$(ls -t "${CADDYFILE}".bak-* | head -1)" "$CADDYFILE"
            die "Caddy не принял конфиг, файл возвращён из резервной копии"
        fi
        systemctl reload caddy || die "systemctl reload caddy не удался (резервная копия: ${CADDYFILE}.bak-*)"
        log "блок добавлен в $CADDYFILE, Caddy перечитал конфиг (резервная копия рядом)"
    else
        echo
        echo "Caddy держит порты 80/443. Добавьте в $CADDYFILE этот блок (скрипт ваш Caddyfile без --apply-caddy не трогает):"
        echo "-----------------------------------------------------------------"
        cat "$snippet"
        echo "-----------------------------------------------------------------"
        echo "Затем:  caddy validate --config $CADDYFILE --adapter caddyfile && systemctl reload caddy"
        echo "Либо одной командой (резервная копия Caddyfile делается):  ./deploy/init-ssl.sh --mode caddy --apply-caddy"
        exit 0
    fi
    log "жду сертификат и ответ https://${DOMAIN}/api/health (до 120 с; сертификат Caddy получает при первом обращении)"
    if wait_https 120; then log "HTTPS работает: https://${DOMAIN}"; else die "https://${DOMAIN}/api/health не ответил 200 за 120 с: смотрите journalctl -u caddy -n 50 (частая причина - лимит Let's Encrypt для nip.io, см. README)"; fi
    if [ "$CLOSE_PLAIN" = 1 ]; then set_env WEB_BIND 127.0.0.1; $DC up -d nginx >/dev/null; log "HTTP-вход веб-версии (8080) закрыт снаружи, работает через https"; fi
    exit 0
fi

# ---------------------------------------------------------------------------------------------------------------- режим nginx + certbot
mkdir -p data/certbot/conf data/certbot/www/.well-known/acme-challenge data/certbot/ssl.d
set_env WEB_HTTP_BIND 0.0.0.0
set_env WEB_HTTP_PORT 80
set_env WEB_HTTPS_BIND 0.0.0.0
set_env WEB_HTTPS_PORT 443
log "поднимаю nginx на портах 80 и 443 (HTTPS-блок появится после получения сертификата)"
$DC up -d --build web nginx >/dev/null

echo "ping-$$" > data/certbot/www/.well-known/acme-challenge/ping
if ! curl -fsS --max-time 10 "http://${DOMAIN}/.well-known/acme-challenge/ping" 2>/dev/null | grep -q "ping-$$"; then
    rm -f data/certbot/www/.well-known/acme-challenge/ping
    die "http://${DOMAIN}/.well-known/acme-challenge/ не отвечает с этого сервера: порт 80 закрыт файрволом / домен не указывает на сервер (nslookup ${DOMAIN})"
fi
rm -f data/certbot/www/.well-known/acme-challenge/ping

cert="data/certbot/conf/live/${DOMAIN}/fullchain.pem"
if [ -s "$cert" ]; then
    log "сертификат уже есть ($cert), повторно не запрашиваю"
else
    # nginx в контейнере работает НЕ от root (uid/gid 101): закрытый ключ certbot по умолчанию 0600 root, и nginx его не прочёл бы. Хук выставляет группе 101 чтение ключа
    # (640) и запоминается в renewal-конфиге: он срабатывает и при каждом продлении (новый ключ снова получил бы 0600).
    hook='chgrp -R 101 /etc/letsencrypt/archive && chmod 750 /etc/letsencrypt/archive /etc/letsencrypt/archive/* && chmod 640 /etc/letsencrypt/archive/*/privkey*.pem'
    args=(certonly --webroot -w /var/www/certbot -d "$DOMAIN" --agree-tos --no-eff-email --non-interactive --keep-until-expiring --deploy-hook "$hook")
    if [ -n "$EMAIL" ]; then args+=(--email "$EMAIL"); else args+=(--register-unsafely-without-email); log "ВНИМАНИЕ: email не задан - Let's Encrypt не сможет предупредить об истечении (--email АДРЕС)"; fi
    [ "$STAGING" = 1 ] && args+=(--staging)
    log "запрашиваю сертификат у Let's Encrypt$([ "$STAGING" = 1 ] && echo ' (ТЕСТОВЫЙ, staging)')"
    $DC run --rm --entrypoint certbot certbot "${args[@]}" || die "Let's Encrypt отказал (вывод выше). Для nip.io частая причина - общий лимит выпуска на домен nip.io; попробуйте позже или --mode caddy"
    [ -s "$cert" ] || die "certbot отработал, но $cert нет"
fi

cp web/nginx-ssl.conf data/certbot/ssl.d/tradeai.conf
$DC up -d nginx >/dev/null
$DC exec -T nginx nginx -t || die "nginx не принял конфиг с HTTPS (вывод выше); файл data/certbot/ssl.d/tradeai.conf можно удалить - прежний HTTP-вход продолжит работать"
$DC exec -T nginx nginx -s reload
$DC up -d certbot >/dev/null
log "контейнер certbot продлевает сертификат автоматически (проверка раз в 12 часов), nginx перечитывает его раз в 6 часов"

if wait_https 60; then log "HTTPS работает: https://${DOMAIN}  (веб-версия), https://${DOMAIN}/api/health (API)"; else
    [ "$STAGING" = 1 ] && log "staging-сертификат браузеры и curl не принимают - это нормально; проверка: curl -k https://${DOMAIN}/api/health" || die "https://${DOMAIN}/api/health не ответил 200: $DC logs nginx"
fi
if [ "$CLOSE_PLAIN" = 1 ]; then set_env WEB_BIND 127.0.0.1; $DC up -d nginx >/dev/null; log "HTTP-вход веб-версии (8080) закрыт снаружи"; fi
log "готово. Дальше: клиентам - https://${DOMAIN} (десктоп переключится сам), ./deploy/init-ssl.sh --close-plain после проверки"
