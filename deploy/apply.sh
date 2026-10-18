#!/bin/sh
# Автоприменение обновления на сервере БЕЗ ручного ввода `shadow.sh up` / `up-live` (решение пользователя 2026-10-18).
#
#   ./deploy/apply.sh --from ФАЙЛ.tar.gz   # применить присланный архив (его кладёт CI или scripts/auto_deploy.py)
#   ./deploy/apply.sh --pending            # применить архив из ~/trading_incoming, если он ещё не применён (для cron: ждёт, пока позиции закроются)
#   ./deploy/apply.sh --install-cron       # добавить в crontab запуск --pending каждые 10 минут (идемпотентно)
#   ./deploy/apply.sh --status             # что применено, какой контур запущен, есть ли отложенное обновление
#   добавки: --force (не откладывать из-за открытых боевых позиций)
#
# Что делает:
#   1. Определяет работающий контур: shadow-live -> live, иначе shadow (или DEPLOY_CONTOUR=live|shadow в .env.shadow, если ничего не запущено).
#      Контур сохраняется: деплой больше не «роняет» боевой контур в Shadow.
#   2. Боевой контур с открытыми позициями или ждущими заявками (data/live/live_engine_state.json): обновление ОТКЛАДЫВАЕТСЯ (код 75), архив остаётся в
#      ~/trading_incoming, cron применит его, когда позиций не будет. Не позже APPLY_MAX_DEFER_H часов (по умолчанию 12; 0 = ждать бесконечно): рестарт позиции
#      не закрывает - стопы стоят на бирже, движок при старте подтягивает позиции (`recover`).
#   3. Проверяет sha256, сохраняет код предыдущей версии (prev.tar.gz), распаковывает, собирает образ и запускает контур тем же `shadow.sh`
#      (для live - `up-live --yes`: подтверждение REAL заменено явным LIVE_ALLOW_MAINNET=true / USE_TESTNET=true в .env.shadow, они обязательны).
#   4. Проверка здоровья: контейнер running, «пульс» свежее запуска, в логе нет Traceback/CRITICAL/ConfigError. Провал -> автоматический откат
#      на prev.tar.gz и перезапуск прежней версии (код 1).
#   5. После УСПЕШНОГО (не отложенного) применения - Strategy Quant Analyzer: `shadow.sh analyzer-up` (пароль роли analyzer_ro в .env.shadow, SQL-роль,
#      сборка и запуск контейнера профиля analytics). Он изолирован: его сбой НЕ откатывает бота (только предупреждение в логе; шаг 9 auto_deploy его проверяет).
#      Отключить: APPLY_ANALYZER=false (окружение или .env.shadow).
#   Данные (БД, ./data, ./logs, .env*) не трогаются. Сборка образа не удалась -> прежний контейнер продолжает работать, файлы возвращаются.
set -eu

# Распаковка обновления заменяет и сам deploy/apply.sh: работаем с его копией во временном файле, каталог проекта передаём через APPLY_ROOT
if [ -z "${APPLY_REEXEC:-}" ]; then
    root="$(cd "$(dirname "$0")/.." && pwd)"
    tmp="$(mktemp)"
    cp "$0" "$tmp"
    APPLY_REEXEC="$tmp" APPLY_ROOT="$root" exec sh "$tmp" "$@"
fi
trap 'rm -f "$APPLY_REEXEC"' EXIT
cd "$APPLY_ROOT"
ROOT="$(pwd)"
ENV_FILE=".env.shadow"
INCOMING="${APPLY_INCOMING_DIR:-$HOME/trading_incoming}"
ARCHIVE_NAME="trading_shadow.tar.gz"
DC="docker compose --env-file $ENV_FILE"
LOCKDIR=".apply.lock"
EX_TEMPFAIL=75

log() { printf '%s apply: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() { log "ОШИБКА: $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "не найден '$1'"; }

env_value() { [ -f "$ENV_FILE" ] && grep "^$1=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true; }

# контейнеры ищем по метке compose, а не через `compose ps`: тот не видит сервисы выключенных профилей
svc_cid() { docker ps -aq --filter "label=com.docker.compose.service=$1" --filter "label=com.docker.compose.project.working_dir=$ROOT" | head -n1; }
svc_state() { cid="$(svc_cid "$1")"; if [ -n "$cid" ]; then docker inspect -f '{{.State.Status}}' "$cid"; else echo none; fi; }

running_contour() {
    if [ "$(svc_state shadow-live)" = running ]; then echo live
    elif [ "$(svc_state shadow-run)" = running ]; then echo shadow
    else echo none; fi
}

wanted_contour() {
    now="$(running_contour)"
    if [ "$now" != none ]; then echo "$now"; return; fi
    last="$(cat .deploy_contour 2>/dev/null || true)"
    cfg="$(env_value DEPLOY_CONTOUR)"
    case "${cfg:-$last}" in live) echo live ;; *) echo shadow ;; esac
}

live_has_positions() {
    f="data/live/live_engine_state.json"
    [ -f "$f" ] || return 1
    grep -Eq '"trades": *\[ *\]' "$f" && return 1
    return 0
}

archive_sha() { sha256sum "$1" | cut -d' ' -f1; }

start_contour() {  # $1 = live|shadow
    if [ "$1" = live ]; then ./deploy/shadow.sh up-live --yes; else ./deploy/shadow.sh up; fi
}

healthy() {  # $1 = live|shadow, $2 = маркер времени запуска
    svc=shadow-run; hb="data/shadow/.shadow_heartbeat"
    if [ "$1" = live ]; then svc=shadow-live; hb="data/live/.shadow_heartbeat"; fi
    i=0
    while [ "$i" -lt "${APPLY_HEALTH_TRIES:-48}" ]; do  # до 4 минут: история и подключение потоков
        state="$(svc_state "$svc")"
        if [ "$state" = running ] && [ -f "$hb" ] && [ "$hb" -nt "$2" ]; then
            if docker logs --tail 120 "$(svc_cid "$svc")" 2>&1 | grep -Eq 'Traceback|CRITICAL|ConfigError|FATAL'; then
                log "в логе $svc есть ошибки:" >&2
                docker logs --tail 40 "$(svc_cid "$svc")" >&2 || true
                return 1
            fi
            return 0
        fi
        if [ "$state" = exited ] || [ "$state" = dead ]; then log "$svc остановился" >&2; return 1; fi
        i=$((i + 1)); sleep "${APPLY_HEALTH_SLEEP:-5}"
    done
    log "за отведённое время $svc не показал свежий пульс" >&2
    return 1
}

deploy_analyzer() {  # изолированный сервис: любой сбой - предупреждение, бот и его откат не затрагиваются
    flag="${APPLY_ANALYZER:-$(env_value APPLY_ANALYZER)}"
    case "${flag:-true}" in false|0|no|off) log "анализатор: автоуправление выключено (APPLY_ANALYZER)"; return 0 ;; esac
    [ -f analyzer/Dockerfile ] || { log "анализатор: в архиве нет analyzer/Dockerfile - пропуск"; return 0; }
    log "анализатор: пароль роли, SQL-роль, сборка и запуск (shadow.sh analyzer-up)"
    if ./deploy/shadow.sh analyzer-up; then log "анализатор запущен"; else log "ПРЕДУПРЕЖДЕНИЕ: анализатор не запустился (бот работает; ./deploy/shadow.sh logs analyzer)" >&2; fi
    return 0
}

unpack() {  # $1 = архив
    tar --force-local -xzf "$1" -C "$ROOT"
    chmod +x deploy/shadow.sh deploy/apply.sh
    [ -f "$ENV_FILE" ] && chmod 600 "$ENV_FILE" || true
}

cmd_status() {
    echo "применена версия: $(cat .deploy_applied 2>/dev/null || echo нет)"
    echo "контур (последний применённый): $(cat .deploy_contour 2>/dev/null || echo нет); сейчас работает: $(running_contour)"
    if [ -f "$INCOMING/$ARCHIVE_NAME" ]; then
        sha="$(archive_sha "$INCOMING/$ARCHIVE_NAME")"
        if [ "$sha" = "$(cat .deploy_applied 2>/dev/null || true)" ]; then echo "в $INCOMING архив уже применён"; else echo "ОТЛОЖЕНО: в $INCOMING лежит неприменённый архив ${sha%"${sha#????????????}"}"; fi
    else
        echo "отложенных обновлений нет"
    fi
    live_has_positions && echo "боевые позиции/заявки: ЕСТЬ (обновление live-контура ждёт их закрытия)" || echo "боевые позиции/заявки: нет"
}

cmd_install_cron() {
    need crontab
    line="*/10 * * * * cd $ROOT && ./deploy/apply.sh --pending >> logs/apply.log 2>&1"
    mkdir -p logs
    if crontab -l 2>/dev/null | grep -Fq "deploy/apply.sh --pending"; then log "cron уже установлен"; return; fi
    { crontab -l 2>/dev/null || true; echo "$line"; } | crontab -
    log "cron установлен: $line"
}

apply_archive() {  # $1 = архив, $2 = force|normal
    need docker; need tar; need sha256sum
    docker info >/dev/null 2>&1 || die "демон Docker недоступен"
    [ -f docker-compose.yml ] && [ -f "$ENV_FILE" ] || die "запускайте из каталога проекта, где есть docker-compose.yml и $ENV_FILE (первый запуск - deploy/shadow.sh up)"
    archive="$1"
    [ -f "$archive" ] || die "нет файла $archive"
    if [ -f "$archive.sha256" ]; then
        expected="$(cut -d' ' -f1 "$archive.sha256")"
        [ "$expected" = "$(archive_sha "$archive")" ] || die "sha256 архива не совпал: файл повреждён при передаче"
    fi
    sha="$(archive_sha "$archive")"
    if [ "$sha" = "$(cat .deploy_applied 2>/dev/null || true)" ]; then log "версия ${sha%"${sha#????????????}"} уже применена"; return 0; fi

    contour="$(wanted_contour)"
    if [ "$contour" = live ] && [ "$2" != force ] && live_has_positions; then
        age_h=$(( ( $(date +%s) - $(stat -c %Y "$archive") ) / 3600 ))
        max="${APPLY_MAX_DEFER_H:-$(env_value APPLY_MAX_DEFER_H)}"; max="${max:-12}"
        if [ "$max" = 0 ] || [ "$age_h" -lt "$max" ]; then
            log "ОТЛОЖЕНО: в боевом контуре есть открытые позиции/заявки; повтор по cron (ждём уже ${age_h} ч, предел ${max} ч, 0 = без предела)"
            return "$EX_TEMPFAIL"
        fi
        log "предел ожидания ${max} ч исчерпан: применяю несмотря на позиции (стопы на бирже, движок подтянет позиции при старте)"
    fi

    mkdir -p "$INCOMING"
    log "контур: $contour; версия ${sha%"${sha#????????????}"}; сохраняю прежний код"
    tar --force-local -czf "$INCOMING/prev.tar.gz" --exclude=./data --exclude=./logs --exclude=./backups --exclude='./.env*' --exclude="./$LOCKDIR" \
        --exclude=./.deploy_applied --exclude=./.deploy_contour -C "$ROOT" .
    marker="$(mktemp)"; touch "$marker"; sleep 1
    unpack "$archive"
    if start_contour "$contour" && healthy "$contour" "$marker"; then
        printf '%s\n' "$sha" > .deploy_applied
        printf '%s\n' "$contour" > .deploy_contour
        mkdir -p "$INCOMING/applied"
        mv -f "$archive" "$INCOMING/applied/$ARCHIVE_NAME" 2>/dev/null || true
        rm -f "$archive.sha256" "$marker"
        log "ГОТОВО: версия ${sha%"${sha#????????????}"} применена, контур $contour"
        deploy_analyzer
        return 0
    fi
    log "ПРОВАЛ применения: откат на прежнюю версию" >&2
    rm -f "$marker"
    unpack "$INCOMING/prev.tar.gz"
    marker="$(mktemp)"; touch "$marker"; sleep 1
    if start_contour "$contour" && healthy "$contour" "$marker"; then
        rm -f "$marker"
        die "новая версия не запустилась, прежняя восстановлена и работает (разберите логи: ./deploy/shadow.sh logs)"
    fi
    rm -f "$marker"
    die "новая версия не запустилась, и прежняя тоже: нужна ручная проверка (./deploy/shadow.sh logs)"
}

mode=""; src=""; force=normal
while [ $# -gt 0 ]; do
    case "$1" in
        --from) mode=from; src="${2:?--from требует путь к архиву}"; shift ;;
        --pending) mode=pending ;;
        --install-cron) mode=cron ;;
        --status) mode=status ;;
        --force) force=force ;;
        *) echo "Использование: $0 {--from ФАЙЛ | --pending | --install-cron | --status} [--force]" >&2; exit 2 ;;
    esac
    shift
done
[ -n "$mode" ] || { echo "Использование: $0 {--from ФАЙЛ | --pending | --install-cron | --status} [--force]" >&2; exit 2; }

case "$mode" in
    status) cmd_status; exit 0 ;;
    cron) cmd_install_cron; exit 0 ;;
esac

# один применяющий процесс за раз (cron + CI); замок старше часа считается брошенным
if [ -d "$LOCKDIR" ] && [ -n "$(find "$LOCKDIR" -maxdepth 0 -mmin +60 2>/dev/null)" ]; then rmdir "$LOCKDIR" 2>/dev/null || true; fi
mkdir "$LOCKDIR" 2>/dev/null || { log "другое применение уже идёт"; exit 0; }
trap 'rmdir "$LOCKDIR" 2>/dev/null || true; rm -f "$APPLY_REEXEC"' EXIT

if [ "$mode" = pending ]; then
    [ -f "$INCOMING/$ARCHIVE_NAME" ] || exit 0
    src="$INCOMING/$ARCHIVE_NAME"
fi
rc=0
apply_archive "$src" "$force" || rc=$?
exit "$rc"
