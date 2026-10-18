"""Автодеплой Shadow Mode на VPS одной командой: python -m scripts.auto_deploy --host 1.2.3.4 [--user root] [--key PATH].

Параметры: аргументы CLI > окружение (VPS_HOST, VPS_PORT, VPS_USER, VPS_PASSWORD, VPS_SSH_KEY_PATH) > ввод в консоли.
Лучше ключ (`--key`) или `VPS_PASSWORD`: пароль в `--password` виден в истории оболочки.
Ключи MetaApi (Forex/металлы в Shadow): `--metaapi-token` / `--metaapi-account` или окружение METAAPI_TOKEN / METAAPI_ACCOUNT_ID (окружение
безопаснее: аргументы видны в истории оболочки). Значения вписываются в `~/trading/.env.shadow` (chmod 600) через временный файл по SFTP:
в командах SSH, в выводе и логах их нет, в архив и в Git они не попадают. Не заданы - `.env.shadow` на сервере не меняется (уже вписанные
ключи остаются); заглушки не пишутся: shadow-run с фиктивным токеном упал бы при подключении к MetaApi (R1).

Шаги: 1 архив (deploy_pack, цель shadow, без ключей) · 2 SSH + SFTP · 3 Docker · 4 распаковка · 5 права .env* ·
6 остановка старых контейнеров · 7 запуск · 8 PostgreSQL и миграции · 9 глубокая проверка (контейнеры, логи, блокировка IP Binance).
Первый деплой (нет ~/trading/.env.shadow): распаковка и `deploy/shadow.sh up`. Повторный деплой (2026-10-18): шаги 4/6/7 заменены на
`deploy/apply.sh --from ...` - контур (Shadow или боевой shadow-live) СОХРАНЯЕТСЯ, `up-live` вводить не нужно, обновление боевого контура откладывается, пока
есть открытые позиции (код 75: деплой считается принятым, дальше применяет cron), при провале запуска - автоматический откат на прежнюю версию. Любая ошибка команды на сервере немедленно прерывает деплой (R1). Данные БД не удаляются:
`shadow.sh down` только останавливает контейнеры, том `pgdata` и `./data/shadow` сохраняются.
Требует: pip install -r requirements-dev.txt (paramiko).
"""
from __future__ import annotations

import argparse
import getpass
import os
import re
import shlex
import socket
import sys
import time
from pathlib import Path

try:
    import paramiko
except ImportError:
    sys.exit("Не установлен paramiko. Выполните: pip install -r requirements-dev.txt")

from persistence.schema import SCHEMA_VERSION
from scripts.deploy_pack import ROOT, build_archive

ARCHIVE = "trading_shadow.tar.gz"
TIMEOUT = 20
TOTAL = 9
DC = "docker compose --env-file .env.shadow"
REQUIRED_SERVICES = ("postgres", "shadow-run", "api", "analyzer")  # analyzer - изолированный сервис профиля analytics (его поднимает apply.sh / analyzer-up)
LOG_WINDOW = "10m"  # окно логов шага 9: только свежее (старые ошибки прошлых запусков не считаются)
LIVE_REQUIRED_SERVICES = ("postgres", "shadow-live", "api", "analyzer")
INCOMING = "trading_incoming"  # каталог на сервере для присланных архивов (его же читает cron: deploy/apply.sh --pending)
EX_DEFERRED = 75  # deploy/apply.sh: обновление отложено до закрытия боевых позиций
ENV_FILE = ".env.shadow"
METAAPI_TMP = ".metaapi.tmp"
METAAPI_KEYS = ("METAAPI_TOKEN", "METAAPI_ACCOUNT_ID")

# Признаки блокировки IP биржей (HTTP 451 — гео/юрисдикция, 429/418 — лимиты и бан IP, отказ рукопожатия WebSocket).
BLOCK_PATTERNS = (
    re.compile(r"HTTP[ /]*(?:status[ :=]*)?(451|429|418)\b", re.I),
    re.compile(r"\bstatus(?:[ _]code)?[ :=]+(451|429|418)\b", re.I),
    re.compile(r"Handshake failed", re.I),
    re.compile(r"rejected WebSocket connection", re.I),
    re.compile(r"restricted location", re.I),
)
ERROR_PATTERNS = tuple(re.compile(p) for p in (r"Traceback", r"\bCRITICAL\b", r"\bFATAL\b", r"ConfigError"))

# os.system("") — константный вызов (без пользовательского ввода): включает ANSI-цвета в консоли Windows
_COLOR = sys.stdout.isatty() and (os.name != "nt" or os.system("") == 0)


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOR else text


def step(n: int, text: str) -> None:
    print(_c("1;36", f"\n[{n}/{TOTAL}] {text}"))


def ok(text: str) -> None:
    print(_c("32", f"  OK  {text}"))


def fail(text: str) -> "NoReturn":  # noqa: F821
    print(_c("1;31", f"  FAIL: {text}"), file=sys.stderr)
    raise SystemExit(1)


def scan_logs(text: str) -> tuple[list[str], list[str]]:
    """Возвращает (строки блокировки IP, строки ошибок). Чистая функция — проверяется тестом."""
    blocked, errors = [], []
    for line in text.splitlines():
        if any(p.search(line) for p in BLOCK_PATTERNS):
            blocked.append(line.strip())
        elif any(p.search(line) for p in ERROR_PATTERNS):
            errors.append(line.strip())
    return blocked, errors


def parse_services(ps_output: str) -> dict[str, str]:
    """Строки `<service> <state>` → {service: state}."""
    result: dict[str, str] = {}
    for line in ps_output.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            result[parts[0]] = parts[1]
    return result


def parse_health(ps_output: str) -> dict[str, str]:
    """Строки `<service> <state> <health>` -> {service: health} (у сервисов без healthcheck колонки нет)."""
    return {p[0]: p[2] for p in (line.split() for line in ps_output.splitlines()) if len(p) >= 3}


def validate_metaapi(token: str | None, account: str | None) -> dict[str, str]:
    """Проверка ключей MetaApi до любых действий. Возвращает {имя: значение} только для заданных; значения в сообщения об ошибках не попадают."""
    if bool(token) != bool(account):
        fail("MetaApi: токен и id счёта задаются вместе (--metaapi-token и --metaapi-account)")
    if not token:
        return {}
    if not re.fullmatch(r"[!-~]+", token) or "'" in token or '"' in token or "\\" in token:
        fail("--metaapi-token: только печатные ASCII-символы без пробелов, кавычек и обратной косой черты")
    if not re.fullmatch(r"[A-Za-z0-9-]{8,64}", account):
        fail("--metaapi-account: ожидается id счёта MetaApi (латиница, цифры, дефис)")
    return {"METAAPI_TOKEN": token, "METAAPI_ACCOUNT_ID": account}


def merge_env_command(d: str) -> str:
    """Команда на сервере: строки METAAPI_* из временного файла заменяют/дописывают одноимённые в .env.shadow. Секретов в тексте команды нет."""
    return (f"cd {d} && umask 077 && touch {ENV_FILE} && {{ [ -z \"$(tail -c1 {ENV_FILE})\" ] || echo >> {ENV_FILE}; }} && "
            f"for k in {' '.join(METAAPI_KEYS)}; do if grep -q \"^$k=\" {METAAPI_TMP}; then sed -i \"/^$k=/d\" {ENV_FILE}; "
            f"grep \"^$k=\" {METAAPI_TMP} >> {ENV_FILE}; fi; done; rc=$?; rm -f {METAAPI_TMP}; chmod 600 {ENV_FILE}; exit $rc")


def push_metaapi(cli: paramiko.SSHClient, remote_dir: str, values: dict[str, str]) -> None:
    sftp = cli.open_sftp()
    tmp = f"{remote_dir}/{METAAPI_TMP}"
    try:
        with sftp.open(tmp, "w") as f:
            sftp.chmod(tmp, 0o600)  # права до записи секрета
            f.write("".join(f"{k}={v}\n" for k, v in values.items()))
    finally:
        sftp.close()
    run(cli, merge_env_command(f"~/{remote_dir}"), "запись ключей MetaApi в .env.shadow")  # временный файл удаляется самой командой


def get_params(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default=os.environ.get("VPS_HOST"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("VPS_PORT", 22)))
    ap.add_argument("--user", default=os.environ.get("VPS_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("VPS_PASSWORD"), help="лучше VPS_PASSWORD или --key")
    ap.add_argument("--key", default=os.environ.get("VPS_SSH_KEY_PATH"), help="путь к приватному SSH-ключу")
    ap.add_argument("--metaapi-token", default=os.environ.get("METAAPI_TOKEN"), help="токен MetaApi (лучше окружение METAAPI_TOKEN); в логи не выводится")
    ap.add_argument("--metaapi-account", default=os.environ.get("METAAPI_ACCOUNT_ID"), help="id счёта MetaApi (или METAAPI_ACCOUNT_ID)")
    ap.add_argument("--remote-dir", default="trading", help="каталог на сервере относительно домашнего (по умолчанию trading)")
    ap.add_argument("--health-wait", type=int, default=15, help="секунд ожидания перед глубокой проверкой (по умолчанию 15)")
    ap.add_argument("--keep-archive", action="store_true", help="не удалять локальный архив после деплоя")
    a = ap.parse_args(argv)
    if not a.host:
        a.host = input("VPS host: ").strip()
    if not a.host:
        fail("хост не задан")
    if not re.fullmatch(r"[A-Za-z0-9._/-]+", a.remote_dir) or a.remote_dir.startswith("/") or ".." in a.remote_dir:
        fail("--remote-dir: только относительный путь из латиницы, цифр и ._/-")
    a.metaapi = validate_metaapi(a.metaapi_token, a.metaapi_account)
    if not a.key and not a.password:
        a.password = getpass.getpass(f"Пароль {a.user}@{a.host}: ")
    return a


def connect(a: argparse.Namespace) -> paramiko.SSHClient:
    cli = paramiko.SSHClient()
    cli.load_system_host_keys()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # TOFU: новый ключ принимается при первом входе
    try:
        cli.connect(a.host, port=a.port, username=a.user, password=a.password,
                    key_filename=a.key, timeout=TIMEOUT, banner_timeout=TIMEOUT, auth_timeout=TIMEOUT,
                    look_for_keys=not a.password, allow_agent=not a.password)
    except paramiko.AuthenticationException:
        fail("аутентификация отклонена: проверьте пользователя, пароль или ключ")
    except (TimeoutError, socket.timeout):
        fail(f"таймаут подключения к {a.host}:{a.port} (хост недоступен или порт закрыт файрволом)")
    except socket.gaierror:
        fail(f"не удалось разрешить имя хоста {a.host}")
    except OSError as e:
        fail(f"сетевая ошибка при подключении к {a.host}:{a.port}: {e.strerror or e}")
    except paramiko.SSHException as e:
        fail(f"ошибка SSH: {e}")
    return cli


def run(cli: paramiko.SSHClient, cmd: str, what: str, check: bool = True, quiet: bool = False) -> tuple[int, str]:
    """Выполняет команду, стримит вывод, возвращает (код, вывод). check=True → выход при коде != 0 (Fail-Fast)."""
    print(_c("2", f"  $ {cmd}"))
    _, out, _ = cli.exec_command(cmd, get_pty=True, timeout=900)
    buf: list[str] = []
    for line in iter(out.readline, ""):
        buf.append(line)
        if not quiet:
            print("    " + line.rstrip())
    code = out.channel.recv_exit_status()
    if check and code != 0:
        fail(f"{what}: код возврата {code}")
    return code, "".join(buf)


def main(argv: list[str] | None = None) -> int:
    a = get_params(argv)
    d = f"~/{a.remote_dir}"
    archive: Path | None = None
    cli: paramiko.SSHClient | None = None
    try:
        step(1, "Сборка архива (цель shadow, без ключей API)")
        archive = build_archive(ROOT / ARCHIVE, "shadow")
        if not archive.is_file() or archive.stat().st_size == 0:
            fail("архив не создан")
        ok(archive.name)

        step(2, f"SSH/SFTP: загрузка на {a.user}@{a.host}:{a.port}")
        cli = connect(a)
        sftp = cli.open_sftp()
        remote = f"{sftp.normalize('.')}/{ARCHIVE}"
        sftp.put(str(archive), remote)
        if sftp.stat(remote).st_size != archive.stat().st_size:
            fail("размер загруженного файла не совпал с локальным")
        sftp.close()
        ok(f"{remote} ({archive.stat().st_size // 1024} КиБ)")

        step(3, "Docker и Compose v2")
        have, _ = run(cli, "docker --version && docker compose version", "проверка Docker", check=False)
        if have != 0:
            print(_c("33", "  Docker/compose не найдены — устанавливаю (Ubuntu/Debian)"))
            sudo = "" if a.user == "root" else "sudo -n "
            run(cli, f"export DEBIAN_FRONTEND=noninteractive; {sudo}apt-get update && "
                     f"({sudo}apt-get install -y docker.io docker-compose-v2 || "
                     f"(curl -fsSL https://get.docker.com | {sudo}sh)) && {sudo}systemctl enable --now docker",
                "установка Docker")
            run(cli, "docker --version && docker compose version", "Docker после установки")
        run(cli, "docker info >/dev/null", "демон Docker недоступен (права пользователя? запущен ли сервис?)")
        ok("Docker и compose v2 готовы")

        step(4, f"Распаковка в {d}")
        incremental = run(cli, f"test -f {d}/.env.shadow", "первый деплой?", check=False, quiet=True)[0] == 0
        contour, deferred = "shadow", False
        if incremental:
            run(cli, f"mkdir -p ~/{INCOMING} {d}/deploy && mv -f {shlex.quote(ARCHIVE)} ~/{INCOMING}/ && mv -f {shlex.quote(ARCHIVE + '.sha256')} ~/{INCOMING}/ 2>/dev/null || true",
                "перенос архива в каталог обновлений")
            run(cli, f"tar --force-local -xzf ~/{INCOMING}/{ARCHIVE} -C {d} deploy/apply.sh && chmod +x {d}/deploy/apply.sh", "свежий apply.sh из архива")
            ok(f"архив в ~/{INCOMING}, применение - deploy/apply.sh (контур сохраняется)")
        else:
            run(cli, f"mkdir -p {d} && tar -xzf {shlex.quote(ARCHIVE)} -C {d} && rm -f {shlex.quote(ARCHIVE)}", "распаковка архива")
            run(cli, f"cd {d} && chmod +x deploy/shadow.sh deploy/apply.sh && ls docker-compose.yml Dockerfile deploy/shadow.sh deploy/apply.sh", "проверка состава")
            ok("распаковано (первый деплой; .env.shadow с паролем БД архивом не затирается)")

        step(5, "Права на секреты")
        run(cli, f"cd {d} && for f in .env .env.shadow; do if [ -f $f ]; then chmod 600 $f; fi; done", "chmod 600")
        ok(".env / .env.shadow (если есть) — 600")
        if a.metaapi:
            push_metaapi(cli, a.remote_dir, a.metaapi)
            ok("METAAPI_TOKEN / METAAPI_ACCOUNT_ID записаны в .env.shadow (значения не выводятся)")
        else:
            print(_c("33", "  MetaApi: ключи не заданы, .env.shadow не менялся (Forex/металлы заработают после ввода ключей)"))

        if incremental:
            step(6, "Остановка старых контейнеров не нужна: deploy/apply.sh сам перезапускает действующий контур")
            ok("пропущено")
            step(7, "Применение обновления (deploy/apply.sh --from)")
            code, _ = run(cli, f"cd {d} && ./deploy/apply.sh --install-cron && ./deploy/apply.sh --from ~/{INCOMING}/{ARCHIVE}", "применение обновления", check=False)
            if code == EX_DEFERRED:
                deferred = True
                print(_c("33", "  Обновление ОТЛОЖЕНО: в боевом контуре есть открытые позиции. Архив ждёт на сервере, cron применит его, когда позиции закроются "
                               "(проверка: ./deploy/apply.sh --status)."))
            elif code != 0:
                fail(f"применение обновления: код возврата {code} (при провале запуска apply.sh уже вернул прежнюю версию, см. вывод выше)")
            _, contour_out = run(cli, f"cat {d}/.deploy_contour 2>/dev/null || echo shadow", "контур", check=False, quiet=True)
            contour = "live" if contour_out.strip() == "live" else "shadow"
            ok(f"обновление {'отложено' if deferred else 'применено'}, контур: {contour}")
        else:
            step(6, "Остановка старых контейнеров (данные сохраняются)")
            run(cli, f"cd {d} && if [ -f .env.shadow ]; then ./deploy/shadow.sh down; else echo первый-деплой-останавливать-нечего; fi", "остановка shadow")
            ok("остановлено")
            step(7, "Запуск профиля shadow (deploy/shadow.sh up)")
            run(cli, f"cd {d} && ./deploy/shadow.sh up", "запуск shadow")
            run(cli, f"cd {d} && chmod 600 .env.shadow && ./deploy/apply.sh --install-cron", "chmod .env.shadow, cron автоприменения")
            run(cli, f"cd {d} && ./deploy/shadow.sh analyzer-up", "запуск анализатора (пароль роли, SQL-роль, сборка)", check=False)  # сбой покажет шаг 9
            ok("postgres + shadow-run + api запущены, анализатор запрошен")

        if deferred:  # старый код ещё работает: схему БД и логи нового кода проверять рано
            ok("проверка нового кода отложена вместе с обновлением")
            print(_c("1;32", "\nДеплой принят (обновление ждёт закрытия позиций). Дальше на сервере: cd " + d + " && ./deploy/apply.sh --status"))
            return 0

        step(8, "PostgreSQL и миграции")
        run(cli, f"cd {d} && {DC} exec -T postgres pg_isready -U tradeai -d tradeai", "PostgreSQL не готов")
        applied = ""
        for _ in range(12):  # миграции применяет shadow-run при старте — даём до минуты
            code, applied = run(cli, f"cd {d} && {DC} exec -T postgres psql -U tradeai -d tradeai -tA "
                                     f"-c 'SELECT max(version) FROM schema_migrations'", "версия схемы", check=False, quiet=True)
            if code == 0 and applied.strip().isdigit():
                break
            time.sleep(5)
        if not applied.strip().isdigit():
            fail("таблица schema_migrations не появилась: миграции не применены (см. логи shadow-run)")
        if int(applied.strip()) != SCHEMA_VERSION:
            fail(f"версия схемы в БД {applied.strip()}, ожидалась {SCHEMA_VERSION}")
        ok(f"схема БД версии {SCHEMA_VERSION}")

        step(9, f"Глубокая проверка (ожидание {a.health_wait} с)")
        time.sleep(a.health_wait)
        required = LIVE_REQUIRED_SERVICES if contour == "live" else REQUIRED_SERVICES
        svc = "shadow-live" if contour == "live" else "shadow-run"
        _, ps = run(cli, f"cd {d} && {DC} --profile live --profile analytics ps -a --format '{{{{.Service}}}} {{{{.State}}}} {{{{.Health}}}}'", "docker compose ps", quiet=True)
        states, health = parse_services(ps), parse_health(ps)
        bad = {s: states.get(s, "нет") for s in required if states.get(s) != "running"}
        bad.update({s: "unhealthy (healthcheck не проходит)" for s in required if health.get(s) == "unhealthy"})
        blocked: list[str] = []
        errors: list[str] = []
        for s_ in required:  # логи ВСЕХ обязательных сервисов (а не только бота): ошибка анализатора/БД/API не должна проходить незамеченной
            _, logs = run(cli, f"cd {d} && {DC} --profile live --profile analytics logs --no-color --since {LOG_WINDOW} --tail 200 {s_}", f"логи {s_}", quiet=True)
            b, e = scan_logs(logs)
            blocked += b if s_ == svc else []  # блокировка IP Binance - только про бота
            errors += [f"[{s_}] {x}" for x in e]
        if blocked:
            print("\n".join("    " + x for x in blocked[:10]), file=sys.stderr)
            fail("VPS IP blocked by Binance (HTTP 451/429/418 или Handshake failed). Выберите VPS в другом регионе "
                 "и повторите деплой")
        if bad:
            print("\n".join(f"    {s}: {st}" for s, st in bad.items()), file=sys.stderr)
            for s_ in bad:  # причина падения: последние строки лога каждого неработающего сервиса (в т.ч. уже вышедшего)
                run(cli, f"cd {d} && {DC} --profile live --profile analytics logs --no-color --tail 40 {s_}", f"лог {s_}", check=False)
            fail("контейнеры не в состоянии running")
        if errors:
            print("\n".join("    " + x for x in errors[:10]), file=sys.stderr)
            fail("в логах сервисов за последние " + LOG_WINDOW + " есть ошибки (Traceback/CRITICAL/FATAL/ConfigError)")
        ok(f"{', '.join(required)} running, блокировки IP и ошибок в логах нет")
    except (TimeoutError, socket.timeout):
        fail("таймаут во время выполнения команды на сервере")
    except paramiko.SSHException as e:
        fail(f"ошибка SSH: {e}")
    except OSError as e:
        fail(f"сетевая ошибка/ошибка файла: {e}")
    finally:
        if cli is not None:
            cli.close()
        if archive is not None and not a.keep_archive:
            archive.unlink(missing_ok=True)
            archive.with_name(archive.name + ".sha256").unlink(missing_ok=True)
    print(_c("1;32", f"\nДеплой завершён. Дальше на сервере: cd {d} && ./deploy/shadow.sh status | logs | stats"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        fail("прервано пользователем")
