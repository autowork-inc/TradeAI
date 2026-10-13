"""Автодеплой Shadow Mode на VPS одной командой: python -m scripts.auto_deploy --host 1.2.3.4 [--user root] [--key PATH].

Параметры: аргументы CLI > окружение (VPS_HOST, VPS_PORT, VPS_USER, VPS_PASSWORD, VPS_SSH_KEY_PATH) > ввод в консоли.
Лучше ключ (`--key`) или `VPS_PASSWORD`: пароль в `--password` виден в истории оболочки.

Шаги: 1 архив (deploy_pack, цель shadow, без ключей) · 2 SSH + SFTP · 3 Docker · 4 распаковка · 5 права .env* ·
6 остановка старых контейнеров · 7 запуск (deploy/shadow.sh up) · 8 PostgreSQL и миграции · 9 глубокая проверка (контейнеры, логи,
блокировка IP Binance). Любая ошибка команды на сервере немедленно прерывает деплой (R1). Данные БД не удаляются:
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
REQUIRED_SERVICES = ("postgres", "shadow-run", "api")

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


def get_params(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default=os.environ.get("VPS_HOST"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("VPS_PORT", 22)))
    ap.add_argument("--user", default=os.environ.get("VPS_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("VPS_PASSWORD"), help="лучше VPS_PASSWORD или --key")
    ap.add_argument("--key", default=os.environ.get("VPS_SSH_KEY_PATH"), help="путь к приватному SSH-ключу")
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
        run(cli, f"mkdir -p {d} && tar -xzf {shlex.quote(ARCHIVE)} -C {d} && rm -f {shlex.quote(ARCHIVE)}", "распаковка архива")
        run(cli, f"cd {d} && chmod +x deploy/shadow.sh && ls docker-compose.yml Dockerfile deploy/shadow.sh", "проверка состава")
        ok("распаковано (.env.shadow с паролем БД архивом не затирается)")

        step(5, "Права на секреты")
        run(cli, f"cd {d} && for f in .env .env.shadow; do if [ -f $f ]; then chmod 600 $f; fi; done", "chmod 600")
        ok(".env / .env.shadow (если есть) — 600")

        step(6, "Остановка старых контейнеров (данные сохраняются)")
        run(cli, f"cd {d} && if [ -f .env.shadow ]; then ./deploy/shadow.sh down; else echo первый-деплой-останавливать-нечего; fi",
            "остановка shadow")
        ok("остановлено")

        step(7, "Запуск профиля shadow (deploy/shadow.sh up)")
        run(cli, f"cd {d} && ./deploy/shadow.sh up", "запуск shadow")
        run(cli, f"cd {d} && chmod 600 .env.shadow", "chmod .env.shadow")
        ok("postgres + shadow-run + api запущены")

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
        _, ps = run(cli, f"cd {d} && {DC} ps --format '{{{{.Service}}}} {{{{.State}}}}'", "docker compose ps", quiet=True)
        states = parse_services(ps)
        bad = {s: states.get(s, "нет") for s in REQUIRED_SERVICES if states.get(s) != "running"}
        _, logs = run(cli, f"cd {d} && {DC} logs --no-color --tail 200 shadow-run", "логи shadow-run", quiet=True)
        blocked, errors = scan_logs(logs)
        if blocked:
            print("\n".join("    " + x for x in blocked[:10]), file=sys.stderr)
            fail("VPS IP blocked by Binance (HTTP 451/429/418 или Handshake failed). Выберите VPS в другом регионе "
                 "и повторите деплой")
        if bad:
            print("\n".join(f"    {s}: {st}" for s, st in bad.items()), file=sys.stderr)
            fail("контейнеры не в состоянии running")
        if errors:
            print("\n".join("    " + x for x in errors[:10]), file=sys.stderr)
            fail("в последних 200 строках лога shadow-run есть ошибки (Traceback/CRITICAL/FATAL/ConfigError)")
        ok("postgres, shadow-run и api running, блокировки IP и ошибок в логах нет")
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
