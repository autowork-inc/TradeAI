"""Упаковка проекта для переноса на сервер: python -m scripts.deploy_pack [--target shadow|full] [--output trading_docker.tar.gz].

Кросс-платформенно (только stdlib). Что кладётся в архив определяется Dockerfile: каждая строка `COPY <путь> ...` - это
исходники, без которых образ не соберётся (раньше список был зашит здесь и разошёлся с Dockerfile: в архиве не оказалось
features/strategies/orchestrator). Плюс образ API мониторинга (api/Dockerfile и всё, что он копирует) и файлы запуска: docker-compose.yml, Dockerfile, .dockerignore, requirements.txt,
.env.shadow.example, deploy/shadow.sh, deploy/apply.sh (автоприменение обновления), README.md. Исключено всегда: .venv, __pycache__, .git, tests, data, logs, backups,
client (настольный клиент Этапа 6 работает на вашем ПК и на сервер не отправляется).

Цели:
  shadow (по умолчанию) - Shadow Mode (PostgreSQL + shadow-run) для VPS. Ключей API в архиве НЕТ и быть не должно (D5: Shadow
      использует только публичные данные); любой `.env`, `.env.shadow`, ключи/сертификаты в архиве - ошибка, архив не создаётся.
  full - как раньше (live-run): дополнительно кладётся `.env` с ключами API. Не выкладывайте такой архив нигде, кроме своего сервера.

После сборки рядом с архивом пишется <архив>.sha256 (проверка целостности после копирования: `sha256sum -c`).
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUN_FILES = ("docker-compose.yml", "Dockerfile", ".dockerignore", "requirements.txt", ".env.shadow.example", ".env.example",
             "README.md", "deploy/shadow.sh", "deploy/apply.sh", "deploy/init-ssl.sh", "deploy/caddy-tradeai.caddy", "web/nginx.conf", "web/nginx-ssl.conf", "web/nginx-locations.inc")
FULL_ONLY = (".env",)
EXCLUDED_PARTS = {"__pycache__", ".venv", ".git", "tests", ".pytest_cache", "data", "logs", "backups", "node_modules", "algo_live", "client", "mcp_server"}
EXTRA_DOCKERFILES = ("api/Dockerfile", "analyzer/Dockerfile", "web/Dockerfile")  # образы дополнительных сервисов; контекст сборки у всех - корень проекта
FORBIDDEN_NAMES = {".env", ".env.shadow"}  # в shadow-архиве недопустимы
FORBIDDEN_SUFFIXES = (".pem", ".key", ".p12", ".pfx")
EXECUTABLE = {"deploy/shadow.sh", "deploy/apply.sh", "deploy/init-ssl.sh"}
ARCHIVE_DEFAULT = "trading_docker.tar.gz"


def dockerfile_sources(dockerfile: Path = ROOT / "Dockerfile") -> list[str]:
    """Пути из `COPY <src> <dst>` (кроме COPY --from): то, что образу нужно из контекста сборки."""
    sources: list[str] = []
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*COPY\s+(?!--from)(.+?)\s+\S+\s*$", line)
        if m:
            sources.extend(m.group(1).split())
    return sources


def _walk(path: Path, root: Path) -> list[Path]:
    if path.is_file():
        return [path]
    # исключения считаются от самого пути-источника: `client/tradeai_client` для веб-образа попадает в архив, хотя каталог `client` целиком (venv, dist, build) - нет
    base = path.parent if path.parent != root else root
    return [p for p in sorted(path.rglob("*")) if p.is_file() and not (EXCLUDED_PARTS & set(p.relative_to(base).parts)) and p.suffix != ".pyc"]


def collect(target: str = "shadow", root: Path = ROOT) -> list[Path]:
    """Файлы архива. Нет файла, который требует Dockerfile или запуск, - выход с понятной ошибкой."""
    extra = [d for d in EXTRA_DOCKERFILES if (root / d).exists()]
    wanted = list(dict.fromkeys([*dockerfile_sources(root / "Dockerfile"), *(src for d in extra for src in dockerfile_sources(root / d)),
                                 *extra, *RUN_FILES, *(FULL_ONLY if target == "full" else ())]))
    optional = {".env.example", ".env.shadow.example", "README.md", "web/nginx.conf"}
    missing = [w for w in wanted if not (root / w).exists() and w not in optional]
    if missing:
        sys.exit(f"Нет обязательных файлов/каталогов: {', '.join(missing)}")
    files: list[Path] = []
    for w in wanted:
        p = root / w
        if p.exists():
            files.extend(_walk(p, root))
    files = list(dict.fromkeys(files))
    if target == "shadow":
        bad = [str(p.relative_to(root)) for p in files if p.name in FORBIDDEN_NAMES or p.suffix in FORBIDDEN_SUFFIXES]
        if bad:
            sys.exit(f"В shadow-архив попали файлы с секретами: {', '.join(bad)} - архив не создан")
    return files


def _content(p: Path, rel: str) -> bytes:
    data = p.read_bytes()
    if rel.endswith(".sh"):
        data = data.replace(b"\r\n", b"\n")  # скрипт, написанный на Windows, на Linux с CRLF не запустится (/bin/sh^M)
    return data


def build_archive(out: Path, target: str = "full", root: Path = ROOT) -> Path:
    """Собирает архив по пути `out`, возвращает его путь. target по умолчанию full - прежнее поведение для scripts/auto_deploy.py."""
    out = out.resolve()
    files = collect(target, root)
    import io
    with tarfile.open(out, "w:gz") as tar:
        for p in files:
            rel = p.relative_to(root).as_posix()
            data = _content(p, rel)
            info = tarfile.TarInfo(rel)
            info.size = len(data)
            info.mtime = int(p.stat().st_mtime)
            info.mode = 0o755 if rel in EXECUTABLE else (0o600 if p.name == ".env" else 0o644)
            tar.addfile(info, io.BytesIO(data))
    digest = hashlib.sha256(out.read_bytes()).hexdigest()
    out.with_name(out.name + ".sha256").write_text(f"{digest}  {out.name}\n", encoding="utf-8")
    print(f"{out}: {len(files)} файлов, {out.stat().st_size / 1024:.0f} КиБ, цель {target}, sha256 {digest[:16]}…")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--target", choices=["shadow", "full"], default="shadow")
    ap.add_argument("--output", type=Path, default=None, help="по умолчанию trading_docker.tar.gz (full) / trading_shadow.tar.gz (shadow)")
    a = ap.parse_args()
    out = a.output or ROOT / ("trading_shadow.tar.gz" if a.target == "shadow" else ARCHIVE_DEFAULT)
    archive = build_archive(out, a.target)
    if a.target == "shadow":
        print(f"""
Дальше (замените user@server и каталог на свои; пароли никуда передавать не нужно):
  scp {archive.name} {archive.name}.sha256 user@server:~/
  ssh user@server
    sha256sum -c {archive.name}.sha256
    mkdir -p ~/trading && tar xzf {archive.name} -C ~/trading && cd ~/trading
    chmod +x deploy/shadow.sh && ./deploy/shadow.sh up        # то же, что: docker compose --env-file .env.shadow up -d --build postgres shadow-run
    ./deploy/shadow.sh status                                 # затем logs | stats | backup
    ./deploy/shadow.sh apikey                                 # ключ API для клиента Этапа 6 (client/), SSH-туннель - в README клиента""")
    else:
        print("Содержит .env с ключами. На сервере: tar xzf trading_docker.tar.gz -C ~/trading && cd ~/trading")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
