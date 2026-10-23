"""Упаковка и инфраструктура Shadow Mode (Этап 5): архив для VPS, compose, серверный скрипт, настройки монет."""

import hashlib
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

import config
from scripts import deploy_pack
from utils.errors import ConfigError

ROOT = Path(__file__).resolve().parent.parent
DOCKER = shutil.which("docker")
SH = shutil.which("sh")


def mini_project(tmp_path: Path, **extra: str) -> Path:
    """Крошечный проект с Dockerfile: проверяем правила упаковки, не завися от содержимого настоящего репозитория."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "Dockerfile").write_text("FROM python:3.12-slim\nCOPY config.py ./\nCOPY app ./app\nCOPY out ./out\nCMD [\"true\"]\n", encoding="utf-8")
    (tmp_path / "config.py").write_text("X = 1\n", encoding="utf-8")
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "main.py").write_text("print(1)\n", encoding="utf-8")
    (tmp_path / "app" / "__pycache__").mkdir()
    (tmp_path / "app" / "__pycache__" / "main.cpython-312.pyc").write_bytes(b"x")
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "model.json").write_text("{}", encoding="utf-8")
    for name in ("docker-compose.yml", "requirements.txt", ".dockerignore"):
        (tmp_path / name).write_text("x\n", encoding="utf-8")
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "shadow.sh").write_bytes(b"#!/bin/sh\r\necho hi\r\n")  # CRLF, как после правки на Windows
    (tmp_path / "deploy" / "apply.sh").write_bytes(b"#!/bin/sh\r\necho apply\r\n")
    (tmp_path / "deploy" / "init-ssl.sh").write_bytes(b"#!/bin/sh\r\necho ssl\r\n")
    (tmp_path / "deploy" / "caddy-tradeai.caddy").write_text("x\n", encoding="utf-8")
    (tmp_path / "web").mkdir()
    for name in ("nginx-ssl.conf", "nginx-locations.inc"):
        (tmp_path / "web" / name).write_text("x\n", encoding="utf-8")
    for name, text in extra.items():
        p = tmp_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    return tmp_path


def members(archive: Path) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(archive) as t:
        return {m.name: m for m in t.getmembers()}


# ================================================================================== архив


def test_archive_sources_follow_the_dockerfile_not_a_hardcoded_list(tmp_path):
    root = mini_project(tmp_path)
    assert deploy_pack.dockerfile_sources(root / "Dockerfile") == ["config.py", "app", "out"]
    archive = deploy_pack.build_archive(tmp_path / "a.tar.gz", "shadow", root)
    names = set(members(archive))
    assert {"config.py", "app/main.py", "out/model.json", "Dockerfile", "docker-compose.yml", "deploy/shadow.sh"} <= names
    assert not any("__pycache__" in n or n.endswith(".pyc") for n in names)


def test_real_repo_archive_contains_everything_the_image_needs_and_no_secrets(tmp_path):
    archive = deploy_pack.build_archive(tmp_path / "shadow.tar.gz", "shadow")
    names = set(members(archive))
    for d in deploy_pack.dockerfile_sources():
        assert any(n == d or n.startswith(d + "/") for n in names), f"в архиве нет {d}, а Dockerfile его копирует"
    assert {"persistence/schema.py", "persistence/pg.py", "orchestrator/tracker.py", "features/smc.py", "strategies/smc_setups.py",
            "docker-compose.yml", "deploy/shadow.sh", ".env.shadow.example", "requirements.txt"} <= names
    assert not {".env", ".env.shadow"} & names and not any(n.endswith((".pem", ".key")) for n in names)
    assert not any(n.startswith(("tests/", "data/", "logs/", "out/algo_live/", ".venv/")) for n in names)
    # Этап 6: образ API едет на сервер (api/Dockerfile и всё, что он копирует), настольный клиент - нет
    assert {"api/Dockerfile", "api/requirements.txt", "api/main.py", "api/security.py", "api/livestate.py"} <= names
    # настольный клиент на сервер не едет; в архиве только то, что нужно веб-образу (client/tradeai_client и client/web_app.py), без venv/dist/build
    assert not any(n.startswith("client/") and not n.startswith(("client/tradeai_client/", "client/web_app.py")) for n in names)
    assert {"web/Dockerfile", "web/requirements.txt", "web/nginx.conf", "client/web_app.py", "client/tradeai_client/web/app.py"} <= names
    assert not any(".venv" in n or n.startswith(("client/dist", "client/build")) for n in names)


def test_client_directory_is_never_packed_even_if_it_sits_inside_a_copied_directory(tmp_path):
    root = mini_project(tmp_path, **{"app/client/ui.py": "x = 1", "client/main.py": "x = 2"})
    names = set(members(deploy_pack.build_archive(tmp_path / "a.tar.gz", "shadow", root)))
    assert "app/main.py" in names and not any(n.startswith("client/") or "/client/" in n for n in names)


def test_extra_dockerfile_sources_are_packed(tmp_path):
    dockerfile = "\n".join(["FROM python:3.12-slim", "COPY api/requirements.txt ./requirements.txt", "COPY api ./api", ""])
    root = mini_project(tmp_path, **{"api/Dockerfile": dockerfile, "api/requirements.txt": "fastapi", "api/main.py": "x = 1"})
    names = set(members(deploy_pack.build_archive(tmp_path / "a.tar.gz", "shadow", root)))
    assert {"api/Dockerfile", "api/requirements.txt", "api/main.py"} <= names


def test_shell_script_is_executable_with_lf_endings_and_checksum_matches(tmp_path):
    root = mini_project(tmp_path)
    archive = deploy_pack.build_archive(tmp_path / "a.tar.gz", "shadow", root)
    m = members(archive)["deploy/shadow.sh"]
    assert m.mode == 0o755
    with tarfile.open(archive) as t:
        assert t.extractfile("deploy/shadow.sh").read() == b"#!/bin/sh\necho hi\n"  # CRLF -> LF: иначе на Linux «/bin/sh^M: not found»
    line = (tmp_path / "a.tar.gz.sha256").read_text(encoding="utf-8").split()
    assert line == [hashlib.sha256(archive.read_bytes()).hexdigest(), "a.tar.gz"]


def test_shadow_archive_refuses_secrets_even_when_they_sneak_in_through_copied_directories(tmp_path):
    root = mini_project(tmp_path, **{"app/.env": "BINANCE_API_SECRET=x\n"})
    with pytest.raises(SystemExit, match="секрет"):
        deploy_pack.build_archive(tmp_path / "a.tar.gz", "shadow", root)
    assert not (tmp_path / "a.tar.gz").exists()
    root2 = mini_project(tmp_path / "p2", **{"app/server.pem": "-----BEGIN-----\n"})
    with pytest.raises(SystemExit, match="секрет"):
        deploy_pack.build_archive(tmp_path / "b.tar.gz", "shadow", root2)


def test_full_target_adds_env_with_private_mode_and_missing_required_file_is_an_error(tmp_path):
    root = mini_project(tmp_path, **{".env": "BINANCE_API_KEY=k\n"})
    m = members(deploy_pack.build_archive(tmp_path / "f.tar.gz", "full", root))
    assert ".env" in m and m[".env"].mode == 0o600
    assert ".env" not in members(deploy_pack.build_archive(tmp_path / "s.tar.gz", "shadow", root))  # shadow: ключей нет
    (root / "app" / "main.py").unlink()
    shutil.rmtree(root / "app")
    with pytest.raises(SystemExit, match="Нет обязательных"):
        deploy_pack.build_archive(tmp_path / "x.tar.gz", "shadow", root)


# ================================================================================== compose и скрипт


def service_block(text: str, name: str) -> str:
    start = text.index(f"\n  {name}:\n") + 1
    rest = text[start + 1:]
    import re
    nxt = re.search(r"\n  (?:#.*\n  )*[a-z-]+:\n|\nvolumes:", rest)
    return text[start: start + 1 + nxt.start()] if nxt else text[start:]


def test_postgres_service_is_private_persistent_and_healthchecked():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    pg = service_block(text, "postgres")
    assert "image: postgres:16" in pg and 'profiles: ["shadow", "live"]' in pg
    assert "ports:" not in pg and "network_mode" not in pg  # порт БД наружу не публикуется
    assert "pgdata:/var/lib/postgresql/data" in pg and "pg_isready" in pg and "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD" in pg
    assert "\nvolumes:\n  pgdata:" in text
    assert "PASSWORD: tradeai" not in text and "password" not in pg.lower().replace("postgres_password", "")  # пароля в файле нет


def test_shadow_run_uses_the_database_without_getting_api_keys():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    sh = service_block(text, "shadow-run")
    assert "condition: service_healthy" in sh and "DATABASE_URL: postgresql://tradeai:${POSTGRES_PASSWORD" in sh and "@postgres:5432/tradeai" in sh
    assert "env_file" not in sh and "BINANCE_API" not in sh and "read_only: true" in sh  # изоляция D5 сохранена
    live = service_block(text, "live-run")
    assert "profiles" not in live and "postgres" not in live.lower() and "DATABASE_URL" not in live  # боевой ML-контур не затронут


@pytest.mark.skipif(DOCKER is None, reason="нужен docker")
def test_compose_file_is_valid_for_both_stacks():
    env = {**os.environ, "POSTGRES_PASSWORD": "x" * 32, "API_KEY": "k" * 40}
    shadow = subprocess.run([DOCKER, "compose", "--profile", "shadow", "config", "--services"], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", env=env)
    plain = subprocess.run([DOCKER, "compose", "config", "--services"], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", env={**env, "POSTGRES_PASSWORD": ""})
    assert shadow.returncode == 0, shadow.stderr
    assert set(shadow.stdout.split()) == {"live-run", "shadow-run", "postgres", "api"}  # collectors/jobs - свои профили
    assert plain.returncode == 0 and plain.stdout.split() == ["live-run"]  # обычный `up` по-прежнему стартует только боевой контур


@pytest.mark.skipif(SH is None, reason="нужен sh")
def test_server_script_has_valid_syntax_and_no_windows_line_endings():
    path = ROOT / "deploy" / "shadow.sh"
    assert b"\r" not in path.read_bytes()
    assert subprocess.run([SH, "-n", str(path)], capture_output=True).returncode == 0
    usage = subprocess.run([SH, str(path), "bogus"], capture_output=True, text=True, encoding="utf-8", cwd=ROOT)
    assert usage.returncode == 2 and "Использование" in usage.stderr


def test_api_service_is_read_only_published_on_8001_and_keyed():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    api = service_block(text, "api")
    assert "dockerfile: api/Dockerfile" in api and 'profiles: ["shadow", "live"]' in api and "condition: service_healthy" in api
    # прямой доступ по IP сервера (решение пользователя 2026-10-11); наружу 8001, т.к. 8000 на сервере занят; внутри контейнера 8000
    assert '"${API_BIND:-0.0.0.0}:${API_PORT:-8001}:8000"' in api
    assert "API_KEY: ${API_KEY" in api and "DATABASE_URL: postgresql://tradeai:${POSTGRES_PASSWORD" in api
    assert "read_only: true" in api and "cap_drop" in api and "no-new-privileges" in api and "/app/data/shadow:ro" in api
    assert "env_file" not in api and "BINANCE_API" not in api  # ключей биржи у API нет
    assert "ports:" not in service_block(text, "postgres") and "ports:" not in service_block(text, "shadow-run")


def test_api_image_is_separate_and_light():
    df = (ROOT / "api" / "Dockerfile").read_text(encoding="utf-8")
    assert "USER app" in df and "COPY api ./api" in df and "COPY config.py" not in df and "COPY execution" not in df
    reqs = (ROOT / "api" / "requirements.txt").read_text(encoding="utf-8")
    assert "fastapi" in reqs and "uvicorn" in reqs and "asyncpg" in reqs and "polars" not in reqs and "flet" not in reqs


def test_shell_script_creates_and_shows_the_api_key():
    text = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    assert "ensure_api_key" in text and "apikey)" in text and "api" in text.split('SERVICES="', 1)[1].split('"', 1)[0].split()
    assert "API_KEY=" in (ROOT / ".env.shadow.example").read_text(encoding="utf-8")


def test_server_script_never_publishes_secrets_or_drops_data():
    text = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    assert "down -v" not in text and "--volumes" not in text and "docker volume rm" not in text and "rm -rf" not in text
    assert "chmod 600" in text and "umask 077" in text and "live-run" not in text.replace("не трогает live-run", "")


def test_dockerfile_and_requirements_cover_persistence():
    assert "COPY persistence ./persistence" in (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "asyncpg" in (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert ".env.shadow" in (ROOT / ".dockerignore").read_text(encoding="utf-8")


# =============================================================================================== монеты


def test_shadow_symbols_are_the_combat_pool_without_touching_the_ml_bot_symbols():
    assert config.SYMBOLS == ("BTCUSDT", "ETHUSDT")  # сборщики стакана и live_run читают SYMBOLS: пул ML-контура не меняется
    # 2026-10-11: боевой пул вместо тестового; BTC/ETH остаются только у ML-контура (SYMBOLS)
    # 2026-10-18: статичная основа - закреплённые BTC/ETH и MT5; остальные Binance-монеты определяет автоселектор по объёму (tests/orchestrator/test_universe.py)
    assert config.SHADOW_SYMBOLS == ("BTCUSDT", "ETHUSDT", "EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "XAGUSD")
    config.validate()


def test_validate_rejects_duplicate_or_lowercase_shadow_symbols(monkeypatch):
    monkeypatch.setattr(config, "SHADOW_SYMBOLS", ("BTCUSDT", "BTCUSDT"))
    with pytest.raises(ConfigError, match="SHADOW_SYMBOLS"):
        config.validate()
    monkeypatch.setattr(config, "SHADOW_SYMBOLS", ("btcusdt",))
    with pytest.raises(ConfigError, match="SHADOW_SYMBOLS"):
        config.validate()


def test_shadow_live_is_isolated_fixed_size_and_does_not_touch_shadow_run():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    live, shadow = service_block(text, "shadow-live"), service_block(text, "shadow-run")
    assert 'profiles: ["live"]' in live and "TRADE_MODE: live" in live and "env_file" not in live and "read_only: true" in live
    # свои каталоги состояния, логов и пульса - не пересекаются с shadow-run
    assert "TRADING_DATA_DIR: /app/data/live" in live and "TRADING_LOG_DIR: /app/logs/live" in live
    assert "./data/live:/app/data/live" in live and "./logs/live:/app/logs/live" in live and "/app/data/live/.shadow_heartbeat" in live
    assert "data/shadow" not in live and "logs/shadow" not in live
    # фиксированный размер и лимиты
    for needle in ("LIVE_MARGIN_USDT: ${LIVE_MARGIN_USDT:-10}", "LIVE_LEVERAGE: ${LIVE_LEVERAGE:-10}",
                   "LIVE_DAILY_LOSS_LIMIT_USDT: ${LIVE_DAILY_LOSS_LIMIT_USDT:-50}", "LIVE_MAX_OPEN_POSITIONS: ${LIVE_MAX_OPEN_POSITIONS:-4}",
                   "LIVE_ALLOW_MAINNET: ${LIVE_ALLOW_MAINNET:-false}"):
        assert needle in live, needle
    # shadow-run остался прежним: ни ключей биржи, ни live
    assert "BINANCE_API" not in shadow and "TRADE_MODE" not in shadow and "/app/data/shadow/.shadow_heartbeat" in shadow
    api = service_block(text, "api")
    assert 'profiles: ["shadow", "live"]' in api and "./data/live:/app/data/live:ro" in api and "LIVE_DATA_DIR: /app/data/live" in api
    assert "BINANCE_API" not in api


def test_shadow_script_has_up_live_with_guards():
    sh = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    assert "up-live)" in sh and "LIVE_ALLOW_MAINNET=true" in sh and "stop shadow-run" in sh and "BINANCE_API_SECRET" in sh
    assert "env_file" not in sh


def test_apply_script_ships_in_the_archive_and_keeps_contour_without_interactive_prompts():
    assert "deploy/apply.sh" in deploy_pack.RUN_FILES and "deploy/apply.sh" in deploy_pack.EXECUTABLE
    text = (ROOT / "deploy" / "apply.sh").read_text(encoding="utf-8")
    assert "up-live --yes" in text and "./deploy/shadow.sh up" in text  # контур сохраняется, ввод REAL не нужен
    assert "live_has_positions" in text and "EX_TEMPFAIL" in text and "prev.tar.gz" in text  # отсрочка при позициях и откат
    assert "--install-cron" in text and "APPLY_MAX_DEFER_H" in text
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8")
    assert "scripts.deploy_pack --target shadow" in workflow and "StrictHostKeyChecking=yes" in workflow and "needs: test" in workflow
    assert "BINANCE_API" not in workflow and "CTRADER" not in workflow  # ключи биржи в CI не попадают


# ================================================================================== Strategy Quant Analyzer (изоляция от бота)


def test_analyzer_image_ships_to_the_server_without_tests_or_venv(tmp_path):
    archive = deploy_pack.build_archive(tmp_path / "shadow.tar.gz", "shadow")
    names = set(members(archive))
    assert {"analyzer/Dockerfile", "analyzer/requirements.txt", "analyzer/main.py", "analyzer/app.py", "analyzer/current_params.json", "analyzer/sql/001_analyzer_ro.sql",
            "api/analysis_proxy.py"} <= names
    assert not any(n.startswith(("analyzer/tests/", "analyzer/.venv/")) for n in names)


def test_analyzer_is_a_separate_internal_service_that_cannot_break_the_bot_deploy():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    block = service_block(text, "analyzer")
    assert "dockerfile: analyzer/Dockerfile" in block and 'profiles: ["analytics"]' in block  # не входит в up / up-live / автодеплой (apply.sh)
    assert "ports:" not in block and "BINANCE_API" not in block and "env_file" not in block  # наружу не смотрит, ключей биржи нет
    assert "postgresql://analyzer_ro@postgres" in block and "ANALYZER_DB_PASSWORD: ${ANALYZER_DB_PASSWORD" in block and "postgresql://tradeai" not in block  # под ролью только на чтение, не под учёткой бота
    assert "read_only: true" in block and "cap_drop" in block and "no-new-privileges" in block and "mem_limit: 1g" in block and "cpus: 1.0" in block
    assert "analyzer_data:/data" in block and "analyzer_data:" in text.split("volumes:")[-1]
    api = service_block(text, "api")
    assert "ANALYZER_URL: http://analyzer:8000" in api and "ANALYZER_TOKEN: ${API_KEY" in api
    for bot in ("shadow-run", "shadow-live"):
        assert "analyzer" not in service_block(text, bot)
    df = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "analyzer" not in df  # образ бота о нём не знает


def test_analyzer_dockerfile_is_light_and_unprivileged():
    df = (ROOT / "analyzer" / "Dockerfile").read_text(encoding="utf-8")
    assert "USER app" in df and "COPY analyzer ./analyzer" in df and "COPY config.py" not in df and "COPY orchestrator" not in df and "COPY exchange" not in df
    reqs = (ROOT / "analyzer" / "requirements.txt").read_text(encoding="utf-8")
    assert "pandas" in reqs and "fastapi" in reqs and "psycopg" in reqs and "polars" not in reqs and "flet" not in reqs


@pytest.mark.skipif(SH is None, reason="нужен sh")
def test_shadow_script_has_analyzer_commands_and_apply_runs_it_after_a_successful_bot_deploy():
    text = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    for cmd in ("analyzer-init)", "analyzer-up)", "analyzer-down)"):
        assert cmd in text
    apply = (ROOT / "deploy" / "apply.sh").read_text(encoding="utf-8")
    assert "./deploy/shadow.sh analyzer-up" in apply and "APPLY_ANALYZER" in apply
    ok_at, call_at = apply.index('ГОТОВО: версия'), apply.index("        deploy_analyzer")
    assert ok_at < call_at < apply.index("ПРОВАЛ применения")  # только после успешного применения и проверки здоровья бота, не в ветке отката
    fn = apply[apply.index("deploy_analyzer() {"):apply.index("unpack() {")]
    assert "return 0" in fn and "exit" not in fn and "die " not in fn  # сбой анализатора не откатывает и не валит выкатку бота
    ad = (ROOT / "scripts" / "auto_deploy.py").read_text(encoding="utf-8")
    assert '"analyzer", "web", "nginx")' in ad and ad.count('"analyzer"') >= 2 and "--profile analytics --profile web ps -a" in ad  # шаг 9 проверяет анализатор и веб
    assert 'env_append "ANALYZER_DB_PASSWORD=$pw"' in text


def test_web_client_is_deployed_isolated_from_the_bot_and_checked_by_the_pipeline():
    """Веб-версия (2026-10-20): отдельный профиль compose, запуск ПОСЛЕ успешного применения бота, сбой не откатывает бота; вход только из .env.shadow; шаг 9 её проверяет."""
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    web = compose[compose.index("\n  web:"):compose.index("\nvolumes:")]
    assert 'profiles: ["web"]' in web and "read_only: true" in web and "cap_drop:" in web and "WEB_PASSWORD: ${WEB_PASSWORD:-}" in web
    assert "nginxinc/nginx-unprivileged" in web and "${WEB_PUBLIC_PORT:-8080}:8080" in web and "./web/nginx.conf:/etc/nginx/conf.d/default.conf:ro" in web
    assert "BINANCE" not in web and "DATABASE_URL" not in web  # ни ключей биржи, ни доступа к БД
    shadow = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    assert "web-up)" in shadow and "web-down)" in shadow and "--profile web up -d --build" in shadow and "не задан $k" in shadow
    apply = (ROOT / "deploy" / "apply.sh").read_text(encoding="utf-8")
    fn = apply[apply.index("deploy_web() {"):apply.index("unpack() {")]
    assert "return 0" in fn and "exit" not in fn and "die " not in fn and "APPLY_WEB" in fn
    assert apply.index('ГОТОВО: версия') < apply.index("        deploy_web") < apply.index("ПРОВАЛ применения")
    nginx = (ROOT / "web" / "nginx.conf").read_text(encoding="utf-8") + (ROOT / "web" / "nginx-locations.inc").read_text(encoding="utf-8")  # маршруты - в общем include (HTTP и HTTPS)
    assert "$connection_upgrade" in nginx and "proxy_set_header Upgrade" in nginx and "web:8550" in nginx  # WebSocket проксируется
    assert "location /chart/" in nginx and "web:8551" in nginx  # графики сделок: aiohttp-сервер внутри контейнера web
    ad = (ROOT / "scripts" / "auto_deploy.py").read_text(encoding="utf-8")
    assert "shadow.sh web-up" in ad and "--web-login" in ad and "--web-password" in ad
