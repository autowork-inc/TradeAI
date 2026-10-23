"""HTTPS для веб-версии и API (2026-10-23): конфиги nginx, certbot в compose, deploy/init-ssl.sh, блок Caddy, состав архива деплоя."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts import deploy_pack

ROOT = Path(__file__).resolve().parents[1]
DOMAIN = "193.233.75.254.nip.io"


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_port_80_serves_only_acme_and_a_301_redirect_to_https():
    conf = read("web/nginx.conf")
    block = conf.split("listen 8081;", 1)[1].split("\n}\n", 1)[0]
    assert "location /.well-known/acme-challenge/" in block and "root /var/www/certbot;" in block
    assert f"return 301 https://{DOMAIN}$request_uri;" in block
    assert block.count("location ") == 2  # ничего кроме acme-challenge и редиректа
    assert "proxy_pass" not in block


def test_https_server_terminates_tls_and_proxies_web_api_and_websockets():
    ssl = read("web/nginx-ssl.conf")
    assert "listen 8443 ssl;" in ssl and f"server_name {DOMAIN};" in ssl
    assert f"/etc/letsencrypt/live/{DOMAIN}/fullchain.pem" in ssl and f"/etc/letsencrypt/live/{DOMAIN}/privkey.pem" in ssl
    assert "TLSv1.2 TLSv1.3" in ssl and "Strict-Transport-Security" in ssl
    api = ssl.split("location /api/", 1)[1].split("location /ws/live_state", 1)[0]
    assert "http://api:8000" in api
    ws = ssl.split("location /ws/live_state", 1)[1].split("include ", 1)[0]
    assert "http://api:8000" in ws and "proxy_set_header Upgrade $http_upgrade;" in ws and "proxy_set_header Connection $connection_upgrade;" in ws
    assert "include /etc/nginx/snippets/locations.inc;" in ssl
    loc = read("web/nginx-locations.inc")
    assert "http://web:8550" in loc and "proxy_set_header Upgrade $http_upgrade;" in loc and "proxy_set_header Connection $connection_upgrade;" in loc
    assert "X-Forwarded-Proto $scheme" in loc  # за TLS приложение видит https
    assert "map $http_upgrade $connection_upgrade" in read("web/nginx.conf")


def test_nginx_starts_without_a_certificate_the_ssl_block_is_an_optional_include():
    conf = read("web/nginx.conf")
    assert "include /etc/nginx/ssl.d/*.conf;" in conf and "ssl_certificate" not in conf  # нет сертификата - HTTPS-блока нет, а деплой веба не ломается


def test_compose_has_certbot_with_shared_webroot_and_certificate_volumes_and_safe_default_ports():
    compose = read("docker-compose.yml").replace("\r\n", "\n")
    certbot = compose.split("\n  certbot:", 1)[1].split("\nvolumes:", 1)[0]
    assert "certbot/certbot:v" in certbot and 'profiles: ["ssl"]' in certbot and "certbot renew --webroot -w /var/www/certbot" in certbot
    assert "./data/certbot/conf:/etc/letsencrypt" in certbot and "./data/certbot/www:/var/www/certbot" in certbot
    nginx = compose.split("\n  nginx:", 1)[1].split("\n  certbot:", 1)[0]
    assert "./data/certbot/conf:/etc/letsencrypt:ro" in nginx and "./data/certbot/www:/var/www/certbot:ro" in nginx and "./data/certbot/ssl.d:/etc/nginx/ssl.d:ro" in nginx
    assert ":${WEB_HTTP_PORT:-8081}:8081" in nginx and ":${WEB_HTTPS_PORT:-8443}:8443" in nginx  # 80/443 по умолчанию не занимаются (на VPS они у Caddy)
    assert "${WEB_HTTP_BIND:-127.0.0.1}" in nginx and "${WEB_HTTPS_BIND:-127.0.0.1}" in nginx
    assert "nginx -s reload" in nginx  # продлённый сертификат подхватывается


def test_init_ssl_script_is_lf_valid_bash_and_covers_both_modes():
    raw = (ROOT / "deploy/init-ssl.sh").read_bytes()
    assert b"\r\n" not in raw and raw.startswith(b"#!/usr/bin/env bash")
    text = raw.decode("utf-8")
    for needle in ("certonly --webroot", "--staging", "--keep-until-expiring", "--deploy-hook", "port_owner", "caddy validate", "WEB_HTTP_PORT 80", "WEB_HTTPS_PORT 443", "tradeai.conf",
                   "--apply-caddy", "--close-plain", "nginx -t"):
        assert needle in text, needle
    assert "--standalone" not in text  # порт 80 занят нашим nginx: standalone конфликтовал бы
    bash = shutil.which("bash")
    if bash:
        done = subprocess.run([bash, "-n", str(ROOT / "deploy/init-ssl.sh")], capture_output=True, text=True)
        assert done.returncode == 0, done.stderr


def test_caddy_block_routes_api_and_websocket_to_8001_and_everything_else_to_nginx_8080():
    block = read("deploy/caddy-tradeai.caddy")
    assert block.splitlines()[2].startswith(DOMAIN + " {")
    assert "@api path /api/* /ws/live_state" in block and "reverse_proxy 127.0.0.1:8001" in block and "reverse_proxy 127.0.0.1:8080" in block


def test_deploy_archive_carries_ssl_files_and_no_keys():
    files = {p.relative_to(ROOT).as_posix() for p in deploy_pack.collect("shadow", ROOT)}
    assert {"deploy/init-ssl.sh", "deploy/caddy-tradeai.caddy", "web/nginx.conf", "web/nginx-ssl.conf", "web/nginx-locations.inc"} <= files
    assert "deploy/init-ssl.sh" in deploy_pack.EXECUTABLE
    assert not any(f.endswith((".pem", ".key")) for f in files) and not any("certbot" in f for f in files)  # сертификаты и ключи создаются на сервере и в архив не попадают


def test_no_cors_middleware_is_needed():
    """Десктопный клиент - не браузер (CORS не применяется), веб-версия ходит к API с сервера внутри docker-сети: CORS на бэкенде не нужен и не включён."""
    assert not re.search(r"CORSMiddleware|allow_origins", "".join(p.read_text(encoding="utf-8") for p in (ROOT / "api").glob("*.py")))
