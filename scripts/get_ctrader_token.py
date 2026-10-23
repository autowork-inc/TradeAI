"""Получение Access Token для cTrader Open API (OAuth 2.0): python -m scripts.get_ctrader_token

Что делает (документация «App and account authentication»):
  1. Печатает ссылку авторизации https://id.ctrader.com/my/settings/openapi/grantingaccess/?client_id=...&redirect_uri=...&scope=trading&product=web и открывает её
     в браузере. Вы входите под своим cTID и разрешаете приложению доступ к счетам.
  2. Браузер возвращается на redirect_uri с одноразовым `?code=...` (живёт ~1 минуту). Скрипт поднимает на этот адрес временный локальный сервер и ловит код сам;
     если redirect_uri чужой (не localhost) - вставьте в консоль либо весь адрес, на который вас перебросило, либо только значение code.
  3. Меняет код на пару токенов запросом GET https://openapi.ctrader.com/apps/token?grant_type=authorization_code... и печатает строки для .env.shadow:
     CTRADER_ACCESS_TOKEN (живёт ~30 дней) и CTRADER_REFRESH_TOKEN (бессрочный: по нему бот сам обновляет токен).
  4. Проверяет токен у сервера (ProtoOAGetAccountListByAccessTokenReq) и показывает счета, доступные токену: среди них должен быть ваш CTRADER_ACCOUNT_ID.

Один раз в портале https://openapi.ctrader.com/apps -> ваше приложение -> Edit -> Redirect URIs добавьте адрес (по умолчанию http://localhost:5555/callback).

Ключи приложения берутся из окружения CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET (или --client-id; секрет, если не задан, спрашивается скрытым вводом). Токены печатаются в консоль
и, только с --save ФАЙЛ, пишутся в файл (положите его вне репозитория). В git, логи и сервер скрипт ничего не отправляет: значения в .env.shadow на сервере вписываете вы.
"""

import argparse
import asyncio
import getpass
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from exchange.ctrader import CTraderError
from utils.errors import ConfigError

AUTH_URL = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
TOKEN_URL = "https://openapi.ctrader.com/apps/token"
DEFAULT_REDIRECT = "http://localhost:5555/callback"
SCOPES = ("trading", "accounts")


class TokenError(Exception):
    """Не удалось получить токен (сервер ответил ошибкой, код не получен, неверные параметры)."""


def build_auth_url(client_id: str, redirect_uri: str, scope: str = "trading") -> str:
    if scope not in SCOPES:
        raise TokenError(f"scope: {' | '.join(SCOPES)} (trading - ордера, accounts - только чтение), получено {scope!r}")
    if not client_id:
        raise TokenError("не задан Client ID")
    query = urllib.parse.urlencode({"client_id": client_id, "redirect_uri": redirect_uri, "scope": scope, "product": "web"})
    return f"{AUTH_URL}?{query}"


def extract_code(text: str) -> str:
    """Из вставленного пользователем текста - код авторизации: либо `?code=...` в адресе, либо сам код. Пусто/ошибка в адресе (`error=`) - TokenError."""
    text = text.strip()
    if not text:
        raise TokenError("пустой ввод")
    if "://" in text or text.startswith("?") or "code=" in text:
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(text if "://" in text else "http://x/?" + text.lstrip("?")).query)
        if "error" in query:
            raise TokenError(f"авторизация отклонена: {query['error'][0]} {query.get('error_description', [''])[0]}".strip())
        if "code" not in query:
            raise TokenError("в адресе нет параметра code")
        return query["code"][0]
    return text


def exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str, *, opener=urllib.request.urlopen) -> dict:
    """Код авторизации -> {accessToken, refreshToken, expiresIn, tokenType}. Ошибка сервера (errorCode) - TokenError с его описанием."""
    return _token_request({"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri, "client_id": client_id, "client_secret": client_secret}, opener)


def refresh(client_id: str, client_secret: str, refresh_token: str, *, opener=urllib.request.urlopen) -> dict:
    return _token_request({"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id, "client_secret": client_secret}, opener)


def _token_request(params: dict, opener) -> dict:
    req = urllib.request.Request(f"{TOKEN_URL}?{urllib.parse.urlencode(params)}", headers={"Accept": "application/json", "Content-Type": "application/json"})
    try:
        with opener(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")  # сервер объясняет причину в теле ответа
    except (urllib.error.URLError, OSError) as exc:
        raise TokenError(f"нет связи с {TOKEN_URL}: {exc}") from exc
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise TokenError(f"сервер вернул не JSON: {body[:200]!r}") from exc
    if data.get("errorCode") or not data.get("accessToken"):
        raise TokenError(f"сервер отказал: {data.get('errorCode')} {data.get('description')}".strip())
    return data


class _Callback(BaseHTTPRequestHandler):
    code: str | None = None
    error: str | None = None
    done = threading.Event()

    def do_GET(self) -> None:  # noqa: N802 (имя задано http.server)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        type(self).code = (query.get("code") or [None])[0]
        type(self).error = (query.get("error") or [None])[0]
        ok = type(self).code is not None
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(("<html><body style='font-family:sans-serif'><h3>%s</h3></body></html>" % (
            "Готово. Окно можно закрыть, токен показан в консоли." if ok else "Код не получен. Вернитесь в консоль.")).encode("utf-8"))
        type(self).done.set()

    def log_message(self, *args) -> None:  # код авторизации не должен попадать в вывод
        return None


def wait_for_code(redirect_uri: str, timeout_s: float = 300.0) -> str | None:
    """Поднять локальный сервер на адресе redirect_uri (только localhost/127.0.0.1) и ждать `?code=`. None - не дождались."""
    parts = urllib.parse.urlsplit(redirect_uri)
    if parts.hostname not in ("localhost", "127.0.0.1") or not parts.port:
        raise TokenError("локальный приём кода возможен только для http://localhost:ПОРТ/...")
    _Callback.code = _Callback.error = None
    _Callback.done = threading.Event()
    try:
        server = HTTPServer((parts.hostname, parts.port), _Callback)
    except OSError as exc:
        raise TokenError(f"порт {parts.port} занят ({exc}): задайте другой --redirect-uri и добавьте его в портале") from exc
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _Callback.done.wait(timeout_s)
    finally:
        server.shutdown()
        server.server_close()
    if _Callback.error:
        raise TokenError(f"авторизация отклонена: {_Callback.error}")
    return _Callback.code


async def check_accounts(client_id: str, client_secret: str, access_token: str) -> list[dict]:
    """Счета, доступные токену (с demo- и live-сервера): [{ctidTraderAccountId, isLive, traderLogin, brokerTitleShort}]."""
    from exchange.ctrader import fetch_accounts

    return await fetch_accounts(client_id, client_secret, access_token)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Получение Access Token cTrader Open API (OAuth 2.0)")
    ap.add_argument("--client-id", default=os.environ.get("CTRADER_CLIENT_ID"))
    ap.add_argument("--client-secret", default=os.environ.get("CTRADER_CLIENT_SECRET"), help="лучше окружение CTRADER_CLIENT_SECRET или скрытый ввод")
    ap.add_argument("--redirect-uri", default=DEFAULT_REDIRECT, help=f"адрес из Redirect URIs приложения (по умолчанию {DEFAULT_REDIRECT})")
    ap.add_argument("--scope", default="trading", choices=SCOPES, help="trading - ордера и данные; accounts - только чтение (для Shadow достаточно)")
    ap.add_argument("--account", default=os.environ.get("CTRADER_ACCOUNT_ID"), help="ваш ctidTraderAccountId: проверить, что токен его видит")
    ap.add_argument("--code", help="уже полученный код авторизации (или весь адрес возврата): браузер не нужен")
    ap.add_argument("--no-browser", action="store_true", help="не открывать браузер: только напечатать ссылку")
    ap.add_argument("--save", type=Path, help="сохранить токены в этот JSON-файл (положите его вне репозитория)")
    ap.add_argument("--refresh", metavar="REFRESH_TOKEN", help="не авторизоваться заново, а обновить токен по Refresh Token")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    client_id = (a.client_id or input("Client ID: ")).strip()
    client_secret = (a.client_secret or getpass.getpass("Client Secret (ввод скрыт): ")).strip()
    try:
        if a.refresh:
            tokens = refresh(client_id, client_secret, a.refresh.strip())
        else:
            if a.code:
                code = extract_code(a.code)
            else:
                url = build_auth_url(client_id, a.redirect_uri, a.scope)
                print("\n1) Откройте ссылку, войдите под cTID и нажмите «Allow access» (если браузер не открылся сам):\n\n   " + url + "\n")
                if not a.no_browser:
                    webbrowser.open(url)
                code = None
                if urllib.parse.urlsplit(a.redirect_uri).hostname in ("localhost", "127.0.0.1"):
                    print(f"2) Жду возврата на {a.redirect_uri} (до 5 минут)... Ctrl+C - ввести вручную.")
                    try:
                        code = wait_for_code(a.redirect_uri)
                    except KeyboardInterrupt:
                        code = None
                if code is None:
                    code = extract_code(input("2) Вставьте адрес, на который вас перебросило (или только значение code): "))
            print("3) Меняю код на токены...")
            tokens = exchange_code(client_id, client_secret, code, a.redirect_uri)
        accounts = asyncio.run(check_accounts(client_id, client_secret, tokens["accessToken"]))
    except TokenError as exc:
        print(f"\nОШИБКА: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - утилита пользователя: понятное сообщение вместо трейсбека (токен уже получен, его не теряем)
        print(f"\nТокен получен, но проверка счетов не удалась: {exc}", file=sys.stderr)
        accounts = []
    print("\n=== Впишите в ~/trading/.env.shadow на сервере (значения никуда больше не отправлялись) ===")
    print(f"CTRADER_ACCESS_TOKEN={tokens['accessToken']}")
    if tokens.get("refreshToken"):
        print(f"CTRADER_REFRESH_TOKEN={tokens['refreshToken']}")
    print(f"# access token живёт {tokens.get('expiresIn', 2628000)} с (~30 дней); с CTRADER_REFRESH_TOKEN бот обновляет его сам")
    if accounts:
        print("\nСчета, доступные токену:")
        for acc in accounts:
            print(f"  ctidTraderAccountId={acc.get('ctidTraderAccountId')}  {'LIVE' if acc.get('isLive') else 'DEMO'}  логин={acc.get('traderLogin')}  брокер={acc.get('brokerTitleShort', '')}")
        if a.account and str(a.account) not in {str(x.get("ctidTraderAccountId")) for x in accounts}:
            print(f"\nВНИМАНИЕ: счёта {a.account} среди них нет - при авторизации отметьте именно этот счёт.", file=sys.stderr)
    if a.save:
        a.save.write_text(json.dumps(tokens, indent=1), encoding="utf-8")
        print(f"\nТокены сохранены в {a.save} - держите файл вне репозитория.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
