"""scripts/get_ctrader_token.py: ссылка авторизации, разбор кода, обмен на токен (OAuth 2.0), приём кода на локальном адресе, проверка счетов токена."""

import asyncio
import io
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

from exchange.ctrader import fetch_accounts
from scripts import get_ctrader_token as G
from tests.exchange.fake_ctrader_server import CTID, FakeCTraderServer


class Resp:
    def __init__(self, body: str):
        self._b = body.encode()

    def read(self):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_auth_url_follows_the_documented_format():
    url = G.build_auth_url("123_abc", "http://localhost:5555/callback", "trading")
    parts = urllib.parse.urlsplit(url)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
    assert urllib.parse.parse_qs(parts.query) == {"client_id": ["123_abc"], "redirect_uri": ["http://localhost:5555/callback"], "scope": ["trading"], "product": ["web"]}
    with pytest.raises(G.TokenError, match="scope"):
        G.build_auth_url("x", "http://localhost/", "admin")
    with pytest.raises(G.TokenError, match="Client ID"):
        G.build_auth_url("", "http://localhost/")


@pytest.mark.parametrize("text,code", [
    ("abc_DEF-123", "abc_DEF-123"),
    ("  http://localhost:5555/callback?code=zzz_1&x=2  ", "zzz_1"),
    ("?code=q1", "q1"),
    ("code=q2", "q2"),
])
def test_extract_code_accepts_url_query_or_bare_code(text, code):
    assert G.extract_code(text) == code


@pytest.mark.parametrize("text,msg", [("", "пустой"), ("http://localhost/callback?error=access_denied", "access_denied"), ("http://localhost/callback", "нет параметра code")])
def test_extract_code_rejects_bad_input(text, msg):
    with pytest.raises(G.TokenError, match=msg):
        G.extract_code(text)


def test_exchange_code_sends_the_documented_request_and_returns_tokens():
    seen = {}

    def opener(req, timeout=0):
        seen["url"] = req.full_url
        return Resp(json.dumps({"accessToken": "A", "tokenType": "bearer", "expiresIn": 2628000, "refreshToken": "R", "errorCode": None, "description": None}))

    tokens = G.exchange_code("cid", "sec", "the-code", "http://localhost:5555/callback", opener=opener)
    parts = urllib.parse.urlsplit(seen["url"])
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == "https://openapi.ctrader.com/apps/token"
    assert urllib.parse.parse_qs(parts.query) == {"grant_type": ["authorization_code"], "code": ["the-code"], "redirect_uri": ["http://localhost:5555/callback"],
                                                  "client_id": ["cid"], "client_secret": ["sec"]}
    assert tokens["accessToken"] == "A" and tokens["refreshToken"] == "R"


def test_token_errors_are_reported_with_the_servers_reason():
    with pytest.raises(G.TokenError, match="ACCESS_DENIED.*bad code"):
        G.exchange_code("c", "s", "x", "r", opener=lambda req, timeout=0: Resp(json.dumps({"accessToken": None, "errorCode": "ACCESS_DENIED", "description": "bad code"})))

    def http_error(req, timeout=0):
        raise urllib.error.HTTPError(req.full_url, 400, "Bad", {}, io.BytesIO(json.dumps({"errorCode": "INVALID_GRANT", "description": "expired"}).encode()))

    with pytest.raises(G.TokenError, match="INVALID_GRANT"):
        G.exchange_code("c", "s", "x", "r", opener=http_error)
    with pytest.raises(G.TokenError, match="не JSON"):
        G.exchange_code("c", "s", "x", "r", opener=lambda req, timeout=0: Resp("<html>"))

    def offline(req, timeout=0):
        raise urllib.error.URLError("dns")

    with pytest.raises(G.TokenError, match="нет связи"):
        G.exchange_code("c", "s", "x", "r", opener=offline)


def test_refresh_uses_the_refresh_token_grant():
    seen = {}
    G.refresh("cid", "sec", "RT", opener=lambda req, timeout=0: (seen.setdefault("q", urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query)),
                                                                Resp(json.dumps({"accessToken": "A2", "refreshToken": "R2"})))[1])
    assert seen["q"]["grant_type"] == ["refresh_token"] and seen["q"]["refresh_token"] == ["RT"]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_local_callback_server_catches_the_code():
    port = free_port()
    out = {}

    def browser():
        time.sleep(0.2)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/callback?code=CODE123", timeout=5) as r:
            out["page"] = r.read().decode("utf-8")

    t = threading.Thread(target=browser)
    t.start()
    assert G.wait_for_code(f"http://127.0.0.1:{port}/callback", timeout_s=10) == "CODE123"
    t.join()
    assert "Готово" in out["page"]


def test_local_callback_rejects_foreign_hosts_busy_ports_and_times_out():
    with pytest.raises(G.TokenError, match="localhost"):
        G.wait_for_code("https://example.com/callback")
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        with pytest.raises(G.TokenError, match="занят"):
            G.wait_for_code(f"http://127.0.0.1:{busy.getsockname()[1]}/callback", timeout_s=0.1)
    assert G.wait_for_code(f"http://127.0.0.1:{free_port()}/callback", timeout_s=0.2) is None


def test_fetch_accounts_lists_the_tokens_accounts_from_the_server():
    async def main():
        srv = await FakeCTraderServer(is_live=True).start()
        try:
            return await fetch_accounts("cid", "sec", "tok-1", hosts=("127.0.0.1",), port=srv.port, use_tls=False)
        finally:
            await srv.stop()

    accounts = asyncio.run(main())
    assert [(a["ctidTraderAccountId"], a["isLive"]) for a in accounts] == [(CTID, True)]
