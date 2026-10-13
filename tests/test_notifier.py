import asyncio
import logging

import pytest
from aiohttp import web

from execution.state import SafeMode
from tests.conftest import serve
from utils.notifier import Notifier

TOKEN = "123456:SECRET-TOKEN"
CHAT = "42"


class FakeTelegram:
    def __init__(self, status=200, body="{}"):
        self.status, self.body = status, body
        self.messages: list[dict] = []
        self.paths: list[str] = []
        self.app = web.Application()
        self.app.router.add_post("/bot{token}/sendMessage", self._send)

    async def _send(self, request):
        self.paths.append(request.path)
        self.messages.append(await request.json())
        return web.Response(status=self.status, text=self.body, content_type="application/json")


def run(coro):
    return asyncio.run(coro)


def test_without_credentials_it_only_logs(caplog, monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    n = Notifier.from_env()
    assert not n.enabled

    async def go():
        n.critical("SAFE MODE: тест", {"k": 1})  # из корутины: фоновая задача, но отправки нет
        n.trade(action="ВХОД", symbol="BTCUSDT", side="buy", qty=0.1, price=60000.0, position=0.1)
        assert await n.send("ещё одно") is False
        await n.aclose()

    with caplog.at_level(logging.INFO, logger="notifier"):
        run(go())
    text = caplog.text
    assert "SAFE MODE: тест" in text and "ВХОД BTCUSDT" in text and "ещё одно" in text
    assert n.sent == 0 and n.failed == 0


def test_sync_call_without_event_loop_does_not_raise(caplog):
    n = Notifier(TOKEN, CHAT)
    with caplog.at_level(logging.CRITICAL, logger="notifier"):
        n.critical("нет цикла событий")  # не в корутине
    assert "нет цикла событий" in caplog.text


def test_partial_credentials_count_as_disabled():
    assert not Notifier(TOKEN, "").enabled and not Notifier("", CHAT).enabled and Notifier(TOKEN, CHAT).enabled


def test_message_is_posted_to_telegram_api():
    async def go():
        tg = FakeTelegram()
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}")
            assert await n.send("привет") is True
            await n.aclose()
        return tg, n

    tg, n = run(go())
    assert tg.paths == [f"/bot{TOKEN}/sendMessage"]
    assert tg.messages == [{"chat_id": CHAT, "text": "привет", "disable_web_page_preview": True}]
    assert n.sent == 1


def test_http_error_is_swallowed_and_token_is_redacted(caplog):
    async def go():
        tg = FakeTelegram(status=401, body=f'{{"description": "bad token {TOKEN}"}}')
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}")
            with caplog.at_level(logging.WARNING, logger="notifier"):
                ok = await n.send("x")
            await n.aclose()
        return ok, n

    ok, n = run(go())
    assert ok is False and n.failed == 1
    assert "HTTP 401" in caplog.text and TOKEN not in caplog.text


def test_unreachable_telegram_never_raises(caplog):
    async def go():
        n = Notifier(TOKEN, CHAT, api_url="http://127.0.0.1:1", timeout_s=1.0)
        with caplog.at_level(logging.WARNING, logger="notifier"):
            ok = await n.send("x")
        await n.aclose()
        return ok, n

    ok, n = run(go())
    assert ok is False and n.failed == 1 and TOKEN not in caplog.text


def test_background_messages_keep_order_and_are_flushed():
    async def go():
        tg = FakeTelegram()
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}")
            for i in range(6):
                n.notify_nowait(f"msg-{i}")
            await n.aclose()  # дожидается отправки
        return tg

    tg = run(go())
    assert [m["text"] for m in tg.messages] == [f"msg-{i}" for i in range(6)]


def test_long_text_is_truncated_to_telegram_limit():
    async def go():
        tg = FakeTelegram()
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}")
            await n.send("я" * 10_000)
            await n.aclose()
        return tg

    assert len(run(go()).messages[0]["text"]) <= 4096


def test_prefix_is_prepended():
    async def go():
        tg = FakeTelegram()
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}", prefix="[testnet] ")
            await n.send("старт")
            await n.aclose()
        return tg

    assert run(go()).messages[0]["text"] == "[testnet] старт"


def test_notifier_is_a_safe_mode_alerter(tmp_path):
    async def go():
        tg = FakeTelegram()
        async with serve(tg.app) as host:
            n = Notifier(TOKEN, CHAT, api_url=f"http://{host}")
            safe = SafeMode(tmp_path / ".safe_mode", alerter=n)
            assert safe.trip("расхождение позиций", {"symbol": "BTCUSDT"}) is True
            await n.aclose()
        return tg

    tg = run(go())
    assert len(tg.messages) == 1
    text = tg.messages[0]["text"]
    assert "SAFE MODE: расхождение позиций" in text and "BTCUSDT" in text


def test_drawdown_message_contains_numbers(caplog):
    n = Notifier()

    async def go():
        n.drawdown(drawdown=0.081, limit=0.05, peak=10_000.0, equity=9_190.0)
        await n.aclose()

    with caplog.at_level(logging.CRITICAL, logger="notifier"):
        run(go())
    assert "8.10%" in caplog.text and "5.00%" in caplog.text and "9190.00" in caplog.text


@pytest.mark.parametrize("secret", ["", None])
def test_redact_ignores_empty_secret(secret):
    from utils.notifier import _redact

    assert _redact("abc", secret) == "abc"
