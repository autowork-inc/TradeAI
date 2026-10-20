"""Веб-версия клиента (2026-10-20): вход (логин/пароль, «Запомнить меня», защита от перебора), конфигурация контейнера, сборка экранов и мобильная раскладка."""

import asyncio
import time
from unittest.mock import MagicMock

import flet as ft
import pytest

from tradeai_client.web import app as web_app
from tradeai_client.web import auth

KEY = "k" * 40
ENV = {"WEB_LOGIN": "Santa", "WEB_PASSWORD": "Cheshir", "TRADEAI_API_KEY": KEY}


@pytest.fixture(autouse=True)
def detached_controls(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)


_REAL_SLEEP = asyncio.sleep


def fast_sleep(monkeypatch):
    """Пауза замедления перебора в тестах не ждётся (подмена asyncio.sleep внутри модуля, а не рекурсивная)."""
    monkeypatch.setattr(web_app.asyncio, "sleep", lambda s: _REAL_SLEEP(0))


def walk(c):
    yield c
    for name in ("controls", "content", "tabs"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


# ======================================================================================== вход


def test_credentials_are_checked_exactly_but_the_login_ignores_case():
    ok = auth.check_credentials
    assert ok("Santa", "Cheshir", "Santa", "Cheshir") and ok("santa", "Cheshir", "Santa", "Cheshir") and ok(" SANTA ", "Cheshir", "Santa", "Cheshir")
    assert not ok("Santa", "cheshir", "Santa", "Cheshir") and not ok("Santa", "", "Santa", "Cheshir") and not ok("", "Cheshir", "Santa", "Cheshir")
    assert not ok("Santa", "Cheshir ", "Santa", "Cheshir") and not ok("Santa2", "Cheshir", "Santa", "Cheshir")


def test_remember_me_token_is_signed_expires_and_dies_with_the_secret():
    secret = auth.derive_secret("Cheshir", KEY)
    token = auth.make_token(secret, "Santa", now=1000.0, ttl_s=100)
    assert auth.verify_token(secret, token, "Santa", now=1050.0)
    assert not auth.verify_token(secret, token, "Santa", now=1101.0)  # истёк
    assert not auth.verify_token(auth.derive_secret("Другой", KEY), token, "Santa", now=1050.0)  # сменили пароль -> токены недействительны
    assert not auth.verify_token(auth.derive_secret("Cheshir", "z" * 40), token, "Santa", now=1050.0)  # сменили ключ API
    assert not auth.verify_token(secret, token, "Другой", now=1050.0)
    login, exp, sig = token.split(".")
    for forged in (f"{login}.{int(exp) + 9999}.{sig}", f"{login}.{exp}.{'0' * 64}", "santa.1.2.3", "", None, 5, f"{login}.abc.{sig}"):
        assert not auth.verify_token(secret, forged, "Santa", now=1050.0)
    assert "Cheshir" not in token  # пароль в токен не попадает
    assert auth.derive_secret("a", "b", "явный") == "явный".encode("utf-8")


def test_login_guard_locks_after_repeated_failures_and_resets_on_success():
    g = auth.LoginGuard(max_fails=3, lock_s=60)
    assert g.locked_for(now=0) == 0
    for _ in range(2):
        g.failed(now=0)
    assert g.locked_for(now=0) == 0
    g.failed(now=0)
    assert g.locked_for(now=10) == pytest.approx(50) and g.locked_for(now=61) == 0
    g.failed(now=100)
    g.succeeded()
    g.failed(now=100)
    g.failed(now=100)
    assert g.locked_for(now=100) == 0  # счётчик сброшен успешным входом


# ======================================================================================== конфигурация


def test_config_requires_login_password_and_a_long_api_key():
    cfg = web_app.WebConfig.from_env(ENV)
    assert (cfg.login, cfg.api_url, cfg.port) == ("Santa", "http://api:8000", 8550) and cfg.api_key == KEY
    for bad in ({**ENV, "WEB_LOGIN": ""}, {**ENV, "WEB_PASSWORD": ""}, {**ENV, "TRADEAI_API_KEY": "короткий"}, {"WEB_LOGIN": "a", "WEB_PASSWORD": "b"}):
        with pytest.raises(web_app.WebConfigError):
            web_app.WebConfig.from_env(bad)  # без пароля вход не может быть открытым: Fail-Fast


# ======================================================================================== экран входа и сессия


class FakePrefs:
    def __init__(self, store=None):
        self.store = dict(store or {})

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value):
        self.store[key] = value
        return True

    async def remove(self, key):
        return self.store.pop(key, None) is not None


def make_app(prefs=None, width=390):
    page = MagicMock()
    page.width, page.height, page.controls = width, 800, []
    page.add = lambda *c: page.controls.extend(c)
    page.services = []
    cfg = web_app.WebConfig.from_env(ENV)
    app = web_app.WebApp(page, cfg)
    app.prefs = prefs or FakePrefs()
    app.open_app = _fake_open(app)
    return app, page


def _fake_open(app):
    async def open_app():
        app.shell = object()
    return open_app


def login_controls(page):
    flat = [x for c in page.controls for x in walk(c)]
    fields = [x for x in flat if isinstance(x, ft.TextField)]
    return fields[0], fields[1], next(x for x in flat if isinstance(x, ft.Checkbox)), next(x for x in flat if isinstance(x, ft.FilledButton)), flat


def submit(page, login, password, remember=True):
    user, pw, box, button, _ = login_controls(page)
    user.value, pw.value, box.value = login, password, remember
    asyncio.run(button.on_click(None))


def test_login_screen_is_mobile_sized_and_has_remember_me_checked_by_default(monkeypatch):
    fast_sleep(monkeypatch)
    app, page = make_app(width=360)
    asyncio.run(app._enter())
    user, pw, box, button, flat = login_controls(page)
    assert user.label == "Логин" and pw.label == "Пароль" and pw.password and box.label == "Запомнить меня" and box.value is True
    card = next(x for x in flat if isinstance(x, ft.Container) and x.width)
    assert card.width <= 360 - 32  # карточка входа помещается в ширину iPhone


def test_correct_login_with_remember_me_stores_a_signed_token_not_the_password(monkeypatch):
    fast_sleep(monkeypatch)
    prefs = FakePrefs()
    app, page = make_app(prefs)
    asyncio.run(app._enter())
    submit(page, "santa", "Cheshir")
    assert app.shell is not None
    token = prefs.store[web_app.TOKEN_KEY]
    assert "Cheshir" not in token and auth.verify_token(app.cfg.secret, token, "Santa")


def test_without_remember_me_nothing_is_stored_and_wrong_password_is_rejected(monkeypatch):
    fast_sleep(monkeypatch)
    web_app.LOGIN_GUARD = auth.LoginGuard()
    prefs = FakePrefs({web_app.TOKEN_KEY: "старый-мусор"})
    app, page = make_app(prefs)
    asyncio.run(app._enter())  # мусорный токен -> экран входа
    submit(page, "Santa", "неверный")
    assert app.shell is None and any(x.value == "Неверный логин или пароль" for x in login_controls(page)[4] if isinstance(x, ft.Text))
    submit(page, "Santa", "Cheshir", remember=False)
    assert app.shell is not None and web_app.TOKEN_KEY not in prefs.store


def test_repeated_wrong_passwords_lock_the_form(monkeypatch):
    fast_sleep(monkeypatch)
    web_app.LOGIN_GUARD = auth.LoginGuard(max_fails=2, lock_s=60)
    app, page = make_app()
    asyncio.run(app._enter())
    submit(page, "Santa", "x")
    submit(page, "Santa", "y")
    submit(page, "Santa", "Cheshir")  # даже верный пароль не пускает, пока вход закрыт
    assert app.shell is None and any("Слишком много" in x.value for x in login_controls(page)[4] if isinstance(x, ft.Text))
    web_app.LOGIN_GUARD = auth.LoginGuard()


def test_saved_token_skips_the_login_screen_on_the_next_visit():
    cfg = web_app.WebConfig.from_env(ENV)
    prefs = FakePrefs({web_app.TOKEN_KEY: auth.make_token(cfg.secret, "Santa")})
    app, page = make_app(prefs)
    asyncio.run(app._enter())
    assert app.shell is not None and not page.controls  # экран входа не показывался


def test_storage_that_never_answers_falls_back_to_the_login_screen(monkeypatch):
    class Hanging(FakePrefs):
        async def get(self, key):
            await _REAL_SLEEP(60)

    real_wait_for = asyncio.wait_for
    monkeypatch.setattr(web_app.asyncio, "wait_for", lambda coro, timeout: real_wait_for(coro, 0.05))
    app, page = make_app(Hanging())
    asyncio.run(app._enter())
    assert app.shell is None and page.controls  # показан вход, а не бесконечное ожидание


# ======================================================================================== вкладки и раскладка


def test_web_shell_has_the_four_tabs_and_a_two_row_header_without_expand_in_wrap():
    cfg = web_app.WebConfig.from_env(ENV)
    page = MagicMock()
    page.width, page.height = 390, 800
    shell = web_app.WebShell(page, cfg, on_logout=lambda: None)
    flat = list(walk(shell.control))
    tabs = [x for x in flat if isinstance(x, ft.Tab)]
    assert [t.label for t in tabs] == ["Dashboard", "Statistics", "History", "Favorites"]
    bar = next(x for x in flat if isinstance(x, ft.TabBar))
    assert bar.scrollable  # на узком экране вкладки листаются
    for row in (x for x in flat if isinstance(x, ft.Row) and x.wrap):  # Flutter не допускает expand-детей внутри Wrap (ErrorWidget - серый экран)
        assert not any(getattr(c, "expand", None) for c in row.controls), "в Row(wrap=True) нельзя класть expand-элементы"
    assert shell.profile_strategies() is None and shell.profile_coins() is None  # у веб-версии профиль SANTA: без ограничений


def test_history_filters_wrap_into_a_column_on_a_narrow_screen():
    cfg = web_app.WebConfig.from_env(ENV)
    shell = web_app.WebShell(MagicMock(), cfg, on_logout=lambda: None)
    row = next(x for x in walk(shell.history.control) if isinstance(x, ft.Row) and shell.history._bar.control in x.controls)
    assert row.wrap and not any(getattr(c, "expand", None) for c in row.controls)
    table_header = [x for x in walk(shell.history.control) if isinstance(x, ft.Row) and x.scroll is not None]
    assert table_header  # таблица сделок листается по горизонтали


def test_trade_card_is_shown_as_text_and_keeps_the_gap_warning_out_of_the_table():
    panel = {"result": {"status": "Закрыта", "r": "-1.05 R", "pnl": "-5.25 USDT", "closes": [{"n": 1, "label": "Стоп", "time": "09.10 08:00", "price": "100", "r": "-1.05 R"}],
                        "warning": "Сделка помечена gap_affected"},
             "review": {"headline": "5/10", "points": [{"text": "Стоп"}]}, "reason": {"title": "VWAP", "steps": ["1. касание"]}}
    texts = [x.value for c in web_app.panel_controls(panel) for x in walk(c) if isinstance(x, ft.Text) and x.value]
    assert "Закрыта · -1.05 R · -5.25 USDT" in texts and "gap_affected" in " ".join(texts) and "VWAP" in texts
    assert [x.value for x in web_app.panel_controls({}) if isinstance(x, ft.Text)] == ["Нет данных для карточки"]


def test_narrow_screens_get_compact_page_padding():
    app, page = make_app(width=390)
    app._adapt()
    assert page.padding == 8
    page.width = 1200
    app._adapt()
    assert page.padding == 16
