"""Блок 5 - система версий: версия 1.0, changelog.json, окно «Приложение обновлено» (один раз на версию), «История обновлений» в настройках, связь с автообновлением."""

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import flet as ft
import pytest

from tradeai_client import APP_NAME, __version__, changelog, updater
from tradeai_client.settings import ClientSettings, load_settings, save_settings
from tradeai_client.ui import widgets as w

CLIENT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def detached_controls(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)


def walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


def texts(c):
    return [x.value for x in walk(c) if isinstance(x, ft.Text) and x.value]


def entry(version, *changes, date="2026-10-20", title=""):
    return {"version": version, "date": date, "title": title, "changes": [{"type": t, "text": x} for t, x in changes]}


# ======================================================================================== данные


def test_current_version_is_1_2_1_and_the_changelog_starts_from_the_1_0_base():
    assert __version__ == "1.6.2"
    releases = changelog.load()
    assert releases[0].version == "1.6.2" and releases[-1].version == "1.0" and releases[-1].date and releases[-1].changes
    assert changelog.release_for(releases, __version__) is not None and changelog.release_for(releases, "1.0.0") is not None  # 1.0 == 1.0.0
    assert {c.type for r in releases for c in r.changes} <= set(changelog.TYPES)


def test_changelog_is_sorted_newest_first_and_validated():
    rel = changelog.parse({"versions": [entry("1.0", ("new", "a")), entry("1.10", ("fix", "b")), entry("1.2", ("improve", "c"))]})
    assert [r.version for r in rel] == ["1.10", "1.2", "1.0"]  # 1.10 новее 1.2 (числовое сравнение, а не строковое)
    assert changelog.parse([entry("1.0", ("new", "x"))])[0].changes[0].text == "x" and changelog.parse({"versions": [{"version": "1.0", "changes": ["строка"]}]})[0].changes[0].type == "new"
    for bad in ({"versions": "x"}, {"versions": [{"changes": []}]}, {"versions": [entry("v1", ("new", "a"))]}, {"versions": [entry("1.0", ("bug", "a"))]},
                {"versions": [entry("1.0", ("new", "  "))]}, {"versions": [entry("1.0", ("new", "a")), entry("1.0.0", ("new", "b"))]}):
        with pytest.raises(changelog.ChangelogError):
            changelog.parse(bad)


def test_missed_versions_and_whats_new_rule():
    rel = changelog.parse({"versions": [entry("1.0", ("new", "a")), entry("1.1", ("new", "b")), entry("1.2", ("new", "c")), entry("1.3", ("new", "d"))]})
    assert [r.version for r in changelog.missed(rel, "1.0", "1.2")] == ["1.2", "1.1"]  # пропущенное между виденной и текущей; 1.3 ещё не установлена
    assert [r.version for r in changelog.missed(rel, "", "1.1")] == ["1.1", "1.0"]
    assert changelog.should_show_whats_new("", "1.0") and changelog.should_show_whats_new("1.0", "1.1") and changelog.should_show_whats_new("0.2.0", "1.0")
    assert not changelog.should_show_whats_new("1.1", "1.1") and not changelog.should_show_whats_new("1.2", "1.1") and not changelog.should_show_whats_new("1.0", "1.0.0")


def test_last_seen_version_is_stored_in_settings_json(tmp_path):
    path = tmp_path / "settings.json"
    save_settings(ClientSettings(base_url="http://127.0.0.1:1", api_key="x", last_seen_version="1.0"), path)
    assert json.loads(path.read_text(encoding="utf-8"))["last_seen_version"] == "1.0" and load_settings(path, env={}).last_seen_version == "1.0"
    path.write_text(json.dumps({"last_seen_version": 5}), encoding="utf-8")
    assert load_settings(path, env={}).last_seen_version == ""  # мусор в файле не роняет запуск
    assert load_settings(tmp_path / "нет.json", env={}).last_seen_version == ""


# ======================================================================================== окна


def make_shell(monkeypatch, *, seen="", first_run=False, releases=None):
    from tradeai_client.ui import shell as shell_mod

    saved = []
    monkeypatch.setattr(shell_mod, "load_settings", lambda: ClientSettings(api_key="k" * 40, last_seen_version=seen, profile_chosen=True))
    monkeypatch.setattr(shell_mod, "save_settings", lambda s, path=None: saved.append(s.last_seen_version))
    monkeypatch.setattr(shell_mod, "settings_exist", lambda: not first_run)
    if releases is not None:
        monkeypatch.setattr(changelog, "load", lambda path=None: releases)
    sh = shell_mod.Shell(MagicMock())
    sh.run = lambda coro: asyncio.run(coro)
    return sh, saved


def dialog_of(sh):
    return sh.page.show_dialog.call_args.args[0]


def popup_of(sh):
    """Всплывающее окно «Что нового» / экран выбора пользователя: последний элемент, добавленный в page.overlay."""
    return sh.page.overlay.append.call_args.args[0]


def button_of(c, label):
    return next(x for x in walk(c) if isinstance(x, ft.FilledButton) and x.content == label)


def test_whats_new_dialog_shows_this_versions_changes_and_marks_it_seen(monkeypatch):
    rel = changelog.parse({"versions": [entry("0.9", ("new", "старое")), entry(__version__, ("new", "Новая вкладка"), ("fix", "Починили график"), ("improve", "Быстрее"), title="Большое обновление")]})
    sh, saved = make_shell(monkeypatch, seen="0.9", releases=rel)
    after = []
    sh.show_whats_new(then=lambda: after.append("next"))
    d = popup_of(sh)
    assert d.expand and d.blur is not None and d.alignment == ft.Alignment.CENTER  # на весь экран, фон размыт, карточка по центру
    t = texts(d)
    assert t[0] == f"v{__version__}"  # версия - вверху по центру, под ней описание
    assert "Новая вкладка" in t and "Починили график" in t and "Быстрее" in t and "старое" not in t  # именно изменения этой версии
    assert {"Новое", "Исправлено", "Улучшено"} <= set(t) and "Большое обновление" in t
    assert sh.settings.last_seen_version == "0.9" and not after  # пока окно открыто, версия не помечена
    button_of(d, "Круто!").on_click(None)  # белая кнопка закрывает окно навсегда (для этой версии)
    assert button_of(d, "Круто!").style.bgcolor == "#ffffff"
    assert sh.settings.last_seen_version == __version__ and saved[-1] == __version__ and after == ["next"] and sh.page.overlay.remove.called


def test_whats_new_mentions_skipped_versions_and_survives_a_missing_entry(monkeypatch):
    rel = changelog.parse({"versions": [entry("0.5", ("new", "a")), entry("0.7", ("new", "b")), entry(__version__, ("new", "c"))]})
    sh, _ = make_shell(monkeypatch, seen="0.5", releases=rel)
    sh.show_whats_new()
    assert any("Пропущенные версии: v0.7" in x for x in texts(popup_of(sh)))
    sh2, saved = make_shell(monkeypatch, seen="0.1", releases=changelog.parse({"versions": [entry("0.2", ("new", "x"))]}))  # записи для текущей версии нет
    done = []
    sh2.show_whats_new(then=lambda: done.append(1))
    assert not sh2.page.overlay.append.called and done == [1] and saved[-1] == __version__  # не зацикливаемся: версия помечена виденной


def test_first_ever_launch_does_not_claim_an_update(monkeypatch):
    sh, _ = make_shell(monkeypatch, first_run=True)
    assert sh.settings.last_seen_version == __version__ and not changelog.should_show_whats_new(sh.settings.last_seen_version, __version__)
    old, _ = make_shell(monkeypatch, seen="")  # файл настроек есть, версии нет (обновление со старого клиента): окно покажем
    assert changelog.should_show_whats_new(old.settings.last_seen_version, __version__)


def test_startup_shows_whats_new_first_and_checks_updates_only_after_closing_it(monkeypatch):
    sh, _ = make_shell(monkeypatch, seen="0.2.0", releases=changelog.parse({"versions": [entry(__version__, ("new", "x"))]}))
    runs = []
    sh.page.run_task = lambda fn, *a: runs.append(getattr(fn, "__name__", str(fn)))

    async def connect():
        pass

    sh.connect = connect
    asyncio.run(sh.start())
    assert "_check_update" not in runs and sh.page.overlay.append.called  # сначала «Что нового»
    button_of(popup_of(sh), "Круто!").on_click(None)
    assert "_check_update" in runs  # после закрытия - проверка обновления (окна не наслаиваются)
    sh2, _ = make_shell(monkeypatch, seen=__version__)
    runs2 = []
    sh2.page.run_task = lambda fn, *a: runs2.append(getattr(fn, "__name__", str(fn)))
    sh2.connect = connect
    asyncio.run(sh2.start())
    assert "_check_update" in runs2 and not sh2.page.overlay.append.called  # версия уже виденная: сразу проверка обновления, окна нет


def test_settings_has_a_changelog_button_that_opens_a_scrollable_full_history(monkeypatch):
    rel = changelog.parse({"versions": [entry("0.8", ("new", "базовая")), entry("0.9", ("new", "вторая")), entry(__version__, ("fix", "текущая"))]})
    sh, _ = make_shell(monkeypatch, releases=rel)
    sh.show_settings()
    settings_dialog = dialog_of(sh)
    button = next(x for x in walk(settings_dialog.content) if isinstance(x, ft.OutlinedButton) and str(x.content).startswith("История обновлений"))
    button.on_click(None)  # закрывает настройки и открывает историю
    hist = dialog_of(sh)
    assert hist.title.value == "История обновлений"
    column = hist.content.content
    assert column.scroll is not None and hist.content.height and len(column.controls) == 3
    t = texts(hist.content)
    assert t.index("v0.9") < t.index("v0.8") and "базовая" in t and "вторая" in t and "текущая" in t and "текущая" in [x for x in t if x == "текущая"]


# ======================================================================================== связь с автообновлением


def test_update_info_carries_the_server_changelog_and_the_offer_shows_it(monkeypatch):
    raw = {"version": "9.9", "sha256": "a" * 64, "size": 100, "notes": "кратко", "changelog": [entry("9.9", ("new", "Огромная фича"), ("fix", "Баг"), title="Мажор")]}
    info = updater.parse_info(raw)
    assert info.changelog[0].version == "9.9" and info.changelog[0].changes[1].type == "fix"
    sh, _ = make_shell(monkeypatch)
    sh.client = MagicMock()
    sh._offer_update(info)
    t = texts(dialog_of(sh).content)
    assert f"Доступна версия 9.9 (у вас {__version__})." in t and "Огромная фича" in t and "Мажор" in t and "кратко" not in t  # красивая карточка вместо сухих заметок
    broken = updater.parse_info({**raw, "changelog": [{"version": "x"}]})
    assert broken.changelog == () and broken.notes == "кратко"  # битая история на сервере не мешает обновлению
    old_server = updater.parse_info({"version": "9.9", "sha256": "a" * 64, "size": 1, "notes": "заметки"})
    sh._offer_update(old_server)
    assert "заметки" in texts(dialog_of(sh).content)  # сервер без истории (прежний формат version.json)


def test_build_script_requires_a_release_entry_publishes_the_history_and_bundles_the_file():
    ps = (CLIENT / "build_exe.ps1").read_text(encoding="utf-8-sig")
    assert "changelog.json" in ps and "нет (или дважды) записи для версии" in ps  # без записи в changelog.json сборка останавливается
    assert "changelog = @($changelog.versions)" in ps and "ConvertTo-Json -Depth 8" in ps  # на сервер уходит вся история
    assert '"tradeai_client/changelog.json:tradeai_client"' in ps and "$productVersion" in ps
    data = json.loads((CLIENT / "tradeai_client" / "changelog.json").read_text(encoding="utf-8"))
    assert data["versions"][0]["version"] == __version__  # запись текущей версии - в начале файла


def test_card_widgets_render_every_change_type():
    from tradeai_client.ui.changelog_view import release_card

    r = changelog.parse([entry("2.0", ("new", "n"), ("improve", "i"), ("fix", "f"), title="T")])[0]
    t = texts(release_card(r, current=True))
    assert t[0] == "v2.0" and "T" in t and "2026-10-20" in t and "текущая" in t and {"Новое", "Улучшено", "Исправлено"} <= set(t)
    assert APP_NAME and w.GREEN


# ======================================================================================== экран выбора пользователя


def make_login_shell(monkeypatch, *, chosen):
    from tradeai_client.ui import shell as shell_mod

    saved = []
    monkeypatch.setattr(shell_mod, "load_settings", lambda: ClientSettings(api_key="k" * 40, last_seen_version=__version__, profile_chosen=chosen))
    monkeypatch.setattr(shell_mod, "save_settings", lambda s, path=None: saved.append((s.profile, s.profile_chosen)))
    monkeypatch.setattr(shell_mod, "settings_exist", lambda: True)
    sh = shell_mod.Shell(MagicMock())
    sh.run = lambda coro: asyncio.run(coro)
    return sh, saved


def test_login_screen_offers_santa_and_chr_remembers_the_choice(monkeypatch):
    sh, saved = make_login_shell(monkeypatch, chosen=False)
    picked = []
    screen = sh.show_login(picked.append)
    assert screen.expand and "Выберите пользователя" in texts(screen) and {"SANTA", "CHR"} <= set(texts(screen))
    chr_tile = next(x for x in walk(screen) if isinstance(x, ft.Container) and x.on_click and "CHR" in texts(x))
    chr_tile.on_click(None)
    assert picked == ["CHR"] and sh.settings.profile == "CHR" and sh.settings.profile_chosen and saved[-1] == ("CHR", True)
    assert sh.page.overlay.remove.called  # экран закрыт, дальше обычный запуск


def test_startup_asks_for_the_user_only_when_none_was_chosen_yet(monkeypatch):
    async def connect():
        pass

    new, _ = make_login_shell(monkeypatch, chosen=False)
    new.connect = connect
    new.page.run_task = lambda *a: None
    shown = []
    new.show_login = lambda cb: shown.append(cb) or cb("SANTA")  # пользователь сразу «выбирает»
    asyncio.run(new.start())
    assert len(shown) == 1
    old, _ = make_login_shell(monkeypatch, chosen=True)  # выбор уже сделан (в т.ч. до пересборки .exe): экран не показывается
    old.connect = connect
    old.page.run_task = lambda *a: None
    old.show_login = lambda cb: pytest.fail("экран выбора не должен показываться повторно")
    asyncio.run(old.start())


def test_legacy_http_url_switches_to_https_only_when_the_https_address_answers():
    from tradeai_client.settings import HTTPS_URL, LEGACY_HTTP_URL, ClientSettings, upgrade_to_https

    s = ClientSettings(base_url=LEGACY_HTTP_URL)
    assert upgrade_to_https(s, probe=lambda url: False) is False and s.base_url == LEGACY_HTTP_URL  # сертификата ещё нет - всё по-старому
    asked = []
    assert upgrade_to_https(s, probe=lambda url: asked.append(url) or True) is True and s.base_url == HTTPS_URL and asked == [HTTPS_URL]
    assert s.cleartext_remote is False and s.ws_url == "wss://193.233.75.254.nip.io/ws/live_state"
    other = ClientSettings(base_url="http://127.0.0.1:8001")
    assert upgrade_to_https(other, probe=lambda url: True) is False and other.base_url == "http://127.0.0.1:8001"  # свой адрес/туннель не трогаем
