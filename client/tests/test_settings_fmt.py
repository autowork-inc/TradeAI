import json

import pytest

from tradeai_client import fmt
import sys

from tradeai_client import build_defaults, settings as settings_mod
from tradeai_client.settings import (ClientSettings, SettingsError, load_settings, save_settings, settings_path)


def test_url_validation_and_ws_url():
    s = ClientSettings(base_url="http://127.0.0.1:8000/").validate()
    assert s.normalized_url == "http://127.0.0.1:8000" and s.ws_url == "ws://127.0.0.1:8000/ws/live_state"
    assert ClientSettings(base_url="https://mon.example.com/x").ws_url == "wss://mon.example.com/x/ws/live_state"
    for bad in ("", "127.0.0.1:8000", "ftp://h", "http://", "http://u:p@h:1", "http://h:1/?api_key=x", "http://h:1/#f"):
        with pytest.raises(SettingsError):
            ClientSettings(base_url=bad).validate()


def test_cleartext_warning_only_for_remote_http():
    assert not ClientSettings(base_url="http://127.0.0.1:8000").cleartext_remote  # SSH-туннель
    assert not ClientSettings(base_url="http://localhost:8000").cleartext_remote
    assert not ClientSettings(base_url="https://srv.example.com").cleartext_remote
    assert ClientSettings(base_url="http://203.0.113.5:8000").cleartext_remote  # ключ пошёл бы по сети открытым текстом


def test_save_and_load_roundtrip_and_key_can_be_forgotten(tmp_path):
    p = tmp_path / "TradeAI Monitor" / "settings.json"
    save_settings(ClientSettings(base_url="http://127.0.0.1:9000", api_key="secret-key", remember_key=True), p)
    s = load_settings(p, env={})
    assert s.base_url == "http://127.0.0.1:9000" and s.api_key == "secret-key"
    save_settings(ClientSettings(api_key="secret-key", remember_key=False), p)
    assert load_settings(p, env={}).api_key == "" and "secret-key" not in p.read_text(encoding="utf-8")
    assert not list(p.parent.glob("*.tmp"))  # временный файл не остаётся


def test_env_overrides_file_and_unknown_fields_are_ignored(tmp_path):
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"base_url": "http://a:1", "api_key": "from-file", "future_option": 1}), encoding="utf-8")
    s = load_settings(p, env={"TRADEAI_API_KEY": "from-env", "TRADEAI_API_URL": "http://b:2"})
    assert (s.base_url, s.api_key) == ("http://b:2", "from-env")


def test_broken_settings_file_is_reported_not_ignored(tmp_path):
    p = tmp_path / "s.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(SettingsError, match="нечитаем"):
        load_settings(p, env={})
    p.write_text("[1]", encoding="utf-8")
    with pytest.raises(SettingsError, match="формат"):
        load_settings(p, env={})


def test_settings_file_lives_in_the_persistent_user_folder_not_next_to_exe(monkeypatch, tmp_path):
    exe = tmp_path / "TradeAI-Monitor.exe"
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(exe))
    monkeypatch.setattr(settings_mod.os, "name", "nt")
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
    assert settings_path() == tmp_path / "Roaming" / "TradeAI-Monitor" / "settings.json"
    assert settings_mod.legacy_settings_path() == tmp_path / "settings.json"  # прежнее место - только запасной источник


def test_settings_survive_a_rebuild_and_the_legacy_file_is_migrated_once(monkeypatch, tmp_path):
    """Пересборка/обновление .exe стирает папку с .exe, но не %APPDATA%: ключ API не запрашивается заново. Файл рядом с .exe переносится в папку пользователя."""
    monkeypatch.setattr(settings_mod.os, "name", "nt")
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    exe_dir = tmp_path / "dist"
    exe_dir.mkdir()
    monkeypatch.setattr(sys, "executable", str(exe_dir / "TradeAI-Monitor.exe"))
    (exe_dir / "settings.json").write_text(json.dumps({"base_url": "http://203.0.113.7:8001", "api_key": "k" * 40, "profile": "CHR"}), encoding="utf-8")
    first = load_settings(env={})  # старый файл рядом с .exe
    assert first.api_key == "k" * 40 and first.profile == "CHR" and first.profile_chosen  # профиль уже был выбран -> экран выбора не нужен
    assert settings_path().exists()  # перенесён
    (exe_dir / "settings.json").unlink()  # «пересборка»: папка dist очищена
    again = load_settings(env={})
    assert again.api_key == "k" * 40 and again.profile == "CHR" and again.base_url == "http://203.0.113.7:8001"
    save_settings(ClientSettings(api_key="n" * 40, profile="SANTA", profile_chosen=True))
    assert load_settings(env={}).api_key == "n" * 40 and not (exe_dir / "settings.json").exists()


def test_profile_chosen_defaults_to_false_for_a_new_install_and_is_not_forced_by_garbage(monkeypatch, tmp_path):
    assert ClientSettings().profile_chosen is False and ClientSettings(profile_chosen="да").profile_chosen is False
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"api_key": "k" * 40}), encoding="utf-8")
    assert load_settings(p, env={}).profile_chosen is False  # профиля в файле нет -> покажем экран выбора


def test_defaults_come_from_build_config_when_no_file(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "app.exe"))
    s = load_settings(env={"APPDATA": str(tmp_path / "nowhere")})
    assert s.base_url == build_defaults.SERVER_URL == "http://193.233.75.254:8001" and s.api_key == build_defaults.API_KEY


def test_first_run_saves_next_to_exe_and_second_run_loads_it(monkeypatch, tmp_path):
    """Сценарий «один клик»: ввод адреса и ключа один раз -> settings.json в папке пользователя -> следующий запуск (в т.ч. новый .exe) берёт их оттуда."""
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "app.exe"))
    monkeypatch.setattr(settings_mod.os, "name", "nt")
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    written = save_settings(ClientSettings(base_url="http://203.0.113.7:8001", api_key="k" * 40))
    assert written == tmp_path / "appdata" / "TradeAI-Monitor" / "settings.json" and not (tmp_path / "settings.json").exists()
    s = load_settings(env={})
    assert (s.base_url, s.api_key) == ("http://203.0.113.7:8001", "k" * 40)


def test_stale_appdata_file_is_ignored(monkeypatch, tmp_path):
    """Старый файл в профиле Windows (адрес 127.0.0.1:8000 от туннеля) не должен перебивать адрес сборки."""
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "app.exe"))
    old = tmp_path / "appdata" / "TradeAI Monitor"
    old.mkdir(parents=True)
    (old / "settings.json").write_text(json.dumps({"base_url": "http://127.0.0.1:8000", "api_key": "old"}), encoding="utf-8")
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    s = load_settings()
    assert s.base_url == build_defaults.SERVER_URL and s.api_key == build_defaults.API_KEY


def test_unwritable_user_folder_raises_oserror(monkeypatch, tmp_path):
    monkeypatch.setattr(settings_mod.os, "name", "nt")
    (tmp_path / "ro").write_text("файл вместо папки: писать нельзя", encoding="utf-8")
    monkeypatch.setenv("APPDATA", str(tmp_path / "ro"))
    with pytest.raises(OSError):
        save_settings(ClientSettings(base_url="http://h:1", api_key="z" * 40))


def test_formatting():
    assert fmt.fmt_r(1.234) == "+1.23 R" and fmt.fmt_r(-0.5) == "−0.50 R" and fmt.fmt_r(None) == fmt.DASH
    assert fmt.fmt_pct(0.5556) == "55.6%" and fmt.fmt_pct(None) == fmt.DASH
    assert fmt.fmt_pf({"profit_factor": 1.4129}) == "1.41" and fmt.fmt_pf({"profit_factor": None, "profit_factor_infinite": True}) == "∞"
    assert fmt.fmt_pf({"profit_factor": None, "profit_factor_infinite": False}) == fmt.DASH
    assert fmt.fmt_price(97123.456789) == "97123.46" and fmt.fmt_price(2539.6203) == "2539.62" and fmt.fmt_price(0.0000043) == "0.00000430"
    assert fmt.fmt_price(1.08234567) == "1.08235" and fmt.fmt_price(31.45678) == "31.457" and fmt.fmt_price(2.5) == "2.50000" and fmt.fmt_price(None) == fmt.DASH
    assert fmt.coin_short("1000PEPEUSDT") == "1000PEPE" and fmt.coin_short("BTCUSDT") == "BTC"
    assert fmt.to_ms("2026-10-10T12:00:00.000Z") == 1_791_633_600_000 and fmt.to_ms(None) is None
    assert fmt.fmt_age(5) == "5 с назад" and fmt.fmt_age(600) == "10 мин назад" and fmt.fmt_age(7200) == "2.0 ч назад" and fmt.fmt_age(None) == fmt.DASH
    assert fmt.r_color(1) == fmt.GREEN and fmt.r_color(-1) == fmt.RED and fmt.r_color(0) == fmt.MUTED == fmt.r_color(None)


def test_setup_text_includes_reject_reason():
    assert fmt.setup_text({"state": 2, "label": "свип + FVG", "reject_label": None}) == "S2 · свип + FVG"
    assert fmt.setup_text({"state": 3, "label": "CHoCH отклонён", "reject_label": "плечо выше потолка"}) == "S3 · CHoCH отклонён (плечо выше потолка)"


def test_strategy_filters_cover_the_five_shadow_strategies_and_legacy_names():
    ids = ["SMC_Pro_Trend", "SMC_Counter_Trend", "OBI_Breakout", "ICT_Killzones", "VWAP_Reversion"]
    assert sorted(k for k, _ in fmt.STRATEGY_OPTIONS[1:6]) == sorted(ids)  # пять активных стратегий идут перед архивом
    assert {"smc", "breakout"} <= set(fmt.STRATEGY_RU)  # сделки до мульти-стратегийного релиза остаются видны в фильтрах
    assert all(fmt.STRATEGY_RU[i] for i in ids) and {"SOLUSDT", "NEARUSDT", "BTCUSDT"} <= set(fmt.COINS) and fmt.EXIT_RU["scale_2r"]


def test_strategy_filter_groups_live_shadow_archive_in_that_order():
    opts = fmt.STRATEGY_OPTIONS
    assert [k for k, _ in opts] == ["all", "SMC_Pro_Trend", "VWAP_Reversion", "SMC_Counter_Trend", "OBI_Breakout", "ICT_Killzones", "Horizontal_Breakout",
                                     "Trendline_Breakout", "Horizontal_Breakout_CHR", "Trendline_Breakout_CHR", "smc", "breakout"]
    labels = dict(opts)
    assert labels["SMC_Pro_Trend"] == "🟢 [LIVE] SMC Pro Trend" and labels["VWAP_Reversion"].startswith("🟢 [LIVE]")
    assert all(labels[k].startswith("🟡 [SHADOW]") for k in ("OBI_Breakout", "SMC_Counter_Trend", "ICT_Killzones", "Horizontal_Breakout", "Trendline_Breakout",
                                                                 "Horizontal_Breakout_CHR", "Trendline_Breakout_CHR"))
    assert labels["Trendline_Breakout_CHR"] == "🟡 [SHADOW] Наклонка (CHR)"
    assert labels["smc"] == "⚪ [АРХИВ] SMC Swing (до 11.10)" and labels["breakout"].startswith("⚪ [АРХИВ]")
    assert fmt.strategy_label("UNKNOWN") == "⚪ [АРХИВ] UNKNOWN"
    assert [k for k, _ in fmt.MODE_OPTIONS] == ["all", "live", "shadow"] and fmt.MODE_OPTIONS[1][1] == "🟢 Только LIVE"


def test_account_plaque_and_default_mode():
    live = {"available": True, "balance": 1234.5, "equity": 1250.0}
    assert fmt.account_text(live) == "Баланс: *** USDT" and fmt.default_mode(live) == "live"  # по умолчанию сумма скрыта, Equity в UI нет
    assert fmt.account_text(live, True) == "Баланс: $1,234.50 USDT" and "Эквити" not in fmt.account_text(live, True)
    for none in (None, {}, {"available": False, "balance": None, "equity": None}, {"available": True, "balance": None, "equity": 1}):
        assert fmt.account_text(none) == "Shadow Mode (Тест)"
    assert fmt.default_mode(None) == "shadow" and fmt.default_mode({"available": False}) == "shadow"


def test_restart_status_is_human_readable():
    assert fmt.EXIT_RU["process_restart"] == "Прервана (Рестарт бота)" and "в WinRate не входит" in fmt.RESTART_HINT


def test_ui_preferences_roundtrip(tmp_path):
    path = tmp_path / "settings.json"
    assert not ClientSettings().balance_visible and ClientSettings().mode_filter == ""
    save_settings(ClientSettings(base_url="http://127.0.0.1:8000", balance_visible=True, mode_filter="all"), path)
    loaded = load_settings(path, env={})
    assert loaded.balance_visible is True and loaded.mode_filter == "all"
