"""Профили SANTA/CHR, мультивыбор монет, избранное (звёздочка), автообновление, картинка эталона."""

import asyncio
import hashlib
import json
import time
from unittest.mock import MagicMock

import flet as ft
import pytest
from aiohttp import web

from tradeai_client import fmt, profiles, updater
from tradeai_client.api import ApiClient, ApiError
from tradeai_client.golden_image import parse_color, render_png
from tradeai_client.settings import ClientSettings, load_settings, save_settings
from tradeai_client.ui import widgets as w
from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.history_view import HistoryView
from tradeai_client.ui.stats_view import StatsView

KEY = "k" * 40


@pytest.fixture(autouse=True)
def detached_controls(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)


class Host:
    """Минимальный Shell: профиль, запуск корутин, запоминание режима."""

    def __init__(self, profile=profiles.SANTA, extra=(), client=None):
        self.profile, self.extra, self.client = profile, list(extra), client
        self.default_mode, self.saved_mode, self.runs, self.toggled = "shadow", "", [], []
        self.page = MagicMock()

    def profile_strategies(self):
        return profiles.strategies_for(self.profile)

    def profile_coins(self):
        return profiles.coins_for(self.profile, self.extra)

    def remember_mode(self, mode):
        pass

    def run(self, coro):
        self.runs.append(coro)
        asyncio.run(coro)

    async def toggle_favorite(self, trade):
        self.toggled.append(trade["id"])
        trade["is_favorite"] = not trade.get("is_favorite")
        return trade["is_favorite"]


class FakeClient:
    def __init__(self, fx):
        self.fx, self.trade_calls, self.stat_calls = fx, [], []

    async def trades(self, **kw):
        self.trade_calls.append(kw)
        return self.fx("trades.json")

    async def stats(self, **kw):
        self.stat_calls.append(kw)
        return self.fx("stats.json")


# ======================================================================================== профили


def test_normalize_coin_maps_short_names_to_real_symbols():
    n = profiles.normalize_coin
    assert [n(x) for x in ("eur", "GBP", "jpy", "xau", " btc ", "SOL")] == ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSDT", "SOLUSDT"]
    assert [n(x) for x in ("BTCUSDT", "EURUSD", "USDJPY", "1000PEPE")] == ["BTCUSDT", "EURUSD", "USDJPY", "1000PEPEUSDT"]
    for bad in ("", "  ", "a/b", "рубль", "X"):
        with pytest.raises(profiles.ProfileError):
            n(bad)


def test_profile_scopes():
    assert profiles.strategies_for("SANTA") is None and profiles.coins_for("SANTA", ["SOLUSDT"]) is None
    assert profiles.strategies_for("CHR") == ["Trendline_Breakout_CHR", "Horizontal_Breakout_CHR"]
    assert profiles.coins_for("CHR") == ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSDT", "ETHUSDT"]
    assert profiles.coins_for("CHR", ["SOLUSDT", "BTCUSDT"])[-1] == "SOLUSDT" and profiles.coins_for("CHR", ["BTCUSDT"]).count("BTCUSDT") == 1
    assert profiles.normalize_profile("???") == "SANTA" and profiles.normalize_profile(None) == "SANTA" and profiles.normalize_profile("CHR") == "CHR"


def test_profile_and_extra_coins_are_remembered_between_launches(tmp_path):
    path = tmp_path / "settings.json"
    s = ClientSettings(base_url="http://127.0.0.1:1", api_key="x", profile="CHR", chr_extra_coins=["SOLUSDT"])
    save_settings(s, path)
    loaded = load_settings(path, env={})
    assert loaded.profile == "CHR" and loaded.chr_extra_coins == ["SOLUSDT"]
    path.write_text(json.dumps({"profile": "хакер", "chr_extra_coins": "SOL"}), encoding="utf-8")
    broken = load_settings(path, env={})
    assert broken.profile == "SANTA" and broken.chr_extra_coins == []  # ошибка в файле не роняет запуск
    assert load_settings(tmp_path / "нет.json", env={}).profile == "SANTA"  # по умолчанию - SANTA


def test_chr_balance_always_starts_from_zero_and_follows_only_pnl():
    assert fmt.chr_balance_text(None, True) == "Баланс CHR: $0.00" and fmt.chr_balance_text(0.0, True) == "Баланс CHR: $0.00"
    assert fmt.chr_balance_text(12.5, True) == "Баланс CHR: $12.50" and fmt.chr_balance_text(-5.0, True) == "Баланс CHR: −$5.00"
    assert fmt.chr_balance_text(12.5, False) == "Баланс CHR: ***"
    assert fmt.fmt_usd(None) == fmt.DASH and fmt.fmt_usd(3.0) == "+$3.00" and fmt.fmt_usd(-2.5) == "−$2.50" and fmt.fmt_usd(0) == "$0.00"


# ======================================================================================== мультивыбор


def make_multi(options=None, calls=None):
    calls = calls if calls is not None else []
    host = Host()
    m = w.MultiSelect("Монеты", options or [("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"), ("EURUSD", "EURUSD")], lambda: calls.append(m.filter()), host)
    return m, calls


def test_multiselect_all_selected_means_no_filter_and_summary_is_readable():
    m, _ = make_multi()
    assert m.filter() is None and m.summary() == "Монеты: все (3)"
    m.set_selected(["BTCUSDT"])
    assert m.filter() == ["BTCUSDT"] and m.summary() == "Монеты: BTC"
    m.set_selected(["BTCUSDT", "ETHUSDT"])
    assert m.summary() == "Монеты: BTC, ETH"
    m.set_selected([])
    assert m.is_empty and m.summary() == "Монеты: ничего" and m.filter() == []
    m.set_selected(["BTCUSDT", "ETHUSDT", "EURUSD"])
    assert m.filter() is None


def test_multiselect_new_options_join_when_everything_was_selected_else_selection_is_kept():
    m, _ = make_multi()
    m.set_options([("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"), ("EURUSD", "EURUSD"), ("SOLUSDT", "SOL")])
    assert m.filter() is None and "SOLUSDT" in m.selected  # появившаяся монета видна сразу
    m.set_selected(["BTCUSDT"])
    m.set_options([("BTCUSDT", "BTC"), ("SOLUSDT", "SOL"), ("NEWUSDT", "NEW")])
    assert m.selected == ["BTCUSDT"] and m.filter() == ["BTCUSDT"]  # выбор пользователя не перезаписывается, ушедшие варианты выпадают


def test_multiselect_click_updates_instantly_with_debounce_and_has_select_all_buttons():
    m, calls = make_multi()
    m.DEBOUNCE_S = 0.01

    async def body():
        m._toggle("ETHUSDT", False)  # три быстрых клика -> одно обновление по последнему
        m._toggle("EURUSD", False)
        m._toggle("BTCUSDT", False)
        await asyncio.sleep(0.05)

    host_runs = []
    m._host.run = lambda coro: host_runs.append(asyncio.ensure_future(coro))  # не блокирующий запуск: корутины живут в одном цикле
    asyncio.run(body())
    assert m.is_empty and m.summary() == "Монеты: ничего"
    m._open()  # диалог: окно ограничено по высоте, есть «Выделить всё / Снять всё»
    dialog = m._host.page.show_dialog.call_args.args[0]
    column = dialog.content.content
    tools, rows = column.controls
    assert [b.content for b in tools.controls] == ["Выделить всё", "Снять всё"]
    m._host.run = lambda coro: coro.close()  # дальше цикла нет: обновления по кликам не запускаем
    assert rows.scroll is not None and rows.height <= w.MultiSelect.MAX_LIST_HEIGHT
    tools.controls[0].on_click(None)
    assert m.filter() is None and all(box.value for box in m._boxes.values())
    tools.controls[1].on_click(None)
    assert m.is_empty and not any(box.value for box in m._boxes.values())


def test_multiselect_list_height_is_capped_for_long_lists():
    m, _ = make_multi([(f"C{i}USDT", f"C{i}") for i in range(60)])
    m._open()
    rows = m._host.page.show_dialog.call_args.args[0].content.content.controls[1]
    assert rows.height == w.MultiSelect.MAX_LIST_HEIGHT and rows.scroll is not None and len(rows.controls) == 60


# ======================================================================================== профиль в видах


def test_stats_filters_for_chr_use_its_strategies_and_coin_pool_and_hide_mode(fx):
    santa = StatsView(Host())
    f = santa._filters()
    assert f["strategies"] is None and f["coins"] is None and santa._mode.visible is not False
    chr_view = StatsView(Host("CHR", ["SOLUSDT"]))
    f = chr_view._filters()
    assert f["strategies"] == ["Trendline_Breakout_CHR", "Horizontal_Breakout_CHR"] and f["mode"] == "shadow"
    assert f["coins"] == profiles.coins_for("CHR", ["SOLUSDT"]) and chr_view._mode.visible is False
    assert [k for k, _ in chr_view._coin._options] == f["coins"] and [o.key for o in chr_view._strategy.options] == ["all", *profiles.CHR_STRATEGIES]
    chr_view._coin.set_selected(["BTCUSDT"])
    chr_view._strategy.value = "Horizontal_Breakout_CHR"
    f = chr_view._filters()
    assert f["coins"] == ["BTCUSDT"] and f["strategies"] == ["Horizontal_Breakout_CHR"]


def test_stats_render_for_chr_shows_pnl_from_zero(fx):
    data = fx("stats.json")
    data["overall"]["net_pnl"] = 25.0
    data["equity_curve"] = [{"ts": "2026-10-10T10:00:00Z", "cum_r": 1.0, "cum_pnl": 5.0}, {"ts": "2026-10-10T11:00:00Z", "cum_r": 3.0, "cum_pnl": 15.0}]
    shown = [x.value for c in StatsView(Host("CHR"))._render(data) for x in _walk(c) if isinstance(x, ft.Text) and x.value]
    assert "+$25.00" in shown and any("Накопленный PnL (USDT) от нуля" in t for t in shown)
    shown = [x.value for c in StatsView(Host())._render(data) for x in _walk(c) if isinstance(x, ft.Text) and x.value]
    assert "+$25.00" not in shown and any(t.startswith("Накопленный R") for t in shown)


def test_empty_coin_selection_queries_nothing(fx):
    client = FakeClient(fx)
    v = StatsView(Host(client=client))
    v._bar.set(True)  # «ничего не выбрано» возможно только в мультивыборе
    v._coin.set_selected([])
    asyncio.run(v.refresh())
    assert client.stat_calls == []
    h = HistoryView(Host(client=client))
    h._bar.set(True)
    h._coin.set_selected([])
    asyncio.run(h.refresh())
    assert client.trade_calls == [] and h._total == 0


def test_history_chr_scope_and_shared_favorites_tab(fx):
    client = FakeClient(fx)
    host = Host("CHR", client=client)
    hist, fav = HistoryView(host), HistoryView(host, favorites=True)
    asyncio.run(hist.refresh())
    asyncio.run(fav.refresh())
    h_call, f_call = client.trade_calls
    assert h_call["strategies"] == list(profiles.CHR_STRATEGIES) and h_call["coins"] == profiles.coins_for("CHR") and h_call["mode"] == "shadow" and not h_call["favorite"]
    assert f_call["favorite"] is True and f_call["strategies"] is None and f_call["coins"] is None and f_call["mode"] == "all"  # избранное общее: профиль его не режет
    santa_fav = HistoryView(Host("SANTA", client=client), favorites=True)
    asyncio.run(santa_fav.refresh())
    assert client.trade_calls[-1] == f_call


def test_history_star_toggles_via_host_and_updates_the_row(fx):
    host = Host()
    v = HistoryView(host)
    v._items = [dict(t, is_favorite=False) for t in fx("trades.json")["items"]]
    v._rebuild()
    row = v._rows.controls[0]
    star = next(x for x in _walk(row) if isinstance(x, ft.IconButton) and x.icon == ft.Icons.STAR_BORDER)
    star.on_click(None)
    assert host.toggled == [v._items[0]["id"]] and v._items[0]["is_favorite"] is True
    star = next(x for x in _walk(v._rows.controls[0]) if isinstance(x, ft.IconButton) and x.icon == ft.Icons.STAR)
    assert star.tooltip == "Убрать из избранного"
    v.set_favorite(v._items[1]["id"], True)  # звёздочку поставили из окна графика
    assert v._items[1]["is_favorite"] is True


def test_dashboard_for_chr_shows_only_its_coins_and_positions(fx):
    msg = fx("live_state.json")
    full = DashboardView(Host())
    full.on_message(msg)
    chr_dash = DashboardView(Host("CHR"))
    chr_dash.on_message(msg)
    full_text = [x.value for x in _walk(full._cards) if isinstance(x, ft.Text) and x.value]
    chr_text = [x.value for x in _walk(chr_dash._cards) if isinstance(x, ft.Text) and x.value]
    assert "DOGE" in full_text and "DOGE" not in chr_text and "BTC" in chr_text  # DOGE вне пула CHR
    assert not any(t.startswith("S2 ·") or t.startswith("S3 ·") for t in chr_text)  # автомат SMC для CHR не показывается


def _walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from _walk(x)


# ======================================================================================== Shell: профиль, избранное


def make_shell(monkeypatch, tmp_path, profile="SANTA"):
    from tradeai_client.ui import shell as shell_mod

    saved = []
    monkeypatch.setattr(shell_mod, "load_settings", lambda: ClientSettings(profile=profile))
    monkeypatch.setattr(shell_mod, "save_settings", lambda s, path=None: saved.append((s.profile, list(s.chr_extra_coins))))
    sh = shell_mod.Shell(MagicMock())
    sh.run = lambda coro: asyncio.run(coro)
    return sh, saved


def test_switching_profile_is_saved_and_reconfigures_every_view(monkeypatch, tmp_path, fx):
    sh, saved = make_shell(monkeypatch, tmp_path)
    sh.client = FakeClient(fx)
    sh.client.info = lambda: asyncio.sleep(0, result={"coins": ["BTCUSDT"], "account": None})
    asyncio.run(sh.set_profile("CHR"))
    assert saved[-1][0] == "CHR" and sh.profile_strategies() == list(profiles.CHR_STRATEGIES)
    assert sh.stats._mode.visible is False and sh.history._mode.visible is False
    assert [k for k, _ in sh.stats._coin._options] == profiles.coins_for("CHR")
    assert sh._chr_pnl is not None and sh._account.value.startswith("Баланс CHR:")  # баланс считается по стратегиям CHR, а не по счёту биржи
    assert sh.client.stat_calls[-1]["strategies"] == list(profiles.CHR_STRATEGIES) and sh.client.stat_calls[-1]["mode"] == "shadow"
    asyncio.run(sh.set_profile("SANTA"))
    assert saved[-1][0] == "SANTA" and sh.profile_strategies() is None and sh.stats._mode.visible is not False
    assert sh._profile_btn.content == "Пользователь: SANTA"


def test_chr_balance_text_in_header_starts_at_zero_before_any_trade(monkeypatch, tmp_path):
    sh, _ = make_shell(monkeypatch, tmp_path, profile="CHR")
    sh.settings.balance_visible = True
    sh._render_account()
    assert sh._account.value == "Баланс CHR: $0.00"
    sh._chr_pnl = -3.0
    sh._render_account()
    assert sh._account.value == "Баланс CHR: −$3.00"


def test_chr_coin_pool_changes_propagate_and_persist(monkeypatch, tmp_path):
    sh, saved = make_shell(monkeypatch, tmp_path, profile="CHR")
    sh.settings.chr_extra_coins = ["SOLUSDT"]
    sh._after_coin_pool_change("x")
    assert saved[-1] == ("CHR", ["SOLUSDT"]) and "SOLUSDT" in [k for k, _ in sh.history._coin._options]


class StarClient:
    def __init__(self):
        self.added, self.removed = [], []

    async def favorite_add(self, tid, profile, png=None):
        self.added.append((tid, profile, png))
        return {}

    async def favorite_remove(self, tid):
        self.removed.append(tid)
        return {}


def test_shell_star_sends_profile_and_picture_then_removes(monkeypatch, tmp_path):
    sh, _ = make_shell(monkeypatch, tmp_path, profile="CHR")
    sh.client = StarClient()

    async def fake_png(trade):
        return b"\x89PNG-fake", ""

    sh._golden_png = fake_png
    trade = {"id": "BTCUSDT:CHR:1", "has_snapshot": True}
    assert asyncio.run(sh.toggle_favorite(trade)) is True and trade["is_favorite"] is True
    assert sh.client.added == [("BTCUSDT:CHR:1", "CHR", b"\x89PNG-fake")]
    assert asyncio.run(sh.toggle_favorite(trade)) is False and sh.client.removed == ["BTCUSDT:CHR:1"]


def test_shell_star_survives_picture_failure_and_reports_server_errors(monkeypatch, tmp_path):
    sh, _ = make_shell(monkeypatch, tmp_path)
    sh.client = StarClient()
    trade = {"id": "x", "has_snapshot": False}
    assert asyncio.run(sh.toggle_favorite(trade)) is True and sh.client.added == [("x", "SANTA", None)]  # без снапшота - данные без картинки

    class Broken(StarClient):
        async def favorite_add(self, *a, **k):
            raise ApiError("server", "boom")

    sh.client = Broken()
    t2 = {"id": "y", "has_snapshot": False}
    assert asyncio.run(sh.toggle_favorite(t2)) is None and not t2.get("is_favorite")  # ошибка сервера: состояние не меняется


# ======================================================================================== API-клиент


def test_api_client_favorite_update_and_trade_filters():
    seen = []

    async def body():
        app = web.Application()

        async def guard(request):
            seen.append((request.method, request.path, dict(request.query), request.headers.get("X-API-Key"), await request.read()))
            return request.headers.get("X-API-Key") == KEY

        async def ok(request):
            return web.json_response({"ok": True}) if await guard(request) else web.json_response({"detail": "invalid_api_key"}, status=401)

        async def no_version(request):
            await guard(request)
            return web.json_response({"detail": "no_update_published"}, status=404)

        async def exe(request):
            await guard(request)
            return web.Response(body=b"M" * 1000)

        app.router.add_get("/api/trades", ok)
        app.router.add_put("/api/favorites/{tid}", ok)
        app.router.add_delete("/api/favorites/{tid}", ok)
        app.router.add_get("/api/update/version.json", no_version)
        app.router.add_get("/api/update/TradeAI-Monitor.exe", exe)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        client = ApiClient(ClientSettings(base_url=f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}", api_key=KEY))
        try:
            await client.trades(favorite=True, mode="all")
            await client.favorite_add("BTC:1:live", "CHR", b"\x89PNGdata")
            await client.favorite_add("BTC:2", "SANTA")
            await client.favorite_remove("BTC:1:live")
            version = await client.update_version()
            chunks = [c async for c in client.download_update()]
            return version, b"".join(chunks)
        finally:
            await client.close()
            await runner.cleanup()

    version, blob = asyncio.run(body())
    assert version is None and blob == b"M" * 1000
    assert seen[0][2]["favorite"] == "true" and seen[0][2]["mode"] == "all"
    method, path, _, key, raw = seen[1]
    assert (method, path, key) == ("PUT", "/api/favorites/BTC:1:live", KEY) and json.loads(raw)["profile"] == "CHR" and json.loads(raw)["image_png_b64"]
    assert "image_png_b64" not in json.loads(seen[2][4]) and seen[3][0] == "DELETE"
    assert all(k == KEY for _, _, _, k, _ in seen)


# ======================================================================================== автообновление


def test_version_comparison_and_info_validation():
    assert updater.is_newer("0.2.0", "0.1.0") and updater.is_newer("0.10.0", "0.9.9") and updater.is_newer("1.0", "0.9.9") and not updater.is_newer("0.1.0", "0.1.0")
    assert not updater.is_newer("0.1.0", "0.1.1") and not updater.is_newer("0.1", "0.1.0")
    sha = "a" * 64
    info = updater.parse_info({"version": "0.2.0", "sha256": sha, "size": 10, "notes": "правки"})
    assert (info.version, info.size, info.notes) == ("0.2.0", 10, "правки")
    for bad in ({}, {"version": "x", "sha256": sha, "size": 1}, {"version": "0.2.0", "sha256": "zz", "size": 1}, {"version": "0.2.0", "sha256": sha, "size": 0},
                {"version": "0.2.0", "sha256": sha, "size": "много"}):
        with pytest.raises(updater.UpdateError):
            updater.parse_info(bad)


class UpdClient:
    def __init__(self, raw=None, chunks=(), error=None):
        self.raw, self.chunks, self.error = raw, chunks, error

    async def update_version(self):
        if self.error:
            raise self.error
        return self.raw

    async def download_update(self):
        for c in self.chunks:
            yield c


def test_check_for_update_is_silent_on_trouble_and_reports_only_newer():
    sha = "b" * 64
    newer = {"version": "9.9.9", "sha256": sha, "size": 5}
    assert asyncio.run(updater.check_for_update(UpdClient(newer), "0.1.0")).version == "9.9.9"
    assert asyncio.run(updater.check_for_update(UpdClient(newer), "9.9.9")) is None
    assert asyncio.run(updater.check_for_update(UpdClient(None), "0.1.0")) is None  # ничего не опубликовано
    assert asyncio.run(updater.check_for_update(UpdClient(error=ApiError("network", "нет сети")), "0.1.0")) is None
    assert asyncio.run(updater.check_for_update(UpdClient({"version": "oops"}), "0.1.0")) is None  # битый version.json не мешает работе


def test_download_verifies_size_and_hash(tmp_path):
    data = b"MZ" + b"x" * 5000
    info = updater.UpdateInfo("0.2.0", hashlib.sha256(data).hexdigest(), len(data))
    progress = []
    out = asyncio.run(updater.download(UpdClient(chunks=[data[:3000], data[3000:]]), info, tmp_path / "t.new", progress.append))
    assert out.read_bytes() == data and progress[-1] == 1.0 and not list(tmp_path.glob("*.part"))
    bad = updater.UpdateInfo("0.2.0", "0" * 64, len(data))
    with pytest.raises(updater.UpdateError, match="повреждён"):
        asyncio.run(updater.download(UpdClient(chunks=[data]), bad, tmp_path / "bad.new"))
    assert not (tmp_path / "bad.new").exists() and not list(tmp_path.glob("*.part"))  # битый файл не остаётся
    short = updater.UpdateInfo("0.2.0", info.sha256, len(data) + 1)
    with pytest.raises(updater.UpdateError):
        asyncio.run(updater.download(UpdClient(chunks=[data]), short, tmp_path / "short.new"))


def test_swap_script_replaces_exe_waits_and_restarts(tmp_path):
    script = updater.write_swap_script(tmp_path / "A.exe.new", tmp_path / "A.exe", tmp_path)
    text = script.read_text(encoding="utf-8")
    assert f'set "NEW={tmp_path / "A.exe.new"}"' in text and 'move /Y "%NEW%" "%EXE%"' in text and 'start "" "%EXE%"' in text and "goto retry" in text
    assert "\r\n" in script.read_bytes().decode("utf-8")
    with pytest.raises(updater.UpdateError):
        updater.write_swap_script(tmp_path / 'ev"il.new', tmp_path / "A.exe", tmp_path)


def test_update_offer_and_install_only_from_a_built_exe(monkeypatch, tmp_path):
    sh, _ = make_shell(monkeypatch, tmp_path)
    info = updater.UpdateInfo("9.9.9", "c" * 64, 100, "что нового")
    sh.client = MagicMock()
    sh._offer_update(info)
    dialog = sh.page.show_dialog.call_args.args[0]
    assert [a.content for a in dialog.actions] == ["Позже", "Обновить"]
    monkeypatch.setattr(updater, "is_frozen", lambda: False)
    notes = []
    sh.notify = lambda text, error=False: notes.append(text)
    asyncio.run(sh._install_update(info))
    assert notes and "собранном .exe" in notes[0]


# ======================================================================================== картинка эталона


def test_golden_png_is_rendered_from_the_chart_spec(fx):
    from tradeai_client.chart_spec import build_chart_spec

    snap = fx("snapshot_smc_short.json")
    png = render_png(build_chart_spec(snap, None, now_ms=int(time.time() * 1000)))
    assert png.startswith(b"\x89PNG\r\n\x1a\n") and 5_000 < len(png) < 4_000_000  # лимит сервера - 4 МБ
    assert parse_color("#2bb673") == (43, 182, 115, 255) and parse_color("rgba(10,20,30,0.5)")[3] == 127 and parse_color("мусор")[3] == 255


def test_significance_card_sits_in_the_same_row_as_the_filters_on_wide_screens_and_below_them_on_phones():
    """Statistics (2026-10-20): ResponsiveRow - на широком экране карточка «Значимость выводов» в одной строке с фильтрами справа (md 9 + 3), на узком (телефон) - под фильтрами (xs 12);
    раньше фильтры сжимались рядом с карточкой до 200 px и текст налезал друг на друга. Analysis - прежняя строка."""
    from tradeai_client.ui.analysis_view import AnalysisView

    stats = StatsView(Host())
    rr = next(c for c in stats.control.controls if isinstance(c, ft.ResponsiveRow))
    filters, goal_box = rr.controls
    assert stats._mode.control in filters.controls and filters.wrap and filters.col == {"xs": 12, "md": 9}
    assert stats._goal.control in goal_box.controls and goal_box.col == {"xs": 12, "md": 3}
    assert not any(getattr(c, "expand", None) for c in filters.controls)  # в Row(wrap=True) нельзя класть expand-элементы (Flutter ErrorWidget)
    analysis = AnalysisView(Host())
    rows = [c for c in analysis.control.controls if isinstance(c, ft.Row) and analysis._goal.control in c.controls]
    assert len(rows) == 1 and rows[0].controls[-1] is analysis._goal.control and rows[0].controls[0].wrap


# ======================================================================================== тумблер «Мультивыбор» для всех фильтров


def test_filters_are_plain_dropdowns_until_multiselect_is_switched_on():
    """ВЫКЛ (по умолчанию): все фильтры, включая монеты, - обычные Dropdown; ВКЛ: каждый - кнопка, открывающая диалог с чекбоксами; обратно - снова Dropdown."""
    h = HistoryView(Host())
    s = StatsView(Host())
    for view, names in ((h, ("_mode", "_status", "_coin", "_strategy")), (s, ("_mode", "_coin", "_strategy", "_side"))):
        assert view._bar.switch.value is False
        for name in names:
            assert isinstance(getattr(view, name).control.content, ft.Dropdown), name
        view._bar.set(True)
        for name in names:
            f = getattr(view, name)
            assert f.multi and isinstance(f.control.content, ft.OutlinedButton), name
        view._bar.set(False)
        for name in names:
            assert isinstance(getattr(view, name).control.content, ft.Dropdown), name
    assert isinstance(s._period, ft.Dropdown)  # период - один диапазон времени: мультивыбора нет


def test_filter_select_carries_the_selection_between_views():
    f = w.FilterSelect("Стратегия", [("a", "A"), ("b", "B"), ("c", "C")], lambda: None, Host())
    assert f.keys() is None and [o.key for o in f.options] == ["all", "a", "b", "c"]
    f.value = "b"
    f.set_multi(True)
    assert f.keys() == ["b"]  # один вариант -> одна отметка
    f._multi.set_selected(["a", "c"])
    assert f.keys() == ["a", "c"]
    f.set_multi(False)
    assert f.value == "all" and f.keys() is None  # несколько отметок в обычный вид не помещаются -> «Все»
    f.set_multi(True)
    assert f.keys() is None  # «Все» -> все отмечены -> без фильтра
    f._multi.set_selected([])
    assert f.is_empty
    f.set_options([("a", "A"), ("z", "Z")])
    assert f.is_empty and [k for k, _ in f._options] == ["a", "z"]
    f.set_selected(["z"])
    assert f.keys() == ["z"] and f.value == "z"


def test_multiselect_of_strategies_statuses_modes_and_sides_reaches_the_requests(fx):
    client = FakeClient(fx)
    h = HistoryView(Host(client=client))
    h._bar.set(True)
    h._strategy.set_selected(["SMC_Pro_Trend", "VWAP_Reversion"])
    h._status._multi.set_selected(["closed", "active"])
    h._mode._multi.set_selected(["live"])
    h._coin.set_selected(["BTCUSDT"])
    asyncio.run(h.refresh())
    call = client.trade_calls[-1]
    assert call["strategies"] == ["SMC_Pro_Trend", "VWAP_Reversion"] and call["statuses"] == ["closed", "open", "pending_limit", "pending_market"]
    assert call["mode"] == "live" and call["coins"] == ["BTCUSDT"]
    h._mode._multi.set_selected(["live", "shadow"])  # оба контура = all
    h._status._multi.set_selected(["closed", "cancelled", "aborted", "active"])
    asyncio.run(h.refresh())
    assert client.trade_calls[-1]["mode"] == "all"

    s = StatsView(Host(client=client))
    s._bar.set(True)
    s._strategy.set_selected(["SMC_Pro_Trend", "OBI_Breakout"])
    s._side._multi.set_selected(["long"])
    asyncio.run(s.refresh())
    f = client.stat_calls[-1]
    assert f["strategies"] == ["SMC_Pro_Trend", "OBI_Breakout"] and f["sides"] == ["long"]
    s._side._multi.set_selected(["long", "short"])  # обе стороны = без фильтра
    assert s._filters()["sides"] is None


def test_any_empty_multiselect_filter_queries_nothing(fx):
    client = FakeClient(fx)
    h = HistoryView(Host(client=client))
    h._bar.set(True)
    h._strategy.set_selected([])
    h._status._multi.set_selected([])
    asyncio.run(h.refresh())
    assert client.trade_calls == [] and h._total == 0
    s = StatsView(Host(client=client))
    s._bar.set(True)
    s._side._multi.set_selected([])
    asyncio.run(s.refresh())
    assert client.stat_calls == [] and "Сторона" in s._body.controls[0].value


def test_switching_the_toggle_refreshes_and_single_mode_still_filters_by_one_value(fx):
    client = FakeClient(fx)
    h = HistoryView(Host(client=client))
    h._status.value = "closed"
    h._strategy.value = "VWAP_Reversion"
    asyncio.run(h.refresh())
    call = client.trade_calls[-1]
    assert call["statuses"] == ["closed"] and call["strategies"] == ["VWAP_Reversion"]
    h._bar.switch.value = True
    asyncio.run(h._bar._changed())  # тумблер: переключает фильтры и перезапрашивает таблицу
    assert h._status.multi and len(client.trade_calls) == 2 and client.trade_calls[-1]["strategies"] == ["VWAP_Reversion"]
    h._bar.switch.value = False
    asyncio.run(h._bar._changed())
    assert not h._status.multi and len(client.trade_calls) == 3


# ======================================================================================== History: таблица листается по горизонтали, не сжимается


def test_history_table_keeps_its_width_and_scrolls_horizontally_with_a_fixed_header():
    from tradeai_client.ui import history_view as hv

    v = HistoryView(Host())
    scrollers = [c for c in v.control.controls if isinstance(c, ft.Row) and c.scroll is not None]
    assert len(scrollers) == 1, "ровно одна горизонтально прокручиваемая область: заголовок и строки едут вместе"
    table = scrollers[0].controls[0]
    assert scrollers[0].expand and table.width == hv.TABLE_W and table.width >= sum(hv.WIDTHS)  # ширина = сумма колонок: на узком экране таблица уходит за край, а не сжимается
    header, body = table.controls
    assert body.scroll is not None and body.expand and v._rows in body.controls  # строки листаются по вертикали внутри


def test_phone_tap_opens_the_trade_chart_while_desktop_keeps_double_click(fx):
    opened = []

    class PhoneHost(Host):
        tap_opens_chart = True

        async def open_trade_chart(self, trade):
            opened.append(trade["id"])

    trade = fx("trades.json")["items"][0]
    phone = HistoryView(PhoneHost())
    row = phone._row(trade)
    assert row.on_double_tap is None and row.on_tap is not None
    row.on_tap(None)
    assert phone.host.runs or opened is not None  # тап запускает открытие графика через host.run
    desktop = HistoryView(Host())
    drow = desktop._row(trade)
    assert drow.on_double_tap is not None  # настольный клиент: одиночный клик выделяет, двойной открывает
