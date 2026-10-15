"""Интерфейс без запуска Flet: сборка представлений из настоящих ответов API и проводка обработчиков (двойной клик, фильтры)."""

import asyncio

import flet as ft
import pytest

from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.history_view import HistoryView
from tradeai_client.ui.stats_view import StatsView


@pytest.fixture(autouse=True)
def detached_controls(monkeypatch):
    """Контролы без страницы: .update() у них недопустим, подменяем на пустышку (проверяется структура и обработчики, не отрисовка)."""
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)


def walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


def texts(c) -> list[str]:
    return [x.value for x in walk(c) if isinstance(x, ft.Text) and x.value]


class Host:
    def __init__(self, client=None):
        self.client, self.opened, self.ran = client, [], []
        self.default_mode = "shadow"

    def run(self, coro):
        self.ran.append(coro)
        asyncio.run(coro)

    async def open_trade_chart(self, trade):
        self.opened.append(trade["id"])


def test_dashboard_shows_every_coin_with_state_positions_and_events(fx):
    v = DashboardView()
    v.on_message(fx("live_state.json"))
    t = texts(v.control)
    assert [x for x in texts(v._cards) if x in ("BTC", "ETH", "DOGE", "1000PEPE")] == ["BTC", "ETH", "DOGE", "1000PEPE"]
    assert any(x.startswith("S2 · свип + FVG") for x in t) and any(x.startswith("S3 · CHoCH отклонён (плечо выше потолка)") for x in t)
    assert any("Ждёт лимитку" == x for x in t) and any("Открыта" == x for x in t) and any(x.startswith("Остаток: 50%") for x in t)
    assert any("Автомат сетапа: short:S1 → short:S2" in x for x in t)
    assert any(x.startswith("Shadow") for x in t)


def test_dashboard_shows_server_errors_and_hides_them_after_recovery(fx):
    v = DashboardView()
    v.on_message(fx("live_state.json"))
    v.on_message({"type": "error", "server_time": "x", "detail": "database_unavailable"})
    assert v._error.visible and any("не может прочитать базу" in x for x in texts(v._error))
    v.on_message({"type": "ping", "server_time": "x"})  # пинг ничего не меняет
    assert v._error.visible
    v.on_message(fx("live_state.json"))
    assert not v._error.visible


def test_stats_view_renders_kpis_ranking_and_warning(fx):
    v = StatsView(Host())
    t = texts(ft.Column(v._render(fx("stats.json"))))
    assert "Win Rate" in t and "55.6%" in t and "Profit Factor" in t and "1.41" in t and "Net R" in t
    assert any(x.startswith("Лучшая: DOGE") for x in t) and "нет сделок" not in t
    assert any("выводов о прибыльности делать нельзя" in x for x in t)  # предупреждение о малой выборке
    names = [x for x in t if x in ("DOGE", "BTC", "ETH", "1000PEPE")]
    assert names[:4] == ["DOGE", "BTC", "ETH", "1000PEPE"]  # порядок по Net R


def test_stats_view_marks_coins_without_trades(fx):
    d = fx("stats.json")
    d["by_coin"][-1].update(trades=0, rank=None, net_r=0, win_rate=None, profit_factor=None, avg_r=None)
    t = texts(ft.Column(StatsView(Host())._render(d)))
    assert "нет сделок" in t


def history_with(fx):
    host = Host()
    v = HistoryView(host)
    v._items = fx("trades.json")["items"]
    v._rebuild()
    return v, host


def test_history_double_tap_and_button_open_the_chart_for_that_trade(fx):
    v, host = history_with(fx)
    rows = [c for c in v._rows.controls if isinstance(c, ft.GestureDetector)]
    assert len(rows) == len(v._items)
    rows[3].on_double_tap(None)
    assert host.opened == [v._items[3]["id"]]
    button = next(x for x in walk(rows[1]) if isinstance(x, ft.IconButton))
    button.on_click(None)
    assert host.opened[-1] == v._items[1]["id"]


def test_history_single_tap_selects_without_rebuilding_rows(fx):
    """Пересоздание строк при касании сорвало бы второй клик двойного клика (проверено на браузерном прогоне)."""
    v, host = history_with(fx)
    before = list(v._rows.controls)
    rows = [c for c in v._rows.controls if isinstance(c, ft.GestureDetector)]
    rows[2].on_tap(None)
    rows[4].on_tap(None)
    assert v._rows.controls == before and all(a is b for a, b in zip(v._rows.controls, before))
    assert v._boxes[v._items[4]["id"]].bgcolor != v._boxes[v._items[2]["id"]].bgcolor  # выбрана только последняя
    assert host.opened == []


def test_history_row_without_snapshot_has_disabled_button(fx):
    v, _ = history_with(fx)
    item = dict(v._items[0], has_snapshot=False)
    button = next(x for x in walk(v._row(item)) if isinstance(x, ft.IconButton))
    assert button.disabled is True


def test_history_row_shows_gap_warning_and_signs(fx):
    v, _ = history_with(fx)
    item = dict(v._items[0], gap_affected=True, status="closed", r_multiple=-1.5, pnl=-12.34)
    t = texts(v._row(item))
    assert any(x.startswith("Закрыта") and x.endswith("⚠") for x in t) and "−1.50 R" in t and "−12.34" in t


def test_closing_the_window_terminates_the_whole_process(monkeypatch):
    """Закрытие окна = выход процесса: окна графиков закрываются, затем os._exit(0) (иначе приложение висело в фоне)."""
    import os
    import subprocess
    import sys
    import time

    from tradeai_client import chart_window, main as app_main

    calls = []
    monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: calls.append(("kill_tree", cmd[:3], cmd[-1] == str(os.getpid()))))  # иначе taskkill убил бы сам pytest
    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setattr(chart_window, "terminate_all", lambda: calls.append("charts"))
    monkeypatch.setattr(os, "_exit", lambda code: calls.append(("exit", code)))
    app_main._shutdown()
    assert calls[0] == "charts" and calls[-1] == ("exit", 0)
    assert ("kill_tree", ["taskkill", "/F", "/T"], True) in calls or sys.platform != "win32"  # дерево процессов (с окном flet.exe) гасится целиком


def test_dashboard_splits_crypto_and_mt5_into_sections():
    from tradeai_client.ui.dashboard import CRYPTO_COL, MT5_COL, split_coins

    coins = [{"coin": c} for c in ("BTCUSDT", "EURUSD", "ETHUSDT", "XAUUSD", "XAGUSD")]
    crypto, mt5 = split_coins(coins)
    assert [c["coin"] for c in crypto] == ["BTCUSDT", "ETHUSDT"] and [c["coin"] for c in mt5] == ["EURUSD", "XAUUSD", "XAGUSD"]
    assert 12 // CRYPTO_COL["md"] == 4 and 12 // MT5_COL["md"] == 3  # 4 в ряд (8 крипто = 2 ровных ряда), 3 в ряд (3 MT5 = 1 ровный ряд)


def test_dashboard_renders_two_labelled_sections(fx):
    v = DashboardView()
    msg = fx("live_state.json")
    v.on_message(msg)
    t = texts(v._cards)
    assert any(x.startswith("Крипта (Binance)") for x in t)


def test_stats_and_history_explain_filters_and_follow_active_mode(fx):
    v = StatsView(Host())
    t = texts(ft.Column(v._render(fx("stats.json"))))
    assert any("учитываются только завершённые сделки (по TP/SL/BE)" in x and "отфильтрованы" in x for x in t)
    assert any(x.startswith("В расчёте:") for x in t)
    assert v._filters()["mode"] == "shadow" and v.follow_default_mode("live") and v._filters()["mode"] == "live"
    v._mode_touched = True
    assert not v.follow_default_mode("shadow") and v._filters()["mode"] == "live"  # выбор пользователя не перезаписывается
    h = HistoryView(Host())
    assert h._mode.value == "shadow" and h.follow_default_mode("live") and h._mode.value == "live"


def test_history_row_marks_restart_with_readable_status_and_tooltip():
    h = HistoryView(Host())
    base = {"id": "a", "signal_time": "2026-10-10T12:00:00Z", "coin": "BTCUSDT", "strategy_name": "OBI_Breakout", "mode": "shadow", "side": "long",
            "status": "aborted", "entry_price": 100.0, "exit_price": None, "r_multiple": None, "pnl": None, "exit_reason": "process_restart",
            "gap_affected": False, "has_snapshot": False}
    row = h._row(base).content
    assert "Прервана (Рестарт бота)" in texts(row) and "SHADOW" in texts(row) and row.tooltip.endswith("в WinRate не входит")
