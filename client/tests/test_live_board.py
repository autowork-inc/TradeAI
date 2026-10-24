"""Live-Dashboard (SANTA) без запуска Flet: настоящие ответы сервиса watch (fixtures/watch_views.json, сняты с playground/watch_server на BTC) -> панели."""

import asyncio
import json
from pathlib import Path

import flet as ft
import flet.canvas as cv
import pytest

from tradeai_client import profiles
from tradeai_client.settings import ClientSettings
from tradeai_client.ui import live_board
from tradeai_client.ui.candles import candle_chart, position_levels
from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.live_board import LiveBoard

FX = json.loads((Path(__file__).parent / "fixtures" / "watch_views.json").read_text(encoding="utf-8"))
POS = {"strategy_name": "SMC_Pro_Trend", "side": "short", "status": "open", "be_armed": False, "remaining_frac": 1.0, "r_multiple": None, "entry_price": 81739.3, "planned_entry": None, "sl": 82077.28, "tp": 80387.4}


@pytest.fixture(autouse=True)
def detached(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)


def walk(c):
    yield c
    for name in ("controls", "content", "shapes"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, (ft.Control, cv.Shape)) if hasattr(cv, "Shape") else isinstance(x, ft.Control):
                yield from walk(x)


def texts(c):
    out = []
    for x in walk(c):
        if isinstance(x, ft.Text) and isinstance(x.value, str):
            out.append(x.value)
        if isinstance(x, cv.Text):
            out.append(str(x.value))
    return " | ".join(out)


class Client:
    def __init__(self):
        self.views = []

    async def watch_meta(self):
        return FX["meta"]

    async def watch_coins(self, strategy):
        coins = FX["meta"]["strategies"][strategy]["coins"]
        return {"strategy": strategy, "threshold": 2, "total": len(coins), "pending": [], "hidden": len(coins) - 1,
                "coins": [{"coin": coins[-1], "score": 3, "side": "short", "stage": 2}]}

    async def watch_view(self, strategy, symbol):
        self.views.append((strategy, symbol))
        return FX["views"].get(strategy, FX["views"]["ICT_Killzones"])


class Host:
    def __init__(self, profile=profiles.SANTA):
        self.settings = ClientSettings(base_url="http://x", api_key="k", profile=profile)
        self.client, self._tab, self.saved, self.runs = Client(), 0, {}, []
        self.page = None

    def profile_strategies(self):
        return profiles.strategies_for(self.settings.profile)

    def profile_coins(self):
        return profiles.coins_for(self.settings.profile)

    def shared_coins(self):
        return []

    def run(self, coro):
        self.runs.append(coro)
        coro.close()  # фоновый цикл обновления в тестах не запускаем

    def view_state(self, name):
        return self.saved.get(name)

    def remember_view(self, name, state):
        self.saved[name] = state


STATE = {"coins": [{"coin": "BTCUSDT", "setups": [{"strategy": "SMC_Pro_Trend", "side": "short", "state": 1, "label": "свип", "since": None, "reject": None, "reject_label": None}], "positions": [POS]}],
         "events": [], "shadow": {"alive": True, "heartbeat_age_s": 1}, "server_time": "2026-10-24T13:00:00Z"}


def test_one_live_chart_per_live_strategy_and_other_strategies_window():
    host = Host()
    b = LiveBoard(host)
    b.on_state(STATE)
    asyncio.run(b.refresh())
    assert [p.strategy for p in b.live_panels] == FX["meta"]["live_strategies"] == ["SMC_Pro_Trend"]  # одна боевая стратегия - один LIVE-график
    assert b.other is not None and b.other.strategy not in FX["meta"]["live_strategies"]
    assert ("SMC_Pro_Trend", "BTCUSDT") in host.client.views  # монета с позицией бота - в выборе всегда, даже если фильтр её не прошёл
    others = FX["meta"]["strategies"][b.other.strategy]["coins"]
    assert b.other.coin == others[-1] and [o.key for o in b.other._coin_dd.options] == [others[-1]]  # остальные: только прошедшие фильтр
    assert "3✓ ▼" in b.other._coin_dd.options[0].text and "≥2 условий" in b.other.coins_note
    body = texts(b.control)
    assert "LIVE" in body and "Позиция бота" in body and "SL 82077.28" in body.replace("SL ", "SL ") and "наблюдает" in body


def test_switching_strategy_resets_coin_and_is_remembered():
    host = Host()
    b = LiveBoard(host)
    asyncio.run(b.refresh())
    b.other._strat_dd.value = "Trendline_Breakout_CHR"
    b.other._strategy_changed()
    asyncio.run(b.other._switch())
    assert b.other.strategy == "Trendline_Breakout_CHR" and b.other.coin == FX["meta"]["strategies"]["Trendline_Breakout_CHR"]["coins"][-1]
    assert host.saved["live_board"]["other"] == ["Trendline_Breakout_CHR", b.other.coin]
    asyncio.run(b.other.load())
    assert "наклон" in texts(b.other.control)


def test_chart_draws_position_levels_and_off_scale_strategy_lines():
    v = FX["views"]["SMC_Pro_Trend"]
    chart = candle_chart(v["candles"], interval_ms=300_000, geometry=[*v["geometry"], {"type": "line", "t1": v["candles"][0]["open_time"], "t2": v["candles"][-1]["open_time"],
                                                                                        "p1": 1.0, "p2": 1.0, "label": "далёкий уровень", "side": "long"}],
                         levels=position_levels([POS]), forming=v.get("forming"))
    labels = [str(s.value) for s in chart.shapes if isinstance(s, cv.Text)]
    assert any(t.startswith("SL ") for t in labels) and any(t.startswith("вход ") for t in labels) and any(t.startswith("TP ") for t in labels)
    assert any("↓ далёкий уровень" in t for t in labels)  # за краем шкалы - подпись у края, свечи не сплющены


def test_dashboard_shows_live_board_only_for_santa():
    santa = DashboardView(Host())
    santa.on_message({**STATE, "type": "state"})
    assert santa.live.control.visible and santa._live_started
    chr_dash = DashboardView(Host(profiles.CHR))
    chr_dash.on_message({**STATE, "type": "state"})
    assert not chr_dash.live.control.visible and not chr_dash._live_started
