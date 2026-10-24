"""Вкладка Playground без запуска Flet: запуск прогона, история прогонов по профилю, итоги и сделки, открытие графика сделки прогона."""

import asyncio

import flet as ft
import pytest

from tradeai_client import profiles
from tradeai_client.settings import ClientSettings
from tradeai_client.ui import playground_view
from tradeai_client.ui.playground_view import PlaygroundView

T = 1_761_264_000_000


@pytest.fixture(autouse=True)
def detached(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)
    monkeypatch.setattr(playground_view, "POLL_S", 0.0)


def walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


def texts(c):
    return " | ".join(x.value for x in walk(c) if isinstance(x, ft.Text) and isinstance(x.value, str))


def stats(closed, sum_r):
    return {"signals": closed + 1, "closed": closed, "wins": 1, "losses": closed - 1, "winrate": 1 / closed, "sum_r": sum_r, "avg_r": sum_r / closed,
            "pnl": 5 * sum_r, "profit_factor": 1.5, "max_dd_r": 2.0, "open": 0, "cancelled": 1, "exit_reasons": {"sl": closed - 1, "tp": 1}, "skipped": {},
            "curve": [[T + i, i * 0.5, i * 2.5] for i in range(1, closed + 1)]}


RUN = {"id": "20261024-100000-abcdef", "created_ms": T, "status": "done", "symbol": "BTCUSDT", "strategies": ["ICT_Killzones"], "start_ms": T,
       "end_ms": T + 30 * 86_400_000, "progress": 1, "bars": 2880, "seconds": 600, "code_version": "abc123", "error": None, "label": None,
       "summary": {"overall": stats(3, 1.5), "by_strategy": {"ICT_Killzones": stats(3, 1.5)}}}
TRADE = {"id": "ICT_Killzones:BTCUSDT:15m:1:long", "strategy_id": "ICT_Killzones", "coin": "BTCUSDT", "side": "long", "status": "closed",
         "signal_time": "2025-10-24T10:00:00.000Z", "entry_price": 100.0, "sl": 99.0, "exit_price": 104.0, "exit_reason": "tp", "r_multiple": 3.8, "pnl": 19.0,
         "has_snapshot": True}


class FakeClient:
    def __init__(self):
        self.created, self.scopes, self.status = [], [], "done"

    async def playground_runs(self, scope="santa"):
        self.scopes.append(scope)
        return {"items": [{**RUN, "status": self.status}]}

    async def playground_create(self, symbol, strategies, start, end, profile):
        self.created.append((symbol, strategies, start, end, profile))
        return {"id": RUN["id"]}

    async def playground_run(self, run_id):
        return {**RUN, "status": self.status}

    async def playground_trades(self, run_id):
        return {"items": [TRADE]}


class Host:
    def __init__(self, profile=profiles.SANTA):
        self.settings = ClientSettings(base_url="http://x", api_key="k", profile=profile)
        self.client = FakeClient()
        self.opened, self.notes = [], []

    def profile_strategies(self):
        return profiles.strategies_for(self.settings.profile)

    def run(self, coro):
        asyncio.run(coro)

    def notify(self, text, error=False):
        self.notes.append(text)

    async def open_trade_chart(self, trade):
        self.opened.append(trade)


def test_chr_profile_runs_only_its_strategies_and_sees_its_history():
    host = Host(profiles.CHR)
    v = PlaygroundView(host)
    assert set(v._strategies.selected) == set(profiles.CHR_STRATEGIES)
    v._coin.value = "eth"
    asyncio.run(v._run_clicked())
    sym, names, *_ , profile = host.client.created[0]
    assert sym == "ETHUSDT" and set(names) == set(profiles.CHR_STRATEGIES) and profile == "CHR" and host.client.scopes[-1] == "chr"


def test_santa_has_all_strategies_but_obi_and_rejects_forex():
    host = Host()
    v = PlaygroundView(host)
    assert "OBI_Breakout" not in v._strategies.selected and "SMC_Pro_Trend" in v._strategies.selected and "Trendline_Breakout_CHR" in v._strategies.selected
    v._coin.value = "EURUSD"
    asyncio.run(v._run_clicked())
    assert host.client.created == [] and "крипту" in host.notes[-1]


def test_done_run_shows_summary_trades_and_opens_chart_of_playground_trade():
    host = Host()
    v = PlaygroundView(host)
    asyncio.run(v.refresh())
    assert "BTC" in texts(v._runs) and "+1.50 R" in texts(v._runs)
    asyncio.run(v._open(RUN["id"]))
    body = texts(v._detail)
    assert "Сумма R" in body and "ICT Killzones" in body and "Тейк" in body and "+3.80 R" in body
    row = next(x for x in walk(v._detail) if isinstance(x, ft.Container) and x.tooltip == "Открыть график сделки")
    row.on_click(None)
    assert host.opened[0]["playground_run"] == RUN["id"] and host.opened[0]["id"] == TRADE["id"]


def test_running_run_shows_progress():
    host = Host()
    host.client.status = "running"
    v = PlaygroundView(host)
    asyncio.run(v._open(RUN["id"]))
    assert "считается" in texts(v._detail)
