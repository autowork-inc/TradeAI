"""Вкладка MFE Simulator без запуска Flet: настоящий отчёт сервера (снят с api/mfe_sim.build_report) -> виджеты; запуск и опрос задания."""

import asyncio
import json
from pathlib import Path

import flet as ft
import pytest

from tradeai_client.api import ApiError
from tradeai_client.ui import mfe_view
from tradeai_client.ui.mfe_view import MfeView

REPORT = json.loads((Path(__file__).parent / "fixtures" / "mfe_report.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def detached(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)
    monkeypatch.setattr(mfe_view, "POLL_S", 0.0)


def walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


def texts(c):
    return " | ".join(x.value for x in walk(c) if isinstance(x, ft.Text) and isinstance(x.value, str))


class FakeClient:
    def __init__(self, statuses):
        self.statuses, self.runs = list(statuses), []

    async def mfe_run(self, since, until=None, scope="santa"):
        self.runs.append((since, until, scope))
        return {"status": "running"}

    async def mfe_status(self, scope="santa"):
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    async def strategy_state(self, scope="santa"):
        return {"auto_tp": {"strategies": {"ICT_Killzones": {"tp_r": 6.5, "n": 7}}, "since": "2026-10-22", "updated_ms": 1},
                "overrides": {}, "proposals": [{"id": "p", "strategy": "ICT_Killzones", "title": "Шаблон X", "status": "template"}], "how_to": "manage_snapshots"}


class Settings:
    def __init__(self, profile="SANTA"):
        self.profile = profile


class Host:
    def __init__(self, client, profile="SANTA"):
        self.client, self.settings = client, Settings(profile)


def test_report_shows_verdict_pnl_phrases_stops_and_coverage():
    v = MfeView(Host(None))
    v._show(REPORT)
    t = texts(v.control)
    best = REPORT["verdict"]["best_tp"]
    assert f"Оптимальный Take-Profit: 1:{best:g}" in t and REPORT["verdict"]["text"] in t
    for r in REPORT["tp_matrix"]:
        assert f"Если бы Take-Profit был 1:{r['tp']:g} — заработок" in t
    assert "выбивание сквизом" in t and REPORT["padding"]["text"] in t
    assert "Учтены все сделки" in t and "прервана (рестарт)" in t and "EURUSD" in t
    assert "ICT_Killzones" in t or "ICT" in t


def test_button_starts_the_server_job_and_polls_until_done():
    client = FakeClient([{"status": "running", "progress": {"done": 0, "total": 1, "stage": "свечи Binance"}}, {"status": "done", "report": REPORT}])
    v = MfeView(Host(client))
    asyncio.run(v._run_clicked(None))
    assert client.runs == [("2026-10-20", None, "santa")] and not v._running and "Оптимальный Take-Profit" in texts(v.control)


def test_chr_profile_runs_its_own_job_with_the_date_range_and_shows_templates():
    client = FakeClient([{"status": "done", "report": REPORT}])
    v = MfeView(Host(client, "CHR"))
    v._until.value = "2026-10-23"
    asyncio.run(v._run_clicked(None))
    assert client.runs == [("2026-10-20", "2026-10-23", "chr")]
    asyncio.run(v._load_side())
    side = " | ".join(x.value for x in walk(v._side.controls[0]) if isinstance(x, ft.Text) and isinstance(x.value, str))
    assert "1:6.5" in side and "Шаблон X" in side and "manage_snapshots" in side
    v._since.value = "20.10.2026"
    asyncio.run(v._run_clicked(None))
    assert "ГГГГ-ММ-ДД" in texts(v.control) and len(client.runs) == 1


def test_failure_is_shown_not_swallowed():
    v = MfeView(Host(FakeClient([{"status": "failed", "error": "Binance ограничил запросы"}])))
    asyncio.run(v._run_clicked(None))
    assert "Binance ограничил запросы" in texts(v.control)


def test_already_running_on_server_just_follows():
    class Busy(FakeClient):
        async def mfe_run(self, since, until=None, scope="santa"):
            raise ApiError("conflict", "симуляция уже идёт", 409)

    v = MfeView(Host(Busy([{"status": "done", "report": REPORT}])))
    asyncio.run(v._run_clicked(None))
    assert "Оптимальный Take-Profit" in texts(v.control)


def test_dashboard_news_card_shows_upcoming_high_impact_events():
    from tradeai_client.ui.dashboard import DashboardView

    d = DashboardView(None)
    d.set_news({"events": [{"ts": "2026-10-20T10:00:00Z", "currency": "USD", "title": "Old"},
                           {"ts": "2026-10-28T18:00:00Z", "currency": "USD", "title": "Federal Funds Rate"}]}, now_iso="2026-10-24T12:00:00Z")
    t = texts(d._news_box)
    assert d._news_box.visible and "Federal Funds Rate" in t and "Old" not in t and "±5 минут" in t
