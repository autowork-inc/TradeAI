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

    async def mfe_run(self, since):
        self.runs.append(since)
        return {"status": "running"}

    async def mfe_status(self):
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]


class Host:
    def __init__(self, client):
        self.client = client


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
    assert client.runs == ["2026-10-20"] and not v._running and "Оптимальный Take-Profit" in texts(v.control)


def test_failure_is_shown_not_swallowed():
    v = MfeView(Host(FakeClient([{"status": "failed", "error": "Binance ограничил запросы"}])))
    asyncio.run(v._run_clicked(None))
    assert "Binance ограничил запросы" in texts(v.control)


def test_already_running_on_server_just_follows():
    class Busy(FakeClient):
        async def mfe_run(self, since):
            raise ApiError("conflict", "симуляция уже идёт", 409)

    v = MfeView(Host(Busy([{"status": "done", "report": REPORT}])))
    asyncio.run(v._run_clicked(None))
    assert "Оптимальный Take-Profit" in texts(v.control)
