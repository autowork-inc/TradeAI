"""Вкладка Analysis без запуска Flet: отчёт анализатора (настоящий, снятый с analyzer.engine) -> виджеты; запуск/опрос задания; Dry Run; ApiClient против фейкового сервера."""

import asyncio
import json

import aiohttp
import flet as ft
import pytest
from aiohttp import web

from tradeai_client.api import ApiClient, ApiError
from tradeai_client.settings import ClientSettings
from tradeai_client.ui import analysis_view
from tradeai_client.ui.analysis_view import AnalysisView
from tradeai_client.ui import widgets as w

KEY = "k" * 40


@pytest.fixture(autouse=True)
def detached_controls(monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)
    monkeypatch.setattr(analysis_view, "POLL_S", 0.0)


def walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from walk(x)


def texts(c) -> list[str]:
    return [x.value for x in walk(c) if isinstance(x, ft.Text) and x.value]


class FakePage:
    def __init__(self):
        self.dialogs, self.services = [], []

    def show_dialog(self, d):
        self.dialogs.append(d)

    def pop_dialog(self):
        self.dialogs.pop()


class FakeClient:
    def __init__(self, report, job_steps=("queued", "running", "done")):
        self.report, self.steps, self.calls, self.dry = report, list(job_steps), [], {"diff": "--- a\n+++ b\n+x", "note": "Dry run: ничего не записано."}

    async def analysis_start(self, body):
        self.calls.append(("start", body))
        return {"job_id": "0123456789ab", "status": "queued"}

    async def analysis_job(self, job_id):
        st = self.steps.pop(0) if len(self.steps) > 1 else self.steps[0]
        return {"id": job_id, "status": st, "progress": 0.4 if st == "running" else (1.0 if st == "done" else 0.0), "stage": "metrics", "error": "boom" if st == "failed" else None}

    async def analysis_report(self, job_id):
        return {"job": {"id": job_id}, "report": self.report}

    async def analysis_dry_run(self, run_id, ids):
        self.calls.append(("dry", run_id, ids))
        return self.dry

    async def analysis_status(self):
        return {"version": "1.0", "worker_alive": True, "calendar": {"events": 14, "from": "2026-09-01T03:00:00+00:00", "to": "2026-10-10T03:00:00+00:00"}, "min_conclusions": 200, "queue": {}}

    async def analysis_runs(self, limit=10):
        return {"items": [{"id": "0123456789ab", "status": "done"}]}


class Host:
    def __init__(self, client):
        self.client, self.page, self.notes = client, FakePage(), []
        self.default_mode = "shadow"

    def notify(self, text, *, error=False):
        self.notes.append((text, error))


@pytest.fixture
def report(fx):
    return fx("analysis_report.json")


def test_report_renders_metrics_news_versions_sessions_and_recommendations(report):
    v = AnalysisView(Host(None))
    v._show_report("0123456789ab", report)
    t = texts(ft.Column(v._body.controls))
    for needle in ("Метрики по стратегиям", "Profit Factor", "Sharpe / Sortino", "Матожидание", "Макс. просадка", "News Impact: все сделки против «бот выключен на новостях»",
                   "Только у новостей", "Рекомендации системы", "Торговые сессии (UTC, по времени входа)", "Версии стратегий: A/B и деградация"):
        assert needle in t, needle
    assert any(x.startswith("Выборка: ") for x in t) and "SMC Pro Trend" in t and "OBI Breakout" in t
    assert any("symbols.exclude" in x and "BADUSDT" in x for x in t)  # рекомендация «отключить монету» с параметром и значением
    assert any("Это гипотезы по прошлым данным" in x for x in t)


def test_chart_series_can_be_toggled_and_cover_strategies_news_and_versions(report):
    v = AnalysisView(Host(None))
    v._show_report("0123456789ab", report)
    names = [s["name"] for s in v._series]
    assert "SMC Pro Trend" in names and "OBI Breakout" in names and any(n.startswith("Без новостей") for n in names) and any(n.startswith("Версия ") for n in names)
    assert set(v._checks) == set(names)
    before = len(list(walk(v._chart_slot.content)))
    v._toggle(names[0], False)
    assert not v._series[0]["visible"] and v._chart_slot.content is not None
    for n in names:
        v._toggle(n, False)
    assert any("Нет кривых" in x for x in texts(v._chart_slot.content))
    v._toggle(names[0], True)
    assert before > 0 and not any("Нет кривых" in x for x in texts(v._chart_slot.content))


def test_line_chart_draws_one_path_per_visible_series():
    import flet.canvas as cv
    chart = w.line_chart([{"name": "a", "color": "#fff", "x": [1, 2, 3], "y": [0, 1, -1]}, {"name": "b", "color": "#000", "x": [1, 2], "y": [0, 2]}, {"name": "short", "color": "#111", "x": [1], "y": [1]}])
    canvas = next(x for x in walk(chart) if isinstance(x, cv.Canvas))
    assert sum(isinstance(s, cv.Path) for s in canvas.shapes) == 2  # серия из одной точки не рисуется
    assert isinstance(w.line_chart([]), ft.Text)


def test_run_deep_analysis_polls_with_spinner_then_shows_report(report):
    client = FakeClient(report)
    host = Host(client)
    v = AnalysisView(host)
    seen = []
    orig = v._busy
    v._busy = lambda on, stage="", progress=None: (seen.append((on, stage)), orig(on, stage, progress))[1]
    asyncio.run(v._run_clicked())
    assert client.calls[0][0] == "start" and client.calls[0][1]["mode"] == "shadow" and client.calls[0][1]["news"]["before_min"] == 5.0
    assert seen[0][0] is True and any("в очереди" in s for _, s in seen) and any("считаю" in s for _, s in seen) and seen[-1] == (False, "")
    assert not v._ring.visible and not v._run.disabled and v._report is report
    assert "Метрики по стратегиям" in texts(ft.Column(v._body.controls))


def test_request_body_clamps_news_window_and_survives_garbage():
    v = AnalysisView(Host(None))
    v._before.value, v._after.value = "abc", "999"
    v._mode.value = "live"
    b = v._request_body()
    assert b["mode"] == "live" and b["news"]["before_min"] == 5.0 and b["news"]["after_min"] == 240.0


def test_failed_job_and_analyzer_outage_are_shown_as_banners(report):
    host = Host(FakeClient(report, ("queued", "failed")))
    v = AnalysisView(host)
    asyncio.run(v._run_clicked())
    assert any("Анализ не выполнен: boom" in x for x in texts(ft.Column(v._body.controls))) and not v._running

    class Down(FakeClient):
        async def analysis_start(self, body):
            raise ApiError("analyzer", "Анализатор не запущен. На сервере: ./deploy/shadow.sh analyzer-up", 503)

    v2 = AnalysisView(Host(Down(report)))
    asyncio.run(v2._run_clicked())
    assert any("analyzer-up" in x for x in texts(ft.Column(v2._body.controls))) and not v2._running


def test_dry_run_sends_selected_ids_and_shows_copyable_diff(report):
    client = FakeClient(report)
    host = Host(client)
    v = AnalysisView(host)
    v._show_report("0123456789ab", report)
    assert v._dry_btn.disabled is True
    first = next(iter(v._picked))
    v._picked[first].value = True
    v._pick_changed()
    assert v._dry_btn.disabled is False and v.selected_ids() == [first]
    asyncio.run(v._dry_run_clicked())
    assert client.calls[-1] == ("dry", "0123456789ab", [first])
    dialog = host.page.dialogs[-1]
    t = texts(dialog.content)
    assert any("+x" in x for x in t) and any("ничего не записано" in x for x in t)
    assert [a.content if hasattr(a, "content") else a.text for a in dialog.actions]  # кнопки «Скопировать diff» и «Закрыть»
    labels = [getattr(a, "content", None) or getattr(a, "text", None) for a in dialog.actions]
    assert "Скопировать diff" in labels and "Закрыть" in labels


def test_refresh_shows_status_and_last_done_report(report):
    host = Host(FakeClient(report))
    v = AnalysisView(host)
    asyncio.run(v.refresh())
    assert v._report is report and "календарь: 14 событий" in v._info.value and "воркер работает" in v._info.value


def test_calendar_is_automatic_no_upload_ui_and_news_window_defaults_to_5_5(report):
    v = AnalysisView(Host(FakeClient(report)))
    assert not hasattr(v, "_calendar") and not hasattr(v, "_calendar_dialog")
    assert not any(isinstance(x, ft.OutlinedButton) and "Календарь" in str(getattr(x, "content", "") or getattr(x, "text", "")) for x in walk(v.control))
    assert v._before.value == "5" and v._after.value == "5"
    assert v._request_body()["news"]["before_min"] == 5.0 and v._request_body()["news"]["after_min"] == 5.0
    v._before.value = "abc"
    assert v._request_body()["news"]["before_min"] == 5.0


def test_api_client_analysis_calls_and_error_mapping():
    seen = []

    async def handler(request: web.Request) -> web.Response:
        seen.append((request.method, request.path, dict(request.query), request.headers.get("X-API-Key"), request.headers.get("Content-Type"), await request.read()))
        if request.path.endswith("/status") and request.headers.get("X-Fail"):
            return web.json_response({"detail": "analyzer_unavailable"}, status=503)
        if request.path.endswith("/dry-run"):
            return web.json_response({"diff": "", "note": "n"})
        return web.json_response({"job_id": "0123456789ab", "status": "queued", "items": []}, status=202 if request.method == "POST" else 200)

    async def body():
        app = web.Application()
        app.router.add_route("*", "/api/analysis/{tail:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        s = ClientSettings(base_url=f"http://127.0.0.1:{port}", api_key=KEY)
        client = ApiClient(s)
        try:
            await client.analysis_start({"mode": "shadow"})
            await client.analysis_job("0123456789ab")
            await client.analysis_report("0123456789ab")
            await client.analysis_runs(5)
            await client.analysis_dry_run("0123456789ab", ["a" * 10])
            async with aiohttp.ClientSession() as sess:  # прямой вызов с заголовком отказа
                client._session = sess
                client._owns = False
                client.session.headers.update({"X-Fail": "1"})
                with pytest.raises(ApiError) as exc:
                    await client.analysis_status()
            return exc.value
        finally:
            await runner.cleanup()

    err = asyncio.run(body())
    assert err.kind == "analyzer" and "analyzer-up" in str(err) and err.status == 503
    methods = [(m, p) for m, p, *_ in seen]
    assert methods[:5] == [("POST", "/api/analysis/runs"), ("GET", "/api/analysis/runs/0123456789ab"), ("GET", "/api/analysis/runs/0123456789ab/report"), ("GET", "/api/analysis/runs"),
                           ("POST", "/api/analysis/dry-run")]
    assert json.loads(seen[0][5]) == {"mode": "shadow"} and seen[3][2] == {"limit": "5"} and all(r[3] == KEY for r in seen[:5])
    assert json.loads(seen[4][5]) == {"run_id": "0123456789ab", "recommendation_ids": ["a" * 10]}


def test_shell_wires_the_analysis_tab_as_the_fourth_tab(monkeypatch):
    from unittest.mock import MagicMock

    from tradeai_client.ui import shell as shell_mod

    monkeypatch.setattr(shell_mod, "load_settings", lambda: ClientSettings())  # не читать настоящий settings.json рядом с клиентом
    page = MagicMock()
    sh = shell_mod.Shell(page)
    tabs = page.add.call_args.args[2]
    bar, view = tabs.content.controls
    assert tabs.length == len(bar.tabs) == len(view.controls) == 5 and bar.tabs[shell_mod.TAB_ANALYSIS].label == "Analysis" and bar.tabs[shell_mod.TAB_FAVORITES].label == "Favorites"
    assert view.controls[shell_mod.TAB_ANALYSIS] is sh.analysis.control and view.controls[shell_mod.TAB_FAVORITES] is sh.favorites.control
