"""apply_spec на статическом графике lightweight-charts (тот же AbstractChart, что и у окна): без GUI и без процессов."""

import json
import re

import pandas as pd
import pytest

from tradeai_client import fmt
from tradeai_client.chart_spec import build_chart_spec
from tradeai_client.chart_window import INNER_WIDTH, _frame, _ts, apply_spec

lwc_widgets = pytest.importorskip("lightweight_charts.widgets")
IV = 900_000


def static_html(spec) -> str:
    chart = lwc_widgets.StaticLWC(width=1200, height=700, inner_width=INNER_WIDTH)
    apply_spec(chart, spec)
    chart.load()
    return chart._html


def make_spec(fx, name="snapshot_smc_short.json", post=True):
    snap = fx(name)
    last = snap["candles"][-1]
    klines = [{"t": last["open_time"] + i * IV, "o": last["close"], "h": last["close"] * 1.001, "l": last["close"] * 0.999, "c": last["close"], "v": 5.0}
              for i in range(1, 60)] if post else None
    return snap, build_chart_spec(snap, klines, now_ms=fmt.to_ms(snap["trade"]["signal_time"]) + 400 * IV)


def test_pandas_time_resolution_cannot_break_the_time_axis():
    """pandas 3 хранит время в микросекундах, а lightweight-charts делит int64 на 10**9: ось времени превращается в мусор (1970 год)."""
    df = _frame([{"t": 1_791_600_000_000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}], {"open": "o"})
    assert str(df["time"].dtype) == "datetime64[ns]" and df["time"].astype("int64").iloc[0] // 10**9 == 1_791_600_000
    assert _ts(1_791_600_000_000).timestamp() == 1_791_600_000.0  # наивное время трактуется как UTC


def test_all_drawings_reach_the_chart_script(fx):
    snap, spec = make_spec(fx)
    html = static_html(spec)
    for needle in ("Причина входа", "Результат", "FVG", "createPriceLine",  "Сигнал", "свип", "CHoCH", "Хронология", "Вход ", "Трейлинг"):
        assert needle in html or json.dumps(needle)[1:-1] in html, needle  # тексты маркеров библиотека кладёт в JSON с \uXXXX-экранированием
    # данные свечей ушли с корректными секундами эпохи (не 1970 год), сдвинутыми на пояс ПК (ось графика - в локальном времени)
    first = (spec["bars"][0]["t"] + spec["tz_offset_ms"]) // 1000
    assert f'"time":{first}' in html.replace(" ", "")
    times = [int(x) for x in re.findall(r'"time":(\d+)', html.replace(" ", ""))]
    assert times and min(times) > 1_700_000_000 and max(times) < 2_000_000_000


def test_chart_renders_without_post_candles_and_for_long_breakout(fx):
    for name, post in (("snapshot_smc_short.json", False), ("snapshot_breakout_long.json", True), ("snapshot_pending.json", False), ("snapshot_open.json", True)):
        _, spec = make_spec(fx, name, post)
        assert "Причина входа" in static_html(spec), name


def test_marker_times_are_on_the_helper_grid(fx):
    _, spec = make_spec(fx)
    grid = {t for t, _ in spec["helper"]}
    assert all(m["t"] in grid for m in spec["trade_markers"]) and all(m["t"] in {b["t"] for b in spec["bars"]} for m in spec["setup_markers"])
    assert isinstance(pd.Timestamp(spec["bars"][0]["t"], unit="ms"), pd.Timestamp)


def test_side_panel_is_plain_html_without_python_callbacks(fx):
    """Клик по тексту панели не должен вызывать событий в Python: таблицы lightweight-charts (Lib.Table) убраны, подписи идут через textContent."""
    _, spec = make_spec(fx)
    html = static_html(spec)
    assert "Lib.Table" not in html and "tradeai-panel" in html and "Разбор сделки" in html and "textContent" in html
    assert "+214.00 USDT" in html


def test_panel_script_cannot_be_glued_to_the_previous_statement(fx):
    """Скрипты окна склеиваются через перевод строки без `;`: панель, начатая с `(`, превращалась в ВЫЗОВ предыдущего выражения - справа была пустота."""
    from tradeai_client.chart_window import _PANEL_JS

    assert _PANEL_JS.lstrip().startswith(";(function")
    _, spec = make_spec(fx)
    assert "tradeai-panel" in static_html(spec)
