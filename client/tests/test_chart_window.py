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


def test_favorite_star_button_is_on_the_chart_and_sends_the_request(fx):
    """Звёздочка в верхней панели графика: кнопка в HTML, нажатие -> PUT с картинкой / DELETE, подпись меняется; ошибка сервера показывается на кнопке."""
    from tradeai_client import chart_favorite

    _, spec = make_spec(fx)
    spec["favorite"] = {"url": "http://127.0.0.1:1", "key": "k" * 40, "trade_id": "BTCUSDT:1", "profile": "CHR", "is_favorite": False}
    html = static_html(spec)
    assert 'makeButton("☆ В избранное"' in html
    assert "k" * 40 not in html  # ключ API в HTML окна не попадает

    class Widget:
        def __init__(self):
            self.text = None

        def set(self, text):
            self.text = text

    class Chart:
        def __init__(self):
            self.widget, self.handler = Widget(), None
            self.topbar = self

        def button(self, name, text, separator=False, func=None):
            self.handler = func

        def __getitem__(self, name):
            return self.widget

    sent = []
    chart = Chart()
    chart_favorite.attach_favorite_button(chart, spec, send=lambda fav, add, png: sent.append((fav["trade_id"], fav["profile"], add, png)), render=lambda sp: b"\x89PNG-x")
    chart.handler(chart)
    chart.handler(chart)
    assert sent == [("BTCUSDT:1", "CHR", True, b"\x89PNG-x"), ("BTCUSDT:1", "CHR", False, None)] and chart.widget.text == chart_favorite.LABEL_OFF

    def broken(fav, add, png):
        raise chart_favorite.FavoriteSendError("сервер недоступен")

    chart2 = Chart()
    chart_favorite.attach_favorite_button(chart2, spec, send=broken, render=lambda sp: b"x")
    chart2.handler(chart2)
    assert chart2.widget.text == "⚠ сервер недоступен"


def test_send_favorite_builds_the_right_request():
    import io
    import urllib.error

    from tradeai_client import chart_favorite

    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def opener(req, timeout):
        seen.update(url=req.full_url, method=req.get_method(), key=req.get_header("X-api-key"), body=req.data)
        return Resp()

    fav = {"url": "http://h:8001/", "key": "K" * 40, "trade_id": "A:B:live", "profile": "CHR"}
    chart_favorite.send_favorite(fav, True, b"\x89PNG", opener)
    assert seen["url"] == "http://h:8001/api/favorites/A:B:live" and seen["method"] == "PUT" and seen["key"] == "K" * 40 and json.loads(seen["body"])["profile"] == "CHR"
    chart_favorite.send_favorite(fav, False, None, opener)
    assert seen["method"] == "DELETE" and seen["body"] is None

    def refuse(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 401, "no", {}, io.BytesIO(b""))

    with pytest.raises(chart_favorite.FavoriteSendError, match="401"):
        chart_favorite.send_favorite(fav, True, None, refuse)
