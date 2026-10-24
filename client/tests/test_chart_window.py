"""Окно графика: сборка страницы (конфиг без секретов, JS-файлы внутри), мост ChartApi (свечи, рисунки, избранное), хранилище рисунков, JS без синтаксических ошибок.
Сам WebView без окна не проверить: живой прогон - `python app.py --selftest-chart снимок.png` (проверен на WebView2)."""

import io
import json
import shutil
import subprocess
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from tradeai_client import chart_favorite, chart_window, drawings_store, fmt
from tradeai_client.chart_spec import build_chart_spec
from tradeai_client.chart_window import ChartApi, build_config, build_html

IV = 900_000
SECRET = "S" * 40


def make_spec(fx, name="snapshot_smc_short.json", source=None):
    snap = fx(name)
    spec = build_chart_spec(snap, None, now_ms=fmt.to_ms(snap["trade"]["signal_time"]) + 40 * IV)
    spec["source"] = source or {"kind": "binance", "symbol": "BTCUSDT", "rest": "http://127.0.0.1:1"}
    spec["favorite"] = {"url": "http://127.0.0.1:1", "key": SECRET, "trade_id": snap["trade"]["id"], "profile": "CHR", "is_favorite": False}
    return spec


def test_config_and_html_contain_everything_but_no_secrets(fx):
    spec = make_spec(fx, source={"kind": "server", "symbol": "EURUSD", "url": "http://srv:8001", "key": SECRET})
    cfg = build_config(spec, [])
    html = build_html(cfg)
    assert SECRET not in html and "srv:8001" not in html and "<script>window.__CFG__" in html  # ключ и адрес сервера остаются в мосте Python
    assert cfg["interval"] == "15m" and cfg["timeframes"] == ["5m", "15m", "1h", "4h"] and cfg["source"] == {"kind": "server", "symbol": "EURUSD"}
    assert cfg["favorite"] == {"is_favorite": False} and cfg["bot"] is None and cfg["chr"] is False and cfg["focus_ms"]
    for marker in ("/*__CSS__*/", "/*__LIB__*/", "/*__CFG__*/", "/*__APP__*/"):
        assert marker not in html
    assert "LightweightCharts" in html and "createChart" in html and "Что увидел бот" in html and html.count("</script>") == 3  # ни один из файлов не закрывает тег раньше времени
    assert "https://" not in html.split("window.__CFG__")[0].split("<style>")[0]  # страница не тянет внешние файлы


def test_timeframes_include_the_trade_interval_when_it_is_not_standard(fx):
    spec = make_spec(fx)
    spec["interval"], spec["interval_ms"] = "1d", 86_400_000
    assert build_config(spec, [])["timeframes"][-1] == "1d"


def test_html_survives_hostile_text_in_the_spec(fx):
    spec = make_spec(fx)
    spec["title"] = "</script><script>alert(1)</script>"
    html = build_html(build_config(spec, []))
    assert "</script><script>alert(1)" not in html and html.count("</script>") == 3


class _Candles(BaseHTTPRequestHandler):
    seen: list = []

    def do_GET(self):
        _Candles.seen.append((self.path, self.headers.get("X-API-Key")))
        if "fail" in self.path:
            self.send_response(503)
            self.end_headers()
            return
        rows = [[1_790_000_000_000 + i * IV, "1.5", "2", "1", "1.7", "10"] for i in range(3)]
        body = json.dumps({"items": rows} if "/api/candles" in self.path else rows).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture()
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Candles)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    _Candles.seen.clear()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_bridge_loads_binance_klines_without_a_key(fx, server):
    spec = make_spec(fx, source={"kind": "binance", "symbol": "BTCUSDT", "rest": server})
    rows = ChartApi(spec).klines("5m", 1_790_000_999_999, 500)
    assert rows[0] == [1_790_000_000_000, 1.5, 2.0, 1.0, 1.7, 10.0] and len(rows) == 3
    path, key = _Candles.seen[0]
    assert path.startswith("/fapi/v1/klines?") and "symbol=BTCUSDT" in path and "interval=5m" in path and "endTime=1790000999999" in path and key is None


def test_bridge_loads_server_candles_with_the_key_and_reports_errors(fx, server):
    api = ChartApi(make_spec(fx, source={"kind": "server", "symbol": "EURUSD", "url": server, "key": SECRET}))
    assert len(api.klines("1h", None, 100)) == 3
    path, key = _Candles.seen[0]
    assert path.startswith("/api/candles?") and "coin=EURUSD" in path and "interval=1h" in path and "end=" not in path and key == SECRET
    with pytest.raises(ValueError):
        api.klines("2h")
    with pytest.raises(RuntimeError, match="нет источника"):
        ChartApi(make_spec(fx, source={"kind": "none"})).klines("5m")
    bad = ChartApi(make_spec(fx, source={"kind": "binance", "symbol": "fail", "rest": server}))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        bad.klines("5m")
    with pytest.raises(RuntimeError, match="недоступен"):
        ChartApi(make_spec(fx, source={"kind": "binance", "symbol": "X", "rest": "http://127.0.0.1:1"})).klines("5m")


def test_bridge_saves_drawings_per_symbol_and_toggles_favorite(fx, tmp_path, monkeypatch):
    monkeypatch.setattr(chart_window, "drawings_path", lambda: tmp_path / "drawings.json")
    spec = make_spec(fx)
    api = ChartApi(spec)
    good = {"id": "a1", "type": "trend", "color": "#ffb300", "width": 2, "opacity": 0.9, "p1": {"t": 1_790_000_000, "p": 100.0}, "p2": {"t": 1_790_001_000, "p": 101.0}}
    assert api.save_drawings(json.dumps([good, {"id": "bad", "type": "evil"}])) == 1
    assert drawings_store.load(tmp_path / "drawings.json", spec["coin"]) == [good]
    _, html = chart_window._window_args(spec)  # при следующем открытии монеты рисунки подставляются в конфиг страницы
    assert '"id":"a1"' in html

    sent = []
    monkeypatch.setattr(chart_favorite, "send_favorite", lambda fav, add, png: sent.append((fav["trade_id"], add, bool(png))))
    monkeypatch.setattr("tradeai_client.golden_image.render_png", lambda sp: b"\x89PNG")
    assert api.toggle_favorite(True) == {"ok": True, "is_favorite": True} and api.toggle_favorite(False)["is_favorite"] is False
    assert sent == [(spec["favorite"]["trade_id"], True, True), (spec["favorite"]["trade_id"], False, False)]

    def refuse(fav, add, png):
        raise chart_favorite.FavoriteSendError("сервер недоступен")

    monkeypatch.setattr(chart_favorite, "send_favorite", refuse)
    assert api.toggle_favorite(True) == {"ok": False, "error": "сервер недоступен"}
    assert ChartApi({**spec, "favorite": None}).toggle_favorite(True)["ok"] is False


def test_drawings_store_cleans_foreign_and_broken_entries(tmp_path):
    path = tmp_path / "drawings.json"
    ok = {"id": "r1", "type": "rect", "color": "#00ff00", "width": 3, "opacity": 0.3, "p1": {"t": 1_790_000_000, "p": 1.0}, "p2": {"t": 1_790_000_900, "p": 2.0}}
    hline = {"id": "h1", "type": "hline", "color": "#FFFFFF", "width": 1, "opacity": 1, "p1": {"t": 1_790_000_000, "p": 5.0}, "extra": "x"}
    junk = [{"id": "x", "type": "rect", "color": "red", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "y", "type": "trend", "color": "#000000", "width": 99, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "z z", "type": "trend", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "n", "type": "trend", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": float("nan"), "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "no2", "type": "ray", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}}, "строка", None]
    assert drawings_store.save(path, "BTCUSDT", [ok, hline, *junk]) == 2
    assert drawings_store.save(path, "ETHUSDT", [ok]) == 1
    loaded = drawings_store.load(path, "BTCUSDT")
    assert [d["id"] for d in loaded] == ["r1", "h1"] and "extra" not in loaded[1] and drawings_store.load(path, "XRPUSDT") == []
    assert drawings_store.save(path, "BTCUSDT", []) == 0 and "BTCUSDT" not in json.loads(path.read_text(encoding="utf-8"))  # пустой список убирает запись монеты
    path.write_text("{ не json", encoding="utf-8")
    with pytest.raises(drawings_store.DrawingsError):
        drawings_store.load(path, "BTCUSDT")
    with pytest.raises(drawings_store.DrawingsError):
        drawings_store.save(path, "BTCUSDT", [ok])  # битый файл не затирается молча


def test_send_favorite_builds_the_right_request():
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


@pytest.mark.skipif(shutil.which("node") is None, reason="нужен node для проверки синтаксиса JS")
def test_chart_js_has_valid_syntax():
    out = subprocess.run(["node", "--check", str(chart_window.WEB_DIR / "chart.js")], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


def test_chart_js_guards_the_known_pitfalls():
    """Страховки, найденные при проверке в браузере: холст выше слоёв графика, диапазон - после первой раскладки, подгрузка истории не срабатывает на сбросе диапазона."""
    js, css = (chart_window.WEB_DIR / "chart.js").read_text(encoding="utf-8"), (chart_window.WEB_DIR / "chart.css").read_text(encoding="utf-8")
    assert "#ov{position:absolute;inset:0;pointer-events:none;z-index:5}" in css
    assert "chart.timeScale().width() <= 0" in js and "!S.quiet && !S.view" in js and "tickMarkFormatter" in js and "setMarkers" in js and "createPriceLine" in js
    assert "subscribeVisibleLogicalRangeChange" in js and "wss://fstream.binance.com/market/stream" in js and "'4h'" in js


def test_drawings_map_by_absolute_time_from_the_candle_centre():
    """Рисунки хранят (время UTC, цена); индекс считается от середины свечи [open, open + iv), поэтому смена таймфрейма не сдвигает их на полсвечи."""
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "tradeai_client" / "chart_web" / "chart.js").read_text(encoding="utf-8")
    assert "rawToLogical(t - barIv() / 2)" in js and "rawToTime(L)" in js and "S.barsIv = ivSec()" in js
    assert "specIv()" in js  # правый край зон сигнала - по таймфрейму сделки, а не выбранному



def _fake_webview(monkeypatch, calls):
    import sys
    import types

    fake = types.ModuleType("webview")

    class Hook:
        def __init__(self):
            self.fns = []

        def __iadd__(self, fn):
            self.fns.append(fn)
            return self

        def fire(self):
            for fn in self.fns:
                fn()

    class Events:
        def __init__(self):
            self.maximized, self.restored, self.closing, self.closed = Hook(), Hook(), Hook(), Hook()

    class Win:
        def __init__(self):
            self.on_top, self.shown, self.maximized_calls, self.events = False, 0, 0, Events()
            self.x, self.y, self.width, self.height = 10, 20, 900, 700

        def show(self):
            self.shown += 1

        def maximize(self):
            self.maximized_calls += 1

    def create_window(title, **kw):
        calls["create"] = (title, kw)
        calls["win"] = Win()
        return calls["win"]

    def start(func=None, *args):
        win = args[0]
        win.events.closing.fire()  # окно сразу «закрывается»: проверяем и показ, и запоминание места
        win.events.closed.fire()
        func(*args)

    fake.create_window, fake.start = create_window, start
    monkeypatch.setitem(sys.modules, "webview", fake)


def test_first_chart_opens_full_screen_in_front_and_closing_remembers_the_place(monkeypatch, tmp_path):
    """1.6.0: первое окно - на весь экран и спереди; при закрытии запоминаются место и размер - по ним откроется следующее окно."""
    from tradeai_client import chart_window

    calls = {}
    _fake_webview(monkeypatch, calls)
    monkeypatch.setattr(chart_window, "STACK_DIR", tmp_path)
    monkeypatch.setattr(chart_window, "GEOMETRY_FILE", tmp_path / "last_geometry.json")
    monkeypatch.setattr(chart_window, "_window_args", lambda spec: (object(), "<html></html>"))
    monkeypatch.setattr(chart_window, "_own_hwnd", lambda title: 111)
    monkeypatch.setattr(chart_window.time, "sleep", lambda s: None)
    chart_window.run_chart_window({"title": "BTC", "chart_order": 1})
    title, kw = calls["create"]
    assert kw["maximized"] is True and kw["focus"] is True and title.endswith("BTC") and calls["win"].shown == 1
    saved = chart_window.load_geometry()
    assert saved["maximized"] is True  # закрыли развёрнутым - следующее тоже развёрнуто
    (tmp_path / "last_geometry.json").write_text('{"maximized": false, "x": 50, "y": 60, "width": 1000, "height": 700}', encoding="utf-8")
    chart_window.run_chart_window({"title": "ETH", "chart_order": 2})
    _, kw = calls["create"]
    assert (kw["x"], kw["y"], kw["width"], kw["height"]) == (50, 60, 1000, 700) and "maximized" not in kw  # место последнего закрытого


def test_later_charts_open_in_the_background_under_the_previous_ones(monkeypatch, tmp_path):
    """a, b, c открыты: новое окно d встаёт сразу ПОД c (сверху вниз a, b, c, d), без фокуса и без подъёма."""
    import json as _json
    import time as _time

    from tradeai_client import chart_window

    calls, placed = {}, []
    _fake_webview(monkeypatch, calls)
    monkeypatch.setattr(chart_window, "STACK_DIR", tmp_path)
    monkeypatch.setattr(chart_window, "GEOMETRY_FILE", tmp_path / "last_geometry.json")
    monkeypatch.setattr(chart_window, "_window_args", lambda spec: (object(), "<html></html>"))
    monkeypatch.setattr(chart_window, "_own_hwnd", lambda title: 444)
    monkeypatch.setattr(chart_window, "_insert_below", lambda hwnd, after: placed.append((hwnd, after)))
    monkeypatch.setattr(chart_window, "_is_right_below", lambda hwnd, after: True)
    allowed = []
    monkeypatch.setattr(chart_window, "_allow_activation", allowed.append)
    for key, order, hwnd in (("1", 1, 111), ("2", 2, 222), ("3", 3, 333)):
        (tmp_path / f"{key}.json").write_text(_json.dumps({"order": order, "beat": _time.time() + 60, "hwnd": hwnd}), encoding="utf-8")
    chart_window.run_chart_window({"title": "DOGE", "chart_order": 4})
    _, kw = calls["create"]
    assert kw["focus"] is False and placed == [(444, 333)] and calls["win"].shown == 0
    assert allowed == [444]  # окно в фоне, но его можно активировать (без WS_EX_NOACTIVATE)
    assert chart_window.place_after({"a": {"order": 1, "beat": 9, "hwnd": 1}, "z": {"order": 9, "beat": 9}}, "z", 10) == 1

"""Окно графика: сборка страницы (конфиг без секретов, JS-файлы внутри), мост ChartApi (свечи, рисунки, избранное), хранилище рисунков, JS без синтаксических ошибок.
Сам WebView без окна не проверить: живой прогон - `python app.py --selftest-chart снимок.png` (проверен на WebView2)."""

import io
import json
import shutil
import subprocess
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from tradeai_client import chart_favorite, chart_window, drawings_store, fmt
from tradeai_client.chart_spec import build_chart_spec
from tradeai_client.chart_window import ChartApi, build_config, build_html

IV = 900_000
SECRET = "S" * 40


def make_spec(fx, name="snapshot_smc_short.json", source=None):
    snap = fx(name)
    spec = build_chart_spec(snap, None, now_ms=fmt.to_ms(snap["trade"]["signal_time"]) + 40 * IV)
    spec["source"] = source or {"kind": "binance", "symbol": "BTCUSDT", "rest": "http://127.0.0.1:1"}
    spec["favorite"] = {"url": "http://127.0.0.1:1", "key": SECRET, "trade_id": snap["trade"]["id"], "profile": "CHR", "is_favorite": False}
    return spec


def test_config_and_html_contain_everything_but_no_secrets(fx):
    spec = make_spec(fx, source={"kind": "server", "symbol": "EURUSD", "url": "http://srv:8001", "key": SECRET})
    cfg = build_config(spec, [])
    html = build_html(cfg)
    assert SECRET not in html and "srv:8001" not in html and "<script>window.__CFG__" in html  # ключ и адрес сервера остаются в мосте Python
    assert cfg["interval"] == "15m" and cfg["timeframes"] == ["5m", "15m", "1h", "4h"] and cfg["source"] == {"kind": "server", "symbol": "EURUSD"}
    assert cfg["favorite"] == {"is_favorite": False} and cfg["bot"] is None and cfg["chr"] is False and cfg["focus_ms"]
    for marker in ("/*__CSS__*/", "/*__LIB__*/", "/*__CFG__*/", "/*__APP__*/"):
        assert marker not in html
    assert "LightweightCharts" in html and "createChart" in html and "Что увидел бот" in html and html.count("</script>") == 3  # ни один из файлов не закрывает тег раньше времени
    assert "https://" not in html.split("window.__CFG__")[0].split("<style>")[0]  # страница не тянет внешние файлы


def test_timeframes_include_the_trade_interval_when_it_is_not_standard(fx):
    spec = make_spec(fx)
    spec["interval"], spec["interval_ms"] = "1d", 86_400_000
    assert build_config(spec, [])["timeframes"][-1] == "1d"


def test_html_survives_hostile_text_in_the_spec(fx):
    spec = make_spec(fx)
    spec["title"] = "</script><script>alert(1)</script>"
    html = build_html(build_config(spec, []))
    assert "</script><script>alert(1)" not in html and html.count("</script>") == 3


class _Candles(BaseHTTPRequestHandler):
    seen: list = []

    def do_GET(self):
        _Candles.seen.append((self.path, self.headers.get("X-API-Key")))
        if "fail" in self.path:
            self.send_response(503)
            self.end_headers()
            return
        rows = [[1_790_000_000_000 + i * IV, "1.5", "2", "1", "1.7", "10"] for i in range(3)]
        body = json.dumps({"items": rows} if "/api/candles" in self.path else rows).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture()
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Candles)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    _Candles.seen.clear()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_bridge_loads_binance_klines_without_a_key(fx, server):
    spec = make_spec(fx, source={"kind": "binance", "symbol": "BTCUSDT", "rest": server})
    rows = ChartApi(spec).klines("5m", 1_790_000_999_999, 500)
    assert rows[0] == [1_790_000_000_000, 1.5, 2.0, 1.0, 1.7, 10.0] and len(rows) == 3
    path, key = _Candles.seen[0]
    assert path.startswith("/fapi/v1/klines?") and "symbol=BTCUSDT" in path and "interval=5m" in path and "endTime=1790000999999" in path and key is None


def test_bridge_loads_server_candles_with_the_key_and_reports_errors(fx, server):
    api = ChartApi(make_spec(fx, source={"kind": "server", "symbol": "EURUSD", "url": server, "key": SECRET}))
    assert len(api.klines("1h", None, 100)) == 3
    path, key = _Candles.seen[0]
    assert path.startswith("/api/candles?") and "coin=EURUSD" in path and "interval=1h" in path and "end=" not in path and key == SECRET
    with pytest.raises(ValueError):
        api.klines("2h")
    with pytest.raises(RuntimeError, match="нет источника"):
        ChartApi(make_spec(fx, source={"kind": "none"})).klines("5m")
    bad = ChartApi(make_spec(fx, source={"kind": "binance", "symbol": "fail", "rest": server}))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        bad.klines("5m")
    with pytest.raises(RuntimeError, match="недоступен"):
        ChartApi(make_spec(fx, source={"kind": "binance", "symbol": "X", "rest": "http://127.0.0.1:1"})).klines("5m")


def test_bridge_saves_drawings_per_symbol_and_toggles_favorite(fx, tmp_path, monkeypatch):
    monkeypatch.setattr(chart_window, "drawings_path", lambda: tmp_path / "drawings.json")
    spec = make_spec(fx)
    api = ChartApi(spec)
    good = {"id": "a1", "type": "trend", "color": "#ffb300", "width": 2, "opacity": 0.9, "p1": {"t": 1_790_000_000, "p": 100.0}, "p2": {"t": 1_790_001_000, "p": 101.0}}
    assert api.save_drawings(json.dumps([good, {"id": "bad", "type": "evil"}])) == 1
    assert drawings_store.load(tmp_path / "drawings.json", spec["coin"]) == [good]
    _, html = chart_window._window_args(spec)  # при следующем открытии монеты рисунки подставляются в конфиг страницы
    assert '"id":"a1"' in html

    sent = []
    monkeypatch.setattr(chart_favorite, "send_favorite", lambda fav, add, png: sent.append((fav["trade_id"], add, bool(png))))
    monkeypatch.setattr("tradeai_client.golden_image.render_png", lambda sp: b"\x89PNG")
    assert api.toggle_favorite(True) == {"ok": True, "is_favorite": True} and api.toggle_favorite(False)["is_favorite"] is False
    assert sent == [(spec["favorite"]["trade_id"], True, True), (spec["favorite"]["trade_id"], False, False)]

    def refuse(fav, add, png):
        raise chart_favorite.FavoriteSendError("сервер недоступен")

    monkeypatch.setattr(chart_favorite, "send_favorite", refuse)
    assert api.toggle_favorite(True) == {"ok": False, "error": "сервер недоступен"}
    assert ChartApi({**spec, "favorite": None}).toggle_favorite(True)["ok"] is False


def test_drawings_store_cleans_foreign_and_broken_entries(tmp_path):
    path = tmp_path / "drawings.json"
    ok = {"id": "r1", "type": "rect", "color": "#00ff00", "width": 3, "opacity": 0.3, "p1": {"t": 1_790_000_000, "p": 1.0}, "p2": {"t": 1_790_000_900, "p": 2.0}}
    hline = {"id": "h1", "type": "hline", "color": "#FFFFFF", "width": 1, "opacity": 1, "p1": {"t": 1_790_000_000, "p": 5.0}, "extra": "x"}
    junk = [{"id": "x", "type": "rect", "color": "red", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "y", "type": "trend", "color": "#000000", "width": 99, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "z z", "type": "trend", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "n", "type": "trend", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": float("nan"), "p": 1}, "p2": {"t": 2, "p": 2}},
            {"id": "no2", "type": "ray", "color": "#000000", "width": 1, "opacity": 1, "p1": {"t": 1, "p": 1}}, "строка", None]
    assert drawings_store.save(path, "BTCUSDT", [ok, hline, *junk]) == 2
    assert drawings_store.save(path, "ETHUSDT", [ok]) == 1
    loaded = drawings_store.load(path, "BTCUSDT")
    assert [d["id"] for d in loaded] == ["r1", "h1"] and "extra" not in loaded[1] and drawings_store.load(path, "XRPUSDT") == []
    assert drawings_store.save(path, "BTCUSDT", []) == 0 and "BTCUSDT" not in json.loads(path.read_text(encoding="utf-8"))  # пустой список убирает запись монеты
    path.write_text("{ не json", encoding="utf-8")
    with pytest.raises(drawings_store.DrawingsError):
        drawings_store.load(path, "BTCUSDT")
    with pytest.raises(drawings_store.DrawingsError):
        drawings_store.save(path, "BTCUSDT", [ok])  # битый файл не затирается молча


def test_send_favorite_builds_the_right_request():
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


@pytest.mark.skipif(shutil.which("node") is None, reason="нужен node для проверки синтаксиса JS")
def test_chart_js_has_valid_syntax():
    out = subprocess.run(["node", "--check", str(chart_window.WEB_DIR / "chart.js")], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr


def test_chart_js_guards_the_known_pitfalls():
    """Страховки, найденные при проверке в браузере: холст выше слоёв графика, диапазон - после первой раскладки, подгрузка истории не срабатывает на сбросе диапазона."""
    js, css = (chart_window.WEB_DIR / "chart.js").read_text(encoding="utf-8"), (chart_window.WEB_DIR / "chart.css").read_text(encoding="utf-8")
    assert "#ov{position:absolute;inset:0;pointer-events:none;z-index:5}" in css
    assert "chart.timeScale().width() <= 0" in js and "!S.quiet && !S.view" in js and "tickMarkFormatter" in js and "setMarkers" in js and "createPriceLine" in js
    assert "subscribeVisibleLogicalRangeChange" in js and "wss://fstream.binance.com/market/stream" in js and "'4h'" in js


def test_drawings_map_by_absolute_time_from_the_candle_centre():
    """Рисунки хранят (время UTC, цена); индекс считается от середины свечи [open, open + iv), поэтому смена таймфрейма не сдвигает их на полсвечи."""
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "tradeai_client" / "chart_web" / "chart.js").read_text(encoding="utf-8")
    assert "rawToLogical(t - barIv() / 2)" in js and "rawToTime(L)" in js and "S.barsIv = ivSec()" in js
    assert "specIv()" in js  # правый край зон сигнала - по таймфрейму сделки, а не выбранному
