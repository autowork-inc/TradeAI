"""Окно графика сделки (2026-10-19, замена прежнего статичного): интерактивный график TradingView Lightweight Charts 4.1 во WebView (pywebview) в ОТДЕЛЬНОМ процессе.

Почему процесс: pywebview хочет собственный цикл событий GUI, а Flet владеет своим окном и asyncio-циклом - вместе в одном процессе они не живут. Каждый график -
`multiprocessing` (spawn), не daemon. В собранном .exe обязателен `multiprocessing.freeze_support()` в точке входа (client/app.py).

Страница (tradeai_client/chart_web: index.html + chart.js + chart.css + lightweight-charts.js) собирается ОДНОЙ строкой (`build_html`): без внешних файлов и сети при загрузке.
Данные идут так: история и опрос - мост Python (`ChartApi.klines`: Binance Futures REST или `/api/candles` сервера для MT5-символов), реальное время Binance - WebSocket kline прямо в странице.
Мост также хранит рисунки (`drawings.json`) и ставит звёздочку «Избранное». Ключ API в страницу не попадает: он живёт только в `ChartApi`.
"""

import atexit
import json
import logging
import multiprocessing
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from tradeai_client import drawings_store

log = logging.getLogger("tradeai_client.chart")
WEB_DIR = Path(__file__).resolve().parent / "chart_web"
TIMEFRAMES = ("5m", "15m", "1h", "4h")
_INTERVAL_BY_MS = {60_000: "1m", 300_000: "5m", 900_000: "15m", 3_600_000: "1h", 14_400_000: "4h"}
SPEC_KEYS = ("bars", "boxes", "segments", "hlines", "vlines", "trade_markers", "setup_markers", "panel", "visible", "digits", "interval_ms", "title")
HTTP_TIMEOUT_S = 12
MAX_KLINES = 1500


def drawings_path() -> Path:
    """Рисунки хранятся в папке пользователя (переживают пересборку .exe); старый файл рядом с .exe копируется туда один раз."""
    from tradeai_client.settings import app_dir, user_dir
    path, legacy = user_dir() / drawings_store.FILE_NAME, app_dir() / drawings_store.FILE_NAME
    if not path.exists() and legacy.exists():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(legacy.read_bytes())
        except OSError:
            return legacy
    return path


def _read(name: str) -> str:
    return (WEB_DIR / name).read_text(encoding="utf-8")


def _js_json(value: Any) -> str:
    """JSON для вставки в <script>: ASCII-режим и экранирование «<» (нет закрывающего тега, U+2028/2029)."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).replace("<", "\\u003c")


def build_config(spec: dict[str, Any], drawings: list[dict[str, Any]]) -> dict[str, Any]:
    """Всё, что нужно странице. БЕЗ секретов: адрес/ключ сервера и ключ API остаются в ChartApi."""
    interval = spec.get("interval") or _INTERVAL_BY_MS.get(int(spec["interval_ms"]), "15m")
    frames = list(TIMEFRAMES) if interval in TIMEFRAMES else [*TIMEFRAMES, interval]
    src = spec.get("source") or {"kind": "none"}
    fav = spec.get("favorite")
    return {
        "interval": interval, "timeframes": frames, "digits": int(spec["digits"]), "focus_ms": spec.get("signal_ms"),
        "source": {"kind": src["kind"], "symbol": src.get("symbol", "")},
        "spec": {k: spec[k] for k in SPEC_KEYS}, "bot": spec.get("bot"), "chr": bool(spec.get("bot")),
        "drawings": drawings, "favorite": {"is_favorite": bool(fav.get("is_favorite"))} if fav else None,
    }


def build_html(cfg: dict[str, Any], shim: str = "") -> str:
    """shim - необязательный скрипт перед приложением: веб-версия подставляет сюда HTTP-мост вместо моста pywebview (tradeai_client/web/charts.py)."""
    html = _read("index.html")
    if not shim:  # окно pywebview: моста-заглушки нет, лишнего <script> в странице тоже
        html = html.replace("<script>/*__SHIM__*/</script>\n", "", 1)
    for marker, text in (("/*__CSS__*/", _read("chart.css")), ("/*__LIB__*/", _read("lightweight-charts.js")), ("/*__CFG__*/", _js_json(cfg)), ("/*__SHIM__*/", shim),
                         ("/*__APP__*/", _read("chart.js"))):
        html = html.replace(marker, text, 1)
    return html


class ChartApi:
    """Мост JS -> Python (pywebview js_api). Публичны только методы: состояние спрятано в `_s`, чтобы pywebview не обходил его как API."""

    def __init__(self, spec: dict[str, Any]):
        self._s = {"spec": spec, "source": spec.get("source") or {"kind": "none"}, "fav": spec.get("favorite"), "symbol": spec.get("coin", "")}

    # ---- свечи
    def klines(self, interval: str, end_ms: int | None = None, limit: int = 1000) -> list[list[float]]:
        """Свечи [open_time_ms, o, h, l, c, v] по возрастанию времени. end_ms - последняя свеча не позже этого момента (прокрутка истории)."""
        src, limit = self._s["source"], max(1, min(int(limit), MAX_KLINES))
        if interval not in _INTERVAL_BY_MS.values():
            raise ValueError(f"неизвестный таймфрейм {interval!r}")
        if src["kind"] == "binance":
            q = {"symbol": src["symbol"], "interval": interval, "limit": limit, **({"endTime": int(end_ms)} if end_ms else {})}
            url, headers = f"{src['rest'].rstrip('/')}/fapi/v1/klines?{urllib.parse.urlencode(q)}", {}
        elif src["kind"] == "server":
            q = {"coin": src["symbol"], "interval": interval, "limit": limit, **({"end": int(end_ms)} if end_ms else {})}
            url, headers = f"{src['url'].rstrip('/')}/api/candles?{urllib.parse.urlencode(q)}", {"X-API-Key": src["key"]}
        else:
            raise RuntimeError("для этого инструмента нет источника свечей")
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=HTTP_TIMEOUT_S) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"источник свечей ответил HTTP {exc.code}") from exc
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"источник свечей недоступен: {type(exc).__name__}") from exc
        rows = body["items"] if isinstance(body, dict) else body
        return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])] for r in rows]

    # ---- рисунки
    def save_drawings(self, text: str) -> int:
        return drawings_store.save(drawings_path(), self._s["symbol"], json.loads(text))

    # ---- избранное
    def toggle_favorite(self, add: bool) -> dict[str, Any]:
        from tradeai_client.chart_favorite import FavoriteSendError, send_favorite

        fav = self._s["fav"]
        if not fav:
            return {"ok": False, "error": "избранное недоступно"}
        try:
            png = None
            if add:
                from tradeai_client.golden_image import render_png
                png = render_png(self._s["spec"])
            send_favorite(fav, bool(add), png)
        except (FavoriteSendError, ValueError, OSError) as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "is_favorite": bool(add)}


def _window_args(spec: dict[str, Any]) -> tuple[ChartApi, str]:
    try:
        drawings = drawings_store.load(drawings_path(), spec.get("coin", ""))
    except drawings_store.DrawingsError as exc:
        log.warning("%s", exc)
        drawings = []
    return ChartApi(spec), build_html(build_config(spec, drawings))


STACK_DIR = Path(__import__("tempfile").gettempdir()) / "tradeai_charts"  # общая «стопка» окон графиков (все процессы окон этого ПК)
STACK_FRESH_S = 3.0  # запись окна старше - окно закрыто/процесс умер
STACK_POLL_S = 0.5


GEOMETRY_FILE = STACK_DIR / "last_geometry.json"  # место и размер последнего ЗАКРЫТОГО окна графика: по нему открывается следующее


def stack_front(entries: dict[str, dict[str, Any]], now: float) -> str | None:
    """Ключ ПЕРВОГО открытого из живых окон графиков."""
    alive = [(e["order"], k) for k, e in entries.items() if now - e.get("beat", 0) <= STACK_FRESH_S]
    return min(alive)[1] if alive else None


def place_after(entries: dict[str, dict[str, Any]], me: str, now: float) -> int | None:
    """HWND окна, ПОД которым должно встать новое окно: самое позднее из открытых раньше него (живых). Открыли a, b, c, d, e - сверху вниз
    a, b, c, d, e: каждое следующее уходит под предыдущие и не перекрывает первое. None - открытых раньше нет (окно встаёт спереди)."""
    order = entries[me]["order"]
    older = [(e["order"], e.get("hwnd")) for k, e in entries.items()
             if k != me and e["order"] < order and now - e.get("beat", 0) <= STACK_FRESH_S and e.get("hwnd")]
    return max(older)[1] if older else None


def load_geometry() -> dict[str, Any]:
    """Место и размер последнего закрытого окна графика; нет - на весь экран."""
    try:
        g = json.loads(GEOMETRY_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"maximized": True}
    ok = isinstance(g, dict) and all(isinstance(g.get(k), (int, float)) for k in ("x", "y", "width", "height")) and g["width"] >= 400 and g["height"] >= 300
    return g if ok else {"maximized": True}


def _read_stack() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for f in STACK_DIR.glob("*.json"):
        if f == GEOMETRY_FILE:
            continue
        try:
            out[f.stem] = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):  # файл соседнего окна пишется прямо сейчас или уже удалён - пропускаем этот опрос
            continue
    return out


def _own_hwnd(title: str) -> int | None:
    """HWND окна этого процесса с нашим заголовком (Windows); иначе None."""
    import ctypes
    import os
    from ctypes import wintypes

    if os.name != "nt":
        return None
    user32 = ctypes.windll.user32
    found: list[int] = []
    pid = os.getpid()

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def each(hwnd, _):
        owner = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value == pid and user32.IsWindowVisible(hwnd):
            buf = ctypes.create_unicode_buffer(512)
            user32.GetWindowTextW(hwnd, buf, 512)
            if buf.value == title:
                found.append(int(hwnd))
                return False
        return True

    user32.EnumWindows(each, 0)
    return found[0] if found else None


def _insert_below(hwnd: int, after: int) -> None:
    """Поставить окно сразу ПОД `after` в порядке окон, без активации и без изменения места/размера (Windows SetWindowPos)."""
    import ctypes
    import os

    if os.name == "nt":
        ctypes.windll.user32.SetWindowPos(hwnd, after, 0, 0, 0, 0, 0x0001 | 0x0002 | 0x0010)  # SWP_NOSIZE | SWP_NOMOVE | SWP_NOACTIVATE


def _allow_activation(hwnd: int) -> None:
    """Снять WS_EX_NOACTIVATE, который pywebview ставит окну с focus=False: с ним окно нельзя активировать - оно проваливается под все приложения
    и не выходит наверх, когда закрывают окно над ним (жалоба 2026-10-24)."""
    import ctypes
    import os

    if os.name == "nt":
        user32 = ctypes.windll.user32
        user32.SetWindowLongW(hwnd, -20, user32.GetWindowLongW(hwnd, -20) & ~0x8000000)


def _is_right_below(hwnd: int, after: int) -> bool:
    """Окно стоит сразу под `after` в порядке окон (GetWindow(after, GW_HWNDNEXT) == hwnd)."""
    import ctypes
    import os

    return os.name != "nt" or int(ctypes.windll.user32.GetWindow(after, 2) or 0) == hwnd


def _raise_once(win: Any) -> None:
    """Поднять окно поверх остальных один раз (Windows не даёт фокус новому процессу - короткий on_top это обходит) и не держать его «всегда сверху»."""
    win.on_top = True
    win.show()
    time.sleep(0.2)
    win.on_top = False


def run_chart_window(spec: dict[str, Any]) -> None:
    """Точка входа процесса окна: строит страницу и блокируется до закрытия окна (1.6.0).
    Место и размер - как у последнего закрытого окна графика (первый раз - на весь экран). Порядок: открыли a, b, c, d, e - сверху вниз a, b, c, d, e
    (каждое следующее открывается в фоне под предыдущими, без фокуса); закрытие окна никого не поднимает."""
    import os
    import threading

    import webview

    api, html = _window_args(spec)
    STACK_DIR.mkdir(parents=True, exist_ok=True)
    key = f"{os.getpid()}"
    order = int(spec.get("chart_order") or time.time_ns())
    me = STACK_DIR / f"{key}.json"
    me.write_text(json.dumps({"order": order, "beat": time.time()}), encoding="utf-8")
    first = stack_front(_read_stack(), time.time()) == key
    geo = load_geometry()
    title = f"TradeAI · {spec['title']}"
    kw: dict[str, Any] = {"maximized": True} if geo.get("maximized") else {"x": int(geo["x"]), "y": int(geo["y"]), "width": int(geo["width"]),
                                                                          "height": int(geo["height"])}
    window = webview.create_window(title, html=html, js_api=api, background_color="#0e1117", focus=first, **{"width": 1500, "height": 900, **kw})
    closed = threading.Event()
    state = {"maximized": bool(geo.get("maximized")), "hwnd": None}

    def on_maximized() -> None:
        state["maximized"] = True

    def on_restored() -> None:
        state["maximized"] = False

    def on_closing() -> None:
        """Запомнить место и размер закрываемого окна: следующее окно графика откроется так же."""
        g = {"maximized": state["maximized"]}
        for k in ("x", "y", "width", "height"):
            v = getattr(window, k, None)
            if isinstance(v, (int, float)):
                g[k] = v
        GEOMETRY_FILE.write_text(json.dumps(g), encoding="utf-8")

    window.events.maximized += on_maximized
    window.events.restored += on_restored
    window.events.closing += on_closing
    window.events.closed += closed.set

    def keep_stack(win: Any) -> None:
        """Поток окна: место в стопке сразу после показа, затем «пульс» (живые окна стопки) раз в 0.5 с."""
        if state["maximized"]:
            win.maximize()
        state["hwnd"] = _own_hwnd(title)
        me.write_text(json.dumps({"order": order, "beat": time.time(), "hwnd": state["hwnd"]}), encoding="utf-8")
        after = place_after(_read_stack(), key, time.time())
        if after is not None and state["hwnd"] is not None:
            for _ in range(10):  # в фоне, прямо под предыдущим окном графика; показ/разворот окна может переставить его - проверяем и повторяем
                _insert_below(state["hwnd"], after)
                time.sleep(0.15)
                if _is_right_below(state["hwnd"], after):
                    break
        elif first:
            _raise_once(win)  # самое первое окно - спереди
        if state["hwnd"] is not None:
            win.focus = True  # pywebview больше не вернёт WS_EX_NOACTIVATE при активации
            _allow_activation(state["hwnd"])  # окно в фоне, но его можно активировать (клик, закрытие окна над ним)
        while not closed.is_set():
            me.write_text(json.dumps({"order": order, "beat": time.time(), "hwnd": state["hwnd"]}), encoding="utf-8")
            closed.wait(STACK_POLL_S)

    try:
        webview.start(keep_stack, window)
    finally:
        me.unlink(missing_ok=True)


_PROCESSES: list["multiprocessing.process.BaseProcess"] = []


def terminate_all() -> None:
    """Закрыть окна графиков, открытые этой сессией (вызывается при выходе: иначе процесс приложения ждал бы их закрытия)."""
    for p in _PROCESSES:
        if p.is_alive():
            p.terminate()
    _PROCESSES.clear()


def open_chart(spec: dict[str, Any]) -> "multiprocessing.process.BaseProcess":
    """Открывает окно графика в отдельном процессе и сразу возвращается (интерфейс не блокируется)."""
    if not _PROCESSES:
        atexit.register(terminate_all)  # регистрируется позже atexit-обработчика multiprocessing -> выполняется раньше его join()
    _PROCESSES[:] = [p for p in _PROCESSES if p.is_alive()]
    spec = {**spec, "chart_order": time.time_ns()}  # порядок открытия: первое окно - верхнее в стопке
    process = multiprocessing.get_context("spawn").Process(target=run_chart_window, args=(spec,), name=f"chart-{spec['trade_id']}"[:60])
    process.start()
    _PROCESSES.append(process)
    return process


# ------------------------------------------------------------------------------------------------ самопроверка


def _selftest_spec() -> dict[str, Any]:
    """Мини-сделка из выдуманных свечей: проверяет цепочку «spec -> окно -> снимок», а не данные (источник свечей не задан: показываются свечи снапшота)."""
    from tradeai_client.chart_spec import build_chart_spec

    iv, t0, price = 900_000, 1_790_000_100_000 - 1_790_000_100_000 % 900_000, 100.0
    candles = []
    for i in range(60):
        o = price + (i % 7 - 3) * 0.2
        candles.append({"open_time": t0 + i * iv, "open": o, "high": o + 0.8, "low": o - 0.8, "close": o + 0.1, "volume": 10.0 + i, "atr": 1.0})
        price = o + 0.1
    sig_ms = t0 + 60 * iv + 1000
    entry_ms = sig_ms + 2 * iv
    trade = {"id": "selftest", "strategy_name": "smc", "coin": "TESTUSDT", "side": "short", "kind": "limit", "mode": "shadow", "status": "closed",
             "signal_time": _iso(sig_ms), "entry_time": _iso(entry_ms), "exit_time": _iso(entry_ms + 4 * iv),
             "planned_entry": 100.0, "entry_price": 100.0, "exit_price": 98.0, "sl": 102.0, "tp": 94.0, "r_multiple": 1.0, "pnl": 10.0,
             "exit_reason": "tp", "gap_affected": False, "plan": {"levels": [[1.0, 0.5], [3.0, 0.4]]}}
    closes = [{"seq": 1, "ts": _iso(entry_ms + 4 * iv), "reason": "scale_1r", "frac": 0.5, "price": 98.0, "net_r": 0.5}]
    geometry = [{"type": "zone", "t1": t0 + 50 * iv, "t2": sig_ms + 10 * iv, "p1": 100.5, "p2": 99.5, "label": "FVG"},
                {"type": "marker", "t1": t0 + 45 * iv, "t2": t0 + 45 * iv, "p1": 101.0, "p2": 101.0, "label": "свип"}]
    snap = {"trade": trade, "closes": closes, "candles": candles, "geometry": geometry, "meta": {"interval": "15m"},
            "reason": {"title": "Самопроверка", "summary": "Проверка окна графика.", "facts": [{"label": "Таймфрейм", "value": "15m"}]}}
    return build_chart_spec(snap, None, now_ms=sig_ms + 20 * iv)


def _iso(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _selftest_target(spec: dict[str, Any], out: str) -> None:
    import base64

    import webview

    api, html = _window_args(spec)
    window = webview.create_window("TradeAI · самопроверка", html=html, js_api=api, width=1300, height=800)

    def work() -> None:
        for _ in range(60):  # ждём, пока страница построит график
            time.sleep(0.5)
            if window.evaluate_js("window.__ready === true"):
                break
        time.sleep(1.0)
        data = window.evaluate_js("window.__selftest()")
        if isinstance(data, str) and data.startswith("data:image/png;base64,"):
            Path(out).write_bytes(base64.b64decode(data.split(",", 1)[1]))
        window.destroy()

    webview.start(work)


def selftest(out: str, timeout_s: float = 90.0) -> int:
    """`TradeAI-Monitor.exe --selftest-chart снимок.png`: открывает пробное окно графика и сохраняет снимок. 0 - окно работает.
    Нужна для проверки собранного .exe (состав PyInstaller, WebView2) и диагностики «окно графика пустое»."""
    target = Path(out)
    target.unlink(missing_ok=True)
    process = multiprocessing.get_context("spawn").Process(target=_selftest_target, args=(_selftest_spec(), str(target)))
    process.start()
    process.join(timeout_s)
    if process.is_alive():
        process.terminate()
        return 1
    return 0 if target.exists() and target.stat().st_size > 5_000 and process.exitcode == 0 else 1
