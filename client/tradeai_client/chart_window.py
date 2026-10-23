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


def run_chart_window(spec: dict[str, Any]) -> None:
    """Точка входа процесса окна: строит страницу и блокируется до закрытия окна."""
    import webview

    api, html = _window_args(spec)
    # 2026-10-23: окно сразу развёрнуто на весь экран, поверх остальных окон и в фокусе (раньше открывалось маленьким, и его приходилось искать и разворачивать).
    window = webview.create_window(f"TradeAI · {spec['title']}", html=html, js_api=api, width=1500, height=900, background_color="#0e1117",
                                   maximized=True, on_top=True, focus=True)
    webview.start(_bring_to_front, window)


def _bring_to_front(window: Any) -> None:
    """Вызывается pywebview уже после показа окна: повторно поднимает его поверх главного дашборда (Windows может отказать в фокусе новому процессу, `on_top` это обходит)."""
    window.on_top = True
    window.show()


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
