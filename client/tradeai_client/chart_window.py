"""Окно графика сделки: lightweight-charts-python (обёртка TradingView) в ОТДЕЛЬНОМ процессе.

Почему процесс. Окно lightweight-charts - это pywebview, который хочет собственный цикл событий GUI; Flet владеет своим окном и
asyncio-циклом, поэтому оба в одном процессе не живут. Каждый график - `multiprocessing` (spawn) процесс, не daemon (библиотека
сама запускает дочерний процесс webview, а у daemon-процессов дочерних быть не может). В собранном .exe обязателен
`multiprocessing.freeze_support()` в точке входа (client/app.py).

`apply_spec(chart, spec)` не знает, какой именно график ему дали (окно или статический HTML) - на этом держатся тесты и проверка
глазами без открытия окон.
"""

import atexit
import multiprocessing
import time
from pathlib import Path
from typing import Any

BG, TEXT, GRID = "#0e1117", "#c9d1d9", "rgba(255,255,255,0.05)"
UP, DOWN = "#26a69a", "#ef5350"
INVISIBLE = "rgba(0,0,0,0)"
INNER_WIDTH = 0.69  # доля окна под сам график; остальное - боковая панель с текстом (так задумано в lightweight-charts: inner_width + таблицы справа)
PANEL_WIDTH = 0.30


def _ts(ms: int):
    import pandas as pd
    return pd.Timestamp(int(ms), unit="ms")  # наивное время = UTC; ns-разрешение: lightweight-charts делит на 10**9 (pandas 3 даёт us и ломает ось)


def _precision(price: float) -> int:
    return 2 if price >= 100 else 4 if price >= 1 else 7


def _frame(rows: list[dict[str, Any]], columns: dict[str, str]):
    import pandas as pd
    df = pd.DataFrame({"time": pd.DatetimeIndex([_ts(r["t"]) for r in rows]).astype("datetime64[ns]"),
                       **{dst: [r[src] for r in rows] for dst, src in columns.items()}})
    return df


def apply_spec(chart: Any, spec: dict[str, Any]) -> None:
    """Рисует spec (см. chart_spec.build_chart_spec) на `chart`: свечи, зона FVG, линии SL/TP/входа, маркеры, панели с текстом."""
    import pandas as pd

    bars = spec["bars"]
    chart.layout(background_color=BG, text_color=TEXT, font_size=12)
    chart.grid(color=GRID)
    chart.candle_style(up_color=UP, down_color=DOWN, border_up_color=UP, border_down_color=DOWN, wick_up_color=UP, wick_down_color=DOWN)
    chart.volume_config(up_color="rgba(38,166,154,0.35)", down_color="rgba(239,83,80,0.35)")
    chart.legend(visible=True, ohlc=True, percent=False, lines=False, font_size=12)
    chart.precision(_precision(bars[-1]["c"]))
    chart.set(_frame(bars, {"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"}))

    # невидимый ряд на всей сетке: продлевает ось времени за последнюю свечу и держит маркеры/линии вне диапазона свечей
    helper = chart.create_line(name="grid", color=INVISIBLE, width=1, price_line=False, price_label=False)
    helper.set(pd.DataFrame({"time": pd.DatetimeIndex([_ts(t) for t, _ in spec["helper"]]).astype("datetime64[ns]"),
                             "grid": [v for _, v in spec["helper"]]}))

    for box in spec["boxes"]:
        chart.box(_ts(box["t1"]), box["p1"], _ts(box["t2"]), box["p2"], color=box["color"], fill_color=box["fill"], width=1)
    for h in spec["hlines"]:
        chart.horizontal_line(h["price"], color=h["color"], width=h["width"], style=h["style"], text=h["text"])
    for i, seg in enumerate(spec["segments"]):
        line = chart.create_line(name=f"seg{i}", color=seg["color"], style="dashed", width=1, price_line=False, price_label=False)
        line.set(pd.DataFrame({"time": pd.DatetimeIndex([_ts(seg["t1"]), _ts(max(seg["t2"], seg["t1"] + spec["interval_ms"]))]).astype("datetime64[ns]"),
                               f"seg{i}": [seg["price"], seg["price"]]}))
        line.marker(time=_ts(seg["t1"]), position="above", shape="circle", color=seg["color"], text=seg["label"])
    for v in spec["vlines"]:
        chart.vertical_line(_ts(v["t"]), color=v["color"], width=1, style="dashed", text=v["text"])

    if spec["setup_markers"]:
        chart.marker_list([{"time": _ts(m["t"]), "position": m["position"], "shape": m["shape"], "color": m["color"], "text": m["text"]}
                           for m in sorted(spec["setup_markers"], key=lambda m: m["t"])])
    if spec["trade_markers"]:
        helper.marker_list([{"time": _ts(m["t"]), "position": m["position"], "shape": m["shape"], "color": m["color"], "text": m["text"]}
                            for m in sorted(spec["trade_markers"], key=lambda m: m["t"])])

    chart.set_visible_range(_ts(spec["visible"][0]), _ts(spec["visible"][1]))
    _panels(chart, spec)


def _panels(chart: Any, spec: dict[str, Any]) -> None:
    """Боковая панель справа от графика: «Причина входа» и «Результат» (одна строка таблицы - одна строка текста, длинное - с прокруткой)."""
    reason, result = spec["reason_lines"], spec["result_lines"]
    reason_h = min(0.64, 0.05 + 0.0245 * len(reason))
    result_h = min(0.98 - reason_h, 0.05 + 0.0245 * len(result))
    for title, lines, height in (("Причина входа", reason, reason_h), ("Результат", result, result_h)):
        if not lines:
            continue
        table = chart.create_table(width=PANEL_WIDTH, height=height, headings=(title,), widths=(1.0,), alignments=("left",), position="right",
                                   draggable=False, background_color="rgba(14,17,23,0.95)", border_color="rgb(60,66,78)")
        for line in lines:
            table.new_row(line)


def run_chart_window(spec: dict[str, Any]) -> None:
    """Точка входа процесса окна: строит график и блокируется до закрытия окна."""
    from lightweight_charts import Chart

    chart = Chart(width=1400, height=800, title=f"TradeAI · {spec['title']}", toolbox=False, inner_width=INNER_WIDTH)
    apply_spec(chart, spec)
    chart.show(block=True)


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
    """Мини-сделка из выдуманных свечей: проверяет цепочку «spec -> окно lightweight-charts -> снимок», а не данные."""
    from tradeai_client.chart_spec import build_chart_spec

    iv, t0, price = 900_000, 1_790_000_100_000 - 1_790_000_100_000 % 900_000, 100.0
    candles = []
    for i in range(60):
        o = price + (i % 7 - 3) * 0.2
        candles.append({"open_time": t0 + i * iv, "open": o, "high": o + 0.8, "low": o - 0.8, "close": o + 0.1, "volume": 10.0 + i, "atr": 1.0})
        price = o + 0.1
    sig_ms = t0 + 60 * iv + 1000
    trade = {"id": "selftest", "strategy_name": "smc", "coin": "TESTUSDT", "side": "short", "kind": "limit", "mode": "shadow", "status": "closed",
             "signal_time": "2026-09-21T10:00:01.000Z", "entry_time": "2026-09-21T10:30:00.000Z", "exit_time": "2026-09-21T12:00:00.000Z",
             "planned_entry": 100.0, "entry_price": 100.0, "exit_price": 98.0, "sl": 102.0, "tp": 94.0, "r_multiple": 1.0, "pnl": 10.0,
             "exit_reason": "tp", "gap_affected": False, "plan": {"levels": [[1.0, 0.5], [3.0, 0.4]]}}
    entry_ms = sig_ms + 2 * iv
    closes = [{"seq": 1, "ts": "2026-09-21T12:00:00.000Z", "reason": "scale_1r", "frac": 0.5, "price": 98.0, "net_r": 0.5}]
    trade["signal_time"] = _iso(sig_ms)
    trade["entry_time"], trade["exit_time"] = _iso(entry_ms), _iso(entry_ms + 4 * iv)
    closes[0]["ts"] = trade["exit_time"]
    geometry = [{"type": "zone", "t1": t0 + 50 * iv, "t2": sig_ms + 10 * iv, "p1": 100.5, "p2": 99.5, "label": "FVG"},
                {"type": "marker", "t1": t0 + 45 * iv, "t2": t0 + 45 * iv, "p1": 101.0, "p2": 101.0, "label": "свип"}]
    snap = {"trade": trade, "closes": closes, "candles": candles, "geometry": geometry, "meta": {"interval": "15m"},
            "reason": {"title": "Самопроверка", "summary": "Проверка окна графика.", "facts": [{"label": "Таймфрейм", "value": "15m"}]}}
    return build_chart_spec(snap, None, now_ms=sig_ms + 20 * iv)


def _iso(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _selftest_target(spec: dict[str, Any], out: str) -> None:
    from lightweight_charts import Chart

    chart = Chart(width=1000, height=640, title="TradeAI · самопроверка", toolbox=False, inner_width=INNER_WIDTH)
    apply_spec(chart, spec)
    chart.show(block=False)
    time.sleep(5)  # дать WebView2 нарисовать
    Path(out).write_bytes(chart.screenshot())
    chart.exit()


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
