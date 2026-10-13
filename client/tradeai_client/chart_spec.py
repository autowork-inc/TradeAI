"""Ответ /api/trades/{id}/snapshot (+ свечи после сигнала) -> `spec`: чистый словарь с примитивами для окна графика.

Здесь вся «геометрия» графика, но нет ни Flet, ни lightweight-charts: spec легко тестировать и безопасно передавать в
отдельный процесс (multiprocessing). Время везде - миллисекунды UTC; бары привязаны к сетке таймфрейма.

Единый формат геометрии `{type: zone|line|marker, t1, t2, p1, p2, label}` приходит из снапшота: клиент рисует его, ничего не зная о
стратегиях. Линии плана (SL/TP/вход) рисуются из полей самой сделки (фактические цены), а не из геометрии - чтобы не дублировать.
"""

import re
import textwrap
from datetime import datetime, timezone
from typing import Any

from tradeai_client import fmt

UP, DOWN, AMBER, BLUE, VIOLET, GREY = "#26a69a", "#ef5350", "#ffb300", "#4aa3ff", "#b388ff", "#9aa3b2"
MAX_GRID_BARS = 900
PANEL_WIDTH_CHARS = 44  # ширина боковой панели ~31% окна (см. chart_window.PANEL_WIDTH)
_UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
_PLAN_LABEL = re.compile(r"^(SL|TP|вход)", re.IGNORECASE)  # линии плана сделки: рисуются из полей сделки
_CANDLE_KEYS = ("open_time", "open", "high", "low", "close", "volume")


class ChartDataError(ValueError):
    """Снапшот нельзя нарисовать (пустые свечи, неизвестный таймфрейм, разрыв сетки)."""


def interval_to_ms(interval: str) -> int:
    m = re.fullmatch(r"(\d+)([mhd])", str(interval))
    if not m or int(m.group(1)) <= 0:
        raise ChartDataError(f"Неизвестный таймфрейм в снапшоте: {interval!r}")
    return int(m.group(1)) * _UNIT_MS[m.group(2)]


def _bars(candles: list[dict[str, Any]], iv: int) -> list[dict[str, Any]]:
    if not candles:
        raise ChartDataError("В снапшоте нет свечей")
    bars: list[dict[str, Any]] = []
    for c in candles:
        if any(c.get(k) is None for k in _CANDLE_KEYS):
            raise ChartDataError(f"Свеча снапшота неполная: {c!r}")
        t = int(c["open_time"])
        if bars and t != bars[-1]["t"] + iv:
            raise ChartDataError(f"Разрыв в свечах снапшота: {bars[-1]['t']} -> {t} (ожидалась сетка {iv} мс)")
        bars.append({"t": t, "o": float(c["open"]), "h": float(c["high"]), "l": float(c["low"]), "c": float(c["close"]),
                     "v": float(c["volume"]), "post": False})
    return bars


_UNSAFE = re.compile(r"[\"'`\\\x00-\x1f$<>]")


def clean_text(text: Any) -> str:
    """Текст, который библиотека графика подставляет в JavaScript без экранирования (ячейки таблиц, подписи линий): убираем кавычки,
    обратные косые, управляющие символы, `$`, `<`, `>`. Подписи приходят из БД; чистим их независимо от того, насколько им доверяем."""
    return _UNSAFE.sub(lambda m: "\u2033" if m.group(0) in "\"'`" else " ", str(text)).strip()


def clean_text_lines(text: Any) -> list[str]:
    return [clean_text(x) for x in str(text).splitlines()] or [""]


def _wrap(text: str) -> list[str]:
    out: list[str] = []
    for line in clean_text_lines(text):
        out += textwrap.wrap(line, PANEL_WIDTH_CHARS, subsequent_indent="   ") or [""]
    return out


def _utc(ms: int | None) -> str:
    if ms is None:
        return fmt.DASH
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%d.%m %H:%M")


def build_chart_spec(snap: dict[str, Any], post_klines: list[dict[str, float]] | None = None, *, now_ms: int,
                     post_note: str | None = None) -> dict[str, Any]:
    trade, meta, closes = snap["trade"], snap.get("meta") or {}, snap.get("closes") or []
    iv = interval_to_ms(meta.get("interval", ""))
    bars = _bars(snap["candles"], iv)
    snapshot_last, snapshot_close = bars[-1]["t"], bars[-1]["c"]
    first_t = bars[0]["t"]
    long = trade["side"] == "long"

    # свечи после сигнала (если удалось получить): только строго после последней свечи снапшота и только закрытые
    for k in sorted(post_klines or [], key=lambda x: x["t"]):
        if k["t"] > bars[-1]["t"]:
            bars.append({"t": k["t"], "o": k["o"], "h": k["h"], "l": k["l"], "c": k["c"], "v": k["v"], "post": True})
    last_bar_t = bars[-1]["t"]

    def snap_t(ms: int) -> int:
        return ms - ms % iv

    signal_ms = fmt.to_ms(trade.get("signal_time"))
    entry_ms, exit_ms = fmt.to_ms(trade.get("entry_time")), fmt.to_ms(trade.get("exit_time"))
    close_events = [(fmt.to_ms(c["ts"]), c) for c in closes]
    mark_times = [snap_t(x) for x in (entry_ms, exit_ms, *(t for t, _ in close_events)) if x is not None]
    grid_end = max([last_bar_t, *mark_times]) + (3 * iv if mark_times else 0)
    if trade["status"] in ("open", "pending_limit", "pending_market") and not post_klines:
        grid_end = max(grid_end, snap_t(now_ms))
    grid_end = min(grid_end, first_t + (MAX_GRID_BARS - 1) * iv)
    last_bar_t = min(last_bar_t, grid_end)
    bars = [b for b in bars if b["t"] <= grid_end]

    ref_price = trade.get("entry_price") or trade.get("planned_entry") or snapshot_close
    candle_ts = {b["t"] for b in bars}

    # маркеры сделки (вход, закрытия): {t, price, position, shape, color, text}
    trade_markers: list[dict[str, Any]] = []
    if entry_ms is not None and trade.get("entry_price") is not None:
        trade_markers.append({"t": snap_t(entry_ms), "price": trade["entry_price"], "position": "below" if long else "above",
                              "shape": "arrow_up" if long else "arrow_down", "color": BLUE, "text": f"Вход {fmt.fmt_price(trade['entry_price'])}"})
    for i, (ts, c) in enumerate(close_events):
        if ts is None:
            continue
        net = c.get("net_r")
        color = GREY if not net else (UP if net > 0 else DOWN)
        label = clean_text(fmt.EXIT_RU.get(c["reason"], c["reason"]))
        trade_markers.append({"t": snap_t(ts), "price": c["price"], "position": "above" if long else "below",
                              "shape": "square" if trade["status"] == "closed" and i == len(close_events) - 1 else "circle", "color": color,
                              "text": f"{label} {fmt.fmt_r(net)}"})
    trade_markers = [m for m in trade_markers if m["t"] <= grid_end]

    # геометрия сетапа
    boxes, segments, setup_markers = [], [], []
    for g in snap.get("geometry") or []:
        label = clean_text(g.get("label", ""))
        if g["type"] == "zone":
            lo, hi = sorted((float(g["p1"]), float(g["p2"])))
            boxes.append({"t1": max(int(g["t1"]), first_t), "t2": min(int(g["t2"]), grid_end), "p1": hi, "p2": lo, "label": label,
                          "color": AMBER, "fill": "rgba(255,179,0,0.20)"})
        elif g["type"] == "line":
            if not _PLAN_LABEL.match(label):
                segments.append({"t1": snap_t(max(int(g["t1"]), first_t)), "t2": snap_t(min(int(g["t2"]), grid_end)), "price": float(g["p1"]),
                                 "label": label, "color": VIOLET if "пул" in label.lower() else AMBER})
        elif g["type"] == "marker":
            t = snap_t(int(g["t1"]))
            if t in candle_ts:
                bar = next(b for b in bars if b["t"] == t)
                above = float(g["p1"]) >= (bar["h"] + bar["l"]) / 2
                setup_markers.append({"t": t, "position": "above" if above else "below", "shape": "circle",
                                      "color": AMBER if label == "свип" else VIOLET, "text": label})
        else:
            raise ChartDataError(f"Неизвестный тип геометрии: {g['type']!r}")

    # горизонтальные линии плана
    hlines: list[dict[str, Any]] = []
    sl, tp = trade.get("sl"), trade.get("tp")
    planned, entry = trade.get("planned_entry"), trade.get("entry_price")
    if sl is not None:
        hlines.append({"price": sl, "color": DOWN, "style": "dashed", "text": f"SL {fmt.fmt_price(sl)}", "width": 2})
    if tp is not None:
        hlines.append({"price": tp, "color": UP, "style": "dashed", "text": f"TP {fmt.fmt_price(tp)}", "width": 2})
    if entry is not None:
        hlines.append({"price": entry, "color": BLUE, "style": "solid", "text": f"Вход {fmt.fmt_price(entry)}", "width": 1})
    if planned is not None and (entry is None or abs(planned - entry) > 1e-12 * max(1.0, abs(entry))):
        hlines.append({"price": planned, "color": BLUE, "style": "dotted", "text": f"План входа {fmt.fmt_price(planned)}", "width": 1})
    base, risk = (entry if entry is not None else planned), None
    if base is not None and sl is not None:
        risk = abs(base - sl)
    plan_levels = (trade.get("plan") or {}).get("levels") or []
    if risk:
        for k, frac in plan_levels:
            price = base + k * risk if long else base - k * risk
            if tp is not None and abs(price - tp) <= 1e-9 * max(1.0, abs(tp)):
                continue  # совпадает с TP - не дублируем
            hlines.append({"price": price, "color": UP, "style": "dotted", "text": f"+{k:g}R ({frac * 100:.0f}%)", "width": 1})

    # вспомогательный невидимый ряд: продлевает ось времени за последнюю свечу (маркеры/зоны/линии не должны пропадать)
    by_bar: dict[int, float] = {}
    for m in trade_markers:
        by_bar.setdefault(m["t"], m["price"])
    helper = [[t, by_bar.get(t, ref_price)] for t in range(first_t, grid_end + 1, iv)]

    vlines = []
    if signal_ms is not None:
        vlines.append({"t": snap_t(signal_ms), "color": AMBER, "text": "Сигнал"})

    # текстовые панели
    reason = snap.get("reason") or {}
    reason_lines = _wrap(reason.get("title", "")) + [""] + [ln for part in str(reason.get("summary", "")).split("\n") for ln in _wrap(part)]
    reason_lines += [""] + [ln for f in reason.get("facts", []) for ln in _wrap(f"{f['label']}: {f['value']}")]
    result = [f"Статус: {fmt.STATUS_RU.get(trade['status'], trade['status'])}   Итог: {fmt.fmt_r(trade.get('r_multiple'))}"
              + (f"   PnL: {trade['pnl']:+.2f} USDT".replace("-", "−") if trade.get("pnl") is not None else "")]
    if trade.get("exit_reason"):
        result.append(f"Причина выхода: {fmt.EXIT_RU.get(trade['exit_reason'], trade['exit_reason'])}")
    for i, (ts, c) in enumerate(close_events, start=1):
        result.append(f"{i}. {fmt.EXIT_RU.get(c['reason'], c['reason'])}  {_utc(ts)} UTC  @ {fmt.fmt_price(c['price'])}  {fmt.fmt_r(c.get('net_r'))}")
    if trade.get("gap_affected"):
        result.append("⚠ Сделка помечена gap_affected (обрыв потока тиков): в статистику не входит")
    notes = []
    if post_note:
        notes.append(post_note)
    elif not any(b["post"] for b in bars):
        notes.append("Свечи после сигнала не загружены: показаны только свечи снапшота.")
    result += notes

    return {
        "title": clean_text(f"{trade['coin']} {meta.get('interval', '')} · {fmt.SIDE_RU.get(trade['side'], trade['side'])} · {fmt.STATUS_RU.get(trade['status'], trade['status'])}"),
        "trade_id": trade["id"], "interval_ms": iv, "snapshot_last_t": snapshot_last,
        "bars": bars, "helper": helper, "trade_markers": trade_markers, "setup_markers": setup_markers,
        "boxes": boxes, "segments": segments, "hlines": hlines, "vlines": vlines,
        "visible": [max(first_t, (snap_t(signal_ms) if signal_ms else snapshot_last) - 45 * iv), grid_end + 2 * iv],
        "reason_lines": reason_lines, "result_lines": [x for r in result for x in _wrap(r)],
    }
