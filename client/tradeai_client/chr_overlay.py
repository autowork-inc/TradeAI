"""Разметка логики стратегий CHR для графика: «что увидел бот» (2026-10-19).

Бот пишет в снапшот сделки линии (уровень / наклонная линия / поджатие), маркеры (2-я опора, пробой) и в `meta` - ЧИСЛО касаний и ЦЕНЫ точек поджатия, но не время
и цену каждого касания. Недостающие точки клиент восстанавливает по свечам снапшота ТЕМ ЖЕ правилом, что детекторы (strategies/chr_common.py, chr_horizontal.py,
chr_trendline.py): пивот (D2, окно k = 2) подтверждён на баре i + k, поэтому для сигнальной свечи j берутся пивоты с i <= j - 1 - k; касание уровня - экстремум пивота
не дальше touch_atr * ATR[j-1] от уровня; касание наклонной линии - то же относительно значения линии в момент пивота; поджатие - три последних пивота противоположного вида.
Сверка с ботом: число касаний и цены точек поджатия сравниваются с `meta`; результат («совпало» / «не совпало») показывается в панели, а не скрывается.
Пороги - константы CHR_H_* / CHR_T_* стратегий; здесь продублированы значениями по умолчанию (если вы меняете калибровку бота - обновите и их).

Результат - примитивы для JS-графика: line / point / zone / text (время в мс UTC, цены), плюс `notes` (текстовое объяснение) и `verified`."""

from typing import Any

PIVOT_K = 2
TOUCH_ATR = 0.25
HORIZONTAL_WINDOW = 24
TRENDLINE_WINDOW = 48
TOUCH_DEPTH = 4  # пивотов цепочки, проверяемых на касание наклонной линии (CHR_T_TOUCH_DEPTH)
SQUEEZE_POINTS = 3
STRATEGIES = ("Horizontal_Breakout_CHR", "Trendline_Breakout_CHR")

AMBER, VIOLET, BLUE, GREY = "#ffb300", "#b07cf0", "#4aa3ff", "#8b93a3"


def is_chr(strategy_id: str | None) -> bool:
    return strategy_id in STRATEGIES


def pivots(candles: list[dict[str, Any]], kind: str, k: int = PIVOT_K) -> list[int]:
    """Индексы баров-пивотов (D2): high[i-k..i-1] < high[i] >= high[i+1..i+k] (для 'low' зеркально). Пивот подтверждён на баре i + k."""
    out = []
    for i in range(k, len(candles) - k):
        v = candles[i][kind]
        left = [candles[x][kind] for x in range(i - k, i)]
        right = [candles[x][kind] for x in range(i + 1, i + k + 1)]
        if kind == "high":
            ok = max(left) < v and v >= max(right)
        else:
            ok = min(left) > v and v <= min(right)
        if ok:
            out.append(i)
    return out


def _find(geometry: list[dict[str, Any]], kind: str, *labels: str) -> dict[str, Any] | None:
    return next((g for g in geometry if g.get("type") == kind and any(g.get("label", "").startswith(x) for x in labels)), None)


def _near(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b)) + 1e-12


def build_overlay(snap: dict[str, Any]) -> dict[str, Any] | None:
    """-> {"items": [...], "notes": [...], "verified": bool | None} или None, если сделка не CHR / нет данных для разметки."""
    trade, meta = snap["trade"], snap.get("meta") or {}
    if not is_chr(trade.get("strategy_id") or trade.get("strategy_name")):
        return None
    candles, geometry = snap.get("candles") or [], snap.get("geometry") or []
    if len(candles) < 2 * PIVOT_K + 3:
        return None
    horizontal = (trade.get("strategy_id") or trade.get("strategy_name")) == "Horizontal_Breakout_CHR"
    long = trade["side"] == "long"
    times = [int(c["open_time"]) for c in candles]
    mark = _find(geometry, "marker", "пробой")
    t_break = int(mark["t1"]) if mark else times[-1]
    j = times.index(t_break) if t_break in times else len(candles) - 1
    if j < 1:
        return None
    atr0 = candles[j - 1].get("atr") or meta.get("atr") or 0.0
    tol = TOUCH_ATR * atr0
    last_pivot_index = j - 1 - PIVOT_K
    items: list[dict[str, Any]] = []
    notes: list[str] = []
    iv = times[1] - times[0]

    # ---- линия: уровень или наклонная
    level_kind = "high" if long else "low"  # экстремумы, по которым строится уровень / линия (лонг пробивает сопротивление)
    sq_kind = "low" if long else "high"
    if horizontal:
        g = _find(geometry, "line", "уровень")
        if g is None:
            return None
        level = float(g["p1"])
        t1, t2 = int(g["t1"]), int(g["t2"])
        window = int(meta.get("window") or HORIZONTAL_WINDOW)
        items.append({"kind": "line", "t1": t1, "p1": level, "t2": t2 + 3 * iv, "p2": level, "color": AMBER, "width": 2, "dash": [], "label": f"Уровень {level:g}"})
        items.append({"kind": "zone", "t1": t1, "t2": t2, "p1": level, "p2": level - tol if long else level + tol, "color": AMBER, "fill": "rgba(255,179,0,0.12)", "label": ""})
        value_at = lambda t: level  # noqa: E731
        first_ok = j - window
        touch_idx = [i for i in pivots(candles, level_kind) if first_ok <= i <= last_pivot_index
                     and ((candles[i]["high"] >= level - tol) if long else (candles[i]["low"] <= level + tol))]
        what = "горизонтальный уровень"
    else:
        g = _find(geometry, "line", "наклонная")
        if g is None:
            return None
        t1, p1, t2, p2 = int(g["t1"]), float(g["p1"]), int(g["t2"]), float(g["p2"])
        span = max(1, t2 - t1)
        value_at = lambda t: p1 + (p2 - p1) * (t - t1) / span  # noqa: E731
        window = int(meta.get("window") or TRENDLINE_WINDOW)
        items.append({"kind": "line", "t1": t1, "p1": p1, "t2": t2, "p2": p2, "color": AMBER, "width": 2, "dash": [], "label": "Наклонная линия"})
        items.append({"kind": "line", "t1": t2, "p1": p2, "t2": t2 + 3 * iv, "p2": value_at(t2 + 3 * iv), "color": AMBER, "width": 1, "dash": [6, 4], "label": ""})
        first_ok = j - window
        chain = [i for i in pivots(candles, level_kind) if i <= last_pivot_index][-TOUCH_DEPTH:]
        touch_idx = [i for i in chain if i >= first_ok and abs(candles[i][level_kind] - value_at(times[i])) <= tol]
        what = "наклонная линия"
        second = _find(geometry, "marker", "2-я опора")
        if second:
            items.append({"kind": "point", "t": int(second["t1"]), "p": float(second["p1"]), "color": BLUE, "label": "2-я опора", "r": 6})

    for n, i in enumerate(touch_idx, start=1):
        items.append({"kind": "point", "t": times[i], "p": candles[i][level_kind], "color": AMBER, "label": f"Касание {n}", "r": 5})

    # ---- поджатие: три последних пивота противоположного вида (higher lows у лонга / lower highs у шорта)
    sq_idx = [i for i in pivots(candles, sq_kind) if i <= last_pivot_index][-SQUEEZE_POINTS:]
    sq_prices = [candles[i][sq_kind] for i in sq_idx]
    for n, i in enumerate(sq_idx, start=1):
        items.append({"kind": "point", "t": times[i], "p": candles[i][sq_kind], "color": VIOLET, "label": f"Поджатие {n}", "r": 5})
    for a, b in zip(sq_idx, sq_idx[1:]):
        items.append({"kind": "line", "t1": times[a], "p1": candles[a][sq_kind], "t2": times[b], "p2": candles[b][sq_kind], "color": VIOLET, "width": 2, "dash": [], "label": ""})
    squeeze_geo = _find(geometry, "line", "поджатие")
    if squeeze_geo and len(sq_idx) >= 2:  # продолжение линии поджатия до пробойной свечи: куда шла структура
        items.append({"kind": "line", "t1": times[sq_idx[-1]], "p1": candles[sq_idx[-1]][sq_kind], "t2": t_break, "p2": candles[sq_idx[-1]][sq_kind], "color": VIOLET,
                      "width": 1, "dash": [3, 4], "label": "Стоп за последней точкой"})

    items.append({"kind": "point", "t": t_break, "p": candles[j]["close"], "color": BLUE, "label": "Пробой", "r": 7})

    # ---- сверка с тем, что сохранил бот
    want_touches = meta.get("touches")
    want_squeeze = [float(x) for x in meta.get("squeeze_points") or []]
    touches_ok = want_touches is None or len(touch_idx) == int(want_touches)
    squeeze_ok = not want_squeeze or (len(sq_prices) == len(want_squeeze) and all(_near(a, b) for a, b in zip(sq_prices, want_squeeze)))
    verified = touches_ok and squeeze_ok
    bias = meta.get("htf_label") or meta.get("htf_bias")
    side_word = "лонг (пробой вверх)" if long else "шорт (пробой вниз)"
    notes.append(f"Бот увидел: {what}; {side_word}.")
    notes.append(f"Касаний: {len(touch_idx)}" + (f" (бот записал {want_touches})" if want_touches is not None else "") + f"; допуск касания {tol:.6g} (= {TOUCH_ATR} ATR).")
    kind_word = "повышающиеся минимумы (higher lows)" if long else "понижающиеся максимумы (lower highs)"
    notes.append(f"Поджатие: {len(sq_idx)} {kind_word}: " + ", ".join(f"{p:.6g}" for p in sq_prices) + ".")
    if bias:
        notes.append(f"Тренд H4: {bias} (жёсткий фильтр CHR).")
    if meta.get("body_frac") is not None:
        notes.append(f"Пробойная свеча полнотелая: тело {float(meta['body_frac']) * 100:.0f}% размаха.")
    notes.append("Точки восстановлены клиентом по свечам снапшота; " + ("совпадают с данными бота." if verified else
                 "НЕ совпали с данными бота (число касаний или цены поджатия): калибровка бота могла измениться, смотрите линии из снапшота."))
    return {"items": items, "notes": notes, "verified": verified}
