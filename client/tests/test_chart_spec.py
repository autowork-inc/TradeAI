import copy

import pytest

from tradeai_client import fmt
from tradeai_client.chart_spec import PANEL_WIDTH_CHARS, ChartDataError, build_chart_spec, interval_to_ms

IV = 900_000


def post_klines(snap, n=40):
    """Свечи после сигнала: сетка продолжается от последней свечи снапшота."""
    last = snap["candles"][-1]
    out, price = [], last["close"]
    for i in range(1, n + 1):
        out.append({"t": last["open_time"] + i * IV, "o": price, "h": price * 1.001, "l": price * 0.999, "c": price, "v": 10.0})
    return out


def spec_for(snap, post=None, now_offset_bars=300):
    now = fmt.to_ms(snap["trade"]["signal_time"]) + now_offset_bars * IV
    return build_chart_spec(snap, post, now_ms=now)


def test_interval_parsing():
    assert interval_to_ms("15m") == IV and interval_to_ms("1h") == 3_600_000 and interval_to_ms("1d") == 86_400_000
    for bad in ("", "15", "m15", "0m", "-1m", "15x", None):
        with pytest.raises(ChartDataError):
            interval_to_ms(bad)


def test_closed_smc_short_has_all_drawings(fx):
    snap = fx("snapshot_smc_short.json")
    sp = spec_for(snap)
    t = snap["trade"]
    assert len(sp["bars"]) == 100 and not any(b["post"] for b in sp["bars"]) and sp["interval_ms"] == IV
    # горизонтальные линии: SL, TP, вход; +1R из плана Scale Out; +3R совпадает с TP и не дублируется
    texts = [h["text"] for h in sp["hlines"]]
    assert any(x.startswith("SL") for x in texts) and any(x.startswith("TP") for x in texts) and any(x.startswith("Вход") for x in texts)
    assert any(x.startswith("+1R") for x in texts) and not any(x.startswith("+3R") for x in texts)
    sl = next(h for h in sp["hlines"] if h["text"].startswith("SL"))
    assert sl["price"] == t["sl"] and sl["color"] == "#ff3d3d" and sl["style"] == "solid" and sl["title"] == "SL"
    assert next(h for h in sp["hlines"] if h["text"].startswith("TP"))["price"] == t["tp"]
    # зона FVG закрашивается прямоугольником: верхняя цена выше нижней, время зоны внутри сетки
    assert len(sp["boxes"]) == 1 and sp["boxes"][0]["label"] == "FVG" and sp["boxes"][0]["p1"] > sp["boxes"][0]["p2"]
    assert sp["boxes"][0]["t1"] >= sp["bars"][0]["t"] and sp["boxes"][0]["t2"] <= sp["helper"][-1][0]
    # линии плана из геометрии (SL/TP/вход) не дублируются; пул ликвидности - отрезок
    assert [s["label"] for s in sp["segments"]] == ["пул ликвидности"]
    assert {m["text"] for m in sp["setup_markers"]} == {"свип", "CHoCH"}


def test_entry_and_close_markers_follow_the_trade_and_sit_on_the_grid(fx):
    snap = fx("snapshot_smc_short.json")
    sp = spec_for(snap)
    ms = sorted(sp["trade_markers"], key=lambda m: m["t"])
    assert [m["text"].split()[0] for m in ms] == ["Вход", "+1R", "+3R", "Трейлинг"]
    assert ms[0]["shape"] == "arrow_down" and ms[0]["position"] == "above"  # шорт: стрелка вниз над ценой
    assert ms[-1]["shape"] == "square" and all(m["t"] % IV == 0 for m in ms)
    assert ms[1]["color"] == "#26a69a"  # прибыльное закрытие - зелёное
    grid = [t for t, _ in sp["helper"]]
    assert grid == sorted(grid) and all(b - a == IV for a, b in zip(grid, grid[1:]))  # непрерывная сетка
    assert all(m["t"] in grid for m in ms) and grid[-1] >= ms[-1]["t"] + 3 * IV  # ось времени продлена за последний маркер
    by_t = {t: v for t, v in sp["helper"]}
    assert by_t[ms[0]["t"]] == snap["trade"]["entry_price"]  # значение невидимого ряда = цена маркера


def test_long_trade_mirrors_marker_directions(fx):
    snap = fx("snapshot_breakout_long.json")
    sp = spec_for(snap)
    entry = next(m for m in sp["trade_markers"] if m["text"].startswith("Вход"))
    assert entry["shape"] == "arrow_up" and entry["position"] == "below"
    assert {s["label"] for s in sp["segments"]} == {"уровень равных хаёв"} and not sp["boxes"]
    levels = {h["text"].split()[0] for h in sp["hlines"] if h["text"].startswith("+")}
    assert levels == {"+1R"} and any(h["text"].startswith("TP") for h in sp["hlines"])  # +3R совпадает с TP сделки (цена последнего уровня плана)
    no_tp = copy.deepcopy(snap)
    no_tp["trade"]["tp"] = None
    assert {h["text"].split()[0] for h in spec_for(no_tp)["hlines"] if h["text"].startswith("+")} == {"+1R", "+3R"}  # TP нет - оба уровня плана рисуются
    one = next(h for h in sp["hlines"] if h["text"].startswith("+1R"))
    assert one["price"] > snap["trade"]["entry_price"] > snap["trade"]["sl"]


def test_post_signal_candles_are_appended_after_the_snapshot_only(fx):
    snap = fx("snapshot_smc_short.json")
    post = post_klines(snap, 40)
    stale = {"t": snap["candles"][-1]["open_time"], "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}  # дубль последней свечи снапшота
    sp = spec_for(snap, [*reversed(post), stale])
    assert sum(b["post"] for b in sp["bars"]) >= 1 and len(sp["bars"]) > 100
    ts = [b["t"] for b in sp["bars"]]
    assert ts == sorted(set(ts)) and sp["bars"][99]["c"] == snap["candles"][-1]["close"] and not sp["bars"][99]["post"]
    assert not any("не загружены" in x for x in sp["result_lines"])


def test_without_post_candles_a_note_explains_why(fx):
    snap = fx("snapshot_smc_short.json")
    assert any("не загружены" in x for x in spec_for(snap)["result_lines"])
    sp = build_chart_spec(snap, None, now_ms=fmt.to_ms(snap["trade"]["signal_time"]) + 10 * IV, post_note="Binance недоступен с этого ПК")
    assert "Binance недоступен с этого ПК" in sp["result_lines"] 


def test_pending_limit_has_plan_lines_but_no_entry_or_close_markers(fx):
    sp = spec_for(fx("snapshot_pending.json"))
    assert sp["trade_markers"] == []
    texts = [h["text"] for h in sp["hlines"]]
    assert any(x.startswith("SL") for x in texts) and any(x.startswith("План входа") for x in texts) and not any(x.startswith("Вход ") for x in texts)
    assert sp["result_lines"][0].startswith("Статус: Ждёт лимитку")


def test_open_trade_extends_the_grid_to_now_and_marks_partial_close(fx):
    snap = fx("snapshot_open.json")
    now = fmt.to_ms(snap["trade"]["signal_time"]) + 60 * IV
    sp = build_chart_spec(snap, None, now_ms=now)
    assert sp["helper"][-1][0] >= now - now % IV
    assert [m["text"].split()[0] for m in sorted(sp["trade_markers"], key=lambda m: m["t"])] == ["Вход", "+1R"]
    assert sp["trade_markers"][-1]["shape"] == "circle"  # сделка ещё открыта: частичное закрытие - не итоговое (не квадрат)


def test_panels_contain_reason_and_results(fx):
    sp = spec_for(fx("snapshot_smc_short.json"))
    text = "\n".join(sp["reason_lines"])
    assert "SMC Swing · шорт · BTCUSDT" in text and "Свип ликвидности" in text and "Таймфрейм: 15m" in text
    assert all(len(line) <= PANEL_WIDTH_CHARS + 3 for line in sp["reason_lines"])  # перенос строк: одна строка таблицы - одна строка на экране
    res = "\n".join(sp["result_lines"])
    assert "Статус: Закрыта" in res and "Причина выхода: Трейлинг" in res and "1. +1R (50%)" in res and "3. Трейлинг" in res
    assert sp["title"].startswith("BTCUSDT 15m · Шорт · Закрыта")


def test_visible_range_focuses_on_the_signal(fx):
    snap = fx("snapshot_smc_short.json")
    sp = spec_for(snap, post_klines(snap, 20))
    sig = fmt.to_ms(snap["trade"]["signal_time"])
    lo, hi = sp["visible"]
    assert lo <= sig - sig % IV - 40 * IV and hi > sp["helper"][-1][0]


@pytest.mark.parametrize("mutate,msg", [
    (lambda s: s.update(candles=[]), "нет свечей"),
    (lambda s: s["candles"].pop(50), "Разрыв"),
    (lambda s: s["candles"][3].update(close=None), "неполная"),
    (lambda s: s["meta"].update(interval="15x"), "таймфрейм"),
    (lambda s: s["geometry"].append({"type": "arc", "t1": 0, "t2": 0, "p1": 0, "p2": 0, "label": "x"}), "геометрии"),
])
def test_broken_snapshot_is_rejected_loudly(fx, mutate, msg):
    snap = copy.deepcopy(fx("snapshot_smc_short.json"))
    mutate(snap)
    with pytest.raises(ChartDataError, match=msg):
        spec_for(snap)


def test_panel_is_structured_and_colored_by_result(fx):
    snap = fx("snapshot_smc_short.json")
    sp = spec_for(snap)
    panel = sp["panel"]
    assert panel["result"]["tone"] == "good" and panel["result"]["pnl"] == "+214.00 USDT" and len(panel["result"]["closes"]) == 3
    assert panel["reason"]["steps"] and ["Таймфрейм", "15m"] in panel["reason"]["facts"]
    assert panel["review"] is not None and 1 <= panel["review"]["score"] <= 10
    assert sp["digits"] == 2  # BTC: цены без десятитысячных
    assert all(h["width"] >= 2 for h in sp["hlines"])
    assert next(h for h in sp["hlines"] if h["text"].startswith("SL"))["width"] == 3
    loss = copy.deepcopy(snap)
    loss["trade"].update(pnl=-100.0, r_multiple=-1.0)
    assert spec_for(loss)["panel"]["result"]["tone"] == "bad"


def test_review_absent_for_open_trade(fx):
    assert spec_for(fx("snapshot_open.json"))["panel"]["review"] is None
