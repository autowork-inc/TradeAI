"""Create Strategy (пакет tradeai_client.lab): детекторы понятий, сетапы без будущего, сравнение с разметкой трейдера, подбор под уроки,
хранилище (уроки, стратегии с откатом, словарь), мост окна полигона и вкладка приложения. Сети нет: свечи синтетические."""

import asyncio
import json
import random

import flet as ft
import pytest

from tradeai_client.lab import detect, lab_window, market, match, setups, store, tune, vocab

IV = 900
T0 = 1_760_000_400 - 1_760_000_400 % IV


def bar(i, o, h, l, c, v=100.0, bv=50.0):  # noqa: E741
    return {"t": T0 + i * IV, "o": o, "h": h, "l": l, "c": c, "v": v, "bv": bv}


def walk(n=400, seed=3, start=100.0):
    rng = random.Random(seed)
    out, p = [], start
    for i in range(n):
        o = p
        p = max(1.0, p + rng.gauss(0, 0.6))
        out.append(bar(i, o, max(o, p) + abs(rng.gauss(0, 0.3)), min(o, p) - abs(rng.gauss(0, 0.3)), p, 100 + rng.random() * 50, 50 + rng.random() * 25))
    return out


def range_then_breakout():
    """Тренд вверх, потом боковик 100-103 (30 свечей) и пробой вверх объёмной полнотелой свечой."""
    bars, i = [], 0
    for k in range(60):
        o = 80 + k * 0.33
        bars.append(bar(i, o, o + 0.6, o - 0.4, o + 0.3)); i += 1
    for k in range(30):  # узкий коридор ~101-102.2 мелкими свечами
        o = 101.6 + (0.4 if k % 4 < 2 else -0.4)
        c = 101.6 - (0.3 if k % 4 < 2 else -0.3)
        bars.append(bar(i, o, max(o, c) + 0.15, min(o, c) - 0.15, c)); i += 1
    bars.append(bar(i, 102.0, 107.5, 101.8, 107.2, v=900)); i += 1
    for k in range(6):
        o = 107.2 + k * 0.3
        bars.append(bar(i, o, o + 0.5, o - 0.3, o + 0.3)); i += 1
    return bars


@pytest.fixture
def lab_root(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "lab"))
    return tmp_path / "lab"


# ------------------------------------------------------------------------------------------ детекторы


def test_every_detector_runs_and_objects_are_well_formed():
    objs, ctx = detect.run(walk(), IV)
    assert set(detect.DETECTORS) == set(ctx.out)
    types = {o["type"] for o in objs}
    assert {"high", "low", "trend", "level", "zone"} <= types
    for o in objs:
        assert o["kind"] in ("hline", "box", "line", "point", "label") and o["t1"] <= o["t2"] or o["kind"] == "line"
        assert set(vocab.BUILTIN_IDS) >= {o["type"]}
        json.dumps(o)


def test_range_box_and_breakout_up_are_found():
    objs, _ = detect.run(range_then_breakout(), IV)
    boxes = [o for o in objs if o["type"] == "range"]
    assert any(101.9 <= b["p2"] <= 102.5 for b in boxes)  # верх коридора; низ может захватить хвост тренда перед боковиком
    br = [o for o in objs if o["type"] == "breakout" and o["side"] == "long"]
    assert br and abs(br[-1]["p1"] - 107.2) < 1e-9


def test_pivots_need_k_closed_bars_on_the_right():
    bars = walk(80)
    hi, lo = detect.pivots(bars, 3)
    assert all(i <= len(bars) - 1 - 3 for i in hi + lo)  # последние 3 свечи ещё не могут быть подтверждённым экстремумом


def test_gap_and_fill():
    bars = [bar(i, 100, 100.5, 99.5, 100) for i in range(30)]
    bars += [bar(30, 100, 100.4, 99.8, 100.2), bar(31, 100.3, 104, 100.3, 103.8), bar(32, 103.8, 104.5, 102.0, 104.2)]  # бычий FVG 100.4..102.0
    bars += [bar(33 + k, 104, 104.4, 103.6, 104) for k in range(5)] + [bar(38, 104, 104.1, 100.0, 100.5)]  # заполнен
    objs, _ = detect.run(bars, IV, ["gap"])
    g = [o for o in objs if o["type"] == "gap" and o["side"] == "long" and abs(o["p1"] - 100.4) < 1e-9]
    assert g and g[0]["label"] == "гэп заполнен" and g[0]["t2"] == T0 + 38 * IV


def test_setups_only_look_at_given_bars_and_return_text():
    bars = range_then_breakout()
    _, ctx = detect.run(bars[:91 + 1], IV)  # до пробойной свечи включительно
    ev = setups.evaluate(ctx)
    assert all(e["t"] == bars[91]["t"] for e in ev)
    sig = [e for e in ev if e["kind"] == "signal"]
    for s in sig:
        assert s["side"] in ("long", "short") and s["text"].startswith("СИГНАЛ") and (s["stop"] < s["entry"] < s["target"] if s["side"] == "long" else s["target"] < s["entry"] < s["stop"])


# ------------------------------------------------------------------------------------------ сравнение и подбор


def lesson_with(drawings, bars=None):
    bars = bars or walk()
    return {"id": "L1", "symbol": "TESTUSDT", "tf": "15m", "iv": IV, "bars": bars, "drawings": drawings,
            "view": {"from_t": bars[0]["t"], "to_t": bars[-1]["t"]}}


def test_compare_matched_missed_and_extra_only_for_marked_terms():
    bars = walk()
    objs, _ = detect.run(bars, IV, ["level", "zone"])
    lv = next(o for o in objs if o["type"] == "level")
    drawings = [{"id": "a", "tool": "hline", "term": "level", "p1": {"t": lv["t1"], "p": lv["p1"]}},
                {"id": "b", "tool": "hline", "term": "level", "p1": {"t": lv["t1"], "p": lv["p1"] * 3}}]
    rep = match.compare(lesson_with(drawings, bars), objs)
    st = {m.get("user_id"): m["status"] for m in rep["marks"] if m.get("user_id")}
    assert st == {"a": "matched", "b": "missed"}
    assert "zone" not in rep["by_type"]  # зоны трейдер не размечал - лишними они не считаются
    assert rep["by_type"]["level"]["tp"] == 1 and rep["by_type"]["level"]["fn"] == 1


def test_user_line_matches_bot_line_with_atr_tolerance():
    u = {"kind": "line", "t1": 0, "t2": 100 * IV, "p1": 100, "p2": 90}
    b = {"kind": "line", "t1": 0, "t2": 120 * IV, "p1": 100.3, "p2": 88.3}
    assert match.same(u, b, atr=1.0, iv=IV)
    assert not match.same(u, {**b, "p1": 103}, atr=1.0, iv=IV)


def test_tune_removes_extras_without_losing_matches():
    bars = walk(500, seed=11)
    objs, _ = detect.run(bars, IV, ["level"])
    keep = sorted((o for o in objs if o["type"] == "level"), key=lambda o: -o["score"])[:1]  # трейдер видит только самый сильный уровень
    drawings = [{"id": f"u{i}", "tool": "hline", "term": "level", "p1": {"t": o["t1"], "p": o["p1"]}} for i, o in enumerate(keep)]
    res = tune.tune([lesson_with(drawings, bars)], {}, ["level"])
    r = res["report"]["level"]
    assert r["after"]["f1"] >= r["before"]["f1"] and r["after"]["tp"] >= r["before"]["tp"]
    assert r["after"]["fp"] <= r["before"]["fp"]


# ------------------------------------------------------------------------------------------ хранилище


def test_lesson_roundtrip_and_validation(lab_root):
    rows = [[b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b["bv"]] for b in walk(60)]
    saved = store.save_lesson({"symbol": "wldusdt", "tf": "15m", "bars": rows, "drawings": [
        {"id": "x", "tool": "rect", "term": "zone", "note": "отскакивала", "p1": {"t": rows[5][0], "p": 99}, "p2": {"t": rows[20][0], "p": 101}},
        {"id": "bad", "tool": "spiral", "p1": {"t": 1, "p": 1}}], "notes": "разбор"})
    assert saved["symbol"] == "WLDUSDT" and len(saved["drawings"]) == 1 and saved["bars"][0]["bv"] == rows[0][6]
    assert store.list_lessons()[0]["terms"] == {"zone": 1}
    assert store.load_lesson(saved["id"])["notes"] == "разбор"
    with pytest.raises(store.StoreError):
        store.save_lesson({"symbol": "WLDUSDT", "tf": "15m", "bars": rows[:10]})
    store.delete_lesson(saved["id"])
    assert store.list_lessons() == []


def test_strategy_history_and_rollback(lab_root):
    st = store.list_strategies()[0]
    store.save_strategy({**st, "params": {"level": {"min_touches": 4}}}, "подбор")
    store.save_strategy({**store.get_strategy(st["name"]), "enabled": ["zone"]}, "только зоны")
    cur = store.get_strategy(st["name"])
    assert cur["enabled"] == ["zone"] and len(cur["history"]) == 2
    back = store.rollback_strategy(st["name"])
    assert back["enabled"] != ["zone"] and back["params"] == {"level": {"min_touches": 4}}
    with pytest.raises(store.StoreError):
        store.rollback_strategy(st["name"], steps=5)


def test_custom_term_added_once_and_marked_without_detector(lab_root):
    t = store.add_term("Имбаланс", "box", "#123456")
    assert t.id.startswith("u_") and not t.detector
    assert any(x.id == t.id for x in store.load_vocab())
    with pytest.raises(vocab.VocabError):
        store.add_term("Имбаланс", "box")


def test_random_spot_is_inside_history_with_room_for_replay():
    coins = [{"symbol": "AUSDT", "onboard_s": 0}, {"symbol": "BUSDT", "onboard_s": 0}]
    now = 1_790_000_000
    for seed in range(20):
        s = market.random_spot(coins, rng=random.Random(seed), now_s=now)
        iv = market.IV[s["tf"]]
        assert s["at_s"] % iv == 0 and now - 365 * 86400 <= s["at_s"] <= now - 300 * iv


# ------------------------------------------------------------------------------------------ окно и вкладка


def test_page_is_assembled_without_placeholders():
    html = lab_window.build_html({"author": "SANTA"})
    assert "/*__" not in html and "LightweightCharts" in html and '"author":"SANTA"' in html


def test_bridge_analyze_compare_and_save(lab_root, monkeypatch):
    sent = []
    monkeypatch.setattr(lab_window.remote, "push_lesson", lambda url, key, ls: sent.append(ls["id"]))
    api = lab_window.LabApi({"author": "SANTA", "server": {"url": "http://x", "key": "k"}})
    rows = [[b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b["bv"]] for b in walk(300)]
    r = api.analyze(rows, "15m", "", True)
    assert r["ok"] and r["objects"] and r["trend"] in (-1, 0, 1)
    lv = next(o for o in r["objects"] if o["type"] == "level")
    lesson = {"symbol": "TESTUSDT", "tf": "15m", "bars": rows, "drawings": [{"id": "u", "tool": "hline", "term": "level", "p1": {"t": lv["t1"], "p": lv["p1"]}}]}
    c = api.compare(lesson)
    assert c["ok"] and c["report"]["by_type"]["level"]["tp"] == 1
    s = api.save_lesson(lesson)
    assert s["ok"] and s["sent"] is True and sent == [s["id"]]
    assert api.boot()["ok"] and api.list_lessons()["items"][0]["author"] == "SANTA"


class _Host:
    def __init__(self):
        from tradeai_client.settings import ClientSettings
        self.settings = ClientSettings(base_url="http://x", api_key="k", profile="SANTA")
        self.notes = []
        self.page = None

    def run(self, coro):
        asyncio.run(coro)

    def notify(self, text, error=False):
        self.notes.append(text)


def test_strategy_tab_lists_lessons_and_tunes(lab_root, monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)
    from tradeai_client.ui.strategy_view import StrategyLabView

    rows = [[b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b["bv"]] for b in walk(300)]
    objs, _ = detect.run([store.to_bar(r) for r in rows], IV, ["level"])
    lv = next(o for o in objs if o["type"] == "level")
    store.save_lesson({"symbol": "BTCUSDT", "tf": "15m", "bars": rows, "author": "CHR", "drawings": [{"id": "u", "tool": "hline", "term": "level", "p1": {"t": lv["t1"], "p": lv["p1"]}}]})
    host = _Host()
    v = StrategyLabView(host)
    asyncio.run(v.refresh())
    txt = " ".join(x.value for x in _walk(v.control) if isinstance(x, ft.Text) and isinstance(x.value, str))
    assert "BTC 15m" in txt and "автор CHR" in txt and "Уровень 1" in txt
    asyncio.run(v._tune())
    txt = " ".join(x.value for x in _walk(v._tune_box) if isinstance(x, ft.Text) and isinstance(x.value, str))
    assert "Уровень" in txt and "%" in txt
    asyncio.run(v._apply_tune())
    assert store.get_strategy(store.active_name())["history"]


def _walk(c):
    yield c
    for name in ("controls", "content"):
        child = getattr(c, name, None)
        for x in (child if isinstance(child, list) else [child]):
            if isinstance(x, ft.Control):
                yield from _walk(x)
