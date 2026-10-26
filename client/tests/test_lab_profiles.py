"""Create Strategy 1.9.1: у SANTA и CHR своё видение рынка - уроки, словарь и стратегии не пересекаются ни на ПК, ни при обмене с сервером.
Плюс проверки расчётов: сравнение разметки и подбор параметров считают то, что должны."""

import asyncio
import io
import json
import random

import flet as ft
import pytest

from tradeai_client.lab import detect, lab_window, match, remote, store, tune
from tradeai_client.lab import sync as lab_sync
from tradeai_client.lab.market import IV

from test_lab import _Host, _walk, walk


@pytest.fixture
def base(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "lab"))
    return tmp_path / "lab"


def rows(n=300, seed=3):
    return [[b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b["bv"]] for b in walk(n, seed)]


def level_lesson(author: str, symbol: str = "BTCUSDT") -> dict:
    r = rows()
    objs, _ = detect.run([store.to_bar(x) for x in r], IV["15m"], ["level"])
    lv = next(o for o in objs if o["type"] == "level")
    return {"symbol": symbol, "tf": "15m", "bars": r, "author": author,
            "drawings": [{"id": "u", "tool": "hline", "term": "level", "p1": {"t": lv["t1"], "p": lv["p1"]}}]}


# ------------------------------------------------------------------------------------------ раздельные пространства на ПК


def test_spaces_are_separate_folders(base):
    s, c = store.lab_dir("SANTA"), store.lab_dir("CHR")
    assert s == base / "santa" and c == base / "chr" and s != c
    assert store.lab_dir("кто-то") == s  # неизвестный профиль - как у клиента, SANTA по умолчанию


def test_lessons_vocab_strategies_do_not_cross(base):
    s, c = store.lab_dir("SANTA"), store.lab_dir("CHR")
    store.save_lesson(level_lesson("SANTA"), s)
    store.add_term("Имбаланс", "box", root=s)
    st = store.list_strategies(s)[0]
    store.save_strategy({**st, "params": {"level": {"min_touches": 4}}}, "подбор SANTA", root=s)

    assert store.list_lessons(c) == [] and store.load_lessons(c) == []
    assert not any(t.id.startswith("u_") for t in store.load_vocab(c))
    assert store.get_strategy(store.active_name(c), c)["params"] == {}
    store.add_term("Имбаланс", "box", root=c)  # то же имя у CHR - своё понятие, не «уже есть»
    with pytest.raises(store.StoreError):
        store.load_lesson(store.list_lessons(s)[0]["id"], c)


def test_tuning_one_profile_leaves_the_other_untouched(base):
    s, c = store.lab_dir("SANTA"), store.lab_dir("CHR")
    store.save_lesson(level_lesson("CHR"), c)
    santa_before = store.get_strategy(store.active_name(s), s)
    res = tune.tune(store.load_lessons(c), {}, ["level"])
    st = store.get_strategy(store.active_name(c), c)
    store.save_strategy({**st, "params": res["params"]}, "подбор CHR", root=c)
    assert store.get_strategy(store.active_name(s), s) == santa_before
    assert store.load_lessons(s) == []


def test_legacy_190_folder_is_split_by_author(base):
    (base / "lessons").mkdir(parents=True)
    (base / "strategies").mkdir()
    for lid, author in (("20261026-000001-aaaaaa", "SANTA"), ("20261026-000002-bbbbbb", "CHR"), ("20261026-000003-cccccc", "")):
        (base / "lessons" / f"{lid}.json").write_text(json.dumps({"id": lid, "author": author}), encoding="utf-8")
    (base / "strategies" / "Моя стратегия.json").write_text("{}", encoding="utf-8")
    (base / "vocab.json").write_text("[]", encoding="utf-8")
    store.lab_dir("CHR")
    assert sorted(p.name for p in (base / "santa" / "lessons").iterdir()) == ["20261026-000001-aaaaaa.json", "20261026-000003-cccccc.json"]
    assert [p.name for p in (base / "chr" / "lessons").iterdir()] == ["20261026-000002-bbbbbb.json"]
    assert (base / "santa" / "strategies" / "Моя стратегия.json").exists() and (base / "santa" / "vocab.json").exists()
    assert not (base / "lessons").exists() and not (base / "vocab.json").exists()


# ------------------------------------------------------------------------------------------ окно полигона и вкладка


def test_lab_window_of_chr_saves_and_pushes_only_into_chr(base, monkeypatch):
    sent = []
    monkeypatch.setattr(lab_window.remote, "push_lesson", lambda url, key, space, ls: sent.append((space, ls["author"])))
    monkeypatch.setattr(lab_window.remote, "push_strategy", lambda url, key, space, st: sent.append((space, st["name"])))
    api = lab_window.LabApi({"profile": "CHR", "author": "CHR", "server": {"url": "http://x", "key": "k"}})
    assert api.boot()["space"] == "chr"
    assert api.save_lesson(level_lesson("CHR"))["ok"]
    st = api.get_strategy()["strategy"]
    assert api.save_strategy({**st, "params": {"level": {"k": 2}}}, "тест")["ok"]
    assert sent == [("chr", "CHR"), ("chr", st["name"])]
    assert len(store.list_lessons(store.lab_dir("CHR"))) == 1 and store.list_lessons(store.lab_dir("SANTA")) == []
    santa = lab_window.LabApi({"profile": "SANTA", "server": {}})
    assert santa.list_lessons()["items"] == [] and santa.get_strategy()["strategy"]["params"] == {}


def test_strategy_tab_follows_the_active_profile(base, monkeypatch):
    monkeypatch.setattr(ft.BaseControl, "update", lambda self: None)
    from tradeai_client.ui.strategy_view import StrategyLabView

    store.save_lesson(level_lesson("CHR", "ETHUSDT"), store.lab_dir("CHR"))
    host = _Host()
    v = StrategyLabView(host)
    opened = []
    monkeypatch.setattr(lab_window, "open_lab", lambda cfg: opened.append(cfg))
    text = lambda: " ".join(x.value for x in _walk(v.control) if isinstance(x, ft.Text) and isinstance(x.value, str))  # noqa: E731
    asyncio.run(v.refresh())
    assert "ETH" not in text() and "Уроков пока нет" in text() and "профиля SANTA" in text()
    host.settings.profile = "CHR"
    asyncio.run(v.refresh())
    assert "ETH 15m" in text() and "профиля CHR" in text()
    v._open({"symbol": "ETHUSDT"})
    assert opened[0]["profile"] == "CHR" and opened[0]["author"] == "CHR"


# ------------------------------------------------------------------------------------------ сервер: пути и синхронизация


def test_remote_puts_profile_space_into_every_path(monkeypatch):
    seen = []

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout):
        seen.append((req.get_method(), req.full_url))
        return Resp(b'{"items": []}')

    monkeypatch.setattr(remote.urllib.request, "urlopen", fake_urlopen)
    remote.list_lessons("http://s", "k", "chr")
    remote.get_vocab("http://s", "k", "santa")
    remote.push_strategy("http://s", "k", "chr", {"name": "Моя"})
    assert seen[0] == ("GET", "http://s/api/lab/chr/lessons") and seen[1] == ("GET", "http://s/api/lab/santa/vocab")
    assert seen[2][1].startswith("http://s/api/lab/chr/strategies/")
    with pytest.raises(remote.RemoteError):
        remote.list_lessons("http://s", "k", "")
    with pytest.raises(remote.RemoteError):
        remote.list_lessons("http://s", "k", "../santa")


class FakeServer:
    """Сервер как в api/lab.py: каталог на каждое пространство."""

    def __init__(self):
        self.data = {"santa": {"lessons": {}, "vocab": [], "snaps": {}}, "chr": {"lessons": {}, "vocab": [], "snaps": {}}}

    def install(self, monkeypatch):
        sp = lambda space: self.data[remote._sp(space)]  # noqa: E731
        monkeypatch.setattr(remote, "list_lessons", lambda b, k, space: [{"id": i} for i in sp(space)["lessons"]])
        monkeypatch.setattr(remote, "get_lesson", lambda b, k, space, lid: sp(space)["lessons"][lid])
        monkeypatch.setattr(remote, "push_lesson", lambda b, k, space, ls: sp(space)["lessons"].__setitem__(ls["id"], ls))
        monkeypatch.setattr(remote, "get_vocab", lambda b, k, space: list(sp(space)["vocab"]))
        monkeypatch.setattr(remote, "push_vocab", lambda b, k, space, items: sp(space).__setitem__("vocab", list(items)))
        monkeypatch.setattr(remote, "list_snapshots", lambda b, k, space: [{"id": i} for i in sp(space)["snaps"]])
        monkeypatch.setattr(remote, "get_snapshot", lambda b, k, space, i: sp(space)["snaps"][i])
        monkeypatch.setattr(remote, "push_snapshot", lambda b, k, space, m: sp(space)["snaps"].__setitem__(m["id"], m))


def test_sync_exchanges_only_inside_own_space(tmp_path, monkeypatch):
    srv = FakeServer()
    srv.install(monkeypatch)
    # ПК SANTA
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc_santa"))
    store.save_lesson(level_lesson("SANTA"), store.lab_dir("SANTA"))
    store.add_term("Имбаланс", "box", root=store.lab_dir("SANTA"))
    assert lab_sync.sync("u", "k", "SANTA") == {"space": "santa", "up": 1, "down": 0, "terms": 1, "snap_up": 0, "snap_down": 0}
    # ПК CHR: свой урок уходит в chr, урок SANTA не приходит
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc_chr"))
    store.save_lesson(level_lesson("CHR", "ETHUSDT"), store.lab_dir("CHR"))
    assert lab_sync.sync("u", "k", "CHR") == {"space": "chr", "up": 1, "down": 0, "terms": 0, "snap_up": 0, "snap_down": 0}
    assert len(srv.data["santa"]["lessons"]) == 1 and len(srv.data["chr"]["lessons"]) == 1
    assert [x["author"] for x in srv.data["chr"]["lessons"].values()] == ["CHR"] and srv.data["chr"]["vocab"] == []
    assert [x["symbol"] for x in store.list_lessons(store.lab_dir("CHR"))] == ["ETHUSDT"]
    # второй ПК SANTA получает только уроки SANTA и его понятия
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc_santa2"))
    assert lab_sync.sync("u", "k", "SANTA") == {"space": "santa", "up": 0, "down": 1, "terms": 1, "snap_up": 0, "snap_down": 0}
    assert [x["author"] for x in store.list_lessons(store.lab_dir("SANTA"))] == ["SANTA"]
    assert store.list_lessons(store.lab_dir("CHR")) == []


# ------------------------------------------------------------------------------------------ расчёты


def _bars(n=300):
    return [store.to_bar(x) for x in rows(n)]


def test_compare_bot_markup_against_itself_is_perfect():
    bars = _bars()
    objs, _ = detect.run(bars, IV["15m"], ["level", "zone", "trendline", "range"])
    tool = {"hline": "hline", "box": "rect", "line": "trend", "point": "point"}
    drawings = [{"id": str(i), "tool": tool[o["kind"]], "term": o["type"], "p1": {"t": o["t1"], "p": o["p1"]}, "p2": {"t": o["t2"], "p": o["p2"]}}
                for i, o in enumerate(objs) if o["kind"] in tool and o["type"] in ("level", "zone", "trendline", "range")]
    lesson = {"bars": bars, "iv": IV["15m"], "drawings": drawings}
    r = match.compare(lesson, objs)
    n = len(drawings)
    assert n >= 3
    # трейдер нарисовал ровно то же, что бот: всё совпало, пропусков и лишнего нет, F1 = 100%
    assert (r["overall"]["tp"], r["overall"]["fn"], r["overall"]["fp"]) == (n, 0, 0)
    assert r["overall"]["f1"] == pytest.approx(1.0)


def test_compare_counts_are_consistent_on_random_markup():
    bars = _bars()
    objs, _ = detect.run(bars, IV["15m"])
    rng = random.Random(7)
    for _ in range(25):
        drawings = []
        for i in range(rng.randint(1, 8)):
            t1, t2 = sorted(rng.sample([b["t"] for b in bars], 2))
            p1, p2 = rng.uniform(95, 110), rng.uniform(95, 110)
            drawings.append({"id": str(i), "tool": rng.choice(["hline", "rect", "trend"]), "term": rng.choice(["level", "zone", "trendline"]),
                             "p1": {"t": t1, "p": p1}, "p2": {"t": t2, "p": p2}})
        r = match.compare({"bars": bars, "iv": IV["15m"], "drawings": drawings}, objs)
        for term in {d["term"] for d in drawings}:
            s = r["by_type"][term]
            assert s["tp"] + s["fn"] == sum(1 for d in drawings if d["term"] == term)  # каждый рисунок - ровно совпал или пропущен
        matched_bot = [m["bot_index"] for m in r["marks"] if m["status"] == "matched"]
        assert len(matched_bot) == len(set(matched_bot))  # один объект бота не засчитан дважды
        for s in [*r["by_type"].values(), r["overall"]]:
            if s["f1"] is not None:
                assert 0.0 <= s["f1"] <= 1.0


def test_tune_never_makes_agreement_worse_and_is_deterministic():
    bars = _bars()
    objs, _ = detect.run(bars, IV["15m"], ["level"])
    lv = [o for o in objs if o["type"] == "level"][:2]
    lesson = {"bars": bars, "iv": IV["15m"], "drawings": [{"id": str(i), "tool": "hline", "term": "level", "p1": {"t": o["t1"], "p": o["p1"]}} for i, o in enumerate(lv)]
              + [{"id": "z", "tool": "hline", "term": "level", "p1": {"t": bars[50]["t"], "p": 104.4}}]}
    a = tune.tune([lesson], {}, ["level", "zone"])
    b = tune.tune([lesson], {}, ["level", "zone"])
    assert a == b
    rep = a["report"]["level"]
    assert rep["after"]["f1"] >= rep["before"]["f1"] and rep["lessons"] == 1
    assert "zone" not in a["report"]  # понятие без разметки в уроках не подбирается
    if rep["changed"]:  # применённые параметры дают ровно ту оценку, что в отчёте
        tuned, _ = detect.run(bars, IV["15m"], ["level"], a["params"])
        s = match.compare({**lesson, "complete_types": ["level"]}, tuned)["by_type"]["level"]
        assert (s["tp"], s["fn"], s["fp"]) == (rep["after"]["tp"], rep["after"]["fn"], rep["after"]["fp"])



# ------------------------------------------------------------------------------------------ 1.9.2: слепки с комментарием, 1D, уровни вне экрана


PNG = "data:image/png;base64," + __import__("base64").b64encode(b"\x89PNG\r\n\x1a\n" + b"0" * 64).decode()


def snap(comment="Здесь вход, потому что уровень отработал 3 раза", **kw):
    r = rows(60)
    return {"symbol": "wldusdt", "tf": "1d", "comment": comment, "image": PNG, "bars": r, "t": r[-1][0], "drawings": [{"id": "a", "tool": "hline", "term": "level"}],
            "bot_objects": [{"type": "level", "kind": "hline", "p1": 1.0}], **kw}


def test_snapshot_keeps_picture_comment_and_market_data_per_profile(base):
    s, c = store.lab_dir("SANTA"), store.lab_dir("CHR")
    meta = store.save_snapshot(snap(author="SANTA"), s)
    assert meta["symbol"] == "WLDUSDT" and meta["tf"] == "1d" and len(meta["bars"]) == 60 and meta["drawings"][0]["term"] == "level"
    assert (s / "snapshots" / f"{meta['id']}.png").read_bytes().startswith(b"\x89PNG")
    full = store.load_snapshot(meta["id"], s, with_image=True)
    assert full["image"] == PNG and full["comment"].startswith("Здесь вход")
    assert [x["id"] for x in store.list_snapshots(s)] == [meta["id"]] and store.list_snapshots(c) == []
    with pytest.raises(store.StoreError):
        store.load_snapshot(meta["id"], c)
    store.delete_snapshot(meta["id"], s)
    assert store.list_snapshots(s) == [] and not list((s / "snapshots").iterdir())


@pytest.mark.parametrize("bad", [{"comment": "  "}, {"image": "data:image/png;base64,@@@"}, {"image": "data:image/png;base64," + "AAAA"}, {"image": ""}, {"tf": "2h"}, {"symbol": "../x"}])
def test_snapshot_rejects_empty_comment_and_broken_input(base, bad):
    with pytest.raises(store.StoreError):
        store.save_snapshot({**snap(), **bad}, store.lab_dir("SANTA"))
    assert store.list_snapshots(store.lab_dir("SANTA")) == []


def test_snapshot_bridge_and_sync_stay_inside_the_profile(tmp_path, monkeypatch):
    srv = FakeServer()
    srv.install(monkeypatch)
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc1"))
    api = lab_window.LabApi({"profile": "CHR", "author": "CHR", "server": {"url": "u", "key": "k"}})
    saved = api.save_snapshot(snap())
    assert saved["ok"] and saved["sent"] is True and list(srv.data["chr"]["snaps"]) == [saved["id"]] and srv.data["santa"]["snaps"] == {}
    assert api.save_snapshot(snap(comment=""))["ok"] is False
    got = api.get_snapshot(saved["id"])["snapshot"]
    assert got["image"] == PNG and got["author"] == "CHR" and api.list_snapshots()["items"][0]["comment"].startswith("Здесь вход")
    # второй ПК CHR получает слепок, ПК SANTA - нет
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc2"))
    assert lab_sync.sync("u", "k", "CHR")["snap_down"] == 1 and len(store.list_snapshots(store.lab_dir("CHR"))) == 1
    monkeypatch.setenv("TRADEAI_LAB_DIR", str(tmp_path / "pc3"))
    assert lab_sync.sync("u", "k", "SANTA")["snap_down"] == 0 and store.list_snapshots(store.lab_dir("SANTA")) == []
    assert api.delete_snapshot(saved["id"])["ok"] is True and api.list_snapshots()["items"] == []


def test_daily_timeframe_works_end_to_end(base):
    from tradeai_client.lab import market

    assert market.IV["1d"] == 86400
    bars = [store.to_bar([1_700_000_000 + i * 86400, 100 + i * 0.1, 101 + i * 0.1, 99 + i * 0.1, 100.5 + i * 0.1, 10, 5]) for i in range(120)]
    objs, ctx = detect.run(bars, market.IV["1d"])
    assert objs and set(detect.DETECTORS) == set(ctx.out)
    lesson = store.save_lesson({"symbol": "BTCUSDT", "tf": "1d", "bars": [[b["t"], b["o"], b["h"], b["l"], b["c"], b["v"], b["bv"]] for b in bars], "drawings": []}, store.lab_dir("SANTA"))
    assert lesson["tf"] == "1d" and lesson["iv"] == 86400


def test_levels_and_liquidity_are_found_over_the_whole_analysis_window():
    """1.9.2: уровни и ликвидность живут по всему окну (1500 свечей), а не только 300/200 последних - иначе за краем экрана они пропадали."""
    assert detect.DEFAULT_PARAMS["level"]["lookback"] >= 1500 and detect.DEFAULT_PARAMS["liquidity"]["lookback"] >= 1500
    assert lab_window.ANALYZE_BARS >= 1500
    bars = _bars(1200)
    objs, _ = detect.run(bars, IV["15m"], ["level", "liquidity"])
    old = [o for o in objs if o["type"] in ("level", "liquidity") and o["kind"] == "hline" and o["t1"] < bars[-400]["t"]]
    assert old, "уровни/ликвидность старше 400 свечей должны оставаться"


def test_page_has_done_cancel_snapshot_and_1d_controls():
    html = lab_window.build_html({"author": "SANTA"})
    assert 'id="m-cancel"' in html and 'id="m-skip"' not in html and "Без подписи</button>" not in html
    assert 'id="b-snap"' in html and 'id="s-ok"' in html and "'1d'" in html and "takeScreenshot" in html
    assert "▲ " in html and "flushHLabels" in html
