"""Автоселектор монет (orchestrator/universe.py): уровни по объёму, Close-Only, подключение на лету, Fail-Fast (решение пользователя 2026-10-18)."""

import asyncio
import json

import pytest

import config
from exchange.binance_live import BinanceLiveClient
from exchange.router import ExecutionRouter
from orchestrator.kline_feed import MultiFeed
from orchestrator.microstructure import MicroAggregator
from orchestrator.registry import OBI_BREAKOUT, SHADOW_STRATEGIES, strategy_allowed
from orchestrator.universe import (GROUP_BATCH, Hooks, Tiers, Universe, UniverseManager, classify, parse_listings, parse_volumes)
from utils.errors import ConfigError, DataError, HttpServerError, NetworkError, RateLimitError
from utils.symbolset import SymbolSet

M = 1_000_000.0
NOW = 2_000_000_000_000
OLD = NOW - 200 * 86_400_000
PINNED = ("BTCUSDT", "ETHUSDT")


def listing(symbol, *, status="TRADING", kind="PERPETUAL", quote="USDT", onboard=OLD):
    return {"symbol": symbol, "status": status, "contractType": kind, "quoteAsset": quote, "onboardDate": onboard}


def vols(**kw):
    return {"BTCUSDT": 4000 * M, "ETHUSDT": 3000 * M, **{k: v * M for k, v in kw.items()}}


# ---------------------------------------------------------------------------------------------- классификация

def test_classify_three_tiers_pinned_first_sorted_by_volume():
    v = vols(AAAUSDT=900, BBBUSDT=400, CCCUSDT=399.9, DDDUSDT=100, EEEUSDT=99.9, FFFUSDT=1)
    t = classify(v, set(v), pinned=PINNED)
    assert t.live == ("BTCUSDT", "ETHUSDT", "AAAUSDT", "BBBUSDT")  # >= 400M включительно
    assert t.test == ("CCCUSDT", "DDDUSDT")  # 100M..400M, граница 100M включительно
    assert "EEEUSDT" not in t.all() and "FFFUSDT" not in t.all()  # < 100M игнорируются


def test_pinned_stay_live_even_with_tiny_volume():
    t = classify({"BTCUSDT": 1 * M, "ETHUSDT": 2 * M}, {"BTCUSDT", "ETHUSDT"}, pinned=PINNED)
    assert t.live == PINNED and t.test == ()


def test_classify_uses_only_eligible_and_requires_pinned_in_data():
    v = vols(AAAUSDT=900, ZZZUSDT=900)
    assert "ZZZUSDT" not in classify(v, {"AAAUSDT"}, pinned=PINNED).all()
    with pytest.raises(DataError, match="закреплённых"):
        classify({"AAAUSDT": 900 * M}, {"AAAUSDT"}, pinned=PINNED)


def test_classify_cap_keeps_most_liquid_live_before_test():
    v = vols(**{f"L{i}USDT": 1000 - i for i in range(5)}, **{f"T{i}USDT": 300 - i for i in range(5)})
    t = classify(v, set(v), pinned=PINNED, max_symbols=2 + 7)
    assert len(t.all()) == 9 and len(t.live) == 7 and len(t.test) == 2
    assert t.live[2:] == tuple(f"L{i}USDT" for i in range(5)) and t.test == ("T0USDT", "T1USDT")


def test_classify_rejects_bad_thresholds():
    with pytest.raises(ConfigError):
        classify(vols(), set(), pinned=PINNED, live_min=100 * M, test_min=400 * M)


def test_parse_listings_filters_contract_status_quote_age_and_symbol_shape():
    info = {"serverTime": NOW, "symbols": [
        listing("OKUSDT"), listing("OLDUSDT", onboard=NOW - 70 * 86_400_000), listing("NEWUSDT", onboard=NOW - 69 * 86_400_000),
        listing("BTCUSDT_261225", kind="CURRENT_QUARTER"), listing("HALTUSDT", status="BREAK"), listing("USDCUSD", quote="USDC"),
        listing("币安人生USDT"), listing("lowerUSDT")]}
    assert parse_listings(info, min_listing_days=70) == {"OKUSDT", "OLDUSDT"}


@pytest.mark.parametrize("bad", [None, {}, {"serverTime": 1, "symbols": [{"symbol": "X"}]}, {"serverTime": "x", "symbols": []}])
def test_parse_listings_garbage_is_dataerror(bad):
    with pytest.raises(DataError):
        parse_listings(bad, min_listing_days=70)


def test_parse_volumes_and_garbage():
    assert parse_volumes([{"symbol": "A", "quoteVolume": "12.5"}]) == {"A": 12.5}
    for bad in (None, {}, [{"symbol": "A"}], [{"symbol": "A", "quoteVolume": "nan"}], [{"symbol": "A", "quoteVolume": "-1"}], [{"symbol": "A", "quoteVolume": "x"}]):
        with pytest.raises(DataError):
            parse_volumes(bad)


# ---------------------------------------------------------------------------------------------- состав и Close-Only

def make_universe(tmp_path=None):
    live, test = SymbolSet(PINNED), SymbolSet()
    u = Universe(live=live, test=test, pinned=PINNED, mt5=("EURUSD",), state_path=None if tmp_path is None else tmp_path / "u.json")
    u.book.update(PINNED)  # как в shadow_run: стакан закреплённых монет подключён при старте
    return u, live, test


def apply(u, live=(), test=(), extra_subscribed=()):
    t = Tiers(PINNED + tuple(live), tuple(test), {})
    u.subscribed.update(t.all(), extra_subscribed)
    return u.apply(t)


def test_apply_promote_demote_drop_and_in_place_sets():
    u, live, test = make_universe()
    d = apply(u, live=("AAAUSDT", "BBBUSDT"), test=("CCCUSDT",))
    assert d.promoted == ("AAAUSDT", "BBBUSDT") and d.added_test == ("CCCUSDT",) and not d.demoted and not d.dropped
    assert list(live) == ["BTCUSDT", "ETHUSDT", "AAAUSDT", "BBBUSDT"] and list(test) == ["CCCUSDT"]  # те же объекты обновлены на месте
    d = apply(u, live=("AAAUSDT",), test=("BBBUSDT",))  # BBB упал в Test (Close-Only), CCC < 100M
    assert d.demoted == ("BBBUSDT",) and d.dropped == ("CCCUSDT",) and not d.promoted and not d.added_test
    assert "BBBUSDT" not in live and "BBBUSDT" in test
    d = apply(u, live=("AAAUSDT", "CCCUSDT"))  # CCC вернулась сразу в live, BBB < 100M
    assert d.promoted == ("CCCUSDT",) and d.dropped == ("BBBUSDT",) and not d.demoted  # BBB уже была в Test, а не в live


def test_tracked_only_live_test_and_mt5_muted_symbols_get_no_new_signals():
    u, _, _ = make_universe()
    apply(u, live=("AAAUSDT",), test=("CCCUSDT",))
    assert all(u.tracked(s) for s in ("BTCUSDT", "AAAUSDT", "CCCUSDT", "EURUSD"))
    apply(u, live=(), test=())  # AAA и CCC опустились ниже 100M
    assert not u.tracked("AAAUSDT") and not u.tracked("CCCUSDT") and u.tracked("BTCUSDT")
    assert "AAAUSDT" in u.subscribed  # потоки живут: открытые сделки получают тики до закрытия


def test_apply_refuses_symbols_without_streams_and_overlap():
    u, live, test = make_universe()
    with pytest.raises(ConfigError, match="без потоков"):
        u.apply(Tiers(PINNED + ("ZZZUSDT",), (), {}))
    u.subscribed.add("ZZZUSDT")
    live.replace([*PINNED, "ZZZUSDT"])
    test.replace(["ZZZUSDT"])
    with pytest.raises(ConfigError, match="только Shadow"):
        u._check_disjoint()
    live.replace(["ETHUSDT"])
    test.replace(["BTCUSDT"])
    with pytest.raises(ConfigError, match="Закреплённые"):
        u._check_disjoint()


def test_snapshot_roundtrip_and_corrupt_file(tmp_path):
    u, _, _ = make_universe(tmp_path)
    apply(u, live=("AAAUSDT",), test=("CCCUSDT",))
    u.save()
    data = json.loads((tmp_path / "u.json").read_text(encoding="utf-8"))
    assert data["live"] == ["BTCUSDT", "ETHUSDT", "AAAUSDT"] and data["test"] == ["CCCUSDT"] and data["mt5"] == ["EURUSD"]
    assert make_universe(tmp_path)[0].load_saved() == (("BTCUSDT", "ETHUSDT", "AAAUSDT"), ("CCCUSDT",))
    (tmp_path / "u.json").write_text("{oops", encoding="utf-8")
    with pytest.raises(DataError, match="повреждён"):
        make_universe(tmp_path)[0].load_saved()
    assert make_universe(tmp_path / "missing")[0].load_saved() == ((), ())


# ---------------------------------------------------------------------------------------------- live-маршрут и OBI

def test_demoted_symbol_is_rejected_by_router_but_promoted_is_accepted_without_rebuild():
    u, live, _ = make_universe()
    router = ExecutionRouter(binance=BinanceLiveClient(object()), binance_symbols=live)  # тот же объект, как config.LIVE_BINANCE_SYMBOLS
    assert router.reject_unsupported("AAAUSDT").reason == "unsupported_symbol"
    apply(u, live=("AAAUSDT",))
    assert router.venue_of("AAAUSDT") is not None and router.reject_unsupported("AAAUSDT") is None
    apply(u, test=("AAAUSDT",))  # объём упал: Close-Only
    assert router.reject_unsupported("AAAUSDT").reason == "unsupported_symbol"
    assert router.venue_of("BTCUSDT") is not None  # закреплённые не затронуты


def test_default_config_sets_are_the_ones_router_and_obi_use():
    assert isinstance(config.LIVE_BINANCE_SYMBOLS, SymbolSet) and config.STRATEGY_SYMBOLS[OBI_BREAKOUT] is config.LIVE_BINANCE_SYMBOLS
    router = ExecutionRouter(binance=BinanceLiveClient(object()))
    assert router._binance_symbols is config.LIVE_BINANCE_SYMBOLS


def test_obi_runs_only_on_live_tier_other_strategies_on_all_tracked(monkeypatch):
    live, test = SymbolSet(PINNED), SymbolSet()
    monkeypatch.setattr(config, "LIVE_BINANCE_SYMBOLS", live)
    monkeypatch.setattr(config, "BINANCE_TEST_SYMBOLS", test)
    monkeypatch.setattr(config, "STRATEGY_SYMBOLS", {OBI_BREAKOUT: live})
    u = Universe(live=live, test=test, pinned=PINNED, mt5=("EURUSD",))
    apply(u, live=("AAAUSDT",), test=("CCCUSDT",))
    assert strategy_allowed(OBI_BREAKOUT, "AAAUSDT") and strategy_allowed(OBI_BREAKOUT, "BTCUSDT")
    assert not strategy_allowed(OBI_BREAKOUT, "CCCUSDT") and not strategy_allowed(OBI_BREAKOUT, "EURUSD")
    assert all(strategy_allowed(n, "CCCUSDT") for n in SHADOW_STRATEGIES if n != OBI_BREAKOUT)
    apply(u, test=("AAAUSDT",))  # live -> Test: стакан уже не нужен
    assert not strategy_allowed(OBI_BREAKOUT, "AAAUSDT")


# ---------------------------------------------------------------------------------------------- менеджер

class FakeApi:
    def __init__(self, listings, volumes):
        self.listings, self.volumes, self.calls, self.fail = listings, volumes, [], []

    async def get(self, path, params=None, weight=1):
        self.calls.append(path)
        if self.fail:
            raise self.fail.pop(0)
        if path.endswith("exchangeInfo"):
            return {"serverTime": NOW, "symbols": [listing(s) for s in self.listings]}
        return [{"symbol": s, "quoteVolume": str(v)} for s, v in self.volumes.items()]


class Recorder:
    def __init__(self):
        self.groups, self.prepared, self.books, self.open = [], [], [], []

    def hooks(self, *, with_live=True, with_book=True):
        async def add_group(batch):
            self.groups.append(batch)

        async def prepare(syms):
            self.prepared.append(tuple(syms))

        return Hooks(add_group=add_group, prepare_live=prepare if with_live else None, add_book=(lambda b: self.books.append(b)) if with_book else None,
                     open_symbols=lambda: self.open)


def manager(api, rec, tmp_path=None, **kw):
    u, live, test = make_universe(tmp_path)
    return UniverseManager(api, u, rec.hooks(), retry_s=0.0, **kw), u, live, test


def test_refresh_connects_new_groups_prepares_live_filters_and_book_before_applying():
    api = FakeApi(["BTCUSDT", "ETHUSDT", "AAAUSDT", "CCCUSDT", "TINYUSDT"], vols(AAAUSDT=900, CCCUSDT=150, TINYUSDT=5))
    rec = Recorder()
    mgr, u, live, test = manager(api, rec)
    diff = asyncio.run(mgr.refresh())
    assert rec.groups == [("AAAUSDT", "CCCUSDT")] and rec.prepared == [("AAAUSDT",)] and rec.books == [("AAAUSDT",)]
    assert list(live) == [*PINNED, "AAAUSDT"] and list(test) == ["CCCUSDT"] and diff.promoted == ("AAAUSDT",) and "TINYUSDT" not in u.subscribed
    assert api.calls == ["/fapi/v1/exchangeInfo", "/fapi/v1/ticker/24hr"]


def test_second_refresh_only_changes_membership_without_new_streams():
    api = FakeApi(["AAAUSDT", "CCCUSDT"], vols(AAAUSDT=900, CCCUSDT=150))
    rec = Recorder()
    mgr, u, live, test = manager(api, rec)
    asyncio.run(mgr.refresh())
    api.volumes = vols(AAAUSDT=300, CCCUSDT=450)  # AAA падает в Test (Close-Only), CCC растёт в live
    diff = asyncio.run(mgr.refresh())
    assert rec.groups == [("AAAUSDT", "CCCUSDT")]  # потоки уже есть, новых соединений нет
    assert diff.demoted == ("AAAUSDT",) and diff.promoted == ("CCCUSDT",)
    assert list(live) == [*PINNED, "CCCUSDT"] and list(test) == ["AAAUSDT"]
    assert rec.prepared == [("AAAUSDT",), ("CCCUSDT",)] and rec.books == [("AAAUSDT",), ("CCCUSDT",)]
    api.volumes = vols(AAAUSDT=50, CCCUSDT=450)  # AAA < 100M: не отслеживается, но подписана
    diff = asyncio.run(mgr.refresh())
    assert diff.dropped == ("AAAUSDT",) and not u.tracked("AAAUSDT") and "AAAUSDT" in u.subscribed


def test_group_batches_and_open_position_symbol_keeps_streams_even_outside_tiers():
    names = [f"S{i:02d}USDT" for i in range(GROUP_BATCH + 5)]
    api = FakeApi(names + ["OLDPOSUSDT"], vols(**{n: 500 for n in names}))
    rec = Recorder()
    rec.open = ["OLDPOSUSDT", "EURUSD"]  # позиция на монете вне уровней; MT5 потоков здесь не строит
    mgr, u, *_ = manager(api, rec, max_failures=3)
    mgr._hooks.open_symbols = lambda: rec.open
    asyncio.run(mgr.refresh())
    assert [len(g) for g in rec.groups] == [GROUP_BATCH, 5 + 1]  # 45 уровней + монета с позицией, пачками
    assert "OLDPOSUSDT" in u.subscribed and "OLDPOSUSDT" in rec.groups[-1] and "EURUSD" not in u.subscribed and not u.tracked("OLDPOSUSDT")


def test_transient_exchange_failure_is_retried_then_dies_after_limit():
    api = FakeApi(["AAAUSDT"], vols(AAAUSDT=900))
    mgr, u, live, _ = manager(api, Recorder(), max_failures=3)
    asyncio.run(mgr.refresh())
    assert "AAAUSDT" in live

    async def recover():
        api.fail = [NetworkError("x"), HttpServerError(503, "u", "b")]  # два временных сбоя подряд (< лимита 3), потом опрос удаётся
        api.volumes = vols(AAAUSDT=10)
        mgr._refresh_s = 3600.0
        before = len(api.calls)
        task = asyncio.create_task(mgr.run())
        for _ in range(100):
            await asyncio.sleep(0.01)
            if mgr.refreshes == 2:
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert mgr.refreshes == 2 and len(api.calls) - before == 2 + 2  # две неудачные попытки exchangeInfo + удачная пара запросов

    asyncio.run(recover())
    assert "AAAUSDT" not in live  # состав сменился только после удачного опроса

    async def die():
        api.fail = [NetworkError("1"), NetworkError("2"), NetworkError("3")]
        with pytest.raises(NetworkError):
            await mgr.run()

    asyncio.run(die())


def test_ban_and_garbage_are_not_retried():
    for exc in (RateLimitError(418, 60, "u"), DataError("мусор")):
        api = FakeApi(["AAAUSDT"], vols(AAAUSDT=900))
        api.fail = [exc]
        mgr, *_ = manager(api, Recorder())
        with pytest.raises(type(exc)):
            asyncio.run(mgr.run())


def test_manager_check_raises_task_failure():
    api = FakeApi([], {})
    api.fail = [DataError("boom")]
    mgr, *_ = manager(api, Recorder())

    async def body():
        mgr.start()
        await asyncio.sleep(0.05)
        with pytest.raises(DataError, match="boom"):
            mgr.check()

    asyncio.run(body())


# ---------------------------------------------------------------------------------------------- вспомогательное

def test_multifeed_add_starts_new_feed_only_when_already_running_and_polls_snapshot():
    class F:
        def __init__(self, name):
            self.name, self.started = name, False

        def start(self):
            self.started = True

        async def aclose(self):
            pass

        def check(self):
            pass

        async def poll(self):
            return [self.name]

    a, b = F("a"), F("b")
    m = MultiFeed([a])
    m.add(b)
    assert not b.started  # набор ещё не запущен: запустит start()
    m.start()
    assert a.started and b.started
    c = F("c")
    m.add(c)
    assert c.started and asyncio.run(m.poll()) == ["a", "b", "c"]


def test_micro_aggregator_marks_window_open_before_symbol_added_as_incomplete():
    agg = MicroAggregator(("BTCUSDT",), 900_000, grace_s=10.0)
    agg.on_trade("BTCUSDT", 0, 1.0, False)
    agg.on_depth("BTCUSDT", 0, [["1", "2"]], [["1", "1"]])
    assert agg.summary("BTCUSDT", 0).valid
    agg.add_symbols(("AAAUSDT",), 500_000)  # добавлена посреди окна [0, 900000)
    agg.on_trade("AAAUSDT", 600_000, 1.0, False)
    agg.on_depth("AAAUSDT", 600_000, [["1", "2"]], [["1", "1"]])
    assert not agg.summary("AAAUSDT", 0).valid  # неполное окно: ms_obi / ms_delta = null
    agg.on_trade("AAAUSDT", 900_000, 1.0, False)
    agg.on_depth("AAAUSDT", 900_000, [["1", "2"]], [["1", "1"]])
    assert agg.summary("AAAUSDT", 900_000).valid  # следующее окно целиком после добавления
    with pytest.raises(DataError):
        agg.on_trade("ZZZUSDT", 0, 1.0, False)


def test_symbolset_behaves_like_tuple_and_updates_in_place():
    s = SymbolSet(("A", "B", "A"))
    assert list(s) == ["A", "B"] and len(s) == 2 and "A" in s and "C" not in s and s == ("A", "B") and s == ["A", "B"] and s != ("B", "A")
    it = iter(s)
    s.replace(("C",))
    assert list(s) == ["C"] and s == SymbolSet(("C",))
    with pytest.raises(TypeError):
        hash(s)


# ---------------------------------------------------------------------------------------------- подключение группы

class FakeStateFeed:
    def __init__(self, warm_after):
        self.warm_after, self.polls, self.started = warm_after, 0, False

    def warmed(self):
        return self.polls >= self.warm_after


class Bag:
    def __init__(self, order, name):
        self.items, self._order, self._name = [], order, name

    def add(self, feed):
        self.items.append(feed)
        self._order.append(self._name)


def test_group_connector_adds_state_first_waits_for_warmup_then_work_and_ticks():
    from orchestrator.universe import GroupConnector
    order = []
    state_feed, work_feed = FakeStateFeed(warm_after=3), object()
    work, state, ticks = Bag(order, "work"), Bag(order, "state"), Bag(order, "ticks")
    clock = {"t": 0.0}

    async def sleep(dt):
        clock["t"] += dt
        state_feed.polls += 1  # каждую секунду ShadowRun опрашивает фид состояния

    conn = GroupConnector(make_feeds=lambda b: ([work_feed], [state_feed]), work=work, state=state, ticks=ticks, make_tick_feed=lambda b: ("ticks", b),
                          warmup_timeout_s=10, settle_s=2.0, clock=lambda: clock["t"], sleep=sleep)
    asyncio.run(conn(("AAAUSDT",)))
    assert order == ["state", "work", "ticks"] and work.items == [work_feed] and ticks.items == [("ticks", ("AAAUSDT",))]
    assert clock["t"] == 3 + 2.0  # 3 опроса до готовности состояния + пауза на последний цикл


def test_group_connector_fails_fast_if_higher_timeframe_state_never_warms_up():
    from orchestrator.universe import GroupConnector
    clock = {"t": 0.0}

    async def sleep(dt):
        clock["t"] += dt

    never = FakeStateFeed(warm_after=10**9)
    work, state, ticks = Bag([], "w"), Bag([], "s"), Bag([], "t")
    conn = GroupConnector(make_feeds=lambda b: ([object()], [never]), work=work, state=state, ticks=ticks, make_tick_feed=lambda b: b, warmup_timeout_s=5,
                          clock=lambda: clock["t"], sleep=sleep)
    with pytest.raises(DataError, match="не загрузилась"):
        asyncio.run(conn(("AAAUSDT",)))
    assert work.items == [] and ticks.items == []  # рабочие свечи и тики не подключены: монета без bias торговать не начнёт


def test_group_connector_without_state_set_and_state_feeds_is_a_config_error():
    from orchestrator.universe import GroupConnector
    conn = GroupConnector(make_feeds=lambda b: ([], [FakeStateFeed(0)]), work=Bag([], "w"), state=None, ticks=Bag([], "t"), make_tick_feed=lambda b: b)
    with pytest.raises(ConfigError):
        asyncio.run(conn(("AAAUSDT",)))


def test_kline_feed_warmed_requires_history_and_for_state_feeds_the_consumed_latest_bar():
    from orchestrator.feature_engine import FeatureEngine
    from orchestrator.kline_feed import SmcKlineFeed
    from tests.orchestrator.test_kline_multi_interval import Api
    sym = "BTCUSDT"

    async def body():
        api = Api("1h", 130)
        feed = SmcKlineFeed(api, None, FeatureEngine(min_bars=100), symbols=(sym,), interval="1h", history_bars=120, clock_ms=lambda: 1, emit_latest_on_start=True)  # type: ignore[arg-type]
        assert not feed.warmed()  # история ещё не загружена
        await feed._bootstrap()
        assert not feed.warmed()  # загружена, но последняя закрытая свеча ещё не отдана потребителю (MtfState её не видел)
        assert len(await feed.poll()) == 1
        assert feed.warmed()
        plain = SmcKlineFeed(api, None, FeatureEngine(min_bars=100), symbols=(sym,), interval="1h", history_bars=120, clock_ms=lambda: 1)  # type: ignore[arg-type]
        await plain._bootstrap()
        assert plain.warmed()  # рабочему фиду достаточно истории

    asyncio.run(body())


def test_shadow_run_tick_raises_when_universe_manager_task_failed():
    from scripts.shadow_run import ShadowRun

    class Quiet:
        async def poll(self):
            return []

        def check(self):
            pass

    api = FakeApi([], {})
    api.fail = [DataError("боевой ответ мусор")]
    mgr, *_ = manager(api, Recorder())

    async def body():
        run = ShadowRun(Quiet(), None, None, None, universe=mgr)
        mgr.start()
        await asyncio.sleep(0.05)
        with pytest.raises(DataError, match="мусор"):
            await run.tick()

    asyncio.run(body())
