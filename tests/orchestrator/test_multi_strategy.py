"""Мульти-стратегийный Shadow (2026-10-11): позиции изолированы по составному ключу (символ, strategy_id), стратегии не перезаписывают
состояния друг друга, strategy_id попадает во все записи БД, реестр собирает пять стратегий с индивидуальными планами."""

import asyncio
import dataclasses
import json

import aiohttp
import polars as pl
import pytest
from aiohttp import web

import config
from features.smc import SmcParams, add_features
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.microstructure import DepthFeed, MicroStructure
from orchestrator.multi_strategies import (
    HORIZONTAL_BREAKOUT, ICT_KILLZONES, OBI_BREAKOUT, SMC_COUNTER_TREND, TRENDLINE_BREAKOUT, VWAP_REVERSION, StrategyCounterTrend,
    StrategyHorizontalBreakout, StrategyKillzones, StrategyObiBreakout, StrategyTrendlineBreakout, StrategyVwapReversion,
)
from orchestrator.registry import LEVEL_BREAKOUT_PLAN, SCALP_1R_PLAN, SHADOW_STRATEGIES, VWAP_PLAN, build_strategies, plans_for
from orchestrator.strategies import SMC_PRO_TREND, StrategySMC
from orchestrator.tracker import CLOSED, DEFAULT_PLAN, OPEN, PENDING_LIMIT, TrackerError, VirtualTracker
from orchestrator.types import BarInfo, Direction, FeatureContext, Kind, Mode, Tick, position_key
from persistence.recorder import MemoryRecorder, RecordingEventLog
from strategies.counter_trend import CounterTrendParams
from strategies.horizontal_breakout import HorizontalParams
from strategies.obi_breakout import ObiParams
from strategies.smc_setups import SmcSetupParams
from strategies.trendline_breakout import TrendlineParams
from strategies.vwap_reversion import VwapParams
from tests.conftest import serve
from tests.features.test_smc import random_walk
from tests.orchestrator.helpers import IV, SYM, Scripted, ctx, mk, tick
from tests.orchestrator.test_feeds import settle
from utils.errors import ConfigError, DataError

A, B, C, D, E, F, G = SHADOW_STRATEGIES


def make(**kw) -> tuple[VirtualTracker, MemoryRecorder]:
    rec = MemoryRecorder()
    return VirtualTracker(RecordingEventLog(MemoryEventLog(), rec), equity=10_000.0, recorder=rec, plans=plans_for(SHADOW_STRATEGIES), **kw), rec


# =================================================================================== составной ключ позиции


def test_position_key_is_symbol_plus_strategy_id():
    s = mk(SMC_PRO_TREND, symbol="SOLUSDT")
    assert s.strategy_id == SMC_PRO_TREND and s.position_key == position_key("SOLUSDT", SMC_PRO_TREND) == "SOLUSDT_SMC_Pro_Trend"


def test_opposite_positions_of_two_strategies_on_one_coin_do_not_collide():
    """Шорт SMC_Pro_Trend и лонг VWAP_Reversion по ОДНОЙ монете: у каждой свой стоп, свои закрытия, свои строки в БД."""
    tr, rec = make()
    short = mk(A, direction=Direction.SHORT, bar=1)  # вход 100, SL 105, R = 5
    long = mk(VWAP_REVERSION, direction=Direction.LONG, bar=1)  # вход 100, SL 95, R = 5
    assert tr.submit(short, 1000).accepted and tr.submit(long, 1000).accepted
    ts, tl = tr.trades
    assert [t.trade_id for t in tr.active(SYM)] == [ts.trade_id, tl.trade_id]
    assert tr.active(SYM, strategy=VWAP_REVERSION) == [tl] and tr.active(SYM, strategy="нет такой") == []

    tr.on_tick(tick(100.5, 10))  # цена вверх: шорт-лимитка исполнена, лонг-лимитка (покупка по 100) - нет
    assert (ts.status, tl.status) == (OPEN, PENDING_LIMIT)
    tr.on_tick(tick(105.5, 20))  # стоп шорта; лонг не затронут
    assert ts.status == CLOSED and ts.exit_reason == "sl" and tl.status == PENDING_LIMIT and tl.remaining == 1.0
    assert tr.active(SYM) == [tl]  # ключ шорта освободился, ключ лонга живёт

    tr.on_tick(tick(99.5, 30))  # цена вниз: исполнилась лонг-лимитка
    assert tl.status == OPEN and tl.fill_price == 100.0 and ts.status == CLOSED and ts.exit_reason == "sl"  # закрытая сделка не «оживает»
    tr.on_tick(tick(106.0, 40))  # +1R лонга: у VWAP_REVERSION план 50% на +1R, остаток дальше
    assert tl.status == OPEN and tl.remaining == pytest.approx(0.5) and len(tl.legs) == 1 and len(ts.legs) == 1  # у шорта своё единственное закрытие

    rows = {r["id"]: r for r in rec.trades.values()}
    assert {r["strategy_id"] for r in rows.values()} == {A, VWAP_REVERSION} and all(r["strategy_name"] == r["strategy_id"] for r in rows.values())
    assert {c["trade_id"] for c in rec.closes} == {ts.trade_id, tl.trade_id}
    assert sorted(c["reason"] for c in rec.closes if c["trade_id"] == ts.trade_id) == ["sl"]
    assert sorted(c["reason"] for c in rec.closes if c["trade_id"] == tl.trade_id) == ["scale_1r"]


def test_second_position_under_the_same_key_is_a_protocol_error_but_a_free_key_is_reusable():
    tr, _ = make()
    assert tr.submit(mk(A, bar=1), 1000).accepted
    with pytest.raises(TrackerError, match="SOLUSDT_SMC_Pro_Trend|BTCUSDT_SMC_Pro_Trend"):
        tr.submit(mk(A, direction=Direction.LONG, bar=2), 2000)  # та же монета и стратегия, другая сторона и бар
    assert len(tr.trades) == 1  # отказ не оставил следа
    assert tr.submit(mk(B, direction=Direction.LONG, bar=2), 2000).accepted  # другая стратегия - можно
    tr.on_tick(tick(100.5, 10))
    tr.on_tick(tick(105.5, 20))  # SL шорта A
    assert tr.submit(mk(A, direction=Direction.LONG, bar=3), 3000).accepted  # ключ освободился


def test_each_strategy_gets_its_own_scale_out_plan_on_the_same_coin_and_move():
    tr, _ = make()
    tr.submit(mk(A, kind=Kind.MARKET, bar=1), 1000)  # DEFAULT_PLAN: 50% на +1R, 40% на +3R, 10% runner
    tr.submit(mk(B, kind=Kind.MARKET, bar=1), 1000)  # SCALP_1R_PLAN: 100% на +1R
    tr.on_tick(tick(100.0, 10))  # MARKET исполнены по первому тику
    ta, tb = tr.trades
    assert ta.plan is DEFAULT_PLAN and tb.plan is SCALP_1R_PLAN and tr.plan_for(E) is VWAP_PLAN and tr.plan_for("неизвестная") is DEFAULT_PLAN
    tr.on_tick(tick(94.9, 20))  # +1R обеих (R = 5)
    assert ta.status == OPEN and ta.remaining == pytest.approx(0.5)
    assert tb.status == CLOSED and tb.exit_reason == "tp" and len(tb.legs) == 1 and tb.net_r == pytest.approx(tb.legs[0].net_r)


def test_gap_and_bar_close_touch_every_position_of_the_coin_independently():
    tr, rec = make()
    tr.submit(mk(A, bar=1), 1000)
    tr.submit(mk(C, direction=Direction.LONG, bar=1), 1000)
    tr.mark_gap(SYM, 5)
    assert all(t.gap_affected for t in tr.trades) and all(r["gap_affected"] for r in rec.trades.values())
    for i in range(2, 12):  # 10 свечей без исполнения: у обеих лимиток срок жизни (TTL) истекает каждой отдельно
        tr.on_bar_closed(SYM, i * IV, 100.0, 1, BarInfo(100.5, 99.5, 1.0))
    assert [t.exit_reason for t in tr.trades] == ["ttl", "ttl"] and tr.active() == []


def test_other_coins_are_untouched():
    tr, _ = make()
    tr.submit(mk(A, symbol="SOLUSDT", bar=1), 1000)
    tr.submit(mk(A, symbol="XRPUSDT", bar=1), 1000)  # тот же strategy_id, другая монета - другой ключ
    tr.on_tick(tick(100.5, 10, symbol="SOLUSDT"))
    sol, xrp = tr.trades
    assert sol.status == OPEN and xrp.status == PENDING_LIMIT


# ======================================================================== Оркестратор: пять стратегий параллельно


def build(strategies, tracker=None, **kw):
    rec = kw.pop("recorder", None) or MemoryRecorder()
    log = RecordingEventLog(MemoryEventLog(), rec)
    tracker = tracker or VirtualTracker(log, equity=10_000.0, recorder=rec, plans=plans_for([s.name for s in strategies]))
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log, recorder=rec, **kw)
    return orch, tracker, rec


def test_all_strategies_trade_the_same_coin_on_the_same_bar_without_blocking_each_other():
    sigs = {A: mk(A, direction=Direction.SHORT, bar=1), B: mk(B, direction=Direction.LONG, kind=Kind.MARKET, bar=1),
            C: mk(C, direction=Direction.LONG, kind=Kind.MARKET, bar=1), D: mk(D, direction=Direction.SHORT, kind=Kind.MARKET, bar=1),
            E: mk(E, direction=Direction.SHORT, kind=Kind.MARKET, bar=1), F: mk(F, direction=Direction.LONG, kind=Kind.MARKET, bar=1),
            G: mk(G, direction=Direction.SHORT, kind=Kind.MARKET, bar=1)}
    orch, tracker, rec = build([Scripted(n, {1: [s]}) for n, s in sigs.items()])
    events = [e for i in range(2) for e in orch.on_context(ctx(SYM, i))]
    assert [e.kind for e in events] == ["signal_accepted"] * 7  # никто не получил skipped_due_to_lock: у каждой стратегии своя блокировка (О14)
    assert {t.signal.strategy for t in tracker.active(SYM)} == set(SHADOW_STRATEGIES)
    assert sorted(orch.held().values()) == sorted((SYM, f"shadow:{n}") for n in SHADOW_STRATEGIES)
    assert {s["strategy_id"] for s in rec.snapshots} == set(SHADOW_STRATEGIES)  # снапшот каждой сделки несёт strategy_id
    assert {r["strategy_id"] for r in rec.trades.values()} == set(SHADOW_STRATEGIES)
    ev_ids = {e["strategy_id"] for e in rec.state_events if e["signal_id"]}
    assert ev_ids == set(SHADOW_STRATEGIES)  # и события состояний


def test_second_signal_of_the_same_strategy_on_the_busy_coin_is_still_blocked_by_its_own_lock():
    first, second = mk(A, direction=Direction.SHORT, bar=1), mk(A, direction=Direction.LONG, bar=2)
    orch, tracker, _ = build([Scripted(A, {1: [first], 2: [second]})])
    ev = [e for i in range(3) for e in orch.on_context(ctx(SYM, i))]
    assert [e.kind for e in ev] == ["signal_accepted", "skipped_due_to_lock"] and len(tracker.active(SYM)) == 1


def test_one_strategy_closing_does_not_release_the_lock_of_another():
    sigs = {A: mk(A, bar=1), B: mk(B, direction=Direction.LONG, bar=1)}
    orch, tracker, _ = build([Scripted(n, {1: [s]}) for n, s in sigs.items()])
    for i in range(2):
        orch.on_context(ctx(SYM, i))
    tracker.on_tick(tick(100.5, 10))
    tracker.on_tick(tick(105.5, 20))  # SL шорта A
    assert set(orch.held().values()) == {(SYM, f"shadow:{B}")}  # блокировка A снята, B - держится


# ================================================================================================= реестр


def test_registry_builds_the_seven_strategies_with_unique_ids_and_individual_plans():
    names = [s.name for s in build_strategies()]
    expected = [SMC_PRO_TREND, SMC_COUNTER_TREND, OBI_BREAKOUT, ICT_KILLZONES, VWAP_REVERSION, HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT]
    assert names == expected == list(SHADOW_STRATEGIES) and len(set(names)) == 7
    plans = plans_for(names)
    assert plans[SMC_COUNTER_TREND] is SCALP_1R_PLAN and plans[VWAP_REVERSION] is VWAP_PLAN
    assert plans[HORIZONTAL_BREAKOUT] is plans[TRENDLINE_BREAKOUT] is LEVEL_BREAKOUT_PLAN
    assert LEVEL_BREAKOUT_PLAN.levels == ((1.0, 0.5), (2.0, 0.5)) and LEVEL_BREAKOUT_PLAN.be_after_first and LEVEL_BREAKOUT_PLAN.runner_frac == 0
    assert all(plans[n] is DEFAULT_PLAN for n in (SMC_PRO_TREND, OBI_BREAKOUT, ICT_KILLZONES))
    assert SCALP_1R_PLAN.levels == ((1.0, 1.0),) and SCALP_1R_PLAN.runner_frac == 0 and VWAP_PLAN.runner_frac == 0
    assert build_strategies()[0].params.htf_filter is True  # Pro_Trend - только по тренду H4


def test_registry_rejects_unknown_and_duplicate_names(monkeypatch):
    with pytest.raises(ConfigError, match="Неизвестные"):
        build_strategies(("Nope",))
    with pytest.raises(ConfigError, match="Дубли"):
        build_strategies((A, A))
    monkeypatch.setattr("features.defaults.HTF_FILTER_ENABLED", False)
    with pytest.raises(ConfigError, match="HTF"):
        build_strategies((A,))
    assert [s.name for s in build_strategies((D,))] == [D]  # стратегиям без H4 фильтр не нужен


# ============================== сквозной прогон: пять настоящих детекторов на одном ряду свечей, окно 300 баров, тики на каждую свечу

SOFT = {
    SMC_PRO_TREND: lambda: StrategySMC(SmcSetupParams(htf_filter=True, max_exposure=1e9), name=SMC_PRO_TREND),
    SMC_COUNTER_TREND: lambda: StrategyCounterTrend(CounterTrendParams(min_sweep_atr=0.05, volume_mult=0.5)),
    OBI_BREAKOUT: lambda: StrategyObiBreakout(ObiParams(impulse_atr_mult=0.3, obi_min=0.2, delta_min=0.2, delta_volume_mult=0.5)),
    ICT_KILLZONES: StrategyKillzones,
    VWAP_REVERSION: lambda: StrategyVwapReversion(VwapParams(sd_mult=1.5, min_day_bars=8)),
    HORIZONTAL_BREAKOUT: lambda: StrategyHorizontalBreakout(HorizontalParams(volume_mult=0.5, min_clv=0.3, max_box_atr=8.0, min_touches=2,
                                                                             min_body_atr=0.1, max_risk_atr=6.0)),
    TRENDLINE_BREAKOUT: lambda: StrategyTrendlineBreakout(TrendlineParams(volume_mult=0.5, sweep_volume_mult=0.5, min_clv=0.3, min_body_atr=0.1,
                                                                          max_risk_atr=6.0)),
}


def test_five_real_detectors_on_one_price_series_keep_their_states_apart():
    df = random_walk(1800, 51, 0.012)
    fp = SmcParams(swing_atr_mult=0.5)
    strategies = [SOFT[n]() for n in SHADOW_STRATEGIES]
    rec = MemoryRecorder()
    log = RecordingEventLog(MemoryEventLog(), rec)
    tracker = VirtualTracker(log, equity=10_000.0, recorder=rec, plans=plans_for(SHADOW_STRATEGIES), max_hold_bars=40)
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log, recorder=rec, max_signal_age_ms=10**12)
    max_active_per_key = 0
    for i in range(320, df.height):
        window = df.slice(i - 299, 300)
        f = add_features(window, fp, interval_ms=IV)
        f = f.with_columns((((pl.col("open_time") // IV // 80) % 3) - 1).cast(pl.Int8).alias("htf_bias"),
                           ((pl.col("open_time") // IV * 7919 % 200 - 100) / 100).alias("ms_obi"),
                           ((pl.col("open_time") // IV * 104729 % 200 - 100) / 100).alias("ms_delta"))
        t_close = int(window["open_time"][-1]) + IV
        orch.on_context(FeatureContext(SYM, "15m", IV, f, t_close + 1000))
        last = float(window["close"][-1])
        tracker.on_tick(Tick(SYM, last, t_close + 2000, "trade", 1.0, False))  # первый тик после закрытия: MARKET-заявки исполняются
        keys = [t.signal.position_key for t in tracker.active()]
        assert len(keys) == len(set(keys))  # на ключ - не больше одной активной позиции
        max_active_per_key = max(max_active_per_key, max((keys.count(k) for k in keys), default=0))
    traded = {t.signal.strategy for t in tracker.trades}
    assert len(traded) >= 4, f"на тестовом ряду торговали только {traded}"
    ids = [t.trade_id for t in tracker.trades]
    assert len(ids) == len(set(ids)) and all(t.trade_id.startswith(t.signal.strategy + ":") for t in tracker.trades)
    for row in rec.trades.values():
        assert row["strategy_id"] == row["strategy_name"] and row["id"].startswith(row["strategy_id"] + ":")
    assert max_active_per_key == 1
    # у каждой завершённой сделки закрытия принадлежат именно ей, сумма долей = 1, strategy_id в строке сделки тот же
    by_trade: dict[str, float] = {}
    for c in rec.closes:
        by_trade[c["trade_id"]] = by_trade.get(c["trade_id"], 0.0) + c["frac"]
    for t in tracker.trades:
        if t.status == CLOSED:
            assert by_trade[t.trade_id] == pytest.approx(1.0) and rec.trades[t.trade_id]["strategy_id"] == t.signal.strategy


# =================================================================================== микроструктура (стакан и лента)


def test_micro_aggregator_delta_obi_and_validity():
    ms = MicroStructure((SYM,), IV, obi_window_s=120.0, grace_s=10.0)
    agg = ms.aggregator
    bar_open = 4 * IV
    book = ([["100", "30"], ["99.9", "10"]], [["100.1", "10"], ["100.2", "10"]])  # bid 40 / ask 20 -> OBI +1/3
    agg.on_depth(SYM, bar_open + 1_000, *book)  # старый снимок: вылетит из окна 120 с
    for k in range(10):
        agg.on_depth(SYM, bar_open + IV - 100_000 + k * 500, *book)
        agg.on_trade(SYM, bar_open + 2_000 + k, 2.0, False)  # агрессор купил
    agg.on_trade(SYM, bar_open + 5_000, 5.0, True)  # агрессор продал
    s = agg.summary(SYM, bar_open)
    assert s.valid and s.trades == 11 and s.delta == pytest.approx((20.0 - 5.0) / 25.0) and s.obi == pytest.approx(1 / 3) and s.obi_samples == 10
    assert agg.summary(SYM, bar_open + IV).valid is False and agg.summary(SYM, bar_open + IV).delta is None  # свечи без данных нет


def test_micro_bar_is_invalid_when_the_stream_started_late_or_broke():
    ms = MicroStructure((SYM,), IV, grace_s=10.0)
    agg = ms.aggregator
    bar_open = 4 * IV
    agg.on_trade(SYM, bar_open + 30_000, 1.0, False)  # поток начался на 30-й секунде свечи (допуск 10 с)
    agg.on_depth(SYM, bar_open + 1_000, [["1", "1"]], [["2", "1"]])
    assert agg.summary(SYM, bar_open).valid is False
    agg = MicroStructure((SYM,), IV, grace_s=10.0).aggregator  # чистый старт до начала свечи, затем обрыв
    agg.on_trade(SYM, bar_open + 1_000, 1.0, False)
    agg.on_depth(SYM, bar_open + 1_000, [["1", "1"]], [["2", "1"]])
    assert agg.summary(SYM, bar_open).valid is True
    agg.mark_gap("depth")
    assert agg.summary(SYM, bar_open).valid is False  # потерянный стакан: свеча неполная
    with pytest.raises(ConfigError):
        agg.mark_gap("bogus")


def test_micro_attach_puts_values_only_on_the_last_row_and_requires_trade_fields():
    ms = MicroStructure((SYM,), IV)
    ms.aggregator.on_trade(SYM, 1_000, 3.0, False)
    ms.aggregator.on_depth(SYM, 1_000, [["1", "3"]], [["2", "1"]])
    df = pl.DataFrame({"open_time": [-IV, 0], "close": [1.0, 1.0]})
    out = ms.attach(FeatureContext(SYM, "15m", IV, df, 5))
    assert out.frame["ms_obi"].to_list() == [None, 0.5] and out.frame["ms_delta"].to_list() == [None, 1.0]
    with pytest.raises(DataError, match="уже есть"):
        ms.attach(out)
    with pytest.raises(DataError, match="объёма"):
        ms.on_tick(Tick(SYM, 1.0, 1, "trade"))  # тик ленты без qty/сторон - нарушение контракта (R1)
    ms.on_tick(Tick(SYM, 1.0, 1, "mark"))  # mark price микроструктуру не интересует
    with pytest.raises(DataError, match="неизвестная монета"):
        ms.aggregator.on_trade("XXX", 1, 1.0, False)


def test_depth_message_parsing_and_garbage():
    async def body():
        ms = MicroStructure((SYM,), IV)
        feed = DepthFeed(None, ms.aggregator, symbols=(SYM,))
        good = {"stream": "btcusdt@depth10@500ms", "data": {"e": "depthUpdate", "E": 1005, "T": 1000, "s": SYM, "U": 1, "u": 2, "pu": 0,
                                                            "b": [["100.0", "5"]], "a": [["100.1", "15"]]}}
        feed._on_frame(json.dumps(good), 0)
        assert ms.aggregator.summary(SYM, 0).obi_samples == 1  # время сообщения T = 1000 мс -> свеча 0
        for bad in ({**good, "data": {**good["data"], "b": [["x", "y"]]}}, {**good, "data": {**good["data"], "s": "ETHUSDT"}},
                    {**good, "data": {k: v for k, v in good["data"].items() if k not in ("T", "E")}}, {**good, "stream": "foo"}):
            with pytest.raises(DataError):
                feed._on_frame(json.dumps(bad), 0)

    asyncio.run(body())


def test_depth_feed_over_a_local_websocket_reconnects_and_reports_the_gap():
    queries, conns = [], []
    msg = {"stream": "btcusdt@depth10@500ms", "data": {"e": "depthUpdate", "E": 1005, "T": 1000, "s": SYM, "U": 1, "u": 2, "pu": 0,
                                                       "b": [["100.0", "5"]], "a": [["100.1", "15"]]}}

    async def ws_handler(request):
        conns.append(1)
        queries.append(request.query_string)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_str(json.dumps(msg))
        if len(conns) == 1:
            await asyncio.sleep(0.1)
            await ws.close()
        else:
            await asyncio.sleep(1)
        return ws

    app = web.Application()
    app.router.add_get("/public/stream", ws_handler)

    async def body():
        ms = MicroStructure((SYM,), IV)
        async with serve(app) as host, aiohttp.ClientSession() as session:
            feed = DepthFeed(session, ms.aggregator, symbols=(SYM,), url=f"ws://{host}/public/stream", backoff_s=0.01, backoff_max_s=0.02)
            try:
                feed.start()
                await settle(lambda: len(conns) >= 2 and feed.stats()["received"] >= 2)
                feed.check()
                assert ms.aggregator.summary(SYM, 0).obi_samples == 2  # оба соединения доставили снимок (обе - в свечу 0)
            finally:
                await feed.aclose()
            return feed.stats()

    stats = asyncio.run(body())
    assert stats["reconnects"] == 1 and queries[0] == "streams=btcusdt@depth10@500ms"


def test_micro_structure_is_not_needed_for_other_strategies():
    from orchestrator.registry import NEEDS_HTF, NEEDS_MICRO
    assert NEEDS_MICRO == (OBI_BREAKOUT,) and set(NEEDS_HTF) == {SMC_PRO_TREND, SMC_COUNTER_TREND, HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT}
    assert config.DEPTH_LEVELS == 10 and config.DEPTH_UPDATE_SPEED == "500ms"  # DepthFeed подписывается ровно на depth10@500ms


def test_signal_dataclass_stays_frozen():
    s = mk(A)
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.strategy = "other"  # type: ignore[misc]
