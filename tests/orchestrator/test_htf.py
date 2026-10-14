"""HTF Bias в оркестраторе и Shadow-цикле (2026-10-11): H4-контекст -> bias, присоединение к M15, фильтр SMC, причина входа, фид H4."""

import asyncio
import dataclasses
import inspect

import polars as pl
import pytest

import config
import scripts.shadow_run as shadow_run
from features import defaults as D
from features.htf import BIAS_COLUMN
from orchestrator.core import Orchestrator, Route
from orchestrator.htf import HtfBias
from orchestrator.journal import MemoryEventLog
from orchestrator.registry import build_strategies
from orchestrator.strategies import StrategyBreakout, StrategySMC
from orchestrator.tracker import VirtualTracker
from orchestrator.types import FeatureContext, Mode
from scripts.shadow_run import ShadowRun
from strategies.smc_setups import SmcSetupParams
from tests.orchestrator.test_feeds import IV as FEED_IV, SYM as FEED_SYM, feed_without_net, kline_msg, run as run_async
from tests.orchestrator.test_shadow_run import FakeFeed, scenario_frame
from tests.orchestrator.test_strategies import context
from tests.strategies.test_smc_setups import STEP
from utils.errors import ConfigError, DataError

H4 = config.INTERVAL_MS["4h"]
SYM = "BTCUSDT"
BULL, BEAR, NEUT = (110.0, 105.0, 100.0, 95.0), (100.0, 105.0, 90.0, 95.0), (110.0, 105.0, 90.0, 95.0)  # sh1, sh2, sl1, sl2


def h4_context(structure: tuple, *, open_time: int = -H4, symbol: str = SYM, received: int = 1) -> FeatureContext:
    """H4-контекст из двух свечей (open_time и +H4) с одной структурой: M15-свечи сценария 0..19 (5 часов) всегда видят закрытую H4."""
    sh1, sh2, sl1, sl2 = structure
    cols = ("sh1_price", "sh2_price", "sl1_price", "sl2_price")
    frame = pl.DataFrame({"open_time": [open_time, open_time + H4], "atr": [1.0] * 2, "sh1_price": [sh1] * 2, "sh2_price": [sh2] * 2,
                          "sl1_price": [sl1] * 2, "sl2_price": [sl2] * 2},
                         schema={"open_time": pl.Int64, "atr": pl.Float64, **{n: pl.Float64 for n in cols}})
    return FeatureContext(symbol, "4h", H4, frame, received)


def htf_with(structure: tuple, events=None) -> HtfBias:
    htf = HtfBias("4h", H4, events)
    htf.update(h4_context(structure))
    return htf


# ------------------------------------------------------------------------------------------------- HtfBias


def test_attach_adds_bias_columns_and_keeps_the_frame():
    ctx = context(scenario_frame(), 19)
    out = htf_with(BEAR).attach(ctx)
    assert out.frame[BIAS_COLUMN][-1] == -1 and out.frame.height == ctx.frame.height and out.symbol == SYM
    assert out.frame[BIAS_COLUMN].null_count() == 0 and set(out.frame[BIAS_COLUMN].to_list()) == {-1}
    assert out.frame.drop([BIAS_COLUMN, "htf_avail_ms"]).equals(ctx.frame)  # прочие данные не тронуты


def test_missing_stale_or_wrong_interval_data_is_an_error_r1():
    ctx = context(scenario_frame(), 19)
    with pytest.raises(DataError, match="нет данных H4"):
        HtfBias("4h", H4).attach(ctx)
    with pytest.raises(DataError, match="отстают"):  # H4-фид не получил свечу: последняя доступная закрылась больше 4 часов назад
        htf_with(BULL).attach(dataclasses.replace(ctx, frame=ctx.frame.with_columns(pl.col("open_time") + H4)))
    with pytest.raises(DataError, match="нет закрытой H4"):  # история H4 короче истории M15
        htf = HtfBias("4h", H4)
        htf.update(h4_context(BULL, open_time=10**12))
        htf.attach(ctx)
    with pytest.raises(DataError, match="ждёт 4h"):
        htf_with(BULL).update(dataclasses.replace(h4_context(BULL), interval="1h"))
    with pytest.raises(ConfigError):
        HtfBias("4h", 0)


def test_bias_change_is_journaled_once_per_change():
    events = MemoryEventLog()
    htf = htf_with(BULL, events)
    htf.update(h4_context(BULL))  # то же состояние - записи нет
    htf.update(h4_context(BEAR))
    htf.update(h4_context(NEUT, symbol="ETHUSDT"))
    got = [(e.symbol, e.detail["bias"]) for e in events.of_kind("htf_bias")]
    assert got == [(SYM, "Bullish"), (SYM, "Bearish"), ("ETHUSDT", "Neutral")]
    assert htf.current(SYM) == -1 and htf.current("XRPUSDT") is None and htf.has("ETHUSDT")


# --------------------------------------------------------------------------------- стратегия: фильтр, meta, причина


def test_strategy_with_filter_blocks_against_trend_and_records_h4_context_in_signal_meta():
    smc = StrategySMC(SmcSetupParams(htf_filter=True))
    ctx = context(scenario_frame(), 19)
    assert smc.detect(htf_with(BULL).attach(ctx)) == ()  # H4 бычий: шорт M15 не ищем
    (sig,) = smc.detect(htf_with(BEAR).attach(ctx))
    assert sig.meta["htf_bias"] == -1 and sig.meta["htf_label"] == "Bearish" and sig.meta["htf_interval"] == "4h"
    (neutral,) = smc.detect(htf_with(NEUT).attach(ctx))
    assert neutral.meta["htf_label"] == "Neutral"
    (plain,) = StrategySMC().detect(ctx)  # без фильтра и без присоединённого bias - прежнее поведение, meta без H4
    assert "htf_label" not in plain.meta and plain.entry == sig.entry


def test_breakout_is_not_filtered_by_the_bias_column():
    frame = scenario_frame().with_columns(pl.lit(1, dtype=pl.Int8).alias(BIAS_COLUMN))
    assert StrategyBreakout().detect(context(frame, 19)) == ()  # поджатия в сценарии нет; колонка bias стратегии не мешает


# ------------------------------------------------------------------------------------- цикл Shadow с фидом H4


class FakeHtfFeed:
    def __init__(self, batches):
        self.batches = list(batches)

    async def poll(self):
        return self.batches.pop(0) if self.batches else []


def shadow_stack(tmp_path, structure, htf_batches=None):
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0)
    strategies = [StrategySMC(SmcSetupParams(htf_filter=D.HTF_FILTER_ENABLED)), StrategyBreakout()]
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log)
    frame = scenario_frame()
    htf = HtfBias("4h", H4, log)
    htf_feed = FakeHtfFeed(htf_batches if htf_batches is not None else [[h4_context(structure)]])
    runner = ShadowRun(FakeFeed([[context(frame, i + 1)] for i in range(20)]), orch, tracker, None, heartbeat=tmp_path / ".hb",
                       htf_feed=htf_feed, htf=htf)
    return runner, tracker, log


def drive(runner, ticks=20):
    async def body():
        for _ in range(ticks):
            await runner.tick()

    asyncio.run(body())


def test_shadow_loop_opens_no_counter_trend_trade_and_logs_why(tmp_path):
    runner, tracker, log = shadow_stack(tmp_path, BULL)  # H4 бычий, а сценарий - шорт SMC на M15
    drive(runner)
    assert tracker.active() == [] and not log.of_kind("signal_accepted")
    assert not [e for e in log.of_kind("setup_state") if e.detail["side"] == "short"]  # автомат шорта не покидал S0
    assert [e.detail["bias"] for e in log.of_kind("htf_bias")] == ["Bullish"]


def test_shadow_loop_trades_with_the_trend(tmp_path):
    runner, tracker, log = shadow_stack(tmp_path, BEAR)
    drive(runner)
    (accepted,) = log.of_kind("signal_accepted")
    assert accepted.detail["direction"] == "short" and tracker.active()[0].signal.meta["htf_label"] == "Bearish"


def test_missing_h4_stops_the_loop_instead_of_trading_blind_r1(tmp_path):
    runner, _, _ = shadow_stack(tmp_path, BEAR, htf_batches=[[]])  # H4-фид молчит: bias нет -> M15-свеча не обрабатывается вслепую
    with pytest.raises(DataError, match="нет данных H4"):
        drive(runner, 1)
    with pytest.raises(ConfigError, match="вместе"):
        ShadowRun(FakeFeed([]), None, None, None, htf_feed=FakeHtfFeed([]))  # type: ignore[arg-type]


def test_shadow_main_wires_the_filter_on_by_default():
    assert D.HTF_FILTER_ENABLED is True and D.HTF_INTERVAL == "4h" and config.INTERVAL_MS[D.HTF_INTERVAL] == H4
    src = inspect.getsource(shadow_run.shadow_main)
    assert "build_strategies(names, mtf=mtf, interval=opts.interval)" in src and "interval=D.HTF_INTERVAL" in src and "emit_latest_on_start=True" in src
    smc, counter = build_strategies(("SMC_Pro_Trend", "SMC_Counter_Trend"))
    assert smc.params.htf_filter is True and counter.name == "SMC_Counter_Trend"  # обе читают H4; боевой набор Shadow включает фильтр
    assert D.HTF_HISTORY_BARS >= 300  # хватает на 2+ подтверждённых свинга с каждой стороны (k = 5, амплитуда > 1.5 ATR)


# ------------------------------------------------------------------------------------------- фид H4: стартовый контекст


def test_emit_latest_on_start_gives_exactly_one_context_with_history_then_only_new_bars():
    async def body():
        feed, _ = feed_without_net()
        feed._emit_latest = True
        await feed._bootstrap()
        (ctx,) = await feed.poll()  # состояние H4 нужно сразу после старта, а не через 4 часа
        assert ctx.bar_open_time == 200 * FEED_IV - FEED_IV and ctx.frame.height == 120
        assert await feed.poll() == []
        feed.on_kline_message(FEED_SYM, kline_msg(200), 5)
        assert [c.bar_open_time for c in await feed.poll()] == [200 * FEED_IV]

    run_async(body())
