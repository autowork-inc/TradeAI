"""Оркестрация мульти-таймфреймового режима: стратегия считается только на своём таймфрейме, цепочки свечей независимы по (монета, таймфрейм), приёмник живёт по барам
своих сделок (сроки в календарном времени не меняются), состояние старших таймфреймов (MtfState) присоединяется без заглядывания вперёд и с fail-fast."""

import dataclasses

import polars as pl
import pytest

import config
from features import defaults as D
from features.htf import BIAS_COLUMN
from features.mtf import D1_AVAIL, D1_BIAS, H1_AVAIL, attach_table, d1_bias_table, h1_table
from features.smc import SmcParams, add_features
from orchestrator.core import Orchestrator, OrchestratorError, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.journal import MemoryEventLog
from orchestrator.mtf import MtfState
from orchestrator.registry import SHADOW_STRATEGIES, build_strategies
from orchestrator.strategies import SMC_PRO_TREND, StrategySMC
from orchestrator.tracker import CANCELLED, OPEN, VirtualTracker
from orchestrator.types import BarInfo, FeatureContext, Mode
from strategies.smc_setups import SmcSetupParams
from tests.features.test_mtf import BASE, H1, M5, resample, walk5
from tests.features.test_smc import random_walk
from tests.orchestrator.helpers import SYM, mk, tick
from utils.errors import ConfigError, DataError

M15, D1 = 900_000, 86_400_000
INTERVALS = dict(config.INTERVAL_MS)


def mk_ctx(interval: str, idx: int, symbol: str = SYM, close: float = 100.0) -> FeatureContext:
    iv = INTERVALS[interval]
    frame = pl.DataFrame({"open_time": [idx * iv], "close": [close]})
    return FeatureContext(symbol, interval, iv, frame, (idx + 1) * iv + 1000)


class Spy:
    def __init__(self, name: str, interval: str | None):
        self.name, self.interval, self.seen = name, interval, []

    def detect(self, c: FeatureContext):
        self.seen.append((c.symbol, c.interval, c.bar_open_time))
        return ()


class Sink:
    def __init__(self):
        self.bars, self.listeners = [], []

    def submit(self, signal, now_ms):
        raise AssertionError("не должен вызываться")

    def on_bar_closed(self, symbol, bar_open_time, close, now_ms, bar=None, interval=None):
        self.bars.append((symbol, interval, bar_open_time))

    def add_close_listener(self, listener):
        self.listeners.append(listener)


# --------------------------------------------------------------------------------------------------- Оркестратор


def test_each_strategy_is_computed_only_on_its_own_interval():
    pro, other, legacy = Spy("pro", "5m"), Spy("other", "15m"), Spy("any", None)
    sink = Sink()
    orch = Orchestrator([pro, other, legacy], {s.name: Route(Mode.SHADOW, sink) for s in (pro, other, legacy)}, MemoryEventLog())
    orch.on_context(mk_ctx("5m", 0))
    orch.on_context(mk_ctx("15m", 0))
    orch.on_context(mk_ctx("5m", 1))
    assert [x[1] for x in pro.seen] == ["5m", "5m"] and [x[1] for x in other.seen] == ["15m"] and [x[1] for x in legacy.seen] == ["5m", "15m", "5m"]
    assert [(i, b) for _, i, b in sink.bars] == [("5m", 0), ("15m", 0), ("5m", M5)]  # приёмник узнаёт таймфрейм каждой закрытой свечи


def test_candle_chains_are_checked_per_symbol_and_interval():
    spy = Spy("s", None)
    orch = Orchestrator([spy], {"s": Route(Mode.SHADOW, Sink())}, MemoryEventLog())
    for i in range(3):  # 5m и 15m одной монеты идут вперемешку и не мешают друг другу: ключ (монета, таймфрейм)
        orch.on_context(mk_ctx("5m", i))
        orch.on_context(mk_ctx("15m", i))
    orch.on_context(mk_ctx("5m", 0, symbol="ETHUSDT"))
    with pytest.raises(OrchestratorError, match="5m"):
        orch.on_context(mk_ctx("5m", 5))  # пропуск 5m-свечи - ошибка, хотя 15m в порядке
    orch.on_context(mk_ctx("15m", 3))


# ------------------------------------------------------------------------------------------------------ трекер


def five_minute(signal):
    return dataclasses.replace(signal, interval="5m", bar_open_time=0, bar_close_ms=M5)


def test_a_5m_trade_ignores_15m_bars_and_expires_after_the_same_wall_clock_time_as_before():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    assert tr.submit(five_minute(mk()), 1000).accepted  # лимитка, не исполняется: цена не доходит
    t = tr.trades[-1]
    for k in range(1, 40):  # 39 закрытий 15m не «стареют» заявку 5m
        tr.on_bar_closed(SYM, k * M15, 100.0, k, interval="15m")
    assert t.status == "pending_limit" and t.bars_closed == 0
    scale = D.bars_scale(M5)
    assert scale == 3 and D.bars_scale(M15) == 1 and D.bars_scale(H1) == 1
    for k in range(1, D.LIMIT_TTL_BARS * scale):  # 29 баров 5m
        tr.on_bar_closed(SYM, k * M5, 100.0, k, interval="5m")
    assert t.status == "pending_limit"
    tr.on_bar_closed(SYM, D.LIMIT_TTL_BARS * scale * M5, 100.0, 99, interval="5m")  # 30-й: 10 баров M15 = 150 минут
    assert t.status == CANCELLED


def test_a_15m_trade_keeps_ten_bar_ttl_and_ignores_5m_bars():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    assert tr.submit(mk(), 1000).accepted
    t = tr.trades[-1]
    for k in range(1, 100):
        tr.on_bar_closed(SYM, k * M5, 100.0, k, interval="5m")
    assert t.status == "pending_limit"
    for k in range(1, D.LIMIT_TTL_BARS):
        tr.on_bar_closed(SYM, k * M15, 100.0, k, interval="15m")
    assert t.status == "pending_limit"
    tr.on_bar_closed(SYM, D.LIMIT_TTL_BARS * M15, 100.0, 99, interval="15m")
    assert t.status == CANCELLED


def test_max_hold_scales_with_the_signal_interval():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0, max_hold_bars=4)
    tr.submit(five_minute(mk()), 1000)
    t = tr.trades[-1]
    tr.on_tick(tick(100.5, 10))
    assert t.status == OPEN
    for k in range(1, 13):  # max_hold 4 бара M15 = 12 баров 5m (бар входа не считается: fill_bar_no = 1): 12 закрытий - позиция жива
        tr.on_bar_closed(SYM, k * M5, 100.0, k, BarInfo(100.5, 99.5, 1.0), interval="5m")
    assert t.status == OPEN
    tr.on_bar_closed(SYM, 13 * M5, 100.0, 13, BarInfo(100.5, 99.5, 1.0), interval="5m")
    assert t.exit_reason == "timeout"


def test_runner_trailing_uses_only_bars_of_its_own_interval():
    """Трейлинг runner'а (10%): ATR берётся из закрытых 5m-свечей сделки; 15m-свечи с другим ATR на стоп не влияют."""
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    tr.submit(five_minute(mk()), 1000)
    t = tr.trades[-1]
    tr.on_tick(tick(100.5, 10))
    tr.on_tick(tick(84.0, 20))  # +3.2R: уровни 1R и 3R закрыты, остаётся runner, лучшая цена 84
    assert t.runner_active
    tr.on_bar_closed(SYM, 1 * M15, 95.0, 30, BarInfo(95.5, 94.5, 50.0), interval="15m")  # чужой таймфрейм: не считается вовсе
    assert t.stop_m == 100.0 and t.stop_kind == "be"
    tr.on_bar_closed(SYM, 1 * M5, 90.0, 31, BarInfo(90.5, 89.5, 2.0), interval="5m")  # 5m: стоп = 84 + 3 * 2 = 90
    assert t.stop_m == pytest.approx(90.0) and t.stop_kind == "trail"
    tr.on_tick(tick(90.4, 40))  # откат до стопа: выход остатка по цене тика
    assert t.exit_reason == "trail" and t.legs[-1].price == 90.4


def test_untagged_bars_keep_the_old_behaviour_for_existing_callers():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    tr.submit(mk(), 1000)
    t = tr.trades[-1]
    for k in range(1, D.LIMIT_TTL_BARS + 1):
        tr.on_bar_closed(SYM, k * M15, 100.0, k)
    assert t.status == CANCELLED


def test_tracker_rejects_a_signal_with_an_unknown_interval():
    tr = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    tr.submit(dataclasses.replace(mk(), interval="7m"), 1000)
    with pytest.raises(ConfigError, match="7m"):
        tr.on_bar_closed(SYM, M15, 100.0, 1, interval="7m")


# -------------------------------------------------------------------------------------- стратегия и реестр


def test_smc_strategy_requires_an_interval_in_mtf_mode_and_refuses_foreign_candles():
    with pytest.raises(ConfigError, match="interval"):
        StrategySMC(SmcSetupParams(mtf=True))
    s = StrategySMC(SmcSetupParams(htf_filter=True), name="x", interval="5m")
    with pytest.raises(DataError, match="только на 5m"):
        s.analyze(mk_ctx("15m", 0))  # Оркестратор такой контекст не передаёт; прямой вызов - баг
    assert StrategySMC().interval is None  # по умолчанию - любой (старые тесты и бэктест)


def test_registry_builds_pro_trend_on_5m_and_the_rest_on_the_legacy_interval():
    from orchestrator.multi_strategies import VWAP_REVERSION

    built = {s.name: s for s in build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")}
    pro = built[SMC_PRO_TREND]
    assert pro.interval == "5m" and pro.params.mtf and pro.params.htf_filter and pro.params.setup_ttl_bars == D.SETUP_TTL_BARS * 3  # 30 баров M15 = 90 баров 5m
    assert {n: s.interval for n, s in built.items() if n not in (SMC_PRO_TREND, VWAP_REVERSION)} == {n: "15m" for n in built if n not in (SMC_PRO_TREND, VWAP_REVERSION)}
    assert built[VWAP_REVERSION].interval == "5m"  # с 2026-10-15 вместе с SMC_Pro_Trend
    old = build_strategies(SHADOW_STRATEGIES)
    assert all(getattr(s, "interval", None) is None for s in old) and not old[0].params.mtf  # по умолчанию - прежний одно-таймфреймовый режим
    with pytest.raises(ConfigError, match="interval"):
        build_strategies(SHADOW_STRATEGIES, mtf=True)


# ----------------------------------------------------------------------------------------------------- MtfState


def feature_ctx(interval: str, df: pl.DataFrame, received: int = 0) -> FeatureContext:
    return FeatureEngine(SmcParams(), min_bars=30).build(SYM, interval, INTERVALS[interval], df, received)


def daily(n: int = 80, seed: int = 2) -> pl.DataFrame:
    """1d-история, заканчивающаяся через ~9 суток после BASE (последняя свеча закрывается на BASE + 10 суток)."""
    return random_walk(n, seed, 0.02).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * D1 + BASE - 70 * D1).alias("open_time"))


def test_mtf_state_attaches_1h_and_1d_columns_exactly_like_the_pure_functions():
    df5 = walk5(3000, 5)
    h1_df, d1_df = resample(df5, H1), daily()
    state = MtfState(INTERVALS, MemoryEventLog())
    state.update(feature_ctx("1h", h1_df))
    state.update(feature_ctx("1d", d1_df))
    five = feature_ctx("5m", df5.head(2000))
    out = state.attach(five)
    want = attach_table(attach_table(five.frame, d1_bias_table(add_features(d1_df, SmcParams(), interval_ms=D1), D1), D1_AVAIL),
                        h1_table(add_features(h1_df, SmcParams(), interval_ms=H1), H1), H1_AVAIL)  # порядок присоединения как в MtfState: сначала 1d, затем 1h
    assert out.frame.columns == want.columns and out.frame.equals(want)
    assert BIAS_COLUMN not in out.frame.columns and D1_BIAS in out.frame.columns and out.interval == "5m" and out.symbol == SYM
    assert state.has(SYM) is False  # 4h для 15m-стратегий ещё не пришёл


def test_mtf_state_fails_fast_without_state_or_with_lagging_1h_or_1d():
    df5 = walk5(3000, 5)
    state = MtfState(INTERVALS, MemoryEventLog())
    five = feature_ctx("5m", df5.head(2000))
    with pytest.raises(DataError, match="нет данных 1h"):
        state.attach(five)
    state.update(feature_ctx("1h", resample(df5, H1).head(40)))  # 1h-история кончилась за много часов до конца 5m-истории
    with pytest.raises(DataError, match="нет данных 1D"):
        state.attach(five)
    state.update(feature_ctx("1d", daily().head(40)))  # 1d кончились за ~месяц до 5m
    with pytest.raises(DataError, match="1D-данные отстают"):
        state.attach(five)
    state.update(feature_ctx("1d", daily()))
    with pytest.raises(DataError, match="1h-данные отстают"):
        state.attach(five)


def test_mtf_state_routes_by_interval_and_rejects_foreign_contexts():
    state = MtfState(INTERVALS, MemoryEventLog())
    assert set(state.state_intervals) == {"1h", "1d", "4h"}
    with pytest.raises(DataError, match="не относится"):
        state.update(mk_ctx("15m", 0))
    with pytest.raises(DataError, match="не настроен"):
        state.attach(mk_ctx("1h", 0))
    only_legacy = MtfState(INTERVALS, MemoryEventLog(), base=None)
    assert only_legacy.state_intervals == ("4h",)
    with pytest.raises(DataError, match="не относится"):
        only_legacy.update(mk_ctx("1h", 0))
    with pytest.raises(DataError, match="не настроен"):
        only_legacy.attach(mk_ctx("5m", 0))
    with pytest.raises(ConfigError):
        MtfState(INTERVALS, MemoryEventLog(), base=None, legacy=None)
    with pytest.raises(ConfigError, match="нет длительности"):
        MtfState({"5m": M5}, MemoryEventLog())


def test_mtf_state_attaches_h4_bias_to_15m_contexts_like_htf_bias():
    h4_ms = INTERVALS["4h"]
    h4_df = random_walk(120, 3, 0.02).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * h4_ms + BASE).alias("open_time"))
    state = MtfState(INTERVALS, MemoryEventLog())
    state.update(feature_ctx("4h", h4_df))
    assert state.has(SYM) is False  # MTF включён, а 1h/1d ещё нет
    m15 = random_walk(40, 9, 0.01).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * M15 + BASE + 120 * h4_ms - 28 * M15).alias("open_time"))  # последняя 15m-свеча - через 11 свечей после закрытия последней 4h
    out = state.attach(feature_ctx("15m", m15))
    assert BIAS_COLUMN in out.frame.columns and D1_BIAS not in out.frame.columns  # 15m-стратегии читают прежний htf_bias


def test_the_1d_bias_event_is_logged_with_its_interval():
    log = MemoryEventLog()
    state = MtfState(INTERVALS, log)
    state.update(feature_ctx("1d", daily()))
    events = [e for e in log.events if e.kind == "htf_bias"]
    assert len(events) == 1 and events[0].detail["interval"] == "1d" and events[0].strategy == "htf"


def test_shadow_run_wires_mtf_state_and_feeds_in_the_documented_order():
    import inspect

    from scripts import shadow_run

    src = inspect.getsource(shadow_run.shadow_main)
    assert "MtfState(" in src and "m15_view(" in src and "h1_view(" in src and "d1_view(" in src and "emit=mtf" in src and "emit_latest_on_start=True" in src
    assert src.index("m15_view(") < src.index("mt5_feed])")  # производный 15m раньше первичного M5: M5 последним забирает снимок цикла
    assert D.MTF_ENABLED and D.MTF_BASE_INTERVAL == "5m" and D.MTF_STRUCT_INTERVAL == "1h" and D.MTF_BIAS_INTERVAL == "1d"
    assert all(i in config.INTERVAL_MS for i in (D.MTF_BASE_INTERVAL, D.MTF_STRUCT_INTERVAL, D.MTF_BIAS_INTERVAL))


# ------------------------------------------------------------------ VWAP_Reversion на 5m вместе с SMC_Pro_Trend (2026-10-15)


def test_registry_puts_vwap_on_5m_next_to_pro_trend_with_equivalent_calendar_windows():
    from orchestrator.multi_strategies import VWAP_REVERSION

    built = {s.name: s for s in build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")}
    vwap = built[VWAP_REVERSION]
    assert vwap.interval == "5m" and (vwap.params.min_day_bars, vwap.params.touch_lookback) == (48, 9)  # 4 часа и 45 минут - как 16 и 3 бара M15
    assert built[SMC_PRO_TREND].interval == vwap.interval == "5m"
    assert all(built[n].interval == "15m" for n in built if n not in (SMC_PRO_TREND, VWAP_REVERSION))  # остальные - без изменений
    legacy = {s.name: s for s in build_strategies(SHADOW_STRATEGIES, interval="15m")}[VWAP_REVERSION]  # MTF выключен: VWAP остаётся на 15m со старыми окнами
    assert legacy.interval == "15m" and (legacy.params.min_day_bars, legacy.params.touch_lookback) == (16, 3)
    assert {s.name: s for s in build_strategies(SHADOW_STRATEGIES)}[VWAP_REVERSION].interval is None  # старый вызов без таймфрейма


def test_approved_strategies_pro_trend_and_vwap_share_one_5m_clock_for_the_live_engine():
    from orchestrator.registry import approved_interval

    strategies = build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")
    assert approved_interval(strategies, ("SMC_Pro_Trend", "VWAP_Reversion"), "15m") == "5m"
    assert approved_interval(strategies, ("SMC_Pro_Trend",), "15m") == "5m" and approved_interval(strategies, ("OBI_Breakout",), "15m") == "15m"
    with pytest.raises(ConfigError, match="нужен ровно один"):
        approved_interval(strategies, ("SMC_Pro_Trend", "OBI_Breakout"), "15m")  # 5m + 15m: единый таймфрейм не получится
    with pytest.raises(ConfigError):
        approved_interval(strategies, (), "15m")
    old = build_strategies(SHADOW_STRATEGIES)  # без таймфрейма стратегии считаются работающими на запасном
    assert approved_interval(old, ("SMC_Pro_Trend", "VWAP_Reversion"), "15m") == "15m"


def test_orchestrator_runs_the_real_vwap_strategy_on_the_5m_clock_only():
    from orchestrator.multi_strategies import VWAP_REVERSION
    from tests.strategies.test_multi_strategies import vwap_frame

    (vwap,) = build_strategies((VWAP_REVERSION,), mtf=True, interval="15m")
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0)
    orch = Orchestrator([vwap], {VWAP_REVERSION: Route(Mode.SHADOW, tracker)}, log, max_signal_age_ms=10**12)
    # кадр >= суток (288 баров): полночь на строке 240, к строке 301 в новых сутках накоплено 61 бар; касание 3 sd на строке 300, поглощение на 301
    frame = vwap_frame(n=320, at=300, offset=48).head(302)
    for end in range(295, 302):
        orch.on_context(FeatureContext(SYM, "5m", M5, frame.head(end), end * M5 + 1000))
    events = orch.on_context(FeatureContext(SYM, "5m", M5, frame, 302 * M5 + 1000))
    assert [e.kind for e in events] == ["signal_accepted"] and events[0].strategy == VWAP_REVERSION
    (trade,) = tracker.active()
    assert trade.signal.interval == "5m" and trade.signal.signal_id.startswith("VWAP_Reversion:BTCUSDT:5m:")
    assert orch.on_context(mk_ctx("15m", 0)) == ()  # 15m-свеча стратегию не запускает (в кадре нет колонок: вызов детектора упал бы)


def test_vwap_history_guard_fails_loudly_at_startup_and_at_runtime():
    from orchestrator.multi_strategies import VWAP_REVERSION
    from orchestrator.registry import ensure_history_depth
    from strategies.vwap_reversion import M5_MS, min_history_bars
    from tests.strategies.test_multi_strategies import vwap_frame

    assert M5_MS == config.INTERVAL_MS["5m"] and min_history_bars(M5) == 300 and min_history_bars(M15) == 108  # сутки + запас 12 баров
    five = build_strategies(SHADOW_STRATEGIES, mtf=True, interval="15m")
    ensure_history_depth(five, 300, "15m")  # граница: ровно 300 - можно
    for bad in (299, 150, 100):
        with pytest.raises(ConfigError, match="--history >= 300"):
            ensure_history_depth(five, bad, "15m")
    legacy = build_strategies(SHADOW_STRATEGIES, interval="15m")  # MTF выключен: VWAP на 15m, хватает 108
    ensure_history_depth(legacy, 108, "15m")
    with pytest.raises(ConfigError, match="15m"):
        ensure_history_depth(legacy, 107, "15m")
    ensure_history_depth([s for s in five if s.name != VWAP_REVERSION], 100, "15m")  # без VWAP глубина не проверяется
    (vwap,) = build_strategies((VWAP_REVERSION,), mtf=True, interval="15m")
    short = vwap_frame(n=287, at=270, offset=0)  # 287 свечей: целых суток нет
    with pytest.raises(DataError, match="целые сутки"):
        vwap.detect(FeatureContext(SYM, "5m", M5, short, 1))
    assert vwap.detect(FeatureContext(SYM, "5m", M5, vwap_frame(n=288, at=270), 1)) == ()  # 288: ровно сутки - проходит (сигнала на последней строке нет)
    import inspect

    from scripts import shadow_run

    assert "ensure_history_depth(strategies, opts.history, opts.interval)" in inspect.getsource(shadow_run.shadow_main)
