"""Торговля только по тренду дневки (2026-10-26, решение пользователя): тренд = структура SMC на 1d (HH+HL Bullish / LH+LL Bearish, иначе Neutral).

Проверяется: (1) 15m-кадр получает дневной тренд в `htf_bias` (фильтры стратегий работают по 1d) и 4h-структуру отдельной колонкой; (2) Оркестратор
отсекает любой сигнал против дневки, Neutral и «нет данных» - у ВСЕХ стратегий, даже без своего фильтра; (3) SMC_Counter_Trend берёт откат против 4h,
а не против дневки; (4) без заглядывания: дневная свеча влияет на рабочую только после своего закрытия; (5) боевой процесс и Playground включают фильтр."""

import inspect

import polars as pl
import pytest

import config
import playground.engine as pg_engine
import scripts.shadow_run as shadow_run
from features import defaults as D
from features.htf import BIAS_COLUMN, H4_BIAS
from features.mtf import D1_BIAS
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.mtf import MtfState
from orchestrator.multi_strategies import StrategyCounterTrend
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Direction, FeatureContext, Kind, Mode
from tests.orchestrator.helpers import IV, SYM, Scripted, mk
from utils.errors import DataError

D1, H4 = config.INTERVAL_MS["1d"], config.INTERVAL_MS["4h"]
BULL, BEAR, NEUT = (110.0, 105.0, 100.0, 95.0), (100.0, 105.0, 90.0, 95.0), (110.0, 105.0, 90.0, 95.0)  # sh1, sh2, sl1, sl2


def state_ctx(tf: str, ms: int, structure: tuple, opens: list[int]) -> FeatureContext:
    sh1, sh2, sl1, sl2 = structure
    n = len(opens)
    frame = pl.DataFrame({"open_time": opens, "atr": [1.0] * n, "sh1_price": [sh1] * n, "sh2_price": [sh2] * n, "sl1_price": [sl1] * n, "sl2_price": [sl2] * n},
                         schema={"open_time": pl.Int64, "atr": pl.Float64, **{c: pl.Float64 for c in ("sh1_price", "sh2_price", "sl1_price", "sl2_price")}})
    return FeatureContext(SYM, tf, ms, frame, 1)


def work_ctx(open_time: int, close: float = 100.0) -> FeatureContext:
    frame = pl.DataFrame({"open_time": [open_time - IV, open_time], "close": [close, close]}, schema={"open_time": pl.Int64, "close": pl.Float64})
    return FeatureContext(SYM, "15m", IV, frame, open_time + IV + 1000)


def legacy_state(daily: tuple, h4: tuple, day0: int = 10 * D1) -> MtfState:
    """Только 15m-группа (без SMC_Pro_Trend): дневка закрылась в day0, 4h - в day0 тоже."""
    st = MtfState(config.INTERVAL_MS, base=None, legacy="15m", legacy_bias="4h")
    st.update(state_ctx("1d", D1, daily, [day0 - 2 * D1, day0 - D1]))
    st.update(state_ctx("4h", H4, h4, [day0 - 2 * H4, day0 - H4]))
    return st


# ------------------------------------------------------------------------------------------ MtfState


def test_daily_trend_feed_is_needed_even_without_smc_pro_trend():
    st = MtfState(config.INTERVAL_MS, base=None, legacy="15m", legacy_bias="4h")
    assert "1d" in st.state_intervals and "4h" in st.state_intervals and "1h" not in st.state_intervals
    full = MtfState(config.INTERVAL_MS)
    assert full.state_intervals.count("1d") == 1 and set(full.state_intervals) == {"1h", "1d", "4h"}


def test_15m_frame_gets_daily_trend_for_strategies_and_4h_structure_separately():
    st = legacy_state(BULL, BEAR)
    out = st.attach(work_ctx(10 * D1 + 4 * IV))
    assert out.frame[D1_BIAS][-1] == 1 and out.frame[BIAS_COLUMN][-1] == 1  # фильтры стратегий (htf_bias) - по дневке
    assert out.frame[H4_BIAS][-1] == -1  # 4h-структура - только для SMC_Counter_Trend
    assert st.current(SYM) == 1


def test_daily_candle_counts_only_after_it_closes():
    """Без заглядывания: рабочая свеча, открытая до закрытия дневки, видит прошлый день."""
    st = MtfState(config.INTERVAL_MS, base=None, legacy="15m", legacy_bias="4h")
    day0 = 10 * D1
    # вчерашняя дневка (закрылась в day0) - Bearish; сегодняшняя (закроется в day0 + D1) - Bullish
    bear = state_ctx("1d", D1, BEAR, [day0 - D1])
    bull = state_ctx("1d", D1, BULL, [day0])
    frame = pl.concat([bear.frame, bull.frame])
    st.update(FeatureContext(SYM, "1d", D1, frame.with_columns(
        pl.Series("sh1_price", [BEAR[0], BULL[0]]), pl.Series("sh2_price", [BEAR[1], BULL[1]]),
        pl.Series("sl1_price", [BEAR[2], BULL[2]]), pl.Series("sl2_price", [BEAR[3], BULL[3]])), 1))
    st.update(state_ctx("4h", H4, NEUT, [day0 + D1 - 2 * H4, day0 + D1 - H4]))
    before = st.attach(work_ctx(day0 + D1 - IV))  # последняя 15m дня: дневка дня ещё не закрыта
    after = st.attach(work_ctx(day0 + D1))  # первая 15m следующих суток: дневка закрылась
    assert before.frame[D1_BIAS][-1] == -1 and after.frame[D1_BIAS][-1] == 1


def test_missing_daily_state_is_an_error_not_a_silent_pass():
    st = MtfState(config.INTERVAL_MS, base=None, legacy="15m", legacy_bias="4h")
    st.update(state_ctx("4h", H4, BULL, [10 * D1 - H4]))
    assert not st.has(SYM)
    with pytest.raises(DataError):
        st.attach(work_ctx(10 * D1))


# ------------------------------------------------------------------------------------------ Оркестратор: только по тренду


def orch_with(signal, trend_column=D1_BIAS):
    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0)
    strat = Scripted(signal.strategy, {signal.bar_open_time // IV: [signal]})
    return Orchestrator([strat], {strat.name: Route(Mode.SHADOW, tracker)}, log, trend_column=trend_column), log


def trend_frame_ctx(bar: int, daily) -> FeatureContext:
    frame = pl.DataFrame({"open_time": [bar * IV], "close": [100.0], D1_BIAS: [daily]}, schema={"open_time": pl.Int64, "close": pl.Float64, D1_BIAS: pl.Int8})
    return FeatureContext(SYM, "15m", IV, frame, (bar + 1) * IV + 1000)


@pytest.mark.parametrize("direction,daily,passes", [
    (Direction.LONG, 1, True), (Direction.SHORT, -1, True),
    (Direction.LONG, -1, False), (Direction.SHORT, 1, False),
    (Direction.LONG, 0, False), (Direction.SHORT, 0, False),
    (Direction.LONG, None, False), (Direction.SHORT, None, False),
])
def test_every_signal_must_follow_the_daily_trend(direction, daily, passes):
    sig = mk(strategy="OBI_Breakout", direction=direction, kind=Kind.MARKET, bar=5)  # у OBI своего фильтра тренда нет - отсекает Оркестратор
    orch, log = orch_with(sig)
    orch.on_context(trend_frame_ctx(5, daily))
    kinds = [e.kind for e in log.events if e.kind in ("signal_accepted", "skipped_filter")]
    if passes:
        assert kinds == ["signal_accepted"]
    else:
        ev = next(e for e in log.events if e.kind == "skipped_filter")
        assert kinds == ["skipped_filter"] and ev.detail["filter"] == "d1_trend" and "тренд 1d" in ev.detail["reason"]


def test_trend_filter_without_daily_column_fails_loudly():
    sig = mk(strategy="X", direction=Direction.LONG, kind=Kind.MARKET, bar=5)
    orch, _ = orch_with(sig)
    frame = pl.DataFrame({"open_time": [5 * IV], "close": [100.0]})
    with pytest.raises(DataError):
        orch.on_context(FeatureContext(SYM, "15m", IV, frame, 6 * IV + 1000))


def test_trend_filter_off_keeps_old_behaviour_for_unit_tests():
    sig = mk(strategy="X", direction=Direction.LONG, kind=Kind.MARKET, bar=5)
    orch, log = orch_with(sig, trend_column=None)
    orch.on_context(trend_frame_ctx(5, -1))
    assert any(e.kind == "signal_accepted" for e in log.events)


# ------------------------------------------------------------------------------------------ SMC_Counter_Trend: откат против 4h внутри дневного тренда


def test_counter_trend_reads_4h_structure_not_the_daily_trend(monkeypatch):
    seen = {}

    def fake_detect(frame, params):
        seen["bias"] = frame[BIAS_COLUMN].to_list()
        return frame.with_columns(*(pl.lit(False).alias(f"ct_{s}_signal") for s in ("short", "long")))

    monkeypatch.setattr("orchestrator.multi_strategies.detect_counter_trend", fake_detect)
    frame = pl.DataFrame({"open_time": [0, IV], "close": [1.0, 1.0], BIAS_COLUMN: [1, 1], D1_BIAS: [1, 1], H4_BIAS: [-1, -1]},
                         schema={"open_time": pl.Int64, "close": pl.Float64, BIAS_COLUMN: pl.Int8, D1_BIAS: pl.Int8, H4_BIAS: pl.Int8})
    assert StrategyCounterTrend().detect(FeatureContext(SYM, "15m", IV, frame, 2 * IV)) == ()
    assert seen["bias"] == [-1, -1]  # детектору дана 4h-структура: лонг-откат против медвежьей 4h; дневка Bullish проверяется Оркестратором


# ------------------------------------------------------------------------------------------ настройки и подключение


def test_defaults_trade_only_with_the_daily_trend():
    assert D.TREND_ONLY is True and D.TREND_INTERVAL == "1d" == D.MTF_BIAS_INTERVAL


def test_live_process_and_playground_turn_the_filter_on():
    src = inspect.getsource(shadow_run)
    assert "trend_column=D1_BIAS if D.TREND_ONLY else None" in src
    assert "interval=D.MTF_BIAS_INTERVAL" in src.split("# дневка - всегда")[1][:400]  # 1d-фид поднимается и без SMC_Pro_Trend
    assert "trend_column=D1_BIAS if D.TREND_ONLY else None" in inspect.getsource(pg_engine)
