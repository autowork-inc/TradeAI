"""Запись в рекордер на уровне Оркестратора и цикла Shadow: снапшот входа, состояния автомата сетапа, отказ писателя БД, охрана пакета."""

import asyncio
import re
from pathlib import Path

import polars as pl
import pytest

import config
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.strategies import StrategySMC
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Direction, FeatureContext, Mode
from persistence import schema
from persistence.pg import to_args
from persistence.recorder import MemoryRecorder, RecordingEventLog
from scripts.shadow_run import ShadowRun, shadow_main, Options
from tests.orchestrator.helpers import IV, Scripted, ctx, mk
from tests.orchestrator.test_shadow_run import FakeFeed, scenario_frame
from tests.orchestrator.test_strategies import context
from utils.errors import ConfigError, TradingInfraError

ROOT = Path(__file__).resolve().parents[2]


def candles_ctx(n: int = 5, upto: int | None = None) -> FeatureContext:
    frame = pl.DataFrame({
        "open_time": [i * IV for i in range(n)], "open": [100.0 + i for i in range(n)], "high": [101.0 + i for i in range(n)],
        "low": [99.0 + i for i in range(n)], "close": [100.5 + i for i in range(n)], "volume": [1.0] * n, "atr": [None] + [1.5] * (n - 1),
    })
    k = upto if upto is not None else n
    return FeatureContext("BTCUSDT", "15m", IV, frame.head(k), k * IV + 1000)


def stack(by_bar):
    rec = MemoryRecorder()
    log = RecordingEventLog(MemoryEventLog(), rec)
    tracker = VirtualTracker(log, equity=10_000.0, recorder=rec)
    orch = Orchestrator([Scripted("smc", by_bar)], {"smc": Route(Mode.SHADOW, tracker)}, log, recorder=rec, snapshot_bars=3)
    return orch, tracker, rec


def test_accepted_signal_gets_a_snapshot_with_candles_and_geometry_after_the_trade_row():
    orch, tracker, rec = stack({2: [mk(bar=2)]})
    for k in (1, 2, 3):
        orch.on_context(candles_ctx(5, k))
    (snap,) = rec.snapshots
    assert snap["trade_id"] == "smc:BTCUSDT:15m:2:short" and snap["created_at"] == 3 * IV + 1000
    assert [c["open_time"] for c in snap["candles"]] == [0, IV, 2 * IV] and snap["candles"][1]["atr"] == 1.5 and snap["candles"][0]["atr"] is None
    assert set(snap["candles"][0]) == {"open_time", "open", "high", "low", "close", "volume", "atr"}
    assert snap["meta"]["sl"] == 105.0 and snap["meta"]["tp"] == 85.0 and snap["geometry"] == []
    to_args(snap, schema.SNAPSHOT_COLUMNS)
    assert rec.trades[snap["trade_id"]]["status"] == "pending_limit"  # строка сделки записана раньше снапшота (внешний ключ)


def test_skipped_signals_get_no_snapshot():
    orch, tracker, rec = stack({2: [mk(bar=2), mk(bar=2, direction=Direction.LONG)]})
    for k in (1, 2, 3):
        orch.on_context(candles_ctx(5, k))
    assert len(rec.snapshots) == 1 and [e["kind"] for e in rec.state_events if e["kind"].startswith("skipped")] == ["skipped_due_to_lock"]


def test_snapshot_without_feature_columns_falls_back_to_available_ones():
    c = ctx("BTCUSDT", 2, 100.0)  # кадр только с open_time и close
    assert c.snapshot_candles(10) == [{"open_time": 2 * IV, "close": 100.0}] and c.bar.high is None and c.bar.atr is None


def test_setup_state_transitions_of_the_smc_automaton_are_journaled_once_per_change():
    frame = scenario_frame()
    rec = MemoryRecorder()
    log = RecordingEventLog(MemoryEventLog(), rec)
    tracker = VirtualTracker(log, equity=10_000.0, recorder=rec)
    orch = Orchestrator([StrategySMC()], {StrategySMC().name: Route(Mode.SHADOW, tracker)}, log, recorder=rec)
    for i in range(19):
        orch.on_context(context(frame, i + 1))
    transitions = [(e["detail"]["side"], e["detail"]["from"], e["detail"]["to"]) for e in rec.state_events if e["kind"] == "setup_state"]
    shorts = [(a, b) for s, a, b in transitions if s == "short"]
    assert shorts and shorts[0][0] == 0 and shorts[-1][1] != shorts[-1][0] and any(b == 4 for _, b in shorts)
    assert all(a != b for _, a, b in transitions)  # без повторов: только изменения
    first = next(e for e in rec.state_events if e["kind"] == "setup_state")
    assert first["from_state"].startswith("short:S") and first["signal_id"] is None and first["strategy"] == "SMC_Pro_Trend"
    for e in rec.state_events:
        to_args(e, schema.STATE_EVENT_COLUMNS)


def test_strategy_analyze_is_the_same_computation_as_detect():
    frame = scenario_frame()
    s = StrategySMC()
    for i in (10, 17, 18, 19):
        c = context(frame, i + 1)
        signals, states = s.analyze(c)
        assert signals == s.detect(c) and set(states) == {"short", "long"} and all(0 <= v["state"] <= 4 for v in states.values())


class DeadRecorder:
    def check(self):
        raise TradingInfraError("писатель БД упал")


def test_shadow_loop_stops_when_the_database_writer_dies_r1(tmp_path):
    orch, tracker, _ = stack({})
    run = ShadowRun(FakeFeed([]), orch, tracker, None, heartbeat=tmp_path / ".hb", recorder=DeadRecorder())
    with pytest.raises(TradingInfraError, match="писатель"):
        asyncio.run(run.tick())
    assert not (tmp_path / ".hb").exists()  # пульс не пишется: healthcheck контейнера увидит зависание


def test_shadow_main_rejects_a_malformed_database_url_before_any_network(monkeypatch):
    monkeypatch.setattr(config, "DATABASE_URL", "mysql://u:p@h/x")
    monkeypatch.setattr(config, "IS_TESTNET", False)
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        asyncio.run(shadow_main(Options("15m", 10_000.0, 300, False)))


def test_persistence_package_does_not_suppress_errors_r1():
    for path in (ROOT / "persistence").glob("*.py"):
        code = "\n".join(line.split("#")[0] for line in path.read_text(encoding="utf-8").splitlines())
        assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", code), path.name
    tracker = (ROOT / "orchestrator" / "tracker.py").read_text(encoding="utf-8")
    assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", tracker)
