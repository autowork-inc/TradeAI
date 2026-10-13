"""scripts/smc_live_run.py: охранные условия запуска, цикл, диспетчер приватного потока, синхронизация после подключения."""

import asyncio
import json
import re
from pathlib import Path

import pytest

import config
from execution.reconcile import DiscrepancyKind
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, RawEvent, parse_event
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import MemoryEventLog
from orchestrator.live_route import LiveBarrierSink, LiveTradeStore
from orchestrator.types import Direction, Kind, Mode
from scripts import smc_live_run as srun
from tests.execution.fake_algo import FakeAlgoExchange
from tests.execution.helpers import make_env
from tests.orchestrator.helpers import IV, Scripted, ctx, mk
from utils.errors import ConfigError, SafeModeActive, StreamStalled

SYM = "BTCUSDT"


def run(coro):
    return asyncio.run(coro)


class FakeFeed:
    def __init__(self, batches):
        self.batches = list(batches)
        self.polls = 0

    async def poll(self):
        self.polls += 1
        return self.batches.pop(0) if self.batches else []


class FakeTicks:
    def __init__(self):
        self.checked = 0

    def check(self):
        self.checked += 1


def algo_env(tmp_path):
    return make_env(tmp_path, exchange=FakeAlgoExchange(60_000.0))


async def build(e, tmp_path, feed, ticks=None, heartbeat=None):
    journal = MemoryEventLog()

    async def equity() -> float:
        return 10_000.0

    sink = LiveBarrierSink(e.barrier, e.safe, journal, LiveTradeStore(tmp_path / "t.json"), equity, e.rest.now_ms, interval_ms=IV, symbols=(SYM,))
    scripted = Scripted("smc", {})
    orch = Orchestrator([scripted], {"smc": Route(Mode.LIVE, sink)}, journal)
    runner = srun.SmcLiveRun(rest=e.rest, barrier=e.barrier, state=e.state, store=e.store, safe=e.safe, sink=sink, orchestrator=orch,
                             feed=feed, ticks=ticks, heartbeat=heartbeat)
    return runner, sink, scripted, journal


def test_environment_guards(monkeypatch, tmp_path):
    opts = srun.Options()
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "live_smc")
    monkeypatch.setattr(config, "IS_TESTNET", False)
    with pytest.raises(ConfigError, match="allow-mainnet"):
        srun.guard_environment(opts)
    srun.guard_environment(srun.Options(allow_mainnet=True))
    monkeypatch.setattr(config, "IS_TESTNET", True)
    srun.guard_environment(opts)
    monkeypatch.setattr(config, "DATA_DIR", config.BASE_DIR / "data")  # каталог боевого ML-контура
    with pytest.raises(ConfigError, match="TRADING_DATA_DIR"):
        srun.guard_environment(opts)


def test_arguments_and_strategies():
    assert srun.parse_args([]) == srun.Options("15m", 300, False, False)
    opts = srun.parse_args(["--interval", "1h", "--with-breakout", "--allow-mainnet"])
    assert opts == srun.Options("1h", 300, True, True)
    assert [s.name for s in srun.build_strategies(srun.Options())] == ["smc"]
    assert [s.name for s in srun.build_strategies(opts)] == ["smc", "breakout"]
    with pytest.raises(ConfigError):
        srun.parse_args(["--interval", "5m"])  # код выхода 2 у нас - «бан биржи», поэтому ошибка аргументов - ConfigError


def test_tick_feeds_candles_to_the_orchestrator_and_writes_heartbeat(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            hb = tmp_path / "hb"
            ticks = FakeTicks()
            feed = FakeFeed([[ctx(SYM, 100, 60_000.0), ctx(SYM, 101, 60_100.0)], []])
            runner, sink, scripted, journal = await build(e, tmp_path, feed, ticks, hb)
            n1, n2 = await runner.tick(), await runner.tick()
            return n1, n2, scripted.calls, ticks.checked, hb.exists()

    n1, n2, calls, checked, hb = run(body())
    assert (n1, n2, calls, checked, hb) == (2, 0, 2, 2, True)


def test_safe_mode_stops_the_loop(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            runner, *_ = await build(e, tmp_path, FakeFeed([]))
            e.safe.trip("тест")
            with pytest.raises(SafeModeActive):
                await runner.tick()

    run(body())


def test_stream_events_are_dispatched_to_the_barrier(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            runner, *_ = await build(e, tmp_path, FakeFeed([]))
            seen = []
            e.barrier.on_order_update = lambda u: seen.append(("order", u.client_order_id))
            e.barrier.on_account_update = lambda u: seen.append(("account", u.reason))
            e.barrier.on_algo_update = lambda u: seen.append(("algo", u.client_algo_id))
            o = e.ex.seed_order("c1")
            a = e.ex.seed_algo("a1")
            for payload in (e.ex.order_event(o), e.ex.account_event(SYM, 0.0), e.ex.algo_event(a, "NEW"), {"e": "TRADE_LITE", "x": 1}):
                runner.on_stream_event(parse_event(json.dumps(payload)))
            assert not e.safe.active
            runner.on_stream_event(RawEvent("MARGIN_CALL", {"e": "MARGIN_CALL"}))
            return seen, e.safe.active

    seen, tripped = run(body())
    assert seen == [("order", "c1"), ("account", "ORDER"), ("algo", "a1")] and tripped


def test_after_connect_resync_runs_and_reconnect_adds_full_reconcile(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            runner, *_ = await build(e, tmp_path, FakeFeed([]))
            await runner.on_connected(False)
            first = (runner.connected.is_set(), [r for r in e.ex.requests if "positionRisk" in r])
            await runner.on_connected(True)  # сверка после обрыва: чисто
            clean_requests = [r for r in e.ex.requests if "positionRisk" in r]
            e.ex.seed_position(SYM, 0.3)  # пока поток был оборван, на бирже появилась позиция, о которой бот не знает
            with pytest.raises(SafeModeActive, match="Сверка"):
                await runner.on_connected(True)
            return first, clean_requests, e.safe.active

    first, clean_requests, tripped = run(body())
    assert first == (True, []) and len(clean_requests) == 1 and tripped  # первое подключение сверку не делает (её сделал старт)


def test_connect_resync_timeout_is_a_stream_stall(tmp_path, monkeypatch):
    async def body():
        async with algo_env(tmp_path) as e:
            runner, sink, *_ = await build(e, tmp_path, FakeFeed([]))
            monkeypatch.setattr(config, "LIVE_RESYNC_TIMEOUT_S", 0.05)
            await sink.lock.acquire()  # исполнитель завис с блокировкой: синхронизация не может начаться
            with pytest.raises(StreamStalled):
                await runner.on_connected(False)

    run(body())


def test_runner_module_does_not_suppress_errors_r1():
    src = Path("scripts/smc_live_run.py").read_text(encoding="utf-8")
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    assert not re.search(r"except\s*:|except\s+(Base)?Exception\b|suppress\(", code)
