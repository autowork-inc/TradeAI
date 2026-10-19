"""Цикл Shadow Mode (scripts/shadow_run.py): сквозной сценарий на настоящих стратегиях, пульс, настройки, запрет подавления ошибок (R1)."""

import asyncio
import json
import re
from pathlib import Path

import polars as pl
import pytest

import config
from orchestrator.core import Orchestrator, Route
from orchestrator.journal import JsonlEventLog
from orchestrator.strategies import StrategyBreakout, StrategySMC
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Mode
from scripts.shadow_run import Options, ShadowRun, parse_args, shadow_main
from tests.orchestrator.helpers import tick
from tests.orchestrator.test_strategies import context
from tests.strategies.test_smc_setups import short_scenario
from utils.errors import ConfigError, WebSocketClosed

ROOT = Path(__file__).resolve().parents[2]


class FakeFeed:
    def __init__(self, batches):
        self.batches = list(batches)

    async def poll(self):
        return self.batches.pop(0) if self.batches else []


class FakeTicks:
    def __init__(self, error=None):
        self.error = error

    def check(self):
        if self.error:
            raise self.error


def scenario_frame() -> pl.DataFrame:
    """Сценарий SMC-шорта + нейтральные колонки Breakout (поджатия нет), чтобы работали обе стратегии."""
    return short_scenario().with_columns(
        volume=pl.lit(10.0), buildup=pl.lit(False), buildup_level=pl.lit(None, dtype=pl.Float64),
        buildup_low3=pl.lit(None, dtype=pl.Float64), vol_ratio=pl.lit(1.0),
    )


def stack(tmp_path, batches, ticks=None):
    log = JsonlEventLog(tmp_path / "shadow_events.jsonl")
    tracker = VirtualTracker(log, equity=10_000.0)
    strategies = [StrategySMC(), StrategyBreakout()]
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, log)
    run = ShadowRun(FakeFeed(batches), orch, tracker, ticks, heartbeat=tmp_path / ".hb")
    return run, tracker, log


def test_end_to_end_real_strategies_ticks_and_journal(tmp_path):
    frame = scenario_frame()
    batches = [[context(frame, i + 1)] for i in range(20)]  # свечи 0..19; сигнал SMC-шорта на свече 18
    run, tracker, log = stack(tmp_path, batches)

    async def body():
        for _ in range(19):
            await run.tick()  # свечи 0..18
        (trade,) = tracker.active()
        assert trade.signal.strategy == "SMC_Pro_Trend" and trade.signal.entry == 105.0 and trade.status == "pending_limit"
        tracker.on_tick(tick(105.5, 1))  # цена прошла лимитку 105 -> исполнена
        tracker.on_tick(tick(97.7, 2, "mark"))  # R = 7.2, +1R = 97.8: безубытка нет, стоп не двигается
        assert trade.fill_price == 105.0 and not trade.be_armed
        tracker.on_tick(tick(105.3, 3))  # возврат к входу: позиция жива
        assert tracker.active() == [trade]
        tracker.on_tick(tick(113.0, 4))  # выход только по исходному SL
        assert trade.exit_reason == "sl" and tracker.active() == []
        await run.tick()  # свеча 19: монета свободна, нового сигнала нет
        return trade

    trade = asyncio.run(body())
    assert (tmp_path / ".hb").read_text().isdigit()  # пульс для healthcheck контейнера
    log.close()
    lines = [json.loads(x) for x in (tmp_path / "shadow_events.jsonl").read_text(encoding="utf-8").splitlines()]
    states = [x for x in lines if x["kind"] == "setup_state"]
    assert states and all(x["detail"]["side"] in ("short", "long") and x["detail"]["from"] != x["detail"]["to"] for x in states)
    assert any(x["detail"]["side"] == "short" and x["detail"]["to"] == 4 for x in states)  # автомат дошёл до S4 «заявка»
    lines = [x for x in lines if x["kind"] != "setup_state"]
    assert [x["kind"] for x in lines] == ["trade_registered", "signal_accepted", "limit_filled", "trade_closed"]
    reg = lines[0]["detail"]
    assert len(reg["geometry"]) == 7 and reg["geometry"][0]["label"] == "FVG"  # геометрия в журнале для будущего клиента
    assert lines[-1]["detail"]["record"]["exit_reason"] == "sl" and trade.net_r == pytest.approx(lines[-1]["detail"]["record"]["net_r"])
    assert tracker.summary()["SMC_Pro_Trend/short"]["sl"] == 1


def test_tick_feed_failure_stops_the_loop_r1(tmp_path):
    run, _, _ = stack(tmp_path, [], FakeTicks(WebSocketClosed("поток тиков упал")))
    with pytest.raises(WebSocketClosed):
        asyncio.run(run.tick())


def test_summary_is_logged_without_errors(tmp_path, caplog):
    run, tracker, _ = stack(tmp_path, [[context(scenario_frame(), i + 1)] for i in range(19)])

    async def body():
        for _ in range(19):
            await run.tick()

    with caplog.at_level("INFO", logger="shadow_run"):
        asyncio.run(body())
        run.log_summary()
    assert any("СВОДКА SMC_Pro_Trend/short" in r.message for r in caplog.records)


def test_options_and_profile_guard(monkeypatch):
    o = parse_args([])
    assert (o.interval, o.equity, o.history, o.allow_testnet) == ("15m", 10_000.0, 300, False)
    assert parse_args(["--interval", "1h", "--equity", "500", "--allow-testnet"]).allow_testnet is True
    monkeypatch.setattr(config, "IS_TESTNET", True)  # цены testnet нереальные: без явного флага не запускаемся
    with pytest.raises(ConfigError, match="mainnet"):
        asyncio.run(shadow_main(Options("15m", 10_000.0, 300, False)))


def test_compose_service_is_isolated_from_live_run():
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    block = text[text.index("  shadow-run:"):]
    block = block[: block.index("\n  funding-downloader:")] if "\n  funding-downloader:" in block else block
    assert 'profiles: ["shadow"]' in block and "env_file" not in block  # не стартует с live-run, ключей API нет
    assert "BINANCE_PROFILE: mainnet" in block and "TRADING_DATA_DIR: /app/data/shadow" in block
    assert "read_only: true" in block and "cap_drop" in block and "./data/shadow:/app/data/shadow" in block
    live = text[text.index("  live-run:"): text.index("  # Фандинг")]
    assert "profiles" not in live and "shadow" not in live.lower() and "env_file: .env" in live  # live-run не затронут
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert all(f"COPY {d} ./{d}" in dockerfile for d in ("features", "strategies", "orchestrator"))


def test_shadow_has_no_api_credentials_and_cannot_sign_requests():
    src = (ROOT / "scripts" / "shadow_run.py").read_text(encoding="utf-8")
    assert not re.search(r"Credentials|SignedRest|BINANCE_API_KEY|BINANCE_API_SECRET|LiveBarrier", src.split('"""', 2)[2])


def test_no_error_suppression_in_strategy_and_orchestrator_code_r1():
    banned = re.compile(r"except\s*:|except\s+(Base)?Exception\b|contextlib\.suppress|\bsuppress\(")
    files = [*(ROOT / "orchestrator").glob("*.py"), *(ROOT / "strategies").glob("*.py"), *(ROOT / "features").glob("*.py"),
             ROOT / "scripts" / "shadow_run.py"]
    assert len(files) > 10
    for f in files:
        code = "\n".join(line.split("#")[0] for line in f.read_text(encoding="utf-8").splitlines())
        assert not banned.search(code), f"{f.name}: подавление ошибок запрещено (R1)"


def test_shadow_loop_attaches_micro_columns_polls_depth_feed_and_fails_with_it():
    """Мульти-стратегийный цикл: перед Оркестратором контекст получает ms_obi / ms_delta; упавший фид стакана роняет цикл (R1)."""
    from orchestrator.microstructure import MicroStructure
    from orchestrator.types import FeatureContext
    from tests.orchestrator.helpers import IV

    class Spy:
        name = "spy"

        def __init__(self):
            self.seen = []

        def detect(self, c):
            self.seen.append(c.frame.columns)
            return ()

    class FakeDepth:
        def __init__(self, error=None):
            self.error, self.checked = error, 0

        def check(self):
            self.checked += 1
            if self.error:
                raise self.error

    from orchestrator.journal import MemoryEventLog

    log = MemoryEventLog()
    tracker = VirtualTracker(log, equity=10_000.0)
    spy = Spy()
    orch = Orchestrator([spy], {"spy": Route(Mode.SHADOW, tracker)}, log)
    micro = MicroStructure(("BTCUSDT",), IV)
    frame = pl.DataFrame({"open_time": [0, IV], "close": [1.0, 1.0]})
    ctx0 = FeatureContext("BTCUSDT", "15m", IV, frame, 2 * IV + 1000)
    depth = FakeDepth()
    run = ShadowRun(FakeFeed([[ctx0]]), orch, tracker, None, micro=micro, depth_feed=depth)
    asyncio.run(run.tick())
    assert spy.seen == [["open_time", "close", "ms_obi", "ms_delta"]] and depth.checked == 1
    failing = ShadowRun(FakeFeed([]), orch, tracker, None, micro=micro, depth_feed=FakeDepth(WebSocketClosed("стакан упал")))
    with pytest.raises(WebSocketClosed):
        asyncio.run(failing.tick())
    with pytest.raises(ConfigError, match="вместе"):
        ShadowRun(FakeFeed([]), orch, tracker, None, micro=micro)  # агрегатор без фида стакана бессмыслен


def test_shadow_main_uses_the_registry_and_wires_micro_and_depth():
    import inspect

    from scripts import shadow_run

    src = inspect.getsource(shadow_run.shadow_main)
    assert "build_strategies(names, mtf=mtf, interval=opts.interval)" in src and "plans_for(names)" in src and "DepthFeed(" in src and "NEEDS_MICRO" in src
    assert "depth_feed.start()" in src and "depth_feed.aclose()" in src and "micro.on_tick(tick)" in src and 'mark_gap("trade")' in src


def test_compose_shadow_run_has_no_cpu_or_memory_limits():
    """2026-10-11 (решение пользователя): сервер 6 ядер / 8 ГБ, жёсткие лимиты shadow-run сняты."""
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    block = text[text.index("  shadow-run:"): text.index("  # API мониторинга")]
    assert "mem_limit" not in block and "cpus:" not in block
