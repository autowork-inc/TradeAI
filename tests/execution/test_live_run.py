"""Оркестрация боевого цикла (scripts/live_run.py) на имитации биржи: порядок старта, Safe Mode, fail-fast."""

import asyncio
import contextlib
import json
import random
from pathlib import Path

import aiohttp
import polars as pl
import pytest

import config
from backtester.contracts import Side
from backtester.ml_strategy import MLModelStrategy, ModelError, RidgeRegressor
from backtester.risk import OrderRequest
from backtester.strategy import BaselineStrategy, FeatureContext, Strategy
from execution.live_barrier import LiveBarrier
from execution.live_feed import KlineObiFeed
from execution.state import LocalState, StateStore
from execution.ws_live_features import WsLiveFeatureFeed
from scripts import live_run as live_module
from scripts.live_run import (
    LiveRun,
    Options,
    build_ml_strategy,
    classify_fill,
    live_main,
    parse_args,
    parse_equity,
    round_qty,
)
from tests.conftest import serve
from tests.execution.fake_exchange import API_KEY, API_SECRET, FakeExchange
from utils import ban_lock
from utils.errors import (
    EXIT_CONFIG,
    EXIT_SAFE_MODE,
    BanActive,
    ConfigError,
    DataError,
    InstanceAlreadyRunning,
    NetworkError,
    SafeModeActive,
    StreamStalled,
    WebSocketClosed,
)
from utils.instance_lock import InstanceLock
from utils.notifier import Notifier


def run(coro):
    return asyncio.run(coro)


class RecordingNotifier(Notifier):
    def __init__(self):
        super().__init__()
        self.events: list[tuple[str, str]] = []

    def notify_nowait(self, text, *, level="info"):
        self.events.append((level, text))

    def texts(self):
        return [t for _, t in self.events]

    def has(self, fragment: str) -> bool:
        return any(fragment in t for t in self.texts())


class ScriptedStrategy(Strategy):
    """Один раз по первой строке отдаёт заданные запросы, дальше молчит."""

    def __init__(self, *requests: OrderRequest):
        self._requests = list(requests)
        self.calls = 0
        self.contexts: list[FeatureContext] = []
        self.events: list = []

    def decide(self, ctx):
        self.calls += 1
        self.contexts.append(ctx)
        out, self._requests = self._requests, []
        return out

    def on_event(self, event):
        self.events.append(event)


class OneShotFeed:
    def __init__(self, price=60_000.0, symbol="BTCUSDT"):
        self._row = {"symbol": symbol, "asof_ts": 1, "close": price}
        self.polls = 0

    async def poll(self, now_ms):
        self.polls += 1
        rows, self._row = ([self._row] if self._row else []), None
        return rows


class FailingFeed:
    def __init__(self, error, *, fail_first=None):
        self.error, self.fail_first, self.polls = error, fail_first, 0

    async def poll(self, now_ms):
        self.polls += 1
        if self.fail_first is None or self.polls <= self.fail_first:
            raise self.error
        return []


def const_equity(*values):
    seq = list(values)

    async def source():
        return seq.pop(0) if len(seq) > 1 else seq[0]

    return source


@contextlib.asynccontextmanager
async def live_env(monkeypatch, tmp_path, *, exchange=None):
    ex = exchange or FakeExchange()
    async with serve(ex.app) as host:
        monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
        monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(config, "SYMBOLS", ("BTCUSDT",))
        monkeypatch.setattr(config, "BINANCE_PROFILE", "testnet")
        monkeypatch.setattr(config, "IS_TESTNET", True)
        monkeypatch.setattr(config, "REST_BASE_URL", f"http://{host}")
        monkeypatch.setattr(config, "WS_PRIVATE_URL", f"ws://{host}/private")
        monkeypatch.setattr(config, "LIVE_LOOP_INTERVAL_S", 0.02)
        monkeypatch.setattr(config, "LIVE_STARTUP_TIMEOUT_S", 3)
        monkeypatch.setattr(config, "LIVE_RESYNC_TIMEOUT_S", 3)
        monkeypatch.setattr(config, "LIVE_RECONCILE_INTERVAL_S", 3600)
        monkeypatch.setattr(config, "USER_STREAM_BACKOFF_S", 0.01)
        monkeypatch.setattr(config, "USER_STREAM_BACKOFF_MAX_S", 0.02)
        monkeypatch.setenv("BINANCE_API_KEY", API_KEY)
        monkeypatch.setenv("BINANCE_API_SECRET", API_SECRET)
        yield ex


def start(opts=None, *, strategy=None, feed=None, equity=None, notifier=None):
    notifier = notifier or RecordingNotifier()
    task = asyncio.create_task(live_main(
        opts or Options(), strategy=strategy or ScriptedStrategy(), feed=feed or OneShotFeed(),
        equity_source=equity or const_equity(10_000.0), notifier=notifier,
    ))
    return task, notifier


async def settle(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("условие не наступило")
        await asyncio.sleep(0.01)


async def stop(task):
    if not task.done():
        task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def posts(ex):
    return [r for r in ex.requests if r.startswith("POST /fapi/v1/order")]


def write_state(tmp_path, positions):
    path = tmp_path / "data" / config.ML_STATE_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    StateStore(path).save(LocalState(positions=positions))


# ------------------------------------------------------------------- шаги 1-3: до первого запроса


def test_active_ban_exits_before_any_request(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ban_lock.record_ban(600, 429, "тест")
            task, _ = start()
            with pytest.raises(BanActive):
                await task
            assert ex.requests == []

    run(go())


def test_second_instance_is_refused(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            with InstanceLock(config.LIVE_INSTANCE_NAME):
                task, _ = start()
                with pytest.raises(InstanceAlreadyRunning):
                    await task
            assert ex.requests == []

    run(go())


def test_safe_mode_latch_stops_start_with_code_4_and_no_requests(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            latch = tmp_path / "data" / config.SAFE_MODE_LOCK_NAME
            latch.parent.mkdir(parents=True)
            latch.write_text(json.dumps({"reason": "прошлый запуск: расхождение"}), encoding="utf-8")
            task, notifier = start()
            with pytest.raises(SafeModeActive) as info:
                await task
            assert info.value.exit_code == EXIT_SAFE_MODE
            assert "прошлый запуск" in str(info.value)
            assert ex.requests == [] and latch.exists()
            assert notifier.has("Старт отклонён") and notifier.has("Позиции по локальному учёту")

    run(go())


def test_mainnet_is_refused_without_explicit_flag(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "IS_TESTNET", False)
            monkeypatch.setattr(config, "BINANCE_PROFILE", "mainnet")
            task, _ = start()
            with pytest.raises(ConfigError, match="allow-mainnet"):
                await task
            assert ex.requests == []

    run(go())


def test_missing_credentials_are_a_config_error(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.delenv("BINANCE_API_KEY")
            task, _ = start()
            with pytest.raises(ConfigError):
                await task
            assert ex.requests == []

    run(go())


def test_lot_step_is_required_for_every_symbol(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(config, "SYMBOLS", ("BTCUSDT", "DOGEUSDT"))
            task, _ = start()
            with pytest.raises(ConfigError, match="DOGEUSDT"):
                await task

    run(go())


def test_initial_discrepancy_sets_safe_mode_and_places_nothing(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.seed_position("BTCUSDT", 0.5)  # на бирже висит позиция, о которой бот не знает
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, notifier = start(strategy=strategy)
            with pytest.raises(SafeModeActive) as info:
                await task
            assert info.value.exit_code == EXIT_SAFE_MODE
            assert (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()  # защёлка на диске
            assert posts(ex) == [] and "POST /fapi/v1/listenKey" not in ex.requests  # до потока дело не дошло
            assert strategy.calls == 0
            assert notifier.has("SAFE MODE") and notifier.has("Выход с кодом 4")

    run(go())


def test_unverifiable_state_is_fail_closed(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.fault("open_orders", "http500")
            task, notifier = start()
            with pytest.raises(SafeModeActive):
                await task
            assert (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()
            assert posts(ex) == [] and notifier.has("SAFE MODE")

    run(go())


def test_rate_limit_during_startup_propagates_for_ban_lock(monkeypatch, tmp_path):
    from utils.errors import RateLimitError

    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.fault("position_risk", "http429")
            task, _ = start()
            with pytest.raises(RateLimitError) as info:
                await task
            assert info.value.retry_after == 45
            assert not (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()  # бан - не Safe Mode

    run(go())


# ---------------------------------------------------------------- шаги 4-5: поток и торговый цикл


def test_startup_sequence_order(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, _ = start()
            await ex.wait_clients(1)
            order = ["GET /fapi/v1/time", "GET /fapi/v3/positionRisk", "GET /fapi/v1/openOrders", "POST /fapi/v1/listenKey"]
            idx = [ex.requests.index(r) for r in order]
            assert idx == sorted(idx)
            await stop(task)
            assert "DELETE /fapi/v1/listenKey" in ex.requests  # при остановке listenKey закрыт

    run(go())


def test_clean_start_trades_through_live_barrier_and_notifies_entry(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, notifier = start(strategy=strategy)
            await settle(lambda: notifier.has("ВХОД BTCUSDT"))
            assert len(ex.post_log) == 1
            p = ex.post_log[0]
            assert p["newClientOrderId"].startswith(config.CLIENT_ORDER_ID_PREFIX + "-")
            assert (p["symbol"], p["side"], p["type"], p["quantity"]) == ("BTCUSDT", "BUY", "MARKET", "0.1")
            assert "reduceOnly" not in p
            assert ex.positions["BTCUSDT"] == pytest.approx(0.1)
            assert notifier.has("Боевой цикл запущен")
            assert any(type(e).__name__ == "Fill" for e in strategy.events)  # стратегия увидела исполнение
            await stop(task)
            saved = StateStore(tmp_path / "data" / config.ML_STATE_NAME).load()
            assert saved.position("BTCUSDT") == pytest.approx(0.1)

    run(go())


def test_closing_order_is_reduce_only_and_exit_is_notified(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.seed_position("BTCUSDT", 0.1)
            write_state(tmp_path, {"BTCUSDT": 0.1})
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.SELL, 0.1))
            task, notifier = start(strategy=strategy)
            await settle(lambda: notifier.has("ВЫХОД BTCUSDT"))
            assert ex.post_log[0]["reduceOnly"] == "true"
            assert ex.positions["BTCUSDT"] == pytest.approx(0.0)
            await stop(task)

    run(go())


def test_quantity_is_rounded_down_to_lot_step(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.12349))
            task, notifier = start(strategy=strategy)
            await settle(lambda: notifier.has("ВХОД"))
            assert ex.post_log[0]["quantity"] == "0.123"
            await stop(task)

    run(go())


def test_risk_guard_clips_oversized_request(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            # эквити 10 000, цена 60 000, потолок 1x -> не больше 0.166 BTC
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 5.0))
            task, notifier = start(strategy=strategy)
            await settle(lambda: notifier.has("ВХОД"))
            assert float(ex.post_log[0]["quantity"]) == pytest.approx(0.166)
            await stop(task)

    run(go())


def test_dry_run_computes_decisions_but_sends_no_orders(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, notifier = start(Options(dry_run=True), strategy=strategy)
            await settle(lambda: strategy.calls >= 1)
            await asyncio.sleep(0.1)
            assert ex.post_log == [] and posts(ex) == []
            assert notifier.has("DRY-RUN") and not notifier.has("ВХОД")
            await stop(task)

    run(go())


def test_strategy_context_has_state_of_the_account(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.seed_position("BTCUSDT", 0.1)
            write_state(tmp_path, {"BTCUSDT": 0.1})
            strategy = ScriptedStrategy()
            task, _ = start(strategy=strategy, equity=const_equity(12_345.0))
            await settle(lambda: strategy.calls >= 1)
            ctx = strategy.contexts[0]
            assert ctx.equity == 12_345.0 and ctx.symbol == "BTCUSDT"
            assert ctx.position == pytest.approx(0.1) and ctx.price == 60_000.0
            await stop(task)

    run(go())


def test_drawdown_guard_trigger_is_notified_and_blocks_new_entries(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "LIVE_MAX_DRAWDOWN", 0.05)
            feed = OneShotFeed()

            async def late_poll(now_ms):  # решение уходит уже ПОСЛЕ просадки
                feed.polls += 1
                return [] if feed.polls < 5 else await OneShotFeed.poll(feed, now_ms)

            feed.poll = late_poll
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, notifier = start(strategy=strategy, feed=feed, equity=const_equity(10_000.0, 9_000.0))
            await settle(lambda: notifier.has("Drawdown Guard"))
            assert notifier.has("10.00%")
            await settle(lambda: strategy.calls >= 1)
            await asyncio.sleep(0.1)
            assert ex.post_log == []  # новая позиция заблокирована гуардом
            await stop(task)

    run(go())


# --------------------------------------------------------------------------------- fail-fast


def test_foreign_fill_mid_run_trips_safe_mode_and_exits_4(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, notifier = start()
            await ex.wait_clients(1)
            ex.seed_order("stop-by-exchange", qty=0.1)
            await ex.fill("stop-by-exchange", 0.1)  # исполнение под чужим clientOrderId: стоп / ликвидация / ручное
            with pytest.raises(SafeModeActive) as info:
                await asyncio.wait_for(task, 5)
            assert info.value.exit_code == EXIT_SAFE_MODE
            assert (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()
            assert notifier.has("SAFE MODE") and notifier.has("Выход с кодом 4")

    run(go())


def test_margin_call_trips_safe_mode(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, notifier = start()
            await ex.wait_clients(1)
            await ex.push({"e": "MARGIN_CALL", "E": 1, "cw": "100", "p": []})
            with pytest.raises(SafeModeActive):
                await asyncio.wait_for(task, 5)
            assert notifier.has("MARGIN_CALL")

    run(go())


def test_stream_that_cannot_reconnect_crashes_the_process(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "USER_STREAM_MAX_RECONNECTS", 2)
            task, notifier = start()
            await ex.wait_clients(1)
            ex.always["listen_key_post"] = "http500"
            await ex.close_clients()
            with pytest.raises(WebSocketClosed):
                await asyncio.wait_for(task, 5)
            assert notifier.has("Аварийный выход")

    run(go())


def test_reconnect_runs_full_reconciliation(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, _ = start()
            await ex.wait_clients(1)
            assert ex.requests.count("GET /fapi/v3/positionRisk") == 1  # стартовая сверка
            await ex.close_clients()
            await settle(lambda: ex.requests.count("GET /fapi/v3/positionRisk") == 2)
            await ex.wait_clients(1)
            assert not task.done()
            await stop(task)

    run(go())


def test_position_changed_during_outage_trips_safe_mode_on_reconnect(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, notifier = start()
            await ex.wait_clients(1)
            ex.seed_position("BTCUSDT", 1.0)  # пока поток лежал, позиция изменилась без нас
            await ex.close_clients()
            with pytest.raises(SafeModeActive):
                await asyncio.wait_for(task, 5)
            assert (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()
            assert notifier.has("SAFE MODE")

    run(go())


def test_hung_resync_is_converted_to_stream_failure(monkeypatch, tmp_path):
    async def hang(self):
        await asyncio.sleep(30)

    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(LiveBarrier, "resync_all", hang)
            monkeypatch.setattr(config, "LIVE_RESYNC_TIMEOUT_S", 0.1)
            monkeypatch.setattr(config, "USER_STREAM_MAX_RECONNECTS", 1)
            task, _ = start()
            with pytest.raises(WebSocketClosed):
                await asyncio.wait_for(task, 5)

    run(go())


def test_startup_times_out_if_stream_never_becomes_ready(monkeypatch, tmp_path):
    async def hang(self):
        await asyncio.sleep(30)

    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(LiveBarrier, "resync_all", hang)
            monkeypatch.setattr(config, "LIVE_STARTUP_TIMEOUT_S", 0.2)
            monkeypatch.setattr(config, "LIVE_RESYNC_TIMEOUT_S", 30)
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, _ = start(strategy=strategy)
            with pytest.raises(StreamStalled):
                await asyncio.wait_for(task, 5)
            assert strategy.calls == 0

    run(go())


def test_transient_network_errors_are_tolerated(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(config, "LIVE_MAX_CONSECUTIVE_ERRORS", 3)
            feed = FailingFeed(NetworkError("обрыв"), fail_first=2)
            task, _ = start(feed=feed)
            await settle(lambda: feed.polls >= 8)
            assert not task.done()
            await stop(task)

    run(go())


def test_persistent_network_errors_crash_the_process(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(config, "LIVE_MAX_CONSECUTIVE_ERRORS", 3)
            feed = FailingFeed(NetworkError("обрыв"))
            task, notifier = start(feed=feed)
            with pytest.raises(NetworkError):
                await asyncio.wait_for(task, 5)
            assert feed.polls == 3 and notifier.has("Аварийный выход")

    run(go())


def test_garbage_from_feed_is_fatal_immediately(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            feed = FailingFeed(DataError("мусор"))
            task, _ = start(feed=feed)
            with pytest.raises(DataError):
                await asyncio.wait_for(task, 5)
            assert feed.polls == 1

    run(go())


def test_strategy_error_is_fatal(monkeypatch, tmp_path):
    class Broken(Strategy):
        def decide(self, ctx):
            raise DataError("нет признака")

    async def go():
        async with live_env(monkeypatch, tmp_path):
            task, _ = start(strategy=Broken())
            with pytest.raises(DataError):
                await asyncio.wait_for(task, 5)

    run(go())


def test_wrong_strategy_output_is_fatal(monkeypatch, tmp_path):
    class Wrong(Strategy):
        def decide(self, ctx):
            return ["buy"]

    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            task, _ = start(strategy=Wrong())
            with pytest.raises(DataError, match="OrderRequest"):
                await asyncio.wait_for(task, 5)
            assert ex.post_log == []

    run(go())


def test_rate_limit_on_order_exits_for_ban_lock(monkeypatch, tmp_path):
    from utils.errors import RateLimitError

    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.fault("order_post", "http429")
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, _ = start(strategy=strategy)
            with pytest.raises(RateLimitError):
                await asyncio.wait_for(task, 5)
            assert not (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()

    run(go())


def test_unknown_order_outcome_trips_safe_mode_and_exits_4(monkeypatch, tmp_path):
    from utils.errors import OrderStatusUnknown

    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            ex.fault("order_post", "drop_before")
            ex.always["order_get"] = "drop"  # выяснить исход не удаётся
            monkeypatch.setattr(config, "ORDER_RETRY_BACKOFF_S", 0.0)
            strategy = ScriptedStrategy(OrderRequest("BTCUSDT", Side.BUY, 0.1))
            task, notifier = start(strategy=strategy)
            with pytest.raises((SafeModeActive, OrderStatusUnknown)) as info:
                await asyncio.wait_for(task, 10)
            assert info.value.exit_code == EXIT_SAFE_MODE
            assert (tmp_path / "data" / config.SAFE_MODE_LOCK_NAME).exists()
            assert notifier.has("SAFE MODE")

    run(go())


def test_periodic_reconcile_catches_silent_drift(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "LIVE_RECONCILE_INTERVAL_S", 0.1)
            task, notifier = start()
            await ex.wait_clients(1)
            ex.seed_position("BTCUSDT", 2.0)  # поток молчит, ACCOUNT_UPDATE не пришёл
            with pytest.raises(SafeModeActive):
                await asyncio.wait_for(task, 5)
            assert notifier.has("SAFE MODE")

    run(go())


def test_periodic_reconcile_is_skipped_while_orders_are_live(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "LIVE_RECONCILE_INTERVAL_S", 0.05)
            ex.seed_order("tb-live-1", qty=0.1)
            state = LocalState()
            from execution.state import LocalOrder
            from backtester.contracts import OrderType

            state.orders["tb-live-1"] = LocalOrder(
                "tb-live-1", 1, "BTCUSDT", Side.BUY, OrderType.LIMIT, 0.1, 59_000.0, 3_600_000, 1, 2,
                4_000_000_000_000, status="NEW",  # срок жизни в далёком будущем: expire_due ордер не тронет
            )
            state.next_ticket_id = 2
            path = tmp_path / "data" / config.ML_STATE_NAME
            path.parent.mkdir(parents=True, exist_ok=True)
            StateStore(path).save(state)
            task, _ = start()
            await ex.wait_clients(1)
            await settle(lambda: ex.requests.count("GET /fapi/v1/openOrders") == 1)
            await asyncio.sleep(0.4)  # несколько интервалов периодической сверки
            assert ex.requests.count("GET /fapi/v1/openOrders") == 1  # сверка отложена: ордер живой
            assert not task.done()
            await stop(task)

    run(go())


# ----------------------------------------------------------- --model-path: ML-модель в боевом цикле


def save_model(path, feature="sig", weight=0.01, *, names=None):
    """Модель pred = weight * feature (на гауссовых данных без шума)."""
    rng = random.Random(3)
    xs = [rng.gauss(0, 1) for _ in range(300)]
    frame = pl.DataFrame({feature: xs, "y_fwd_log_ret_300000ms": [weight * x for x in xs]})
    RidgeRegressor(1e-12).fit(frame, [feature], "y_fwd_log_ret_300000ms").save_json(path)
    return path


class SignalFeed:
    """Один раз отдаёт строку с признаком модели."""

    def __init__(self, sig):
        self._row = {"symbol": "BTCUSDT", "asof_ts": 1, "close": 60_000.0, "sig": sig}

    async def poll(self, now_ms):
        rows, self._row = ([self._row] if self._row else []), None
        return rows


class CapturedRun(LiveRun):
    """LiveRun, который запоминает собранный стек и сразу выходит: проверяем сборку, а не цикл."""

    built: list = []

    def __init__(self, **kw):
        super().__init__(**kw)
        CapturedRun.built.append(self)

    async def run(self):
        return


def test_cli_parses_model_path_and_thresholds():
    assert parse_args([]).model_path is None
    opts = parse_args(["--model-path", "m.json", "--threshold-bps", "25", "--exit-threshold-bps", "5"])
    assert (opts.model_path, opts.threshold_bps, opts.exit_threshold_bps) == (Path("m.json"), 25.0, 5.0)
    assert parse_args(["--dry-run"]).threshold_bps == config.LIVE_ML_THRESHOLD_BPS  # по умолчанию - как в обучении


def test_build_ml_strategy_loads_the_model_and_converts_bps(tmp_path):
    assert build_ml_strategy(Options()) is None  # без --model-path - каркас
    path = save_model(tmp_path / "m.json")
    strategy, features = build_ml_strategy(Options(model_path=path, threshold_bps=25.0, exit_threshold_bps=5.0))
    assert isinstance(strategy, MLModelStrategy) and features == ("sig",)
    assert (strategy._thr, strategy._exit, strategy._fraction) == (0.0025, 0.0005, config.LIVE_INITIAL_EQUITY_FRACTION)
    with pytest.raises(ConfigError, match="exit_threshold"):  # порог выхода обязан быть меньше порога входа
        build_ml_strategy(Options(model_path=path, threshold_bps=5.0, exit_threshold_bps=5.0))


def test_unreadable_model_stops_before_any_request(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            bad = tmp_path / "bad.json"
            bad.write_text("{не json", encoding="utf-8")
            for path in (tmp_path / "missing.json", bad):
                task, _ = start_ml(path, SignalFeed(sig=2.0))
                with pytest.raises(ModelError):
                    await task
            assert ex.requests == []

    run(go())


def test_model_with_features_the_feed_cannot_build_stops_before_any_request(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            path = save_model(tmp_path / "m.json", feature="no_such_feature")
            task = asyncio.create_task(live_main(
                Options(model_path=path), equity_source=const_equity(10_000.0), notifier=RecordingNotifier()))
            with pytest.raises(ConfigError, match="no_such_feature"):
                await task
            assert ex.requests == []

    run(go())


def test_model_path_selects_ml_strategy_and_the_websocket_feed(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(live_module, "LiveRun", CapturedRun)
            CapturedRun.built.clear()
            path = save_model(tmp_path / "m.json", feature="obi_1")  # признак, который канонический фид умеет считать
            await live_main(Options(model_path=path), equity_source=const_equity(10_000.0), notifier=RecordingNotifier())
            (live,) = CapturedRun.built
            # именно WS-фид (потоки 500 мс, как при обучении), а не REST-снимки раз в шаг цикла
            assert isinstance(live.strategy, MLModelStrategy) and type(live.feed) is WsLiveFeatureFeed
            assert live.strategy._model.feature_names == ("obi_1",)

    run(go())


def test_feed_is_closed_when_the_run_ends(monkeypatch, tmp_path):
    """WS-фид держит сокеты и фоновые задачи: live_main обязан закрыть его на выходе, иначе утечка при перезапуске."""
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(live_module, "LiveRun", CapturedRun)
            closed = []

            class ClosableFeed(SignalFeed):
                async def aclose(self):
                    closed.append(True)

            await live_main(Options(), feed=ClosableFeed(1.0), equity_source=const_equity(10_000.0),
                            notifier=RecordingNotifier())
            assert closed == [True]

    run(go())


def test_close_uninterrupted_finishes_the_close_despite_repeated_cancellation():
    """Второй Ctrl+C во время выхода не должен прерывать закрытие: иначе aiohttp-сессия остаётся незакрытой."""
    async def go():
        finished = []

        async def slow_close():
            await asyncio.sleep(0.1)
            finished.append(True)

        async def shutdown():
            await live_module._close_uninterrupted(slow_close, "тест")

        task = asyncio.create_task(shutdown())
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()  # повторная отмена посреди закрытия
        await task  # закрытие дождалось конца, а исключение отмены наружу не вышло
        assert finished == [True]

    run(go())


def test_close_uninterrupted_logs_a_failed_close_and_does_not_raise(caplog):
    async def go():
        async def broken_close():
            raise RuntimeError("сокет не закрылся")

        await live_module._close_uninterrupted(broken_close, "тестовый ресурс")

    with caplog.at_level("WARNING", logger="live_run"):
        run(go())
    assert any("тестовый ресурс" in r.getMessage() and "RuntimeError" in r.getMessage() for r in caplog.records)


def test_session_is_closed_even_if_the_process_is_stopped_twice(monkeypatch, tmp_path):
    """Регрессия «asyncio: Unclosed client session» при Ctrl+C: сигнал приходит дважды, второй - во время session.close()."""
    async def go():
        async with live_env(monkeypatch, tmp_path):
            original_close = aiohttp.ClientSession.close
            seen: list[aiohttp.ClientSession] = []
            closing = asyncio.Event()

            async def slow_close(self):
                seen.append(self)
                closing.set()
                await asyncio.sleep(0.1)  # окно, в которое попадает вторая отмена
                await original_close(self)

            monkeypatch.setattr(aiohttp.ClientSession, "close", slow_close)
            task, notifier = start()
            await settle(lambda: notifier.has("Боевой цикл запущен"))
            task.cancel()
            await asyncio.wait_for(closing.wait(), 5)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            assert seen and all(s.closed for s in seen)
            assert notifier.has("остановлен сигналом")

    run(go())


def test_prediction_is_logged_for_each_decision_row(monkeypatch, tmp_path, caplog):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            path = save_model(tmp_path / "m.json", weight=0.01)
            with caplog.at_level("INFO", logger="live_run"):
                task, _ = start_ml(path, SignalFeed(sig=2.0))  # pred = 0.02 -> 200 bps
                await settle(lambda: any("ПРЕДИКТ" in r.getMessage() for r in caplog.records))
                await stop(task)
            line = next(r.getMessage() for r in caplog.records if "ПРЕДИКТ" in r.getMessage())
            assert "BTCUSDT" in line and "+200.00 bps" in line and "ЛОНГ" in line

    run(go())


def test_without_model_path_falls_back_to_baseline_and_kline_feed(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path):
            monkeypatch.setattr(live_module, "LiveRun", CapturedRun)
            CapturedRun.built.clear()
            await live_main(Options(), equity_source=const_equity(10_000.0), notifier=RecordingNotifier())
            (live,) = CapturedRun.built
            assert isinstance(live.strategy, BaselineStrategy) and isinstance(live.feed, KlineObiFeed)

    run(go())


def start_ml(path, feed):
    """live_main с моделью из файла: стратегию НЕ подставляем (start() подставил бы ScriptedStrategy)."""
    notifier = RecordingNotifier()
    task = asyncio.create_task(live_main(
        Options(model_path=path), feed=feed, equity_source=const_equity(10_000.0), notifier=notifier))
    return task, notifier


def test_model_signal_from_the_feed_becomes_an_order(monkeypatch, tmp_path):
    """Полный контур: модель из файла -> прогноз по строке фида -> риск-гуарды -> ордер на бирже."""
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            path = save_model(tmp_path / "m.json")  # pred = 0.01 * sig; порог 10 bps
            task, notifier = start_ml(path, SignalFeed(sig=2.0))
            await settle(lambda: notifier.has("ВХОД"))
            order = ex.post_log[0]
            assert (order["side"], order["type"]) == ("BUY", "MARKET")
            assert float(order["quantity"]) == pytest.approx(0.016)  # 10% от 10 000 при цене 60 000, вниз до шага
            await stop(task)

    run(go())


class QuotedSignalFeed(SignalFeed):
    """Фид с живой котировкой (как WsLiveFeatureFeed.best_quote): включает Smart Execution."""

    def __init__(self, sig, quote=(59_999.0, 60_001.0)):
        super().__init__(sig)
        self.quote = quote

    def best_quote(self, symbol):
        return self.quote


def test_quoted_feed_sends_the_model_signal_as_a_post_only_limit_at_best_bid(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            path = save_model(tmp_path / "m.json")
            task, _ = start_ml(path, QuotedSignalFeed(sig=2.0))
            await settle(lambda: ex.post_log)
            order = ex.post_log[0]
            assert (order["side"], order["type"], order["timeInForce"], order["price"]) == ("BUY", "LIMIT", "GTX", "59999")
            assert float(order["quantity"]) == pytest.approx(0.016)
            await stop(task)

    run(go())


def test_live_run_chases_an_unfilled_limit_with_a_fresh_client_order_id(monkeypatch, tmp_path):
    """Контур целиком: лимитка не исполняется -> TTL -> DELETE -> переспрос модели -> новая лимитка по новой котировке."""
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            monkeypatch.setattr(config, "ORDER_TTL_SECONDS", 0.2)
            monkeypatch.setattr(config, "LIVE_CHASE_POLL_S", 0.02)
            path = save_model(tmp_path / "m.json")
            feed = QuotedSignalFeed(sig=2.0)
            task, _ = start_ml(path, feed)
            await settle(lambda: len(ex.post_log) >= 1)
            feed.quote = (59_990.0, 59_992.0)  # рынок ушёл: перевыставление пойдёт по новой котировке
            await settle(lambda: len(ex.post_log) >= 2, timeout=5)
            first, second = ex.post_log[:2]
            assert first["newClientOrderId"] != second["newClientOrderId"]
            assert (second["type"], second["price"], second["timeInForce"]) == ("LIMIT", "59990", "GTX")
            assert float(second["quantity"]) == pytest.approx(0.016)  # исполнений не было: остаток = весь объём
            assert ex.order(first["newClientOrderId"])["status"] == "CANCELED"
            assert "DELETE /fapi/v1/order" in ex.requests
            await stop(task)

    run(go())


def test_weak_model_signal_places_no_order(monkeypatch, tmp_path):
    async def go():
        async with live_env(monkeypatch, tmp_path) as ex:
            path = save_model(tmp_path / "m.json")
            feed = SignalFeed(sig=0.01)  # pred = 0.0001 < 10 bps
            task, _ = start_ml(path, feed)
            await settle(lambda: feed._row is None)
            await asyncio.sleep(0.1)
            assert ex.post_log == []
            await stop(task)

    run(go())


# ------------------------------------------------------------------------------ чистые функции


@pytest.mark.parametrize("qty,expected", [(0.12349, 0.123), (0.1, 0.1), (0.0009, 0.0), (1.0, 1.0), (0.3, 0.3)])
def test_round_qty_floors_to_step(qty, expected):
    assert round_qty("BTCUSDT", qty) == pytest.approx(expected)


def test_round_qty_requires_known_symbol():
    with pytest.raises(ConfigError):
        round_qty("XYZUSDT", 1.0)


@pytest.mark.parametrize("before,after,label", [
    (0.0, 0.1, "ВХОД"), (0.1, 0.0, "ВЫХОД"), (0.1, -0.1, "РАЗВОРОТ"), (0.1, 0.2, "ИЗМЕНЕНИЕ"), (-0.3, -0.1, "ИЗМЕНЕНИЕ"),
    (0.1 + 0.2 - 0.3, 0.0, "ВЫХОД"),
])
def test_classify_fill(before, after, label):
    assert classify_fill(before, after) == label


def test_parse_equity_variants_and_garbage():
    assert parse_equity({"totalMarginBalance": "1234.5"}) == 1234.5
    assert parse_equity({"totalWalletBalance": "1000", "totalUnrealizedProfit": "-50"}) == 950.0
    for bad in ([], {}, {"totalMarginBalance": "abc"}, {"totalMarginBalance": "nan"}, None):
        with pytest.raises(DataError):
            parse_equity(bad)


def test_cli_parsing_and_usage_error_is_config_error():
    assert parse_args([]) == Options(False, False)
    assert parse_args(["--dry-run", "--allow-mainnet"]) == Options(True, True)
    with pytest.raises(ConfigError) as info:  # argparse по умолчанию вышел бы кодом 2 («бан»)
        parse_args(["--bogus"])
    assert info.value.exit_code == EXIT_CONFIG


def test_testnet_profile_endpoints_are_defined():
    assert config._PROFILES["testnet"]["rest"] == "https://testnet.binancefuture.com"
    assert config._PROFILES["testnet"]["ws"].startswith("wss://stream.binancefuture.com")
    assert config._PROFILES["testnet"]["ws_private"].startswith("wss://stream.binancefuture.com")
    assert config._PROFILES["mainnet"]["rest"] == "https://fapi.binance.com"


def test_unknown_profile_is_rejected_by_validate(monkeypatch):
    monkeypatch.setattr(config, "BINANCE_PROFILE", "testnnet")
    with pytest.raises(ConfigError, match="BINANCE_PROFILE"):
        config.validate()
