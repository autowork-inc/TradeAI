"""Live Execution Engine на имитации биржи (FakeRest): порядок вызовов, защита позиции, отмены, аварийные пути, зеркальный приёмник.

Клиент настоящий (`BinanceLiveClient`): запросы проходят ту же сборку и локальную валидацию, что и в бою; подменён только
канал `rest.request`. Боевых ордеров не отправляется никогда.
"""

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

import config
from exchange.binance_live import BinanceLiveClient
from execution.state import SafeMode
from orchestrator.journal import MemoryEventLog
from orchestrator.live_engine import (
    CLOSED, ENTRY, OPEN, LiveExecutionEngine, LiveExecutionError, LiveMirrorSink, LiveStateStore, build_live_engine,
)
from orchestrator.live_risk import utc_day_start_ms
from orchestrator.tracker import Submission
from orchestrator.types import Direction, Kind
from tests.orchestrator.fake_live_rest import FakeRest, api_error, network_error
from tests.orchestrator.live_helpers import BAR, FILTERS, STRATEGY, T0, manager, signal
from utils.errors import ConfigError

ROOT = Path(__file__).resolve().parents[2]
NOW = T0 + 60_000
SYM = "SOLUSDT"


class Clock:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t


class Env:
    def __init__(self, tmp_path, price=100.0, risk_kw=None, **engine_kw):
        self.tmp_path = tmp_path
        self.rest = FakeRest(price)
        self.client = BinanceLiveClient(self.rest)
        self.client.set_filters(FILTERS)
        self.journal = MemoryEventLog()
        self.safe = SafeMode(tmp_path / ".safe_mode")
        self.clock = Clock()
        self.store = LiveStateStore(tmp_path / "state.json")
        self.engine_kw = {"post_only": False, **engine_kw}
        self.risk_kw = risk_kw or {}
        self.risk = manager(tmp_path, **self.risk_kw)
        self.engine = LiveExecutionEngine(self.client, self.risk, self.journal, self.safe, now_ms=self.clock, interval_ms=BAR,
                                          store=self.store, **self.engine_kw)

    def reborn(self):
        """Процесс перезапущен: новый менеджер риска и движок над той же биржей, тем же файлом состояния и журналом."""
        self.risk = manager(self.tmp_path, **self.risk_kw)
        self.engine = LiveExecutionEngine(self.client, self.risk, self.journal, self.safe, now_ms=self.clock, interval_ms=BAR,
                                          store=LiveStateStore(self.tmp_path / "state.json"), **self.engine_kw)
        return self.engine

    def kinds(self):
        return [e.kind for e in self.journal.events]

    def event(self, kind):
        (e,) = self.journal.of_kind(kind)
        return e

    def trade(self):
        return self.engine.trades[SYM]

    def posts(self, type_=None):
        return [p for p in self.rest.calls("POST", config.ORDER_PATH) if type_ in (None, p["type"])]

    def algos(self):
        return self.rest.calls("POST", config.ALGO_ORDER_PATH)


def run(coro):
    return asyncio.run(coro)


# ------------------------------------------------------------------------------------- MARKET


def test_market_long_entry_then_sl_and_scale_out_levels_in_the_right_order(tmp_path):
    env = Env(tmp_path, price=100.5)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    assert env.rest.names() == ["GET income", "GET balance", "GET mark", "POST margin", "POST leverage", "POST order:MARKET",
                                "POST algo:STOP_MARKET", "POST algo:TAKE_PROFIT_MARKET", "POST algo:TAKE_PROFIT_MARKET"]
    (entry,) = env.posts()
    assert (entry["side"], entry["quantity"], entry["symbol"]) == ("BUY", "1", SYM) and "reduceOnly" not in entry
    sl, l1, l2 = env.algos()
    assert (sl["side"], sl["triggerPrice"], sl["quantity"], sl["reduceOnly"]) == ("SELL", "98", "1", True)
    # R от ФАКТИЧЕСКОЙ цены 100.5: R = 2.5; уровни плана SMC_Pro_Trend: 50% на +1R и 40% на +3R, остальные 10% - runner без тейка
    assert [(a["triggerPrice"], a["quantity"], a["side"], a["reduceOnly"]) for a in (l1, l2)] == [("103", "0.5", "SELL", True),
                                                                                                    ("108", "0.4", "SELL", True)]
    assert env.rest.calls("POST", config.MARGIN_TYPE_PATH) == [{"symbol": SYM, "marginType": "ISOLATED"}]
    assert env.rest.calls("POST", config.LEVERAGE_PATH) == [{"symbol": SYM, "leverage": 10}]
    assert env.kinds() == ["live_intent", "live_entry_filled", "live_protected"] and env.trade().status == OPEN
    assert env.rest.calls("GET", config.INCOME_PATH)[0]["startTime"] == utc_day_start_ms(NOW)  # сутки UTC


def test_entry_price_falls_back_to_position_risk_when_the_exchange_sends_no_avg_price(tmp_path):
    """Testnet 2026-10-14: ответ RESULT на MARKET без avgPrice -> раньше fill_price = 0 (R, безубыток, трейлинг считались бы от нуля)."""
    env = Env(tmp_path, price=100.5)
    env.rest.hide_avg_price = True
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    assert env.trade().fill_price == 100.5 and env.trade().status == OPEN
    assert env.event("live_entry_filled").detail["avg_price"] == 100.5
    assert [a["triggerPrice"] for a in env.algos()] == ["98", "103", "108"]  # уровни - от фактической цены, как при avgPrice в ответе


def test_market_short_is_mirrored(tmp_path):
    env = Env(tmp_path, price=100.0)
    run(env.engine.execute(signal(direction=Direction.SHORT, entry=100.0, sl=102.0), NOW))
    assert env.posts()[0]["side"] == "SELL" and env.rest.positions[SYM] == -1.0
    sl, l1, l2 = env.algos()
    assert (sl["side"], sl["triggerPrice"], l1["side"], l1["triggerPrice"], l2["triggerPrice"]) == ("BUY", "102", "BUY", "98", "94")


def test_intent_is_journaled_and_flushed_before_the_first_order_request(tmp_path):
    seen = {}
    holder = {}

    async def flush():
        seen["intent_first"] = bool(holder["env"].journal.of_kind("live_intent")) and not holder["env"].posts()
        seen["no_algo_yet"] = not holder["env"].algos()

    env = holder["env"] = Env(tmp_path, flush=flush)
    run(env.engine.execute(signal(), NOW))
    assert seen == {"intent_first": True, "no_algo_yet": True}  # R7: причина записана ДО ордера
    intent = env.event("live_intent").detail
    assert intent["max_loss_usdt"] == 2.0 and intent["leverage"] == 10 and intent["meta"] == {"htf_label": "Bullish"}


def test_second_signal_on_a_busy_symbol_is_rejected_without_orders(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(), NOW))
    before = len(env.posts())
    run(env.engine.execute(signal(direction=Direction.SHORT, entry=100.0, sl=102.0, n=2), NOW))
    assert env.event("live_rejected").detail["reason"] == "symbol_busy" and len(env.posts()) == before


# -------------------------------------------------------------------------------------- фильтры


def test_unapproved_strategy_places_nothing(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(strategy="OBI_Breakout"), NOW))
    assert env.event("live_rejected").detail["reason"] == "strategy_not_approved"
    assert env.posts() == [] and env.algos() == [] and env.rest.calls("POST", config.LEVERAGE_PATH) == []


def test_insufficient_balance_and_active_safe_mode_place_nothing(tmp_path):
    env = Env(tmp_path)
    env.rest.available = 9.99  # меньше маржи 10 USDT
    run(env.engine.execute(signal(), NOW))
    assert env.event("live_rejected").detail["reason"] == "insufficient_balance" and env.posts() == [] and env.risk.busy() == {}

    env2 = Env(tmp_path / "2")
    env2.safe.trip("тест")
    run(env2.engine.execute(signal(), NOW))
    assert env2.event("live_rejected").detail["reason"] == "safe_mode" and env2.rest.requests == []  # даже income не запрашивался


def test_daily_loss_limit_blocks_trading_and_the_event_is_journaled(tmp_path):
    env = Env(tmp_path)
    env.rest.add_income("REALIZED_PNL", -45.0)
    env.rest.add_income("COMMISSION", -5.0)  # -50.0: ровно лимит
    run(env.engine.execute(signal(), NOW))
    assert env.event("live_daily_limit").detail["pnl_usdt"] == -50.0
    assert env.event("live_rejected").detail["reason"] == "trading_blocked" and env.posts() == []
    assert (tmp_path / "risk.json").exists()  # защёлка на диске: переживёт рестарт


def test_daily_loss_limit_cancels_pending_entries_but_keeps_open_positions_protected(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0), NOW))  # лимитка висит
    limit_cid = env.trade().entry_cid
    env.rest.price = 2.0
    run(env.engine.execute(signal(symbol="XRPUSDT", entry=2.0, sl=1.9, n=2), NOW))  # вторая монета: рыночный вход, уже под защитой
    assert env.engine.trades["XRPUSDT"].status == OPEN and env.trade().status == ENTRY
    env.rest.add_income("REALIZED_PNL", -60.0)
    env.clock.t = NOW + 31_000  # пора обновить PnL
    run(env.engine.poll())
    assert SYM not in env.engine.trades  # неисполненный вход снят
    assert env.rest.orders[limit_cid]["status"] == "CANCELED"
    assert env.engine.trades["XRPUSDT"].status == OPEN  # открытая позиция остаётся под защитой на бирже и сопровождается дальше
    assert [a["algoStatus"] for a in env.rest.algos.values()] == ["NEW", "NEW", "NEW"]


# --------------------------------------------------------------------------------------- LIMIT


def limit_env(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    return env


def test_limit_entry_waits_then_is_protected_after_the_fill_and_closes_on_stop(tmp_path):
    env = limit_env(tmp_path)
    assert env.trade().status == ENTRY and env.algos() == [] and env.posts()[0]["type"] == "LIMIT"
    assert env.posts()[0]["price"] == "100" and env.posts()[0]["timeInForce"] == "GTC"
    run(env.engine.poll())
    assert env.trade().status == ENTRY and env.algos() == []  # не исполнена: защиты нет (позиции нет)

    env.rest.fill_limit(env.trade().entry_cid)
    run(env.engine.poll())
    assert env.trade().status == OPEN
    sl, l1, l2 = env.algos()
    assert (sl["triggerPrice"], l1["triggerPrice"], l2["triggerPrice"]) == ("98", "102", "106")  # от цены лимитки 100, R = 2

    stop_cid, level_cids = env.trade().stop_cid, [v.cid for v in env.trade().levels]
    run(env.engine.poll())
    assert env.trade().status == OPEN  # позиция есть - ничего не меняется
    env.rest.trigger(stop_cid)
    run(env.engine.poll())
    assert SYM not in env.engine.trades and env.risk.busy() == {}  # монета освобождена
    assert env.event("live_closed").detail["reason"] == "sl"
    assert all(env.rest.algos[c]["algoStatus"] == "CANCELED" for c in level_cids)  # уровни сняты: они опасны для следующей сделки


def test_last_level_without_runner_is_reported_as_tp(tmp_path):
    from orchestrator.registry import VWAP_PLAN  # 50% на +1R и 50% на +2R, остатка нет

    env = Env(tmp_path, risk_kw={"plans": {STRATEGY: VWAP_PLAN}})
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    env.rest.fill_limit(env.trade().entry_cid)
    run(env.engine.poll())
    stop_cid = env.trade().stop_cid
    env.rest.set_price(102.0)  # +1R
    run(env.engine.poll())
    env.rest.set_price(104.0)  # +2R: последний уровень
    run(env.engine.poll())
    assert [e.detail["reason"] for e in env.journal.of_kind("live_partial_close")] == ["scale_1r", "tp"]
    assert env.event("live_closed").detail["reason"] == "tp" and env.rest.algos[stop_cid]["algoStatus"] != "NEW"
    assert env.rest.positions[SYM] == 0.0 and SYM not in env.engine.trades


def test_position_closed_outside_the_bot_is_reported_as_unknown_and_orders_are_cleaned(tmp_path):
    env = limit_env(tmp_path)
    env.rest.fill_limit(env.trade().entry_cid)
    run(env.engine.poll())
    env.rest.positions[SYM] = 0.0  # закрыли руками
    run(env.engine.poll())
    assert env.event("live_closed").detail["reason"] == "unknown"
    assert {a["algoStatus"] for a in env.rest.algos.values()} == {"CANCELED"} and env.risk.busy() == {}


def test_limit_ttl_cancels_the_order_after_ten_bars(tmp_path):
    env = limit_env(tmp_path)
    cid = env.trade().entry_cid
    env.clock.t = NOW + 9 * BAR
    run(env.engine.poll())
    assert env.trade().status == ENTRY
    env.clock.t = NOW + 10 * BAR
    run(env.engine.poll())
    assert env.rest.orders[cid]["status"] == "CANCELED" and SYM not in env.engine.trades and env.algos() == []
    assert env.event("live_trade_cancelled").detail["reason"] == "ttl"


@pytest.mark.parametrize("close, reason", [(97.5, "invalidated"), (106.5, "tp_before_fill")])
def test_limit_is_cancelled_like_in_shadow_when_invalidated_or_tp_reached_first(tmp_path, close, reason):
    env = limit_env(tmp_path)
    env.engine.on_bar_closed(SYM, T0 + BAR, close, NOW + BAR)
    run(env.engine.poll())
    assert env.event("live_trade_cancelled").detail["reason"] == reason and env.algos() == []


def test_close_inside_the_corridor_keeps_the_limit(tmp_path):
    env = limit_env(tmp_path)
    env.engine.on_bar_closed(SYM, T0 + BAR, 101.0, NOW + BAR)
    run(env.engine.poll())
    assert env.trade().status == ENTRY


def test_partial_fill_cancels_the_rest_and_protects_exactly_the_filled_quantity(tmp_path):
    env = Env(tmp_path, risk_kw={"margin": 100.0})  # 1000 USDT: объём 10.0, план делится и после частичного исполнения
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0, tp=106.0, inval=98.0), NOW))
    cid = env.trade().entry_cid
    env.rest.fill_limit(cid, qty=8.0)
    run(env.engine.poll())
    assert env.rest.orders[cid]["status"] == "CANCELED"  # остаток снят: больше позиция не вырастет
    sl, l1, l2 = env.algos()
    assert (sl["quantity"], l1["quantity"], l2["quantity"]) == ("8", "4", "3.2")  # 50% / 40% от ИСПОЛНЕННОГО объёма, runner 0.8
    assert env.trade().status == OPEN and env.trade().filled_qty == 8.0


def test_partial_fill_that_the_plan_cannot_split_is_closed_without_safe_mode(tmp_path):
    env = limit_env(tmp_path)
    env.rest.fill_limit(env.trade().entry_cid, qty=0.4)  # 0.4 на шаге 0.1 не делится 50/40/10
    run(env.engine.poll())
    assert env.rest.positions[SYM] == 0.0 and env.algos() == [] and SYM not in env.engine.trades and not env.safe.active
    assert env.event("live_closed").detail["reason"] == "plan_not_representable"


def test_post_only_rejection_by_status_cancels_the_trade(tmp_path):
    env = Env(tmp_path)
    env.rest.limit_status = "EXPIRED"  # GTX: ордер стал бы тейкером
    run(env.engine.execute(signal(kind=Kind.LIMIT, entry=100.0, sl=98.0), NOW))
    assert SYM not in env.engine.trades and env.risk.busy() == {} and env.algos() == []


# ------------------------------------------------------------------------------ аварийные пути


def assert_flat_and_safe(env, expected_cause):
    assert env.safe.active and expected_cause in env.safe.reason
    assert env.rest.positions[SYM] == 0.0  # позиция закрыта reduce-only рынком
    closer = env.posts("MARKET")[-1]
    assert closer.get("reduceOnly") is True and closer["side"] == "SELL"
    assert env.risk.busy() == {} and "live_emergency" in env.kinds()
    assert all(a["algoStatus"] != "NEW" for a in env.rest.algos.values())  # то, что успели поставить, снято


def test_failed_stop_loss_flattens_the_position_trips_safe_mode_and_stops_the_process(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ALGO_ORDER_PATH)] = [api_error(-2021, "Order would immediately trigger.")]
    with pytest.raises(LiveExecutionError):
        run(env.engine.execute(signal(), NOW))
    assert_flat_and_safe(env, "protection_failed")
    assert env.algos() == [{**env.algos()[0]}] and len(env.algos()) == 1  # тейк уже не ставился


def test_failed_take_profit_cancels_the_stop_and_flattens(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ALGO_ORDER_PATH)] = [None, api_error(-4130, "closePosition exists")]  # SL проходит, TP - нет
    with pytest.raises(LiveExecutionError):
        run(env.engine.execute(signal(), NOW))
    assert_flat_and_safe(env, "protection_failed")
    assert len(env.algos()) == 2 and [a["algoStatus"] for a in env.rest.algos.values()] == ["CANCELED"]


def test_unknown_outcome_while_placing_the_stop_is_treated_as_failed_protection(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ALGO_ORDER_PATH)] = [network_error()]
    with pytest.raises(LiveExecutionError):
        run(env.engine.execute(signal(), NOW))
    assert_flat_and_safe(env, "protection_failed")


def test_fill_beyond_the_stop_is_closed_immediately(tmp_path):
    env = Env(tmp_path, price=97.5, max_entry_deviation=0.5)  # гэп: вход исполнился уже ниже стопа 98
    with pytest.raises(LiveExecutionError):
        run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    assert_flat_and_safe(env, "fill_beyond_stop") and env.algos() == []


def test_market_entry_far_from_the_signal_price_is_rejected_before_any_order(tmp_path):
    env = Env(tmp_path, price=97.5)  # mark ушла на 2.5% от цены сигнала (допуск 1%)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    d = env.event("live_rejected").detail
    assert d["reason"] == "price_deviation" and d["mark"] == 97.5 and env.posts() == [] and env.risk.busy() == {}


def test_lost_response_to_the_entry_is_resolved_by_client_order_id(tmp_path):
    env = Env(tmp_path)
    env.rest.lose_next_order_response = True  # ордер принят биржей, ответ не дошёл
    run(env.engine.execute(signal(), NOW))
    assert len(env.posts()) == 1 and env.trade().status == OPEN  # повторной отправки нет, позиция найдена и защищена
    assert env.rest.calls("GET", config.ORDER_PATH) == [{"symbol": SYM, "origClientOrderId": env.trade().entry_cid}]


def test_request_that_never_reached_the_exchange_leaves_no_trade(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ORDER_PATH)] = network_error()  # ордер НЕ принят
    run(env.engine.execute(signal(), NOW))
    assert env.risk.busy() == {} and SYM not in env.engine.trades and env.rest.positions == {}
    assert "live_entry_not_placed" in env.kinds()


def test_exchange_rejection_of_the_entry_is_a_normal_outcome(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ORDER_PATH)] = api_error(-2019, "Margin is insufficient.")
    run(env.engine.execute(signal(), NOW))
    assert env.event("live_entry_rejected").detail["code"] == -2019 and env.risk.busy() == {} and not env.safe.active


def test_rejected_api_key_is_fatal_and_trips_safe_mode(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.ORDER_PATH)] = api_error(-2015, "Invalid API-key, IP, or permissions for action.")
    with pytest.raises(LiveExecutionError):
        run(env.engine.execute(signal(), NOW))
    assert env.safe.active


def test_failure_before_the_entry_is_sent_releases_the_symbol(tmp_path):
    env = Env(tmp_path)
    env.rest.fail[("POST", config.LEVERAGE_PATH)] = api_error(-4028, "Leverage not valid")
    with pytest.raises(Exception, match="-4028|4028|error|Leverage"):
        run(env.engine.execute(signal(), NOW))
    assert env.risk.busy() == {} and env.posts() == []  # ордера не было, монета свободна


def test_position_with_the_opposite_sign_stops_the_process(tmp_path):
    env = limit_env(tmp_path)
    env.rest.fill_limit(env.trade().entry_cid)
    run(env.engine.poll())
    env.rest.positions[SYM] = -1.0  # кто-то развернул позицию
    with pytest.raises(LiveExecutionError):
        run(env.engine.poll())
    assert env.safe.active


def test_leftover_algo_order_that_cannot_be_cancelled_is_fatal(tmp_path):
    env = limit_env(tmp_path)
    env.rest.fill_limit(env.trade().entry_cid)
    run(env.engine.poll())
    env.rest.trigger(env.trade().stop_cid)
    # отмена не проходит ни у сработавшего стопа (он терминален - это допустимо), ни у первого уровня (он ещё NEW - это авария)
    env.rest.fail[("DELETE", config.ALGO_ORDER_PATH)] = [api_error(-1000, "boom"), api_error(-1000, "boom")]
    with pytest.raises(LiveExecutionError):
        run(env.engine.poll())


# -------------------------------------------------------------- задача исполнителя и зеркальный приёмник


class Primary:
    def __init__(self, accept=True):
        self.accept, self.signals, self.bars, self.listeners = accept, [], [], []

    def submit(self, s, now_ms):
        self.signals.append(s)
        return Submission(self.accept, None if self.accept else "exposure")

    def on_bar_closed(self, symbol, bar_open_time, close, now_ms, bar=None, interval=None):
        self.bars.append((symbol, close))

    def add_close_listener(self, listener):
        self.listeners.append(listener)


def test_mirror_sink_always_feeds_shadow_and_forwards_only_approved_accepted_signals(tmp_path):
    env = Env(tmp_path)
    primary = Primary()
    sink = LiveMirrorSink(primary, env.engine)
    other, mine = signal(strategy="VWAP_Reversion", n=1), signal(n=2)
    assert sink.submit(other, NOW).accepted and sink.submit(mine, NOW).accepted
    assert primary.signals == [other, mine]  # Shadow-трекер получил ВСЁ
    assert env.engine._queue.qsize() == 1  # в боевой движок ушёл только одобренный
    refused = LiveMirrorSink(Primary(accept=False), env.engine)
    assert not refused.submit(signal(n=3), NOW).accepted and env.engine._queue.qsize() == 1  # отказ трекера - не торгуем
    sink.on_bar_closed(SYM, T0, 101.0, NOW)
    sink.add_close_listener(print)
    assert primary.bars == [(SYM, 101.0)] and primary.listeners == [print]
    run(env.engine.step())  # исполнитель разбирает очередь
    assert env.trade().plan.signal_id == mine.signal_id


def test_engine_task_failure_surfaces_through_check(tmp_path):
    env = Env(tmp_path, poll_interval_s=0.01)
    env.rest.fail[("POST", config.ALGO_ORDER_PATH)] = [api_error(-2021)]

    async def body():
        env.engine.check()  # пока всё в порядке
        env.engine.start()
        env.engine.enqueue(signal(), NOW)
        await asyncio.sleep(0.2)
        with pytest.raises(LiveExecutionError):
            env.engine.check()
        await env.engine.aclose()

    run(body())


def test_real_smc_signal_goes_through_the_mirror_to_real_orders(tmp_path):
    """Сквозной путь: настоящий SMC-сигнал -> Оркестратор -> зеркало -> Shadow-трекер И боевой движок (имитация биржи)."""
    from orchestrator.core import Orchestrator, Route
    from orchestrator.strategies import StrategySMC
    from orchestrator.tracker import VirtualTracker
    from orchestrator.types import Mode
    from tests.orchestrator.test_shadow_run import scenario_frame
    from tests.orchestrator.test_strategies import SYMBOL, context
    from execution.exchange_filters import SymbolFilters

    env = Env(tmp_path)
    env.risk._filters[SYMBOL] = SymbolFilters(SYMBOL, 0.1, 0.001, 0.001, 5.0)
    env.client.set_filters({SYMBOL: env.risk._filters[SYMBOL]})
    tracker = VirtualTracker(env.journal, equity=10_000.0)
    strategy = StrategySMC()
    orch = Orchestrator([strategy], {strategy.name: Route(Mode.SHADOW, LiveMirrorSink(tracker, env.engine))}, env.journal)
    frame = scenario_frame()
    ctx = context(frame, 19)
    for i in range(1, 19):
        orch.on_context(context(frame, i))
    orch.on_context(ctx)
    assert len(tracker.active()) == 1 and env.engine._queue.qsize() == 1  # Shadow-сделка есть, боевой сигнал в очереди
    env.clock.t = ctx.received_ms
    run(env.engine.execute(*env.engine._queue.get_nowait()))
    t = env.engine.trades[SYMBOL]
    assert t.plan.side == "SELL" and t.plan.kind is Kind.LIMIT and t.status == ENTRY
    assert env.posts()[0]["price"] == "105" and env.posts()[0]["type"] == "LIMIT"
    assert t.plan.sl == pytest.approx(112.2) and t.plan.tp == pytest.approx(83.4)


# ----------------------------------------------------------------- сборка и защита от включения


@pytest.fixture
def build_env(tmp_path, monkeypatch):
    rest = FakeRest()
    rest.filters = dict(FILTERS)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "TRADE_MODE", "live")
    monkeypatch.setattr(config, "USE_TESTNET", True)
    monkeypatch.setattr(config, "LIVE_ALLOW_MAINNET", False)
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", (STRATEGY,))
    monkeypatch.setattr(config, "LIVE_REST_URL", config.TESTNET_REST_URL)
    monkeypatch.setattr(BinanceLiveClient, "from_env", classmethod(lambda cls, session, base_url=None: BinanceLiveClient(rest)))
    return rest


def build(**kw):
    args = dict(symbols=("SOLUSDT", "XRPUSDT"), strategy_names=(STRATEGY, "VWAP_Reversion"), interval_ms=BAR)
    return run(build_live_engine(None, MemoryEventLog(), **{**args, **kw}))


def test_build_succeeds_only_with_every_guard_satisfied(build_env):
    engine = build()
    assert engine.risk.approved == {STRATEGY} and engine.risk.margin_usdt == config.LIVE_MARGIN_USDT == 10.0
    assert engine.risk.leverage == config.LIVE_LEVERAGE == 10 and engine.risk.daily_loss_limit == 50.0
    assert engine.client.filters("SOLUSDT").tick_size == 0.01


def test_build_refuses_in_shadow_mode(build_env, monkeypatch):
    monkeypatch.setattr(config, "TRADE_MODE", "shadow")
    with pytest.raises(ConfigError, match="TRADE_MODE=live"):
        build()
    assert build_env.requests == []  # до сети не дошли


def test_build_refuses_real_money_without_explicit_confirmation(build_env, monkeypatch):
    monkeypatch.setattr(config, "USE_TESTNET", False)
    with pytest.raises(ConfigError, match="LIVE_ALLOW_MAINNET"):
        build()
    monkeypatch.setattr(config, "LIVE_ALLOW_MAINNET", True)
    assert build() is not None


def test_build_refuses_unknown_approved_strategy_and_active_safe_mode(build_env, monkeypatch):
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", ("SMC_Pro_Trendd",))
    with pytest.raises(ConfigError, match="неизвестные"):
        build()
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", (STRATEGY,))
    latch = SafeMode(config.DATA_DIR / config.SAFE_MODE_LOCK_NAME)
    latch.trip("прошлый запуск")
    with pytest.raises(ConfigError, match="Safe Mode"):
        build()
    latch.clear("test")
    assert build() is not None


def test_build_refuses_a_foreign_position_without_a_stop_and_trips_safe_mode(build_env):
    build_env.positions["XRPUSDT"] = 5.0
    build_env.entry_prices["XRPUSDT"] = 2.0  # позиция есть, записи сделки и стопа на бирже нет
    with pytest.raises(LiveExecutionError, match="XRPUSDT"):
        build()
    assert SafeMode(config.DATA_DIR / config.SAFE_MODE_LOCK_NAME).active


def test_trade_mode_defaults_to_shadow_and_is_validated(monkeypatch):
    assert config.TRADE_MODE == "shadow"  # без явного TRADE_MODE=live боевой контур выключен
    assert config.APPROVED_STRATEGIES == ("SMC_Pro_Trend",) and config.LIVE_DAILY_LOSS_LIMIT_USDT == 50.0
    monkeypatch.setattr(config, "TRADE_MODE", "paper")
    with pytest.raises(ConfigError, match="TRADE_MODE"):
        config.validate()
    monkeypatch.setattr(config, "TRADE_MODE", "live")
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", ())
    with pytest.raises(ConfigError, match="APPROVED_STRATEGIES"):
        config.validate()


def test_shadow_mode_never_imports_the_live_code():
    code = "import sys, scripts.shadow_run; print(any(m in sys.modules for m in ('orchestrator.live_engine', 'exchange.binance_live', 'exchange')))"
    out = subprocess.run([sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, r'{ROOT}'); {code}"], capture_output=True, text=True,
                         cwd=ROOT, check=True)
    assert out.stdout.strip() == "False"  # TRADE_MODE=shadow: боевой код не загружается вообще


def test_the_only_live_import_in_shadow_run_sits_behind_the_trade_mode_guard():
    lines = (ROOT / "scripts" / "shadow_run.py").read_text(encoding="utf-8").splitlines()
    imports = [i for i, line in enumerate(lines) if "live_engine" in line and "import" in line]
    assert len(imports) == 1
    guard = [i for i, line in enumerate(lines[: imports[0]]) if line.strip().startswith('if config.TRADE_MODE == "live"')]
    assert guard and imports[0] - guard[-1] <= 3 and lines[imports[0]].startswith(" " * 20)  # внутри блока guard, не на уровне модуля


def test_shadow_loop_stops_when_the_live_engine_crashes(tmp_path):
    from orchestrator.core import Orchestrator
    from orchestrator.tracker import VirtualTracker
    from scripts.shadow_run import ShadowRun
    from tests.orchestrator.test_shadow_run import FakeFeed

    class Live:
        def check(self):
            raise LiveExecutionError("упал")

    tracker = VirtualTracker(MemoryEventLog(), equity=10_000.0)
    run_ = ShadowRun(FakeFeed([]), Orchestrator([], {}, MemoryEventLog()), tracker, None, live=Live())
    with pytest.raises(LiveExecutionError):
        run(run_.tick())


def test_no_error_suppression_in_live_modules_r1():
    import re

    banned = re.compile(r"except\s*:|except\s+(Base)?Exception\b|contextlib\.suppress|\bsuppress\(")
    for f in (ROOT / "orchestrator" / "live_engine.py", ROOT / "orchestrator" / "live_risk.py", ROOT / "exchange" / "binance_live.py"):
        code = "\n".join(line.split("#")[0] for line in f.read_text(encoding="utf-8").splitlines())
        assert not banned.search(code), f.name
    assert CLOSED == "closed"


def test_open_position_limit_sends_extra_signals_to_shadow_only(tmp_path):
    """Лимит одновременных боевых позиций: сверх него сигнал отклоняется боевым контуром (`max_open_positions`), а Shadow-трекер его всё равно получил."""
    env = Env(tmp_path, risk_kw={"max_open_positions": 1})
    primary = Primary()
    sink = LiveMirrorSink(primary, env.engine)
    first, second = signal(symbol="SOLUSDT", n=1), signal(symbol="XRPUSDT", n=2)
    assert sink.submit(first, NOW).accepted and sink.submit(second, NOW).accepted
    assert primary.signals == [first, second]  # виртуальный учёт идёт по обоим
    run(env.engine.step())
    run(env.engine.step())
    rej = env.event("live_rejected")
    assert rej.detail["reason"] == "max_open_positions" and rej.detail["max"] == 1 and rej.symbol == "XRPUSDT"
    assert [o["symbol"] for o in env.posts("MARKET")] == ["SOLUSDT"]  # на биржу ушёл только первый
    # место освободилось - следующий сигнал снова боевой
    env.risk.release("SOLUSDT", first.signal_id)
    assert env.risk.gate(signal(symbol="XRPUSDT", n=3), NOW) is None


def test_default_open_position_limit_is_four(tmp_path):
    assert config.LIVE_MAX_OPEN_POSITIONS == 4 and manager(tmp_path).max_open_positions == 4
    with pytest.raises(ConfigError):
        manager(tmp_path, max_open_positions=0)


def test_build_on_the_5m_clock_for_pro_trend_and_vwap_with_calendar_equal_limits(build_env, monkeypatch):
    """2026-10-15: SMC_Pro_Trend и VWAP_Reversion идут на одном 5m такте; сроки заявки и тайм-аута пересчитаны (x3), чужие таймфреймы движок пропускает."""
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", (STRATEGY, "VWAP_Reversion"))
    engine = build(interval_ms=300_000)
    assert engine.risk.approved == {STRATEGY, "VWAP_Reversion"}
    assert engine._interval_ms == 300_000 and engine._ttl_bars == 30 and engine._max_hold == config.LIVE_MAX_HOLD_BARS * 3
    engine.on_bar_closed(SYM, T0, 100.0, NOW, None, interval="15m")  # чужой таймфрейм: молча пропущен
