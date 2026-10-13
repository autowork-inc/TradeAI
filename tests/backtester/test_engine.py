"""BacktestEngine: учёт денег по событиям барьера, строгость времени, реакция на гуарды и отказы.

Ордера исполняет ТЕСТОВЫЙ барьер (MarketBarrier): market по цене закрытия последней строки решений,
известной к моменту исполнения. Все ожидаемые числа посчитаны вручную в комментариях.

В конце файла - сквозной тест: синтетический рынок -> WatermarkClock -> TimelineAligner -> признаки ->
BaselineStrategy -> BacktestEngine с НАСТОЯЩИМ OrderBookBarrier (исполнение по стакану).
"""

import math
import random
from bisect import bisect_left

import polars as pl
import pytest

from backtester.barrier import OrderBookBarrier
from backtester.contracts import (
    CloseReason,
    ExecutionBarrier,
    ExecutionConfig,
    FundingRecord,
    Liquidity,
    OrderClosed,
    RawFill,
    Side,
)
from backtester.engine import BacktestEngine, BacktestError, DecisionContext, Strategy
from backtester.risk import (
    DAY_MS,
    DrawdownLimitGuard,
    MaxExposureGuard,
    OrderRequest,
    RiskDecision,
    RiskGuard,
)
from backtester.strategy import BaselineStrategy
from pipeline.features.base import Feature, apply_features, compile_features, source_columns
from pipeline.guards.provenance import LookaheadViolation
from pipeline.loaders.schemas import depth_schema
from pipeline.splits.walkforward import PurgedWalkForward
from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import WatermarkClock
from pipeline.types import TimingConfig
from utils.errors import ConfigError

LAT, MAKER, TAKER = 250, 0.0002, 0.0005
CFG = ExecutionConfig(LAT, MAKER, TAKER)
H = 3_600_000
MIN = 60_000
BTC = "BTCUSDT"


def frame(rows, symbol=BTC) -> pl.DataFrame:
    """rows: [(asof_ts, close)]. src_max_ts на 1 мс раньше решения."""
    return pl.DataFrame(
        {"symbol": [symbol] * len(rows), "asof_ts": [r[0] for r in rows],
         "src_max_ts": [r[0] - 1 for r in rows], "close": [r[1] for r in rows]},
        schema={"symbol": pl.String, "asof_ts": pl.Int64, "src_max_ts": pl.Int64, "close": pl.Float64},
    )


class ListFeed:
    def __init__(self, records=()):
        self.records = list(records)

    def settlements(self, symbol, lo, hi):
        return [r for r in self.records if r.symbol == symbol and lo <= r.funding_time < hi]


class MarketBarrier(ExecutionBarrier):
    """Исполняет market целиком (или долей fill_fraction) в exec_ts по последнему close строго раньше exec_ts."""

    def __init__(self, frames, funding=None, start=0, fill_fraction=1.0, reject=()):
        super().__init__(CFG, funding or ListFeed(), start)
        self._px = {}
        for df in frames:
            for sym, part in df.group_by("symbol"):
                part = part.sort("asof_ts")
                self._px[sym[0]] = (part["asof_ts"].to_list(), part["close"].to_list())
        self._queue, self._fraction, self._reject = [], fill_fraction, set(reject)

    def _accept(self, ticket):
        self._queue.append(ticket)

    def _match(self, lo, hi):
        out = []
        for t in sorted(self._queue, key=lambda t: (t.exec_ts, t.ticket_id)):
            if not lo <= t.exec_ts < hi:
                continue
            qty = t.intent.qty
            if t.intent.symbol in self._reject:
                out.append(OrderClosed(t.ticket_id, t.exec_ts, qty, CloseReason.REJECTED))
                continue
            times, closes = self._px[t.intent.symbol]
            price = closes[bisect_left(times, t.exec_ts) - 1]
            out.append(RawFill(t.ticket_id, t.exec_ts, qty * self._fraction, price, Liquidity.TAKER))
            if self._fraction < 1.0:
                out.append(OrderClosed(t.ticket_id, t.exec_ts, qty * (1 - self._fraction), CloseReason.NO_LIQUIDITY))
        return out


class Script(Strategy):
    """plan: {(asof_ts, symbol): [OrderRequest]}. Запоминает контексты и события."""

    def __init__(self, plan):
        self.plan, self.contexts, self.events = plan, [], []

    def decide(self, ctx: DecisionContext):
        self.contexts.append(ctx)
        return self.plan.get((ctx.ts, ctx.symbol), [])

    def on_event(self, event):
        self.events.append(event)


def buy(qty, symbol=BTC):
    return OrderRequest(symbol, Side.BUY, qty)


def sell(qty, symbol=BTC):
    return OrderRequest(symbol, Side.SELL, qty)


def run(rows, plan, guards=(), *, equity=10_000.0, barrier_kw=None, funding=None, strategy=None):
    df = frame(rows)
    barrier = MarketBarrier([df], funding=funding, **(barrier_kw or {}))
    strategy = strategy or Script(plan)
    engine = BacktestEngine(barrier, strategy, guards, initial_equity=equity)
    return engine.run(df), strategy, barrier


def own(res):
    """Ордера стратегии: без принудительного выкупа позиций в конце данных."""
    return res.orders.filter(~pl.col("forced"))


def curve(result, col):
    return result.equity_curve[col].to_list()


# ----------------------------------------------------------------- PnL и деньги


def test_round_trip_pnl_fees_and_equity_curve():
    rows = [(1_000, 100.0), (61_000, 100.0), (121_000, 110.0), (181_000, 110.0)]
    res, _, _ = run(rows, {(1_000, BTC): [buy(1.0)], (121_000, BTC): [sell(1.0)]})
    fee_in, fee_out = 100 * TAKER, 110 * TAKER  # 0.05 и 0.055
    # точки: 1000 (ордер ещё не исполнен), 61000 (long 1 @100), 121000 (mark 110), 181000 (закрыто), финальная
    assert curve(res, "ts")[:4] == [1_000, 61_000, 121_000, 181_000]
    assert curve(res, "equity")[:4] == pytest.approx([10_000.0, 10_000 - fee_in, 10_000 - fee_in + 10, 10_000 + 10 - fee_in - fee_out])
    assert curve(res, "unrealized_pnl")[:4] == pytest.approx([0.0, 0.0, 10.0, 0.0])
    assert curve(res, "realized_pnl")[3] == pytest.approx(10.0)
    assert res.total_fees == pytest.approx(fee_in + fee_out)
    assert res.final_equity == pytest.approx(10_000 + 10 - 0.105)
    assert res.total_pnl == pytest.approx(9.895)
    assert res.fills["fee"].to_list() == pytest.approx([fee_in, fee_out])


def test_order_log_has_barrier_stamped_exec_ts():
    res, _, _ = run([(1_000, 100.0), (61_000, 100.0)], {(1_000, BTC): [buy(1.0)]})
    row = res.orders.row(0, named=True)
    assert (row["asof_ts"], row["exec_ts"], row["status"], row["filled_qty"]) == (1_000, 1_000 + LAT, "filled", 1.0)
    assert res.fills["ts"].to_list()[0] == 1_000 + LAT


def test_order_does_not_affect_equity_at_its_own_decision_time():
    res, strategy, _ = run([(1_000, 100.0), (61_000, 100.0)], {(1_000, BTC): [buy(1.0)]})
    first = strategy.contexts[0]
    assert first.positions == {} and first.equity == 10_000.0
    assert strategy.contexts[1].positions == {BTC: 1.0}  # исполнение (1250) видно со следующего шага


def test_short_position_pnl():
    rows = [(1_000, 100.0), (61_000, 90.0)]
    res, _, _ = run(rows, {(1_000, BTC): [sell(2.0)]})
    # short 2 @100, mark 90 -> +20; комиссия 2*100*0.0005 = 0.1
    assert curve(res, "unrealized_pnl")[1] == pytest.approx(20.0)
    assert curve(res, "equity")[1] == pytest.approx(10_000 + 20 - 0.1)


def test_flip_and_average_entry_price():
    rows = [(1_000, 100.0), (61_000, 100.0), (121_000, 110.0), (181_000, 110.0), (241_000, 100.0), (301_000, 100.0)]
    plan = {(1_000, BTC): [buy(1.0)], (61_000, BTC): [buy(1.0)],  # две покупки по 100 -> long 2 @100
            (121_000, BTC): [sell(5.0)],  # продажа 5 @110: закрыли 2 (+20), открыт short 3 @110
            (181_000, BTC): [], (241_000, BTC): [buy(3.0)]}  # покупка 3 @100: закрыли short, +30
    res, _, barrier = run(rows, plan)
    assert curve(res, "realized_pnl")[-1] == pytest.approx(20.0 + 30.0)
    assert res.fills["qty"].to_list() == [1.0, 1.0, 5.0, 3.0]
    assert curve(res, "unrealized_pnl")[-1] == 0.0 and barrier.position(BTC) == 0.0
    fees = TAKER * (100 + 100 + 5 * 110 + 3 * 100)
    assert res.final_equity == pytest.approx(10_000 + 50 - fees)


def test_funding_is_taken_from_barrier_settlements():
    rows = [(1_000, 100.0), (60_000, 100.0), (6_000_000, 100.0)]
    feed = ListFeed([FundingRecord(BTC, 5_000_000, 0.0001, 100.0)])
    res, strategy, _ = run(rows, {(1_000, BTC): [buy(1.0)]}, funding=feed)
    # long 1 @ mark 100, ставка 0.01% -> платим 0.01
    assert res.funding.row(0, named=True) == {
        "funding_time": 5_000_000, "symbol": BTC, "position_qty": 1.0, "mark_price": 100.0,
        "funding_rate": 0.0001, "funding_fee": pytest.approx(0.01),
    }
    assert curve(res, "funding")[1] == 0.0 and curve(res, "funding")[2] == pytest.approx(0.01)
    assert res.total_funding == pytest.approx(0.01)
    assert res.final_equity == pytest.approx(10_000 - 2 * 100 * TAKER - 0.01)  # вход + выкуп в конце данных
    assert any(type(e).__name__ == "FundingSettlement" for e in strategy.events)


def test_short_receives_funding():
    rows = [(1_000, 100.0), (6_000_000, 100.0)]
    feed = ListFeed([FundingRecord(BTC, 5_000_000, 0.0001, 100.0)])
    res, _, _ = run(rows, {(1_000, BTC): [sell(1.0)]}, funding=feed)
    assert res.total_funding == pytest.approx(-0.01)


def test_order_at_last_row_is_filled_in_final_flush():
    res, _, barrier = run([(1_000, 100.0)], {(1_000, BTC): [buy(2.0)]})
    assert res.orders["status"].to_list() == ["filled", "filled"] and res.orders["forced"].to_list() == [False, True]
    assert res.equity_curve["ts"][-1] == 1_000 + LAT + 1
    # вход 2 @100 исполнен, позиция выкуплена в конце данных по последней цене (100): две комиссии, PnL 0
    assert barrier.position(BTC) == 0.0
    assert res.final_equity == pytest.approx(10_000 - 2 * 2 * 100 * TAKER)


def test_no_orders_gives_flat_curve():
    res, _, _ = run([(1_000, 100.0), (61_000, 101.0)], {})
    assert set(curve(res, "equity")) == {10_000.0} and res.orders.height == 0 and res.max_drawdown == 0.0
    assert res.fills.schema == pl.DataFrame(schema={"ts": pl.Int64}).schema | res.fills.schema  # схема стабильна


def test_max_drawdown_of_curve():
    rows = [(1_000, 100.0), (61_000, 100.0), (121_000, 90.0), (181_000, 95.0)]
    res, _, _ = run(rows, {(1_000, BTC): [buy(10.0)]})
    # пик - стартовые 10000 (до комиссии); минимум 10000 - 0.5 - 100 = 9899.5 при цене 90
    assert res.max_drawdown == pytest.approx(100.5 / 10_000)


def test_multi_symbol_rows_with_same_ts_make_one_step_and_one_curve_point():
    a, b = frame([(1_000, 100.0), (61_000, 100.0)], BTC), frame([(1_000, 10.0), (61_000, 12.0)], "ETHUSDT")
    df = pl.concat([a, b])
    barrier = MarketBarrier([df])
    strategy = Script({(1_000, "ETHUSDT"): [buy(10.0, "ETHUSDT")], (1_000, BTC): [sell(1.0)]})
    res = BacktestEngine(barrier, strategy, initial_equity=10_000.0).run(df)
    assert curve(res, "ts")[:2] == [1_000, 61_000]
    # ETH long 10 @10, mark 12 -> +20; BTC short 1 @100, mark 100 -> 0
    assert curve(res, "unrealized_pnl")[1] == pytest.approx(20.0)
    assert curve(res, "gross_exposure")[1] == pytest.approx(10 * 12 + 100)
    assert [(c.ts, c.symbol) for c in strategy.contexts] == [(1_000, BTC), (1_000, "ETHUSDT"), (61_000, BTC), (61_000, "ETHUSDT")]


# ------------------------------------------------------------------ отказы барьера


def test_barrier_rejection_is_logged_and_frees_pending():
    rows = [(1_000, 100.0), (61_000, 100.0)]
    res, strategy, _ = run(rows, {(1_000, BTC): [buy(1.0)]}, barrier_kw=dict(reject={BTC}))
    assert res.orders["status"].to_list() == ["rejected"] and res.orders["filled_qty"].to_list() == [0.0]
    assert res.fills.height == 0 and res.final_equity == 10_000.0
    assert strategy.contexts[1].pending == {} and strategy.contexts[1].positions == {}
    assert [type(e).__name__ for e in strategy.events] == ["OrderClosed"]


def test_partial_fill_remainder_is_closed_and_not_counted_as_pending():
    rows = [(1_000, 100.0), (61_000, 100.0)]
    res, strategy, barrier = run(rows, {(1_000, BTC): [buy(2.0)]}, barrier_kw=dict(fill_fraction=0.5))
    assert res.orders.row(0, named=True)["status"] == "no_liquidity"
    assert own(res)["filled_qty"].to_list() == [1.0]
    assert strategy.contexts[1].positions == {BTC: 1.0} and strategy.contexts[1].pending == {}


# --------------------------------------------------------------- риск в движке


def test_max_exposure_guard_sees_orders_pending_in_the_same_decision():
    rows = [(1_000, 100.0), (61_000, 100.0)]
    res, _, _ = run(rows, {(1_000, BTC): [buy(60.0), buy(60.0)]}, [MaxExposureGuard(1.0)])
    # лимит 10_000 / 100 = 100: вторая заявка урезана 60 -> 40
    assert own(res)["qty"].to_list() == [60.0, 40.0]
    assert res.risk_events.row(0, named=True) == {
        "ts": 1_000, "guard": "max_exposure", "symbol": BTC, "requested_qty": 60.0,
        "allowed_qty": 40.0, "reason": res.risk_events["reason"][0],
    }


def test_guard_chain_passes_clipped_qty_to_next_guard_and_stops_on_zero():
    seen = []

    class Spy(RiskGuard):
        name = "spy"

        def check(self, request, ctx):
            seen.append(request.qty)
            return RiskDecision.allow(request.qty)

    rows = [(1_000, 100.0), (61_000, 100.0)]
    res, _, _ = run(rows, {(1_000, BTC): [buy(500.0)], (61_000, BTC): [buy(500.0)]}, [MaxExposureGuard(1.0), Spy()])
    assert seen == [100.0]  # второй запрос заблокирован первым гуардом (позиция уже на лимите), до Spy не дошёл
    assert own(res)["qty"].to_list() == [100.0]
    assert res.risk_events["allowed_qty"].to_list() == [100.0, 0.0]


def test_guard_returning_more_than_requested_is_a_bug():
    class Greedy(RiskGuard):
        def check(self, request, ctx):
            return RiskDecision.allow(request.qty * 2)

    with pytest.raises(BacktestError, match="недопустимое решение"):
        run([(1_000, 100.0)], {(1_000, BTC): [buy(1.0)]}, [Greedy()])


def drawdown_scenario(guard):
    t0, t1, t2, t3, t4 = 10 * H, 11 * H, 12 * H, DAY_MS - MIN, DAY_MS
    rows = [(t0, 100.0), (t1, 100.0), (t2, 40.0), (t3, 40.0), (t4, 40.0), (t4 + MIN, 40.0)]
    plan = {
        (t0, BTC): [buy(10.0)],  # long 10 @100
        (t2, BTC): [buy(5.0)],  # просадка 6% -> блок
        (t3, BTC): [sell(4.0), buy(2.0)],  # закрытие проходит, открытие (с учётом ожидающей продажи) - блок
        (t4, BTC): [buy(1.0)],  # новые сутки UTC
    }
    return (t0, t1, t2, t3, t4), run(rows, plan, [guard])


def test_drawdown_limit_blocks_openings_for_the_rest_of_the_day_and_reacts_in_engine():
    guard = DrawdownLimitGuard(0.05)
    (t0, t1, t2, t3, t4), (res, strategy, barrier) = drawdown_scenario(guard)
    # long 10 @100, комиссия 0.5; при цене 40: нереализованный -600 -> эквити 9399.5, просадка от пика 10000 = 6.0%
    assert curve(res, "equity")[2] == pytest.approx(10_000 - 0.5 - 600)
    assert [t.ts for t in guard.triggers] == [t2]
    assert guard.triggers[0].drawdown == pytest.approx(600.5 / 10_000)
    # заблокированы обе открывающие заявки дня, закрывающая прошла, в новые сутки покупка снова разрешена
    assert res.risk_events.select("ts", "guard", "requested_qty", "allowed_qty").rows() == [
        (t2, "drawdown_limit", 5.0, 0.0), (t3, "drawdown_limit", 2.0, 0.0),
    ]
    assert own(res).select("asof_ts", "side", "qty", "status").rows() == [
        (t0, "buy", 10.0, "filled"), (t3, "sell", 4.0, "filled"), (t4, "buy", 1.0, "filled"),
    ]
    assert strategy.contexts[-1].positions[BTC] == pytest.approx(7.0)  # 10 - 4 + 1: позиции гуард сам не закрывает
    # стратегия видела блок только как отсутствие ордера, а не как исключение
    assert [c.ts for c in strategy.contexts][:3] == [t0, t1, t2]


def test_drawdown_limit_run_reference_keeps_trading_halted_on_the_next_day():
    guard = DrawdownLimitGuard(0.05, reference="run")
    (t0, t1, t2, t3, t4), (res, _, _) = drawdown_scenario(guard)
    assert [t.ts for t in guard.triggers] == [t2, t4]
    assert own(res)["asof_ts"].to_list() == [t0, t3]  # покупка в новые сутки заблокирована: пик прежний
    assert res.risk_events["ts"].to_list() == [t2, t3, t4]


def test_guard_state_is_reset_for_each_run():
    guard = DrawdownLimitGuard(0.05)
    drawdown_scenario(guard)
    assert len(guard.triggers) == 1
    drawdown_scenario(guard)
    assert len(guard.triggers) == 1  # не накопилось от прошлого прогона


# ------------------------------------------------------------- строгость входа


def test_decisions_with_labels_are_rejected():
    df = frame([(1_000, 100.0)]).with_columns(y_fwd_log_ret_60000ms=pl.lit(0.01))
    with pytest.raises(BacktestError, match="метки"):
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0).run(df)


def test_decisions_without_provenance_are_rejected():
    df = frame([(1_000, 100.0), (61_000, 100.0)])
    with pytest.raises(LookaheadViolation):
        bad = df.with_columns(src_max_ts=pl.col("asof_ts"))  # источник известен не раньше решения
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0).run(bad)
    with pytest.raises(LookaheadViolation):
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0).run(df.drop("src_max_ts"))
    with pytest.raises(LookaheadViolation):  # extra *_ts колонки проверяются тоже
        late = df.with_columns(funding_avail_ts=pl.col("asof_ts") + 5)
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0).run(late)
    with pytest.raises(LookaheadViolation):  # дубль решения: сетка сломана
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0).run(pl.concat([df, df]))
    # проверку можно отключить осознанно
    ok = BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0, verify_provenance=False)
    assert ok.run(df.drop("src_max_ts")).equity_curve.height >= 1


def test_missing_columns_and_bad_config():
    df = frame([(1_000, 100.0)])
    with pytest.raises(BacktestError, match="нет колонок"):
        BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0, price_col="mid").run(df)
    for bad in (0, -1.0, float("nan"), True):
        with pytest.raises(ConfigError):
            BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=bad)
    with pytest.raises(ConfigError):
        BacktestEngine(MarketBarrier([df]), object(), initial_equity=1.0)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        BacktestEngine(MarketBarrier([df]), Script({}), [object()], initial_equity=1.0)  # type: ignore[list-item]


def test_engine_is_single_use_and_barrier_clock_is_checked():
    df = frame([(1_000, 100.0)])
    engine = BacktestEngine(MarketBarrier([df]), Script({}), initial_equity=1.0)
    engine.run(df)
    with pytest.raises(BacktestError, match="одноразовый"):
        engine.run(df)
    with pytest.raises(BacktestError, match="позже первого решения"):
        BacktestEngine(MarketBarrier([df], start=5_000), Script({}), initial_equity=1.0).run(df)


def test_strategy_must_return_order_requests_and_sees_only_current_row():
    df = frame([(1_000, 100.0), (61_000, 101.0)])

    class Wrong(Strategy):
        def decide(self, ctx):
            return [("buy", 1.0)]

    with pytest.raises(BacktestError, match="OrderRequest"):
        BacktestEngine(MarketBarrier([df]), Wrong(), initial_equity=1.0).run(df)

    strategy = Script({})
    BacktestEngine(MarketBarrier([df]), strategy, initial_equity=1.0).run(df)
    first = strategy.contexts[0]
    assert first.row["close"] == 100.0 and dict(first.marks) == {BTC: 100.0}  # цены 101.0 из будущего нет
    assert not hasattr(first, "frame") and not hasattr(first, "decisions")
    with pytest.raises(TypeError):
        first.row["close"] = 0.0  # type: ignore[index]


def test_request_for_symbol_without_price_is_dropped_and_logged():
    res, _, _ = run([(1_000, 100.0)], {(1_000, BTC): [buy(1.0, "ETHUSDT")]})
    assert res.orders.height == 0
    assert res.risk_events.row(0, named=True)["guard"] == "engine"


def test_engine_run_is_deterministic():
    rows = [(1_000, 100.0), (61_000, 103.0), (121_000, 99.0), (181_000, 101.0)]
    plan = {(1_000, BTC): [buy(1.0)], (121_000, BTC): [sell(2.0)]}
    a, _, _ = run(rows, plan)
    b, _, _ = run(rows, plan)
    assert a.equity_curve.equals(b.equity_curve) and a.fills.equals(b.fills)


# ============================================================================== сквозной прогон
# Синтетический рынок -> конвейер признаков -> BaselineStrategy -> BacktestEngine + OrderBookBarrier.
# Это проверка ПРОВОДКИ (время, учёт, исполнение по стакану), а не торговой идеи: цена - синтетические
# тренды, стакан смещён в сторону тренда, поэтому PnL ничего не говорит о реальном edge.

START = 28_333_334 * MIN  # кратно минуте
N_MIN = 360
LEVELS = 5
TIMING = TimingConfig()
FUNDING = [FundingRecord(BTC, START + 200 * MIN + 5, 0.0001, 100.0)]


def synthetic_market(seed: int = 11) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Свечи 1m и стакан раз в секунду (top-5). Режим тренда (+/-) меняется каждые 90 минут."""
    rng = random.Random(seed)
    px, regime = 100.0, 1
    candles, regimes = [], []
    for m in range(N_MIN):
        if m and m % 90 == 0:
            regime = -regime
        o = px
        px *= math.exp(regime * 0.0004 + rng.gauss(0, 0.0004))
        candles.append({
            "symbol": BTC, "open_time": START + m * MIN, "open": o, "high": max(o, px) * 1.0001,
            "low": min(o, px) * 0.9999, "close": px, "volume": 10.0 + rng.random(),
        })
        regimes.append(regime)

    rows, u = [], 0
    for s in range((N_MIN + 3) * 60):  # стакан живёт и после последней свечи: ордерам в конце данных есть против чего исполняться
        m = min(s // 60, N_MIN - 1)
        c = candles[m]
        mid = c["open"] + (c["close"] - c["open"]) * min((s % 60 + 1) / 60, 1.0) if s // 60 < N_MIN else c["close"]
        recv = START + s * 1000 + 20
        u += 1
        row = {"recv_ts": recv, "symbol": BTC, "E": recv - 100, "T": recv - 100, "u": u, "pu": u - 1}
        for i in range(LEVELS):
            row[f"bid_px_{i}"], row[f"ask_px_{i}"] = mid - 0.05 - 0.1 * i, mid + 0.05 + 0.1 * i
            row[f"bid_qty_{i}"] = max(5.0 * (1 + 0.4 * regimes[m] + 0.3 * rng.uniform(-1, 1)), 0.5)
            row[f"ask_qty_{i}"] = max(5.0 * (1 - 0.4 * regimes[m] + 0.3 * rng.uniform(-1, 1)), 0.5)
        rows.append(row)
    return (
        pl.DataFrame(candles, schema={"symbol": pl.String, "open_time": pl.Int64, "open": pl.Float64, "high": pl.Float64,
                                      "low": pl.Float64, "close": pl.Float64, "volume": pl.Float64}),
        pl.DataFrame(rows, schema=depth_schema(LEVELS)),
    )


class TrendFeatures(Feature):
    name = "trend"

    def __init__(self, fast: int, slow: int):
        self.fast, self.slow = fast, slow

    def build(self, fb):
        close = fb.col("close")
        return {"ma_fast": close.rolling_mean(self.fast), "ma_slow": close.rolling_mean(self.slow)}


class BookImbalance(Feature):
    name = "book_imbalance"

    def build(self, fb):
        bid, ask = fb.col("bid_depth"), fb.col("ask_depth")
        return {"obi": (bid - ask) / (bid + ask)}  # [-1, 1]; > 0 - давление покупателей


def build_decisions(candles: pl.DataFrame, book: pl.DataFrame) -> pl.DataFrame:
    """Конвейер этапа 2: водяной знак -> доступность свечей -> сетка решений -> стакан -> признаки."""
    clock, aligner = WatermarkClock(TIMING), TimelineAligner(TIMING)
    avail = clock.candle_availability(candles.lazy(), MIN, clock.watermark(book.lazy()))
    decisions = aligner.decisions(avail, MIN)
    last = lambda side: pl.sum_horizontal([pl.col(f"{side}_qty_{i}") for i in range(LEVELS)]).last()  # noqa: E731
    decisions = aligner.attach_book(decisions, book.lazy(), [
        pl.col("bid_px_0").last().alias("bid_px_0"), pl.col("ask_px_0").last().alias("ask_px_0"),
        last("bid").alias("bid_depth"), last("ask").alias("ask_depth"),
    ])
    compiled = compile_features([TrendFeatures(5, 20), BookImbalance()], source_columns(decisions))
    return apply_features(decisions, compiled).collect()


def run_baseline(decisions: pl.DataFrame, book: pl.DataFrame):
    barrier = OrderBookBarrier(CFG, ListFeed(FUNDING), 0, book)
    engine = BacktestEngine(
        barrier, BaselineStrategy(obi_threshold=0.05),
        [MaxExposureGuard(1.0), DrawdownLimitGuard(0.2)], initial_equity=10_000.0,
    )
    return engine.run(decisions), barrier


def test_end_to_end_pipeline_baseline_strategy_real_orderbook_barrier(capsys):
    candles, book = synthetic_market()
    decisions = build_decisions(candles, book)

    # конвейер: каждая свеча стала решением, прогрев признаков - null (а не ноль), стакан свежий
    assert decisions.height == N_MIN and decisions["book_ok"].all()
    assert decisions["ma_slow"].null_count() == 19 and decisions["ma_fast"].null_count() == 4
    assert decisions["obi"].is_between(-1, 1).all()
    assert (decisions["src_max_ts"] < decisions["asof_ts"]).all()  # ни одной строки со знанием из будущего

    res, barrier = run_baseline(decisions, book)

    # стратегия торговала, ордера исполнены по стакану, закрытия лишних остатков журналируются
    own_orders = own(res)
    assert own_orders.height > 2 and res.fills.height >= own_orders.height
    assert set(res.fills["liquidity"]) == {"taker"}  # BaselineStrategy подаёт только market
    assert barrier.position(BTC) == 0.0  # конец данных: позиция выкуплена, ничего не висит
    assert res.equity_curve["unrealized_pnl"][-1] == 0.0

    # время: ордер стратегии исполняется ровно в exec_ts = asof_ts + order_latency_ms (market), не раньше;
    # принудительный выкуп в конце данных создаёт барьер: у него нет задержки (asof_ts = exec_ts = конец данных)
    joined = res.fills.join(res.orders.select("ticket_id", "asof_ts", "exec_ts", "forced"), on="ticket_id")
    mine, forced = joined.filter(~pl.col("forced")), joined.filter(pl.col("forced"))
    assert (mine["exec_ts"] == mine["asof_ts"] + LAT).all() and (mine["ts"] == mine["exec_ts"]).all()
    assert (forced["ts"] == forced["exec_ts"]).all() and forced.height <= 1

    # деньги: независимый пересчёт из журнала исполнений. Позиция плоская -> касса и есть валовый PnL.
    sign = pl.when(pl.col("side") == "buy").then(1.0).otherwise(-1.0)
    gross = float(res.fills.select((-sign * pl.col("qty") * pl.col("price")).sum()).item())
    fees = float(res.fills["fee"].sum())
    funding = float(res.funding["funding_fee"].sum())
    assert res.total_fees == pytest.approx(fees) and res.total_funding == pytest.approx(funding)
    assert res.total_pnl == pytest.approx(gross - fees - funding)
    assert res.fills.select(((sign * pl.col("qty")).sum())).item() == pytest.approx(0.0, abs=1e-9)

    # детерминизм: тот же вход -> та же кривая капитала
    again, _ = run_baseline(decisions, book)
    assert again.equity_curve.equals(res.equity_curve) and again.fills.equals(res.fills)

    with capsys.disabled():
        print(
            f"\n--- сквозной прогон: {N_MIN} баров 1m, {own_orders.height} ордеров стратегии, {res.fills.height} исполнений ---"
            f"\n  стартовый капитал : {res.initial_equity:12.2f}"
            f"\n  валовый PnL       : {gross:12.2f}"
            f"\n  комиссии (taker)  : {-fees:12.2f}"
            f"\n  фандинг           : {-funding:12.2f}"
            f"\n  ИТОГО PnL         : {res.total_pnl:12.2f}  ({res.total_pnl / res.initial_equity:+.2%})"
            f"\n  итоговый капитал  : {res.final_equity:12.2f}"
            f"\n  макс. просадка    : {res.max_drawdown:12.2%}"
            f"\n  урезано риском    : {res.risk_events.height}"
            f"\n  частично исполнено (остаток IOC закрыт no_liquidity): {int((res.orders['status'] == 'no_liquidity').sum())}"
        )


def test_walkforward_folds_drive_oos_backtests_on_pipeline_frame(capsys):
    """Purged Walk-Forward поверх готовой таблицы решений: каждый тестовый фолд - свой чистый прогон движка.

    BaselineStrategy ничего не обучает, поэтому train здесь только проверяется на зазор (purge); на реальной
    модели именно по train подбираются параметры, а метрики берутся с test.
    """
    candles, book = synthetic_market()
    decisions = build_decisions(candles, book)  # признаки считаются по ВСЕЙ истории (каузально), затем режем
    gen = PurgedWalkForward(3, 60, label_horizon_bars=5, purge_bars=5, min_train_size=100)

    pnl = []
    for ff in gen.split_frame(decisions):
        assert ff.train["asof_ts"].max() < ff.test["asof_ts"].min()
        assert ff.train["asof_ts"].n_unique() == ff.fold.n_train and ff.test.height == 60
        res, barrier = run_baseline(ff.test, book)
        assert res.equity_curve["ts"][0] == ff.test["asof_ts"][0] and barrier.position(BTC) == 0.0
        pnl.append(res.total_pnl)

    assert len(pnl) == 3
    with capsys.disabled():
        print("\n--- walk-forward OOS: PnL по фолдам " + ", ".join(f"{p:+.2f}" for p in pnl) + f" | сумма {sum(pnl):+.2f}")
