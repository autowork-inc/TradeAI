"""OrderBookBarrier: проход по стакану (VWAP), пессимистичные LIMIT, синхронизация, конец данных.

Все ожидаемые числа посчитаны вручную в комментариях. Время: LAT = 250 мс, поэтому ордер с
asof_ts = 1000 приходит на биржу в exec_ts = 1250.
"""

import polars as pl
import pytest

from backtester.barrier import (
    BookSnapshot,
    FrameBookFeed,
    OrderBookBarrier,
    Sweep,
    crossing_levels,
    walk_book,
)
from backtester.contracts import (
    CloseReason,
    ContractViolation,
    ExecutionConfig,
    Fill,
    FundingCoverageError,
    Liquidity,
    OrderClosed,
    OrderIntent,
    OrderType,
    Side,
)
from backtester.engine import BacktestEngine, DecisionContext, Strategy
from backtester.risk import DAY_MS, OrderRequest
from pipeline.loaders.schemas import depth_schema
from utils.errors import ConfigError, DataError

LAT, MAKER, TAKER = 250, 0.0002, 0.0005
CFG = ExecutionConfig(LAT, MAKER, TAKER)
BTC, ETH = "BTCUSDT", "ETHUSDT"
LEVELS = 5
ASOF, EXEC = 1_000, 1_000 + LAT


# ------------------------------------------------------------------- хелперы


class ListFeed:
    def __init__(self, records=()):
        self.records = list(records)

    def settlements(self, symbol, lo, hi):
        return [r for r in self.records if r.symbol == symbol and lo <= r.funding_time < hi]


class StrictFeed(ListFeed):
    """Как настоящий фид: окно за границей покрытия данных - FundingCoverageError."""

    def __init__(self, covered_until):
        super().__init__()
        self.covered_until = covered_until

    def settlements(self, symbol, lo, hi):
        if hi > self.covered_until:
            raise FundingCoverageError(f"фандинг известен до {self.covered_until}, запрошено до {hi}")
        return super().settlements(symbol, lo, hi)


def msg(recv_ts, bids=(), asks=(), symbol=BTC, u=None):
    """Сообщение стакана; bids/asks - [(цена, объём)] от лучшей цены. Уровни сверх списка - null."""
    u = u if u is not None else recv_ts
    row = {"recv_ts": recv_ts, "symbol": symbol, "E": recv_ts, "T": recv_ts, "u": u, "pu": u - 1}
    for i in range(LEVELS):
        for side, levels in (("bid", bids), ("ask", asks)):
            row[f"{side}_px_{i}"], row[f"{side}_qty_{i}"] = levels[i] if i < len(levels) else (None, None)
    return row


def book(*rows) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=depth_schema(LEVELS))


def make(*rows, **kw) -> OrderBookBarrier:
    return OrderBookBarrier(CFG, kw.pop("funding", ListFeed()), 0, book(*rows), **kw)


def market(side, qty, asof=ASOF, symbol=BTC) -> OrderIntent:
    return OrderIntent(symbol, side, qty, asof)


def limit(side, qty, price, ttl=1_000, asof=ASOF) -> OrderIntent:
    return OrderIntent(BTC, side, qty, asof, order_type=OrderType.LIMIT, limit_price=price, ttl_ms=ttl)


def fills(events):
    return [e for e in events if isinstance(e, Fill)]


def closes(events):
    return [e for e in events if isinstance(e, OrderClosed)]


def kinds(events):
    return [type(e).__name__ for e in events]


# --------------------------------------------------- MARKET: walking the book, VWAP


def test_market_buy_eats_three_levels_and_pays_exact_vwap():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 1), (101, 2), (102, 3)]))
    b.submit(market(Side.BUY, 4))
    events = b.advance(5_000)
    # 1 @100 + 2 @101 + 1 @102 = 100 + 202 + 102 = 404 -> VWAP = 404 / 4 = 101
    (fill,) = events
    assert isinstance(fill, Fill) and fill.qty == 4 and fill.price == pytest.approx(101.0)
    assert fill.liquidity is Liquidity.TAKER and fill.ts == EXEC
    assert fill.notional == pytest.approx(404.0)
    assert fill.fee == pytest.approx(404.0 * TAKER)  # комиссия по номиналу = сумма комиссий по уровням
    assert b.position(BTC) == 4 and b.open_ticket_ids() == ()


def test_market_sell_walks_the_bids_downwards():
    b = make(msg(EXEC, bids=[(99, 1), (98, 2), (97, 5)], asks=[(100, 5)]))
    b.submit(market(Side.SELL, 3.5))
    (fill,) = b.advance(5_000)
    # 1 @99 + 2 @98 + 0.5 @97 = 99 + 196 + 48.5 = 343.5 -> VWAP = 343.5 / 3.5
    assert fill.qty == 3.5 and fill.price == pytest.approx(343.5 / 3.5) and fill.price == pytest.approx(98.142857, abs=1e-6)
    assert b.position(BTC) == -3.5


def test_vwap_is_best_price_when_first_level_is_deep_enough():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 1), (101, 9)]))
    b.submit(market(Side.BUY, 0.5))
    (fill,) = b.advance(5_000)
    assert fill.price == 100.0  # никакой константы проскальзывания: ровно ask_px_0


def test_vwap_stops_at_the_level_that_completes_the_order():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 1), (101, 2), (105, 9)]))
    b.submit(market(Side.BUY, 3))
    (fill,) = b.advance(5_000)
    assert fill.price == pytest.approx((100 + 202) / 3)  # дорогой третий уровень не тронут


def test_market_exhausting_all_levels_is_partial_ioc_with_vwap_of_what_was_collected():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 1), (101, 2), (102, 3)]))
    ticket = b.submit(market(Side.BUY, 10))
    events = b.advance(5_000)
    # глубина 1 + 2 + 3 = 6: 100 + 202 + 306 = 608 -> VWAP = 608 / 6; остаток 4 отклонён
    assert kinds(events) == ["Fill", "OrderClosed"]
    fill, closed = events
    assert fill.qty == 6 and fill.price == pytest.approx(608 / 6)
    assert (closed.ticket_id, closed.ts, closed.reason) == (ticket.ticket_id, EXEC, CloseReason.NO_LIQUIDITY)
    assert closed.remaining_qty == 4
    assert b.position(BTC) == 6 and b.open_ticket_ids() == ()  # остаток не висит в книге


def test_market_with_empty_opposing_side_is_closed_without_fill():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[]))
    b.submit(market(Side.BUY, 1))
    events = b.advance(5_000)
    assert kinds(events) == ["OrderClosed"] and events[0].remaining_qty == 1 and events[0].reason is CloseReason.NO_LIQUIDITY


def test_zero_volume_and_missing_levels_are_skipped():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 0), (101, 1), (102, 1)]))
    b.submit(market(Side.BUY, 2))
    (fill,) = b.advance(5_000)
    assert fill.price == pytest.approx(101.5)  # (101 + 102) / 2: уровень с нулевым объёмом не участвует


def test_book_levels_are_walked_from_best_price_even_if_stored_out_of_order():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(102, 3), (100, 1), (101, 2)]))
    b.submit(market(Side.BUY, 1))
    (fill,) = b.advance(5_000)
    assert fill.price == 100.0


def test_walk_book_pure_function():
    levels = [(100.0, 1.0), (101.0, 2.0), (102.0, 3.0)]
    assert walk_book(levels, 4.0) == Sweep(4.0, pytest.approx(101.0), 3, True)
    assert walk_book(levels, 1.0) == Sweep(1.0, 100.0, 1, True)
    assert walk_book(levels, 6.0).complete and walk_book(levels, 6.0).vwap == pytest.approx(608 / 6)
    partial = walk_book(levels, 7.0)
    assert (partial.qty, partial.complete, partial.levels) == (6.0, False, 3)
    assert walk_book([], 1.0) == Sweep(0.0, None, 0, False)


def test_walk_book_vwap_does_not_drift_on_many_small_levels():
    levels = [(100.0 + 0.01 * i, 0.1) for i in range(10)]  # 0.1 в сумме с накоплением ошибки
    sweep = walk_book(levels, 1.0)
    assert sweep.complete and sweep.qty == 1.0
    assert sweep.vwap == pytest.approx(sum(p for p, _ in levels) / 10, abs=1e-12)


def test_crossing_levels_is_a_prefix_of_the_book():
    asks = [(99.0, 1.0), (100.0, 2.0), (101.0, 3.0)]
    assert crossing_levels(asks, Side.BUY, 100.0) == ((99.0, 1.0), (100.0, 2.0))  # равенство - пересечение
    assert crossing_levels(asks, Side.BUY, 98.0) == ()
    bids = [(101.0, 1.0), (100.0, 2.0), (99.0, 3.0)]
    assert crossing_levels(bids, Side.SELL, 100.0) == ((101.0, 1.0), (100.0, 2.0))
    assert crossing_levels(bids, Side.SELL, 102.0) == ()


# ------------------------------------------------------------- синхронизация


@pytest.mark.parametrize(
    "messages, expected_ask",  # [(recv_ts, ask_px_0)]
    [
        ([(EXEC - 1, 10.0), (EXEC, 20.0), (EXEC + 1, 40.0)], 20.0),  # ровно в exec_ts - это «не раньше»: берём его
        ([(EXEC - 1, 10.0), (EXEC + 400, 30.0)], 30.0),  # в exec_ts сообщения нет: первое после, а не последнее до
        ([(EXEC - 500, 1.0), (EXEC - 1, 10.0), (EXEC + 1, 40.0)], 40.0),  # всё строго до exec_ts в игре не участвует
    ],
)
def test_market_executes_against_first_message_with_recv_ts_at_or_after_exec_ts(messages, expected_ask):
    b = make(*[msg(t, bids=[(1.0, 5)], asks=[(ask, 5)]) for t, ask in messages])
    b.submit(market(Side.BUY, 1))
    (fill,) = b.advance(5_000)
    assert fill.price == expected_ask
    assert fill.ts == EXEC  # время сделки по контракту - exec_ts, а не recv_ts сообщения


def test_market_never_uses_a_book_older_than_exec_ts():
    b = make(msg(EXEC - 1, bids=[(99, 5)], asks=[(100, 5)]))  # новее нет
    b.submit(market(Side.BUY, 1))
    events = b.advance(5_000)
    assert kinds(events) == ["OrderClosed"] and events[0].reason is CloseReason.NO_LIQUIDITY


def test_market_book_too_far_after_exec_ts_is_a_data_gap_not_a_price():
    rows = [msg(EXEC + 500, bids=[(99, 5)], asks=[(100, 5)])]
    ok = make(*rows, max_book_delay_ms=500)
    ok.submit(market(Side.BUY, 1))
    assert kinds(ok.advance(5_000)) == ["Fill"]  # ровно на границе допустимой задержки
    late = make(*rows, max_book_delay_ms=499)
    late.submit(market(Side.BUY, 1))
    assert kinds(late.advance(5_000)) == ["OrderClosed"]


def test_book_with_several_symbols_is_matched_per_symbol():
    b = make(msg(EXEC, bids=[(9, 5)], asks=[(10, 5)], symbol=ETH), msg(EXEC, bids=[(99, 5)], asks=[(100, 5)]))
    b.submit(market(Side.BUY, 1, symbol=ETH))
    b.submit(market(Side.BUY, 1, asof=ASOF))
    prices = {f.symbol: f.price for f in fills(b.advance(5_000))}
    assert prices == {ETH: 10.0, BTC: 100.0}


def test_unknown_symbol_is_a_config_error_and_does_not_consume_a_ticket_id():
    b = make(msg(EXEC, bids=[(99, 5)], asks=[(100, 5)]))
    with pytest.raises(ConfigError, match="ETHUSDT"):
        b.submit(market(Side.BUY, 1, symbol=ETH))
    assert b.submit(market(Side.BUY, 1)).ticket_id == 1


def test_book_feed_validates_its_input():
    with pytest.raises(ConfigError):
        FrameBookFeed(pl.DataFrame({"recv_ts": [1], "symbol": [BTC]}))
    with pytest.raises(DataError):
        FrameBookFeed(book(msg(1, asks=[(100, 1)])).with_columns(recv_ts=pl.lit(None, dtype=pl.Int64)))
    feed = FrameBookFeed(book(msg(10, bids=[(99, 1)], asks=[(100, 1)]), msg(20, bids=[(98, 1)], asks=[(101, 1)])))
    assert feed.first_at_or_after(BTC, 10) == BookSnapshot(10, ((99.0, 1.0),), ((100.0, 1.0),))
    assert feed.first_at_or_after(BTC, 11).recv_ts == 20 and feed.first_at_or_after(BTC, 21) is None
    assert [s.recv_ts for s in feed.messages(BTC, 10, 20)] == [10]  # [from, to)


# ------------------------------------------------------- LIMIT: пессимистичное исполнение

EXPIRE = EXEC + 1_000  # ttl 1000 по умолчанию


def test_buy_limit_is_not_filled_when_only_the_bid_reaches_the_limit():
    b = make(
        msg(1_300, bids=[(100, 9)], asks=[(101, 9)]),  # бид на нашей цене: нас «догнали» в очереди, но не купили
        msg(1_500, bids=[(100.5, 9)], asks=[(101, 9)]),  # бид выше нашей цены - тоже не повод
    )
    ticket = b.submit(limit(Side.BUY, 1, 100.0))
    events = b.advance(5_000)
    assert kinds(events) == ["OrderClosed"]
    assert (events[0].ts, events[0].reason, events[0].remaining_qty) == (EXPIRE, CloseReason.EXPIRED, 1)
    assert b.position(BTC) == 0 and ticket.expire_ts == EXPIRE


def test_buy_limit_fills_only_when_ask_px_0_drops_to_the_limit_inclusive():
    b = make(
        msg(1_300, bids=[(98, 9)], asks=[(100.01, 9)]),  # на копейку выше: не исполняется
        msg(1_500, bids=[(98, 9)], asks=[(100.0, 9)]),  # ask_px_0 == лимитка: исполняется
    )
    b.submit(limit(Side.BUY, 2, 100.0))
    (fill,) = b.advance(5_000)
    assert (fill.ts, fill.qty, fill.price, fill.liquidity) == (1_500, 2, 100.0, Liquidity.MAKER)
    assert fill.fee == pytest.approx(2 * 100 * MAKER)  # ждавший в книге ордер - мейкер
    assert b.open_ticket_ids() == ()


def test_resting_buy_limit_gets_its_own_price_never_a_better_one():
    b = make(
        msg(1_300, bids=[(98, 9)], asks=[(101, 9)]),
        msg(1_500, bids=[(96, 9)], asks=[(98.0, 9)]),  # рынок упал глубоко ниже лимитки
    )
    b.submit(limit(Side.BUY, 1, 100.0))
    (fill,) = b.advance(5_000)
    assert fill.price == 100.0  # а не 98: улучшения цены нет


def test_sell_limit_needs_the_bid_to_rise_to_the_limit_not_the_ask():
    b = make(
        msg(1_300, bids=[(99, 9)], asks=[(100, 9)]),  # аск на нашей цене - не повод
        msg(1_400, bids=[(99.5, 9)], asks=[(100.5, 9)]),
        msg(1_500, bids=[(100.0, 9)], asks=[(101, 9)]),  # bid_px_0 == лимитка
    )
    b.submit(limit(Side.SELL, 3, 100.0))
    (fill,) = b.advance(5_000)
    assert (fill.ts, fill.qty, fill.price, fill.liquidity, fill.side) == (1_500, 3, 100.0, Liquidity.MAKER, Side.SELL)
    assert b.position(BTC) == -3


def test_resting_limit_is_capped_by_crossing_depth_and_never_sells_the_same_liquidity_twice():
    b = make(
        msg(1_300, bids=[(98, 9)], asks=[(101, 9)]),  # приход: не пересекает, ордер встаёт в книгу
        msg(1_500, bids=[(98, 9)], asks=[(99, 1), (100, 3), (101, 9)]),  # пересекают 1 + 3 = 4
        msg(1_600, bids=[(98, 9)], asks=[(99, 1), (100, 3), (101, 9)]),  # те же уровни: новой ликвидности нет
        msg(1_700, bids=[(98, 9)], asks=[(101, 9)]),  # пересечение кончилось: счётчик сбрасывается
        msg(1_800, bids=[(98, 9)], asks=[(100, 5)]),  # новая ликвидность 5, нужно ещё 3
    )
    b.submit(limit(Side.BUY, 10, 100.0))
    events = b.advance(5_000)
    assert [(f.ts, f.qty, f.price) for f in fills(events)] == [(1_500, 4, 100.0), (1_800, 5, 100.0)]
    (closed,) = closes(events)
    assert (closed.ts, closed.reason, closed.remaining_qty) == (EXPIRE, CloseReason.EXPIRED, 1)  # 10 - 4 - 5
    assert b.position(BTC) == 9


def test_marketable_limit_at_arrival_is_taker_vwap_within_the_limit_and_remainder_rests():
    b = make(
        msg(1_260, bids=[(97, 9)], asks=[(99, 1), (100, 2), (101, 5)]),  # приход: пересекают 99 и 100 (3 шт.)
        msg(1_500, bids=[(97, 9)], asks=[(99, 1), (100, 2), (101, 5)]),  # то же самое: уже продано нам
        msg(1_700, bids=[(97, 9)], asks=[(102, 5)]),
        msg(1_800, bids=[(97, 9)], asks=[(100, 4)]),
    )
    b.submit(limit(Side.BUY, 5, 100.0))
    first, second = fills(b.advance(5_000))
    # TAKER: 1 @99 + 2 @100 = 299 -> VWAP 299 / 3 (не хуже лимитки 100); уровень 101 за лимиткой не трогаем
    assert (first.ts, first.qty, first.liquidity) == (1_260, 3, Liquidity.TAKER)
    assert first.price == pytest.approx(299 / 3) and first.price <= 100.0
    assert first.fee == pytest.approx(299 * TAKER)
    # остаток 2 дождался нового пересечения в 1800 (в 1500 те же 3 шт. не считаются) и ушёл как MAKER по лимитке
    assert (second.ts, second.qty, second.price, second.liquidity) == (1_800, 2, 100.0, Liquidity.MAKER)
    assert b.position(BTC) == 5 and b.open_ticket_ids() == ()


def test_late_first_message_is_a_gap_so_the_limit_waited_in_the_book_as_maker():
    rows = [msg(EXEC + 600, bids=[(97, 9)], asks=[(99, 5)])]  # единственное сообщение пришло через 600 мс
    fresh = make(*rows, max_book_delay_ms=600)
    fresh.submit(limit(Side.BUY, 1, 100.0))
    (taker,) = fills(fresh.advance(5_000))
    assert (taker.liquidity, taker.price) == (Liquidity.TAKER, 99.0)
    gap = make(*rows, max_book_delay_ms=599)
    gap.submit(limit(Side.BUY, 1, 100.0))
    (maker,) = fills(gap.advance(5_000))
    assert (maker.liquidity, maker.price) == (Liquidity.MAKER, 100.0)  # в дыре нас исполнил поток: по своей цене


def test_limit_lives_in_half_open_window_exec_ts_inclusive_expire_ts_exclusive():
    crossing = dict(bids=[(97, 9)], asks=[(99, 9)])
    b = make(
        msg(EXEC - 1, **crossing),  # до прихода ордера
        msg(EXPIRE, **crossing),  # ровно в момент истечения: ордер уже не жив
    )
    b.submit(limit(Side.BUY, 1, 100.0))
    events = b.advance(5_000)
    assert kinds(events) == ["OrderClosed"] and events[0].ts == EXPIRE and events[0].reason is CloseReason.EXPIRED
    on_arrival = make(msg(EXEC, **crossing))  # ровно в exec_ts - жив
    on_arrival.submit(limit(Side.BUY, 1, 100.0))
    (fill,) = on_arrival.advance(5_000)
    assert fill.ts == EXEC and fill.liquidity is Liquidity.TAKER


def test_limit_events_do_not_depend_on_how_time_is_sliced():
    rows = (
        msg(1_260, bids=[(97, 9)], asks=[(99, 1), (100, 2), (101, 5)]),
        msg(1_500, bids=[(97, 9)], asks=[(99, 1), (100, 2)]),
        msg(1_700, bids=[(97, 9)], asks=[(102, 5)]),
        msg(1_800, bids=[(97, 9)], asks=[(100, 1)]),
    )
    once = make(*rows)
    once.submit(limit(Side.BUY, 5, 100.0))
    expected = once.advance(5_000)
    assert kinds(expected) == ["Fill", "Fill", "OrderClosed"]
    for cuts in ([1_260, 1_261, 1_800, 1_801, 2_249, 2_250, 2_251], [1_600, 1_700, 3_000], list(range(1_251, 2_400, 7))):
        sliced = make(*rows)
        sliced.submit(limit(Side.BUY, 5, 100.0))
        got = [e for cut in [*cuts, 5_000] for e in sliced.advance(cut)]
        assert got == list(expected), cuts


# --------------------------------------------------------------- конец данных


def test_finalize_expires_live_orders_and_buys_back_positions_at_marks():
    b = make(
        msg(EXEC, bids=[(99, 9)], asks=[(100, 9)]),
        msg(2_000, bids=[(99, 9)], asks=[(100, 9)]),
    )
    b.submit(market(Side.BUY, 2))
    hanging = b.submit(limit(Side.SELL, 1, 500.0, ttl=DAY_MS, asof=1_500))  # ждёт цены, которой не будет
    b.advance(3_000)
    assert b.open_ticket_ids() == (hanging.ticket_id,) and b.position(BTC) == 2
    end = b.finalize(3_000, {BTC: 101.0})
    closed, sell = end.events
    assert (closed.ticket_id, closed.ts, closed.reason, closed.remaining_qty) == (hanging.ticket_id, 3_000, CloseReason.EXPIRED, 1)
    assert isinstance(sell, Fill) and (sell.side, sell.qty, sell.price, sell.ts, sell.liquidity) == (Side.SELL, 2, 101.0, 3_000, Liquidity.TAKER)
    assert sell.fee == pytest.approx(2 * 101 * TAKER)
    assert [t.ticket_id for t in end.liquidation_tickets] == [sell.ticket_id]
    assert b.position(BTC) == 0 and b.open_ticket_ids() == ()


def test_finalize_covers_short_positions_and_symbols_independently():
    b = make(
        msg(EXEC, bids=[(99, 9)], asks=[(100, 9)]),
        msg(EXEC, bids=[(9, 9)], asks=[(10, 9)], symbol=ETH),
    )
    b.submit(market(Side.SELL, 2))
    b.submit(market(Side.BUY, 3, symbol=ETH))
    b.advance(2_000)
    end = b.finalize(2_000, {BTC: 98.0, ETH: 11.0})
    assert [(f.symbol, f.side, f.qty, f.price) for f in fills(end.events)] == [
        (BTC, Side.BUY, 2, 98.0), (ETH, Side.SELL, 3, 11.0),
    ]
    assert b.position(BTC) == 0 and b.position(ETH) == 0


def test_finalize_rules():
    b = make(msg(EXEC, bids=[(99, 9)], asks=[(100, 9)]))
    b.submit(market(Side.BUY, 1))
    b.advance(2_000)
    with pytest.raises(ContractViolation, match="advance"):
        b.finalize(2_500, {BTC: 100.0})  # время барьера - 2000
    with pytest.raises(ContractViolation, match="Нет цены"):
        b.finalize(2_000, {})  # позицию без цены не выкупить
    with pytest.raises(ContractViolation, match="Нет цены"):
        b.finalize(2_000, {BTC: float("nan")})
    assert b.position(BTC) == 1  # неудачные попытки ничего не изменили
    b.finalize(2_000, {BTC: 100.0})
    for call in (lambda: b.finalize(2_000, {}), lambda: b.advance(3_000), lambda: b.submit(market(Side.BUY, 1, asof=2_500))):
        with pytest.raises(ContractViolation, match="закрыт"):
            call()


# ---------------------------------------------- BacktestEngine + OrderBookBarrier


def decisions(rows) -> pl.DataFrame:
    return pl.DataFrame(
        {"symbol": [BTC] * len(rows), "asof_ts": [r[0] for r in rows], "src_max_ts": [r[0] - 1 for r in rows],
         "close": [r[1] for r in rows]},
        schema={"symbol": pl.String, "asof_ts": pl.Int64, "src_max_ts": pl.Int64, "close": pl.Float64},
    )


class Script(Strategy):
    def __init__(self, plan):
        self.plan = plan

    def decide(self, ctx: DecisionContext):
        return self.plan.get(ctx.ts, [])


def test_engine_books_the_vwap_fill_and_the_end_of_data_buyback():
    rows = [(1_000, 100.0), (61_000, 100.0)]
    df = decisions(rows)
    barrier = OrderBookBarrier(CFG, ListFeed(), 0, book(
        msg(1_250, bids=[(99, 5)], asks=[(100, 1), (101, 2), (102, 3)]),
    ))
    res = BacktestEngine(barrier, Script({1_000: [OrderRequest(BTC, Side.BUY, 4.0)]}), initial_equity=10_000.0).run(df)
    entry = res.fills.row(0, named=True)
    assert entry["qty"] == 4.0 and entry["price"] == pytest.approx(101.0) and entry["liquidity"] == "taker"
    fee_in = 404 * TAKER  # 0.202
    # на 61000: long 4 @101, mark 100 -> нереализованный -4
    assert res.equity_curve["equity"].to_list()[1] == pytest.approx(10_000 - fee_in - 4)
    # конец данных: выкуп 4 @100 (последняя цена) + комиссия 400 * 0.0005 = 0.2; убыток -4 реализован
    exit_ = res.fills.row(1, named=True)
    assert (exit_["side"], exit_["qty"], exit_["price"], exit_["ts"]) == ("sell", 4.0, 100.0, 61_001)
    assert res.orders["forced"].to_list() == [False, True]
    assert res.final_equity == pytest.approx(10_000 - 4 - fee_in - 400 * TAKER)
    assert barrier.position(BTC) == 0.0


def test_engine_survives_hanging_limit_at_end_of_data_without_funding_coverage_error():
    rows = [(1_000, 100.0), (61_000, 100.0), (121_000, 101.0)]
    df = decisions(rows)
    books = book(
        msg(1_250, bids=[(99, 9)], asks=[(100, 10)]),
        msg(121_250, bids=[(101, 10)], asks=[(102, 10)]),  # лимитка на 150 так и не достигается
    )
    last = rows[-1][0]
    covered_until = last + 10_000  # фандинг-фид знает только окно сразу за последней строкой
    plan = {
        1_000: [OrderRequest(BTC, Side.BUY, 1.0)],
        last: [OrderRequest(BTC, Side.SELL, 1.0, OrderType.LIMIT, limit_price=150.0, ttl_ms=DAY_MS)],
    }
    # предпосылка: прежняя логика доводила время до expire_ts = last + LAT + DAY_MS - и упиралась бы в фид
    probe = OrderBookBarrier(CFG, StrictFeed(covered_until), 0, books)
    probe.submit(market(Side.BUY, 1.0))
    with pytest.raises(FundingCoverageError):
        probe.advance(last + LAT + DAY_MS)

    barrier = OrderBookBarrier(CFG, StrictFeed(covered_until), 0, books)
    res = BacktestEngine(barrier, Script(plan), initial_equity=10_000.0).run(df)

    assert res.orders.select("side", "order_type", "status", "filled_qty", "forced").rows() == [
        ("buy", "market", "filled", 1.0, False),
        ("sell", "limit", "expired", 0.0, False),  # висящая лимитка принудительно истекла
        ("sell", "market", "filled", 1.0, True),  # позиция выкуплена
    ]
    end = last + LAT + 1
    assert res.equity_curve["ts"][-1] == end and barrier.position(BTC) == 0.0
    exit_ = res.fills.row(1, named=True)
    assert (exit_["ts"], exit_["price"], exit_["liquidity"]) == (end, 101.0, "taker")  # последняя известная цена
    # вход 1 @100, выход 1 @101: +1; комиссии 100 * 0.0005 + 101 * 0.0005
    assert res.final_equity == pytest.approx(10_000 + 1 - 201 * TAKER)
    assert res.total_fees == pytest.approx(201 * TAKER)
