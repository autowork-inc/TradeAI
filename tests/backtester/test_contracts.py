"""Контракт ExecutionBarrier: задержка, комиссии, фандинг по fundingTime, жизненный цикл ордера.

Движка нет: ScriptedBarrier отдаёт заранее заданные события, а базовый класс обязан их проверить.
"""

import pytest

from backtester.contracts import (
    CloseReason,
    ContractViolation,
    ExecutionBarrier,
    ExecutionConfig,
    Fill,
    FundingCoverageError,
    FundingRecord,
    FundingSettlement,
    Liquidity,
    OrderClosed,
    OrderIntent,
    OrderType,
    RawFill,
    Side,
    funding_fee,
)
from pipeline.types import TimingConfig
from utils.errors import ConfigError

LAT, MAKER, TAKER = 250, 0.0002, 0.0005
CFG = ExecutionConfig(LAT, MAKER, TAKER)
T = Liquidity.TAKER
M = Liquidity.MAKER


class ScriptedBarrier(ExecutionBarrier):
    def __init__(self, funding=None, script=(), start=0, config=CFG, leaky=False):
        super().__init__(config, funding or ListFeed(), start)
        self.script, self.leaky, self.accepted = list(script), leaky, []

    def _accept(self, ticket):
        self.accepted.append(ticket)

    def _match(self, lo, hi):
        return list(self.script) if self.leaky else [e for e in self.script if lo <= e.ts < hi]


class ListFeed:
    def __init__(self, records=(), error=None):
        self.records, self.error, self.calls = list(records), error, []

    def settlements(self, symbol, lo, hi):
        self.calls.append((symbol, lo, hi))
        if self.error:
            raise self.error
        return [r for r in self.records if r.symbol == symbol and lo <= r.funding_time < hi]


def intent(asof=1000, qty=1.0, side=Side.BUY, symbol="BTCUSDT", **kw) -> OrderIntent:
    return OrderIntent(symbol, side, qty, asof, **kw)


def rec(t, rate=0.0001, mark=20_000.0, symbol="BTCUSDT") -> FundingRecord:
    return FundingRecord(symbol, t, rate, mark)


def market_fill(ticket_id, ts, qty=1.0, price=20_000.0) -> RawFill:
    return RawFill(ticket_id, ts, qty, price, T)


# ---------------------------------------------------------------------- конфиг


def test_exec_ts_is_asof_plus_latency_and_strictly_later():
    assert CFG.exec_ts(1000) == 1250
    assert CFG.exec_ts(1000) > 1000


def test_latency_comes_from_timing_config_single_source():
    timing = TimingConfig(order_latency_ms=400)
    assert ExecutionConfig.from_timing(timing, maker_fee_rate=MAKER, taker_fee_rate=TAKER).order_latency_ms == 400


def test_fees_by_liquidity_role_and_rebate():
    assert CFG.fee(T, 2.0, 20_000.0) == pytest.approx(40_000 * TAKER)
    assert CFG.fee(M, 2.0, 20_000.0) == pytest.approx(40_000 * MAKER)
    rebate = ExecutionConfig(LAT, -0.0001, TAKER)
    assert rebate.fee(M, 1.0, 10_000.0) == pytest.approx(-1.0)  # отрицательная комиссия = ребейт


@pytest.mark.parametrize(
    "args",
    [
        (0, MAKER, TAKER),
        (-5, MAKER, TAKER),
        (True, MAKER, TAKER),
        (LAT, 0.05, 0.1),  # проценты вместо долей
        (LAT, MAKER, -0.0001),  # отрицательный taker
        (LAT, 0.001, 0.0005),  # maker дороже taker
        (LAT, float("nan"), TAKER),
        (LAT, MAKER, float("inf")),
        (LAT, "0.0002", TAKER),
    ],
)
def test_config_rejects_invalid(args):
    with pytest.raises(ConfigError):
        ExecutionConfig(*args)


def test_funding_fee_sign_convention():
    assert funding_fee(1.0, 20_000.0, 0.0001) == pytest.approx(2.0)  # лонг платит при ставке > 0
    assert funding_fee(-1.0, 20_000.0, 0.0001) == pytest.approx(-2.0)  # шорт получает
    assert funding_fee(1.0, 20_000.0, -0.0001) == pytest.approx(-2.0)  # лонг получает при ставке < 0
    assert funding_fee(0.0, 20_000.0, 0.01) == 0.0


# ------------------------------------------------------------------ OrderIntent


def test_intent_has_no_way_to_set_exec_time():
    with pytest.raises(TypeError):
        OrderIntent("BTCUSDT", Side.BUY, 1.0, 1000, exec_ts=1001)  # type: ignore[call-arg]
    assert intent(side="sell").side is Side.SELL


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(qty=0.0),
        dict(qty=-1.0),
        dict(qty=float("nan")),
        dict(asof=0),
        dict(asof=True),
        dict(symbol=""),
        dict(limit_price=100.0),  # MARKET не принимает лимит
        dict(ttl_ms=1000),
        dict(order_type=OrderType.LIMIT),  # LIMIT без цены и срока
        dict(order_type=OrderType.LIMIT, limit_price=100.0),  # без ttl: бессрочные ордера запрещены
        dict(order_type=OrderType.LIMIT, limit_price=-1.0, ttl_ms=10),
        dict(order_type=OrderType.LIMIT, limit_price=100.0, ttl_ms=0),
    ],
)
def test_intent_rejects_invalid(kwargs):
    with pytest.raises(ContractViolation):
        intent(**kwargs)


def test_funding_record_requires_mark_price_and_sane_rate():
    with pytest.raises(ContractViolation):
        FundingRecord("BTCUSDT", 5000, 0.0001, None)  # type: ignore[arg-type]
    with pytest.raises(ContractViolation):
        FundingRecord("BTCUSDT", 5000, 1.5, 20_000.0)
    with pytest.raises(ContractViolation):
        FundingRecord("BTCUSDT", 0, 0.0001, 20_000.0)


# ----------------------------------------------------------------------- submit


def test_submit_stamps_exec_ts_and_expiry():
    b = ScriptedBarrier()
    t1 = b.submit(intent(asof=1000))
    t2 = b.submit(intent(asof=1000, order_type=OrderType.LIMIT, limit_price=19_900.0, ttl_ms=5000))
    assert (t1.ticket_id, t1.exec_ts, t1.expire_ts) == (1, 1250, None)
    assert (t2.ticket_id, t2.exec_ts, t2.expire_ts) == (2, 1250, 6250)
    assert b.accepted == [t1, t2]


def test_submit_rejects_backdated_and_non_intent():
    b = ScriptedBarrier(script=[market_fill(1, 750), market_fill(2, 850)], start=500)
    with pytest.raises(ContractViolation, match="задним числом"):
        b.submit(intent(asof=499))
    b.submit(intent(asof=500))  # ровно «сейчас» допустимо
    b.submit(intent(asof=600))
    with pytest.raises(ContractViolation, match="задним числом"):
        b.submit(intent(asof=599))  # решения идут по неубыванию
    b.advance(1000)
    with pytest.raises(ContractViolation, match="задним числом"):
        b.submit(intent(asof=999))
    with pytest.raises(ContractViolation):
        b.submit({"symbol": "BTCUSDT"})  # type: ignore[arg-type]


def test_rejected_submit_leaves_no_trace():
    b = ScriptedBarrier(start=500)
    with pytest.raises(ContractViolation):
        b.submit(intent(asof=100))
    assert b.accepted == [] and b.open_ticket_ids() == ()
    assert b.submit(intent(asof=500)).ticket_id == 1


# ---------------------------------------------------------------------- advance


def test_events_become_visible_strictly_after_their_timestamp():
    b = ScriptedBarrier(script=[market_fill(1, 1250)])
    b.submit(intent(asof=1000))
    assert b.advance(1250) == ()  # исполнение в 1250 стратегии на решении 1250 ещё не известно
    (fill,) = b.advance(1251)
    assert isinstance(fill, Fill) and fill.ts == 1250
    assert b.advance(5000) == ()  # событие выпускается ровно один раз


def test_time_cannot_go_backwards():
    b = ScriptedBarrier()
    b.advance(1000)
    b.advance(1000)  # пустое окно допустимо
    with pytest.raises(ContractViolation, match="назад"):
        b.advance(999)


def test_fee_is_charged_by_barrier_from_liquidity_role():
    lim = dict(order_type=OrderType.LIMIT, limit_price=20_000.0, ttl_ms=10_000)
    b = ScriptedBarrier(script=[
        market_fill(1, 1250, qty=2.0, price=20_000.0),
        RawFill(2, 3000, 1.0, 19_990.0, M),
    ])
    b.submit(intent(asof=1000, qty=2.0))
    b.submit(intent(asof=1000, qty=1.0, **lim))
    taker, maker = b.advance(4000)
    assert taker.fee == pytest.approx(2 * 20_000 * TAKER)
    assert maker.fee == pytest.approx(19_990 * MAKER)
    assert (taker.liquidity, maker.liquidity) == (T, M)
    assert b.position("BTCUSDT") == pytest.approx(3.0)


def test_position_follows_signed_fills():
    b = ScriptedBarrier(script=[market_fill(1, 1250, 1.0), market_fill(2, 2250, 0.4)])
    b.submit(intent(asof=1000, qty=1.0))
    b.submit(intent(asof=2000, qty=0.4, side=Side.SELL))
    b.advance(3000)
    assert b.position("BTCUSDT") == pytest.approx(0.6)
    assert b.position("ETHUSDT") == 0.0
    assert b.open_ticket_ids() == ()


# ------------------------------------------------------- проверки событий движка


@pytest.mark.parametrize(
    "events, message",
    [
        ([market_fill(1, 1249)], "раньше прихода"),  # быстрее задержки
        ([market_fill(1, 1251)], "ровно в exec_ts"),  # MARKET позже exec_ts
        ([RawFill(1, 1250, 1.0, 20_000.0, M)], "TAKER"),
        ([market_fill(7, 1250)], "неизвестен"),
        ([market_fill(1, 1250, qty=1.5)], "Переисполнение"),
        ([market_fill(1, 1250), market_fill(1, 1250)], "неизвестен или уже закрыт"),  # двойное исполнение
        ([market_fill(1, 5000)], "вне окна"),  # событие из будущего относительно now
    ],
)
def test_engine_events_are_validated(events, message):
    b = ScriptedBarrier(script=events, leaky=True)
    b.submit(intent(asof=1000))
    with pytest.raises(ContractViolation, match=message):
        b.advance(2000)


def test_engine_cannot_replay_events_from_a_released_window():
    b = ScriptedBarrier(script=[market_fill(1, 1250)], leaky=True)
    b.submit(intent(asof=1000))
    assert len(b.advance(1300)) == 1
    with pytest.raises(ContractViolation, match="вне окна"):
        b.advance(2000)  # движок снова вернул событие 1250 < прежнего времени 1300


def test_engine_events_must_be_time_ordered():
    b = ScriptedBarrier(script=[market_fill(2, 2250), market_fill(1, 1250)], leaky=True)
    b.submit(intent(asof=1000))
    b.submit(intent(asof=2000))
    with pytest.raises(ContractViolation, match="не упорядочены"):
        b.advance(3000)


def test_engine_may_not_return_foreign_event_types():
    b = ScriptedBarrier(script=[FundingSettlement("BTCUSDT", 100, 1.0, 1.0, 0.0, 0.0)], leaky=True, start=50)
    with pytest.raises(ContractViolation, match="допустимы"):
        b.advance(1000)


def test_limit_price_and_lifetime_are_enforced():
    lim = lambda side, px: intent(asof=1000, side=side, order_type=OrderType.LIMIT, limit_price=px, ttl_ms=1000)
    for side, bad_px in ((Side.BUY, 20_001.0), (Side.SELL, 19_999.0)):
        b = ScriptedBarrier(script=[RawFill(1, 1300, 1.0, bad_px, M)])
        b.submit(lim(side, 20_000.0))
        with pytest.raises(ContractViolation, match="хуже лимитной"):
            b.advance(2000)
    for side, ok_px in ((Side.BUY, 19_999.0), (Side.SELL, 20_001.0)):  # лучше лимита - допустимо
        b = ScriptedBarrier(script=[RawFill(1, 1300, 1.0, ok_px, M)])
        b.submit(lim(side, 20_000.0))
        assert len(b.advance(2000)) == 1
    b = ScriptedBarrier(script=[RawFill(1, 2250, 1.0, 19_999.0, M)], leaky=True)  # expire_ts = 2250
    b.submit(lim(Side.BUY, 20_000.0))
    with pytest.raises(ContractViolation, match="после срока"):
        b.advance(3000)


def test_limit_partial_fill_then_expiry_closes_exactly_at_expire_ts():
    b = ScriptedBarrier(script=[
        RawFill(1, 1500, 0.3, 19_990.0, M),
        OrderClosed(1, 2250, 0.7, CloseReason.EXPIRED),
    ])
    b.submit(intent(asof=1000, order_type=OrderType.LIMIT, limit_price=20_000.0, ttl_ms=1000))
    events = b.advance(3000)
    assert [type(e).__name__ for e in events] == ["Fill", "OrderClosed"]
    assert b.open_ticket_ids() == ()


def test_expiry_and_close_reasons_are_validated():
    lim = intent(asof=1000, order_type=OrderType.LIMIT, limit_price=20_000.0, ttl_ms=1000)
    cases = [
        (lim, OrderClosed(1, 2000, 1.0, CloseReason.EXPIRED), "EXPIRED"),  # не в expire_ts
        (lim, OrderClosed(1, 2250, 0.5, CloseReason.EXPIRED), "Остаток"),  # неверный остаток
        (lim, OrderClosed(1, 1250, 1.0, CloseReason.NO_LIQUIDITY), "только к MARKET"),
        (intent(asof=1000), OrderClosed(1, 1250, 1.0, CloseReason.EXPIRED), "EXPIRED"),  # MARKET не истекает
    ]
    for order, closed, message in cases:
        b = ScriptedBarrier(script=[closed], leaky=True)
        b.submit(order)
        with pytest.raises(ContractViolation, match=message):
            b.advance(3000)


def test_market_without_liquidity_closes_remainder_at_exec_ts():
    b = ScriptedBarrier(script=[market_fill(1, 1250, qty=1.0), OrderClosed(1, 1250, 1.0, CloseReason.NO_LIQUIDITY)])
    b.submit(intent(asof=1000, qty=2.0))
    assert [type(e).__name__ for e in b.advance(2000)] == ["Fill", "OrderClosed"]
    assert b.position("BTCUSDT") == pytest.approx(1.0)


def test_rejected_is_impossible_after_a_fill():
    b = ScriptedBarrier(script=[market_fill(1, 1250, qty=1.0), OrderClosed(1, 1250, 1.0, CloseReason.REJECTED)])
    b.submit(intent(asof=1000, qty=2.0))
    with pytest.raises(ContractViolation, match="REJECTED"):
        b.advance(2000)


def test_no_hanging_orders():
    b = ScriptedBarrier()
    b.submit(intent(asof=1000))  # MARKET: исполнение или закрытие обязаны быть в 1250
    b.advance(1250)  # срок ещё не наступил (событие в 1250 станет видно позже)
    with pytest.raises(ContractViolation, match="не закрыт"):
        b.advance(1251)

    b = ScriptedBarrier()
    b.submit(intent(asof=1000, order_type=OrderType.LIMIT, limit_price=20_000.0, ttl_ms=1000))
    b.advance(2250)
    with pytest.raises(ContractViolation, match="не закрыт"):
        b.advance(2251)


# ------------------------------------------------------------------------ фандинг


def hold_long(feed, *, script_extra=(), qty=1.0, asof=1000, **kw):
    """Лонг открыт market-ордером в asof + LAT."""
    b = ScriptedBarrier(feed, script=[market_fill(1, asof + LAT, qty=qty), *script_extra], **kw)
    b.submit(intent(asof=asof, qty=qty))
    return b


def test_long_pays_positive_funding_at_funding_time():
    b = hold_long(ListFeed([rec(5000, rate=0.0001, mark=20_000.0)]))
    events = b.advance(6000)
    (settle,) = [e for e in events if isinstance(e, FundingSettlement)]
    assert (settle.funding_time, settle.ts) == (5000, 5000)  # время события - fundingTime, не момент advance
    assert settle.position_qty == 1.0 and settle.mark_price == 20_000.0
    assert settle.funding_fee == pytest.approx(2.0)  # списано


def test_short_receives_positive_funding_and_long_receives_negative():
    b = ScriptedBarrier(ListFeed([rec(5000)]), script=[market_fill(1, 1250, qty=0.5)])
    b.submit(intent(asof=1000, qty=0.5, side=Side.SELL))
    (settle,) = [e for e in b.advance(6000) if isinstance(e, FundingSettlement)]
    assert settle.position_qty == -0.5 and settle.funding_fee == pytest.approx(-1.0)

    b = hold_long(ListFeed([rec(5000, rate=-0.0003)]))
    (settle,) = [e for e in b.advance(6000) if isinstance(e, FundingSettlement)]
    assert settle.funding_fee == pytest.approx(-6.0)


def test_settlement_is_visible_only_after_funding_time():
    b = hold_long(ListFeed([rec(5000)]))
    assert not [e for e in b.advance(5000) if isinstance(e, FundingSettlement)]
    assert [e for e in b.advance(5001) if isinstance(e, FundingSettlement)]


def test_each_funding_time_is_settled_exactly_once_across_chained_windows():
    b = hold_long(ListFeed([rec(5000), rec(13_000)]))
    got = []
    for now in (2000, 4000, 5000, 5001, 9000, 13_000, 13_001, 50_000):
        got += [e.funding_time for e in b.advance(now) if isinstance(e, FundingSettlement)]
    assert got == [5000, 13_000]


def test_no_funding_for_flat_position_and_feed_is_not_asked_about_flat_symbols():
    feed = ListFeed([rec(5000)])
    b = ScriptedBarrier(feed, script=[market_fill(1, 1250), market_fill(2, 2250)])
    b.submit(intent(asof=1000))
    b.submit(intent(asof=2000, side=Side.SELL))
    b.advance(3000)  # открыли и закрыли до списания
    feed.calls.clear()
    assert [e for e in b.advance(9000) if isinstance(e, FundingSettlement)] == []
    assert feed.calls == []  # дыра в данных фандинга вне позиции никого не касается
    assert b.advance(20_000) == ()


def test_position_is_taken_strictly_before_funding_time():
    # Вход ровно в T: фандинг не платится. Выход ровно в T: платится.
    entry_at_T = ScriptedBarrier(ListFeed([rec(5000)]), script=[market_fill(1, 5000)])
    entry_at_T.submit(intent(asof=5000 - LAT))
    assert [e for e in entry_at_T.advance(9000) if isinstance(e, FundingSettlement)] == []

    exit_at_T = hold_long(ListFeed([rec(5000)]), script_extra=[market_fill(2, 5000)])
    exit_at_T.submit(intent(asof=5000 - LAT, side=Side.SELL))
    settles = [e for e in exit_at_T.advance(9000) if isinstance(e, FundingSettlement)]
    assert len(settles) == 1 and settles[0].position_qty == 1.0
    assert exit_at_T.position("BTCUSDT") == pytest.approx(0.0)

    just_before = ScriptedBarrier(ListFeed([rec(5000)]), script=[market_fill(1, 4999)])
    just_before.submit(intent(asof=4999 - LAT))
    assert len([e for e in just_before.advance(9000) if isinstance(e, FundingSettlement)]) == 1


def test_position_size_at_each_funding_time_follows_fills():
    b = hold_long(
        ListFeed([rec(5000), rec(13_000)]),
        script_extra=[market_fill(2, 7250, qty=2.0)],
    )
    b.submit(intent(asof=7000, qty=2.0))
    qtys = [e.position_qty for e in b.advance(20_000) if isinstance(e, FundingSettlement)]
    assert qtys == [1.0, 3.0]


def test_funding_survives_window_boundaries_with_fill_in_same_advance():
    # Один advance покрывает и исполнение, и списание после него: порядок внутри окна значения не имеет.
    b = hold_long(ListFeed([rec(5000)]))
    events = b.advance(20_000)
    assert [type(e).__name__ for e in events] == ["Fill", "FundingSettlement"]


def test_same_timestamp_order_is_funding_then_fill():
    b = ScriptedBarrier(ListFeed([rec(5000)]), script=[market_fill(1, 1250), market_fill(2, 5000)])
    b.submit(intent(asof=1000))
    b.submit(intent(asof=5000 - LAT, side=Side.SELL))
    names = [type(e).__name__ for e in b.advance(6000) if e.ts == 5000]
    assert names == ["FundingSettlement", "Fill"]


def test_multiple_symbols_settle_independently():
    feed = ListFeed([rec(5000, symbol="BTCUSDT"), rec(5000, rate=0.0002, mark=1_000.0, symbol="ETHUSDT")])
    b = ScriptedBarrier(feed, script=[market_fill(1, 1250), market_fill(2, 1250, qty=10.0, price=1_000.0)])
    b.submit(intent(asof=1000, symbol="BTCUSDT"))
    b.submit(intent(asof=1000, qty=10.0, symbol="ETHUSDT", side=Side.SELL))
    fees = {e.symbol: e.funding_fee for e in b.advance(6000) if isinstance(e, FundingSettlement)}
    assert fees == {"BTCUSDT": pytest.approx(2.0), "ETHUSDT": pytest.approx(-2.0)}


def test_missing_funding_coverage_is_loud_for_open_position():
    boom = FundingCoverageError("пропущено списание 2026-01-01T08:00")
    b = hold_long(ListFeed(error=boom))
    with pytest.raises(FundingCoverageError):
        b.advance(20_000)


def test_bad_feed_records_are_rejected():
    class BadFeed(ListFeed):
        def settlements(self, symbol, lo, hi):
            return self.records

    for records in (
        [rec(5000, symbol="ETHUSDT")],  # чужой символ
        [rec(999_999)],  # вне окна
        [rec(6000), rec(5000)],  # не по возрастанию
        [rec(5000), rec(5000)],  # дубль
    ):
        with pytest.raises(ContractViolation, match="некорректную"):
            hold_long(BadFeed(records)).advance(20_000)
