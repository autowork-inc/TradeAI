"""Гуарды риска: чистая логика без движка."""

import pytest

from backtester.contracts import ContractViolation, OrderType, Side
from backtester.risk import (
    DAY_MS,
    DrawdownLimitGuard,
    MaxExposureGuard,
    OrderRequest,
    RiskContext,
    split_closing_opening,
)
from utils.errors import ConfigError

H = 3_600_000


def req(side=Side.BUY, qty=1.0, symbol="BTCUSDT") -> OrderRequest:
    return OrderRequest(symbol, side, qty)


def ctx(ts=10 * H, equity=10_000.0, price=100.0, position=0.0, pending=0.0) -> RiskContext:
    return RiskContext(ts, "BTCUSDT", equity, price, position, pending)


# ---------------------------------------------------------------- OrderRequest


def test_order_request_uses_intent_validation():
    assert OrderRequest("BTCUSDT", "sell", 1.0).side is Side.SELL
    for kwargs in (dict(qty=0.0), dict(qty=-1.0), dict(qty=float("nan")), dict(symbol=""),
                   dict(order_type=OrderType.LIMIT), dict(limit_price=10.0)):
        base = dict(symbol="BTCUSDT", side=Side.BUY, qty=1.0)
        with pytest.raises(ContractViolation):
            OrderRequest(**{**base, **kwargs})
    assert req().to_intent(5000).asof_ts == 5000
    assert req(qty=3.0).with_qty(1.5).qty == 1.5
    with pytest.raises(TypeError):
        OrderRequest("BTCUSDT", Side.BUY, 1.0, asof_ts=1)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "side, qty, exposure, expected",
    [
        (Side.BUY, 2.0, 0.0, (0.0, 2.0)),
        (Side.BUY, 2.0, 3.0, (0.0, 2.0)),  # добавление
        (Side.SELL, 2.0, 3.0, (2.0, 0.0)),  # частичное закрытие
        (Side.SELL, 3.0, 3.0, (3.0, 0.0)),
        (Side.SELL, 5.0, 3.0, (3.0, 2.0)),  # разворот: закрытие + открытие
        (Side.BUY, 5.0, -3.0, (3.0, 2.0)),
        (Side.BUY, 1.0, -3.0, (1.0, 0.0)),
    ],
)
def test_split_closing_opening(side, qty, exposure, expected):
    assert split_closing_opening(side, qty, exposure) == expected


# --------------------------------------------------------- DrawdownLimitGuard


@pytest.mark.parametrize("bad", [0, 0.0, 1, 1.5, -0.05, 5, float("nan"), True, "0.05"])
def test_drawdown_guard_rejects_percent_like_or_invalid_limit(bad):
    with pytest.raises(ConfigError):
        DrawdownLimitGuard(bad)
    with pytest.raises(ConfigError):
        DrawdownLimitGuard(0.05, reference="week")


def test_drawdown_triggers_only_strictly_above_limit():
    g = DrawdownLimitGuard(0.05)
    g.on_equity(1 * H, 10_000.0)
    g.on_equity(2 * H, 9_500.0)  # ровно 5% - не срабатывает
    assert not g.is_blocked(2 * H) and g.check(req(), ctx(2 * H)).qty == 1.0
    g.on_equity(3 * H, 9_499.0)
    assert g.is_blocked(3 * H) and g.triggers[0].ts == 3 * H
    assert g.triggers[0].drawdown == pytest.approx(0.0501)


def test_drawdown_blocks_opening_until_end_of_day_then_releases():
    g = DrawdownLimitGuard(0.05)
    g.on_equity(1 * H, 10_000.0)
    g.on_equity(2 * H, 9_000.0)
    for ts in (2 * H, 12 * H, DAY_MS - 1):
        d = g.check(req(Side.BUY, 1.0), ctx(ts, position=2.0))
        assert d.qty == 0.0 and "daily stop" in d.reason
    g.on_equity(DAY_MS, 9_000.0)  # новые сутки: пик = вчерашнее эквити, просадки нет
    assert not g.is_blocked(DAY_MS)
    assert g.check(req(Side.BUY, 1.0), ctx(DAY_MS)).qty == 1.0
    assert len(g.triggers) == 1


def test_drawdown_lets_closing_through_and_cuts_the_opening_part_of_a_flip():
    g = DrawdownLimitGuard(0.05)
    g.on_equity(1 * H, 10_000.0)
    g.on_equity(2 * H, 9_000.0)
    assert g.check(req(Side.SELL, 2.0), ctx(position=3.0)).qty == 2.0  # закрытие
    assert g.check(req(Side.SELL, 5.0), ctx(position=3.0)).qty == 3.0  # разворот урезан до закрытия
    assert g.check(req(Side.SELL, 1.0), ctx(position=0.0)).qty == 0.0  # новая короткая позиция
    # экспозиция учитывает ожидающие ордера: позиция 0, но ожидает покупка 2 -> продажа 2 закрывает её
    assert g.check(req(Side.SELL, 2.0), ctx(position=0.0, pending=2.0)).qty == 2.0


def test_drawdown_reference_day_resets_peak_but_run_does_not():
    day, run = DrawdownLimitGuard(0.05), DrawdownLimitGuard(0.05, reference="run")
    for g in (day, run):
        g.on_equity(1 * H, 10_000.0)
        g.on_equity(2 * H, 9_000.0)
        g.on_equity(DAY_MS + 1, 9_000.0)
    assert not day.is_blocked(DAY_MS + 1) and len(day.triggers) == 1
    assert run.is_blocked(DAY_MS + 1) and len(run.triggers) == 2  # всё ещё в просадке от общего пика


def test_drawdown_gap_down_across_midnight_counts_for_the_new_day():
    g = DrawdownLimitGuard(0.05)
    g.on_equity(23 * H, 10_000.0)
    g.on_equity(DAY_MS + H, 9_000.0)  # эквити упало за ночь
    assert g.is_blocked(DAY_MS + H)


def test_drawdown_nonpositive_peak_is_full_drawdown_and_reset_clears_state():
    g = DrawdownLimitGuard(0.05)
    g.on_equity(H, 0.0)
    assert g.is_blocked(H)
    g.reset()
    assert g.triggers == () and not g.is_blocked(H)


# ------------------------------------------------------------ MaxExposureGuard


@pytest.mark.parametrize("bad", [0, -0.02, float("inf"), float("nan"), True, None])
def test_max_exposure_rejects_invalid_fraction(bad):
    with pytest.raises(ConfigError):
        MaxExposureGuard(bad)


def test_max_exposure_allows_within_limit_and_clips_beyond():
    g = MaxExposureGuard(0.02)  # 2% от 10 000 = 200 USDT = 2 BTC по 100
    assert g.check(req(qty=1.5), ctx()).qty == 1.5
    d = g.check(req(qty=5.0), ctx())
    assert d.qty == pytest.approx(2.0) and "лимит экспозиции" in d.reason
    d = g.check(req(qty=5.0), ctx(position=1.5))
    assert d.qty == pytest.approx(0.5)
    assert g.check(req(side=Side.SELL, qty=5.0), ctx()).qty == pytest.approx(2.0)  # шорт тоже ограничен


def test_max_exposure_counts_pending_orders():
    g = MaxExposureGuard(0.02)
    assert g.check(req(qty=1.0), ctx(position=0.5, pending=1.5)).qty == 0.0


def test_max_exposure_blocks_when_over_limit_but_never_blocks_reduction():
    g = MaxExposureGuard(0.02)  # лимит 2 BTC, позиция 3 (цена выросла/эквити упало)
    assert g.check(req(qty=1.0), ctx(position=3.0)).qty == 0.0
    assert g.check(req(side=Side.SELL, qty=1.0), ctx(position=3.0)).qty == 1.0  # уменьшение проходит
    assert g.check(req(side=Side.SELL, qty=3.0), ctx(position=3.0)).qty == 3.0


def test_max_exposure_flip_gets_closing_part_plus_at_most_the_limit():
    g = MaxExposureGuard(0.02)
    assert g.check(req(side=Side.SELL, qty=10.0), ctx(position=1.0)).qty == pytest.approx(1.0 + 2.0)
    assert g.check(req(side=Side.SELL, qty=2.0), ctx(position=1.0)).qty == 2.0  # шорт 1 внутри лимита


def test_max_exposure_scales_with_current_equity_and_price_and_supports_leverage():
    g = MaxExposureGuard(0.02)
    assert g.check(req(qty=5.0), ctx(equity=5_000.0)).qty == pytest.approx(1.0)
    assert g.check(req(qty=5.0), ctx(price=400.0)).qty == pytest.approx(0.5)
    assert MaxExposureGuard(3.0).check(req(qty=500.0), ctx()).qty == pytest.approx(300.0)


def test_max_exposure_without_equity_or_price_blocks_opening_only():
    g = MaxExposureGuard(0.02)
    assert g.check(req(qty=1.0), ctx(equity=0.0)).qty == 0.0
    assert g.check(req(qty=1.0), ctx(equity=-5.0)).qty == 0.0
    assert g.check(req(qty=1.0), ctx(price=None)).qty == 0.0
    assert g.check(req(side=Side.SELL, qty=1.0), ctx(price=None, position=1.0)).qty == 1.0
    assert g.check(req(side=Side.SELL, qty=3.0), ctx(price=None, position=1.0)).qty == 1.0
