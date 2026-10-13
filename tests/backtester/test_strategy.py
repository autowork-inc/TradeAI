"""Интерфейс стратегии и BaselineStrategy: решения по таблице состояний (ожидаемые числа посчитаны вручную)."""

from types import MappingProxyType

import pytest

import backtester.engine as engine
from backtester.contracts import OrderType, Side
from backtester.risk import OrderRequest
from backtester.strategy import BaselineStrategy, DecisionContext, FeatureContext, Strategy, StrategyError
from utils.errors import ConfigError

BTC = "BTCUSDT"


def ctx(*, fast=102.0, slow=100.0, obi=0.3, position=0.0, pending=0.0, equity=10_000.0, price=100.0, extra=None):
    row = {"ma_fast": fast, "ma_slow": slow, "obi": obi, **(extra or {})}
    return FeatureContext(
        ts=1_000, symbol=BTC, row=MappingProxyType(row), equity=equity,
        positions=MappingProxyType({BTC: position} if position else {}),
        pending=MappingProxyType({BTC: pending} if pending else {}),
        marks=MappingProxyType({BTC: price} if price is not None else {}),
    )


def decide(c, **kw):
    return list(BaselineStrategy(**kw).decide(c))


def req(side, qty):
    return OrderRequest(BTC, side, qty)


# ----------------------------------------------------------------------- контекст


def test_context_accessors():
    c = ctx(position=2.0, pending=-0.5)
    assert (c.position, c.pending_qty, c.exposure, c.price) == (2.0, -0.5, 1.5, 100.0)
    flat = ctx()
    assert (flat.position, flat.pending_qty, flat.exposure) == (0.0, 0.0, 0.0)
    assert ctx(price=None).price is None


def test_feature_distinguishes_missing_value_from_missing_column():
    c = ctx(obi=None, extra={"nan": float("nan"), "inf": float("inf"), "flag": True, "name": "x"})
    assert c.feature("ma_fast") == 102.0
    assert [c.feature(n) for n in ("obi", "nan", "inf", "flag", "name")] == [None] * 5
    with pytest.raises(StrategyError, match="нет признака"):
        c.feature("нет_такого")


def test_context_is_read_only_and_old_names_still_work():
    c = ctx()
    with pytest.raises(TypeError):
        c.row["ma_fast"] = 0.0  # type: ignore[index]
    with pytest.raises(AttributeError):
        c.equity = 1.0  # type: ignore[misc]
    assert DecisionContext is FeatureContext and engine.Strategy is Strategy and engine.DecisionContext is FeatureContext


def test_strategy_is_abstract():
    with pytest.raises(TypeError):
        Strategy()  # type: ignore[abstract]


# --------------------------------------------------------------------- baseline


def test_enters_long_when_trend_up_and_book_confirms():
    # 10% от 10_000 при цене 100 = 10 BTC
    assert decide(ctx()) == [req(Side.BUY, 10.0)]


def test_enters_short_when_trend_down_and_book_confirms():
    assert decide(ctx(fast=98.0, obi=-0.3)) == [req(Side.SELL, 10.0)]


def test_orders_are_market_orders_without_time():
    (order,) = decide(ctx())
    assert order.order_type is OrderType.MARKET and order.limit_price is None and order.ttl_ms is None


def test_holds_when_already_positioned_in_trend_direction():
    assert decide(ctx(position=10.0)) == []
    assert decide(ctx(fast=98.0, obi=-0.3, position=-7.0)) == []  # размер не пересчитывается


def test_order_in_flight_counts_as_exposure_and_is_not_duplicated():
    assert decide(ctx(pending=10.0)) == []


def test_reversal_with_confirmation_flips_in_one_order():
    # long 10 -> short 10: продать 10 + 10
    assert decide(ctx(fast=98.0, obi=-0.3, position=10.0)) == [req(Side.SELL, 20.0)]


def test_reversal_without_confirmation_only_flattens():
    assert decide(ctx(fast=98.0, obi=0.3, position=10.0)) == [req(Side.SELL, 10.0)]
    assert decide(ctx(fast=98.0, obi=0.3)) == []  # плоские и без подтверждения - не входим


def test_unconfirmed_entry_counts_pending_and_flattens_it():
    assert decide(ctx(fast=98.0, obi=0.3, pending=4.0)) == [req(Side.SELL, 4.0)]


def test_stale_book_is_not_confirmation():
    assert decide(ctx(obi=None)) == []
    assert decide(ctx(fast=98.0, obi=None, position=10.0)) == [req(Side.SELL, 10.0)]


@pytest.mark.parametrize("missing", ["fast", "slow"])
def test_no_action_during_warmup(missing):
    kw = {missing: None}
    assert decide(ctx(**kw, position=3.0)) == []


def test_no_action_without_price_or_on_flat_trend():
    assert decide(ctx(price=None)) == []
    assert decide(ctx(fast=100.0, slow=100.0)) == []


def test_obi_threshold_and_zero_imbalance():
    assert decide(ctx(obi=0.04), obi_threshold=0.05) == []
    assert decide(ctx(obi=0.05), obi_threshold=0.05) == [req(Side.BUY, 10.0)]
    assert decide(ctx(obi=0.0)) == []  # нулевой дисбаланс не подтверждает ни одну сторону


def test_obi_filter_can_be_disabled():
    c = ctx(extra=None)
    row = dict(c.row)
    row.pop("obi")
    no_obi = FeatureContext(c.ts, c.symbol, MappingProxyType(row), c.equity, c.positions, c.pending, c.marks)
    assert decide(no_obi, obi_col=None) == [req(Side.BUY, 10.0)]
    with pytest.raises(StrategyError):
        decide(no_obi)  # колонка obi настроена, но в кадре её нет - это ошибка, а не «нет сигнала»


def test_size_scales_with_equity_and_price():
    assert decide(ctx(equity=20_000.0, price=200.0), equity_fraction=0.5) == [req(Side.BUY, 50.0)]


@pytest.mark.parametrize("bad", [0, -0.1, float("nan"), float("inf"), True, None])
def test_equity_fraction_validation(bad):
    with pytest.raises(ConfigError):
        BaselineStrategy(equity_fraction=bad)


@pytest.mark.parametrize("bad", [-0.1, 1.0, float("nan"), True])
def test_obi_threshold_validation(bad):
    with pytest.raises(ConfigError):
        BaselineStrategy(obi_threshold=bad)
