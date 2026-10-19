"""Расчёт лотов MT5 (`exchange.mt5_risk`): математика на реальных типах контрактов (Forex 100k, золото 100 oz, JPY-пара, индекс),
округление вниз по шагу, границы объёма, маржа и причины отказа. Без сети."""

import pytest

from exchange.mt5_risk import Mt5OrderPlan, Mt5Rejected, Mt5RiskParams, approx_margin_per_lot, floor_to_step, loss_per_lot, size_position
from exchange.symbol_mapper import parse_spec
from utils.errors import ConfigError

EQUITY = 100_000.0
RISK = 1_000.0  # риск для проверки математики на «круглых» числах (боевой дефолт - 5 USD, см. test_default_risk_is_fixed_five_usd)


def spec(internal, broker, **kw):
    raw = {"contractSize": 100_000, "minVolume": 0.01, "maxVolume": 100, "volumeStep": 0.01, "tickSize": 0.00001, "digits": 5,
           "point": 0.00001, "stopsLevel": 0, "fillingModes": ["SYMBOL_FILLING_FOK"], "tradeMode": "SYMBOL_TRADE_MODE_FULL",
           "profitCurrency": "USD", "marginCurrency": "EUR", **kw}
    return parse_spec(internal, broker, raw)


EURUSD = spec("EURUSD", "EURUSD")  # 1 лот = 100 000 EUR, тик 0.00001 -> 1 USD, пункт (pip) = 10 тиков = 10 USD
XAUUSD = spec("XAUUSD", "XAUUSD", contractSize=100, tickSize=0.01, point=0.01, digits=2, minVolume=0.01, volumeStep=0.01, maxVolume=50,
              marginCurrency="XAU")  # 1 лот = 100 унций, тик 0.01 = 1 USD
USDJPY = spec("USDJPY", "USDJPY", tickSize=0.001, point=0.001, digits=3, profitCurrency="JPY", marginCurrency="USD")
US500 = spec("US500", "US500", contractSize=1, tickSize=0.01, point=0.01, digits=2, minVolume=0.1, volumeStep=0.1, maxVolume=50)


def ok(plan) -> Mt5OrderPlan:
    assert isinstance(plan, Mt5OrderPlan), plan
    return plan


def kw(**over):
    base = dict(equity=EQUITY, free_margin=EQUITY, leverage=100, params=Mt5RiskParams(risk_usdt=RISK))
    return {**base, **over}


# ------------------------------------------------------------------------------------------------ математика лота


def test_eurusd_example_from_the_spec_20_pips_1000_usd_is_5_lots():
    # SL 20 пипсов = 0.0020; потеря на лот: 20 пипсов * 10 USD = 200 USD; риск 1000 -> 5.0 лота
    plan = ok(size_position(EURUSD, side="BUY", entry=1.1000, sl=1.0980, tick_value=1.0, **kw()))
    assert plan.lots == 5.0 and plan.loss_per_lot == pytest.approx(200.0)
    assert plan.risk_amount == pytest.approx(1000.0) and plan.risk_target == pytest.approx(1000.0)
    assert plan.sl_distance == pytest.approx(0.0020) and plan.broker_symbol == "EURUSD" and plan.close_side == "SELL"


def test_same_result_through_contract_size_when_profit_currency_is_the_account_currency():
    a = ok(size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, **kw()))
    b = ok(size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, quote_to_account=1.0, **kw()))  # d * contractSize * 1
    assert a.lots == b.lots == 5.0 and b.loss_per_lot == pytest.approx(0.002 * 100_000)


def test_gold_100_oz_contract():
    # SL 10 USD: 100 унций * 10 = 1000 USD на лот -> 1.00 лот; SL 12.5 -> 1250 на лот -> 0.8 лота
    assert ok(size_position(XAUUSD, side="BUY", entry=2000.0, sl=1990.0, tick_value=1.0, **kw())).lots == 1.0
    plan = ok(size_position(XAUUSD, side="SELL", entry=2000.0, sl=2012.5, tick_value=1.0, **kw()))
    assert plan.lots == 0.8 and plan.loss_per_lot == pytest.approx(1250.0) and plan.close_side == "BUY"


def test_jpy_pair_uses_account_currency_tick_value_not_raw_contract():
    # 0.30 иены = 300 тиков; потеря за тик на лот = 100 JPY = 100/150 USD; на лот 300 * 0.6667 = 200 USD -> 5 лотов
    via_tick = ok(size_position(USDJPY, side="BUY", entry=150.0, sl=149.70, tick_value=100 / 150, **kw(leverage=None, margin_per_lot=1000.0)))
    via_rate = ok(size_position(USDJPY, side="BUY", entry=150.0, sl=149.70, quote_to_account=1 / 150, **kw(leverage=None, margin_per_lot=1000.0)))
    assert via_tick.lots == via_rate.lots == 5.0 and via_tick.loss_per_lot == pytest.approx(200.0) == pytest.approx(via_rate.loss_per_lot)
    with pytest.raises(ConfigError, match="lossTickValue"):  # без курса JPY->USD считать нельзя: молча принять 1.0 было бы неверно в 150 раз
        size_position(USDJPY, side="BUY", entry=150.0, sl=149.70, **kw())


def test_index_cfd_with_fractional_lots():
    # контракт 1, тик 0.01 = 0.01 USD на лот; SL 25 пунктов = 25 USD на лот -> 40 лотов
    assert ok(size_position(US500, side="BUY", entry=5000.0, sl=4975.0, tick_value=0.01, **kw(leverage=20))).lots == 40.0


def test_lots_are_floored_to_the_step_never_rounded_up():
    # сырой 1000/195.5 = 5.1150...: шаг 0.01 -> 5.11; шаг 0.1 -> 5.1; шаг 1 -> 5; риск не выше целевого
    for step, expected in ((0.01, 5.11), (0.1, 5.1), (1.0, 5.0)):
        s = spec("EURUSD", "EURUSD", volumeStep=step, minVolume=min(step, 0.01))
        plan = ok(size_position(s, side="BUY", entry=1.1, sl=1.1 - 0.001955, tick_value=1.0, **kw()))
        assert plan.lots == pytest.approx(expected)
        assert plan.risk_amount <= plan.risk_target + 1e-9 and plan.risk_amount <= RISK + 1e-9


def test_floor_to_step_has_no_float_noise():
    s = spec("EURUSD", "EURUSD", volumeStep=0.1)
    assert floor_to_step(s, 0.1 + 0.2) == pytest.approx(0.3)  # в float 0.30000000000000004 / 0.1 = 3.0000000000000004, но 0.2+0.1 даёт и 2.9999...
    assert floor_to_step(s, 0.29) == pytest.approx(0.2)


def test_mirror_buy_sell_gives_equal_lots():
    buy = ok(size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, **kw()))
    sell = ok(size_position(EURUSD, side="SELL", entry=1.1, sl=1.102, tick_value=1.0, **kw()))
    assert buy.lots == sell.lots


# ------------------------------------------------------------------------------------------------ отказы


def test_below_min_size():
    # риск 3 USD, на лот 200 -> 0.015 лота -> вниз до 0.01: ровно минимум, сделка допустима (риск 2 USD <= 3)
    P = lambda usd: Mt5RiskParams(risk_usdt=usd)  # noqa: E731
    edge = ok(size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=300.0, free_margin=300.0, leverage=1000, params=P(3.0)))
    assert edge.lots == 0.01 and edge.risk_amount == pytest.approx(2.0)
    r = size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=100.0, free_margin=100.0, leverage=100, params=P(1.0))  # риск 1 USD -> 0.005 лота
    assert r.reason == "below_min_size" and r.detail["risk_needed_for_min_lot"] == pytest.approx(2.0)


def test_above_max_volume_is_exposure_and_not_clamped():
    r = size_position(US500, side="BUY", entry=5000.0, sl=4995.0, tick_value=0.01, **kw(leverage=20))  # 5 USD на лот -> 200 лотов > 50
    assert isinstance(r, Mt5Rejected) and r.reason == "exposure" and r.detail["why"] == "above_max_volume" and r.detail["lots"] == 200.0


def test_margin_limit_is_exposure():
    # 5 лотов EURUSD при плече 30: 100000 * 1.1 / 30 = 3666.67 на лот -> 18 333 USD
    common = dict(side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=EQUITY, leverage=30, params=Mt5RiskParams(risk_usdt=RISK))
    r = size_position(EURUSD, free_margin=10_000.0, **common)
    assert isinstance(r, Mt5Rejected) and r.reason == "exposure" and r.detail["why"] == "margin" and r.detail["margin_required"] == pytest.approx(5 * 3666.6667, rel=1e-4)
    assert ok(size_position(EURUSD, free_margin=20_000.0, **common)).margin_required == pytest.approx(18333.33, rel=1e-4)
    assert size_position(EURUSD, free_margin=20_000.0, **{**common, "params": Mt5RiskParams(risk_usdt=RISK, max_margin_use=0.5)}).reason == "exposure"  # запас по марже


def test_explicit_margin_per_lot_overrides_the_leverage_estimate():
    r = size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=EQUITY, free_margin=1_000.0, margin_per_lot=300.0, params=Mt5RiskParams(risk_usdt=RISK))  # 5 * 300 = 1500
    assert r.reason == "exposure" and r.detail["margin_required"] == pytest.approx(1500.0)


def test_bad_stops_and_inputs():
    wrong = size_position(EURUSD, side="BUY", entry=1.1, sl=1.102, tick_value=1.0, **kw())  # SL выше входа у покупки
    assert wrong.reason == "bad_risk"
    assert size_position(EURUSD, side="SELL", entry=1.1, sl=1.098, tick_value=1.0, **kw()).reason == "bad_risk"
    assert size_position(EURUSD, side="BUY", entry=1.1, sl=1.1, tick_value=1.0, **kw()).reason == "bad_risk"
    far = spec("EURUSD", "EURUSD", stopsLevel=50)  # 50 пунктов = 0.0005
    close = size_position(far, side="BUY", entry=1.1, sl=1.0997, tick_value=1.0, **kw())
    assert close.reason == "stop_too_close" and close.detail["min_distance"] == pytest.approx(0.0005)
    assert isinstance(size_position(far, side="BUY", entry=1.1, sl=1.0990, tick_value=1.0, **kw()), Mt5OrderPlan)
    assert size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=None, free_margin=1.0, leverage=30).reason == "equity_unavailable"
    assert size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=0.0, free_margin=1.0, leverage=30).reason == "equity_unavailable"


def test_programming_errors_raise_config_error():
    with pytest.raises(ConfigError):
        size_position(EURUSD, side="LONG", entry=1.1, sl=1.098, tick_value=1.0, **kw())
    with pytest.raises(ConfigError):
        size_position(EURUSD, side="BUY", entry=0, sl=1.098, tick_value=1.0, **kw())
    with pytest.raises(ConfigError, match="margin_per_lot"):
        size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=EQUITY, free_margin=EQUITY)
    with pytest.raises(ConfigError):
        loss_per_lot(EURUSD, 0.0, tick_value=1.0)
    with pytest.raises(ConfigError):
        Mt5RiskParams(risk_usdt=0)
    with pytest.raises(ConfigError):
        Mt5RiskParams(max_margin_use=0)


def test_helpers():
    assert approx_margin_per_lot(EURUSD, 1.1, 100) == pytest.approx(1100.0)
    assert loss_per_lot(XAUUSD, 10.0, tick_value=1.0) == pytest.approx(1000.0)
    assert loss_per_lot(XAUUSD, 10.0, quote_to_account=1.0) == pytest.approx(1000.0)  # 10 * 100 унций


def test_default_risk_is_fixed_five_usd_regardless_of_equity_and_stop_size():
    for equity in (100.0, 100_000.0):  # EURUSD: потеря на лот при SL 20 пипсов = 200 USD -> 5 / 200 = 0.025 -> 0.02 лота
        plan = ok(size_position(EURUSD, side="BUY", entry=1.1, sl=1.098, tick_value=1.0, equity=equity, free_margin=equity, leverage=100))
        assert plan.lots == 0.02 and plan.risk_target == 5.0 and plan.risk_amount == pytest.approx(4.0) and plan.risk_amount <= 5.0


def test_default_risk_below_one_minimum_lot_is_rejected_not_rounded_up():
    r = size_position(EURUSD, side="BUY", entry=1.1, sl=1.09, tick_value=1.0, equity=EQUITY, free_margin=EQUITY, leverage=100)  # 5 / 1000 = 0.005 < 0.01
    assert isinstance(r, Mt5Rejected) and r.reason == "below_min_size"
