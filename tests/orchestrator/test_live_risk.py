"""LiveRiskManager: фильтр стратегий, фиксированный объём, суточный лимит убытка с защёлкой до 00:00 UTC, стоп vs ликвидация."""

import json

import pytest

from orchestrator.live_risk import DAY_MS, LiveOrderPlan, LiveRejected, LiveRiskManager, utc_day_start_ms
from orchestrator.types import Direction, Kind
from tests.orchestrator.live_helpers import FILTERS, STRATEGY, T0, manager, signal
from utils.errors import ConfigError, DataError

NOW = T0 + 60_000


def ready(tmp_path, **kw) -> LiveRiskManager:
    m = manager(tmp_path, **kw)
    m.update_daily_pnl(0.0, NOW)  # свежий PnL: без него новых сделок нет
    return m


def test_fixed_notional_is_margin_times_leverage_and_qty_is_rounded_down(tmp_path):
    m = ready(tmp_path)
    plan = m.evaluate(signal(entry=100.0, sl=98.0), NOW)
    assert isinstance(plan, LiveOrderPlan)
    assert (plan.qty, plan.leverage, plan.side, plan.close_side) == (1.0, 10, "BUY", "SELL")
    assert (plan.notional_usdt, plan.margin_usdt, plan.max_loss_usdt) == (100.0, 10.0, 2.0)
    m.release("SOLUSDT", plan.signal_id)
    odd = m.evaluate(signal(entry=33.33, sl=32.5, n=2), NOW)  # 100 / 33.33 = 3.0003 -> 3.0; 99.99 USDT, а не больше 100
    assert odd.qty == 3.0 and odd.notional_usdt <= 100.0 and odd.margin_usdt <= 10.0
    m.release("SOLUSDT", odd.signal_id)
    up = m.evaluate(signal(entry=28.0, sl=27.0, n=3), NOW)  # 100 / 28 = 3.571: вниз до 3.5 (а не к ближайшему 3.6 = 100.8 USDT)
    assert up.qty == 3.5 and up.notional_usdt == 98.0


def test_short_side_and_limit_prices_are_put_on_the_tick_grid(tmp_path):
    m = ready(tmp_path)
    plan = m.evaluate(signal(direction=Direction.SHORT, kind=Kind.LIMIT, entry=100.004, sl=101.996, tp=94.001, inval=101.996), NOW)
    assert (plan.side, plan.close_side) == ("SELL", "BUY")
    assert (plan.entry, plan.sl, plan.tp) == (100.0, 102.0, 94.0)


def test_unapproved_strategy_is_rejected_and_nothing_is_reserved(tmp_path):
    m = ready(tmp_path)
    r = m.evaluate(signal(strategy="OBI_Breakout"), NOW)
    assert r == LiveRejected("strategy_not_approved") and m.busy() == {}
    assert m.is_approved(STRATEGY) and not m.is_approved("OBI_Breakout")


def test_daily_loss_limit_blocks_everything_until_midnight_utc(tmp_path):
    m = ready(tmp_path)
    assert not m.update_daily_pnl(-49.99, NOW)  # ещё в пределах лимита
    first = m.evaluate(signal(), NOW)
    assert isinstance(first, LiveOrderPlan)
    m.release("SOLUSDT", first.signal_id)
    assert m.update_daily_pnl(-50.0, NOW + 1000)  # достигнут: блокировка включена ИМЕННО сейчас
    assert not m.update_daily_pnl(-80.0, NOW + 2000)  # повторно не «включается» заново
    midnight = utc_day_start_ms(NOW) + DAY_MS
    assert m.blocked_until_ms == midnight
    for t in (NOW + 3000, midnight - 1):
        r = m.evaluate(signal(n=2), t)
        assert r.reason == "trading_blocked", t
        m.update_daily_pnl(-80.0, t)  # свежий PnL не отменяет блокировку
    m.update_daily_pnl(0.0, midnight)  # новые сутки: PnL с биржи уже с нуля
    assert isinstance(m.evaluate(signal(n=3), midnight), LiveOrderPlan)


def test_block_survives_a_restart_and_a_corrupt_latch_is_an_error(tmp_path):
    m = ready(tmp_path)
    m.update_daily_pnl(-75.0, NOW)
    reborn = LiveRiskManager(FILTERS, approved_strategies=(STRATEGY,), margin_usdt=10, leverage=10, daily_loss_limit_usdt=50,
                             state_path=tmp_path / "risk.json")
    assert reborn.blocked(NOW + 1) and not reborn.blocked(utc_day_start_ms(NOW) + DAY_MS)
    assert json.loads((tmp_path / "risk.json").read_text())["pnl_usdt"] == -75.0
    (tmp_path / "risk.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(DataError):
        LiveRiskManager(FILTERS, approved_strategies=(STRATEGY,), margin_usdt=10, leverage=10, daily_loss_limit_usdt=50,
                        state_path=tmp_path / "risk.json")


def test_without_fresh_pnl_there_are_no_new_trades(tmp_path):
    m = manager(tmp_path)
    assert m.evaluate(signal(), NOW).reason == "pnl_unavailable"  # PnL ни разу не получен
    m.update_daily_pnl(0.0, NOW)
    plan = m.evaluate(signal(), NOW + 119_000)
    assert isinstance(plan, LiveOrderPlan)
    m.release("SOLUSDT", plan.signal_id)
    assert m.evaluate(signal(n=2), NOW + 121_000).reason == "pnl_unavailable"  # протух (> 120 с)


def test_one_position_per_symbol_until_released(tmp_path):
    m = ready(tmp_path)
    first = m.evaluate(signal(), NOW)
    again = m.evaluate(signal(direction=Direction.SHORT, entry=100.0, sl=102.0, n=2), NOW)
    assert again == LiveRejected("symbol_busy", {"held_by": first.signal_id})
    assert isinstance(m.evaluate(signal(symbol="XRPUSDT", entry=2.0, sl=1.9), NOW), LiveOrderPlan)  # другая монета свободна
    with pytest.raises(DataError):
        m.release("SOLUSDT", "чужой")
    m.release("SOLUSDT", first.signal_id)
    with pytest.raises(DataError):
        m.release("SOLUSDT", first.signal_id)  # повторный release - ошибка протокола
    assert isinstance(m.evaluate(signal(n=3), NOW), LiveOrderPlan)


def test_stop_beyond_the_liquidation_buffer_is_rejected(tmp_path):
    m = ready(tmp_path)  # 10x: буфер 0.8 / 10 = 8%
    ok = m.evaluate(signal(entry=100.0, sl=92.5), NOW)  # 7.5%
    assert isinstance(ok, LiveOrderPlan)
    m.release("SOLUSDT", ok.signal_id)
    r = m.evaluate(signal(entry=100.0, sl=91.0, n=2), NOW)  # 9% - ликвидация раньше стопа
    assert r.reason == "sl_too_far" and r.detail["max"] == pytest.approx(0.08)
    m5 = ready(tmp_path / "x", leverage=5, margin=20.0)  # при 5x буфер 16%
    assert isinstance(m5.evaluate(signal(entry=100.0, sl=86.0), NOW), LiveOrderPlan)


def test_below_minimum_size_and_degenerate_stop(tmp_path):
    m = ready(tmp_path, margin=0.4)  # 4 USDT нотионал < MIN_NOTIONAL 5
    r = m.evaluate(signal(), NOW)
    assert r.reason == "below_min_size" and m.busy() == {}
    m2 = ready(tmp_path / "y")
    r2 = m2.evaluate(signal(entry=100.0, sl=100.004, direction=Direction.SHORT), NOW)  # после округления стоп == вход
    assert r2.reason == "bad_risk"


def test_missing_symbol_filters_and_bad_arguments_are_configuration_errors(tmp_path):
    m = ready(tmp_path)
    with pytest.raises(ConfigError):
        m.evaluate(signal(symbol="DOGEUSDT"), NOW)
    for kw in ({"margin": 0}, {"leverage": 0}, {"leverage": 200}, {"limit": 0}, {"approved": ()}, {"approved": ("a", "a")}):
        with pytest.raises(ConfigError):
            manager(tmp_path / "bad", **kw)
    with pytest.raises(DataError):
        m.update_daily_pnl(float("nan"), NOW)


def test_utc_day_start():
    assert utc_day_start_ms(0) == 0 and utc_day_start_ms(DAY_MS - 1) == 0 and utc_day_start_ms(DAY_MS) == DAY_MS
    assert utc_day_start_ms(T0) % DAY_MS == 0 and T0 - utc_day_start_ms(T0) == 10 * 3_600_000


def test_scale_out_plan_must_be_representable_on_the_lot_grid_instead_of_silently_changing_the_math(tmp_path):
    from execution.exchange_filters import SymbolFilters
    from orchestrator.live_risk import split_levels
    from orchestrator.tracker import DEFAULT_PLAN

    f = SymbolFilters("X", 0.01, 0.1, 0.1, 5.0)
    assert split_levels(DEFAULT_PLAN, 1.0, f) == (((1.0, 0.5), (3.0, 0.4)), pytest.approx(0.1))  # 50% / 40% / runner 10%
    assert split_levels(DEFAULT_PLAN, 10.0, f) == (((1.0, 5.0), (3.0, 4.0)), pytest.approx(1.0))
    assert split_levels(DEFAULT_PLAN, 0.5, f) is None  # 0.2 / 0.2 / 0.1 = 40% / 40% / 20%: не 50 / 40 / 10
    big = SymbolFilters("Y", 0.0001, 1.0, 1.0, 5.0)
    levels, runner = split_levels(DEFAULT_PLAN, 8123.0, big)  # остаток округления уходит в runner
    assert [q for _, q in levels] == [4061.0, 3249.0] and runner == 813.0 and sum(q for _, q in levels) + runner == 8123.0
    m = ready(tmp_path, margin=5.0)  # нотионал 50 USDT -> объём 0.5: план не делится
    r = m.evaluate(signal(), NOW)
    assert r.reason == "plan_not_representable" and m.busy() == {}
    assert r.detail["runner_frac"] == pytest.approx(0.1)


def test_exit_plan_comes_from_the_strategy_registry_like_in_shadow(tmp_path):
    from orchestrator.registry import SCALP_1R_PLAN, VWAP_PLAN
    from orchestrator.tracker import DEFAULT_PLAN, ScaleOutPlan

    m = ready(tmp_path, plans={"SMC_Counter_Trend": SCALP_1R_PLAN, STRATEGY: VWAP_PLAN}, approved=(STRATEGY, "SMC_Counter_Trend", "OBI_Breakout"))
    assert m.evaluate(signal(), NOW).exit_plan == VWAP_PLAN
    assert m.evaluate(signal(symbol="XRPUSDT", strategy="SMC_Counter_Trend", entry=2.0, sl=1.9), NOW).exit_plan == SCALP_1R_PLAN
    other = m.evaluate(signal(symbol="XRPUSDT", strategy="OBI_Breakout", entry=2.0, sl=1.9, n=9), NOW)
    assert isinstance(other, LiveRejected) and other.reason == "symbol_busy"  # монета XRP занята контртрендом
    m.release("XRPUSDT", signal(symbol="XRPUSDT", strategy="SMC_Counter_Trend", entry=2.0, sl=1.9).signal_id)
    assert m.evaluate(signal(symbol="XRPUSDT", strategy="OBI_Breakout", entry=2.0, sl=1.9, n=9), NOW).exit_plan == DEFAULT_PLAN  # нет в словаре: общий
    assert isinstance(DEFAULT_PLAN, ScaleOutPlan)
