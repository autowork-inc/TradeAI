"""Запись боевых сделок в БД (через MemoryRecorder - эталон формата PgRecorder): id `<signal_id>:live`, mode='live', фактические цены,
копия снапшота, закрытия, PnL с учётом направления и комиссий, восстановление после рестарта, теневые строки не затронуты."""

import asyncio

import pytest

import config
from orchestrator.live_engine import LIVE_ID_SUFFIX, leg_pnl, live_trade_id
from orchestrator.types import Direction
from persistence.recorder import MemoryRecorder
from tests.orchestrator.live_helpers import signal
from tests.orchestrator.test_live_engine import NOW, SYM, Env, run
from utils.errors import ConfigError, DataError

TAKER = config.LIVE_TAKER_FEE_RATE


def shadow_snapshot(sig) -> dict:
    return {"trade_id": sig.signal_id, "strategy_id": sig.strategy, "created_at": NOW, "candles": [[1, 2, 3]], "geometry": [{"type": "line"}],
            "meta": {"htf_label": "Bullish"}}


def make_env(tmp_path, sig, *, with_shadow=True, price=100.0) -> tuple[Env, MemoryRecorder]:
    rec = MemoryRecorder()
    if with_shadow:
        rec.snapshot(shadow_snapshot(sig))
        rec.trade({"id": sig.signal_id, "mode": "shadow", "status": "open"})
    return Env(tmp_path, price=price, recorder=rec), rec


def test_entry_is_written_as_live_row_with_real_fill_price_and_copied_snapshot(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig, price=100.5)
    run(env.engine.execute(sig, NOW))
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert row["id"] == sig.signal_id + LIVE_ID_SUFFIX and row["mode"] == "live" and row["status"] == "open"
    assert row["entry_price"] == pytest.approx(100.5) and row["qty"] == pytest.approx(1.0)  # цена биржи, а не сигнала (100.0)
    assert row["plan"]["live"] is True and row["plan"]["fees_estimated"] is True
    assert rec.trades[sig.signal_id] == {"id": sig.signal_id, "mode": "shadow", "status": "open"}  # теневая строка не тронута
    snap = next(s for s in rec.snapshots if s["trade_id"] == live_trade_id(sig.signal_id))
    assert snap["candles"] == [[1, 2, 3]] and snap["meta"]["live"] is True and snap["meta"]["source_trade_id"] == sig.signal_id


def test_long_stop_loss_is_closed_in_db_with_negative_pnl_and_fees(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    env.rest.set_price(98.0)
    run(env.engine.poll())
    row = rec.trades[live_trade_id(sig.signal_id)]
    fees = TAKER * 100.0 + TAKER * 98.0
    assert row["status"] == "closed" and row["exit_reason"] == "sl" and row["exit_price"] == pytest.approx(98.0)
    assert row["fees"] == pytest.approx(fees) and row["pnl"] == pytest.approx(-2.0 - fees)  # лонг: (98 - 100) * 1 - комиссии
    assert row["r_multiple"] == pytest.approx((-2.0 - fees) / 2.0) and row["plan"]["exit_price_source"] == "exchange"
    (leg,) = rec.closes
    assert leg["reason"] == "sl" and leg["frac"] == pytest.approx(1.0) and leg["trade_id"] == live_trade_id(sig.signal_id)
    assert rec.trades[sig.signal_id]["status"] == "open"


def test_short_stop_loss_subtracts_in_the_short_direction(tmp_path):
    sig = signal(direction=Direction.SHORT, entry=100.0, sl=102.0)
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    env.rest.set_price(102.0)
    run(env.engine.poll())
    row = rec.trades[live_trade_id(sig.signal_id)]
    fees = TAKER * 100.0 + TAKER * 102.0
    assert row["status"] == "closed" and row["pnl"] == pytest.approx(-2.0 - fees) and row["r_multiple"] < 0  # шорт: рост цены - убыток


def test_scale_out_levels_write_each_close_and_final_row_sums_them(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    env.rest.set_price(102.0)
    run(env.engine.poll())
    mid = rec.trades[live_trade_id(sig.signal_id)]
    assert mid["status"] == "open" and mid["be_armed"] is True and mid["remaining_frac"] == pytest.approx(0.5)
    env.rest.set_price(106.0)
    run(env.engine.poll())
    env.rest.set_price(100.0)  # runner выбит безубытком
    run(env.engine.poll())
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert [c["reason"] for c in rec.closes] == ["scale_1r", "scale_3r", "be"] and [c["seq"] for c in rec.closes] == [1, 2, 3]
    assert sum(c["frac"] for c in rec.closes) == pytest.approx(1.0)
    assert row["status"] == "closed" and row["pnl"] == pytest.approx(sum(c["pnl"] for c in rec.closes))
    assert row["pnl"] > 0 and row["r_multiple"] > 0


def test_timeout_close_is_recorded_with_market_price(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    env.trade().timeout_pending = True
    env.rest.set_price(101.0)
    run(env.engine.poll())
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert row["status"] == "closed" and row["exit_reason"] == "timeout" and row["exit_price"] == pytest.approx(101.0)
    assert row["plan"]["exit_price_source"] in ("exchange", "last_tick")


def test_leg_pnl_respects_direction_and_subtracts_fees():
    g, f, n = leg_pnl(+1.0, 100.0, 103.0, 2.0, 0.0005, 0.0005)
    assert g == pytest.approx(6.0) and f == pytest.approx(0.0005 * 200 + 0.0005 * 206) and n == pytest.approx(g - f)
    g, f, n = leg_pnl(-1.0, 100.0, 103.0, 2.0, 0.0002, 0.0005)
    assert g == pytest.approx(-6.0) and f == pytest.approx(0.0002 * 200 + 0.0005 * 206) and n == pytest.approx(g - f)


def test_missing_source_snapshot_is_fail_fast_on_a_new_trade(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, _ = make_env(tmp_path, sig, with_shadow=False)
    with pytest.raises(DataError):
        run(env.engine.execute(sig, NOW))


def test_restart_rewrites_the_open_trade_and_revives_an_aborted_row(tmp_path):
    sig = signal(entry=100.0, sl=98.0)
    env, rec = make_env(tmp_path, sig)
    run(env.engine.execute(sig, NOW))
    lid = live_trade_id(sig.signal_id)
    rec.trades[lid]["status"] = "aborted"  # как будто строку пометили оборванной
    del rec.snapshots[:]  # источника снапшота после рестарта тоже нет
    engine = env.reborn()
    run(engine.recover([SYM]))
    assert rec.trades[lid]["status"] == "open" and rec.trades[lid]["mode"] == "live" and rec.trades[lid]["entry_price"] == pytest.approx(100.0)
    assert lid in rec.missing_snapshots  # отсутствие снапшота при восстановлении не роняет процесс


def test_recorder_is_optional_and_null_by_default(tmp_path):
    env = Env(tmp_path)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    env.rest.set_price(98.0)
    run(env.engine.poll())
    assert SYM not in env.engine.trades


def test_shadow_run_refuses_live_mode_without_database(monkeypatch):
    import scripts.shadow_run as sr

    monkeypatch.setattr(config, "TRADE_MODE", "live")
    monkeypatch.setattr(config, "DATABASE_URL", "")
    monkeypatch.setattr(config, "validate", lambda: None)
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        asyncio.run(sr.shadow_main(sr.parse_args([])))
