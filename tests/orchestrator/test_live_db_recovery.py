"""State Recovery при старте (2026-10-18): открытые боевые сделки из БД, которых нет в файле состояния, сверяются с биржей.

Сценарий «файл состояния потерян»: сделка открыта и записана в БД (MemoryRecorder), файл состояния удалён, за время простоя на бирже что-то
произошло (стоп / уровень / ничего). Новый движок получает строки БД (`DbOpenTrade`) и обязан: закрытую позицию дописать в БД по фактическим
исполнениям (`userTrades`), живую - подхватить под тем же id и сопровождать дальше, противоречие - Safe Mode.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import config
from exchange.binance_live import USER_TRADES_PAGE, USER_TRADES_WINDOW_MS, BinanceLiveClient
from orchestrator.live_engine import LiveExecutionError, live_trade_id
from orchestrator.types import Direction
from persistence.pg import from_db
from persistence.recorder import DbOpenTrade, MemoryRecorder
from tests.orchestrator.fake_live_rest import FakeRest
from tests.orchestrator.live_helpers import FILTERS, STRATEGY, signal
from tests.orchestrator.test_live_engine import NOW, SYM, Env, run
from utils.errors import DataError

TAKER = config.LIVE_TAKER_FEE_RATE


def opened(tmp_path, sig=None, price=100.0):
    sig = sig or signal(entry=100.0, sl=98.0)
    rec = MemoryRecorder()
    rec.snapshot({"trade_id": sig.signal_id, "strategy_id": sig.strategy, "created_at": NOW, "candles": [], "geometry": [], "meta": {}})
    env = Env(tmp_path, price=price, recorder=rec)
    env.rest.filters = dict(FILTERS)
    env.rest.now_ms = NOW
    run(env.engine.execute(sig, NOW))
    return env, rec, sig


def db_rows(rec, sig) -> list[DbOpenTrade]:
    lid = live_trade_id(sig.signal_id)
    return [DbOpenTrade(dict(rec.trades[lid]), tuple(c for c in rec.closes if c["trade_id"] == lid))]


def lose_state_and_restart(env, rows):
    (env.tmp_path / "state.json").unlink()
    env.clock.t = NOW + 10_000
    engine = env.reborn()
    run(engine.recover([SYM], rows))
    return engine


def test_position_closed_by_stop_while_down_is_closed_in_db_with_exchange_price_and_pnl(tmp_path):
    env, rec, sig = opened(tmp_path)
    rows = db_rows(rec, sig)
    env.rest.now_ms = NOW + 5_000
    env.rest.set_price(98.0)  # стоп сработал, пока бота не было
    engine = lose_state_and_restart(env, rows)
    row = rec.trades[live_trade_id(sig.signal_id)]
    fees = TAKER * 100.0 + 0.0005 * 98.0  # вход - по ставке, выход - фактическая комиссия из userTrades
    assert row["status"] == "closed" and row["exit_reason"] == "closed_offline" and row["exit_price"] == pytest.approx(98.0)
    assert row["pnl"] == pytest.approx(-2.0 - fees) and row["r_multiple"] == pytest.approx((-2.0 - fees) / 2.0)
    assert row["exit_time"] == NOW + 5_000 and row["stop_price"] is None and row["remaining_frac"] == 0.0 and not row["gap_affected"]
    assert row["plan"]["exit_price_source"] == "exchange" and row["plan"]["reconciled_offline"] is True
    (leg,) = rec.closes
    assert (leg["seq"], leg["reason"], leg["qty"], leg["price"]) == (1, "closed_offline", pytest.approx(1.0), pytest.approx(98.0))
    assert SYM not in engine.trades and engine.open_symbols() == []
    assert [e.detail["source"] for e in env.journal.of_kind("live_closed_offline")] == ["exchange"]


def test_offline_close_after_a_recorded_partial_continues_the_close_sequence(tmp_path):
    env, rec, sig = opened(tmp_path)
    env.rest.now_ms = NOW + 1_000
    env.rest.set_price(102.0)  # первый уровень (+1R, половина) - записан в БД при работающем боте
    env.clock.t = NOW + 2_000
    run(env.engine.poll())
    rows = db_rows(rec, sig)
    env.rest.now_ms = NOW + 5_000
    env.rest.set_price(100.0)  # остаток выбит безубытком, пока бота не было
    lose_state_and_restart(env, rows)
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert [c["seq"] for c in rec.closes] == [1, 2] and rec.closes[1]["reason"] == "closed_offline"
    assert rec.closes[1]["qty"] == pytest.approx(0.5) and rec.closes[1]["price"] == pytest.approx(100.0)
    assert row["status"] == "closed" and row["exit_price"] == pytest.approx(101.0)  # (0.5 * 102 + 0.5 * 100) / 1
    assert row["pnl"] == pytest.approx(sum(c["pnl"] for c in rec.closes)) and row["pnl"] > 0


def test_open_position_is_picked_up_under_its_own_id_and_managed_further(tmp_path):
    env, rec, sig = opened(tmp_path)
    rows = db_rows(rec, sig)
    engine = lose_state_and_restart(env, rows)
    t = engine.trades[SYM]
    assert t.db_id == live_trade_id(sig.signal_id) and t.plan.strategy == STRATEGY and not t.adopted and t.recovered
    assert t.risk == pytest.approx(2.0) and t.stop_price == 98.0 and engine.open_symbols() == [SYM]  # монета - в потоки (open_symbols)
    assert env.journal.of_kind("live_recovered_db") and not env.journal.of_kind("live_adopted")
    env.rest.set_price(98.0)
    run(engine.poll())
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert row["status"] == "closed" and row["exit_reason"] == "sl" and row["strategy_id"] == STRATEGY
    assert row["r_multiple"] == pytest.approx((-2.0 - TAKER * 100.0 - TAKER * 98.0) / 2.0)


def test_open_position_after_a_level_keeps_the_original_volume_and_close_numbering(tmp_path):
    env, rec, sig = opened(tmp_path)
    env.rest.set_price(102.0)
    run(env.engine.poll())
    rows = db_rows(rec, sig)
    engine = lose_state_and_restart(env, rows)
    t = engine.trades[SYM]
    assert t.filled_qty == pytest.approx(1.0) and t.remaining == pytest.approx(0.5) and t.legs_put == 1
    env.rest.set_price(100.0)
    run(engine.poll())
    assert [c["seq"] for c in rec.closes] == [1, 2]
    assert sum(c["frac"] for c in rec.closes) == pytest.approx(1.0)


def test_position_against_the_db_trade_trips_safe_mode(tmp_path):
    env, rec, sig = opened(tmp_path)
    rows = db_rows(rec, sig)
    env.rest.positions[SYM] = -1.0  # на бирже шорт, а в БД открыт лонг
    (env.tmp_path / "state.json").unlink()
    with pytest.raises(LiveExecutionError):
        run(env.reborn().recover([SYM], rows))
    assert env.safe.active


def test_missing_fills_close_with_unknown_price_outside_r_statistics(tmp_path):
    env, rec, sig = opened(tmp_path)
    rows = db_rows(rec, sig)
    env.rest.positions[SYM] = 0.0  # позицию закрыли, но исполнений биржа не отдала
    lose_state_and_restart(env, rows)
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert row["status"] == "closed" and row["gap_affected"] is True and row["plan"]["exit_price_source"] == "unknown"


def test_trade_known_from_the_state_file_is_not_reconciled_twice(tmp_path):
    env, rec, sig = opened(tmp_path)
    rows = db_rows(rec, sig)
    engine = env.reborn()
    run(engine.recover([SYM], rows))
    assert env.rest.calls("GET", config.USER_TRADES_PATH) == [] and not env.journal.of_kind("live_recovered_db")
    assert engine.trades[SYM].db_id == live_trade_id(sig.signal_id)


def test_short_offline_close_counts_profit_on_a_falling_price(tmp_path):
    env, rec, sig = opened(tmp_path, signal(direction=Direction.SHORT, entry=100.0, sl=102.0))
    rows = db_rows(rec, sig)
    env.rest.now_ms = NOW + 5_000
    env.rest.price = 97.0
    env.rest._move(SYM, "BUY", 1.0, 97.0, "manual")  # закрыли вручную по 97
    lose_state_and_restart(env, rows)
    row = rec.trades[live_trade_id(sig.signal_id)]
    assert row["exit_price"] == pytest.approx(97.0) and row["pnl"] == pytest.approx(3.0 - TAKER * 100.0 - 0.0005 * 97.0)


def test_non_live_row_is_rejected(tmp_path):
    env, rec, sig = opened(tmp_path)
    row = dict(rec.trades[live_trade_id(sig.signal_id)], mode="shadow")
    (env.tmp_path / "state.json").unlink()
    with pytest.raises(DataError):
        run(env.reborn().recover([SYM], [DbOpenTrade(row)]))


# ------------------------------------------------------------------------------------- клиент и БД


def test_user_trades_walks_seven_day_windows_and_fails_on_a_full_page():
    rest = FakeRest()
    client = BinanceLiveClient(rest)
    asyncio.run(client.user_trades(SYM, 0, 2 * USER_TRADES_WINDOW_MS))
    windows = [(p["startTime"], p["endTime"]) for p in rest.calls("GET", config.USER_TRADES_PATH)]
    assert len(windows) == 3 and windows[0] == (0, USER_TRADES_WINDOW_MS - 1) and windows[-1][1] == 2 * USER_TRADES_WINDOW_MS
    rest.fills = [{"symbol": SYM, "side": "SELL", "qty": 0.1, "price": 1.0, "time": 5} for _ in range(USER_TRADES_PAGE)]
    with pytest.raises(DataError):
        asyncio.run(client.user_trades(SYM, 0, 10))


def test_from_db_converts_back_to_raw_row_types():
    assert from_db("ts", datetime.fromtimestamp(1.5, tz=timezone.utc)) == 1500
    assert from_db("num", Decimal("1.25")) == 1.25 and from_db("json", '{"a": 1}') == {"a": 1} and from_db("text", None) is None
