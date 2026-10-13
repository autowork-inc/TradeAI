"""CoinOwnershipLock (D4), RiskSizer (D10, О8/О12), типы (Signal, Tick, журнал)."""

import json

import pytest

from orchestrator.journal import JsonlEventLog, MemoryEventLog
from orchestrator.lock import CoinOwnershipLock, LockError, Owner
from orchestrator.risk import RiskSizer, Sizing, SizingRejected
from orchestrator.types import Direction, Event, Kind, Signal, Tick, geometry
from tests.orchestrator.helpers import IV, mk
from utils.errors import ConfigError, DataError

A, B = Owner("smc", "t1"), Owner("breakout", "t2")


def test_first_owner_wins_and_other_is_refused_with_holder_named_d4():
    lock = CoinOwnershipLock()
    assert lock.try_acquire("BTCUSDT", A).acquired
    res = lock.try_acquire("BTCUSDT", B)
    assert not res.acquired and res.held_by == A
    assert lock.try_acquire("ETHUSDT", B).acquired  # другая монета свободна
    assert lock.owner("BTCUSDT") == A and set(lock.snapshot()) == {"BTCUSDT", "ETHUSDT"}


def test_release_by_owner_frees_the_coin():
    lock = CoinOwnershipLock()
    lock.try_acquire("BTCUSDT", A)
    lock.release("BTCUSDT", A)
    assert lock.owner("BTCUSDT") is None and lock.try_acquire("BTCUSDT", B).acquired


def test_protocol_violations_are_errors_not_silence_r1():
    lock = CoinOwnershipLock()
    with pytest.raises(LockError):
        lock.release("BTCUSDT", A)  # освобождать нечего
    lock.try_acquire("BTCUSDT", A)
    with pytest.raises(LockError):
        lock.release("BTCUSDT", B)  # чужая блокировка
    with pytest.raises(LockError):
        lock.try_acquire("BTCUSDT", A)  # повторный захват тем же владельцем


# ------------------------------------------------------------------------------------------- RiskSizer


def test_sizing_one_percent_risk_and_exposure():
    s = RiskSizer().size(10_000, entry=100.0, sl=105.0)
    assert isinstance(s, Sizing)
    assert s.risk_amount == pytest.approx(100.0) and s.qty == pytest.approx(20.0)  # 100 / R(5)
    assert s.notional == pytest.approx(2000.0) and s.exposure == pytest.approx(0.2)


def test_stop_03_percent_needs_about_3_3x_and_passes_cap_5_o12():
    s = RiskSizer().size(10_000, entry=100.0, sl=100.3)
    assert isinstance(s, Sizing) and s.exposure == pytest.approx(0.01 / 0.003)


def test_exposure_above_cap_is_rejected_not_shrunk_o8():
    r = RiskSizer().size(10_000, entry=100.0, sl=100.1)  # плечо 10 > 5
    assert isinstance(r, SizingRejected) and r.reason == "exposure" and r.exposure == pytest.approx(10.0)
    assert isinstance(RiskSizer(max_exposure=1.0).size(10_000, 100.0, 100.5), SizingRejected)  # старый потолок 1.0
    assert isinstance(RiskSizer().size(10_000, 100.0, 100.0), SizingRejected)  # нулевой риск


def test_sizer_validates_input_and_config():
    with pytest.raises(DataError):
        RiskSizer().size(0, 100.0, 105.0)
    with pytest.raises(ConfigError):
        RiskSizer(risk_per_trade=0)
    assert RiskSizer().max_exposure == 5.0 and RiskSizer().risk_per_trade == 0.01


# ------------------------------------------------------------------------------------------ типы


def test_signal_validation_catches_wrong_side_prices_at_creation_r1():
    assert mk().direction is Direction.SHORT
    with pytest.raises(DataError, match="SL"):
        mk(sl=99.0)  # шорт: SL ниже входа
    with pytest.raises(DataError, match="TP"):
        mk(tp=101.0)  # шорт: TP выше входа
    with pytest.raises(DataError, match="SL"):
        mk(direction=Direction.LONG, sl=101.0)
    with pytest.raises(DataError, match="tp и inval"):
        Signal("x", "smc", "BTCUSDT", "15m", Direction.SHORT, Kind.LIMIT, 0, IV, 100.0, 105.0, None, None)
    with pytest.raises(DataError, match="формате"):
        Signal("x", "smc", "BTCUSDT", "15m", Direction.SHORT, Kind.MARKET, 0, IV, 100.0, 105.0, None, None, geometry=({"type": "zone"},))


def test_geometry_unified_format_and_tick_validation():
    g = geometry("zone", 1, 2, 3.0, 4.0, "FVG")
    assert tuple(g) == ("type", "t1", "t2", "p1", "p2", "label")
    with pytest.raises(DataError):
        geometry("circle", 1, 2, 3.0, 4.0, "x")
    with pytest.raises(DataError):
        Tick("BTCUSDT", 0.0, 1, "trade")
    with pytest.raises(DataError):
        Tick("BTCUSDT", 1.0, 1, "bid")


# ---------------------------------------------------------------------------------------------- журнал


def test_jsonl_journal_writes_one_json_line_per_event_and_is_strict(tmp_path):
    log = JsonlEventLog(tmp_path / "sub" / "events.jsonl")
    log.emit(Event(5, "signal_accepted", "BTCUSDT", "smc", "id1", {"direction": Direction.SHORT, "entry": 100.0}))
    log.emit(Event(6, "trade_closed", "BTCUSDT", "smc", "id1", {"net_r": -1.1}))
    with pytest.raises(TypeError):
        log.emit(Event(7, "x", "BTCUSDT", "smc", None, {"bad": object()}))  # не прячем баг через default=str
    with pytest.raises(ValueError):
        log.emit(Event(8, "x", "BTCUSDT", "smc", None, {"nan": float("nan")}))
    log.close()
    lines = (tmp_path / "sub" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(x)["kind"] for x in lines] == ["signal_accepted", "trade_closed"]
    assert json.loads(lines[0])["detail"]["direction"] == "short"
    assert len(log.events) == 2 and MemoryEventLog().events == []
