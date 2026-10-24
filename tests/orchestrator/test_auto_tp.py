"""Авто-подстройка финального тейка (orchestrator/auto_tp.py): Scale Out 50% на +1R + финал X, X - лучший по доигрыванию сделок, без потолка."""

import asyncio
import json
from datetime import datetime, timezone

import pytest

from orchestrator import auto_tp as a
from orchestrator.tracker import ScaleOutPlan

T0 = 1_792_800_000_000 // 60_000 * 60_000
NOW = T0 + 3 * 86_400_000
FEE_IN = 0.0005 * 100  # MARKET, вход 100, R = 1


def bars(*hl, close=None):
    return [(T0 + (i + 1) * a.MIN_MS, h, lo, close if close is not None else (h + lo) / 2) for i, (h, lo) in enumerate(hl)]


def path(*hl, side="long", now=NOW, close=None):
    return a.build_path(100.0, 99.0 if side == "long" else 101.0, side, "market", T0 + 1000, bars(*hl, close=close), now)


def test_scale_plan_is_half_at_1r_and_rest_at_the_final_take():
    assert a.scale_plan(4.0) == ScaleOutPlan(((1.0, 0.5), (4.0, 0.5)))
    assert a.scale_plan(1.0) == ScaleOutPlan(((1.0, 1.0),)) and a.scale_plan(0.75) == ScaleOutPlan(((0.75, 1.0),))


def test_outcome_half_at_1r_then_stop():
    p = path((101.5, 99.5), (100.5, 98.9))  # +1.5R, потом стоп
    r = a.outcome(p, 3.0)
    expected = 0.5 * 1 + 0.5 * -1 - FEE_IN - 0.5 * 0.0005 * 101 - 0.5 * 0.0005 * 99
    assert r == pytest.approx(expected)


def test_no_ceiling_ten_r_is_picked_when_it_pays_most():
    runners = [path((101.0, 99.5), (105.0, 100.5), (110.5, 104.0), (106.0, 98.5)) for _ in range(3)]  # до +10.5R, потом стоп
    losers = [path((100.5, 98.9))] * 2
    x, total = a.best_final_r(runners + losers)
    assert x == 10.5 and total > 0  # сетка до самого дальнего хода, 1:10.5 - лучший
    assert max(a.candidates(runners)) == 10.5


def test_both_stop_and_take_in_one_candle_is_a_stop():
    p = path((102.5, 98.9))
    assert p.stopped and p.fav == ()


def test_unfinished_path_is_not_used():
    assert path((100.5, 99.5), now=T0 + 10 * a.MIN_MS) is None


def rows_for(n, strategy="ICT_Killzones", coin="BTCUSDT"):
    return [{"id": f"t{i}", "strategy": strategy, "coin": coin, "side": "long", "kind": "market", "entry_price": 100.0,
             "entry_time": datetime.fromtimestamp((T0 + 1000) / 1000, tz=timezone.utc), "sl": 99.0} for i in range(n)]


class Api:
    def __init__(self, data):
        self.data, self.calls = data, []

    async def get(self, path, params, weight=1):
        self.calls.append(params)
        return [[b[0], "0", str(b[1]), str(b[2]), str(b[3]), "0"] for b in self.data if b[0] >= params["startTime"]][:1000]


def tuner(tmp_path, rows, data, applied, min_trades=5, strategies=("ICT_Killzones", "VWAP_Reversion")):
    async def fetch(since):
        return rows
    return a.AutoTpTuner(fetch, Api(data), list(strategies), lambda n, p: applied.append((n, p)), state_path=tmp_path / ".auto_tp.json",
                         since=datetime(2026, 10, 22, tzinfo=timezone.utc), min_trades=min_trades, default_r=4.0, skip={"VWAP_Reversion"}, now_ms=lambda: NOW)


def test_tune_applies_best_take_saves_state_and_survives_restart(tmp_path):
    data = bars((101.0, 99.5), (103.2, 100.5), (102.0, 98.5))  # до +3.2R, потом стоп
    applied = []
    t = tuner(tmp_path, rows_for(6), data, applied)
    rep = asyncio.run(t.tune())
    assert rep["ICT_Killzones"]["tp_r"] == 3.0 and rep["ICT_Killzones"]["n"] == 6 and "VWAP_Reversion" not in rep
    assert applied == [("ICT_Killzones", a.scale_plan(3.0))]
    saved = json.loads((tmp_path / ".auto_tp.json").read_text(encoding="utf-8"))
    assert saved["strategies"]["ICT_Killzones"]["tp_r"] == 3.0 and saved["since"] == "2026-10-22"
    again = []
    tuner(tmp_path, [], data, again).apply_saved()  # рестарт: последний подобранный тейк сразу
    assert again == [("ICT_Killzones", a.scale_plan(3.0))]


def test_too_few_trades_keeps_the_last_valid_take(tmp_path):
    applied = []
    t = tuner(tmp_path, rows_for(3), bars((101.0, 99.5), (103.2, 100.5), (102.0, 98.5)), applied)
    rep = asyncio.run(t.tune())
    assert rep["ICT_Killzones"]["tp_r"] == 4.0 and not rep["ICT_Killzones"]["changed"] and applied == []


def test_network_failure_keeps_takes_and_retries(monkeypatch, tmp_path):
    from utils.errors import NetworkError

    class Broken(Api):
        async def get(self, path, params, weight=1):
            raise NetworkError("timeout")

    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)
        raise asyncio.CancelledError

    t = tuner(tmp_path, rows_for(6), [], [])
    t._api = Broken([])
    monkeypatch.setattr(a.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(t._loop())
    assert sleeps == [3600]
