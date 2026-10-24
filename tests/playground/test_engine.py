"""Playground: прогон по истории тем же кодом, что Shadow, без заглядывания в будущее."""

import json

import math
import random
import polars as pl
import pytest

from orchestrator.multi_strategies import SMC_COUNTER_TREND, VWAP_REVERSION, ICT_KILLZONES, _market_signal
from orchestrator.types import Direction
from playground import engine as E
from playground.engine import WARMUP_MS, RunSpec, simulate, summarize
from playground.history import MINUTE_MS, resample
from playground.store import Store

DAY = 86_400_000
T0 = 1_735_689_600_000  # 2025-01-01 00:00 UTC
START = T0 + WARMUP_MS
END = START + 3 * DAY


def minutes(days_after_start: int = 3, seed: int = 7, start_price: float = 100.0) -> pl.DataFrame:
    n = (WARMUP_MS + days_after_start * DAY) // MINUTE_MS
    rng = random.Random(seed)
    price, rows = start_price, {"open_time": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
    for i in range(n):
        o = price
        price = price * math.exp(rng.gauss(0, 0.0012))
        w = abs(rng.gauss(0, 0.0008)) * price
        for k, v in (("open_time", T0 + i * MINUTE_MS), ("open", o), ("high", max(o, price) + w), ("low", min(o, price) - w), ("close", price),
                     ("volume", rng.uniform(10, 100))):
            rows[k].append(v)
    return pl.DataFrame(rows)


class Toy:
    """Решение только по последней закрытой свече окна: если бы движок подсунул будущее, решения зависели бы от него."""

    name, interval = SMC_COUNTER_TREND, "15m"

    def __init__(self):
        self.seen_last: list[int] = []

    def detect(self, ctx):
        f = ctx.frame
        self.seen_last.append(ctx.bar_open_time)
        close, prev = float(f["close"][-1]), float(f["close"][-5])
        if (ctx.bar_open_time // 900_000) % 8 or close == prev:
            return ()
        side = Direction.LONG if close > prev else Direction.SHORT
        sl = close * (0.996 if side is Direction.LONG else 1.004)
        return (_market_signal(self.name, ctx, side, sl, (), {"why": "toy"}),)


@pytest.fixture
def toy(monkeypatch):
    holder = {}

    def build(names, *, mtf=False, interval=None):
        holder["s"] = Toy()
        return [holder["s"]]

    monkeypatch.setattr(E, "build_strategies", build)
    return holder


def test_bars_are_closed_candles_and_market_fills_at_next_minute_open(toy):
    m = minutes()
    res = simulate(RunSpec("TESTUSDT", (SMC_COUNTER_TREND,), START, END), m)
    s = toy["s"]
    assert s.seen_last[0] == START  # первая рабочая свеча - закрывшаяся на START + 15m (без бара, начатого до начала периода... он открылся в START)
    assert all(b + 900_000 <= START + (i + 1) * 900_000 for i, b in enumerate(s.seen_last))  # стратегия видит только закрытые свечи
    assert res.trades, "игрушечная стратегия должна торговать"
    by_open = dict(zip(m["open_time"].to_list(), m["open"].to_list()))
    for t in res.trades:
        if t["entry_time"] is not None:
            assert t["entry_time"] == t["signal_time"] and t["entry_price"] == pytest.approx(by_open[t["signal_time"]])  # первый тик после сигнала
    closed = [t for t in res.trades if t["status"] == "closed"]
    for t in closed:  # цена внутри минуты непрерывна: стоп исполняется на уровне стопа (гэп - только на open минуты)
        stops = [c for c in res.closes[t["id"]] if c["reason"] == "sl"]
        for c in stops:
            assert c["price"] == pytest.approx(t["sl"]) or c["price"] == pytest.approx(by_open[c["ts"]])
    assert closed and all(t["exit_reason"] in ("sl", "tp", "scale_1r", "timeout") for t in closed)
    assert all(res.snapshots[t["id"]]["candles"][-1]["open_time"] + 900_000 == t["signal_time"] for t in res.trades)  # снапшот кончается сигнальной свечой
    assert all(any(e["kind"] == "trade_registered" for e in res.events[t["id"]]) for t in res.trades)


def test_future_data_does_not_change_past_decisions(toy):
    """Без заглядывания в будущее: другие минутки ПОСЛЕ отсечки не меняют ни одного решения и исхода, завершённого до неё."""
    base = minutes()
    cut = START + 2 * DAY
    mutated = base.with_columns([pl.when(pl.col("open_time") >= cut).then(pl.col(c) * 1.3).otherwise(pl.col(c)).alias(c) for c in ("open", "high", "low", "close")])
    a = simulate(RunSpec("TESTUSDT", (SMC_COUNTER_TREND,), START, END), base)
    b = simulate(RunSpec("TESTUSDT", (SMC_COUNTER_TREND,), START, END), mutated)

    def before(res):
        return sorted((t["id"], t["status"], t["entry_price"], t["exit_price"], t["exit_reason"]) for t in res.trades
                      if t["signal_time"] < cut and t["status"] in ("closed", "cancelled") and (t["exit_time"] or 0) < cut)

    assert before(a) and before(a) == before(b)


def test_real_strategies_run_on_history_without_lookahead():
    """Настоящие стратегии (быстрые) на случайном блуждании: прогон проходит, решения до отсечки не зависят от данных после неё."""
    base = minutes(days_after_start=2)
    cut = START + DAY
    mutated = base.with_columns([pl.when(pl.col("open_time") >= cut).then(pl.col(c) * 0.7).otherwise(pl.col(c)).alias(c) for c in ("open", "high", "low", "close")])
    names = (SMC_COUNTER_TREND, ICT_KILLZONES, VWAP_REVERSION)
    a = simulate(RunSpec("TESTUSDT", names, START, START + 2 * DAY), base)
    b = simulate(RunSpec("TESTUSDT", names, START, START + 2 * DAY), mutated)
    assert a.bars == b.bars > 0
    pick = lambda r: sorted((t["id"], t["status"]) for t in r.trades if t["signal_time"] < cut)  # noqa: E731
    assert pick(a) == pick(b)
    assert [s for s in a.skipped if s["ts"] < cut] == [s for s in b.skipped if s["ts"] < cut]


def test_spec_rejects_obi_and_non_crypto():
    with pytest.raises(Exception):
        RunSpec("BTCUSDT", ("OBI_Breakout",), START, END).validate()
    with pytest.raises(Exception):
        RunSpec("EURUSD", (SMC_COUNTER_TREND,), START, END).validate()


def test_resample_builds_utc_aligned_candles():
    m = minutes(days_after_start=1)
    d1 = resample(m, DAY)
    assert (d1["open_time"] % DAY == 0).all()
    first = m.filter(pl.col("open_time") < T0 + DAY)
    assert d1["high"][0] == first["high"].max() and d1["close"][0] == first["close"][-1] and d1["volume"][0] == pytest.approx(first["volume"].sum())


def test_store_roundtrip_summary_and_export(toy, tmp_path):
    res = simulate(RunSpec("TESTUSDT", (SMC_COUNTER_TREND,), START, END), minutes())
    store = Store(tmp_path / "a.sqlite")
    rid = store.create("TESTUSDT", (SMC_COUNTER_TREND,), START, END, profile="SANTA", batch="b1")
    summary = store.save_result(rid, res, summarize(res.trades, res.skipped))
    o = summary["overall"]
    closed = [t for t in res.trades if t["status"] == "closed"]
    assert o["closed"] == len(closed) and o["sum_r"] == pytest.approx(sum(t["r_multiple"] for t in closed))
    assert summary == summarize(res.trades, res.skipped)
    run = store.run(rid)
    assert run["status"] == "done" and run["summary"]["overall"]["closed"] == o["closed"]
    tid = res.trades[0]["id"]
    one = store.trade(rid, tid)
    assert one["trade"]["id"] == tid and one["snapshot"]["meta"]["why"] == "toy" and one["events"]
    assert store.runs(strategies_subset=("Trendline_Breakout_CHR",)) == []  # профиль CHR не видит прогоны чужих стратегий
    other = Store(tmp_path / "b.sqlite")
    assert store.export_runs([rid], other) == 1 and other.trade(rid, tid)["trade"] == one["trade"]
    json.dumps(other.run(rid))


def test_replay_minute_fills_stop_and_take_at_their_levels():
    from orchestrator.journal import MemoryEventLog
    from orchestrator.tracker import ScaleOutPlan, VirtualTracker
    from orchestrator.types import Kind, Signal

    def sig(i, sl):
        return Signal(f"x{i}", SMC_COUNTER_TREND, "TESTUSDT", "15m", Direction.LONG, Kind.MARKET, 0, 900_000, 100.0, sl, None, None)

    tr = VirtualTracker(MemoryEventLog(), plans={SMC_COUNTER_TREND: ScaleOutPlan(((1.0, 0.5), (2.0, 0.5)))})
    tr.submit(sig(1, 99.0), 900_001)
    tr.replay_minute("TESTUSDT", 900_000, 100.0, 103.5, 99.9, 103.0)  # вход по open 100, путь вниз до 99.9 (стоп 99 не задет), вверх до 103.5
    t = tr.trades[0]
    assert t.status == "closed" and [round(g.price, 6) for g in t.legs] == [101.0, 102.0]  # +1R и +2R ровно по уровням, не по high минуты
    tr.submit(sig(2, 99.0), 960_001)
    tr.replay_minute("TESTUSDT", 960_000, 100.0, 100.4, 97.0, 98.0)
    assert tr.trades[1].legs[0].price == pytest.approx(99.0)  # стоп ровно по 99, а не по low 97
    tr.submit(sig(3, 99.0), 1_020_001)
    tr.replay_minute("TESTUSDT", 1_020_000, 100.0, 100.1, 99.5, 99.8)
    tr.replay_minute("TESTUSDT", 1_080_000, 98.0, 98.5, 97.5, 98.2)  # гэп ниже стопа на open: выход по open
    assert tr.trades[2].legs[0].price == pytest.approx(98.0)


def test_store_strategy_list_matches_engine():
    from playground.store import AVAILABLE_STRATEGIES, CHR_STRATEGIES
    from playground.cli import CHR_GROUP

    assert set(AVAILABLE_STRATEGIES) == set(E.PLAYGROUND_STRATEGIES) and set(CHR_STRATEGIES) == set(CHR_GROUP)
