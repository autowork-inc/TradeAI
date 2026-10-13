"""Тесты пессимистичного симулятора (strategies/simulate.py): каждое правило из докстринга на ручных барах."""

import polars as pl
import pytest

from strategies.simulate import SimParams, apply_ownership_lock, simulate
from utils.errors import DataError

STEP = 900_000


def bars(rows: list[tuple[float, float, float, float]]) -> pl.DataFrame:
    """rows: (open, high, low, close); бар 0 - бар сигнала."""
    return pl.DataFrame({
        "open_time": [i * STEP for i in range(len(rows))], "open": [r[0] for r in rows], "high": [r[1] for r in rows],
        "low": [r[2] for r in rows], "close": [r[3] for r in rows], "volume": [1.0] * len(rows),
    })


FLAT = (100.0, 100.0, 100.0, 100.0)
SCHEMA = {"sig_id": pl.Int64, "idx": pl.Int64, "sgn": pl.Float64, "kind": pl.String, "entry": pl.Float64,
          "sl": pl.Float64, "tp": pl.Float64, "inval": pl.Float64}


def sig(**kw) -> pl.DataFrame:
    base = {"sig_id": 0, "idx": 0, "sgn": 1.0, "kind": "limit", "entry": 100.0, "sl": 105.0, "tp": 85.0, "inval": 106.0}
    base.update(kw)
    return pl.DataFrame([base], schema=SCHEMA)


NOFEE = SimParams(maker_fee=0.0, taker_fee=0.0)


def one(b, s, p: SimParams | None = None) -> dict:
    return simulate(bars(b), s, p or NOFEE).row(0, named=True)


# Шорт: вход 100, SL 105 (R = 5), +1R = 95, TP 85 (3R).


def test_short_limit_tp_gross_3r():
    r = one([FLAT, (99, 101, 99, 100), (99.5, 99.5, 90, 92), (92, 93, 84, 85)], sig())
    assert r["outcome"] == "tp" and r["fill_off"] == 1 and r["exit_off"] == 3 and r["exit_px"] == 85.0
    assert r["gross_r"] == pytest.approx(3.0)


def test_short_limit_sl_minus_1r():
    r = one([FLAT, (99, 101, 99, 100), (100, 105, 99, 104)], sig())
    assert r["outcome"] == "sl" and r["gross_r"] == pytest.approx(-1.0)


def test_touch_without_crossing_is_not_a_fill():
    r = one([FLAT, (99, 100.0, 99, 99.5)] + [(99, 100.0, 98, 99)] * 12, sig())  # high == entry: на 1 тик не прошло
    assert r["outcome"] == "unfilled"


def test_limit_expires_after_ttl_even_if_price_returns_later():
    rows = [FLAT] + [(99, 99.5, 98, 99)] * 10 + [(99, 101, 98, 100)]  # проход на 11-м баре, ttl = 10
    assert one(rows, sig())["outcome"] == "unfilled"
    rows = [FLAT] + [(99, 99.5, 98, 99)] * 9 + [(99, 101, 98, 100), (99.5, 99.5, 84, 85)]  # проход на 10-м баре - успел
    assert one(rows, sig())["outcome"] == "tp"


def test_fill_bar_ignores_favorable_move_pessimistic():
    r = one([FLAT, (99, 101, 80, 90), (90, 91, 89, 90)], sig())  # на баре входа low 80 (мог быть TP) - не засчитывается
    assert r["outcome"] != "tp" and r["fill_off"] == 1


def test_fill_bar_adverse_move_is_counted():
    r = one([FLAT, (99, 106, 98, 100)], sig())  # прошло вход и сразу выше SL в том же баре
    assert r["outcome"] == "sl" and r["exit_off"] == 1


def test_sl_beats_tp_in_the_same_bar():
    r = one([FLAT, (99, 101, 99, 100), (100, 105.5, 84, 90)], sig())
    assert r["outcome"] == "sl"


def test_breakeven_after_plus_1r_then_return_to_entry():
    r = one([FLAT, (99, 101, 99, 100), (99.5, 99.5, 94, 95), (95, 100.5, 94, 99)], sig())  # +1R на баре 2, бар 3 - к входу
    assert r["outcome"] == "be" and r["gross_r"] == pytest.approx(0.0) and r["exit_off"] == 3


def test_breakeven_same_bar_as_arm_is_worst_case():
    r = one([FLAT, (99, 101, 99, 100), (100, 100.5, 94, 99)], sig())  # в баре и +1R, и возврат к входу -> BE
    assert r["outcome"] == "be"


def test_after_be_armed_tp_still_possible():
    r = one([FLAT, (99, 101, 99, 100), (100, 99.9, 94, 94), (94, 94, 84, 85)], sig())
    assert r["outcome"] == "tp"


def test_cancel_when_close_above_sweep_high_before_fill():
    r = one([FLAT, (99, 99.5, 98, 106.5), (100, 101, 99, 100)], sig(inval=106.0))
    assert r["outcome"] == "unfilled"


def test_cancel_when_tp_reached_before_fill():
    r = one([FLAT, (99, 99.5, 84, 90), (100, 101, 99, 100)], sig())
    assert r["outcome"] == "unfilled"


def test_timeout_closes_at_close_after_max_hold():
    p = SimParams(maker_fee=0.0, taker_fee=0.0, max_hold=5)
    rows = [FLAT, (99, 101, 99, 100)] + [(100, 101, 99, 100)] * 5 + [(100, 101, 99, 98)]
    r = one(rows, sig(), p)
    assert r["outcome"] == "timeout" and r["exit_off"] == 6 and r["exit_px"] == 100.0  # бар fill + 5 = 6, его close 100
    assert one([FLAT, (99, 101, 99, 100), (100, 101, 99, 100)], sig(), p)["outcome"] == "unresolved"


def test_fees_are_charged_maker_in_taker_out():
    p = SimParams(maker_fee=0.0002, taker_fee=0.0005)
    r = one([FLAT, (99, 101, 99, 100), (99.5, 99.5, 84, 85)], sig(), p)
    assert r["net_r"] == pytest.approx(3.0 - (0.0002 * 100 + 0.0005 * 85) / 5.0)


def test_long_limit_is_mirror_of_short():
    b = [FLAT, (101, 101, 99, 100), (100.5, 110, 100.5, 108), (108, 116, 108, 115)]  # лонг: вход 100, SL 95, TP 115
    r = one(b, sig(sgn=-1.0, entry=100.0, sl=95.0, tp=115.0, inval=94.0))
    assert r["outcome"] == "tp" and r["gross_r"] == pytest.approx(3.0) and r["exit_px"] == 115.0 and r["entry_px"] == 100.0


# MARKET: вход по open следующего бара, R и TP считаются от него.


def market_long(**kw) -> pl.DataFrame:
    return sig(sgn=-1.0, kind="market", entry=100.0, sl=96.0, tp=None, inval=None, **kw)


def test_market_long_uses_next_open_and_computes_tp_from_it():
    p = SimParams(maker_fee=0, taker_fee=0, max_hold=2)
    r = one([FLAT, (101, 102, 100, 101), (101.5, 110, 101.5, 108), (108, 114, 108, 113)], market_long(), p)
    assert r["entry_px"] == 101.0 and r["risk"] == 5.0 and r["outcome"] == "timeout"  # TP 116 не достигнут за 2 бара
    r2 = one([FLAT, (101, 102, 100, 101), (101.5, 110, 101.5, 108), (108, 117, 108, 116)], market_long())
    assert r2["outcome"] == "tp" and r2["exit_px"] == 116.0 and r2["gross_r"] == pytest.approx(3.0)


def test_market_rejected_when_leverage_exceeds_cap_o8():
    b = [FLAT, (100, 101, 99, 100), (100, 101, 99, 100)]
    s = sig(sgn=-1.0, kind="market", sl=99.5, tp=None, inval=None)  # R = 0.5 -> плечо 0.01 * 100 / 0.5 = 2
    assert one(b, s, SimParams(maker_fee=0, taker_fee=0, max_exposure=1.0))["outcome"] == "rejected_exposure"
    assert one(b, s, SimParams(max_exposure=3.0))["outcome"] != "rejected_exposure"
    assert SimParams().max_exposure == 5.0  # дефолт после О12
    tight = sig(sgn=-1.0, kind="market", sl=99.9, tp=None, inval=None)  # R = 0.1 -> плечо 10 > 5
    assert one(b, tight)["outcome"] == "rejected_exposure"
    assert one(b, s)["outcome"] != "rejected_exposure"  # плечо 2 проходит при дефолтном потолке 5


def test_market_without_next_bar_or_nonpositive_risk():
    s = sig(sgn=-1.0, kind="market", sl=99.0, tp=None, inval=None)
    assert one([FLAT], s)["outcome"] == "no_data"
    assert one([FLAT, (98, 99, 97, 98)], s)["outcome"] == "bad_risk"  # открылись ниже SL лонга


def test_simulate_validates_input_and_empty():
    with pytest.raises(DataError, match="уникальным"):
        simulate(bars([FLAT, FLAT]), pl.concat([sig(), sig()]))
    with pytest.raises(DataError, match="нет колонок"):
        simulate(bars([FLAT, FLAT]), sig().drop("tp"))
    assert simulate(bars([FLAT, FLAT]), sig().head(0)).height == 0


def test_ownership_lock_skips_signals_while_busy_d4():
    t = pl.DataFrame({"idx": [10, 12, 20, 21, 40], "end_idx": [18, 15, 25, 30, 41]})
    assert apply_ownership_lock(t)["skipped_due_to_lock"].to_list() == [False, True, False, True, False]
    assert apply_ownership_lock(t.head(0)).height == 0


def test_touching_entry_after_arm_counts_as_breakeven_stop_hit():
    # бар открылся на уровне входа (high == entry) и дошёл до +1R: стоп на входе мог сработать - худший случай, BE
    r = one([FLAT, (99, 101, 99, 100), (100, 100, 94, 95)], sig())
    assert r["outcome"] == "be" and r["exit_off"] == 2
