"""Лаборатория сделок (scripts/trade_lab, scripts/analyze_tp.py): симуляция выходов по MFE, сверка, геометрия стопа, корреляции, путь по 1m."""

from datetime import datetime, timezone

import pytest

from scripts.trade_lab import correlate, exits, klines, periods, stops
from scripts.trade_lab.data import LabError, build, classify, to_trade

T0 = int(datetime(2026, 10, 20, 10, tzinfo=timezone.utc).timestamp() * 1000)


def row(**kw):
    """Строка `trades` как её отдаёт запрос data.QUERY. По умолчанию: лонг MARKET, вход 100, SL 99 (R = 1), план 1:4, выбит стопом."""
    entry = kw.pop("entry", 100.0)
    sl = kw.pop("sl", 99.0)
    exit_price = kw.pop("exit_price", sl)
    reason = kw.pop("reason", "sl")
    base = {
        "id": "s1", "strategy": "VWAP_Reversion", "coin": "BTCUSDT", "side": "long", "kind": "market", "mode": "shadow", "status": "closed",
        "signal_time": T0, "entry_time": T0 + 1000, "exit_time": T0 + 3_600_000, "entry_price": entry, "exit_price": exit_price, "sl": sl,
        "risk": abs(entry - sl), "risk_amount": 5.0, "r_multiple": -1.0, "gross_r": -1.0, "mfe_r": 0.3, "mae_r": 1.0, "exit_reason": reason,
        "be_armed": False, "gap_affected": False, "plan": {"levels": [[4.0, 1.0]], "tp_anchor": "fill"}, "candles": [], "meta": {},
        "closes": [{"seq": 1, "ts": T0 + 3_600_000, "reason": reason, "frac": 1.0, "price": exit_price, "gross_r": -1.0, "net_r": -1.0}],
        "restored": False, "tick_gap": False,
    }
    base.update(kw)
    return base


def trade(**kw):
    return to_trade(row(**kw))


FEE = (0.0005 * 100) / 1.0  # комиссия входа MARKET в R при входе 100 и R = 1


# ------------------------------------------------------------------------------------------- выходы

def test_tp_below_mfe_is_taken_before_the_stop():
    t = trade(mfe_r=1.6)
    r = exits.simulate(t, exits.tp(1.5)).net_r
    assert r == pytest.approx(1.5 - FEE - 0.0005 * 101.5)


def test_tp_above_mfe_keeps_the_actual_stop():
    t = trade(mfe_r=1.6)
    r = exits.simulate(t, exits.tp(2.0)).net_r
    assert r == pytest.approx(-1.0 - FEE - 0.0005 * 99.0)  # фактический стоп по цене 99


def test_simulating_the_actual_plan_reproduces_the_actual_result():
    """Сделка 1:4, взят тейк по 104: симуляция 1:4 = факт (с теми же комиссиями, что у трекера)."""
    fact = 4.0 - FEE - 0.0005 * 104.0
    t = trade(mfe_r=4.2, exit_price=104.0, reason="tp", r_multiple=fact)
    assert exits.simulate(t, exits.CURRENT).net_r == pytest.approx(t.actual_r)


def test_old_plan_on_a_trade_that_went_1_2r_then_stopped_gives_half_r():
    """MFE 1.2 → стоп: старый план берёт 50% на +1R, остаток уходит в безубыток (цена прошла через вход к стопу)."""
    t = trade(mfe_r=1.2)
    r = exits.simulate(t, exits.OLD_PLAN).net_r
    assert r == pytest.approx(0.5 - FEE - 0.5 * 0.0005 * 101 - 0.5 * 0.0005 * 100)


def test_timeout_after_break_even_with_price_above_entry_is_pessimistic_zero():
    t = trade(mfe_r=1.5, exit_price=100.7, reason="timeout", r_multiple=0.6)
    res = exits.simulate(t, exits.Plan("BE1 TP3", ((3.0, 1.0),), be=1.0))
    assert res.approx and res.net_r == pytest.approx(-FEE - 0.0005 * 100)
    unarmed = exits.simulate(t, exits.tp(3.0))
    assert not unarmed.approx and unarmed.net_r == pytest.approx(0.7 - FEE - 0.0005 * 100.7)


def test_old_trade_closed_at_break_even_is_unknown_for_a_plan_without_break_even():
    t = trade(mfe_r=1.4, exit_price=100.0, reason="be", be_armed=True, plan={"levels": [[1.0, 0.5], [3.0, 0.4], [3.0, 0.1]]})
    assert t.old_plan
    assert exits.simulate(t, exits.tp(2.0)).net_r is None  # что было бы после безубытка - не наблюдалось
    assert exits.simulate(t, exits.tp(1.0)).net_r is not None  # уровень ниже MFE - известно
    assert exits.simulate(t, exits.OLD_PLAN).net_r is not None  # безубыток плана тоже сработал


def test_level_above_the_actual_take_profit_is_unknown():
    t = trade(mfe_r=4.0, exit_price=104.0, reason="tp", r_multiple=3.9)
    assert exits.simulate(t, exits.tp(5.0)).net_r is None


def test_short_side_prices_mirror():
    t = trade(side="short", entry=100.0, sl=101.0, mfe_r=2.1, exit_price=101.0)
    r = exits.simulate(t, exits.tp(2.0)).net_r
    assert r == pytest.approx(2.0 - FEE - 0.0005 * 98.0)


def test_stats_and_walk_forward():
    ts = [trade(id=f"s{i}", signal_time=T0 + i * 60_000, mfe_r=1.2 if i % 2 else 0.2) for i in range(20)]
    st = exits.stats_of(exits.run(ts, exits.tp(1.0)))
    assert st.known == 20 and st.win_rate == pytest.approx(0.5) and st.mdd_r > 0
    best = exits.best_tp(ts)
    assert best is not None and best[0] <= 1.25
    wf = exits.walk_forward(ts)
    assert wf is not None and wf["n_train"] == 10 and wf["n_test"] == 10
    boot = exits.bootstrap_diff(ts, exits.tp(1.0), exits.CURRENT)
    assert boot is not None and boot[1] > 0  # тейк 1R строго лучше 1:4 на этой выборке


# --------------------------------------------------------------------------------------------- сверка

def test_ledger_puts_every_row_into_exactly_one_category():
    rows = [row(id="a"), row(id="b", gap_affected=True), row(id="c", status="cancelled", entry_price=None),
            row(id="d", status="open", r_multiple=None), row(id="e", status="aborted"), row(id="f", status="aborted", entry_price=None),
            row(id="g", mfe_r=None)]
    trades, ledger = build(rows)
    assert ledger.total == 7 and sum(ledger.counts.values()) == 7
    assert [t.category for t in trades] == ["closed_clean", "closed_gap", "cancelled", "active", "aborted_filled", "aborted_unfilled", "closed_no_data"]
    assert ledger.excluded_ids["closed_no_data"] == ["g"]


def test_ledger_fails_on_unknown_status():
    with pytest.raises(LabError):
        classify(row(status="weird"))


def test_ledger_detects_a_lost_row():
    from scripts.trade_lab.data import ledger_of

    with pytest.raises(LabError):
        ledger_of([trade()], total=2)


# ---------------------------------------------------------------------------------------------- стоп

def candles(lows, highs=None, closes=None):
    highs = highs or [lo + 2.0 for lo in lows]
    closes = closes or [lo + 1.0 for lo in lows]
    return [{"open_time": i, "open": c, "high": h, "low": lo, "close": c} for i, (lo, h, c) in enumerate(zip(lows, highs, closes))]


def flat_with_swing(swing_low):
    lows = [100.0] * 40
    lows[20] = swing_low  # вне окна ATR (последние 15 свечей), чтобы ATR = 2; пивот k=2: строго ниже двух слева, не выше двух справа
    return candles(lows)


def test_stop_just_behind_the_swing_is_ok():
    t = trade(entry=102.0, sl=97.6, candles=flat_with_swing(98.0))  # ATR = 2, якорь 98, SL на 0.2 ATR дальше
    g = stops.geometry(t)
    assert g["anchor"] == pytest.approx(98.0) and g["beyond_anchor_atr"] == pytest.approx(0.2)
    assert g["stop_class"] == "ok"


def test_stop_far_behind_the_swing_sags():
    g = stops.geometry(trade(entry=102.0, sl=96.0, candles=flat_with_swing(98.0)))
    assert g["beyond_anchor_atr"] == pytest.approx(1.0) and g["stop_class"] == "sag"


def test_stop_inside_recent_lows_is_noise():
    g = stops.geometry(trade(entry=102.0, sl=98.5, candles=flat_with_swing(98.0)))
    assert g["pierce_20"] == 1 and g["stop_class"] == "noise"


def test_stop_with_no_structure_in_front_is_in_the_air():
    g = stops.geometry(trade(entry=110.0, sl=103.0, candles=candles([104.0] * 40)))  # минимум 3 свечей 104 - в 0.5 ATR; поднимем вход выше
    assert g["stop_class"] in ("ok", "wide")
    g2 = stops.geometry(trade(entry=103.5, sl=90.0, candles=candles([104.0] * 40)))  # между SL и входом нет ни одной структуры
    assert g2["anchor"] is None and g2["stop_class"] == "air"


def test_no_candles_is_no_data():
    assert stops.geometry(trade())["stop_class"] == "no_data"


# ------------------------------------------------------------------------------------- корреляции/версии

def test_spearman_known_values():
    assert correlate.spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert correlate.spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)
    assert correlate.spearman([1, 1, 1], [1, 2, 3]) is None


def test_correlation_finds_the_feature_that_separates_winners():
    ts = []
    for i in range(40):
        good = i % 2 == 0
        ts.append(trade(id=f"t{i}", mfe_r=1.5 if good else 0.1, meta={"vol_ratio": 3.0 + i * 0.01 if good else 1.0 + i * 0.01, "noise": float(i % 7)}))
    found = correlate.analyse(ts, exits.tp(1.0))
    assert found[0].feature == "meta.vol_ratio" and found[0].rho_hit1 > 0.8


def test_version_labels_follow_the_change_dates():
    assert periods.version_of(trade(signal_time=int(datetime(2026, 10, 19, 12, tzinfo=timezone.utc).timestamp() * 1000))) == "с 19.10"
    assert periods.version_of(trade(signal_time=int(datetime(2026, 10, 14, tzinfo=timezone.utc).timestamp() * 1000))) == "до 16.10"


# ------------------------------------------------------------------------------------------- путь 1m

def bar(i, hi, lo, close=None):
    return (T0 + 1000 - 1000 + (i + 1) * klines.MIN_MS, hi, lo, close if close is not None else (hi + lo) / 2)


def test_path_sim_is_pessimistic_inside_one_minute():
    t = trade(entry_time=T0)
    both = [bar(0, 102.5, 98.5)]  # одна свеча задела и тейк 2R, и стоп
    assert klines.path_sim(t, both, 99.0, 2.0, now_ms=T0 + 10 * klines.HORIZON_MS) == pytest.approx(-1.0 - (0.0005 * 100 + 0.0005 * 99))


def test_path_sim_target_first_and_tighter_stop_rescales_r():
    t = trade(entry_time=T0)
    path = [bar(0, 100.8, 99.8), bar(1, 101.2, 100.2)]
    r = klines.path_sim(t, path, 99.5, 2.0, now_ms=T0 + 10 * klines.HORIZON_MS)  # стоп вдвое ближе: R' = 0.5, тейк 2R' = 101
    assert r == pytest.approx(2.0 - (0.0005 * 100 + 0.0005 * 101) / 0.5)


def test_path_sim_unfinished_path_is_unknown():
    t = trade(entry_time=T0)
    assert klines.path_sim(t, [bar(0, 100.5, 99.5)], 99.0, 4.0, now_ms=T0 + klines.MIN_MS * 5) is None


def test_stop_hunt_sees_price_coming_back():
    t = trade(entry_time=T0, exit_time=T0 + 10 * klines.MIN_MS)
    after = [(T0 + 10 * klines.MIN_MS, 99.2, 98.7, 99.0), (T0 + 40 * klines.MIN_MS, 101.5, 99.5, 101.0)]
    h = klines.stop_hunt(t, after, atr=1.0)
    assert h["back_to_entry"] and h["reach1"] and not h["reach2"] and h["overshoot_atr"] == pytest.approx(0.3)


def test_kline_store_rejects_non_crypto(tmp_path):
    with pytest.raises(klines.Unavailable):
        klines.KlineStore(tmp_path, now_ms=T0).range("EURUSD", T0, T0 + 60_000)


# ------------------------------------------------------------------------------------------------ R1

def test_no_blind_excepts_in_trade_lab():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "scripts"
    for p in list((root / "trade_lab").glob("*.py")) + [root / "analyze_tp.py"]:
        text = p.read_text(encoding="utf-8")
        assert "except Exception" not in text and "except:" not in text, p


# ---------------------------------------------------------------------------------------- сквозной прогон

def test_full_report_without_database(tmp_path, monkeypatch):
    """CLI целиком на подставных строках БД: отчёт и CSV собираются, сверка в отчёте, все блоки на месте."""
    import random

    import scripts.analyze_tp as cli

    rng = random.Random(3)
    rows = []
    for i in range(120):
        strat = ("VWAP_Reversion", "ICT_Killzones", "SMC_Pro_Trend")[i % 3]
        day = T0 - 6 * 86_400_000 + i * 3_600_000 * 2
        mfe = rng.choice((0.1, 0.4, 0.8, 1.3, 2.2, 4.1))
        old = day < int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
        won = mfe >= 4.0
        lows = [100.0 - rng.random() for _ in range(40)]
        side = rng.choice(("long", "short")) if i % 5 else "long"
        sl, tp_price = (99.0, 109.0) if side == "long" else (103.0, 93.0)
        rows.append(row(id=f"{strat}:{i}", strategy=strat, signal_time=day, entry_time=day + 1000, exit_time=day + 7_200_000, mfe_r=mfe,
                        side=side, entry=101.0, sl=sl, exit_price=tp_price if won else sl,
                        reason="tp" if won else "sl", r_multiple=(4.0 - 0.0005 * 101 / 2 - 0.0005 * tp_price / 2) if won else (-1.0 - 0.0005 * 101 / 2 - 0.0005 * sl / 2), restored=i % 17 == 0, gap_affected=i % 17 == 0,
                        plan={"levels": [[1.0, 0.5], [3.0, 0.4], [3.0, 0.1]]} if old else {"levels": [[4.0, 1.0]]},
                        candles=candles(lows), meta={"vol_ratio": rng.random() * 3, "htf_label": rng.choice(("Bullish", "Bearish"))}))
    rows.append(row(id="cancel", status="cancelled", entry_price=None))

    async def fake_fetch(url, since, mode, exclude):
        assert mode == "shadow" and "Horizontal_Breakout_CHR" in exclude
        return rows

    monkeypatch.setattr(cli, "fetch_rows", fake_fetch)
    monkeypatch.setattr(cli, "read_database_url", lambda: "postgresql://x")
    assert cli.main(["--out", str(tmp_path), "--history-from", "2026-10-13"]) == 0
    text = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "РАСХОЖДЕНИЕ" not in text  # симуляция плана 1:4 воспроизводит факт
    for needle in ("сверка сошлась", "Блок 1", "Блок 2", "Блок 3", "Блок 4", "Блок 5", "Старый: 50% 1R", "Лучший фиксированный тейк"):
        assert needle in text, needle
    lines = (tmp_path / "trades.csv").read_text(encoding="utf-8-sig").splitlines()
    assert len(lines) == 1 + len(rows)
