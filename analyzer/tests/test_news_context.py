import json

import pandas as pd
import pytest

from analyzer.context import session_of, sessions_report, success_profile, vsa_bins
from analyzer.errors import CalendarError
from analyzer.frame import build_frame
from analyzer.news import classify_event, compare_news, currencies_for, flag_news, news_window_grid, parse_calendar

from .conftest import T0, make_trades

D = pd.Timestamp("2026-09-10 12:00", tz="UTC")  # время новости


def ev(*items):
    return pd.DataFrame([{"ts": t, "currency": c, "impact": i, "event": n} for t, c, i, n in items])


def trade(entry, exit_, coin="BTCUSDT", r=1.0, i=0):
    return {"id": f"t{i}", "strategy_id": "S", "coin": coin, "side": "long", "kind": "limit", "mode": "shadow", "signal_time": pd.Timestamp(entry), "entry_time": pd.Timestamp(entry),
            "exit_time": pd.Timestamp(exit_), "pnl": r * 100, "r_multiple": r, "gross_r": r, "mfe_r": 1.0, "mae_r": 0.5, "plan": {}, "meta": {}}


def test_parse_csv_with_synonyms_and_timezone_handling():
    csv = "Date,Time,Currency,Impact,Event\n2026-09-10,12:30,USD,High,CPI m/m\n2026-09-11,08:00,EUR,3,ECB Rate\n"
    e = parse_calendar(csv)
    assert e["impact"].tolist() == ["high", "high"] and e["currency"].tolist() == ["USD", "EUR"] and e["ts"].iloc[0] == pd.Timestamp("2026-09-10 12:30", tz="UTC")
    e2 = parse_calendar("timestamp,currency,impact,event\n1788000000,USD,red,NFP\n1788003600000,USD,low,x\n")
    assert e2["ts"].iloc[0].tzinfo is not None and e2["impact"].tolist() == ["high", "low"]


def test_parse_forexfactory_json_converts_offsets_to_utc():
    data = [{"title": "Core CPI m/m", "country": "USD", "date": "2026-09-10T08:30:00-04:00", "impact": "High"}]
    e = parse_calendar(json.dumps(data).encode())
    assert e["ts"].iloc[0] == pd.Timestamp("2026-09-10 12:30", tz="UTC") and e["event"].iloc[0] == "Core CPI m/m"


@pytest.mark.parametrize("text, msg", [("", "пуст"), ("timestamp,currency,impact\n2026-01-01 00:00,USD,Spicy\n", "важность"), ("currency,impact\nUSD,high\n", "нет времени"),
                                       ("timestamp,currency,impact\nnot-a-date,USD,high\n", "время"), ("{not json", "JSON")])
def test_parse_calendar_rejects_garbage_with_row_number(text, msg):
    with pytest.raises(CalendarError, match=msg):
        parse_calendar(text)


def test_currency_mapping():
    assert currencies_for("BTCUSDT") == {"USD"} and currencies_for("XAUUSD") == {"USD"} and currencies_for("EURUSD") == {"EUR", "USD"}
    assert classify_event("Core CPI m/m") == "CPI" and classify_event("Non-Farm Employment Change") == "NFP" and classify_event("FOMC Statement") == "FOMC" and classify_event("zzz") == "Other"


def test_news_window_touch_vs_span_and_edges():
    events = ev((D, "USD", "high", "CPI"))
    rows = [trade("2026-09-10 11:50", "2026-09-10 13:00", i=0),        # вход ровно на границе -10 мин: внутри
            trade("2026-09-10 11:49:59", "2026-09-10 11:55", i=1),     # выход в окне
            trade("2026-09-10 11:00", "2026-09-10 11:30", i=2),        # целиком до окна
            trade("2026-09-10 11:00", "2026-09-10 14:00", i=3),        # окно целиком внутри позиции: touch - нет, span - да
            trade("2026-09-10 12:10:01", "2026-09-10 13:00", i=4)]     # вход через 10 мин 1 с после
    df = build_frame(rows)
    by_id = lambda mode: dict(zip(df["id"], flag_news(df, events, before_min=10, after_min=10, mode=mode)["in_news"]))  # noqa: E731 (build_frame сортирует по выходу)
    assert by_id("touch") == {"t0": True, "t1": True, "t2": False, "t3": False, "t4": False}
    assert by_id("span") == {"t0": True, "t1": True, "t2": False, "t3": True, "t4": False}
    assert set(flag_news(df, events, before_min=10, after_min=10)["event"].dropna()) == {"CPI"}


def test_news_respects_asymmetric_windows_and_currency_scope():
    events = ev((D, "EUR", "high", "ECB"), (D, "ALL", "high", "Global"))
    df = build_frame([trade("2026-09-10 12:20", "2026-09-10 12:25", coin="EURUSD", i=0), trade("2026-09-10 12:20", "2026-09-10 12:25", coin="BTCUSDT", i=1)])
    def hits(e, after):
        return dict(zip(df["id"], flag_news(df, e, before_min=0, after_min=after)["in_news"]))
    assert hits(events.iloc[:1], 30) == {"t0": True, "t1": False}  # EUR-новость касается EURUSD, но не BTC
    assert hits(events.iloc[:1], 10) == {"t0": False, "t1": False}  # 12:20 вне окна +10
    assert hits(events.iloc[1:], 30) == {"t0": True, "t1": True}  # ALL - всем


def test_compare_news_only_judges_trades_inside_calendar_coverage(cfg):
    events = ev((D - pd.Timedelta(days=1), "USD", "high", "FOMC"), (D, "USD", "high", "CPI"), (D + pd.Timedelta(days=2), "USD", "high", "NFP"))
    rows = [trade(D + pd.Timedelta(minutes=m), D + pd.Timedelta(minutes=m + 5), r=-1.0, i=i) for i, m in enumerate([-5, 0, 3, 5])]            # убыточные у новости
    rows += [trade(D + pd.Timedelta(hours=h), D + pd.Timedelta(hours=h, minutes=30), r=1.0, i=10 + h) for h in range(2, 30)]                        # спокойные прибыльные
    rows += [trade(D - pd.Timedelta(days=5), D - pd.Timedelta(days=5) + pd.Timedelta(hours=1), r=5.0, i=99)]                                       # вне календаря
    out = compare_news(build_frame(rows), events, cfg, before_min=10, after_min=10)
    assert out["available"] and out["coverage"]["trades_outside"] == 1
    assert out["all"]["n"] == 32 and out["n_excluded"] == 4 and out["without_news"]["n"] == 28
    assert out["without_news"]["expectancy_r"] > out["all"]["expectancy_r"] and out["delta"]["expectancy_r"] > 0
    assert out["news_only"]["expectancy_r"] == pytest.approx(-1.0) and [k["event_kind"] for k in out["by_event_kind"]] == ["CPI"]
    grid = news_window_grid(build_frame(rows), events, cfg)
    assert [g["window_min"] for g in grid] == [5, 10, 15, 30, 60] and grid[1]["news_only_n"] == 4


def test_compare_news_unavailable_without_overlap_or_events(cfg):
    df = build_frame([trade("2026-01-01 00:00", "2026-01-01 01:00")])
    assert compare_news(df, ev((D, "USD", "high", "x")), cfg)["available"] is False
    assert compare_news(df, ev((D, "USD", "low", "x")), cfg)["available"] is False  # нет событий выбранной важности


def test_sessions_by_entry_time_utc_and_best_session(cfg):
    ts = pd.Series(pd.to_datetime(["2026-09-01 03:00", "2026-09-01 08:00", "2026-09-01 13:00", "2026-09-01 22:00", "2026-09-01 07:00", "2026-09-01 21:00"], utc=True))
    assert session_of(ts).tolist() == ["Asia", "London", "New York", "Off-hours", "London", "Off-hours"]
    rows = [trade(f"2026-09-{1 + i // 24:02d} {7 + i % 5:02d}:00", f"2026-09-{1 + i // 24:02d} {7 + i % 5:02d}:30", r=1.0 if i % 3 else -1.0, i=i) for i in range(60)]  # London
    rows += [trade(f"2026-10-{1 + i // 24:02d} {2 + i % 4:02d}:00", f"2026-10-{1 + i // 24:02d} {2 + i % 4:02d}:30", r=-1.0, i=100 + i) for i in range(60)]  # Asia: все убыточные
    rep = sessions_report(build_frame(rows), cfg)
    names = {r["session"]: r for r in rep["rows"]}
    assert rep["best"] == "London" and names["Asia"]["expectancy_r"] == pytest.approx(-1.0) and names["Asia"]["enough"] is True


def test_vsa_bins_and_success_profile_find_volume_effect(cfg):
    rows = make_trades(200, seed=2)
    for r in rows:  # победы приходят на аномальном объёме
        r["vol_ratio"] = 2.5 if r["r_multiple"] > 0 else 0.7
    f = build_frame(rows)
    prof = success_profile(f, cfg)
    top = prof["features"][0]
    assert top["feature"] == "vol_ratio" and top["winners_median"] > top["others_median"] and top["p_value"] < 0.01 and top["effect_d"] > 1
    assert prof["winners"] + prof["others"] == 200
    bins = {b["label"]: b for b in vsa_bins(f, cfg)}
    assert bins["2..3"]["winrate"] == pytest.approx(1.0) and bins["0.8..1.2"] if "0.8..1.2" in bins else True
    assert success_profile(f.iloc[0:0], cfg)["features"] == []
