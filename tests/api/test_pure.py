"""API мониторинга без БД: настройки, защита ключом, статистика, текст причины входа, разбор состояния автомата, сериализация."""

import math
from datetime import datetime, timedelta, timezone

import pytest

import config

from api.livestate import build_state, fingerprint, heartbeat_age_s, parse_setup_row
from api.reason import build_reason, fmt_price
from api.security import FailureThrottle, key_matches
from api.serialize import DataIntegrityError, iso, num
from api.settings import ApiConfigError, Settings, load_settings
from api.stats import MIN_TRADES_FOR_CONCLUSIONS, compute_stats, counts_from_rows

DSN = "postgresql://tradeai:pw@postgres:5432/tradeai"
KEY = "k" * 40
T0 = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


# ================================================================================== настройки


def test_settings_require_a_long_key_and_a_valid_dsn():
    Settings(DSN, KEY, shadow_data_dir=__import__("pathlib").Path("."))
    for key in ("", "short", "x" * 31, " " + "x" * 40, "x" * 20 + " " + "x" * 20):
        with pytest.raises(ApiConfigError, match="API_KEY"):
            Settings(DSN, key, shadow_data_dir=__import__("pathlib").Path("."))
    for dsn in ("", "mysql://u:p@h/x", "postgresql://u:p@h"):
        with pytest.raises(ApiConfigError, match="DATABASE_URL"):
            Settings(dsn, KEY, shadow_data_dir=__import__("pathlib").Path("."))


def test_load_settings_reads_environment_and_normalizes_coins():
    s = load_settings({"DATABASE_URL": DSN, "API_KEY": KEY, "API_COINS": "btcusdt, ethusdt"})
    assert s.coins == ("BTCUSDT", "ETHUSDT") and s.port == 8000
    assert load_settings({"DATABASE_URL": DSN, "API_KEY": KEY}).coins == config.SHADOW_SYMBOLS  # образ API без config.py: список продублирован в api/settings.py и сверяется здесь
    with pytest.raises(ApiConfigError):
        load_settings({"DATABASE_URL": DSN})  # ключа нет - сервер не стартует, «открытого» режима не существует
    with pytest.raises(ApiConfigError, match="Числовая"):
        load_settings({"DATABASE_URL": DSN, "API_KEY": KEY, "API_POLL_INTERVAL_S": "abc"})


# ================================================================================== безопасность


def test_key_comparison():
    assert key_matches(KEY, KEY) and not key_matches(KEY[:-1], KEY) and not key_matches(KEY + "x", KEY)
    assert not key_matches(None, KEY) and not key_matches("", KEY)
    assert key_matches("ключ-" + "я" * 30, "ключ-" + "я" * 30)  # не-ASCII не роняет сравнение


def test_failure_throttle_blocks_after_limit_and_recovers_after_window():
    now = [0.0]
    th = FailureThrottle(3, 60.0, clock=lambda: now[0])
    assert not th.blocked("a")
    for _ in range(3):
        th.record_failure("a")
        now[0] += 1
    assert th.blocked("a") and not th.blocked("b") and 50 <= th.retry_after_s("a") <= 60
    now[0] += 58  # первая неудача (t=0) вышла из окна, остались две
    assert not th.blocked("a")
    now[0] += 100
    assert not th.blocked("a") and th.retry_after_s("a") == 1


def test_throttle_memory_is_bounded_under_scan_from_many_addresses():
    now = [0.0]
    th = FailureThrottle(5, 1.0, clock=lambda: now[0])
    for i in range(5000):
        th.record_failure(f"ip{i}")
    now[0] += 2
    th.record_failure("new")  # устаревшее вычищается
    assert len(th._fails) < 10


# ================================================================================== статистика


def trade(coin, r, minutes, strategy="SMC_Pro_Trend", side="short", reason="tp"):
    return {"id": f"{coin}{minutes}", "coin": coin, "strategy_name": strategy, "strategy_id": strategy, "side": side, "exit_reason": reason,
            "r_multiple": r, "exit_time": T0 + timedelta(minutes=minutes)}


COINS = ("BTCUSDT", "ETHUSDT", "DOGEUSDT", "1000PEPEUSDT")


def sample():
    return [trade("BTCUSDT", 2.0, 1), trade("BTCUSDT", -1.0, 2, reason="sl"), trade("ETHUSDT", -1.0, 3, reason="sl"),
            trade("ETHUSDT", -1.0, 4, reason="sl"), trade("BTCUSDT", 0.5, 5, reason="trail"), trade("DOGEUSDT", 3.0, 6, strategy="breakout", side="long")]


def test_overall_stats_win_rate_profit_factor_and_drawdown():
    st = compute_stats(reversed(sample()), COINS)  # порядок на входе не важен: кривая строится по времени закрытия
    o = st["overall"]
    assert (o["trades"], o["wins"], o["losses"], o["flat"]) == (6, 3, 3, 0)
    assert o["win_rate"] == pytest.approx(0.5) and o["net_r"] == pytest.approx(2.5) and o["profit_factor"] == pytest.approx(5.5 / 3)
    assert o["max_drawdown_r"] == pytest.approx(3.0)  # накопленный R: 2, 1, 0, -1, -0.5, 2.5 -> от пика 2 до дна -1
    assert [p["cum_r"] for p in st["equity_curve"]] == pytest.approx([2, 1, 0, -1, -0.5, 2.5])
    assert o["avg_r_stderr"] > 0


def test_coins_are_ranked_by_net_r_and_coins_without_trades_come_last():
    st = compute_stats(sample(), COINS)
    assert [c["coin"] for c in st["by_coin"]] == ["DOGEUSDT", "BTCUSDT", "ETHUSDT", "1000PEPEUSDT"]
    assert [c["rank"] for c in st["by_coin"]] == [1, 2, 3, None]
    assert st["best_coin"] == "DOGEUSDT" and st["worst_coin"] == "ETHUSDT"
    pepe = st["by_coin"][-1]
    assert pepe["trades"] == 0 and pepe["win_rate"] is None and pepe["net_r"] == 0 and pepe["profit_factor"] is None
    assert [s["strategy"] for s in st["by_strategy"]] == ["breakout", "SMC_Pro_Trend"] and {s["side"] for s in st["by_side"]} == {"short", "long"}
    assert {(x["coin"], x["strategy"]) for x in st["by_coin_strategy"]} == {("BTCUSDT", "SMC_Pro_Trend"), ("ETHUSDT", "SMC_Pro_Trend"), ("DOGEUSDT", "breakout")}
    assert dict((e["reason"], e["trades"]) for e in st["by_exit_reason"]) == {"sl": 3, "tp": 2, "trail": 1}


def test_unknown_coin_in_the_data_is_still_reported():
    st = compute_stats([trade("SOLUSDT", 1.0, 1)], COINS)
    assert "SOLUSDT" in [c["coin"] for c in st["by_coin"]] and st["best_coin"] == "SOLUSDT"


def test_profit_factor_is_undefined_without_losses_and_json_safe():
    st = compute_stats([trade("BTCUSDT", 1.0, 1), trade("BTCUSDT", 2.0, 2)], COINS)
    o = st["overall"]
    assert o["profit_factor"] is None and o["profit_factor_infinite"] is True and o["max_drawdown_r"] == 0
    assert all(math.isfinite(v) for v in (o["net_r"], o["gross_profit_r"]))
    empty = compute_stats([], COINS)["overall"]
    assert empty["trades"] == 0 and empty["win_rate"] is None and empty["profit_factor"] is None and not empty["profit_factor_infinite"]


def test_small_sample_carries_an_explicit_warning_instead_of_a_conclusion():
    st = compute_stats(sample(), COINS)
    assert st["significance"]["enough"] is False and "выводов о прибыльности делать нельзя" in st["significance"]["note"]
    many = [trade("BTCUSDT", 1.0 if i % 2 else -1.0, i) for i in range(MIN_TRADES_FOR_CONCLUSIONS)]
    assert compute_stats(many, COINS)["significance"] == {"min_trades": MIN_TRADES_FOR_CONCLUSIONS, "enough": True, "note": None}


def test_closed_trade_without_r_is_a_data_error_not_silently_skipped():
    bad = {**trade("BTCUSDT", 1.0, 1), "r_multiple": None}
    with pytest.raises(DataIntegrityError):
        compute_stats([bad], COINS)


def test_equity_curve_is_downsampled_but_keeps_the_last_point():
    rows = [trade("BTCUSDT", 1.0, i) for i in range(5000)]
    curve = compute_stats(rows, COINS)["equity_curve"]
    assert len(curve) <= 2001 and curve[-1]["cum_r"] == 5000


def test_counts_separate_gap_affected_and_reject_unknown_status():
    rows = [{"status": "closed", "gap_affected": False, "n": 5}, {"status": "closed", "gap_affected": True, "n": 2},
            {"status": "pending_limit", "gap_affected": False, "n": 1}, {"status": "pending_market", "gap_affected": False, "n": 1},
            {"status": "open", "gap_affected": False, "n": 3}, {"status": "aborted", "gap_affected": False, "n": 4}]
    assert counts_from_rows(rows) == {"closed": 5, "closed_gap_affected": 2, "open": 3, "pending": 2, "cancelled": 0, "aborted": 4}
    with pytest.raises(DataIntegrityError):
        counts_from_rows([{"status": "weird", "gap_affected": False, "n": 1}])


# ================================================================================== причина входа


def test_smc_reason_describes_the_setup_in_the_direction_of_the_trade():
    meta = {"pool_level": 100500.5, "sweep_time": 1_760_000_000_000, "bars_since_sweep": 7, "risk_pct": 0.0042, "atr": 310.25, "interval": "15m"}
    short = build_reason({"strategy_name": "smc", "side": "short", "coin": "BTCUSDT", "planned_entry": 99900.0, "sl": 100800.0, "tp": 97200.0},
                         meta, [{"label": "FVG"}, {"label": "SL"}])
    assert short["title"] == "SMC Swing · шорт · BTCUSDT"
    assert "Swing High 100500.5" in short["summary"] and "Медвежий FVG" in short["summary"] and "99900" in short["summary"] and "7 бар. назад" in short["summary"]
    facts = {f["label"]: f["value"] for f in short["facts"]}
    assert facts["Расстояние до SL, % от цены"] == "0.42" and facts["Таймфрейм"] == "15m" and short["geometry_labels"] == ["FVG", "SL"]
    long = build_reason({"strategy_name": "smc", "side": "long", "coin": "ETHUSDT", "planned_entry": 1.0, "sl": 0.9, "tp": 1.3}, meta, [])
    assert "Swing Low" in long["summary"] and "Бычий FVG" in long["summary"] and long["title"].endswith("лонг · ETHUSDT")


def test_smc_reason_shows_h4_trend_in_title_summary_and_facts_and_old_trades_stay_readable():
    base = {"pool_level": 150.5, "sweep_time": 1_760_000_000_000, "bars_since_sweep": 4, "risk_pct": 0.004, "atr": 1.2, "interval": "15m"}
    trade = {"strategy_name": "smc", "side": "short", "coin": "SOLUSDT", "planned_entry": 150.0, "sl": 152.0, "tp": 144.0}
    r = build_reason(trade, {**base, "htf_interval": "4h", "htf_bias": -1, "htf_label": "Bearish"}, [])
    assert r["title"] == "SMC Swing · шорт · SOLUSDT (H4: Bearish)"
    assert r["summary"].splitlines()[0].startswith("0. Контекст старшего таймфрейма (H4: Bearish): шорт по тренду")
    assert {"label": "Тренд H4 (HTF Bias)", "value": "Bearish"} in r["facts"]
    neutral = build_reason(trade, {**base, "htf_interval": "4h", "htf_bias": 0, "htf_label": "Neutral"}, [])
    assert "(H4: Neutral)" in neutral["title"] and "нейтральная" in neutral["summary"]
    long_bull = build_reason({**trade, "side": "long"}, {**base, "htf_interval": "4h", "htf_bias": 1, "htf_label": "Bullish"}, [])
    assert long_bull["title"].endswith("лонг · SOLUSDT (H4: Bullish)") and "лонг по тренду" in long_bull["summary"]
    old = build_reason(trade, base, [])  # сделки до 2026-10-11 без H4 в meta: прежний текст без «0.» и без скобок
    assert old["title"] == "SMC Swing · шорт · SOLUSDT" and old["summary"].startswith("1.") and all("H4" not in f["label"] for f in old["facts"])
    bo = build_reason({"strategy_name": "breakout", "side": "long", "coin": "SOLUSDT", "entry_price": 1.0, "sl": 0.9},
                      {"level": 1.0, "interval": "15m", "htf_interval": "4h", "htf_bias": -1, "htf_label": "Bearish"}, [])
    assert bo["title"] == "Breakout Buildup · лонг · SOLUSDT" and any("справочно" in f["label"] for f in bo["facts"])  # фильтр Breakout не применяется


def test_breakout_reason_and_unknown_strategy_fallback():
    r = build_reason({"strategy_name": "breakout", "side": "long", "coin": "DOGEUSDT", "entry_price": 0.1523, "sl": 0.149},
                     {"level": 0.15, "eqh_spread": 0.0007, "vol_ratio": 2.4, "atr": 0.002, "interval": "15m"}, [])
    assert "0.15" in r["summary"] and "0.070%" in r["summary"] and "×2.40" in r["summary"] and "0.1523" in r["summary"]
    other = build_reason({"strategy_name": "future", "side": "long", "coin": "X", "sl": 1.0}, {"alpha": 1.5, "tag": "z"}, [])
    assert "future" in other["title"] and {f["label"] for f in other["facts"]} == {"alpha", "tag"}
    assert build_reason({"strategy_name": "smc", "side": "long", "coin": "X", "sl": 1.0}, None, [])["facts"]  # снапшот без meta не ломает просмотр


def test_price_formatting_handles_pepe_and_btc():
    assert fmt_price(97123.456789) == "97123.46" and fmt_price(0.0000043) == "0.00000430" and fmt_price(1.5) == "1.50000" and fmt_price(None) == "—"


# ================================================================================== живое состояние


def srow(coin, side, st, detail=None, strategy="SMC_Pro_Trend", at=T0):
    return {"coin": coin, "strategy": strategy, "side": side, "to_state": f"{side}:S{st}", "ts": at, "reason": None, "detail": detail}


def test_default_state_is_s0_for_every_coin_and_side():
    s = build_state(COINS, [], [], [], None, T0)
    assert [c["coin"] for c in s["coins"]] == list(COINS) and s["shadow"] == {"heartbeat_age_s": None, "alive": None}
    for c in s["coins"]:
        assert [(x["side"], x["state"], x["since"]) for x in c["setups"]] == [("short", 0, None), ("long", 0, None)]


def test_state_rows_override_defaults_and_unknown_coins_are_added():
    s = build_state(COINS, [srow("BTCUSDT", "short", 2), srow("BTCUSDT", "long", 3, {"reject": "exposure"}), srow("SOLUSDT", "long", 1)],
                    [], [], 5.0, T0)
    btc = s["coins"][0]["setups"]
    assert (btc[0]["state"], btc[0]["label"]) == (2, "свип + FVG сформирован")
    assert (btc[1]["state"], btc[1]["reject"], btc[1]["reject_label"]) == (3, "exposure", "плечо выше потолка")
    assert s["coins"][-1]["coin"] == "SOLUSDT" and s["coins"][-1]["setups"][1]["state"] == 1 and s["shadow"]["alive"] is True
    assert build_state(COINS, [], [], [], 500.0, T0)["shadow"]["alive"] is False  # пульс старше 120 с


def test_unreadable_state_is_a_data_error():
    for bad in ("short:S9", "S2", "long:S2", "", None):  # «long:S2» в строке стороны short - несовпадение
        with pytest.raises(DataIntegrityError):
            parse_setup_row({**srow("BTCUSDT", "short", 1), "to_state": bad})


def test_fingerprint_ignores_clock_and_heartbeat_but_not_state():
    a = build_state(COINS, [srow("BTCUSDT", "short", 1)], [], [], 1.0, T0)
    b = build_state(COINS, [srow("BTCUSDT", "short", 1)], [], [], 9.0, T0 + timedelta(seconds=8))
    c = build_state(COINS, [srow("BTCUSDT", "short", 2)], [], [], 1.0, T0)
    assert fingerprint(a) == fingerprint(b) != fingerprint(c)


def test_heartbeat_age_reads_file_mtime(tmp_path):
    assert heartbeat_age_s(tmp_path) is None
    hb = tmp_path / ".shadow_heartbeat"
    hb.write_text("x")
    mtime = hb.stat().st_mtime
    assert heartbeat_age_s(tmp_path, now=mtime + 7.5) == pytest.approx(7.5) and heartbeat_age_s(tmp_path, now=mtime - 5) == 0.0


# ================================================================================== сериализация


def test_serialization_rejects_non_finite_numbers_and_naive_time():
    assert num(None) is None and num(5) == 5.0
    for bad in (float("nan"), float("inf"), "1", True):
        with pytest.raises(DataIntegrityError):
            num(bad)
    assert iso(T0) == "2026-10-10T12:00:00.000Z" and iso(None) is None
    with pytest.raises(DataIntegrityError):
        iso(datetime(2026, 1, 1))


def test_api_strategy_list_matches_the_registry_and_reasons_exist_for_every_strategy():
    from api.settings import DEFAULT_STRATEGIES
    from orchestrator.registry import SHADOW_STRATEGIES

    assert DEFAULT_STRATEGIES == SHADOW_STRATEGIES  # образ API не содержит orchestrator/: список продублирован, расхождение ловит этот тест
    meta = {"interval": "15m", "atr": 1.0, "pool_level": 10.0, "sweep_extreme": 11.0, "sweep_depth_atr": 2.7, "vol_ratio": 3.1, "bos_level": 10.0,
            "obi": 0.3, "delta": 0.4, "via_obi": True, "via_delta": False, "session": "london", "asia_high": 10.5, "asia_low": 9.5, "vwap": 10.0,
            "sd": 0.2, "band": 10.6, "sd_mult": 3.0, "target_vwap_r": 2.2}
    for name in SHADOW_STRATEGIES:
        for side in ("short", "long"):
            r = build_reason({"strategy_name": name, "strategy_id": name, "side": side, "coin": "SOLUSDT", "entry_price": 10.0, "sl": 11.0, "planned_entry": 10.0, "tp": 8.0},
                             meta, [])
            assert r["title"].endswith("· SOLUSDT") or "SOLUSDT" in r["title"]
            assert "не описана" not in r["summary"] and r["summary"] and r["facts"], name  # у каждой из пяти - своя причина входа, а не общий текст


def test_price_scale_matches_the_client():
    """Образ API не содержит клиента: шкала знаков продублирована в api/reason.py и client/tradeai_client/fmt.py - здесь они сверяются."""
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location("client_fmt", Path(__file__).parents[2] / "client" / "tradeai_client" / "fmt.py")
    client_fmt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client_fmt)
    for v in (97123.456, 2539.6203, 150.0, 31.457, 1.08234567, 0.45, 0.0123, 0.0000043, -12.5):
        assert fmt_price(v) == client_fmt.fmt_price(v), v
