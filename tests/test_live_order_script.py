"""scripts/test_live_order.py: принудительная тестовая сделка на testnet (проверяется на имитации биржи, сеть не используется)."""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import config
from exchange.binance_live import BinanceLiveClient
from execution.exchange_filters import SymbolFilters
from scripts import test_live_order as script
from tests.orchestrator.fake_live_rest import FakeRest, api_error
from utils.errors import ConfigError

ROOT = Path(__file__).resolve().parents[1]
PEPE = "1000PEPEUSDT"
PEPE_FILTERS = SymbolFilters(PEPE, tick_size=0.0000001, step_size=1.0, min_qty=1.0, min_notional=5.0)


def make(price=0.0123):
    rest = FakeRest(price)
    rest.filters = {PEPE: PEPE_FILTERS}
    return rest, BinanceLiveClient(rest)


def opts(**kw):
    return script.Options(**{"symbol": PEPE, "notional": 10.0, "sl_pct": 2.0, "tp_pct": 3.0, "leverage": 10, **kw})


def check(client, o=None):
    report = script.Report()
    asyncio.run(script.run_check(client, o or opts(), report))
    return report


def status(report, name):
    return [r["status"] for r in report.rows if r["name"] == name]


def test_happy_path_opens_protects_moves_to_breakeven_and_cleans_up():
    rest, client = make()
    report = check(client)
    assert not report.failed, report.table()
    for name in ("time_and_signature", "exchange_filters", "balance", "market_entry", "position_risk", "stop_loss_placed",
                 "take_profit_placed_with_stop", "get_algo_order", "open_algo_orders", "breakeven_new_stop_with_two_live",
                 "breakeven_old_stop_cancelled", "cleanup:flat_and_no_algo", "income"):
        assert status(report, name) == ["PASS"], (name, report.table())
    entry = [p for p in rest.calls("POST", config.ORDER_PATH) if p["type"] == "MARKET" and "reduceOnly" not in p][0]
    assert entry["symbol"] == PEPE and entry["side"] == "BUY" and float(entry["quantity"]) * 0.0123 >= 5.0  # около 10 USDT, не ниже минимума
    assert 9.0 <= float(entry["quantity"]) * 0.0123 <= 11.0
    stops = [p for p in rest.calls("POST", config.ALGO_ORDER_PATH) if p["type"] == "STOP_MARKET"]
    tp = [p for p in rest.calls("POST", config.ALGO_ORDER_PATH) if p["type"] == "TAKE_PROFIT_MARKET"]
    assert len(tp) == 1 and tp[0]["triggerPrice"] == "0.0126690" .rstrip("0") and tp[0]["reduceOnly"] is True  # +3% от 0.0123
    assert stops[0]["triggerPrice"] == "0.012054" and stops[0]["quantity"] == entry["quantity"]  # -2%
    # позиция закрыта reduce-only рынком, живых algo-ордеров нет, счёт чист
    assert rest.positions[PEPE] == 0.0 and all(a["algoStatus"] != "NEW" for a in rest.algos.values())
    closer = [p for p in rest.calls("POST", config.ORDER_PATH) if p.get("reduceOnly")]
    assert len(closer) == 1 and closer[0]["side"] == "SELL"
    # безубыток: новый стоп поставлен РАНЬШЕ, чем снят старый
    names = rest.names()
    first_new_stop = [i for i, n in enumerate(names) if n == "POST algo:STOP_MARKET"][1]
    first_delete = names.index("DELETE algo")
    assert first_new_stop < first_delete
    assert any(r["name"] == "probe_immediate_trigger" and "-2021" in r["detail"] for r in report.rows)


def test_existing_position_on_the_symbol_means_exit_without_a_single_order():
    rest, client = make()
    rest.positions[PEPE] = 500.0
    rest.entry_prices[PEPE] = 0.01
    report = check(client)
    assert status(report, "clean_symbol") == ["FAIL"] and rest.calls("POST", config.ORDER_PATH) == [] and rest.calls("POST", config.ALGO_ORDER_PATH) == []
    assert rest.positions[PEPE] == 500.0  # чужую позицию скрипт не трогает (уборка касается только своих ордеров, но позиция сохраняется)


def test_exchange_that_refuses_sl_plus_tp_is_reported_and_everything_is_cleaned():
    rest, client = make()
    rest.fail[("POST", config.ALGO_ORDER_PATH)] = [None, api_error(-4130, "reduceOnly sum exceeds position")]  # SL принят, TP - нет
    report = check(client)
    assert report.failed and status(report, "exchange_rejected") == ["FAIL"]
    assert rest.positions[PEPE] == 0.0 and all(a["algoStatus"] != "NEW" for a in rest.algos.values())  # позиция закрыта, SL снят
    assert status(report, "cleanup:flat_and_no_algo") == ["PASS"]
    assert report.facts["rejection"]["code"] == -4130


def test_guard_refuses_without_use_testnet_or_with_a_non_testnet_url(monkeypatch):
    monkeypatch.setattr(config, "USE_TESTNET", False)
    with pytest.raises(ConfigError, match="USE_TESTNET"):
        script.guard()
    monkeypatch.setattr(config, "USE_TESTNET", True)
    monkeypatch.setattr(config, "LIVE_REST_URL", "https://fapi.binance.com")
    with pytest.raises(ConfigError, match="не testnet"):
        script.guard()
    monkeypatch.setattr(config, "LIVE_REST_URL", config.TESTNET_REST_URL)
    script.guard()


def test_main_runs_end_to_end_and_writes_the_json_report(monkeypatch, tmp_path, capsys):
    rest, client = make()
    monkeypatch.setattr(config, "USE_TESTNET", True)
    monkeypatch.setattr(config, "LIVE_REST_URL", config.TESTNET_REST_URL)
    monkeypatch.setattr(script, "OUT_DIR", tmp_path / "out")
    monkeypatch.setattr(BinanceLiveClient, "from_env", classmethod(lambda cls, session, base_url=None: client))
    assert asyncio.run(script.main_async(opts())) == 0
    (path,) = list((tmp_path / "out").glob("report-*.json"))
    data = json.loads(path.read_text(encoding="utf-8"))
    assert {r["status"] for r in data["rows"]} <= {"PASS", "INFO"} and data["facts"]["entry_order"]["status"] == "FILLED"
    assert any(q["path"] == config.ALGO_ORDER_PATH and q["method"] == "POST" for q in data["requests"])  # запросы с ответами сохранены
    assert "Итог: OK" in capsys.readouterr().out


def test_command_line_exits_with_config_error_code_before_any_network_when_not_on_testnet():
    env = {k: v for k, v in os.environ.items() if k not in ("USE_TESTNET", "BINANCE_API_KEY", "BINANCE_API_SECRET")}
    out = subprocess.run([sys.executable, "-m", "scripts.test_live_order"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
    assert out.returncode == 3  # ConfigError


def test_arguments_are_validated():
    assert script.parse_args(["--symbol", "ADAUSDT", "--notional", "12"]).notional == 12.0
    with pytest.raises(ConfigError):
        script.parse_args(["--sl-pct", "0"])
