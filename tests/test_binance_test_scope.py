"""Скоуп «Binance Test»: тикеры только в Shadow, никогда в live (Fail-Fast в config.validate); состав задаёт автоселектор (tests/orchestrator/test_universe.py)."""

import pytest

import config
from utils.errors import ConfigError
from utils.symbolset import SymbolSet


def test_default_scope_is_empty_pinned_are_live_and_config_valid():
    assert isinstance(config.BINANCE_TEST_SYMBOLS, SymbolSet) and len(config.BINANCE_TEST_SYMBOLS) == 0
    assert list(config.LIVE_BINANCE_SYMBOLS)[:2] == ["BTCUSDT", "ETHUSDT"] and config.UNIVERSE_PINNED == ("BTCUSDT", "ETHUSDT")
    config.validate()


@pytest.mark.parametrize("live_name", ["LIVE_BINANCE_SYMBOLS", "LIVE_MT5_SYMBOLS"])
def test_test_symbol_in_live_list_fails_fast(monkeypatch, live_name):
    monkeypatch.setattr(config, "BINANCE_TEST_SYMBOLS", SymbolSet(("AAAUSDT",)))
    live = getattr(config, live_name)
    monkeypatch.setattr(config, live_name, SymbolSet([*live, "AAAUSDT"]) if isinstance(live, SymbolSet) else (*live, "AAAUSDT"))
    with pytest.raises(ConfigError, match="только Shadow"):
        config.validate()


def test_test_symbol_in_core_pool_or_bad_format_fails(monkeypatch):
    monkeypatch.setattr(config, "BINANCE_TEST_SYMBOLS", SymbolSet(("EURUSD",)))
    with pytest.raises(ConfigError):
        config.validate()
    monkeypatch.setattr(config, "BINANCE_TEST_SYMBOLS", SymbolSet(("aaausdt",)))
    with pytest.raises(ConfigError, match="BINANCE_TEST_SYMBOLS"):
        config.validate()


@pytest.mark.parametrize("name, value", [("UNIVERSE_TEST_MIN_USDT", 5e8), ("UNIVERSE_REFRESH_S", 0.0), ("UNIVERSE_MAX_SYMBOLS", 1), ("UNIVERSE_MIN_LISTING_DAYS", 0)])
def test_universe_parameters_are_validated(monkeypatch, name, value):
    monkeypatch.setattr(config, name, value)
    with pytest.raises(ConfigError, match="Автоселектор"):
        config.validate()
