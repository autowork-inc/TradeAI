"""Маппинг символов MT5 (`exchange.symbol_mapper`): суффиксы брокера, псевдонимы, неоднозначность, строгий разбор спецификации."""

import asyncio

import pytest

from exchange.symbol_mapper import SymbolMapper, asset_class_mismatch, is_suffix_variant, parse_overrides, parse_spec, resolve_name
from utils.errors import ConfigError

RAW = {"contractSize": 100_000, "minVolume": 0.01, "maxVolume": 100, "volumeStep": 0.01, "tickSize": 0.00001, "digits": 5, "point": 0.00001,
       "stopsLevel": 10, "freezeLevel": 0, "fillingModes": ["SYMBOL_FILLING_FOK", "SYMBOL_FILLING_IOC"], "tradeMode": "SYMBOL_TRADE_MODE_FULL",
       "profitCurrency": "USD", "marginCurrency": "EUR", "path": "Forex\\EURUSD", "description": "Euro vs US Dollar"}


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize("name,expected", [
    ("EURUSDm", True), ("EURUSD.ecn", True), ("EURUSD#", True), ("EURUSD_i", True), ("EURUSDpro", True), ("eurusdm", True),
    ("EURUSDT", False), ("EURUSDX", False), ("EURUSD", False), ("EURUSDmicrolots", False), ("EURGBP", False),
])
def test_suffix_rule(name, expected):
    assert is_suffix_variant("EURUSD", name) is expected


def test_exact_name_wins_over_suffix_variants():
    assert resolve_name("EURUSD", ["EURUSDm", "EURUSD", "EURUSD.ecn"]) == "EURUSD"


def test_single_suffix_variant_is_accepted_and_ambiguous_ones_fail():
    assert resolve_name("EURUSD", ["GBPUSD", "EURUSD.ecn"]) == "EURUSD.ecn"
    assert resolve_name("XAUUSD", ["XAUUSDm", "EURUSDm"]) == "XAUUSDm"
    with pytest.raises(ConfigError, match="неоднозначно") as ei:
        resolve_name("EURUSD", ["EURUSDm", "EURUSD.ecn"])
    assert "MT5_SYMBOL_MAP" in str(ei.value)
    assert resolve_name("EURUSD", ["EURUSDm", "EURUSD.ecn"], overrides={"EURUSD": "EURUSD.ecn"}) == "EURUSD.ecn"


def test_other_instrument_is_not_taken_for_the_symbol():
    with pytest.raises(ConfigError, match="не найден"):
        resolve_name("EURUSD", ["EURUSDT", "GBPUSD"])


def test_aliases_for_commodities_and_indices():
    assert resolve_name("XAUUSD", ["GOLD", "EURUSD"]) == "GOLD"
    assert resolve_name("XAUUSD", ["GOLD", "XAUUSD"]) == "XAUUSD"  # первое базовое имя приоритетнее
    assert resolve_name("BRN", ["UKOIL", "USOIL"]) == "UKOIL"
    assert resolve_name("BRN", ["UKOIL.cash", "USOIL"]) == "UKOIL.cash"
    assert resolve_name("US500", ["SPX500", "US30"]) == "SPX500"
    assert resolve_name("XAGUSD", ["SILVERm"]) == "SILVERm"


def test_case_insensitive_exact_and_unknown_symbol():
    assert resolve_name("EURUSD", ["eurusd"]) == "eurusd"
    with pytest.raises(ConfigError, match="не найден"):
        resolve_name("NZDCAD", ["EURUSD"])


def test_override_must_exist_at_broker():
    with pytest.raises(ConfigError, match="отсутствует у брокера"):
        resolve_name("BRN", ["UKOIL"], overrides={"BRN": "BRENT.cash"})
    assert resolve_name("BRN", ["BRENT.cash", "UKOIL"], overrides={"BRN": "BRENT.cash"}) == "BRENT.cash"


def test_parse_overrides():
    assert parse_overrides("BRN=UKOIL, us500=US500.cash") == {"BRN": "UKOIL", "US500": "US500.cash"}
    assert parse_overrides("") == {}
    for bad in ("BRN", "=X", "BRN="):
        with pytest.raises(ConfigError):
            parse_overrides(bad)


# ----------------------------------------------------------------------------------------------- спецификации


def test_parse_spec_fields():
    s = parse_spec("EURUSD", "EURUSDm", RAW)
    assert (s.internal, s.broker, s.contract_size, s.min_volume, s.max_volume, s.volume_step, s.tick_size) == ("EURUSD", "EURUSDm", 100_000, 0.01, 100, 0.01, 0.00001)
    assert s.filling_modes == ("SYMBOL_FILLING_FOK", "SYMBOL_FILLING_IOC") and s.min_stop_distance == pytest.approx(0.0001)


@pytest.mark.parametrize("patch,match", [
    ({"contractSize": 0}, "contractSize"), ({"volumeStep": None}, "volumeStep"), ({"tickSize": -1}, "tickSize"),
    ({"minVolume": 5, "maxVolume": 1}, "minVolume"), ({"tradeMode": "SYMBOL_TRADE_MODE_DISABLED"}, "запрещена"), ({"digits": "5"}, "digits"),
])
def test_parse_spec_rejects_bad_specs(patch, match):
    with pytest.raises(ConfigError, match=match):
        parse_spec("EURUSD", "EURUSD", {**RAW, **patch})


class FakeClient:
    def __init__(self, symbols, specs):
        self.symbols, self.specs, self.spec_calls = symbols, specs, []

    async def get_symbols(self):
        return self.symbols

    async def get_symbol_specification(self, symbol):
        self.spec_calls.append(symbol)
        return self.specs[symbol]


def test_mapper_loads_and_caches_specs():
    gold = {**RAW, "contractSize": 100, "path": "Metals\\XAUUSD"}
    oil = {**RAW, "contractSize": 1000, "path": "Commodities\\UKOIL"}
    client = FakeClient(["EURUSDm", "XAUUSDm", "UKOIL"], {"EURUSDm": {**RAW, "path": "Forex\\EURUSDm"}, "XAUUSDm": gold, "UKOIL": oil})
    mapper = SymbolMapper(client, overrides={})
    run(mapper.load(["EURUSD", "XAUUSD", "BRN"]))
    assert [mapper.broker_name(x) for x in ("EURUSD", "XAUUSD", "BRN")] == ["EURUSDm", "XAUUSDm", "UKOIL"]
    assert mapper.spec("XAUUSD").contract_size == 100 and mapper.spec("BRN").contract_size == 1000 and mapper.spec("BRN").path == "Commodities\\UKOIL"
    assert mapper.internal_name("XAUUSDm") == "XAUUSD" and "EURUSD" in mapper and mapper.internals == ("EURUSD", "XAUUSD", "BRN")
    assert client.spec_calls == ["EURUSDm", "XAUUSDm", "UKOIL"]  # спецификации запрошены один раз и лежат в кеше
    with pytest.raises(ConfigError, match="не загружен"):
        mapper.spec("US500")
    with pytest.raises(ConfigError, match="нет в SymbolMapper"):
        mapper.internal_name("XYZ")


def test_mapper_fail_fast_does_not_change_cache():
    client = FakeClient(["EURUSD"], {"EURUSD": RAW})
    mapper = SymbolMapper(client, overrides={})
    run(mapper.load(["EURUSD"]))
    with pytest.raises(ConfigError, match="не найден"):
        run(mapper.load(["EURUSD", "XAUUSD"]))
    assert mapper.internals == ("EURUSD",)
    bad = FakeClient(["EURUSD"], {"EURUSD": {**RAW, "volumeStep": 0}})
    with pytest.raises(ConfigError, match="volumeStep"):
        run(SymbolMapper(bad, overrides={}).load(["EURUSD"]))


def test_mapper_reads_overrides_from_env(monkeypatch):
    monkeypatch.setenv("MT5_SYMBOL_MAP", "BRN=BRENT.cash")
    client = FakeClient(["BRENT.cash", "UKOIL"], {"BRENT.cash": RAW, "UKOIL": RAW})
    mapper = SymbolMapper(client)
    run(mapper.load(["BRN"]))
    assert mapper.broker_name("BRN") == "BRENT.cash"


# ----------------------------------------------------------------------------------------------- класс актива (тёзки-акции)


STOCK = {**RAW, "contractSize": 1, "path": "Nasdaq\\Stock\\BRN", "description": "Barnwell Industries Inc"}


def test_asset_class_rules():
    assert asset_class_mismatch("EURUSD", {"path": "Forex\\EURUSD"}) is None
    assert asset_class_mismatch("XAUUSD", {"path": "Metals\\XAUUSD"}) is None
    assert asset_class_mismatch("US500", {"path": "Indexes\\US500"}) is None
    assert asset_class_mismatch("BRN", {"path": "Commodities\\UKOIL"}) is None
    assert "акция" in asset_class_mismatch("BRN", {"path": "Nasdaq\\Stock\\BRN"})
    assert "акция" in asset_class_mismatch("XAUUSD", {"path": "Nasdaq\\Stock\\GOLD"})
    assert "ETF" in asset_class_mismatch("US500", {"path": "Nasdaq\\ETF\\SPXL"})
    assert "не похож" in asset_class_mismatch("EURUSD", {"path": "Metals\\EURUSD"})  # шестибуквенная пара обязана лежать в Forex
    assert "нет path" in asset_class_mismatch("EURUSD", {})  # путь неизвестен - класс не проверить, значит не принимаем


def test_stock_with_the_same_ticker_is_rejected_and_next_alias_is_used():
    client = FakeClient(["BRN", "UKOIL"], {"BRN": STOCK, "UKOIL": {**RAW, "contractSize": 1000, "path": "Commodities\\UKOIL"}})
    mapper = SymbolMapper(client, overrides={})
    run(mapper.load(["BRN"]))
    assert mapper.broker_name("BRN") == "UKOIL" and client.spec_calls == ["BRN", "UKOIL"]


def test_only_a_stock_is_available_fails_fast_and_names_the_rejected():
    client = FakeClient(["BRN", "GOLD"], {"BRN": STOCK, "GOLD": {**STOCK, "path": "Nasdaq\\Stock\\GOLD", "description": "Barrick Gold"}})
    with pytest.raises(ConfigError, match="отвергнуты по классу актива") as ei:
        run(SymbolMapper(client, overrides={}).load(["BRN"]))
    assert "BRN" in str(ei.value) and "акция" in str(ei.value)
    with pytest.raises(ConfigError, match="отвергнуты") as ei:  # золото: XAUUSD нет, а GOLD у брокера - акция
        run(SymbolMapper(client, overrides={}).load(["XAUUSD"]))


def test_explicit_override_skips_the_asset_class_check():
    client = FakeClient(["BRN"], {"BRN": STOCK})
    mapper = SymbolMapper(client, overrides={"BRN": "BRN"})  # человек сам решил
    run(mapper.load(["BRN"]))
    assert mapper.broker_name("BRN") == "BRN"


def test_disabled_symbol_is_a_hard_error_not_a_silent_skip():
    index = {**RAW, "contractSize": 1, "path": "Indexes\\US500", "tradeMode": "SYMBOL_TRADE_MODE_DISABLED"}
    client = FakeClient(["US500", "US500M"], {"US500": index, "US500M": {**index, "path": "Indexes\\US500M"}})
    with pytest.raises(ConfigError, match="запрещена брокером"):
        run(SymbolMapper(client, overrides={}).load(["US500"]))


def test_suffix_variants_are_filtered_by_class_before_ambiguity_is_declared():
    client = FakeClient(["EURUSDm", "EURUSD.ecn"], {"EURUSDm": {**RAW, "path": "Forex\\EURUSDm"}, "EURUSD.ecn": {**RAW, "path": "Crypto\\EURUSD.ecn"}})
    mapper = SymbolMapper(client, overrides={})
    run(mapper.load(["EURUSD"]))
    assert mapper.broker_name("EURUSD") == "EURUSDm"
    client = FakeClient(["EURUSDm", "EURUSD.ecn"], {"EURUSDm": {**RAW, "path": "Forex\\a"}, "EURUSD.ecn": {**RAW, "path": "Forex\\b"}})
    with pytest.raises(ConfigError, match="неоднозначно"):
        run(SymbolMapper(client, overrides={}).load(["EURUSD"]))
