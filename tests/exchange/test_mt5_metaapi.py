"""Скелет MetaApi: Fail-Fast на отсутствие ключей, секрет не в repr, торговые методы - заглушки (без сети)."""

import asyncio

import pytest

from exchange.mt5_metaapi import MetaApiCredentials, MetaApiLiveClient
from utils.errors import ConfigError

OK = {"METAAPI_TOKEN": "super-secret-token", "ACCOUNT_ID": "acc-123"}


@pytest.mark.parametrize("env", [{}, {"METAAPI_TOKEN": "t"}, {"ACCOUNT_ID": "a"}, {"METAAPI_TOKEN": "  ", "ACCOUNT_ID": "a"}])
def test_missing_credentials_fail_fast(env):
    with pytest.raises(ConfigError, match="METAAPI_TOKEN|ACCOUNT_ID"):
        MetaApiLiveClient(env)


def test_constructor_does_not_touch_the_network_and_hides_the_token():
    client = MetaApiLiveClient(OK)
    assert client.account_id == "acc-123"
    assert "super-secret-token" not in repr(client) + repr(MetaApiCredentials.from_env(OK))


def test_account_id_alias_has_priority():
    assert MetaApiCredentials.from_env({**OK, "METAAPI_ACCOUNT_ID": "alias"}).account_id == "alias"


def test_trading_methods_are_stubs():
    client = MetaApiLiveClient(OK)
    with pytest.raises(NotImplementedError):
        asyncio.run(client.get_balance())
    with pytest.raises(NotImplementedError):
        asyncio.run(client.market_order("EURUSD", "BUY", 0.01, client_id="x"))
    with pytest.raises(NotImplementedError):
        client.round_lots("EURUSD", 0.013)
