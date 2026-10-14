import logging

import config


def test_obi_breakout_allowed_on_every_binance_symbol_and_blocked_on_mt5():
    from orchestrator.registry import SHADOW_STRATEGIES, strategy_allowed
    crypto = [s for s in config.SHADOW_SYMBOLS if s not in config.LIVE_MT5_SYMBOLS]
    assert len(crypto) == 8 and "SOLUSDT" in crypto and "LINKUSDT" in crypto
    for s in crypto:
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES)
    for s in ("EURUSD", "XAUUSD", "XAGUSD"):
        assert not strategy_allowed("OBI_Breakout", s)
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES if n != "OBI_Breakout")


def test_websocket_library_loggers_muted(tmp_path, monkeypatch):
    from utils.logger import setup_logging
    monkeypatch.setattr(config, "LOG_DIR", tmp_path)
    setup_logging("t")
    for name in ("engineio.client", "socketio.client", "metaapi_cloud_sdk"):
        assert logging.getLogger(name).level == logging.WARNING
