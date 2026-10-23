import logging

import config


def test_obi_breakout_allowed_only_on_live_tier_and_blocked_on_mt5_and_binance_test(monkeypatch):
    from orchestrator.registry import SHADOW_STRATEGIES, strategy_allowed
    from utils.symbolset import SymbolSet
    live = SymbolSet(("BTCUSDT", "ETHUSDT", "SOLUSDT"))  # BTC/ETH закреплены; SOL прошла порог 400M (состав задаёт автоселектор)
    monkeypatch.setattr(config, "LIVE_BINANCE_SYMBOLS", live)
    monkeypatch.setattr(config, "STRATEGY_SYMBOLS", {"OBI_Breakout": live})
    for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES)
    for s in ("EURUSD", "XAUUSD", "XAGUSD", "LOWUSDT"):  # MT5 и монета Binance Test (вне live-набора): без стакана OBI не работает
        assert not strategy_allowed("OBI_Breakout", s)
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES if n != "OBI_Breakout")


def test_websocket_library_loggers_muted(tmp_path, monkeypatch):
    from utils.logger import setup_logging
    monkeypatch.setattr(config, "LOG_DIR", tmp_path)
    setup_logging("t")
    for name in ("engineio.client", "socketio.client", "httpx", "httpcore"):
        assert logging.getLogger(name).level == logging.WARNING
