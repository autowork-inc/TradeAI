

def test_obi_breakout_only_on_binance_symbols():
    from orchestrator.registry import SHADOW_STRATEGIES, strategy_allowed
    for s in ("BTCUSDT", "ETHUSDT"):
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES)
    for s in ("EURUSD", "XAUUSD", "XAGUSD"):
        assert not strategy_allowed("OBI_Breakout", s)
        assert all(strategy_allowed(n, s) for n in SHADOW_STRATEGIES if n != "OBI_Breakout")
