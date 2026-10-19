"""Общие фабрики для тестов боевого контура: сигналы, фильтры, менеджер риска."""

from pathlib import Path

from execution.exchange_filters import SymbolFilters
from orchestrator.live_risk import DAY_MS, LiveRiskManager
from orchestrator.types import Direction, Kind, Signal

STRATEGY = "SMC_Pro_Trend"
T0 = 1_760_000_000_000 - 1_760_000_000_000 % DAY_MS + 10 * 3_600_000  # 10:00 UTC какого-то дня
BAR = 900_000
FILTERS = {
    "SOLUSDT": SymbolFilters("SOLUSDT", tick_size=0.01, step_size=0.1, min_qty=0.1, min_notional=5.0),
    "XRPUSDT": SymbolFilters("XRPUSDT", tick_size=0.0001, step_size=0.1, min_qty=0.1, min_notional=5.0),
}


def signal(*, strategy=STRATEGY, symbol="SOLUSDT", direction=Direction.LONG, kind=Kind.MARKET, entry=100.0, sl=98.0, tp=None,
           inval=None, n=1) -> Signal:
    if kind is Kind.LIMIT:
        sgn = direction.sgn
        tp = tp if tp is not None else entry - sgn * 6.0
        inval = inval if inval is not None else sl
    return Signal(
        signal_id=f"{strategy}_{symbol}_{direction.value}_{n}", strategy=strategy, symbol=symbol, interval="15m", direction=direction,
        kind=kind, bar_open_time=T0, bar_close_ms=T0 + BAR, entry=entry, sl=sl, tp=tp, inval=inval,
        meta={"htf_label": "Bullish"},
    )


def manager(tmp_path: Path, *, approved=(STRATEGY,), margin=10.0, leverage=10, limit=50.0, filters=FILTERS, **kw) -> LiveRiskManager:
    # Стандартная фикстура: вход 100, SL 98 (стоп 2%). Риск подобран так, чтобы объём остался прежним (margin * leverage / 100): margin 10 -> 2 USDT -> 1.0 лот.
    kw.setdefault("risk_usdt", margin * leverage * 0.02)
    return LiveRiskManager(filters, approved_strategies=approved, margin_usdt=margin, leverage=leverage, daily_loss_limit_usdt=limit,
                           state_path=tmp_path / "risk.json", **kw)
