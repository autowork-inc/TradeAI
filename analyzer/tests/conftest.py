"""Синтетические сделки и настройки для тестов анализатора (без PostgreSQL и сети)."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analyzer.config import Settings
from analyzer.frame import build_frame

T0 = pd.Timestamp("2026-09-01 00:00", tz="UTC")
TOKEN = "t" * 40
DSN = "postgresql://analyzer_ro:pw@postgres:5432/tradeai"
PLAN = {"levels": [[1.0, 0.5], [3.0, 0.4]], "be_after_first": True, "runner_frac": 0.1, "trail_mode": "atr", "trail_atr_mult": 3.0, "trail_wick_bars": 3, "tp_anchor": "signal"}


@pytest.fixture
def cfg(tmp_path: Path) -> Settings:
    return Settings(database_url=DSN, token=TOKEN, data_dir=tmp_path / "data", bootstrap_n=200, min_conclusions=50, min_group_trades=10)


def make_trades(n: int = 120, *, seed: int = 1, strategy: str = "SMC_Pro_Trend", coin: str = "BTCUSDT", start: pd.Timestamp = T0, step_h: float = 6.0,
                win_p: float = 0.45, plan: dict | None = None, meta_fn=None, prefix: str = "") -> list[dict]:
    """Список строк как из БД. Исход: победа +2R (MFE 2.4), стоп -1R (MAE 1.0), безубыток ~0 (MAE до 0.3), комиссия 0.05R."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        u = rng.random()
        if u < win_p:
            gross, mfe, mae = 2.0, 2.4, float(rng.uniform(0.0, 0.6))
        elif u < win_p + 0.35:
            gross, mfe, mae = -1.0, float(rng.uniform(0.0, 0.8)), 1.0
        else:
            gross, mfe, mae = 0.0, 1.1, float(rng.uniform(0.0, 0.3))
        fee = 0.05
        sig = start + pd.Timedelta(hours=step_h * i)
        entry = sig + pd.Timedelta(minutes=10)
        exit_ = entry + pd.Timedelta(hours=2)
        rows.append({"id": f"{prefix}{strategy}-{coin}-{i}", "strategy_id": strategy, "coin": coin, "side": "long" if i % 2 else "short", "kind": "limit", "mode": "shadow",
                     "signal_time": sig, "entry_time": entry, "exit_time": exit_, "pnl": (gross - fee) * 100.0, "fees": fee * 100.0, "r_multiple": gross - fee, "gross_r": gross,
                     "mfe_r": mfe, "mae_r": mae, "exit_reason": "x", "plan": plan if plan is not None else dict(PLAN),
                     "meta": meta_fn(i, rng) if meta_fn else {"interval": "5m", "entry": 100.0, "sl": 99.0}, "vol_ratio": float(rng.uniform(0.5, 3.0)),
                     "range_atr": float(rng.uniform(0.5, 2.5))})
    return rows


@pytest.fixture
def frame() -> pd.DataFrame:
    return build_frame(make_trades(120))
