"""Паритет разметки клиента (client/tradeai_client/chr_overlay.py) с настоящими детекторами CHR: точки касаний и поджатия, которые клиент восстанавливает
по свечам снапшота, обязаны совпасть с тем, что записал бот (meta.touches, meta.squeeze_points). Сценарии - те же, что в test_chr_strategies.py."""

import sys
from pathlib import Path

import pytest

from orchestrator.multi_strategies import HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR, StrategyHorizontalBreakoutChr, StrategyTrendlineBreakoutChr
from orchestrator.types import FeatureContext
from tests.strategies.test_chr_strategies import J, STEP, horizontal, mirror, trendline

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "client"))
from tradeai_client import chr_overlay  # noqa: E402


def snapshot(df, signal, strategy):
    candles = [{"open_time": r["open_time"], "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"], "volume": 0.0, "atr": r["atr"]}
               for r in df.iter_rows(named=True)]
    side = signal.direction.value if hasattr(signal.direction, "value") else str(signal.direction)
    return {"trade": {"strategy_id": strategy, "side": side, "coin": "BTCUSDT"}, "candles": candles, "geometry": list(signal.geometry), "meta": dict(signal.meta)}


def run(strategy_cls, df):
    (sig,) = strategy_cls().detect(FeatureContext("BTCUSDT", "15m", STEP, df, J * STEP + STEP + 500))
    return sig


@pytest.mark.parametrize("flip", [False, True])
def test_horizontal_overlay_matches_the_detector(flip):
    df = horizontal(n=J + 1)
    df = mirror(df) if flip else df
    sig = run(StrategyHorizontalBreakoutChr, df)
    out = chr_overlay.build_overlay(snapshot(df, sig, HORIZONTAL_BREAKOUT_CHR))
    assert out is not None and out["verified"] is True, out and out["notes"]
    touches = [i for i in out["items"] if i["kind"] == "point" and i["label"].startswith("Касание")]
    squeeze = [i for i in out["items"] if i["kind"] == "point" and i["label"].startswith("Поджатие")]
    assert [t["t"] for t in touches] == [34 * STEP, 44 * STEP] and len(squeeze) == 3
    assert [s["p"] for s in squeeze] == sig.meta["squeeze_points"]
    assert any(i["kind"] == "point" and i["label"] == "Пробой" for i in out["items"])


@pytest.mark.parametrize("flip", [False, True])
def test_trendline_overlay_matches_the_detector(flip):
    df = trendline(n=J + 1)
    df = mirror(df) if flip else df
    sig = run(StrategyTrendlineBreakoutChr, df)
    out = chr_overlay.build_overlay(snapshot(df, sig, TRENDLINE_BREAKOUT_CHR))
    assert out is not None and out["verified"] is True, out and out["notes"]
    touches = [i for i in out["items"] if i["kind"] == "point" and i["label"].startswith("Касание")]
    assert len(touches) == sig.meta["touches"] == 3
    assert [i["label"] for i in out["items"] if i["kind"] == "point" and i["label"] == "2-я опора"] == ["2-я опора"]


def test_overlay_flags_a_mismatch_instead_of_hiding_it():
    df = horizontal(n=J + 1)
    sig = run(StrategyHorizontalBreakoutChr, df)
    snap = snapshot(df, sig, HORIZONTAL_BREAKOUT_CHR)
    snap["meta"]["touches"] = 5
    out = chr_overlay.build_overlay(snap)
    assert out["verified"] is False and any("НЕ совпали" in n for n in out["notes"])
    snap["trade"]["strategy_id"] = "SMC_Pro_Trend"
    assert chr_overlay.build_overlay(snap) is None  # чужие стратегии не размечаются
