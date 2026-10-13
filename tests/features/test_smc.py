"""Тесты features/smc.py: ручные паттерны (D2, D3, FVG, поджатие), тест обрезки R4, паритет окна R8, статический R2."""

import math
import random
import re
from pathlib import Path

import polars as pl
import pytest

from features.smc import SmcParams, add_features, feature_columns, validate_candles
from utils.errors import ConfigError, DataError

STEP = 900_000  # 15m
SMALL = SmcParams(k=2, atr_period=2, swing_atr_mult=0.0, impulse_atr_mult=1.5, eq_tolerance=0.001, volume_sma=2)


def candles(highs, lows=None, *, opens=None, closes=None, volume=10.0) -> pl.DataFrame:
    """Свечи из списков high/low; open/close по умолчанию - середина свечи."""
    lows = lows if lows is not None else [h - 1.0 for h in highs]
    mids = [(h + low) / 2 for h, low in zip(highs, lows)]
    return pl.DataFrame({
        "open_time": [i * STEP for i in range(len(highs))],
        "open": opens or mids, "high": highs, "low": lows, "close": closes or mids,
        "volume": [volume] * len(highs),
    })


def true_idx(out: pl.DataFrame, col: str) -> list[int]:
    return [i for i, v in enumerate(out[col].to_list()) if v]


def random_walk(n: int, seed: int, vol: float = 0.01) -> pl.DataFrame:
    rnd = random.Random(seed)
    rows, c = [], 100.0
    for i in range(n):
        o = c
        c = o * math.exp(rnd.gauss(0, vol))
        h = max(o, c) * (1 + abs(rnd.gauss(0, vol / 3)))
        low = min(o, c) * (1 - abs(rnd.gauss(0, vol / 3)))
        rows.append((i * STEP, o, h, low, c, rnd.uniform(10, 100)))
    return pl.DataFrame(rows, schema=["open_time", "open", "high", "low", "close", "volume"], orient="row")


# ----------------------------------------------------------------------------------- свинги (R3, D2, D3)


def test_swing_high_flag_appears_at_i_plus_k_not_earlier():
    highs = [10, 10, 10, 10, 11, 12, 15, 12, 11, 10, 10, 10, 10]
    out = add_features(candles(highs), SMALL, interval_ms=STEP)
    assert true_idx(out, "swing_high") == [8]  # пик на i = 6, k = 2 -> флаг на j = 8
    row = out.row(8, named=True)
    assert row["sh1_price"] == 15 and row["sh1_time"] == 6 * STEP
    assert out["sh1_price"][7] is None  # на j = 7 пика ещё «не видно»


def test_equal_highs_first_one_is_the_swing_d2():
    highs = [10, 10, 10, 10, 11, 12, 15, 12, 15, 12, 11, 10, 10, 10]
    out = add_features(candles(highs), SMALL, interval_ms=STEP)
    # i = 6: справа есть равный хай (>= выполняется), слева строго меньше -> свинг; i = 8: слева равный -> не свинг
    assert true_idx(out, "swing_high") == [8]
    assert out["sh1_time"][8] == 6 * STEP


def test_swing_low_mirrors_swing_high():
    lows = [20, 20, 20, 20, 19, 18, 15, 18, 19, 20, 20, 20, 20]
    out = add_features(candles([x + 1 for x in lows], lows), SMALL, interval_ms=STEP)
    assert true_idx(out, "swing_low") == [8]
    assert out["sl1_price"][8] == 15 and out["sl1_time"][8] == 6 * STEP


def test_swing_amplitude_filter_d3():
    highs = [10, 10, 10, 10, 11, 12, 15, 12, 11, 10, 10, 10, 10]
    strict = SmcParams(k=2, atr_period=2, swing_atr_mult=1000.0, volume_sma=2)
    assert true_idx(add_features(candles(highs), strict, interval_ms=STEP), "swing_high") == []


def test_swing_amplitude_boundary_uses_atr_before_candidate():
    # Пик i = 6 (15) с широкой свечой (low 10). Амплитуда = 15 - min(low окна [4, 8]) = 5. Порог = mult * ATR[5].
    highs = [12, 12, 12, 12, 13, 14, 15, 14, 13, 12, 12, 12, 12]
    lows = [h - 2 for h in highs]
    lows[6] = 10
    amplitude = 15 - min(lows[4:9])
    assert amplitude == 5
    base = add_features(candles(highs, lows), SmcParams(k=2, atr_period=2, swing_atr_mult=0.0, volume_sma=2), interval_ms=STEP)
    atr_before, atr_at_i = base["atr"][5], base["atr"][6]
    assert atr_before != atr_at_i  # иначе тест не отличает ATR[i-1] от ATR[i]
    ratio = amplitude / atr_before  # при mult = ratio порог равен амплитуде
    for mult, expected in ((ratio * 0.99, [8]), (ratio * 1.01, [])):
        p = SmcParams(k=2, atr_period=2, swing_atr_mult=mult, volume_sma=2)
        assert true_idx(add_features(candles(highs, lows), p, interval_ms=STEP), "swing_high") == expected, mult
    wrong_ratio = amplitude / atr_at_i  # если бы брался ATR[i], при mult между ratio и wrong_ratio результат был бы другим
    assert abs(wrong_ratio - ratio) / ratio > 0.02


# ------------------------------------------------------------------------------------------------- FVG


def fvg_frame(body_close: float) -> pl.DataFrame:
    """Шесть ровных свечей (диапазон 100..102) + свечи a (i-2), импульс b (i-1), c (i): медвежий разрыв 100 -> 96."""
    n = 6
    highs, lows = [102.0] * n, [100.0] * n
    opens, closes = [101.0] * n, [101.0] * n
    highs += [102.0, 101.5, 96.0]
    lows += [100.0, 94.5, 93.0]
    opens += [101.0, 101.0, 95.0]
    closes += [101.0, body_close, 94.0]
    return candles(highs, lows, opens=opens, closes=closes)


def test_bearish_fvg_zone_and_midpoint_d7():
    out = add_features(fvg_frame(95.0), SMALL, interval_ms=STEP)  # тело 6 > 1.5 * ATR[i-2] = 3
    assert true_idx(out, "fvg_bear") == [8]
    r = out.row(8, named=True)
    assert (r["fvg_bear_top"], r["fvg_bear_bottom"], r["fvg_bear_mid"]) == (100.0, 96.0, 98.0)
    assert r["fvg_bear_t1"] == 6 * STEP
    assert not any(out["fvg_bull"])


def test_fvg_requires_impulse_body():
    out = add_features(fvg_frame(99.5), SMALL, interval_ms=STEP)  # тело 1.5 <= 3 -> разрыв есть, импульса нет
    assert true_idx(out, "fvg_bear") == []


def test_bullish_fvg_is_mirror_of_bearish():
    bear = fvg_frame(95.0)
    mirror = bear.with_columns(
        high=400.0 - pl.col("low"), low=400.0 - pl.col("high"), open=400.0 - pl.col("open"), close=400.0 - pl.col("close")
    )
    out = add_features(mirror, SMALL, interval_ms=STEP)
    assert true_idx(out, "fvg_bull") == [8] and true_idx(out, "fvg_bear") == []
    r = out.row(8, named=True)
    assert (r["fvg_bull_top"], r["fvg_bull_bottom"], r["fvg_bull_mid"]) == (304.0, 300.0, 302.0)


# ----------------------------------------------------------------------------------------- поджатие


def zigzag(waypoints: list[tuple[int, float]], half_range: float = 0.01) -> pl.DataFrame:
    highs, lows, mids = [], [], []
    for (i0, p0), (i1, p1) in zip(waypoints, waypoints[1:]):
        for i in range(i0, i1):
            mid = p0 + (p1 - p0) * (i - i0) / (i1 - i0)
            mids.append(mid)
    mids.append(waypoints[-1][1])
    highs, lows = [m + half_range for m in mids], [m - half_range for m in mids]
    return candles(highs, lows)


BUILDUP = [(0, 95.0), (6, 100.0), (9, 90.0), (12, 100.05), (15, 92.0), (18, 100.02), (21, 94.0), (24, 97.0), (27, 98.0)]


def test_buildup_detected_when_third_low_is_confirmed():
    out = add_features(zigzag(BUILDUP), SMALL, interval_ms=STEP)
    assert true_idx(out, "swing_high") == [8, 14, 20]
    assert true_idx(out, "swing_low") == [11, 17, 23]
    assert not any(out["buildup"][:23])  # третий лоу подтверждён только на i + k = 23
    assert not any(out["eqh_ok"][:20]) and out["eqh_spread"][:20].null_count() == 20  # меньше трёх хаёв - не «равные»
    r = out.row(23, named=True)
    assert r["buildup"] and r["eqh_ok"] and r["hl_ok"]
    assert r["buildup_level"] == pytest.approx(100.06) and r["buildup_low3"] == pytest.approx(93.99)
    assert r["eqh_spread"] == pytest.approx((100.06 - 100.01) / 100.01)


def test_buildup_rejected_when_highs_not_equal():
    wp = [(i, 101.5 if i == 12 else p) for i, p in BUILDUP]  # средний хай выше на 1.5%
    out = add_features(zigzag(wp), SMALL, interval_ms=STEP)
    assert not any(out["buildup"]) and not any(out["eqh_ok"])


def test_buildup_rejected_when_lows_not_rising():
    wp = [(i, 91.0 if i == 21 else p) for i, p in BUILDUP]  # третий лоу ниже второго
    out = add_features(zigzag(wp), SMALL, interval_ms=STEP)
    assert not any(out["buildup"]) and not any(out["hl_ok"])


def test_volume_ratio_excludes_current_bar():
    df = candles([10.0] * 6).with_columns(volume=pl.Series([10.0, 10.0, 10.0, 10.0, 10.0, 50.0]))
    out = add_features(df, SMALL, interval_ms=STEP)  # volume_sma = 2: SMA по барам 3 и 4 (без текущего пятого)
    assert out["vol_sma_prev"][5] == 10.0 and out["vol_ratio"][5] == 5.0


# ------------------------------------------------------------------------------------ R4: тест обрезки


def assert_truncation_invariant(fn, df: pl.DataFrame, ts: list[int]) -> None:
    """Признаки по df[:t+1] обязаны совпасть со строкой t расчёта по всей истории (точное равенство)."""
    full = fn(df)
    for t in ts:
        cut = fn(df.head(t + 1)).row(t, named=True)
        whole = full.row(t, named=True)
        diff = {c: (cut[c], whole[c]) for c in whole if cut[c] != whole[c]}
        assert not diff, f"t={t}: обрезка и полная история расходятся: {diff}"


@pytest.mark.parametrize(
    ("params", "seed", "vol"),
    [
        (SmcParams(), 11, 0.01),  # боевые параметры
        (SmcParams(swing_atr_mult=0.5, eq_tolerance=0.02), 12, 0.01),  # мягкие: чаще свинги и поджатия
        (SmcParams(k=3, atr_period=5, swing_atr_mult=0.0, eq_tolerance=0.03, volume_sma=5), 13, 0.02),
    ],
)
def test_truncation_random_t_r4(params, seed, vol):
    df = random_walk(700, seed, vol)
    fn = lambda d: add_features(d, params, interval_ms=STEP)  # noqa: E731
    rnd = random.Random(seed)
    ts = sorted(rnd.sample(range(df.height), 300))  # сотни случайных t, включая прогрев
    assert_truncation_invariant(fn, df, ts)
    full = fn(df)  # тест не пустой: каждый вид признака реально встречается
    assert full["swing_high"].sum() > 5 and full["swing_low"].sum() > 5 and full["hl_ok"].sum() > 0
    assert full["fvg_bear"].sum() + full["fvg_bull"].sum() > 0


def test_truncation_harness_detects_lookahead():
    cheat = lambda d: d.with_columns(x=pl.col("high").shift(-1))  # noqa: E731
    with pytest.raises(AssertionError, match="расходятся"):
        assert_truncation_invariant(cheat, random_walk(50, 1), [10, 20, 30])


def test_truncation_covers_buildup_true_rows():
    df = zigzag(BUILDUP)
    fn = lambda d: add_features(d, SMALL, interval_ms=STEP)  # noqa: E731
    assert_truncation_invariant(fn, df, list(range(df.height)))  # все t подряд, в т.ч. 23, где buildup становится True


# ---------------------------------------------------------------------- R8: окно live = полная история


@pytest.mark.parametrize("window", [120, 300])
def test_live_window_matches_full_history_after_warmup(window):
    p = SmcParams(swing_atr_mult=0.5, eq_tolerance=0.02)
    df = random_walk(900, 21)
    full = add_features(df, p, interval_ms=STEP)
    cols = [c for c in feature_columns() if c != "vol_ratio"]
    checked = 0
    for t in range(window, df.height, 7):
        win = add_features(df.slice(t - window + 1, window), p, interval_ms=STEP).row(window - 1, named=True)
        ref = full.row(t, named=True)
        cutoff = df["open_time"][t - window + 1] + p.warmup_bars * STEP
        used = [win[f"{n}_time"] for n in ("sh1", "sh2", "sh3", "sl1", "sl2", "sl3")]
        if any(v is None for v in used) or min(used) < cutoff:
            continue  # какой-то из трёх свингов подтверждён внутри прогрева окна - там расхождение допустимо
        assert {c: win[c] for c in cols} == {c: ref[c] for c in cols}, f"t={t}"
        checked += 1
    assert checked > 10


# ------------------------------------------------------------------------------- R1/R2: контракты и код


def test_inputs_are_validated_fail_fast():
    ok = random_walk(30, 3)
    with pytest.raises(DataError, match="шагом"):
        validate_candles(ok.filter(pl.col("open_time") != 5 * STEP), STEP)  # пропуск свечи
    with pytest.raises(DataError, match="шагом"):
        validate_candles(ok.reverse(), STEP)
    with pytest.raises(DataError, match="Невозможная"):
        validate_candles(ok.with_columns(high=pl.col("low") - 1), STEP)
    with pytest.raises(DataError, match="null"):
        validate_candles(ok.with_columns(close=pl.when(pl.col("open_time") == 0).then(None).otherwise(pl.col("close"))), STEP)
    with pytest.raises(DataError, match="нет колонок"):
        validate_candles(ok.drop("volume"), STEP)
    with pytest.raises(DataError, match="несколько символов"):
        validate_candles(ok.with_columns(symbol=pl.when(pl.col("open_time") < 5 * STEP).then(pl.lit("A")).otherwise(pl.lit("B"))), STEP)
    with pytest.raises(ConfigError):
        SmcParams(k=0)


def test_input_columns_are_preserved_and_output_schema_is_stable():
    df = random_walk(60, 4).with_columns(symbol=pl.lit("BTCUSDT"))
    out = add_features(df, interval_ms=STEP)
    assert out.columns == [*df.columns, *feature_columns()]
    assert out.height == df.height and out.drop(feature_columns()).equals(df)


def test_no_row_iteration_in_feature_module_r2():
    src = Path(__file__).resolve().parents[2].joinpath("features", "smc.py").read_text(encoding="utf-8")
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    banned = r"\.apply\(|iter_rows|map_elements|map_rows|map_batches|rolling_map|\.rows\(|\.iter_|to_dicts"
    assert re.search(banned, code) is None, re.findall(banned, code)


def test_defaults_match_approved_parameters():
    from features import defaults as d

    p = SmcParams()
    assert (p.k, p.atr_period, p.swing_atr_mult, p.eq_tolerance, p.volume_sma) == (5, 14, 1.5, 0.001, 20)
    assert (d.RISK_PER_TRADE, d.SL_BUFFER_ATR, d.TP_R, d.BE_TRIGGER_R) == (0.01, 0.2, 3.0, 1.0)
    assert (d.SETUP_TTL_BARS, d.LIMIT_TTL_BARS, d.FVG_ENTRY_FRACTION) == (30, 10, 0.5)  # TTL 15 -> 30 по О12
    assert (d.IMPULSE_ATR_MULT, d.MAX_EXPOSURE) == (1.0, 5.0) and p.impulse_atr_mult == 1.0  # О12
