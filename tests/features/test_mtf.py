"""features/mtf.py: таблицы 1h (свипы пула ликвидности) и 1d (bias), присоединение к 5m без заглядывания в будущее (R3/R4), зеркальность лонг/шорт, R2."""

import random
import re
from pathlib import Path

import polars as pl
import pytest

from features.htf import BIAS_LABELS
from features.mtf import (D1_AVAIL, D1_BIAS, H1_AVAIL, H1_OPEN, attach_mtf, attach_table, d1_bias_table, h1_columns, h1_sweep_columns, h1_table,
                          mirrored_columns, new_h1_event, sweep_raw)
from features.smc import SmcParams, add_features
from tests.features.test_smc import random_walk
from utils.errors import DataError

M5, H1, D1 = 300_000, 3_600_000, 86_400_000
BASE = 1785715200000  # 2026-08-03 00:00 UTC: кратно суткам
FP = SmcParams(swing_atr_mult=0.5)


def walk5(n: int, seed: int, vol: float = 0.012) -> pl.DataFrame:
    return random_walk(n, seed, vol).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * M5 + BASE).alias("open_time"))


def resample(df5: pl.DataFrame, target: int) -> pl.DataFrame:
    """M5 -> target (UTC-сетка); неполная последняя свеча отбрасывается (старшая свеча ещё формируется)."""
    per = target // M5
    g = df5.group_by((pl.col("open_time") - pl.col("open_time") % target).alias("t"), maintain_order=True).agg(
        pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(), pl.col("volume").sum(), pl.len().alias("n"))
    return g.filter(pl.col("n") == per).select(pl.col("t").alias("open_time"), "open", "high", "low", "close", "volume")


def feature_frames(df5: pl.DataFrame):
    f5 = add_features(df5, FP, interval_ms=M5)
    h1 = add_features(resample(df5, H1), FP, interval_ms=H1)
    return f5, h1


def test_h1_table_has_the_documented_columns_and_availability_is_the_close_time():
    _, h1 = feature_frames(walk5(1200, 3))
    t = h1_table(h1, H1)
    assert set(t.columns) == set(h1_columns())
    assert t[H1_AVAIL].to_list() == [x + H1 for x in h1["open_time"].to_list()] and t[H1_OPEN].to_list() == h1["open_time"].to_list()
    assert t["1h_sh1_price"].to_list() == h1["sh1_price"].to_list() and t["1h_high"].to_list() == h1["high"].to_list()  # forward-fill значения 1h-свечи


def test_h1_sweep_is_the_same_definition_as_the_single_timeframe_sweep_r8():
    """Свип 1h = `sweep_raw` на 1h-кадре, т.е. то же выражение, что и у одно-таймфреймового автомата: сверяем с автоматом, запущенным прямо на 1h-кадре."""
    from strategies.smc_setups import SmcSetupParams, detect_smc

    _, h1 = feature_frames(walk5(6000, 11))
    t = h1_table(h1, H1)
    det = detect_smc(h1, SmcSetupParams(max_exposure=1e9))
    for side in ("short", "long"):
        names = h1_sweep_columns(side)
        assert t[names[0]].to_list() == det[f"{side}_sweep"].to_list()
        assert t[names[0]].sum() > 3  # тест не пустой
        assert t[names[1]].filter(t[names[0]]).to_list() == det[f"{side}_pool_level"].filter(det[f"{side}_sweep"]).to_list()
        assert t[names[3]].filter(t[names[0]]).to_list() == det[f"{side}_sweep_extreme"].filter(det[f"{side}_sweep"]).to_list()  # на баре свипа экстремум = хай/лоу этой свечи
        assert t[names[1]].null_count() == t.height - t[names[0]].sum()  # вне свипа значения null


def test_attach_gives_each_5m_row_only_the_1h_candle_closed_not_later_than_its_open():
    f5, h1 = feature_frames(walk5(3000, 5))
    out = attach_mtf(f5, h1_table(h1, H1), d1_bias_table(_fake_d1(f5), D1))
    assert out.height == f5.height and out.columns[: f5.width] == f5.columns
    avail = out[H1_AVAIL].to_list()
    opens = out["open_time"].to_list()
    assert all(a is None or a <= o for a, o in zip(avail, opens))  # никогда не видим свечу, закрывающуюся позже открытия 5m-строки
    assert all(o - a < H1 for a, o in zip(avail, opens) if a is not None)  # и не отстаём больше часа
    first_hour_end = h1["open_time"][0] + H1
    assert all(a is None for a, o in zip(avail, opens) if o < first_hour_end) and all(a is not None for a, o in zip(avail, opens) if o >= first_hour_end)


def _fake_d1(f5: pl.DataFrame) -> pl.DataFrame:
    """Кадр 1d с признаками из того же блуждания (достаточно, чтобы построить таблицу bias; смысл bias тут не проверяется)."""
    n = 40
    d = random_walk(n, 2, 0.02).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * D1 + BASE - 30 * D1).alias("open_time"))
    return add_features(d, FP, interval_ms=D1)


def test_event_flag_is_true_exactly_on_the_first_5m_row_after_each_1h_close():
    f5, h1 = feature_frames(walk5(1200, 9))
    out = attach_table(f5, h1_table(h1, H1), H1_AVAIL).with_columns(new_h1_event().alias("ev"))
    ev_rows = out.filter(pl.col("ev"))
    assert ev_rows["open_time"].to_list() == [x + H1 for x in h1["open_time"].to_list() if x + H1 <= f5["open_time"][-1]]  # по одному событию на закрытую 1h-свечу
    assert out["ev"].sum() == ev_rows.height and out.filter(pl.col(H1_AVAIL).is_null())["ev"].sum() == 0


def test_attached_columns_are_truncation_invariant_r4():
    df5 = walk5(3600, 21)
    f5, h1 = feature_frames(df5)
    full = attach_table(f5, h1_table(h1, H1), H1_AVAIL)
    rnd = random.Random(4)
    for t in sorted(rnd.sample(range(300, 3600), 25)):
        cut5, cut_h1 = feature_frames(df5.head(t + 1))  # вход, обрезанный на 5m-свече t (недозакрытый час отброшен resample'ом)
        got = attach_table(cut5, h1_table(cut_h1, H1), H1_AVAIL).row(t, named=True)
        want = full.row(t, named=True)
        diff = {c: (got[c], want[c]) for c in want if c.startswith("1h_") and got[c] != want[c]}
        assert diff == {}, f"t={t}: {diff}"


def test_d1_bias_table_uses_its_own_column_names_and_the_h4_definition():
    d1 = add_features(random_walk(60, 7, 0.02).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * D1 + BASE).alias("open_time")), FP, interval_ms=D1)
    t = d1_bias_table(d1, D1)
    assert t.columns == [D1_AVAIL, D1_BIAS] and t[D1_AVAIL].to_list() == [x + D1 for x in d1["open_time"].to_list()]
    assert set(t[D1_BIAS].drop_nulls().to_list()) <= set(BIAS_LABELS)  # +1 / 0 / -1


def test_attach_refuses_clashes_and_missing_inputs():
    f5, h1 = feature_frames(walk5(1200, 3))
    t = h1_table(h1, H1)
    with pytest.raises(DataError, match="уже есть"):
        attach_table(attach_table(f5, t, H1_AVAIL), t, H1_AVAIL)
    with pytest.raises(DataError, match="нет колонок"):
        h1_table(h1.drop("sh1_price"), H1)
    with pytest.raises(DataError, match="пустой"):
        h1_table(h1.head(0), H1)
    with pytest.raises(DataError, match="сторона"):
        mirrored_columns("up")


def test_sweep_raw_marks_close_back_below_an_unbroken_pool_only():
    w = pl.DataFrame({"h": [10.0, 12.0, 13.0, 12.0], "c": [9.0, 11.0, 9.5, 9.0], "grp": [1, 1, 1, 1], "L": [None, 10.0, 10.0, 10.0]}, schema={
        "h": pl.Float64, "c": pl.Float64, "grp": pl.Int64, "L": pl.Float64})
    out = sweep_raw(w)
    assert out["sweep_raw"].to_list() == [False, False, False, False]  # бар 1 закрылся выше пула -> пул пробит, последующие «свипы» не считаются
    w2 = w.with_columns(pl.Series("c", [9.0, 9.5, 9.5, 9.0]))
    assert sweep_raw(w2)["sweep_raw"].to_list() == [False, True, False, False]  # первый high > пула с возвратом под него - свип; повторный - уже нет


def test_no_row_iteration_in_mtf_module_r2():
    src = (Path(__file__).resolve().parents[2] / "features" / "mtf.py").read_text(encoding="utf-8")
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    assert not re.search(r"\.apply\(|iter_rows|map_elements|\.map_rows\(|for\s+\w+\s+in\s+.*\.rows\(", code)
