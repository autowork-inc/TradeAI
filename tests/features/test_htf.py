"""HTF Bias (features/htf.py, orchestrator/htf.py): определение тренда H4, привязка к M15 без будущего (R3/R4), фильтр автомата."""

import polars as pl
import pytest

from features.htf import (BEARISH, BIAS_COLUMN, BIAS_LABELS, BULLISH, NEUTRAL, attach_htf_bias, htf_bias_table, side_allowed)
from features.smc import SmcParams, add_features
from tests.features.test_smc import random_walk
from utils.errors import DataError

H4 = 4 * 3_600_000
M15 = 900_000


def h4_rows(*rows: tuple) -> pl.DataFrame:
    """(sh1, sh2, sl1, sl2[, atr]) -> кадр с нужными колонками; open_time = i * H4."""
    data = {"open_time": [i * H4 for i in range(len(rows))], "atr": [r[4] if len(r) > 4 else 1.0 for r in rows]}
    for j, name in enumerate(("sh1_price", "sh2_price", "sl1_price", "sl2_price")):
        data[name] = [r[j] for r in rows]
    return pl.DataFrame(data, schema={"open_time": pl.Int64, "atr": pl.Float64, **{n: pl.Float64 for n in ("sh1_price", "sh2_price", "sl1_price", "sl2_price")}})


def bias_of(*row) -> int | None:
    return htf_bias_table(h4_rows(row), H4)[BIAS_COLUMN][0]


def test_structure_rules():
    assert bias_of(110, 105, 100, 95) == BULLISH  # HH + HL
    assert bias_of(100, 105, 90, 95) == BEARISH  # LH + LL
    assert bias_of(110, 105, 90, 95) == NEUTRAL  # HH + LL: расширение, не тренд
    assert bias_of(100, 105, 100, 95) == NEUTRAL  # LH + HL: сужение
    assert bias_of(105, 105, 100, 95) == NEUTRAL  # равные хаи - не HH
    assert bias_of(None, None, None, None) == NEUTRAL  # свингов ещё нет: смешанная структура, фильтр не запрещает
    assert bias_of(110, 105, None, None) == NEUTRAL  # лоу меньше двух
    assert bias_of(110, 105, 100, 95, None) is None  # у свечи нет ATR (прогрев) -> bias неизвестен


def test_labels_cover_all_biases():
    assert BIAS_LABELS == {1: "Bullish", -1: "Bearish", 0: "Neutral"}


def test_side_allowed_truth_table():
    df = pl.DataFrame({BIAS_COLUMN: [1, 0, -1, None]}, schema={BIAS_COLUMN: pl.Int8})
    out = df.select(short=side_allowed("short"), long=side_allowed("long"))
    assert out["short"].to_list() == [False, False, True, False]  # шорт только при Bearish; Neutral и нет bias - запрет
    assert out["long"].to_list() == [True, False, False, False]
    with pytest.raises(DataError):
        df.select(side_allowed("flat"))


def test_bias_table_validates_input():
    with pytest.raises(DataError, match="нет колонок"):
        htf_bias_table(pl.DataFrame({"open_time": [0]}), H4)
    with pytest.raises(DataError, match="пустой"):
        htf_bias_table(h4_rows((1, 1, 1, 1)).clear(), H4)


# --------------------------------------------------------------------------- привязка к M15: только закрытые H4-свечи


def table_two_bars() -> pl.DataFrame:
    # H4 #0 (open 0) закрывается в H4 -> Bullish; H4 #1 (open H4) закрывается в 2*H4 -> Bearish
    return htf_bias_table(h4_rows((110, 105, 100, 95), (100, 105, 90, 95)), H4)


def m15_at(*open_times: int) -> pl.DataFrame:
    return pl.DataFrame({"open_time": list(open_times)}, schema={"open_time": pl.Int64})


def test_attach_uses_only_closed_h4_bars_and_boundary_is_exact():
    t = table_two_bars()
    out = attach_htf_bias(m15_at(0, H4 - M15, H4, 2 * H4 - M15, 2 * H4, 3 * H4), t)
    assert out[BIAS_COLUMN].to_list() == [None, None, BULLISH, BULLISH, BEARISH, BEARISH]
    # M15 с open_time ровно в момент закрытия H4 уже видит её bias (запас 15 минут до события закрытия M15-свечи);
    # свеча, открытая за 15 минут до закрытия H4, - ещё нет (там H4 формируется)


def test_attach_rejects_clash():
    t = table_two_bars()
    with pytest.raises(DataError, match="уже есть"):
        attach_htf_bias(attach_htf_bias(m15_at(H4), t), t)


def test_bias_has_no_lookahead_truncation_invariant():
    """R4 для HTF: bias по H4-истории до момента t совпадает с bias по всей истории (точное равенство, много точек t)."""
    h4 = random_walk(520, seed=11, vol=0.012).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * H4).alias("open_time"))
    feats = lambda df: add_features(df, SmcParams(), interval_ms=H4)  # noqa: E731
    full = htf_bias_table(feats(h4), H4)
    changed = 0
    for t in range(60, 520, 7):
        cut = htf_bias_table(feats(h4.head(t + 1)), H4)
        assert cut[BIAS_COLUMN][-1] == full[BIAS_COLUMN][t], f"t={t}"
        changed += int(full[BIAS_COLUMN][t] != full[BIAS_COLUMN][t - 1])
    assert changed > 0, "тест бессмыслен, если bias ни разу не менялся"
    assert {BULLISH, BEARISH}.issubset(set(full[BIAS_COLUMN].drop_nulls().to_list()))  # случайное блуждание даёт оба тренда


def test_m15_window_parity_live_vs_whole_history():
    """R8: M15-кадр с bias, посчитанный по короткому окну H4 (live: 300 баров), даёт то же, что по длинной истории, после прогрева."""
    h4 = random_walk(700, seed=5, vol=0.012).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * H4).alias("open_time"))
    feats = lambda df: add_features(df, SmcParams(), interval_ms=H4)  # noqa: E731
    long_t, short_t = htf_bias_table(feats(h4), H4), htf_bias_table(feats(h4.tail(300)), H4)
    probe = m15_at(*[(500 * 16 + i) * M15 for i in range(0, 16 * 200, 13)])  # M15-свечи внутри последних 200 H4
    assert attach_htf_bias(probe, long_t)[BIAS_COLUMN].to_list() == attach_htf_bias(probe, short_t)[BIAS_COLUMN].to_list()
