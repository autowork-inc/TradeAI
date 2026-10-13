"""ProvenanceGuard и сквозная проверка: aligner -> FeatureBuilder -> guard."""

import polars as pl
import pytest

from pipeline.features.base import Feature, FeatureBuilder, apply_features, compile_features, source_columns
from pipeline.guards.provenance import LookaheadViolation, ProvenanceGuard
from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import WatermarkClock
from pipeline.types import TimingConfig

MIN = 60_000


def good_frame() -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": ["A", "A", "A", "B", "B"],
        "asof_ts": [1_000, 2_000, 3_000, 1_500, 2_500],
        "src_max_ts": [900, 1_990, 2_999, 1_499, 2_400],
        "h1_avail_ts": [None, 500, 500, None, 100],
    })


def test_valid_frame_passes_and_reports():
    report = ProvenanceGuard(extra_ts_cols=["h1_avail_ts"]).check(good_frame())
    assert report.n_rows == 5 and report.n_symbols == 2
    assert report.checked_columns == ("src_max_ts", "h1_avail_ts")  # null допустим: источник не присоединён


@pytest.mark.parametrize("bad_src", [3_000, 3_001, 10**12])
def test_src_not_strictly_before_asof_is_a_violation(bad_src):
    frame = good_frame().with_columns(
        src_max_ts=pl.when(pl.col("asof_ts") == 3_000).then(bad_src).otherwise(pl.col("src_max_ts"))
    )
    with pytest.raises(LookaheadViolation, match="src_max_ts >= asof_ts"):
        ProvenanceGuard().check(frame)  # равенство тоже нарушение: строгое «до»


def test_null_provenance_is_a_violation():
    frame = good_frame().with_columns(src_max_ts=pl.when(pl.col("asof_ts") == 2_000).then(None).otherwise(pl.col("src_max_ts")))
    with pytest.raises(LookaheadViolation, match="происхождение неизвестно"):
        ProvenanceGuard().check(frame)


def test_extra_ts_column_is_checked():
    frame = good_frame().with_columns(h1_avail_ts=pl.when(pl.col("asof_ts") == 2_500).then(2_500).otherwise(pl.col("h1_avail_ts")))
    ProvenanceGuard().check(frame)  # без extra - не проверяется
    with pytest.raises(LookaheadViolation, match="h1_avail_ts >= asof_ts"):
        ProvenanceGuard(extra_ts_cols=["h1_avail_ts"]).check(frame)


def test_non_increasing_or_duplicate_asof_is_a_violation():
    dup = good_frame().with_columns(
        asof_ts=pl.when(pl.col("asof_ts") == 2_000).then(1_000).otherwise(pl.col("asof_ts")),
        src_max_ts=pl.lit(0),  # чтобы сработала именно проверка сетки, а не происхождения
    )
    with pytest.raises(LookaheadViolation, match="не растёт строго"):
        ProvenanceGuard().check(dup)
    unsorted = good_frame().reverse()
    with pytest.raises(LookaheadViolation, match="не растёт строго"):
        ProvenanceGuard().check(unsorted)


def test_missing_provenance_columns():
    with pytest.raises(LookaheadViolation, match="нет колонок"):
        ProvenanceGuard().check(good_frame().drop("src_max_ts"))


# --------------------------------------------------------------- сквозная проверка


class Momentum(Feature):
    name = "momentum"

    def build(self, fb: FeatureBuilder):
        close = fb.col("close")
        return {
            "ret_1": close.pct_change(1),
            "vol_5": close.pct_change(1).rolling_std(5),
            "px_vs_mean": fb.col("px_last") / fb.col("px_last").rolling_mean(3) - 1.0,
        }


def test_pipeline_output_passes_guard_and_features_use_only_known_data():
    timing = TimingConfig()
    n, e_end = 12, 12 * MIN + 5_000
    book = pl.DataFrame(
        [
            {"symbol": "A", "recv_ts": e + 37, "E": e, "u": e // 500, "px": 100.0 + e / MIN}
            for e in range(0, e_end, 500)
        ],
        schema={"symbol": pl.String, "recv_ts": pl.Int64, "E": pl.Int64, "u": pl.Int64, "px": pl.Float64},
    ).lazy()
    candles = pl.DataFrame({
        "symbol": ["A"] * n,
        "open_time": [i * MIN for i in range(n)],
        "close": [100.0 + i for i in range(n)],
    }).lazy()

    clock, aligner = WatermarkClock(timing), TimelineAligner(timing)
    avail = clock.candle_availability(candles, MIN, clock.watermark(book))
    d = aligner.attach_book(aligner.decisions(avail, MIN), book, [pl.col("px").last().alias("px_last")])
    compiled = compile_features([Momentum()], source_columns(d))
    out = apply_features(d, compiled).collect()

    report = ProvenanceGuard().check(out)
    assert report.n_rows == out.height
    # Признак свечи k не может зависеть от свечи k+1: ret_1[k] = close[k]/close[k-1] - 1.
    closes = out["close"].to_list()
    assert out["ret_1"].to_list()[1:] == pytest.approx([closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))])
    assert out["ret_1"][0] is None
