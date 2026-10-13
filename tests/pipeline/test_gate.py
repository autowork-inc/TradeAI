"""DataQualityGate: каузальный фильтр выбросов E, шаги локальных часов, карантин."""

import random

import polars as pl
import pytest

from pipeline.quality.gate import REASON_OUTLIER, REASON_REGRESS, DataQualityGate, GateConfig
from pipeline.sync.watermark import AVAIL_WATERMARK, WatermarkClock
from pipeline.types import TimingConfig
from utils.errors import ConfigError

STEP = 500
THETA = 30  # d = recv_ts - E в «спокойном» режиме
SCHEMA = {"symbol": pl.String, "u": pl.Int64, "E": pl.Int64, "recv_ts": pl.Int64}


def stream(symbol="A", n=300, u0=1_000, e0=1_000_000):
    return [
        {"symbol": symbol, "u": u0 + i, "E": e0 + i * STEP, "recv_ts": e0 + i * STEP + THETA}
        for i in range(n)
    ]


def lf(*chunks) -> pl.LazyFrame:
    return pl.DataFrame([r for chunk in chunks for r in chunk], schema=SCHEMA).lazy()


def gate(**cfg) -> DataQualityGate:
    return DataQualityGate(TimingConfig(), GateConfig(**cfg))


def reasons(result_frame: pl.DataFrame) -> dict[int, str]:
    return dict(zip(result_frame["u"].to_list(), result_frame["gate_reason"].to_list()))


# ------------------------------------------------------------------ чистый поток


def test_clean_stream_passes_untouched():
    res = gate().apply(lf(stream()))
    clean, quarantine, steps = res.clean.collect(), res.quarantine.collect(), res.steps.collect()
    assert clean.height == 300 and quarantine.height == 0 and steps.height == 0
    assert clean["clock_epoch"].unique().to_list() == [0]
    # Первые baseline_min_samples сообщений не оцениваются; дальше baseline = обычное d.
    assert clean["clock_baseline_ms"].is_null().sum() == GateConfig().baseline_min_samples
    assert clean["clock_baseline_ms"].drop_nulls().unique().to_list() == [float(THETA)]
    assert clean["recv_ts"].is_sorted()
    assert {"clock_d_ms", "clock_baseline_ms", "clock_epoch"} <= set(clean.columns)


def test_jitter_below_threshold_is_not_a_problem():
    rng = random.Random(1)
    rows = stream()
    for r in rows:
        r["recv_ts"] += rng.randint(0, 300)  # задержка 0..300 мс
    res = gate().apply(lf(rows))
    assert res.quarantine.collect().height == 0 and res.steps.collect().height == 0


# ------------------------------------------------------------------ выбросы E


def test_isolated_future_spike_goes_to_quarantine_not_silently_dropped():
    rows = stream()
    rows[100]["E"] += 3_600_000  # E из будущего на час
    res = gate().apply(lf(rows))
    q = res.quarantine.collect()
    assert q["u"].to_list() == [1_100] and q["gate_reason"].to_list() == [REASON_OUTLIER]
    assert res.clean.collect().height == 299
    assert q["clock_d_ms"].to_list() == [THETA - 3_600_000]  # диагностика сохранена


def test_isolated_past_spike_and_short_bursts_are_outliers():
    rows = stream()
    rows[100]["E"] -= 900_000  # E из прошлого
    rows[150]["E"] += 60_000
    rows[151]["E"] += 60_000  # серия из двух
    for k, i in enumerate(range(200, 212)):  # серия 12 подряд, но d разбросан сильно
        rows[i]["E"] += 20_000 * (k + 1)
    q = gate().apply(lf(rows)).quarantine.collect()
    assert set(q["u"].to_list()) == {1_100, 1_150, 1_151, *range(1_200, 1_212)}
    assert set(q["gate_reason"].to_list()) == {REASON_OUTLIER}
    assert gate().apply(lf(rows)).steps.collect().height == 0  # это не шаг часов: разброс d велик


def test_deviation_within_tolerance_is_accepted():
    rows = stream()
    rows[100]["E"] -= 3_000  # E на 3 с в прошлом: выброс по меркам джиттера, но < tolerance 5 с
    assert gate().apply(lf(rows)).quarantine.collect().height == 0


def test_poisoned_E_would_break_watermark_but_gate_protects_it():
    rows = stream(e0=0, n=270)
    # Самый опасный случай - E чуть дальше конца свечи (c=60_000): E=61_000 вместо 50_000.
    # Огромный спайк WatermarkClock отсёк бы сам (состояние gap), этот выглядит как «плотное»
    # доказательство закрытия.
    rows[100]["E"] += 11_000
    book = lf(rows)
    candles = pl.DataFrame({"symbol": ["A"], "open_time": [0]}, schema={"symbol": pl.String, "open_time": pl.Int64}).lazy()
    clock = WatermarkClock(TimingConfig())

    raw = clock.candle_availability(candles, 60_000, clock.watermark(book)).collect().row(0, named=True)
    assert raw["avail_ts"] < raw["c"]  # «узнали» о закрытии свечи ДО её закрытия - lookahead

    clean = gate().apply(book).clean
    good = clock.candle_availability(candles, 60_000, clock.watermark(clean)).collect().row(0, named=True)
    assert good["avail_source"] == AVAIL_WATERMARK
    assert good["avail_ts"] == 60_000 + THETA + TimingConfig().candle_publish_lag_ms


# ----------------------------------------------------------- шаги локальных часов


def test_forward_clock_step_is_detected_and_starts_new_epoch():
    rows = stream()
    for r in rows[200:]:
        r["recv_ts"] += 10_000  # локальные часы прыгнули на 10 с вперёд
    res = gate().apply(lf(rows))
    q, steps, clean = res.quarantine.collect(), res.steps.collect(), res.clean.collect()
    # До подтверждения серии (step_min_run=10) сообщения |dev| > tolerance - в карантине.
    assert q["u"].to_list() == list(range(1_200, 1_209)) and set(q["gate_reason"]) == {REASON_OUTLIER}
    assert steps.height == 1
    s = steps.row(0, named=True)
    assert (s["u"], s["kind"], s["delta_ms"], s["clock_epoch"]) == (1_209, "forward", 10_000, 1)
    epochs = dict(zip(clean["u"].to_list(), clean["clock_epoch"].to_list()))
    assert {epochs[u] for u in range(1_000, 1_200)} == {0}
    assert {epochs[u] for u in range(1_209, 1_300)} == {1}  # после шага всё принимается


def test_backward_clock_step_quarantines_overlap_and_keeps_recv_monotone():
    rows = stream()
    for r in rows[200:]:
        r["recv_ts"] -= 3_000  # часы отскочили на 3 с назад (NTP step)
    res = gate().apply(lf(rows))
    q, steps, clean = res.quarantine.collect(), res.steps.collect(), res.clean.collect()
    # Сообщения, чей recv_ts ещё меньше уже виденного максимума: 1_200..1_204.
    assert q["u"].to_list() == list(range(1_200, 1_205)) and set(q["gate_reason"]) == {REASON_REGRESS}
    s = steps.row(0, named=True)
    assert (s["kind"], s["delta_ms"], s["clock_epoch"]) == ("backward", -3_000, 1)
    # Принятое можно безопасно упорядочивать по recv_ts: порядок recv_ts совпадает с порядком u.
    assert clean["recv_ts"].is_sorted() and clean["u"].is_sorted()


def test_small_step_below_threshold_is_not_a_step():
    rows = stream()
    for r in rows[200:]:
        r["recv_ts"] += 300  # < step_threshold_ms: укладывается в консервативный запас lag
    res = gate().apply(lf(rows))
    assert res.steps.collect().height == 0 and res.quarantine.collect().height == 0


def test_step_in_one_symbol_does_not_touch_another():
    a, b = stream("A", u0=1_000), stream("B", u0=50_000)
    for r in a[200:]:
        r["recv_ts"] += 10_000
    res = gate().apply(lf(a, b))
    q, steps, clean = res.quarantine.collect(), res.steps.collect(), res.clean.collect()
    assert set(q["symbol"]) == {"A"} and steps["symbol"].to_list() == ["A"]
    assert clean.filter(pl.col("symbol") == "B")["clock_epoch"].unique().to_list() == [0]
    assert clean.filter(pl.col("symbol") == "B").height == 300


# ----------------------------------------- каузальность самой очистки (свойство)


def _messy_stream(seed: int) -> list[dict]:
    rng = random.Random(seed)
    rows = stream(n=420)
    for r in rows:
        r["recv_ts"] += rng.randint(0, 200)
    for i in rng.sample(range(30, 400), 6):
        rows[i]["E"] += rng.choice([-700_000, 900_000, 40_000])
    f_at = rng.randint(120, 200)
    for r in rows[f_at:]:
        r["recv_ts"] += 8_000
    b_at = rng.randint(260, 330)
    for r in rows[b_at:]:
        r["recv_ts"] -= 4_000
    return rows


@pytest.mark.parametrize("seed", range(6))
def test_classification_is_prefix_invariant(seed):
    """Решение по сообщению i не зависит от сообщений > i: классификация префикса
    совпадает с началом классификации полного набора."""
    rows = _messy_stream(seed)
    g = gate()
    cols = ["u", "gate_reason", "clock_epoch", "clock_baseline_ms", "recv_regress"]
    full = g.classify(lf(rows)).select(cols).collect()
    for n in (35, 90, 151, 222, 300, 419):
        part = g.classify(lf(rows[:n])).select(cols).collect()
        assert part.equals(full.head(n)), f"seed={seed}, префикс {n}"


def test_accepted_stream_is_always_monotone_in_recv():
    for seed in range(6):
        clean = gate().apply(lf(_messy_stream(seed))).clean.collect()
        assert clean["recv_ts"].is_sorted()
        assert clean["u"].is_sorted()


# --------------------------------------------------------------- отчёт и настройки


def test_clock_report_summarises_per_symbol():
    rows = stream()
    rows[100]["E"] += 3_600_000
    for r in rows[200:]:
        r["recv_ts"] -= 3_000
    rep = gate().clock_report(lf(rows, stream("B", u0=9_000)))
    a = rep.filter(pl.col("symbol") == "A").row(0, named=True)
    assert (a["n"], a["n_outliers"], a["n_regress"], a["n_steps"]) == (300, 1, 5, 1)
    b = rep.filter(pl.col("symbol") == "B").row(0, named=True)
    assert (b["n_outliers"], b["n_regress"], b["n_steps"], b["d_p50"]) == (0, 0, 0, THETA)
    assert b["share_negative"] == 0


def test_gate_config_validation():
    for bad in ({"baseline_min_samples": 0}, {"baseline_min_samples": 500}, {"step_min_run": 1},
                {"step_threshold_ms": 0}):
        with pytest.raises(ConfigError):
            GateConfig(**bad)
