"""Фандинг в конвейере: ставка не может попасть в строку раньше, чем она стала известна.

Сквозная проверка: стакан -> WatermarkClock (W-часы) -> TimelineAligner.attach_funding.
Сценарий: 8-часовые списания T1 = 08:00:00.004 и T2 = 16:00:00.002 (дрожание в мс, как у биржи),
плотный поток стакана (1 сообщение/с) с 07:30 до 17:00, минутные свечи.
"""

import random
from dataclasses import replace
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pipeline.features.base import Feature, FeatureBuilder, apply_features, compile_features, source_columns
from pipeline.guards.provenance import ProvenanceGuard
from pipeline.loaders.funding_loader import FundingLoader
from pipeline.loaders.schemas import FUNDING_SCHEMA
from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import (
    AVAIL_ASSUMED,
    AVAIL_GAP,
    AVAIL_UNOBSERVED,
    AVAIL_WATERMARK,
    WatermarkClock,
)
from pipeline.types import TimingConfig
from utils.errors import ConfigError, DataError
from utils.storage import atomic_write_parquet

MIN = 60_000
H = 3_600_000
H8 = 8 * H
STEP, THETA, LAT = 1_000, 7, 30  # шаг стакана; d = recv_ts - E = THETA + LAT
TIMING = TimingConfig()  # funding_publish_lag_ms = 2000, candle_publish_lag_ms = 1000
TOL = H8 + TIMING.gap_break_ms  # предельный возраст ставки

T0, T1, T2 = 1, 8 * H + 4, 16 * H + 2  # T0 раньше начала потока
R = (0.0001, 0.0002, 0.0003)
MARK = (100.0, 200.0, 300.0)
FIRST, LAST = int(7.5 * H), 17 * H


def make_book(e_from=FIRST, e_to=LAST, symbol="A", outage=None, u0=0) -> pl.DataFrame:
    df = pl.DataFrame({"E": pl.int_range(e_from, e_to + 1, STEP, eager=True)}).with_columns(
        symbol=pl.lit(symbol),
        recv_ts=pl.col("E") + THETA + LAT,
        u=pl.int_range(pl.len()) + u0,
    )
    if outage:
        df = df.filter(~((pl.col("E") >= outage[0]) & (pl.col("E") < outage[1])))
    return df.select("symbol", "recv_ts", "E", "u")


def make_candles(symbol="A", first=FIRST, last=int(16.5 * H)) -> pl.DataFrame:
    opens = pl.int_range(first, last + 1, MIN, eager=True)
    return pl.DataFrame({"open_time": opens}).with_columns(
        symbol=pl.lit(symbol), close=(pl.col("open_time") // MIN).cast(pl.Float64)
    ).select("symbol", "open_time", "close")


def make_events(times=(T0, T1, T2), rates=R, marks=MARK, symbol="A") -> pl.DataFrame:
    return pl.DataFrame(
        {"symbol": [symbol] * len(times), "funding_time": list(times),
         "funding_rate": list(rates), "mark_price": list(marks)},
        schema={"symbol": pl.String, "funding_time": pl.Int64, "funding_rate": pl.Float64, "mark_price": pl.Float64},
    )


def run(book, candles, events, timing=TIMING, assume=False) -> pl.DataFrame:
    clock, aligner = WatermarkClock(timing), TimelineAligner(timing)
    wm = clock.watermark(book.lazy())
    candle_avail = clock.candle_availability(candles.lazy(), MIN, wm)
    funding_avail = clock.funding_availability(events.lazy(), wm, H8, assume_pre_stream=assume)
    decisions = aligner.decisions(candle_avail, MIN)
    return aligner.attach_funding(decisions, funding_avail, H8).collect()


def funding_avail_frame(book, events, timing=TIMING, assume=False) -> pl.DataFrame:
    clock = WatermarkClock(timing)
    return clock.funding_availability(events.lazy(), clock.watermark(book.lazy()), H8,
                                      assume_pre_stream=assume).collect().sort("funding_time")


def first_recv(book: pl.DataFrame, t: int) -> int:
    """Независимый расчёт: recv_ts первого сообщения с E >= t."""
    return book.filter(pl.col("E") >= t).sort("recv_ts")["recv_ts"][0]


A1 = first_recv(make_book(), T1) + TIMING.funding_publish_lag_ms
A2 = first_recv(make_book(), T2) + TIMING.funding_publish_lag_ms


def expected(asof, avails):
    """Эталон: последнее событие с A < asof (строго) и возрастом <= TOL. avails: [(rate, A|None)]."""
    best = None
    for rate, avail in avails:
        if avail is not None and avail < asof and asof - avail <= TOL and (best is None or avail > best[1]):
            best = (rate, avail)
    return best


# ----------------------------------------------------- ставка не раньше публикации


def test_every_row_sees_exactly_the_latest_published_rate():
    out = run(make_book(), make_candles(), make_events())  # T0 раньше потока -> unobserved
    avails = [(R[1], A1), (R[2], A2)]
    for row in out.iter_rows(named=True):
        exp = expected(row["asof_ts"], avails)
        got = (row["funding_rate"], row["funding_avail_ts"])
        assert got == (exp if exp else (None, None)), f"asof={row['asof_ts']}"
        assert (row["funding_age_ms"] is None) == (exp is None)
    # Тест не пустой: есть и «ещё нет ставки», и обе ставки.
    rates = set(out["funding_rate"].to_list())
    assert rates == {None, R[1], R[2]}
    assert (out["funding_avail_ts"].drop_nulls() < out.filter(pl.col("funding_avail_ts").is_not_null())["asof_ts"]).all()
    ProvenanceGuard(extra_ts_cols=["funding_avail_ts"]).check(out)  # src_max_ts < asof_ts


def test_rate_is_invisible_at_exactly_avail_ts_and_visible_one_ms_later():
    book, events = make_book(), make_events()
    clock, aligner = WatermarkClock(TIMING), TimelineAligner(TIMING)
    fav = clock.funding_availability(events.lazy(), clock.watermark(book.lazy()), H8, assume_pre_stream=True)
    # Две ручные «свечи»: asof_ts ровно A1 и A1 + 1 (asof = avail свечи + decision_compute_ms).
    c = TIMING.decision_compute_ms
    candles_avail = pl.DataFrame({
        "symbol": ["A", "A"], "open_time": [8 * H, 8 * H + MIN], "avail_ts": [A1 - c, A1 + 1 - c],
    }).lazy()
    out = aligner.attach_funding(aligner.decisions(candles_avail, MIN), fav, H8).collect()
    assert out["asof_ts"].to_list() == [A1, A1 + 1]
    assert out["funding_rate"].to_list() == [R[0], R[1]]  # при равенстве - предыдущая ставка


def test_publish_lag_delays_the_settlement_bar():
    """Решение по свече, закрывшейся в 08:00, принимается через ~1.05 с после сообщения E>=08:00.
    С lag=2 с новая ставка в нём ещё не видна, со lag=0 - уже видна: поэтому lag консервативен."""
    book, candles, events = make_book(), make_candles(), make_events()
    row_at = lambda out, open_time: out.filter(pl.col("open_time") == open_time).row(0, named=True)  # noqa: E731

    safe = run(book, candles, events, assume=True)
    close_bar, next_bar = row_at(safe, 8 * H - MIN), row_at(safe, 8 * H)
    assert close_bar["funding_rate"] == R[0] and next_bar["funding_rate"] == R[1]

    unsafe = run(book, candles, events, timing=replace(TIMING, funding_publish_lag_ms=0), assume=True)
    assert row_at(unsafe, 8 * H - MIN)["funding_rate"] == R[1]  # ставка 'просочилась' в бар закрытия
    assert row_at(unsafe, 8 * H - MIN)["asof_ts"] == row_at(safe, 8 * H - MIN)["asof_ts"]


def test_naive_forward_join_by_server_time_leaks_but_aligner_does_not():
    book, candles, events = make_book(), make_candles(), make_events()
    out = run(book, candles, events, assume=True)
    # Типичная ошибка: «ставка ближайшего будущего списания» по серверному времени.
    naive = out.lazy().sort("open_time").join_asof(
        events.lazy().sort("funding_time").select("funding_time", pl.col("funding_rate").alias("naive_rate")),
        left_on="open_time", right_on="funding_time", strategy="forward",
    ).collect()
    before_publish = naive.filter(pl.col("asof_ts") <= A1)
    # Все 30 минутных решений 07:31..08:00 получают ставку списания, которого ещё не было.
    assert (before_publish["naive_rate"] == R[1]).sum() == before_publish.height == 30
    assert (before_publish["funding_rate"] == R[1]).sum() == 0  # честный join: R1 здесь не виден


# ---------------------------------------------- будущее не влияет на прошлые строки


def test_changing_or_removing_future_funding_does_not_change_past_rows():
    book, candles, events = make_book(), make_candles(), make_events()
    base = run(book, candles, events, assume=True)

    variants = {
        "T2 rate changed": (make_events(rates=(R[0], R[1], 0.5)), A2),
        "T2 removed": (make_events(times=(T0, T1), rates=R[:2], marks=MARK[:2]), A2),
        "T1 rate changed": (make_events(rates=(R[0], 0.4, R[2])), A1),
    }
    for name, (alt_events, cutoff) in variants.items():
        alt = run(book, candles, alt_events, assume=True)
        past = lambda df: df.filter(pl.col("asof_ts") <= cutoff)  # noqa: E731
        assert_frame_equal(past(alt), past(base)), name
        future_alt, future_base = (df.filter(pl.col("asof_ts") > cutoff) for df in (alt, base))
        assert not future_alt["funding_rate"].equals(future_base["funding_rate"]), f"{name}: тест пустой"


@pytest.mark.parametrize("seed", range(6))
def test_prefix_invariance_with_funding(seed):
    """P(D)[asof <= c] == P(D[<= c]): добавление данных после c не меняет строки до c."""
    rng = random.Random(seed)
    book, candles, events = make_book(), make_candles(), make_events()
    full = run(book, candles, events, assume=True)
    for _ in range(2):
        cut = rng.randint(int(7.8 * H), int(16.4 * H))
        part = run(
            book.filter(pl.col("recv_ts") <= cut),
            candles.filter(pl.col("open_time") + MIN <= cut),
            events.filter(pl.col("funding_time") <= cut),
            assume=True,
        )
        a, b = (df.filter(pl.col("asof_ts") <= cut) for df in (part, full))
        assert_frame_equal(a, b), f"seed={seed}, cut={cut}"


# ------------------------------------------------------------ остановка и пропуски


def test_stream_stopped_before_settlement_never_yields_the_unpublished_rate():
    book = make_book(e_to=16 * H - 30 * MIN)  # поток замолчал за 30 минут до T2
    fav = funding_avail_frame(book, make_events(), assume=True)
    states = dict(zip(fav["funding_time"].to_list(), fav["avail_source"].to_list()))
    assert states == {T0: AVAIL_ASSUMED, T1: AVAIL_WATERMARK, T2: AVAIL_UNOBSERVED}
    assert fav.filter(pl.col("funding_time") == T2)["avail_ts"][0] is None

    clock, aligner = WatermarkClock(TIMING), TimelineAligner(TIMING)
    wm = clock.watermark(book.lazy())
    faf = clock.funding_availability(make_events().lazy(), wm, H8, assume_pre_stream=True)
    c = TIMING.decision_compute_ms
    asofs = [16 * H + 100, 17 * H]  # сразу после T2 и через час, а потока всё нет
    candles_avail = pl.DataFrame({
        "symbol": ["A", "A"], "open_time": [16 * H, 16 * H + 5 * MIN], "avail_ts": [a - c for a in asofs],
    }).lazy()
    out = aligner.attach_funding(aligner.decisions(candles_avail, MIN), faf, H8).collect()
    assert R[2] not in out["funding_rate"].to_list()
    assert out["funding_rate"][0] == R[1]  # ещё свежая R1 (возраст ~8 ч - 3 с)
    assert out["funding_rate"][1] is None  # через час возраст R1 > H8 + gap_break: null, не протухшее


def test_outage_across_settlement_marks_gap_and_leaves_no_rate():
    resume = 16 * H + 30 * MIN
    book = make_book(outage=(16 * H - 10 * MIN, resume))
    fav = funding_avail_frame(book, make_events(), assume=True)
    t2 = fav.filter(pl.col("funding_time") == T2).row(0, named=True)
    assert t2["avail_source"] == AVAIL_GAP and t2["avail_ts"] is None
    assert t2["evidence_lag_ms"] == resume - T2  # доказательство пришло только после возобновления

    out = run(book, make_candles(), make_events(), assume=True)
    after = out.filter(pl.col("asof_ts") > first_recv(book, resume))
    assert after.height > 0
    # R2 не придумывается задним числом; R1 к этому моменту старше предельного возраста.
    assert after["funding_rate"].null_count() == after.height
    assert R[2] not in out["funding_rate"].to_list()
    before = out.filter(pl.col("asof_ts") < 16 * H - 10 * MIN)
    assert set(before["funding_rate"].drop_nulls().to_list()) == {R[0], R[1]}


def test_events_before_stream_are_unobserved_unless_assumed():
    book, events = make_book(), make_events()
    default = funding_avail_frame(book, events)
    assert default.filter(pl.col("funding_time") == T0).row(0, named=True)["avail_source"] == AVAIL_UNOBSERVED
    assumed = funding_avail_frame(book, events, assume=True).filter(pl.col("funding_time") == T0).row(0, named=True)
    assert (assumed["avail_source"], assumed["avail_ts"]) == (AVAIL_ASSUMED, T0 + TIMING.funding_publish_lag_ms)
    # Реальные события внутри потока от флага не зависят.
    same = funding_avail_frame(book, events, assume=True).filter(pl.col("funding_time") > T0)
    assert same["avail_ts"].to_list() == default.filter(pl.col("funding_time") > T0)["avail_ts"].to_list()
    assert same["avail_ts"].to_list() == [A1, A2]


# ------------------------------------------------------ обобщённые event_*-интерфейсы


def test_event_availability_is_generic_and_per_symbol():
    book = pl.concat([make_book(symbol="A"), make_book(symbol="B", e_to=9 * H, u0=10**6)])
    events = pl.DataFrame({
        "symbol": ["A", "B", "B"], "ts": [9 * H + 5, 9 * H + 5, 12 * H], "payload": [1, 2, 3],
    })
    clock = WatermarkClock(TIMING)
    out = clock.event_availability(
        events.lazy(), "ts", clock.watermark(book.lazy()), publish_lag_ms=700, min_spacing_ms=MIN,
    ).collect().sort("symbol", "ts")
    assert out.columns == ["symbol", "ts", "payload", "avail_ts", "avail_source", "evidence_lag_ms"]
    a, b1, b2 = out.row(0, named=True), out.row(1, named=True), out.row(2, named=True)
    assert a["avail_ts"] == first_recv(make_book(symbol="A"), 9 * H + 5) + 700 and a["avail_source"] == AVAIL_WATERMARK
    assert b1["avail_source"] == AVAIL_UNOBSERVED  # у B поток кончился в 09:00:00
    assert b2["avail_source"] == AVAIL_UNOBSERVED and b2["avail_ts"] is None
    assert a["payload"] == 1  # колонки события сохраняются


def test_event_availability_validations():
    clock = WatermarkClock(TIMING)
    wm = clock.watermark(make_book(e_to=FIRST + 10_000).lazy())
    ev = make_events().lazy()
    with pytest.raises(ConfigError):  # gap_break_ms >= расстояния между событиями
        clock.event_availability(ev, "funding_time", wm, publish_lag_ms=0, min_spacing_ms=TIMING.gap_break_ms)
    with pytest.raises(ConfigError):
        clock.event_availability(ev, "funding_time", wm, publish_lag_ms=-1, min_spacing_ms=H8)
    with pytest.raises(DataError, match="служебные"):
        clock.event_availability(ev.with_columns(avail_ts=pl.lit(1)), "funding_time", wm,
                                 publish_lag_ms=0, min_spacing_ms=H8)
    with pytest.raises(DataError):
        clock.event_availability(ev, "no_such_column", wm, publish_lag_ms=0, min_spacing_ms=H8)
    with pytest.raises(ConfigError):
        TimingConfig(funding_publish_lag_ms=-1)


def _ev_avail(rows):
    """rows: (symbol, avail_ts, value)."""
    return pl.DataFrame({
        "symbol": [r[0] for r in rows], "avail_ts": [r[1] for r in rows], "val": [r[2] for r in rows],
    }, schema={"symbol": pl.String, "avail_ts": pl.Int64, "val": pl.Float64}).lazy()


def _decisions(asofs, symbol="A"):
    c = TIMING.decision_compute_ms
    cand = pl.DataFrame({
        "symbol": [symbol] * len(asofs), "open_time": [i * MIN for i in range(len(asofs))],
        "avail_ts": [a - c for a in asofs],
    }).lazy()
    return TimelineAligner(TIMING).decisions(cand, MIN)


def test_attach_events_strictness_age_limit_and_provenance():
    aligner = TimelineAligner(TIMING)
    d = _decisions([1_000, 1_001, 1_000 + 500, 1_000 + 501])
    events = _ev_avail([("A", 1_000, 7.0), ("A", None, 9.0)])
    out = aligner.attach_events(d, events, ["val"], max_age_ms=500, tag="e_").collect().sort("asof_ts")
    assert out["asof_ts"].to_list() == [1_000, 1_001, 1_500, 1_501]
    assert out["e_val"].to_list() == [None, 7.0, 7.0, None]  # равенство - нет; возраст == лимиту - да; +1 - нет
    assert out["e_age_ms"].to_list() == [None, 1, 500, None]
    assert out["e_avail_ts"].to_list() == [None, 1_000, 1_000, None]
    assert (out["src_max_ts"] < out["asof_ts"]).all()  # null-avail события пропущены, не использованы


def test_attach_events_name_checks_and_explicit_names():
    aligner = TimelineAligner(TIMING)
    d, events = _decisions([1_000]), _ev_avail([("A", 10, 1.0)])
    with pytest.raises(DataError):  # пустой tag без явных имён: avail_ts уже есть в решениях
        aligner.attach_events(d, events, ["val"], max_age_ms=10)
    with pytest.raises(DataError):
        aligner.attach_events(d, events, ["val"], max_age_ms=0, tag="e_")
    with pytest.raises(DataError):  # повтор имён
        aligner.attach_events(d, events, ["val"], max_age_ms=10, tag="", avail_name="val", age_name="x")
    ok = aligner.attach_events(d, events, ["val"], max_age_ms=10_000, avail_name="v_ts", age_name="v_age").collect()
    assert {"val", "v_ts", "v_age"} <= set(ok.columns) and ok["v_age"][0] == 1_000 - 10
    with pytest.raises(DataError, match="уже есть"):
        aligner.attach_events(ok.lazy(), events, ["val"], max_age_ms=10, avail_name="v_ts2", age_name="v_age2")


def test_candle_and_funding_wrappers_keep_their_contracts():
    aligner = TimelineAligner(TIMING)
    d = _decisions([5_000])
    cand = pl.DataFrame({"symbol": ["A"], "avail_ts": [100], "close": [1.5]}).lazy()
    out = aligner.attach_candles(d, cand, 60_000, "h1_", ["close"]).collect()
    assert {"h1_close", "h1_avail_ts", "h1_age_ms"} <= set(out.columns) and out["h1_close"][0] == 1.5
    with pytest.raises(DataError, match="tag"):
        aligner.attach_candles(d, cand, 60_000, "", ["close"])
    fund = pl.DataFrame({"symbol": ["A"], "avail_ts": [100], "funding_rate": [0.1], "mark_price": [5.0]}).lazy()
    fo = aligner.attach_funding(d, fund, H8).collect()
    assert {"funding_rate", "mark_price", "funding_avail_ts", "funding_age_ms"} <= set(fo.columns)
    assert fo["funding_age_ms"][0] == 5_000 - 100


# ------------------------------------------------------------------- загрузчик


def write_year(root: Path, symbol: str, year: int, times, rates=None, marks=None):
    rates = rates or [0.0001] * len(times)
    marks = marks or [50.0] * len(times)
    df = pl.DataFrame({"funding_time": list(times), "funding_rate": rates, "mark_price": marks},
                      schema=FUNDING_SCHEMA)
    atomic_write_parquet(df, root / symbol / f"{year:04d}.parquet")


def ms_utc(y, m, d, h=0):
    from datetime import datetime, timezone
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


def test_funding_loader_range_lookback_gaps_and_errors(tmp_path):
    root = tmp_path / "funding"
    dec = [ms_utc(2024, 12, 31, h) + 2 for h in (0, 8, 16)]
    jan = [ms_utc(2025, 1, 1, 0) + 2, ms_utc(2025, 1, 1, 8) + 2, ms_utc(2025, 1, 2, 0) + 2]  # 16:00 пропущено
    write_year(root, "BTCUSDT", 2024, dec)
    write_year(root, "BTCUSDT", 2025, jan)
    (root / "BTCUSDT" / "2026.tmp.parquet").write_bytes(b"junk")
    loader = FundingLoader(root)

    start, end = ms_utc(2025, 1, 1, 6), ms_utc(2025, 1, 2, 1)
    got = loader.scan("BTCUSDT", start, end).collect()
    assert got["funding_time"].to_list() == [jan[1], jan[2]] and set(got["symbol"]) == {"BTCUSDT"}
    assert got["gap_before"].to_list() == [False, True]  # между 08:00 и 00:00 пропущено списание

    # Запас 16 ч назад от 06:00 -> с 31.12 14:00: захватывает запись 31.12 16:00 из файла 2024 года.
    padded = loader.scan("BTCUSDT", start, end, lookback_ms=2 * H8).collect()
    assert padded["funding_time"].to_list() == [dec[2], jan[0], jan[1], jan[2]]
    assert loader.scan("BTCUSDT", 0, 1).collect().is_empty()
    assert loader.scan("ETHUSDT", start, end).collect().is_empty()
    with pytest.raises(DataError, match="диапазон"):
        loader.scan("BTCUSDT", 5, 5)

    pl.DataFrame({"funding_time": [1], "funding_rate": ["x"]}).write_parquet(root / "BTCUSDT" / "2026.parquet")
    with pytest.raises(DataError, match="FUNDING_SCHEMA"):
        loader.scan("BTCUSDT", ms_utc(2026, 1, 1), ms_utc(2026, 2, 1)).collect()


def test_funding_loader_prepares_events_for_the_aligner(tmp_path):
    root = tmp_path / "funding"
    write_year(root, "A", 1970, [T0, T1, T2], rates=list(R), marks=list(MARK))  # метки теста - в январе 1970 года
    book = make_book()
    clock = WatermarkClock(TIMING)
    wm = clock.watermark(book.lazy())
    prepared = FundingLoader(root).scan_with_availability("A", FIRST, LAST, clock, wm).collect()
    # Запас назад на два интервала захватывает записи до начала диапазона (здесь - T0).
    assert prepared["funding_time"].to_list() == [T0, T1, T2]
    assert prepared["avail_source"].to_list() == [AVAIL_UNOBSERVED, AVAIL_WATERMARK, AVAIL_WATERMARK]
    assert prepared["avail_ts"].to_list() == [None, A1, A2]
    assert {"evidence_lag_ms", "funding_rate", "mark_price", "gap_before"} <= set(prepared.columns)

    aligner = TimelineAligner(TIMING)
    decisions = aligner.decisions(clock.candle_availability(make_candles().lazy(), MIN, wm), MIN)
    out = aligner.attach_funding(decisions, prepared.lazy(), H8).collect()
    assert_frame_equal(out, run(book, make_candles(), make_events()))  # тот же результат, что и без загрузчика


# ------------------------------------------------------ фандинг как источник признаков


class FundingCarry(Feature):
    name = "funding_carry"

    def build(self, fb: FeatureBuilder):
        rate = fb.col("funding_rate")
        return {"funding_bps": rate * 10_000.0, "funding_change": rate.diff(1)}


def test_funding_is_a_valid_feature_source_but_its_timestamps_are_not():
    out = run(make_book(), make_candles(), make_events(), assume=True)
    lf = out.lazy()
    cols = source_columns(lf)
    assert {"funding_rate", "mark_price"} <= cols
    assert not ({"funding_avail_ts", "funding_age_ms"} & cols)
    with pytest.raises(Exception):
        FeatureBuilder(cols | {"funding_age_ms"})  # служебные колонки нельзя делать источниками

    res = apply_features(lf, compile_features([FundingCarry()], cols)).collect()
    assert res["funding_bps"].to_list()[-1] == pytest.approx(R[2] * 10_000)
    ProvenanceGuard(extra_ts_cols=["funding_avail_ts"]).check(res)
    # funding_change ненулевой ровно в строках, где ставка сменилась (после публикации, не раньше).
    changed = res.filter(pl.col("funding_change").fill_null(0.0) != 0.0)
    assert (changed["funding_avail_ts"] < changed["asof_ts"]).all()
    assert changed.height == 2  # R0 -> R1 и R1 -> R2
