"""WatermarkClock: когда мы узнаём о закрытии свечи, не зная смещения часов."""

import random

import polars as pl
import pytest

from pipeline.sync.watermark import (
    AVAIL_ASSUMED,
    AVAIL_GAP,
    AVAIL_UNOBSERVED,
    AVAIL_WATERMARK,
    WatermarkClock,
)
from pipeline.types import TimingConfig
from utils.errors import ConfigError

MIN = 60_000
LAG = 1_000  # candle_publish_lag_ms по умолчанию
THETA = 7  # смещение часов в «обычных» сценариях
LATENCY = 30

BOOK_SCHEMA = {"symbol": pl.String, "recv_ts": pl.Int64, "E": pl.Int64, "u": pl.Int64}


def stream(symbol="BTCUSDT", e_from=0, e_to=0, step=500, theta=THETA, latency=LATENCY,
           skip=(), u0=0):
    """Сообщения стакана с E из [e_from, e_to] (включительно), recv_ts = E + theta + latency."""
    rows = []
    for n, e in enumerate(range(e_from, e_to + 1, step)):
        if e in skip:
            continue
        rows.append({"symbol": symbol, "recv_ts": e + theta + latency, "E": e, "u": u0 + n})
    return rows


def book_df(*chunks) -> pl.LazyFrame:
    rows = [r for chunk in chunks for r in chunk]
    return pl.DataFrame(rows, schema=BOOK_SCHEMA).lazy()


def candles_df(open_times, symbol="BTCUSDT") -> pl.LazyFrame:
    return pl.DataFrame(
        {"symbol": [symbol] * len(open_times), "open_time": list(open_times)},
        schema={"symbol": pl.String, "open_time": pl.Int64},
    ).lazy()


def availability(book, open_times, *, interval_ms=MIN, symbol="BTCUSDT", timing=None, **kw):
    clock = WatermarkClock(timing or TimingConfig())
    wm = clock.watermark(book)
    return clock.candle_availability(candles_df(open_times, symbol), interval_ms, wm, **kw).collect()


# ---------------------------------------------------------------- базовое поведение


def test_avail_is_first_message_reaching_candle_end():
    book = book_df(stream(e_from=58_000, e_to=62_000))
    row = availability(book, [0]).row(0, named=True)
    # c = 60_000; первое сообщение с E >= c - E=60_000, recv = 60_000+7+30
    assert row["c"] == MIN
    assert row["avail_source"] == AVAIL_WATERMARK
    assert row["avail_ts"] == 60_000 + THETA + LATENCY + LAG
    assert row["evidence_lag_ms"] == 0


def test_candle_end_is_inclusive_but_one_ms_earlier_is_not_enough():
    # E=59_999 ещё не доказывает закрытие (сервер был строго до c), E=60_000 доказывает.
    book = book_df([
        {"symbol": "BTCUSDT", "recv_ts": 100, "E": 59_999, "u": 1},
        {"symbol": "BTCUSDT", "recv_ts": 200, "E": 60_000, "u": 2},
    ])
    row = availability(book, [0]).row(0, named=True)
    assert row["avail_ts"] == 200 + LAG


@pytest.mark.parametrize("theta", [3_600_000, -3_600_000, 17, 0])
def test_result_does_not_depend_on_clock_offset(theta):
    """Смещение локальных часов (даже на час в любую сторону) не меняет момент знания
    относительно прихода сообщений: оценка θ нигде не используется."""
    base = availability(book_df(stream(e_from=58_000, e_to=130_000, theta=0)), [0, MIN])
    shifted = availability(book_df(stream(e_from=58_000, e_to=130_000, theta=theta)), [0, MIN])
    assert shifted["avail_ts"].to_list() == [a + theta for a in base["avail_ts"].to_list()]


def test_late_message_with_older_E_does_not_lower_or_duplicate_watermark():
    book = book_df([
        {"symbol": "BTCUSDT", "recv_ts": 1_000, "E": 60_100, "u": 1},
        {"symbol": "BTCUSDT", "recv_ts": 1_100, "E": 59_900, "u": 2},  # запоздавшее
        {"symbol": "BTCUSDT", "recv_ts": 1_200, "E": 60_600, "u": 3},
        {"symbol": "BTCUSDT", "recv_ts": 1_300, "E": 60_000, "u": 4},  # запоздавшее
    ])
    wm = WatermarkClock(TimingConfig()).watermark(book).collect()
    assert wm["W"].to_list() == [60_100, 60_600]  # строки только там, где W выросла
    assert wm["recv_ts"].to_list() == [1_000, 1_200]
    assert availability(book, [0]).row(0, named=True)["avail_ts"] == 1_000 + LAG


def test_lag_is_applied_from_timing_config():
    book = book_df(stream(e_from=58_000, e_to=62_000))
    row = availability(book, [0], timing=TimingConfig(candle_publish_lag_ms=2_500)).row(0, named=True)
    assert row["avail_ts"] == 60_000 + THETA + LATENCY + 2_500


# ------------------------------------------------ остановка потока (W перестала расти)


def test_stream_stopped_candles_after_stop_are_unobserved():
    # Поток живёт до E=100_000 и навсегда замолкает: свеча 0 (c=60k) известна,
    # свечи 1 и 2 (c=120k, 180k) - нет.
    book = book_df(stream(e_from=0, e_to=100_000))
    out = availability(book, [0, MIN, 2 * MIN])
    assert out["avail_source"].to_list() == [AVAIL_WATERMARK, AVAIL_UNOBSERVED, AVAIL_UNOBSERVED]
    assert out["avail_ts"].to_list()[0] is not None
    assert out["avail_ts"].to_list()[1:] == [None, None]
    assert out["evidence_lag_ms"].to_list()[1:] == [None, None]


def test_stream_alive_but_E_frozen_watermark_stops_updating():
    # Соединение живо (recv_ts идёт), но E «застрял» на 100_000 и ниже: W не растёт,
    # новых строк водяного знака нет, свеча с c > W_max остаётся неизвестной.
    live = stream(e_from=0, e_to=100_000)
    frozen = [
        {"symbol": "BTCUSDT", "recv_ts": 200_000 + 500 * n, "E": 100_000 - (n % 3), "u": 1_000 + n}
        for n in range(200)
    ]
    book = book_df(live, frozen)
    wm = WatermarkClock(TimingConfig()).watermark(book).collect()
    assert wm["W"].max() == 100_000
    assert wm["recv_ts"].max() < 200_000  # ни одно замёрзшее сообщение не подняло W
    out = availability(book, [0, MIN])
    assert out["avail_source"].to_list() == [AVAIL_WATERMARK, AVAIL_UNOBSERVED]
    assert out["avail_ts"].to_list()[1] is None


def test_stream_outage_then_resume_gives_gap_not_a_late_guess():
    # Поток идёт до E=62_000, молчит 6 минут и возобновляется с E=420_000.
    book = book_df(
        stream(e_from=58_000, e_to=62_000),
        stream(e_from=420_000, e_to=422_000, u0=100),
    )
    opens = [0, MIN, 2 * MIN, 3 * MIN, 4 * MIN, 5 * MIN, 6 * MIN]  # c = 60k .. 420k
    out = availability(book, opens)
    assert out["avail_source"].to_list() == [
        AVAIL_WATERMARK,                 # c=60k: плотное доказательство до обрыва
        AVAIL_GAP, AVAIL_GAP, AVAIL_GAP, AVAIL_GAP, AVAIL_GAP,  # c=120k..360k: «слепой» интервал
        AVAIL_WATERMARK,                 # c=420k: поток вернулся ровно к концу свечи
    ]
    assert out["avail_ts"].to_list()[1:6] == [None] * 5
    # Для «слепых» свечей виден масштаб опоздания доказательства.
    assert out["evidence_lag_ms"].to_list()[1] == 420_000 - 2 * MIN


def test_candle_close_with_lost_packets_is_dated_by_first_surviving_message():
    # Потеряны сообщения E=119_500..121_000: ближайшее с E>=120_000 это E=121_500.
    book = book_df(stream(e_from=118_000, e_to=124_000, skip={119_500, 120_000, 120_500, 121_000}))
    row = availability(book, [MIN]).row(0, named=True)
    assert row["avail_source"] == AVAIL_WATERMARK  # опоздание 1.5 с < gap_break_ms
    assert row["evidence_lag_ms"] == 1_500
    assert row["avail_ts"] == 121_500 + THETA + LATENCY + LAG  # позже «идеального», не раньше


def test_one_stream_stopping_does_not_affect_other_symbol():
    book = book_df(
        stream("BTCUSDT", 0, 100_000),
        stream("ETHUSDT", 0, 300_000, u0=1_000),
    )
    clock = WatermarkClock(TimingConfig())
    candles = pl.concat([candles_df([MIN], "BTCUSDT"), candles_df([MIN], "ETHUSDT")])
    out = clock.candle_availability(candles, MIN, clock.watermark(book)).collect().sort("symbol")
    assert out["symbol"].to_list() == ["BTCUSDT", "ETHUSDT"]
    # BTC: c=120_000 > W_max=100_000 -> нет доказательства; ETH живёт -> известна.
    assert out["avail_source"].to_list() == [AVAIL_UNOBSERVED, AVAIL_WATERMARK]
    assert out["avail_ts"].to_list()[0] is None and out["avail_ts"].to_list()[1] is not None


# --------------------------------------------------------------- история до потока


def test_candles_before_stream_start():
    book = book_df(stream(e_from=1_000_000, e_to=1_002_000))
    opens = [0, 880_000, 940_000]  # c = 60k, 940k (до потока), 1_000k (ровно начало)
    default = availability(book, opens)
    assert default["avail_source"].to_list() == [AVAIL_UNOBSERVED, AVAIL_UNOBSERVED, AVAIL_WATERMARK]
    assert default["avail_ts"].to_list()[:2] == [None, None]

    assumed = availability(book, opens, assume_pre_stream=True)
    assert assumed["avail_source"].to_list() == [AVAIL_ASSUMED, AVAIL_ASSUMED, AVAIL_WATERMARK]
    assert assumed["avail_ts"].to_list()[:2] == [60_000 + LAG, 940_000 + LAG]  # c + lag, θ=0


def test_symbol_absent_from_book_is_unobserved_or_assumed():
    book = book_df(stream("BTCUSDT", 0, 200_000))
    assert availability(book, [0], symbol="XRPUSDT")["avail_source"].to_list() == [AVAIL_UNOBSERVED]
    assert availability(book, [0], symbol="XRPUSDT", assume_pre_stream=True)["avail_source"].to_list() == [
        AVAIL_ASSUMED
    ]


# ------------------------------------------------------------------- инварианты


def test_random_streams_match_bruteforce_and_never_precede_evidence():
    """Сравнение с прямым перебором: avail = recv первого (по recv_ts) сообщения с E >= c."""
    for seed in range(30):
        rng = random.Random(seed)
        rows, recv = [], 0
        for n in range(400):
            recv += rng.choice([100, 500, 500, 500, 700, 3_000, 20_000])  # пропуски и «дыры»
            e = recv - rng.randint(0, 800) + rng.choice([0, 0, 0, -2_000])  # шум и запоздавшие E
            rows.append({"symbol": "BTCUSDT", "recv_ts": recv, "E": e, "u": n})
        shuffled = rows[:]
        rng.shuffle(shuffled)  # вход в произвольном порядке строк
        book = book_df(shuffled)
        opens = list(range(0, max(r["E"] for r in rows), MIN))
        out = availability(book, opens)

        for r in out.iter_rows(named=True):
            first = next((m for m in rows if m["E"] >= r["c"]), None)  # rows уже по recv_ts
            if first is None:
                assert r["avail_source"] == AVAIL_UNOBSERVED and r["avail_ts"] is None
            elif r["avail_source"] == AVAIL_WATERMARK:
                assert r["avail_ts"] == first["recv_ts"] + LAG
                assert first["E"] - r["c"] <= TimingConfig().gap_break_ms
            elif r["avail_source"] in (AVAIL_GAP, AVAIL_UNOBSERVED):
                assert r["avail_ts"] is None
        known = [r["avail_ts"] for r in out.sort("c").iter_rows(named=True) if r["avail_ts"] is not None]
        assert known == sorted(known)  # avail не убывает с ростом c


# ------------------------------------------------------------------- конфигурация


def test_gap_break_must_be_shorter_than_interval():
    book = book_df(stream(e_from=0, e_to=1_000))
    clock = WatermarkClock(TimingConfig(gap_break_ms=MIN))
    with pytest.raises(ConfigError):
        clock.candle_availability(candles_df([0]), MIN, clock.watermark(book))


def test_timing_defaults_and_validation():
    t = TimingConfig()
    assert t.order_latency_ms == 250
    assert t.candle_publish_lag_ms == 1_000 and t.book_max_staleness_ms == 2_000
    for bad in ({"decision_compute_ms": 0}, {"order_latency_ms": 0}, {"candle_publish_lag_ms": -1}):
        with pytest.raises(ConfigError):
            TimingConfig(**bad)
