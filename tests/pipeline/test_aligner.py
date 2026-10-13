"""TimelineAligner: строгое «до» при присоединении стакана и свечей, пропущенные пакеты."""

import random

import polars as pl
import pytest

from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import AVAIL_GAP, AVAIL_UNOBSERVED, AVAIL_WATERMARK, WatermarkClock
from pipeline.types import TimingConfig
from utils.errors import DataError

MIN = 60_000
HOUR = 3_600_000
COMPUTE = 50  # decision_compute_ms
STALE = 2_000  # book_max_staleness_ms
TIMING = TimingConfig()


def aligner() -> TimelineAligner:
    return TimelineAligner(TIMING)


def avail_candles(rows, symbol="BTCUSDT") -> pl.LazyFrame:
    """rows: (open_time, avail_ts | None)."""
    return pl.DataFrame(
        {
            "symbol": [symbol] * len(rows),
            "open_time": [r[0] for r in rows],
            "avail_ts": [r[1] for r in rows],
            "close": [float(r[0]) for r in rows],
        },
        schema={"symbol": pl.String, "open_time": pl.Int64, "avail_ts": pl.Int64, "close": pl.Float64},
    ).lazy()


def msgs(rows) -> pl.LazyFrame:
    """rows: (symbol, recv_ts, seq[, px])."""
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "recv_ts": [r[1] for r in rows],
            "seq": [r[2] for r in rows],
            "px": [float(r[3]) if len(r) > 3 else float(r[2]) for r in rows],
        },
        schema={"symbol": pl.String, "recv_ts": pl.Int64, "seq": pl.Int64, "px": pl.Float64},
    ).lazy()


def two_decisions():
    """Решения: asof=100_000 (окно [40_000,100_000)) и asof=160_000 (окно [100_000,160_000))."""
    cand = avail_candles([(0, 99_950), (MIN, 159_950)])
    return aligner().decisions(cand, MIN)


AGGS = [
    pl.col("px").last().alias("px_last"),
    pl.col("px").mean().alias("px_mean"),
]


# ------------------------------------------------------------------------ decisions


def test_decisions_grid_and_segments():
    cand = avail_candles([(0, 60_900), (MIN, 120_900), (2 * MIN, None), (3 * MIN, 240_900), (4 * MIN, 300_900)])
    d = aligner().decisions(cand, MIN).collect()
    assert d["open_time"].to_list() == [0, MIN, 3 * MIN, 4 * MIN]  # свеча без avail_ts отброшена
    assert d["asof_ts"].to_list() == [60_900 + COMPUTE, 120_900 + COMPUTE, 240_900 + COMPUTE, 300_900 + COMPUTE]
    assert d["contiguous"].to_list() == [False, True, False, True]
    assert d["segment_id"].to_list() == [0, 0, 1, 1]  # разрыв открывает новый сегмент
    assert d["window_start"].to_list() == [
        60_950 - MIN,  # первая строка: один интервал назад
        60_950,  # внутри сегмента: предыдущий asof_ts
        240_950 - MIN,  # после пропуска: один интервал назад, а не «все пропущенные минуты»
        240_950,
    ]
    assert (d["src_max_ts"] < d["asof_ts"]).all()


# ---------------------------------------------------- строгое неравенство asof_ts > recv_ts


def test_message_exactly_at_asof_ts_belongs_to_next_decision():
    d = two_decisions()  # asof 100_000 и 160_000
    m = msgs([
        ("BTCUSDT", 99_999, 1),   # 1 мс до решения 0 -> корзина 100_000
        ("BTCUSDT", 100_000, 2),  # ровно asof_ts -> ещё «не известно», корзина 160_000
        ("BTCUSDT", 159_999, 3),  # -> корзина 160_000
        ("BTCUSDT", 160_000, 4),  # ровно последний asof_ts, решений после нет -> отброшено
        ("BTCUSDT", 160_001, 5),  # после последнего решения -> отброшено
    ])
    out = aligner().assign_book(d, m).sort("seq").collect()
    assert out["seq"].to_list() == [1, 2, 3]
    assert out["asof_ts"].to_list() == [100_000, 160_000, 160_000]
    assert (out["recv_ts"] < out["asof_ts"]).all()


def test_message_before_window_start_is_not_assigned():
    d = two_decisions()
    m = msgs([("BTCUSDT", 39_999, 1), ("BTCUSDT", 40_000, 2)])  # окно первого решения [40_000, 100_000)
    assert aligner().assign_book(d, m).collect()["seq"].to_list() == [2]


def _reference_assign(decisions, messages):
    """Прямой перебор: первое решение того же символа с asof_ts > recv_ts и recv_ts >= window_start."""
    out = set()
    for m in messages:
        cands = [x for x in decisions if x["symbol"] == m["symbol"] and x["asof_ts"] > m["recv_ts"]]
        if not cands:
            continue
        x = min(cands, key=lambda r: r["asof_ts"])
        if m["recv_ts"] >= x["window_start"]:
            out.add((m["symbol"], m["seq"], x["asof_ts"]))
    return out


def test_assign_book_matches_bruteforce_on_random_data_and_is_strict():
    """Случайные решения и сообщения (в том числе ровно на asof_ts и на ±1 мс, вход перемешан):
    корзины совпадают с прямым перебором, и ни одно сообщение не имеет recv_ts >= asof_ts."""
    for seed in range(40):
        rng = random.Random(seed)
        cand_rows, decisions = [], []
        for symbol in ("BTCUSDT", "ETHUSDT"):
            t, k = rng.randint(0, 5_000), 0
            for _ in range(rng.randint(3, 12)):
                if rng.random() < 0.25:
                    k += rng.randint(1, 3)  # пропущенные свечи
                t += rng.randint(30_000, 70_000)
                k += 1
                cand_rows.append((symbol, k * MIN, t))
        cand = pl.DataFrame(
            cand_rows, schema={"symbol": pl.String, "open_time": pl.Int64, "avail_ts": pl.Int64}, orient="row"
        ).lazy()
        d = aligner().decisions(cand, MIN)
        dec_rows = d.select("symbol", "asof_ts", "window_start").collect().to_dicts()

        messages = []
        for n in range(300):
            x = rng.choice(dec_rows)
            if rng.random() < 0.5:
                recv = x["asof_ts"] + rng.choice([-1, 0, 1])  # границы
            elif rng.random() < 0.5:
                recv = x["window_start"] + rng.choice([-1, 0, 1])
            else:
                recv = rng.randint(0, max(r["asof_ts"] for r in dec_rows) + 5_000)
            messages.append({"symbol": x["symbol"], "recv_ts": recv, "seq": n})
        rng.shuffle(messages)
        m = pl.DataFrame(messages, schema={"symbol": pl.String, "recv_ts": pl.Int64, "seq": pl.Int64}).lazy()

        got = aligner().assign_book(d, m).collect()
        assert (got["recv_ts"] < got["asof_ts"]).all(), f"seed={seed}: найдено recv_ts >= asof_ts"
        assert (got["recv_ts"] >= got["window_start"]).all()
        assert set(zip(got["symbol"], got["seq"], got["asof_ts"])) == _reference_assign(dec_rows, messages)
        assert got["seq"].n_unique() == got.height  # сообщение попадает максимум в одну корзину


def test_attach_book_aggregates_use_only_messages_before_decision():
    d = two_decisions()
    dense = [("BTCUSDT", t, i, float(i)) for i, t in enumerate(range(40_200, 100_000, 500))]  # до 99_700
    poison = [("BTCUSDT", 100_000, 9_999, 999.0)]  # ровно на asof_ts: решения 0 касаться не должно
    out = aligner().attach_book(d, msgs(dense + poison), AGGS).collect().sort("asof_ts")
    first = out.row(0, named=True)
    assert first["book_ok"] is True
    assert first["px_last"] == 119.0  # 999.0 не просочилось
    assert first["book_last_recv_ts"] == 99_700
    assert first["src_max_ts"] == 99_950  # = max(avail_ts свечи, recv_ts стакана)
    assert (out["src_max_ts"] < out["asof_ts"]).all()


# ------------------------------------------------------ пропущенные пакеты стакана


def packets(start, stop, step=500, skip=()):
    return [("BTCUSDT", t, i) for i, t in enumerate(range(start, stop, step)) if i not in skip]


def bucket1(m_rows):
    """Строка решения 1 (asof=160_000, окно [100_000,160_000)) при заданных сообщениях."""
    out = aligner().attach_book(two_decisions(), msgs(m_rows), AGGS).collect().sort("asof_ts")
    return out.row(1, named=True)


def test_complete_stream_is_ok():
    row = bucket1(packets(100_200, 160_000))  # 120 пакетов, последний на 159_700
    assert row["book_ok"] is True
    assert row["book_n_msgs"] == 120
    assert row["book_age_ms"] == 300  # последний пакет 159_700, решение в 160_000
    assert row["book_max_gap_ms"] == 500  # диагностика: самый длинный разрыв - обычный шаг
    assert row["px_last"] == 119.0


def test_internal_gap_is_ignored_when_last_packet_is_fresh():
    # Дыра 2.1 с посреди окна, но последний пакет пришёл за 100 мс до asof_ts: корзина валидна.
    ts = [100_100, 100_600] + list(range(102_700, 160_000, 500)) + [159_900]
    row = bucket1([("BTCUSDT", t, i) for i, t in enumerate(ts)])
    assert row["book_max_gap_ms"] == 2_100  # дыра видна в диагностике ...
    assert row["book_age_ms"] == 100
    assert row["book_ok"] is True  # ... но на валидность не влияет
    assert row["px_last"] is not None and row["px_mean"] is not None


def test_many_lost_packets_inside_window_do_not_null_aggregates():
    row = bucket1(packets(100_200, 160_000, skip=set(range(10, 60))))  # потеряно 50 пакетов (25 с)
    assert row["book_max_gap_ms"] == 25_500
    assert row["book_ok"] is True
    assert row["book_n_msgs"] == 70  # число принятых сообщений - для фильтров ниже по конвейеру
    assert row["px_last"] == 119.0


def test_freshness_threshold_is_exactly_staleness():
    dense = [("BTCUSDT", t, i) for i, t in enumerate(range(100_200, 157_000, 500))]
    ok = bucket1(dense + [("BTCUSDT", 158_000, 999)])  # возраст ровно 2000 мс == порог
    bad = bucket1(dense + [("BTCUSDT", 157_999, 999)])  # возраст 2001 мс
    assert (ok["book_age_ms"], ok["book_ok"], ok["px_last"]) == (2_000, True, 999.0)
    assert (bad["book_age_ms"], bad["book_ok"], bad["px_last"]) == (2_001, False, None)


def test_stream_stops_before_decision_makes_bucket_stale():
    row = bucket1(packets(100_200, 150_000))  # последний пакет 149_700, до 160_000 тишина
    assert row["book_age_ms"] == 160_000 - row["book_last_recv_ts"]
    assert row["book_ok"] is False
    assert row["px_last"] is None and row["px_mean"] is None  # устаревший стакан не протягиваем вперёд
    assert row["book_n_msgs"] > 0  # факт приёма сохранён для диагностики


def test_late_first_message_is_only_a_diagnostic():
    row = bucket1(packets(105_000, 160_000))  # первые 5 с окна без данных
    assert row["book_max_gap_ms"] == 5_000
    assert row["book_ok"] is True  # свежесть последнего состояния в порядке


def test_empty_bucket_is_not_ok_and_provenance_stays_candle_only():
    out = aligner().attach_book(two_decisions(), msgs([("BTCUSDT", 99_900, 1)]), AGGS).collect().sort("asof_ts")
    empty = out.row(1, named=True)
    assert empty["book_n_msgs"] == 0
    assert empty["book_ok"] is False
    assert empty["book_age_ms"] is None
    assert empty["book_max_gap_ms"] == MIN  # вся длина окна
    assert empty["px_last"] is None and empty["book_last_recv_ts"] is None
    assert empty["src_max_ts"] == empty["avail_ts"]


def test_first_bucket_ignores_data_from_long_before():
    m = msgs([("BTCUSDT", 1_000, 1), ("BTCUSDT", 2_000, 2)] + packets(100_200, 160_000))
    first = aligner().attach_book(two_decisions(), m, AGGS).collect().sort("asof_ts").row(0, named=True)
    assert first["book_n_msgs"] == 0  # окно [40_000, 100_000): давние сообщения в него не попали


def test_bucket_after_missing_candle_covers_one_interval_only():
    cand = avail_candles([(0, 99_950), (2 * MIN, 219_950)])  # свеча MIN пропущена
    d = aligner().decisions(cand, MIN)  # решения: 100_000 и 220_000, окно второго [160_000, 220_000)
    m = msgs([("BTCUSDT", 150_000, 1), ("BTCUSDT", 161_000, 2)] + [("BTCUSDT", t, 10 + i) for i, t in enumerate(range(160_100, 220_000, 500))])
    out = aligner().attach_book(d, m, [pl.col("seq").min().alias("seq_min")]).collect().sort("asof_ts")
    second = out.row(1, named=True)
    assert second["book_ok"] is True
    assert second["seq_min"] == 2  # сообщение seq=1 (recv 150_000, пропущенная минута) не учтено
    assert second["segment_id"] == 1


def test_aggregate_name_clash_is_rejected():
    with pytest.raises(DataError):
        aligner().attach_book(two_decisions(), msgs([("BTCUSDT", 99_000, 1)]), [pl.col("px").last().alias("close")])


# ------------------------------------------------------------------ свечи другого ТФ


def decisions_in_hour_one():
    """Решения внутри второго часа: asof = 3_700_000 (минута 1), 5_400_000 (минута 30)."""
    cand = avail_candles([(HOUR + MIN, 3_699_950), (HOUR + 30 * MIN, 5_399_950)])
    return aligner().decisions(cand, MIN)


def hour_candles():
    # h0: open 0, закрыта в 3_600_000, avail 3_601_000; h1: open 3_600_000, avail 7_201_000
    return avail_candles([(0, 3_601_000), (HOUR, 7_201_000)])


def test_hourly_candle_joined_by_avail_not_by_open_time():
    out = aligner().attach_candles(decisions_in_hour_one(), hour_candles(), HOUR, "h1_", ["close"]).collect()
    # Внутри второго часа видна только h0 (avail 3_601_000). h1 (open_time=3_600_000 <= asof!)
    # ещё не закрыта - join по open_time дал бы здесь заглядывание на целый час.
    assert out["h1_avail_ts"].to_list() == [3_601_000, 3_601_000]
    assert out["h1_close"].to_list() == [0.0, 0.0]
    assert (out["src_max_ts"] < out["asof_ts"]).all()


def test_hourly_candle_known_exactly_at_asof_ts_is_not_visible():
    cand = avail_candles([(HOUR, 3_600_950)])  # asof_ts = 3_601_000
    d = aligner().decisions(cand, MIN)
    assert d.collect()["asof_ts"].to_list() == [3_601_000]
    out = aligner().attach_candles(d, hour_candles(), HOUR, "h1_", ["close"]).collect()
    assert out["h1_avail_ts"].to_list() == [None]  # avail_ts == asof_ts: строго «до» не выполнено
    later = aligner().attach_candles(
        aligner().decisions(avail_candles([(HOUR, 3_600_951)]), MIN), hour_candles(), HOUR, "h1_", ["close"]
    ).collect()
    assert later["h1_avail_ts"].to_list() == [3_601_000]  # asof_ts = 3_601_001


def test_stale_hourly_candle_is_not_forward_filled_indefinitely():
    d = aligner().decisions(avail_candles([(HOUR + MIN, 11_000_000)]), MIN)
    out = aligner().attach_candles(d, hour_candles(), HOUR, "h1_", ["close"]).collect()
    assert out["h1_avail_ts"].to_list() == [None]  # последняя 1h-свеча старше 1 ч + gap_break_ms


# ----------------------------------------------- интеграция: поток останавливается


def _stream(e_from, e_to, step=500, skip=(), theta=7, lat=30):
    return [
        {"symbol": "BTCUSDT", "recv_ts": e + theta + lat, "E": e, "u": e // step, "px": float(e), "seq": e // step}
        for e in range(e_from, e_to + 1, step)
        if e not in skip
    ]


def test_end_to_end_stream_stop_and_lost_packets_near_close():
    # Поток: E 0..200_000, потеряны пакеты вокруг закрытия 2-й свечи (c=120_000), затем стоп.
    lost = {119_500, 120_000, 120_500, 121_000}
    book = pl.DataFrame(
        _stream(0, 200_000, skip=lost),
        schema={"symbol": pl.String, "recv_ts": pl.Int64, "E": pl.Int64, "u": pl.Int64, "px": pl.Float64, "seq": pl.Int64},
    ).lazy()
    candles = pl.DataFrame(
        {"symbol": ["BTCUSDT"] * 6, "open_time": [i * MIN for i in range(6)]},
        schema={"symbol": pl.String, "open_time": pl.Int64},
    ).lazy()  # c = 60k .. 360k; после E=200_000 поток молчит

    clock = WatermarkClock(TIMING)
    avail = clock.candle_availability(candles, MIN, clock.watermark(book))
    d = aligner().decisions(avail, MIN)
    out = aligner().attach_book(d, book, AGGS).collect().sort("asof_ts")

    srcs = avail.collect().sort("open_time")["avail_source"].to_list()
    assert srcs == [AVAIL_WATERMARK] * 3 + [AVAIL_UNOBSERVED] * 3  # c=60k,120k,180k известны
    assert out["open_time"].to_list() == [0, MIN, 2 * MIN]  # после остановки решений нет
    assert (out["src_max_ts"] < out["asof_ts"]).all()
    assert (out["book_last_recv_ts"] < out["asof_ts"]).all()

    # Решение для свечи 120_000 датировано первым выжившим сообщением E=121_500 ...
    second = out.row(1, named=True)
    assert second["avail_ts"] == 121_500 + 7 + 30 + TIMING.candle_publish_lag_ms
    # ... в корзине стакана перед ним дыра 2.5 с (E=119_000 -> 121_500), это диагностика;
    # после неё поток восстановился и последний пакет свежий, поэтому корзина валидна.
    assert second["book_max_gap_ms"] >= 2_500
    assert second["book_ok"] is True and second["px_last"] is not None
    assert out["book_ok"].to_list() == [True, True, True]
    # Остановка потока: решение, у которого последний пакет старше порога, невалидно.
    stale = aligner().attach_book(d, book.filter(pl.col("E") < 170_000), AGGS).collect().sort("asof_ts")
    assert stale["book_ok"].to_list() == [True, True, False]  # 3-е решение asof=181_087, последний пакет recv=169_537
    assert out["contiguous"].to_list() == [False, True, True]
