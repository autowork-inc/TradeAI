"""KlinesLoader и DepthLoader: чтение сырых файлов Этапа 1, битые строки, карантин."""

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

import config
from collectors.depth_stream import SNAPSHOT_STREAM
from collectors.klines_downloader import KLINE_SCHEMA as COLLECTOR_KLINE_SCHEMA
from pipeline.loaders.depth_loader import DepthLoader
from pipeline.loaders.klines_loader import KlinesLoader
from pipeline.loaders.schemas import KLINE_SCHEMA, REJECT_SCHEMA, depth_schema
from pipeline.quality.gate import DataQualityGate
from pipeline.types import TimingConfig
from utils.errors import DataError
from utils.storage import HourlyJsonlWriter, atomic_write_parquet

MIN = 60_000
LEVELS = 3


def ms(y, m, d, h=0, mi=0, s=0) -> int:
    return int(datetime(y, m, d, h, mi, s, tzinfo=timezone.utc).timestamp() * 1000)


# ----------------------------------------------------------------------- свечи


def test_kline_schema_matches_collector():
    assert dict(KLINE_SCHEMA) == dict(COLLECTOR_KLINE_SCHEMA)


def kline_rows(open_times):
    return pl.DataFrame(
        {
            "open_time": open_times, "open": [1.0] * len(open_times), "high": [2.0] * len(open_times),
            "low": [0.5] * len(open_times), "close": [1.5] * len(open_times),
            "volume": [10.0] * len(open_times), "close_time": [t + MIN - 1 for t in open_times],
            "quote_volume": [15.0] * len(open_times), "trades": [3] * len(open_times),
            "taker_buy_base": [4.0] * len(open_times), "taker_buy_quote": [6.0] * len(open_times),
        },
        schema=KLINE_SCHEMA,
    )


@pytest.fixture
def klines_root(tmp_path):
    base = tmp_path / "klines" / "BTCUSDT" / "1m"
    jan_end = ms(2026, 2, 1)
    atomic_write_parquet(kline_rows([jan_end - 3 * MIN, jan_end - 2 * MIN, jan_end - MIN]), base / "2026-01.parquet")
    atomic_write_parquet(kline_rows([jan_end, jan_end + MIN, jan_end + 3 * MIN]), base / "2026-02.parquet")  # +2 мин пропущено
    (base / "2026-03.tmp.parquet").write_bytes(b"junk")  # остаток прерванной записи: игнорируется
    return tmp_path / "klines"


def test_klines_span_months_bounds_and_gap_flag(klines_root):
    loader = KlinesLoader(klines_root)
    feb = ms(2026, 2, 1)
    df = loader.scan("BTCUSDT", "1m", feb - 2 * MIN, feb + 3 * MIN + 1).collect()
    assert df["open_time"].to_list() == [feb - 2 * MIN, feb - MIN, feb, feb + MIN, feb + 3 * MIN]
    assert df["gap_before"].to_list() == [False, False, False, False, True]  # пропущена свеча feb+2*MIN
    assert set(df["symbol"]) == {"BTCUSDT"}
    # Верхняя граница исключается.
    assert loader.scan("BTCUSDT", "1m", feb - 2 * MIN, feb + MIN).collect()["open_time"].to_list() == [
        feb - 2 * MIN, feb - MIN, feb]


def test_klines_missing_months_empty_and_errors(klines_root):
    loader = KlinesLoader(klines_root)
    assert loader.scan("BTCUSDT", "1m", ms(2025, 1, 1), ms(2025, 3, 1)).collect().is_empty()
    assert loader.scan("ETHUSDT", "1m", ms(2026, 1, 1), ms(2026, 3, 1)).collect().is_empty()
    with pytest.raises(DataError, match="таймфрейм"):
        loader.scan("BTCUSDT", "7m", 0, 1)
    with pytest.raises(DataError, match="диапазон"):
        loader.scan("BTCUSDT", "1m", 5, 5)


def test_klines_foreign_schema_is_an_error(klines_root):
    bad = klines_root / "BTCUSDT" / "1m" / "2026-04.parquet"
    pl.DataFrame({"open_time": [1], "close": ["x"]}).write_parquet(bad)
    with pytest.raises(DataError, match="KLINE_SCHEMA"):
        KlinesLoader(klines_root).scan("BTCUSDT", "1m", ms(2026, 4, 1), ms(2026, 5, 1))


# ---------------------------------------------------------------------- стакан

BIDS = [["100.0", "1.5"], ["99.5", "2"], ["99.0", "3"]]
ASKS = [["100.5", "1"], ["101.0", "2"], ["101.5", "3"]]
STREAM = "btcusdt@depth10@500ms"


def msg(recv_ts, u, E=None, bids=BIDS, asks=ASKS, symbol="BTCUSDT", pu=None, s=None, stream=STREAM):
    data = {"e": "depthUpdate", "E": E if E is not None else recv_ts - 30, "T": recv_ts - 35,
            "s": s or symbol, "U": u, "u": u, "pu": pu if pu is not None else u - 1, "b": bids, "a": asks}
    return json.dumps({"recv_ts": recv_ts, "symbol": symbol, "stream": stream, "data": data}, separators=(",", ":"))


def snapshot(recv_ts):
    data = {"lastUpdateId": 5, "E": recv_ts - 30, "T": recv_ts - 35, "bids": BIDS * 300, "asks": ASKS * 300}
    return json.dumps({"recv_ts": recv_ts, "symbol": "BTCUSDT", "stream": SNAPSHOT_STREAM, "data": data},
                      separators=(",", ":"))


T0 = ms(2026, 1, 15, 10)


def loader(tmp_path) -> DepthLoader:
    return DepthLoader(tmp_path / "raw", tmp_path / "interim", levels=LEVELS)


def test_parse_file_handles_snapshots_broken_lines_and_rejects(tmp_path):
    lines = [
        msg(T0 + 0, 1),  # 1  ок
        snapshot(T0 + 100),  # 2  снапшот: не входит в данные, но учитывается
        "xx yy",  # 3  мусор -> bad_json (предфильтр)
        msg(T0 + 500, 2),  # 4  ок
        msg(T0 + 1000, 3).replace('"E":' + str(T0 + 970), '"E":"abc"'),  # 5  E не число -> schema
        msg(T0 + 1500, 4, bids=[["101.0", "1"]], asks=[["100.0", "1"]]),  # 6  crossed
        msg(T0 + 2000, 5, bids=[]),  # 7  empty_book
        msg(T0 + 2500, 6, bids=[["100.0", "abc"]]),  # 8  bad_level
        '{"recv_ts":5,"symbol":"BTCUSDT","stream":"x","data":{"E":1}',  # 9  скобка не закрыта -> bad_json (медленный путь)
        msg(T0 + 3000, 10, bids=BIDS[:2], asks=ASKS[:2]),  # 10 ок, но уровней меньше
        msg(T0 + 3500, 11, s="ETHUSDT"),  # 11 s не совпадает с symbol -> schema
        '{"recv_ts":99,"symb',  # 12 оборванная последняя строка, без \n
    ]
    path = tmp_path / "10.jsonl"
    path.write_text("\n".join(lines), encoding="utf-8")

    parsed = loader(tmp_path).parse_file(path, "BTCUSDT", source="2026-01-15/10.jsonl")
    assert parsed.n_lines == 12 and parsed.n_snapshots == 1
    assert parsed.valid["u"].to_list() == [1, 2, 10]
    assert parsed.valid.schema == depth_schema(LEVELS)
    first = parsed.valid.row(0, named=True)
    assert first["bid_px_0"] == 100.0 and first["bid_qty_0"] == 1.5 and first["ask_px_2"] == 101.5
    assert (first["E"], first["T"], first["pu"]) == (T0 - 30, T0 - 35, 0)  # снимок stream-сообщения
    short = parsed.valid.row(2, named=True)
    assert short["bid_px_1"] == 99.5 and short["bid_px_2"] is None and short["ask_qty_2"] is None

    rej = parsed.rejects
    assert rej.schema == REJECT_SCHEMA
    by_line = dict(zip(rej["line_no"].to_list(), rej["reason"].to_list()))
    assert by_line == {3: "bad_json", 5: "schema", 6: "crossed", 7: "empty_book", 8: "bad_level",
                       9: "bad_json", 11: "schema", 12: "bad_json"}
    garbage = rej.filter(pl.col("line_no") == 3).row(0, named=True)
    assert garbage["raw"] == "xx yy" and garbage["source"] == "2026-01-15/10.jsonl"


def test_parse_file_empty_and_blank(tmp_path):
    path = tmp_path / "00.jsonl"
    path.write_text("\n\n", encoding="utf-8")
    parsed = loader(tmp_path).parse_file(path, "BTCUSDT")
    assert parsed.n_lines == 0 and parsed.valid.is_empty() and parsed.rejects.is_empty()


def write_raw(tmp_path, items):
    """items: (recv_ts, line). Пишет настоящим HourlyJsonlWriter сборщика (час по recv_ts)."""
    writer = HourlyJsonlWriter(tmp_path / "raw")
    for recv_ts, line in items:
        writer.write("BTCUSDT", recv_ts, line)
    writer.close()


DAY = date(2026, 1, 15)


def test_normalize_day_dedup_quarantine_and_idempotence(tmp_path):
    h10, h11 = ms(2026, 1, 15, 10), ms(2026, 1, 15, 11)
    write_raw(tmp_path, [
        (h10, snapshot(h10)),
        (h10 + 100, msg(h10 + 100, 1)),
        (h10 + 600, msg(h10 + 600, 2)),
        (h11 + 100, msg(h11 + 100, 3)),
        (h11 + 200, msg(h11 + 200, 2)),  # повтор u=2 в соседнем часу (например, после рестарта)
    ])
    with open(tmp_path / "raw" / "BTCUSDT" / "2026-01-15" / "10.jsonl", "a", encoding="utf-8") as fh:
        fh.write('{"recv_ts":7,"symb')  # процесс упал на середине строки

    ld = loader(tmp_path)
    res = ld.normalize_day("BTCUSDT", DAY)
    assert (res.n_files, res.n_snapshots, res.n_valid, res.n_duplicates, res.n_bad_json) == (2, 1, 3, 1, 1)
    assert res.n_rejected == 2 and not res.skipped

    out = pl.read_parquet(ld.interim_path("BTCUSDT", DAY))
    assert out["u"].to_list() == [1, 2, 3] and out["recv_ts"].to_list() == [h10 + 100, h10 + 600, h11 + 100]
    rej = pl.read_parquet(ld.rejects_path("BTCUSDT", DAY))
    assert sorted(rej["reason"].to_list()) == ["bad_json", "duplicate_u"]  # ничего не пропало молча
    assert rej.filter(pl.col("reason") == "duplicate_u")["recv_ts"].to_list() == [h11 + 200]  # остаётся первое

    again = ld.normalize_day("BTCUSDT", DAY)
    assert again.skipped  # свежее источников: повторный разбор не нужен
    forced = ld.normalize_day("BTCUSDT", DAY, force=True)
    assert not forced.skipped and forced.n_valid == 3


def test_normalize_day_without_sources_and_stale_rejects_removed(tmp_path):
    ld = loader(tmp_path)
    assert ld.normalize_day("BTCUSDT", DAY).n_files == 0
    write_raw(tmp_path, [(T0, msg(T0, 1))])
    stale = ld.rejects_path("BTCUSDT", DAY)
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"old")
    res = ld.normalize_day("BTCUSDT", DAY, force=True)
    assert res.n_valid == 1 and res.n_rejected == 0
    assert not stale.exists()  # карантин не должен остаться от прошлой версии разбора


def test_scan_range_gaps_and_cross_day_duplicates(tmp_path):
    d1, d2 = date(2026, 1, 15), date(2026, 1, 16)
    t_a, t_b = ms(2026, 1, 15, 23, 59, 58), ms(2026, 1, 16, 0, 0, 3)
    write_raw(tmp_path, [
        (t_a, msg(t_a, 1)), (t_a + 500, msg(t_a + 500, 2)),
        (t_b, msg(t_b, 5, pu=4)),  # пропущены u=3,4: gap_before
        (t_b + 500, msg(t_b + 500, 6)),
        (t_b + 900, msg(t_b + 900, 2)),  # u=2 уже был в прошлом дне
    ])
    ld = loader(tmp_path)
    results = ld.normalize_range("BTCUSDT", t_a, t_b + 1_000)
    assert [(r.day, r.n_valid, r.n_duplicates) for r in results] == [(d1, 2, 0), (d2, 3, 0)]
    df = ld.scan("BTCUSDT", t_a, t_b + 1_000).collect()  # u=2 второго дня сохранён в файле дня, убирается при scan
    assert df["u"].to_list() == [1, 2, 5, 6]  # дубль u=2 убран и между днями
    assert df["gap_before"].to_list() == [False, False, True, False]
    assert df["recv_gap_ms"].to_list() == [None, 500, t_b - (t_a + 500), 500]
    # Полуоткрытый диапазон по recv_ts.
    assert ld.scan("BTCUSDT", t_a + 500, t_b).collect()["u"].to_list() == [2]
    # Отсутствующий день - не ошибка, просто нет строк.
    assert ld.scan("BTCUSDT", ms(2026, 3, 1), ms(2026, 3, 2)).collect().is_empty()


def test_scan_rejects_cache_with_different_level_count(tmp_path):
    write_raw(tmp_path, [(T0, msg(T0, 1))])
    loader(tmp_path).normalize_day("BTCUSDT", DAY)
    other = DepthLoader(tmp_path / "raw", tmp_path / "interim", levels=LEVELS + 1)
    with pytest.raises(DataError, match="force=True"):
        other.scan("BTCUSDT", T0, T0 + 1).collect()


def test_loader_to_gate_pipeline_quarantines_poisoned_E(tmp_path):
    items = []
    for i in range(60):
        t = T0 + i * 500
        e = t - 30 + (3_600_000 if i == 40 else 0)  # одно сообщение с E из будущего
        items.append((t, msg(t, i + 1, E=e)))
    write_raw(tmp_path, items)
    ld = loader(tmp_path)
    ld.normalize_day("BTCUSDT", DAY)
    book = ld.scan("BTCUSDT", T0, T0 + 60 * 500)
    res = DataQualityGate(TimingConfig()).apply(book)
    q = res.quarantine.collect()
    assert q["u"].to_list() == [41] and q["gate_reason"].to_list() == ["e_outlier"]
    assert res.clean.collect().height == 59


def test_default_config_paths_are_read_at_call_time(sandbox):
    """Без явных корней загрузчики берут config.* в момент вызова (как и сборщики)."""
    assert KlinesLoader().pair_dir("BTCUSDT", "1m") == config.KLINES_DIR / "BTCUSDT" / "1m"
    assert DepthLoader().levels == config.DEPTH_LEVELS
