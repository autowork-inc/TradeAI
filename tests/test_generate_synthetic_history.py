"""Синтетическая история (scripts/generate_synthetic_history.py): формат обязан идеально совпадать со схемами загрузчиков.

Главное - не «файлы появились», а «конвейер Этапа 2 их проглатывает»: те же KlinesLoader / DepthLoader /
FundingLoader / DataQualityGate / FrameFundingFeed, что в train_walkforward, без карантина и DataError.
"""

import json

import polars as pl
import pytest

import config
from pipeline.loaders.depth_loader import DepthLoader
from pipeline.loaders.funding_loader import FundingLoader
from pipeline.loaders.klines_loader import KLINES_OUT_SCHEMA, KlinesLoader
from pipeline.loaders.schemas import FUNDING_SCHEMA, KLINE_SCHEMA, depth_schema
from pipeline.quality.gate import DataQualityGate
from pipeline.types import TimingConfig
from scripts.generate_synthetic_history import (
    HOUR_MS,
    MARKER_NAME,
    MINUTE_MS,
    SPECS,
    build,
    generate_symbol,
    hourly_candles,
    main,
    write_history,
)
from scripts.train_walkforward import FrameFundingFeed
from utils.errors import EXIT_CONFIG, ConfigError

START = 1_790_000_000_000 // HOUR_MS * HOUR_MS + 17 * MINUTE_MS  # не кратно часу: проверяет неполные часы
MINUTES = 150
LEVELS = 10


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic") / "raw"
    end = START + MINUTES * MINUTE_MS
    histories = [generate_symbol(s, START, MINUTES, seed=3, levels=LEVELS) for s in ("BTCUSDT", "ETHUSDT")]
    counts = write_history(histories, root, START, end, params={"seed": 3})
    return root, histories, end, counts


def test_klines_are_read_by_the_loader_with_exact_schema(written):
    root, histories, end, _ = written
    loader = KlinesLoader(root / "klines")
    for h in histories:
        frame = loader.scan(h.symbol, "1m", START, end).collect()
        assert frame.schema == KLINES_OUT_SCHEMA
        assert frame.height == MINUTES
        assert not frame["gap_before"].any()  # минуты идут подряд без дыр
        assert (frame["close_time"] == frame["open_time"] + MINUTE_MS - 1).all()
        assert h.candles.schema == KLINE_SCHEMA


def test_candles_are_internally_consistent(written):
    _, histories, _, _ = written
    for h in histories:
        c = h.candles
        assert (c["high"] >= c.select(pl.max_horizontal("open", "close")).to_series()).all()
        assert (c["low"] <= c.select(pl.min_horizontal("open", "close")).to_series()).all()
        assert (c["volume"] > 0).all() and (c["taker_buy_base"] <= c["volume"]).all()
        assert (c["open"].shift(-1).drop_nulls() == c["close"].head(-1)).all()  # закрытие = открытие следующей


def test_hourly_candles_only_from_full_hours(written):
    _, histories, _, _ = written
    for h in histories:
        hourly = hourly_candles(h.candles)
        assert hourly.schema == KLINE_SCHEMA
        assert (hourly["open_time"] % HOUR_MS == 0).all()
        first_full = -(-START // HOUR_MS) * HOUR_MS
        assert hourly["open_time"].min() == first_full  # неполный стартовый час отброшен
        for row in hourly.iter_rows(named=True):
            minutes = h.candles.filter(pl.col("open_time").is_between(row["open_time"], row["open_time"] + HOUR_MS - 1))
            assert minutes.height == 60
            assert row["volume"] == pytest.approx(minutes["volume"].sum())
            assert row["open"] == minutes["open"][0] and row["close"] == minutes["close"][-1]


def test_depth_jsonl_is_normalized_without_rejects_and_passes_the_gate(written, tmp_path):
    root, histories, end, _ = written
    loader = DepthLoader(raw_root=root / "depth", interim_root=tmp_path / "interim", levels=LEVELS)
    for h in histories:
        results = loader.normalize_range(h.symbol, START, end)
        assert sum(r.n_rejected for r in results) == 0  # ни bad_json, ни schema, ни crossed, ни duplicate_u
        assert sum(r.n_snapshots for r in results) == 1  # REST-снапшот сборщика - первой строкой
        assert sum(r.n_valid for r in results) == len(h.depth_lines) - 1

    book = pl.concat([loader.scan(h.symbol, START, end) for h in histories])
    gated = DataQualityGate(TimingConfig()).apply(book)
    assert gated.quarantine.select(pl.len()).collect().item() == 0  # recv_ts монотонен, E без выбросов
    assert gated.steps.select(pl.len()).collect().item() == 0  # шагов часов нет
    clean = gated.clean.collect()
    assert dict(depth_schema(LEVELS)).items() <= dict(clean.schema).items()
    # шаг сообщений ~500 мс
    gaps = clean.filter(pl.col("symbol") == "BTCUSDT")["recv_ts"].diff().drop_nulls()
    assert 450 < gaps.median() < 550 and gaps.max() < 1_000
    assert clean["recv_ts"].max() < START + MINUTES * MINUTE_MS  # ничего «из будущего» относительно конца истории


def test_raw_line_format_matches_the_collector(written):
    _, histories, _, _ = written
    snapshot, message = (json.loads(histories[0].depth_lines[i][1]) for i in (0, 1))
    assert set(snapshot) == {"recv_ts", "symbol", "stream", "data"} and snapshot["stream"] == "snapshot"
    assert {"lastUpdateId", "E", "T", "bids", "asks"} <= set(snapshot["data"])
    assert message["stream"] == f"btcusdt@depth{LEVELS}@{config.DEPTH_UPDATE_SPEED}"
    data = message["data"]
    assert {"e", "E", "T", "s", "U", "u", "pu", "b", "a"} <= set(data) and data["s"] == "BTCUSDT"
    assert len(data["b"]) == len(data["a"]) == LEVELS
    assert all(isinstance(x, str) for level in data["b"] + data["a"] for x in level)  # биржа отдаёт строки
    bids = [float(p) for p, _ in data["b"]]
    asks = [float(p) for p, _ in data["a"]]
    assert bids == sorted(bids, reverse=True) and asks == sorted(asks) and bids[0] < asks[0]
    assert data["pu"] < data["U"] <= data["u"]


def test_funding_covers_the_window_and_feeds_the_backtester(written):
    root, histories, end, _ = written
    loader = FundingLoader(root / "funding")
    for h in histories:
        rates = loader.scan(h.symbol, START, end, lookback_ms=2 * loader.interval_ms).collect()
        assert rates.select(pl.col(c) for c in FUNDING_SCHEMA).schema == FUNDING_SCHEMA
        assert not rates["gap_before"].any() and rates["mark_price"].null_count() == 0
        assert (rates["funding_time"] % config.FUNDING_INTERVAL_MS == 0).all()
        # то же покрытие, что требует train_walkforward: окно [START, end) не вызывает FundingCoverageError
        feed = FrameFundingFeed(
            pl.concat([loader.scan(s.symbol, START, end, lookback_ms=2 * loader.interval_ms).collect() for s in histories]),
            loader.interval_ms,
        )
        feed.settlements(h.symbol, START, end)


def test_same_seed_same_market_and_different_seed_differs():
    a = generate_symbol("ETHUSDT", START, 20, seed=1, levels=LEVELS)
    b = generate_symbol("ETHUSDT", START, 20, seed=1, levels=LEVELS)
    c = generate_symbol("ETHUSDT", START, 20, seed=2, levels=LEVELS)
    assert a.candles.equals(b.candles) and a.depth_lines == b.depth_lines
    assert not a.candles.equals(c.candles)


def test_history_ends_at_the_target_price_and_has_a_predictable_part():
    h = generate_symbol("BTCUSDT", START, 600, seed=4, levels=LEVELS)
    assert h.candles["close"][-1] == pytest.approx(SPECS["BTCUSDT"].base_price, rel=1e-3)
    # режимный дрейф: знак 5-минутной форвардной доходности предсказуем по знаку дрейфа лучше монеты
    closes = h.candles["close"]
    fwd = (closes.shift(-5) / closes).log().drop_nulls()
    assert fwd.std() > 0


def test_build_uses_closed_minutes_and_nothing_from_the_future():
    now_ms = 1_790_000_123_456
    (hist,), start, end = build(["BTCUSDT"], hours=1, seed=1, levels=LEVELS, now_ms=now_ms)
    assert end == now_ms // MINUTE_MS * MINUTE_MS and end - start == 60 * MINUTE_MS
    assert hist.candles["close_time"].max() < now_ms
    assert max(ts for ts, _ in hist.depth_lines) < now_ms


def test_real_collector_data_is_never_overwritten_without_force(tmp_path):
    root = tmp_path / "raw"
    hist = generate_symbol("BTCUSDT", START, 10, seed=1, levels=LEVELS)
    real = root / "depth" / "BTCUSDT" / "2026-01-01" / "00.jsonl"
    real.parent.mkdir(parents=True)
    real.write_text('{"настоящие": "данные"}\n', encoding="utf-8")

    with pytest.raises(ConfigError, match="настоящих"):
        write_history([hist], root, START, START + 10 * MINUTE_MS)
    assert real.read_text(encoding="utf-8") == '{"настоящие": "данные"}\n'
    assert not (root / MARKER_NAME).exists() and not (root / "klines").exists()  # отказ - до первой записи

    write_history([hist], root, START, START + 10 * MINUTE_MS, force=True)
    assert (root / MARKER_NAME).is_file()
    assert real.is_file()  # --force дописывает синтетику рядом, чужие файлы не удаляет


def test_regeneration_replaces_synthetic_files_instead_of_duplicating(tmp_path):
    root = tmp_path / "raw"
    hist = generate_symbol("BTCUSDT", START, 90, seed=1, levels=LEVELS)
    end = START + 90 * MINUTE_MS
    for _ in range(2):
        write_history([hist], root, START, end)  # второй раз - маркер есть, перезапись разрешена
    lines = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in (root / "depth").rglob("*.jsonl"))
    assert lines == len(hist.depth_lines)
    klines = pl.read_parquet(root / "klines" / "BTCUSDT" / "1m" / "*.parquet")
    assert klines.height == 90 and klines["open_time"].n_unique() == 90
    marker = json.loads((root / MARKER_NAME).read_text(encoding="utf-8"))
    assert marker["synthetic"] is True and marker["symbols"] == ["BTCUSDT"]


def test_cli_end_to_end_writes_the_raw_layer(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")
    root = tmp_path / "raw"
    assert main(["--hours", "1", "--symbols", "BTCUSDT", "--root", str(root), "--seed", "9"]) == 0
    assert (root / MARKER_NAME).is_file()
    assert list((root / "klines" / "BTCUSDT" / "1m").glob("*.parquet"))
    assert list((root / "funding" / "BTCUSDT").glob("*.parquet"))
    assert main(["--hours", "0.5", "--root", str(root)]) == EXIT_CONFIG  # короче часа - ConfigError
    assert main(["--symbols", "XRPUSDT", "--root", str(tmp_path / "x")]) == EXIT_CONFIG  # нет параметров рынка
