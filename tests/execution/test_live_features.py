"""LiveFeatureFeed: признаки в бою = признаки бэктеста (No Train-Serve Skew), буфер хватает окнам и EWMA.

Главный тест - сверка с офлайн-конвейером: один и тот же синтетический рынок (tests/synthetic.py) прогоняется
(а) целиком через build_dataset, как при обучении, и (б) по минутам через фид с имитацией REST биржи, который
видит только то, что видела бы живая система на момент poll(). Строки совпадают по признакам.
"""

import asyncio

import aiohttp
import polars as pl
import pytest
from aiohttp import web

import config
from execution.live_features import (
    LiveFeatureFeed,
    default_live_timing,
    parse_book_snapshot,
    parse_klines,
)
from pipeline.features.base import Feature, FeatureBuilder, FeatureExpr
from pipeline.features.microstructure import default_features
from scripts.train_walkforward import build_dataset
from tests.conftest import serve
from tests.synthetic import LEVELS, MIN, START, synthetic_market
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError, DataError

STEP = 5_000  # шаг боевого цикла: снимок стакана раз в 5 с (LIVE_LOOP_INTERVAL_S)
TIMING = default_live_timing()


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True, scope="module")
def no_weight_throttle():
    """Симуляция гоняет за секунды часы боевых запросов: настоящий троттлинг веса уснул бы до следующей минуты."""
    patch = pytest.MonkeyPatch()
    patch.setattr(config, "WEIGHT_SOFT_LIMIT_1M", 10**9)
    yield
    patch.undo()


# ----------------------------------------------------------------------- имитация биржи


def rest_kline(row) -> list:
    return [
        row["open_time"], str(row["open"]), str(row["high"]), str(row["low"]), str(row["close"]), str(row["volume"]),
        row["close_time"], str(row["quote_volume"]), row["trades"], str(row["taker_buy_base"]),
        str(row["taker_buy_quote"]), "0",
    ]


def rest_depth(msg) -> dict:
    return {
        "lastUpdateId": msg["u"], "E": msg["E"], "T": msg["T"],
        "bids": [[str(msg[f"bid_px_{i}"]), str(msg[f"bid_qty_{i}"])] for i in range(LEVELS)],
        "asks": [[str(msg[f"ask_px_{i}"]), str(msg[f"ask_qty_{i}"])] for i in range(LEVELS)],
    }


class FakeMarket:
    """REST Binance поверх синтетического рынка: свечи с open_time <= now (последняя - формирующаяся) и
    последнее сообщение стакана с recv_ts <= now. Запоминает параметры запросов."""

    def __init__(self, candles: pl.DataFrame, book: pl.DataFrame):
        self.candles = candles.to_dicts()
        self.book = book.to_dicts()
        self.now = 0
        self.kline_limits: list[int] = []
        self.depth_calls = 0
        self.app = web.Application()
        self.app.router.add_get("/fapi/v1/klines", self._klines)
        self.app.router.add_get("/fapi/v1/depth", self._depth)

    async def _klines(self, request):
        limit = int(request.query["limit"])
        self.kline_limits.append(limit)
        visible = [c for c in self.candles if c["open_time"] <= self.now]
        return web.json_response([rest_kline(c) for c in visible[-limit:]])

    async def _depth(self, request):
        self.depth_calls += 1
        visible = [m for m in self.book if m["recv_ts"] <= self.now]
        return web.json_response(rest_depth(visible[-1]))


def thin(book: pl.DataFrame) -> pl.DataFrame:
    """Стакан раз в STEP, а не раз в секунду: как снимки боевого цикла."""
    return book.filter(((pl.col("recv_ts") - START - 20) % STEP) == 0)


async def drive(market: FakeMarket, feed: LiveFeatureFeed, first: int, last: int) -> list[dict]:
    """Шаг цикла каждые STEP мс в моменты прихода сообщений: [first, last]. Все отданные строки."""
    rows = []
    for now in range(first, last + 1, STEP):
        market.now = now
        rows += await feed.poll(now)
    return rows


async def with_feed(market: FakeMarket, body, **kw):
    kw.setdefault("levels", LEVELS)
    kw.setdefault("timing", TIMING)
    kw.setdefault("monotonic", lambda: 0.0)  # recv_ts == now_ms: детерминированное время получения
    async with serve(market.app) as host:
        async with aiohttp.ClientSession() as session:
            feed = LiveFeatureFeed(BinanceRest(session, f"http://{host}"), ("BTCUSDT",), **kw)
            return await body(feed)


@pytest.fixture(scope="module")
def market_data():
    candles, book = synthetic_market(240, seed=7)
    return candles, thin(book)


@pytest.fixture(scope="module")
def offline(market_data):
    candles, book = market_data
    return build_dataset(candles, book, features=default_features(taker_flow=True), horizon_bars=5, timing=TIMING)


FEED_START = START + 150 * MIN + 20  # к этому моменту в REST уже 150 свечей > lookback_bars + 1 = 120


@pytest.fixture(scope="module")
def live(market_data):
    candles, book = market_data
    market = FakeMarket(candles, book)
    end = START + 235 * MIN + 20

    async def body(feed):
        return await drive(market, feed, FEED_START, end)

    return run(with_feed(market, body)), market


# ------------------------------------------------------------------ сверка с обучением

WARMUP_ROWS = 15  # ofi_norm_15: первая корзина после старта содержит лишь 2 снимка и «пачкает» окно 15 строк


def test_live_rows_equal_offline_pipeline_rows_after_warmup(live, offline):
    """Когда буфер стакана прогрет, каждый признак каждой боевой строки совпадает с признаком бэктеста."""
    rows, _ = live
    off = {int(r["asof_ts"]): r for r in offline.decisions.iter_rows(named=True)}
    for row in rows:
        ref = off[row["asof_ts"]]  # строка с тем же asof_ts существует и в бэктесте
        assert row["open_time"] == ref["open_time"] and row["close"] == ref["close"]
    for row in rows[WARMUP_ROWS:]:
        ref = off[row["asof_ts"]]
        for name in offline.model_features:
            assert row[name] is not None, name
            assert row[name] == pytest.approx(ref[name], rel=1e-9, abs=1e-12), name
    assert len(rows) - WARMUP_ROWS >= 60  # после прогрева сверено больше часа строк


def test_warmup_gives_nulls_never_wrong_values(live, offline):
    rows, _ = live
    off = {int(r["asof_ts"]): r for r in offline.decisions.iter_rows(named=True)}
    first, ref = rows[0], off[rows[0]["asof_ts"]]
    # свечные признаки готовы сразу (история свечей из REST) и равны признакам бэктеста
    for name in ("vol_rel_30", "ret_5", "rv_30", "taker_imb_30"):
        assert first[name] == pytest.approx(ref[name], rel=1e-9), name
    # признаки последнего состояния стакана тоже точны с первой строки: корзине нужен лишь последний снимок
    for name in ("obi_1", "obi_5", "spread_bps", "microprice_dev_bps"):
        assert first[name] == pytest.approx(ref[name], rel=1e-9), name
    # а окна потока ордеров - 15 корзин по >= 2 снимка: null, пока не накоплены, а не усечённое окно
    assert first["ofi_norm_15"] is None and first["ofi_norm_5"] is None
    ofi5 = [r["ofi_norm_5"] is not None for r in rows]
    ofi15 = [r["ofi_norm_15"] is not None for r in rows]
    assert not any(ofi5[:4]) and all(ofi5[4:])
    assert not any(ofi15[:14]) and all(ofi15[14:])


def test_one_row_per_closed_candle_and_row_is_the_last_candle(live):
    rows, _ = live
    opens = [r["open_time"] for r in rows]
    assert opens == sorted(set(opens)) and len(opens) == len(rows)
    assert all(b - a == MIN for a, b in zip(opens, opens[1:]))  # ни одной пропущенной минуты
    assert all(r["symbol"] == "BTCUSDT" and r["close"] > 0 for r in rows)


# --------------------------------------------------------------------- момент выдачи


def test_row_is_withheld_until_the_decision_bucket_is_closed(market_data):
    """Решение бэктеста принимается в asof_ts = recv_ts доказательства + 1000 + 50, корзина - сообщения строго до него.

    Раньше этого момента строку отдавать нельзя: признаки были бы посчитаны по неполной корзине.
    """
    candles, book = market_data
    market = FakeMarket(candles, book)
    c = START + 160 * MIN  # закрытие свечи open_time = c - MIN
    # первый снимок с E >= c: recv = c + 5020 (E = recv - 100); asof = c + 5020 + 1000 + 50
    asof = c + 5020 + 1050

    async def body(feed):
        out = {}
        for label, now in (("before", c + 1020), ("evidence", c + 5020), ("late", c + 10020), ("again", c + 15020)):
            market.now = now
            out[label] = await feed.poll(now)
        return out

    out = run(with_feed(market, body))
    assert out["before"] == [] and out["evidence"] == []  # на снимке-доказательстве строка ещё не созрела
    (row,) = out["late"]
    assert row["asof_ts"] == asof and row["open_time"] == c - MIN
    assert out["again"] == []  # по этой свече решение уже отдано


def test_klines_are_not_requested_before_a_candle_can_be_ready(market_data):
    candles, book = market_data
    market = FakeMarket(candles, book)
    c = START + 160 * MIN

    async def body(feed):
        market.now = c + 500  # < lag + compute: строка созреть не может
        await feed.poll(c + 500)
        return list(market.kline_limits)

    assert run(with_feed(market, body)) == []
    assert market.depth_calls == 1  # а стакан копится на каждом шаге


def test_candle_dropped_by_the_pipeline_never_yields_a_stale_row(market_data):
    """Поток «слеп» при закрытии свечи -> конвейер её отбрасывает -> решения нет (а не строка прошлой свечи)."""
    candles, book = market_data
    market = FakeMarket(candles, book)
    c = START + 160 * MIN  # закрытие свечи open_time = c - MIN
    # снимков нет с c-10с до c+20с: первый с E >= c приходит через ~25 с после закрытия > gap_break_ms
    blind = pl.col("recv_ts").is_between(c - 10_000, c + 20_000)
    assert 24_000 > TIMING.gap_break_ms

    async def body(feed):
        market.book = book.filter(~blind).to_dicts()
        return await drive(market, feed, START + 150 * MIN + 20, c + MIN + 15_020)

    rows = run(with_feed(market, body))
    opens = [r["open_time"] for r in rows]
    assert c - MIN not in opens  # свеча без достоверного доказательства закрытия: решения нет
    assert c - 2 * MIN in opens and c in opens  # соседние свечи не пострадали
    assert all(r["asof_ts"] <= c + MIN + 15_020 for r in rows)  # и подмены строкой прошлой свечи тоже нет


# ---------------------------------------------------------- lookback, EWMA и размер буфера


class Ewma(Feature):
    """Признак с бесконечной памятью: фасад считает прогрев span * 10 баров."""

    name = "ewma"

    def __init__(self, span: int):
        self.span = span

    def build(self, fb: FeatureBuilder) -> dict[str, FeatureExpr]:
        close = fb.col("close")
        return {"ewma": close.ewm_mean(self.span), "roll": close.rolling_mean(self.span)}


def test_history_covers_lookback_so_ewma_and_rolling_windows_are_not_null(market_data):
    candles, book = market_data
    market = FakeMarket(candles, book)

    async def body(feed):
        assert feed.lookback_bars == 80  # EWMA: span * EWM_MEMORY_SPANS = 80 баров
        market.now = START + 200 * MIN + 20
        (row,) = await drive(market, feed, START + 200 * MIN + 20, START + 200 * MIN + 20 + 2 * STEP)
        return row

    row = run(with_feed(market, body, features=[Ewma(8)], extra_bars=2))
    assert row["ewma"] is not None and row["roll"] is not None
    # запрошено N > lookback_bars закрытых свечей (+ формирующаяся): 80 + 1 + 2 запаса + 1
    assert set(market.kline_limits) == {84}


def test_default_features_need_119_bars_and_the_feed_asks_for_more(market_data):
    candles, book = market_data
    market = FakeMarket(candles, book)

    async def body(feed):
        market.now = FEED_START
        await drive(market, feed, FEED_START, FEED_START + 2 * MIN)
        return feed.lookback_bars

    assert run(with_feed(market, body)) == 119
    assert set(market.kline_limits) == {119 + 1 + 3 + 1}


def test_too_short_history_gives_null_features_not_partial_windows(market_data):
    candles, book = market_data
    market = FakeMarket(candles.head(60), book)  # на бирже только 60 свечей < 119 + 1

    async def body(feed):
        market.now = START + 60 * MIN + 20
        return await drive(market, feed, START + 60 * MIN + 20, START + 60 * MIN + 20 + 3 * STEP)

    (row,) = run(with_feed(market, body))
    assert row["vol_rel_30"] is None and row["ret_1"] is not None  # короткое окно - да, длинное - null


# --------------------------------------------------------------- конфигурация и разбор


def test_required_features_must_be_produced_by_the_feed():
    api = BinanceRest(None, "http://x")
    LiveFeatureFeed(api, ("BTCUSDT",), levels=5, required_features=["obi_1", "taker_imb_5"])
    with pytest.raises(ConfigError, match="no_such_feature"):
        LiveFeatureFeed(api, ("BTCUSDT",), levels=5, required_features=["obi_1", "no_such_feature"])
    # модель, обученная с --no-taker-flow, работает на фиде с taker_flow (лишние колонки не мешают),
    # а наоборот - нельзя: фид без taker_flow не отдаст taker_imb_*
    with pytest.raises(ConfigError, match="taker_imb_5"):
        LiveFeatureFeed(api, ("BTCUSDT",), levels=5, features=default_features(taker_flow=False),
                        required_features=["taker_imb_5"])


def test_bad_parameters_are_config_errors(monkeypatch):
    api = BinanceRest(None, "http://x")
    for kw in ({"levels": 0}, {"levels": 2000}, {"extra_bars": -1}, {"features": [Ewma(200)]}):
        with pytest.raises(ConfigError):
            LiveFeatureFeed(api, ("BTCUSDT",), **kw)  # Ewma(200): память 2000 баров > лимита klines
    with pytest.raises(ConfigError):
        LiveFeatureFeed(api, ())
    monkeypatch.setattr(config, "LIVE_FEED_INTERVAL", "1h")  # модели обучены на 1m
    with pytest.raises(ConfigError, match="1m"):
        LiveFeatureFeed(api, ("BTCUSDT",))


def test_default_timing_widens_only_gap_break():
    t = default_live_timing()
    assert t.gap_break_ms > 5_000 and t.gap_break_ms < config.INTERVAL_MS["1m"]
    assert (t.candle_publish_lag_ms, t.decision_compute_ms, t.book_max_staleness_ms) == (1_000, 50, 2_000)


def kline_row(i, close="10"):
    open_time = START + i * MIN
    return [open_time, "9", "11", "8", close, "5", open_time + MIN - 1, "50", 7, "2", "20", "0"]


def test_parse_klines_keeps_only_closed_candles_in_the_kline_schema():
    frame = parse_klines([kline_row(0), kline_row(1), kline_row(2)], "BTCUSDT", START + 2 * MIN + 5)
    assert frame.columns[0] == "symbol" and frame.height == 2  # третья ещё формируется
    assert frame["open_time"].to_list() == [START, START + MIN] and frame["taker_buy_base"].to_list() == [2.0, 2.0]
    assert parse_klines([], "BTCUSDT", START).height == 0


@pytest.mark.parametrize("raw", [
    {}, None, [[1, 2]], [kline_row(0, "x")], [kline_row(0, "-5")], [kline_row(0, "nan")],
    [kline_row(1), kline_row(0)], [kline_row(0)[:8]],
])
def test_garbage_klines_are_data_error(raw):
    with pytest.raises(DataError):
        parse_klines(raw, "BTCUSDT", START + 10 * MIN)


def depth(bids, asks, **kw):
    return {"lastUpdateId": 5, "E": 1_000, "T": 999, "bids": bids, "asks": asks, **kw}


def test_parse_book_snapshot_builds_a_depth_schema_row():
    row = parse_book_snapshot(depth([["99", "3"], ["98", "1"]], [["101", "2"]]), "BTCUSDT", 1_050, 3)
    assert (row["recv_ts"], row["E"], row["T"], row["u"]) == (1_050, 1_000, 999, 5)
    assert (row["bid_px_0"], row["bid_qty_1"], row["ask_px_0"], row["ask_qty_0"]) == (99.0, 1.0, 101.0, 2.0)
    assert row["bid_px_2"] is None and row["ask_px_1"] is None  # уровней меньше запрошенных - null


@pytest.mark.parametrize("raw", [
    depth([], [["101", "1"]]), depth([["99", "1"]], []),  # пустая сторона
    depth([["101", "1"]], [["100", "1"]]), depth([["100", "1"]], [["100", "1"]]),  # пересечён
    depth([["0", "1"]], [["101", "1"]]), depth([["99", "-1"]], [["101", "1"]]),  # цена <= 0, объём < 0
    depth([["99", "inf"]], [["101", "1"]]),
])
def test_unusable_books_are_rejected_like_the_loader_quarantine(raw):
    assert parse_book_snapshot(raw, "BTCUSDT", 1, 5) is None


@pytest.mark.parametrize("raw", [
    None, [], {}, depth([["x", "1"]], [["101", "1"]]), depth([["99"]], [["101", "1"]]),
    {"bids": [["99", "1"]], "asks": [["101", "1"]]},  # нет E и lastUpdateId
    depth([["99", "1"]], [["101", "1"]], E="1"), depth("x", []),
])
def test_garbage_book_is_data_error(raw):
    with pytest.raises(DataError):
        parse_book_snapshot(raw, "BTCUSDT", 1, 5)


def test_duplicate_update_id_is_not_buffered_twice(market_data):
    """Стакан не менялся (тот же lastUpdateId) - дубль, как duplicate_u у DepthLoader."""
    candles, book = market_data
    market = FakeMarket(candles, book)

    async def body(feed):
        market.now = START + 10 * MIN + 20
        await feed.poll(market.now)
        await feed.poll(market.now + 1)  # то же сообщение стакана
        return len(feed._books["BTCUSDT"])

    assert run(with_feed(market, body)) == 1


def test_book_buffer_is_trimmed_to_the_loaded_history(market_data):
    candles, book = market_data
    market = FakeMarket(candles, book)

    async def body(feed):
        assert feed.lookback_bars == 30
        await drive(market, feed, START + 5 * MIN + 20, START + 90 * MIN + 20)
        buf = feed._books["BTCUSDT"]
        return buf[-1]["recv_ts"] - buf[0]["recv_ts"], feed._retention_ms

    span, retention = run(with_feed(market, body, features=[Ewma(3)]))
    assert retention == (30 + 1 + 3 + 1) * MIN
    assert span <= retention and span > retention - 2 * MIN  # хранится вся история свечей, но не больше
