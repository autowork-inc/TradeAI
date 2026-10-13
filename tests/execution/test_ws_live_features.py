"""WsLiveFeatureFeed: потоки 500 мс вместо REST-снимков = признаки в бою совпадают с признаками обучения.

Главный тест (паритет) - те же сырые сообщения стакана и свечи гоняются (а) офлайн: jsonl -> DepthLoader ->
DataQualityGate -> build_dataset, как в train_walkforward, и (б) через фид: сообщение за сообщением, в порядке
прихода, с событиями закрытия свечи. Признаки строк совпадают. Прогон повторён со сдвигом локальных часов на минуту
(на реальной машине замерено именно такое расхождение с биржей): решение запускается событием закрытия свечи, а не
«ожидаемой минутой по часам хоста».

Остальное - протокол на локальном WS-сервере: адреса и подписки, что REST не опрашивается, переподключение,
исчерпание попыток, мусор и serverShutdown.
"""

import asyncio
import json
import math

import aiohttp
import polars as pl
import pytest
from aiohttp import web

import config
from execution.live_features import LiveFeatureFeed, default_live_timing, parse_klines
from execution.ws_live_features import WsLiveFeatureFeed, decode_frame
from pipeline.features.microstructure import default_features
from pipeline.loaders.depth_loader import DepthLoader
from pipeline.loaders.klines_loader import KlinesLoader
from pipeline.quality.gate import DataQualityGate
from pipeline.types import TimingConfig
from scripts.generate_synthetic_history import MINUTE_MS, generate_symbol, write_history
from scripts.train_walkforward import build_dataset
from tests.conftest import serve
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError, DataError, ServerShutdown, WebSocketClosed

START = 1_790_000_000_000 // MINUTE_MS * MINUTE_MS
MINUTES = 200
LEVELS = 10


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True, scope="module")
def no_weight_throttle():
    patch = pytest.MonkeyPatch()
    patch.setattr(config, "WEIGHT_SOFT_LIMIT_1M", 10**9)
    yield
    patch.undo()


def kline_event(symbol: str, row: dict, *, closed: bool = True) -> dict:
    return {"e": "kline", "E": row["close_time"] + 30, "s": symbol, "k": {
        "t": row["open_time"], "T": row["close_time"], "s": symbol, "i": "1m", "o": str(row["open"]),
        "c": str(row["close"]), "h": str(row["high"]), "l": str(row["low"]), "v": str(row["volume"]),
        "n": row["trades"], "x": closed, "q": str(row["quote_volume"]), "V": str(row["taker_buy_base"]),
        "Q": str(row["taker_buy_quote"]),
    }}


# ------------------------------------------------------------------------------ паритет с обучением


@pytest.fixture(scope="module")
def market(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("parity")
    hist = generate_symbol("BTCUSDT", START, MINUTES, seed=5, levels=LEVELS)
    end = START + MINUTES * MINUTE_MS
    write_history([hist], tmp / "raw", START, end)

    timing = TimingConfig()
    depth = DepthLoader(raw_root=tmp / "raw" / "depth", interim_root=tmp / "interim", levels=LEVELS)
    depth.normalize_range("BTCUSDT", START, end)
    book = DataQualityGate(timing).apply(depth.scan("BTCUSDT", START, end)).clean.collect()
    candles = KlinesLoader(tmp / "raw" / "klines").scan("BTCUSDT", "1m", START, end).collect()
    offline = build_dataset(
        candles, book, features=default_features(taker_flow=True), horizon_bars=5, timing=timing
    ).decisions
    return hist, offline


async def replay(hist, *, offset_ms: int, first_minute: int = 0) -> dict[int, dict]:
    """Прогон по минутам: сообщения - по времени получения, событие закрытия свечи, poll после созревания строки."""
    now = [0]
    feed = WsLiveFeatureFeed(None, None, ("BTCUSDT",), autostart=False, clock_ms=lambda: now[0])
    messages = [json.loads(line) for _, line in hist.depth_lines]
    messages = [m for m in messages if m["stream"] != "snapshot"]
    pos, rows = 0, {}

    def deliver(until: int) -> None:
        nonlocal pos
        while pos < len(messages) and messages[pos]["recv_ts"] + offset_ms <= until:
            m = messages[pos]
            feed.on_depth_message(m["symbol"], m["data"], m["recv_ts"] + offset_ms)
            pos += 1

    for candle in hist.candles.iter_rows(named=True):
        close = candle["close_time"] + 1
        deliver(close + 30 + offset_ms)
        now[0] = close + 30 + offset_ms
        feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle), close + 30 + offset_ms)
        deliver(close + 2_500 + offset_ms)
        now[0] = close + 2_500 + offset_ms  # строка созрела: asof_ts = recv_ts доказательства + 1 с + 50 мс
        for row in await feed.poll(0):
            rows[row["open_time"]] = row
    return rows, feed


@pytest.mark.parametrize("offset_ms", [0, -60_000, 60_000], ids=["часы_верны", "хост_отстаёт_на_минуту", "хост_спешит_на_минуту"])
def test_live_rows_equal_training_rows(market, offset_ms):
    hist, offline = market
    live, feed = run(replay(hist, offset_ms=offset_ms))
    assert feed.stats()["depth_rejected"] == 0 and feed.stats()["depth_duplicate"] == 0

    names = feed.feature_names
    reference = {r["open_time"]: r for r in offline.iter_rows(named=True)}
    compared = 0
    for open_time, row in live.items():
        if open_time < START + 150 * MINUTE_MS:  # окна признаков (120 баров) у фида и офлайна прогреты одинаково
            continue
        ref = reference[open_time]
        assert row["asof_ts"] - offset_ms == ref["asof_ts"]  # то же решение в тот же момент (в шкале recv_ts)
        for name in (*names, "close", "book_n_msgs"):
            a, b = row[name], ref[name]
            assert (a is None) == (b is None), name
            if a is not None:
                assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12), (open_time, name, a, b)
        compared += 1
    assert compared >= 40
    # сравнение содержательное: поток ордеров (ofi_norm_15) посчитан, а не пустые null на обеих сторонах
    assert all(live[t]["ofi_norm_15"] is not None for t in live if t >= START + 150 * MINUTE_MS)


def test_ws_feed_has_same_message_cadence_as_training_data(market):
    """Сообщения подаются как есть (~500 мс): в корзине минуты ~120 сообщений, а не 12 снимков по REST."""
    hist, _ = market
    live, _ = run(replay(hist, offset_ms=0))
    late = [r for t, r in live.items() if t >= START + 150 * MINUTE_MS]
    assert all(100 <= r["book_n_msgs"] <= 125 for r in late)
    rest_snapshots_per_minute = 60_000 // int(config.LIVE_LOOP_INTERVAL_S * 1000)
    assert min(r["book_n_msgs"] for r in late) > 5 * rest_snapshots_per_minute


def test_rest_feed_buckets_are_sparse_so_the_skew_was_real():
    """Контроль: у REST-фида timing расширен (gap 15 с), у WS-фида - тренировочный."""
    assert WsLiveFeatureFeed(None, None)._timing == TimingConfig()
    assert default_live_timing().gap_break_ms > TimingConfig().gap_break_ms


# ---------------------------------------------------------------------------- разбор сообщений


def depth_data(symbol="BTCUSDT", u=100, bids=None, asks=None, **extra):
    bids = [["100.0", "1.0"], ["99.9", "2.0"]] if bids is None else bids
    asks = [["100.1", "1.5"], ["100.2", "2.5"]] if asks is None else asks
    return {"e": "depthUpdate", "E": 1_000, "T": 999, "s": symbol, "U": u - 5, "u": u, "pu": u - 10, "b": bids, "a": asks, **extra}


def new_feed(**kw):
    kw.setdefault("autostart", False)
    return WsLiveFeatureFeed(None, None, ("BTCUSDT",), **kw)


def test_depth_message_becomes_a_schema_row():
    feed = new_feed()
    assert feed.on_depth_message("BTCUSDT", depth_data(u=100), recv_ts=1_050)
    (row,) = feed._books["BTCUSDT"]
    assert (row["recv_ts"], row["E"], row["T"], row["u"], row["pu"]) == (1_050, 1_000, 999, 100, 90)
    assert (row["bid_px_0"], row["bid_qty_1"], row["ask_px_1"]) == (100.0, 2.0, 100.2)
    assert row["bid_px_5"] is None  # уровней меньше, чем DEPTH_LEVELS: как у DepthLoader - null


@pytest.mark.parametrize("bad", [
    depth_data(bids=[]), depth_data(asks=[]),
    depth_data(bids=[["100.2", "1"]], asks=[["100.1", "1"]]),  # пересечён
    depth_data(bids=[["0", "1"]]), depth_data(bids=[["100", "-1"]]), depth_data(bids=[["x", "1"]]),
])
def test_bad_book_is_dropped_like_the_loader_quarantine(bad):
    feed = new_feed()
    assert feed.on_depth_message("BTCUSDT", bad, 1_050) is False
    assert not feed._books["BTCUSDT"] and feed.stats()["depth_rejected"] == 1


def test_duplicate_update_id_is_dropped_and_first_wins():
    feed = new_feed()
    assert feed.on_depth_message("BTCUSDT", depth_data(u=100), 1_050)
    assert not feed.on_depth_message("BTCUSDT", depth_data(u=100), 1_550)
    assert feed.on_depth_message("BTCUSDT", depth_data(u=101), 1_600)
    assert [m["recv_ts"] for m in feed._books["BTCUSDT"]] == [1_050, 1_600] and feed.stats()["depth_duplicate"] == 1


@pytest.mark.parametrize("data", [
    {"s": "BTCUSDT", "E": 1, "u": 1, "b": "x", "a": []}, {"E": 1}, depth_data(symbol="ETHUSDT"),
])
def test_structural_garbage_is_fatal(data):
    with pytest.raises(DataError):
        new_feed().on_depth_message("BTCUSDT", data, 1_050)


def test_best_quote_is_the_last_book_message_and_goes_stale():
    """Котировка для Smart Execution: лучшие bid/ask последнего сообщения; старше LIVE_QUOTE_MAX_AGE_MS - None."""
    now = [1_050]
    feed = new_feed(clock_ms=lambda: now[0])
    assert feed.best_quote("BTCUSDT") is None  # сообщений ещё нет
    feed.on_depth_message("BTCUSDT", depth_data(u=100, bids=[["100.0", "1"]], asks=[["100.1", "1"]]), recv_ts=1_050)
    feed.on_depth_message("BTCUSDT", depth_data(u=101, bids=[["100.2", "1"]], asks=[["100.3", "1"]]), recv_ts=1_550)
    now[0] = 1_600
    assert feed.best_quote("BTCUSDT") == (100.2, 100.3)  # последнее, а не первое
    now[0] = 1_550 + config.LIVE_QUOTE_MAX_AGE_MS
    assert feed.best_quote("BTCUSDT") == (100.2, 100.3)  # ровно на границе возраста ещё годится
    now[0] += 1
    assert feed.best_quote("BTCUSDT") is None  # поток встал: пассивный ордер по такой цене не ставим
    assert feed.best_quote("BTCUSDT", max_age_ms=10_000) == (100.2, 100.3)


def test_buffer_is_trimmed_to_the_history_window():
    feed = new_feed()
    for i in range(5):
        feed.on_depth_message("BTCUSDT", depth_data(u=100 + i), i * (feed._retention_ms // 2))
    assert [m["u"] for m in feed._books["BTCUSDT"]] == [102, 103, 104]


def test_only_closed_klines_are_stored_and_trigger_a_decision():
    feed = new_feed()
    candle = dict(open_time=60_000, close_time=119_999, open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0,
                  trades=7, quote_volume=4.0, taker_buy_base=1.0, taker_buy_quote=1.4)
    assert feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle, closed=False), 10) is False
    assert not feed._candles["BTCUSDT"] and "BTCUSDT" not in feed._closed_event
    assert feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle), 120_040) is True
    assert list(feed._candles["BTCUSDT"]) == [60_000] and feed._closed_event["BTCUSDT"] == (60_000, 120_040)
    frame = feed._candle_frame("BTCUSDT")
    assert frame.columns[0] == "symbol" and frame["close"].to_list() == [1.5]


def test_same_candle_from_rest_and_stream_is_one_record():
    feed = new_feed()
    candle = dict(open_time=60_000, close_time=119_999, open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0,
                  trades=7, quote_volume=4.0, taker_buy_base=1.0, taker_buy_quote=1.4)
    feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle), 1)
    feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle), 2)
    assert len(feed._candles["BTCUSDT"]) == 1


VOLUME_COLUMNS = ("volume", "taker_buy_base")


def assert_volume_columns_filled(frame: pl.DataFrame, volume: list[float], taker_buy: list[float]) -> None:
    """Объёмы свечи дошли до кадра фида числами (Float64), а не null: от них зависят vol_rel_* и taker_imb_*."""
    for name in VOLUME_COLUMNS:
        assert frame[name].dtype == pl.Float64, name
        assert frame[name].null_count() == 0, name
    assert frame["volume"].to_list() == volume
    assert frame["taker_buy_base"].to_list() == taker_buy


def test_kline_message_volumes_are_parsed_to_non_null_floats():
    """WS-кадр @kline_1m: k.v (объём) и k.V (объём тейкеров-покупателей) приходят строками -> volume и taker_buy_base."""
    feed = new_feed()
    for t, (v, big_v) in enumerate([("3.5", "1.25"), ("0.00000000", "0.00000000"), ("12", "7")]):
        c = dict(open_time=60_000 * (t + 1), close_time=60_000 * (t + 1) + 59_999, open=1.0, high=2.0, low=0.5,
                 close=1.5, volume=0.0, trades=7, quote_volume=4.0, taker_buy_base=0.0, taker_buy_quote=1.4)
        event = kline_event("BTCUSDT", c)
        event["k"].update(v=v, V=big_v)  # как на бирже: числа - строками
        assert feed.on_kline_message("BTCUSDT", event, 120_040 + 60_000 * t)
    assert_volume_columns_filled(feed._candle_frame("BTCUSDT"), [3.5, 0.0, 12.0], [1.25, 0.0, 7.0])


@pytest.mark.parametrize("field", ["v", "V"])
@pytest.mark.parametrize("bad", ["abc", "nan", None])
def test_kline_message_with_unreadable_volume_is_fatal_not_a_silent_null(field, bad):
    """Нечитаемый объём - DataError, а не свеча с null: иначе vol_rel_* молча стали бы null навсегда."""
    candle = dict(open_time=60_000, close_time=119_999, open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0,
                  trades=7, quote_volume=4.0, taker_buy_base=1.0, taker_buy_quote=1.4)
    event = kline_event("BTCUSDT", candle)
    event["k"][field] = bad
    feed = new_feed()
    with pytest.raises(DataError):
        feed.on_kline_message("BTCUSDT", event, 1)
    assert not feed._candles["BTCUSDT"]


def test_rest_history_volumes_are_cast_to_non_null_floats():
    """/fapi/v1/klines отдаёт объёмы строками (индексы 5 и 9): parse_klines обязан привести их к float."""
    raw = [[60_000 * i, "1", "2", "0.5", "1.5", "3.5", 60_000 * i + 59_999, "4", 7, "1.25", "1.4", "0"] for i in range(3)]
    feed = new_feed()
    feed._store_candles("BTCUSDT", parse_klines(raw, "BTCUSDT", 10**9))
    assert_volume_columns_filled(feed._candle_frame("BTCUSDT"), [3.5] * 3, [1.25] * 3)


def test_bootstrap_loads_non_null_volumes_from_rest_before_the_first_stream_candle():
    fake = FakeBinance()

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: feed.stats()["klines_closed"] >= 1)
        frame = feed._candle_frame("BTCUSDT")
        assert frame.height >= feed._history  # история прогрева загружена целиком
        for name in VOLUME_COLUMNS:
            assert frame[name].dtype == pl.Float64 and frame[name].null_count() == 0, name
        assert frame["volume"].min() > 0 and frame["taker_buy_base"].min() >= 0

    run(with_ws_feed(fake, body))


async def replay_warm_start(hist, *, stream_from: int, outage: tuple[int, int] | None = None) -> list[dict]:
    """Боевой старт: свечи до минуты stream_from - история REST, дальше только поток (сообщения стакана и закрытия
    свечей). outage = (с какой минуты потока, сколько минут) стакан не приходит (обрыв сети). Строки решений по порядку."""
    now = [0]
    feed = WsLiveFeatureFeed(None, None, ("BTCUSDT",), autostart=False, clock_ms=lambda: now[0])
    candles = list(hist.candles.iter_rows(named=True))
    t0 = START + stream_from * MINUTE_MS
    raw = [[c["open_time"], str(c["open"]), str(c["high"]), str(c["low"]), str(c["close"]), str(c["volume"]),
            c["close_time"], str(c["quote_volume"]), c["trades"], str(c["taker_buy_base"]),
            str(c["taker_buy_quote"]), "0"] for c in candles[:stream_from]]
    feed._store_candles("BTCUSDT", parse_klines(raw, "BTCUSDT", t0))
    messages = [json.loads(line) for _, line in hist.depth_lines]
    messages = [m for m in messages if m["stream"] != "snapshot" and m["recv_ts"] >= t0]
    if outage is not None:
        lost_from, lost_to = t0 + outage[0] * MINUTE_MS + 5_000, t0 + (outage[0] + outage[1]) * MINUTE_MS + 20_000
        messages = [m for m in messages if not lost_from <= m["recv_ts"] < lost_to]
    pos, rows = 0, []

    def deliver(until: int) -> None:
        nonlocal pos
        while pos < len(messages) and messages[pos]["recv_ts"] <= until:
            feed.on_depth_message(messages[pos]["symbol"], messages[pos]["data"], messages[pos]["recv_ts"])
            pos += 1

    for candle in candles[stream_from:]:
        close = candle["close_time"] + 1
        deliver(close + 30)
        now[0] = close + 30
        feed.on_kline_message("BTCUSDT", kline_event("BTCUSDT", candle), close + 30)
        deliver(close + 2_500)
        now[0] = close + 2_500
        rows += await feed.poll(0)
    return rows


def test_volume_features_are_non_null_from_the_first_row_after_a_warm_start(market):
    """Сценарий боя: 150 свечей REST + поток с нуля. Объёмные признаки готовы сразу, ждёт только поток ордеров."""
    hist, _ = market
    rows = run(replay_warm_start(hist, stream_from=150))
    assert len(rows) >= 40
    for row in rows:
        assert row["volume"] is not None and row["taker_buy_base"] is not None
        for name in ("vol_sum_5", "vol_rel_5", "vol_rel_30", "taker_imb_5", "taker_imb_30"):
            assert row[name] is not None and math.isfinite(row[name]), (row["open_time"], name)
    assert all(r["ofi_norm_15"] is None for r in rows[:5])  # прогрев потока ордеров - штатный (~15 минут)
    assert all(r["ofi_norm_15"] is not None for r in rows[20:])


def test_book_outage_blanks_windowed_volume_features_but_not_the_volumes_themselves():
    """Минута без стакана выбивает свечу из таблицы решений, окна признаков рвутся (segment_id): vol_rel_*/taker_imb_*
    после неё null (так же, как в обучении), но сами volume и taker_buy_base из потока и REST на месте."""
    hist = generate_symbol("BTCUSDT", START, MINUTES, seed=5, levels=LEVELS)
    rows = run(replay_warm_start(hist, stream_from=150, outage=(20, 1)))
    before = [r for r in rows if r["open_time"] < START + 169 * MINUTE_MS]
    after = [r for r in rows if r["open_time"] >= START + 173 * MINUTE_MS]
    assert before and after
    assert all(r["vol_rel_30"] is not None for r in before)
    assert all(r["vol_rel_30"] is None and r["vol_rel_5"] is None for r in after)  # окно в 120 баров ещё не набралось
    assert all(r["volume"] is not None and r["taker_buy_base"] is not None for r in rows)


@pytest.mark.parametrize("mutate", [
    lambda e: e["k"].update(s="ETHUSDT"), lambda e: e["k"].update(i="5m"), lambda e: e["k"].pop("c"),
    lambda e: e["k"].update(c="nan"), lambda e: e["k"].update(c="-1"), lambda e: e.update(k=None),
])
def test_kline_garbage_is_fatal(mutate):
    candle = dict(open_time=60_000, close_time=119_999, open=1.0, high=2.0, low=0.5, close=1.5, volume=3.0,
                  trades=7, quote_volume=4.0, taker_buy_base=1.0, taker_buy_quote=1.4)
    event = kline_event("BTCUSDT", candle)
    mutate(event)
    with pytest.raises(DataError):
        new_feed().on_kline_message("BTCUSDT", event, 1)


def test_decode_frame_rejects_foreign_streams_garbage_and_shutdown():
    expected = {"btcusdt@kline_1m": "BTCUSDT"}
    assert decode_frame(json.dumps({"stream": "btcusdt@kline_1m", "data": {"k": {}}}), expected) == ("BTCUSDT", {"k": {}})
    for raw in ("не json", "[]", json.dumps({"stream": "ethusdt@kline_1m", "data": {}}),
                json.dumps({"stream": "btcusdt@kline_1m", "data": []})):
        with pytest.raises(DataError):
            decode_frame(raw, expected)
    with pytest.raises(ServerShutdown):
        decode_frame(json.dumps({"e": "serverShutdown"}), expected)


def test_constructor_validates_levels_features_and_stays_offline():
    with pytest.raises(ConfigError, match="@depth"):
        new_feed(levels=7)
    with pytest.raises(ConfigError, match="no_such_feature"):
        new_feed(required_features=("no_such_feature",))
    assert isinstance(new_feed(), LiveFeatureFeed)  # тот же конвейер, что у REST-фида
    with pytest.raises(ConfigError, match="api и session"):
        WsLiveFeatureFeed(None, None).start()


def test_market_route_is_separate_from_public_depth_route():
    """Проверено вживую: /public/stream игнорирует подписку на kline, свечи отдаёт только /market/stream."""
    assert config.WS_BASE_URL.endswith("/public/stream") and config.WS_MARKET_URL.endswith("/market/stream")
    assert config.WS_MARKET_URL.split("/market")[0] == config.WS_BASE_URL.split("/public")[0]


# ------------------------------------------------------------------------ сокеты: локальный WS-сервер


class FakeBinance:
    """/public/stream (depth) и /market/stream (kline) + REST /fapi/v1/time и /fapi/v1/klines. REST depth НЕ определён:
    обращение к нему дало бы 404, то есть тест упадёт, если фид вернётся к REST-опросу стакана."""

    def __init__(self):
        self.connections = {"public": 0, "market": 0}
        self.queries: dict[str, str] = {}
        self.klines_calls: list[dict] = []
        self.on_connect = {"public": self._depth_script, "market": self._market_script}
        self.app = web.Application()
        self.app.router.add_get("/public/stream", self._public)
        self.app.router.add_get("/market/stream", self._market)
        self.app.router.add_get("/fapi/v1/time", self._time)
        self.app.router.add_get("/fapi/v1/klines", self._klines)

    async def _public(self, request):
        return await self._ws("public", request)

    async def _market(self, request):
        return await self._ws("market", request)

    async def _ws(self, kind, request):
        self.connections[kind] += 1
        self.queries[kind] = request.query_string
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await self.on_connect[kind](ws, self.connections[kind])
        return ws

    @staticmethod
    async def _depth_script(ws, n):
        for i in range(3):
            await ws.send_str(json.dumps({"stream": "btcusdt@depth10@500ms", "data": depth_data(u=1000 * n + i)}))
        await asyncio.sleep(0.5)

    @staticmethod
    async def _market_script(ws, n):
        c = dict(open_time=60_000 * n, close_time=60_000 * n + 59_999, open=1.0, high=2.0, low=0.5, close=1.5,
                 volume=3.0, trades=7, quote_volume=4.0, taker_buy_base=1.0, taker_buy_quote=1.4)
        await ws.send_str(json.dumps({"stream": "btcusdt@kline_1m", "data": kline_event("BTCUSDT", c, closed=False)}))
        await ws.send_str(json.dumps({"stream": "btcusdt@kline_1m", "data": kline_event("BTCUSDT", c)}))
        await asyncio.sleep(0.5)

    async def _time(self, request):
        return web.json_response({"serverTime": 10 * 60_000 * 60})

    async def _klines(self, request):
        self.klines_calls.append(dict(request.query))
        return web.json_response([
            [i * 60_000, "1", "2", "0.5", "1.5", "3", i * 60_000 + 59_999, "4", 7, "1", "1.4", "0"]
            for i in range(int(request.query["limit"]))
        ])


async def with_ws_feed(fake: FakeBinance, body, **kw):
    kw.setdefault("backoff_s", 0.01)
    kw.setdefault("backoff_max_s", 0.02)
    async with serve(fake.app) as host:
        async with aiohttp.ClientSession() as session:
            feed = WsLiveFeatureFeed(
                BinanceRest(session, f"http://{host}"), session, ("BTCUSDT",),
                depth_url=f"ws://{host}/public/stream", market_url=f"ws://{host}/market/stream", **kw,
            )
            try:
                return await body(feed)
            finally:
                await feed.aclose()


async def settle(predicate, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def test_streams_subscriptions_history_bootstrap_and_no_rest_depth():
    fake = FakeBinance()

    async def body(feed):
        assert await feed.poll(0) == []  # первый poll запускает потоки, сети не ждёт
        await settle(lambda: feed.stats()["depth_ok"] >= 3 and feed.stats()["klines_closed"] >= 1)
        assert fake.queries == {"public": "streams=btcusdt@depth10@500ms", "market": "streams=btcusdt@kline_1m"}
        stats = feed.stats()
        assert stats["book_buffer"] == {"BTCUSDT": 3}
        assert len(feed._candles["BTCUSDT"]) >= 2 and feed._closed_event["BTCUSDT"][0] == 60_000  # формирующаяся - нет
        # история свечей - один раз на подключение, REST depth не трогали (его маршрута нет - был бы HttpClientError)
        assert [c["limit"] for c in fake.klines_calls] == [str(feed._history + 1)]
        assert fake.klines_calls[0]["symbol"] == "BTCUSDT" and fake.klines_calls[0]["interval"] == "1m"
        assert feed.clock_offset_ms is not None

    run(with_ws_feed(fake, body))


def test_depth_stream_reconnects_and_keeps_buffering():
    fake = FakeBinance()

    async def short_then_long(ws, n):
        for i in range(2):
            await ws.send_str(json.dumps({"stream": "btcusdt@depth10@500ms", "data": depth_data(u=1000 * n + i)}))
        if n == 1:
            await ws.close()  # обрыв
        else:
            await asyncio.sleep(1)

    fake.on_connect["public"] = short_then_long

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: feed.stats()["reconnects"]["depth"] == 1 and feed.stats()["depth_ok"] >= 4)
        assert fake.connections["public"] >= 2
        assert [m["u"] for m in feed._books["BTCUSDT"]] == [1000, 1001, 2000, 2001]  # буфер пережил обрыв
        await feed.poll(0)  # потоки живы: исключения нет

    run(with_ws_feed(fake, body))


def test_market_reconnect_reloads_candle_history_from_rest():
    fake = FakeBinance()

    async def drop_first(ws, n):
        await FakeBinance._market_script(ws, n) if n > 1 else await ws.close()

    fake.on_connect["market"] = drop_first

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: feed.stats()["reconnects"]["market"] >= 1 and feed.stats()["klines_closed"] >= 1)
        assert len(fake.klines_calls) >= 2  # пропущенные при обрыве свечи догружаются по REST после каждого подключения

    run(with_ws_feed(fake, body))


def test_server_shutdown_frame_triggers_reconnect_not_failure():
    fake = FakeBinance()

    async def shutdown_then_ok(ws, n):
        if n == 1:
            await ws.send_str(json.dumps({"e": "serverShutdown"}))
            await asyncio.sleep(0.2)
        else:
            await FakeBinance._depth_script(ws, n)

    fake.on_connect["public"] = shutdown_then_ok

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: feed.stats()["reconnects"]["depth"] == 1 and feed.stats()["depth_ok"] >= 3)
        await feed.poll(0)

    run(with_ws_feed(fake, body))


def test_exhausted_reconnects_surface_through_poll():
    fake = FakeBinance()

    async def close_at_once(ws, n):
        await ws.close()

    fake.on_connect["public"] = close_at_once

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: any(t.done() for t in feed._tasks))
        with pytest.raises(WebSocketClosed, match="не восстановился за 2"):
            await feed.poll(0)  # live_run выходит кодом 1: fail-fast вместо молчаливо мёртвого фида

    run(with_ws_feed(fake, body, max_reconnects=2))


def test_garbage_from_the_stream_is_fatal_through_poll():
    fake = FakeBinance()

    async def garbage(ws, n):
        await ws.send_str("это не json")
        await asyncio.sleep(0.5)

    fake.on_connect["public"] = garbage

    async def body(feed):
        await feed.poll(0)
        await settle(lambda: any(t.done() for t in feed._tasks))
        with pytest.raises(DataError):
            await feed.poll(0)

    run(with_ws_feed(fake, body))


def test_aclose_stops_the_background_tasks():
    fake = FakeBinance()

    async def body(feed):
        await feed.poll(0)
        tasks = list(feed._tasks)
        assert tasks and not any(t.done() for t in tasks)
        await feed.aclose()
        assert all(t.done() for t in tasks) and feed._tasks == []

    run(with_ws_feed(fake, body))
