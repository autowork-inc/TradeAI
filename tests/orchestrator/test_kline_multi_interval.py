"""SmcKlineFeed на интервалах мульти-таймфреймового режима (5m, 1h, 4h, 1d): один экземпляр на интервал, состояние старших - `emit_latest_on_start`, MultiFeed склеивает."""

import asyncio

import polars as pl
import pytest

import config
from orchestrator.feature_engine import FeatureEngine
from orchestrator.kline_feed import MultiFeed, SmcKlineFeed
from tests.features.test_smc import random_walk
from utils.errors import DataError

SYM = "BTCUSDT"


@pytest.fixture(autouse=True)
def no_weight_throttle(monkeypatch):
    monkeypatch.setattr(config, "WEIGHT_SOFT_LIMIT_1M", 10**9)


def candles(interval: str, n: int = 140) -> tuple[pl.DataFrame, int]:
    iv = config.INTERVAL_MS[interval]
    return random_walk(n, 5, 0.01).with_columns((pl.int_range(pl.len(), dtype=pl.Int64) * iv).alias("open_time")), iv


class Api:
    def __init__(self, interval: str, closed: int):
        self.interval, self.closed = interval, closed
        self.df, self.iv = candles(interval, closed + 2)

    async def server_time_ms(self) -> int:
        return self.closed * self.iv + 5_000

    async def get(self, path, params, weight=1):
        assert path == config.KLINES_PATH and params["interval"] == self.interval  # запрашивается именно свой интервал
        hi = self.closed + 1
        return [self.raw(i) for i in range(max(0, hi - int(params["limit"])), hi)]

    def raw(self, i: int) -> list:
        r = self.df.row(i, named=True)
        return [r["open_time"], str(r["open"]), str(r["high"]), str(r["low"]), str(r["close"]), str(r["volume"]), r["open_time"] + self.iv - 1, "1", 10, "0.5", "0.5", "0"]

    def msg(self, i: int, interval: str | None = None, closed: bool = True) -> dict:
        r = self.df.row(i, named=True)
        return {"e": "kline", "s": SYM, "k": {"t": r["open_time"], "T": r["open_time"] + self.iv - 1, "s": SYM, "i": interval or self.interval, "o": str(r["open"]),
                                               "c": str(r["close"]), "h": str(r["high"]), "l": str(r["low"]), "v": str(r["volume"]), "n": 10, "x": closed, "q": "1",
                                               "V": "0.5", "Q": "0.5"}}


@pytest.mark.parametrize("interval, min_bars, history", [("5m", 100, 120), ("1h", 100, 120), ("4h", 100, 120), ("1d", 60, 90)])
def test_every_interval_bootstraps_its_own_history_and_emits_new_closed_bars(interval, min_bars, history):
    async def body():
        api = Api(interval, 130)
        feed = SmcKlineFeed(api, None, FeatureEngine(min_bars=min_bars), symbols=(SYM,), interval=interval, history_bars=history, clock_ms=lambda: 1)  # type: ignore[arg-type]
        await feed._bootstrap()
        assert await feed.poll() == []  # история на старте не эмитится
        assert feed.on_kline_message(SYM, api.msg(130), 7) is True
        (ctx,) = await feed.poll()
        assert ctx.interval == interval and ctx.interval_ms == api.iv and ctx.bar_open_time == 130 * api.iv and ctx.frame.height == history
        with pytest.raises(DataError, match="Неожиданное сообщение"):
            feed.on_kline_message(SYM, api.msg(131, interval="15m" if interval != "15m" else "5m"), 8)  # свеча чужого интервала в потоке - ошибка
        feed.check()  # потока нет (тест без сети), но проверка не падает

    asyncio.run(body())


def test_state_feed_emits_the_latest_closed_bar_once_at_start_and_multi_feed_merges_feeds():
    async def body():
        state = {}
        for interval, history, mb in (("1h", 120, 100), ("1d", 90, 60)):
            api = Api(interval, 130)
            state[interval] = SmcKlineFeed(api, None, FeatureEngine(min_bars=mb), symbols=(SYM,), interval=interval, history_bars=history, clock_ms=lambda: 1,  # type: ignore[arg-type]
                                           emit_latest_on_start=True)
            await state[interval]._bootstrap()
        multi = MultiFeed(list(state.values()))
        first = await multi.poll()
        assert [(c.interval, c.bar_open_time // c.interval_ms) for c in first] == [("1h", 129), ("1d", 129)]  # по одной последней закрытой свече: это состояние, не сигнал
        assert await multi.poll() == []
        multi.check()

    asyncio.run(body())


def test_kline_url_subscribes_to_the_requested_interval_only():
    for interval in ("5m", "1h", "1d"):
        feed = SmcKlineFeed(Api(interval, 130), None, FeatureEngine(min_bars=60), symbols=(SYM, "ETHUSDT"), interval=interval, history_bars=90)  # type: ignore[arg-type]
        assert feed._url.endswith(f"btcusdt@kline_{interval}/ethusdt@kline_{interval}") and config.WS_MARKET_URL in feed._url
