"""Изоляция MetaApi от крипты (2026-10-18): менеджер подключения `Mt5Link` (пауза ровно 5 минут и переподключение), фиды MT5 через него,
догрузка свечей после простоя, кеш списка символов брокера на 24 часа, автозапуск API мониторинга."""

import asyncio
import json
from pathlib import Path

import pytest

import config
from exchange.mt5_metaapi import MetaApiError, MetaApiLiveClient, MetaApiTradeError
from orchestrator.mt5_feeds import M5_MS, Mt5TickFeed
from orchestrator.mt5_link import Mt5Link
from tests.exchange.test_mt5_metaapi import OK, FakeRpc
from tests.orchestrator.test_mt5_feeds import AFTER, SYM, Clock, FakeSource, make_bars, make_feed
from utils.errors import ConfigError, DataError

ROOT = Path(__file__).resolve().parents[2]


class FakeClient:
    def __init__(self, fail_connect: int = 0):
        self.fail_connect = fail_connect
        self.connects = self.closes = 0

    async def connect(self):
        self.connects += 1
        if self.fail_connect > 0:
            self.fail_connect -= 1
            raise MetaApiError("нет сети")

    async def close(self):
        self.closes += 1


class FakeMapper:
    def __init__(self):
        self.loads = 0

    async def load(self, symbols):
        self.loads += 1

    def broker_name(self, s):
        return s + ".r"


class Sleeps:
    def __init__(self):
        self.calls: list[float] = []

    async def __call__(self, s):
        self.calls.append(s)


def link(client=None, sleeps=None, **kw):
    return Mt5Link(client or FakeClient(), FakeMapper(), (SYM,), sleep=sleeps or Sleeps(), clock_ms=lambda: 42, **kw)


def test_default_pause_is_exactly_five_minutes():
    assert config.METAAPI_RECONNECT_PAUSE_S == 300.0 and link().pause_s == 300.0


def test_first_call_connects_and_resolves_symbols_without_pause():
    sleeps = Sleeps()
    lk = link(sleeps=sleeps)

    async def op():
        return "ok"

    assert asyncio.run(lk.call("x", op)) == "ok"
    assert lk.up and lk.client.connects == 1 and lk.mapper.loads == 1 and sleeps.calls == []


def test_network_failure_marks_down_pauses_five_minutes_reconnects_and_retries():
    sleeps, downs = Sleeps(), []
    lk = link(sleeps=sleeps)
    lk.on_down(downs.append)
    results = [MetaApiError("таймаут"), "ok"]

    async def op():
        r = results.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    assert asyncio.run(lk.call("x", op)) == "ok"
    assert downs == [42] and lk.outages == 1 and lk.client.closes == 1
    assert sleeps.calls == [300.0] and lk.client.connects == 2 and lk.mapper.loads == 1  # символы разрешаются один раз


def test_failed_reconnects_wait_five_minutes_each_and_never_raise():
    sleeps = Sleeps()
    lk = link(FakeClient(fail_connect=4), sleeps)

    async def op():
        return 1

    assert asyncio.run(lk.call("x", op)) == 1  # старт при недоступном MetaApi: попытки до успеха, процесс не падает
    assert sleeps.calls == [300.0] * 4 and lk.attempts == 5


@pytest.mark.parametrize("exc", [ConfigError("401"), DataError("кривая свеча"), MetaApiTradeError("отказ")])
def test_non_network_errors_stay_fail_fast(exc):
    lk = link()

    async def op():
        raise exc

    with pytest.raises(type(exc)):
        asyncio.run(lk.call("x", op))


def test_tick_feed_over_link_survives_outage_and_marks_gap_once():
    gaps, ticks = [], []
    lk = link()

    class Flaky(FakeSource):
        n = 0

        async def get_price(self, symbol):
            Flaky.n += 1
            if Flaky.n in (2, 3):
                raise MetaApiError("обрыв")
            return await super().get_price(symbol)

    async def scenario():
        f = Mt5TickFeed(Flaky([], 0), ticks.append, gaps.append, broker_name=lk.broker_name, symbols=(SYM,), clock_ms=lambda: 7, poll_s=0.001,
                        max_failures=1, link=lk)
        f.start()
        await asyncio.sleep(0.1)
        f.check()  # задача жива: сбой MetaApi процесс не роняет
        await f.aclose()

    asyncio.run(scenario())
    assert gaps == [42, 42] and lk.outages == 2 and len(ticks) >= 4  # каждое падение связи - отметка gap; после восстановления тики идут


def test_kline_group_over_link_waits_instead_of_crashing():
    bars = make_bars(110)

    class Down(FakeSource):
        fails = 3

        async def get_candles(self, *a, **kw):
            if Down.fails:
                Down.fails -= 1
                raise MetaApiError("таймаут")
            return await super().get_candles(*a, **kw)

    lk = link()
    feed = make_feed(Down(bars, AFTER), Clock(bars[AFTER - 2][0] + M5_MS + 5_000), max_failures=1, link=lk)
    asyncio.run(feed._g._guarded(feed._g.bootstrap))
    assert feed._g.ready and feed._g.store[SYM] and lk.outages == 3  # max_failures=1 не действует: решает менеджер связи


def test_refresh_after_outage_backfills_every_missed_candle_instead_of_flat_ones():
    bars = make_bars(110)
    source = FakeSource(bars, visible=AFTER)
    clock = Clock(bars[AFTER - 2][0] + M5_MS + 5_000)
    feed = make_feed(source, clock)
    g = feed._g
    asyncio.run(g.bootstrap())
    last = max(g.store[SYM])
    gap_to = AFTER + 1500  # простой ~5 суток торговых M5 (больше одной страницы)
    source.visible = gap_to
    clock.t = bars[gap_to - 2][0] + M5_MS + 5_000
    asyncio.run(g.refresh())
    real = {b[0] for b in bars[:gap_to - 1] if b[0] > last}
    assert real <= set(g.store[SYM])
    assert not any(g.stale_until[SYM].get(t) for t in real)  # реальные свечи простоя не объявлены «закрытым рынком»


# ---------------------------------------------------------------------------------------- кеш символов


def cached_client(tmp_path, now=1000.0):
    c = MetaApiLiveClient(OK, symbols_cache=tmp_path / "sym.json", wall_clock=lambda: now)
    c._connection = FakeRpc()
    return c


def test_get_symbols_is_cached_on_disk_for_24_hours(tmp_path):
    c = cached_client(tmp_path)
    assert asyncio.run(c.get_symbols()) == ["EURUSD", "BTCUSDm"]
    data = json.loads((tmp_path / "sym.json").read_text(encoding="utf-8"))
    assert data == {"account_id": "acc-123", "saved_at": 1000.0, "symbols": ["EURUSD", "BTCUSDm"]}
    c2 = MetaApiLiveClient(OK, symbols_cache=tmp_path / "sym.json", wall_clock=lambda: 1000.0 + 86_399)
    assert asyncio.run(c2.get_symbols()) == ["EURUSD", "BTCUSDm"]  # без соединения: ответ из кеша (иначе MetaApiError «не подключён»)


@pytest.mark.parametrize("content", [
    {"account_id": "acc-123", "saved_at": 0.0, "symbols": ["OLD"]},  # старше 24 ч
    {"account_id": "other", "saved_at": 1000.0, "symbols": ["OLD"]},  # чужой аккаунт
    "{битый json",
])
def test_stale_foreign_or_broken_cache_is_a_miss(tmp_path, content):
    (tmp_path / "sym.json").write_text(content if isinstance(content, str) else json.dumps(content), encoding="utf-8")
    c = cached_client(tmp_path, now=1000.0 + 86_400 if isinstance(content, dict) and content["account_id"] == "acc-123" else 1000.0)
    assert asyncio.run(c.get_symbols()) == ["EURUSD", "BTCUSDm"]
    assert json.loads((tmp_path / "sym.json").read_text(encoding="utf-8"))["symbols"] == ["EURUSD", "BTCUSDm"]


# --------------------------------------------------------------------------------------------- деплой


def test_api_restarts_unless_stopped_and_shadow_sh_autostarts_it():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    api = compose.split("\n  api:", 1)[1].split("\n  pgdata:", 1)[0] if "\n  pgdata:" in compose else compose.split("\n  api:", 1)[1]
    assert "restart: unless-stopped" in api and "on-failure" not in api.split("healthcheck")[0]
    sh = (ROOT / "deploy" / "shadow.sh").read_bytes()
    assert b"\r\n" not in sh
    text = sh.decode("utf-8")
    assert "ensure_api()" in text and "    api)" in text and text.count("ensure_api\n") >= 4  # up, up-live, status, api
