"""Smart Execution: лимитки post-only с чейзингом по стакану, TTL-отмены и откат на MARKET (execution/smart_execution.py).

Барьер настоящий (LiveBarrier на имитации биржи), подменены только источники: котировка, часы, переспрос стратегии.
"""

import asyncio
import contextlib
import math
from dataclasses import dataclass, field

import pytest

from backtester.contracts import Side
from backtester.risk import OrderRequest
from execution.live_barrier import CLIENT_ORDER_ID_RE
from execution.smart_execution import Reassessment, SmartExecutor
from execution.user_stream import parse_order_update
from scripts.live_run import round_qty
from tests.execution.helpers import make_env

T0 = 1_700_000_000_000
TTL_S = 10.0
PAST_TTL_MS = int(TTL_S * 1000) + 100  # + задержка исполнения барьера (50 мс) с запасом


def run(coro):
    return asyncio.run(coro)


@dataclass
class Ctl:
    now: int = T0
    quote: tuple[float, float] | None = (59_999.0, 60_001.0)
    answer: Reassessment | None = Reassessment(1.0, math.inf)
    force: bool = False
    asked: list = field(default_factory=list)


@contextlib.asynccontextmanager
async def smart(tmp_path, *, qty_rounder=round_qty, max_reposts=3):
    async with make_env(tmp_path) as e:
        ctl, lock = Ctl(), asyncio.Lock()

        def reassess(symbol, side, reduce_only):
            ctl.asked.append((symbol, side, reduce_only))
            return ctl.answer

        executor = SmartExecutor(
            e.barrier, quote=lambda symbol: ctl.quote, clock_ms=lambda: ctl.now, lock=lock, round_qty=qty_rounder,
            reassess=reassess, force_market=lambda symbol, reduce_only: ctl.force,
            ttl_s=TTL_S, max_reposts=max_reposts, market_strength=2.0, poll_s=0.01,
        )
        yield e, executor, ctl


def request(side=Side.BUY, qty=0.5, symbol="BTCUSDT"):
    return OrderRequest(symbol, side, qty)


async def expire_and_step(executor, ctl):
    ctl.now += PAST_TTL_MS
    await executor.step()


def stream_fill(e, cid, trade):
    e.barrier.on_order_update(parse_order_update(e.ex.order_event(e.ex.order(cid), trade, "TRADE")))


def cids(e):
    return [p["newClientOrderId"] for p in e.ex.post_log]


# ------------------------------------------------------------------------------ постановка


def test_buy_rests_at_best_bid_and_sell_at_best_ask_as_post_only(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(Side.BUY, 0.5, "BTCUSDT"), reduce_only=False)
            await ex.execute(request(Side.SELL, 0.25, "ETHUSDT"), reduce_only=False)
            return e

    buy, sell = run(go()).ex.post_log
    assert (buy["type"], buy["side"], buy["price"], buy["timeInForce"], buy["quantity"]) == \
        ("LIMIT", "BUY", "59999", "GTX", "0.5")
    assert (sell["type"], sell["side"], sell["price"], sell["timeInForce"]) == ("LIMIT", "SELL", "60001", "GTX")
    assert "reduceOnly" not in buy


def test_closing_order_is_a_reduce_only_limit(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(Side.SELL, 0.5), reduce_only=True)
            return e

    (post,) = run(go()).ex.post_log
    assert post["type"] == "LIMIT" and post["reduceOnly"] == "true" and post["price"] == "60001"


def test_quantity_is_floored_to_the_lot_step(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(qty=0.12345), reduce_only=False)
            await ex.execute(request(qty=0.0004, symbol="ETHUSDT"), reduce_only=False)  # меньше шага: заявки нет
            return e

    e = run(go())
    assert [p["quantity"] for p in e.ex.post_log] == ["0.123"]


# --------------------------------------------------------------------------- TTL и чейзинг


def test_nothing_happens_before_ttl_then_expiry_cancels_and_reposts_at_the_new_quote(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(), reduce_only=False)
            ctl.now += 5_000
            await ex.step()
            assert "DELETE /fapi/v1/order" not in e.ex.requests and len(e.ex.post_log) == 1  # TTL не вышел
            ctl.quote = (59_990.0, 59_992.0)  # рынок ушёл вниз
            await expire_and_step(ex, ctl)
            return e, ex, ctl

    e, ex, ctl = run(go())
    first, second = e.ex.post_log
    assert e.ex.requests.count("DELETE /fapi/v1/order") == 1
    assert e.ex.order(first["newClientOrderId"])["status"] == "CANCELED"
    assert (second["price"], second["quantity"], second["timeInForce"]) == ("59990", "0.5", "GTX")
    assert e.ex.order(second["newClientOrderId"])["status"] == "NEW"
    assert ctl.asked == [("BTCUSDT", Side.BUY, False)]  # перед перевыставлением стратегию переспросили
    assert ex.stats["reposts"] == 1 and ex.active_symbols == ("BTCUSDT",)


def test_client_order_id_rotates_on_every_repost_and_is_never_reused(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(), reduce_only=False)
            for _ in range(3):
                await expire_and_step(ex, ctl)
            return e

    e = run(go())
    ids = cids(e)
    assert len(ids) == 4 and len(set(ids)) == 4 and all(CLIENT_ORDER_ID_RE.match(c) and len(c) <= 36 for c in ids)
    assert [e.ex.order(c)["status"] for c in ids] == ["CANCELED", "CANCELED", "CANCELED", "NEW"]
    assert list(e.state.orders) == [ids[-1]]  # локально жив только последний


def test_partial_fill_then_ttl_reposts_only_the_remainder(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(qty=0.5), reduce_only=False)
            cid = cids(e)[0]
            trade = await e.ex.fill(cid, 0.2, push=False)
            stream_fill(e, cid, trade)
            await expire_and_step(ex, ctl)
            return e

    e = run(go())
    assert [p["quantity"] for p in e.ex.post_log] == ["0.5", "0.3"]  # не полный объём: иначе удвоенная позиция
    assert e.barrier.position("BTCUSDT") == pytest.approx(0.2)


def test_remainder_is_floored_to_the_lot_step(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(qty=0.5), reduce_only=False)
            cid = cids(e)[0]
            stream_fill(e, cid, await e.ex.fill(cid, 0.1, push=False))
            stream_fill(e, cid, await e.ex.fill(cid, 0.2, push=False))
            await expire_and_step(ex, ctl)
            return e

    assert [p["quantity"] for p in run(go()).ex.post_log] == ["0.5", "0.2"]  # 0.5-0.1-0.2 = 0.2 (с хвостом fp) -> 0.2


def test_full_fill_completes_the_chase_and_nothing_is_reposted(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(qty=0.5), reduce_only=False)
            cid = cids(e)[0]
            stream_fill(e, cid, await e.ex.fill(cid, 0.5, push=False))
            await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    assert len(e.ex.post_log) == 1 and "DELETE /fapi/v1/order" not in e.ex.requests
    assert ex.stats["completed"] == 1 and ex.active_symbols == () and e.barrier.position("BTCUSDT") == 0.5


def test_post_only_rejection_is_reposted_at_a_fresh_quote_without_waiting_for_ttl(tmp_path):
    """-5022: рынок успел пересечь цену. Это не отказ биржи: следующий шаг перевыставляет по новой котировке."""
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            e.ex.book = (59_999.0, 60_000.0)
            ctl.quote = (60_000.0, 60_001.0)  # устаревшая котировка: bid стал равен новому ask -> пересечение
            await ex.execute(request(), reduce_only=False)
            assert len(e.ex.orders) == 0 and ex.active_symbols == ("BTCUSDT",)  # на бирже не встал, чейз жив
            ctl.quote = (59_999.0, 60_000.0)
            await ex.step()  # TTL не вышел: перевыставление идёт по закрытию, а не по таймеру
            return e, ex

    e, ex = run(go())
    assert [p["price"] for p in e.ex.post_log] == ["60000", "59999"]
    assert len(e.ex.orders) == 1 and not e.safe.active and ex.stats["reposts"] == 1


# --------------------------------------------------------------- переспрос стратегии и откат


def test_signal_gone_after_ttl_stops_the_chase(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(), reduce_only=False)
            ctl.answer = None  # стратегия больше не просит эту сторону
            await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    assert len(e.ex.post_log) == 1 and ex.active_symbols == () and ex.stats["abandoned"] == 1
    assert e.state.orders == {}


def test_reassessed_quantity_caps_the_repost(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(qty=0.5), reduce_only=False)
            ctl.answer = Reassessment(1.0, 0.2)  # риск/позиция урезали заявку
            await expire_and_step(ex, ctl)
            return e

    assert [p["quantity"] for p in run(go()).ex.post_log] == ["0.5", "0.2"]


def test_weak_signal_gives_up_after_max_reposts_and_never_hits_the_market(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            ctl.answer = Reassessment(1.4, math.inf)  # выше порога входа, но не 2x
            await ex.execute(request(), reduce_only=False)
            for _ in range(4):
                await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    assert [p["type"] for p in e.ex.post_log] == ["LIMIT"] * 4  # первая + 3 перевыставления, MARKET нет
    assert ex.active_symbols == () and ex.stats["abandoned"] == 1 and ex.stats["market_fallback"] == 0
    assert e.barrier.position("BTCUSDT") == 0.0 and e.state.orders == {}


def test_strong_signal_falls_back_to_market_for_the_remainder_after_max_reposts(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            ctl.answer = Reassessment(2.5, math.inf)  # >= 2 порогов входа
            await ex.execute(request(qty=0.5), reduce_only=False)
            cid = cids(e)[0]
            stream_fill(e, cid, await e.ex.fill(cid, 0.2, push=False))  # часть уже набрана
            for _ in range(4):
                await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    kinds = [p["type"] for p in e.ex.post_log]
    assert kinds == ["LIMIT"] * 4 + ["MARKET"]
    assert e.ex.post_log[-1]["quantity"] == "0.3" and ex.stats["market_fallback"] == 1
    assert e.barrier.position("BTCUSDT") == pytest.approx(0.5)  # 0.2 мейкером + 0.3 рынком
    assert ex.active_symbols == () and e.state.orders == {}
    assert len(set(cids(e))) == 5  # у рыночного ордера тоже свой clientOrderId


def test_market_fallback_of_a_closing_order_stays_reduce_only(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            ctl.answer = Reassessment(3.0, math.inf)
            await ex.execute(request(Side.SELL, 0.5), reduce_only=True)
            for _ in range(4):
                await expire_and_step(ex, ctl)
            return e

    last = run(go()).ex.post_log[-1]
    assert last["type"] == "MARKET" and last["reduceOnly"] == "true"


def test_drawdown_guard_sends_a_closing_order_straight_to_market(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            ctl.force = True
            await ex.execute(request(Side.SELL, 0.5), reduce_only=True)
            return e, ex

    e, ex = run(go())
    (post,) = e.ex.post_log
    assert post["type"] == "MARKET" and post["reduceOnly"] == "true" and ex.stats["market_fallback"] == 1
    assert ex.stats["limit_placed"] == 0


# ------------------------------------------------------------------------- нет котировки


def test_no_live_quote_skips_an_entry_but_closes_a_position_at_market(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            ctl.quote = None
            await ex.execute(request(Side.BUY, 0.5, "BTCUSDT"), reduce_only=False)
            assert e.ex.post_log == [] and ex.active_symbols == ()  # пассивный ордер по устаревшей цене не ставим
            await ex.execute(request(Side.SELL, 0.5, "ETHUSDT"), reduce_only=True)
            return e

    (post,) = run(go()).ex.post_log
    assert post["type"] == "MARKET" and post["symbol"] == "ETHUSDT" and post["reduceOnly"] == "true"


def test_quote_lost_during_chase_abandons_an_entry(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(), reduce_only=False)
            ctl.quote = None
            await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    assert len(e.ex.post_log) == 1 and ex.active_symbols == () and e.state.orders == {}


# -------------------------------------------------------------------- повторы и встречные


def test_repeated_same_side_request_is_ignored_and_opposite_cancels_the_chase(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(Side.BUY), reduce_only=False)
            await ex.execute(request(Side.BUY), reduce_only=False)  # тот же сигнал: удвоения нет
            assert len(e.ex.post_log) == 1
            await ex.execute(request(Side.SELL), reduce_only=False)  # встречный
            return e, ex

    e, ex = run(go())
    assert len(e.ex.post_log) == 1 and e.ex.requests.count("DELETE /fapi/v1/order") == 1
    assert ex.active_symbols == () and e.state.orders == {}


def test_exchange_rejection_is_final_and_not_chased(tmp_path):
    async def go():
        async with smart(tmp_path, qty_rounder=lambda symbol, qty: qty) as (e, ex, ctl):
            await ex.execute(request(qty=0.0005), reduce_only=False)  # LOT_SIZE (-1013): без округления дойдёт до биржи
            await ex.step()
            return e, ex

    e, ex = run(go())
    assert len(e.ex.post_log) == 1 and ex.active_symbols == ()  # перевыставление ничего бы не изменило


def test_safe_mode_stops_entry_chase_but_not_a_closing_one(tmp_path):
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(Side.BUY, 0.5, "BTCUSDT"), reduce_only=False)
            await ex.execute(request(Side.SELL, 0.5, "ETHUSDT"), reduce_only=True)
            e.safe.trip("тест")
            await expire_and_step(ex, ctl)
            return e, ex

    e, ex = run(go())
    posted = [(p["symbol"], p["type"]) for p in e.ex.post_log]
    assert posted == [("BTCUSDT", "LIMIT"), ("ETHUSDT", "LIMIT"), ("ETHUSDT", "LIMIT")]  # вход не перевыставлен, выход - да
    assert ex.active_symbols == ("ETHUSDT",)


# ---------------------------------------------------------------- фоновая асинхронная задача


def test_background_task_cancels_by_ttl_and_reposts_without_manual_steps(tmp_path):
    """run(): сама снимает просроченную лимитку (DELETE) и ставит новую, пока остальной цикл занят своим."""
    async def go():
        async with make_env(tmp_path) as e:
            lock = asyncio.Lock()
            executor = SmartExecutor(
                e.barrier, quote=lambda s: (59_999.0, 60_001.0), clock_ms=e.rest.now_ms, lock=lock,
                round_qty=round_qty, ttl_s=0.15, max_reposts=3, market_strength=2.0, poll_s=0.02,
            )
            task = asyncio.create_task(executor.run())
            try:
                async with lock:
                    await executor.execute(request(), reduce_only=False)
                async with asyncio.timeout(5):
                    while len(e.ex.post_log) < 3:
                        await asyncio.sleep(0.02)
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            return e

    e = run(go())
    ids = cids(e)[:3]
    assert len(set(ids)) == 3 and e.ex.requests.count("DELETE /fapi/v1/order") >= 2
    assert [e.ex.order(c)["status"] for c in ids[:2]] == ["CANCELED", "CANCELED"]


def test_close_event_reaches_the_executor_before_advance_is_called(tmp_path):
    """Реакция на закрытие не ждёт шага основного цикла (advance раз в 5 с): слушатель получает событие сразу."""
    async def go():
        async with smart(tmp_path) as (e, ex, ctl):
            await ex.execute(request(), reduce_only=False)
            ctl.now += PAST_TTL_MS
            await e.barrier.expire_due(ctl.now)  # только барьер: executor.step() не вызывался, advance() тоже
            return ex

    ex = run(go())
    assert ex.active_symbols == ("BTCUSDT",) and ex._chases["BTCUSDT"].state.value == "repost"  # noqa: SLF001
