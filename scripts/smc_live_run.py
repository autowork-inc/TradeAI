"""LIVE-контур стратегий SMC Swing / Breakout Buildup: сигналы Оркестратора -> LiveBarrier (ордера и стопы на бирже).

    TRADING_DATA_DIR=data/live_smc python -m scripts.smc_live_run [--interval 15m] [--with-breakout] [--allow-mainnet]

Отличия от Shadow (scripts/shadow_run.py): тут есть ключи API и настоящие ордера, поэтому охранных условий больше.
  * Профиль testnet по умолчанию; mainnet - только с --allow-mainnet (сознательное решение владельца).
  * Каталог данных ОБЯЗАН отличаться от data/ боевого ML-контура (live_run): там его live_state.json и .safe_mode.
    Задайте TRADING_DATA_DIR (например data/live_smc), иначе ConfigError до единого запроса. Аккаунт тоже должен быть своим:
    сверка при старте не пропустит чужие позиции и ордера (расхождение -> Safe Mode, ордеров нет).
  * Старт: config.validate() -> .ban_lock -> замок экземпляра -> защёлка Safe Mode (стоит -> выход, код 4) -> сверка config.LIVE_*
    с exchangeInfo (шаги цены/лота, минимумы; расхождение -> ConfigError) -> reconcile позиций, ордеров и algo-ордеров ->
    эквити -> возврат сделок из прошлого запуска (recover) -> фиды -> приватный поток с ALGO_UPDATE -> цикл.
  * Fail-Fast (R1): любая из задач (поток свечей, тиков, приватный поток, исполнитель, цикл) упала или завершилась - остальные
    отменяются, процесс выходит; Docker перезапускает, а Safe Mode (если он включился) блокирует новые входы. Бот НЕ закрывает
    позиции при выходе: стопы и тейки остаются на бирже (R6), решение за оператором.

Что живёт где: LiveBarrierSink (orchestrator/live_route.py) ведёт сделки; barrier.pump() ставит и переносит защиту;
markPrice@1s идёт в barrier.on_mark_price (безубыток при +1R, D11). Журнал решений и сделок - <DATA_DIR>/smc_live_events.jsonl.
"""

import argparse
import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

import config
from backtester.contracts import ExecutionConfig
from execution.exchange_filters import fetch_symbol_filters, verify_config
from execution.live_barrier import LiveBarrier
from execution.reconcile import reconcile
from execution.rest import Credentials, SignedRest
from execution.state import LocalState, SafeMode, StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, RawEvent, StreamEvent, UserDataStream
from features.smc import SmcParams
from orchestrator.core import Orchestrator, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.journal import JsonlEventLog
from orchestrator.kline_feed import SmcKlineFeed
from orchestrator.live_route import LiveBarrierSink, LiveTradeStore
from orchestrator.strategies import Strategy, StrategyBreakout, StrategySMC
from orchestrator.tick_feed import TickFeed
from orchestrator.types import Mode
from scripts.live_run import AccountEquity
from utils import ban_lock
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError, SafeModeActive, StreamStalled, TradingInfraError, WebSocketClosed
from utils.instance_lock import InstanceLock
from utils.notifier import Notifier
from utils.runner import run

log = logging.getLogger("smc_live_run")

INSTANCE_NAME = "smc_live_run"
JOURNAL_NAME = "smc_live_events.jsonl"
TRADES_NAME = "smc_live_trades.json"
HEARTBEAT_NAME = ".smc_live_heartbeat"
POLL_INTERVAL_S = 1.0
RECONCILE_EVERY_S = 300.0


@dataclass(frozen=True)
class Options:
    interval: str = "15m"
    history: int = 300
    with_breakout: bool = False
    allow_mainnet: bool = False


class _Parser(argparse.ArgumentParser):
    def error(self, message: str):  # argparse выходит кодом 2, а 2 у нас - «бан биржи»
        raise ConfigError(f"Аргументы командной строки: {message}")


def parse_args(argv: Sequence[str] | None = None) -> Options:
    p = _Parser(prog="smc_live_run", description="LIVE-контур SMC Swing / Breakout Buildup")
    p.add_argument("--interval", default="15m", choices=["15m", "1h"], help="таймфрейм стратегий")
    p.add_argument("--history", type=int, default=300, help="сколько закрытых свечей держать для признаков")
    p.add_argument("--with-breakout", action="store_true", help="добавить Breakout Buildup (по умолчанию только SMC Swing)")
    p.add_argument("--allow-mainnet", action="store_true", help="разрешить основную сеть (по умолчанию только testnet)")
    a = p.parse_args(list(argv) if argv is not None else None)
    return Options(a.interval, a.history, a.with_breakout, a.allow_mainnet)


class _AlertTap:
    """Alerter для SafeMode: поднимает событие (его ждёт наблюдатель) и пересылает алерт в Notifier."""

    def __init__(self, notifier: Notifier, tripped: asyncio.Event):
        self._notifier, self.tripped = notifier, tripped

    def critical(self, title: str, details: Mapping[str, Any]) -> None:
        self.tripped.set()
        self._notifier.critical(title, details)


class SmcLiveRun:
    """Цикл LIVE-контура. Все зависимости передаются снаружи, поэтому тесты подставляют поддельные."""

    def __init__(
        self, *, rest: SignedRest, barrier: LiveBarrier, state: LocalState, store: StateStore, safe: SafeMode, sink: LiveBarrierSink,
        orchestrator: Orchestrator, feed: Any, ticks: Any = None, heartbeat: Path | None = None, notifier: Notifier | None = None,
    ):
        self.rest, self.barrier, self.state, self.store, self.safe = rest, barrier, state, store, safe
        self.sink, self.orch, self.feed, self.ticks = sink, orchestrator, feed, ticks
        self._heartbeat, self._notifier = heartbeat, notifier
        self.connected = asyncio.Event()

    # ------------------------------------------------------------------------- приватный поток

    def on_stream_event(self, event: StreamEvent) -> None:
        if isinstance(event, OrderUpdate):
            self.barrier.on_order_update(event)
        elif isinstance(event, AccountUpdate):
            self.barrier.on_account_update(event)
        elif isinstance(event, AlgoUpdate):
            self.barrier.on_algo_update(event)
        elif isinstance(event, RawEvent):
            if event.name == "MARGIN_CALL":
                self.safe.trip("MARGIN_CALL от биржи: позиция под угрозой ликвидации", {"payload": event.payload})
            else:
                log.info("Событие потока %s пропущено", event.name)

    async def on_connected(self, reconnect: bool) -> None:
        """После каждого (пере)подключения потока: статусы ордеров и algo-ордеров по REST; после обрыва - ещё и полная сверка."""
        try:
            async with asyncio.timeout(config.LIVE_RESYNC_TIMEOUT_S):
                async with self.sink.lock:
                    await self.barrier.resync_all()
                    if reconnect:
                        await self._reconcile("после переподключения потока")
        except TimeoutError as exc:
            raise StreamStalled(f"Синхронизация после подключения не завершилась за {config.LIVE_RESYNC_TIMEOUT_S} с") from exc
        self.connected.set()

    async def _reconcile(self, why: str) -> None:
        report = await reconcile(self.rest, self.state, self.safe, store=self.store, symbols=config.SYMBOLS)
        if not report.clean:
            kinds = ", ".join(sorted({d.kind.value for d in report.discrepancies}))
            raise SafeModeActive(f"Сверка ({why}): {kinds}; Safe Mode включён")

    async def periodic_reconcile(self) -> None:
        while True:
            await asyncio.sleep(RECONCILE_EVERY_S)
            async with self.sink.lock:
                if self.state.orders:
                    log.info("Периодическая сверка отложена: есть живые ордера входа (%d)", len(self.state.orders))
                    continue
                await self._reconcile("периодическая")

    # ------------------------------------------------------------------------------------ цикл

    async def tick(self) -> int:
        """Один шаг: упавшие фиды -> исключение; Safe Mode -> выход; новые закрытые свечи -> Оркестратор."""
        if self.ticks is not None:
            self.ticks.check()
        if self.safe.active:
            raise SafeModeActive(f"Safe Mode: {self.safe.reason}")
        contexts = await self.feed.poll()
        for ctx in contexts:
            self.orch.on_context(ctx)
        if self._heartbeat is not None:
            self._heartbeat.write_text(str(int(time.time())), encoding="utf-8")
        return len(contexts)

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(POLL_INTERVAL_S)


def build_strategies(opts: Options) -> list[Strategy]:
    # LIVE-контур заморожен (Этап 4) и сохраняет прежнее имя стратегии "smc": без H4-фида он работает без HTF-фильтра, имя SMC_Pro_Trend было бы ложью
    smc = StrategySMC(name="smc")
    return [smc, StrategyBreakout()] if opts.with_breakout else [smc]


def guard_environment(opts: Options) -> None:
    config.validate()
    if not config.IS_TESTNET and not opts.allow_mainnet:
        raise ConfigError("Профиль mainnet: LIVE-контур запрещён без --allow-mainnet (для проверки: BINANCE_PROFILE=testnet)")
    if config.DATA_DIR.resolve() == (config.BASE_DIR / "data").resolve():
        raise ConfigError("LIVE-контур SMC не должен работать в data/ боевого ML-контура (там его live_state.json и .safe_mode): "
                          "задайте TRADING_DATA_DIR, например data/live_smc")


async def smc_live_main(opts: Options, *, notifier: Notifier | None = None) -> None:
    guard_environment(opts)
    ban_lock.ensure_not_banned()
    notifier = notifier if notifier is not None else Notifier.from_env(prefix=f"[{config.BINANCE_PROFILE}/smc] ")
    try:
        with InstanceLock(INSTANCE_NAME):
            tripped = asyncio.Event()
            safe = SafeMode(alerter=_AlertTap(notifier, tripped))
            if safe.active:
                notifier.critical("Старт отклонён: активен Safe Mode", {"reason": safe.reason})
                raise SafeModeActive(f"Safe Mode: {safe.reason}. Снимите защёлку вручную после разбора причины")
            await _run_locked(opts, safe, tripped, notifier)
    except asyncio.CancelledError:
        notifier.info("LIVE-контур SMC остановлен сигналом")
        raise
    except SafeModeActive:
        notifier.warning("Выход с кодом 4 (Safe Mode). Позиции и их стопы остались на бирже - решение за оператором.")
        raise
    finally:
        await notifier.aclose()


async def _run_locked(opts: Options, safe: SafeMode, tripped: asyncio.Event, notifier: Notifier) -> None:
    creds = Credentials.from_env()
    timeout = aiohttp.ClientTimeout(total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rest = SignedRest(session, config.REST_BASE_URL, creds)
        await rest.sync_time()
        verify_config(await fetch_symbol_filters(rest, config.SYMBOLS))  # шаги и минимумы - по бирже, а не по памяти
        store = StateStore()
        state = store.load()
        report = await reconcile(rest, state, safe, store=store, symbols=config.SYMBOLS)
        if not report.clean:
            kinds = ", ".join(sorted({d.kind.value for d in report.discrepancies})) or "?"
            raise SafeModeActive(f"Первичная сверка не чистая ({kinds}): Safe Mode включён, ордера не отправлялись")
        barrier = LiveBarrier(
            rest, ExecutionConfig(config.LIVE_EXEC_LATENCY_MS, config.LIVE_MAKER_FEE_RATE, config.LIVE_TAKER_FEE_RATE), state, safe, store=store,
        )
        journal = JsonlEventLog(config.DATA_DIR / JOURNAL_NAME)
        try:
            sink = LiveBarrierSink(barrier, safe, journal, LiveTradeStore(config.DATA_DIR / TRADES_NAME), AccountEquity(rest), rest.now_ms,
                                   interval_ms=config.INTERVAL_MS[opts.interval])
            strategies = build_strategies(opts)
            orch = Orchestrator(strategies, {s.name: Route(Mode.LIVE, sink) for s in strategies}, journal)
            await sink.refresh_equity()
            alive = sink.recover(orch)
            orphans = sink.orphans()
            if orphans:
                raise ConfigError(f"На бирже/в состоянии есть позиция или защита по {orphans}, а записи о сделке нет ({TRADES_NAME} потерян?): "
                                  "разберите вручную (закройте позицию и снимите ордера), затем запускайте")
            feed = SmcKlineFeed(BinanceRest(session, config.REST_BASE_URL), session, FeatureEngine(SmcParams()), interval=opts.interval,
                                history_bars=opts.history)
            ticks = TickFeed(session, sink.on_tick, lambda ts: log.warning("Обрыв потока тиков: mark price мог быть пропущен (безубыток по ближайшему тику)"))
            runner = SmcLiveRun(rest=rest, barrier=barrier, state=state, store=store, safe=safe, sink=sink, orchestrator=orch, feed=feed,
                                ticks=ticks, heartbeat=config.DATA_DIR / HEARTBEAT_NAME, notifier=notifier)
            stream = UserDataStream(rest, session, runner.on_stream_event, on_connected=runner.on_connected,
                                    events=config.USER_STREAM_EVENTS_WITH_ALGO)
            log.info("LIVE-контур SMC: профиль %s, %s, стратегии %s, вернулось сделок: %d, журнал %s", config.BINANCE_PROFILE, opts.interval,
                     [s.name for s in strategies], len(alive), journal.path)
            notifier.info(f"LIVE-контур SMC запущен: {config.BINANCE_PROFILE}, {opts.interval}, стратегии {[s.name for s in strategies]}")
            feed.start()
            ticks.start()
            tasks = [asyncio.create_task(stream.run(), name="smc.user_stream")]
            try:
                await _wait_connected(runner, tasks[0])
                tasks += [
                    asyncio.create_task(sink.run(), name="smc.executor"),
                    asyncio.create_task(runner.run(), name="smc.loop"),
                    asyncio.create_task(_watch_safe_mode(safe, tripped), name="smc.safe_mode_watch"),
                    asyncio.create_task(runner.periodic_reconcile(), name="smc.reconcile"),
                ]
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                failures = [t.exception() for t in done if not t.cancelled() and t.exception() is not None]
                failures.sort(key=lambda e: not isinstance(e, SafeModeActive))  # Safe Mode - первопричина
                if failures:
                    raise failures[0]
                raise TradingInfraError(f"Задача {next(iter(done)).get_name()} завершилась без ошибки: так быть не должно")
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await feed.aclose()
                await ticks.aclose()
        finally:
            journal.close()


async def _wait_connected(runner: SmcLiveRun, stream_task: asyncio.Task) -> None:
    waiter = asyncio.create_task(runner.connected.wait())
    try:
        done, _ = await asyncio.wait({stream_task, waiter}, timeout=config.LIVE_STARTUP_TIMEOUT_S, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
    if stream_task in done:
        stream_task.result()
        raise WebSocketClosed("Приватный поток завершился при старте")
    if not done:
        raise StreamStalled(f"Приватный поток не подключился за {config.LIVE_STARTUP_TIMEOUT_S} с")


async def _watch_safe_mode(safe: SafeMode, tripped: asyncio.Event) -> None:
    await tripped.wait()
    raise SafeModeActive(f"Safe Mode включён: {safe.reason}")


async def main() -> None:
    await smc_live_main(parse_args())


if __name__ == "__main__":
    run(main, "smc_live_run")
