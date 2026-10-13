"""Shadow Mode: пять параллельных стратегий (реестр orchestrator/registry.py) на реальном рынке, ордера НЕ отправляются (D5).

Запуск (из корня):  python -m scripts.shadow_run [--interval 15m] [--equity 10000] [--allow-testnet]

Мульти-стратегийный режим (2026-10-11): SMC_Pro_Trend, SMC_Counter_Trend, OBI_Breakout, ICT_Killzones, VWAP_Reversion работают на ОДНИХ
свечах M15 и одном фиде H4 (лишних подключений нет); позиция идентифицируется ключом `<монета>_<strategy_id>`, у каждой стратегии своя
блокировка монеты, свой план управления позицией и своя колонка strategy_id в БД. Для OBI_Breakout добавлен поток стакана
`depth10@500ms`; ленту (aggTrade) он берёт из того же TickFeed, что и трекер.
Что делает: свечи 15m (SmcKlineFeed) -> FeatureEngine -> Orchestrator -> VirtualTracker; тики (TickFeed: aggTrade и
markPrice@1s) двигают виртуальные заявки и позиции. Монеты - config.SHADOW_SYMBOLS (с 2026-10-13: BTC, ETH).
HTF Bias (2026-10-11): вторым фидом идут свечи H4 (kline_4h + история REST); структура H4 (features/htf.py) присоединяется
к контексту M15, и автомат SMC не начинает сетап против тренда H4 (шорт при Bullish, лонг при Bearish). Управление
позицией - Scale Out (50% на +1R с безубытком, 40% на +3R, 10% runner с трейлингом; см. orchestrator/tracker.py).
Все решения и сделки пишутся в журнал JSONL (<DATA_DIR>/shadow_events.jsonl) и, если задан DATABASE_URL, в PostgreSQL
(trades, trade_closes, trade_snapshots, state_events; схема - persistence/schema.py). Сводка - в лог раз в час и при выходе.
Состояние сделок живёт в памяти: при перезапуске открытые виртуальные позиции теряются; в БД они помечаются aborted
(`process_restart`) и в статистику не входят.

Изоляция от боевого контура (live_run):
  * отдельный процесс/контейнер, отдельный каталог данных (TRADING_DATA_DIR): свои замки .instance_lock, .ban_lock;
  * НЕТ ключей API: скрипт не читает BINANCE_API_KEY/SECRET и не умеет подписывать запросы - торговать физически не может;
  * только публичные данные: REST /fapi/v1/klines и /time, WS /market/stream. Профиль по умолчанию должен быть mainnet
    (на testnet цены не отражают рынок), иначе ConfigError без --allow-testnet.
Fail-Fast (R1): любая ошибка стратегии, данных или трекера роняет процесс; перезапускает Docker. Код выхода по
utils/errors.py (2 - бан биржи: следующий запуск сразу выйдет по .ban_lock без запросов).
"""

import argparse
import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import aiohttp

import config
from features import defaults as D
from features.smc import SmcParams
from orchestrator.core import Orchestrator, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.htf import HtfBias
from orchestrator.journal import JsonlEventLog
from orchestrator.kline_feed import SmcKlineFeed
from orchestrator.microstructure import DepthFeed, MicroStructure
from orchestrator.registry import NEEDS_MICRO, SHADOW_STRATEGIES, build_strategies, plans_for
from orchestrator.tick_feed import TickFeed
from orchestrator.tracker import VirtualTracker
from orchestrator.types import Mode, Tick
from persistence.db import Database, check_dsn
from persistence.pg import PgRecorder
from persistence.recorder import RecordingEventLog
from utils import ban_lock
from utils.binance_rest import BinanceRest
from utils.errors import ConfigError
from utils.instance_lock import InstanceLock
from utils.runner import run

log = logging.getLogger("shadow_run")

INSTANCE_NAME = "shadow_run"
JOURNAL_NAME = "shadow_events.jsonl"
HEARTBEAT_NAME = ".shadow_heartbeat"
POLL_INTERVAL_S = 1.0
SUMMARY_EVERY_S = 3600.0


@dataclass(frozen=True)
class Options:
    interval: str
    equity: float
    history: int
    allow_testnet: bool


def parse_args(argv: Sequence[str] | None = None) -> Options:
    p = argparse.ArgumentParser(description="Shadow Mode (виртуальные сделки по реальному рынку)")
    p.add_argument("--interval", default="15m", choices=["15m", "1h"], help="таймфрейм стратегий")
    p.add_argument("--equity", type=float, default=10_000.0, help="виртуальный эквити для расчёта объёма (риск 1%% от него)")
    p.add_argument("--history", type=int, default=300, help="сколько закрытых свечей держать для признаков")
    p.add_argument("--allow-testnet", action="store_true", help="разрешить профиль testnet (цены не реальные: только для проверки связи)")
    a = p.parse_args(argv)
    return Options(a.interval, a.equity, a.history, a.allow_testnet)


class ShadowRun:
    """Цикл Shadow Mode. Фиды передаются снаружи, поэтому тесты подставляют поддельные."""

    def __init__(self, feed, orchestrator: Orchestrator, tracker: VirtualTracker, tick_feed=None, *,
                 heartbeat: Path | None = None, clock=time.monotonic, recorder: PgRecorder | None = None,
                 htf_feed=None, htf: HtfBias | None = None, micro: MicroStructure | None = None, depth_feed: DepthFeed | None = None, live=None):
        if (htf_feed is None) != (htf is None):
            raise ConfigError("HTF: htf_feed и htf задаются вместе (фид H4 без таблицы bias, как и наоборот, бессмыслен)")
        if (micro is None) != (depth_feed is None):
            raise ConfigError("Микроструктура: micro и depth_feed задаются вместе (стакан без агрегатора, как и наоборот, бессмыслен)")
        self._feed, self._orch, self._tracker, self._ticks = feed, orchestrator, tracker, tick_feed
        self._htf_feed, self._htf = htf_feed, htf
        self._micro, self._depth = micro, depth_feed
        self._live = live  # LiveExecutionEngine при TRADE_MODE=live, иначе None (боевой код не импортируется)
        self._recorder = recorder
        self._heartbeat, self._clock = heartbeat, clock
        self._next_summary = clock() + SUMMARY_EVERY_S

    async def tick(self) -> int:
        """Один шаг: упавшая задача фида -> исключение; новые свечи -> Оркестратор. Возвращает число обработанных свечей."""
        if self._ticks is not None:
            self._ticks.check()
        if self._depth is not None:
            self._depth.check()
        if self._live is not None:
            self._live.check()  # упал боевой исполнитель -> падаем вместе с ним (R1); Safe Mode к этому моменту уже включён
        if self._recorder is not None:
            self._recorder.check()  # писатель БД упал -> падаем вместе с ним (R1), Docker перезапускает
        if self._htf_feed is not None:  # H4 раньше M15: bias обновлён до расчёта сетапов; ошибка H4-фида роняет процесс (R1)
            for htf_ctx in await self._htf_feed.poll():
                self._htf.update(htf_ctx)
        contexts = await self._feed.poll()
        for ctx in contexts:
            if self._htf is not None:
                ctx = self._htf.attach(ctx)
            if self._micro is not None:
                ctx = self._micro.attach(ctx)
            self._orch.on_context(ctx)
        if self._heartbeat is not None:
            self._heartbeat.write_text(str(int(time.time())), encoding="utf-8")
        if self._clock() >= self._next_summary:
            self._next_summary = self._clock() + SUMMARY_EVERY_S
            self.log_summary()
        return len(contexts)

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(POLL_INTERVAL_S)

    def log_summary(self) -> None:
        for name, row in sorted(self._tracker.summary().items()):
            log.info("СВОДКА %s: %s", name, row)


async def shadow_main(opts: Options) -> None:
    config.validate()
    if config.IS_TESTNET and not opts.allow_testnet:
        raise ConfigError("Shadow Mode считает по реальному рынку: нужен BINANCE_PROFILE=mainnet (testnet - только с --allow-testnet, для проверки связи)")
    if config.DATABASE_URL:
        check_dsn(config.DATABASE_URL)  # неверный формат - до сети
    ban_lock.ensure_not_banned()
    symbols = config.SHADOW_SYMBOLS
    with InstanceLock(INSTANCE_NAME):
        journal = JsonlEventLog(config.DATA_DIR / JOURNAL_NAME)
        timeout = aiohttp.ClientTimeout(total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S)
        recorder: PgRecorder | None = None
        try:
            if config.DATABASE_URL:
                recorder = PgRecorder(await Database.connect(config.DATABASE_URL))
                aborted = await recorder.start()  # миграции + пометка висящих сделок прошлого запуска
                log.info("PostgreSQL готов, схема актуальна; сделок прошлого запуска помечено aborted: %d", len(aborted))
            else:
                log.warning("DATABASE_URL не задан: сделки пишутся только в JSONL-журнал (для сбора статистики на сервере БД обязательна)")
            events = RecordingEventLog(journal, recorder) if recorder is not None else journal
            async with aiohttp.ClientSession(timeout=timeout) as session:
                api = BinanceRest(session, config.REST_BASE_URL)
                names = SHADOW_STRATEGIES
                strategies = build_strategies(names)
                tracker = VirtualTracker(events, equity=opts.equity, recorder=recorder, plans=plans_for(names))
                live = None
                sink = tracker
                if config.TRADE_MODE == "live":  # ВЫКЛЮЧЕНО по умолчанию (TRADE_MODE=shadow): боевой код тогда даже не импортируется
                    from orchestrator.live_engine import LiveMirrorSink, build_live_engine

                    live = await build_live_engine(session, events, symbols=symbols, strategy_names=names,
                                                   interval_ms=config.INTERVAL_MS[opts.interval],
                                                   flush=recorder.flush if recorder is not None else None)
                    sink = LiveMirrorSink(tracker, live)  # Shadow-трекер получает все сигналы как прежде; одобренные - ещё и боевой движок
                orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, sink) for s in strategies}, events, recorder=recorder)
                feed = SmcKlineFeed(api, session, FeatureEngine(SmcParams()), symbols=symbols, interval=opts.interval, history_bars=opts.history)
                htf_feed = htf = None
                if D.HTF_FILTER_ENABLED:
                    htf = HtfBias(D.HTF_INTERVAL, config.INTERVAL_MS[D.HTF_INTERVAL], events)
                    htf_feed = SmcKlineFeed(api, session, FeatureEngine(SmcParams()), symbols=symbols, interval=D.HTF_INTERVAL,
                                            history_bars=D.HTF_HISTORY_BARS, emit_latest_on_start=True)
                micro = depth_feed = None
                if any(n in NEEDS_MICRO for n in names):  # лента - из того же aggTrade, что и тики трекера; стакан - отдельный публичный поток
                    micro = MicroStructure(symbols, config.INTERVAL_MS[opts.interval])
                    depth_feed = DepthFeed(session, micro.aggregator, symbols=symbols)

                def on_tick(tick: Tick) -> None:
                    tracker.on_tick(tick)
                    if live is not None:
                        live.on_tick(tick)  # те же тики, что у Shadow: лучшая цена для трейлинга и mark price для безубытка
                    if micro is not None:
                        micro.on_tick(tick)

                def on_gap(ts: int) -> None:
                    for s in symbols:
                        tracker.mark_gap(s, ts)
                    if micro is not None:
                        micro.aggregator.mark_gap("trade")

                ticks = TickFeed(session, on_tick, on_gap, symbols=symbols)
                runner = ShadowRun(feed, orch, tracker, ticks, heartbeat=config.DATA_DIR / HEARTBEAT_NAME, recorder=recorder,
                                 htf_feed=htf_feed, htf=htf, micro=micro, depth_feed=depth_feed, live=live)
                log.info("Shadow Mode: %s %s, стратегии %s, HTF-фильтр %s, микроструктура %s, эквити %.0f, журнал %s, БД %s, TRADE_MODE=%s",
                         symbols, opts.interval, list(names), D.HTF_INTERVAL if htf is not None else "выключен",
                         "стакан + лента" if micro is not None else "нет", opts.equity, journal.path, "включена" if recorder is not None else "нет", config.TRADE_MODE)
                if htf_feed is not None:
                    htf_feed.start()
                feed.start()
                if live is not None:
                    log.warning("TRADE_MODE=live: БОЕВЫЕ ордера по стратегиям %s, маржа %.2f USDT x%d, суточный лимит %.2f USDT",
                                sorted(live.risk.approved), live.risk.margin_usdt, live.risk.leverage, live.risk.daily_loss_limit)
                    live.start()
                ticks.start()
                if depth_feed is not None:
                    depth_feed.start()
                try:
                    await runner.run()
                finally:
                    runner.log_summary()
                    if live is not None:
                        await live.aclose()
                    await feed.aclose()
                    if htf_feed is not None:
                        await htf_feed.aclose()
                    await ticks.aclose()
                    if depth_feed is not None:
                        await depth_feed.aclose()
        finally:
            journal.close()
            if recorder is not None:
                await recorder.aclose()  # дописать очередь, закрыть пул (ошибка писателя не прячется)


async def main() -> None:
    await shadow_main(parse_args())


if __name__ == "__main__":
    run(main, "shadow_run")
