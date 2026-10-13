"""Боевой оркестратор: собирает замки, сверку, приватный поток и торговый цикл в один процесс.

    python -m scripts.live_run --dry-run      # решения считаются и логируются, ордера НЕ отправляются
    python -m scripts.live_run                # ордера уходят на биржу выбранного профиля (BINANCE_PROFILE)
    python -m scripts.live_run --allow-mainnet  # без флага основная сеть запрещена (Этап 8 - только testnet)
    python -m scripts.live_run --model-path model.json   # боевая ML-модель (train_walkforward --save-model)

Последовательность старта (каждый шаг - условие следующего; сбой на любом = выход без ордеров):
  1. config.validate(), .ban_lock (код 2), замок .instance_lock.live_run (второй экземпляр - код 1);
  2. защёлка .safe_mode: стоит -> алерт и немедленный выход с кодом 4;
  3. поправка часов, первичная сверка reconcile() по позициям и открытым ордерам: расхождение или
     невозможность сверить -> Safe Mode (защёлка на диске) и выход с кодом 4;
  4. приватный поток (UserDataStream, listenKey с keepalive): ждём первого подключения и синхронизации
     живых ордеров; не дождались за LIVE_STARTUP_TIMEOUT_S -> выход;
  5. торговый цикл: события барьера -> сроки ордеров -> эквити и Drawdown Guard -> источник решений ->
     стратегия -> риск-гуарды -> LiveBarrier.submit.

Fail-fast в работе. Любая из фоновых задач (поток, торговый цикл, наблюдатель Safe Mode, периодическая
сверка) завершилась - остальные отменяются, процесс выходит:
  * Safe Mode включён чем угодно (барьер, сверка, MARGIN_CALL) -> код 4. Бот НЕ закрывает позиции сам:
    выход оставляет их на бирже, об этом идёт алерт - решение за оператором;
  * приватный поток не восстановился за USER_STREAM_MAX_RECONNECTS попыток (зависание ловит ping/pong
    aiohttp) -> код 1; после каждого переподключения - синхронизация ордеров и полная сверка с биржей;
  * 429/418 -> код 2 и запись .ban_lock (общий runner); мусорный ответ биржи / ошибка стратегии -> код 1;
  * сетевой сбой шага цикла терпится до LIVE_MAX_CONSECUTIVE_ERRORS подряд, затем код 1.

Ограничения: периодическая сверка идёт только когда живых ордеров нет (иначе гонка с событиями потока дала
бы ложное расхождение; после переподключения сверка полная, она безопасна). Без --model-path источник решений -
каркасный (execution/live_feed.py), стратегия - BaselineStrategy: это проверка контура, не торговая идея.

С --model-path: модель читается из JSON ДО обращения к бирже (битый файл - выход без единого запроса),
стратегия - MLModelStrategy, источник решений - WsLiveFeatureFeed (execution/ws_live_features.py): стакан и
свечи идут по WebSocket (@depth10@500ms и @kline_1m, те же потоки, что у сборщика обучающих данных), признаки
считает тот же конвейер, что в обучении (REST-снимки раз в 5 с давали Train-Serve Skew и убраны). Признаки модели
обязаны входить в набор фида (иначе ConfigError при старте). После каждого запуска ~15 минут признаки потока
ордеров null (буфер стакана копится в памяти): стратегия в это время не торгует. Подробности - в докстринге
execution/ws_live_features.py.
"""

import argparse
import asyncio
import contextlib
import logging
import math
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any

import aiohttp

import config
from backtester.contracts import BarrierEvent, ExecutionConfig, Fill, OrderClosed, Side
from backtester.ml_strategy import MLModelStrategy, RidgeRegressor
from backtester.risk import (
    QTY_EPS,
    DrawdownLimitGuard,
    MaxExposureGuard,
    OrderRequest,
    RiskContext,
    RiskGuard,
)
from backtester.strategy import BaselineStrategy, FeatureContext, Strategy
from execution.live_barrier import LiveBarrier
from execution.live_feed import DecisionFeed, KlineObiFeed
from execution.reconcile import ReconcileReport, reconcile
from execution.rest import Credentials, SignedRest
from execution.smart_execution import Reassessment, SmartExecutor
from execution.state import LocalState, SafeMode, StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, RawEvent, StreamEvent, UserDataStream
from execution.ws_live_features import WsLiveFeatureFeed
from utils import ban_lock
from utils.binance_rest import BinanceRest
from utils.errors import (
    ConfigError,
    DataError,
    HttpServerError,
    NetworkError,
    SafeModeActive,
    StreamStalled,
    TradingInfraError,
    WebSocketClosed,
)
from utils.instance_lock import InstanceLock
from utils.notifier import Notifier
from utils.runner import run

log = logging.getLogger("live_run")

EquitySource = Callable[[], Awaitable[float]]


# ------------------------------------------------------------------------ вспомогательное


@dataclass(frozen=True)
class Options:
    dry_run: bool = False
    allow_mainnet: bool = False
    model_path: Path | None = None  # None - каркас: BaselineStrategy + KlineObiFeed
    threshold_bps: float = config.LIVE_ML_THRESHOLD_BPS
    exit_threshold_bps: float = config.LIVE_ML_EXIT_THRESHOLD_BPS


class _Parser(argparse.ArgumentParser):
    def error(self, message: str):  # argparse выходит кодом 2, а 2 у нас - «бан биржи»
        raise ConfigError(f"Аргументы командной строки: {message}")


def parse_args(argv: Sequence[str] | None = None) -> Options:
    p = _Parser(prog="live_run", description="Боевой цикл Binance USDⓈ-M Futures")
    p.add_argument("--dry-run", action="store_true", help="не отправлять ордера: только решения в лог")
    p.add_argument("--allow-mainnet", action="store_true", help="разрешить основную сеть (по умолчанию только testnet)")
    p.add_argument("--model-path", type=Path, default=None, metavar="MODEL.json",
                   help="ML-модель из train_walkforward --save-model: MLModelStrategy + канонические признаки")
    p.add_argument("--threshold-bps", type=float, default=config.LIVE_ML_THRESHOLD_BPS,
                   help="порог входа MLModelStrategy, bps лог-доходности за горизонт метки (с --model-path)")
    p.add_argument("--exit-threshold-bps", type=float, default=config.LIVE_ML_EXIT_THRESHOLD_BPS,
                   help="порог удержания позиции MLModelStrategy, bps; меньше порога входа (с --model-path)")
    ns = p.parse_args(list(argv) if argv is not None else sys.argv[1:])
    return Options(
        dry_run=ns.dry_run, allow_mainnet=ns.allow_mainnet, model_path=ns.model_path,
        threshold_bps=ns.threshold_bps, exit_threshold_bps=ns.exit_threshold_bps,
    )


def build_ml_strategy(opts: Options) -> tuple[MLModelStrategy, tuple[str, ...]] | None:
    """--model-path -> (MLModelStrategy, признаки модели). Нет пути - None (каркас). Ошибка файла - до сети."""
    if opts.model_path is None:
        return None
    model = RidgeRegressor.load_json(opts.model_path)
    strategy = MLModelStrategy(
        model, threshold=opts.threshold_bps / 1e4, exit_threshold=opts.exit_threshold_bps / 1e4,
        equity_fraction=config.LIVE_INITIAL_EQUITY_FRACTION,
    )
    return strategy, model.feature_names


def round_qty(symbol: str, qty: float) -> float:
    """Округление ВНИЗ до шага лота: LiveBarrier количество не округляет, биржа неточное отвергает (-1013)."""
    step = config.LIVE_QTY_STEP.get(symbol)
    if step is None:
        raise ConfigError(f"Для {symbol} не задан шаг лота в config.LIVE_QTY_STEP")
    d_step = Decimal(repr(float(step)))
    return float((Decimal(repr(float(qty))) // d_step) * d_step)


def classify_fill(before: float, after: float, eps: float = 1e-9) -> str:
    """Что исполнение сделало с позицией: ВХОД / ВЫХОД / РАЗВОРОТ / ИЗМЕНЕНИЕ."""
    if abs(after) <= eps:
        return "ВЫХОД"
    if abs(before) <= eps:
        return "ВХОД"
    return "РАЗВОРОТ" if before * after < 0 else "ИЗМЕНЕНИЕ"


def parse_equity(data: Any) -> float:
    """Эквити = маржинальный баланс счёта (кошелёк + нереализованный PnL)."""
    if not isinstance(data, dict):
        raise DataError(f"account: ожидается объект, получено {str(data)[:200]}")
    try:
        if "totalMarginBalance" in data:
            value = float(data["totalMarginBalance"])
        else:
            value = float(data["totalWalletBalance"]) + float(data["totalUnrealizedProfit"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DataError(f"account: нет полей баланса: {str(data)[:200]}") from exc
    if not math.isfinite(value):
        raise DataError(f"account: баланс не число: {value!r}")
    return value


class AccountEquity:
    def __init__(self, rest: SignedRest):
        self._rest = rest

    async def __call__(self) -> float:
        return parse_equity(await self._rest.request("GET", config.ACCOUNT_PATH))


class _AlertTap:
    """Alerter для SafeMode: поднимает событие (его ждёт наблюдатель) и пересылает алерт в Notifier."""

    def __init__(self, notifier: Notifier, tripped: asyncio.Event):
        self._notifier, self.tripped = notifier, tripped

    def critical(self, title: str, details: Mapping[str, Any]) -> None:
        self.tripped.set()
        self._notifier.critical(title, details)


# ------------------------------------------------------------------------- боевой цикл


class LiveRun:
    def __init__(
        self,
        *,
        rest: SignedRest,
        session: aiohttp.ClientSession,
        barrier: LiveBarrier,
        state: LocalState,
        store: StateStore,
        safe: SafeMode,
        safe_tripped: asyncio.Event,
        notifier: Notifier,
        strategy: Strategy,
        feed: DecisionFeed,
        equity_source: EquitySource,
        guards: Sequence[RiskGuard],
        dry_run: bool = False,
        quote_source: Callable[[str], tuple[float, float] | None] | None = None,
    ):
        """quote_source - живая котировка (best bid, best ask) символа: есть - ордера идут Smart Execution (лимитки
        post-only с чейзингом), нет - прежний MARKET (каркасный фид, тесты)."""
        self.rest, self.session, self.barrier = rest, session, barrier
        self.state, self.store, self.safe, self.notifier = state, store, safe, notifier
        self.strategy, self.feed, self.equity_source = strategy, feed, equity_source
        self.guards, self.dry_run = tuple(guards), dry_run
        self._safe_tripped = safe_tripped
        self._lock = asyncio.Lock()  # один «исполнитель»: шаг цикла / (пере)синхронизация / сверка не пересекаются
        self._connected = asyncio.Event()
        self._marks: dict[str, float] = {}
        self._pos: dict[str, float] = dict(state.positions)  # позиция ДО очередного исполнения (для ВХОД/ВЫХОД)
        self._seen_triggers = 0
        self._last_row: dict[str, Mapping[str, Any]] = {}  # последняя строка признаков символа (для переспроса стратегии)
        self._equity = 0.0
        self.stream: UserDataStream | None = None
        self.ticks = 0
        self.executor: SmartExecutor | None = None
        if quote_source is not None:
            self.executor = SmartExecutor(
                barrier, quote=quote_source, clock_ms=rest.now_ms, lock=self._lock, round_qty=round_qty,
                reassess=self._reassess, force_market=self._force_market,
            )

    # ----------------------------------------------------------------- управление

    async def run(self) -> None:
        for g in self.guards:
            g.reset()
        self.stream = UserDataStream(self.rest, self.session, self._on_stream_event, on_connected=self._on_connected)
        stream_task = asyncio.create_task(self.stream.run(), name="live.user_stream")
        tasks = [stream_task]
        try:
            await self._wait_first_connection(stream_task)
            self.notifier.info(
                f"Боевой цикл запущен: профиль {config.BINANCE_PROFILE}, символы {', '.join(config.SYMBOLS)}, "
                f"{'DRY-RUN (ордера не отправляются)' if self.dry_run else 'ордера отправляются'}"
            )
            tasks += [
                asyncio.create_task(self._strategy_loop(), name="live.strategy_loop"),
                asyncio.create_task(self._watch_safe_mode(), name="live.safe_mode_watch"),
                asyncio.create_task(self._periodic_reconcile(), name="live.reconcile"),
            ]
            if self.executor is not None:
                tasks.append(asyncio.create_task(self.executor.run(), name="live.smart_execution"))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failures = [t.exception() for t in done if not t.cancelled() and t.exception() is not None]
            # Safe Mode - первопричина, остальные сбои после него вторичны.
            failures.sort(key=lambda e: not isinstance(e, SafeModeActive))
            if failures:
                raise failures[0]
            raise TradingInfraError(f"Задача {next(iter(done)).get_name()} завершилась без ошибки: так быть не должно")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _wait_first_connection(self, stream_task: asyncio.Task) -> None:
        waiter = asyncio.create_task(self._connected.wait())
        try:
            done, _ = await asyncio.wait(
                {stream_task, waiter}, timeout=config.LIVE_STARTUP_TIMEOUT_S, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiter.cancel()
        if stream_task in done:
            stream_task.result()
            raise WebSocketClosed("Приватный поток завершился при старте")
        if not done:
            raise StreamStalled(f"Приватный поток не подключился за {config.LIVE_STARTUP_TIMEOUT_S} с")

    # ------------------------------------------------------------ приватный поток

    async def _on_connected(self, reconnect: bool) -> None:
        """После каждого подключения (порядок важен: сокет уже открыт, события копятся в буфере)."""
        try:
            async with asyncio.timeout(config.LIVE_RESYNC_TIMEOUT_S):
                async with self._lock:
                    await self.barrier.resync_all()
                    if reconnect:
                        # В обрыве мог пройти стоп/ликвидация под чужим clientOrderId: их видит только сверка позиций.
                        await self._reconcile("после переподключения потока")
        except TimeoutError as exc:
            raise StreamStalled(f"Синхронизация после подключения не завершилась за {config.LIVE_RESYNC_TIMEOUT_S} с") from exc
        self._connected.set()

    def _on_stream_event(self, event: StreamEvent) -> None:
        if isinstance(event, OrderUpdate):
            self.barrier.on_order_update(event)
        elif isinstance(event, AccountUpdate):
            self.barrier.on_account_update(event)
        elif isinstance(event, AlgoUpdate):  # Этап 4: статусы условных ордеров (ML-контур их не выставляет; чужие -> Safe Mode)
            self.barrier.on_algo_update(event)
        elif isinstance(event, RawEvent):
            if event.name == "MARGIN_CALL":
                self.safe.trip("MARGIN_CALL от биржи: позиция под угрозой ликвидации", {"payload": event.payload})
            else:
                log.info("Событие потока %s пропущено", event.name)

    # ------------------------------------------------------------------- наблюдатели

    async def _watch_safe_mode(self) -> None:
        await self._safe_tripped.wait()
        raise SafeModeActive(f"Safe Mode включён: {self.safe.reason}")

    async def _reconcile(self, why: str) -> None:
        report: ReconcileReport = await reconcile(
            self.rest, self.state, self.safe, store=self.store, symbols=config.SYMBOLS
        )
        if not report.clean:
            kinds = ", ".join(sorted({d.kind.value for d in report.discrepancies}))
            raise SafeModeActive(f"Сверка ({why}): {kinds}; Safe Mode включён")
        self._pos = dict(self.state.positions)

    async def _periodic_reconcile(self) -> None:
        while True:
            await asyncio.sleep(config.LIVE_RECONCILE_INTERVAL_S)
            async with self._lock:
                if self.state.orders:
                    log.info("Периодическая сверка отложена: есть живые ордера (%d)", len(self.state.orders))
                    continue
                await self._reconcile("периодическая")

    # ------------------------------------------------------------------ торговый цикл

    async def _strategy_loop(self) -> None:
        errors = 0
        while True:
            try:
                await self.tick()
                errors = 0
            except (NetworkError, HttpServerError) as exc:
                errors += 1
                log.warning("Шаг цикла: сбой %d/%d (%s: %s)", errors, config.LIVE_MAX_CONSECUTIVE_ERRORS,
                            type(exc).__name__, exc)
                if errors >= config.LIVE_MAX_CONSECUTIVE_ERRORS:
                    raise
            await asyncio.sleep(config.LIVE_LOOP_INTERVAL_S)

    async def tick(self) -> None:
        """Один шаг цикла (вынесен для тестов)."""
        self.ticks += 1
        async with self._lock:
            now = max(self.rest.now_ms(), self.barrier.now_ts)  # смещение часов может сдвинуться назад
            self._drain(self.barrier.advance(now))
            await self.barrier.expire_due(now)
            await self.barrier.resync_pending()
        equity = await self.equity_source()
        self._equity = equity
        for g in self.guards:
            g.on_equity(now, equity)
        self._report_drawdown()
        rows = await self.feed.poll(now)
        if not rows:
            return
        async with self._lock:
            if self.safe.active:
                raise SafeModeActive(f"Safe Mode: {self.safe.reason}")
            for row in rows:
                await self._decide(row, now, equity)

    def _report_drawdown(self) -> None:
        for g in self.guards:
            if not isinstance(g, DrawdownLimitGuard):
                continue
            triggers = g.triggers
            for t in triggers[self._seen_triggers:]:
                self.notifier.drawdown(
                    drawdown=t.drawdown, limit=config.LIVE_MAX_DRAWDOWN, peak=t.peak, equity=t.equity
                )
            self._seen_triggers = len(triggers)

    def _drain(self, events: Sequence[BarrierEvent]) -> None:
        for e in events:
            if isinstance(e, Fill):
                before = self._pos.get(e.symbol, 0.0)
                after = before + e.signed_qty
                self._pos[e.symbol] = 0.0 if abs(after) <= 1e-9 else after
                self.notifier.trade(action=classify_fill(before, after), symbol=e.symbol, side=e.side.value,
                                    qty=e.qty, price=e.price, position=self._pos[e.symbol])
            elif isinstance(e, OrderClosed):
                log.warning("Ордер #%d закрыт без исполнения остатка %g: %s", e.ticket_id, e.remaining_qty, e.reason.value)
                if e.reason.value == "rejected":
                    self.notifier.warning(f"Биржа отклонила ордер #{e.ticket_id} (остаток {e.remaining_qty:g})")
            self.strategy.on_event(e)

    def _pending(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for o in self.state.orders.values():
            out[o.symbol] = out.get(o.symbol, 0.0) + o.side.sign * o.remaining
        return out

    async def _decide(self, row: Mapping[str, Any], now: int, equity: float) -> None:
        symbol = row["symbol"]
        price = row["close"]
        if price is not None and math.isfinite(price) and price > 0:
            self._marks[symbol] = float(price)
        self._last_row[symbol] = row
        ctx = self._context(symbol, row, now, equity)
        if isinstance(self.strategy, MLModelStrategy):
            self._log_prediction(symbol, ctx)
        for request in self.strategy.decide(ctx) or ():
            if not isinstance(request, OrderRequest):
                raise DataError(f"Стратегия вернула {type(request).__name__}; ожидается OrderRequest")
            await self._submit(request, now, equity)

    def _context(self, symbol: str, row: Mapping[str, Any], now: int, equity: float) -> FeatureContext:
        return FeatureContext(
            ts=now, symbol=symbol, row=MappingProxyType(dict(row)), equity=equity,
            positions=MappingProxyType({s: q for s, q in self.state.positions.items() if abs(q) > QTY_EPS}),
            pending=MappingProxyType(self._pending()), marks=MappingProxyType(dict(self._marks)),
        )

    def _reassess(self, symbol: str, side: Side, reduce_only: bool) -> Reassessment | None:
        """Переспрос стратегии при перевыставлении лимитки: просит ли она по-прежнему эту сторону, при ТЕКУЩИХ позиции
        и эквити (после частичных исполнений), и насколько сильно. None - сигнал угас или риск-гуарды не дают.

        Берётся последняя строка признаков символа (новых признаков внутри свечи нет). Не-ML стратегии переспросить
        нельзя (decide может быть с состоянием): сигнал считается актуальным, силы для отката на MARKET нет."""
        strategy = self.strategy
        if not isinstance(strategy, MLModelStrategy):
            return Reassessment(1.0, math.inf)
        row = self._last_row.get(symbol)
        if row is None:
            return None
        now = self.rest.now_ms()
        ctx = self._context(symbol, row, now, self._equity)
        for request in strategy.decide(ctx) or ():
            if isinstance(request, OrderRequest) and request.symbol == symbol and request.side is side:
                qty = self._clip_by_guards(request, now, self._equity)  # дневной стоп закрывает и продолжение входа
                if qty <= QTY_EPS:
                    return None
                pred = strategy.predict(ctx)
                return Reassessment(abs(pred) / strategy.threshold if pred is not None else 0.0, qty)
        return None

    def _force_market(self, symbol: str, reduce_only: bool) -> bool:
        """Сработавший Drawdown Guard: закрывающий ордер не ждёт лимитки (позицию надо снять немедленно)."""
        now = self.rest.now_ms()
        return reduce_only and any(isinstance(g, DrawdownLimitGuard) and g.is_blocked(now) for g in self.guards)

    def _log_prediction(self, symbol: str, ctx: FeatureContext) -> None:
        """Прогноз модели на строке: виден и в DRY-RUN, и в бою (None - признаки не готовы, причина - в логе фида)."""
        pred = self.strategy.predict(ctx)  # type: ignore[union-attr]  (вызывается только для MLModelStrategy)
        if pred is None:
            log.info("ПРЕДИКТ %s: нет (признаки не готовы: прогрев буфера стакана)", symbol)
            return
        thr = self.strategy.threshold  # type: ignore[union-attr]
        signal = "ЛОНГ" if pred >= thr else "ШОРТ" if pred <= -thr else "ниже порога"
        log.info("ПРЕДИКТ %s: %+.2f bps за горизонт метки (порог входа ±%.1f bps) -> %s, цена %g",
                 symbol, pred * 1e4, thr * 1e4, signal, ctx.price)

    def _clip_by_guards(self, request: OrderRequest, now: int, equity: float) -> float:
        qty = request.qty
        price = self._marks.get(request.symbol)
        if price is None:
            log.warning("%s: нет цены, ордер не оценить", request.symbol)
            return 0.0
        for g in self.guards:
            rctx = RiskContext(
                ts=now, symbol=request.symbol, equity=equity, price=price,
                position=self.state.position(request.symbol), pending=self._pending().get(request.symbol, 0.0),
            )
            decision = g.check(request.with_qty(qty), rctx)
            allowed = decision.qty
            if not isinstance(allowed, (int, float)) or not math.isfinite(allowed) or not 0 <= allowed <= qty + QTY_EPS:
                raise DataError(f"{g.name}: недопустимое решение {decision!r} для qty={qty}")
            if allowed < qty - QTY_EPS:
                log.warning("Риск %s: %s %g -> %g (%s)", g.name, request.symbol, qty, allowed, decision.reason)
            qty = min(allowed, qty)
            if qty <= QTY_EPS:
                return 0.0
        return qty

    async def _submit(self, request: OrderRequest, now: int, equity: float) -> None:
        qty = self._clip_by_guards(request, now, equity)
        qty = round_qty(request.symbol, qty) if qty > QTY_EPS else 0.0
        if qty <= 0:
            return
        position = self.state.position(request.symbol)
        # Ордер, только уменьшающий позицию, - reduce-only: проходит и в Safe Mode, и не может её развернуть.
        reduce_only = position != 0.0 and request.side.sign * position < 0 and qty <= abs(position) + QTY_EPS
        intent = request.with_qty(qty).to_intent(now)
        if self.dry_run:
            log.info("DRY-RUN: ордер не отправлен: %s %s %g%s", intent.side.value.upper(), intent.symbol, intent.qty,
                     " reduce-only" if reduce_only else "")
            return
        if self.executor is not None:  # Smart Execution: пассивная лимитка с чейзингом, а не MARKET
            answer = self._reassess(request.symbol, request.side, reduce_only)
            await self.executor.execute(
                request.with_qty(qty), reduce_only=reduce_only, strength=answer.strength if answer else 0.0
            )
            return
        ticket = await self.barrier.submit(intent, reduce_only=reduce_only)
        log.info("Ордер отправлен: тикет #%d %s %s %g%s", ticket.ticket_id, intent.side.value.upper(), intent.symbol,
                 intent.qty, " reduce-only" if reduce_only else "")


# ------------------------------------------------------------------------------ точка входа


async def _close_uninterrupted(close: Callable[[], Awaitable[None]], what: str) -> None:
    """Закрытие ресурса ДО КОНЦА, даже если задачу отменяют повторно (второй Ctrl+C во время выхода).

    Первая отмена уже летит по стеку (мы в finally), вторая прервала бы await close() посередине: у aiohttp
    ClientSession.close(), прерванный на закрытии коннектора, оставляет сессию незакрытой (asyncio: Unclosed client
    session), у WS-фида - висящие сокеты. Закрытие идёт отдельной задачей под shield: повторная отмена лишь
    откладывается, пока оно не завершится. Исключение закрытия - в лог: оно не должно подменять причину выхода,
    код выхода определяет исходная ошибка.
    """
    task = asyncio.ensure_future(close())
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                break
            log.warning("Повторная отмена во время закрытия (%s): дожидаюсь завершения закрытия", what)
        except Exception as exc:
            log.warning("Не удалось закрыть %s: %s: %s", what, type(exc).__name__, exc)


@contextlib.asynccontextmanager
async def _client_session(timeout: aiohttp.ClientTimeout):
    """aiohttp-сессия, закрываемая при любом выходе: нормальном, по ошибке, по SIGINT/SIGTERM (в т.ч. двойному)."""
    session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield session
    finally:
        await _close_uninterrupted(session.close, "aiohttp-сессию")


async def live_main(
    opts: Options,
    *,
    strategy: Strategy | None = None,
    feed: DecisionFeed | None = None,
    equity_source: EquitySource | None = None,
    notifier: Notifier | None = None,
) -> None:
    config.validate()
    if not config.IS_TESTNET and not opts.allow_mainnet:
        raise ConfigError("Профиль mainnet: боевой запуск запрещён без --allow-mainnet (для проверки: BINANCE_PROFILE=testnet)")
    missing = [s for s in config.SYMBOLS if s not in config.LIVE_QTY_STEP]
    if missing:
        raise ConfigError(f"Нет шага лота в config.LIVE_QTY_STEP для {missing}")
    ml = build_ml_strategy(opts)  # битая/чужая модель - выход до единого запроса к бирже
    if ml is not None and strategy is None:
        strategy = ml[0]
    if ml is not None and feed is None:
        WsLiveFeatureFeed(None, None, required_features=ml[1])  # type: ignore[arg-type]  (ранняя проверка без сети: конструктор её не трогает)
    ban_lock.ensure_not_banned()  # шаг 1: бан -> BanActive (код 2), без единого запроса к бирже
    notifier = notifier if notifier is not None else Notifier.from_env(prefix=f"[{config.BINANCE_PROFILE}] ")
    safe: SafeMode | None = None
    try:
        with InstanceLock(config.LIVE_INSTANCE_NAME):  # шаг 1: второй экземпляр -> InstanceAlreadyRunning
            tripped = asyncio.Event()
            safe = SafeMode(alerter=_AlertTap(notifier, tripped))  # читает защёлку с диска
            if safe.active:  # шаг 2
                notifier.critical("Старт отклонён: активен Safe Mode", {"reason": safe.reason})
                raise SafeModeActive(f"Safe Mode: {safe.reason}. Снимите защёлку вручную после разбора причины")
            await _run_locked(
                opts, safe, tripped, notifier, strategy, feed, equity_source,
                model_features=ml[1] if ml is not None else None,
            )
    except asyncio.CancelledError:
        notifier.info("Боевой цикл остановлен сигналом")
        raise
    except SafeModeActive:
        notifier.warning(
            f"Выход с кодом 4 (Safe Mode). Позиции по локальному учёту: {_local_positions()}. "
            "Бот их не закрывает - решение за оператором."
        )
        raise
    except Exception as exc:
        if safe is None or not safe.active:  # о Safe Mode алерт уже ушёл
            notifier.critical(f"Аварийный выход: {type(exc).__name__}", {"error": str(exc)[:500]})
        raise
    finally:
        await _close_uninterrupted(notifier.aclose, "уведомитель")  # его Telegram-сессия создаётся лениво


def _local_positions() -> Any:
    """Позиции из файла состояния для текста алерта; нечитаемый файл не должен мешать выходу."""
    try:
        return {s: q for s, q in StateStore().load().positions.items() if q} or "нет"
    except Exception:
        return "неизвестно (файл состояния не читается)"


async def _run_locked(
    opts: Options,
    safe: SafeMode,
    tripped: asyncio.Event,
    notifier: Notifier,
    strategy: Strategy | None,
    feed: DecisionFeed | None,
    equity_source: EquitySource | None,
    *,
    model_features: Sequence[str] | None = None,
) -> None:
    creds = Credentials.from_env()
    timeout = aiohttp.ClientTimeout(total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S)
    async with _client_session(timeout) as session:
        rest = SignedRest(session, config.REST_BASE_URL, creds)
        await rest.sync_time()
        store = StateStore()
        state = store.load()
        # шаг 3: первичная сверка. Расхождение -> reconcile сам включил Safe Mode и отправил алерт.
        report = await reconcile(rest, state, safe, store=store, symbols=config.SYMBOLS)
        if not report.clean:
            kinds = ", ".join(sorted({d.kind.value for d in report.discrepancies})) or "?"
            raise SafeModeActive(f"Первичная сверка не чистая ({kinds}): Safe Mode включён, ордера не отправлялись")
        barrier = LiveBarrier(
            rest,
            ExecutionConfig(config.LIVE_EXEC_LATENCY_MS, config.LIVE_MAKER_FEE_RATE, config.LIVE_TAKER_FEE_RATE),
            state, safe, store=store,
        )
        if feed is None:
            api = BinanceRest(session, config.REST_BASE_URL)
            feed = (
                # признаки модели - из канонического конвейера на потоках 500 мс (как при обучении)
                WsLiveFeatureFeed(api, session, required_features=model_features)
                if model_features is not None
                else KlineObiFeed(api)
            )
        live = LiveRun(
            rest=rest, session=session, barrier=barrier, state=state, store=store, safe=safe, safe_tripped=tripped,
            notifier=notifier,
            strategy=strategy or BaselineStrategy(equity_fraction=config.LIVE_INITIAL_EQUITY_FRACTION),
            feed=feed,
            equity_source=equity_source or AccountEquity(rest),
            guards=(DrawdownLimitGuard(config.LIVE_MAX_DRAWDOWN), MaxExposureGuard(config.LIVE_MAX_EXPOSURE)),
            dry_run=opts.dry_run,
            # живая котировка есть только у WS-фида: нет её - нечем ставить пассивный ордер, остаётся MARKET
            quote_source=getattr(feed, "best_quote", None) if config.LIVE_SMART_EXECUTION else None,
        )
        try:
            await live.run()  # шаги 4-5
        finally:
            close = getattr(feed, "aclose", None)  # WS-фид держит сокеты и фоновые задачи
            if close is not None:
                await _close_uninterrupted(close, "источник решений")


async def main() -> None:
    await live_main(parse_args())


if __name__ == "__main__":
    run(main, "live_run")
