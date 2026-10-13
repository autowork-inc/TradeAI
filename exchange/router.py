"""Единый маршрутизатор исполнений: между `LiveExecutionEngine` и клиентами бирж (мульти-биржевой боевой контур, вариант А, 2026-10-13).

```
 LiveExecutionEngine ──► ExecutionRouter ──┬─ BTCUSDT, ETHUSDT ───────► BinanceLiveClient   (движок исполняет сам, разд. 3.23 CLAUDE.md)
   сигнал (Signal)                         ├─ EURUSD, XAUUSD, XAGUSD ─► MetaApiLiveClient    (ниже: имя брокера, лоты, ордер со стопом)
                                           └─ любой другой символ ────► отказ `unsupported_symbol`
```

Что делает маршрутизатор.
  * `venue_of(symbol)` / `reject_unsupported(symbol)`: куда идёт символ; чужой символ - `RouteRejected("unsupported_symbol")`. Набор символов -
    `config.LIVE_BINANCE_SYMBOLS` / `config.LIVE_MT5_SYMBOLS` (не пересекаются). Если площадка не подключена (нет клиента), её символы тоже
    `unsupported_symbol` (причина в деталях): торговать «на другой» площадке нельзя.
  * MT5-вход в два шага (между ними движок обязан записать намерение, R7): `prepare_mt5` - всё считается и проверяется, ордер НЕ отправляется;
    `send_mt5` - отправка. `open_mt5` - оба шага подряд (для тестов и простых вызовов).
        1. внутренний символ -> точное имя брокера и спецификация (`SymbolMapper`; чужой символ - `ConfigError`);
        2. торговые часы: свежая спецификация, `tradeMode != FULL` -> отказ `market_closed` (в т.ч. при старте вне сессии);
        3. счёт (Equity, свободная маржа), котировка (Bid/Ask и стоимость тика в валюте счёта), маржа 1 лота от терминала брокера;
        4. лоты по риску 1% Equity (`exchange.mt5_risk.size_position`): отказы `below_min_size` / `exposure` / `bad_risk` / `stop_too_close`;
        5. MARKET: цена брокера не дальше `max_entry_deviation` от цены сигнала (иначе `price_deviation`); LIMIT: цена на сетке тика, срок жизни;
        6. отправка ордера СО СТОПОМ (и тейком сигнала, если есть) в одном запросе (R6).
    Рынок закрыт (ответ 10018 `MARKET_CLOSED` или `tradeMode != FULL`) - ОБЫЧНЫЙ отказ сигнала `market_closed`: никакого Safe Mode и падения.
    Прочие отказы брокера (недостаточно средств, неверные стопы, реквота...) - отказ `broker_rejected` с кодом MT5. Исключение - исход
    неизвестен (таймаут, обрыв, неизвестный код): ордер мог дойти до брокера, поэтому ошибка пробрасывается (R1), повторять вслепую нельзя.
  * Сопровождение MT5-позиций (`poll_mt5`): отслеживает отложенные ордера и позиции по `clientId`; позиция исчезла (сработал SL/TP брокера) -
    событие `closed`, движок освобождает монету. ❓ Scale Out (частичные тейки), безубыток, трейлинг и тайм-аут для MT5 НЕ реализованы:
    позиция живёт со стопом и тейком, поставленными при входе; состояние берётся у брокера (источник правды), после рестарта
    `recover_mt5` подхватывает позиции и отложенные ордера, а позицию без стопа считает аварией (`Mt5UnprotectedPosition`).
  * Суточный результат площадок для сквозного лимита убытка: `realized_pnl(start_ms)` -> {"binance": ..., "mt5": ...} (USD/USDT 1:1;
    счёт MT5 в другой валюте при подключении отвергается).
"""

import logging
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import config
from exchange.binance_live import BinanceLiveClient, client_id
from exchange.mt5_metaapi import MarketClosedError, MetaApiError, MetaApiLiveClient, MetaApiOrder, MetaApiTradeError
from exchange.mt5_risk import Mt5OrderPlan, Mt5Rejected, Mt5RiskParams, size_position
from exchange.symbol_mapper import Mt5SymbolSpec, SymbolMapper
from orchestrator.types import Direction, Kind, Signal
from utils.errors import ConfigError, TradingInfraError

log = logging.getLogger("router")


class Venue(str, Enum):
    BINANCE = "binance"
    MT5 = "mt5"


class Mt5UnprotectedPosition(TradingInfraError):
    """На счёте MT5 найдена позиция нашего символа без стопа (R6): автоматически не чиним, нужен Safe Mode и решение оператора."""


@dataclass(frozen=True)
class RouteRejected:
    """Обычный отказ сигнала (не сбой): причина попадёт в журнал как `live_rejected`."""

    reason: str  # unsupported_symbol | market_closed | price_deviation | tick_value_unavailable | broker_rejected | below_min_size | exposure | bad_risk | stop_too_close | equity_unavailable
    detail: dict[str, Any] | None = None


@dataclass(frozen=True)
class Mt5Prepared:
    """MT5-вход рассчитан и проверен, ордер ещё не отправлен."""

    signal: Signal
    plan: Mt5OrderPlan
    kind: Kind
    client_id: str
    price: float  # MARKET: справочная цена брокера (Ask для BUY / Bid для SELL); LIMIT: цена заявки
    expires_at: datetime | None


@dataclass
class Mt5Tracked:
    """Живая MT5-сделка (в памяти; источник правды - брокер)."""

    signal_id: str
    strategy: str
    symbol: str  # внутреннее имя
    broker_symbol: str
    side: str
    kind: Kind
    client_id: str
    lots: float
    order_id: str | None
    position_id: str | None
    status: str  # pending (отложенный ордер) | open (позиция)
    plan: Mt5OrderPlan | None = None
    adopted: bool = False


@dataclass(frozen=True)
class Mt5Event:
    kind: str  # opened | closed | cancelled
    trade: Mt5Tracked
    detail: dict[str, Any] = field(default_factory=dict)


def _snap(price: float, spec: Mt5SymbolSpec) -> float:
    return round(round(price / spec.tick_size) * spec.tick_size, spec.digits)


class ExecutionRouter:
    def __init__(
        self,
        *,
        binance: BinanceLiveClient | None = None,
        mt5: MetaApiLiveClient | None = None,
        mapper: SymbolMapper | None = None,
        binance_symbols: Collection[str] = config.LIVE_BINANCE_SYMBOLS,
        mt5_symbols: Collection[str] = config.LIVE_MT5_SYMBOLS,
        risk_params: Mt5RiskParams | None = None,
        max_entry_deviation: float = config.LIVE_MAX_ENTRY_DEVIATION,
        limit_ttl_ms: int = 10 * 15 * 60_000,
        now_ms: Callable[[], int] | None = None,
    ):
        self._binance_symbols, self._mt5_symbols = tuple(binance_symbols), tuple(mt5_symbols)
        both = set(self._binance_symbols) & set(self._mt5_symbols)
        if both:
            raise ConfigError(f"Символ не может исполняться на двух площадках: {sorted(both)}")
        if binance is None and mt5 is None:
            raise ConfigError("ExecutionRouter: нужен хотя бы один клиент биржи")
        if mt5 is not None and mapper is None:
            raise ConfigError("ExecutionRouter: для MT5 нужен SymbolMapper (имена брокера и спецификации)")
        if not 0 < max_entry_deviation < 1 or limit_ttl_ms <= 0:
            raise ConfigError("ExecutionRouter: max_entry_deviation в (0, 1), limit_ttl_ms > 0")
        self.binance, self.mt5, self.mapper = binance, mt5, mapper
        self._params = risk_params or Mt5RiskParams()
        self._max_dev, self._ttl_ms = max_entry_deviation, limit_ttl_ms
        self._now = now_ms or (lambda: int(time.time() * 1000))
        self._prefix = (binance or mt5).id_prefix  # type: ignore[union-attr]  # одинаков у обоих клиентов (config.CLIENT_ORDER_ID_PREFIX)
        self._tracked: dict[str, Mt5Tracked] = {}  # внутренний символ -> сделка (одна на символ: LiveRiskManager.busy)

    # ------------------------------------------------------------------------------------------ маршрут

    @property
    def venues(self) -> tuple[str, ...]:
        """Подключённые площадки (порядок фиксирован): по ним риск-менеджер складывает суточный результат."""
        return tuple(v.value for v, client in ((Venue.BINANCE, self.binance), (Venue.MT5, self.mt5)) if client is not None)

    def venue_of(self, symbol: str) -> Venue | None:
        """Площадка символа или None (чужой символ либо площадка не подключена)."""
        if symbol in self._binance_symbols and self.binance is not None:
            return Venue.BINANCE
        if symbol in self._mt5_symbols and self.mt5 is not None:
            return Venue.MT5
        return None

    def reject_unsupported(self, symbol: str) -> RouteRejected | None:
        if self.venue_of(symbol) is not None:
            return None
        why = "площадка не подключена" if symbol in self._binance_symbols or symbol in self._mt5_symbols else "символ не входит ни в одну площадку"
        return RouteRejected("unsupported_symbol", {"symbol": symbol, "why": why,
                                                    "binance": list(self._binance_symbols), "mt5": list(self._mt5_symbols)})

    @property
    def tracked_mt5(self) -> dict[str, Mt5Tracked]:
        return dict(self._tracked)

    # ------------------------------------------------------------------------------- суточный результат

    async def realized_pnl(self, start_ms: int) -> dict[str, float]:
        """Реализованный результат каждой подключённой площадки с `start_ms` (убыток отрицателен), в USD/USDT."""
        out: dict[str, float] = {}
        if self.binance is not None:
            out[Venue.BINANCE.value] = await self.binance.realized_pnl_since(start_ms)
        if self.mt5 is not None:
            out[Venue.MT5.value] = await self.mt5.realized_pnl_since(start_ms)
        return out

    async def check_mt5_account(self) -> None:
        """Старт: валюта счёта MT5 обязана быть долларовой (иначе сумма с Binance была бы неверной) - `ConfigError`."""
        if self.mt5 is None:
            return
        info = await self.mt5.get_account_info()
        if info.currency not in config.LIVE_MT5_ACCOUNT_CURRENCIES:
            raise ConfigError(f"Счёт MT5 в валюте {info.currency}: суточный лимит убытка складывается с Binance 1:1, допустимы {config.LIVE_MT5_ACCOUNT_CURRENCIES}")

    # ---------------------------------------------------------------------------------------- MT5 вход

    async def prepare_mt5(self, signal: Signal, now_ms: int) -> Mt5Prepared | RouteRejected:
        """Рассчитать и проверить MT5-вход (сеть - только чтение). Ордер не отправляется."""
        assert self.mt5 is not None and self.mapper is not None  # venue_of уже проверил
        spec = self.mapper.spec(signal.symbol)  # ConfigError, если символ не загружен: Fail-Fast
        broker = spec.broker
        raw = await self.mt5.get_symbol_specification(broker)  # свежий tradeMode (кеш клиента обновляется)
        try:
            self.mt5.ensure_entry_allowed(broker)
        except MarketClosedError as exc:
            return RouteRejected("market_closed", {"why": str(exc), "trade_mode": raw.get("tradeMode")})
        side = "BUY" if signal.direction is Direction.LONG else "SELL"
        info = await self.mt5.get_account_info()
        quote = await self.mt5.get_price(broker)
        if quote.loss_tick_value is None:
            return RouteRejected("tick_value_unavailable", {"symbol": broker})
        if signal.kind is Kind.LIMIT:
            entry, expires = _snap(signal.entry, spec), datetime.fromtimestamp((now_ms + self._ttl_ms) / 1000, tz=timezone.utc)
        else:
            entry, expires = quote.ask if side == "BUY" else quote.bid, None
            dev = abs(entry - signal.entry) / signal.entry
            if dev > self._max_dev:
                return RouteRejected("price_deviation", {"broker_price": entry, "signal_entry": signal.entry, "deviation": dev, "max": self._max_dev})
        sl = _snap(signal.sl, spec)
        tp = _snap(signal.tp, spec) if signal.tp is not None else None
        margin_1lot = await self.mt5.calculate_margin(broker, side, 1.0, entry)
        sized = size_position(spec, side=side, entry=entry, sl=sl, tp=tp, equity=info.equity, free_margin=info.free_margin,
                              tick_value=quote.loss_tick_value, margin_per_lot=margin_1lot, params=self._params)
        if isinstance(sized, Mt5Rejected):
            return RouteRejected(sized.reason, sized.detail)
        return Mt5Prepared(signal, sized, signal.kind, client_id(self._prefix, "E", signal.signal_id), entry, expires)

    async def send_mt5(self, prepared: Mt5Prepared) -> Mt5Tracked | RouteRejected:
        """Отправить подготовленный MT5-вход (стоп/тейк - в том же запросе). Рынок закрыт и отказы брокера - значения; неизвестный исход - исключение."""
        assert self.mt5 is not None and self.mapper is not None
        plan, sig = prepared.plan, prepared.signal
        try:
            if prepared.kind is Kind.LIMIT:
                order = await self.mt5.limit_order(plan.broker_symbol, plan.side, plan.lots, prepared.price, plan.sl, plan.tp,
                                                   client_id=prepared.client_id, expires_at=prepared.expires_at)
            else:
                order = await self.mt5.market_order(plan.broker_symbol, plan.side, plan.lots, plan.sl, plan.tp, client_id=prepared.client_id)
        except MarketClosedError as exc:
            return RouteRejected("market_closed", {"why": str(exc), "code": exc.numeric_code})
        except MetaApiTradeError as exc:
            if exc.outcome_unknown:
                raise  # ордер мог дойти до брокера: сверка по clientId - решение вызывающего (R1)
            return RouteRejected("broker_rejected", {"code": exc.numeric_code, "name": exc.retcode_name, "broker_reason": exc.reason, "retryable": exc.retryable})
        return self._track(prepared, order)

    async def open_mt5(self, signal: Signal, now_ms: int) -> Mt5Tracked | RouteRejected:
        prepared = await self.prepare_mt5(signal, now_ms)
        return prepared if isinstance(prepared, RouteRejected) else await self.send_mt5(prepared)

    def _track(self, p: Mt5Prepared, order: MetaApiOrder) -> Mt5Tracked:
        market = p.kind is not Kind.LIMIT
        t = Mt5Tracked(p.signal.signal_id, p.signal.strategy, p.signal.symbol, p.plan.broker_symbol, p.plan.side, p.kind, p.client_id, p.plan.lots,
                       order.order_id, order.position_id if market else None, "open" if market else "pending", p.plan)
        self._tracked[t.symbol] = t
        return t

    # -------------------------------------------------------------------------------------- MT5 сопровождение

    async def poll_mt5(self) -> list[Mt5Event]:
        """Сверка живых MT5-сделок с брокером: отложенный ордер исполнен/снят, позиция закрыта (SL/TP брокера). Один запрос на всё."""
        if self.mt5 is None or not self._tracked:
            return []
        positions = await self.mt5.get_positions()
        orders = await self.mt5.get_orders()
        by_id = {p.position_id: p for p in positions}
        by_cid = {p.client_id: p for p in positions if p.client_id}
        pending_ids = {str(o.get("id")) for o in orders}
        events: list[Mt5Event] = []
        for symbol, t in list(self._tracked.items()):
            if t.status == "pending":
                if t.order_id is not None and t.order_id in pending_ids:
                    continue
                pos = by_cid.get(t.client_id)
                if pos is not None:
                    t.status, t.position_id = "open", pos.position_id
                    events.append(Mt5Event("opened", t, {"position_id": pos.position_id, "open_price": pos.open_price, "lots": pos.lots}))
                else:
                    del self._tracked[symbol]
                    events.append(Mt5Event("cancelled", t, {"reason": "expired_or_cancelled"}))
            elif t.position_id in by_id or (t.client_id in by_cid and t.position_id is None):
                continue
            else:
                del self._tracked[symbol]
                events.append(Mt5Event("closed", t, {"reason": "sl_or_tp_at_broker", "position_id": t.position_id}))
        return events

    async def cancel_pending_mt5(self) -> list[Mt5Tracked]:
        """Снять неисполненные MT5-лимитки (суточный лимит убытка достигнут: новых входов нет). Открытые позиции не трогаем."""
        cancelled: list[Mt5Tracked] = []
        if self.mt5 is None:
            return cancelled
        for symbol, t in list(self._tracked.items()):
            if t.status == "pending" and t.order_id is not None:
                await self.mt5.cancel_order(t.order_id)
                del self._tracked[symbol]
                cancelled.append(t)
        return cancelled

    async def recover_mt5(self) -> list[Mt5Tracked]:
        """Старт: подхватить позиции и наши отложенные ордера на MT5-символах. Позиция без стопа - `Mt5UnprotectedPosition`."""
        if self.mt5 is None or self.mapper is None:
            return []
        internal_of = {self.mapper.broker_name(s): s for s in self.mapper.internals if s in self._mt5_symbols}
        found: list[Mt5Tracked] = []
        for p in await self.mt5.get_positions():
            symbol = internal_of.get(p.symbol)
            if symbol is None:
                continue
            if p.stop_loss is None:
                raise Mt5UnprotectedPosition(f"{p.symbol}: на счёте MT5 позиция {p.position_id} ({p.side} {p.lots}) без стопа - Safe Mode, разберите вручную")
            if symbol in self._tracked:
                raise MetaApiError(f"{symbol}: на счёте MT5 две позиции по символу, а контур допускает одну")
            t = Mt5Tracked(f"adopted_{p.position_id}", "adopted", symbol, p.symbol, p.side, Kind.MARKET, p.client_id or "", p.lots, None, p.position_id,
                           "open", adopted=True)
            self._tracked[symbol] = t
            found.append(t)
        for o in await self.mt5.get_orders():
            symbol = internal_of.get(str(o.get("symbol")))
            cid = str(o.get("clientId") or "")
            if symbol is None or not cid.startswith(self._prefix):
                continue
            if symbol in self._tracked:
                raise MetaApiError(f"{symbol}: на счёте MT5 позиция и наш отложенный ордер одновременно")
            side = "BUY" if "BUY" in str(o.get("type", "")) else "SELL"
            t = Mt5Tracked(f"adopted_{o.get('id')}", "adopted", symbol, str(o["symbol"]), side, Kind.LIMIT, cid, float(o.get("volume") or 0.0),
                           str(o.get("id")), None, "pending", adopted=True)
            self._tracked[symbol] = t
            found.append(t)
        return found


async def build_router(binance: BinanceLiveClient | None, *, with_mt5: bool,
                       risk_params: Mt5RiskParams | None = None) -> ExecutionRouter:
    """Собирает маршрутизатор боевого контура. MT5 подключается только при `with_mt5` (ключи MetaApi из окружения, иначе `ConfigError`):
    подключение к аккаунту, загрузка имён и спецификаций `LIVE_MT5_SYMBOLS` (`allow_disabled=True`: закрытый рынок при старте не роняет процесс),
    проверка валюты счёта."""
    if not with_mt5:
        return ExecutionRouter(binance=binance, risk_params=risk_params)
    mt5 = MetaApiLiveClient()
    await mt5.connect()
    mapper = SymbolMapper(mt5, allow_disabled=True)
    await mapper.load(config.LIVE_MT5_SYMBOLS)
    router = ExecutionRouter(binance=binance, mt5=mt5, mapper=mapper, risk_params=risk_params)
    await router.check_mt5_account()
    return router
