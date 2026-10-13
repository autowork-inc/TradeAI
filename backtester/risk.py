"""Риск-менеджмент: гуарды, через которые проходит КАЖДЫЙ ордер до отправки в ExecutionBarrier.

Гуард не подаёт ордера и не трогает позицию: он получает запрос стратегии и снимок
состояния (RiskContext) и возвращает решение - сколько из запрошенного количества можно
отправить (RiskDecision.qty: 0 = блок, меньше запрошенного = урезание, столько же = пропуск).
Гуарды применяются по цепочке: следующий видит уже урезанное количество предыдущего.

Правила, общие для всех гуардов:
  * Риск считается по ЭКСПОЗИЦИИ = исполненная позиция + ещё не исполненные ордера. Иначе два
    запроса подряд в одном решении, каждый из которых по отдельности проходит лимит, вместе
    его нарушили бы.
  * Ордер, уменьшающий экспозицию, лимитами «на открытие» не блокируется: заблокировать
    закрытие убыточной позиции значит увеличить убыток. Ордер, разворачивающий позицию,
    делится на закрывающую и открывающую часть (split_closing_opening); блокируется только
    открывающая.
  * Гуарды видят только то, что известно в момент решения (эквити считается по ценам,
    известным на asof_ts). Состояние обновляется через on_equity(ts, equity) - движок зовёт
    его на каждом шаге ДО решения стратегии, поэтому просадка замечена в тот же момент, а не
    при следующей попытке выставить ордер.

Торговый день - сутки UTC (ts // DAY_MS).
"""

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace

from backtester.contracts import OrderIntent, OrderType, Side
from utils.errors import ConfigError, DataError

DAY_MS = 86_400_000
QTY_EPS = 1e-12


class RiskError(DataError):
    """Гуард вернул недопустимое решение."""


@dataclass(frozen=True)
class OrderRequest:
    """Запрос стратегии. Времени решения здесь нет: asof_ts проставляет движок."""

    symbol: str
    side: Side
    qty: float
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    ttl_ms: int | None = None

    def __post_init__(self) -> None:
        # Единые правила с OrderIntent (qty > 0, LIMIT = цена + срок жизни): проверяем его же кодом.
        probe = self.to_intent(1)
        object.__setattr__(self, "side", probe.side)
        object.__setattr__(self, "order_type", probe.order_type)

    def to_intent(self, asof_ts: int) -> OrderIntent:
        return OrderIntent(
            self.symbol, self.side, self.qty, asof_ts,
            order_type=self.order_type, limit_price=self.limit_price, ttl_ms=self.ttl_ms,
        )

    def with_qty(self, qty: float) -> "OrderRequest":
        return replace(self, qty=qty)


@dataclass(frozen=True)
class RiskContext:
    ts: int
    symbol: str
    equity: float  # на момент решения, по известным ценам
    price: float | None  # последняя известная цена символа; None - цены ещё не было
    position: float  # исполненная знаковая позиция символа
    pending: float  # знаковая сумма неисполненных остатков открытых ордеров символа

    @property
    def exposure(self) -> float:
        return self.position + self.pending

    @property
    def day(self) -> int:
        return self.ts // DAY_MS


@dataclass(frozen=True)
class RiskDecision:
    qty: float  # сколько можно отправить: 0 - блок
    reason: str | None = None

    @classmethod
    def allow(cls, qty: float) -> "RiskDecision":
        return cls(qty)

    @classmethod
    def block(cls, reason: str) -> "RiskDecision":
        return cls(0.0, reason)

    @classmethod
    def clip(cls, qty: float, reason: str) -> "RiskDecision":
        return cls(max(qty, 0.0), reason)


def split_closing_opening(side: Side, qty: float, exposure: float) -> tuple[float, float]:
    """Ордер -> (закрывающая часть, открывающая часть) относительно знаковой экспозиции.

    Закрывающая уменьшает |экспозицию|, открывающая её увеличивает (в том числе после разворота):
    экспозиция +1, SELL 3 -> (1, 2).
    """
    closing = min(qty, abs(exposure)) if side.sign * exposure < 0 else 0.0
    return closing, max(qty - closing, 0.0)


class RiskGuard(ABC):
    """Проверка запроса перед отправкой в барьер. Гуард хранит состояние: на каждый прогон reset()."""

    name: str = "guard"

    def reset(self) -> None:
        """Сброс состояния перед новым прогоном."""

    def on_equity(self, ts: int, equity: float) -> None:
        """Новая точка эквити (на каждом шаге движка, до решения стратегии)."""

    @abstractmethod
    def check(self, request: OrderRequest, ctx: RiskContext) -> RiskDecision:
        """Сколько из request.qty можно отправить. Больше запрошенного вернуть нельзя."""


@dataclass(frozen=True)
class DrawdownTrigger:
    ts: int
    drawdown: float
    peak: float
    equity: float


class DrawdownLimitGuard(RiskGuard):
    """Daily stop-loss: просадка эквити выше max_drawdown -> новые позиции закрыты до конца дня UTC.

    max_drawdown - ДОЛЯ (0.05 = 5%). Просадка = (пик - эквити) / пик, срабатывает строго при >.
    Срабатывание блокирует открывающую часть любых ордеров до 00:00 UTC следующих суток;
    закрывающие ордера проходят. Открытые позиции гуард сам не закрывает.

    reference - от какого пика считается просадка:
      "day" (по умолчанию) - пик сбрасывается в начале суток на последнее эквити предыдущих
            суток: лимит относится к убытку за торговый день, новый день начинается с чистого листа;
      "run" - пик за весь прогон (high-water mark). Если просадка после полуночи всё ещё выше
            лимита, гуард срабатывает снова в первой же точке нового дня, т.е. торговля стоит, пока
            эквити не вернётся к пику (оставшиеся позиции или ничего).
    """

    name = "drawdown_limit"

    def __init__(self, max_drawdown: float, *, reference: str = "day"):
        if isinstance(max_drawdown, bool) or not isinstance(max_drawdown, (int, float)) \
                or not math.isfinite(max_drawdown) or not 0 < max_drawdown < 1:
            raise ConfigError(f"max_drawdown - доля в (0, 1) (0.05 = 5%), получено {max_drawdown!r}")
        if reference not in ("day", "run"):
            raise ConfigError(f"reference должен быть 'day' или 'run', получено {reference!r}")
        self._limit = float(max_drawdown)
        self._reference = reference
        self.reset()

    @property
    def triggers(self) -> tuple[DrawdownTrigger, ...]:
        return tuple(self._triggers)

    def reset(self) -> None:
        self._day: int | None = None
        self._peak = 0.0
        self._last_equity = 0.0
        self._blocked_day: int | None = None
        self._triggers: list[DrawdownTrigger] = []

    def on_equity(self, ts: int, equity: float) -> None:
        day = ts // DAY_MS
        if self._day is None:
            self._peak = equity
        elif day != self._day and self._reference == "day":
            self._peak = self._last_equity  # перенос эквити через полночь: гэп на открытии засчитывается
        self._day = day
        self._peak = max(self._peak, equity)
        self._last_equity = equity
        drawdown = (self._peak - equity) / self._peak if self._peak > 0 else 1.0
        if drawdown > self._limit and self._blocked_day != day:
            self._blocked_day = day
            self._triggers.append(DrawdownTrigger(ts, drawdown, self._peak, equity))

    def is_blocked(self, ts: int) -> bool:
        return self._blocked_day is not None and self._blocked_day == ts // DAY_MS

    def check(self, request: OrderRequest, ctx: RiskContext) -> RiskDecision:
        if not self.is_blocked(ctx.ts):
            return RiskDecision.allow(request.qty)
        reason = f"daily stop: просадка > {self._limit:.2%}, новые позиции закрыты до конца дня UTC"
        closing, _ = split_closing_opening(request.side, request.qty, ctx.exposure)
        return RiskDecision.clip(closing, reason) if closing > QTY_EPS else RiskDecision.block(reason)


class MaxExposureGuard(RiskGuard):
    """Потолок плеча: |экспозиция| * цена <= max_exposure * эквити, на каждый символ.

    max_exposure - отношение номинала позиции к эквити: 1.0 = торговля без плеча, 3.0 = плечо 3x,
    0.02 = позиция не больше 2% эквити. Это лимит размера позиции, а не риск «до стоп-лосса»:
    расстояния до стопа гуард не знает (риск по стопу - отдельный гуард).
    Запрос урезается до остатка лимита; уменьшающие экспозицию ордера проходят целиком, а при
    развороте противоположная сторона получает не больше лимита. Нет цены или эквити <= 0 -
    открывать нельзя.
    """

    name = "max_exposure"

    def __init__(self, max_exposure: float):
        if isinstance(max_exposure, bool) or not isinstance(max_exposure, (int, float)) \
                or not math.isfinite(max_exposure) or max_exposure <= 0:
            raise ConfigError(f"max_exposure должен быть > 0 (1.0 = без плеча, 3.0 = плечо 3x), получено {max_exposure!r}")
        self._fraction = float(max_exposure)

    def check(self, request: OrderRequest, ctx: RiskContext) -> RiskDecision:
        exposure = ctx.exposure
        closing, opening = split_closing_opening(request.side, request.qty, exposure)
        if opening <= QTY_EPS:
            return RiskDecision.allow(request.qty)  # только уменьшает экспозицию
        if ctx.price is None or ctx.price <= 0:
            return RiskDecision.clip(closing, "нет цены символа: размер позиции не оценить") \
                if closing > QTY_EPS else RiskDecision.block("нет цены символа: размер позиции не оценить")
        max_qty = max(self._fraction * ctx.equity / ctx.price, 0.0)
        # Что уже занято в сторону запроса: при добавлении - |экспозиция|, при развороте - 0 (закрытая часть
        # освобождает место, после неё позиция начинается с нуля).
        used = abs(exposure) if closing <= QTY_EPS else 0.0
        room = max(max_qty - used, 0.0)
        allowed = closing + min(opening, room)
        if allowed >= request.qty - QTY_EPS:
            return RiskDecision.allow(request.qty)
        reason = f"лимит экспозиции {self._fraction:g}x эквити ({max_qty:.8g} при цене {ctx.price:.8g})"
        return RiskDecision.clip(allowed, reason) if allowed > QTY_EPS else RiskDecision.block(reason)
