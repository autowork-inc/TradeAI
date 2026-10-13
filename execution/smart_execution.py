"""Smart Execution (Этап 8.2): пассивное исполнение с чейзингом вместо рыночных ордеров.

Зачем. Taker на USDT-M фьючерсах стоит 0.05% против 0.02% у maker: круг входа и выхода по рынку - 0.10%, и он съедает
ожидание Ridge-модели. Здесь сигнал исполняется лимитным post-only ордером (LiveBarrier шлёт LIMIT с timeInForce=GTX):
BUY - по best bid, SELL - по best ask из локального стакана (WsLiveFeatureFeed.best_quote).

Жизненный цикл «чейза» (одна цель на символ):

    execute() -> LIMIT по best bid/ask ----(исполнился целиком)----------------------------------> готово
                       |
                 TTL (ORDER_TTL_SECONDS) вышел / post-only не принят (-5022)
                       |    барьер отменяет ордер (DELETE /fapi/v1/order), приходит OrderClosed с остатком
                       v
              переспрос стратегии (reassess): сигнал жив? ---нет---> отказ (позиция/ордер не нужны)
                       | да
                       v
        перевыставлений меньше LIVE_CHASE_MAX_REPOSTS? --да--> новый LIMIT по НОВОЙ котировке на остаток
                       | нет (цена убегает)
                       v
        сигнал >= LIVE_CHASE_MARKET_STRENGTH порогов входа или Drawdown Guard? --да--> MARKET на остаток
                       | нет
                       v
                 отказ от чейза (стратегия вернётся к сигналу на следующей свече)

Идемпотентность перевыставления. Каждое LiveBarrier.submit() создаёт НОВЫЙ clientOrderId (UUID4) и новый тикет:
старый id не используется повторно. Новый ордер ставится только после того, как барьер подтвердил закрытие
старого (OrderClosed) и сообщил остаток: ждать нельзя ни слепой отправки поверх живого ордера (двойная
экспозиция), ни перевыставления на полный объём после частичного исполнения. Если отмена не успела (ордер
исполнился), барьер сам узнает исполнение (userTrades) и остаток уменьшится, а при полном исполнении чейз кончается.

Окружение. Исполнитель вызывается под общим замком боевого цикла (execute() - вызывающий уже держит замок,
run()/step() берут его сами), поэтому шаги цикла, сверка и чейзинг не пересекаются. События барьера приходят через
add_listener сразу, а не раз в шаг цикла. Состояние чейза в памяти: после перезапуска «осиротевшие» лимитки
доживают до своего TTL (expire_due) и снимаются, чейз не продолжается - стратегия решит заново.

Безопасность.
  * Без живой котировки вход не ставится (пассивный ордер по устаревшей цене бессмыслен), а ВЫХОД (reduce-only)
    уходит по рынку: оставить позицию без управления хуже, чем заплатить taker.
  * Drawdown Guard: закрывающий ордер при сработавшем дневном стопе не ждёт лимитки - сразу MARKET.
  * Безвозвратные отказы биржи (маржа, фильтры) чейз не повторяет; post-only отказ (-5022) - не ошибка ордера, а
    пересечение стакана: ордер перевыставляется по свежей котировке.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple

import config
from backtester.contracts import BarrierEvent, CloseReason, Fill, OrderClosed, OrderIntent, OrderType, Side
from backtester.risk import OrderRequest
from execution.live_barrier import LiveBarrier
from utils.errors import SafeModeActive

log = logging.getLogger("smart_execution")

_EPS = 1e-12


class Reassessment(NamedTuple):
    """Ответ стратегии на «сигнал ещё актуален?»: сила в порогах входа (|прогноз| / порог) и допустимый объём."""

    strength: float
    qty: float


Quote = Callable[[str], tuple[float, float] | None]  # символ -> (best bid, best ask) или None (нет живой котировки)
Reassess = Callable[[str, Side, bool], Reassessment | None]  # (символ, сторона, reduce_only) -> None: сигнал угас
ForceMarket = Callable[[str, bool], bool]  # (символ, reduce_only) -> True: Drawdown Guard и т.п., ждать нельзя


class _State(Enum):
    LIVE = "live"  # лимитка стоит на бирже
    REPOST = "repost"  # лимитка закрыта (TTL / не принята), ждём решения о перевыставлении


@dataclass
class _Chase:
    symbol: str
    side: Side
    reduce_only: bool
    remaining: float
    strength: float
    attempts: int = 0  # сколько лимиток уже поставлено (первая + перевыставления)
    ticket_id: int | None = None
    state: _State = _State.LIVE


class SmartExecutor:
    def __init__(
        self,
        barrier: LiveBarrier,
        *,
        quote: Quote,
        clock_ms: Callable[[], int],
        lock: asyncio.Lock,
        round_qty: Callable[[str, float], float],
        reassess: Reassess | None = None,
        force_market: ForceMarket | None = None,
        ttl_s: float | None = None,
        max_reposts: int | None = None,
        market_strength: float | None = None,
        poll_s: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        """quote - источник котировок; clock_ms - время биржи (как у барьера); lock - общий замок боевого цикла;
        round_qty - округление объёма вниз до шага лота (остаток после частичного исполнения - не кратен шагу);
        reassess - переспрос стратегии (None - сигнал считается вечно актуальным); force_market - обход ожидания."""
        self._barrier, self._quote, self._clock = barrier, quote, clock_ms
        self._lock, self._round_qty = lock, round_qty
        self._reassess, self._force = reassess, force_market
        self._ttl_ms = int((ttl_s if ttl_s is not None else config.ORDER_TTL_SECONDS) * 1000)
        self._max_reposts = max_reposts if max_reposts is not None else config.LIVE_CHASE_MAX_REPOSTS
        self._market_strength = market_strength if market_strength is not None else config.LIVE_CHASE_MARKET_STRENGTH
        self._poll_s = poll_s if poll_s is not None else config.LIVE_CHASE_POLL_S
        self._sleep = sleep
        self._chases: dict[str, _Chase] = {}
        self.stats = {"limit_placed": 0, "reposts": 0, "market_fallback": 0, "completed": 0, "abandoned": 0}
        barrier.add_listener(self.on_event)

    @property
    def active_symbols(self) -> tuple[str, ...]:
        return tuple(self._chases)

    def _lots(self, symbol: str, qty: float) -> float:
        """Объём вниз до шага лота. Остаток = объём минус частичные исполнения несёт шум float (0.5 - 0.1 - 0.2 =
        0.19999999999999998): прямое округление вниз потеряло бы целый лот (0.199), поэтому перед ним - поправка 1e-9
        от величины, много меньше любого реального шага."""
        return self._round_qty(symbol, qty * (1.0 + 1e-9))

    # ------------------------------------------------------------------------------ вход

    async def execute(self, request: OrderRequest, *, reduce_only: bool, strength: float = 1.0) -> None:
        """Берёт сигнал в исполнение. Вызывающий УЖЕ держит замок боевого цикла (как и при barrier.submit).

        strength - сила сигнала в порогах входа (|прогноз| / порог): от неё зависит, можно ли в конце чейза бить
        по рынку. Повторный запрос в ту же сторону по символу с живым чейзом игнорируется (стратегия не видит
        «зазор» между отменой и перевыставлением и просит то же самое); встречный - снимает чейз."""
        existing = self._chases.get(request.symbol)
        if existing is not None:
            if existing.side is request.side:
                log.info("%s: чейз %s уже идёт, повторный запрос пропущен", request.symbol, existing.side.value)
                return
            log.info("%s: встречный сигнал, чейз %s снимается", request.symbol, existing.side.value)
            await self._abort(existing)
            return  # встречный запрос посчитан по позиции, которая могла измениться при снятии: решит следующая свеча
        qty = self._lots(request.symbol, request.qty)
        if qty <= _EPS:
            return
        chase = _Chase(request.symbol, request.side, reduce_only, qty, strength)
        self._chases[request.symbol] = chase
        try:
            await self._advance(chase, first=True)
        except BaseException:
            self._chases.pop(request.symbol, None)
            raise

    # --------------------------------------------------------------------------- события

    def on_event(self, event: BarrierEvent) -> None:
        """Слушатель событий барьера (синхронный). Fill уменьшает остаток, OrderClosed - даёт решение о перевыставлении."""
        if not isinstance(event, (Fill, OrderClosed)):
            return
        chase = next((c for c in self._chases.values() if c.ticket_id == event.ticket_id), None)
        if chase is None:
            return
        if isinstance(event, Fill):
            chase.remaining -= event.qty
            if self._lots(chase.symbol, max(chase.remaining, 0.0)) <= _EPS:
                self._finish(chase, "исполнен", completed=True)
            return
        chase.ticket_id = None
        chase.remaining = event.remaining_qty  # факт барьера: учтены все частичные исполнения
        if event.reason is CloseReason.REJECTED:
            self._finish(chase, "биржа отклонила ордер")  # маржа/фильтр: повтор ничего не изменит
        else:
            chase.state = _State.REPOST  # TTL, отмена или post-only не принят

    # ------------------------------------------------------------------ фоновый цикл

    async def run(self) -> None:
        """Фоновая задача: каждые poll_s снимает просроченные лимитки и перевыставляет закрытые. Живёт до отмены."""
        while True:
            await self._sleep(self._poll_s)
            async with self._lock:
                await self.step()

    async def step(self) -> None:
        """Один шаг (вызывающий держит замок): TTL-отмены, затем решения по закрытым лимиткам."""
        await self._barrier.expire_due(self._clock())
        for chase in list(self._chases.values()):
            if self._chases.get(chase.symbol) is not chase or chase.state is not _State.REPOST:
                continue
            try:
                await self._advance(chase, first=False)
            except SafeModeActive as exc:
                self._finish(chase, f"Safe Mode: {exc}")

    # ---------------------------------------------------------------------------- логика

    async def _advance(self, chase: _Chase, *, first: bool) -> None:
        """Поставить лимитку, перевыставить её, откатиться на MARKET или отказаться - по правилам модуля."""
        if not first:
            answer = self._reassess(chase.symbol, chase.side, chase.reduce_only) if self._reassess else \
                Reassessment(chase.strength, chase.remaining)
            if answer is None:
                self._finish(chase, "сигнал больше не актуален")
                return
            chase.strength = answer.strength
            chase.remaining = self._lots(chase.symbol, min(chase.remaining, answer.qty))
            if chase.remaining <= _EPS:
                self._finish(chase, "остаток меньше шага лота")
                return
        if self._force is not None and self._force(chase.symbol, chase.reduce_only):
            await self._market(chase, "защитное правило (Drawdown Guard): ждать лимитку нельзя")
        elif chase.attempts > self._max_reposts:
            if chase.strength >= self._market_strength:
                await self._market(chase, f"цена убегает после {self._max_reposts} перевыставлений, сигнал сильный "
                                          f"({chase.strength:.2f} порога)")
            else:
                self._finish(chase, f"цена убегает после {self._max_reposts} перевыставлений, сигнал не настолько "
                                    f"сильный для MARKET ({chase.strength:.2f} < {self._market_strength:g})")
        else:
            quote = self._quote(chase.symbol)
            if quote is None or not 0 < quote[0] < quote[1]:
                if chase.reduce_only:
                    await self._market(chase, "нет живой котировки, позицию нужно закрыть")
                else:
                    self._finish(chase, "нет живой котировки: вход не ставится")
            else:
                await self._limit(chase, quote)

    async def _limit(self, chase: _Chase, quote: tuple[float, float]) -> None:
        bid, ask = quote
        price = bid if chase.side is Side.BUY else ask  # пассивно: на своей стороне стакана
        intent = OrderIntent(
            chase.symbol, chase.side, chase.remaining, self._clock(), OrderType.LIMIT, price, self._ttl_ms
        )
        chase.attempts += 1
        self.stats["limit_placed"] += 1
        if chase.attempts > 1:
            self.stats["reposts"] += 1
        chase.state = _State.LIVE
        chase.ticket_id = self._barrier.next_ticket_id  # до submit: отказ может прийти событием ещё внутри него
        log.info("%s: %s LIMIT %g @ %g (попытка %d, TTL %.0f с%s)", chase.symbol, chase.side.value.upper(),
                 chase.remaining, price, chase.attempts, self._ttl_ms / 1000, ", reduce-only" if chase.reduce_only else "")
        await self._barrier.submit(intent, reduce_only=chase.reduce_only)

    async def _market(self, chase: _Chase, why: str) -> None:
        qty = chase.remaining
        self._finish(chase, f"MARKET на {qty:g}: {why}", fallback=True)  # до submit: дальше за ордером следит барьер
        intent = OrderIntent(chase.symbol, chase.side, qty, self._clock())
        await self._barrier.submit(intent, reduce_only=chase.reduce_only)

    async def _abort(self, chase: _Chase) -> None:
        ticket_id = chase.ticket_id
        self._finish(chase, "снят встречным сигналом")
        if ticket_id is not None:
            await self._barrier.cancel(ticket_id)

    def _finish(self, chase: _Chase, why: str, *, completed: bool = False, fallback: bool = False) -> None:
        if self._chases.get(chase.symbol) is not chase:
            return
        del self._chases[chase.symbol]
        chase.ticket_id = None
        if fallback:
            self.stats["market_fallback"] += 1
            log.warning("%s: %s", chase.symbol, why)
        elif completed:
            self.stats["completed"] += 1
            log.info("%s: чейз завершён (%s, попыток %d)", chase.symbol, why, chase.attempts)
        else:
            self.stats["abandoned"] += 1
            log.info("%s: чейз прекращён: %s (остаток %g, попыток %d)", chase.symbol, why, chase.remaining, chase.attempts)
