"""Менеджер подключения cTrader для данных Forex/металлов (в коде площадка называется MT5; 2026-10-18, переведён с MetaApi на cTrader 2026-10-23): изоляция Forex/металлов от крипты и переподключение с паузой ровно 5 минут.

Зачем. Раньше фиды MT5 (`Mt5KlineFeed`, `Mt5TickFeed`) после 20 сбоев cTrader подряд поднимали исключение - падал весь процесс, а с ним и
торговля криптой (Binance), у которой с cTrader нет ничего общего. Теперь все запросы данных MT5 идут через `Mt5Link.call`:

  * сбой СВЯЗИ (`CTraderError`: таймаут, обрыв соединения, сервер отключил сессию, ошибка API) -> связь объявляется упавшей: слушатели
    `on_down` получают время (тики MT5 за простой потеряны - активные MT5-сделки Shadow помечаются `gap_affected`), соединение SDK закрывается,
    ПАУЗА `pause_s` (по умолчанию 300 с = 5 минут, `CTRADER_RECONNECT_PAUSE_S`), затем новое подключение; не вышло - снова пауза. Бесконечно:
    крипта продолжает работать, MT5-символы просто молчат (новых свечей и сигналов по ним нет);
  * всё остальное - Fail-Fast (R1), процесс падает: `ConfigError` (401/403, нет ключей, символ не найден у брокера), `DataError` (кривая свеча),
    ошибки кода. Это не «временная сеть», а поломка, которую молча переживать нельзя.

Старт тоже изолирован: первое подключение и разрешение символов (`SymbolMapper.load`) делает тот же цикл внутри фоновой задачи фида, поэтому
недоступный cTrader при запуске не задерживает и не роняет крипту. Каждое подключение заново проходит авторизацию и догружает справочники;
заодно клиент выполняет Position Recovery (`ReconcileReq`): позиции счёта читаются с сервера.

Один `Mt5Link` на процесс (общий для свечей и котировок): переподключается один из ожидающих (замок), остальные ждут его результата.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Collection
from typing import Any, TypeVar

import config
from exchange.ctrader import CTraderError, CTraderTradeError

log = logging.getLogger("orchestrator.mt5_link")

T = TypeVar("T")


class Mt5Link:
    def __init__(self, client: Any, mapper: Any, symbols: Collection[str], *, pause_s: float | None = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, clock_ms: Callable[[], int] = lambda: int(time.time() * 1000)):
        self.client, self.mapper, self.symbols = client, mapper, tuple(symbols)
        self.pause_s = config.CTRADER_RECONNECT_PAUSE_S if pause_s is None else pause_s
        if self.pause_s <= 0:
            raise ValueError("Mt5Link: pause_s > 0")
        self._sleep, self._clock = sleep, clock_ms
        self._lock = asyncio.Lock()
        self.up = False  # подключено и символы брокера разрешены
        self._mapped = False
        self._pause_next = False  # перед следующей попыткой подключения выдержать паузу (после сбоя)
        self.outages = 0  # сколько раз связь падала
        self.attempts = 0  # попыток подключения всего
        self._listeners: list[Callable[[int], None]] = []

    def on_down(self, listener: Callable[[int], None]) -> None:
        """Подписка на падение связи (аргумент - время, мс): тики MT5 с этого момента теряются."""
        self._listeners.append(listener)

    def broker_name(self, internal: str) -> str:
        """Имя символа у брокера (вызывать только из операции внутри `call`: до первого подключения маппер пуст)."""
        return self.mapper.broker_name(internal)

    async def ensure_up(self) -> None:
        """Вернуться, только когда связь есть. Пока её нет: пауза `pause_s` (если прошлая попытка или запрос упали) -> подключение -> символы."""
        async with self._lock:
            while not self.up:
                if self._pause_next:
                    log.warning("cTrader: переподключение через %.0f с (MT5-символы стоят, крипта работает)", self.pause_s)
                    await self._sleep(self.pause_s)
                self.attempts += 1
                try:
                    await self.client.connect()
                    if not self._mapped:
                        await self.mapper.load(self.symbols)
                        self._mapped = True
                except CTraderError as exc:
                    if isinstance(exc, CTraderTradeError):
                        raise
                    log.warning("cTrader: подключение не удалось (попытка %d): %s", self.attempts, exc)
                    await self.client.close()
                    self._pause_next = True
                    continue
                self.up, self._pause_next = True, False
                if self.outages:
                    log.warning("cTrader: связь восстановлена (попытка %d, падений всего %d)", self.attempts, self.outages)

    async def call(self, what: str, op: Callable[[], Awaitable[T]]) -> T:
        """Выполнить запрос данных MT5. Сбой связи -> связь упала, пауза и переподключение, затем повтор этого же запроса."""
        while True:
            await self.ensure_up()
            try:
                return await op()
            except CTraderError as exc:
                if isinstance(exc, CTraderTradeError):
                    raise  # торговый отказ брокера - не сбой связи (здесь торговых вызовов нет: это ошибка кода)
                await self._down(what, exc)

    async def _down(self, what: str, exc: BaseException) -> None:
        if self.up:
            self.up, self._pause_next = False, True
            self.outages += 1
            ts = self._clock()
            log.error("cTrader: связь потеряна (%s): %s. MT5-символы остановлены, крипта работает; переподключение через %.0f с", what, exc, self.pause_s)
            for listener in self._listeners:
                listener(ts)
            await self.client.close()  # следующий connect() поднимет новое соединение SDK (на подключённом клиенте он ничего не делает)
