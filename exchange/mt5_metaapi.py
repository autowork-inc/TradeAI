"""Архитектурный скелет клиента традиционных рынков (Forex, металлы) через облачный шлюз MetaApi -> MetaTrader 5.

СТАТУС: ЗАГЛУШКА. Бизнес-логики ордеров здесь нет: все торговые методы поднимают `NotImplementedError`. Файл фиксирует
контракт (какие методы, какие аргументы, какие инварианты) и политику Fail-Fast; реализация - отдельным утверждённым этапом.

Почему MetaApi, а не пакет `MetaTrader5`. Наш VPS - Linux, а нативная библиотека `MetaTrader5` работает только в Windows
(она общается с запущенным терминалом MT5 через его DLL). MetaApi (metaapi.cloud) держит терминал MT5 на своей стороне и отдаёт
REST/WebSocket-API; мы подключаемся к нему из Python-кода на любой ОС: `pip install metaapi-cloud-sdk` (импорт `metaapi_cloud_sdk`).

Соответствие `exchange.binance_live.BinanceLiveClient` (тот же набор операций, чтобы `LiveExecutionEngine` не знал о бирже):

    Binance (USDⓈ-M)                    MetaApi / MT5                           Отличие, которое нельзя потерять
    ----------------------------------  --------------------------------------  --------------------------------------------
    get_balance() -> Balance            get_balance() -> Balance                валюта счёта (USD/EUR...), не USDT; equity vs balance
    get_position(sym) -> PositionInfo   get_position(sym) -> PositionInfo       netting-счёт: одна позиция на символ; hedging: несколько
    market_order(.., qty, ..)           market_order(.., lots, ..)              объём в ЛОТАХ (volumeStep/volumeMin), не в монетах
    limit_order(.., qty, price, ..)     limit_order(.., lots, price, ..)        нет post-only (GTX); есть срок жизни ордера
    stop_loss / take_profit             set_stop_loss / set_take_profit         SL/TP - поля позиции/ордера MT5, а не отдельные
                                                                                условные ордера (нет Algo Service, нет reduceOnly)
    cancel_order / get_order            cancel_order / get_order                идемпотентность: поле `clientId` ордера (<= 28 симв.)
    realized_pnl_since                  realized_pnl_since                      история сделок (deals) терминала: профит + комиссия + своп

Что отличается от крипты и должно быть учтено при реализации:
  * Рабочие часы. У Forex/металлов есть выходные и дневная пауза: торговать вне сессии нельзя (ответ `TRADE_RETCODE_MARKET_CLOSED`).
  * Спред и своп вместо комиссии/фандинга; стопы и лимиты исполняются по Bid/Ask, SL шорта срабатывает по Ask.
  * Минимальная дистанция стопа (`stopsLevel`/`freezeLevel` брокера): слишком близкий SL отклоняется.
  * Шаг цены (`tickSize`/digits), контракт (`contractSize`), лот (`volumeStep`) приходят из спецификации символа брокера.
  * Подключение асинхронное: аккаунту нужно состояние `DEPLOYED` и `CONNECTED` (терминал связан с брокером) до первого ордера.

Fail-Fast (R1): ключи читаются из окружения в конструкторе; нет `METAAPI_TOKEN` или идентификатора аккаунта - мгновенный
`ConfigError` (код выхода 3, Docker не перезапускает вслепую), никаких попыток подключения и «режима без ключа».
Секрет в `repr`, логи и исключения не попадает.

Переменные окружения:
    METAAPI_TOKEN        токен API MetaApi (https://app.metaapi.cloud/token)
    ACCOUNT_ID           идентификатор MetaApi-аккаунта (допустим и синоним METAAPI_ACCOUNT_ID; если заданы оба - берётся METAAPI_ACCOUNT_ID)

❓ Не проверено (нужен живой аккаунт MetaApi): точные имена методов SDK `metaapi_cloud_sdk` (`MetaApi`, `get_account`,
`get_rpc_connection`, `create_market_buy_order`, ...), коды ответов `TRADE_RETCODE_*`, поведение `clientId` при повторной отправке,
набор символов и режим счёта (netting/hedging) у выбранного брокера.
"""

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import config  # noqa: F401  (подгружает .env в окружение процесса, как в exchange.binance_live)
from exchange.binance_live import Balance, PositionInfo
from utils.errors import ConfigError

log = logging.getLogger(__name__)

TOKEN_ENV = "METAAPI_TOKEN"
ACCOUNT_ENV = "ACCOUNT_ID"
ACCOUNT_ENV_ALIAS = "METAAPI_ACCOUNT_ID"

_NOT_IMPLEMENTED = "MetaApiLiveClient - архитектурный скелет: бизнес-логика ордеров MT5 ещё не реализована (см. docstring модуля)"


@dataclass(frozen=True)
class MetaApiOrder:
    """Результат операции с ордером MT5 (минимальный контракт; поля уточняются при реализации)."""

    symbol: str
    client_id: str  # clientId ордера (идемпотентность, аналог clientOrderId Binance)
    order_id: str | None  # id ордера у брокера (MT5 ticket)
    state: str  # PLACED | PARTIALLY_FILLED | FILLED | CANCELED | EXPIRED | REJECTED (нормализованные имена)
    filled_lots: float
    avg_price: float


@dataclass(frozen=True)
class MetaApiCredentials:
    token: str
    account_id: str

    def __repr__(self) -> str:  # токен не должен попадать в логи и трейсбеки
        return f"MetaApiCredentials(account_id={self.account_id!r}, token=***)"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "MetaApiCredentials":
        """Токен и аккаунт из окружения. Любого нет или пуст - `ConfigError` до какого-либо обращения к сети."""
        src = os.environ if env is None else env
        token = (src.get(TOKEN_ENV) or "").strip()
        account_id = (src.get(ACCOUNT_ENV_ALIAS) or src.get(ACCOUNT_ENV) or "").strip()
        missing = [name for name, value in ((TOKEN_ENV, token), (ACCOUNT_ENV, account_id)) if not value]
        if missing:
            raise ConfigError(f"MetaApi: не заданы переменные окружения {', '.join(missing)} (токен и id аккаунта: https://app.metaapi.cloud)")
        return cls(token=token, account_id=account_id)


class MetaApiLiveClient:
    """Клиент приватного API MT5 через MetaApi: те же операции, что у `BinanceLiveClient`, но в терминах MT5 (лоты, SL/TP позиции).

    Создание объекта НЕ подключается к сети: `__init__` только проверяет ключи (Fail-Fast). Подключение - `connect()`.
    Все торговые методы - заглушки (`NotImplementedError`).
    """

    def __init__(self, env: Mapping[str, str] | None = None, *, id_prefix: str | None = None):
        self._creds = MetaApiCredentials.from_env(env)  # ConfigError здесь - раньше любого подключения
        self.id_prefix = id_prefix if id_prefix is not None else config.CLIENT_ORDER_ID_PREFIX
        self._api: Any = None  # metaapi_cloud_sdk.MetaApi (импортируется лениво в connect(), чтобы модуль грузился без SDK)
        self._connection: Any = None  # RPC-соединение с терминалом
        self._symbols: dict[str, Any] = {}  # спецификации символов брокера: digits, tickSize, volumeStep, volumeMin, stopsLevel

    def __repr__(self) -> str:
        return f"MetaApiLiveClient(account_id={self._creds.account_id!r}, connected={self._connection is not None})"

    @property
    def account_id(self) -> str:
        return self._creds.account_id

    # ------------------------------------------------------------------------------- подключение и спецификации

    async def connect(self, *, timeout_s: float = 120.0) -> None:
        """Подключиться к аккаунту: развернуть (DEPLOYED), дождаться связи терминала с брокером (CONNECTED) и синхронизации.

        Таймаут - исключение (R1), а не «продолжаем без связи». Повторное подключение идемпотентно.
        """
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def close(self) -> None:
        """Закрыть соединение с MetaApi (позиции и ордера на стороне брокера не затрагиваются)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def load_symbols(self, symbols: list[str]) -> None:
        """Загрузить спецификации символов (digits, tickSize, volumeStep/Min/Max, contractSize, stopsLevel). Без этого ордера не уйдут."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def round_lots(self, symbol: str, lots: float) -> float:
        """Объём вниз до `volumeStep` брокера (риск не больше рассчитанного)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    def round_price(self, symbol: str, price: float) -> float:
        """Цена на сетку `tickSize` символа."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    # ------------------------------------------------------------------------------- счёт и позиции

    async def get_balance(self) -> Balance:
        """Баланс счёта в валюте депозита: wallet = balance, available = free margin, unrealized_pnl = equity - balance."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def get_position(self, symbol: str) -> PositionInfo:
        """Чистая позиция по символу: объём в лотах со знаком (> 0 buy, < 0 sell) и средняя цена входа (0.0, пока позиции нет)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def get_price(self, symbol: str) -> tuple[float, float]:
        """Текущие (bid, ask) терминала. У Forex нет единой mark price: стопы и вход считаются по Bid/Ask своей стороны."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def realized_pnl_since(self, start_ms: int, end_ms: int | None = None) -> float:
        """Результат счёта с `start_ms` по истории сделок терминала: profit + commission + swap (валюта депозита, убыток отрицателен)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    # ------------------------------------------------------------------------------- ордера

    async def market_order(self, symbol: str, side: str, lots: float, *, client_id: str, sl: float | None = None,
                           tp: float | None = None) -> MetaApiOrder:
        """MARKET по лотам. `side` - BUY | SELL. SL/TP можно приложить к ордеру (в MT5 это поля позиции); для входа без них позиция
        остаётся без стопа - вызывающий обязан сразу вызвать `set_stop_loss` (R6)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def limit_order(self, symbol: str, side: str, lots: float, price: float, *, client_id: str, expires_at_ms: int | None = None,
                          sl: float | None = None, tp: float | None = None) -> MetaApiOrder:
        """Отложенный лимитный ордер (BUY_LIMIT / SELL_LIMIT) со сроком жизни. Аналога post-only нет."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def set_stop_loss(self, symbol: str, stop_price: float, *, position_id: str | None = None) -> None:
        """Выставить/сдвинуть SL позиции (перенос в безубыток = новая цена этим же методом; окна без стопа нет: модификация атомарна)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def set_take_profit(self, symbol: str, take_price: float, *, position_id: str | None = None) -> None:
        """Выставить/сдвинуть TP позиции. Частичные тейки (Scale Out) в MT5 реализуются частичным закрытием (`close_position_part`)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def close_position_part(self, symbol: str, lots: float, *, client_id: str, position_id: str | None = None) -> MetaApiOrder:
        """Закрыть часть (или всю) позиции встречным рыночным ордером (аналог reduce-only MARKET)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def cancel_order(self, order_id: str) -> None:
        """Отменить отложенный ордер по id брокера."""
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def get_order(self, client_id: str) -> MetaApiOrder:
        """Состояние ордера по `clientId` (сверка при неизвестном исходе отправки и при перезапуске)."""
        raise NotImplementedError(_NOT_IMPLEMENTED)
