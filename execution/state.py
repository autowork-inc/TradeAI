"""Локальное состояние бота и Safe Mode.

LocalState - то, что бот сам считает правдой: позиции и свои живые ордера. Он пишется на диск
(атомарно) и переживает перезапуск; после старта reconcile сверяет его с биржей.

Запись «до отправки» (write-ahead): ордер попадает в LocalState и на диск раньше, чем уйдёт POST.
Если процесс умер между отправкой и ответом, после перезапуска сверка по clientOrderId узнает, что с
ордером случилось. Обратный порядок (сначала POST, потом запись) оставил бы «чужой» ордер на бирже.

Safe Mode - защёлка на диске (<DATA_DIR>/.safe_mode), как .ban_lock: переживает перезапуск процесса и
контейнера и снимается ТОЛЬКО вручную (SafeMode.clear или удаление файла) после разбора причины.
Пока защёлка стоит, новые позиции не открываются (SafeModeActive); закрывающие ордера (reduce-only) и
отмены разрешены, иначе бот не смог бы сам уменьшить риск. Если защёлку записать не удалось, режим всё
равно включён в памяти.
"""

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import config
from backtester.contracts import OrderType, Side
from utils.errors import DataError, SafeModeActive
from utils.storage import atomic_write_text

log = logging.getLogger(__name__)

STATE_VERSION = 1  # без algo-данных файл пишется версией 1: ML-контур и старые читатели не ломаются
STATE_VERSION_ALGO = 2  # + algo_orders, protections, triggered_order_ids (Этап 4)
MAX_TRIGGERED_IDS = 500
ALGO_ACTIVE = ("PENDING", "NEW", "TRIGGERING", "TRIGGERED")  # algo-ордер ещё не завершён
ALGO_TERMINAL = ("FINISHED", "CANCELED", "EXPIRED", "REJECTED")
PENDING, NEW, PARTIALLY_FILLED = "PENDING", "NEW", "PARTIALLY_FILLED"  # статусы живых локальных ордеров


# ------------------------------------------------------------------ локальное состояние


@dataclass
class LocalOrder:
    """Свой живой ордер. Хранит всё, чтобы после перезапуска восстановить OrderTicket."""

    client_order_id: str
    ticket_id: int
    symbol: str
    side: Side
    order_type: OrderType
    qty: float
    limit_price: float | None
    ttl_ms: int | None
    asof_ts: int
    exec_ts: int
    expire_ts: int | None
    reduce_only: bool = False
    status: str = PENDING  # PENDING: записан, ответа биржи ещё нет
    filled_qty: float = 0.0

    @property
    def remaining(self) -> float:
        return self.qty - self.filled_qty

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["side"], d["order_type"] = self.side.value, self.order_type.value
        return d

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "LocalOrder":
        try:
            return cls(**{**d, "side": Side(d["side"]), "order_type": OrderType(d["order_type"])})
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Повреждённая запись ордера в состоянии: {exc}; {str(d)[:200]}") from exc


@dataclass
class LocalAlgoOrder:
    """Свой условный (algo) ордер: защитный стоп или тейк. Записывается в состояние ДО отправки (status=PENDING),
    как и обычный ордер: упал процесс - сверка найдёт его по clientAlgoId."""

    client_algo_id: str
    symbol: str
    side: Side  # сторона ЗАКРЫТИЯ: SELL закрывает лонг, BUY - шорт
    role: str  # sl | tp | be_sl
    order_type: str  # STOP_MARKET | TAKE_PROFIT_MARKET
    trigger_price: float
    created_ms: int
    close_position: bool = False  # О18: защита ставится quantity + reduceOnly; closePosition=true оставлен для чтения старых файлов
    qty: float = 0.0  # объём ордера (при close_position=False); стопы и тейки покрывают позицию суммой объёмов
    working_type: str = "CONTRACT_PRICE"
    parent: str | None = None  # clientOrderId входа / id сигнала
    algo_id: int | None = None  # algoId биржи (после ответа)
    status: str = "PENDING"  # PENDING (ответа ещё нет) | NEW | TRIGGERING | TRIGGERED | FINISHED | CANCELED | EXPIRED | REJECTED
    actual_order_id: int | None = None  # ai: id ордера в матчинг-движке после срабатывания
    replaces: str | None = None  # clientAlgoId старого стопа, вместо которого поставлен этот (перенос в безубыток)
    filled_qty: float = 0.0

    @property
    def active(self) -> bool:
        return self.status in ALGO_ACTIVE

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["side"] = self.side.value
        return d

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "LocalAlgoOrder":
        try:
            return cls(**{**d, "side": Side(d["side"])})
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Повреждённая запись algo-ордера в состоянии: {exc}; {str(d)[:200]}") from exc


@dataclass
class Protection:
    """Намерение защитить позицию монеты: стоп (и тейк) и безубыток. Хранится на диске, пока позиция жива:
    если стопа на бирже нет, а позиция есть, barrier.pump() поставит его заново (R6), в том числе после падения
    посреди переноса в безубыток. Одна защита на монету (правило владельца монеты, D4).

    Цена входа уточняется по фактическим исполнениям родительского ордера (fill_qty / fill_cost): безубыток и
    TP по R считаются от неё, а не от плановой цены (у MARKET-входа они различаются)."""

    symbol: str
    direction: int  # +1 лонг, -1 шорт: знак ожидаемой позиции
    entry_price: float  # плановая цена входа; после исполнений - средняя фактическая
    sl_price: float
    parent: str  # clientOrderId входного ордера
    created_ms: int
    tp_price: float | None = None  # явный тейк (LIMIT-вход, цена известна заранее)
    tp_r: float | None = None  # или тейк в R от фактического входа (MARKET-вход); оба None - без тейка
    be_r: float | None = None  # перенос стопа на вход при +be_r*R (D11: 1.0); None - без безубытка
    be_done: bool = False  # безубыток запрошен/выполнен: нужный стоп - be_sl на entry_price (старые sl снимаются после него)
    be_failed: bool = False  # биржа окончательно отказала в be_sl: остаётся исходный sl, перенос больше не запрашивается
    fill_qty: float = 0.0
    fill_cost: float = 0.0

    @property
    def closing_side(self) -> Side:
        return Side.SELL if self.direction > 0 else Side.BUY

    @property
    def risk(self) -> float:
        return abs(self.entry_price - self.sl_price)

    @property
    def stop_price(self) -> float:
        return self.entry_price if self.be_done else self.sl_price

    def effective_tp(self) -> float | None:
        if self.tp_price is not None:
            return self.tp_price
        if self.tp_r is not None:
            return self.entry_price + self.direction * self.tp_r * self.risk
        return None

    def be_trigger(self) -> float | None:
        return None if self.be_r is None else self.entry_price + self.direction * self.be_r * self.risk

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Protection":
        try:
            return cls(**d)
        except (TypeError, ValueError) as exc:
            raise DataError(f"Повреждённая запись защиты в состоянии: {exc}; {str(d)[:200]}") from exc


@dataclass
class LocalState:
    positions: dict[str, float] = field(default_factory=dict)  # знаковая позиция, one-way режим
    orders: dict[str, LocalOrder] = field(default_factory=dict)  # clientOrderId -> ордер
    next_ticket_id: int = 1
    algo_orders: dict[str, LocalAlgoOrder] = field(default_factory=dict)  # clientAlgoId -> условный ордер
    protections: dict[str, Protection] = field(default_factory=dict)  # symbol -> защита позиции
    triggered_order_ids: dict[str, str] = field(default_factory=dict)  # id ордера в движке (ai) -> clientAlgoId: «свои» исполнения

    def position(self, symbol: str) -> float:
        return self.positions.get(symbol, 0.0)

    def algo_for(self, symbol: str, *roles: str, active_only: bool = True) -> list[LocalAlgoOrder]:
        return [a for a in self.algo_orders.values()
                if a.symbol == symbol and (not roles or a.role in roles) and (a.active or not active_only)]

    def remember_triggered(self, order_id: int, client_algo_id: str) -> None:
        self.triggered_order_ids[str(order_id)] = client_algo_id
        while len(self.triggered_order_ids) > MAX_TRIGGERED_IDS:
            self.triggered_order_ids.pop(next(iter(self.triggered_order_ids)))

    def to_json(self) -> str:
        has_algo = bool(self.algo_orders or self.protections or self.triggered_order_ids)
        payload: dict[str, Any] = {
            "version": STATE_VERSION_ALGO if has_algo else STATE_VERSION,
            "next_ticket_id": self.next_ticket_id,
            "positions": self.positions,
            "orders": [o.to_dict() for o in self.orders.values()],
            "written_at": int(time.time() * 1000),
        }
        if has_algo:
            payload["algo_orders"] = [a.to_dict() for a in self.algo_orders.values()]
            payload["protections"] = [p.to_dict() for p in self.protections.values()]
            payload["triggered_order_ids"] = self.triggered_order_ids
        return json.dumps(payload, ensure_ascii=False, indent=1)

    @classmethod
    def from_json(cls, raw: str) -> "LocalState":
        try:
            data = json.loads(raw)
            if data["version"] not in (STATE_VERSION, STATE_VERSION_ALGO):
                raise ValueError(f"версия {data['version']!r}, ожидается {STATE_VERSION} или {STATE_VERSION_ALGO}")
            orders = [LocalOrder.from_dict(o) for o in data["orders"]]
            algo = [LocalAlgoOrder.from_dict(a) for a in data.get("algo_orders", ())]
            protections = [Protection.from_dict(p) for p in data.get("protections", ())]
            return cls(
                positions={str(s): float(q) for s, q in data["positions"].items()},
                orders={o.client_order_id: o for o in orders},
                next_ticket_id=int(data["next_ticket_id"]),
                algo_orders={a.client_algo_id: a for a in algo},
                protections={p.symbol: p for p in protections},
                triggered_order_ids={str(k): str(v) for k, v in data.get("triggered_order_ids", {}).items()},
            )
        except DataError:
            raise
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise DataError(f"Файл состояния повреждён ({exc}); начало: {raw[:200]!r}") from exc


class StateStore:
    """Файл состояния. Отсутствует -> пустое состояние (первый запуск); повреждён -> DataError."""

    def __init__(self, path: Path | None = None):
        self._path = path

    @property
    def path(self) -> Path:
        return self._path if self._path is not None else config.DATA_DIR / config.ML_STATE_NAME

    def load(self) -> LocalState:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return LocalState()
        except OSError as exc:
            raise DataError(f"Не удалось прочитать {self.path}: {exc}") from exc
        # Битый файл НЕ заменяем пустым состоянием: пустое состояние = «позиций нет», а они могут быть.
        return LocalState.from_json(raw)

    def save(self, state: LocalState) -> None:
        atomic_write_text(self.path, state.to_json() + "\n")


# ------------------------------------------------------------------------------ алерты


class Alerter(Protocol):
    def critical(self, title: str, details: Mapping[str, Any]) -> None: ...


class LogAlerter:
    """Критический алерт в лог (консоль + файл). Подмените на отправку в Telegram/PagerDuty."""

    def critical(self, title: str, details: Mapping[str, Any]) -> None:
        log.critical("АЛЕРТ: %s | %s", title, json.dumps(details, ensure_ascii=False, default=str))


# --------------------------------------------------------------------------- Safe Mode


class SafeMode:
    def __init__(self, path: Path | None = None, alerter: Alerter | None = None):
        self._path = path
        self._alerter: Alerter = alerter if alerter is not None else LogAlerter()
        self._active = False
        self._reason = ""
        self._load()

    @property
    def path(self) -> Path:
        return self._path if self._path is not None else config.DATA_DIR / config.SAFE_MODE_LOCK_NAME

    @property
    def active(self) -> bool:
        return self._active

    @property
    def reason(self) -> str:
        return self._reason

    def _load(self) -> None:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except OSError as exc:
            self._active, self._reason = True, f"замок {self.path} не читается: {exc}"  # не знаем -> считаем включённым
            log.critical("Safe Mode: %s", self._reason)
            return
        try:
            self._reason = str(json.loads(raw)["reason"])
        except (ValueError, KeyError, TypeError):
            self._reason = f"замок {self.path} повреждён: {raw[:100]!r}"
        self._active = True
        log.critical("Safe Mode остаётся включённым с прошлого запуска: %s. Снять - только вручную.", self._reason)

    def require_trading(self) -> None:
        """Вызывается перед любым ордером, увеличивающим риск."""
        if self._active:
            raise SafeModeActive(f"Safe Mode: {self._reason}")

    def trip(self, reason: str, details: Mapping[str, Any] | None = None) -> bool:
        """Включает Safe Mode. True - включён сейчас, False - уже был включён (алерт не дублируется)."""
        if self._active:
            log.warning("Safe Mode уже включён (%s); новая причина: %s", self._reason, reason)
            return False
        self._active, self._reason = True, reason
        payload = {"reason": reason, "details": dict(details or {}), "at_ms": int(time.time() * 1000)}
        try:
            atomic_write_text(self.path, json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        except Exception:
            log.critical("Safe Mode: защёлку записать не удалось, режим включён только в памяти", exc_info=True)
        try:
            self._alerter.critical(f"SAFE MODE: {reason}", payload["details"])
        except Exception:
            log.critical("Не удалось отправить алерт о Safe Mode: %s", reason, exc_info=True)
        return True

    def clear(self, by: str) -> None:
        """Ручное снятие после разбора причины. by - кто снял (в лог)."""
        self.path.unlink(missing_ok=True)
        log.warning("Safe Mode снят вручную: %s (причина была: %s)", by, self._reason)
        self._active, self._reason = False, ""
