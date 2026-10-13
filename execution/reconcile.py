"""Сверка состояния при старте (State Reconciliation): биржа - источник истины, локальное состояние - гипотеза.

После любого перезапуска бот НЕ верит своему файлу состояния. Он читает с биржи
  * GET positionRisk  - реальные позиции (путь - config.POSITION_RISK_PATH, v3: v1/v2 выводятся из обращения);
  * GET openOrders    - живые ордера;
  * GET order по clientOrderId - судьбу каждого своего ордера, которого среди живых уже нет,
и сравнивает с LocalState. Любое существенное расхождение = Safe Mode + критический алерт: упавший бот
мог пропустить закрытие по стопу, ликвидацию, исполнение лимитки, чужой ордер - торговать, не зная
реальной позиции, нельзя.

Что считается расхождением (критическим):
  POSITION_MISMATCH         позиции различаются больше tol (в т.ч. «на бирже висит, у нас нет» и
                            «у нас есть, на бирже закрыта по стопу»);
  UNKNOWN_EXCHANGE_ORDER    на бирже живой ордер, которого нет в локальном состоянии;
  ORDER_MISMATCH            ордер есть и там и там, но параметры или исполненный объём различаются;
  ORDER_RESOLVED_OFFLINE    свой ордер исполнился (хотя бы частично) или пропал, пока бота не было;
  UNSUPPORTED_POSITION_MODE позиция в режиме Hedge (LONG/SHORT): бот ведёт только one-way (BOTH);
  UNVERIFIED                сверку выполнить не удалось (сеть, 5xx, 4xx, непонятный ответ): fail-closed.
Безобидное (не расхождение, а заметка): свой ордер без исполнений был отменён / истёк / отклонён пока бот
стоял, либо PENDING-ордер вообще не дошёл до биржи (процесс умер между записью и POST). Такие ордера
при чистой сверке удаляются из локального состояния.

429/418 и действующий бан (RateLimitError, BanActive) НЕ превращаются в Safe Mode: они выходят наружу,
чтобы общий runner записал .ban_lock. Это не расхождение состояния, а ограничение доступа.

Safe Mode ставит защёлку на диск и не снимается чистой сверкой: снимает только оператор
(SafeMode.clear) после разбора причины. Чистая сверка при включённой защёлке состояние всё равно
принимает - оно подтверждено биржей.

Рекомендация: бот должен работать на отдельном (суб)аккаунте. Позиции и ордера любых других символов
и ручные сделки на том же аккаунте сверка честно считает расхождением.
"""

import logging
from collections.abc import Collection, Mapping
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

import config
from backtester.contracts import OrderType, Side
from execution.rest import ERR_CANCEL_UNKNOWN, ERR_ORDER_NOT_FOUND, BinanceAPIError, SignedRest
from execution.state import LocalAlgoOrder, LocalOrder, LocalState, SafeMode, StateStore
from utils.errors import BanActive, DataError, RateLimitError, TradingInfraError

log = logging.getLogger("reconcile")

_TERMINAL = frozenset({"FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"})


class DiscrepancyKind(str, Enum):
    POSITION_MISMATCH = "position_mismatch"
    UNKNOWN_EXCHANGE_ORDER = "unknown_exchange_order"
    ORDER_MISMATCH = "order_mismatch"
    ORDER_RESOLVED_OFFLINE = "order_resolved_offline"
    UNSUPPORTED_POSITION_MODE = "unsupported_position_mode"
    UNVERIFIED = "unverified"
    UNKNOWN_EXCHANGE_ALGO = "unknown_exchange_algo"  # Этап 4: живой algo-ордер, которого нет у бота
    ALGO_MISMATCH = "algo_mismatch"
    ALGO_RESOLVED_OFFLINE = "algo_resolved_offline"
    POSITION_UNPROTECTED = "position_unprotected"  # R6: позиция без стопа на бирже


@dataclass(frozen=True)
class Discrepancy:
    kind: DiscrepancyKind
    symbol: str | None
    local: Any
    exchange: Any
    detail: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["kind"] = self.kind.value
        return d


@dataclass(frozen=True)
class ExchangeOrder:
    client_order_id: str
    symbol: str
    side: Side
    order_type: str
    qty: float
    price: float
    executed_qty: float
    status: str


@dataclass
class ReconcileReport:
    verified: bool  # данные биржи получены и разобраны
    discrepancies: tuple[Discrepancy, ...] = ()
    notes: tuple[str, ...] = ()
    exchange_positions: dict[str, float] = field(default_factory=dict)
    safe_mode_entered: bool = False  # именно эта сверка включила Safe Mode (False и если он уже стоял)

    @property
    def clean(self) -> bool:
        return self.verified and not self.discrepancies


# ----------------------------------------------------------------- данные биржи


def _float(src: Mapping, key: str, ctx: str) -> float:
    try:
        return float(src[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise DataError(f"{ctx}: нет числового поля {key!r}: {str(src)[:200]}") from exc


def parse_positions(raw: Any) -> tuple[dict[str, float], list[tuple[str, str, float]]]:
    """-> (one-way позиции {symbol: qty} только ненулевые, [(symbol, positionSide, qty)] позиции hedge-режима)."""
    if not isinstance(raw, list):
        raise DataError(f"positionRisk: ожидается список, получено {str(raw)[:200]}")
    oneway: dict[str, float] = {}
    hedge: list[tuple[str, str, float]] = []
    for p in raw:
        if not isinstance(p, dict) or not isinstance(p.get("symbol"), str):
            raise DataError(f"positionRisk: неожиданная запись {str(p)[:200]}")
        qty = _float(p, "positionAmt", "positionRisk")
        side = str(p.get("positionSide", "BOTH"))
        if side != "BOTH":
            hedge.append((p["symbol"], side, qty))
        elif qty != 0.0:
            oneway[p["symbol"]] = qty
    return oneway, hedge


def parse_orders(raw: Any) -> dict[str, ExchangeOrder]:
    if not isinstance(raw, list):
        raise DataError(f"openOrders: ожидается список, получено {str(raw)[:200]}")
    out: dict[str, ExchangeOrder] = {}
    for o in raw:
        try:
            order = ExchangeOrder(
                client_order_id=str(o["clientOrderId"]), symbol=str(o["symbol"]), side=Side(str(o["side"]).lower()),
                order_type=str(o["type"]), qty=_float(o, "origQty", "openOrders"), price=_float(o, "price", "openOrders"),
                executed_qty=_float(o, "executedQty", "openOrders"), status=str(o["status"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"openOrders: неожиданная запись {str(o)[:200]}") from exc
        out[order.client_order_id] = order
    return out


async def _fetch(rest: SignedRest, symbols: Collection[str] | None):
    positions_raw = await rest.request("GET", config.POSITION_RISK_PATH)
    if symbols is None:
        orders_raw = await rest.request("GET", config.OPEN_ORDERS_PATH)
    else:  # по символу вес 1 против 40 без символа
        orders_raw = []
        for symbol in sorted(symbols):
            part = await rest.request("GET", config.OPEN_ORDERS_PATH, {"symbol": symbol})
            if not isinstance(part, list):
                raise DataError(f"openOrders {symbol}: ожидается список, получено {str(part)[:200]}")
            orders_raw.extend(part)
    positions, hedge = parse_positions(positions_raw)
    if symbols is not None:
        positions = {s: q for s, q in positions.items() if s in symbols}
        hedge = [h for h in hedge if h[0] in symbols]
    return positions, hedge, parse_orders(orders_raw)


async def _resolve_missing(rest: SignedRest, order: LocalOrder) -> dict | None:
    """Судьба своего ордера, которого нет среди живых: снимок с биржи или None (биржа его не знает)."""
    try:
        data = await rest.request(
            "GET", config.ORDER_PATH, {"symbol": order.symbol, "origClientOrderId": order.client_order_id}
        )
    except BinanceAPIError as exc:
        if exc.code == ERR_ORDER_NOT_FOUND:
            return None
        raise
    if not isinstance(data, dict) or data.get("clientOrderId") != order.client_order_id:
        raise DataError(f"Неожиданный ответ по ордеру {order.client_order_id}: {str(data)[:200]}")
    return data


# ------------------------------------------------------------------- algo-ордера (Этап 4)

STOP_TYPES = frozenset({"STOP_MARKET", "STOP"})


@dataclass(frozen=True)
class ExchangeAlgoOrder:
    client_algo_id: str
    symbol: str
    side: Side
    order_type: str
    trigger_price: float
    status: str
    close_position: bool
    qty: float = 0.0  # quantity: объём защитного ордера (О18: quantity + reduceOnly); при closePosition биржа может отдавать 0
    reduce_only: bool = False


def parse_algo_orders(raw: Any) -> dict[str, ExchangeAlgoOrder]:
    if not isinstance(raw, list):
        raise DataError(f"openAlgoOrders: ожидается список, получено {str(raw)[:200]}")
    out: dict[str, ExchangeAlgoOrder] = {}
    for o in raw:
        try:
            a = ExchangeAlgoOrder(
                client_algo_id=str(o["clientAlgoId"]), symbol=str(o["symbol"]), side=Side(str(o["side"]).lower()),
                order_type=str(o["orderType"]), trigger_price=_float(o, "triggerPrice", "openAlgoOrders"),
                status=str(o.get("algoStatus", "NEW")), close_position=bool(o.get("closePosition", False)),
                qty=_float(o, "quantity", "openAlgoOrders") if o.get("quantity") not in (None, "") else 0.0,
                reduce_only=bool(o.get("reduceOnly", False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"openAlgoOrders: неожиданная запись {str(o)[:200]}") from exc
        out[a.client_algo_id] = a
    return out


async def _fetch_algo(rest: SignedRest, symbols: Collection[str] | None) -> dict[str, ExchangeAlgoOrder]:
    raw: list = []
    for symbol in sorted(symbols) if symbols is not None else [None]:
        part = await rest.request("GET", config.OPEN_ALGO_ORDERS_PATH, {"symbol": symbol} if symbol else None)
        if not isinstance(part, list):
            raise DataError(f"openAlgoOrders {symbol}: ожидается список, получено {str(part)[:200]}")
        raw.extend(part)
    return parse_algo_orders(raw)


async def _resolve_missing_algo(rest: SignedRest, algo: LocalAlgoOrder) -> dict | None:
    """Судьба своего algo-ордера, которого нет среди живых: снимок или None (биржа его не знает)."""
    try:
        data = await rest.request("GET", config.ALGO_ORDER_PATH, {"clientAlgoId": algo.client_algo_id})
    except BinanceAPIError as exc:
        if exc.code in (ERR_ORDER_NOT_FOUND, ERR_CANCEL_UNKNOWN):
            return None
        raise
    if not isinstance(data, dict) or data.get("clientAlgoId") != algo.client_algo_id:
        raise DataError(f"Неожиданный ответ по algo-ордеру {algo.client_algo_id}: {str(data)[:200]}")
    return data


def compare_algo(
    local: LocalState,
    positions: Mapping[str, float],
    exchange: Mapping[str, ExchangeAlgoOrder],
    resolved: Mapping[str, dict | None],
    *,
    tol: float,
) -> tuple[list[Discrepancy], list[str], list[str]]:
    """Чистое сравнение algo-ордеров. -> (расхождения, заметки, clientAlgoId безобидных к удалению)."""
    found: list[Discrepancy] = []
    notes: list[str] = []
    droppable: list[str] = []
    K = DiscrepancyKind

    for caid, ex in exchange.items():
        mine = local.algo_orders.get(caid)
        if mine is None:
            found.append(Discrepancy(K.UNKNOWN_EXCHANGE_ALGO, ex.symbol, None, asdict(ex) | {"side": ex.side.value},
                                     "живой algo-ордер на бирже, которого нет в локальном состоянии"))
            continue
        diffs = []
        if (mine.symbol, mine.side, mine.order_type) != (ex.symbol, ex.side, ex.order_type):
            diffs.append("символ/сторона/тип")
        if abs(mine.trigger_price - ex.trigger_price) > 1e-9:
            diffs.append(f"триггер {mine.trigger_price} != {ex.trigger_price}")
        if mine.close_position != ex.close_position:
            diffs.append(f"closePosition {mine.close_position} != {ex.close_position}")
        if not mine.close_position and (abs(mine.qty - ex.qty) > tol or not ex.reduce_only):
            diffs.append(f"объём/reduceOnly: у бота {mine.qty} reduceOnly, на бирже {ex.qty} reduceOnly={ex.reduce_only}")
        if diffs:
            found.append(Discrepancy(K.ALGO_MISMATCH, ex.symbol, mine.to_dict(), asdict(ex) | {"side": ex.side.value}, "; ".join(diffs)))

    for caid, mine in local.algo_orders.items():
        if caid in exchange:
            continue
        snap = resolved.get(caid)
        if snap is None:
            if mine.status == "PENDING":
                notes.append(f"{caid}: PENDING algo-ордер не дошёл до биржи (процесс умер до отправки) - удаляю")
                droppable.append(caid)
            else:
                found.append(Discrepancy(K.ALGO_RESOLVED_OFFLINE, mine.symbol, mine.to_dict(), None,
                                         "algo-ордер был на бирже и пропал: судьба неизвестна"))
            continue
        status = str(snap.get("algoStatus"))
        if status in ("CANCELED", "EXPIRED"):
            notes.append(f"{caid}: {status}, пока бот стоял - удаляю")
            droppable.append(caid)
        else:  # FINISHED / TRIGGERING / TRIGGERED / REJECTED: стоп сработал или отклонён, пока бота не было
            found.append(Discrepancy(K.ALGO_RESOLVED_OFFLINE, mine.symbol, mine.to_dict(), snap,
                                     f"algo-ордер {status} пока бот стоял: исполнения не учтены (позиции сверяются отдельно)"))

    for symbol, qty in positions.items():  # R6: позиция без стопа на бирже (по объёму: стопы quantity+reduceOnly покрывают сумму)
        stops = [a for a in exchange.values() if a.symbol == symbol and a.order_type in STOP_TYPES and a.status == "NEW"]
        covered = float("inf") if any(a.close_position for a in stops) else sum(a.qty for a in stops if a.reduce_only)
        if abs(qty) > tol and covered < abs(qty) - tol:
            found.append(Discrepancy(K.POSITION_UNPROTECTED, symbol, local.position(symbol), qty,
                                     f"позиция {qty}, а стопы (STOP_MARKET) на бирже покрывают {covered}: pump() защиты может достроить покрытие"))
    return found, notes, droppable


# ---------------------------------------------------------------------- сравнение


def compare_states(
    local: LocalState,
    positions: Mapping[str, float],
    hedge: Collection[tuple[str, str, float]],
    orders: Mapping[str, ExchangeOrder],
    resolved: Mapping[str, dict | None],
    *,
    tol: float,
) -> tuple[list[Discrepancy], list[str], list[str]]:
    """Чистое сравнение (без сети). -> (расхождения, заметки, clientOrderId безобидных ордеров к удалению)."""
    found: list[Discrepancy] = []
    notes: list[str] = []
    droppable: list[str] = []
    K = DiscrepancyKind

    for symbol, side, qty in hedge:
        if abs(qty) > tol:
            found.append(Discrepancy(K.UNSUPPORTED_POSITION_MODE, symbol, local.position(symbol), qty,
                                     f"позиция {side} в режиме Hedge: бот ведёт только one-way (BOTH)"))

    for symbol in sorted(set(positions) | {s for s, q in local.positions.items() if abs(q) > tol}):
        mine, theirs = local.position(symbol), positions.get(symbol, 0.0)
        if abs(mine - theirs) <= tol:
            continue
        if abs(theirs) <= tol:
            why = "на бирже позиции нет, у бота есть: закрыта по стопу / ликвидирована / закрыта вручную?"
        elif abs(mine) <= tol:
            why = "на бирже висит позиция, о которой бот не знает (упал после исполнения?)"
        else:
            why = "объёмы различаются"
        found.append(Discrepancy(K.POSITION_MISMATCH, symbol, mine, theirs, why))

    for cid, ex in orders.items():
        mine = local.orders.get(cid)
        if mine is None:
            found.append(Discrepancy(K.UNKNOWN_EXCHANGE_ORDER, ex.symbol, None, _order_dict(ex),
                                     "живой ордер на бирже, которого нет в локальном состоянии"
                                     + (" (наш префикс: состояние потеряно?)" if cid.startswith(config.CLIENT_ORDER_ID_PREFIX + "-") else "")))
            continue
        diffs = []
        if (mine.symbol, mine.side) != (ex.symbol, ex.side):
            diffs.append("символ/сторона")
        if abs(mine.qty - ex.qty) > tol:
            diffs.append(f"объём {mine.qty} != {ex.qty}")
        if mine.order_type is OrderType.LIMIT and mine.limit_price is not None and abs(mine.limit_price - ex.price) > 1e-12:
            diffs.append(f"цена {mine.limit_price} != {ex.price}")
        if abs(mine.filled_qty - ex.executed_qty) > tol:
            diffs.append(f"исполнено {mine.filled_qty} != {ex.executed_qty}")
        if diffs:
            found.append(Discrepancy(K.ORDER_MISMATCH, ex.symbol, mine.to_dict(), _order_dict(ex), "; ".join(diffs)))

    for cid, mine in local.orders.items():
        if cid in orders:
            continue
        snap = resolved.get(cid)
        if snap is None:
            if mine.status == "PENDING":
                notes.append(f"{cid}: PENDING-ордер не дошёл до биржи (процесс умер до отправки) - удаляю")
                droppable.append(cid)
            else:
                found.append(Discrepancy(K.ORDER_RESOLVED_OFFLINE, mine.symbol, mine.to_dict(), None,
                                         "ордер был на бирже и пропал: судьба неизвестна"))
            continue
        executed, status = _float(snap, "executedQty", "order"), str(snap.get("status"))
        if executed > mine.filled_qty + tol:
            found.append(Discrepancy(K.ORDER_RESOLVED_OFFLINE, mine.symbol, mine.to_dict(), snap,
                                     f"ордер {status}, исполнено {executed} при учтённых {mine.filled_qty}: бот пропустил исполнения"))
        elif status in _TERMINAL:
            notes.append(f"{cid}: {status} без исполнений, пока бот стоял - удаляю")
            droppable.append(cid)
        else:
            found.append(Discrepancy(K.ORDER_MISMATCH, mine.symbol, mine.to_dict(), snap,
                                     f"ордер {status} по запросу, но среди живых его не было (гонка): повторите сверку"))
    return found, notes, droppable


def _order_dict(o: ExchangeOrder) -> dict[str, Any]:
    d = asdict(o)
    d["side"] = o.side.value
    return d


# ------------------------------------------------------------------------ сверка


async def reconcile(
    rest: SignedRest,
    state: LocalState,
    safe_mode: SafeMode,
    *,
    store: StateStore | None = None,
    symbols: Collection[str] | None = None,
    tol: float | None = None,
    adopt_when_clean: bool = True,
    check_algo: bool | None = None,
) -> ReconcileReport:
    """Вызывать при старте, до первого ордера, и после долгого обрыва приватного потока.

    Расхождение или невозможность проверить -> Safe Mode (защёлка на диске + критический алерт),
    состояние НЕ перезаписывается (нужно для разбора). Чисто -> state подгоняется под биржу и сохраняется.
    """
    tol = tol if tol is not None else config.RECONCILE_QTY_TOLERANCE
    # algo-ордера сверяются, только если бот ими пользуется (или попросили явно): ML-контур без условных ордеров не получает
    # лишнего запроса и не зависит от ещё не проверенного на testnet пути openAlgoOrders.
    algo_on = check_algo if check_algo is not None else bool(state.algo_orders or state.protections)
    try:
        positions, hedge, orders = await _fetch(rest, symbols)
        resolved = {
            cid: await _resolve_missing(rest, order) for cid, order in state.orders.items() if cid not in orders
        }
        algo_ex: dict[str, ExchangeAlgoOrder] = {}
        algo_resolved: dict[str, dict | None] = {}
        if algo_on:
            algo_ex = await _fetch_algo(rest, symbols)
            algo_resolved = {
                caid: await _resolve_missing_algo(rest, a) for caid, a in state.algo_orders.items() if caid not in algo_ex
            }
    except (RateLimitError, BanActive):
        raise  # не расхождение: runner запишет .ban_lock
    except TradingInfraError as exc:
        reason = f"Сверка с биржей не выполнена ({type(exc).__name__}): торговать вслепую нельзя"
        entered = safe_mode.trip(reason, {"error": str(exc)[:500]})
        unverified = Discrepancy(DiscrepancyKind.UNVERIFIED, None, None, None, str(exc)[:300])
        return ReconcileReport(verified=False, discrepancies=(unverified,), safe_mode_entered=entered)

    found, notes, droppable = compare_states(state, positions, hedge, orders, resolved, tol=tol)
    algo_drop: list[str] = []
    if algo_on:
        a_found, a_notes, algo_drop = compare_algo(state, positions, algo_ex, algo_resolved, tol=tol)
        found, notes = found + a_found, notes + a_notes
    for note in notes:
        log.info("Сверка: %s", note)

    if found:
        kinds = ", ".join(sorted({d.kind.value for d in found}))
        entered = safe_mode.trip(
            f"Расхождение состояния бота и биржи ({len(found)}): {kinds}",
            {"discrepancies": [d.to_dict() for d in found], "notes": notes},
        )
        return ReconcileReport(True, tuple(found), tuple(notes), dict(positions), entered)

    if adopt_when_clean:
        for cid in droppable:
            state.orders.pop(cid, None)
        for caid in algo_drop:
            state.algo_orders.pop(caid, None)
        state.positions = dict(positions)  # равны с точностью tol: берём точные значения биржи
        if store is not None:
            store.save(state)
    if safe_mode.active:
        log.critical("Сверка чистая, но Safe Mode остаётся включённым (%s): снимите вручную после разбора", safe_mode.reason)
    else:
        log.info("Сверка чистая: позиций %d, живых ордеров %d", len(positions), len(orders))
    return ReconcileReport(True, (), tuple(notes), dict(positions))
