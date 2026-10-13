"""Живая проверка Algo API на отдельном TESTNET-аккаунте (Этап 4, О19). Запускает пользователь, руками.

    python -m scripts.test_algo_live --confirm-separate-account [--symbol BTCUSDT] [--with-trigger] [--trigger-timeout 300]
    python -m scripts.test_algo_live --confirm-separate-account --cleanup     # только закрыть позицию и снять ордера

ПЕРЕД ЗАПУСКОМ: в .env подставьте ключи ОТДЕЛЬНОГО чистого testnet-аккаунта (BINANCE_PROFILE=testnet). Ключи ML-бота не
подходят: скрипт открывает и закрывает позицию на выбранной монете. Защита от ошибки: профиль обязан быть testnet (иначе
ConfigError без обхода), есть обязательный флаг --confirm-separate-account, а в начале проверяется, что на счёте НЕТ позиций
и открытых ордеров (обычных и условных) - иначе выход без единого ордера. Состояние барьера и Safe Mode скрипта лежат в
out/algo_live/ и НЕ пересекаются с data/ боевого процесса.

Что делает (сценарий basic, по умолчанию):
  1. сверяет config.LIVE_PRICE_TICK / LIVE_QTY_STEP / LIVE_MIN_QTY / LIVE_MIN_NOTIONAL с exchangeInfo;
  2. подключает приватный поток с событием ALGO_UPDATE и проверяет, приходят ли события;
  3. через LiveBarrier: arm_protection -> MARKET-вход минимального объёма -> pump(): стоп и тейк STOP_MARKET / TAKE_PROFIT_MARKET
     с quantity + reduceOnly (О18); сверяет их с openAlgoOrders; прогоняет reconcile (сверка на настоящих ответах биржи);
  4. имитирует перенос в безубыток: mark price «достигает +1R» (on_mark_price) -> POST нового стопа, затем DELETE старого.
     Уровень безубытка искусственно ставится на 0.5% НИЖЕ рынка, иначе стоп «на входе» сразу бы сработал и проверялся бы
     не перенос, а отказ -2021;
  5. зондирует то, что в документации не описано: можно ли сумму reduceOnly-ордеров держать больше позиции, объём больше
     позиции, второй closePosition-стоп (-4130), коды отказов GET/DELETE неизвестного algo-ордера, отказ «сразу сработал»;
  6. закрывает позицию и снимает все ордера (всегда, даже при ошибке), проверяет, что счёт снова чистый.
С --with-trigger добавляет сценарий trigger: вход с SL/TP в ±0.04% от рынка и ожидание до --trigger-timeout секунд, пока
сработает настоящий стоп/тейк: проверяет, что исполнение порождённого ордера опознано как своё (Safe Mode НЕ включился),
позиция списана, парный ордер снят; записывает последовательность событий и clientOrderId порождённого ордера.

Результат: таблица PASS/FAIL/INFO в консоли и полный JSON (в т.ч. все запросы к бирже) в out/algo_live/report-<время>.json.
Скопируйте вывод консоли или приложите JSON. Код выхода 0 - нет FAIL; 1 - есть FAIL или ошибка.
"""

import argparse
import asyncio
import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

import config
from backtester.contracts import ExecutionConfig, OrderIntent, Side
from execution.exchange_filters import SymbolFilters, config_mismatches, fetch_symbol_filters, min_order_qty
from execution.live_barrier import LiveBarrier, fmt_decimal, new_client_algo_id
from execution.reconcile import reconcile
from execution.rest import BinanceAPIError, Credentials, SignedRest
from execution.state import SafeMode, StateStore
from execution.user_stream import AccountUpdate, AlgoUpdate, OrderUpdate, RawEvent, StreamEvent, UserDataStream
from utils import ban_lock
from utils.errors import ConfigError, DataError
from utils.runner import run

log = logging.getLogger("test_algo_live")

OUT_DIR = config.BASE_DIR / "out" / "algo_live"
PROBE_PREFIX = "tbprobe"
PASS, FAIL, INFO, WARN, SKIP = "PASS", "FAIL", "INFO", "WARN", "SKIP"


@dataclass(frozen=True)
class Options:
    symbol: str = "BTCUSDT"
    confirm_separate_account: bool = False
    with_trigger: bool = False
    trigger_timeout_s: float = 300.0
    cleanup_only: bool = False


def parse_args(argv: Sequence[str] | None = None) -> Options:
    p = argparse.ArgumentParser(description="Живая проверка Algo API на отдельном testnet-аккаунте")
    p.add_argument("--symbol", default="BTCUSDT", choices=sorted(config.LIVE_PRICE_TICK))
    p.add_argument("--confirm-separate-account", action="store_true",
                   help="подтверждение: в .env ключи ОТДЕЛЬНОГО testnet-аккаунта, не аккаунта ML-бота")
    p.add_argument("--with-trigger", action="store_true", help="ещё и дождаться срабатывания настоящего стопа/тейка")
    p.add_argument("--trigger-timeout", type=float, default=300.0, help="сколько секунд ждать срабатывания (с --with-trigger)")
    p.add_argument("--cleanup", action="store_true", help="только закрыть позицию и снять ордера, ничего не открывая")
    a = p.parse_args(argv)
    return Options(a.symbol, a.confirm_separate_account, a.with_trigger, a.trigger_timeout, a.cleanup)


# ------------------------------------------------------------------------------------------------ отчёт


@dataclass
class Report:
    checks: list[dict[str, str]] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    wire: list[dict[str, Any]] = field(default_factory=list)
    safe_mode: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def add(self, name: str, status: str, detail: str) -> None:
        self.checks.append({"name": name, "status": status, "detail": detail})
        log.info("[%s] %s: %s", status, name, detail)

    @property
    def failed(self) -> bool:
        return self.error is not None or any(c["status"] == FAIL for c in self.checks)

    def table(self) -> str:
        width = max((len(c["name"]) for c in self.checks), default=10)
        lines = [f"{c['status']:<5} {c['name']:<{width}}  {c['detail']}" for c in self.checks]
        if self.facts:
            lines += ["", "ФАКТЫ ДЛЯ CLAUDE.md:"] + [f"  {k}: {json.dumps(v, ensure_ascii=False, default=str)}" for k, v in self.facts.items()]
        if self.error:
            lines += ["", f"ОШИБКА СКРИПТА: {self.error}"]
        return "\n".join(lines)

    def to_json(self) -> str:
        return json.dumps({"checks": self.checks, "facts": self.facts, "events": self.events, "wire": self.wire,
                           "safe_mode": self.safe_mode, "error": self.error}, ensure_ascii=False, indent=1, default=str)


class RecordingRest(SignedRest):
    """SignedRest, записывающий каждый запрос и итог (для отчёта: что именно ушло на биржу и что ответила)."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.wire: list[dict[str, Any]] = []  # run_live_check подменяет его журналом отчёта

    async def request(self, method, path, params=None, *, signed=True, trace=None):  # type: ignore[override]
        entry: dict[str, Any] = {"t": self.now_ms(), "method": method, "path": path, "params": dict(params or {})}
        self.wire.append(entry)
        try:
            data = await super().request(method, path, params, signed=signed, trace=trace)
        except BinanceAPIError as exc:
            entry.update(ok=False, code=exc.code, msg=exc.msg)
            raise
        except BaseException as exc:  # записываем и пробрасываем дальше: журнал ничего не подавляет
            entry.update(ok=False, error=f"{type(exc).__name__}: {exc}"[:300])
            raise
        entry.update(ok=True, resp=json.dumps(data, ensure_ascii=False)[:600])
        return data


class _Alerts:
    def __init__(self, report: Report):
        self._report = report

    def critical(self, title: str, details: Any) -> None:
        self._report.safe_mode.append({"title": title, "details": dict(details)})
        log.critical("SAFE MODE: %s %s", title, dict(details))


# ----------------------------------------------------------------------------------- биржевые помощники


def _f(x: Any) -> float:
    return float(x)


async def open_positions(rest: SignedRest) -> dict[str, float]:
    rows = await rest.request("GET", config.POSITION_RISK_PATH)
    if not isinstance(rows, list):
        raise DataError(f"positionRisk: ожидается список, получено {str(rows)[:200]}")
    return {r["symbol"]: _f(r["positionAmt"]) for r in rows if abs(_f(r["positionAmt"])) > 0}


async def open_algo(rest: SignedRest, symbol: str) -> list[dict]:
    rows = await rest.request("GET", config.OPEN_ALGO_ORDERS_PATH, {"symbol": symbol})
    if not isinstance(rows, list):
        raise DataError(f"openAlgoOrders: ожидается список, получено {str(rows)[:200]}")
    return rows


async def account_problems(rest: SignedRest, symbols: Sequence[str]) -> list[str]:
    """Что на счёте мешает чистой проверке (пусто - счёт чистый)."""
    problems = [f"позиция {s} = {q}" for s, q in (await open_positions(rest)).items()]
    for s in symbols:
        orders = await rest.request("GET", config.OPEN_ORDERS_PATH, {"symbol": s})
        problems += [f"открытый ордер {s}: {o.get('clientOrderId')}" for o in orders]
        problems += [f"открытый algo-ордер {s}: {o.get('clientAlgoId')} ({o.get('orderType')})" for o in await open_algo(rest, s)]
    return problems


async def flatten_everything(rest: SignedRest, symbol: str, report: Report) -> None:
    """Снять все ордера и закрыть позицию НАПРЯМУЮ через REST (не через барьер: уборка не должна зависеть от проверяемого кода)."""
    for a in await open_algo(rest, symbol):
        try:
            await rest.request("DELETE", config.ALGO_ORDER_PATH, {"clientAlgoId": a["clientAlgoId"]})
        except BinanceAPIError as exc:
            report.add("cleanup_algo", WARN, f"не снят {a.get('clientAlgoId')}: {exc.msg} ({exc.code})")
    for o in await rest.request("GET", config.OPEN_ORDERS_PATH, {"symbol": symbol}):
        try:
            await rest.request("DELETE", config.ORDER_PATH, {"symbol": symbol, "origClientOrderId": o["clientOrderId"]})
        except BinanceAPIError as exc:
            report.add("cleanup_order", WARN, f"не снят {o.get('clientOrderId')}: {exc.msg} ({exc.code})")
    qty = (await open_positions(rest)).get(symbol, 0.0)
    if abs(qty) > 0:
        side = "SELL" if qty > 0 else "BUY"
        await rest.request("POST", config.ORDER_PATH, {"symbol": symbol, "side": side, "type": "MARKET", "quantity": fmt_decimal(abs(qty)),
                                                       "reduceOnly": True, "newOrderRespType": "RESULT"})
    # algo-ордеры могли появиться/остаться (срабатывание во время уборки): второй проход
    for a in await open_algo(rest, symbol):
        await rest.request("DELETE", config.ALGO_ORDER_PATH, {"clientAlgoId": a["clientAlgoId"]})


async def mark_price(rest: SignedRest, symbol: str) -> float:
    data = await rest.request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol}, signed=False)
    try:
        return _f(data["markPrice"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DataError(f"premiumIndex: нет markPrice: {str(data)[:200]}") from exc


def _event_summary(event: StreamEvent) -> dict[str, Any]:
    if isinstance(event, AlgoUpdate):
        return {"type": "ALGO_UPDATE", "caid": event.client_algo_id, "status": event.status, "order_type": event.order_type,
                "actual_order_id": event.actual_order_id, "reason": event.reason}
    if isinstance(event, OrderUpdate):
        return {"type": "ORDER_TRADE_UPDATE", "c": event.client_order_id, "status": event.status, "order_id": event.order_id,
                "trade_id": event.trade_id, "qty": event.last_qty, "price": event.last_price, "maker": event.is_maker}
    if isinstance(event, AccountUpdate):
        return {"type": "ACCOUNT_UPDATE", "positions": [(p.symbol, p.amount) for p in event.positions]}
    if isinstance(event, RawEvent):
        return {"type": event.name}
    return {"type": type(event).__name__}


# ------------------------------------------------------------------------------------------- проверки


class Live:
    """Контекст проверки: REST, барьер, поток, отчёт."""

    def __init__(self, rest: SignedRest, session: aiohttp.ClientSession, opts: Options, report: Report, out_dir: Path,
                 ws_private_url: str | None, mark_probe: bool):
        self.rest, self.session, self.opts, self.report = rest, session, opts, report
        self.symbol = opts.symbol
        self.out_dir, self.ws_private_url, self.mark_probe = out_dir, ws_private_url, mark_probe
        self.filters: SymbolFilters | None = None
        self.safe = SafeMode(out_dir / ".safe_mode", _Alerts(report))
        self.store = StateStore(out_dir / "state.json")
        self.state = self.store.load()
        self.barrier = LiveBarrier(
            rest, ExecutionConfig(config.LIVE_EXEC_LATENCY_MS, config.LIVE_MAKER_FEE_RATE, config.LIVE_TAKER_FEE_RATE),
            self.state, self.safe, store=self.store,
        )
        self.connected = asyncio.Event()
        self.stream_task: asyncio.Task | None = None
        self.algo_events: list[AlgoUpdate] = []
        self.order_events: list[OrderUpdate] = []
        self.passive = False  # уборка напрямую через REST: события потока записываются, но барьеру не передаются (он счёл бы их чужими)

    # ----------------------------------------------------------------------------- поток

    def _on_event(self, event: StreamEvent) -> None:
        self.report.events.append({"t": self.rest.now_ms(), **_event_summary(event)})
        if self.passive:
            return
        if isinstance(event, OrderUpdate):
            self.order_events.append(event)
            self.barrier.on_order_update(event)
        elif isinstance(event, AccountUpdate):
            self.barrier.on_account_update(event)
        elif isinstance(event, AlgoUpdate):
            if event.client_algo_id.startswith(PROBE_PREFIX + "-"):
                return  # зондирующие ордера скрипта барьеру не принадлежат: иначе он (верно!) счёл бы их чужими и включил Safe Mode
            self.algo_events.append(event)
            self.barrier.on_algo_update(event)

    async def _on_connected(self, reconnect: bool) -> None:
        await self.barrier.resync_all()
        self.connected.set()

    async def start_stream(self) -> None:
        stream = UserDataStream(self.rest, self.session, self._on_event, on_connected=self._on_connected,
                                events=config.USER_STREAM_EVENTS_WITH_ALGO, ws_base_url=self.ws_private_url)
        self.stream_task = asyncio.create_task(stream.run(), name="algo_live.stream")
        waiter = asyncio.create_task(self.connected.wait())
        done, _ = await asyncio.wait({self.stream_task, waiter}, timeout=30, return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        if self.stream_task in done:
            self.stream_task.result()
            raise DataError("Приватный поток завершился при подключении")
        if not done:
            raise DataError("Приватный поток не подключился за 30 с")
        self.report.add("user_stream", PASS, f"подключён с events={'/'.join(config.USER_STREAM_EVENTS_WITH_ALGO)} (биржа рукопожатие приняла)")

    async def stop_stream(self) -> None:
        if self.stream_task is not None:
            self.stream_task.cancel()
            await asyncio.gather(self.stream_task, return_exceptions=True)

    def check_stream_alive(self) -> None:
        if self.stream_task is not None and self.stream_task.done() and not self.stream_task.cancelled():
            raise self.stream_task.exception() or DataError("поток завершился")

    async def settle(self, seconds: float, until=None) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not (until and until()):
            self.check_stream_alive()
            await asyncio.sleep(0.2)

    # ------------------------------------------------------------------------------- шаги

    async def check_filters(self) -> None:
        filters = await fetch_symbol_filters(self.rest, [self.symbol])
        self.filters = filters[self.symbol]
        problems = config_mismatches(filters)
        self.report.facts["filters"] = {"tick": self.filters.tick_size, "step": self.filters.step_size,
                                        "min_qty": self.filters.min_qty, "min_notional": self.filters.min_notional}
        self.report.add("exchange_filters", FAIL if problems else PASS,
                        "; ".join(problems) if problems else f"config.LIVE_* совпадает с exchangeInfo ({self.symbol})")

    async def probe_mark_stream(self) -> None:
        """О2: существует ли <symbol>@markPrice@1s на выбранной сети (на mainnet проверено, на testnet ❓)."""
        url = f"{config.WS_MARKET_URL}?streams={self.symbol.lower()}@markPrice@1s"
        try:
            async with self.session.ws_connect(url, timeout=10) as ws:
                msg = await ws.receive(timeout=10)
                data = json.loads(msg.data) if msg.type == aiohttp.WSMsgType.TEXT else {}
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError) as exc:
            self.report.add("mark_price_stream", FAIL, f"{url}: {type(exc).__name__}: {exc}")
            return
        payload = data.get("data", data)
        ok = payload.get("e") == "markPriceUpdate" and "p" in payload
        self.report.add("mark_price_stream", PASS if ok else FAIL, f"{url}: {str(data)[:160]}")

    def _algo_matches(self, o: dict, type_: str, qty: float, trigger: float | None = None) -> str | None:
        """None - ордер как ожидается; иначе описание расхождения."""
        diffs = []
        if o.get("orderType") != type_ or o.get("side") != "SELL":
            diffs.append(f"тип/сторона {o.get('orderType')}/{o.get('side')}")
        if abs(_f(o.get("quantity", "nan")) - qty) > 1e-12:
            diffs.append(f"quantity {o.get('quantity')} != {qty}")
        if o.get("reduceOnly") is not True:
            diffs.append(f"reduceOnly={o.get('reduceOnly')!r}")
        if o.get("closePosition") not in (False, None):
            diffs.append(f"closePosition={o.get('closePosition')!r}")
        if trigger is not None and abs(_f(o.get("triggerPrice", "nan")) - trigger) > 1e-9:
            diffs.append(f"triggerPrice {o.get('triggerPrice')} != {trigger}")
        return "; ".join(diffs) or None

    async def entry_with_protection(self, *, sl_pct: float, tp_pct: float, be_r: float | None) -> tuple[float, float]:
        """arm_protection -> MARKET BUY минимального объёма. -> (объём, цена рынка в момент расчёта)."""
        assert self.filters is not None
        mark = await mark_price(self.rest, self.symbol)
        qty = min_order_qty(self.filters, mark)
        self.barrier.arm_protection(
            self.symbol, direction=1, entry_price=mark, sl_price=mark * (1 - sl_pct), tp_price=mark * (1 + tp_pct), be_r=be_r,
            ticket_id=self.barrier.next_ticket_id,
        )
        await self.barrier.submit(OrderIntent(self.symbol, Side.BUY, qty, self.rest.now_ms()))
        pos = self.barrier.position(self.symbol)
        self.report.add("entry_fill", PASS if abs(pos - qty) < 1e-9 else FAIL, f"MARKET BUY {qty} {self.symbol} @ ~{mark}: позиция в барьере {pos}")
        return qty, mark

    async def scenario_basic(self) -> None:
        sym = self.symbol
        qty, mark = await self.entry_with_protection(sl_pct=0.015, tp_pct=0.03, be_r=1.0)
        if abs(self.barrier.position(sym) - qty) > 1e-9:
            return
        await self.barrier.pump()
        prot = self.barrier.protection(sym)
        if self.safe.active or prot is None or abs(self.barrier.position(sym)) < 1e-9:
            self.report.add("protection_on_exchange", FAIL, f"защита не поставлена, позиция закрыта барьером: Safe Mode: {self.safe.reason or 'нет'} "
                            f"(см. wire в JSON: какой именно запрос отклонён и с каким кодом)")
            return
        orders = await open_algo(self.rest, sym)
        by_type = {o.get("orderType"): o for o in orders}
        problems = []
        for type_, trigger in (("STOP_MARKET", prot.sl_price), ("TAKE_PROFIT_MARKET", prot.tp_price)):
            o = by_type.get(type_)
            problems.append(f"{type_} нет на бирже" if o is None else self._algo_matches(o, type_, qty, trigger))
        problems = [p for p in problems if p]
        self.report.facts["open_algo_order_sample"] = orders[:2]
        self.report.add("protection_on_exchange", FAIL if problems or len(orders) != 2 else PASS,
                        "; ".join(problems) or f"SL и TP на бирже: STOP_MARKET {prot.sl_price} и TAKE_PROFIT_MARKET {prot.tp_price}, quantity={qty}, reduceOnly=true")
        await self.settle(5, until=lambda: sum(1 for e in self.algo_events if e.status == "NEW") >= 2)
        new_events = [e for e in self.algo_events if e.status == "NEW"]
        self.report.add("algo_update_events", PASS if len(new_events) >= 2 else FAIL,
                        f"получено ALGO_UPDATE(NEW): {len(new_events)} из 2" + ("" if new_events else
                        " - подписка events=ALGO_UPDATE не принята или событие приходит иначе (см. events в JSON)"))
        await self.reconcile_check("reconcile_with_protection")
        await self.probes(qty, mark)
        await self.breakeven(mark)

    async def reconcile_check(self, name: str) -> None:
        report = await reconcile(self.rest, self.state, self.safe, store=None, symbols=[self.symbol], adopt_when_clean=False, check_algo=True)
        kinds = sorted({d.kind.value for d in report.discrepancies})
        self.report.add(name, PASS if report.clean else FAIL,
                        "сверка с биржей чистая (positionRisk, openOrders, openAlgoOrders разобраны)" if report.clean
                        else f"расхождения {kinds}: {[d.to_dict() for d in report.discrepancies][:3]}")
        if not report.clean:
            self.safe.clear("test_algo_live: сверка показала расхождение, проверка продолжается для сбора фактов")

    async def breakeven(self, mark: float) -> None:
        sym = self.symbol
        prot = self.barrier.protection(sym)
        assert prot is not None
        # ИМИТАЦИЯ: «вход» сдвигается на 0.5% ниже рынка, чтобы стоп на входе не сработал сразу (см. докстринг модуля)
        prot.entry_price = self.barrier.round_price(sym, mark * 0.995)
        trigger = prot.be_trigger()
        assert trigger is not None
        start = len(self.report.wire)
        self.barrier.on_mark_price(sym, trigger)  # mark «достигла +1R»
        await self.barrier.pump()
        calls = [(w["method"], w["params"].get("type"), w["params"].get("clientAlgoId"), w.get("ok")) for w in self.report.wire[start:]
                 if w["path"] == config.ALGO_ORDER_PATH and w["method"] in ("POST", "DELETE")]
        order = [m for m, *_ in calls if m in ("POST", "DELETE")]
        post_first = bool(order) and order[0] == "POST" and "DELETE" in order and calls[0][3] is not False
        self.report.add("breakeven_post_then_delete", PASS if post_first and prot.be_done and not prot.be_failed else FAIL,
                        f"последовательность {order}; be_done={prot.be_done}, be_failed={prot.be_failed}"
                        + ("" if post_first else f"; запросы: {calls}"))
        orders = await open_algo(self.rest, sym)
        qty = abs(self.barrier.position(sym))
        stops = [o for o in orders if o.get("orderType") == "STOP_MARKET"]
        ok = (len(stops) == 1 and self._algo_matches(stops[0], "STOP_MARKET", qty, prot.entry_price) is None
              and any(o.get("orderType") == "TAKE_PROFIT_MARKET" for o in orders) and not self.safe.active)
        self.report.add("breakeven_final_state", PASS if ok else FAIL,
                        f"на бирже стопов {len(stops)} (ожидается 1 на {prot.entry_price}), тейк "
                        f"{'есть' if any(o.get('orderType') == 'TAKE_PROFIT_MARKET' for o in orders) else 'ПРОПАЛ'}, Safe Mode {'ВКЛЮЧЁН' if self.safe.active else 'нет'}")
        refused = [w for w in self.report.wire[start:] if w["path"] == config.ALGO_ORDER_PATH and w["method"] == "POST" and w.get("ok") is False]
        if refused:
            self.report.facts["breakeven_post_refused"] = {"code": refused[0].get("code"), "msg": refused[0].get("msg")}
        self.report.facts["breakeven_wire"] = calls

    async def _probe_post(self, label: str, params: dict[str, Any]) -> str | None:
        """Один зондирующий POST algoOrder. -> clientAlgoId, если биржа приняла; факт (код/текст или «принят») идёт в отчёт."""
        caid = f"{PROBE_PREFIX}-{new_client_algo_id()[len(config.CLIENT_ORDER_ID_PREFIX) + 2:]}"[:36]  # уникальный, формат Binance, префикс не наш
        full = {"algoType": "CONDITIONAL", "symbol": self.symbol, "workingType": config.ALGO_WORKING_TYPE, "clientAlgoId": caid, **params}
        try:
            await self.rest.request("POST", config.ALGO_ORDER_PATH, full)
        except BinanceAPIError as exc:
            self.report.facts[label] = {"accepted": False, "code": exc.code, "msg": exc.msg}
            self.report.add(label, INFO, f"биржа отказала: код {exc.code}, «{exc.msg}»")
            return None
        self.report.facts[label] = {"accepted": True}
        self.report.add(label, INFO, "биржа приняла")
        return caid

    async def _drop(self, caids: Sequence[str | None]) -> None:
        for caid in caids:
            if caid:
                await self.rest.request("DELETE", config.ALGO_ORDER_PATH, {"clientAlgoId": caid})

    async def probes(self, qty: float, mark: float) -> None:
        """Факты, которых нет в документации (CLAUDE.md, разд. 4, О19). Только INFO: результат решает не скрипт, а человек."""
        assert self.filters is not None
        sym, f = self.symbol, self.filters
        rp = lambda x: fmt_decimal(self.barrier.round_price(sym, x))  # noqa: E731  (цена на шаге PRICE_FILTER)
        stop = {"side": "SELL", "type": "STOP_MARKET"}
        a = await self._probe_post("probe_reduce_only_sum_over_position", {**stop, "triggerPrice": rp(mark * 0.90), "quantity": fmt_decimal(qty), "reduceOnly": True})
        b = await self._probe_post("probe_reduce_only_qty_above_position", {**stop, "triggerPrice": rp(mark * 0.89), "quantity": fmt_decimal(qty * 2), "reduceOnly": True})
        await self._drop([a, b])
        c = await self._probe_post("probe_close_position_stop", {**stop, "triggerPrice": rp(mark * 0.88), "closePosition": True})
        d = await self._probe_post("probe_second_close_position_stop", {**stop, "triggerPrice": rp(mark * 0.87), "closePosition": True}) if c else None
        await self._drop([c, d])
        e = await self._probe_post("probe_stop_already_beyond_price", {**stop, "triggerPrice": rp(mark * 1.02), "quantity": fmt_decimal(qty), "reduceOnly": True})
        await self._drop([e])
        ghost = f"{PROBE_PREFIX}-ghost-{int(time.time())}"
        for label, method in (("probe_get_unknown_algo_order", "GET"), ("probe_delete_unknown_algo_order", "DELETE")):
            try:
                await self.rest.request(method, config.ALGO_ORDER_PATH, {"clientAlgoId": ghost})
                self.report.facts[label] = {"accepted": True}
                self.report.add(label, INFO, "биржа ответила успехом на неизвестный id (неожиданно)")
            except BinanceAPIError as exc:
                self.report.facts[label] = {"code": exc.code, "msg": exc.msg}
                self.report.add(label, INFO, f"код {exc.code}, «{exc.msg}»")
        if self.mark_probe:
            await self.probe_mark_stream()

    async def scenario_trigger(self) -> None:
        sym = self.symbol
        qty, _ = await self.entry_with_protection(sl_pct=0.0004, tp_pct=0.0004, be_r=None)
        if abs(self.barrier.position(sym) - qty) > 1e-9:
            return
        await self.barrier.pump()
        self.report.add("trigger_armed", INFO, f"SL/TP в ±0.04% от рынка выставлены, жду срабатывания до {self.opts.trigger_timeout_s:.0f} с")
        deadline = time.monotonic() + self.opts.trigger_timeout_s
        while time.monotonic() < deadline and abs(self.barrier.position(sym)) > 1e-9:
            self.check_stream_alive()
            await self.barrier.pump()
            await asyncio.sleep(0.5)
        if abs(self.barrier.position(sym)) > 1e-9:
            self.report.add("trigger_fired", WARN, "стоп/тейк не сработали за отведённое время: сценарий неубедителен (повторите или увеличьте --trigger-timeout)")
            return
        for _ in range(6):  # парный ордер снимается pump'ом
            await self.barrier.pump()
            await asyncio.sleep(0.5)
        self.report.add("trigger_fired", PASS, "позиция списана исполнением порождённого algo-ордера")
        self.report.add("trigger_not_foreign", FAIL if self.safe.active else PASS,
                        f"Safe Mode {'ВКЛЮЧЁН: ' + self.safe.reason if self.safe.active else 'не включился'} (исполнение опознано как своё)")
        left = await open_algo(self.rest, sym)
        self.report.add("trigger_sibling_cleaned", FAIL if left else PASS, f"на бирже algo-ордеров после срабатывания: {len(left)}")
        self.report.add("trigger_protection_removed", FAIL if self.barrier.protection(sym) is not None else PASS,
                        f"защита в локальном состоянии: {'ОСТАЛАСЬ' if self.barrier.protection(sym) else 'снята'}")
        triggered = [e for e in self.order_events if e.has_trade and e.side is Side.SELL]  # вход был BUY, значит SELL - порождённый ордер
        self.report.facts["trigger_event_sequence"] = [(e["type"], e.get("status"), e.get("order_type")) for e in self.report.events
                                                       if e["type"] in ("ALGO_UPDATE", "ORDER_TRADE_UPDATE")][-12:]
        self.report.facts["spawned_order_client_id"] = [e.client_order_id for e in triggered][:3]
        self.report.facts["exit_role"] = self.barrier.pop_exit_role(sym)

    async def close_via_barrier(self) -> None:
        """Закрыть позицию reduce-only ордером барьера и дождаться, пока pump снимет защиту и остатки algo-ордеров."""
        sym = self.symbol
        if abs(self.barrier.position(sym)) > 1e-9:
            await self.barrier.close_position(sym, self.rest.now_ms())
        for _ in range(12):
            await self.barrier.pump()
            if self.barrier.protection(sym) is None and not await open_algo(self.rest, sym):
                return
            await asyncio.sleep(0.3)
        self.report.add("close_via_barrier", WARN, "защита не снялась за 12 проходов pump(): остатки уберёт прямая уборка")

    async def final_cleanup(self) -> None:
        sym = self.symbol
        if not self.safe.active:
            try:
                await self.close_via_barrier()  # штатный путь; ниже - страховка напрямую через REST
            except Exception as exc:  # уборка не должна оборвать отчёт: причина попадёт в него, страховка отработает
                self.report.add("close_via_barrier", WARN, f"{type(exc).__name__}: {exc}")
        self.passive = True
        await flatten_everything(self.rest, sym, self.report)
        await asyncio.sleep(1.0)
        problems = await account_problems(self.rest, [sym])
        self.report.add("final_account_clean", FAIL if problems else PASS, "; ".join(problems) or "позиций и ордеров на счёте нет")
        # reduceOnly без позиции: код отказа (факт для защиты, пришедшей после закрытия позиции)
        if not problems:
            price = await mark_price(self.rest, sym)
            if self.filters is not None:
                caid = await self._probe_post("probe_reduce_only_without_position", {
                    "side": "SELL", "type": "STOP_MARKET", "triggerPrice": fmt_decimal(self.barrier.round_price(sym, price * 0.9)),
                    "quantity": fmt_decimal(self.filters.min_qty), "reduceOnly": True})
                await self._drop([caid])


def reset_workdir(out_dir: Path) -> None:
    """Состояние и защёлка прошлого запуска не должны влиять на этот (вызывается ПОСЛЕ проверки, что счёт чист)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in ("state.json", ".safe_mode"):
        (out_dir / name).unlink(missing_ok=True)


async def run_live_check(opts: Options, rest: SignedRest, session: aiohttp.ClientSession, out_dir: Path, report: Report, *,
                         ws_private_url: str | None = None, mark_probe: bool = True) -> Report:
    """Весь сценарий. report создаёт вызывающий: при исключении его уже накопленная часть всё равно попадёт в файл."""
    if isinstance(rest, RecordingRest):
        rest.wire = report.wire
    problems = await account_problems(rest, [opts.symbol])
    if opts.cleanup_only:
        await flatten_everything(rest, opts.symbol, report)
        left = await account_problems(rest, [opts.symbol])
        report.add("cleanup", FAIL if left else PASS, "; ".join(left) or "счёт чист")
        return report
    if problems:
        raise ConfigError("Счёт не чистый: " + "; ".join(problems) + ". Это не отдельный testnet-аккаунт? Если остатки от прошлого запуска - "
                          "запустите с --cleanup. Ни одного ордера не отправлено.")
    report.add("account_clean", PASS, "позиций и открытых ордеров (обычных и условных) нет")
    reset_workdir(out_dir)
    live = Live(rest, session, opts, report, out_dir, ws_private_url, mark_probe)
    try:
        await live.check_filters()
        await live.start_stream()
        await live.scenario_basic()
        if opts.with_trigger:
            await live.close_via_barrier()  # позиция первого сценария закрывается ТАК ЖЕ, как закрыл бы бот: своим ордером
            await live.scenario_trigger()
    except BaseException as exc:
        report.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            await live.final_cleanup()  # всегда: счёт не должен остаться с позицией
        finally:
            await live.stop_stream()
    return report


# ------------------------------------------------------------------------------------------ точка входа


def guard_testnet() -> None:
    if not config.IS_TESTNET or "testnet" not in config.REST_BASE_URL:
        raise ConfigError(f"Скрипт открывает позиции и работает ТОЛЬКО на testnet (профиль {config.BINANCE_PROFILE}, "
                          f"REST {config.REST_BASE_URL}). Обхода нет.")


async def main_async(opts: Options) -> int:
    config.validate()
    guard_testnet()
    if not opts.confirm_separate_account:
        raise ConfigError("Добавьте --confirm-separate-account: подтверждение, что в .env ключи ОТДЕЛЬНОГО testnet-аккаунта, а не аккаунта ML-бота")
    ban_lock.ensure_not_banned()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rest = RecordingRest(session, config.REST_BASE_URL, Credentials.from_env())
        await rest.sync_time()
        report = Report()
        try:
            await run_live_check(opts, rest, session, OUT_DIR, report)
        finally:  # отчёт пишется и при ошибке: в нём то, что успело выполниться
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            path = OUT_DIR / f"report-{stamp}.json"
            path.write_text(report.to_json(), encoding="utf-8")
            print("\n" + report.table())
            print(f"\nПолный отчёт (все запросы к бирже и события потока): {path}")
    return 1 if report.failed else 0


async def main() -> None:
    code = await main_async(parse_args())
    if code:
        raise SystemExit(code)


if __name__ == "__main__":
    run(main, "test_algo_live")
