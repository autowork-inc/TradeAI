"""Принудительная тестовая сделка на Binance TESTNET: проверка форматов приватного API боевого клиента (`exchange/binance_live.py`).

    python -m scripts.test_live_order [--symbol 1000PEPEUSDT] [--notional 10] [--sl-pct 2] [--tp-pct 3]

Запускает пользователь, руками. Минует риск-менеджер, Оркестратор и сигналы: открывает сделку на `--notional` USDT (по умолчанию
10 USDT нотионала, но не меньше минимума биржи) по `1000PEPEUSDT`, ставит SL и TP (Algo Service, quantity + reduceOnly), проверяет их
по `GET algoOrder` и `openAlgoOrders`, пробует безубыток (POST нового стопа, затем DELETE старого - ровно как движок) и закрывает
позицию с уборкой всех ордеров.

ОХРАНА (без обхода): `USE_TESTNET=true` в окружении/.env обязателен, иначе ConfigError и выход до сети; адрес API обязан быть
testnet; ключи - BINANCE_API_KEY / BINANCE_API_SECRET ОТДЕЛЬНОГО testnet-аккаунта (ключи mainnet и боевого ML-бота не подходят:
на testnet они не заработают, а на mainnet скрипт не пойдёт). Если по монете уже есть позиция или живые algo-ордера - выход
без единого ордера. Уборка (отмена algo-ордеров, reduce-only закрытие) выполняется всегда, в `finally`.

Что проверяется (PASS/FAIL/INFO в таблице; полный JSON с запросами и ответами - out/live_order_test/report-<время>.json):
  1 время и подпись            6 вход MARKET (RESULT: статус, объём, средняя цена) и позиция в positionRisk (объём, entryPrice)
  2 exchangeInfo фильтры       7 SL и TP одновременно, оба quantity + reduceOnly (гипотеза О18: сумма больше позиции допустима?)
  3 баланс (/fapi/v3/balance)  8 GET algoOrder и openAlgoOrders видят оба ордера; формат ответов разбирается клиентом
  4 чистая монета              9 безубыток: второй стоп при живых SL и TP, затем отмена первого - окна без стопа нет
  5 ISOLATED + плечо          10 закрытие reduce-only, уборка, позиция 0, algo-ордеров нет; /fapi/v1/income читается
Код выхода 0 - нет FAIL; 1 - есть FAIL или ошибка; 3 - нарушена охрана/конфигурация. Пришлите вывод консоли или JSON: по нему
закрываются ❓ в CLAUDE.md разд. 4 (формат balance/income/openAlgoOrders, гипотеза О18, коды отказов).
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

import config
from exchange.binance_live import BinanceLiveClient, OrderValidationError, client_id
from execution.exchange_filters import min_order_qty
from execution.rest import BinanceAPIError
from utils.errors import ConfigError, TradingInfraError
from utils.runner import setup_logging

log = logging.getLogger("test_live_order")

OUT_DIR = config.BASE_DIR / "out" / "live_order_test"
DEFAULT_SYMBOL = "1000PEPEUSDT"
TERMINAL = ("FINISHED", "CANCELED", "EXPIRED", "REJECTED", "TRIGGERED")


@dataclass(frozen=True)
class Options:
    symbol: str
    notional: float
    sl_pct: float
    tp_pct: float
    leverage: int


def parse_args(argv: Sequence[str] | None = None) -> Options:
    p = argparse.ArgumentParser(description="Принудительная тестовая сделка на Binance TESTNET (проверка форматов API)")
    p.add_argument("--symbol", default=DEFAULT_SYMBOL)
    p.add_argument("--notional", type=float, default=10.0, help="нотионал входа в USDT (не меньше минимума биржи)")
    p.add_argument("--sl-pct", type=float, default=2.0, help="стоп ниже цены входа, %%")
    p.add_argument("--tp-pct", type=float, default=3.0, help="тейк выше цены входа, %%")
    p.add_argument("--leverage", type=int, default=10)
    a = p.parse_args(argv)
    if not (a.notional > 0 and 0 < a.sl_pct < 50 and 0 < a.tp_pct < 100):
        raise ConfigError("notional > 0, 0 < sl-pct < 50, 0 < tp-pct < 100")
    return Options(a.symbol, a.notional, a.sl_pct, a.tp_pct, a.leverage)


@dataclass
class Report:
    rows: list[dict[str, Any]] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)

    def add(self, status: str, name: str, detail: str = "") -> None:
        self.rows.append({"status": status, "name": name, "detail": detail})
        log.info("%-5s %-34s %s", status, name, detail)

    def ok(self, name: str, detail: str = "") -> None:
        self.add("PASS", name, detail)

    def fail(self, name: str, detail: str = "") -> None:
        self.add("FAIL", name, detail)

    def info(self, name: str, detail: str = "") -> None:
        self.add("INFO", name, detail)

    def check(self, cond: bool, name: str, detail: str = "") -> bool:
        (self.ok if cond else self.fail)(name, detail)
        return cond

    @property
    def failed(self) -> bool:
        return any(r["status"] == "FAIL" for r in self.rows)

    def table(self) -> str:
        w = max((len(r["name"]) for r in self.rows), default=10)
        return "\n".join(f"{r['status']:<5} {r['name']:<{w}}  {r['detail']}" for r in self.rows)


def guard() -> None:
    """Охрана: только testnet, без обхода."""
    if not config.USE_TESTNET:
        raise ConfigError("Скрипт работает ТОЛЬКО на testnet: задайте USE_TESTNET=true (и ключи отдельного testnet-аккаунта)")
    if "testnet" not in config.LIVE_REST_URL:
        raise ConfigError(f"Адрес API {config.LIVE_REST_URL!r} не testnet: отказ")


async def algo_state(client: BinanceLiveClient, cid: str) -> str:
    try:
        return (await client.get_algo_order(cid)).status
    except BinanceAPIError as exc:
        return f"unknown({exc.code})"


async def cleanup(client: BinanceLiveClient, symbol: str, cids: list[str], report: Report) -> None:
    """Всегда: снять свои algo-ордера, закрыть позицию reduce-only рынком, убедиться, что чисто."""
    for cid in cids:
        try:
            await client.cancel_algo_order(cid)
        except BinanceAPIError as exc:
            state = await algo_state(client, cid)
            if not (state in TERMINAL or state.startswith("unknown")):
                report.fail("cleanup:cancel_algo", f"{cid}: {exc.code} {exc.msg}; статус {state}")
    pos = await client.get_position(symbol)
    if abs(pos.amt) > 0:
        side = "SELL" if pos.amt > 0 else "BUY"
        await client.market_order(symbol, side, client.round_qty(symbol, abs(pos.amt)), reduce_only=True,
                                  client_id=client_id(client.id_prefix, "X", f"live-order-test-{int(time.time() * 1000)}"))
    left = await client.get_open_algo_orders(symbol)
    mine = [a for a in left if a.client_algo_id in cids]
    after = await client.get_position(symbol)
    report.check(abs(after.amt) == 0 and not mine, "cleanup:flat_and_no_algo", f"позиция {after.amt}, живых своих algo-ордеров {len(mine)}")


async def run_check(client: BinanceLiveClient, opts: Options, report: Report) -> None:
    symbol, tag = opts.symbol, f"live-order-test-{int(time.time() * 1000)}"
    cids: list[str] = []
    entered = False  # вход отправлен: только тогда нужна уборка (чужую позицию на монете скрипт не трогает никогда)
    started_ms = int(time.time() * 1000)
    try:
        # 1-3: время/подпись, фильтры, баланс
        offset = await client.rest.sync_time()
        report.ok("time_and_signature", f"смещение часов {offset} мс; подписанные запросы принимаются")
        await client.load_filters([symbol])
        f = client.filters(symbol)
        report.ok("exchange_filters", f"tick {f.tick_size}, step {f.step_size}, minQty {f.min_qty}, minNotional {f.min_notional}")
        report.facts["filters"] = f.__dict__
        balance = await client.get_balance()
        report.ok("balance", f"кошелёк {balance.wallet}, доступно {balance.available}, нереализованный PnL {balance.unrealized_pnl}")
        report.facts["balance_before"] = balance.__dict__
        mark = await client.get_mark_price(symbol)
        report.ok("mark_price", f"{mark}")

        # 4: чистая монета
        pos0, algos0 = await client.get_position(symbol), await client.get_open_algo_orders(symbol)
        if abs(pos0.amt) > 0 or algos0:
            report.fail("clean_symbol", f"по {symbol} уже есть позиция {pos0.amt} или живых algo-ордеров {len(algos0)}: выход без ордеров")
            return
        report.ok("clean_symbol", "позиции и algo-ордеров нет")

        # 5: изолированная маржа и плечо
        await client.set_margin_type(symbol, "ISOLATED")
        report.ok("margin_type_isolated", "ISOLATED установлен (или уже был: -4046)")
        await client.set_leverage(symbol, opts.leverage)
        report.ok("leverage", f"{opts.leverage}x применено биржей")

        # 6: вход
        qty = max(client.round_qty(symbol, opts.notional / mark), min_order_qty(f, mark))  # ≈ notional, но не меньше минимума биржи
        entered = True
        entry = await client.market_order(symbol, "BUY", qty, client_id=client_id(client.id_prefix, "E", tag), ref_price=mark)
        report.facts["entry_order"] = entry.__dict__
        report.check(entry.status == "FILLED" and abs(entry.executed_qty - qty) < 1e-9 and entry.avg_price > 0, "market_entry",
                     f"{entry.status}, исполнено {entry.executed_qty}/{qty} по {entry.avg_price} (нотионал ≈ {entry.executed_qty * entry.avg_price:.2f} USDT)")
        pos = await client.get_position(symbol)
        report.check(abs(pos.amt - entry.executed_qty) < 1e-9 and pos.entry_price > 0, "position_risk",
                     f"positionAmt {pos.amt}, entryPrice {pos.entry_price} (ответ разобран строго)")
        fill = entry.avg_price or pos.entry_price

        # 7-8: SL и TP одновременно
        sl_price = client.round_price(symbol, fill * (1 - opts.sl_pct / 100))
        tp_price = client.round_price(symbol, fill * (1 + opts.tp_pct / 100))
        sl_cid, tp_cid = client_id(client.id_prefix, "S0", tag), client_id(client.id_prefix, "T0", tag)
        cids += [sl_cid, tp_cid]
        sl = await client.stop_loss(symbol, "SELL", sl_price, client_id=sl_cid, qty=entry.executed_qty)
        report.ok("stop_loss_placed", f"STOP_MARKET {sl_price}, quantity {entry.executed_qty} + reduceOnly: статус {sl.status}")
        tp = await client.take_profit(symbol, "SELL", tp_price, client_id=tp_cid, qty=entry.executed_qty)
        report.ok("take_profit_placed_with_stop", f"TAKE_PROFIT_MARKET {tp_price} при живом SL: сумма reduceOnly = 2x позиции ПРИНЯТА (гипотеза О18)")
        report.facts["algo_sl"], report.facts["algo_tp"] = sl.__dict__, tp.__dict__
        s_sl, s_tp = await algo_state(client, sl_cid), await algo_state(client, tp_cid)
        report.check(s_sl == "NEW" and s_tp == "NEW", "get_algo_order", f"SL {s_sl}, TP {s_tp}")
        live = {a.client_algo_id: a for a in await client.get_open_algo_orders(symbol)}
        report.check({sl_cid, tp_cid} <= live.keys(), "open_algo_orders", f"видны {sorted(live)}")
        if sl_cid in live:
            a = live[sl_cid]
            report.check(a.order_type == "STOP_MARKET" and a.trigger_price == sl_price and a.reduce_only, "open_algo_orders_fields",
                         f"orderType {a.order_type}, trigger {a.trigger_price}, qty {a.qty}, reduceOnly {a.reduce_only}")

        # 9: безубыток - POST нового стопа, затем DELETE старого (как движок); цена ниже рынка, чтобы не сработал сразу
        mark2 = await client.get_mark_price(symbol)
        be_price = client.round_price(symbol, min(fill, mark2 * 0.995))
        be_cid = client_id(client.id_prefix, "S1", tag)
        cids.append(be_cid)
        await client.stop_loss(symbol, "SELL", be_price, client_id=be_cid, qty=entry.executed_qty)
        report.ok("breakeven_new_stop_with_two_live", f"третий reduceOnly-ордер (новый стоп {be_price}) принят при живых SL и TP")
        await client.cancel_algo_order(sl_cid)
        states = {c: await algo_state(client, c) for c in (sl_cid, tp_cid, be_cid)}
        report.check(states[sl_cid] in TERMINAL and states[tp_cid] == "NEW" and states[be_cid] == "NEW", "breakeven_old_stop_cancelled",
                     f"старый SL {states[sl_cid]}, TP {states[tp_cid]}, новый стоп {states[be_cid]}")
        # отказ «сработал бы сразу»: код ответа биржи (информационно)
        try:
            imm = client_id(client.id_prefix, "S9", tag)
            cids.append(imm)
            await client.stop_loss(symbol, "SELL", client.round_price(symbol, mark2 * 1.05), client_id=imm, qty=entry.executed_qty)
            report.info("probe_immediate_trigger", "стоп выше рынка для SELL ПРИНЯТ (ожидался отказ -2021)")
        except BinanceAPIError as exc:
            report.info("probe_immediate_trigger", f"стоп, который сработал бы сразу, отклонён: код {exc.code} {exc.msg}")
    except (BinanceAPIError, OrderValidationError) as exc:
        code = getattr(exc, "code", None)
        report.fail("exchange_rejected", f"{type(exc).__name__}: {exc} (код {code}) - см. полный JSON")
        report.facts["rejection"] = {"type": type(exc).__name__, "code": code, "text": str(exc)}
    finally:
        if entered:
            await cleanup(client, symbol, cids, report)
            try:
                pnl = await client.realized_pnl_since(started_ms - 1000)
                report.ok("income", f"/fapi/v1/income читается: результат теста {pnl:.6f} USDT")
                after = await client.get_balance()
                report.ok("balance_after", f"доступно {after.available} (было {report.facts.get('balance_before', {}).get('available')})")
            except (TradingInfraError, BinanceAPIError) as exc:
                report.fail("income_or_balance_after", f"{type(exc).__name__}: {exc}")


def write_report(report: Report, out_dir: Path, requests: list[Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"report-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    payload = {"rows": report.rows, "facts": report.facts, "requests": requests}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return path


async def main_async(opts: Options) -> int:
    guard()
    report = Report()
    timeout = aiohttp.ClientTimeout(total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BinanceLiveClient.from_env(session)
        log_: list[Any] = []
        orig = client.rest.request

        async def recording(method, path, params=None, **kw):
            entry = {"method": method, "path": path, "params": {k: v for k, v in (params or {}).items()}}
            log_.append(entry)
            try:
                entry["response"] = await orig(method, path, params, **kw)
            except BinanceAPIError as exc:
                entry["error"] = {"code": exc.code, "msg": exc.msg}
                raise
            return entry["response"]

        client.rest.request = recording  # type: ignore[method-assign]
        await run_check(client, opts, report)
        path = write_report(report, OUT_DIR, log_)
    print("\n" + report.table() + f"\n\nОтчёт: {path}\nИтог: {'FAIL' if report.failed else 'OK'}")
    return 1 if report.failed else 0


def cli() -> None:
    setup_logging("test_live_order")
    try:
        code = asyncio.run(main_async(parse_args()))
    except TradingInfraError as exc:  # охрана/конфигурация/сеть: код выхода из типа ошибки (ConfigError -> 3)
        log.error("%s: %s", type(exc).__name__, exc)
        code = exc.exit_code
    logging.shutdown()
    sys.exit(code)


if __name__ == "__main__":
    cli()
