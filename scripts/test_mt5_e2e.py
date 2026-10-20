"""E2E-проверка MT5: открыть -> проверить SL/TP/риск -> закрыть. python -m scripts.test_mt5_e2e [--symbol EURUSD] [--side BUY] [--sl-pips 20] [--yes].

ОТПРАВЛЯЕТ РЕАЛЬНЫЙ РЫНОЧНЫЙ ОРДЕР на счёт MetaApi из `.env`. Только демо: если в имени сервера брокера нет «demo», скрипт отказывается.
Идёт тем же путём, что боевой контур: `ExecutionRouter.prepare_mt5` (лоты по 5 USD риска, TP 1:4) -> `send_mt5` -> `poll_mt5` (событие
`protected`, выравнивание TP от фактического входа) -> `close_position` по рынку. Ключ `--yes` обязателен (без него - только расчёт, ордер не уходит).
Код выхода: 0 - цикл прошёл и все проверки сошлись; 1 - проверка не сошлась/сбой; 3 - конфигурация/не демо; 4 - расчёт отклонён (рынок закрыт, мин. лот...).
"""

import argparse
import asyncio
import sys
import time

from exchange.mt5_metaapi import MetaApiLiveClient, MetaApiTradeError
from exchange.mt5_risk import Mt5RiskParams
from exchange.router import ExecutionRouter, RouteRejected
from exchange.symbol_mapper import SymbolMapper
from features import defaults as D
from orchestrator.types import Direction, Kind, Signal
from utils.errors import ConfigError, TradingInfraError

EXIT_REJECTED = 4


def _ok(cond: bool, text: str, fails: list[str]) -> None:
    print(f"  [{'OK' if cond else 'ОШИБКА'}] {text}")
    if not cond:
        fails.append(text)


async def run(symbol: str, side: str, sl_pips: float, send: bool, timeout_s: float) -> int:
    client = MetaApiLiveClient()
    await client.connect(timeout_s=timeout_s)
    fails: list[str] = []
    try:
        state, info = client.connection_state(), await client.get_account_info()
        print(f"== Счёт: {info.broker} / {state.server}, {info.currency}, equity {info.equity:,.2f}, free margin {info.free_margin:,.2f}, плечо 1:{info.leverage:g}")
        if "demo" not in f"{state.server} {info.server}".lower():
            raise ConfigError(f"сервер {state.server!r} не похож на демо: реальный ордер не отправляю")
        if info.currency != "USD":
            raise ConfigError(f"валюта счёта {info.currency}, а риск задан в USD")
        mapper = SymbolMapper(client, allow_disabled=True)
        await mapper.load([symbol])
        spec = mapper.spec(symbol)
        router = ExecutionRouter(mt5=client, mapper=mapper, mt5_symbols=(symbol,), binance_symbols=(), risk_params=Mt5RiskParams())
        print(f"== Спецификация {spec.broker}: лот {spec.min_volume}..{spec.max_volume} шаг {spec.volume_step}, тик {spec.tick_size}, digits {spec.digits}, stops_level {spec.stops_level}")

        quote = await client.get_price(spec.broker)
        pip = spec.point * (10 if spec.digits in (3, 5) else 1)
        direction = Direction.LONG if side == "BUY" else Direction.SHORT
        ref = quote.ask if side == "BUY" else quote.bid
        sl = ref - (1 if side == "BUY" else -1) * sl_pips * pip
        sig = Signal(f"e2e-{int(time.time())}", "E2E_Test", symbol, "15m", direction, Kind.MARKET, 0, 0, ref, sl, None, None)
        print(f"== Сигнал: {side} {symbol}, Bid {quote.bid} Ask {quote.ask}, SL-дистанция {sl_pips} пипс")

        prepared = await router.prepare_mt5(sig, int(time.time() * 1000))
        if isinstance(prepared, RouteRejected):
            print(f"ОТКАЗ РАСЧЁТА: {prepared.reason} {prepared.detail}")
            return EXIT_REJECTED
        p = prepared.plan
        rr = abs(p.tp - p.entry) / abs(p.entry - p.sl)
        print(f"== План: {p.lots} лот, вход {p.entry}, SL {p.sl}, TP {p.tp}, риск факт {p.risk_amount:.4f} USD (цель {p.risk_target}), маржа {p.margin_required:.2f}")
        _ok(p.risk_target == D.RISK_USDT == 5.0, f"цель риска = {p.risk_target} USD", fails)
        _ok(p.risk_amount <= 5.0 + 1e-9, f"фактический риск {p.risk_amount:.4f} <= 5 USD (лот округлён вниз)", fails)
        _ok(abs(rr - 4.0) < 0.05 or abs(rr - 4.0) * abs(p.entry - p.sl) <= 2 * spec.tick_size, f"TP/SL по плану = {rr:.3f}R (ожидалось 4.0)", fails)
        steps = p.lots / spec.volume_step
        _ok(abs(steps - round(steps)) < 1e-6 and p.lots >= spec.min_volume, f"лот {p.lots} на сетке шага {spec.volume_step}, >= мин. {spec.min_volume}", fails)
        if not send:
            print("Без --yes ордер не отправляется. Конец.")
            return 0 if not fails else 1

        tracked = await router.send_mt5(prepared)
        if isinstance(tracked, RouteRejected):
            print(f"ОТКАЗ БРОКЕРА: {tracked.reason} {tracked.detail}")
            return EXIT_REJECTED
        print(f"== Ордер отправлен: order_id {tracked.order_id}, position_id {tracked.position_id}")
        await asyncio.sleep(2)
        events = await router.poll_mt5()
        for ev in events:
            print(f"  событие {ev.kind}: {ev.detail}")
        await asyncio.sleep(1)
        await router.poll_mt5()  # второй опрос: выравнивание TP уже отправлено в первом, здесь читаем итог
        positions = [x for x in await client.get_positions() if x.client_id == prepared.client_id]
        if not positions:
            _ok(False, "позиция не найдена у брокера после входа", fails)
            return 1
        pos = positions[0]
        real_risk = abs(pos.open_price - pos.stop_loss) / spec.tick_size * (p.loss_per_lot * spec.tick_size / p.sl_distance) * pos.lots if pos.stop_loss else float("nan")
        real_rr = abs(pos.take_profit - pos.open_price) / abs(pos.open_price - pos.stop_loss) if pos.stop_loss and pos.take_profit else float("nan")
        print(f"== Позиция {pos.position_id}: {pos.side} {pos.lots} @ {pos.open_price}, SL {pos.stop_loss}, TP {pos.take_profit}, плавающий P/L {pos.profit}")
        print(f"   проскальзывание входа: {abs(pos.open_price - p.entry) / pip:.2f} пипс; риск по факту (open-SL) {real_risk:.4f} USD; TP/SL факт {real_rr:.3f}R")
        _ok(pos.lots == p.lots, f"объём у брокера {pos.lots} = план {p.lots}", fails)
        _ok(pos.stop_loss is not None and abs(pos.stop_loss - p.sl) <= spec.tick_size / 2, f"SL у брокера {pos.stop_loss} = план {p.sl}", fails)
        _ok(pos.take_profit is not None and abs(abs(pos.take_profit - pos.open_price) / abs(pos.open_price - p.sl) - 4.0) < 0.05,
            f"TP у брокера {pos.take_profit} = 4R от ФАКТИЧЕСКОГО входа {pos.open_price}", fails)
        _ok(real_risk <= 5.0 * 1.05, f"риск по факту {real_risk:.4f} USD не выше 5 USD (+5% допуск на проскальзывание)", fails)

        print("== Закрытие по рынку")
        closed = False
        try:
            order = await client.close_position(pos.position_id)
            print(f"  ответ: order_id {order.order_id}")
        finally:
            await asyncio.sleep(2)
            left = [x for x in await client.get_positions() if x.position_id == pos.position_id]
            closed = not left
            print(f"  статус: {'ЗАКРЫТА' if closed else 'ВСЁ ЕЩЁ ОТКРЫТА'}")
            if not closed:  # не оставляем тестовую позицию висеть при любом сбое проверок
                await client.close_position(pos.position_id)
        _ok(closed, "позиция закрыта, у брокера её больше нет", fails)
        await router.poll_mt5()
        _ok(spec.internal not in router.tracked_mt5, "роутер освободил символ", fails)
    finally:
        await client.close()
    print("\nИТОГ: " + ("ВСЕ ПРОВЕРКИ ПРОШЛИ" if not fails else f"ПРОВАЛЕНО {len(fails)}: " + "; ".join(fails)))
    return 0 if not fails else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="E2E MT5: вход -> проверка SL/TP/риска -> закрытие (только демо)")
    ap.add_argument("--symbol", default="EURUSD")
    ap.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    ap.add_argument("--sl-pips", type=float, default=20.0)
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--yes", action="store_true", help="реально отправить и закрыть ордер на демо-счёте")
    a = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        return asyncio.run(run(a.symbol, a.side, a.sl_pips, a.yes, a.timeout))
    except ConfigError as exc:
        print(f"ОШИБКА КОНФИГУРАЦИИ: {exc}", file=sys.stderr)
        return 3
    except (MetaApiTradeError, TradingInfraError) as exc:
        print(f"СБОЙ: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
