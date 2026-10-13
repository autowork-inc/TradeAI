"""Проверка связи с MetaApi и брокером MT5: python -m scripts.test_mt5_connection [--symbol BTCUSD EURUSD] [--timeout 180].

ТОЛЬКО ЧТЕНИЕ: скрипт не отправляет ордеров и не меняет позиции. Ключи берутся из окружения / `.env`
(`METAAPI_TOKEN`, `METAAPI_ACCOUNT_ID`, синоним `ACCOUNT_ID`); токен в вывод не попадает.

Что делает: подключается к аккаунту в облаке (развернуть -> связь терминала с брокером -> синхронизация) и печатает
  1) состояние подключения: аккаунт, сервер, статус облака (DEPLOYED) и связи с брокером (CONNECTED);
  2) баланс и средства (Equity), маржу, плечо, разрешена ли торговля;
  3) котировки Bid/Ask по указанным символам (по умолчанию BTCUSD, EURUSD; если точного имени у брокера нет - ищет похожие,
     например с суффиксом `BTCUSDm`);
  4) открытые позиции и отложенные ордера (только количество).
Код выхода: 0 - всё получено; 3 - ошибка конфигурации (нет ключей / токен отклонён); 1 - сбой связи; 5 - подключено, но по символам
котировок нет.
"""

import argparse
import asyncio
import sys

from exchange.mt5_metaapi import AccountInfo, ConnectionState, MetaApiLiveClient
from utils.errors import ConfigError, TradingInfraError

DEFAULT_SYMBOLS = ("BTCUSD", "EURUSD")
EXIT_NO_QUOTES = 5


def _mask(login: str) -> str:
    return login if len(login) <= 3 else "***" + login[-3:]


def print_state(state: ConnectionState) -> None:
    ok = "OK" if state.broker_connected else "НЕТ СВЯЗИ С БРОКЕРОМ"
    print("== Подключение")
    print(f"  аккаунт:            {state.name} (id {state.account_id[:8]}…)")
    print(f"  платформа / тип:    {state.platform} / {state.account_type}, регион {state.region}")
    print(f"  сервер брокера:     {state.server}, логин {_mask(state.login)}")
    print(f"  состояние аккаунта: {state.state}")
    print(f"  связь с брокером:   {state.connection_status}  [{ok}]")


def print_account(info: AccountInfo) -> None:
    print("== Счёт")
    print(f"  брокер:             {info.broker}")
    print(f"  валюта депозита:    {info.currency}")
    print(f"  баланс (Balance):   {info.balance:,.2f}")
    print(f"  средства (Equity):  {info.equity:,.2f}")
    print(f"  маржа / свободная:  {info.margin:,.2f} / {info.free_margin:,.2f}")
    print(f"  плечо:              1:{info.leverage:g}, режим маржи {info.margin_mode}")
    print(f"  торговля разрешена: {'да' if info.trade_allowed else 'НЕТ'}")


async def resolve_symbol(client: MetaApiLiveClient, wanted: str, available: list[str]) -> str | None:
    """Точное имя, иначе первое, начинающееся с него (брокеры добавляют суффиксы: BTCUSDm, EURUSD.r, EURUSD#)."""
    if wanted in available:
        return wanted
    candidates = sorted(s for s in available if s.upper().startswith(wanted.upper()))
    return candidates[0] if candidates else None


async def run(symbols: list[str], timeout_s: float) -> int:
    client = MetaApiLiveClient()
    await client.connect(timeout_s=timeout_s)
    try:
        print_state(client.connection_state())
        print_account(await client.get_account_info())
        available = await client.get_symbols()
        print(f"== Котировки (у брокера символов: {len(available)})")
        got = 0
        for wanted in symbols:
            symbol = await resolve_symbol(client, wanted, available)
            if symbol is None:
                print(f"  {wanted}: символа нет у брокера")
                continue
            quote = await client.get_price(symbol)
            tag = "" if symbol == wanted else f" (как {wanted})"
            print(f"  {symbol}{tag}: Bid {quote.bid}  Ask {quote.ask}  спред {quote.spread:.6g}  время {quote.time}")
            got += 1
        positions, orders = await client.get_positions(), await client.get_orders()
        print(f"== Открыто позиций: {len(positions)}, отложенных ордеров: {len(orders)}")
    finally:
        await client.close()
    print("Готово: подключение к MetaApi и брокеру работает." if got else "Подключено, но котировок получить не удалось.")
    return 0 if got else EXIT_NO_QUOTES


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Проверка связи с MetaApi / MT5 (только чтение)")
    ap.add_argument("--symbol", nargs="+", default=list(DEFAULT_SYMBOLS), help="символы для котировок (по умолчанию BTCUSD EURUSD)")
    ap.add_argument("--timeout", type=float, default=180.0, help="секунд на подключение (по умолчанию 180)")
    a = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        return asyncio.run(run(a.symbol, a.timeout))
    except ConfigError as exc:
        print(f"ОШИБКА КОНФИГУРАЦИИ: {exc}", file=sys.stderr)
        return exc.exit_code
    except TradingInfraError as exc:
        print(f"СБОЙ СВЯЗИ: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
