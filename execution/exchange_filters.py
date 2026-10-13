"""Фильтры символа с биржи (exchangeInfo): шаг цены, шаг и минимум лота, минимальная сумма ордера.

Зачем. `config.LIVE_PRICE_TICK / LIVE_QTY_STEP / LIVE_MIN_QTY / LIVE_MIN_NOTIONAL` записаны по памяти (❓): стопы и объёмы,
округлённые не под шаг биржи, отвергаются (-1111 / -1013), то есть защита позиции не ставится. Поэтому LIVE-контур
сверяет конфиг с биржей при старте и падает при расхождении (R1), а не торгует на предположениях.

Источник: публичный `GET /fapi/v1/exchangeInfo`: symbols[].filters[] с filterType PRICE_FILTER (tickSize),
LOT_SIZE (stepSize, minQty), MIN_NOTIONAL (notional). Формат разбирается строго; нет нужного фильтра - DataError.
"""

import math
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Collection

import config
from execution.rest import SignedRest
from utils.errors import ConfigError, DataError


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float


def parse_exchange_info(data: Any, symbols: Collection[str]) -> dict[str, SymbolFilters]:
    if not isinstance(data, dict) or not isinstance(data.get("symbols"), list):
        raise DataError(f"exchangeInfo: ожидается объект с symbols, получено {str(data)[:200]}")
    by_name = {s.get("symbol"): s for s in data["symbols"] if isinstance(s, dict)}
    out: dict[str, SymbolFilters] = {}
    for symbol in symbols:
        info = by_name.get(symbol)
        if info is None:
            raise DataError(f"exchangeInfo: нет символа {symbol}")
        try:
            filters = {f["filterType"]: f for f in info["filters"]}
            out[symbol] = SymbolFilters(
                symbol=symbol,
                tick_size=float(filters["PRICE_FILTER"]["tickSize"]),
                step_size=float(filters["LOT_SIZE"]["stepSize"]),
                min_qty=float(filters["LOT_SIZE"]["minQty"]),
                min_notional=float(filters["MIN_NOTIONAL"]["notional"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"exchangeInfo {symbol}: нет PRICE_FILTER / LOT_SIZE / MIN_NOTIONAL: {exc!r}") from exc
        f = out[symbol]
        if not all(math.isfinite(x) and x > 0 for x in (f.tick_size, f.step_size, f.min_qty, f.min_notional)):
            raise DataError(f"exchangeInfo {symbol}: недопустимые фильтры {f}")
    return out


async def fetch_symbol_filters(rest: SignedRest, symbols: Collection[str]) -> dict[str, SymbolFilters]:
    data = await rest.request("GET", config.EXCHANGE_INFO_PATH, signed=False)
    return parse_exchange_info(data, symbols)


def config_mismatches(filters: dict[str, SymbolFilters]) -> list[str]:
    """Расхождения config.LIVE_* с биржей (пустой список - всё сходится)."""
    problems: list[str] = []
    for symbol, f in filters.items():
        for name, table, actual in (
            ("LIVE_PRICE_TICK", config.LIVE_PRICE_TICK, f.tick_size), ("LIVE_QTY_STEP", config.LIVE_QTY_STEP, f.step_size),
            ("LIVE_MIN_QTY", config.LIVE_MIN_QTY, f.min_qty), ("LIVE_MIN_NOTIONAL", config.LIVE_MIN_NOTIONAL, f.min_notional),
        ):
            have = table.get(symbol)
            if have is None:
                problems.append(f"{symbol}: нет в config.{name} (на бирже {actual})")
            elif abs(have - actual) > 1e-12:
                problems.append(f"{symbol}: config.{name}={have}, на бирже {actual}")
    return problems


def verify_config(filters: dict[str, SymbolFilters]) -> None:
    problems = config_mismatches(filters)
    if problems:
        raise ConfigError("Конфиг расходится с exchangeInfo: " + "; ".join(problems) + ". Поправьте config.py и перепроверьте.")


def round_qty(step: float, qty: float, *, up: bool = False) -> float:
    """Объём, округлённый ВНИЗ (по умолчанию) или ВВЕРХ до целого числа шагов лота."""
    d_step = Decimal(repr(float(step)))
    steps = (Decimal(repr(float(qty))) / d_step).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR)
    return float(steps * d_step)


def min_order_qty(f: SymbolFilters, price: float, *, margin: float = 1.2) -> float:
    """Наименьший допустимый объём ордера: не меньше minQty и покрывает MIN_NOTIONAL с запасом margin (цена между расчётом и ордером гуляет)."""
    if not price > 0:
        raise DataError(f"{f.symbol}: цена {price!r} недопустима")
    return round_qty(f.step_size, max(f.min_qty, f.min_notional * margin / price), up=True)
