"""Расчёт размера позиции (лотов) для MT5: риск в деньгах и расстояние до SL -> лоты с учётом контракта брокера.

Формула. Деньги счёта, которые теряются на 1 лоте при ходе цены на `d` до стопа:

    loss_per_lot = d / tickSize * tickValue        (tickValue - потеря за ОДИН тик на 1 лот в ВАЛЮТЕ СЧЁТА)
    loss_per_lot = d * contractSize * rate         (если стоимость тика неизвестна; rate - курс валюты котировки к валюте счёта)
    lots         = risk_usdt / loss_per_lot,   затем ВНИЗ до volumeStep

Источник стоимости тика - `lossTickValue` котировки MetaApi (`MetaApiLiveClient.get_price` -> `Quote.loss_tick_value`): брокер уже
пересчитал её в валюту счёта (для USDJPY при счёте в USD это не 100 000 * 0.001 = 100 иен, а доллары). Второй путь (`rate`) годится, когда
валюта прибыли символа совпадает с валютой счёта (`rate = 1`) или курс известен вызывающему. Нет ни того, ни другого - `ConfigError`.

Примеры (счёт USD, фиксированный риск 5 USD):
  * EURUSD, контракт 100 000, тик 0.00001 = 1 USD/лот; SL 20 пипсов (0.0020): 200 USD на лот -> 0.02 лота.
  * XAUUSD, контракт 100 унций, тик 0.01 = 1 USD/лот; SL 10 USD (10.00): 1 000 USD на лот -> 0.005 лота, вниз до volumeStep (при шаге 0.01 - ниже минимума, `below_min_size`).

Правила (все жёсткие, отказ - значение `Mt5Rejected`, не исключение; неверные входные данные - `ConfigError`, R1):
  * лот округляется ТОЛЬКО ВНИЗ до `volumeStep` (риск не больше целевого); в большую сторону не округляем никогда;
  * лот < `minVolume` -> `below_min_size` (в деталях - сколько риска нужно, чтобы торговать минимальным лотом);
  * лот > `maxVolume` -> `exposure` (объём молча не урезается: иначе реальный риск разошёлся бы с планом, ср. О8);
  * требуемая маржа > доступная (free margin) * `max_margin_use` -> `exposure`;
  * SL не по ту сторону входа / нулевое расстояние / нет эквити -> `bad_risk` / `equity_unavailable`;
  * SL ближе `stopsLevel` брокера -> `stop_too_close` (брокер ответил бы 10016 уже после отправки).

Маржа. Точный источник - `MetaApiLiveClient.calculate_margin` (терминал брокера считает по своему режиму и плечу) - передаётся как
`margin_per_lot`. Запасной грубый расчёт `approx_margin_per_lot` (контракт * цена / плечо) верен для Forex и неверен для CFD на индексы
и сырьё с иным режимом расчёта (❓), поэтому для не-Forex символов нужна `calculate_margin`.
"""

import logging
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import Any

from exchange.symbol_mapper import Mt5SymbolSpec
from features.defaults import RISK_USDT
from utils.errors import ConfigError

log = logging.getLogger(__name__)

_EPS = Decimal("1e-9")


@dataclass(frozen=True)
class Mt5RiskParams:
    risk_usdt: float = RISK_USDT  # фиксированный риск на сделку в валюте счёта (2026-10-19: 5 USD), не доля Equity
    max_margin_use: float = 1.0  # какую долю свободной маржи разрешено занять новой позицией (1.0 = вся; ниже - запас)

    def __post_init__(self) -> None:
        if not self.risk_usdt > 0:
            raise ConfigError(f"risk_usdt {self.risk_usdt}: должен быть > 0")
        if not 0 < self.max_margin_use <= 1:
            raise ConfigError(f"max_margin_use {self.max_margin_use}: допустимо (0, 1]")


@dataclass(frozen=True)
class Mt5OrderPlan:
    """Рассчитанная и проверенная позиция: исполнителю пересчитывать нечего."""

    symbol: str  # внутреннее имя
    broker_symbol: str
    side: str  # BUY | SELL
    lots: float  # на сетке volumeStep, в [minVolume, maxVolume]
    entry: float
    sl: float
    tp: float | None
    sl_distance: float
    loss_per_lot: float  # валюта счёта
    risk_target: float  # фиксированный риск (risk_usdt)
    risk_amount: float  # фактический риск = lots * loss_per_lot (<= risk_target)
    margin_required: float
    equity: float

    @property
    def risk_pct_actual(self) -> float:
        return self.risk_amount / self.equity

    @property
    def close_side(self) -> str:
        return "SELL" if self.side == "BUY" else "BUY"


@dataclass(frozen=True)
class Mt5Rejected:
    reason: str  # below_min_size | exposure | bad_risk | stop_too_close | equity_unavailable
    detail: dict[str, Any] | None = None


def loss_per_lot(spec: Mt5SymbolSpec, sl_distance: float, *, tick_value: float | None = None, quote_to_account: float | None = None) -> float:
    """Потеря в валюте счёта на 1 лоте при проходе цены `sl_distance` (в единицах цены).

    `tick_value` (потеря за тик на 1 лот в валюте счёта, `lossTickValue` котировки) - предпочтительный путь; иначе `quote_to_account`
    (курс валюты котировки к валюте счёта) и размер контракта брокера."""
    if not sl_distance > 0:
        raise ConfigError(f"sl_distance должен быть > 0, получено {sl_distance}")
    if tick_value is not None:
        if not tick_value > 0:
            raise ConfigError(f"{spec.broker}: tick_value должен быть > 0, получено {tick_value}")
        return sl_distance / spec.tick_size * tick_value
    if quote_to_account is None or not quote_to_account > 0:
        raise ConfigError(f"{spec.broker}: нужна стоимость тика (lossTickValue) или курс quote_to_account > 0 (валюта прибыли {spec.profit_currency or '?'})")
    return sl_distance * spec.contract_size * quote_to_account


def floor_to_step(spec: Mt5SymbolSpec, lots: float) -> float:
    """Объём вниз до `volumeStep` (Decimal: 0.1 + 0.2 ≠ 0.3 в float не должно давать лишний шаг)."""
    step = Decimal(str(spec.volume_step))
    return float((Decimal(str(lots)) / step + _EPS).to_integral_value(rounding=ROUND_FLOOR) * step)


def approx_margin_per_lot(spec: Mt5SymbolSpec, price: float, leverage: float, *, quote_to_margin: float = 1.0) -> float:
    """Грубая маржа на 1 лот = контракт * цена / плечо * курс валюты маржи к валюте счёта (верно для Forex). Для CFD - `calculate_margin`."""
    if not (price > 0 and leverage > 0 and quote_to_margin > 0):
        raise ConfigError("approx_margin_per_lot: price, leverage, quote_to_margin должны быть > 0")
    return spec.contract_size * price / leverage * quote_to_margin


def size_position(spec: Mt5SymbolSpec, *, side: str, entry: float, sl: float, tp: float | None = None, equity: float | None,
                  free_margin: float | None, tick_value: float | None = None, quote_to_account: float | None = None,
                  margin_per_lot: float | None = None, leverage: float | None = None, quote_to_margin: float = 1.0,
                  params: Mt5RiskParams = Mt5RiskParams()) -> Mt5OrderPlan | Mt5Rejected:
    """Лоты под фиксированный риск `params.risk_usdt` для входа `entry` со стопом `sl` (см. правила в докстринге модуля).

    Маржа: `margin_per_lot` (лучше - из `calculate_margin` на 1 лот) или `leverage` (грубая оценка); нужно что-то одно - иначе `ConfigError`."""
    if side not in ("BUY", "SELL"):
        raise ConfigError(f"side должен быть BUY или SELL, получено {side!r}")
    if not (entry > 0 and sl > 0):
        raise ConfigError(f"entry и sl должны быть > 0 (entry={entry}, sl={sl})")
    if margin_per_lot is None and leverage is None:
        raise ConfigError("нужен margin_per_lot (calculate_margin) или leverage для проверки маржи")
    if equity is None or free_margin is None or not equity > 0 or free_margin < 0:
        return Mt5Rejected("equity_unavailable", {"equity": equity, "free_margin": free_margin})

    distance = abs(entry - sl)
    wrong_side = sl >= entry if side == "BUY" else sl <= entry
    if wrong_side or distance <= 0:
        return Mt5Rejected("bad_risk", {"why": "sl_wrong_side", "side": side, "entry": entry, "sl": sl})
    if distance < spec.min_stop_distance - 1e-12:
        return Mt5Rejected("stop_too_close", {"distance": distance, "min_distance": spec.min_stop_distance, "stops_level_points": spec.stops_level})

    per_lot = loss_per_lot(spec, distance, tick_value=tick_value, quote_to_account=quote_to_account)
    risk_target = params.risk_usdt
    raw_lots = risk_target / per_lot
    lots = floor_to_step(spec, raw_lots)
    if lots < spec.min_volume - 1e-12:
        return Mt5Rejected("below_min_size", {
            "raw_lots": raw_lots, "rounded_lots": lots, "min_volume": spec.min_volume, "volume_step": spec.volume_step,
            "risk_target": risk_target, "risk_needed_for_min_lot": spec.min_volume * per_lot,
        })
    if lots > spec.max_volume + 1e-12:
        return Mt5Rejected("exposure", {"why": "above_max_volume", "lots": lots, "max_volume": spec.max_volume})

    margin_unit = margin_per_lot if margin_per_lot is not None else approx_margin_per_lot(spec, entry, leverage, quote_to_margin=quote_to_margin)  # type: ignore[arg-type]
    margin_required = lots * margin_unit
    margin_cap = free_margin * params.max_margin_use
    if margin_required > margin_cap + 1e-9:
        return Mt5Rejected("exposure", {"why": "margin", "lots": lots, "margin_required": margin_required, "free_margin": free_margin, "margin_cap": margin_cap})

    return Mt5OrderPlan(
        symbol=spec.internal, broker_symbol=spec.broker, side=side, lots=lots, entry=entry, sl=sl, tp=tp, sl_distance=distance,
        loss_per_lot=per_lot, risk_target=risk_target, risk_amount=lots * per_lot, margin_required=margin_required, equity=equity,
    )
