"""RiskSizer: объём из ФИКСИРОВАННОГО риска в USDT (2026-10-19: ровно `D.RISK_USDT` = 5 USDT на сделку) с потолком плеча (О8/О12).

Объём = риск / |вход - SL|: процент стопа на сумму убытка не влияет. Плечо (позиция / эквити) > потолка - сделка ОТКЛОНЯЕТСЯ с причиной,
объём молча не урезается: урезанный объём означал бы риск < 5 USDT и сломал бы математику 1R : 4R. Отказ - обычный исход проверки,
поэтому возвращается значением (SizingRejected), а не исключением; неверные входные данные (цена <= 0 и т.п.) - DataError (R1).
"""

from dataclasses import dataclass

from features import defaults as D
from utils.errors import ConfigError, DataError


@dataclass(frozen=True)
class Sizing:
    qty: float  # объём в базовом активе
    notional: float  # qty * entry
    exposure: float  # notional / equity (плечо)
    risk_amount: float  # фиксированный риск (USDT): потеря при срабатывании SL, без комиссий


@dataclass(frozen=True)
class SizingRejected:
    reason: str  # 'exposure' | 'bad_risk'
    exposure: float | None = None


class RiskSizer:
    def __init__(self, risk_usdt: float = D.RISK_USDT, max_exposure: float = D.MAX_EXPOSURE):
        if not (risk_usdt > 0 and max_exposure > 0):
            raise ConfigError(f"risk_usdt и max_exposure должны быть > 0, получено {risk_usdt}, {max_exposure}")
        self.risk_usdt, self.max_exposure = risk_usdt, max_exposure

    def size(self, equity: float, entry: float, sl: float) -> Sizing | SizingRejected:
        if not (equity > 0 and entry > 0 and sl > 0):
            raise DataError(f"RiskSizer: equity, entry, sl должны быть > 0 ({equity}, {entry}, {sl})")
        risk_per_unit = abs(sl - entry)
        if risk_per_unit == 0:
            return SizingRejected("bad_risk")
        risk_amount = self.risk_usdt
        qty = risk_amount / risk_per_unit
        exposure = qty * entry / equity
        if exposure > self.max_exposure:
            return SizingRejected("exposure", exposure)
        return Sizing(qty, qty * entry, exposure, risk_amount)
