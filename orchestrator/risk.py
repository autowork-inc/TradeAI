"""RiskSizer: объём из процента риска на сделку (D10: строго 1% депозита) с потолком плеча (О8/О12).

Плечо > потолка - сделка ОТКЛОНЯЕТСЯ с причиной, объём молча не урезается: урезанный объём означал бы риск < 1% и
сломал бы математику 1R : 3R на дистанции. Отказ - обычный исход проверки, поэтому возвращается значением
(SizingRejected), а не исключением; неверные входные данные (цена <= 0 и т.п.) - DataError (R1).
"""

from dataclasses import dataclass

from features import defaults as D
from utils.errors import ConfigError, DataError


@dataclass(frozen=True)
class Sizing:
    qty: float  # объём в базовом активе
    notional: float  # qty * entry
    exposure: float  # notional / equity (плечо)
    risk_amount: float  # equity * risk_per_trade: потеря при срабатывании SL, без комиссий


@dataclass(frozen=True)
class SizingRejected:
    reason: str  # 'exposure' | 'bad_risk'
    exposure: float | None = None


class RiskSizer:
    def __init__(self, risk_per_trade: float = D.RISK_PER_TRADE, max_exposure: float = D.MAX_EXPOSURE):
        if not (0 < risk_per_trade < 1 and max_exposure > 0):
            raise ConfigError(f"risk_per_trade в (0, 1) и max_exposure > 0, получено {risk_per_trade}, {max_exposure}")
        self.risk_per_trade, self.max_exposure = risk_per_trade, max_exposure

    def size(self, equity: float, entry: float, sl: float) -> Sizing | SizingRejected:
        if not (equity > 0 and entry > 0 and sl > 0):
            raise DataError(f"RiskSizer: equity, entry, sl должны быть > 0 ({equity}, {entry}, {sl})")
        risk_per_unit = abs(sl - entry)
        if risk_per_unit == 0:
            return SizingRejected("bad_risk")
        risk_amount = equity * self.risk_per_trade
        qty = risk_amount / risk_per_unit
        exposure = qty * entry / equity
        if exposure > self.max_exposure:
            return SizingRejected("exposure", exposure)
        return Sizing(qty, qty * entry, exposure, risk_amount)
