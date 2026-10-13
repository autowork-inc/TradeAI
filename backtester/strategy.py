"""Интерфейс стратегии и каркасная BaselineStrategy.

Стратегия - чистая функция «снимок -> запросы». Она не видит кадр целиком (только текущую строку),
не знает о барьере и не ставит время: asof_ts проставляет движок, exec_ts - барьер. Поэтому
возвращает она OrderRequest (намерение без времени), а не OrderIntent.

FeatureContext - всё, что известно на шаге: строка признаков, эквити, исполненные позиции и
остатки открытых ордеров, последние цены. Будущего в нём нет по построению (строка - read-only
MappingProxyType, кадра в контексте нет, колонки меток y_ движок в кадр не пускает).

DecisionContext - прежнее имя FeatureContext (оно использовалось в engine.py и тестах).

BaselineStrategy - ТОЛЬКО каркас для сквозного прогона движка, не торговая идея: ничего не
подбирает и не обучается. Её PnL не доказывает наличия edge.
"""

import math
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from backtester.contracts import BarrierEvent, Side
from backtester.risk import QTY_EPS, OrderRequest
from utils.errors import ConfigError, DataError


class StrategyError(DataError):
    """Стратегия не может работать с этой строкой (например, нет нужной колонки признака)."""


# ----------------------------------------------------------------------- контекст


@dataclass(frozen=True)
class FeatureContext:
    """Всё, что стратегия знает на шаге. Будущего здесь нет по построению."""

    ts: int  # asof_ts
    symbol: str  # символ строки
    row: Mapping[str, object]  # текущая строка таблицы решений (признаки и т.д.)
    equity: float
    positions: Mapping[str, float]  # исполненные знаковые позиции
    pending: Mapping[str, float]  # знаковые остатки открытых ордеров
    marks: Mapping[str, float]  # последние известные цены

    @property
    def position(self) -> float:
        """Исполненная знаковая позиция символа строки."""
        return self.positions.get(self.symbol, 0.0)

    @property
    def pending_qty(self) -> float:
        """Знаковая сумма неисполненных остатков открытых ордеров символа строки."""
        return self.pending.get(self.symbol, 0.0)

    @property
    def exposure(self) -> float:
        """Позиция + ордера в пути: от неё считается, сколько ещё нужно докупить/продать."""
        return self.position + self.pending_qty

    @property
    def price(self) -> float | None:
        return self.marks.get(self.symbol)

    def feature(self, name: str) -> float | None:
        """Значение признака строки; None - значения нет (null/NaN/inf: прогрев, устаревший стакан).

        Отсутствие самой КОЛОНКИ - ошибка конфигурации, а не «нет сигнала»: StrategyError.
        """
        if name not in self.row:
            raise StrategyError(f"В строке решений нет признака {name!r}; есть: {sorted(self.row)}")
        value = self.row[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None
        return float(value)


DecisionContext = FeatureContext


# ---------------------------------------------------------------------- интерфейс


class Strategy(ABC):
    @abstractmethod
    def decide(self, ctx: FeatureContext) -> Sequence[OrderRequest]:
        """Запросы по строке. Пустой список - ничего не делать."""

    def on_event(self, event: BarrierEvent) -> None:
        """Исполнение, закрытие ордера или списание фандинга (после применения к портфелю)."""


# ------------------------------------------------------------------------ baseline


def _positive_fraction(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} должен быть конечным числом > 0, получено {value!r}")
    return float(value)


class BaselineStrategy(Strategy):
    """Пересечение скользящих средних с фильтром Order Book Imbalance. Только market-ордера.

    Тренд: fast > slow - лонг, fast < slow - шорт. OBI подтверждает ВХОД: лонгу нужен obi > 0 и
    не меньше порога, шорту obi < 0 и |obi| не меньше порога (obi_col=None отключает фильтр). Если направление нужно сменить, а входа
    OBI не подтверждает (или стакан устарел - obi null), позиция просто закрывается. Любой признак
    null (прогрев, устаревший стакан) - ничего не делаем: нет данных, нет действий.

    Размер: целевая позиция = equity_fraction * equity / цена, фиксируется при входе и не
    пересчитывается, пока направление не сменилось (нет дробления ордеров от дрейфа эквити).
    Решение сравнивается с экспозицией (позиция + ордера в пути), поэтому ордер, ещё не
    исполненный к следующему шагу, не дублируется. Шаг лота, минимальный номинал и биржевые
    фильтры количества НЕ моделируются.

    Известное ограничение: market-ордер - IOC, и если глубины стакана не хватило (остаток закрыт
    NO_LIQUIDITY), позиция остаётся МЕНЬШЕ целевой и не докупается до смены тренда: «уже в нужную
    сторону» проверяется по знаку, а не по размеру. Для каркаса это приемлемо, для боевой логики - нет.
    """

    def __init__(
        self,
        *,
        fast_col: str = "ma_fast",
        slow_col: str = "ma_slow",
        obi_col: str | None = "obi",
        obi_threshold: float = 0.0,
        equity_fraction: float = 0.1,
    ):
        if isinstance(obi_threshold, bool) or not isinstance(obi_threshold, (int, float)) \
                or not math.isfinite(obi_threshold) or not 0 <= obi_threshold < 1:
            raise ConfigError(f"obi_threshold - число в [0, 1), получено {obi_threshold!r}")
        self._fast, self._slow, self._obi = fast_col, slow_col, obi_col
        self._thr = float(obi_threshold)
        self._fraction = _positive_fraction(equity_fraction, "equity_fraction")

    def decide(self, ctx: FeatureContext) -> Sequence[OrderRequest]:
        fast, slow = ctx.feature(self._fast), ctx.feature(self._slow)
        price = ctx.price
        if fast is None or slow is None or price is None or price <= 0 or fast == slow:
            return ()
        trend = 1 if fast > slow else -1

        exposure = ctx.exposure
        if abs(exposure) > QTY_EPS and (exposure > 0) == (trend > 0):
            return ()  # уже в нужную сторону

        if self._confirmed(ctx, trend):
            target = trend * self._fraction * ctx.equity / price
        else:
            target = 0.0  # новый вход не подтверждён: только закрываем старую позицию
        delta = target - exposure
        if abs(delta) <= QTY_EPS:
            return ()
        return (OrderRequest(ctx.symbol, Side.BUY if delta > 0 else Side.SELL, abs(delta)),)

    def _confirmed(self, ctx: FeatureContext, trend: int) -> bool:
        if self._obi is None:
            return True
        obi = ctx.feature(self._obi)
        # знак обязан совпасть с трендом (нулевой дисбаланс не подтверждает), и сила - не меньше порога
        return obi is not None and trend * obi > 0 and trend * obi >= self._thr
