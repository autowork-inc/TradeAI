"""Общие помощники тестов оркестратора: сигналы, тики, контексты, скриптованная стратегия."""

import polars as pl

from orchestrator.types import Direction, FeatureContext, Kind, Signal, Tick

IV = 900_000  # 15m
SYM = "BTCUSDT"


def mk(strategy="smc", symbol=SYM, direction=Direction.SHORT, kind=Kind.LIMIT, entry=100.0, sl=None, tp=None, inval=None, bar=0) -> Signal:
    """Шорт: вход 100, SL 105 (R = 5), TP 85, отмена при close > 106. Лонг - зеркально (SL 95, TP 115, отмена < 94)."""
    sgn = direction.sgn
    sl = sl if sl is not None else entry + sgn * 5
    if kind is Kind.LIMIT:
        tp = tp if tp is not None else entry - sgn * 15
        inval = inval if inval is not None else sl + sgn * 1
    return Signal(
        signal_id=f"{strategy}:{symbol}:15m:{bar}:{direction.value}", strategy=strategy, symbol=symbol, interval="15m",
        direction=direction, kind=kind, bar_open_time=bar * IV, bar_close_ms=(bar + 1) * IV, entry=entry, sl=sl,
        tp=tp if kind is Kind.LIMIT else None, inval=inval if kind is Kind.LIMIT else None,
    )


def tick(price: float, ts: int = 1, kind: str = "trade", symbol: str = SYM) -> Tick:
    return Tick(symbol, price, ts, kind)


def ctx(symbol: str = SYM, idx: int = 0, close: float = 100.0, received: int | None = None) -> FeatureContext:
    """Минимальный контекст: оркестратору нужны только open_time и close последней строки."""
    frame = pl.DataFrame({"open_time": [idx * IV], "close": [close]})
    return FeatureContext(symbol, "15m", IV, frame, received if received is not None else (idx + 1) * IV + 1000)


class Scripted:
    """Стратегия, отдающая заранее заданные сигналы по номеру бара (open_time // IV)."""

    def __init__(self, name: str, by_bar: dict[int, list[Signal]]):
        self.name, self.by_bar = name, by_bar
        self.calls = 0

    def detect(self, c: FeatureContext) -> tuple[Signal, ...]:
        self.calls += 1
        return tuple(x for x in self.by_bar.get(c.bar_open_time // IV, ()) if x.symbol == c.symbol)  # как настоящая: только своя монета
