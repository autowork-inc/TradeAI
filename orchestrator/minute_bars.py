"""1m-свечи из тиков сделок и фильтр MARKET-входа во время импульса (Блок 3, 2026-10-18).

Зачем. MARKET-вход исполняется по первому тику после сигнала. Если в этот момент цена летит «ножом» против сделки (дамп для лонга, вынос для шорта),
вход по рынку - ловля падающего ножа: стоп выбивает раньше, чем стратегия успевает оказаться правой. Правило: MARKET-лонг во время дампа разрешён
ТОЛЬКО при бычьем поглощении на последней закрытой 1m-свече (продавец выдохся, покупатель перехватил), MARKET-шорт во время выноса - только при
медвежьем поглощении (зеркально, D6: стратегии симметричны). LIMIT-заявки фильтр не трогает (их защищает отмена по импульсной свече, tracker.py).

Откуда 1m. Отдельного потока kline_1m нет (лишние подключения): `MinuteBars` собирает минутки из тех же тиков сделок (`aggTrade`, у MT5 - середина
bid/ask), что двигают трекер. Минута закрыта, когда пришёл тик следующей минуты. Пропуск минуты (тиков не было, обрыв потока) рвёт историю - она
копится заново: ATR по рваной сетке был бы ложным.

Определения (константы - features/defaults.py):
  * ATR(14) 1m - SMA истинного диапазона 14 закрытых минут перед окном импульса (не включая окно: порог не зависит от самого импульса);
  * дамп - max(high) последних 3 закрытых минут - close последней >= 2.5 * ATR; вынос - close последней - min(low) >= 2.5 * ATR;
  * бычье поглощение - последняя минута бычья, предыдущая медвежья, тело последней накрывает тело предыдущей (open <= close_prev, close >= open_prev);
    медвежье - зеркально.
Fail-closed: закрытых минут меньше 14 + 3 (старт процесса, обрыв, редкие тики) - MARKET-сигнал пропускается с причиной `no_1m_data`: проверить нож
нечем, а вход вслепую - именно то, от чего фильтр защищает.
"""

from collections import deque
from dataclasses import dataclass

from features import defaults as D
from orchestrator.types import Direction, Kind, Signal, Tick

MINUTE_MS = 60_000


@dataclass
class _Bar:
    t: int
    o: float
    h: float
    lo: float
    c: float


class MinuteBars:
    def __init__(self, keep: int = D.MINUTE_ATR_BARS + D.MINUTE_IMPULSE_BARS + 5):
        self._keep = keep
        self._closed: dict[str, deque[_Bar]] = {}
        self._open: dict[str, _Bar] = {}

    def on_tick(self, tick: Tick) -> None:
        if tick.kind != "trade":
            return
        t = tick.ts_ms - tick.ts_ms % MINUTE_MS
        cur = self._open.get(tick.symbol)
        if cur is not None and t < cur.t:
            return  # запоздавший тик прошлой минуты: минута уже закрыта
        if cur is None or t > cur.t:
            if cur is not None:
                closed = self._closed.setdefault(tick.symbol, deque(maxlen=self._keep))
                closed.append(cur)
                if t != cur.t + MINUTE_MS:
                    closed.clear()  # следующая минута не подряд (тиков не было): прежняя история к текущему моменту уже не относится
            self._open[tick.symbol] = _Bar(t, tick.price, tick.price, tick.price, tick.price)
            return
        cur.h, cur.lo, cur.c = max(cur.h, tick.price), min(cur.lo, tick.price), tick.price

    def closed(self, symbol: str) -> list[_Bar]:
        return list(self._closed.get(symbol, ()))


def impulse_check(bars: list[_Bar], direction: Direction, *, atr_bars: int = D.MINUTE_ATR_BARS, window: int = D.MINUTE_IMPULSE_BARS,
                  mult: float = D.MINUTE_IMPULSE_ATR) -> str | None:
    """Причина запрета MARKET-входа (`no_1m_data` | `impulse_dump` | `impulse_pump`) или None - вход разрешён."""
    if len(bars) < atr_bars + window:
        return "no_1m_data"
    base = bars[-(atr_bars + window):-window]
    pc = bars[-(atr_bars + window) - 1].c if len(bars) > atr_bars + window else None  # закрытие перед первой минутой базы (для истинного диапазона)
    trs = []
    for b in base:
        trs.append(b.h - b.lo if pc is None else max(b.h, pc) - min(b.lo, pc))
        pc = b.c
    atr = sum(trs) / atr_bars
    if not atr > 0:
        return None  # минутки без движения: импульса нет
    win, last, prev = bars[-window:], bars[-1], bars[-2]
    if direction is Direction.LONG:
        dump = max(b.h for b in win) - last.c >= mult * atr
        engulf = last.c > last.o and prev.c < prev.o and last.o <= prev.c and last.c >= prev.o
        return "impulse_dump" if dump and not engulf else None
    pump = last.c - min(b.lo for b in win) >= mult * atr
    engulf = last.c < last.o and prev.c > prev.o and last.o >= prev.c and last.c <= prev.o
    return "impulse_pump" if pump and not engulf else None


class MarketEntryGuard:
    """Фильтр сигналов Оркестратора (`signal_gates`): только MARKET-сигналы; LIMIT проходит без проверки."""

    name = "market_impulse"

    def __init__(self, bars: MinuteBars):
        self._bars = bars

    def check(self, signal: Signal, now_ms: int) -> str | None:
        if signal.kind is not Kind.MARKET:
            return None
        return impulse_check(self._bars.closed(signal.symbol), signal.direction)
