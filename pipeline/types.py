"""Базовые типы Этапа 2 (Feature Pipeline).

Все времена - целые миллисекунды UTC. Две шкалы времени не смешиваются:
  * серверная (E, T, open_time, close_time) - когда событие произошло на бирже;
  * шкала знания (recv_ts и всё производное: avail_ts, asof_ts) - когда система о нём узнала.
Признаки и сигналы живут только в шкале знания (см. pipeline/sync).
"""

from dataclasses import dataclass
from enum import Enum

from utils.errors import ConfigError


# Колонки-метки (pipeline/labels) начинаются с этого префикса. Это ЕДИНСТВЕННЫЕ колонки, которые
# смотрят в будущее; признаки не имеют права читать их (см. features.base) и не могут их создавать.
LABEL_PREFIX = "y_"


class Source(str, Enum):
    """Источник данных, от которого зависит признак."""

    CANDLE_1M = "candle_1m"
    CANDLE_1H = "candle_1h"
    BOOK = "book"


@dataclass(frozen=True)
class Lookback:
    """Сколько истории нужно признаку: для прогрева (warm-up) и для purge при разбиении."""

    bars_1m: int = 0
    ms: int = 0

    def __post_init__(self) -> None:
        if self.bars_1m < 0 or self.ms < 0:
            raise ConfigError(f"Lookback не может быть отрицательным: {self}")


@dataclass(frozen=True)
class TimingConfig:
    """Допущения о задержках. От них зависит оптимистичность бэктеста - менять осознанно."""

    # Запас к моменту первого сообщения стакана с E >= конца свечи: финализация свечи на бирже
    # и доставка её данных нам. Консервативно.
    candle_publish_lag_ms: int = 1_000
    # Бюджет расчёта признаков: решение принимается через столько после появления свечи.
    # Строго > 0: именно он гарантирует src_max_ts < asof_ts для самой свечи.
    decision_compute_ms: int = 50
    # RTT до биржи при отправке ордера (сервер вне AWS Tokyo). Читает только бэктестер:
    # exec_ts = asof_ts + order_latency_ms.
    order_latency_ms: int = 250
    # Максимальный возраст последнего сообщения стакана на момент решения (asof_ts - recv_ts).
    # Старше - состояние стакана считается устаревшим и признаки стакана обнуляются.
    # Внутренние дыры окна на это не влияют (при обновлении раз в 500 мс 2 с = до 3 потерянных
    # пакетов подряд прямо перед решением).
    book_max_staleness_ms: int = 2_000
    # Если первое сообщение, доказавшее закрытие свечи, пришло с E позже конца свечи больше
    # чем на столько - поток был «слеп» в момент закрытия, дата знания недостоверна.
    # Должен быть меньше длины любого используемого таймфрейма.
    gap_break_ms: int = 5_000
    # Запас к моменту первого сообщения стакана с E >= fundingTime: финальная ставка
    # публикуется в момент списания, плюс доставка. Консервативнее candle_publish_lag_ms.
    funding_publish_lag_ms: int = 2_000
    # Допуск каузального фильтра выбросов E (DataQualityGate).
    e_outlier_tolerance_ms: int = 5_000

    def __post_init__(self) -> None:
        for name in ("candle_publish_lag_ms", "funding_publish_lag_ms"):
            if getattr(self, name) < 0:
                raise ConfigError(f"{name} не может быть отрицательным")
        for name in (
            "decision_compute_ms",
            "order_latency_ms",
            "book_max_staleness_ms",
            "gap_break_ms",
            "e_outlier_tolerance_ms",
        ):
            if getattr(self, name) < 1:
                raise ConfigError(f"{name} должен быть >= 1 мс, сейчас {getattr(self, name)}")
