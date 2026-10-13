"""LabelBuilder - метки обучения: форвардная доходность на горизонт horizon_ms.

ЕДИНСТВЕННОЕ место конвейера, где заглядывание в будущее разрешено и необходимо: цель
обучения по определению - то, что произойдёт ПОСЛЕ решения. Поэтому метки изолированы:

  * имена колонок меток начинаются с LABEL_PREFIX ("y_"); FeatureBuilder отказывается принимать
    такие колонки источниками и выходами признаков, source_columns() их не отдаёт;
  * build() возвращает ОТДЕЛЬНЫЙ кадр (symbol, asof_ts + метки), а не добавляет колонки к
    таблице решений: признаки и метки встречаются только при сборке обучающей выборки
    (LabelBuilder.attach), уже после ProvenanceGuard;
  * у каждой метки есть пара {имя}_end_ts - момент знания последней точки, использованной
    меткой (asof_ts строки выхода). Метка в строке k «живёт» в [asof_ts_k, end_ts_k]; при
    разбиении на train/test строки train с end_ts >= начала теста нужно вырезать (purge), иначе
    метки перекрываются с тестом. Для эмбарго достаточно LabelBuilder.max_horizon_ms.

Определение. Решение k принимается в asof_ts_k по цене закрытия свечи k (колонка price_col).
Метка - доходность от цены строки k до цены строки выхода, у которой open_time ровно на
horizon_ms позже:

    y_k = ln(P_exit / P_k)        (kind=LOG)       или      P_exit / P_k - 1   (kind=SIMPLE)

Это «брутто, без трения» доходность по цене закрытия: без комиссий, проскальзывания,
задержки исполнения и фандинга. Реальный результат считает симулятор с ExecutionBarrier
(backtester/contracts); метка - лишь цель для модели. При симметричной задержке ордера
вход и выход сдвигаются на одну и ту же order_latency_ms, поэтому сетка метки совпадает с
сеткой симулятора.

Разрывы данных. Строка выхода ищется точным ключом (symbol, segment_id, open_time + horizon),
а не сдвигом на N строк. Следствия:
  * выход в другом сегменте (между входом и выходом пропущена хотя бы одна свеча) - метка null;
    путь цены не наблюдался, а в симуляторе в такой дыре торговать нельзя;
  * хвост каждого сегмента и конец данных - null (будущего ещё нет), а не «ноль» и не
    протянутая вперёд цена;
  * порядок строк и прореженные кадры (удалены строки) не ломают метку: нет строки с нужным
    ключом - нет метки;
  * цена null/NaN/inf/<= 0 у входа или выхода - null.
Строки с null-меткой в обучение не берутся (attach(..., drop_unlabeled=True)).
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

import polars as pl

import config
from pipeline.features.base import ORDER_COL, SEGMENT_COL, SYMBOL_COL
from pipeline.sync._frames import require_columns
from pipeline.types import LABEL_PREFIX
from utils.errors import ConfigError, DataError

_OPEN_COL = "open_time"
_END_SUFFIX = "_end_ts"  # суффикс _ts: колонка автоматически исключается из источников признаков


class LabelError(DataError):
    """Кадр решений не годится для расчёта меток."""


class ReturnKind(str, Enum):
    LOG = "log"
    SIMPLE = "simple"


def _check_ms(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{name} должен быть положительным целым числом мс, получено {value!r}")
    return value


@dataclass(frozen=True)
class LabelSpec:
    """Одна метка: доходность на horizon_ms вперёд по колонке цены price_col."""

    horizon_ms: int
    interval_ms: int = config.INTERVAL_MS["1m"]  # шаг сетки решений
    price_col: str = "close"
    kind: ReturnKind = ReturnKind.LOG

    def __post_init__(self) -> None:
        _check_ms(self.horizon_ms, "horizon_ms")
        _check_ms(self.interval_ms, "interval_ms")
        # Горизонт вне сетки дал бы метку null у каждой строки - молча, поэтому ошибка здесь.
        if self.horizon_ms % self.interval_ms:
            raise ConfigError(
                f"horizon_ms={self.horizon_ms} не кратен шагу сетки interval_ms={self.interval_ms}: "
                "строки выхода не существует, все метки были бы null"
            )
        if not isinstance(self.price_col, str) or not self.price_col or self.price_col.startswith(LABEL_PREFIX):
            raise ConfigError(f"Недопустимая колонка цены {self.price_col!r}")
        object.__setattr__(self, "kind", ReturnKind(self.kind))

    @property
    def name(self) -> str:
        return f"{LABEL_PREFIX}fwd_{self.kind.value}_ret_{self.horizon_ms}ms"

    @property
    def end_name(self) -> str:
        return f"{self.name}{_END_SUFFIX}"


class LabelBuilder:
    def __init__(self, specs: Sequence[LabelSpec]):
        specs = tuple(specs)
        if not specs:
            raise ConfigError("Нужна хотя бы одна LabelSpec")
        if not all(isinstance(s, LabelSpec) for s in specs):
            raise ConfigError("specs должен содержать только LabelSpec")
        names = [s.name for s in specs]
        if len(set(names)) != len(names):
            raise ConfigError(f"Метки повторяются: {names}")
        self._specs = specs

    @property
    def specs(self) -> tuple[LabelSpec, ...]:
        return self._specs

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self._specs)

    @property
    def end_names(self) -> tuple[str, ...]:
        return tuple(s.end_name for s in self._specs)

    @property
    def max_horizon_ms(self) -> int:
        """Минимальное эмбарго между train и test, если purge по {имя}_end_ts не используется."""
        return max(s.horizon_ms for s in self._specs)

    # ---------------------------------------------------------------------- метки

    def build(self, decisions: pl.LazyFrame) -> pl.LazyFrame:
        """Таблица решений -> (symbol, asof_ts, {имя}, {имя}_end_ts, ...), по символу и asof_ts.

        decisions: результат TimelineAligner.decisions (+ колонка цены каждой спецификации):
        symbol, segment_id, asof_ts, open_time. Строк столько же, сколько во входе.
        """
        prices = list(dict.fromkeys(s.price_col for s in self._specs))
        require_columns(decisions, (SYMBOL_COL, SEGMENT_COL, ORDER_COL, _OPEN_COL, *prices), "decisions")
        base = decisions.select(SYMBOL_COL, SEGMENT_COL, ORDER_COL, _OPEN_COL, *prices)

        out = base
        for i, spec in enumerate(self._specs):
            px = pl.col(spec.price_col).cast(pl.Float64)
            valid_px = pl.when(px.is_finite() & (px > 0)).then(px)
            exit_px, entry_px = f"_exit_px_{i}", f"_entry_px_{i}"
            # Правая сторона: строка выхода, пересчитанная в ключ строки входа (open_time - horizon).
            exit_side = base.select(
                SYMBOL_COL,
                SEGMENT_COL,
                (pl.col(_OPEN_COL) - spec.horizon_ms).alias(_OPEN_COL),
                valid_px.alias(exit_px),
                pl.col(ORDER_COL).alias(spec.end_name),
            )
            ratio = pl.col(exit_px) / pl.col(entry_px)
            label = ratio.log() if spec.kind is ReturnKind.LOG else ratio - 1.0
            out = (
                out.with_columns(valid_px.alias(entry_px))
                .join(exit_side, on=[SYMBOL_COL, SEGMENT_COL, _OPEN_COL], how="left", validate="m:1")
                .with_columns(label.alias(spec.name))
                .with_columns(
                    # end_ts без метки не нужен: null-метка не попадёт в обучение, purge ей не требуется.
                    pl.when(pl.col(spec.name).is_not_null()).then(pl.col(spec.end_name)).alias(spec.end_name)
                )
                .drop(exit_px, entry_px)
            )
        columns = [c for s in self._specs for c in (s.name, s.end_name)]
        return out.select(SYMBOL_COL, ORDER_COL, *columns).sort(SYMBOL_COL, ORDER_COL)

    # ------------------------------------------------------------- сборка выборки

    def attach(
        self,
        features: pl.LazyFrame,
        labels: pl.LazyFrame,
        *,
        drop_unlabeled: bool = False,
    ) -> pl.LazyFrame:
        """Присоединяет метки к кадру признаков точным ключом (symbol, asof_ts) - обучающая выборка.

        Единственная точка, где будущее встречается с признаками. Кадр с метками нельзя
        отдавать ProvenanceGuard (метка по определению знает больше, чем asof_ts) - проверяйте
        признаки ДО attach. drop_unlabeled=True убирает строки, у которых ЛЮБАЯ метка null
        (хвосты сегментов, разрывы): в обучение они не годятся.
        """
        require_columns(features, (SYMBOL_COL, ORDER_COL), "features")
        require_columns(labels, (SYMBOL_COL, ORDER_COL, *self.names, *self.end_names), "labels")
        clash = sorted({*self.names, *self.end_names} & set(features.collect_schema().names()))
        if clash:
            raise LabelError(f"Колонки меток уже есть в кадре признаков: {clash}")
        joined = features.join(
            labels.select(SYMBOL_COL, ORDER_COL, *self.names, *self.end_names),
            on=[SYMBOL_COL, ORDER_COL],
            how="left",
            validate="1:1",
        )
        if drop_unlabeled:
            joined = joined.drop_nulls(list(self.names))
        return joined.sort(SYMBOL_COL, ORDER_COL)
