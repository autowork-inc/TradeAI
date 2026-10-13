"""FeatureBuilder - строго типизированный фасад для построения признаков.

Признак строится ТОЛЬКО через этот фасад. Он не отдаёт pl.Expr наружу и не принимает его
на вход: доступны лишь операции, каузальные по построению. Запрещённое (глобальные
агрегаты, отрицательный сдвиг, center=True, reverse, backward-fill, join) не «ловится
линтером», а просто отсутствует как метод; попытка обратиться к нему - ошибка в момент
конструирования признака, а не при исполнении.

Что гарантирует каждая временная операция (shift, rolling_*, ewm_mean):
  * работает внутри (symbol, segment_id): история не просачивается между символами и
    через разрыв сетки решений (пропущенные свечи). Первые окна нового сегмента - null;
  * использует только строки <= текущей (строка k известна в asof_ts_k, см. pipeline/sync);
  * окно полное: пока в окне меньше window_size значений, результат null (без «частичных»
    окон, которые выглядят как валидные).
Поэтому кадр обязан быть отсортирован по (symbol, asof_ts) - это делает apply_features.

Арифметика защищена от мусора: деление на 0, log от неположительного, sqrt от
отрицательного и NaN/inf во входных колонках дают null, а не inf/NaN, которые тихо
отравляют скользящие окна.

Защита от ошибок, а не от злого умысла: приватные атрибуты Python при желании достать
можно. Конструктор FeatureExpr закрыт токеном, apply_features принимает только
FeatureExpr, поэтому «случайно» протащить сырой pl.Expr нельзя.
"""

import math
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import ClassVar

import polars as pl

from pipeline.sync._frames import require_columns
from pipeline.types import LABEL_PREFIX, Lookback
from utils.errors import DataError

SYMBOL_COL = "symbol"
SEGMENT_COL = "segment_id"
ORDER_COL = "asof_ts"
_GROUP = [SYMBOL_COL, SEGMENT_COL]

# Служебные колонки таблицы решений. Это ключи, времена и диагностика, а не источники
# признаков: через col() они недоступны (признак от времени/диагностики - утечка и переобучение).
RESERVED_COLUMNS = frozenset({
    SYMBOL_COL, SEGMENT_COL, ORDER_COL, "src_max_ts", "open_time", "close_time", "c", "avail_ts",
    "avail_source", "evidence_lag_ms", "window_start", "contiguous", "book_ok", "book_n_msgs",
    "book_age_ms", "book_max_gap_ms", "book_last_recv_ts", "funding_avail_ts", "funding_age_ms",
})

# У EWM память бесконечна; для прогрева и purge считаем «эффективной» память в такое число
# спанов: (1 - 2/(span+1))^(10*span) ~ e^-20.
EWM_MEMORY_SPANS = 10


class FeatureBuildError(DataError):
    """Признак построен неверно (ошибка конструирования, а не данных)."""


class ForbiddenOperation(FeatureBuildError, AttributeError):
    """Запрошена операция, которой нет в фасаде. Наследует AttributeError, чтобы hasattr()
    и сторонние библиотеки, «щупающие» объект, вели себя штатно."""


_TOKEN = object()


def _check_int(value: object, name: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FeatureBuildError(f"{name} должен быть целым числом, получено {value!r}")
    if value < minimum:
        raise FeatureBuildError(f"{name} должен быть >= {minimum}, получено {value}")
    return value


class FeatureExpr:
    """Неизменяемый узел признака. Создаётся только FeatureBuilder'ом."""

    __slots__ = ("_e", "_lookback", "_cols")

    def __init__(self, token: object, expr: pl.Expr, lookback_bars: int, columns: frozenset[str]):
        if token is not _TOKEN:
            raise FeatureBuildError("FeatureExpr создаётся только через FeatureBuilder")
        object.__setattr__(self, "_e", expr)
        object.__setattr__(self, "_lookback", lookback_bars)
        object.__setattr__(self, "_cols", columns)

    def __setattr__(self, name, value):
        raise FeatureBuildError("FeatureExpr неизменяем")

    # ------------------------------------------------------------- метаданные

    @property
    def lookback_bars(self) -> int:
        """Сколько прошлых баров нужно, чтобы значение стало не-null (для warm-up и purge)."""
        return self._lookback

    @property
    def columns(self) -> frozenset[str]:
        return self._cols

    # --------------------------------------------------------------- арифметика

    def _wrap(self, expr: pl.Expr, lookback: int | None = None, cols: frozenset[str] | None = None):
        return FeatureExpr(
            _TOKEN, expr, self._lookback if lookback is None else lookback,
            self._cols if cols is None else cols,
        )

    def _coerce(self, other: object) -> tuple[pl.Expr, int, frozenset[str]]:
        if isinstance(other, FeatureExpr):
            return other._e, other._lookback, other._cols
        if isinstance(other, (pl.Expr, pl.Series)):
            raise FeatureBuildError("Сырой pl.Expr/pl.Series запрещён: используйте FeatureBuilder.col()")
        if isinstance(other, bool) or not isinstance(other, (int, float)):
            raise FeatureBuildError(f"Операнд должен быть FeatureExpr или числом, получено {other!r}")
        if not math.isfinite(other):
            raise FeatureBuildError(f"Константа должна быть конечной, получено {other!r}")
        return pl.lit(float(other)), 0, frozenset()

    def _binary(self, other, op, *, reflected: bool = False) -> "FeatureExpr":
        o, lb, cols = self._coerce(other)
        left, right = (o, self._e) if reflected else (self._e, o)
        return self._wrap(op(left, right), max(self._lookback, lb), self._cols | cols)

    def __add__(self, other): return self._binary(other, lambda a, b: a + b)
    def __radd__(self, other): return self._binary(other, lambda a, b: a + b, reflected=True)
    def __sub__(self, other): return self._binary(other, lambda a, b: a - b)
    def __rsub__(self, other): return self._binary(other, lambda a, b: a - b, reflected=True)
    def __mul__(self, other): return self._binary(other, lambda a, b: a * b)
    def __rmul__(self, other): return self._binary(other, lambda a, b: a * b, reflected=True)
    def __truediv__(self, other): return self._binary(other, _safe_div)
    def __rtruediv__(self, other): return self._binary(other, _safe_div, reflected=True)

    def __neg__(self) -> "FeatureExpr":
        return self._wrap(-self._e)

    def __abs__(self) -> "FeatureExpr":
        return self._wrap(self._e.abs())

    def log(self) -> "FeatureExpr":
        """Натуральный логарифм; для x <= 0 - null."""
        return self._wrap(pl.when(self._e > 0).then(self._e.log()))

    def sqrt(self) -> "FeatureExpr":
        """Квадратный корень; для x < 0 - null."""
        return self._wrap(pl.when(self._e >= 0).then(self._e.sqrt()))

    # ------------------------------------------------- каузальные временные операции

    def shift(self, n: int) -> "FeatureExpr":
        """Значение n баров назад (n >= 1). Вперёд сдвигать нельзя."""
        _check_int(n, "n", 1)
        return self._wrap(self._e.shift(n).over(_GROUP), self._lookback + n)

    def diff(self, n: int = 1) -> "FeatureExpr":
        return self - self.shift(n)

    def pct_change(self, n: int = 1) -> "FeatureExpr":
        return self / self.shift(n) - 1.0

    def _rolling(self, method: str, window_size: int, minimum_window: int) -> "FeatureExpr":
        w = _check_int(window_size, "window_size", minimum_window)
        # Только полное окно, окно обрывается на последней строке (center недоступен).
        expr = getattr(self._e, method)(window_size=w, min_samples=w).over(_GROUP)
        return self._wrap(expr, self._lookback + w - 1)

    def rolling_mean(self, window_size: int) -> "FeatureExpr":
        return self._rolling("rolling_mean", window_size, 1)

    def rolling_std(self, window_size: int) -> "FeatureExpr":
        """Выборочное стандартное отклонение (ddof=1), окно >= 2."""
        return self._rolling("rolling_std", window_size, 2)

    def rolling_max(self, window_size: int) -> "FeatureExpr":
        return self._rolling("rolling_max", window_size, 1)

    def rolling_min(self, window_size: int) -> "FeatureExpr":
        return self._rolling("rolling_min", window_size, 1)

    def ewm_mean(self, span: int) -> "FeatureExpr":
        """Рекурсивное EWMA (adjust=False), alpha = 2/(span+1). До span наблюдений - null;
        там, где вход null, выход тоже null (устаревшее среднее вперёд не протягивается)."""
        s = _check_int(span, "span", 2)
        ewm = self._e.ewm_mean(span=s, adjust=False, min_samples=s, ignore_nulls=False).over(_GROUP)
        masked = pl.when(self._e.is_not_null()).then(ewm)
        return self._wrap(masked, max(self._lookback, s * EWM_MEMORY_SPANS))

    # ------------------------------------------------------------ запрещённое

    def __getattr__(self, name: str):
        # Вызывается только для несуществующих атрибутов: mean/sum/std/max/min/quantile/
        # rank/reverse/forward_fill/over/rolling_*(center=...) и т.д.
        raise ForbiddenOperation(
            f"Операция {name!r} недоступна в FeatureBuilder: разрешены только арифметика, "
            "log/sqrt, shift(n>=1), diff, pct_change, rolling_mean/std/max/min, ewm_mean. "
            "Глобальные агрегаты, сдвиг вперёд, center=True и заполнение назад запрещены."
        )

    def __bool__(self):
        raise FeatureBuildError("FeatureExpr нельзя использовать как bool (if/and/or/сравнение)")

    def __eq__(self, other):
        raise ForbiddenOperation("Сравнения не входят в FeatureBuilder (используйте арифметику)")

    __ne__ = __lt__ = __le__ = __gt__ = __ge__ = __eq__
    __hash__ = None  # type: ignore[assignment]


def _safe_div(num: pl.Expr, den: pl.Expr) -> pl.Expr:
    return pl.when(den != 0).then(num / den)  # деление на 0 -> null, а не inf


class FeatureBuilder:
    """Фабрика узлов признака над разрешёнными колонками источников."""

    def __init__(self, allowed_columns: Iterable[str]):
        allowed = frozenset(allowed_columns)
        bad = sorted(allowed & RESERVED_COLUMNS)
        if bad:
            raise FeatureBuildError(f"Служебные колонки нельзя делать источниками признаков: {bad}")
        labels = sorted(n for n in allowed if n.startswith(LABEL_PREFIX))
        if labels:
            raise FeatureBuildError(f"Метки смотрят в будущее и не могут быть источниками признаков: {labels}")
        self._allowed = allowed

    @property
    def allowed_columns(self) -> frozenset[str]:
        return self._allowed

    def col(self, name: str) -> FeatureExpr:
        """Колонка источника как Float64; NaN/inf -> null. Только строка, не pl.Expr."""
        if not isinstance(name, str):
            raise FeatureBuildError(f"col() принимает имя колонки (str), получено {type(name).__name__}")
        if name not in self._allowed:
            raise FeatureBuildError(f"Колонка {name!r} недоступна признакам; доступны: {sorted(self._allowed)}")
        raw = pl.col(name).cast(pl.Float64)
        return FeatureExpr(_TOKEN, pl.when(raw.is_finite()).then(raw), 0, frozenset({name}))

    def lit(self, value: float) -> FeatureExpr:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise FeatureBuildError(f"lit() принимает конечное число, получено {value!r}")
        return FeatureExpr(_TOKEN, pl.lit(float(value)), 0, frozenset())


class Feature(ABC):
    """Признак (или группа признаков). Единственный способ задать вычисление - build()."""

    name: ClassVar[str]

    @abstractmethod
    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        """Имя выходной колонки -> узел признака, построенный через fb."""


@dataclass(frozen=True)
class CompiledFeature:
    name: str
    exprs: Mapping[str, pl.Expr]
    lookback: Lookback  # max по выходам, в барах решений (1m)
    columns: frozenset[str]


def source_columns(frame: pl.LazyFrame) -> frozenset[str]:
    """Числовые и булевы колонки кадра решений, не являющиеся служебными, - допустимые источники."""
    schema = frame.collect_schema()
    return frozenset(
        n for n, dt in schema.items()
        if n not in RESERVED_COLUMNS and not n.endswith("_ts") and not n.startswith(LABEL_PREFIX)
        and (dt.is_numeric() or dt == pl.Boolean)
    )


def compile_features(features: Sequence[Feature], allowed_columns: Iterable[str]) -> tuple[CompiledFeature, ...]:
    """Выполняет build() каждого признака; ошибки конструирования всплывают здесь."""
    allowed = frozenset(allowed_columns)
    compiled, seen = [], set()
    for feature in features:
        built = feature.build(FeatureBuilder(allowed))
        if not isinstance(built, Mapping) or not built:
            raise FeatureBuildError(f"{feature.name}: build() должен вернуть непустой словарь")
        exprs, lookback, cols = {}, 0, set()
        for out_name, node in built.items():
            if not isinstance(node, FeatureExpr):
                raise FeatureBuildError(
                    f"{feature.name}.{out_name}: ожидался FeatureExpr, получено {type(node).__name__} "
                    "(сырые pl.Expr запрещены)"
                )
            if not isinstance(out_name, str) or not out_name.isidentifier() or out_name.startswith("_"):
                raise FeatureBuildError(f"Недопустимое имя выходной колонки {out_name!r}")
            if out_name in RESERVED_COLUMNS or out_name in seen or out_name.startswith(LABEL_PREFIX):
                raise FeatureBuildError(f"Имя выходной колонки {out_name!r} занято")
            seen.add(out_name)
            exprs[out_name] = node._e
            lookback = max(lookback, node._lookback)
            cols |= node._cols
        compiled.append(CompiledFeature(feature.name, exprs, Lookback(bars_1m=lookback), frozenset(cols)))
    return tuple(compiled)


def apply_features(frame: pl.LazyFrame, compiled: Sequence[CompiledFeature]) -> pl.LazyFrame:
    """Добавляет признаки к кадру решений. Сортирует по (symbol, asof_ts): временные операции
    фасада опираются на порядок строк внутри (symbol, segment_id)."""
    require_columns(frame, (SYMBOL_COL, SEGMENT_COL, ORDER_COL), "decisions")
    existing = set(frame.collect_schema().names())
    new_cols = [name for c in compiled for name in c.exprs]
    clash = sorted(existing & set(new_cols))
    if clash:
        raise FeatureBuildError(f"Колонки признаков пересекаются с колонками кадра: {clash}")
    return frame.sort(SYMBOL_COL, ORDER_COL).with_columns(
        [expr.alias(name) for c in compiled for name, expr in c.exprs.items()]
    )
