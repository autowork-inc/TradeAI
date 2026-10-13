"""MLModelStrategy: торговые решения по прогнозу модели + лёгкая Ridge-регрессия без внешних зависимостей.

Модель предсказывает метку y_ (LabelBuilder: форвардная лог-доходность на горизонт) по строке признаков.
Стратегия в каждый момент видит ТОЛЬКО текущую строку (FeatureContext), берёт у модели значения
model.feature_names в том же порядке, получает прогноз и приводит позицию к целевой:

    pred >= +threshold                        -> лонг
    pred <= -threshold                        -> шорт
    |pred| < threshold, позиция открыта       -> держать, пока знак прогноза совпадает с позицией и
                                                 |pred| > exit_threshold; иначе закрыть
    |pred| < threshold, позиции нет           -> ничего

threshold - в единицах метки (лог-доходность за горизонт: 0.001 = 10 bps) и должен покрывать издержки
круга: 2 * taker_fee + спред + проскальзывание. Иначе модель с «правильным» знаком торгует в минус.
exit_threshold < threshold даёт гистерезис: вход по сильному сигналу, выход только когда сигнал выдохся
(иначе позиция дребезжит вокруг порога и платит комиссии). Любой признак null (прогрев, устаревший
стакан) - ничего не делаем: нет данных, нет действий (как BaselineStrategy).

Размер как в BaselineStrategy: целевая позиция = equity_fraction * equity / цена фиксируется при входе и не
пересчитывается, пока направление не сменилось. Решение сравнивается с экспозицией (позиция + ордера в
пути). Известное ограничение то же: market - IOC, при нехватке глубины позиция остаётся меньше целевой.

Модель обучается СНАРУЖИ (scripts/train_walkforward.py) на train-фолде; стратегия модель не обучает и
не дообучает (никакого online-обновления на тесте). Защита от утечки меток в самой модели: fit()
отказывается брать признаками колонки y_* и *_ts и требует, чтобы цель была меткой (y_*).

В бой модель переезжает файлом: RidgeRegressor.save_json / load_json хранят только обученное состояние (имена
признаков, средние, масштабы, веса, средний y), обучающих данных в файле нет.
"""

import json
import math
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path

import polars as pl

from backtester.contracts import Side
from backtester.risk import QTY_EPS, OrderRequest
from backtester.strategy import FeatureContext, Strategy
from pipeline.types import LABEL_PREFIX
from utils.errors import ConfigError, DataError
from utils.storage import atomic_write_text


class ModelError(DataError):
    """Модель не может быть обучена, применена на этих данных или прочитана из файла."""


MODEL_FORMAT = "ridge-regressor/1"


def _number(value: object, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ModelError(f"{what}: ожидалось конечное число, получено {value!r}")
    return float(value)


# --------------------------------------------------------------------------- интерфейс


class FeatureModel(ABC):
    """Регрессор «строка признаков -> число». Любая библиотека (CatBoost, sklearn) подключается этим интерфейсом."""

    @property
    @abstractmethod
    def is_fitted(self) -> bool: ...

    @property
    @abstractmethod
    def feature_names(self) -> tuple[str, ...]:
        """Признаки в порядке, который ждёт predict_row. Пусто до fit()."""

    @abstractmethod
    def fit(self, frame: pl.DataFrame, feature_names: Sequence[str], target: str) -> "FeatureModel":
        """Обучение на frame. Кадр - только train-фолд: модель ничего другого не видит и не хранит."""

    @abstractmethod
    def predict_row(self, values: Sequence[float]) -> float:
        """Прогноз по одной строке: значения в порядке feature_names, все конечные."""

    def predict(self, frame: pl.DataFrame) -> pl.Series:
        """Прогноз по кадру (диагностика, IC). Тот же путь, что и в стратегии: predict_row."""
        if not self.is_fitted:
            raise ModelError("модель не обучена")
        rows = frame.select(self.feature_names).iter_rows()
        return pl.Series("pred", [self.predict_row(r) for r in rows], dtype=pl.Float64)


def check_training_frame(frame: pl.DataFrame, feature_names: Sequence[str], target: str) -> tuple[str, ...]:
    """Общие проверки fit(): имена, отсутствие утечек по имени, отсутствие null/NaN/inf."""
    names = tuple(feature_names)
    if not names or len(set(names)) != len(names):
        raise ModelError(f"feature_names: нужен непустой список без повторов, получено {names!r}")
    leaky = sorted(n for n in names if n.startswith(LABEL_PREFIX) or n.endswith("_ts"))
    if leaky:
        raise ModelError(f"Признаками нельзя брать метки (y_*) и времена (*_ts): {leaky}")
    if not target.startswith(LABEL_PREFIX):
        raise ModelError(f"Цель должна быть меткой LabelBuilder (префикс {LABEL_PREFIX!r}), получено {target!r}")
    missing = [c for c in (*names, target) if c not in frame.columns]
    if missing:
        raise ModelError(f"В кадре нет колонок {missing}")
    bad = frame.select(
        [(pl.col(c).is_null() | ~pl.col(c).cast(pl.Float64).is_finite()).sum().alias(c) for c in (*names, target)]
    ).row(0, named=True)
    bad = {c: n for c, n in bad.items() if n}
    if bad:
        raise ModelError(f"В обучающем кадре null/NaN/inf (уберите строки заранее, drop_nulls): {bad}")
    return names


# ------------------------------------------------------------------------------- Ridge


def _cholesky_solve(a: list[list[float]], b: list[float]) -> list[float]:
    """Решает a x = b для симметричной положительно определённой a (разложение Холецкого)."""
    n = len(b)
    low = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = a[i][j] - sum(low[i][k] * low[j][k] for k in range(j))
            if i == j:
                if s <= 0.0 or not math.isfinite(s):
                    raise ModelError("матрица нормальных уравнений не положительно определена")
                low[i][i] = math.sqrt(s)
            else:
                low[i][j] = s / low[j][j]
    y = [0.0] * n
    for i in range(n):
        y[i] = (b[i] - sum(low[i][k] * y[k] for k in range(i))) / low[i][i]
    x = [0.0] * n
    for i in reversed(range(n)):
        x[i] = (y[i] - sum(low[k][i] * x[k] for k in range(i + 1, n))) / low[i][i]
    return x


class RidgeRegressor(FeatureModel):
    """Ridge-регрессия в замкнутой форме. Признаки стандартизуются СТАТИСТИКАМИ ОБУЧАЮЩЕГО КАДРА.

    alpha - штраф НА НАБЛЮДЕНИЕ: решается (Z'Z + alpha * n * I) w = Z'(y - mean(y)) для стандартизованных Z,
    поэтому одно и то же alpha значит одну и ту же силу регуляризации на фолдах разной длины
    (растущее окно walk-forward). Свободный член не штрафуется. Константный признак (std = 0) получает
    нулевой вес. Детерминирован.
    """

    def __init__(self, alpha: float = 1.0):
        if isinstance(alpha, bool) or not isinstance(alpha, (int, float)) or not math.isfinite(alpha) or alpha <= 0:
            raise ConfigError(f"alpha должен быть конечным числом > 0, получено {alpha!r}")
        self.alpha = float(alpha)
        self._names: tuple[str, ...] = ()
        self._mean: list[float] = []
        self._scale: list[float] = []
        self._weights: list[float] = []  # в стандартизованном пространстве
        self._y_mean = 0.0
        self.n_train = 0

    @property
    def is_fitted(self) -> bool:
        return bool(self._names)

    @property
    def feature_names(self) -> tuple[str, ...]:
        return self._names

    @property
    def coef_(self) -> dict[str, float]:
        """Коэффициенты в единицах исходных признаков."""
        return {n: w / s for n, w, s in zip(self._names, self._weights, self._scale)}

    @property
    def intercept_(self) -> float:
        return self._y_mean - sum(w / s * m for w, s, m in zip(self._weights, self._scale, self._mean))

    def fit(self, frame: pl.DataFrame, feature_names: Sequence[str], target: str) -> "RidgeRegressor":
        names = check_training_frame(frame, feature_names, target)
        n, p = frame.height, len(names)
        if n < p + 2:
            raise ModelError(f"Мало строк для обучения: {n} при {p} признаках")

        x = frame.select([pl.col(c).cast(pl.Float64) for c in names])
        stats = x.select(
            [pl.col(c).mean().alias(f"m{i}") for i, c in enumerate(names)]
            + [pl.col(c).std(ddof=0).alias(f"s{i}") for i, c in enumerate(names)]
        ).row(0)
        mean = list(stats[:p])
        scale = [s if s > 0.0 else 1.0 for s in stats[p:]]
        y_mean = float(frame[target].cast(pl.Float64).mean())

        z = [(pl.col(c).cast(pl.Float64) - mean[i]) / scale[i] for i, c in enumerate(names)]
        yc = pl.col(target).cast(pl.Float64) - y_mean
        pairs = [(i, j) for i in range(p) for j in range(i, p)]
        sums = frame.select(
            [(z[i] * z[j]).sum().alias(f"g{i}_{j}") for i, j in pairs] + [(z[i] * yc).sum().alias(f"b{i}") for i in range(p)]
        ).row(0)
        gram = [[0.0] * p for _ in range(p)]
        for (i, j), v in zip(pairs, sums):
            gram[i][j] = gram[j][i] = v
        for i in range(p):
            gram[i][i] += self.alpha * n
        rhs = list(sums[len(pairs):])

        self._weights = _cholesky_solve(gram, rhs)
        self._mean, self._scale, self._y_mean, self._names, self.n_train = mean, scale, y_mean, names, n
        return self

    def predict_row(self, values: Sequence[float]) -> float:
        if not self.is_fitted:
            raise ModelError("модель не обучена")
        if len(values) != len(self._names):
            raise ModelError(f"Ожидалось {len(self._names)} значений {self._names}, получено {len(values)}")
        total = self._y_mean
        for v, m, s, w in zip(values, self._mean, self._scale, self._weights):
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
                raise ModelError(f"Недопустимое значение признака: {v!r}")
            total += w * (v - m) / s
        return total

    # ------------------------------------------------------------------ сериализация

    def save_json(self, path: Path) -> None:
        """Сохраняет ТОЛЬКО обученное состояние (имена, средние, масштабы, веса, средний y).

        Обучающего кадра в файле нет. alpha и n_train - справочные скаляры (для инспекции файла), на
        предсказание не влияют. Запись атомарна (tmp -> fsync -> replace): боевой процесс не прочтёт
        половину файла. Нечисловое состояние (NaN/inf) - ModelError, а не файл, который не откроется.
        """
        if not self.is_fitted:
            raise ModelError("модель не обучена: сохранять нечего")
        payload = {
            "format": MODEL_FORMAT,
            "alpha": self.alpha,
            "n_train": self.n_train,
            "names": list(self._names),
            "mean": self._mean,
            "scale": self._scale,
            "weights": self._weights,
            "y_mean": self._y_mean,
        }
        try:
            text = json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2)
        except ValueError as exc:
            raise ModelError(f"В состоянии модели NaN/inf, сохранять нельзя: {exc}") from exc
        atomic_write_text(Path(path), text + "\n")

    @classmethod
    def load_json(cls, path: Path) -> "RidgeRegressor":
        """Читает модель, сохранённую save_json. Любое отклонение от формата - ModelError (молча не чиним)."""
        path = Path(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise ModelError(f"Не удалось прочитать модель {path}: {exc}") from exc
        except ValueError as exc:  # JSONDecodeError и UnicodeDecodeError
            raise ModelError(f"Файл модели {path} не является JSON: {exc}") from exc
        if not isinstance(raw, dict) or raw.get("format") != MODEL_FORMAT:
            got = raw.get("format") if isinstance(raw, dict) else type(raw).__name__
            raise ModelError(f"Файл {path}: ожидался формат {MODEL_FORMAT!r}, получено {got!r}")

        names = raw.get("names")
        if not isinstance(names, list) or not names or not all(isinstance(n, str) for n in names):
            raise ModelError(f"Файл {path}: names должен быть непустым списком строк")
        if len(set(names)) != len(names):
            raise ModelError(f"Файл {path}: имена признаков повторяются: {names}")
        leaky = sorted(n for n in names if n.startswith(LABEL_PREFIX) or n.endswith("_ts"))
        if leaky:
            raise ModelError(f"Файл {path}: признаками нельзя брать метки (y_*) и времена (*_ts): {leaky}")

        def numbers(key: str) -> list[float]:
            values = raw.get(key)
            if not isinstance(values, list) or len(values) != len(names):
                raise ModelError(f"Файл {path}: {key} должен быть списком из {len(names)} чисел")
            return [_number(v, f"{path}: {key}") for v in values]

        mean, scale, weights = numbers("mean"), numbers("scale"), numbers("weights")
        if any(s <= 0.0 for s in scale):
            raise ModelError(f"Файл {path}: scale должен быть > 0 (константный признак хранится как 1.0)")
        y_mean = _number(raw.get("y_mean"), f"{path}: y_mean")
        alpha = _number(raw.get("alpha", 1.0), f"{path}: alpha")
        n_train = raw.get("n_train", 0)
        if isinstance(n_train, bool) or not isinstance(n_train, int) or n_train < 0:
            raise ModelError(f"Файл {path}: n_train должен быть целым >= 0")
        try:
            model = cls(alpha)
        except ConfigError as exc:
            raise ModelError(f"Файл {path}: {exc}") from exc
        model._names, model._mean, model._scale, model._weights = tuple(names), mean, scale, weights
        model._y_mean, model.n_train = y_mean, n_train
        return model


# --------------------------------------------------------------------------- стратегия


def _finite(value: object, name: str, *, positive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) \
            or (value <= 0 if positive else value < 0):
        raise ConfigError(f"{name} должен быть конечным числом {'> 0' if positive else '>= 0'}, получено {value!r}")
    return float(value)


class MLModelStrategy(Strategy):
    """Торговля по прогнозу обученной модели. Правила - в докстринге модуля."""

    def __init__(
        self,
        model: FeatureModel,
        *,
        threshold: float,
        exit_threshold: float = 0.0,
        equity_fraction: float = 0.1,
    ):
        if not isinstance(model, FeatureModel):
            raise ConfigError("model должен быть наследником FeatureModel")
        if not model.is_fitted:
            raise ConfigError("Модель не обучена: стратегия не обучает модель сама")
        self._model = model
        self._thr = _finite(threshold, "threshold", positive=True)
        self._exit = _finite(exit_threshold, "exit_threshold", positive=False)
        if self._exit >= self._thr:
            raise ConfigError(f"exit_threshold={self._exit} должен быть меньше threshold={self._thr}")
        self._fraction = _finite(equity_fraction, "equity_fraction", positive=True)

    @property
    def threshold(self) -> float:
        return self._thr

    def predict(self, ctx: FeatureContext) -> float | None:
        """Прогноз модели по строке или None, если строки нет/признак null (прогрев, устаревший стакан)."""
        values = [ctx.feature(name) for name in self._model.feature_names]
        price = ctx.price
        if price is None or price <= 0 or any(v is None for v in values):
            return None
        pred = self._model.predict_row(values)  # type: ignore[arg-type]  (None отсеяны выше)
        return pred if math.isfinite(pred) else None

    def decide(self, ctx: FeatureContext) -> Sequence[OrderRequest]:
        pred = self.predict(ctx)
        price = ctx.price
        if pred is None:
            return ()

        exposure = ctx.exposure
        current = 0 if abs(exposure) <= QTY_EPS else (1 if exposure > 0 else -1)
        if pred >= self._thr:
            want = 1
        elif pred <= -self._thr:
            want = -1
        elif current and current * pred > self._exit:
            want = current  # сигнал слабее порога входа, но позицию поддерживает
        else:
            want = 0
        if want == current:
            return ()

        target = want * self._fraction * ctx.equity / price
        delta = target - exposure
        if abs(delta) <= QTY_EPS:
            return ()
        return (OrderRequest(ctx.symbol, Side.BUY if delta > 0 else Side.SELL, abs(delta)),)
