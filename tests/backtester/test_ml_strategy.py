"""MLModelStrategy и RidgeRegressor: логика порогов, учёт позиции, строгость обучения (ожидаемое посчитано вручную)."""

import json
import math
import random
from types import MappingProxyType

import polars as pl
import pytest

from backtester.contracts import Side
from backtester.ml_strategy import FeatureModel, MLModelStrategy, ModelError, RidgeRegressor
from backtester.risk import OrderRequest
from backtester.strategy import FeatureContext, StrategyError
from utils.errors import ConfigError

BTC = "BTCUSDT"
Y = "y_fwd_log_ret_300000ms"


class StubModel(FeatureModel):
    """Прогноз = значение первого признака; запоминает, что ему передали."""

    def __init__(self, names=("sig",), fitted=True):
        self._names, self._fitted, self.calls = tuple(names), fitted, []

    is_fitted = property(lambda self: self._fitted)
    feature_names = property(lambda self: self._names)

    def fit(self, frame, feature_names, target):
        raise NotImplementedError

    def predict_row(self, values):
        self.calls.append(tuple(values))
        return values[0]


def ctx(sig=0.002, *, position=0.0, pending=0.0, equity=10_000.0, price=100.0, extra=None):
    row = {"sig": sig, **(extra or {})}
    return FeatureContext(
        ts=1_000, symbol=BTC, row=MappingProxyType(row), equity=equity,
        positions=MappingProxyType({BTC: position} if position else {}),
        pending=MappingProxyType({BTC: pending} if pending else {}),
        marks=MappingProxyType({BTC: price} if price is not None else {}),
    )


def decide(c, model=None, **kw):
    kw.setdefault("threshold", 0.001)
    return list(MLModelStrategy(model or StubModel(), **kw).decide(c))


def req(side, qty):
    return OrderRequest(BTC, side, qty)


# ------------------------------------------------------------------------- пороги


def test_enters_long_and_short_beyond_threshold():
    # 10% от 10_000 при цене 100 = 10
    assert decide(ctx(0.002)) == [req(Side.BUY, 10.0)]
    assert decide(ctx(-0.002)) == [req(Side.SELL, 10.0)]
    assert decide(ctx(0.001)) == [req(Side.BUY, 10.0)]  # порог включительно
    assert decide(ctx(-0.001)) == [req(Side.SELL, 10.0)]


def test_no_entry_inside_threshold_band():
    assert decide(ctx(0.00099)) == []
    assert decide(ctx(-0.00099)) == []
    assert decide(ctx(0.0)) == []


def test_holds_position_while_signal_keeps_its_sign():
    assert decide(ctx(0.0004, position=10.0)) == []  # слабее порога входа, но знак верный
    assert decide(ctx(-0.0004, position=-10.0)) == []
    assert decide(ctx(0.002, position=10.0)) == []  # сильный сигнал в сторону позиции: размер не пересчитывается


def test_flattens_when_signal_loses_its_sign():
    assert decide(ctx(-0.0004, position=10.0)) == [req(Side.SELL, 10.0)]
    assert decide(ctx(0.0004, position=-10.0)) == [req(Side.BUY, 10.0)]
    assert decide(ctx(0.0, position=10.0)) == [req(Side.SELL, 10.0)]  # ровно ноль позицию не держит


def test_exit_threshold_gives_hysteresis():
    kw = {"exit_threshold": 0.0005}
    assert decide(ctx(0.0006, position=10.0), **kw) == []  # выше выхода - держим
    assert decide(ctx(0.0004, position=10.0), **kw) == [req(Side.SELL, 10.0)]  # выдохся - закрываем
    assert decide(ctx(-0.0006, position=-10.0), **kw) == []
    assert decide(ctx(-0.0004, position=-10.0), **kw) == [req(Side.BUY, 10.0)]


def test_strong_opposite_signal_reverses_in_one_order():
    assert decide(ctx(-0.002, position=10.0)) == [req(Side.SELL, 20.0)]  # 10 закрыть + 10 открыть
    assert decide(ctx(0.002, position=-10.0)) == [req(Side.BUY, 20.0)]


def test_order_in_flight_counts_as_exposure():
    assert decide(ctx(0.002, pending=10.0)) == []  # уже летит нужная покупка
    assert decide(ctx(-0.0004, pending=10.0)) == [req(Side.SELL, 10.0)]  # отменяем ожидаемую позицию
    assert decide(ctx(0.002, position=4.0, pending=-4.0)) == [req(Side.BUY, 10.0)]  # экспозиция 0 -> набрать целевую


def test_size_scales_with_equity_and_price_and_fraction():
    assert decide(ctx(0.002, equity=20_000.0, price=200.0), equity_fraction=0.5) == [req(Side.BUY, 50.0)]


# --------------------------------------------------------------------- нет данных


def test_missing_values_mean_no_action_even_with_position():
    assert decide(ctx(None)) == []
    assert decide(ctx(None, position=10.0)) == []  # устаревший стакан: ничего не делаем, как BaselineStrategy
    assert decide(ctx(float("nan"))) == []
    assert decide(ctx(0.002, price=None)) == []
    assert decide(ctx(0.002, price=0.0)) == []


def test_missing_column_is_configuration_error_not_silence():
    with pytest.raises(StrategyError, match="нет признака"):
        decide(ctx(0.002), StubModel(("sig", "absent")))


def test_features_reach_the_model_in_its_own_order():
    model = StubModel(("b", "sig", "a"))
    decide(ctx(0.002, extra={"a": 1.0, "b": 2.0}), model)
    assert model.calls == [(2.0, 0.002, 1.0)]


def test_non_finite_prediction_is_ignored():
    class Broken(StubModel):
        def predict_row(self, values):
            return float("nan")

    assert decide(ctx(0.002), Broken()) == []


# --------------------------------------------------------------------- конфигурация


def test_strategy_refuses_untrained_or_foreign_model():
    with pytest.raises(ConfigError, match="не обучена"):
        MLModelStrategy(StubModel(fitted=False), threshold=0.001)
    with pytest.raises(ConfigError):
        MLModelStrategy(object(), threshold=0.001)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        MLModelStrategy(RidgeRegressor(), threshold=0.001)  # Ridge без fit() - тоже не обучен


@pytest.mark.parametrize("kw", [
    {"threshold": 0}, {"threshold": -0.1}, {"threshold": float("nan")}, {"threshold": True}, {"threshold": None},
    {"threshold": 0.001, "exit_threshold": -0.1}, {"threshold": 0.001, "exit_threshold": 0.001},
    {"threshold": 0.001, "exit_threshold": 0.002}, {"threshold": 0.001, "equity_fraction": 0},
    {"threshold": 0.001, "equity_fraction": float("inf")},
])
def test_parameter_validation(kw):
    with pytest.raises(ConfigError):
        MLModelStrategy(StubModel(), **kw)


# ------------------------------------------------------------------------- Ridge


def training_frame(n=400, seed=1, *, noise=0.0, extra_cols=None) -> pl.DataFrame:
    """y = 0.5 + 2*x1 - 3*x2 (+ шум); x1, x2 независимы."""
    rng = random.Random(seed)
    x1 = [rng.gauss(0, 1) for _ in range(n)]
    x2 = [rng.gauss(5, 3) for _ in range(n)]
    y = [0.5 + 2 * a - 3 * b + rng.gauss(0, noise) for a, b in zip(x1, x2)]
    return pl.DataFrame({"x1": x1, "x2": x2, Y: y, **(extra_cols or {})})


def test_ridge_recovers_a_linear_relation():
    model = RidgeRegressor(alpha=1e-12).fit(training_frame(), ["x1", "x2"], Y)
    assert model.coef_["x1"] == pytest.approx(2.0, rel=1e-6)
    assert model.coef_["x2"] == pytest.approx(-3.0, rel=1e-6)
    assert model.intercept_ == pytest.approx(0.5, abs=1e-5)
    assert model.predict_row([1.0, 1.0]) == pytest.approx(0.5 + 2 - 3, abs=1e-5)


def test_ridge_matches_an_independent_closed_form_for_one_feature():
    x, y = [1.0, 2.0, 3.0, 4.0, 5.0], [2.0, 4.0, 5.0, 4.0, 6.0]
    n, alpha = 5, 0.1
    mx, my = sum(x) / n, sum(y) / n
    sd = math.sqrt(sum((v - mx) ** 2 for v in x) / n)
    z = [(v - mx) / sd for v in x]
    w = sum(zi * (yi - my) for zi, yi in zip(z, y)) / (sum(zi * zi for zi in z) + alpha * n)  # одномерная формула
    model = RidgeRegressor(alpha=alpha).fit(pl.DataFrame({"x": x, Y: y}), ["x"], Y)
    for probe in (0.0, 3.0, 10.0):
        assert model.predict_row([probe]) == pytest.approx(my + w * (probe - mx) / sd)


def test_stronger_alpha_shrinks_towards_the_mean():
    df = training_frame(noise=1.0)
    weak = RidgeRegressor(alpha=1e-6).fit(df, ["x1", "x2"], Y)
    strong = RidgeRegressor(alpha=50.0).fit(df, ["x1", "x2"], Y)
    assert abs(strong.coef_["x1"]) < abs(weak.coef_["x1"]) and abs(strong.coef_["x2"]) < abs(weak.coef_["x2"])
    assert strong.predict_row([0.0, 5.0]) == pytest.approx(df[Y].mean(), abs=0.5)


def test_penalty_per_observation_makes_alpha_independent_of_sample_size():
    small = RidgeRegressor(alpha=0.5).fit(training_frame(n=200, seed=3, noise=1.0), ["x1", "x2"], Y)
    big = RidgeRegressor(alpha=0.5).fit(training_frame(n=4000, seed=3, noise=1.0), ["x1", "x2"], Y)
    assert big.coef_["x1"] == pytest.approx(small.coef_["x1"], rel=0.25)  # одна сила регуляризации на разной длине


def test_standardisation_makes_predictions_scale_invariant():
    df = training_frame(noise=0.5)
    a = RidgeRegressor(alpha=0.1).fit(df, ["x1", "x2"], Y)
    scaled = df.with_columns(pl.col("x1") * 1000.0, (pl.col("x2") + 7.0) * 0.001)
    b = RidgeRegressor(alpha=0.1).fit(scaled, ["x1", "x2"], Y)
    probe = df.row(10)
    assert b.predict_row([probe[0] * 1000.0, (probe[1] + 7.0) * 0.001]) == pytest.approx(a.predict_row(list(probe[:2])))


def test_constant_feature_gets_zero_weight():
    df = training_frame(noise=0.1).with_columns(const=pl.lit(7.0))
    model = RidgeRegressor(alpha=0.1).fit(df, ["x1", "x2", "const"], Y)
    assert model.coef_["const"] == 0.0
    assert model.predict_row([0.0, 5.0, 7.0]) == pytest.approx(model.predict_row([0.0, 5.0, 123.0]))


def test_fit_is_deterministic():
    df = training_frame(noise=0.5)
    a = RidgeRegressor(0.3).fit(df, ["x1", "x2"], Y)
    b = RidgeRegressor(0.3).fit(df, ["x1", "x2"], Y)
    assert a.coef_ == b.coef_ and a.intercept_ == b.intercept_


def test_predict_does_not_change_the_model():
    df = training_frame(noise=0.5)
    model = RidgeRegressor(0.3).fit(df, ["x1", "x2"], Y)
    before = (model.coef_, model.intercept_, model.n_train)
    junk = pl.DataFrame({"x1": [1e9, -1e9], "x2": [3.0, 4.0]})
    preds = model.predict(junk)
    assert (model.coef_, model.intercept_, model.n_train) == before  # применение к новым данным ничего не «доучивает»
    assert preds.to_list() == [model.predict_row([1e9, 3.0]), model.predict_row([-1e9, 4.0])]


def test_fit_refuses_leaky_columns_and_dirty_data():
    df = training_frame()
    with pytest.raises(ModelError, match="Признаками нельзя"):
        RidgeRegressor().fit(df.with_columns(y_other=pl.col(Y) * 2), ["x1", "y_other"], Y)
    with pytest.raises(ModelError, match="Признаками нельзя"):
        RidgeRegressor().fit(df.with_columns(avail_ts=pl.lit(1)), ["x1", "avail_ts"], Y)
    with pytest.raises(ModelError, match="меткой LabelBuilder"):
        RidgeRegressor().fit(df.rename({Y: "target"}), ["x1", "x2"], "target")
    with pytest.raises(ModelError, match="Признаками нельзя"):
        RidgeRegressor().fit(df, ["x1", Y], Y)  # сама цель среди признаков - тоже метка
    with pytest.raises(ModelError, match="нет колонок"):
        RidgeRegressor().fit(df, ["x1", "missing"], Y)
    with pytest.raises(ModelError, match="null/NaN/inf"):
        RidgeRegressor().fit(df.with_columns(pl.when(pl.int_range(pl.len()) == 3).then(None).otherwise(pl.col("x1")).alias("x1")),
                             ["x1", "x2"], Y)
    with pytest.raises(ModelError, match="null/NaN/inf"):
        RidgeRegressor().fit(df.with_columns(pl.when(pl.int_range(pl.len()) == 3).then(float("nan")).otherwise(pl.col(Y)).alias(Y)),
                             ["x1", "x2"], Y)
    with pytest.raises(ModelError, match="Мало строк"):
        RidgeRegressor().fit(df.head(3), ["x1", "x2"], Y)
    with pytest.raises(ModelError):
        RidgeRegressor().fit(df, ["x1", "x1"], Y)


def test_predict_row_validation_and_unfitted_model():
    with pytest.raises(ModelError, match="не обучена"):
        RidgeRegressor().predict_row([1.0])
    with pytest.raises(ModelError, match="не обучена"):
        RidgeRegressor().predict(pl.DataFrame({"x1": [1.0]}))
    model = RidgeRegressor().fit(training_frame(), ["x1", "x2"], Y)
    with pytest.raises(ModelError, match="Ожидалось 2"):
        model.predict_row([1.0])
    for bad in (float("nan"), float("inf"), True):
        with pytest.raises(ModelError):
            model.predict_row([bad, 1.0])


@pytest.mark.parametrize("alpha", [0, -1.0, float("nan"), float("inf"), True, None])
def test_alpha_validation(alpha):
    with pytest.raises(ConfigError):
        RidgeRegressor(alpha)


# ------------------------------------------------------------------- сериализация


def fitted(alpha=0.3) -> RidgeRegressor:
    return RidgeRegressor(alpha).fit(training_frame(noise=0.5), ["x1", "x2"], Y)


def test_json_roundtrip_is_bit_exact(tmp_path):
    model = fitted()
    path = tmp_path / "sub" / "model.json"  # каталог создаётся
    model.save_json(path)
    loaded = RidgeRegressor.load_json(path)

    assert (loaded._names, loaded._mean, loaded._scale, loaded._weights, loaded._y_mean) == \
        (model._names, model._mean, model._scale, model._weights, model._y_mean)
    assert loaded.is_fitted and loaded.feature_names == ("x1", "x2") and (loaded.alpha, loaded.n_train) == (0.3, 400)
    for row in ([0.0, 0.0], [1.5, -2.0], [1e-9, 1e9]):  # прогнозы совпадают точно, а не приблизительно
        assert loaded.predict_row(row) == model.predict_row(row)
    assert loaded.coef_ == model.coef_ and loaded.intercept_ == model.intercept_


def test_loaded_model_drives_the_strategy_like_the_original(tmp_path):
    model = fitted()
    model.save_json(tmp_path / "m.json")
    loaded = RidgeRegressor.load_json(tmp_path / "m.json")
    c = FeatureContext(ts=1, symbol=BTC, row=MappingProxyType({"x1": 3.0, "x2": -1.0}), equity=10_000.0,
                       positions=MappingProxyType({}), pending=MappingProxyType({}),
                       marks=MappingProxyType({BTC: 100.0}))
    assert decide(c, loaded, threshold=0.001) == decide(c, model, threshold=0.001) != []


def test_file_contains_only_the_trained_state_not_the_training_data(tmp_path):
    model = fitted()
    model.save_json(tmp_path / "m.json")
    raw = json.loads((tmp_path / "m.json").read_text(encoding="utf-8"))
    assert set(raw) == {"format", "alpha", "n_train", "names", "mean", "scale", "weights", "y_mean"}
    assert raw["names"] == ["x1", "x2"] and len(raw["mean"]) == len(raw["scale"]) == len(raw["weights"]) == 2
    assert (tmp_path / "m.json").stat().st_size < 2_000  # 400 строк обучения в файл не попали
    assert not list(tmp_path.glob("*.tmp"))  # временный файл атомарной записи убран


def test_save_overwrites_atomically_and_refuses_unfitted(tmp_path):
    path = tmp_path / "m.json"
    fitted(0.3).save_json(path)
    other = RidgeRegressor(2.0).fit(training_frame(seed=9), ["x1", "x2"], Y)
    other.save_json(path)
    assert RidgeRegressor.load_json(path).alpha == 2.0
    with pytest.raises(ModelError, match="не обучена"):
        RidgeRegressor().save_json(tmp_path / "x.json")
    assert not (tmp_path / "x.json").exists()


def test_save_refuses_non_finite_state_and_keeps_the_old_file(tmp_path):
    path = tmp_path / "m.json"
    model = fitted()
    model.save_json(path)
    before = path.read_text(encoding="utf-8")
    model._weights[0] = float("nan")
    with pytest.raises(ModelError, match="NaN/inf"):
        model.save_json(path)
    assert path.read_text(encoding="utf-8") == before  # битая модель не затёрла рабочую


def good_payload(tmp_path) -> dict:
    fitted().save_json(tmp_path / "good.json")
    return json.loads((tmp_path / "good.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(format="ridge-regressor/2"),
    lambda p: p.pop("format"),
    lambda p: p.update(names=[]),
    lambda p: p.update(names=["x1", "x1"]),
    lambda p: p.update(names=["x1", 5]),
    lambda p: p.update(names=["x1", "y_fwd_log_ret_300000ms"]),  # утечка метки через имя
    lambda p: p.update(names=["x1", "avail_ts"]),
    lambda p: p.update(mean=[0.0]),  # длина не совпадает с names
    lambda p: p.update(weights=[1.0, "w"]),
    lambda p: p.update(weights=[1.0, None]),
    lambda p: p.update(scale=[1.0, 0.0]),  # деление на ноль при предсказании
    lambda p: p.update(scale=[1.0, -2.0]),
    lambda p: p.update(y_mean=True),
    lambda p: p.pop("y_mean"),
    lambda p: p.update(alpha=0),
    lambda p: p.update(n_train=-1),
])
def test_load_refuses_a_corrupted_model(tmp_path, mutate):
    payload = good_payload(tmp_path)
    mutate(payload)
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ModelError):
        RidgeRegressor.load_json(bad)


def test_load_refuses_unreadable_files(tmp_path):
    with pytest.raises(ModelError, match="Не удалось прочитать"):
        RidgeRegressor.load_json(tmp_path / "missing.json")
    for text in ("", "{оборвано", "[1, 2]", "null", '{"format": "ridge-regressor/1", "names": ["a"], "mean": [NaN]}'):
        bad = tmp_path / "bad.json"
        bad.write_text(text, encoding="utf-8")
        with pytest.raises(ModelError):
            RidgeRegressor.load_json(bad)
    (tmp_path / "bin.json").write_bytes(b"\xff\xfe\x00bad")
    with pytest.raises(ModelError):
        RidgeRegressor.load_json(tmp_path / "bin.json")
