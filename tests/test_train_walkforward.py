"""scripts/train_walkforward: модель не использует информацию теста при обучении, стратегия - будущее.

Главный приём - ОТРАВЛЕНИЕ СЫРЫХ ДАННЫХ (tests/synthetic.poison_from): с выбранного момента свечи и стакан
заменяются правдоподобным мусором, конвейер строится заново от сырья (водяной знак -> сетка решений ->
признаки -> метки -> фолды -> обучение -> движок). Результат, который не имеет права зависеть от этих данных,
обязан не измениться: если он изменился - в цепочке есть утечка. Негативный контроль показывает, что тест
способен её обнаружить: обучение БЕЗ purge на тех же данных реагирует на отравление.

Что и на каких данных проверяется:
  * признаки прошлых строк не меняются при отравлении будущего (каузальность всей цепочки);
  * кадр обучения и коэффициенты модели фолда k не меняются при отравлении данных с начала теста фолда k;
  * то, что видит fit(): строки строго до теста с зазором >= горизонта метки, метки заканчиваются до теста;
  * торговля фолда k не меняется при отравлении всего, что после его теста;
  * отчёт согласован: эквити непрерывна, окна тестов не пересекаются, PnL сходится.
"""

import math

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import config
from backtester.contracts import FundingCoverageError
from backtester.ml_strategy import RidgeRegressor
from execution.live_features import LiveFeatureFeed
from pipeline.features.microstructure import default_features
from pipeline.labels.base import LabelBuilder
from pipeline.loaders.schemas import FUNDING_SCHEMA
from pipeline.types import TimingConfig
from scripts.train_walkforward import (
    FrameFundingFeed,
    WalkForwardConfig,
    build_dataset,
    fit_final_model,
    format_report,
    main,
    walk_forward,
)
from tests.synthetic import MIN, NoFunding, poison_from, synthetic_market

TIMING = TimingConfig()
CFG = WalkForwardConfig(horizon_bars=5, n_splits=3, test_size=100, min_train_size=300, threshold=0.001)
HORIZON = CFG.horizon_bars


def dataset(candles, book):
    return build_dataset(
        candles, book, features=default_features(taker_flow=True), horizon_bars=HORIZON, timing=TIMING
    )


class Recording(RidgeRegressor):
    """Ridge, который запоминает кадр, на котором его обучили."""

    fit_frame: pl.DataFrame

    def fit(self, frame, feature_names, target):
        self.fit_frame = frame
        return super().fit(frame, feature_names, target)


def run(ds, book, cfg=CFG):
    """(отчёт, модели по фолдам) - модели нужны, чтобы сравнить кадры обучения и коэффициенты."""
    models: list[Recording] = []

    def factory():
        models.append(Recording(cfg.alpha))
        return models[-1]

    return walk_forward(ds, book, NoFunding(), cfg, factory), models


@pytest.fixture(scope="module")
def market():
    return synthetic_market(900, seed=11)


@pytest.fixture(scope="module")
def clean(market):
    candles, book = market
    ds = dataset(candles, book)
    report, models = run(ds, book)
    return ds, report, models


def open_time_at(ds, asof_ts: int) -> int:
    return int(ds.decisions.filter(pl.col("asof_ts") == asof_ts)["open_time"][0])


# ---------------------------------------------------------------------------- датасет


def test_dataset_has_features_without_labels_and_with_warmup_nulls(clean):
    ds, _, _ = clean
    assert not [c for c in ds.decisions.columns if c.startswith("y_")]  # метки живут отдельным кадром
    assert ds.labels.columns == ["symbol", "asof_ts", ds.label.name, ds.label.end_name]
    assert {"obi_1", "spread_bps", "microprice_dev_bps", "ofi_norm_5", "vol_rel_5", "taker_imb_5", "ret_5"} <= set(ds.model_features)
    assert "spread" not in ds.model_features and "spread" in ds.all_features
    assert ds.decisions["book_ok"].all() and ds.decisions["obi_1"].is_between(-1, 1).all()
    assert (ds.decisions["spread"] > 0).all()
    assert ds.lookback_bars == 119 and ds.decisions["vol_rel_30"].null_count() == 119  # прогрев - null, а не ноль
    assert (ds.decisions["src_max_ts"] < ds.decisions["asof_ts"]).all()


def test_labels_use_the_future_and_features_do_not(clean):
    ds, _, _ = clean
    # метка строки k - доходность до закрытия свечи k + horizon: смотрит вперёд ровно на горизонт
    px = ds.decisions["close"].to_list()
    label = ds.labels[ds.label.name].to_list()
    assert label[10] == pytest.approx(math.log(px[15] / px[10]))
    assert label[-HORIZON:] == [None] * HORIZON  # хвост: будущего ещё нет
    ends = ds.labels[ds.label.end_name].to_list()
    asof = ds.decisions["asof_ts"].to_list()
    assert ends[10] == asof[15] and all(e > a for e, a in zip(ends[:-HORIZON], asof))


def test_features_of_past_rows_ignore_poisoned_future(market, clean):
    ds, _, _ = clean
    candles, book = market
    cut = int(ds.decisions["asof_ts"][500])
    p_candles, p_book = poison_from(candles, book, from_open=open_time_at(ds, cut), from_recv=cut)
    poisoned = dataset(p_candles, p_book)
    before = pl.col("asof_ts") < cut
    assert_frame_equal(ds.decisions.filter(before), poisoned.decisions.filter(before), check_exact=True)
    # контроль силы теста: после момента отравления признаки действительно другие
    assert not ds.decisions.filter(~before)["obi_1"].equals(poisoned.decisions.filter(~before)["obi_1"])


# ------------------------------------------------------------- что видит fit() и модель


def test_fit_sees_only_train_fold_with_purge_gap(clean):
    ds, report, models = clean
    assert len(models) == len(report.folds) == CFG.n_splits
    axis = sorted(ds.decisions["asof_ts"].unique().to_list())
    pos = {t: i for i, t in enumerate(axis)}
    end_col = ds.label.end_name
    for fold, model in zip(report.folds, models):
        fit = model.fit_frame
        assert fit.height == fold.n_train_rows and fit[ds.label.name].null_count() == 0
        test_ts = set(ds.decisions.filter(pl.col("asof_ts").is_between(fold.test_first_ts, fold.test_last_ts))["asof_ts"])
        assert test_ts and not test_ts & set(fit["asof_ts"])  # ни одной тестовой строки в обучении
        assert fit["asof_ts"].max() < fold.test_first_ts
        # зазор: между последней обучающей строкой и первой тестовой >= горизонта метки
        assert pos[fold.test_first_ts] - pos[int(fit["asof_ts"].max())] - 1 >= HORIZON
        # метки обучающих строк заканчиваются до начала теста: цены теста в них не вошли
        assert fit[end_col].max() < fold.test_first_ts


def test_standardisation_statistics_come_from_the_train_fold_only(clean):
    ds, report, models = clean
    for fold, model in zip(report.folds, models):
        fit = model.fit_frame
        assert model.n_train == fit.height
        for i, name in enumerate(model.feature_names):
            assert model._mean[i] == pytest.approx(fit[name].mean())


def test_model_and_train_frame_ignore_data_from_the_test_window_onward(market, clean):
    ds, report, models = clean
    candles, book = market
    k = 1
    start = report.folds[k].test_first_ts
    p_candles, p_book = poison_from(candles, book, from_open=open_time_at(ds, start), from_recv=start)
    p_ds = dataset(p_candles, p_book)
    p_report, p_models = run(p_ds, p_book)

    for i in range(k + 1):  # фолды 0..k обучены до начала отравленных данных
        assert_frame_equal(models[i].fit_frame, p_models[i].fit_frame, check_exact=True)
        assert p_models[i].coef_ == models[i].coef_ and p_models[i].intercept_ == models[i].intercept_
        assert p_report.folds[i].n_train_rows == report.folds[i].n_train_rows
    # контроль силы: фолд k+1 обучается на отравленном тесте фолда k - его кадр обязан отличаться
    assert len(p_models) > k + 1
    assert not models[k + 1].fit_frame[ds.label.name].equals(p_models[k + 1].fit_frame[ds.label.name])
    # а торговля фолда k на отравленном тесте - другая (иначе отравление ничего не проверяло)
    assert p_report.folds[k].end_equity != report.folds[k].end_equity


def test_negative_control_training_without_purge_reacts_to_poisoned_test(market, clean):
    """Без purge последние обучающие метки содержат цены теста: коэффициенты зависят от отравления.

    Это тот же тест, что выше, но с «наивным» разбиением; если бы он проходил и здесь, проверка утечки
    была бы слепой.
    """
    ds, report, _ = clean
    candles, book = market
    start = report.folds[1].test_first_ts
    p_candles, p_book = poison_from(candles, book, from_open=open_time_at(ds, start), from_recv=start)
    p_ds = dataset(p_candles, p_book)

    labeler = LabelBuilder([ds.label])

    def naive_fit(d):
        labeled = labeler.attach(d.decisions.lazy(), d.labels.lazy()).collect()
        train = labeled.filter(pl.col("asof_ts") < start).drop_nulls([ds.label.name, *d.model_features])
        return RidgeRegressor(CFG.alpha).fit(train, d.model_features, ds.label.name)

    clean_coef, poisoned_coef = naive_fit(ds).coef_, naive_fit(p_ds).coef_
    assert any(abs(clean_coef[n] - poisoned_coef[n]) > 1e-6 * (abs(clean_coef[n]) + 1e-12) for n in clean_coef)


def test_trading_of_a_fold_ignores_everything_after_its_test_window(market, clean):
    ds, report, _ = clean
    candles, book = market
    last = report.folds[0].test_last_ts
    # движок фолда читает стакан до last + 2 интервалов: отравляем строго после
    p_candles, p_book = poison_from(
        candles, book, from_open=open_time_at(ds, last) + MIN, from_recv=last + 3 * MIN
    )
    p_report, _ = run(dataset(p_candles, p_book), p_book)

    a, b = report.folds[0], p_report.folds[0]
    assert (a.start_equity, a.end_equity, a.n_orders, a.fees, a.max_drawdown) == \
        (b.start_equity, b.end_equity, b.n_orders, b.fees, b.max_drawdown)
    assert_frame_equal(
        report.equity_curve.filter(pl.col("fold") == 0), p_report.equity_curve.filter(pl.col("fold") == 0),
        check_exact=True,
    )
    # контроль силы: на следующих фолдах отравленные данные уже влияют
    assert p_report.folds[1].end_equity != report.folds[1].end_equity


# ------------------------------------------------------------------------ отчёт


def test_report_is_consistent(clean):
    ds, report, _ = clean
    folds = report.folds
    assert folds[0].start_equity == CFG.initial_equity
    for prev, nxt in zip(folds, folds[1:]):
        assert nxt.start_equity == prev.end_equity  # капитал переходит из фолда в фолд
        assert prev.test_last_ts < nxt.test_first_ts  # окна тестов не пересекаются
        assert prev.test_first_ts < nxt.test_first_ts
    for f in folds:
        assert f.train_last_ts < f.test_first_ts

    curve = report.equity_curve
    assert curve["fold"].unique(maintain_order=True).to_list() == [f.index for f in folds]
    assert (curve["ts"].diff().drop_nulls() >= 0).all()  # время не идёт назад, в том числе на стыках фолдов
    for f in folds:  # кривая фолда начинается со стартового капитала и заканчивается его итогом
        part = curve.filter(pl.col("fold") == f.index)["equity"]
        assert part[0] == pytest.approx(f.start_equity) and part[-1] == pytest.approx(f.end_equity)
    s = report.summary()
    assert s["end_equity"] == pytest.approx(curve["equity"][-1])
    assert s["end_equity"] - CFG.initial_equity == pytest.approx(sum(f.pnl for f in folds))
    assert s["total_return"] == pytest.approx(s["end_equity"] / CFG.initial_equity - 1)
    assert 0 <= s["max_drawdown"] < 1


def test_model_finds_the_planted_signal_out_of_sample(clean):
    """Проверка проводки обучения, а не торговой идеи: на синтетике связь стакана с доходностью есть по построению."""
    _, report, _ = clean
    assert all(f.ic_oos is not None and f.ic_oos > 0.3 for f in report.folds)
    assert all(f.hit_rate_oos > 0.6 for f in report.folds)
    assert sum(f.n_orders for f in report.folds) > 0
    text = format_report(report)
    assert "ИТОГО OOS" in text and text.count("\n") >= len(report.folds) + 3


def test_no_trades_above_a_huge_threshold(market, clean):
    ds, _, _ = clean
    cfg = WalkForwardConfig(horizon_bars=5, n_splits=3, test_size=100, min_train_size=300, threshold=0.9)
    report, _ = run(ds, market[1], cfg)
    assert all(f.n_orders == 0 and f.end_equity == cfg.initial_equity and f.fees == 0 for f in report.folds)


def test_insufficient_history_is_an_error_not_a_skipped_fold(market, clean):
    ds, _, _ = clean
    too_long = WalkForwardConfig(horizon_bars=5, n_splits=3, test_size=400, min_train_size=300)
    with pytest.raises(Exception, match="min_train_size|не помещаются"):
        run(ds, market[1], too_long)


def test_purge_shorter_than_label_horizon_is_refused(market, clean):
    ds, _, _ = clean
    with pytest.raises(Exception, match="purge_bars"):
        run(ds, market[1], WalkForwardConfig(horizon_bars=5, n_splits=3, test_size=100, min_train_size=300, purge_bars=2))


# ---------------------------------------------------------------------- фандинг-фид

H8 = 8 * 3_600_000
B = 10 * H8  # база времён: FundingRecord требует funding_time > 0


def feed(times, marks=None, symbol="BTCUSDT"):
    marks = marks or [100.0] * len(times)
    df = pl.DataFrame(
        {"symbol": [symbol] * len(times), "funding_time": times, "funding_rate": [0.0001] * len(times), "mark_price": marks},
        schema={"symbol": pl.String, **FUNDING_SCHEMA},
    )
    return FrameFundingFeed(df, H8)


def test_funding_feed_returns_settlements_in_window():
    f = feed([B + k * H8 for k in range(5)])
    got = f.settlements("BTCUSDT", B + H8 - 1, B + 3 * H8)
    assert [r.funding_time for r in got] == [B + H8, B + 2 * H8]
    assert got[0].funding_rate == 0.0001 and got[0].mark_price == 100.0
    assert f.settlements("BTCUSDT", B + 5, B + 10) == []  # окно между списаниями, но в зоне покрытия


def test_funding_feed_refuses_windows_it_knows_nothing_about():
    f = feed([B, B + H8, B + 2 * H8])
    with pytest.raises(FundingCoverageError, match="фандинг известен"):
        f.settlements("BTCUSDT", B, B + 3 * H8 + 1)  # дальше последней записи + интервал
    with pytest.raises(FundingCoverageError, match="фандинг известен"):
        f.settlements("BTCUSDT", B - H8 - 1, B + 10)
    with pytest.raises(FundingCoverageError, match="Нет данных"):
        f.settlements("ETHUSDT", B, B + 10)
    f.settlements("BTCUSDT", B - H8, B + 3 * H8)  # границы покрытия включены


def test_funding_feed_detects_missing_settlements_and_missing_mark():
    hole = feed([B, B + H8, B + 4 * H8, B + 5 * H8])  # пропущены B+2*H8 и B+3*H8
    hole.settlements("BTCUSDT", B, B + 2 * H8)  # до дыры - можно
    with pytest.raises(FundingCoverageError, match="пропущено"):
        hole.settlements("BTCUSDT", B + 2 * H8, B + 3 * H8)
    with pytest.raises(FundingCoverageError, match="пропущено"):
        hole.settlements("BTCUSDT", B + H8, B + 5 * H8)
    no_mark = feed([B, B + H8, B + 2 * H8], marks=[100.0, None, 100.0])
    with pytest.raises(FundingCoverageError, match="mark_price"):
        no_mark.settlements("BTCUSDT", B, B + 2 * H8)

# ---------------------------------------------------------------- сквозной запуск с диска


def write_to_disk(candles, book, start_ms):
    """Кладёт синтетику в формат Этапа 1: свечи, нормализованный стакан (interim), фандинг."""
    symbol = "BTCUSDT"
    (config.KLINES_DIR / symbol / "1m").mkdir(parents=True)
    candles.drop("symbol").write_parquet(config.KLINES_DIR / symbol / "1m" / "2025-01.parquet")
    (config.DEPTH_INTERIM_DIR / symbol).mkdir(parents=True)
    book.write_parquet(config.DEPTH_INTERIM_DIR / symbol / "2025-01-06.parquet")
    (config.FUNDING_DIR / symbol).mkdir(parents=True)
    times = [start_ms + k * H8 for k in range(-2, 3)]
    pl.DataFrame(
        {"funding_time": times, "funding_rate": [0.0001] * 5, "mark_price": [100.0] * 5}, schema=FUNDING_SCHEMA
    ).write_parquet(config.FUNDING_DIR / symbol / "2025.parquet")


@pytest.fixture
def disk(sandbox, monkeypatch):
    monkeypatch.setattr(config, "FUNDING_DIR", sandbox / "funding")
    monkeypatch.setattr(config, "DEPTH_INTERIM_DIR", sandbox / "interim_depth")
    monkeypatch.setattr(config, "DEPTH_LEVELS", 5)
    start_ms = 1_736_121_600_000  # 2025-01-06 00:00:00 UTC
    candles, book = synthetic_market(900, seed=5, start_ms=start_ms)
    write_to_disk(candles, book, start_ms)
    return sandbox


ARGS = ["--symbols", "BTCUSDT", "--start", "2025-01-06", "--end", "2025-01-07",
        "--splits", "3", "--test-bars", "100", "--min-train-bars", "300"]


def test_main_runs_the_whole_pipeline_from_disk(disk, capsys):
    out = disk / "out"
    assert main([*ARGS, "--out-dir", str(out)]) == 0
    printed = capsys.readouterr().out
    assert "ИТОГО OOS" in printed and printed.count(" | ") >= 3 * 8

    curve = pl.read_csv(out / "equity_curve.csv")
    folds = pl.read_csv(out / "folds.csv")
    assert folds.height == 3 and curve["fold"].n_unique() == 3
    assert folds["start_equity"][0] == 10_000.0
    assert folds["start_equity"][1:].to_list() == pytest.approx(folds["end_equity"][:-1].to_list())
    assert (folds["train_last_ts"] < folds["test_first_ts"]).all()


def test_final_model_uses_every_labelled_row_and_no_unlabelled_tail(clean):
    ds, report, _ = clean
    model = fit_final_model(ds, CFG)
    labeled = LabelBuilder([ds.label]).attach(ds.decisions.lazy(), ds.labels.lazy()).collect()
    usable = labeled.drop_nulls([ds.label.name, *ds.model_features])

    assert model.feature_names == ds.model_features and model.alpha == CFG.alpha
    assert model.n_train == usable.height < labeled.height  # прогрев и хвост без меток отброшены
    assert labeled.height - labeled[ds.label.name].null_count() >= usable.height
    assert model.n_train > max(f.n_train_rows for f in report.folds)  # больше любого фолда: обучена на всём
    # хвост меток (будущего ещё нет) в обучение не попал: статистики берутся по строкам с известной меткой
    for i, name in enumerate(model.feature_names):
        assert model._mean[i] == pytest.approx(usable[name].mean())


def test_main_saves_a_final_model_that_the_live_side_can_load(disk, tmp_path):
    path = tmp_path / "models" / "ridge.json"
    assert main([*ARGS, "--save-model", str(path)]) == 0

    model = RidgeRegressor.load_json(path)
    assert model.is_fitted and "obi_1" in model.feature_names and model.n_train > 500
    # признаки модели обязан уметь считать боевой фид: иначе live_run не стартует
    LiveFeatureFeed(None, ("BTCUSDT",), required_features=model.feature_names)  # type: ignore[arg-type]


def test_main_without_save_model_writes_no_model(disk, tmp_path):
    assert main(ARGS) == 0
    assert not list(tmp_path.rglob("*.json"))


def test_main_reports_errors_through_exit_codes(disk, capsys):
    assert main(["--symbols", "ETHUSDT", "--start", "2025-01-06", "--end", "2025-01-07"]) == 1  # данных нет
    assert main(["--symbols", "BTCUSDT", "--start", "2025-01-06", "--end", "2025-01-06"]) == 3  # пустой диапазон
    assert main(["--symbols", "BTCUSDT", "--start", "06.01.2025", "--end", "2025-01-07"]) == 3  # формат даты
    assert main([*ARGS, "--alpha", "0"]) == 3  # конфигурация модели
