"""Walk-forward обучение и OOS-бэктест MLModelStrategy.

    python -m scripts.train_walkforward --symbols BTCUSDT ETHUSDT --start 2025-01-01 --end 2025-03-01

Конвейер:
  1. load_market    свечи 1m, стакан (DataQualityGate), фандинг - с диска (результат сборщиков Этапа 1);
  2. build_dataset  водяной знак -> сетка решений -> агрегаты стакана -> признаки (microstructure) ->
                    ProvenanceGuard; метки LabelBuilder отдельным кадром;
  3. walk_forward   PurgedWalkForward: для каждого фолда модель обучается на train (после purge), затем
                    MLModelStrategy торгует test в BacktestEngine с настоящим OrderBookBarrier;
  4. кривые эквити фолдов сшиваются в одну: капитал переходит из фолда в фолд (compounding).
  5. --save-model: финальная модель обучается на ВСЕХ решениях с известной меткой и сохраняется в JSON
     (RidgeRegressor.save_json) для scripts/live_run.py --model-path. Её качество отчётом не измеряется.

Что гарантирует схема (и что проверяют tests/test_train_walkforward.py):
  * модель видит только train-фолд: строки до теста минус purge, и дополнительно строки, чья метка
    заканчивается (end_ts) не раньше начала теста. Признаки каузальны (FeatureBuilder), статистики
    стандартизации берутся из train-кадра;
  * движку отдаётся тестовый кадр БЕЗ меток и их end_ts; метки теста используются только после прогона,
    для диагностики (IC, hit rate);
  * каждый фолд - новый барьер и новый движок: фолд стартует с плоской позиции и заканчивается выкупом
    остатка по правилу конца данных (комиссии и фандинг учтены), так что стыки фолдов не несут позицию и
    эквити непрерывна;
  * параметры (alpha, threshold, горизонт) задаются заранее и НЕ подбираются по OOS внутри скрипта:
    подбор по тем же фолдах, на которых потом отчитываются метрики, - это утечка через выбор модели.

Ограничения: модель одна на все символы (pooled), безразмерные признаки; нет подбора гиперпараметров;
Sharpe в отчёте - по минутным точкам эквити, ориентир, а не оценка с доверительным интервалом.
"""

import argparse
import logging
import math
import sys
from bisect import bisect_left
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import config
from backtester.barrier import OrderBookBarrier
from backtester.contracts import ExecutionConfig, FundingCoverageError, FundingFeed, FundingRecord
from backtester.engine import BacktestEngine
from backtester.ml_strategy import FeatureModel, MLModelStrategy, RidgeRegressor
from backtester.risk import DrawdownLimitGuard, MaxExposureGuard
from pipeline.features.base import Feature, apply_features, compile_features, source_columns
from pipeline.features.microstructure import book_aggregations, book_levels, default_features, model_feature_names
from pipeline.guards.provenance import ProvenanceGuard
from pipeline.labels.base import LabelBuilder, LabelSpec
from pipeline.loaders.depth_loader import DepthLoader
from pipeline.loaders.funding_loader import FundingLoader
from pipeline.loaders.klines_loader import KlinesLoader
from pipeline.quality.gate import DataQualityGate
from pipeline.splits.walkforward import PurgedWalkForward, bars_for
from pipeline.sync.aligner import TimelineAligner
from pipeline.sync.watermark import WatermarkClock
from pipeline.types import TimingConfig
from utils.errors import EXIT_GENERAL, EXIT_OK, ConfigError, DataError, TradingInfraError
from utils.logger import setup_logging

log = logging.getLogger("train_walkforward")

INTERVAL = "1m"
BARS_PER_YEAR = 365 * 24 * 60


# ------------------------------------------------------------------------ фандинг


class FrameFundingFeed:
    """FundingFeed поверх загруженных ставок. Окно, о котором данные ничего не знают, - FundingCoverageError.

    Известно: окно [lo, hi) покрыто, если lo >= первая_запись - интервал, hi <= последняя_запись + интервал
    (следующее списание само попало бы в окно только при hi больше) и окно не пересекает «дыру» -
    пропуск между соседними записями длиннее 1.5 интервала (то же правило, что у FundingLoader).
    Запись без mark_price не даёт номинал списания - тоже ошибка покрытия, а не молчаливый пропуск.
    """

    def __init__(self, frame: pl.DataFrame, interval_ms: int):
        self._interval = interval_ms
        self._data: dict[str, tuple[list[int], list[tuple], list[tuple[int, int]]]] = {}
        parts = frame.sort("symbol", "funding_time").partition_by("symbol", as_dict=True, maintain_order=True)
        for (symbol,), part in parts.items():
            times = part["funding_time"].to_list()
            rows = part.select("funding_time", "funding_rate", "mark_price").rows()
            # дыра: от ближайшего ожидаемого списания после предыдущей записи до следующей известной
            holes = [
                (times[i - 1] + interval_ms, times[i])
                for i in range(1, len(times)) if times[i] - times[i - 1] > 1.5 * interval_ms
            ]
            self._data[symbol] = (times, rows, holes)

    def settlements(self, symbol: str, from_ts: int, to_ts: int) -> Sequence[FundingRecord]:
        if symbol not in self._data:
            raise FundingCoverageError(f"Нет данных фандинга для {symbol}")
        times, rows, holes = self._data[symbol]
        if from_ts < times[0] - self._interval or to_ts > times[-1] + self._interval:
            raise FundingCoverageError(
                f"{symbol}: фандинг известен в [{times[0] - self._interval}, {times[-1] + self._interval}], "
                f"запрошено [{from_ts}, {to_ts})"
            )
        for a, b in holes:
            if from_ts < b and to_ts > a:
                raise FundingCoverageError(f"{symbol}: в окне [{from_ts}, {to_ts}) пропущено списание ({a}..{b})")
        out = []
        for ts, rate, mark in rows[bisect_left(times, from_ts):bisect_left(times, to_ts)]:
            if mark is None:
                raise FundingCoverageError(f"{symbol}: у списания {ts} нет mark_price, номинал не определён")
            out.append(FundingRecord(symbol, ts, rate, mark))
        return out


# ----------------------------------------------------------------------- данные


@dataclass(frozen=True)
class Market:
    candles: pl.LazyFrame
    book: pl.DataFrame  # принятое DataQualityGate, по порядку (recv_ts, symbol, u)
    funding: FrameFundingFeed
    n_quarantined: int


def load_market(symbols: Sequence[str], start_ms: int, end_ms: int, timing: TimingConfig) -> Market:
    """Читает данные Этапа 1 за [start_ms, end_ms). Стакан нормализуется (идемпотентно) и чистится гейтом."""
    klines, depth, funding = KlinesLoader(), DepthLoader(), FundingLoader()
    candles = pl.concat([klines.scan(s, INTERVAL, start_ms, end_ms) for s in symbols])
    if candles.select(pl.len()).collect().item() == 0:
        raise DataError(f"Нет свечей {INTERVAL} для {list(symbols)} в [{start_ms}, {end_ms})")

    for s in symbols:
        depth.normalize_range(s, start_ms, end_ms)
    gated = DataQualityGate(timing).apply(pl.concat([depth.scan(s, start_ms, end_ms) for s in symbols]))
    book = gated.clean.collect()
    if book.height == 0:
        raise DataError(f"Нет данных стакана для {list(symbols)} в [{start_ms}, {end_ms})")
    n_quarantined = gated.quarantine.select(pl.len()).collect().item()

    rates = pl.concat([
        funding.scan(s, start_ms, end_ms, lookback_ms=2 * funding.interval_ms) for s in symbols
    ]).collect()
    return Market(candles, book, FrameFundingFeed(rates, funding.interval_ms), n_quarantined)


@dataclass(frozen=True)
class Dataset:
    decisions: pl.DataFrame  # таблица решений с признаками, БЕЗ меток
    labels: pl.DataFrame  # (symbol, asof_ts, метка, метка_end_ts)
    label: LabelSpec
    model_features: tuple[str, ...]  # что подаётся в модель (безразмерные)
    all_features: tuple[str, ...]  # все выходы признаков
    lookback_bars: int  # сколько баров истории нужно самому длинному признаку


def build_dataset(
    candles: pl.LazyFrame | pl.DataFrame,
    book: pl.LazyFrame | pl.DataFrame,
    *,
    features: Sequence[Feature],
    horizon_bars: int,
    timing: TimingConfig,
    interval_ms: int = config.INTERVAL_MS[INTERVAL],
) -> Dataset:
    """Свечи + стакан -> таблица решений с признаками + кадр меток. Признаки проверены ProvenanceGuard."""
    candles, book = candles.lazy(), book.lazy()
    clock, aligner = WatermarkClock(timing), TimelineAligner(timing)

    avail = clock.candle_availability(candles, interval_ms, clock.watermark(book))
    decisions = aligner.decisions(avail, interval_ms)
    decisions = aligner.attach_book(decisions, book, book_aggregations(book_levels(book.collect_schema().names())))
    compiled = compile_features(features, source_columns(decisions))
    featured = apply_features(decisions, compiled)

    spec = LabelSpec(horizon_ms=horizon_bars * interval_ms, interval_ms=interval_ms)
    labels = LabelBuilder([spec]).build(featured).collect()
    frame = featured.collect()
    extra = [c for c in frame.columns if c.endswith("_ts") and c not in ("asof_ts", "src_max_ts")]
    ProvenanceGuard(extra_ts_cols=extra).check(frame)  # LookaheadViolation, если в строку попало будущее

    outputs = tuple(n for c in compiled for n in c.exprs)
    return Dataset(
        frame, labels, spec, model_feature_names(outputs), outputs,
        max(c.lookback.bars_1m for c in compiled),
    )


# ------------------------------------------------------------------ walk-forward


@dataclass(frozen=True)
class WalkForwardConfig:
    horizon_bars: int = 5
    n_splits: int = 5
    test_size: int = 1_440  # баров сетки решений в одном тестовом фолде (сутки при 1m)
    min_train_size: int = 2_000
    train_size: int | None = None  # None - растущее окно, число - скользящее
    purge_bars: int | None = None  # None - равно горизонту метки; меньше горизонта нельзя
    alpha: float = 1.0
    threshold: float = 0.0010  # лог-доходность за горизонт; должен покрывать комиссии круга (2 x taker)
    exit_threshold: float = 0.0
    equity_fraction: float = 0.1
    initial_equity: float = 10_000.0
    taker_fee_rate: float = 0.0005
    maker_fee_rate: float = 0.0002
    max_exposure: float = 1.0
    max_drawdown: float = 0.2
    timing: TimingConfig = field(default_factory=TimingConfig)


@dataclass(frozen=True)
class FoldReport:
    index: int
    n_train_rows: int  # строк, на которых обучена модель
    n_train_dropped: int  # train-строк без признаков или метки (прогрев, устаревший стакан, хвосты)
    train_last_ts: int
    test_first_ts: int
    test_last_ts: int
    ic_train: float | None  # корреляция прогноза и метки на train (завышена: модель их видела)
    ic_oos: float | None  # то же на тесте - единственная честная оценка предсказательной силы
    hit_rate_oos: float | None  # доля совпавших знаков прогноза и метки на тесте
    start_equity: float
    end_equity: float
    n_orders: int  # ордера стратегии (без принудительного выкупа в конце фолда)
    fees: float
    max_drawdown: float

    @property
    def pnl(self) -> float:
        return self.end_equity - self.start_equity


@dataclass(frozen=True)
class WalkForwardReport:
    folds: tuple[FoldReport, ...]
    equity_curve: pl.DataFrame  # fold, ts, equity - сшитая кривая

    def summary(self) -> dict[str, float | int | None]:
        eq = self.equity_curve["equity"]
        start, end = float(self.folds[0].start_equity), float(eq[-1])
        peak = eq.cum_max()
        returns = eq.pct_change().drop_nulls()
        std = returns.std()
        ics = [f.ic_oos for f in self.folds if f.ic_oos is not None]
        return {
            "folds": len(self.folds),
            "start_equity": start,
            "end_equity": end,
            "total_return": end / start - 1.0,
            "max_drawdown": float(((peak - eq) / peak).max() or 0.0),
            "sharpe_ann": float(returns.mean() / std * math.sqrt(BARS_PER_YEAR)) if std else None,
            "fees": sum(f.fees for f in self.folds),
            "orders": sum(f.n_orders for f in self.folds),
            "mean_ic_oos": sum(ics) / len(ics) if ics else None,
        }


def _score(model: FeatureModel, frame: pl.DataFrame, target: str) -> tuple[float | None, float | None]:
    """(IC Пирсона, доля совпавших знаков) прогноза и метки. Вызывается только после прогона стратегии."""
    if frame.height < 3:
        return None, None
    pair = pl.DataFrame({"p": model.predict(frame), "y": frame[target]})
    ic = pair.select(pl.corr("p", "y")).item()
    hit = pair.filter(pl.col("y") != 0).select(((pl.col("p") > 0) == (pl.col("y") > 0)).mean()).item()
    return (ic if ic is not None and math.isfinite(ic) else None), hit


def walk_forward(
    ds: Dataset,
    book: pl.DataFrame,
    funding: FundingFeed,
    cfg: WalkForwardConfig,
    model_factory: Callable[[], FeatureModel] | None = None,
) -> WalkForwardReport:
    """Фолды PurgedWalkForward: fit на train, прогон стратегии на test, сшивка эквити. См. докстринг модуля."""
    factory = model_factory or (lambda: RidgeRegressor(cfg.alpha))
    labeler = LabelBuilder([ds.label])
    names, target = ds.model_features, ds.label.name
    label_cols = [*labeler.names, *labeler.end_names]
    horizon = bars_for(ds.label.horizon_ms, ds.label.interval_ms)
    cv = PurgedWalkForward(
        cfg.n_splits, cfg.test_size,
        label_horizon_bars=horizon,
        purge_bars=horizon if cfg.purge_bars is None else cfg.purge_bars,
        feature_lookback_bars=ds.lookback_bars,
        train_size=cfg.train_size,
        min_train_size=cfg.min_train_size,
    )
    exec_cfg = ExecutionConfig.from_timing(
        cfg.timing, maker_fee_rate=cfg.maker_fee_rate, taker_fee_rate=cfg.taker_fee_rate
    )
    labeled = labeler.attach(ds.decisions.lazy(), ds.labels.lazy()).collect()

    equity, folds, curves = cfg.initial_equity, [], []
    for ff in cv.split_frame(labeled, label_end_cols=labeler.end_names):
        fit_rows = ff.train.drop_nulls([target, *names])
        model = factory().fit(fit_rows, names, target)

        # Движку - тест без меток и их end_ts. Метки теста до конца прогона не нужны никому.
        test_view = ff.test.drop(label_cols)
        first, last = int(test_view["asof_ts"].min()), int(test_view["asof_ts"].max())
        fold_book = book.filter(
            (pl.col("recv_ts") >= first) & (pl.col("recv_ts") < last + 2 * ds.label.interval_ms)
        )
        engine = BacktestEngine(
            OrderBookBarrier(exec_cfg, funding, first, fold_book),
            MLModelStrategy(
                model, threshold=cfg.threshold, exit_threshold=cfg.exit_threshold,
                equity_fraction=cfg.equity_fraction,
            ),
            [MaxExposureGuard(cfg.max_exposure), DrawdownLimitGuard(cfg.max_drawdown)],
            initial_equity=equity,
        )
        result = engine.run(test_view)

        ic_train, _ = _score(model, fit_rows, target)
        ic_oos, hit = _score(model, ff.test.drop_nulls([target, *names]), target)
        folds.append(FoldReport(
            index=ff.fold.index, n_train_rows=fit_rows.height, n_train_dropped=ff.train.height - fit_rows.height,
            train_last_ts=int(fit_rows["asof_ts"].max()), test_first_ts=first, test_last_ts=last,
            ic_train=ic_train, ic_oos=ic_oos, hit_rate_oos=hit,
            start_equity=equity, end_equity=result.final_equity,
            n_orders=result.orders.filter(~pl.col("forced")).height,
            fees=result.total_fees, max_drawdown=result.max_drawdown,
        ))
        curves.append(result.equity_curve.select("ts", "equity").with_columns(fold=pl.lit(ff.fold.index)))
        log.info(
            "фолд %d: train %d строк, IC oos %s, PnL %+.2f, ордеров %d",
            ff.fold.index, fit_rows.height, folds[-1].ic_oos, folds[-1].pnl, folds[-1].n_orders,
        )
        equity = result.final_equity
        if equity <= 0:
            log.error("Капитал исчерпан на фолде %d: дальнейшие фолды не запускаются", ff.fold.index)
            break

    return WalkForwardReport(tuple(folds), pl.concat(curves).select("fold", "ts", "equity"))


# ------------------------------------------------------------------ финальная модель


def fit_final_model(ds: Dataset, cfg: WalkForwardConfig) -> RidgeRegressor:
    """Модель для боя: обучение на ВСЕХ решениях с известной меткой и полным набором признаков.

    Хвост без меток (последние horizon_bars строк каждого сегмента: будущего ещё нет), прогрев и строки с
    устаревшим стаканом отбрасываются (drop_nulls). Это уже не OOS-оценка: те же данные участвовали в
    walk-forward, поэтому качество финальной модели отчётом не измеряется - только отчётом фолдов.
    """
    labeled = LabelBuilder([ds.label]).attach(ds.decisions.lazy(), ds.labels.lazy()).collect()
    rows = labeled.drop_nulls([ds.label.name, *ds.model_features])
    log.info("финальная модель: %d строк из %d (без хвоста меток и прогрева)", rows.height, labeled.height)
    return RidgeRegressor(cfg.alpha).fit(rows, ds.model_features, ds.label.name)


# --------------------------------------------------------------------------- отчёт


def _fmt(value: float | None, spec: str) -> str:
    return "      -" if value is None else format(value, spec)


def format_report(report: WalkForwardReport) -> str:
    lines = [
        "фолд | train строк | IC train | IC oos | hit oos | ордеров |  эквити старт ->  конец |       PnL | макс.просадка",
        "-----+-------------+----------+--------+---------+---------+--------------------------+-----------+--------------",
    ]
    for f in report.folds:
        lines.append(
            f"{f.index:4d} | {f.n_train_rows:11d} | {_fmt(f.ic_train, '8.3f')} | {_fmt(f.ic_oos, '6.3f')} | "
            f"{_fmt(f.hit_rate_oos, '7.3f')} | {f.n_orders:7d} | {f.start_equity:11.2f} -> {f.end_equity:10.2f} | "
            f"{f.pnl:+9.2f} | {f.max_drawdown:12.2%}"
        )
    s = report.summary()
    lines += [
        "",
        f"ИТОГО OOS: доходность {s['total_return']:+.2%}, макс. просадка {s['max_drawdown']:.2%}, "
        f"Sharpe(ann., 1m) {_fmt(s['sharpe_ann'], '.2f').strip()}, комиссии {s['fees']:.2f}, ордеров {s['orders']}, "
        f"средний IC oos {_fmt(s['mean_ic_oos'], '.3f').strip()}",
    ]
    return "\n".join(lines)


# ------------------------------------------------------------------------- CLI


def _day_ms(text: str) -> int:
    try:
        day = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ConfigError(f"Дата в формате YYYY-MM-DD, получено {text!r}") from exc
    return int(day.timestamp() * 1000)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Walk-forward обучение и OOS-бэктест MLModelStrategy")
    p.add_argument("--symbols", nargs="+", default=list(config.SYMBOLS))
    p.add_argument("--start", required=True, help="YYYY-MM-DD UTC, включительно")
    p.add_argument("--end", required=True, help="YYYY-MM-DD UTC, не включая")
    d = WalkForwardConfig()
    p.add_argument("--horizon-bars", type=int, default=d.horizon_bars, help="горизонт метки, баров 1m")
    p.add_argument("--splits", type=int, default=d.n_splits)
    p.add_argument("--test-bars", type=int, default=d.test_size)
    p.add_argument("--min-train-bars", type=int, default=d.min_train_size)
    p.add_argument("--train-bars", type=int, default=None, help="скользящее окно train; по умолчанию растущее")
    p.add_argument("--alpha", type=float, default=d.alpha)
    p.add_argument("--threshold-bps", type=float, default=d.threshold * 1e4)
    p.add_argument("--exit-threshold-bps", type=float, default=d.exit_threshold * 1e4)
    p.add_argument("--fraction", type=float, default=d.equity_fraction)
    p.add_argument("--equity", type=float, default=d.initial_equity)
    p.add_argument("--taker-fee", type=float, default=d.taker_fee_rate)
    p.add_argument("--maker-fee", type=float, default=d.maker_fee_rate)
    p.add_argument("--max-exposure", type=float, default=d.max_exposure)
    p.add_argument("--max-drawdown", type=float, default=d.max_drawdown)
    p.add_argument("--no-taker-flow", action="store_true", help="не строить taker_imb_* (нет taker_buy_base)")
    p.add_argument("--out-dir", default=None, help="куда записать equity_curve.csv и folds.csv")
    p.add_argument(
        "--save-model", type=Path, default=None, metavar="PATH.json",
        help="после walk-forward обучить модель на всём датасете и сохранить (для scripts.live_run --model-path)",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    setup_logging("train_walkforward")
    try:
        cfg = WalkForwardConfig(
            horizon_bars=args.horizon_bars, n_splits=args.splits, test_size=args.test_bars,
            min_train_size=args.min_train_bars, train_size=args.train_bars, alpha=args.alpha,
            threshold=args.threshold_bps / 1e4, exit_threshold=args.exit_threshold_bps / 1e4,
            equity_fraction=args.fraction, initial_equity=args.equity, taker_fee_rate=args.taker_fee,
            maker_fee_rate=args.maker_fee, max_exposure=args.max_exposure, max_drawdown=args.max_drawdown,
        )
        start_ms, end_ms = _day_ms(args.start), _day_ms(args.end)
        if end_ms <= start_ms:
            raise ConfigError("--end должен быть позже --start")

        market = load_market(args.symbols, start_ms, end_ms, cfg.timing)
        log.info("стакан: %d сообщений принято, %d в карантине", market.book.height, market.n_quarantined)
        ds = build_dataset(
            market.candles, market.book,
            features=default_features(taker_flow=not args.no_taker_flow),
            horizon_bars=cfg.horizon_bars, timing=cfg.timing,
        )
        log.info("датасет: %d решений, признаки модели: %s", ds.decisions.height, ", ".join(ds.model_features))

        report = walk_forward(ds, market.book, market.funding, cfg)
        print(format_report(report))
        if args.out_dir:
            out = Path(args.out_dir)
            out.mkdir(parents=True, exist_ok=True)
            report.equity_curve.write_csv(out / "equity_curve.csv")
            pl.DataFrame([f.__dict__ | {"pnl": f.pnl} for f in report.folds]).write_csv(out / "folds.csv")
        if args.save_model:
            model = fit_final_model(ds, cfg)
            model.save_json(args.save_model)
            log.info("модель сохранена: %s (признаков %d)", args.save_model, len(model.feature_names))
        return EXIT_OK
    except TradingInfraError as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return exc.exit_code
    except Exception:
        log.exception("Непредвиденная ошибка")
        return EXIT_GENERAL


if __name__ == "__main__":
    sys.exit(main())
