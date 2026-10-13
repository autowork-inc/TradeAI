"""DataQualityGate: каузальная очистка стакана по серверному времени E и локальным часам.

Опорная величина - d_i = recv_ts_i - E_i = θ + λ_i (смещение часов + задержка). Для
сообщений одного символа, упорядоченных по номеру обновления u (порядок биржи, не зависит
от наших часов), строятся:

  baseline_i = median(d_{i-N} .. d_{i-1})            только прошлое, N = baseline_window
  dev_i      = d_i - baseline_i

Все решения принимаются по сообщениям <= i: классификация префикса данных совпадает с
классификацией полного набора (проверяется тестом). Нужно, чтобы очистка сама не создавала
lookahead: живая система видела бы те же сообщения отвергнутыми в тот же момент.

Что считается браком (gate_reason, сообщение уходит в карантин, а не удаляется молча):
  recv_regress - recv_ts меньше уже виденного максимума (шаг локальных часов НАЗАД или
                 нарушение монотонности): сообщение нельзя упорядочить по шкале знания.
                 Карантин держится, пока recv_ts не «догонит» прежний максимум;
  e_outlier    - |dev| > e_outlier_tolerance_ms и это не сдвиг режима часов (см. ниже).
                 Одиночный E из будущего иначе поднял бы водяной знак навсегда.

Сдвиг режима часов (NTP step) - не выброс. Серия подряд идущих сообщений с |dev| >
step_threshold_ms, у которой разброс самого d остаётся <= step_spread_ms и длина достигла
step_min_run, признаётся шагом: начиная с step_min_run-го сообщения серии они принимаются,
clock_epoch растёт на 1, событие попадает в steps (delta_ms = dev, kind = forward/backward).
Первые step_min_run-1 сообщений серии, если |dev| > tolerance, остаются в карантине как
e_outlier (causal: до подтверждения серии отличить шаг от серии выбросов нельзя).

Ограничения (осознанные):
  * шаги меньше step_threshold_ms не детектируются: они укладываются в консервативный
    запас candle_publish_lag_ms;
  * серия сообщений с устойчиво «сдвинутым» E по одному символу неотличима от шага часов
    (подтвердить можно только по нескольким символам сразу - в этой версии не делается);
  * первые baseline_min_samples сообщений диапазона не оцениваются (baseline = null):
    передавайте загрузчику диапазон с запасом назад и отбрасывайте прогрев;
  * clock_epoch начинается с подтверждения шага (запаздывает на <= step_min_run сообщений).
"""

from dataclasses import dataclass

import polars as pl

from pipeline.sync._frames import require_columns
from pipeline.types import TimingConfig
from utils.errors import ConfigError

REASON_REGRESS = "recv_regress"
REASON_OUTLIER = "e_outlier"


@dataclass(frozen=True)
class GateConfig:
    baseline_window: int = 121  # ~1 мин при обновлении раз в 500 мс; нечётное - медиана без усреднения
    baseline_min_samples: int = 20
    step_threshold_ms: int = 500
    step_min_run: int = 10
    step_spread_ms: int = 1_000

    def __post_init__(self) -> None:
        if not 1 <= self.baseline_min_samples <= self.baseline_window:
            raise ConfigError("Нужно 1 <= baseline_min_samples <= baseline_window")
        if self.step_min_run < 2 or self.step_threshold_ms < 1 or self.step_spread_ms < 1:
            raise ConfigError(f"Недопустимые параметры детекции шага часов: {self}")


@dataclass(frozen=True)
class GateResult:
    clean: pl.LazyFrame  # принятые сообщения по порядку recv_ts + clock_d_ms, clock_baseline_ms, clock_epoch
    quarantine: pl.LazyFrame  # отвергнутые сообщения + gate_reason (+ те же диагностические колонки)
    steps: pl.LazyFrame  # события шага часов: symbol, u, recv_ts, delta_ms, kind, clock_epoch


class DataQualityGate:
    def __init__(self, timing: TimingConfig, config: GateConfig | None = None):
        self._timing = timing
        self._cfg = config or GateConfig()

    def classify(self, book: pl.LazyFrame) -> pl.LazyFrame:
        """Все сообщения (в порядке symbol, u) + gate_reason (null = принято), clock_d_ms,
        clock_baseline_ms, clock_epoch и служебные колонки с префиксом _."""
        require_columns(book, ("symbol", "recv_ts", "E", "u"), "book")
        c, tol = self._cfg, self._timing.e_outlier_tolerance_ms
        sym, d = "symbol", pl.col("clock_d_ms")
        return (
            book.sort(sym, "u")
            .with_columns(clock_d_ms=pl.col("recv_ts") - pl.col("E"))
            .with_columns(
                # Окно до текущего сообщения: rolling на i-1, внутри одного символа.
                clock_baseline_ms=d.cast(pl.Float64)
                .rolling_median(window_size=c.baseline_window, min_samples=c.baseline_min_samples)
                .shift(1)
                .over(sym),
                _prev_max_recv=pl.col("recv_ts").cum_max().shift(1).over(sym),
            )
            .with_columns(
                _dev=d - pl.col("clock_baseline_ms"),
                recv_regress=(pl.col("recv_ts") < pl.col("_prev_max_recv")).fill_null(False),
            )
            .with_columns(_big=(pl.col("_dev").abs() > c.step_threshold_ms).fill_null(False))
            .with_columns(
                # Номер серии подряд идущих значений _big; новый символ всегда начинает серию.
                _start=((pl.col("_big") != pl.col("_big").shift(1)) | (pl.col(sym) != pl.col(sym).shift(1)))
                .fill_null(True)
            )
            .with_columns(_run=pl.col("_start").cum_sum())
            .with_columns(
                _pos=pl.int_range(pl.len()).over("_run") + 1,
                _dmax=d.cum_max().over("_run"),
                _dmin=d.cum_min().over("_run"),
            )
            .with_columns(
                _step_msg=pl.col("_big")
                & (pl.col("_pos") >= c.step_min_run)
                & ((pl.col("_dmax") - pl.col("_dmin")) <= c.step_spread_ms)
            )
            .with_columns(
                _epoch_start=pl.col("_step_msg") & (pl.col("_step_msg").cum_sum().over("_run") == 1)
            )
            .with_columns(
                clock_epoch=pl.col("_epoch_start").cast(pl.Int64).cum_sum().over(sym),
                gate_reason=pl.when(pl.col("recv_regress")).then(pl.lit(REASON_REGRESS))
                .when((pl.col("_dev").abs() > tol) & ~pl.col("_step_msg")).then(pl.lit(REASON_OUTLIER)),
            )
        )

    def apply(self, book: pl.LazyFrame) -> GateResult:
        """Стакан (результат DepthLoader.scan) -> принятое, карантин, события шага часов.

        Принятое гарантированно имеет неубывающий recv_ts внутри символа и отсортировано по
        (recv_ts, symbol, u): его можно передавать в WatermarkClock и TimelineAligner.
        """
        classified = self.classify(book)
        diag = ["clock_d_ms", "clock_baseline_ms", "clock_epoch"]
        original = [n for n in book.collect_schema().names()]
        clean = (
            classified.filter(pl.col("gate_reason").is_null())
            .select(*original, *[x for x in diag if x not in original])
            .sort("recv_ts", "symbol", "u")
        )
        quarantine = (
            classified.filter(pl.col("gate_reason").is_not_null())
            .select(*original, *[x for x in diag if x not in original], "gate_reason")
            .sort("recv_ts", "symbol", "u")
        )
        steps = classified.filter(pl.col("_epoch_start")).select(
            "symbol", "u", "recv_ts",
            pl.col("_dev").alias("delta_ms"),
            pl.when(pl.col("_dev") > 0).then(pl.lit("forward")).otherwise(pl.lit("backward")).alias("kind"),
            "clock_epoch",
        )
        return GateResult(clean, quarantine, steps)

    def clock_report(self, book: pl.LazyFrame) -> pl.DataFrame:
        """Сводка по символам: распределение d = recv_ts - E и число отбракованного.

        Прогоните на реальных данных до выбора порогов: по ней видно, насколько часы
        хоста «плавают» (p01/p99, доля d < 0) и были ли шаги."""
        return (
            self.classify(book)
            .group_by("symbol")
            .agg(
                n=pl.len(),
                d_p01=pl.col("clock_d_ms").quantile(0.01),
                d_p50=pl.col("clock_d_ms").median(),
                d_p99=pl.col("clock_d_ms").quantile(0.99),
                d_min=pl.col("clock_d_ms").min(),
                d_max=pl.col("clock_d_ms").max(),
                share_negative=(pl.col("clock_d_ms") < 0).mean(),
                n_regress=(pl.col("gate_reason") == REASON_REGRESS).sum(),
                n_outliers=(pl.col("gate_reason") == REASON_OUTLIER).sum(),
                n_steps=pl.col("_epoch_start").sum(),
            )
            .sort("symbol")
            .collect()
        )
