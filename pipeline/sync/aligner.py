"""Единая таблица решений: свечи + стакан в шкале знания.

Центральное правило: любой агрегат за интервал [a, b) получает ключ b (правый край),
а не a (open_time). Ключ строки - asof_ts, момент, когда решение уже можно принять:

    asof_ts_k = avail_ts_k + decision_compute_ms

Все присоединения строгие: источник попадает в строку только если его recv_ts / avail_ts
СТРОГО меньше asof_ts. Совпадение с точностью до миллисекунды считается «ещё не известно».
Это структурный аналог shift(1) относительно ключа open_time - ручных сдвигов нет.

Каждая строка несёт src_max_ts - максимум времени знания всех входов. Инвариант
src_max_ts < asof_ts проверяется на готовом датасете (guards/provenance).
"""

from collections.abc import Sequence

import polars as pl

from pipeline.sync._frames import asof, require_columns
from pipeline.types import TimingConfig
from utils.errors import DataError

_BOOK_META = ("book_last_recv_ts", "book_n_msgs", "book_age_ms", "book_max_gap_ms", "book_ok")


class TimelineAligner:
    def __init__(self, timing: TimingConfig):
        self._timing = timing

    # ------------------------------------------------------------------ решения

    def decisions(self, candles_avail: pl.LazyFrame, interval_ms: int) -> pl.LazyFrame:
        """Минутные свечи с avail_ts -> сетка решений.

        Добавляет: asof_ts, src_max_ts (=avail_ts), contiguous (предыдущая свеча ровно на
        интервал раньше), segment_id (растёт после каждой пропущенной свечи - окна признаков
        по числу строк не должны пересекать границу сегмента), window_start (левый край окна
        агрегатов стакана). Свечи без avail_ts отбрасываются: это разрыв, а не «ноль».
        """
        require_columns(candles_avail, ("symbol", "open_time", "avail_ts"), "candles_avail")
        t = self._timing
        return (
            candles_avail.filter(pl.col("avail_ts").is_not_null())
            .sort("symbol", "open_time")
            .with_columns(
                asof_ts=pl.col("avail_ts") + t.decision_compute_ms,
                src_max_ts=pl.col("avail_ts"),
            )
            .with_columns(
                _prev_open=pl.col("open_time").shift(1).over("symbol"),
                _prev_asof=pl.col("asof_ts").shift(1).over("symbol"),
            )
            .with_columns(
                contiguous=((pl.col("open_time") - pl.col("_prev_open")) == interval_ms).fill_null(False)
            )
            .with_columns(
                segment_id=(~pl.col("contiguous")).cast(pl.Int64).cum_sum().over("symbol") - 1,
                # Внутри сегмента окно = между соседними решениями; после разрыва или у
                # первой строки - ровно один интервал назад (иначе первая корзина собрала бы
                # часы данных, а корзина после пропуска - все пропущенные минуты).
                window_start=pl.when(pl.col("contiguous"))
                .then(pl.col("_prev_asof"))
                .otherwise(pl.col("asof_ts") - interval_ms),
            )
            .drop("_prev_open", "_prev_asof")
        )

    # ------------------------------------------------------------------- стакан

    def assign_book(self, decisions: pl.LazyFrame, book: pl.LazyFrame) -> pl.LazyFrame:
        """Сообщения стакана -> корзины решений (уровень сообщений).

        Сообщение i попадает в ПЕРВОЕ решение с asof_ts > recv_ts_i (строго) и только если
        recv_ts_i >= window_start. Итого корзина решения k = [window_start_k, asof_ts_k).
        Сообщение с recv_ts == asof_ts_k относится уже к k+1: на момент решения k оно ещё
        не считается полученным. Сообщения после последнего решения отбрасываются.
        """
        require_columns(book, ("symbol", "recv_ts"), "book")
        require_columns(decisions, ("symbol", "asof_ts", "window_start"), "decisions")
        keys = decisions.select("symbol", "asof_ts", "window_start").sort("asof_ts", "symbol")
        return (
            book.sort("recv_ts", "symbol")
            .pipe(
                asof,
                keys,
                left_on="recv_ts",
                right_on="asof_ts",
                strategy="forward",
                allow_exact_matches=False,  # asof_ts > recv_ts строго
            )
            .filter(pl.col("asof_ts").is_not_null() & (pl.col("recv_ts") >= pl.col("window_start")))
        )

    def attach_book(
        self,
        decisions: pl.LazyFrame,
        book: pl.LazyFrame,
        aggs: Sequence[pl.Expr],
    ) -> pl.LazyFrame:
        """Присоединяет агрегаты стакана за корзину решения.

        aggs - агрегирующие выражения над сообщениями корзины с явными alias (например
        pl.col("bid_px_0").last().alias("bid_px_0")). Добавляет:
          book_last_recv_ts  - recv_ts последнего сообщения корзины;
          book_n_msgs        - число сообщений (0, если корзина пуста);
          book_age_ms        - asof_ts - book_last_recv_ts (null, если корзина пуста);
          book_max_gap_ms    - ДИАГНОСТИКА: наибольшая «дыра» между соседними сообщениями,
                               считая от window_start до первого и от последнего до asof_ts;
          book_ok            - book_age_ms <= book_max_staleness_ms (пустая корзина - False).
        Критерий валидности - только СВЕЖЕСТЬ последнего состояния:

            book_ok  <=>  asof_ts - book_last_recv_ts <= book_max_staleness_ms

        Внутренние дыры окна (потерянные пакеты посреди минуты) на book_ok не влияют: для
        решения важен актуальный стакан перед самым asof_ts. book_max_gap_ms остаётся
        диагностикой (его можно использовать как отдельный фильтр), а агрегаты по всему
        окну (mean/sum) при дырах считаются по меньшему числу сообщений - см. book_n_msgs.
        Если book_ok = False (последнее сообщение устарело или корзина пуста), значения aggs
        обнуляются (null): устаревший стакан не заполняется вперёд.
        """
        t = self._timing
        aggs = list(aggs)
        msgs = (
            self.assign_book(decisions, book)
            .with_columns(
                _prev=pl.col("recv_ts").shift(1).over("symbol", "asof_ts").fill_null(pl.col("window_start"))
            )
            .with_columns(_gap=pl.col("recv_ts") - pl.col("_prev"))
        )
        bucket = msgs.group_by("symbol", "asof_ts").agg(
            *aggs,
            pl.col("recv_ts").max().alias("book_last_recv_ts"),
            pl.len().alias("book_n_msgs"),
            pl.col("_gap").max().alias("_inner_gap"),
        )

        agg_names = [
            n
            for n in bucket.collect_schema().names()
            if n not in ("symbol", "asof_ts", "_inner_gap", *_BOOK_META)
        ]
        clash = set(agg_names) & set(decisions.collect_schema().names())
        if clash:
            raise DataError(f"Имена агрегатов стакана пересекаются с колонками решений: {sorted(clash)}")

        asof_ts, n_msgs = pl.col("asof_ts"), pl.col("book_n_msgs")
        out = (
            decisions.join(bucket, on=["symbol", "asof_ts"], how="left")
            .with_columns(
                book_n_msgs=n_msgs.fill_null(0),
                book_max_gap_ms=pl.when(n_msgs.is_null())
                .then(asof_ts - pl.col("window_start"))
                .otherwise(pl.max_horizontal(pl.col("_inner_gap"), asof_ts - pl.col("book_last_recv_ts"))),
            )
            .with_columns(book_age_ms=asof_ts - pl.col("book_last_recv_ts"))
            .with_columns(
                # Пустая корзина: book_age_ms = null -> сравнение даёт null -> fill_null(False).
                book_ok=(pl.col("book_age_ms") <= t.book_max_staleness_ms).fill_null(False)
            )
            .with_columns(
                [pl.when(pl.col("book_ok")).then(pl.col(n)).otherwise(None).alias(n) for n in agg_names]
            )
            .with_columns(src_max_ts=pl.max_horizontal("src_max_ts", "book_last_recv_ts"))
            .drop("_inner_gap")
        )
        return out

    # ------------------------------------------------------- точечные события

    def attach_events(
        self,
        decisions: pl.LazyFrame,
        events_avail: pl.LazyFrame,
        columns: Sequence[str],
        *,
        max_age_ms: int,
        tag: str = "",
        avail_name: str | None = None,
        age_name: str | None = None,
    ) -> pl.LazyFrame:
        """Присоединяет ПОСЛЕДНЕЕ УЖЕ ИЗВЕСТНОЕ событие (свеча другого ТФ, ставка фандинга...).

        events_avail - результат WatermarkClock.event_availability (нужны symbol, avail_ts и
        columns). Ключ - avail_ts события в шкале знания, а НЕ серверное время события:
        join по серверному времени (open_time, fundingTime) смешивает две шкалы и даёт
        заглядывание вперёд. Условие строгое: avail_ts < asof_ts (событие в ту же миллисекунду
        ещё не известно). События с avail_ts = null (gap/unobserved) пропускаются: знание не
        придумывается.

        max_age_ms - предельный возраст: если последнее известное событие старше
        (asof_ts - avail_ts > max_age_ms), присоединяется null, а не устаревшее значение.

        Добавляет: {tag}{column} для каждой колонки, avail_name (по умолчанию {tag}avail_ts) и
        age_name (по умолчанию {tag}age_ms = asof_ts - avail_ts; null, если события нет);
        src_max_ts обновляется. При пустом tag имена avail/age нужно задать явно.
        """
        if max_age_ms < 1:
            raise DataError(f"max_age_ms должен быть >= 1, получено {max_age_ms}")
        columns = list(columns)
        require_columns(events_avail, ("symbol", "avail_ts", *columns), "events_avail")
        avail_name = avail_name or f"{tag}avail_ts"
        age_name = age_name or f"{tag}age_ms"
        names = [f"{tag}{c}" for c in columns] + [avail_name, age_name]
        if len(set(names)) != len(names):
            raise DataError(f"Имена присоединяемых колонок повторяются: {names}")
        clash = set(names) & set(decisions.collect_schema().names())
        if clash:
            raise DataError(f"Колонки {sorted(clash)} уже есть в решениях; задайте другой tag")

        right = (
            events_avail.filter(pl.col("avail_ts").is_not_null())
            .select(
                "symbol",
                pl.col("avail_ts").alias("_avail_key"),
                *[pl.col(c).alias(f"{tag}{c}") for c in columns],
                pl.col("avail_ts").alias(avail_name),
            )
            .sort("_avail_key", "symbol")
        )
        return (
            decisions.sort("asof_ts", "symbol")
            .pipe(
                asof,
                right,
                left_on="asof_ts",
                right_on="_avail_key",
                strategy="backward",
                allow_exact_matches=False,  # avail_ts < asof_ts строго
                tolerance=max_age_ms,
            )
            .with_columns(
                (pl.col("asof_ts") - pl.col(avail_name)).alias(age_name),
                src_max_ts=pl.max_horizontal("src_max_ts", avail_name),
            )
            .drop("_avail_key", strict=False)
            .sort("symbol", "asof_ts")
        )

    def attach_candles(
        self,
        decisions: pl.LazyFrame,
        candles_avail: pl.LazyFrame,
        interval_ms: int,
        tag: str,
        columns: Sequence[str],
    ) -> pl.LazyFrame:
        """Последняя УЖЕ ИЗВЕСТНАЯ свеча другого таймфрейма (например 1h).

        Предельный возраст = interval_ms + gap_break_ms: хватает нормальной вариации avail
        между соседними свечами; при пропущенном периоде присоединяется null.
        Колонки получают префикс tag (обязателен): {tag}{column}, {tag}avail_ts, {tag}age_ms.
        """
        if not tag:
            raise DataError("tag не должен быть пустым")
        return self.attach_events(
            decisions, candles_avail, columns,
            max_age_ms=interval_ms + self._timing.gap_break_ms, tag=tag,
        )

    def attach_funding(
        self,
        decisions: pl.LazyFrame,
        funding_avail: pl.LazyFrame,
        interval_ms: int,
    ) -> pl.LazyFrame:
        """Последняя УЖЕ ОПУБЛИКОВАННАЯ закрытая ставка фандинга.

        funding_avail - результат WatermarkClock.funding_availability (funding_rate, mark_price).
        Добавляет: funding_rate, mark_price, funding_avail_ts, funding_age_ms. Ставка видна
        строке только после avail_ts = (recv_ts первого сообщения с E >= fundingTime) +
        funding_publish_lag_ms, строго раньше asof_ts. Ставка «ближайшего будущего списания»
        на бары до него не распространяется. Предельный возраст = interval_ms + gap_break_ms:
        при пропущенном списании через этот срок будет null, а не ставка двухсуточной давности.
        """
        return self.attach_events(
            decisions, funding_avail, ["funding_rate", "mark_price"],
            max_age_ms=interval_ms + self._timing.gap_break_ms,
            avail_name="funding_avail_ts", age_name="funding_age_ms",
        )
