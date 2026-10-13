"""Водяной знак серверного времени: когда мы УЗНАЛИ о точечном событии биржи.

Проблема. Событие (закрытие свечи, списание фандинга, любое другое) происходит на бирже в
серверное время T, а наши часы (recv_ts) смещены относительно серверных на неизвестное и
плавающее θ. Вычитать θ нельзя: скачок локальных часов вперёд занизит оценку и «откроет»
событие раньше срока - скрытый lookahead.

Решение: θ не оценивается вообще. Для упорядоченных по recv_ts сообщений стакана

    W_i = max_{j <= i} E_j                    (водяной знак: монотонен и каузален)

Сообщение с E_i >= T не могло быть сгенерировано до события, поэтому первое сообщение i*,
у которого W достигла T, - доказательство, что событие уже произошло, а его recv_ts - момент,
когда мы об этом узнали:

    avail = recv_ts[i*] + publish_lag_ms

publish_lag_ms - запас на публикацию данных события (для свечи - финализация, для фандинга -
публикация ставки в момент списания). Ошибка всегда в консервативную сторону. Достоверность
оценки зависит от плотности потока, поэтому события делятся на четыре класса (avail_source):

  watermark  - доказательство «плотное»: W_match - T <= gap_break_ms; avail_ts задан;
  assumed    - событие раньше начала потока (история без стакана) и assume_pre_stream=True:
               avail_ts = T + lag (θ принят равным 0, использовать осознанно);
  gap        - доказательство есть, но пришло слишком поздно (поток «слеп» в момент события,
               например обрыв и возобновление): avail_ts = null;
  unobserved - доказательства нет вовсе (поток остановился, W перестала расти, либо событие
               до начала потока при assume_pre_stream=False): avail_ts = null.

События с avail_ts = null дальше не используются (в таблицу решений не попадают свечи, а
аналог-присоединение событий их пропускает): отсутствующее знание не заменяется догадкой.

Предусловие: стакан уже прошёл DataQualityGate (выбросы E, дубли убраны). Один сбойный E
из далёкого будущего поднял бы W навсегда.
"""

import polars as pl

from pipeline.sync._frames import asof, require_columns
from pipeline.types import TimingConfig
from utils.errors import ConfigError, DataError

AVAIL_WATERMARK = "watermark"
AVAIL_ASSUMED = "assumed"
AVAIL_GAP = "gap"
AVAIL_UNOBSERVED = "unobserved"

# Колонки, которые event_availability добавляет, и временные: во входе их быть не должно.
_OUTPUT_COLS = ("avail_ts", "avail_source", "evidence_lag_ms")
_TEMP_COLS = ("W", "w_match", "evidence_recv_ts", "first_w")


class WatermarkClock:
    def __init__(self, timing: TimingConfig):
        self._timing = timing

    def watermark(self, book: pl.LazyFrame) -> pl.LazyFrame:
        """Сообщения стакана -> моменты, в которые W достигала новых значений.

        Вход: symbol, recv_ts, E (и необязательно u для разрешения ничьих).
        Выход: symbol, W, recv_ts - по одной строке на каждое новое значение W (первое
        достижение), отсортировано по W. Сообщения, не поднявшие W (запоздавшие, повторы,
        «замёрзший» E), в выход не попадают, поэтому после остановки потока новых строк нет.
        """
        require_columns(book, ("symbol", "recv_ts", "E"), "book")
        order = ["recv_ts", "symbol"] + (["u"] if "u" in book.collect_schema().names() else [])
        return (
            book.drop_nulls(["recv_ts", "E"])
            .sort(order)
            .with_columns(W=pl.col("E").cum_max().over("symbol"))
            .group_by("symbol", "W", maintain_order=True)
            .agg(pl.col("recv_ts").first())
            .sort("W", "symbol")
        )

    def event_availability(
        self,
        events: pl.LazyFrame,
        time_col: str,
        wm: pl.LazyFrame,
        *,
        publish_lag_ms: int,
        min_spacing_ms: int,
        assume_pre_stream: bool = False,
    ) -> pl.LazyFrame:
        """События + (avail_ts, avail_source, evidence_lag_ms): когда мы о них узнали.

        events: symbol, time_col (мс, СЕРВЕРНОЕ время события) и любые другие колонки.
        wm: результат watermark(). publish_lag_ms: запас на публикацию данных события.
        min_spacing_ms: минимальное расстояние между соседними событиями одного символа
        (интервал свечи, интервал фандинга). gap_break_ms обязан быть меньше него, иначе два
        соседних события могли бы получить доказательство от одного сообщения.
        evidence_lag_ms = W_match - T: насколько позже события (в серверном времени) мы
        получили доказательство; null, если доказательства нет.
        """
        t = self._timing
        if t.gap_break_ms >= min_spacing_ms:
            raise ConfigError(
                f"gap_break_ms={t.gap_break_ms} должен быть меньше расстояния между событиями {min_spacing_ms}"
            )
        if publish_lag_ms < 0:
            raise ConfigError(f"publish_lag_ms не может быть отрицательным: {publish_lag_ms}")
        require_columns(events, ("symbol", time_col), "events")
        require_columns(wm, ("symbol", "W", "recv_ts"), "watermark")
        taken = set(events.collect_schema().names()) & {*_OUTPUT_COLS, *_TEMP_COLS}
        if taken:
            raise DataError(f"events уже содержит служебные колонки {sorted(taken)}")

        coverage = wm.group_by("symbol").agg(first_w=pl.col("W").min())
        evidence = wm.select(
            "symbol",
            pl.col("W"),
            pl.col("W").alias("w_match"),
            pl.col("recv_ts").alias("evidence_recv_ts"),
        ).sort("W", "symbol")

        joined = (
            events.sort(time_col, "symbol")
            .pipe(asof, evidence, left_on=time_col, right_on="W", strategy="forward")
            .join(coverage, on="symbol", how="left")
        )

        ts, w_match, first_w = pl.col(time_col), pl.col("w_match"), pl.col("first_w")
        tight = w_match.is_not_null() & ((w_match - ts) <= t.gap_break_ms)
        # Событие раньше, чем поток вообще начал идти (или символа в стакане нет).
        pre_stream = first_w.is_null() | (ts < first_w)

        source = (
            pl.when(tight).then(pl.lit(AVAIL_WATERMARK))
            .when(pre_stream).then(pl.lit(AVAIL_ASSUMED if assume_pre_stream else AVAIL_UNOBSERVED))
            .when(w_match.is_not_null()).then(pl.lit(AVAIL_GAP))
            .otherwise(pl.lit(AVAIL_UNOBSERVED))
        )
        avail = (
            pl.when(tight).then(pl.col("evidence_recv_ts") + publish_lag_ms)
            .when(pre_stream & pl.lit(assume_pre_stream)).then(ts + publish_lag_ms)
            .otherwise(pl.lit(None, dtype=pl.Int64))
        )
        return (
            joined.with_columns(avail_ts=avail, avail_source=source, evidence_lag_ms=w_match - ts)
            .drop(*_TEMP_COLS, strict=False)
            .sort("symbol", time_col)
        )

    # ------------------------------------------------------------ частные случаи

    def candle_availability(
        self,
        candles: pl.LazyFrame,
        interval_ms: int,
        wm: pl.LazyFrame,
        *,
        assume_pre_stream: bool = False,
    ) -> pl.LazyFrame:
        """Свечи + (c, avail_ts, avail_source, evidence_lag_ms); c = open_time + interval.

        Событие свечи - её закрытие в серверное время c. candles: symbol, open_time, ...
        """
        require_columns(candles, ("symbol", "open_time"), "candles")
        return self.event_availability(
            candles.with_columns(c=pl.col("open_time") + interval_ms),
            "c",
            wm,
            publish_lag_ms=self._timing.candle_publish_lag_ms,
            min_spacing_ms=interval_ms,
            assume_pre_stream=assume_pre_stream,
        )

    def funding_availability(
        self,
        funding: pl.LazyFrame,
        wm: pl.LazyFrame,
        interval_ms: int,
        *,
        assume_pre_stream: bool = False,
    ) -> pl.LazyFrame:
        """Ставки фандинга + (avail_ts, avail_source, evidence_lag_ms).

        Событие - списание в серверное время funding_time. Финальная ставка публикуется в
        момент списания, поэтому раньше avail_ts она в данных появиться не может.
        funding: symbol, funding_time, funding_rate, ...
        """
        return self.event_availability(
            funding,
            "funding_time",
            wm,
            publish_lag_ms=self._timing.funding_publish_lag_ms,
            min_spacing_ms=interval_ms,
            assume_pre_stream=assume_pre_stream,
        )
