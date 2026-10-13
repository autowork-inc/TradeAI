"""Признаки микроструктуры: стакан (OBI, спред, microprice, order flow), поток объёма, краткосрочный импульс.

Все признаки строятся ТОЛЬКО через FeatureBuilder (pipeline/features/base): арифметика, log/sqrt,
shift(n>=1), rolling_*/ewm_mean. Сырых pl.Expr в признаках нет, поэтому каузальность и работа внутри
(symbol, segment_id) обеспечены фасадом, а не дисциплиной автора. Исключение одно и оно не признак, а
агрегат стакана: book_aggregations() - выражения для TimelineAligner.attach_book, которые сворачивают
сообщения корзины решения [window_start, asof_ts) в числа. Корзина по построению состоит из сообщений
с recv_ts < asof_ts; при устаревшем стакане (book_ok = False) выравниватель обнуляет все агрегаты.

Порядок использования:

    decisions = aligner.attach_book(decisions, book, book_aggregations(levels=5))
    compiled = compile_features(default_features(), source_columns(decisions))
    frame = apply_features(decisions, compiled)

Признаки безразмерны или в bps (дисбалансы, доли, спред к mid): модель обучается на истории с разным
уровнем цены и объёмов, абсолютные величины (спред в USDT, объём в BTC) ей вредят. Абсолютные версии
(spread, vol_sum_*) оставлены для анализа и фильтров.

Прогрев: окна прогона дают null (фасад не выдаёт «частичных» окон), стратегия на null не торгует.
"""

from collections.abc import Iterable, Mapping

import polars as pl

from pipeline.features.base import Feature, FeatureBuilder, FeatureBuildError, FeatureExpr

BPS = 1e4

# Колонки верхнего уровня стакана, которые нужны OFI.
_TOP = ("bid_px_0", "bid_qty_0", "ask_px_0", "ask_qty_0")


def _int_tuple(values: Iterable[int], name: str, minimum: int) -> tuple[int, ...]:
    out = tuple(values)
    if not out or len(set(out)) != len(out):
        raise FeatureBuildError(f"{name}: нужен непустой набор без повторов, получено {out!r}")
    for v in out:
        if isinstance(v, bool) or not isinstance(v, int) or v < minimum:
            raise FeatureBuildError(f"{name}: значения - целые >= {minimum}, получено {v!r}")
    return out


# ------------------------------------------------------------------ агрегаты стакана


def book_aggregations(levels: int) -> list[pl.Expr]:
    """Выражения для TimelineAligner.attach_book (сворачивают сообщения корзины решения).

    Колонки результата:
      bid_px_0, ask_px_0                   лучшие цены на конец корзины (последнее сообщение);
      bid_qty_i, ask_qty_i, i < levels     объёмы уровней на конец корзины;
      ofi_sum                              Order Flow Imbalance верхнего уровня (Cont, Kukanov, Stoikov)
                                           за корзину, в базовом активе.

    Для каждого сообщения n относительно предыдущего n-1 В ТОЙ ЖЕ корзине:

        e_n = [bid_px_n >= bid_px_{n-1}] * bid_qty_n  -  [bid_px_n <= bid_px_{n-1}] * bid_qty_{n-1}
            - [ask_px_n <= ask_px_{n-1}] * ask_qty_n  +  [ask_px_n >= ask_px_{n-1}] * ask_qty_{n-1}

    ofi_sum = sum(e_n) > 0 - давление покупателей (бид растёт или ask «съедается»). Переход от
    последнего сообщения прошлой корзины к первому текущему не учитывается (состояние прошлой корзины
    в агрегат не пускаем): это занижает |ofi_sum| на один шаг из ~120 при обновлении раз в 500 мс.
    Корзина из одного сообщения - ofi_sum = null (потока нет, а не «нулевой поток»).

    Порядок сообщений внутри корзины - по recv_ts (так их сортирует выравниватель), shift(1) берёт
    предыдущее сообщение того же (symbol, asof_ts).
    """
    if isinstance(levels, bool) or not isinstance(levels, int) or levels < 1:
        raise FeatureBuildError(f"levels должен быть целым >= 1, получено {levels!r}")

    bp, bq, ap, aq = (pl.col(c) for c in _TOP)
    bp0, bq0, ap0, aq0 = (e.shift(1) for e in (bp, bq, ap, aq))
    flag = lambda cond: cond.cast(pl.Float64)  # noqa: E731
    e_n = flag(bp >= bp0) * bq - flag(bp <= bp0) * bq0 - flag(ap <= ap0) * aq + flag(ap >= ap0) * aq0

    aggs = [pl.col("bid_px_0").last().alias("bid_px_0"), pl.col("ask_px_0").last().alias("ask_px_0")]
    for i in range(levels):
        aggs.append(pl.col(f"bid_qty_{i}").last().alias(f"bid_qty_{i}"))
        aggs.append(pl.col(f"ask_qty_{i}").last().alias(f"ask_qty_{i}"))
    aggs.append(pl.when(pl.len() > 1).then(e_n.sum()).alias("ofi_sum"))
    return aggs


def book_levels(columns: Iterable[str]) -> int:
    """Сколько уровней bid_qty_i / ask_qty_i есть в колонках стакана (для book_aggregations)."""
    names = set(columns)
    levels = 0
    while f"bid_qty_{levels}" in names and f"ask_qty_{levels}" in names:
        levels += 1
    if levels == 0:
        raise FeatureBuildError("В стакане нет колонок bid_qty_0 / ask_qty_0")
    return levels


# ----------------------------------------------------------------------- helpers


def _depth(fb: FeatureBuilder, side: str, levels: int) -> FeatureExpr:
    """Суммарный объём первых `levels` уровней стороны. Если глубже стакана нет - null."""
    total = fb.col(f"{side}_qty_0")
    for i in range(1, levels):
        total = total + fb.col(f"{side}_qty_{i}")
    return total


def _mid(fb: FeatureBuilder) -> FeatureExpr:
    return (fb.col("ask_px_0") + fb.col("bid_px_0")) * 0.5


# ----------------------------------------------------------------------- признаки


class OrderBookImbalance(Feature):
    """OBI по глубине k: (bid_depth_k - ask_depth_k) / (bid_depth_k + ask_depth_k), в [-1, 1].

    > 0 - объём на бидах больше (давление покупателей). Выходы: obi_{k} для k из levels.
    """

    name = "order_book_imbalance"

    def __init__(self, levels: Iterable[int] = (1, 3, 5)):
        self.levels = _int_tuple(levels, "levels", 1)

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        out = {}
        for k in self.levels:
            bid, ask = _depth(fb, "bid", k), _depth(fb, "ask", k)
            out[f"obi_{k}"] = (bid - ask) / (bid + ask)
        return out


class Spread(Feature):
    """Спред: spread = ask_px_0 - bid_px_0 (в цене) и spread_bps = spread / mid * 1e4."""

    name = "spread"

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        spread = fb.col("ask_px_0") - fb.col("bid_px_0")
        return {"spread": spread, "spread_bps": spread / _mid(fb) * BPS}


class Microprice(Feature):
    """Отклонение microprice от mid, в bps от mid.

    microprice = (ask_px_0 * bid_qty_0 + bid_px_0 * ask_qty_0) / (bid_qty_0 + ask_qty_0): цена,
    сдвинутая к стороне с МЕНЬШИМ объёмом (там ликвидность скоро кончится). Тождественно
    microprice - mid = spread * obi_1 / 2, но линейная модель из spread_bps и obi_1 такое
    произведение не соберёт, поэтому оно вынесено в отдельный признак.
    """

    name = "microprice"

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        bid_px, ask_px = fb.col("bid_px_0"), fb.col("ask_px_0")
        bid_q, ask_q = fb.col("bid_qty_0"), fb.col("ask_qty_0")
        micro = (ask_px * bid_q + bid_px * ask_q) / (bid_q + ask_q)
        mid = _mid(fb)
        return {"microprice_dev_bps": (micro - mid) / mid * BPS}


class OrderFlow(Feature):
    """Order Flow Imbalance (агрегат ofi_sum из book_aggregations), нормированный на глубину стакана.

    ofi_norm = ofi_sum / (bid_depth_k + ask_depth_k) на конец корзины, k = depth_levels. Накопленный поток
    за окно: ofi_norm_{w} = сумма ofi_norm за w последних решений (rolling_mean * w). Окна - в барах
    сетки решений (1m): поток копится только внутри сегмента без разрывов.
    """

    name = "order_flow"

    def __init__(self, windows: Iterable[int] = (5, 15), depth_levels: int = 5):
        self.windows = _int_tuple(windows, "windows", 2)
        self.depth_levels = _int_tuple((depth_levels,), "depth_levels", 1)[0]

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        depth = _depth(fb, "bid", self.depth_levels) + _depth(fb, "ask", self.depth_levels)
        ofi = fb.col("ofi_sum") / depth
        out = {"ofi_norm": ofi}
        for w in self.windows:
            out[f"ofi_norm_{w}"] = ofi.rolling_mean(w) * float(w)
        return out


class VolumeFlow(Feature):
    """Накопленный объём за скользящее окно и его отношение к «обычному» уровню.

    vol_sum_{w}   сумма volume за w последних баров (абсолют, в базовом активе);
    vol_rel_{w}   rolling_mean(volume, w) / rolling_mean(volume, baseline_window): >1 - торговля
                  активнее обычной (безразмерно, годится в модель);
    taker_imb_{w} (если taker_flow=True) агрессия покупателей против продавцов за окно:
                  (2 * taker_buy_base - volume) / volume по суммам окна, в [-1, 1].

    Это поток по свечам (volume, taker_buy_base), а не профиль по ценовым уровням: гистограмма объёма
    по цене требует сделок, которых в данных нет.
    """

    name = "volume_flow"

    def __init__(self, windows: Iterable[int] = (5, 30), baseline_window: int = 120, taker_flow: bool = False):
        self.windows = _int_tuple(windows, "windows", 1)
        self.baseline_window = _int_tuple((baseline_window,), "baseline_window", 2)[0]
        if self.baseline_window <= max(self.windows):
            raise FeatureBuildError("baseline_window должен быть больше всех windows")
        self.taker_flow = bool(taker_flow)

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        volume = fb.col("volume")
        baseline = volume.rolling_mean(self.baseline_window)
        out = {}
        for w in self.windows:
            mean_w = volume.rolling_mean(w)
            out[f"vol_sum_{w}"] = mean_w * float(w)
            out[f"vol_rel_{w}"] = mean_w / baseline
            if self.taker_flow:
                buy_w = fb.col("taker_buy_base").rolling_mean(w)
                out[f"taker_imb_{w}"] = (buy_w * 2.0 - mean_w) / mean_w
        return out


class ShortTermMomentum(Feature):
    """Лог-доходности за n баров (ret_{n}) и реализованная волатильность минутных доходностей (rv_{w})."""

    name = "short_term_momentum"

    def __init__(self, returns: Iterable[int] = (1, 5), vol_window: int = 30):
        self.returns = _int_tuple(returns, "returns", 1)
        self.vol_window = _int_tuple((vol_window,), "vol_window", 2)[0]

    def build(self, fb: FeatureBuilder) -> Mapping[str, FeatureExpr]:
        close = fb.col("close")
        out = {f"ret_{n}": (close / close.shift(n)).log() for n in self.returns}
        out[f"rv_{self.vol_window}"] = (close / close.shift(1)).log().rolling_std(self.vol_window)
        return out


def default_features(*, taker_flow: bool = False) -> list[Feature]:
    """Набор по умолчанию (см. классы). taker_flow=True требует колонку taker_buy_base (свечи с биржи)."""
    return [
        OrderBookImbalance(),
        Spread(),
        Microprice(),
        OrderFlow(),
        VolumeFlow(taker_flow=taker_flow),
        ShortTermMomentum(),
    ]


def model_feature_names(names: Iterable[str]) -> tuple[str, ...]:
    """Из выходных колонок оставляет пригодные для модели: безразмерные или в bps.

    Абсолютные spread (в цене) и vol_sum_* (в базовом активе) в таблице остаются для анализа и
    фильтров, но в обучение не идут: их масштаб дрейфует вместе с ценой и объёмами рынка.
    """
    return tuple(n for n in names if n != "spread" and not n.startswith("vol_sum_"))
