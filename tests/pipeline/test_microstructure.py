"""Микроструктурные признаки: ожидаемые числа посчитаны вручную (в комментариях), построение - только через фасад."""

import math

import polars as pl
import pytest

from pipeline.features.base import (
    FeatureBuildError,
    FeatureBuilder,
    apply_features,
    compile_features,
    source_columns,
)
from pipeline.features.microstructure import (
    Microprice,
    OrderBookImbalance,
    OrderFlow,
    ShortTermMomentum,
    Spread,
    VolumeFlow,
    book_aggregations,
    book_levels,
    default_features,
    model_feature_names,
)


def frame(**cols) -> pl.DataFrame:
    n = len(next(iter(cols.values())))
    base = {"symbol": ["A"] * n, "segment_id": [0] * n, "asof_ts": [1000 * (i + 1) for i in range(n)]}
    return pl.DataFrame({**base, **cols})


def features(df: pl.DataFrame, *feats) -> pl.DataFrame:
    compiled = compile_features(feats, source_columns(df.lazy()))
    return apply_features(df.lazy(), compiled).collect()


# ------------------------------------------------------------------ агрегаты стакана


def test_ofi_aggregation_matches_hand_calculation():
    # Корзина a (порядок сообщений = порядок строк), (bid_px, bid_qty, ask_px, ask_qty):
    #   m0 (100, 5, 101, 4)
    #   m1 (100, 7, 101, 3): бид прежний -> +7 - 5; ask прежний -> -3 + 4          = 3
    #   m2 (101, 2, 102, 6): бид вырос -> +2; ask вырос -> +aq_prev(3)             = 5
    #   m3 (100.5, 9, 102, 1): бид упал -> -bq_prev(2); ask прежний -> -1 + 6      = 3
    # ofi_sum = 3 + 5 + 3 = 11. Корзина b из одного сообщения: потока нет -> null.
    book = pl.DataFrame({
        "g": ["a", "b", "a", "a", "a"],
        "bid_px_0": [100.0, 50.0, 100.0, 101.0, 100.5], "bid_qty_0": [5.0, 1.0, 7.0, 2.0, 9.0],
        "ask_px_0": [101.0, 51.0, 101.0, 102.0, 102.0], "ask_qty_0": [4.0, 1.0, 3.0, 6.0, 1.0],
    })
    out = book.group_by("g").agg(book_aggregations(1)).sort("g")
    assert out["ofi_sum"].to_list() == [11.0, None]
    assert out["bid_px_0"].to_list() == [100.5, 50.0]  # состояние на конец корзины
    assert out["ask_qty_0"].to_list() == [1.0, 1.0]
    assert set(out.columns) == {"g", "bid_px_0", "ask_px_0", "bid_qty_0", "ask_qty_0", "ofi_sum"}


def test_book_aggregations_cover_requested_levels():
    cols = {a.meta.output_name() for a in book_aggregations(3)}
    assert {f"{s}_qty_{i}" for s in ("bid", "ask") for i in range(3)} <= cols
    assert book_levels(["bid_qty_0", "ask_qty_0", "bid_qty_1", "ask_qty_1", "bid_qty_3"]) == 2
    with pytest.raises(FeatureBuildError):
        book_levels(["close"])
    with pytest.raises(FeatureBuildError):
        book_aggregations(0)


# ------------------------------------------------------------------------ признаки


def test_obi_spread_microprice_hand_values():
    # bid: 6 + 3, ask: 2 + 5. obi_1 = (6-2)/(6+2) = 0.5; obi_2 = (9-7)/(9+7) = 0.125
    # spread = 100.1 - 99.9 = 0.2, mid = 100 -> 20 bps
    # microprice = (100.1*6 + 99.9*2)/8 = 100.05 -> +5 bps к mid (= spread * obi_1 / 2 = 0.1 * 0.5)
    df = frame(
        bid_px_0=[99.9], ask_px_0=[100.1], bid_qty_0=[6.0], ask_qty_0=[2.0], bid_qty_1=[3.0], ask_qty_1=[5.0],
    )
    out = features(df, OrderBookImbalance((1, 2)), Spread(), Microprice())
    assert out["obi_1"][0] == pytest.approx(0.5)
    assert out["obi_2"][0] == pytest.approx(0.125)
    assert out["spread"][0] == pytest.approx(0.2)
    assert out["spread_bps"][0] == pytest.approx(20.0)
    assert out["microprice_dev_bps"][0] == pytest.approx(5.0)
    assert out["microprice_dev_bps"][0] == pytest.approx(out["spread_bps"][0] * out["obi_1"][0] / 2)


def test_book_features_are_null_without_book_or_depth():
    # пустой стакан (aggs обнулены выравнивателем) и нулевая глубина дают null, а не inf/NaN/0
    df = frame(
        bid_px_0=[None, 100.0], ask_px_0=[None, 100.2], bid_qty_0=[None, 0.0], ask_qty_0=[None, 0.0],
        bid_qty_1=[None, 1.0], ask_qty_1=[None, 1.0],
    ).cast({"bid_px_0": pl.Float64, "ask_px_0": pl.Float64, "bid_qty_0": pl.Float64, "ask_qty_0": pl.Float64,
            "bid_qty_1": pl.Float64, "ask_qty_1": pl.Float64})
    out = features(df, OrderBookImbalance((1, 2)), Spread(), Microprice())
    assert out["obi_1"].to_list() == [None, None]  # 0 / 0
    assert out["obi_2"].to_list() == [None, 0.0]
    assert out["spread_bps"][0] is None and out["microprice_dev_bps"].to_list() == [None, None]


def test_order_flow_normalisation_and_window_sum():
    # глубина (k=2) = 9 + 7 = 16 ; ofi_norm = ofi_sum / 16 = [0.5, 1.0, -0.5]
    # накопленный за 2 бара: [null, 0.5 + 1.0, 1.0 - 0.5] = [null, 1.5, 0.5]
    df = frame(
        ofi_sum=[8.0, 16.0, -8.0], bid_qty_0=[6.0] * 3, ask_qty_0=[2.0] * 3, bid_qty_1=[3.0] * 3, ask_qty_1=[5.0] * 3,
    )
    out = features(df, OrderFlow(windows=(2,), depth_levels=2))
    assert out["ofi_norm"].to_list() == pytest.approx([0.5, 1.0, -0.5])
    assert out["ofi_norm_2"].to_list()[0] is None
    assert out["ofi_norm_2"].to_list()[1:] == pytest.approx([1.5, 0.5])


def test_volume_flow_accumulated_volume_and_relative_level():
    # volume 1..6, окно 2: накоплено [null, 3, 5, 7, 9, 11]
    # vol_rel_2 = mean2 / mean4: на баре 4 (индекс 3) 3.5 / 2.5 = 1.4 ; индекс 4: 4.5 / 3.5 ; индекс 5: 5.5 / 4.5
    # taker_buy = 0.75 * volume -> (2 * 0.75 - 1) = 0.5 по суммам окна
    vol = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    df = frame(volume=vol, taker_buy_base=[0.75 * v for v in vol])
    out = features(df, VolumeFlow(windows=(2,), baseline_window=4, taker_flow=True))
    assert out["vol_sum_2"].to_list() == [None, 3.0, 5.0, 7.0, 9.0, 11.0]
    rel = out["vol_rel_2"].to_list()
    assert rel[:3] == [None, None, None] and rel[3:] == pytest.approx([1.4, 4.5 / 3.5, 5.5 / 4.5])
    assert out["taker_imb_2"].to_list()[1:] == pytest.approx([0.5] * 5)


def test_volume_flow_without_taker_columns_needs_no_taker_buy_base():
    out = features(frame(volume=[1.0, 2.0, 3.0]), VolumeFlow(windows=(2,), baseline_window=3))
    assert "taker_imb_2" not in out.columns
    with pytest.raises(FeatureBuildError):  # taker_flow=True без колонки - ошибка конструирования
        features(frame(volume=[1.0, 2.0, 3.0]), VolumeFlow(windows=(2,), baseline_window=3, taker_flow=True))


def test_momentum_returns_and_realized_vol():
    close = [100.0, 101.0, 99.0, 102.0]
    out = features(frame(close=close), ShortTermMomentum(returns=(1, 2), vol_window=2))
    r1 = [math.log(close[i] / close[i - 1]) for i in range(1, 4)]
    assert out["ret_1"].to_list()[1:] == pytest.approx(r1)
    assert out["ret_2"].to_list()[2:] == pytest.approx([math.log(99 / 100), math.log(102 / 101)])
    # rv_2 = std (ddof=1) двух последних минутных доходностей
    assert out["rv_2"][2] == pytest.approx(pl.Series(r1[:2]).std())
    assert out["rv_2"][3] == pytest.approx(pl.Series(r1[1:]).std())


def test_features_do_not_cross_segments_or_symbols():
    df = pl.DataFrame({
        "symbol": ["A", "A", "A", "B", "B"], "segment_id": [0, 0, 1, 0, 0],
        "asof_ts": [1000, 2000, 3000, 1000, 2000], "close": [100.0, 110.0, 120.0, 10.0, 20.0],
    })
    out = features(df, ShortTermMomentum(returns=(1,), vol_window=2)).sort("symbol", "asof_ts")
    ret = out["ret_1"].to_list()
    assert ret[0] is None and ret[1] == pytest.approx(math.log(1.1))
    assert ret[2] is None  # новый сегмент: цена из прошлого сегмента не берётся
    assert ret[3] is None and ret[4] == pytest.approx(math.log(2.0))  # символ B не видит символ A


def test_lookback_is_reported_for_purge():
    allowed = {"close", "volume", "taker_buy_base", "ofi_sum", "bid_px_0", "ask_px_0",
               *(f"{s}_qty_{i}" for s in ("bid", "ask") for i in range(5))}
    compiled = compile_features(default_features(taker_flow=True), allowed)
    lookback = max(c.lookback.bars_1m for c in compiled)
    assert lookback == 119  # vol_rel_*: baseline_window=120 -> 119 прошлых баров


def test_default_feature_set_names_and_model_filter():
    allowed = {"close", "volume", "ofi_sum", "bid_px_0", "ask_px_0", *(f"{s}_qty_{i}" for s in ("bid", "ask") for i in range(5))}
    compiled = compile_features(default_features(), allowed)
    outputs = [n for c in compiled for n in c.exprs]
    assert len(outputs) == len(set(outputs))
    assert {"obi_1", "obi_3", "obi_5", "spread", "spread_bps", "microprice_dev_bps", "ofi_norm", "ofi_norm_5",
            "vol_sum_5", "vol_rel_5", "ret_1", "rv_30"} <= set(outputs)
    model = model_feature_names(outputs)
    assert "spread" not in model and not any(n.startswith("vol_sum_") for n in model)
    assert "spread_bps" in model and "vol_rel_5" in model


@pytest.mark.parametrize("make", [
    lambda: OrderBookImbalance(()),
    lambda: OrderBookImbalance((0,)),
    lambda: OrderBookImbalance((1, 1)),
    lambda: OrderFlow(windows=(1,)),
    lambda: OrderFlow(depth_levels=0),
    lambda: VolumeFlow(windows=(5,), baseline_window=5),
    lambda: ShortTermMomentum(returns=(0,)),
    lambda: ShortTermMomentum(vol_window=1),
])
def test_bad_parameters_fail_at_construction(make):
    with pytest.raises(FeatureBuildError):
        make()


def test_features_use_only_the_facade():
    # Построение идёт через FeatureBuilder: чужая колонка (в том числе метка) недоступна на этапе конструирования.
    fb = FeatureBuilder({"close"})
    with pytest.raises(FeatureBuildError):
        OrderBookImbalance().build(fb)
