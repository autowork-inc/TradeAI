"""«Взгляд стратегии» для Live-Dashboard (1.8.0): что стратегия видит на последней ЗАКРЫТОЙ свече - уровни, линии, зоны, чек-лист условий входа по сторонам.

Считается тем же кодом, что торгует (детекторы strategies/*, параметры с одобренными изменениями - registry.build_strategies), но бот этот модуль
не вызывает: только сервис watch (playground/watch_server.py) для показа. Решение о сделке - только сигнал детектора (stage 3); чек-лист - ориентир:
пороги взяты из параметров стратегии, а часть условий (свеча пробоя, объём, полнотелость) проверяется лишь на самой сигнальной свече.

view(strategy, ctx) -> {strategy, symbol, interval, bar_time, summary, sides: {long|short: {stage, text, checks: [{label, ok, value}]}},
                        geometry: [{type: line|zone|marker|series, ...}], signals: [...]}
stage: 0 - сетапа нет, 1 - стратегия наблюдает (есть уровень/диапазон/линия), 2 - почти (большая часть условий выполнена), 3 - сигнал на этой свече.
"""

from typing import Any, Callable

import polars as pl

from features.htf import BIAS_COLUMN
from features.mtf import D1_BIAS
from orchestrator.types import FeatureContext
from strategies.chr_horizontal import detect_chr_horizontal
from strategies.chr_trendline import detect_chr_trendline
from strategies.horizontal_breakout import detect_horizontal_breakout
from strategies.killzones import detect_killzones
from strategies.smc_setups import detect_smc
from strategies.trendline_breakout import detect_trendline_breakout
from strategies.vwap_reversion import detect_vwap_reversion

DAY_MS = 86_400_000
SIDES = ("long", "short")
BIAS_RU = {1: "Bullish", 0: "Neutral", -1: "Bearish", None: "нет данных"}
SMC_TEXT = {0: "ждёт свип пула ликвидности (1h)", 1: "свип был - ждёт FVG", 2: "свип + FVG - ждёт CHoCH", 3: "CHoCH отклонён", 4: "СИГНАЛ: лимитка на 50% FVG"}


def _f(v: Any) -> float | None:
    return None if v is None else float(v)


def _chk(label: str, ok: bool | None, value: str = "") -> dict[str, Any]:
    return {"label": label, "ok": ok, "value": value}


def _atr(x: float | None) -> str:
    return "—" if x is None else f"{x:.2f} ATR"


def _line(t1: int, t2: int, p1: float | None, p2: float | None, label: str, side: str | None = None) -> dict[str, Any] | None:
    if p1 is None or p2 is None or t1 is None or t2 is None:
        return None
    return {"type": "line", "t1": int(t1), "t2": int(t2), "p1": float(p1), "p2": float(p2), "label": label, "side": side}


def _zone(t1: int, t2: int, lo: float | None, hi: float | None, label: str, side: str | None = None) -> dict[str, Any] | None:
    if lo is None or hi is None or t1 is None:
        return None
    return {"type": "zone", "t1": int(t1), "t2": int(t2), "p1": float(lo), "p2": float(hi), "label": label, "side": side}


def _marker(t: int, p: float | None, label: str, side: str | None = None) -> dict[str, Any] | None:
    if p is None or t is None:
        return None
    return {"type": "marker", "t1": int(t), "t2": int(t), "p1": float(p), "p2": float(p), "label": label, "side": side}


def _series(frame: pl.DataFrame, col: str, label: str, since: int) -> dict[str, Any] | None:
    part = frame.filter(pl.col("open_time") >= since).select("open_time", col).drop_nulls()
    if part.height < 2:
        return None
    return {"type": "series", "label": label, "points": [[int(t), float(v)] for t, v in part.iter_rows()]}  # выгрузка для графика, не математика паттернов


def _trend_ok(side: str, bias: int | None) -> bool:
    return bias == (1 if side == "long" else -1)


def _stage(checks: list[dict], context: bool, signal: bool) -> int:
    if signal:
        return 3
    if not context:
        return 0
    known = [c for c in checks if c["ok"] is not None]
    score = sum(1 for c in known if c["ok"]) / len(known) if known else 0.0
    return 2 if score >= 0.75 else 1


def _side(checks: list[dict], context: bool, signal: bool, text: str) -> dict[str, Any]:
    stage = _stage(checks, context, signal)
    return {"stage": stage, "text": "СИГНАЛ" if stage == 3 and not text.startswith("СИГНАЛ") else text, "checks": checks}


def _dist(close: float, level: float | None, atr: float | None) -> float | None:
    if level is None or not atr:
        return None
    return abs(level - close) / atr


# ------------------------------------------------------------------------------------------ стратегии


def _smc(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    det = detect_smc(ctx.frame, s.params)
    last = det.row(det.height - 1, named=True)
    bias = last.get(D1_BIAS)
    t, close = ctx.bar_open_time, ctx.last_close
    sides, geo = {}, []
    for side in SIDES:
        st = int(last.get(f"{side}_state") or 0)
        pool = _f(last.get(f"{side}_pool_level"))
        checks = [_chk("тренд 1d по стороне", _trend_ok(side, bias), BIAS_RU.get(bias)),
                  _chk("пул ликвидности (1h)", pool is not None, "—" if pool is None else f"{pool:g}"),
                  _chk("свип пула", st >= 1, ""), _chk("FVG после свипа", st >= 2, ""), _chk("CHoCH", st >= 4, "")]
        sides[side] = _side(checks, pool is not None, bool(last.get(f"{side}_signal")), SMC_TEXT.get(st, f"S{st}"))
        geo += [_line(last.get(f"{side}_pool_time"), t, pool, pool, "пул " + ("снизу" if side == "long" else "сверху"), side)]
        if st >= 1:
            geo.append(_marker(t, _f(last.get(f"{side}_sweep_extreme")), "свип", side))
        if st >= 2:
            geo.append(_zone(last.get(f"{side}_fvg_t1"), t, _f(last.get(f"{side}_fvg_lo")), _f(last.get(f"{side}_fvg_hi")), "FVG", side))
    best = max(sides.values(), key=lambda x: x["stage"])
    return sides, geo, f"1d: {BIAS_RU.get(bias)}; {best['text']}"


def _counter(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    f = ctx.frame
    last = f.row(f.height - 1, named=True)
    bias, atr, close, t, p = last.get(BIAS_COLUMN), _f(last["atr"]), ctx.last_close, ctx.bar_open_time, s.params
    sides, geo = {}, []
    for side, pool_col, time_col in (("short", "sh1_price", "sh1_time"), ("long", "sl1_price", "sl1_time")):
        pool = _f(last.get(pool_col))
        against = bias == (1 if side == "short" else -1)  # строго против H4
        d = _dist(close, pool, atr)
        checks = [_chk("H4 строго против стороны", against, BIAS_RU.get(bias)), _chk("пул (последний свинг)", pool is not None, "—" if pool is None else f"{pool:g}"),
                  _chk(f"свип глубже {p.min_sweep_atr:g} ATR", None, f"до пула {_atr(d)}"),
                  _chk(f"объём > {p.volume_mult:g}×SMA20", None, f"сейчас ×{_f(last.get('vol_ratio')) or 0:.2f}")]
        text = "ждёт глубокий свип пула и возврат" if against else "H4 не против этой стороны - не торгует"
        sides[side] = _side(checks, pool is not None and against, False, text)
        geo.append(_line(last.get(time_col), t, pool, pool, "пул " + ("сверху" if side == "short" else "снизу"), side))
    return sides, geo, f"H4: {BIAS_RU.get(bias)} - контртренд"


def _obi(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    f = ctx.frame
    last = f.row(f.height - 1, named=True)
    atr, close, t = _f(last["atr"]), ctx.last_close, ctx.bar_open_time
    sides, geo = {}, []
    for side, col, tcol in (("long", "sh1_price", "sh1_time"), ("short", "sl1_price", "sl1_time")):
        lvl = _f(last.get(col))
        checks = [_chk("уровень BOS (свинг)", lvl is not None, "—" if lvl is None else f"{lvl:g}"), _chk("поджатие к уровню", bool(last.get("buildup")), ""),
                  _chk("стакан / лента (OBI, дельта)", None, "в Dashboard не показываются - смотрит только бот на live-монетах")]
        sides[side] = _side(checks, lvl is not None, False, f"ждёт пробой уровня, до него {_atr(_dist(close, lvl, atr))}")
        geo.append(_line(last.get(tcol), t, lvl, lvl, "уровень BOS", side))
    return sides, geo, "пробой структуры с подтверждением стакана/ленты"


def _ict(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_killzones(ctx.frame, ctx.interval_ms, p)
    last = det.row(det.height - 1, named=True)
    t, close, atr = ctx.bar_open_time, ctx.last_close, _f(last["atr"])
    hi, lo, sess, rng = _f(last.get("kz_asia_high")), _f(last.get("kz_asia_low")), last.get("kz_session"), _f(last.get("kz_range_atr"))
    day0 = t - t % DAY_MS
    hour = (t % DAY_MS) // 3_600_000
    sess_ru = {"london": "Killzone Лондон", "new_york": "Killzone Нью-Йорк"}.get(sess) or ("Азия: строится диапазон" if hour < p.asia_end_h else "вне killzone")
    sides, geo = {}, [_zone(day0 + p.asia_start_h * 3_600_000, day0 + p.asia_end_h * 3_600_000, lo, hi, "диапазон Азии")]
    for side, level, name in (("short", hi, "Asia High"), ("long", lo, "Asia Low")):
        checks = [_chk(f"ширина Азии ≥ {p.min_asia_range_atr:g} ATR", None if rng is None else rng >= p.min_asia_range_atr, _atr(rng)),
                  _chk("идёт killzone", sess in ("london", "new_york"), sess_ru),
                  _chk(f"прокол ≥ {p.min_sweep_depth_atr:g} ATR и возврат", None, f"до {name}: {_atr(_dist(close, level, atr))}")]
        if p.require_htf_trend:
            checks.append(_chk("тренд H4 по стороне", _trend_ok(side, last.get(BIAS_COLUMN)), BIAS_RU.get(last.get(BIAS_COLUMN))))
        sides[side] = _side(checks, level is not None, bool(last.get(f"kz_{side}_signal")), f"ждёт ложный пробой {name} в killzone")
        geo.append(_line(day0, ctx.bar_close_ms, level, level, name, side))
    return sides, geo, sess_ru


def _vwap(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_vwap_reversion(ctx.frame, p)
    last = det.row(det.height - 1, named=True)
    day0 = ctx.bar_open_time - ctx.bar_open_time % DAY_MS
    bias, atr, close = last.get(BIAS_COLUMN), _f(last["atr"]), ctx.last_close
    geo = [_series(det, "vw_vwap", "VWAP", day0), _series(det, "vw_upper", f"+{p.sd_mult:g}σ", day0), _series(det, "vw_lower", f"−{p.sd_mult:g}σ", day0)]
    sides = {}
    for side, band_col in (("short", "vw_upper"), ("long", "vw_lower")):
        band = _f(last.get(band_col))
        d = _dist(close, band, atr)
        checks = [_chk("тренд H4 по стороне", _trend_ok(side, bias) if p.require_htf_trend else None, BIAS_RU.get(bias)),
                  _chk(f"касание полосы {p.sd_mult:g}σ", None if d is None else d <= 0.3, f"до полосы {_atr(d)}"),
                  _chk("разворотная свеча", None, "поглощение / падающая звезда на касании")]
        sides[side] = _side(checks, band is not None, bool(last.get(f"vw_{side}_signal")), "ждёт касание полосы и разворот; тейк - линия VWAP")
    sd = _f(last.get("vw_sd"))
    vw = _f(last.get("vw_vwap"))
    z = None if not sd or vw is None else (close - vw) / sd
    return sides, geo, "VWAP дня не готов" if z is None else f"цена {z:+.1f}σ от VWAP"


def _horizontal(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_horizontal_breakout(ctx.frame, p)
    last = det.row(det.height - 1, named=True)
    bias, atr, close, t = last.get(BIAS_COLUMN), _f(last["atr"]), ctx.last_close, ctx.bar_open_time
    hi, lo, box_atr, eff = _f(last.get("hb_box_hi")), _f(last.get("hb_box_lo")), _f(last.get("hb_box_atr")), _f(last.get("hb_efficiency"))
    box_ok = box_atr is not None and p.min_box_atr <= box_atr <= p.max_box_atr and eff is not None and eff <= p.max_efficiency
    geo = [_zone(last.get("hb_box_t1"), ctx.bar_close_ms, lo, hi, f"коробка {p.window} баров")]
    sides = {}
    for side, edge in (("long", hi), ("short", lo)):
        checks = [_chk(f"высота коробки {p.min_box_atr:g}-{p.max_box_atr:g} ATR", None if box_atr is None else p.min_box_atr <= box_atr <= p.max_box_atr, _atr(box_atr)),
                  _chk(f"боковик (ER ≤ {p.max_efficiency:g})", None if eff is None else eff <= p.max_efficiency, "—" if eff is None else f"ER {eff:.2f}"),
                  _chk("H4 не против", None if bias is None else bias != (-1 if side == "long" else 1), BIAS_RU.get(bias)),
                  _chk(f"закрытие за границей ≤ {p.max_extension_atr:g} ATR, объём ≥ {p.volume_mult:g}×", None, f"до границы {_atr(_dist(close, edge, atr))}")]
        sides[side] = _side(checks, box_ok, bool(last.get(f"hb_{side}_signal")), "ждёт пробой коробки" if box_ok else "коробки нет (не боковик или не та высота)")
    return sides, geo, "консолидация найдена" if box_ok else "консолидации нет"


def _trendline(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_trendline_breakout(ctx.frame, ctx.interval_ms, p, keep_internal=True)
    last = det.row(det.height - 1, named=True)
    bias, close, t, atr = last.get(BIAS_COLUMN), ctx.last_close, ctx.bar_open_time, _f(last["atr"])
    sides, geo = {}, []
    for side, k, name in (("long", "r", "сопротивление"), ("short", "s", "поддержка")):
        valid, now = bool(last.get(f"_tl_{k}_valid")), _f(last.get(f"_tl_{k}_now"))
        viol, touches = last.get(f"_tl_{k}_viol"), last.get(f"_tl_{k}_touches")
        checks = [_chk("линия по 2 свингам", valid, f"наклон {_f(last.get(f'_tl_{k}_slope_atr')) or 0:.2f} ATR/бар"),
                  _chk("ни одного закрытия за линией", None if viol is None else viol == 0, f"нарушений {viol}"),
                  _chk("H4 не против", None if bias is None else bias != (-1 if side == "long" else 1), BIAS_RU.get(bias)),
                  _chk("пробой / свип линии", None, f"до линии {_atr(_dist(close, now, atr))}")]
        sides[side] = _side(checks, valid, bool(last.get(f"tl_{side}_signal")), f"ждёт пробой линии ({name})" if valid else f"нет годной линии ({name})")
        geo.append(_line(last.get(f"_tl_{k}_t2"), ctx.bar_close_ms, _f(last.get(f"_tl_{k}_p2")), now, f"линия: {name}", side))
    return sides, geo, f"H4: {BIAS_RU.get(bias)}"


def _chr_checks(p, last: dict, pre: str, side: str, close: float, level: float | None, atr: float | None) -> list[dict]:
    bias = last.get(BIAS_COLUMN)
    touches, decay = last.get(pre + "touches"), _f(last.get(pre + "comp_decay"))
    rub, height, risk = _f(last.get(pre + "comp_rub_atr")), _f(last.get(pre + "comp_height_atr")), _f(last.get(pre + "risk_atr"))
    return [_chk("тренд H4 по стороне (жёстко)", _trend_ok(side, bias), BIAS_RU.get(bias)),
            _chk(f"касаний ≥ {p.min_touches}", None if touches is None else touches >= p.min_touches, f"{touches}"),
            _chk(f"затухание размаха ≤ {p.comp_decay:g}", None if decay is None else decay <= p.comp_decay, "—" if decay is None else f"{decay:.2f}"),
            _chk(f"трение об уровень ≤ {p.comp_rub_atr:g} ATR", None if rub is None else rub <= p.comp_rub_atr, _atr(rub)),
            _chk(f"узкая база ≤ {p.comp_height_atr:g} ATR", None if height is None else height <= p.comp_height_atr, _atr(height)),
            _chk(f"стоп ≤ {p.max_risk_atr:g} ATR", None if risk is None else risk <= p.max_risk_atr, _atr(risk)),
            _chk("пробой полнотелой свечой", None, f"до уровня {_atr(_dist(close, level, atr))}")]


def _chr_h(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_chr_horizontal(ctx.frame, p, keep_internal=True)
    last = det.row(det.height - 1, named=True)
    close, atr, t0 = ctx.last_close, _f(last["atr"]), last.get("_chh_t_start")
    sides, geo = {}, []
    for side in SIDES:
        pre = f"_chh_{side}_"
        level = _f(last.get(pre + "level"))
        checks = _chr_checks(p, last, pre, side, close, level, atr)
        sides[side] = _side(checks, level is not None, bool(last.get(f"chh_{side}_signal")), "ждёт поджатие и пробой уровня")
        geo += [_line(t0, ctx.bar_close_ms, level, level, "уровень", side),
                _line(last.get(pre + "squeeze_start_t"), last.get(pre + "squeeze_last_t"), _f(last.get(pre + "squeeze_2")), _f(last.get(pre + "squeeze_1")), "поджатие", side)]
    return sides, geo, f"H4: {BIAS_RU.get(last.get(BIAS_COLUMN))}"


def _chr_t(s, ctx: FeatureContext) -> tuple[dict, list, str]:
    p = s.params
    det = detect_chr_trendline(ctx.frame, ctx.interval_ms, p, keep_internal=True)
    last = det.row(det.height - 1, named=True)
    close, atr = ctx.last_close, _f(last["atr"])
    sides, geo = {}, []
    for side in SIDES:
        pre = f"_cht_{side}_"
        level, slope = _f(last.get(pre + "level")), _f(last.get(f"_cht_{side}_slope"))
        good_dir = slope is not None and ((slope < 0) if side == "long" else (slope > 0))
        checks = [_chk("наклон: сопротивление вниз (лонг) / поддержка вверх (шорт)", good_dir, f"{_f(last.get(pre + 'slope_atr')) or 0:+.2f} ATR/бар"),
                  *_chr_checks(p, last, pre, side, close, level, atr)]
        sides[side] = _side(checks, level is not None and good_dir, bool(last.get(f"cht_{side}_signal")), "ждёт поджатие к наклонке и пробой")
        if good_dir:
            geo += [_line(last.get(pre + "anchor1_time"), ctx.bar_close_ms, _f(last.get(pre + "anchor1_price")), level, "наклонная линия", side),
                    _line(last.get(pre + "squeeze_start_t"), last.get(pre + "squeeze_last_t"), _f(last.get(pre + "squeeze_2")), _f(last.get(pre + "squeeze_1")),
                          "поджатие", side)]
    return sides, geo, f"H4: {BIAS_RU.get(last.get(BIAS_COLUMN))}"


BUILDERS: dict[str, Callable] = {
    "SMC_Pro_Trend": _smc, "SMC_Counter_Trend": _counter, "OBI_Breakout": _obi, "ICT_Killzones": _ict, "VWAP_Reversion": _vwap,
    "Horizontal_Breakout": _horizontal, "Trendline_Breakout": _trendline, "Horizontal_Breakout_CHR": _chr_h, "Trendline_Breakout_CHR": _chr_t,
}


def view(strategy, ctx: FeatureContext, candles: int = 150) -> dict[str, Any]:
    """Взгляд стратегии на закрытой свече ctx (контекст со старшими таймфреймами, как у оркестратора)."""
    sides, geo, summary = BUILDERS[strategy.name](strategy, ctx)
    analyze = getattr(strategy, "analyze", None)
    if strategy.name == "OBI_Breakout":
        signals = ()  # без стакана и ленты (MicroStructure) детектор не считается: показываются только уровни
    else:
        signals = analyze(ctx)[0] if analyze is not None else strategy.detect(ctx)
    for sig in signals:  # настоящий сигнал детектора: его геометрия (вход, SL, линии) - поверх
        geo += [{**g, "side": sig.direction.value, "signal": True} for g in sig.geometry]
        sides[sig.direction.value]["stage"] = 3
    cols = [c for c in ("open_time", "open", "high", "low", "close", "volume") if c in ctx.frame.columns]
    return {"strategy": strategy.name, "symbol": ctx.symbol, "interval": ctx.interval, "bar_time": ctx.bar_open_time, "summary": summary, "sides": sides,
            "geometry": [g for g in geo if g is not None], "candles": ctx.frame.tail(candles).select(cols).to_dicts(),
            "signals": [{"side": s.direction.value, "kind": s.kind.value, "entry": s.entry, "sl": s.sl, "tp": s.tp} for s in signals]}
