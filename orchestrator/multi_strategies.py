"""Четыре стратегии мульти-стратегийного Shadow (2026-10-11): чистые `detect(FeatureContext) -> tuple[Signal, ...]` поверх векторных детекторов
пакета `strategies/` (R8: один код для бэктеста и live; берётся только последняя строка).

  SMC_Counter_Trend  strategies/counter_trend.py   контртренд H4: глубокий свип (>= 1.5 ATR) + объём > 1.5 SMA20, скальп 1R
  OBI_Breakout       strategies/obi_breakout.py    BOS локального экстремума + дисбаланс стакана / всплеск дельты ленты
  ICT_Killzones      strategies/killzones.py       свип границ Азиатской сессии в killzone Лондона / Нью-Йорка
  VWAP_Reversion     strategies/vwap_reversion.py  возврат от 3-го стандартного отклонения Anchored VWAP (дневной якорь)

Все четыре входят MARKET (по первому тику после закрытия свечи; R и плечо считаются по факту входа в трекере). Планы управления позицией -
orchestrator/registry.py. Состояний автомата сетапа (S0..S4) у них нет: решение принимается на одной закрытой свече.
"""

from typing import Any

import polars as pl

from orchestrator.strategies import _htf_meta, _signal_id
from orchestrator.types import Direction, FeatureContext, Kind, Signal, geometry
from strategies.counter_trend import CounterTrendParams, detect_counter_trend
from strategies.killzones import DAY_MS, HOUR_MS, KillzoneParams, detect_killzones
from strategies.obi_breakout import DELTA_COLUMN, OBI_COLUMN, ObiParams, detect_obi_breakout
from strategies.vwap_reversion import VwapParams, detect_vwap_reversion
from utils.errors import DataError

SMC_COUNTER_TREND = "SMC_Counter_Trend"
OBI_BREAKOUT = "OBI_Breakout"
ICT_KILLZONES = "ICT_Killzones"
VWAP_REVERSION = "VWAP_Reversion"


def _market_signal(name: str, ctx: FeatureContext, direction: Direction, sl: float, geo: tuple[dict[str, Any], ...], meta: dict[str, Any]) -> Signal:
    return Signal(
        signal_id=_signal_id(name, ctx, direction), strategy=name, symbol=ctx.symbol, interval=ctx.interval, direction=direction,
        kind=Kind.MARKET, bar_open_time=ctx.bar_open_time, bar_close_ms=ctx.bar_close_ms, entry=ctx.last_close, sl=sl, tp=None, inval=None,
        geometry=geo, meta=meta,
    )


def _last(det: pl.DataFrame) -> dict[str, Any]:
    return det.row(det.height - 1, named=True)


def _sl_line(ctx: FeatureContext, sl: float) -> dict[str, Any]:
    return geometry("line", ctx.bar_open_time, ctx.bar_close_ms + 8 * ctx.interval_ms, sl, sl, "SL")


class StrategyCounterTrend:
    name = SMC_COUNTER_TREND

    def __init__(self, params: CounterTrendParams | None = None):
        self.params = params or CounterTrendParams()

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        det = detect_counter_trend(ctx.frame, self.params)
        last, prev = _last(det), det.row(det.height - 2, named=True)
        out = []
        for side, pool_time_col in (("short", "sh1_time"), ("long", "sl1_time")):
            if not last[f"ct_{side}_signal"]:
                continue
            direction = Direction(side)
            pool, extreme, sl = last[f"ct_{side}_pool"], last[f"ct_{side}_extreme"], last[f"ct_{side}_sl"]
            t = ctx.bar_open_time
            geo = (
                geometry("line", prev[pool_time_col], t, pool, pool, "пул ликвидности"),
                geometry("marker", t, t, extreme, extreme, f"глубокий свип {last[f'ct_{side}_depth_atr']:.1f} ATR"),
                _sl_line(ctx, sl),
            )
            out.append(_market_signal(self.name, ctx, direction, sl, geo, {
                "pool_level": pool, "sweep_extreme": extreme, "sweep_depth_atr": last[f"ct_{side}_depth_atr"],
                "vol_ratio": last[f"ct_{side}_vol_ratio"], "atr": last["atr"], **_htf_meta(last)}))
        return tuple(out)


class StrategyObiBreakout:
    name = OBI_BREAKOUT

    def __init__(self, params: ObiParams | None = None):
        self.params = params or ObiParams()

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        det = detect_obi_breakout(ctx.frame, self.params)
        last, prev = _last(det), det.row(det.height - 2, named=True)
        out = []
        for side, time_col in (("long", "sh1_time"), ("short", "sl1_time")):
            if not last[f"obi_{side}_signal"]:
                continue
            direction = Direction(side)
            level, sl, t = last[f"obi_{side}_level"], last[f"obi_{side}_sl"], ctx.bar_open_time
            geo = (
                geometry("line", prev[time_col], t, level, level, "уровень BOS"),
                geometry("marker", t, t, ctx.last_close, ctx.last_close, "BOS"),
                _sl_line(ctx, sl),
            )
            out.append(_market_signal(self.name, ctx, direction, sl, geo, {
                "bos_level": level, "obi": last[OBI_COLUMN], "delta": last[DELTA_COLUMN], "vol_ratio": last["vol_ratio"],
                "via_obi": last[f"obi_{side}_via_obi"], "via_delta": last[f"obi_{side}_via_delta"], "atr": last["atr"]}))
        return tuple(out)


class StrategyKillzones:
    name = ICT_KILLZONES

    def __init__(self, params: KillzoneParams | None = None):
        self.params = params or KillzoneParams()

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        det = detect_killzones(ctx.frame, ctx.interval_ms, self.params)
        last = _last(det)
        out = []
        t = ctx.bar_open_time
        day_start = t - t % DAY_MS + self.params.asia_start_h * HOUR_MS
        for side, level_col in (("short", "kz_asia_high"), ("long", "kz_asia_low")):
            if not last[f"kz_{side}_signal"]:
                continue
            direction = Direction(side)
            level, sl = last[level_col], last[f"kz_{side}_sl"]
            geo = (
                geometry("line", day_start, t, level, level, "Asia High" if side == "short" else "Asia Low"),
                geometry("marker", t, t, ctx.last_close, ctx.last_close, "ложный пробой"),
                _sl_line(ctx, sl),
            )
            out.append(_market_signal(self.name, ctx, direction, sl, geo, {
                "session": last["kz_session"], "asia_high": last["kz_asia_high"], "asia_low": last["kz_asia_low"], "atr": last["atr"],
                **_htf_meta(last)}))
        return tuple(out)


class StrategyVwapReversion:
    name = VWAP_REVERSION

    def __init__(self, params: VwapParams | None = None, interval: str | None = None):
        """interval - рабочий таймфрейм (Shadow/live: 5m, вместе с SMC_Pro_Trend); None - любой. Окна параметров по умолчанию - в барах M5."""
        self.params = params or VwapParams()
        self.interval = interval

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        if self.interval is not None and ctx.interval != self.interval:  # Оркестратор такой контекст не передаёт; прямой вызов - баг (R1)
            raise DataError(f"{self.name}: стратегия считается только на {self.interval}, получен контекст {ctx.symbol} {ctx.interval}")
        det = detect_vwap_reversion(ctx.frame, self.params)
        last = _last(det)
        out = []
        t = ctx.bar_open_time
        day_start = t - t % DAY_MS
        for side, band_col in (("short", "vw_upper"), ("long", "vw_lower")):
            if not last[f"vw_{side}_signal"]:
                continue
            direction = Direction(side)
            sl, band, vwap = last[f"vw_{side}_sl"], last[band_col], last["vw_vwap"]
            geo = (
                geometry("line", day_start, ctx.bar_close_ms, vwap, vwap, "VWAP"),
                geometry("line", day_start, ctx.bar_close_ms, band, band, f"VWAP {'+' if side == 'short' else '-'}{self.params.sd_mult:g}σ"),
                geometry("marker", t, t, ctx.last_close, ctx.last_close, "разворотная свеча"),
                _sl_line(ctx, sl),
            )
            out.append(_market_signal(self.name, ctx, direction, sl, geo, {
                "vwap": vwap, "sd": last["vw_sd"], "band": band, "sd_mult": self.params.sd_mult,
                "target_vwap_r": abs(ctx.last_close - vwap) / abs(sl - ctx.last_close), "atr": last["atr"], **_htf_meta(last)}))
        return tuple(out)
