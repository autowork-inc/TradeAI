"""StrategySMC и StrategyBreakout: чистые функции `detect(FeatureContext) -> tuple[Signal, ...]` (разд. 3, R8).

Никакой собственной логики паттернов: стратегия вызывает те же векторные детекторы, что и бэктест
(strategies.smc_setups.detect_smc, strategies.breakout.detect_breakout), и берёт ТОЛЬКО последнюю строку - решение
на только что закрытой свече. Побочных эффектов и состояния между вызовами нет; ошибки не подавляются (R1).

Возвращается кортеж, а не `Signal | None` из первоначальной схемы: SMC на одной свече может дать и лонг, и шорт
(разные сетапы) - оркестратор тогда по правилу D4 берёт первый, второй пишет как skipped_due_to_lock.
"""

from typing import Protocol

import polars as pl

from features import defaults as D
from features.htf import BIAS_COLUMN, BIAS_LABELS
from orchestrator.types import Direction, FeatureContext, Kind, Signal, geometry
from strategies.breakout import BreakoutParams, detect_breakout
from strategies.smc_setups import SIDES, SmcSetupParams, detect_smc
from utils.errors import DataError


class Strategy(Protocol):
    name: str

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]: ...


def _signal_id(strategy: str, ctx: FeatureContext, direction: Direction) -> str:
    return f"{strategy}:{ctx.symbol}:{ctx.interval}:{ctx.bar_open_time}:{direction.value}"


def _htf_meta(last: dict) -> dict:
    """Контекст H4 на сигнале (если оркестратор присоединил htf_bias): +1 Bullish / 0 Neutral / -1 Bearish. Попадает в снапшот и в причину входа."""
    bias = last.get(BIAS_COLUMN)
    return {} if bias is None else {"htf_interval": D.HTF_INTERVAL, "htf_bias": int(bias), "htf_label": BIAS_LABELS[int(bias)]}


SMC_PRO_TREND = "SMC_Pro_Trend"


class StrategySMC:
    """SMC Swing (S0..S4). Под именем `SMC_Pro_Trend` (strategy_id) работает с htf_filter=True: только по тренду H4. Имя можно
    переопределить (`name=`): тесты и сравнение вариантов; в БД оно пишется как strategy_id."""

    name = SMC_PRO_TREND

    def __init__(self, params: SmcSetupParams | None = None, name: str | None = None):
        self.params = params or SmcSetupParams()
        if name is not None:
            self.name = name

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        return self.analyze(ctx)[0]

    def analyze(self, ctx: FeatureContext) -> tuple[tuple[Signal, ...], dict[str, dict]]:
        """Сигналы на закрытой свече и состояния автомата сетапа по сторонам ({side: {state, reject}}) - за ОДИН расчёт детектора."""
        det = detect_smc(ctx.frame, self.params)
        last = det.row(det.height - 1, named=True)
        out = []
        for side in SIDES:
            if last[f"{side}_signal"]:
                out.append(self._signal(ctx, det, last, side))
        states = {side: {"state": last[f"{side}_state"], "reject": last.get(f"{side}_reject")} for side in SIDES}
        return tuple(out), states

    def _signal(self, ctx: FeatureContext, det: pl.DataFrame, last: dict, side: str) -> Signal:
        direction = Direction(side)
        iv, close_ms = ctx.interval_ms, ctx.bar_close_ms
        sweep_idx = last[f"{side}_sweep_idx"]
        if sweep_idx is None:
            raise DataError(f"{ctx.symbol}: сигнал без бара свипа (side={side})")
        sweep_t = int(det["open_time"][sweep_idx])
        pool, extreme = last[f"{side}_pool_level"], last[f"{side}_sweep_extreme"]
        ttl_end = close_ms + D.LIMIT_TTL_BARS * iv
        geo = (
            geometry("zone", last[f"{side}_fvg_t1"], ttl_end, last[f"{side}_fvg_hi"], last[f"{side}_fvg_lo"], "FVG"),
            geometry("line", last[f"{side}_pool_time"], sweep_t, pool, pool, "пул ликвидности"),
            geometry("marker", sweep_t, sweep_t, extreme, extreme, "свип"),
            geometry("marker", ctx.bar_open_time, ctx.bar_open_time, ctx.last_close, ctx.last_close, "CHoCH"),
            geometry("line", close_ms, ttl_end, last[f"{side}_entry"], last[f"{side}_entry"], "вход (50% FVG)"),
            geometry("line", close_ms, ttl_end, last[f"{side}_sl"], last[f"{side}_sl"], "SL"),
            geometry("line", close_ms, ttl_end, last[f"{side}_tp"], last[f"{side}_tp"], "TP 3R"),
        )
        return Signal(
            signal_id=_signal_id(self.name, ctx, direction), strategy=self.name, symbol=ctx.symbol, interval=ctx.interval,
            direction=direction, kind=Kind.LIMIT, bar_open_time=ctx.bar_open_time, bar_close_ms=close_ms,
            entry=last[f"{side}_entry"], sl=last[f"{side}_sl"], tp=last[f"{side}_tp"], inval=extreme, geometry=geo,
            meta={"pool_level": pool, "sweep_time": sweep_t, "bars_since_sweep": det.height - 1 - sweep_idx,
                  "risk_pct": last[f"{side}_risk_pct"], "atr": last["atr"], **_htf_meta(last)},
        )


class StrategyBreakout:
    """Breakout Buildup (разд. 3.6). С 2026-10-11 не входит в боевой набор Shadow (реестр orchestrator/registry.py); код и имя сохранены."""

    name = "breakout"

    def __init__(self, params: BreakoutParams | None = None):
        self.params = params or BreakoutParams()

    def detect(self, ctx: FeatureContext) -> tuple[Signal, ...]:
        det = detect_breakout(ctx.frame, self.params)
        last = det.row(det.height - 1, named=True)
        if not last["breakout_signal"]:
            return ()
        iv, close_ms, t = ctx.interval_ms, ctx.bar_close_ms, ctx.bar_open_time
        level, sl = last["breakout_level"], last["breakout_sl"]
        geo = [geometry("line", last["sh3_time"], t, level, level, "уровень равных хаёв"),
               geometry("line", last["sl3_time"], t, sl, sl, "SL (под Low 3)")]
        for n in (1, 2, 3):
            geo.append(geometry("marker", last[f"sh{n}_time"], last[f"sh{n}_time"], last[f"sh{n}_price"], last[f"sh{n}_price"], f"High {n}"))
            geo.append(geometry("marker", last[f"sl{n}_time"], last[f"sl{n}_time"], last[f"sl{n}_price"], last[f"sl{n}_price"], f"Low {n}"))
        geo.append(geometry("marker", t, t, ctx.last_close, ctx.last_close, "пробой"))
        return (Signal(
            signal_id=_signal_id(self.name, ctx, Direction.LONG), strategy=self.name, symbol=ctx.symbol, interval=ctx.interval,
            direction=Direction.LONG, kind=Kind.MARKET, bar_open_time=t, bar_close_ms=close_ms, entry=ctx.last_close, sl=sl,
            tp=None, inval=None, geometry=tuple(geo),
            meta={"level": level, "vol_ratio": last["vol_ratio"], "eqh_spread": last["eqh_spread"], "atr": last["atr"], **_htf_meta(last)},
        ),)
