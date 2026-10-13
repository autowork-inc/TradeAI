"""Реестр стратегий мульти-стратегийного Shadow (2026-10-11): идентификаторы (strategy_id), планы управления позицией, сборка набора.

strategy_id - то, что пишется в колонку `strategy_id` всех таблиц БД и входит в составной ключ позиции `<символ>_<strategy_id>`
(orchestrator/tracker.py). Пять стратегий работают параллельно на ОДНИХ и тех же свечах M15 (один kline-фид и один фид H4 на всех),
у каждой своя блокировка монеты (О14), свои позиции и свои сделки; общего изменяемого состояния между стратегиями нет.

План управления позицией (`ScaleOutPlan`) задан на стратегию, потому что у стратегий разная идея выхода:
  SMC_Pro_Trend      Scale Out по умолчанию: 50% на +1R (+безубыток), 40% на +3R, 10% runner с трейлингом (решение 2026-10-10);
  SMC_Counter_Trend  скальп: 100% на +1R (контртрендовый отскок - быстрая цель, без трейлинга);
  OBI_Breakout       Scale Out по умолчанию (импульсный пробой - даём прибыли расти);
  ICT_Killzones      Scale Out по умолчанию;
  VWAP_Reversion     возврат к среднему: 50% на +1R (+безубыток), остаток 50% на +2R, без runner'а (цель - вернуться к VWAP, а не уйти
                     за него; точный уровень VWAP на сигнале пишется в meta, тейк-ордера из цены трекер при MARKET-входе не использует).
Параметры планов - стартовые допущения, не из утверждённых: подбирать по статистике Shadow (trade_closes хранит каждую часть сделки),
а не по знаку прибыли.
"""

from collections.abc import Mapping, Sequence

from features import defaults as D
from orchestrator.multi_strategies import (
    ICT_KILLZONES,
    OBI_BREAKOUT,
    SMC_COUNTER_TREND,
    VWAP_REVERSION,
    StrategyCounterTrend,
    StrategyKillzones,
    StrategyObiBreakout,
    StrategyVwapReversion,
)
from orchestrator.strategies import SMC_PRO_TREND, Strategy, StrategySMC
from orchestrator.tracker import DEFAULT_PLAN, ScaleOutPlan
from strategies.smc_setups import SmcSetupParams
from utils.errors import ConfigError

SHADOW_STRATEGIES = (SMC_PRO_TREND, SMC_COUNTER_TREND, OBI_BREAKOUT, ICT_KILLZONES, VWAP_REVERSION)
NEEDS_HTF = (SMC_PRO_TREND, SMC_COUNTER_TREND)  # читают колонку htf_bias (H4)
NEEDS_MICRO = (OBI_BREAKOUT,)  # читают колонки ms_obi / ms_delta (стакан + лента)

SCALP_1R_PLAN = ScaleOutPlan(((1.0, 1.0),), be_after_first=False, trail_mode=None)
VWAP_PLAN = ScaleOutPlan(((1.0, 0.5), (2.0, 0.5)), be_after_first=True, trail_mode=None)
STRATEGY_PLANS: dict[str, ScaleOutPlan] = {SMC_COUNTER_TREND: SCALP_1R_PLAN, VWAP_REVERSION: VWAP_PLAN}


def plans_for(names: Sequence[str]) -> Mapping[str, ScaleOutPlan]:
    """План на каждую стратегию набора (общий DEFAULT_PLAN - там, где особого нет)."""
    return {n: STRATEGY_PLANS.get(n, DEFAULT_PLAN) for n in names}


def build_strategies(names: Sequence[str] = SHADOW_STRATEGIES) -> list[Strategy]:
    """Экземпляры стратегий по strategy_id. Неизвестное имя или дубль - ConfigError (R1)."""
    if len(set(names)) != len(names):
        raise ConfigError(f"Дубли в наборе стратегий: {list(names)}")
    if (SMC_PRO_TREND in names or SMC_COUNTER_TREND in names) and not D.HTF_FILTER_ENABLED:
        raise ConfigError("SMC_Pro_Trend и SMC_Counter_Trend читают HTF Bias: D.HTF_FILTER_ENABLED должен быть True")
    factories = {
        SMC_PRO_TREND: lambda: StrategySMC(SmcSetupParams(htf_filter=True), name=SMC_PRO_TREND),
        SMC_COUNTER_TREND: StrategyCounterTrend,
        OBI_BREAKOUT: StrategyObiBreakout,
        ICT_KILLZONES: StrategyKillzones,
        VWAP_REVERSION: StrategyVwapReversion,
    }
    unknown = [n for n in names if n not in factories]
    if unknown:
        raise ConfigError(f"Неизвестные стратегии {unknown}; известны: {list(factories)}")
    return [factories[n]() for n in names]
