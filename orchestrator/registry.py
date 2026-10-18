"""Реестр стратегий мульти-стратегийного Shadow (2026-10-11): идентификаторы (strategy_id), планы управления позицией, сборка набора.

strategy_id - то, что пишется в колонку `strategy_id` всех таблиц БД и входит в составной ключ позиции `<символ>_<strategy_id>`
(orchestrator/tracker.py). Семь стратегий работают параллельно (две последние - только Shadow, SHADOW_ONLY_STRATEGIES); шесть из них - на свечах M15 (один kline-фид и один фид H4 на всех), а `SMC_Pro_Trend` с
2026-10-14 - в мульти-таймфреймовом режиме (`build_strategies(mtf=True)`): 1d bias -> 1h свип -> 5m FVG/CHoCH/вход. У каждой стратегии свой рабочий таймфрейм
(`strategy.interval`): Оркестратор считает её только на закрытии этих свечей. У каждой своя блокировка монеты (О14), свои позиции и свои сделки; общего
изменяемого состояния между стратегиями нет.

План управления позицией (`ScaleOutPlan`) задан на стратегию, потому что у стратегий разная идея выхода:
  SMC_Pro_Trend      Scale Out по умолчанию: 50% на +1R (+безубыток), 40% на +3R, 10% runner с трейлингом (решение 2026-10-10);
  SMC_Counter_Trend  скальп: 100% на +1R (контртрендовый отскок - быстрая цель, без трейлинга);
  OBI_Breakout       Scale Out по умолчанию (импульсный пробой - даём прибыли расти);
  ICT_Killzones      Scale Out по умолчанию;
  VWAP_Reversion     возврат к среднему: 50% на +1R (+безубыток), остаток 50% на +2R, без runner'а (цель - вернуться к VWAP, а не уйти
                     за него; точный уровень VWAP на сигнале пишется в meta, тейк-ордера из цены трекер при MARKET-входе не использует).
  Horizontal_Breakout / Trendline_Breakout  50% на +1R (+безубыток), 50% на +2R, без runner'а (LEVEL_BREAKOUT_PLAN, 2026-10-18).
Параметры планов - стартовые допущения, не из утверждённых: подбирать по статистике Shadow (trade_closes хранит каждую часть сделки),
а не по знаку прибыли.
"""

from collections.abc import Collection, Mapping, Sequence

import config
from features import defaults as D
from orchestrator.multi_strategies import (
    HORIZONTAL_BREAKOUT,
    ICT_KILLZONES,
    OBI_BREAKOUT,
    SMC_COUNTER_TREND,
    TRENDLINE_BREAKOUT,
    VWAP_REVERSION,
    StrategyCounterTrend,
    StrategyHorizontalBreakout,
    StrategyKillzones,
    StrategyObiBreakout,
    StrategyTrendlineBreakout,
    StrategyVwapReversion,
)
from orchestrator.strategies import SMC_PRO_TREND, Strategy, StrategySMC
from orchestrator.tracker import DEFAULT_PLAN, ScaleOutPlan
from strategies.smc_setups import SmcSetupParams
from strategies.vwap_reversion import VwapParams, min_history_bars
from utils.errors import ConfigError

SHADOW_STRATEGIES = (SMC_PRO_TREND, SMC_COUNTER_TREND, OBI_BREAKOUT, ICT_KILLZONES, VWAP_REVERSION, HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT)
# Стратегии без доступа к live (2026-10-18): никогда не исполняются боевым движком - config.validate и LiveRiskManager отвергают их в
# APPROVED_STRATEGIES (ConfigError), LiveMirrorSink их не пересылает. Список продублирован в config (config не импортирует оркестратор).
SHADOW_ONLY_STRATEGIES = (HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT)
NEEDS_HTF = (SMC_PRO_TREND, SMC_COUNTER_TREND, HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT)  # читают колонку htf_bias (H4)
NEEDS_MICRO = (OBI_BREAKOUT,)  # читают колонки ms_obi / ms_delta (стакан + лента)



def strategy_allowed(name: str, symbol: str) -> bool:
    """Можно ли стратегии работать на символе (config.STRATEGY_SYMBOLS): OBI_Breakout - только Binance Futures (там есть стакан и лента),
    остальные четыре - на любых символах."""
    allowed = config.STRATEGY_SYMBOLS.get(name)
    return allowed is None or symbol in allowed


SCALP_1R_PLAN = ScaleOutPlan(((1.0, 1.0),), be_after_first=False, trail_mode=None)
VWAP_PLAN = ScaleOutPlan(((1.0, 0.5), (2.0, 0.5)), be_after_first=True, trail_mode=None)
# Пробои уровня / линии (2026-10-18): разметка - медиана R:R 1.0, но у четверти сделок цель >= 2R. Половина фиксируется на +1R (стоп в безубыток),
# половина ждёт +2R; без runner'а - трейлинг 3 ATR на 15m для пробоя консолидации слишком широк.
LEVEL_BREAKOUT_PLAN = ScaleOutPlan(((1.0, 0.5), (2.0, 0.5)), be_after_first=True, trail_mode=None)
STRATEGY_PLANS: dict[str, ScaleOutPlan] = {SMC_COUNTER_TREND: SCALP_1R_PLAN, VWAP_REVERSION: VWAP_PLAN,
                                           HORIZONTAL_BREAKOUT: LEVEL_BREAKOUT_PLAN, TRENDLINE_BREAKOUT: LEVEL_BREAKOUT_PLAN}


def plans_for(names: Sequence[str]) -> Mapping[str, ScaleOutPlan]:
    """План на каждую стратегию набора (общий DEFAULT_PLAN - там, где особого нет)."""
    return {n: STRATEGY_PLANS.get(n, DEFAULT_PLAN) for n in names}


def smc_pro_trend_mtf() -> StrategySMC:
    """SMC_Pro_Trend в мульти-таймфреймовом режиме: 1d bias -> 1h свип -> 5m FVG/CHoCH/вход. Срок сетапа в 5m-барах - то же календарное время, что у D8 (30 баров M15)."""
    scale = D.bars_scale(config.INTERVAL_MS[D.MTF_BASE_INTERVAL])
    params = SmcSetupParams(htf_filter=True, mtf=True, setup_ttl_bars=D.SETUP_TTL_BARS * scale)
    return StrategySMC(params, name=SMC_PRO_TREND, interval=D.MTF_BASE_INTERVAL)


def vwap_reversion_on(interval: str) -> StrategyVwapReversion:
    """VWAP_Reversion на таймфрейме `interval`: окна в барах пересчитаны (4 часа накопления суток и 45 минут касания - те же календарные интервалы на любом таймфрейме)."""
    return StrategyVwapReversion(VwapParams.for_interval(config.INTERVAL_MS[interval]), interval=interval)


def ensure_history_depth(strategies: Sequence[Strategy], history: int, default_interval: str) -> None:
    """Проверка глубины истории на старте (до сети): суточный якорь VWAP_Reversion требует в кадре целые сутки + запас (на 15m: 96 + 12 = 108 свечей, на 5m: 300).
    Короче - стратегия молча не давала бы сигналов (кадр без начала суток), поэтому ConfigError сразу - процесс падает и уходит на ретрай, а не торгует вполсилы.
    Порог берётся по таймфрейму самой стратегии, а не фиксированным числом: на 15m 300 свечей (по умолчанию) - запас почти втрое."""
    for s in strategies:
        if s.name != VWAP_REVERSION:
            continue
        interval = getattr(s, "interval", None) or default_interval
        need = min_history_bars(config.INTERVAL_MS[interval])
        if history < need:
            raise ConfigError(f"{VWAP_REVERSION} на {interval} требует --history >= {need} (целые сутки для суточного якоря VWAP + запас), задано {history}: "
                              f"без этого стратегия молча не давала бы сигналов")


def build_strategies(names: Sequence[str] = SHADOW_STRATEGIES, *, mtf: bool = False, interval: str | None = None) -> list[Strategy]:
    """Экземпляры стратегий по strategy_id. Неизвестное имя или дубль - ConfigError (R1).

    mtf=True: SMC_Pro_Trend работает на 5m с колонками 1h/1d (`smc_pro_trend_mtf`), остальные (в т.ч. VWAP_Reversion с исходными окнами 16 / 3) - на `interval`
    (рабочий 15m): Оркестратор считает каждую стратегию только на закрытии её таймфрейма, а живой движок ведёт каждую сделку по барам её таймфрейма.
    mtf=False (по умолчанию): прежний одно-таймфреймовый режим; `interval` (если задан) закрепляет за всеми стратегиями таймфрейм."""
    if len(set(names)) != len(names):
        raise ConfigError(f"Дубли в наборе стратегий: {list(names)}")
    if any(n in NEEDS_HTF for n in names) and not D.HTF_FILTER_ENABLED:
        raise ConfigError(f"{[n for n in names if n in NEEDS_HTF]} читают HTF Bias: D.HTF_FILTER_ENABLED должен быть True")
    if tuple(config.SHADOW_ONLY_STRATEGIES) != SHADOW_ONLY_STRATEGIES:
        raise ConfigError(f"config.SHADOW_ONLY_STRATEGIES {config.SHADOW_ONLY_STRATEGIES} != registry.SHADOW_ONLY_STRATEGIES {SHADOW_ONLY_STRATEGIES}")
    if mtf and interval is None:
        raise ConfigError("build_strategies: с mtf=True нужен рабочий interval остальных стратегий (например '15m')")
    factories = {
        SMC_PRO_TREND: smc_pro_trend_mtf if mtf else (lambda: StrategySMC(SmcSetupParams(htf_filter=True), name=SMC_PRO_TREND)),
        SMC_COUNTER_TREND: StrategyCounterTrend,
        OBI_BREAKOUT: StrategyObiBreakout,
        ICT_KILLZONES: StrategyKillzones,
        VWAP_REVERSION: (lambda: vwap_reversion_on(interval)) if interval else StrategyVwapReversion,  # свой 15m: окна 16 / 3 (откат с 5m 2026-10-15)
        HORIZONTAL_BREAKOUT: StrategyHorizontalBreakout,
        TRENDLINE_BREAKOUT: StrategyTrendlineBreakout,
    }
    unknown = [n for n in names if n not in factories]
    if unknown:
        raise ConfigError(f"Неизвестные стратегии {unknown}; известны: {list(factories)}")
    built = [factories[n]() for n in names]
    if interval is not None:
        for s in built:
            if getattr(s, "interval", None) is None:  # у 5m-стратегий (mtf) таймфрейм уже задан
                s.interval = interval  # type: ignore[attr-defined]  # рабочий таймфрейм: Оркестратор не считает стратегию на чужих свечах
    return built
