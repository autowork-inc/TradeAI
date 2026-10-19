"""Стартовые параметры стратегий, утверждённые в CLAUDE.md (раздел 6, О1-О5). Хардкод: подгонка параметров под
историю - только минимальная и только после того, как система заработает. Параметры признаков - в SmcParams
(features/smc.py), здесь параметры сделок. Детекторы Этапа 2 читают эти константы."""

from utils.errors import ConfigError

RISK_USDT = 5.0  # 2026-10-19: фиксированный риск на сделку в USDT (потеря при срабатывании SL, без комиссий); процент стопа на сумму убытка не влияет
VIRTUAL_EQUITY = 10_000.0  # виртуальный эквити Shadow (scripts/shadow_run --equity); нужен, чтобы выразить плечо сделки в долях эквити
RISK_PER_TRADE = RISK_USDT / VIRTUAL_EQUITY  # риск сделки в долях виртуального эквити: только для проверки плеча внутри детекторов (strategies/smc_setups.py)
MAX_EXPOSURE = 5.0  # О8/О12: |позиция| * цена / эквити; больше - сделку отклоняем и пишем в лог. Было 1.0; стоп 0.3% при риске 1% = плечо ~3.3x.
# Намеренно НЕ равно config.LIVE_MAX_EXPOSURE (1.0): тот потолок - ML-контур live_run, его менять без отдельного решения нельзя.
SL_BUFFER_ATR = 0.2  # О5: буфер стопа = 0.2 * ATR (за хай свипа у SMC, под Low 3 у Breakout)
BE_TRIGGER_R = 1.0  # ТОЛЬКО архивный бэктест strategies/simulate.py и замороженный LiveBarrier; боевой движок, Shadow и MT5 безубыток не используют (2026-10-19)
TP_R = 4.0  # 2026-10-19: единственный тейк на весь объём, строго 1:4 (дистанция до TP = 4 * дистанция до SL)
SETUP_TTL_BARS = 30  # D8/О12: от свипа до CHoCH (было 15)
LIMIT_TTL_BARS = 10  # D8: неисполненный LIMIT
FVG_ENTRY_FRACTION = 0.5  # D7: вход от 50% FVG (fvg_*_mid)
MARK_PRICE_STREAM = "{symbol}@markPrice@1s"  # О2: триггер безубытка; поток существует на mainnet (/market/stream), проверено 2026-10-08; на testnet ❓
IMPULSE_ATR_MULT = 1.0  # О12: тело импульсной свечи FVG > 1.0 * ATR (было 1.5)

# Управление позицией Shadow Mode (2026-10-10, решение пользователя; вместо «100% позиции на 3R» из D9). VirtualTracker, ScaleOutPlan.
SCALE_OUT_LEVELS = ((TP_R, 1.0),)  # один уровень на весь объём: частичных фиксаций, безубытка, остатка-runner и трейлинга нет (2026-10-19)
# Блок 3 (2026-10-18): защита ждущей лимитки от импульса и MARKET-входа от «ножа»
IMPULSE_BAR_ATR = 2.5  # закрытая свеча своего таймфрейма с диапазоном > 2.5 * ATR(14)[i-1], дошедшая до зоны заявки (ближе 1R к цене лимитки), снимает её (`impulse_bar`)
MINUTE_ATR_BARS = 14  # ATR(14) по 1m-свечам из тиков (orchestrator/minute_bars.py)
MINUTE_IMPULSE_BARS = 3  # окно импульса на 1m: от экстремума последних 3 закрытых минут до последнего закрытия
MINUTE_IMPULSE_ATR = 2.5  # импульс (дамп/вынос) = ход за окно >= 2.5 * ATR(14) 1m; MARKET-вход против него - только после поглощения на последней 1m

# HTF Bias (2026-10-11, решение пользователя): сетапы M15 против структуры H4 не открываются (features/htf.py).
HTF_INTERVAL = "4h"  # старший таймфрейм контекста
HTF_FILTER_ENABLED = True  # включает ли Shadow фильтр (SmcSetupParams.htf_filter по умолчанию False: бэктест Этапа 2 идёт без H4)
HTF_HISTORY_BARS = 300  # H4-свечей на монету: 50 суток, хватает на 2+ подтверждённых свинга с каждой стороны

# Мульти-таймфреймовый SMC (2026-10-14, задание пользователя): 1d (HTF Bias) -> 1h (пул ликвидности и свип) -> 5m (FVG, CHoCH, вход).
# Включает только `SMC_Pro_Trend`; остальные четыре стратегии остаются на 15m (и H4 у SMC_Counter_Trend) без изменений.
MTF_ENABLED = True
MTF_BASE_INTERVAL = "5m"  # рабочий таймфрейм: FVG, CHoCH, вход, сопровождение позиции
MTF_STRUCT_INTERVAL = "1h"  # структура и свип пула ликвидности
MTF_BIAS_INTERVAL = "1d"  # HTF Bias
MTF_STRUCT_HISTORY_BARS = 300  # 1h-свечей на монету (12.5 суток)
MTF_BIAS_HISTORY_BARS = 90  # 1d-свечей на монету (❓ выбор Claude: у MT5 цена - ~26 страниц REST истории M5 на символ при старте)

# Сроки в барах заданы для M15 (D8, О24: 30 / 10 / 192). На более мелком таймфрейме они умножаются так, чтобы календарное время не изменилось.
REFERENCE_BAR_MS = 900_000


def bars_scale(interval_ms: int) -> int:
    """Во сколько раз сроки в барах (setup_ttl_bars, LIMIT_TTL_BARS, max_hold_bars) длиннее на таймфрейме interval_ms, чем на M15 (5m -> 3; 15m и старше -> 1)."""
    if isinstance(interval_ms, bool) or not isinstance(interval_ms, int) or interval_ms <= 0:
        raise ConfigError(f"interval_ms должен быть положительным целым, получено {interval_ms!r}")
    return max(1, REFERENCE_BAR_MS // interval_ms)
