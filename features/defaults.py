"""Стартовые параметры стратегий, утверждённые в CLAUDE.md (раздел 6, О1-О5). Хардкод: подгонка параметров под
историю - только минимальная и только после того, как система заработает. Параметры признаков - в SmcParams
(features/smc.py), здесь параметры сделок. Детекторы Этапа 2 читают эти константы."""

from utils.errors import ConfigError

RISK_PER_TRADE = 0.01  # D10: строго 1% депозита на сделку
MAX_EXPOSURE = 5.0  # О8/О12: |позиция| * цена / эквити; больше - сделку отклоняем и пишем в лог. Было 1.0; стоп 0.3% при риске 1% = плечо ~3.3x.
# Намеренно НЕ равно config.LIVE_MAX_EXPOSURE (1.0): тот потолок - ML-контур live_run, его менять без отдельного решения нельзя.
SL_BUFFER_ATR = 0.2  # О5: буфер стопа = 0.2 * ATR (за хай свипа у SMC, под Low 3 у Breakout)
TP_R = 3.0  # D9 и разд. 3.5: тейк = 3R
BE_TRIGGER_R = 1.0  # D11/О4: перенос SL в безубыток при касании +1R (обе стратегии)
SETUP_TTL_BARS = 30  # D8/О12: от свипа до CHoCH (было 15)
LIMIT_TTL_BARS = 10  # D8: неисполненный LIMIT
FVG_ENTRY_FRACTION = 0.5  # D7: вход от 50% FVG (fvg_*_mid)
MARK_PRICE_STREAM = "{symbol}@markPrice@1s"  # О2: триггер безубытка; поток существует на mainnet (/market/stream), проверено 2026-10-08; на testnet ❓
IMPULSE_ATR_MULT = 1.0  # О12: тело импульсной свечи FVG > 1.0 * ATR (было 1.5)

# Управление позицией Shadow Mode (2026-10-10, решение пользователя; вместо «100% позиции на 3R» из D9). VirtualTracker, ScaleOutPlan.
SCALE_OUT_LEVELS = ((1.0, 0.5), (3.0, 0.4))  # (уровень в R, доля ИСХОДНОГО объёма): +1R - 50%, +3R - ещё 40%; остаток 10% - runner
SCALE_OUT_BE_AFTER_FIRST = True  # после первого частичного закрытия стоп переносится на вход
RUNNER_TRAIL_MODE = "atr"  # "atr": стоп = лучшая цена -+ TRAIL_ATR_MULT * ATR (динамический, ATR последней закрытой свечи); "wick": по теням свечей
TRAIL_ATR_MULT = 3.0  # ❓ стартовое значение не из утверждённых: подбирается по статистике Shadow, не по знаку прибыли
TRAIL_WICK_BARS = 3  # режим "wick": стоп за экстремумом последних N закрытых свечей

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
