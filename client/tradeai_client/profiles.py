"""Профили пользователей клиента (2026-10-19): SANTA - как раньше, без ограничений; CHR - только свои стратегии и свой пул монет.
Профиль влияет на то, что клиент ЗАПРАШИВАЕТ и показывает (фильтры стратегий и монет, баланс от нуля); сервер и бот у профилей общие.
«Избранное» (Golden Set) общее для обоих профилей и профильным фильтром не режется."""

import re

SANTA, CHR = "SANTA", "CHR"
PROFILES = (SANTA, CHR)
DEFAULT_PROFILE = SANTA
CHR_STRATEGIES = ("Trendline_Breakout_CHR", "Horizontal_Breakout_CHR")
# EUR/GBP/JPY/XAU/BTC/ETH из задания -> реальные символы бота: форекс и золото идут через MT5 (EURUSD, GBPUSD, USDJPY, XAUUSD), крипта - Binance Futures (USDT)
CHR_DEFAULT_COINS = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSDT", "ETHUSDT")
_FX_ALIASES = {"EUR": "EURUSD", "GBP": "GBPUSD", "JPY": "USDJPY", "XAU": "XAUUSD", "XAG": "XAGUSD", "AUD": "AUDUSD", "CAD": "USDCAD", "CHF": "USDCHF", "NZD": "NZDUSD"}
_SYMBOL = re.compile(r"^[A-Z0-9]{2,15}$")


class ProfileError(ValueError):
    """Неверный ввод пользователя (показывается как есть)."""


def normalize_profile(value: object) -> str:
    return value if value in PROFILES else DEFAULT_PROFILE


def normalize_coin(text: str) -> str:
    """«sol» -> SOLUSDT, «eur» -> EURUSD, «jpy» -> USDJPY, «EURUSD» / «BTCUSDT» - как есть. Пустой или с лишними символами - ProfileError."""
    coin = (text or "").strip().upper()
    if not _SYMBOL.match(coin):
        raise ProfileError("Тикер: латинские буквы и цифры, например SOL или SOLUSDT")
    if coin in _FX_ALIASES:
        return _FX_ALIASES[coin]
    if coin.endswith("USDT") or (len(coin) == 6 and coin.endswith("USD")) or (len(coin) == 6 and coin.startswith("USD")):
        return coin
    return coin + "USDT"


def coins_for(profile: str, extra: list[str] | tuple[str, ...] = ()) -> list[str] | None:
    """Пул монет профиля: None - без ограничения. 1.6.0: и у CHR тоже - его стратегии торгуют на всём пуле бота (альты Binance Test, крипта, MT5),
    а прежний пул из 6 монет прятал почти все его сделки (Dashboard и History были пустыми). Профиль CHR ограничивает только стратегии."""
    return None


def strategies_for(profile: str) -> list[str] | None:
    return list(CHR_STRATEGIES) if normalize_profile(profile) == CHR else None
