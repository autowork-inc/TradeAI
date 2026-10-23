"""Форматирование и подписи для интерфейса (без Flet: тестируется отдельно)."""

import re
from datetime import datetime, timezone

DASH = "—"
STATUS_RU = {"closed": "Закрыта", "open": "Открыта", "pending_limit": "Ждёт лимитку", "pending_market": "Ждёт рынок",
             "cancelled": "Отменена", "aborted": "Прервана"}
EXIT_RU = {"sl": "Стоп", "be": "Безубыток", "trail": "Трейлинг", "tp": "Тейк", "timeout": "Таймаут", "process_restart": "Прервана (Рестарт бота)",
           "scale_1r": "+1R (50%)", "scale_2r": "+2R", "scale_3r": "+3R (40%)", "ttl": "Срок заявки", "invalidated": "Сетап отменён", "tp_before_fill": "Цена ушла до входа",
           "impulse_bar": "Импульсная свеча", "closed_offline": "Закрыта без бота (сверка)",
           "manual_binance": "Закрыта вручную (Binance)"}
SIDE_RU = {"long": "Лонг", "short": "Шорт"}
# strategy_id -> подпись. Пять стратегий мульти-стратегийного Shadow (с 2026-10-11) и прежние имена («smc», «breakout») - сделки до этой даты.
STRATEGY_RU = {"SMC_Pro_Trend": "SMC Pro Trend", "SMC_Counter_Trend": "SMC Counter Trend", "OBI_Breakout": "OBI Breakout",
               "ICT_Killzones": "ICT Killzones", "VWAP_Reversion": "VWAP Reversion",
               "Horizontal_Breakout": "Horizontal Breakout", "Trendline_Breakout": "Trendline Breakout",
               "Horizontal_Breakout_CHR": "Пробой (CHR)", "Trendline_Breakout_CHR": "Наклонка (CHR)", "smc": "SMC Swing (до 11.10)", "breakout": "Breakout (до 11.10)"}
# контур стратегии: боевая (одобрена к live, `config.APPROVED_STRATEGIES`), теневая (только виртуальные сделки) или архивная (прежние имена до 11.10)
STRATEGY_KIND = {"SMC_Pro_Trend": "live", "VWAP_Reversion": "live", "OBI_Breakout": "shadow", "SMC_Counter_Trend": "shadow", "ICT_Killzones": "shadow",
                 "Horizontal_Breakout": "shadow", "Trendline_Breakout": "shadow",
                 "Horizontal_Breakout_CHR": "shadow", "Trendline_Breakout_CHR": "shadow", "smc": "archive", "breakout": "archive"}
KIND_TAG = {"live": "🟢 [LIVE]", "shadow": "🟡 [SHADOW]", "archive": "⚪ [АРХИВ]"}


def strategy_label(strategy_id: str) -> str:
    """«🟢 [LIVE] SMC Pro Trend»; неизвестная стратегия - как архивная (чужое имя не прячем, но и не выдаём за активную)."""
    return f"{KIND_TAG[STRATEGY_KIND.get(strategy_id, 'archive')]} {STRATEGY_RU.get(strategy_id, strategy_id)}"


# фильтры History / Statistics: сначала LIVE, затем SHADOW, архив - в самом конце
STRATEGY_OPTIONS = [("all", "Все")] + [(k, strategy_label(k)) for kind in ("live", "shadow", "archive")
                                       for k in STRATEGY_RU if STRATEGY_KIND[k] == kind]
# контур торговли (поле `mode` сделки): «Все» | только боевые | только виртуальные
MODE_OPTIONS = [("all", "Все"), ("live", "🟢 Только LIVE"), ("shadow", "🟡 Только SHADOW")]
SHADOW_ACCOUNT_TEXT = "Shadow Mode (Тест)"
RESTART_HINT = "Сделка остановлена из-за перезапуска сервера. Не рискует деньгами, в WinRate не входит"
# монеты фильтров: текущий пул Shadow (config.SHADOW_SYMBOLS, с 2026-10-14) и прежние пулы - сделки по ним остаются в БД
COINS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "NEARUSDT", "HYPEUSDT", "SNXUSDT", "ZECUSDT", "ZKUSDT", "KORUUSDT", "EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "XAGUSD",
         "LTCUSDT", "ZECUSDT", "DOTUSDT", "DOGEUSDT", "1000PEPEUSDT")
STATE_COLORS = {0: "#5b6372", 1: "#d9a441", 2: "#4c9be8", 3: "#c0622f", 4: "#2bb673"}  # S0..S4

GREEN, RED, MUTED, TEXT = "#2bb673", "#e5534b", "#8b93a3", "#e6e9ef"


HIDDEN_BALANCE = "***"
TRADES_GOAL = 200  # цель прогресс-бара на вкладке Statistics: сделок для статистически значимых выводов
MODE_KEYS = tuple(k for k, _ in MODE_OPTIONS)


def has_balance(account: dict | None) -> bool:
    return bool(account and account.get("available") and account.get("balance") is not None)


def account_text(account: dict | None, visible: bool = False) -> str:
    """Плашка шапки: «Баланс: $X.XX USDT» (сумма скрыта «***», пока visible=False); без боевого счёта (Shadow, нет данных) - «Shadow Mode (Тест)».
    Equity в интерфейсе не показывается (остаётся в API)."""
    if not has_balance(account):
        return SHADOW_ACCOUNT_TEXT
    return f"Баланс: ${account['balance']:,.2f} USDT" if visible else f"Баланс: {HIDDEN_BALANCE} USDT"


def fmt_usd(v: float | None) -> str:
    """Деньги со знаком: +$12.34 / −$5.00; нет данных - тире."""
    return DASH if v is None else (f"+${v:,.2f}" if v > 0 else f"−${-v:,.2f}" if v < 0 else "$0.00")


def chr_balance_text(net_pnl: float | None, visible: bool = False) -> str:
    """Баланс профиля CHR: всегда от нуля - накопленный результат его сделок в USDT («$0.00» до первой закрытой сделки)."""
    if not visible:
        return f"Баланс CHR: {HIDDEN_BALANCE}"
    v = 0.0 if net_pnl is None else net_pnl
    return f"Баланс CHR: ${v:,.2f}" if v >= 0 else f"Баланс CHR: −${-v:,.2f}"


def default_mode(account: dict | None) -> str:
    """Активный контур по умолчанию: боевой счёт жив - live, иначе shadow."""
    return "live" if account and account.get("available") else "shadow"


def fmt_r(v: float | None, digits: int = 2) -> str:
    return DASH if v is None else f"{v:+.{digits}f} R".replace("-", "−")


def fmt_pct(v: float | None, digits: int = 1) -> str:
    return DASH if v is None else f"{v * 100:.{digits}f}%"


def fmt_pf(item: dict) -> str:
    """Profit Factor: без убыточных сделок он не определён - показываем ∞ (если есть прибыльные) или тире."""
    pf = item.get("profit_factor")
    if pf is not None:
        return f"{pf:.2f}"
    return "∞" if item.get("profit_factor_infinite") else DASH


def price_digits(price: float) -> int:
    """Знаков после запятой по величине цены: ≥100 (BTC, ETH, SOL, золото) - 2; ≥10 (AVAX, LINK, серебро) - 3; ≥1 - 5 (EURUSD, XRP, ADA); мельче - ещё больше."""
    a = abs(price)
    return 2 if a >= 100 else 3 if a >= 10 else 5 if a >= 0.1 else 6 if a >= 0.01 else 8


def fmt_price(v: float | None) -> str:
    return DASH if v is None else f"{v:.{price_digits(v)}f}"


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_ms(s: str | None) -> int | None:
    dt = parse_iso(s)
    return None if dt is None else int(dt.timestamp() * 1000)


def fmt_local(s: str | None) -> str:
    """ISO UTC -> локальное время ПК: 10.10 15:45."""
    dt = parse_iso(s)
    return DASH if dt is None else dt.astimezone().strftime("%d.%m %H:%M")


# Время в часовом поясе пользователя (2026-10-18): сервер и API отдают UTC, всё, что видит человек, - в поясе ПК (Windows), с учётом летнего времени
# на конкретную дату. Пояс читается из ОС при каждом вызове: смена пояса в Windows видна без перезапуска клиента.

def local_offset_ms(ms: int) -> int:
    """Смещение часового пояса ПК от UTC на момент `ms` (мс): UTC+3 -> 10_800_000."""
    off = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().utcoffset()
    return 0 if off is None else int(off.total_seconds() * 1000)


_TS_TOKEN = re.compile(r"\{ts:(-?\d+)\}")  # момент времени из текстов сервера (api/reason.py): миллисекунды UTC
_HM_TOKEN = re.compile(r"\{hm:(\d{1,4})\}")  # время суток UTC в минутах от 00:00 (часы сессии)
_UTC_STAMP = re.compile(r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}) UTC")  # метки прежних версий сервера


def localize_utc_text(text: str) -> str:
    """Метки времени в текстах сервера -> время ПК без подписи пояса: `{ts:мс}` -> «21.09.2026 13:00», `{hm:минуты}` (часы сессии по UTC) -> «03:00»
    (по смещению ПК на сегодня). Метки вида «2026-09-21 10:00 UTC» (сервер до 2026-10-18) переводятся так же."""
    def stamp(ms: int) -> str:
        return fmt_ms_local(ms, "%d.%m.%Y %H:%M")

    def hm(m: re.Match) -> str:
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        return fmt_ms_local(now - now % 86_400_000 + int(m.group(1)) * 60_000, "%H:%M")

    text = _TS_TOKEN.sub(lambda m: stamp(int(m.group(1))), text)
    text = _HM_TOKEN.sub(hm, text)
    return _UTC_STAMP.sub(lambda m: stamp(int(datetime(*(int(g) for g in m.groups()), tzinfo=timezone.utc).timestamp() * 1000)), text)


def fmt_ms_local(ms: int | None, pattern: str = "%d.%m %H:%M") -> str:
    """Миллисекунды UTC -> локальное время ПК."""
    return DASH if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().strftime(pattern)


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return DASH
    if seconds < 90:
        return f"{seconds:.0f} с назад"
    return f"{seconds / 60:.0f} мин назад" if seconds < 5400 else f"{seconds / 3600:.1f} ч назад"


def r_color(v: float | None) -> str:
    return MUTED if v is None or v == 0 else (GREEN if v > 0 else RED)


def coin_short(coin: str) -> str:
    """1000PEPEUSDT -> 1000PEPE; BTCUSDT -> BTC."""
    return coin[:-4] if coin.endswith("USDT") else coin


def setup_text(setup: dict) -> str:
    """Строка состояния автомата: «S2 · свип + FVG сформирован» (+ причина отказа для S3)."""
    base = f"S{setup['state']} · {setup['label']}"
    return f"{base} ({setup['reject_label']})" if setup.get("reject_label") else base


EVENT_RU = {
    "setup_state": "Автомат сетапа", "signal_accepted": "Сигнал принят", "skipped_due_to_lock": "Пропущен: монета занята",
    "skipped_stale": "Пропущен: свеча устарела", "skipped_risk": "Отказ по риску", "trade_registered": "Заявка зарегистрирована",
    "limit_filled": "Лимитка исполнена", "market_filled": "Рыночный вход", "partial_close": "Частичное закрытие", "be_armed": "Стоп в безубыток",
    "trail_moved": "Трейлинг подтянут", "trade_closed": "Сделка закрыта", "trade_cancelled": "Заявка отменена", "data_gap": "Разрыв данных",
}


def event_text(e: dict) -> str:
    """Одна строка ленты событий: «Автомат сетапа: short:S1 → short:S2», «Заявка отменена (ttl)»."""
    base = EVENT_RU.get(e["kind"], e["kind"])
    if e["kind"] == "setup_state" and e.get("from_state") and e.get("to_state"):
        extra = e.get("detail", {}).get("reject")
        return f"{base}: {e['from_state']} → {e['to_state']}" + (f" ({extra})" if extra else "")
    reason = e.get("reason") or e.get("detail", {}).get("reason")
    return f"{base} ({reason})" if reason else base
