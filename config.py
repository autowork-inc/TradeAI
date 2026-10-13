"""Все настройки проекта в одном месте.

Меняются через правку файла; через переменные окружения переопределяются только
пути и адреса (нужно для Docker и для тестов на имитации биржи).
Модули читают значения как config.X в момент вызова, поэтому тесты могут их подменять.
"""
from dotenv import load_dotenv

load_dotenv()
import os
from datetime import datetime, timezone
from pathlib import Path

from utils.errors import ConfigError

# --- Пути ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("TRADING_DATA_DIR", BASE_DIR / "data"))
KLINES_DIR = DATA_DIR / "raw" / "klines"
DEPTH_DIR = DATA_DIR / "raw" / "depth"
FUNDING_DIR = DATA_DIR / "raw" / "funding"
LOG_DIR = Path(os.environ.get("TRADING_LOG_DIR", BASE_DIR / "logs"))
# Этап 2: нормализованный стакан (идемпотентный кэш; пересоздаётся из raw) и признаки.
INTERIM_DIR = DATA_DIR / "interim"
DEPTH_INTERIM_DIR = INTERIM_DIR / "depth"
FEATURES_DIR = DATA_DIR / "features"

# --- Binance USDⓈ-M Futures ---------------------------------------------------
# Профиль сети: BINANCE_PROFILE=mainnet (по умолчанию) | testnet. Явные BINANCE_REST_URL /
# BINANCE_WS_URL / BINANCE_WS_PRIVATE_URL сильнее профиля. Неизвестное имя профиля ловит validate().
# testnet: хост WS взят из документации Binance (wss://stream.binancefuture.com); раскладка маршрутов
# /public и /private на тестовой сети ЖИВЬЁМ НЕ ПРОВЕРЕНА - при отказе рукопожатия переопределите
# BINANCE_WS_URL / BINANCE_WS_PRIVATE_URL (см. README, раздел «Testnet»).
BINANCE_PROFILE = os.environ.get("BINANCE_PROFILE", "mainnet").strip().lower()
_PROFILES = {
    "mainnet": {
        "rest": "https://fapi.binance.com",
        "ws": "wss://fstream.binance.com/public/stream",
        "ws_market": "wss://fstream.binance.com/market/stream",
        "ws_private": "wss://fstream.binance.com/private",
    },
    "testnet": {
        "rest": "https://testnet.binancefuture.com",
        "ws": "wss://stream.binancefuture.com/public/stream",
        "ws_market": "wss://stream.binancefuture.com/market/stream",
        "ws_private": "wss://stream.binancefuture.com/private",
    },
}
_PROFILE = _PROFILES.get(BINANCE_PROFILE, _PROFILES["mainnet"])
IS_TESTNET = BINANCE_PROFILE == "testnet"

REST_BASE_URL = os.environ.get("BINANCE_REST_URL", _PROFILE["rest"])
# Старые адреса /ws и /stream отключены после 2026-04-23. Стакан (@depth) отдаётся
# через маршрут /public.
WS_BASE_URL = os.environ.get("BINANCE_WS_URL", _PROFILE["ws"])
# Свечи (@kline_1m) отдаются НЕ через /public, а через /market (проверено вживую 2026-10-04 на mainnet и testnet:
# подписка на kline в /public/stream молча игнорируется - приходит только depth). Нужен WS-фиду боевого контура.
WS_MARKET_URL = os.environ.get("BINANCE_WS_MARKET_URL", _PROFILE["ws_market"])

SYMBOLS = ("BTCUSDT", "ETHUSDT")
# Монеты Shadow Mode (Этап 3/5). НЕ в SYMBOLS намеренно: SYMBOLS читают сборщики стакана/свечей (стали бы качать данные по новым
# монетам) и боевой ML-контур live_run (на старте требует LIVE_QTY_STEP для каждой монеты и сверяет позиции) - перезапуск ML-бота
# с новыми монетами в SYMBOLS сломал бы или изменил его. Shadow публичных данных достаточно.
# 2026-10-11 (решение пользователя): боевой пул вместо тестового (BTC, ETH, DOGE, 1000PEPE). Все 8 контрактов - PERPETUAL/TRADING
# по публичному exchangeInfo (проверено 2026-10-11). Незакрытые виртуальные сделки по прежним монетам при перезапуске помечаются
# aborted/process_restart (persistence.pg.PgRecorder.start не смотрит на список монет).
# 2026-10-13 (решение пользователя): переход на макро-крипту, только высоколиквидные контракты; альткоины убраны.
SHADOW_SYMBOLS = ("BTCUSDT", "ETHUSDT")
# Этап 5: PostgreSQL для Shadow Mode. Пусто - сделки пишутся только в JSONL-журнал (локальная разработка); задано - в БД.
# Формат postgresql://user:password@host:5432/db. Пароль в логи не попадает (persistence.db.redact_dsn).
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# --- Свечи --------------------------------------------------------------------
KLINE_INTERVALS = ("1m", "15m", "1h")  # 15m/1h - стратегии SMC Swing / Breakout Buildup; 1m - ML-контур
HISTORY_START = datetime(2026, 4, 1, tzinfo=timezone.utc)  # 6 месяцев: хватает для первичного бэктеста (было 2024-01-01)
HISTORY_START_MS = int(HISTORY_START.timestamp() * 1000)

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 3_600_000,
    "2h": 2 * 3_600_000,
    "4h": 4 * 3_600_000,
    "6h": 6 * 3_600_000,
    "8h": 8 * 3_600_000,
    "12h": 12 * 3_600_000,
    "1d": 86_400_000,
}

KLINES_LIMIT = 1000  # максимум биржи 1500, но при limit > 1000 вес запроса растёт с 5 до 10
KLINES_PATH = "/fapi/v1/klines"
SERVER_TIME_PATH = "/fapi/v1/time"
# Сколько пакетов копить в памяти перед записью на диск. После падения максимум
# столько пакетов будет докачано заново (это безопасно: запись идемпотентна).
KLINES_FLUSH_EVERY_BATCHES = 10

# --- Фандинг ------------------------------------------------------------------
# Только закрытые (исторические) ставки. Для BTCUSDT и ETHUSDT интервал стандартный -
# 8 часов (00:00, 08:00, 16:00 UTC), поэтому он задан константой, а не читается из fundingInfo.
FUNDING_PATH = "/fapi/v1/fundingRate"
FUNDING_LIMIT = 1000  # максимум биржи; по умолчанию она отдаёт 100
FUNDING_INTERVAL_MS = 8 * 3_600_000

# --- Лимиты запросов ----------------------------------------------------------
WEIGHT_LIMIT_1M = 2400  # лимит Binance Futures на IP за минуту
WEIGHT_SOFT_LIMIT_1M = 1800  # выше этого порога сами ждём следующей минуты
RETRY_AFTER_DEFAULT_429_S = 60  # если биржа не прислала Retry-After
RETRY_AFTER_DEFAULT_418_S = 600
RETRY_AFTER_MAX_S = 3 * 24 * 3600 + 60  # максимальный бан Binance - 3 суток
# Замок бана: переживает перезапуск контейнера, общий для обоих скриптов (лежит в DATA_DIR).
BAN_LOCK_NAME = ".ban_lock"
# Запас к Retry-After: часы хоста и биржи не синхронны, а повторный запрос внутри бана его удлиняет.
BAN_SAFETY_MARGIN_S = 5

# Замок единственного экземпляра: <DATA_DIR>/.instance_lock.<имя скрипта> (см. utils/instance_lock.py).
INSTANCE_LOCK_NAME = ".instance_lock"

# --- HTTP ---------------------------------------------------------------------
HTTP_TOTAL_TIMEOUT_S = 15
HTTP_CONNECT_TIMEOUT_S = 5

# --- Стакан -------------------------------------------------------------------
DEPTH_LEVELS = 10  # допустимо: 5, 10, 20
# Для фьючерсов допустимо только 100ms / 250ms / 500ms. Значения 1000ms у фьючерсного
# partial depth нет (оно есть только у спота).
DEPTH_UPDATE_SPEED = "500ms"
# REST-снапшот стакана, пишется в тот же jsonl перед первыми сообщениями WebSocket
# (stream="snapshot"). Допустимые глубины: 5, 10, 20, 50, 100, 500, 1000.
DEPTH_SNAPSHOT_PATH = "/fapi/v1/depth"
DEPTH_SNAPSHOT_LIMIT = 1000
WS_CONNECT_TIMEOUT_S = 10
# Данные по BTC/ETH идут дважды в секунду. Тишина дольше этого срока = "зависшее" соединение.
WS_STALL_TIMEOUT_S = 30

# --- Live Execution Engine (2026-10-12): переключатель режима и параметры боевого риск-менеджера -------------------------
# ВЫКЛЮЧЕН по умолчанию: TRADE_MODE=shadow - ордера не отправляются, боевой код даже не импортируется (scripts/shadow_run.py).
# Включение требует сразу трёх вещей: TRADE_MODE=live, ключей BINANCE_API_KEY/SECRET и LIVE_ALLOW_MAINNET=true (на mainnet).
def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError as exc:
        raise ConfigError(f"{name}={raw!r}: ожидается число") from exc


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


TRADE_MODES = ("shadow", "live")
TRADE_MODE = os.environ.get("TRADE_MODE", "shadow").strip().lower()
LIVE_ALLOW_MAINNET = _env_bool("LIVE_ALLOW_MAINNET")  # без него live на mainnet не стартует (реальные деньги)
# Стратегии, которым разрешены реальные ордера; остальные продолжают работать только в Shadow-трекере.
APPROVED_STRATEGIES = tuple(s.strip() for s in os.environ.get("APPROVED_STRATEGIES", "SMC_Pro_Trend").split(",") if s.strip())
LIVE_MARGIN_USDT = _env_float("LIVE_MARGIN_USDT", 10.0)  # фиксированная маржа на сделку (изолированная)
LIVE_LEVERAGE = int(_env_float("LIVE_LEVERAGE", 10.0))  # плечо: размер позиции = маржа * плечо
LIVE_DAILY_LOSS_LIMIT_USDT = _env_float("LIVE_DAILY_LOSS_LIMIT_USDT", 50.0)  # суточный убыток (UTC) -> полная блокировка до 00:00 UTC
LIVE_LIQ_SAFETY = 0.8  # стоп обязан лежать ближе, чем эта доля от расстояния до ликвидации (1/плечо)
LIVE_MAX_HOLD_BARS = 192  # тайм-аут позиции (как у VirtualTracker): закрытие по рынку
LIVE_MAX_ENTRY_DEVIATION = 0.01  # MARKET-вход отклоняется, если mark price биржи ушла от цены сигнала больше чем на эту долю
LIVE_STATE_NAME = "live_engine_state.json"  # состояние боевых сделок (переживает рестарт; сверяется с биржей при старте)
USE_TESTNET = _env_bool("USE_TESTNET")  # ордера боевого контура идут на testnet; данные Shadow (свечи/тики) по-прежнему mainnet
TESTNET_REST_URL = "https://testnet.binancefuture.com"
LIVE_REST_URL = TESTNET_REST_URL if USE_TESTNET else REST_BASE_URL  # базовый адрес клиента ордеров (exchange/binance_live.py)
PREMIUM_INDEX_PATH = "/fapi/v1/premiumIndex"  # публичный: markPrice
LIVE_PNL_MAX_AGE_S = 120.0  # PnL за сутки старше этого возраста для допуска новой сделки не годится (fail-closed)
LIVE_RISK_STATE_NAME = ".live_risk_state.json"  # защёлка блокировки по суточному лимиту (переживает рестарт)
BALANCE_PATH = os.environ.get("BINANCE_BALANCE_PATH", "/fapi/v3/balance")
INCOME_PATH = "/fapi/v1/income"
LEVERAGE_PATH = "/fapi/v1/leverage"
MARGIN_TYPE_PATH = "/fapi/v1/marginType"

# --- Этап 7: боевое исполнение --------------------------------------------------
# Ключи читаются ТОЛЬКО из окружения (BINANCE_API_KEY / BINANCE_API_SECRET), в файлы и логи не попадают.
# Приватный поток: старые /ws и /stream отключены, пользовательские данные идут по маршруту /private
# (wss://.../private/ws?listenKey=...&events=...).
WS_PRIVATE_URL = os.environ.get("BINANCE_WS_PRIVATE_URL", _PROFILE["ws_private"])
USER_STREAM_EVENTS = ("ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE")
# Этап 4: условные ордера (STOP_MARKET / TAKE_PROFIT_MARKET) живут в Algo Service, их статусы приходят событием ALGO_UPDATE.
# ❓ Что биржа примет ALGO_UPDATE в параметре events URL приватного потока, не проверено (документация события не открылась):
# поэтому по умолчанию ML-контур подписан как раньше, а барьер с algo-ордерами включает этот набор явно.
USER_STREAM_EVENTS_WITH_ALGO = USER_STREAM_EVENTS + ("ALGO_UPDATE",)
LISTEN_KEY_PATH = "/fapi/v1/listenKey"
LISTEN_KEY_KEEPALIVE_S = 30 * 60  # ключ живёт 60 минут без продления
USER_STREAM_HEARTBEAT_S = 20  # ping/pong: тишина в приватном потоке штатна, зависание ловим по pong
USER_STREAM_MAX_RECONNECTS = 5  # подряд неудачных переподключений до выхода
USER_STREAM_BACKOFF_S = 1.0
USER_STREAM_BACKOFF_MAX_S = 30.0

ORDER_PATH = "/fapi/v1/order"
# Этап 4. С 2025-12-09 условные ордера - только через Algo Service (старый путь отвечает -4120, см. CLAUDE.md, разд. 4).
# POST/DELETE/GET algoOrder - сверено с документацией Binance (страницы New/Cancel/Query Algo Order);
# путь openAlgoOrders взят из сводки документации (страница не открылась) - ❓ проверить на testnet.
ALGO_ORDER_PATH = "/fapi/v1/algoOrder"
OPEN_ALGO_ORDERS_PATH = "/fapi/v1/openAlgoOrders"
ALGO_WORKING_TYPE = "CONTRACT_PRICE"  # триггер стопа по цене последней сделки (значение по умолчанию биржи); MARK_PRICE - альтернатива
ALGO_UNEXPLAINED_GRACE_MS = 5_000  # сколько ждём ALGO_UPDATE, объясняющий исполнение, пришедшее раньше него, прежде чем Safe Mode
ALGO_PLACE_ATTEMPTS = 3  # повторы выставления защитного стопа при неизвестном исходе (всегда с тем же clientAlgoId)
EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"  # публичный: PRICE_FILTER / LOT_SIZE / MIN_NOTIONAL (сверка с LIVE_* ниже при старте)
OPEN_ORDERS_PATH = "/fapi/v1/openOrders"
USER_TRADES_PATH = "/fapi/v1/userTrades"
# v1/v2 positionRisk выводятся из обращения (v2 помечен устаревшим в 2024); при необходимости - переопределить.
POSITION_RISK_PATH = os.environ.get("BINANCE_POSITION_RISK_PATH", "/fapi/v3/positionRisk")
RECV_WINDOW_MS = 5000  # запрос с timestamp старше этого окна биржа отвергает - на этом строится безопасный повтор
ORDER_RETRY_ATTEMPTS = 3
ORDER_RETRY_BACKOFF_S = 0.5
CLIENT_ORDER_ID_PREFIX = "tb"  # по префиксу сверка отличает свои ордера от чужих
QUOTE_ASSET = "USDT"
# Шаг цены (PRICE_FILTER) для округления стопов/тейков. ❓ Значения из памяти, по exchangeInfo не сверены: проверить до боя
# (на testnet шаги могут отличаться). Нет символа - ConfigError, стоп молча не округляется.
LIVE_PRICE_TICK = {"BTCUSDT": 0.1, "ETHUSDT": 0.01}
RECONCILE_QTY_TOLERANCE = 1e-9
SAFE_MODE_LOCK_NAME = ".safe_mode"  # рядом с .ban_lock; снимается только вручную
LIVE_STATE_NAME = "live_state.json"

# --- Этап 7/8: боевой оркестратор (scripts/live_run.py) ---------------------------
ACCOUNT_PATH = os.environ.get("BINANCE_ACCOUNT_PATH", "/fapi/v3/account")  # эквити: totalMarginBalance
LIVE_INSTANCE_NAME = "live_run"  # замок .instance_lock.live_run
LIVE_LOOP_INTERVAL_S = 5.0  # шаг боевого цикла (события барьера, сроки ордеров, проверка свечей)
LIVE_MAX_CONSECUTIVE_ERRORS = 3  # подряд сетевых сбоев шага цикла до аварийного выхода (код 1)
LIVE_STARTUP_TIMEOUT_S = 60  # первое подключение приватного потока (+ первичная синхронизация)
LIVE_RESYNC_TIMEOUT_S = 60  # синхронизация после (пере)подключения потока: зависла -> выход
LIVE_RECONCILE_INTERVAL_S = 300  # периодическая сверка с биржей (только когда живых ордеров нет)
LIVE_SHUTDOWN_FLUSH_S = 5.0  # сколько ждать отправки алертов перед выходом
# ExecutionConfig для LiveBarrier: время и комиссия исполнений приходят от биржи, ставки нужны только
# как запасной пересчёт, когда комиссия списана не в USDT.
LIVE_EXEC_LATENCY_MS = 50
LIVE_MAKER_FEE_RATE = 0.0002
LIVE_TAKER_FEE_RATE = 0.0005
LIVE_INITIAL_EQUITY_FRACTION = 0.1  # доля эквити на позицию (BaselineStrategy)
LIVE_MAX_DRAWDOWN = 0.05  # Drawdown Guard: доля от пика за сутки UTC
LIVE_MAX_EXPOSURE = 1.0  # потолок |позиция| * цена / эквити на символ
# Шаг лота (LOT_SIZE.stepSize): LiveBarrier не округляет количество сам, а биржа отвергает неточное (-1013).
LIVE_QTY_STEP = {"BTCUSDT": 0.001, "ETHUSDT": 0.001}
# Минимальные объём и сумма ордера (LOT_SIZE.minQty, MIN_NOTIONAL.notional). ❓ Значения из памяти: execution/exchange_filters.py
# сверяет их, шаг цены и шаг лота с exchangeInfo при старте LIVE-контура (расхождение = ConfigError) и в scripts/test_algo_live.py.
LIVE_MIN_QTY = {"BTCUSDT": 0.001, "ETHUSDT": 0.001}
LIVE_MIN_NOTIONAL = {"BTCUSDT": 100.0, "ETHUSDT": 20.0}
# Каркасные признаки для BaselineStrategy, считаются из REST-свечей 1m и стакана (execution/live_feed.py).
LIVE_FEED_INTERVAL = "1m"
LIVE_MA_FAST = 10
LIVE_MA_SLOW = 30
LIVE_OBI_LEVELS = 10
LIVE_FEED_DELAY_S = 2.0  # запас после закрытия свечи, прежде чем её запрашивать
# MLModelStrategy (scripts/live_run.py --model-path): пороги в bps лог-доходности за горизонт метки. Вход обязан
# покрывать круг издержек (2 x taker + спред + проскальзывание); значения по умолчанию = WalkForwardConfig.
LIVE_WS_MAX_RECONNECTS = 5  # WS-фид (execution/ws_live_features.py): подряд неудачных переподключений до выхода
LIVE_WS_BACKOFF_S = 1.0
LIVE_WS_BACKOFF_MAX_S = 30.0
LIVE_WS_CLOCK_WARN_MS = 1000  # расхождение часов хоста и биржи, выше которого WS-фид пишет предупреждение
LIVE_ML_THRESHOLD_BPS = 10.0
LIVE_ML_EXIT_THRESHOLD_BPS = 0.0

# --- Этап 8.2: Smart Execution (execution/smart_execution.py) --------------------
# Вход/выход - пассивным лимитным ордером (maker 0.02% вместо taker 0.05%) с чейзингом по стакану.
LIVE_SMART_EXECUTION = True  # False - прежнее поведение: MARKET по сигналу
LIVE_POST_ONLY = True  # LIMIT уходят с timeInForce=GTX: ордер, который стал бы тейкером, биржа не принимает
ORDER_TTL_SECONDS = 10.0  # сколько лимитка живёт, прежде чем её отменят и (если сигнал жив) перевыставят
LIVE_CHASE_MAX_REPOSTS = 3  # перевыставлений после первой постановки; затем - откат на MARKET или отказ от входа
LIVE_CHASE_MARKET_STRENGTH = 2.0  # откат на MARKET разрешён, если |прогноз| >= этого числа порогов входа
LIVE_CHASE_POLL_S = 1.0  # шаг фоновой задачи чейзинга (TTL-отмены, перевыставления)
LIVE_QUOTE_MAX_AGE_MS = 2000  # котировка старше этого возраста для постановки лимитки не годится

# --- Telegram (utils/notifier.py): не заданы -> алерты только в лог ---------------
TELEGRAM_API_URL = "https://api.telegram.org"
TELEGRAM_TIMEOUT_S = 5.0
# Сами TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID читаются только из окружения.

_ALLOWED_DEPTH_LEVELS = (5, 10, 20)
_ALLOWED_DEPTH_SPEEDS = ("100ms", "250ms", "500ms")
_ALLOWED_SNAPSHOT_LIMITS = (5, 10, 20, 50, 100, 500, 1000)


def validate() -> None:
    """Проверка настроек при старте. Ошибка -> ConfigError (код выхода 3)."""
    if TRADE_MODE not in TRADE_MODES:
        raise ConfigError(f"TRADE_MODE должен быть из {TRADE_MODES}, сейчас {TRADE_MODE!r}")
    if not (LIVE_MARGIN_USDT > 0 and 1 <= LIVE_LEVERAGE <= 125 and LIVE_DAILY_LOSS_LIMIT_USDT > 0):
        raise ConfigError("LIVE_MARGIN_USDT > 0, LIVE_LEVERAGE 1..125, LIVE_DAILY_LOSS_LIMIT_USDT > 0")
    if not (0 < LIVE_LIQ_SAFETY <= 1 and LIVE_PNL_MAX_AGE_S > 0 and LIVE_MAX_HOLD_BARS >= 1 and 0 < LIVE_MAX_ENTRY_DEVIATION < 1):
        raise ConfigError("LIVE_LIQ_SAFETY в (0, 1], LIVE_PNL_MAX_AGE_S > 0, LIVE_MAX_HOLD_BARS >= 1, LIVE_MAX_ENTRY_DEVIATION в (0, 1)")
    if len(set(APPROVED_STRATEGIES)) != len(APPROVED_STRATEGIES):
        raise ConfigError(f"APPROVED_STRATEGIES содержит дубли: {APPROVED_STRATEGIES}")
    if TRADE_MODE == "live" and not APPROVED_STRATEGIES:
        raise ConfigError("TRADE_MODE=live, но APPROVED_STRATEGIES пуст: ни одной стратегии не разрешено")
    if BINANCE_PROFILE not in _PROFILES:
        raise ConfigError(f"BINANCE_PROFILE должен быть из {sorted(_PROFILES)}, сейчас {BINANCE_PROFILE!r}")
    if not LIVE_MA_FAST < LIVE_MA_SLOW:
        raise ConfigError(f"LIVE_MA_FAST ({LIVE_MA_FAST}) должен быть меньше LIVE_MA_SLOW ({LIVE_MA_SLOW})")
    if not 0 < LIVE_MAX_DRAWDOWN < 1:
        raise ConfigError(f"LIVE_MAX_DRAWDOWN - доля в (0, 1), сейчас {LIVE_MAX_DRAWDOWN}")
    if LIVE_LOOP_INTERVAL_S <= 0 or LIVE_MAX_CONSECUTIVE_ERRORS < 1:
        raise ConfigError("LIVE_LOOP_INTERVAL_S должен быть > 0, LIVE_MAX_CONSECUTIVE_ERRORS >= 1")
    if ORDER_TTL_SECONDS <= 0 or LIVE_CHASE_POLL_S <= 0 or LIVE_QUOTE_MAX_AGE_MS <= 0:
        raise ConfigError("ORDER_TTL_SECONDS, LIVE_CHASE_POLL_S и LIVE_QUOTE_MAX_AGE_MS должны быть > 0")
    if LIVE_CHASE_MAX_REPOSTS < 0 or LIVE_CHASE_MARKET_STRENGTH < 1:
        raise ConfigError("LIVE_CHASE_MAX_REPOSTS >= 0, LIVE_CHASE_MARKET_STRENGTH >= 1 (в порогах входа)")
    for name, symbols in (("SYMBOLS", SYMBOLS), ("SHADOW_SYMBOLS", SHADOW_SYMBOLS)):
        if not symbols or len(set(symbols)) != len(symbols) or any(s != s.upper() or not s.isalnum() for s in symbols):
            raise ConfigError(f"{name} должны быть непустыми, уникальными, в верхнем регистре: {symbols}")
    unknown = [i for i in KLINE_INTERVALS if i not in INTERVAL_MS]
    if unknown:
        raise ConfigError(f"Неизвестные таймфреймы {unknown}; известны: {sorted(INTERVAL_MS)}")
    if not 1 <= KLINES_LIMIT <= 1500:
        raise ConfigError(f"KLINES_LIMIT должен быть в диапазоне 1..1500, сейчас {KLINES_LIMIT}")
    if not 1 <= FUNDING_LIMIT <= 1000:
        raise ConfigError(f"FUNDING_LIMIT должен быть в диапазоне 1..1000, сейчас {FUNDING_LIMIT}")
    if DEPTH_LEVELS not in _ALLOWED_DEPTH_LEVELS:
        raise ConfigError(f"DEPTH_LEVELS должен быть из {_ALLOWED_DEPTH_LEVELS}, сейчас {DEPTH_LEVELS}")
    if DEPTH_UPDATE_SPEED not in _ALLOWED_DEPTH_SPEEDS:
        raise ConfigError(
            f"DEPTH_UPDATE_SPEED для фьючерсов должен быть из {_ALLOWED_DEPTH_SPEEDS}, "
            f"сейчас {DEPTH_UPDATE_SPEED!r}"
        )
    if DEPTH_SNAPSHOT_LIMIT not in _ALLOWED_SNAPSHOT_LIMITS:
        raise ConfigError(
            f"DEPTH_SNAPSHOT_LIMIT должен быть из {_ALLOWED_SNAPSHOT_LIMITS}, сейчас {DEPTH_SNAPSHOT_LIMIT}"
        )
    if WEIGHT_SOFT_LIMIT_1M >= WEIGHT_LIMIT_1M:
        raise ConfigError("WEIGHT_SOFT_LIMIT_1M должен быть меньше WEIGHT_LIMIT_1M")
    if not 1 <= RECV_WINDOW_MS <= 60_000:
        raise ConfigError(f"RECV_WINDOW_MS должен быть в диапазоне 1..60000, сейчас {RECV_WINDOW_MS}")
    if ORDER_RETRY_ATTEMPTS < 1:
        raise ConfigError(f"ORDER_RETRY_ATTEMPTS должен быть >= 1, сейчас {ORDER_RETRY_ATTEMPTS}")
    if not CLIENT_ORDER_ID_PREFIX or not CLIENT_ORDER_ID_PREFIX.isalnum() or len(CLIENT_ORDER_ID_PREFIX) > 8:
        raise ConfigError(f"CLIENT_ORDER_ID_PREFIX: 1..8 букв/цифр, сейчас {CLIENT_ORDER_ID_PREFIX!r}")
