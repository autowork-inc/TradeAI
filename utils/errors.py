"""Типы ошибок и коды завершения процесса.

Коды выхода (читает менеджер процессов, например Docker):
    0 - штатное завершение
    1 - общий сбой (сеть, 5xx, обрыв потока, битые данные); перезапуск допустим
    2 - лимит биржи (429/418); к моменту выхода пауза Retry-After уже выдержана
    3 - ошибка конфигурации или запроса (4xx); повтор без вмешательства бесполезен
    4 - Safe Mode (боевое исполнение): состояние бота и биржи не сходится, перезапуск бесполезен
        без решения оператора (замок .safe_mode снимается только вручную)
"""

EXIT_OK = 0
EXIT_GENERAL = 1
EXIT_RATE_LIMIT = 2
EXIT_CONFIG = 3
EXIT_SAFE_MODE = 4


class TradingInfraError(Exception):
    """Базовая ошибка проекта. Подкласс определяет код выхода."""

    exit_code = EXIT_GENERAL


class ConfigError(TradingInfraError):
    exit_code = EXIT_CONFIG


class HttpClientError(TradingInfraError):
    """HTTP 4xx (кроме 429/418): неверный тикер, параметры, блокировка WAF."""

    exit_code = EXIT_CONFIG

    def __init__(self, status: int, url: str, body: str):
        super().__init__(f"HTTP {status} от {url}: {body}")
        self.status = status


class HttpServerError(TradingInfraError):
    """HTTP 5xx."""

    def __init__(self, status: int, url: str, body: str):
        super().__init__(f"HTTP {status} от {url}: {body}")
        self.status = status


class RateLimitError(TradingInfraError):
    """HTTP 429/418. retry_after - сколько секунд биржа просит не обращаться."""

    exit_code = EXIT_RATE_LIMIT

    def __init__(self, status: int, retry_after: int, url: str):
        super().__init__(f"HTTP {status} (лимит/бан IP) от {url}, Retry-After={retry_after}с")
        self.status = status
        self.retry_after = retry_after


class BanActive(TradingInfraError):
    """Замок .ban_lock ещё действует: на биржу не ходим, выходим с кодом 2."""

    exit_code = EXIT_RATE_LIMIT

    def __init__(self, banned_until: float, remaining: float, detail: str):
        super().__init__(f"бан действует ещё {remaining:.0f} с ({detail})")
        self.banned_until = banned_until
        self.remaining = remaining


class InstanceAlreadyRunning(TradingInfraError):
    """Замок .instance_lock занят: этот скрипт уже запущен другим процессом."""


class NetworkError(TradingInfraError):
    """Таймаут, обрыв соединения, ошибка DNS/TLS."""


class DataError(TradingInfraError):
    """Неожиданный формат данных биржи или повреждённый локальный файл."""


class WebSocketClosed(TradingInfraError):
    """Соединение закрыто (в том числе плановый разрыв на 24 часах)."""


class StreamStalled(TradingInfraError):
    """Соединение открыто, но данные не приходят дольше допустимого."""


class ServerShutdown(TradingInfraError):
    """Биржа сообщила о предстоящем отключении (serverShutdown)."""


class SafeModeActive(TradingInfraError):
    """Бот в Safe Mode: новые позиции не открываются, пока оператор не снимет замок."""

    exit_code = EXIT_SAFE_MODE


class OrderStatusUnknown(TradingInfraError):
    """Исход отправки ордера неизвестен (нет ни подтверждения, ни отказа). Бот переведён в Safe Mode."""

    exit_code = EXIT_SAFE_MODE
