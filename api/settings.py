"""Настройки API из переменных окружения. Ошибка настройки - `ApiConfigError`, процесс завершается кодом 3 (как у shadow-run):
сервер без ключа или без БД стартовать не должен (R1), а «открытый» режим без ключа не существует вовсе."""

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

MIN_KEY_LEN = 32
DEFAULT_COINS = ("BTCUSDT", "ETHUSDT", "EURUSD", "XAUUSD", "XAGUSD")  # = config.SHADOW_SYMBOLS (статичная основа); монеты автоселектора API добавляет из .symbol_universe.json; образ API config.py не содержит


DEFAULT_STRATEGIES = ("SMC_Pro_Trend", "SMC_Counter_Trend", "OBI_Breakout", "ICT_Killzones", "VWAP_Reversion",
                      "Horizontal_Breakout", "Trendline_Breakout", "Horizontal_Breakout_CHR", "Trendline_Breakout_CHR")  # = orchestrator.registry.SHADOW_STRATEGIES


class ApiConfigError(Exception):
    """Неверная или отсутствующая настройка API."""


@dataclass(frozen=True)
class Settings:
    database_url: str
    api_key: str
    shadow_data_dir: Path  # откуда читается «пульс» shadow-run (.shadow_heartbeat); каталог монтируется только для чтения
    live_data_dir: Path | None = None  # каталог боевого контура shadow-live (его «пульс» тоже читается; работает либо shadow-run, либо shadow-live)
    coins: tuple[str, ...] = DEFAULT_COINS
    poll_interval_s: float = 2.0  # как часто хаб опрашивает БД для WebSocket
    keepalive_s: float = 10.0  # полное состояние уходит клиентам не реже, чем раз в столько секунд (в т.ч. «пульс» shadow-run)
    state_window_days: int = 3  # состояние автомата сетапа ищется среди событий за это окно (сетап живёт часы, а не дни)
    host: str = "0.0.0.0"
    port: int = 8000
    auth_max_failures: int = 10  # неверных ключей с одного адреса за auth_window_s, после чего 429
    auth_window_s: float = 60.0
    analyzer_url: str = ""  # http://analyzer:8000 (внутренняя сеть compose); пусто - прокси /api/analysis/* отвечает 503 analyzer_not_configured
    analyzer_token: str = ""  # общий секрет с анализатором (X-Analyzer-Token); пусто - берётся api_key
    golden_set_dir: Path | None = None  # куда звёздочка пишет эталонные сетапы (json + png); None - файлы не пишутся, звёздочка живёт только в БД
    update_dir: Path | None = None  # откуда раздаются version.json и TradeAI-Monitor.exe (монтируется только для чтения); None - /api/update/* отвечает 404

    def __post_init__(self) -> None:
        parts = urlsplit(self.database_url)
        if parts.scheme not in ("postgresql", "postgres") or not parts.hostname or not parts.path.strip("/"):
            raise ApiConfigError("DATABASE_URL должен иметь вид postgresql://user:password@host:5432/dbname")
        if len(self.api_key) < MIN_KEY_LEN or self.api_key != self.api_key.strip() or any(c.isspace() for c in self.api_key):
            raise ApiConfigError(f"API_KEY обязателен: не короче {MIN_KEY_LEN} символов, без пробелов (deploy/shadow.sh создаёт его в .env.shadow)")
        if not self.coins or any(c != c.upper() or not c.isalnum() for c in self.coins):
            raise ApiConfigError(f"API_COINS: ожидаются тикеры вида BTCUSDT через запятую, получено {self.coins!r}")
        if self.poll_interval_s <= 0 or self.keepalive_s < self.poll_interval_s or self.state_window_days < 1:
            raise ApiConfigError("poll_interval_s > 0, keepalive_s >= poll_interval_s, state_window_days >= 1")
        if self.auth_max_failures < 1 or self.auth_window_s <= 0:
            raise ApiConfigError("auth_max_failures >= 1 и auth_window_s > 0")
        if self.analyzer_url:
            u = urlsplit(self.analyzer_url)
            if u.scheme not in ("http", "https") or not u.hostname:
                raise ApiConfigError("ANALYZER_URL должен иметь вид http://analyzer:8000")


def load_settings(env: dict[str, str] | None = None) -> Settings:
    e = os.environ if env is None else env
    coins = tuple(c.strip().upper() for c in e.get("API_COINS", "").split(",") if c.strip()) or DEFAULT_COINS
    try:
        port = int(e.get("API_PORT_INTERNAL", "8000"))
        poll = float(e.get("API_POLL_INTERVAL_S", "2"))
    except ValueError as exc:
        raise ApiConfigError(f"Числовая настройка API задана неверно: {exc}") from exc
    return Settings(
        database_url=e.get("DATABASE_URL", ""), api_key=e.get("API_KEY", ""),
        shadow_data_dir=Path(e.get("SHADOW_DATA_DIR", "/app/data/shadow")),
        live_data_dir=Path(e["LIVE_DATA_DIR"]) if e.get("LIVE_DATA_DIR") else None, coins=coins, poll_interval_s=poll, port=port,
        analyzer_url=e.get("ANALYZER_URL", ""), analyzer_token=e.get("ANALYZER_TOKEN", ""),
        golden_set_dir=Path(e["GOLDEN_SET_DIR"]) if e.get("GOLDEN_SET_DIR") else None,
        update_dir=Path(e["UPDATE_DIR"]) if e.get("UPDATE_DIR") else None,
        host=e.get("API_HOST", "0.0.0.0"),  # в контейнере - все интерфейсы (наружу порт публикует compose); локально задайте 127.0.0.1
    )
