"""Настройки клиента: адрес API, ключ, профиль, опции графика. Хранятся в ПОСТОЯННОЙ папке пользователя %APPDATA%\\TradeAI-Monitor\\settings.json (2026-10-20):
ни пересборка .exe, ни перенос файла, ни автообновление их не стирают (раньше файл лежал рядом с .exe и пропадал вместе с папкой сборки - ключ API
запрашивался заново). Старый файл рядом с .exe читается один раз как запасной и переносится в папку пользователя.
Нет файлов - берутся значения из build_defaults.py (адрес и, если был задан при сборке, ключ).

Ключ API лежит в этом файле открытым текстом: не кладите папку с .exe в общие папки/облако. Ключ можно не сохранять
(`remember_key = False`) или задать переменной окружения TRADEAI_API_KEY. Ключ никогда не пишется в логи и не передаётся в адресе запроса.
"""

import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from urllib.parse import urlsplit

from tradeai_client import build_defaults

DEFAULT_URL = build_defaults.SERVER_URL
DEFAULT_API_KEY = build_defaults.API_KEY
SETTINGS_FILE = "settings.json"
log = logging.getLogger(__name__)
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}


class SettingsError(ValueError):
    """Неверная настройка (показывается пользователю как есть)."""


@dataclass
class ClientSettings:
    base_url: str = DEFAULT_URL
    api_key: str = DEFAULT_API_KEY
    remember_key: bool = True
    post_signal_candles: bool = True  # на графике достраивать свечи ПОСЛЕ сигнала из публичного REST Binance (в снапшоте их нет)
    binance_rest: str = "https://fapi.binance.com"
    balance_visible: bool = False  # сумма баланса в шапке по умолчанию скрыта (***)
    profile: str = "SANTA"  # активный профиль (SANTA | CHR): запоминается и загружается при следующем запуске
    chr_extra_coins: list[str] = field(default_factory=list)  # монеты, добавленные пользователем в пул профиля CHR (сверх умолчаний tradeai_client.profiles)
    profile_chosen: bool = False  # пользователь уже выбирал профиль на экране выбора (False - экран покажется при запуске)
    last_seen_version: str = ""  # последняя версия, о которой пользователю показали «Что нового»; пусто - ни одной не видел
    mode_filter: str = ""  # последний выбранный пользователем режим Statistics/History (all|live|shadow); пусто - следовать активному контуру

    def validate(self) -> "ClientSettings":
        parts = urlsplit(self.base_url.strip())
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise SettingsError("Адрес сервера должен начинаться с http:// или https:// (например http://193.233.75.254:8001)")
        if parts.query or parts.fragment or parts.username or parts.password:
            raise SettingsError("В адресе сервера не должно быть параметров и логина/пароля: ключ вводится отдельным полем")
        return self

    def __post_init__(self) -> None:
        """Файл настроек правит человек: неверный профиль -> SANTA, неверный список монет -> пустой (а не падение при старте)."""
        from tradeai_client.profiles import normalize_profile
        self.profile = normalize_profile(self.profile)
        self.profile_chosen = self.profile_chosen is True
        if not isinstance(self.last_seen_version, str):
            self.last_seen_version = ""
        if not isinstance(self.chr_extra_coins, list) or not all(isinstance(c, str) for c in self.chr_extra_coins):
            self.chr_extra_coins = []

    @property
    def normalized_url(self) -> str:
        return self.base_url.strip().rstrip("/")

    @property
    def ws_url(self) -> str:
        parts = urlsplit(self.normalized_url)
        return f"{'wss' if parts.scheme == 'https' else 'ws'}://{parts.netloc}{parts.path.rstrip('/')}/ws/live_state"

    @property
    def cleartext_remote(self) -> bool:
        """Ключ уйдёт по сети открытым текстом: http и не localhost (туннель на 127.0.0.1 - безопасно, прямой http к серверу - нет)."""
        parts = urlsplit(self.normalized_url)
        return parts.scheme == "http" and (parts.hostname or "") not in LOCAL_HOSTS


def app_dir() -> Path:
    """Папка с .exe (PyInstaller) или с client/ при запуске из исходников."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent.parent


def user_dir() -> Path:
    """Постоянная папка данных клиента: %APPDATA%\\TradeAI-Monitor (вне папки сборки/обновления .exe)."""
    base = os.environ.get("APPDATA") if os.name == "nt" else None
    return (Path(base) if base else Path.home() / ".config") / "TradeAI-Monitor"


def settings_path() -> Path:
    """Основной файл настроек: папка пользователя (переживает пересборку и обновление .exe)."""
    return user_dir() / SETTINGS_FILE


def legacy_settings_path() -> Path:
    """Прежнее место (до 2026-10-20): рядом с .exe. Читается как запасное и переносится в папку пользователя."""
    return app_dir() / SETTINGS_FILE


def settings_exist() -> bool:
    return settings_path().exists() or legacy_settings_path().exists()


def load_settings(path: Path | None = None, env: dict[str, str] | None = None) -> ClientSettings:
    e = os.environ if env is None else env
    migrate = path is None
    if path is None:
        path = settings_path() if settings_path().exists() or not legacy_settings_path().exists() else legacy_settings_path()
    values: dict = {}
    if path is not None and path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SettingsError(f"Файл настроек {path} нечитаем ({exc}). Удалите его и введите настройки заново.") from exc
        if not isinstance(raw, dict):
            raise SettingsError(f"Файл настроек {path} имеет неверный формат")
        known = {f.name for f in fields(ClientSettings)}
        values = {k: v for k, v in raw.items() if k in known}
        if "profile" in raw and "profile_chosen" not in raw:  # файл от клиента до экрана выбора: профиль уже выбран, второй раз не спрашиваем
            values["profile_chosen"] = True
    s = ClientSettings(**values)
    if e.get("TRADEAI_API_URL"):
        s.base_url = e["TRADEAI_API_URL"]
    if e.get("TRADEAI_API_KEY"):
        s.api_key = e["TRADEAI_API_KEY"]
    if migrate and path == legacy_settings_path() and path.exists():  # перенос в постоянную папку (ошибку записи не скрываем, но запуск не роняем)
        try:
            _write(s, settings_path())
        except OSError as exc:
            log.warning("Не удалось перенести настройки в %s: %s", settings_path(), exc)
    return s


def _write(s: ClientSettings, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = asdict(s)
    if not s.remember_key:
        data["api_key"] = ""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)  # атомарно: оборванная запись не оставит битый файл
    return path


def save_settings(s: ClientSettings, path: Path | None = None) -> Path:
    """Сохранить настройки в папку пользователя. Нельзя записать - OSError (интерфейс покажет причину)."""
    return _write(s, path if path is not None else settings_path())
