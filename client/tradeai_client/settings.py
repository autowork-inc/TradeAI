"""Настройки клиента: адрес API, ключ, опции графика. Хранятся в settings.json РЯДОМ С .exe (при запуске из исходников - в папке client/).
Старый файл в %APPDATA% больше не читается (он перебивал бы адрес из сборки).
Нет файла - берутся значения из build_defaults.py (адрес и, если был задан при сборке, ключ).

Ключ API лежит в этом файле открытым текстом: не кладите папку с .exe в общие папки/облако. Ключ можно не сохранять
(`remember_key = False`) или задать переменной окружения TRADEAI_API_KEY. Ключ никогда не пишется в логи и не передаётся в адресе запроса.
"""

import json
import os
import sys
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from urllib.parse import urlsplit

from tradeai_client import build_defaults

DEFAULT_URL = build_defaults.SERVER_URL
DEFAULT_API_KEY = build_defaults.API_KEY
SETTINGS_FILE = "settings.json"
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
    mode_filter: str = ""  # последний выбранный пользователем режим Statistics/History (all|live|shadow); пусто - следовать активному контуру

    def validate(self) -> "ClientSettings":
        parts = urlsplit(self.base_url.strip())
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise SettingsError("Адрес сервера должен начинаться с http:// или https:// (например http://193.233.75.254:8001)")
        if parts.query or parts.fragment or parts.username or parts.password:
            raise SettingsError("В адресе сервера не должно быть параметров и логина/пароля: ключ вводится отдельным полем")
        return self

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


def settings_path() -> Path:
    """Основной файл настроек: рядом с .exe."""
    return app_dir() / SETTINGS_FILE


def load_settings(path: Path | None = None, env: dict[str, str] | None = None) -> ClientSettings:
    e = os.environ if env is None else env
    path = path if path is not None else settings_path()
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
    s = ClientSettings(**values)
    if e.get("TRADEAI_API_URL"):
        s.base_url = e["TRADEAI_API_URL"]
    if e.get("TRADEAI_API_KEY"):
        s.api_key = e["TRADEAI_API_KEY"]
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
    """Сохранить настройки рядом с .exe. Нельзя записать - OSError (интерфейс покажет причину: положите .exe в папку с правом записи)."""
    return _write(s, path if path is not None else settings_path())
