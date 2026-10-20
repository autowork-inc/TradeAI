"""Вход в веб-версию (без Flet, тестируется отдельно): проверка логина/пароля, подписанный токен «Запомнить меня», защита от перебора.

Пароль и секрет подписи лежат ТОЛЬКО в окружении контейнера (WEB_LOGIN / WEB_PASSWORD / WEB_SESSION_SECRET из .env.shadow на сервере), в коде и в git их нет.
В браузере хранится не пароль, а токен `логин.срок.подпись` (HMAC-SHA256): украсть пароль из хранилища телефона нельзя, смена пароля или секрета на сервере
сразу обесценивает все выданные токены.
"""

import hashlib
import hmac
import time

TOKEN_TTL_S = 90 * 24 * 3600  # «Запомнить меня»: 90 суток
MAX_FAILS = 5  # неверных попыток подряд -> пауза
LOCK_S = 60.0


class AuthError(ValueError):
    """Неверная настройка входа (показывается в логе контейнера, контейнер не стартует)."""


def check_credentials(login: str, password: str, expected_login: str, expected_password: str) -> bool:
    """Логин сравнивается без учёта регистра, пароль - точно; оба сравнения за постоянное время."""
    ok_login = hmac.compare_digest((login or "").strip().lower().encode("utf-8"), expected_login.strip().lower().encode("utf-8"))
    ok_pass = hmac.compare_digest((password or "").encode("utf-8"), expected_password.encode("utf-8"))
    return ok_login and ok_pass


def derive_secret(password: str, api_key: str, explicit: str = "") -> bytes:
    """Секрет подписи токенов: явный WEB_SESSION_SECRET, иначе производный от пароля и ключа API (смена любого из них отзывает токены)."""
    return (explicit or hashlib.sha256(f"tradeai-web|{password}|{api_key}".encode("utf-8")).hexdigest()).encode("utf-8")


def make_token(secret: bytes, login: str, now: float | None = None, ttl_s: int = TOKEN_TTL_S) -> str:
    exp = int((time.time() if now is None else now) + ttl_s)
    body = f"{login.strip().lower()}.{exp}"
    return f"{body}.{hmac.new(secret, body.encode('utf-8'), hashlib.sha256).hexdigest()}"


def verify_token(secret: bytes, token: object, expected_login: str, now: float | None = None) -> bool:
    if not isinstance(token, str) or token.count(".") != 2:
        return False
    login, exp, sig = token.split(".")
    if not exp.isdigit() or not hmac.compare_digest(login.encode("utf-8"), expected_login.strip().lower().encode("utf-8")):
        return False
    good = hmac.new(secret, f"{login}.{exp}".encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig.encode("utf-8"), good.encode("utf-8")) and int(exp) > (time.time() if now is None else now)


class LoginGuard:
    """Общий для всех сессий счётчик неверных попыток: MAX_FAILS подряд -> вход закрыт на LOCK_S секунд (пароль перебором не подобрать)."""

    def __init__(self, max_fails: int = MAX_FAILS, lock_s: float = LOCK_S) -> None:
        self.max_fails, self.lock_s = max_fails, lock_s
        self._fails, self._locked_until = 0, 0.0

    def locked_for(self, now: float | None = None) -> float:
        """Сколько секунд вход ещё закрыт (0 - можно пробовать)."""
        t = time.monotonic() if now is None else now
        return max(0.0, self._locked_until - t)

    def failed(self, now: float | None = None) -> None:
        t = time.monotonic() if now is None else now
        self._fails += 1
        if self._fails >= self.max_fails:
            self._fails, self._locked_until = 0, t + self.lock_s

    def succeeded(self) -> None:
        self._fails = 0
