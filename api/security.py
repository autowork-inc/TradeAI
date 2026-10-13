"""Защита API: ключ в заголовке `X-API-Key` + ограничение перебора.

* Сравнение ключа - `hmac.compare_digest` (не раскрывает по времени, сколько символов совпало).
* Ключ ходит ТОЛЬКО в заголовке: в строке запроса он попал бы в логи прокси и историю. WebSocket тоже авторизуется заголовком
  рукопожатия (клиент - Python, заголовки ему доступны).
* Перебор: больше `max_failures` неверных ключей с одного адреса за окно - 429 на всё окно, даже если следующий ключ верный.
  За SSH-туннелем/доккером все запросы приходят с одного адреса - для единственного владельца это не мешает.
* Ключ - не шифрование: по обычному HTTP он виден в сети. Поэтому порт публикуется только на 127.0.0.1 и клиент ходит через
  SSH-туннель (docker-compose.yml, README клиента); наружу - только за TLS-прокси.
"""

import hmac
import time
from collections import deque

from fastapi import HTTPException, Request, WebSocket

HEADER = "X-API-Key"


def key_matches(provided: str | None, expected: str) -> bool:
    if not provided:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


class FailureThrottle:
    """Скользящее окно неверных попыток по адресу. `clock` подменяется в тестах."""

    def __init__(self, max_failures: int, window_s: float, clock=time.monotonic):
        self._max, self._window, self._clock = max_failures, window_s, clock
        self._fails: dict[str, deque[float]] = {}

    def _recent(self, who: str) -> deque[float] | None:
        """Неудачи адреса внутри окна (устаревшие отбрасываются); None, если их нет."""
        q = self._fails.get(who)
        if q is None:
            return None
        now = self._clock()
        while q and now - q[0] >= self._window:
            q.popleft()
        if not q:
            del self._fails[who]
            return None
        return q

    def blocked(self, who: str) -> bool:
        q = self._recent(who)
        return q is not None and len(q) >= self._max

    def retry_after_s(self, who: str) -> int:
        q = self._recent(who)
        return 1 if q is None else max(1, int(self._window - (self._clock() - q[0])) + 1)

    def record_failure(self, who: str) -> None:
        if len(self._fails) > 4096:  # перебор с множества адресов: не копим память, выбрасываем всё устаревшее
            for w in list(self._fails):  # _recent сам удаляет устаревшие записи, поэтому обходим копию ключей
                self._recent(w)
        if self._recent(who) is None:
            self._fails[who] = deque()
        self._fails[who].append(self._clock())


def _who(conn: Request | WebSocket) -> str:
    return conn.client.host if conn.client else "unknown"


def check_key(conn: Request | WebSocket, expected: str, throttle: FailureThrottle) -> None:
    """Пропускает или поднимает HTTPException (401/429). Для WebSocket вызывающий переводит это в закрытие рукопожатия."""
    who = _who(conn)
    if throttle.blocked(who):
        raise HTTPException(status_code=429, detail="too_many_failed_attempts", headers={"Retry-After": str(throttle.retry_after_s(who))})
    if not key_matches(conn.headers.get(HEADER), expected):
        throttle.record_failure(who)
        raise HTTPException(status_code=401, detail="invalid_api_key")
