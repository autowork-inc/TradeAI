"""Telegram-алертер: критические события бота -> лог (всегда) и Telegram (если настроен).

Переменные окружения TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID не заданы -> события только пишутся
в лог, ничего не падает (graceful fallback). Заданы, но Telegram недоступен -> предупреждение в лог
и всё: алерт не должен ни ронять торговый цикл, ни тормозить его.

Правила:
  * Отправка никогда не бросает исключений наружу и не ждёт ответа в горячем пути: sync-методы
    (critical / notify_nowait) ставят отправку в фоновую задачу. Без работающего event loop - только лог.
  * Сообщения уходят строго по очереди (asyncio.Lock честный, FIFO) - порядок событий сохраняется.
  * Токен бота входит в URL запроса, а aiohttp кладёт URL в текст исключений: любой текст ошибки
    перед логированием проходит через _redact().
  * Перед выходом процесса вызывайте `await notifier.aclose()`: он дожидается отправки уже поставленных
    сообщений (иначе алерт о Safe Mode может не успеть уйти).

Notifier реализует протокол Alerter из execution.state (метод critical), поэтому его можно
передать прямо в SafeMode(alerter=...).
"""

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from typing import Any

import aiohttp

import config

log = logging.getLogger("notifier")

_MAX_TEXT = 4000  # лимит Telegram - 4096 символов на сообщение
_MAX_DETAILS = 1500


def _redact(text: str, *secrets: str) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def _fmt_details(details: Mapping[str, Any] | None) -> str:
    if not details:
        return ""
    raw = json.dumps(dict(details), ensure_ascii=False, default=str)
    return raw if len(raw) <= _MAX_DETAILS else raw[:_MAX_DETAILS] + "…"


class Notifier:
    def __init__(
        self,
        token: str | None = None,
        chat_id: str | None = None,
        *,
        api_url: str | None = None,
        timeout_s: float | None = None,
        prefix: str = "",
    ):
        self._token = (token or "").strip()
        self._chat_id = (chat_id or "").strip()
        self._api = (api_url or config.TELEGRAM_API_URL).rstrip("/")
        self._timeout = timeout_s if timeout_s is not None else config.TELEGRAM_TIMEOUT_S
        self._prefix = prefix
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()  # с Python 3.10 к loop привязывается лениво, при первом ожидании
        self._pending: set[asyncio.Task] = set()
        self.sent = 0
        self.failed = 0

    @classmethod
    def from_env(cls, *, prefix: str = "") -> "Notifier":
        return cls(os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID"), prefix=prefix)

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    # ------------------------------------------------------------------ отправка

    async def send(self, text: str, *, level: str = "info") -> bool:
        """Лог + (если настроен) Telegram. Не бросает исключений. True - доставлено в Telegram."""
        text = f"{self._prefix}{text}" if self._prefix else text
        _LEVELS.get(level, log.info)("%s", text)
        if not self.enabled:
            return False
        async with self._lock:
            return await self._post(text[:_MAX_TEXT])

    async def _post(self, text: str) -> bool:
        url = f"{self._api}/bot{self._token}/sendMessage"
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))
            async with self._session.post(
                url, json={"chat_id": self._chat_id, "text": text, "disable_web_page_preview": True}
            ) as resp:
                if resp.status == 200:
                    self.sent += 1
                    return True
                body = _redact((await resp.text())[:200], self._token)
                self._fail(f"HTTP {resp.status}: {body}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # алерт не должен ронять бота: любая ошибка - в лог
            self._fail(f"{type(exc).__name__}: {_redact(str(exc), self._token)}")
        return False

    def _fail(self, reason: str) -> None:
        self.failed += 1
        log.warning("Telegram: сообщение не доставлено (%s)", reason)

    def notify_nowait(self, text: str, *, level: str = "info") -> None:
        """Синхронный вызов из любого места: отправка уходит в фон. Нет event loop -> только лог."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            text = f"{self._prefix}{text}" if self._prefix else text
            _LEVELS.get(level, log.info)("%s", text)
            return
        task = loop.create_task(self.send(text, level=level), name="notifier.send")
        self._pending.add(task)  # сильная ссылка: иначе задачу может собрать GC до завершения
        task.add_done_callback(self._pending.discard)

    async def flush(self, timeout: float | None = None) -> None:
        """Дожидается отправки уже поставленных сообщений."""
        if self._pending:
            await asyncio.wait(set(self._pending), timeout=timeout)

    async def aclose(self, timeout: float | None = None) -> None:
        await self.flush(timeout if timeout is not None else config.LIVE_SHUTDOWN_FLUSH_S)
        for task in list(self._pending):
            task.cancel()
        if self._session is not None and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------- события бота

    def critical(self, title: str, details: Mapping[str, Any] | None = None) -> None:
        """Протокол Alerter (execution.state): критическое событие, например Safe Mode."""
        extra = _fmt_details(details)
        self.notify_nowait(f"🚨 {title}" + (f"\n{extra}" if extra else ""), level="critical")

    def safe_mode(self, reason: str, details: Mapping[str, Any] | None = None) -> None:
        self.critical(f"SAFE MODE: {reason}", details)

    def drawdown(self, *, drawdown: float, limit: float, peak: float, equity: float) -> None:
        self.notify_nowait(
            f"📉 Drawdown Guard: просадка {drawdown:.2%} > {limit:.2%} "
            f"(пик {peak:.2f}, эквити {equity:.2f}). Новые позиции закрыты до конца суток UTC.",
            level="critical",
        )

    def trade(self, *, action: str, symbol: str, side: str, qty: float, price: float, position: float) -> None:
        """action: ВХОД / ВЫХОД / РАЗВОРОТ / ИЗМЕНЕНИЕ."""
        self.notify_nowait(
            f"{'🟢' if action == 'ВХОД' else '🔴' if action == 'ВЫХОД' else '🔁'} {action} {symbol}: "
            f"{side.upper()} {qty:g} @ {price:g}; позиция теперь {position:g}"
        )

    def info(self, text: str) -> None:
        self.notify_nowait(text)

    def warning(self, text: str) -> None:
        self.notify_nowait(f"⚠️ {text}", level="warning")


_LEVELS = {
    "info": log.info,
    "warning": log.warning,
    "critical": log.critical,
}
