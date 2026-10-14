"""Точка входа клиента: `python app.py` (разработка) или собранный .exe (`flet pack`, см. client/README.md)."""

import logging
import os

import flet as ft

from tradeai_client.ui.shell import Shell


async def flet_main(page: ft.Page) -> None:
    shell = Shell(page)
    # окно закрыто / соединение с окном потеряно -> приложение завершается ЦЕЛИКОМ (раньше процесс оставался висеть в фоне:
    # фоновые задачи опроса API и WebSocket не дают интерпретатору выйти сами)
    page.on_close = lambda e: _shutdown()
    page.on_disconnect = lambda e: _shutdown()
    await shell.start()


def _shutdown() -> None:
    """Закрыть окна графиков этой сессии и немедленно завершить процесс (os._exit: без ожидания фоновых задач и потоков)."""
    from tradeai_client.chart_window import terminate_all
    terminate_all()
    logging.shutdown()
    os._exit(0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ft.run(flet_main)
    _shutdown()  # ft.run вернулся (окно закрыто): гарантируем выход даже если события on_close не было
