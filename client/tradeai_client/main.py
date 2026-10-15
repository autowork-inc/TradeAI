"""Точка входа клиента: `python app.py` (разработка) или собранный .exe (`flet pack`, см. client/README.md)."""

import asyncio
import logging
import os
import subprocess
import sys
import time

import flet as ft

from tradeai_client.ui.shell import Shell


async def flet_main(page: ft.Page) -> None:
    shell = Shell(page)
    # окно закрыто / соединение с окном потеряно -> приложение завершается ЦЕЛИКОМ (раньше процесс оставался висеть в фоне:
    # фоновые задачи опроса API и WebSocket не дают интерпретатору выйти сами)
    page.on_close = lambda e: _shutdown()
    # prevent_close НЕ включаем: иначе при мёртвой Python-стороне окно (flet.exe) вообще не закрывается крестиком
    page.window.on_event = lambda e: _shutdown() if e.type == ft.WindowEventType.CLOSE else None
    page.on_disconnect = lambda e: _shutdown()
    await shell.start()


def _shutdown() -> None:
    """Закрыть окна графиков этой сессии и немедленно завершить процесс (os._exit: без ожидания фоновых задач и потоков)."""
    from tradeai_client.chart_window import terminate_all
    try:
        for task in asyncio.all_tasks():  # опрос API, WebSocket, таймеры: отменяем, чтобы ничто не держало выход (сессии умрут вместе с процессом)
            task.cancel()
    except RuntimeError:  # вызов вне работающего цикла (после ft.run): задач уже нет
        pass
    terminate_all()
    logging.shutdown()
    if sys.platform == "win32":  # окно Flet (flet.exe) - дочерний процесс: os._exit его не гасит и оно остаётся висеть с «Working...»
        subprocess.Popen(["taskkill", "/F", "/T", "/PID", str(os.getpid())], creationflags=subprocess.CREATE_NO_WINDOW,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)  # taskkill убьёт и нас вместе с деревом; если не успел - выходим сами
    os._exit(0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ft.run(flet_main)
    _shutdown()  # ft.run вернулся (окно закрыто): гарантируем выход даже если события on_close не было
