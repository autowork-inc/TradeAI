"""Точка входа клиента: `python app.py` (разработка) или собранный .exe (`flet pack`, см. client/README.md)."""

import logging

import flet as ft

from tradeai_client.ui.shell import Shell


async def flet_main(page: ft.Page) -> None:
    shell = Shell(page)
    page.on_close = lambda e: _shutdown(shell)
    await shell.start()


def _shutdown(shell: Shell) -> None:
    # закрыть окна графиков, которые открыла эта сессия (иначе процесс не завершится, пока их не закроют)
    from tradeai_client.chart_window import terminate_all
    terminate_all()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ft.run(flet_main)
