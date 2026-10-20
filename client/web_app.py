"""Точка входа веб-версии клиента (контейнер `web` на VPS): WEB_LOGIN, WEB_PASSWORD, TRADEAI_API_KEY [, TRADEAI_API_URL, WEB_PORT] - из окружения.

Локально для проверки:  WEB_LOGIN=x WEB_PASSWORD=y TRADEAI_API_KEY=<32+ символов> python web_app.py"""

from tradeai_client.web.app import run

if __name__ == "__main__":
    run()
