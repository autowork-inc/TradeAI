# Веб-версия клиента (Flet web, 2026-10-20): те же вкладки, что в настольном клиенте, в мобильной раскладке. Контекст сборки - КОРЕНЬ проекта.
# Образ намеренно отдельный и лёгкий: нет кода стратегий, ключей биржи и торгового кода; ключ API бота приходит из окружения и в браузер не попадает.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp

WORKDIR /app
COPY web/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY client/tradeai_client ./tradeai_client
COPY client/web_app.py ./web_app.py

RUN useradd --system --uid 10003 --no-create-home app
USER app

EXPOSE 8550
CMD ["python", "web_app.py"]
