# Образ API мониторинга (Этап 6). Контекст сборки - КОРЕНЬ проекта (см. docker-compose.yml: build.dockerfile = api/Dockerfile).
# Образ намеренно лёгкий и отдельный от торгового: в нём нет кода стратегий, ключей биржи и config.py - API только читает БД.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY api ./api
# Playground: только хранилище прогонов (stdlib sqlite3) - ставит прогон в очередь и читает историю; считает контейнер playground
COPY playground/__init__.py ./playground/__init__.py
COPY playground/store.py ./playground/store.py
# предложения/шаблоны стратегий и одобренные изменения - только для показа во вкладке MFE Simulator (бот их не читает отсюда)
COPY strategy_lab ./strategy_lab
COPY orchestrator/strategy_overrides.json ./strategy_lab/strategy_overrides.json

# не root: даже при уязвимости в веб-слое у процесса нет прав на запись в файловую систему образа
RUN useradd --system --uid 10001 --no-create-home app
USER app

EXPOSE 8000
CMD ["python", "-m", "api.main"]
