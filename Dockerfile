FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TRADING_DATA_DIR=/app/data \
    TRADING_LOG_DIR=/app/logs

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py ./
COPY utils ./utils
COPY collectors ./collectors
# Боевой контур (scripts/live_run.py): backtester.contracts тянет pipeline.types.
COPY backtester ./backtester
COPY pipeline ./pipeline
COPY execution ./execution
COPY exchange ./exchange
# Этап 2-3: признаки SMC, детекторы, Shadow Mode (scripts/shadow_run.py).
COPY features ./features
COPY strategies ./strategies
COPY orchestrator ./orchestrator
# Этап 5: запись сделок в PostgreSQL (asyncpg из requirements.txt).
COPY persistence ./persistence
COPY scripts ./scripts
# Веса модели запечены в образ; compose дополнительно монтирует ./out поверх (можно подменить без пересборки).
COPY out ./out

# Команду для каждого сервиса задаёт docker-compose.yml; по умолчанию - боевой контур.
CMD ["python", "-m", "scripts.live_run", "--model-path", "out/ridge_model.json"]
