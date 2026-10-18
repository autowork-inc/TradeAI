# Образ Strategy Quant Analyzer (CLAUDE.md разд. 3.17). Контекст сборки - КОРЕНЬ проекта (docker-compose.yml: build.dockerfile = analyzer/Dockerfile).
# Образ отдельный и лёгкий: в нём нет кода бота, стратегий и ключей бирж; анализатор читает PostgreSQL ролью analyzer_ro и ничего не пишет в БД бота.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

WORKDIR /app
COPY analyzer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY analyzer ./analyzer

# не root; /data (SQLite: задания, отчёты, версии, календарь) - единственное место записи, том самого контейнера анализатора
RUN useradd --system --uid 10002 --no-create-home app && mkdir /data && chown 10002 /data
USER app
VOLUME /data

EXPOSE 8000
CMD ["python", "-m", "analyzer.main"]
