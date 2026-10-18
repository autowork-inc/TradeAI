# Strategy Quant Analyzer

Отдельный сервис математического анализа сделок TradeAI (FastAPI + Pandas/NumPy). **Не импортирует код бота и не работает в его процессе.**
Связь с ботом: чтение PostgreSQL ролью `analyzer_ro` (только `SELECT`, `REPEATABLE READ`, read-only) и HTTP через основной API (`/api/analysis/*`).
Анализатор **ничего не пишет** в БД бота и в его конфигурацию: рекомендации показываются как diff, применяет их человек (правка кода и push в `main`).

## Что считает

| Модуль | Файл | Содержание |
|---|---|---|
| Метрики | `metrics.py` | Profit Factor, Sharpe/Sortino (на сделку и дневные ×√365), матожидание в R и USD, MDD в R/USD/%, bootstrap-интервалы, порог «достаточно данных» |
| Срезы «что если» | `slices.py` | Только честные срезы по записанным полям: подтяжка стопа по `mae_r`, единая цель по `mfe_r`, срезы по `meta` снапшота (`require_buildup`, `quality`…), `vol_ratio`. Без симулятора и переисполнения |
| Новости | `news.py` | Календарь CSV / JSON ForexFactory, окно `[новость − X, новость + Y]`, режимы `touch`/`span`, сравнение «все сделки» и «бот выключен на новостях», сетка окон 5–60 мин |
| Контекст | `context.py` | Сессии Asia/London/New York (UTC), VSA: объём сигнальной свечи к среднему за 20 баров, профиль победителей против остальных |
| Версии и A/B | `versions.py` | Именованные срезы стратегии (период, `plan_hash`, `meta`), автоверсии по плану выхода, сравнение, вердикт деградации (bootstrap) |
| Рекомендации | `recommend.py` | Только статистически значимые и устойчивые (в обеих половинах периода); Dry Run — unified diff поверх `current_params.json` |

## Запуск на сервере

```bash
./deploy/shadow.sh analyzer-init   # один раз: пароль роли в .env.shadow + analyzer/sql/001_analyzer_ro.sql
./deploy/shadow.sh analyzer-up     # собрать и запустить (профиль analytics; бот не перезапускается)
./deploy/shadow.sh logs analyzer
```

Контейнер: 1 CPU, 1 ГБ, read-only ФС, не root, порт наружу не публикуется; воркер — отдельный процесс с `nice 19`. Данные (задания, отчёты, версии, календарь) —
SQLite в томе `analyzer_data`. После деплоя нового кода пересоберите анализатор той же командой `analyzer-up` (автодеплой `apply.sh` его не трогает намеренно).

## REST (через основной API с `X-API-Key`; внутри — `X-Analyzer-Token`)

`GET /api/analysis/status` · `POST /api/analysis/runs` (→ `job_id`) · `GET /api/analysis/runs/{id}` (статус, прогресс) · `GET /api/analysis/runs/{id}/report` ·
`GET|POST /api/analysis/versions`, `DELETE /api/analysis/versions/{name}` · `GET|POST|DELETE /api/analysis/calendar` · `POST /api/analysis/dry-run`.

## Локально

```powershell
python -m venv analyzer\.venv
analyzer\.venv\Scripts\python.exe -m pip install -r analyzer\requirements-dev.txt
analyzer\.venv\Scripts\python.exe -m pytest analyzer\tests            # из корня проекта
```

Интеграционный тест на настоящем PostgreSQL (схема бота, роль, загрузчик) идёт только с `ANALYZER_TEST_ADMIN_URL=postgresql://postgres:pw@127.0.0.1:54331/tradeai_test`
(БД обязана оканчиваться на `_test`: схема `public` пересоздаётся).

## Ограничения (честно)

* Изменение входа (50% → 25% FVG, новый ATR-буфер) оценить нельзя: сделок, которых стратегия не открыла, в БД нет. Единственная цель по MFE — оценка снизу.
* Рекомендации — гипотезы по прошлым данным; пороги подобраны по той же выборке. Проверяйте на новых сделках.
* Версия стратегии в БД не записывается: версии задаются периодом/`plan_hash`/`meta` (см. `versions.py`).
* `current_params.json` — ручной снимок параметров бота для diff; бот его не читает.
