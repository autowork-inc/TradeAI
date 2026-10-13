# trading-infra: сбор сырых данных (этап 1) и конвейер признаков/меток (этап 2), Binance USDⓈ-M Futures

```
config.py                         все настройки
collectors/klines_downloader.py   свечи   -> data/raw/klines/<SYMBOL>/<TF>/<YYYY-MM>.parquet
collectors/depth_stream.py        стакан  -> data/raw/depth/<SYMBOL>/<YYYY-MM-DD>/<HH>.jsonl
collectors/funding_downloader.py  фандинг -> data/raw/funding/<SYMBOL>/<YYYY>.parquet
utils/                            errors, logger, storage, binance_rest, runner
pipeline/                         loaders, quality, sync (шкала знания), features, labels, guards
backtester/contracts.py           контракт исполнения (ExecutionBarrier): задержка, комиссии, фандинг
backtester/barrier.py             OrderBookBarrier: матчинг по стакану (VWAP walking the book, пессимистичные LIMIT)
backtester/engine.py              BacktestEngine: время, портфель, кривая капитала (барьер подставляется)
backtester/risk.py                RiskGuard, DrawdownLimitGuard (daily stop), MaxExposureGuard
backtester/strategy.py            Strategy, FeatureContext, BaselineStrategy (каркас: MA-кросс + OBI)
pipeline/splits/walkforward.py    PurgedWalkForward: purge (horizon меток) и embargo (train после теста)
execution/rest.py                 подписанный REST (HMAC), коды ошибок биржи, поправка часов (-1021)
execution/state.py                LocalState (+диск), SafeMode (защёлка .safe_mode), алерты
execution/live_barrier.py         LiveBarrier: ордера на Binance, идемпотентный clientOrderId, учёт исполнений
execution/reconcile.py            сверка с биржей при старте: расхождение -> Safe Mode
execution/user_stream.py          приватный WebSocket (listenKey): ORDER_TRADE_UPDATE / ACCOUNT_UPDATE
execution/live_feed.py            каркасный источник решений: ma_fast / ma_slow / obi из REST-свечей и стакана
execution/live_features.py        LiveFeatureFeed: конвейер признаков как в обучении (база; REST-реализация)
execution/ws_live_features.py     WsLiveFeatureFeed: боевой фид для --model-path на WebSocket (depth 500 мс + kline_1m)
scripts/generate_synthetic_history.py  синтетическая сырая история (свечи, стакан 500 мс, фандинг) для обучения без сбора
scripts/live_run.py               боевой оркестратор: замки -> Safe Mode -> сверка -> поток -> торговый цикл
utils/notifier.py                 Telegram-алерты (без токена - только лог)
tests/                            893 теста; сборщики и боевой мост - на имитации биржи (tests/execution/fake_exchange.py)
```

## Запуск

Локально (из корня проекта):

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements-dev.txt
python -m collectors.klines_downloader     # разовая докачка истории свечей
python -m collectors.funding_downloader    # разовая докачка закрытых ставок фандинга
python -m collectors.depth_stream          # постоянная запись стакана
python -m pytest                           # тесты
```

Docker Compose:

```bash
docker compose up -d depth-stream
docker compose run --rm klines-downloader
docker compose run --rm funding-downloader
```

`klines-downloader` и `funding-downloader` - разовые задачи (профиль `jobs`, `restart: "no"`):
их запускают вручную или по расписанию, поэтому `docker compose up` их не стартует. Новая
ставка фандинга появляется раз в 8 часов (00:00, 08:00, 16:00 UTC), чаще докачку запускать
незачем. После кода выхода 2 (бан) повторять нужно осознанно, см. ниже.

## Коды выхода

| Код | Значение |
|---|---|
| 0 | штатно |
| 1 | сбой: сеть, таймаут, 5xx, закрытие/зависание WebSocket, неожиданный формат |
| 2 | HTTP 429/418 (срок бана записан в `data/.ban_lock`, в лог ушёл CRITICAL) либо при старте найден действующий `.ban_lock` |
| 3 | ошибка конфигурации или 4xx-запрос |
| 4 | Safe Mode (боевое исполнение): состояние бота и биржи не сходится, либо исход ордера неизвестен. Перезапуск без решения оператора бесполезен |

## Замок бана `data/.ban_lock`

При 429/418 процесс пишет в `.ban_lock` время окончания бана (+5 с запас) и сразу выходит с
кодом 2. Все сборщики проверяют замок первым делом: пока бан не истёк, они завершаются с
кодом 2 без единого запроса к бирже (замок общий для всех трёх сборщиков). Цикл «Docker
перезапускает, скрипт сразу выходит» безвреден (не более одного старта в минуту). Снять замок досрочно: удалить файл вручную,
только убедившись, что бана нет. Если запись замка невозможна, процесс выдерживает
Retry-After сам.

## Один экземпляр: `data/.instance_lock.<скрипт>`

Каждый скрипт при старте (после проверки конфига и `.ban_lock`) берёт неблокирующий
эксклюзивный замок на свой файл (`fcntl.flock` на Linux/macOS, `msvcrt.locking` на Windows).
Занято - `CRITICAL: Экземпляр уже запущен`, код выхода 1. Замок снимает сама ОС при любом
завершении процесса, включая kill -9, поэтому мёртвых замков нет. Файл замка не удаляется
и не чистится вручную. Замки раздельные: стрим стакана, докачка свечей и докачка фандинга
работают одновременно.
Не покрыт один случай: ручной запуск на Windows-хосте рядом с контейнером Docker Desktop.

## Боевой мост (этап 7): `execution/`

Ключи - только из окружения: `BINANCE_API_KEY`, `BINANCE_API_SECRET` (в файлы и логи не попадают).
Для проверки на тестовой сети переопределяются `BINANCE_REST_URL`, `BINANCE_WS_PRIVATE_URL`.
Приватный поток - `wss://fstream.binance.com/private/ws?listenKey=...&events=...`: старые `/ws` и
`/stream` отключены. `positionRisk` - v3 (`config.POSITION_RISK_PATH`): v1/v2 выводятся из обращения.

Порядок старта боевого цикла: `ban_lock.ensure_not_banned()` -> `SafeMode()` и `StateStore().load()` ->
`reconcile(...)` (расхождение = Safe Mode, ордера не отправляются) -> `LiveBarrier` + `UserDataStream`
с `on_connected=lambda r: barrier.resync_all()`, `handler` -> `barrier.on_order_update / on_account_update`.
В цикле: `barrier.advance(now)`, `await barrier.expire_due(now)`, `await barrier.resync_pending()`.

* **Идемпотентность.** `clientOrderId` (`tb-<время base36>-<UUID4>`) создаётся один раз на ордер и
  не меняется в повторах. Ордер пишется в `data/live_state.json` ДО отправки. Таймаут/5xx = «исход
  неизвестен»: сначала `GET order` по `clientOrderId`; повтор - только если биржа подтвердила «нет»
  и исходный запрос уже протух (timestamp + recvWindow). Нельзя повторять вслепую: id уникален лишь среди
  ОТКРЫТЫХ ордеров, исполненный MARKET освобождает его, и повтор удвоил бы позицию.
* **Safe Mode.** Защёлка `data/.safe_mode` переживает перезапуск; новые позиции блокируются
  (`SafeModeActive`), закрывающие (`reduce_only`) и отмены разрешены. Включают: расхождение при сверке,
  невозможность сверить, неизвестный исход ордера, исполнение «чужого» ордера (стоп, ликвидация),
  расхождение позиции с `ACCOUNT_UPDATE` дольше 3 с, переисполнение, Hedge-режим. Снимается только
  вручную: `SafeMode().clear("кто")` или удаление файла после разбора причины.
* **Не покрыто:** фандинг (`FundingSettlement`) в живом барьере не выпускается; объём и цена должны быть
  уже округлены под LOT_SIZE / PRICE_FILTER; только one-way режим позиций; формат параметра `events`
  приватного потока сверен с документацией, но живьём не проверен - прогоните на тестовой сети.

## Боевой оркестратор и Testnet (этап 8): `scripts/live_run.py`

Что делает при старте (любой сбой = выход без ордеров): `config.validate()` -> `.ban_lock` (код 2) ->
замок `data/.instance_lock.live_run` -> защёлка `.safe_mode` (стоит - выход с кодом 4) -> поправка часов и
первичная сверка `reconcile()` (расхождение - Safe Mode и код 4) -> приватный поток с keepalive `listenKey`
(ждёт первого подключения и синхронизации ордеров) -> торговый цикл: события барьера, сроки ордеров, эквити
и Drawdown Guard, источник решений, стратегия, риск-гуарды, `LiveBarrier.submit`.

Fail-fast в работе: Safe Mode (из барьера, сверки, `MARGIN_CALL`) - выход с кодом 4; приватный поток не
восстановился за `USER_STREAM_MAX_RECONNECTS` попыток или синхронизация после подключения зависла - выход
с кодом 1; после каждого переподключения - полная сверка с биржей (в обрыве мог пройти стоп или
ликвидация); раз в `LIVE_RECONCILE_INTERVAL_S` - периодическая сверка (только если живых ордеров нет);
429/418 - код 2 и `.ban_lock`; сетевой сбой шага цикла терпится `LIVE_MAX_CONSECUTIVE_ERRORS` раз подряд,
мусорный ответ биржи и ошибка стратегии - выход сразу. **Позиции при выходе бот не закрывает**: они остаются
на бирже, в Telegram/лог уходит алерт с позициями по локальному учёту - решает оператор.

### Пошагово: сухой прогон и тестовая сеть

1. Ключи тестовой сети: https://testnet.binancefuture.com (вход через GitHub) -> API Key. Ключи основной сети
   сюда не годятся. На тестовой сети нужен свой депозит (кнопка пополнения в интерфейсе тестнета).
2. Окружение: `cp .env.example .env`, заполните `BINANCE_API_KEY`, `BINANCE_API_SECRET`, оставьте
   `BINANCE_PROFILE=testnet` (REST `https://testnet.binancefuture.com`, WS `wss://stream.binancefuture.com`).
   Для Telegram (необязательно): `TELEGRAM_BOT_TOKEN` (у @BotFather) и `TELEGRAM_CHAT_ID`; без них события
   только в консоли. Локально переменные из `.env` нужно экспортировать в оболочку самому
   (PowerShell: `$env:BINANCE_PROFILE="testnet"; $env:BINANCE_API_KEY="..."`); Docker Compose читает `.env` сам.
3. Тесты: `python -m pytest` (844 теста должны пройти).
4. **Сухой прогон** - решения считаются и пишутся в лог (`DRY-RUN: ордер не отправлен ...`), ордера не
   уходят, но стартовая сверка, замки, приватный поток и эквити работают по-настоящему:
   `python -m scripts.live_run --dry-run` (Docker: `docker compose --profile live run --rm live-run python -m scripts.live_run --dry-run`).
5. **Тестовая сеть с ордерами**: `python -m scripts.live_run` (Docker: `docker compose --profile live run --rm live-run`).
   Основная сеть запрещена: при `BINANCE_PROFILE=mainnet` без `--allow-mainnet` процесс выходит с кодом 3.
6. Остановка - Ctrl+C / SIGTERM (`listenKey` закрывается, позиции остаются). Коды выхода - таблица выше.
7. Выход с кодом 4: прочитайте причину в логе/Telegram и в `data/.safe_mode`, сверьте позиции на бирже с
   `data/live_state.json`, устраните причину, затем удалите `data/.safe_mode` (или `SafeMode().clear("кто")`).
   Без этого запуск отказывает.

Что проверить при первом запуске на тестнете (живьём это ещё не прогонялось, только на имитации биржи):
* **WS-маршруты тестнета.** Хост `wss://stream.binancefuture.com` взят из документации, но раскладка
  `/private/ws?listenKey=...&events=...` на тестовой сети не подтверждена. Рукопожатие отклонено -
  поправьте `BINANCE_WS_PRIVATE_URL` / `BINANCE_WS_URL` по актуальной документации тестнета.
* **Эндпоинт эквити** `GET /fapi/v3/account` (`BINANCE_ACCOUNT_PATH`) и поле `totalMarginBalance`.
* **Шаг лота и минимальный номинал.** `LIVE_QTY_STEP` (0.001) задан в `config.py`; ордер ниже минимального
  номинала биржа отклонит, это придёт алертом «Биржа отклонила ордер» (код отказа - в логе `live_barrier`).
* Без `--model-path` источник решений `execution/live_feed.py` и `BaselineStrategy` - каркас для проверки
  контура, не торговая идея.

**ML-модель в бою (`--model-path`).** Модель обучается и экспортируется офлайн, затем подключается к циклу:

```
python -m scripts.train_walkforward --symbols BTCUSDT ETHUSDT --start 2025-01-01 --end 2025-03-01 --save-model models/ridge.json
python -m scripts.live_run --dry-run --model-path models/ridge.json [--threshold-bps 10 --exit-threshold-bps 0]
```

`--save-model` после walk-forward обучает финальную модель на всех решениях с известной меткой (отчёт фолдов
её качество НЕ измеряет) и пишет JSON: имена признаков, средние, масштабы, веса, средний y. `live_run` читает
файл до обращения к бирже (битый файл или признак, которого нет у фида, - выход без единого запроса), берёт
`MLModelStrategy` и `WsLiveFeatureFeed` (`execution/ws_live_features.py`): стакан и свечи идут по WebSocket
(`<symbol>@depth10@500ms` через `/public`, `<symbol>@kline_1m` через `/market`), сообщения копятся в буфере и
гонятся через тот же конвейер, что обучение (`DataQualityGate` -> `WatermarkClock` -> `TimelineAligner` ->
`compile_features` -> `apply_features`); стратегия получает последнюю строку, а в логе `live_run` видна строка
`ПРЕДИКТ <символ>: ... bps`. Что важно знать:
* **Train-Serve Skew по стакану устранён.** Фид слушает те же потоки, что сборщик обучающих данных, и кладёт те
  же сообщения (`recv_ts` - локальные часы, как у сборщика); `TimingConfig` тренировочный. Паритет доказывает
  `tests/execution/test_ws_live_features.py`: одни и те же сырые сообщения через офлайн-конвейер и через фид дают
  одинаковые признаки (в том числе при сдвиге часов хоста на минуту). Прежний REST-фид (снимки раз в 5 с) остаётся
  только как база конвейера.
* **Свечи идут по `/market/stream`, а не по `/public/stream`:** подписка на `kline` в `/public` молча
  игнорируется (проверено вживую на mainnet и testnet). Адрес - `config.WS_MARKET_URL`
  (`BINANCE_WS_MARKET_URL`).
* **Прогрев ~15 минут после каждого запуска.** `ofi_norm_15` требует 15 минутных корзин стакана подряд, а задним
  числом стакан получить нельзя: до накопления признак `null`, стратегия не торгует (в логе фида видно, какие
  признаки пусты). История свечей грузится по REST один раз при (пере)подключении потока свечей.
* Решение запускает событие закрытия свечи (`k.x`), а не часы хоста: часы могут идти иначе, чем у биржи. Строка
  отдаётся, когда `asof_ts <= now` (~1 с после закрытия, как в бэктесте). Порог `--threshold-bps` должен покрывать
  комиссии круга.
* Обрыв потока: фид переподключается сам (`LIVE_WS_MAX_RECONNECTS` подряд неудач -> выход кодом 1); дыра в
  стакане видна конвейеру, признаки на ней `null`, а не выдуманные.

### Синтетическая история (чтобы не ждать сутки сбора)

```
python -m scripts.generate_synthetic_history [--hours 48] [--seed 7]
python -m scripts.train_walkforward --symbols BTCUSDT ETHUSDT --start 2026-10-02 --end 2026-10-04 --splits 2 --save-model out/ridge_model.json
```

Скрипт пишет в `data/raw` свечи (1m и 1h), стакан `depth10@500ms` в jsonl и фандинг в форматах сборщиков
(их читают `KlinesLoader` / `DepthLoader` / `FundingLoader`). Данные СИНТЕТИЧЕСКИЕ (маркер `data/raw/SYNTHETIC.json`):
в цену заложен режимный дрейф, поэтому IC на них высокий - это проверка проводки, а не edge на рынке. Данные
настоящих сборщиков скрипт не затирает (без `--force` откажется). За 48 часов 5 фолдов по умолчанию не помещаются:
ось считается по `asof_ts` обоих символов, поэтому нужен `--splits 2`.

## Правила для читателей сырых данных

* **Снапшот:** первой строкой каждого запуска идёт `"stream":"snapshot"` (REST, до 1000
  уровней, поля `lastUpdateId/E/T/bids/asks`). Дальше `"stream":"<тикер>@depth10@500ms"` -
  это partial depth, каждое сообщение уже полное состояние топ-10 (поля `E/T/s/U/u/pu/b/a`).

* **jsonl:** последняя строка файла может быть оборвана (падение посреди записи). Строки,
  которые не парсятся как JSON, пропускать. Каждая строка: `recv_ts` (мс UTC, часы
  сборщика), `symbol`, `stream`, `data` (сообщение биржи без изменений: `E`, `T`, `U`,
  `u`, `pu`, `b`, `a`).
* **Дыры:** каждый перезапуск и каждый плановый разрыв на 24 ч - пропуск в стакане.
  Искать по скачкам `recv_ts` / `data.E`.
* **parquet:** `*.tmp.parquet` - остаток прерванной записи, читать не нужно (сборщик
  удалит при следующем запуске). Цены и объёмы хранятся как Float64, время - мс UTC.
* Файлы сырого слоя вручную не правятся.

### Фандинг: `data/raw/funding/<SYMBOL>/<YYYY>.parquet`

* **Схема:** `funding_time` (Int64, мс UTC - серверное время списания), `funding_rate`
  (Float64, доля за интервал: `0.0001` = 0.01%), `mark_price` (Float64, **может быть null**
  у старых записей; для учёта списания он обязателен). Читать только через `FundingLoader`:
  он проверяет схему файла (чужая схема - `DataError`) и дедуплицирует по `funding_time`.
* **Только закрытые ставки.** Запись появляется после списания; будущих и прогнозных ставок в
  данных нет (записи позже серверного времени сборщик не сохраняет). Данные начинаются с
  `HISTORY_START`; до него «ставки нет», а не «ставка ноль».
* **`funding_time` - не время знания.** Серверное время списания и момент, когда система о
  нём узнала, - разные шкалы. Присоединять ставку к признакам по `funding_time` нельзя
  (заглядывание вперёд). Допустим один путь: `WatermarkClock.funding_availability` ->
  `TimelineAligner.attach_funding`; ставка видна строке только после `avail_ts` (первое
  сообщение стакана с `E >= funding_time` + `funding_publish_lag_ms`) и строго раньше `asof_ts`.
* **Для учёта PnL, наоборот, нужен именно `funding_time`:** фандинг списывается с позиции,
  открытой строго до этого момента (контракт - `backtester/contracts.py`, правило 4). Ставка
  в учёте (факт) и ставка в признаках (то, что уже известно) никогда не подменяют друг друга.
* **Знак:** `funding_rate > 0` - лонги платят шортам, `< 0` - шорты платят лонгам. Размер
  списания = `позиция * mark_price * funding_rate`.
* **Время не округлять** к сетке 00/08/16: фактическое списание смещено на миллисекунды.
  Шаг, отличный от 8 часов больше чем на минуту, сборщик пишет в лог (смена интервала или пропуск).
* **Дыры:** отсутствующий годовой файл пропускается молча; пропущенное списание видно по
  `gap_before` (до предыдущей записи больше 1.5 интервала). Позиция, пересёкшая дыру, не
  симулируется - `FundingCoverageError`, а не пропуск.
* **История не переписывается:** если биржа отдала другую ставку для уже сохранённого
  `funding_time`, докачка падает с `DataError`. `*.tmp.parquet` - остаток прерванной записи.

### Метки и будущее

Заглядывать в будущее разрешено **только** в `pipeline/labels` (форвардная доходность на
`horizon_ms`). Колонки меток начинаются с `y_`; признаки не могут их читать и создавать, а
`{метка}_end_ts` - момент знания последней точки метки - служит ключом purge при разбиении
train/test. Метка считается строго внутри `(symbol, segment_id)`: через пропущенную свечу
метка не тянется (null). Строки с null-меткой в обучение не берутся.

## Что известно о границах

* Время `recv_ts` берётся с часов машины. Рассинхрон часов виден как разница `recv_ts - E`
  (она должна быть десятки-сотни мс); держите NTP включённым.
* Скачивание ждёт у порога веса 1800/2400 в минуту (`WEIGHT_SOFT_LIMIT_1M`), чтобы не
  получить 429. Это ожидаемая пауза, а не сбой.

## Развёртывание на Ubuntu 24.04 (Docker)

На Windows-машине (хостовый Python на сервере не нужен):

```
python -m scripts.deploy_pack          # -> trading_docker.tar.gz (внутри .env с ключами!)
```

Загрузите архив через MobaXterm (SFTP), затем на сервере:

```bash
# 1. Docker Engine + compose-плагин (официальный репозиторий)
sudo apt-get update && sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update && sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER && newgrp docker

# 2. Распаковка и запуск
mkdir -p ~/trading && tar xzf ~/trading_docker.tar.gz -C ~/trading && cd ~/trading
chmod 600 .env && mkdir -p data logs
docker compose up -d --build            # стартует live-run (testnet), restart: "no"

# 3. Наблюдение
docker compose logs -f live-run
docker compose ps -a                    # код выхода 4 = Safe Mode, 2 = бан: нужен оператор
docker compose down                     # остановка
```

После выхода с кодом 4 разберитесь с причиной, удалите `data/.safe_mode` и запустите снова.
Часы сервера должны быть синхронны (`timedatectl`), иначе подписанные запросы Binance отклоняются.
Сбор стакана: `docker compose --profile collectors up -d depth-stream`.
