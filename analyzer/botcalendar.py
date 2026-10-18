"""Календарь ForexFactory из хранилища бота. Бот раз в час обновляет `DATA_DIR/.news_calendar.json` (только High Impact, текущая неделя); анализатор
монтирует каталоги данных бота ТОЛЬКО НА ЧТЕНИЕ, подмешивает события в свой SQLite и накапливает историю (бот держит одну неделю).
Формат файла бота: {"loaded_ms": int, "events": [[ts_ms, "USD", "CPI m/m"], ...]}. В сеть анализатор по-прежнему не ходит."""

import json
import logging
from pathlib import Path

import pandas as pd

from analyzer.store import Store

log = logging.getLogger("analyzer.botcalendar")
CACHE_NAME = ".news_calendar.json"  # совпадает с config.NEWS_CACHE_NAME бота (тест сверяет)
IMPACT = "high"  # бот хранит только High Impact


def read_bot_calendar(dirs: tuple[Path, ...]) -> pd.DataFrame:
    """События из файлов бота (live и shadow). Нет файла - пусто; нечитаемый файл - предупреждение и пропуск (анализатор вспомогательный, бот не затрагивается)."""
    ts: list[int] = []
    ccy: list[str] = []
    ev: list[str] = []
    for d in dirs:
        path = d / CACHE_NAME
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            rows = [(int(t), str(c).upper(), str(title)) for t, c, title in data["events"]]
        except FileNotFoundError:
            continue
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log.warning("Календарь бота %s не читается: %s", path, exc)
            continue
        for t, c, title in rows:
            ts.append(t), ccy.append(c), ev.append(title)
    return pd.DataFrame({"ts": pd.to_datetime(ts, unit="ms", utc=True), "currency": ccy, "impact": [IMPACT] * len(ts), "event": ev})


def sync_bot_calendar(store: Store, dirs: tuple[Path, ...]) -> int:
    """Подмешивает календарь бота в хранилище анализатора. Возвращает число новых событий."""
    events = read_bot_calendar(dirs)
    if events.empty:
        return 0
    return store.merge_calendar(events, "bot")
