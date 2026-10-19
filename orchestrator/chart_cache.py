"""Кэш свечей MT5 для графика клиента (2026-10-19). Бот и так держит M5-хранилище Forex/металлов (orchestrator/mt5_feeds._Group) и получает котировку раз в секунду; здесь они
дублируются в файлы `DATA_DIR/.chart_cache/<SYMBOL>.json` (закрытые M5, пишется при появлении новой свечи) и `<SYMBOL>.live.json` (формирующаяся M5 по котировкам, не чаще раза
в секунду). API (api/candles.py, каталог данных смонтирован только для чтения) собирает из них свечи любого таймфрейма: у API нет доступа к MetaApi, и он не должен его получать.

Это ЧИСТО справочные данные для человека: ошибка записи (диск, права) не должна ронять торговый процесс - только предупреждение в лог (ожидаемый отказ ОС, `OSError`).
Ни одна из стратегий и ни один контур исполнения эти файлы не читает."""

import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

log = logging.getLogger("orchestrator.chart_cache")
DIR_NAME = ".chart_cache"
M5_MS = 5 * 60_000
LIVE_EVERY_MS = 1000
WARN_EVERY_MS = 600_000


class ChartCache:
    def __init__(self, directory: Path):
        self._dir = directory
        self._last_closed: dict[str, int] = {}
        self._live: dict[str, list[float]] = {}  # символ -> [bucket_ms, o, h, l, c]
        self._live_written: dict[str, int] = {}
        self._warned_ms = 0

    def _write(self, name: str, payload: Any, now_ms: int) -> None:
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            tmp = self._dir / (name + ".tmp")
            tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, self._dir / name)
        except OSError as exc:
            if now_ms - self._warned_ms >= WARN_EVERY_MS:
                self._warned_ms = now_ms
                log.warning("Кэш свечей для графика не записан (%s): %s. Торговля не затронута", self._dir, exc)

    def store_closed(self, symbol: str, store: Mapping[int, tuple], now_ms: int = 0) -> None:
        """Закрытые M5 символа (строки KLINE_SCHEMA: open_time, o, h, l, c, v, ...). Файл переписывается только когда появилась новая свеча."""
        if not store:
            return
        last = max(store)
        if self._last_closed.get(symbol) == last:
            return
        self._last_closed[symbol] = last
        rows = [[t, r[1], r[2], r[3], r[4], r[5]] for t, r in sorted(store.items())]
        self._write(f"{symbol}.json", {"symbol": symbol, "interval_ms": M5_MS, "candles": rows}, now_ms)

    def on_tick(self, symbol: str, ts_ms: int, price: float) -> None:
        """Котировка (середина bid/ask): растёт формирующаяся M5. Файл `.live.json` обновляется не чаще раза в секунду."""
        bucket = ts_ms - ts_ms % M5_MS
        cur = self._live.get(symbol)
        if cur is None or cur[0] != bucket:
            self._live[symbol] = cur = [bucket, price, price, price, price]
        else:
            cur[2], cur[3], cur[4] = max(cur[2], price), min(cur[3], price), price
        if ts_ms - self._live_written.get(symbol, 0) >= LIVE_EVERY_MS:
            self._live_written[symbol] = ts_ms
            self._write(f"{symbol}.live.json", {"symbol": symbol, "ts": ts_ms, "candle": cur}, ts_ms)
