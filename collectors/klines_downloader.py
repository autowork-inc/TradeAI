"""Скачивание истории свечей Binance USDⓈ-M Futures -> data/raw/klines/<SYMBOL>/<TF>/<YYYY-MM>.parquet.

Запуск (из корня проекта):  python -m collectors.klines_downloader

Свойства:
  * докачка: стартует с последней сохранённой свечи, дублей не создаёт;
  * запись идемпотентна и атомарна (.tmp.parquet -> rename);
  * последняя незакрытая свеча не сохраняется;
  * при старте проверяет .ban_lock (бан не истёк -> код 2 без запросов);
  * берёт .instance_lock.klines_downloader (второй экземпляр -> код 1);
  * любая ошибка - исключение и выход (при 429/418 срок бана пишется в .ban_lock).
"""

import logging
import re
from pathlib import Path

import aiohttp
import polars as pl

import config
from utils import ban_lock
from utils.binance_rest import BinanceRest, klines_weight
from utils.errors import DataError, TradingInfraError
from utils.instance_lock import InstanceLock
from utils.runner import run
from utils.storage import atomic_write_parquet, cleanup_stale_tmp

log = logging.getLogger("klines_downloader")

KLINE_SCHEMA = pl.Schema(
    {
        "open_time": pl.Int64,  # мс UTC
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "close_time": pl.Int64,  # мс UTC
        "quote_volume": pl.Float64,
        "trades": pl.Int64,
        "taker_buy_base": pl.Float64,
        "taker_buy_quote": pl.Float64,
    }
)

# Индекс поля в массиве свечи Binance -> (имя колонки, приведение типа).
# Поле 11 ("ignore") не сохраняется.
_FIELDS = [
    (0, "open_time", int),
    (1, "open", float),
    (2, "high", float),
    (3, "low", float),
    (4, "close", float),
    (5, "volume", float),
    (6, "close_time", int),
    (7, "quote_volume", float),
    (8, "trades", int),
    (9, "taker_buy_base", float),
    (10, "taker_buy_quote", float),
]
_ROW_LEN = 12
_MONTH_FILE = re.compile(r"^\d{4}-\d{2}\.parquet$")  # *.tmp.parquet под шаблон не попадает


def parse_klines(rows: object) -> pl.DataFrame:
    """Ответ биржи -> DataFrame. Любое отклонение от формата - DataError."""
    if not isinstance(rows, list):
        raise DataError(f"Ответ klines не список: {str(rows)[:200]}")
    columns: dict[str, list] = {name: [] for _, name, _ in _FIELDS}
    for row in rows:
        if not isinstance(row, list) or len(row) != _ROW_LEN:
            raise DataError(f"Неожиданная строка свечи: {str(row)[:200]}")
        try:
            for idx, name, cast in _FIELDS:
                columns[name].append(cast(row[idx]))
        except (TypeError, ValueError) as exc:
            raise DataError(f"Не удалось разобрать свечу {str(row)[:200]}: {exc}") from exc
    return pl.DataFrame(columns, schema=KLINE_SCHEMA)


def month_files(pair_dir: Path) -> list[Path]:
    return sorted(p for p in pair_dir.glob("*.parquet") if _MONTH_FILE.match(p.name))


def last_stored_open_time(pair_dir: Path) -> int | None:
    files = month_files(pair_dir)
    if not files:
        return None
    try:
        last = pl.read_parquet(files[-1], columns=["open_time"])["open_time"].max()
    except Exception as exc:
        raise DataError(f"Не удалось прочитать {files[-1]}: {exc}") from exc
    if last is None:
        raise DataError(f"Файл {files[-1]} пуст")
    return int(last)


def upsert_month(path: Path, new: pl.DataFrame) -> int:
    """Объединяет новые свечи с файлом месяца. Возвращает число добавленных строк.

    Идемпотентно: повторная запись тех же свечей файл не меняет.
    """
    old = None
    if path.exists():
        try:
            old = pl.read_parquet(path)
        except Exception as exc:
            raise DataError(f"Не удалось прочитать {path}: {exc}") from exc
        if old.schema != KLINE_SCHEMA:
            raise DataError(f"Схема {path} не совпадает с ожидаемой: {old.schema}")
        merged = pl.concat([old, new])
    else:
        merged = new
    merged = merged.unique(subset="open_time", keep="last", maintain_order=True).sort("open_time")
    if old is not None and merged.equals(old):
        return 0
    atomic_write_parquet(merged, path)
    return merged.height - (old.height if old is not None else 0)


def flush_frames(pair_dir: Path, frames: list[pl.DataFrame]) -> int:
    """Раскладывает накопленные свечи по месячным файлам. Возвращает число новых свечей."""
    if not frames:
        return 0
    df = pl.concat(frames)
    df = df.with_columns(
        pl.from_epoch("open_time", time_unit="ms").dt.strftime("%Y-%m").alias("_month")
    )
    added = 0
    for month in df["_month"].unique().sort().to_list():
        part = df.filter(pl.col("_month") == month).drop("_month")
        added += upsert_month(pair_dir / f"{month}.parquet", part)
    return added


async def download_pair(api: BinanceRest, symbol: str, interval: str) -> int:
    interval_ms = config.INTERVAL_MS[interval]
    pair_dir = config.KLINES_DIR / symbol / interval
    pair_dir.mkdir(parents=True, exist_ok=True)
    for stale in cleanup_stale_tmp(pair_dir):
        log.warning("Удалён остаток прерванной записи: %s", stale)

    last = last_stored_open_time(pair_dir)
    cursor = config.HISTORY_START_MS if last is None else last + interval_ms
    server_now = await api.server_time_ms()
    log.info(
        "%s %s: старт с open_time=%d (%s)", symbol, interval, cursor,
        "докачка" if last is not None else "с начала истории",
    )

    weight = klines_weight(config.KLINES_LIMIT)
    frames: list[pl.DataFrame] = []
    batches = 0
    added_total = 0
    try:
        while cursor + interval_ms <= server_now:  # дальше свеча заведомо ещё не закрыта
            rows = await api.get(
                config.KLINES_PATH,
                {"symbol": symbol, "interval": interval, "startTime": cursor,
                 "limit": config.KLINES_LIMIT},
                weight=weight,
            )
            batch = parse_klines(rows)
            if batch.is_empty():
                break
            opens = batch["open_time"]
            if opens[0] < cursor or not opens.is_sorted(descending=False) or opens.n_unique() != len(opens):
                raise DataError(
                    f"{symbol} {interval}: open_time не растёт или раньше запрошенного "
                    f"({cursor}): {opens.head(3).to_list()}"
                )
            gaps = int((opens.diff().drop_nulls() != interval_ms).sum())
            if gaps:
                log.warning("%s %s: %d разрывов внутри пакета (пропуски на бирже)", symbol, interval, gaps)

            cursor = int(opens[-1]) + interval_ms
            closed = batch.filter(pl.col("close_time") < server_now)
            if not closed.is_empty():
                frames.append(closed)
            batches += 1
            if batches % config.KLINES_FLUSH_EVERY_BATCHES == 0:
                added_total += flush_frames(pair_dir, frames)
                frames.clear()
                log.info("%s %s: пакетов %d, новых свечей %d, курсор %d",
                         symbol, interval, batches, added_total, cursor)
            if batch.height < config.KLINES_LIMIT:
                break  # биржа отдала всё, что есть
    except TradingInfraError:
        # Сохраняем уже проверенное, чтобы после перезапуска не качать заново.
        try:
            added_total += flush_frames(pair_dir, frames)
        except Exception:
            log.exception("%s %s: не удалось сохранить накопленное перед выходом", symbol, interval)
        raise

    added_total += flush_frames(pair_dir, frames)
    log.info("%s %s: готово, новых свечей %d", symbol, interval, added_total)
    return added_total


async def main() -> None:
    config.validate()
    ban_lock.ensure_not_banned()  # бан не истёк -> BanActive, код 2, без обращения к бирже
    with InstanceLock("klines_downloader"):  # второй экземпляр -> код 1, замок держится до выхода
        timeout = aiohttp.ClientTimeout(
            total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            api = BinanceRest(session, config.REST_BASE_URL)
            for symbol in config.SYMBOLS:
                for interval in config.KLINE_INTERVALS:
                    await download_pair(api, symbol, interval)


if __name__ == "__main__":
    run(main, "klines_downloader")
