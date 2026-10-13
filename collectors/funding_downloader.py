"""Скачивание истории фандинга Binance USDⓈ-M Futures -> data/raw/funding/<SYMBOL>/<YYYY>.parquet.

Запуск (из корня проекта):  python -m collectors.funding_downloader

Только закрытые ставки (/fapi/v1/fundingRate): запись {fundingTime, fundingRate, markPrice}
появляется после списания. Свойства те же, что у klines_downloader:
  * докачка: стартует с fundingTime последней сохранённой записи + 1, дублей не создаёт;
  * запись идемпотентна и атомарна (.tmp.parquet -> rename);
  * записи позже серверного времени не сохраняются (будущих ставок в данных быть не должно);
  * при старте проверяет .ban_lock (бан не истёк -> код 2 без запросов);
  * берёт .instance_lock.funding_downloader (второй экземпляр -> код 1);
  * любая ошибка - исключение и выход (при 429/418 срок бана пишется в .ban_lock).

Историю нельзя «переписывать молча»: если для уже сохранённого fundingTime биржа отдаёт
другую ставку, это DataError (ставка влияет на PnL бэктеста и на метки).

Фактическое время списания у биржи может отличаться от сетки 00/08/16:00 на миллисекунды,
поэтому код нигде не округляет fundingTime к сетке. Отклонение от 8-часового шага больше
минуты только пишется в лог (возможна смена интервала у символа).
"""

import logging
import math
import re
from pathlib import Path

import aiohttp
import polars as pl

import config
from utils import ban_lock
from utils.binance_rest import BinanceRest
from utils.errors import DataError
from utils.instance_lock import InstanceLock
from utils.runner import run
from utils.storage import atomic_write_parquet, cleanup_stale_tmp

log = logging.getLogger("funding_downloader")

FUNDING_SCHEMA = pl.Schema(
    {
        "funding_time": pl.Int64,  # мс UTC, момент списания (серверное время)
        "funding_rate": pl.Float64,
        "mark_price": pl.Float64,  # null, если биржа вернула пустую строку
    }
)

_YEAR_FILE = re.compile(r"^\d{4}\.parquet$")  # *.tmp.parquet под шаблон не попадает
_MAX_ABS_RATE = 1.0  # ставка >= 100% за интервал - явный мусор
_SCHEDULE_TOLERANCE_MS = 60_000


def parse_funding(rows: object, symbol: str) -> pl.DataFrame:
    """Ответ биржи -> DataFrame. Любое отклонение от формата - DataError."""
    if not isinstance(rows, list):
        raise DataError(f"Ответ fundingRate не список: {str(rows)[:200]}")
    cols: dict[str, list] = {name: [] for name in FUNDING_SCHEMA}
    for row in rows:
        if not isinstance(row, dict):
            raise DataError(f"Неожиданная запись фандинга: {str(row)[:200]}")
        if row.get("symbol") != symbol:
            raise DataError(f"Запись для другого символа ({row.get('symbol')!r} != {symbol!r}): {str(row)[:200]}")
        funding_time = row.get("fundingTime")
        if isinstance(funding_time, bool) or not isinstance(funding_time, int) or funding_time <= 0:
            raise DataError(f"Некорректный fundingTime: {str(row)[:200]}")
        try:
            rate = float(row["fundingRate"])
        except (KeyError, TypeError, ValueError) as exc:
            raise DataError(f"Некорректный fundingRate: {str(row)[:200]}") from exc
        if not math.isfinite(rate) or abs(rate) >= _MAX_ABS_RATE:
            raise DataError(f"Недопустимое значение fundingRate: {str(row)[:200]}")
        raw_mark = row.get("markPrice")
        mark = None
        if raw_mark not in (None, ""):
            try:
                mark = float(raw_mark)
            except (TypeError, ValueError) as exc:
                raise DataError(f"Некорректный markPrice: {str(row)[:200]}") from exc
            if not math.isfinite(mark) or mark <= 0:
                raise DataError(f"Недопустимое значение markPrice: {str(row)[:200]}")
        cols["funding_time"].append(funding_time)
        cols["funding_rate"].append(rate)
        cols["mark_price"].append(mark)
    return pl.DataFrame(cols, schema=FUNDING_SCHEMA)


def year_files(symbol_dir: Path) -> list[Path]:
    return sorted(p for p in symbol_dir.glob("*.parquet") if _YEAR_FILE.match(p.name))


def last_stored_funding_time(symbol_dir: Path) -> int | None:
    files = year_files(symbol_dir)
    if not files:
        return None
    try:
        last = pl.read_parquet(files[-1], columns=["funding_time"])["funding_time"].max()
    except Exception as exc:
        raise DataError(f"Не удалось прочитать {files[-1]}: {exc}") from exc
    if last is None:
        raise DataError(f"Файл {files[-1]} пуст")
    return int(last)


def upsert_year(path: Path, new: pl.DataFrame) -> int:
    """Объединяет новые записи с файлом года. Возвращает число добавленных строк.

    Идемпотентно. Расхождение ставки для уже сохранённого fundingTime - DataError.
    """
    old = None
    if path.exists():
        try:
            old = pl.read_parquet(path)
        except Exception as exc:
            raise DataError(f"Не удалось прочитать {path}: {exc}") from exc
        if old.schema != FUNDING_SCHEMA:
            raise DataError(f"Схема {path} не совпадает с ожидаемой: {old.schema}")
        clash = old.join(new, on="funding_time", suffix="_new").filter(
            pl.col("funding_rate") != pl.col("funding_rate_new")
        )
        if clash.height:
            raise DataError(
                f"{path}: биржа отдала другую ставку для уже сохранённого fundingTime "
                f"{clash['funding_time'].to_list()[:3]}; историю молча не переписываем"
            )
        merged = pl.concat([old, new])
    else:
        merged = new
    merged = merged.unique(subset="funding_time", keep="last", maintain_order=True).sort("funding_time")
    if old is not None and merged.equals(old):
        return 0
    atomic_write_parquet(merged, path)
    return merged.height - (old.height if old is not None else 0)


def flush_batch(symbol_dir: Path, batch: pl.DataFrame) -> int:
    """Раскладывает записи по годовым файлам. Возвращает число новых записей."""
    if batch.is_empty():
        return 0
    df = batch.with_columns(
        pl.from_epoch("funding_time", time_unit="ms").dt.year().alias("_year")
    )
    added = 0
    for year in df["_year"].unique().sort().to_list():
        part = df.filter(pl.col("_year") == year).drop("_year")
        added += upsert_year(symbol_dir / f"{year:04d}.parquet", part)
    return added


def off_schedule_steps(batch: pl.DataFrame, interval_ms: int) -> int:
    """Сколько шагов между соседними записями отличается от интервала больше чем на минуту."""
    steps = batch["funding_time"].diff().drop_nulls()
    return int(((steps - interval_ms).abs() > _SCHEDULE_TOLERANCE_MS).sum())


async def download_symbol(api: BinanceRest, symbol: str) -> int:
    symbol_dir = config.FUNDING_DIR / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    for stale in cleanup_stale_tmp(symbol_dir):
        log.warning("Удалён остаток прерванной записи: %s", stale)

    last = last_stored_funding_time(symbol_dir)
    cursor = config.HISTORY_START_MS if last is None else last + 1
    server_now = await api.server_time_ms()
    log.info("%s: старт с fundingTime>=%d (%s)", symbol, cursor,
             "докачка" if last is not None else "с начала истории")

    added_total = 0
    while True:
        rows = await api.get(
            config.FUNDING_PATH,
            {"symbol": symbol, "startTime": cursor, "limit": config.FUNDING_LIMIT},
            weight=1,
        )
        batch = parse_funding(rows, symbol)
        if batch.is_empty():
            break
        times = batch["funding_time"]
        if times[0] < cursor or not times.is_sorted() or times.n_unique() != len(times):
            raise DataError(
                f"{symbol}: fundingTime не растёт или раньше запрошенного ({cursor}): {times.head(3).to_list()}"
            )
        off = off_schedule_steps(batch, config.FUNDING_INTERVAL_MS)
        if off:
            log.warning("%s: %d шагов фандинга отличаются от %d мс (смена интервала или пропуск)",
                        symbol, off, config.FUNDING_INTERVAL_MS)

        settled = batch.filter(pl.col("funding_time") <= server_now)
        added_total += flush_batch(symbol_dir, settled)  # сразу на диск: после сбоя докачка с этого места
        cursor = int(times[-1]) + 1
        if settled.height < batch.height or batch.height < config.FUNDING_LIMIT:
            break  # дошли до будущего или биржа отдала всё, что есть

    log.info("%s: готово, новых записей %d", symbol, added_total)
    return added_total


async def main() -> None:
    config.validate()
    ban_lock.ensure_not_banned()  # бан не истёк -> BanActive, код 2, без обращения к бирже
    with InstanceLock("funding_downloader"):  # второй экземпляр -> код 1, замок держится до выхода
        timeout = aiohttp.ClientTimeout(
            total=config.HTTP_TOTAL_TIMEOUT_S, connect=config.HTTP_CONNECT_TIMEOUT_S
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            api = BinanceRest(session, config.REST_BASE_URL)
            for symbol in config.SYMBOLS:
                await download_symbol(api, symbol)


if __name__ == "__main__":
    run(main, "funding_downloader")
