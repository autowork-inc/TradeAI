"""Схемы данных Этапа 1 глазами конвейера.

KLINE_SCHEMA дублирует collectors.klines_downloader.KLINE_SCHEMA намеренно: конвейер не
должен импортировать сборщик (aiohttp, замки). Совпадение проверяет тест test_loaders.
"""

import polars as pl

KLINE_SCHEMA = pl.Schema(
    {
        "open_time": pl.Int64,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
        "close_time": pl.Int64,
        "quote_volume": pl.Float64,
        "trades": pl.Int64,
        "taker_buy_base": pl.Float64,
        "taker_buy_quote": pl.Float64,
    }
)

# Строка jsonl сборщика. data - объединение полей сообщения потока (E,T,s,U,u,pu,b,a)
# и REST-снапшота (lastUpdateId,E,T,bids,asks). Лишние поля (e и т.п.) игнорируются,
# значение не того типа превращается в null (read_ndjson(..., ignore_errors=True)).
_LEVELS = pl.List(pl.List(pl.String))
RAW_DEPTH_SCHEMA = pl.Schema(
    {
        "recv_ts": pl.Int64,
        "symbol": pl.String,
        "stream": pl.String,
        "data": pl.Struct(
            {
                "E": pl.Int64,
                "T": pl.Int64,
                "s": pl.String,
                "U": pl.Int64,
                "u": pl.Int64,
                "pu": pl.Int64,
                "b": _LEVELS,
                "a": _LEVELS,
                "lastUpdateId": pl.Int64,
                "bids": _LEVELS,
                "asks": _LEVELS,
            }
        ),
    }
)

SNAPSHOT_STREAM = "snapshot"

# Закрытые ставки фандинга (collectors.funding_downloader.FUNDING_SCHEMA; дублируется по той же
# причине, что и KLINE_SCHEMA). funding_time - серверное время списания, мс UTC.
FUNDING_SCHEMA = pl.Schema(
    {
        "funding_time": pl.Int64,
        "funding_rate": pl.Float64,
        "mark_price": pl.Float64,  # null, если биржа не отдала (старые записи)
    }
)


def depth_schema(levels: int) -> pl.Schema:
    """Нормализованное сообщение стакана: top-`levels` уровней в плоских колонках."""
    cols: dict[str, pl.DataType] = {
        "recv_ts": pl.Int64,
        "symbol": pl.String,
        "E": pl.Int64,
        "T": pl.Int64,
        "u": pl.Int64,
        "pu": pl.Int64,
    }
    for i in range(levels):
        for side in ("bid", "ask"):
            cols[f"{side}_px_{i}"] = pl.Float64
            cols[f"{side}_qty_{i}"] = pl.Float64
    return pl.Schema(cols)


# Карантин загрузчика: всё, что не попало в нормализованные данные, с причиной.
REJECT_SCHEMA = pl.Schema(
    {
        "symbol": pl.String,
        "source": pl.String,  # <YYYY-MM-DD>/<HH>.jsonl
        "line_no": pl.Int64,
        "reason": pl.String,  # bad_json | schema | empty_book | bad_level | crossed | duplicate_u
        "recv_ts": pl.Int64,
        "u": pl.Int64,
        "raw": pl.String,  # только для bad_json, обрезано
    }
)
