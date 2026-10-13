"""Стакан Этапа 1: data/raw/depth/<SYMBOL>/<YYYY-MM-DD>/<HH>.jsonl -> нормализованный parquet.

Два шага:
  normalize_day()  разбор jsonl одного UTC-дня (по recv_ts, как режет файлы сборщик) в
                   data/interim/depth/<SYMBOL>/<YYYY-MM-DD>.parquet. Идемпотентен, запись
                   атомарна. Всё, что не прошло, уходит в <день>.rejects.parquet с причиной
                   (карантин), молча ничего не теряется;
  scan()           ленивое чтение нормализованных дней по диапазону recv_ts.

Что считается браком (reason в карантине):
  bad_json    - строка не разбирается (оборванная запись после падения, мусор);
  schema      - нет recv_ts/symbol/E/u, значение не того типа, s не совпадает с symbol;
  empty_book  - пустая сторона стакана;
  bad_level   - у существующего уровня цена/объём не число, цена <= 0, объём < 0;
  crossed     - лучший бид >= лучшего аска;
  duplicate_u - повтор (symbol, u): остаётся первое по recv_ts.
REST-снапшоты (stream="snapshot") в нормализованные данные не входят (другая глубина,
в сетку признаков не идут); их число возвращается в NormalizeResult.
"""

import io
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import config
from pipeline.loaders.schemas import RAW_DEPTH_SCHEMA, REJECT_SCHEMA, SNAPSHOT_STREAM, depth_schema
from utils.errors import DataError
from utils.storage import atomic_write_parquet, cleanup_stale_tmp

log = logging.getLogger("depth_loader")

_RAW_SNIPPET = 300  # сколько символов битой строки хранить в карантине


@dataclass(frozen=True)
class ParsedFile:
    valid: pl.DataFrame  # схема depth_schema(levels), без снапшотов, ещё без дедупликации
    rejects: pl.DataFrame  # схема REJECT_SCHEMA
    n_lines: int
    n_snapshots: int


@dataclass(frozen=True)
class NormalizeResult:
    symbol: str
    day: date
    skipped: bool = False  # interim уже свежее источников
    n_files: int = 0
    n_lines: int = 0
    n_valid: int = 0
    n_rejected: int = 0
    n_bad_json: int = 0
    n_duplicates: int = 0
    n_snapshots: int = 0


def _reject_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=REJECT_SCHEMA) if rows else pl.DataFrame(schema=REJECT_SCHEMA)


def _bulk_parse(lines: list[bytes]) -> pl.DataFrame | None:
    """Быстрый разбор. None, если Polars споткнулся о синтаксис (он падает на всём файле)."""
    if not lines:
        return pl.DataFrame(schema=RAW_DEPTH_SCHEMA)
    try:
        return pl.read_ndjson(
            io.BytesIO(b"\n".join(lines)), schema=RAW_DEPTH_SCHEMA, ignore_errors=True
        )
    except pl.exceptions.PolarsError:
        return None


class DepthLoader:
    def __init__(self, raw_root: Path | None = None, interim_root: Path | None = None,
                 levels: int | None = None):
        self._raw_root = raw_root
        self._interim_root = interim_root
        self._levels = levels

    # ------------------------------------------------------------------ пути

    @property
    def levels(self) -> int:
        return self._levels or config.DEPTH_LEVELS

    def _raw(self) -> Path:
        return self._raw_root or config.DEPTH_DIR

    def _interim(self) -> Path:
        return self._interim_root or config.DEPTH_INTERIM_DIR

    def interim_path(self, symbol: str, day: date) -> Path:
        return self._interim() / symbol / f"{day:%Y-%m-%d}.parquet"

    def rejects_path(self, symbol: str, day: date) -> Path:
        return self._interim() / symbol / f"{day:%Y-%m-%d}.rejects.parquet"

    # ------------------------------------------------------------ разбор файла

    def parse_file(self, path: Path, symbol: str, *, source: str | None = None) -> ParsedFile:
        source = source or path.name
        raw = path.read_bytes()
        numbered = [(n, ln.strip()) for n, ln in enumerate(raw.split(b"\n"), 1) if ln.strip()]
        rejects: list[dict] = []

        # Дешёвый предфильтр: оборванная строка почти всегда не заканчивается на "}".
        good: list[tuple[int, bytes]] = []
        for n, ln in numbered:
            if ln.startswith(b"{") and ln.endswith(b"}"):
                good.append((n, ln))
            else:
                rejects.append(self._bad_json(symbol, source, n, ln))

        parsed = _bulk_parse([ln for _, ln in good])
        if parsed is None:
            # Медленный путь: найти строки, на которых падает разбор, поодиночке.
            kept = []
            for n, ln in good:
                try:
                    ok = isinstance(json.loads(ln), dict)
                except ValueError:
                    ok = False
                if ok:
                    kept.append((n, ln))
                else:
                    rejects.append(self._bad_json(symbol, source, n, ln))
            good = kept
            parsed = _bulk_parse([ln for _, ln in good])
            if parsed is None:
                raise DataError(f"{path}: не удалось разобрать файл даже после отсева битых строк")
        if parsed.height != len(good):
            raise DataError(f"{path}: число разобранных строк {parsed.height} != {len(good)}")

        frame = parsed.with_columns(line_no=pl.Series([n for n, _ in good], dtype=pl.Int64)).unnest("data")
        n_snapshots = int((frame["stream"] == SNAPSHOT_STREAM).sum())
        frame = frame.filter(pl.col("stream") != SNAPSHOT_STREAM)

        valid, struct_rejects = self._normalize(frame, symbol, source)
        rejects.extend(struct_rejects.to_dicts())
        return ParsedFile(valid, _reject_frame(rejects), len(numbered), n_snapshots)

    @staticmethod
    def _bad_json(symbol: str, source: str, line_no: int, line: bytes) -> dict:
        return {
            "symbol": symbol, "source": source, "line_no": line_no, "reason": "bad_json",
            "recv_ts": None, "u": None, "raw": line[:_RAW_SNIPPET].decode("utf-8", errors="replace"),
        }

    def _normalize(self, frame: pl.DataFrame, symbol: str, source: str) -> tuple[pl.DataFrame, pl.DataFrame]:
        levels = self.levels
        out_schema = depth_schema(levels)
        n_b = pl.col("b").list.len().fill_null(0)
        n_a = pl.col("a").list.len().fill_null(0)

        level_cols, bad_terms = [], []
        for i in range(levels):
            for side, lst, n in (("bid", "b", n_b), ("ask", "a", n_a)):
                item = pl.col(lst).list.get(i, null_on_oob=True)
                px = item.list.get(0, null_on_oob=True).cast(pl.Float64, strict=False)
                qty = item.list.get(1, null_on_oob=True).cast(pl.Float64, strict=False)
                level_cols += [px.alias(f"{side}_px_{i}"), qty.alias(f"{side}_qty_{i}")]
                bad = (px.is_null() | qty.is_null() | ~px.is_finite() | ~qty.is_finite()
                       | (px <= 0) | (qty < 0))
                bad_terms.append(pl.when(pl.lit(i) < n).then(bad).otherwise(False))

        wide = frame.with_columns(level_cols) if level_cols else frame
        schema_bad = (
            pl.col("recv_ts").is_null() | pl.col("symbol").is_null() | pl.col("E").is_null()
            | pl.col("u").is_null() | (pl.col("s") != pl.col("symbol")) | pl.col("s").is_null()
        )
        crossed = (pl.col("bid_px_0") >= pl.col("ask_px_0")) if levels else pl.lit(False)
        reason = (
            pl.when(schema_bad).then(pl.lit("schema"))
            .when((n_b == 0) | (n_a == 0)).then(pl.lit("empty_book"))
            .when(pl.any_horizontal(bad_terms) if bad_terms else pl.lit(False)).then(pl.lit("bad_level"))
            .when(crossed.fill_null(False)).then(pl.lit("crossed"))
        )
        wide = wide.with_columns(_reason=reason)
        valid = wide.filter(pl.col("_reason").is_null()).select(list(out_schema))
        bad = wide.filter(pl.col("_reason").is_not_null()).select(
            pl.lit(symbol).alias("symbol"),
            pl.lit(source).alias("source"),
            pl.col("line_no"),
            pl.col("_reason").alias("reason"),
            pl.col("recv_ts"),
            pl.col("u"),
            pl.lit(None, dtype=pl.String).alias("raw"),
        )
        return valid, bad

    # ------------------------------------------------------- нормализация дня

    def normalize_day(self, symbol: str, day: date, *, force: bool = False) -> NormalizeResult:
        day_dir = self._raw() / symbol / f"{day:%Y-%m-%d}"
        sources = sorted(day_dir.glob("[0-9][0-9].jsonl")) if day_dir.is_dir() else []
        if not sources:
            return NormalizeResult(symbol, day)

        target = self.interim_path(symbol, day)
        is_closed_day = day < datetime.now(timezone.utc).date()
        if (not force and is_closed_day and target.is_file()
                and target.stat().st_mtime >= max(p.stat().st_mtime for p in sources)):
            return NormalizeResult(symbol, day, skipped=True, n_files=len(sources))

        valid_parts, reject_parts, n_lines, n_snapshots = [], [], 0, 0
        for path in sources:
            parsed = self.parse_file(path, symbol, source=f"{day:%Y-%m-%d}/{path.name}")
            valid_parts.append(parsed.valid)
            reject_parts.append(parsed.rejects)
            n_lines += parsed.n_lines
            n_snapshots += parsed.n_snapshots

        schema = depth_schema(self.levels)
        valid = pl.concat(valid_parts) if valid_parts else pl.DataFrame(schema=schema)
        valid = valid.sort("recv_ts", "u").with_columns(
            _first=pl.struct("symbol", "u").is_first_distinct()
        )
        dups = valid.filter(~pl.col("_first")).select(
            pl.col("symbol"),
            pl.lit("").alias("source"),
            pl.lit(None, dtype=pl.Int64).alias("line_no"),
            pl.lit("duplicate_u").alias("reason"),
            pl.col("recv_ts"),
            pl.col("u"),
            pl.lit(None, dtype=pl.String).alias("raw"),
        )
        valid = valid.filter(pl.col("_first")).drop("_first")
        rejects = pl.concat([*reject_parts, dups.cast(dict(REJECT_SCHEMA))])

        directory = target.parent
        directory.mkdir(parents=True, exist_ok=True)
        for stale in cleanup_stale_tmp(directory):
            log.warning("Удалён остаток прерванной записи: %s", stale)
        atomic_write_parquet(valid, target)
        rejects_target = self.rejects_path(symbol, day)
        if rejects.height:
            atomic_write_parquet(rejects, rejects_target)
        else:
            rejects_target.unlink(missing_ok=True)  # от прошлой версии разбора мог остаться

        return NormalizeResult(
            symbol, day, n_files=len(sources), n_lines=n_lines, n_valid=valid.height,
            n_rejected=rejects.height,
            n_bad_json=int((rejects["reason"] == "bad_json").sum()),
            n_duplicates=dups.height, n_snapshots=n_snapshots,
        )

    def normalize_range(self, symbol: str, start_ms: int, end_ms: int, *, force: bool = False
                        ) -> list[NormalizeResult]:
        return [self.normalize_day(symbol, d, force=force) for d in _days(start_ms, end_ms)]

    # ----------------------------------------------------------------- чтение

    def scan(self, symbol: str, start_ms: int, end_ms: int) -> pl.LazyFrame:
        """Нормализованные сообщения с recv_ts в [start_ms, end_ms), по порядку recv_ts.

        Добавляет recv_gap_ms (пауза с прошлого сообщения символа) и gap_before
        (pu != предыдущий u: пропуск в нумерации биржи; у первой строки False).
        gap_before - диагностика: семантику pu у partial-depth потока сверить на реальных данных.
        Отсутствующие дни пропускаются (дыра видна в recv_gap_ms). Нормализуйте диапазон заранее.
        """
        if end_ms <= start_ms:
            raise DataError(f"Пустой диапазон: [{start_ms}, {end_ms})")
        schema = depth_schema(self.levels)
        files = [p for d in _days(start_ms, end_ms) if (p := self.interim_path(symbol, d)).is_file()]
        for path in files:
            have = pl.read_parquet_schema(path)
            if dict(have) != dict(schema):
                raise DataError(
                    f"Схема {path} не совпадает с ожидаемой (DEPTH_LEVELS={self.levels}); "
                    "пересоберите кэш: normalize_day(..., force=True)"
                )
        base = pl.scan_parquet(files) if files else pl.DataFrame(schema=schema).lazy()
        return (
            base.filter((pl.col("recv_ts") >= start_ms) & (pl.col("recv_ts") < end_ms))
            .sort("recv_ts", "u")
            .unique(subset=["symbol", "u"], keep="first", maintain_order=True)  # дубли между днями
            .with_columns(
                recv_gap_ms=pl.col("recv_ts").diff().over("symbol"),
                gap_before=(pl.col("pu") != pl.col("u").shift(1).over("symbol")).fill_null(False),
            )
        )


def _days(start_ms: int, end_ms: int) -> list[date]:
    first = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
    last = datetime.fromtimestamp((end_ms - 1) / 1000, tz=timezone.utc).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]
