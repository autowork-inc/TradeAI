"""Макро-новости: загрузка календаря (CSV / JSON ForexFactory), кросс-чек с временем сделок, сравнение «все сделки» и «бот выключен на новостях».

Анализатор сам в сеть не ходит (изолирован): календарь загружается файлом через API (`POST /calendar`) и хранится в SQLite анализатора.

Форматы:
  CSV (заголовок обязателен), колонки - любые из синонимов:
      время:  timestamp | ts | datetime | time_utc | date (+ time);  ISO 8601 (без пояса = UTC) либо epoch секунд/мс
      валюта: currency | ccy | country;   важность: impact | importance | volatility (High/Medium/Low, 3/2/1, red/orange/yellow)
      событие: event | title | name
  JSON: список объектов ForexFactory `{"date": "2026-10-14T08:30:00-04:00", "country": "USD", "impact": "High", "title": "CPI m/m"}` (есть у бота: nfs.faireconomy.media).

Окно новости (ТЗ): сделка исключается, если она ОТКРЫЛАСЬ или ЗАКРЫЛАСЬ в интервале [новость - before, новость + after] (режим `touch`);
режим `span` - если позиция была открыта хотя бы часть интервала. Валюты события сверяются с символом: крипта и металлы - USD; пара - обе валюты; событие ALL - всем.
"""

import csv
import io
import json
import re
from typing import Any

import numpy as np
import pandas as pd

from analyzer.errors import CalendarError
from analyzer.metrics import clean, compute_metrics, epoch_ms, equity_curve

MAX_EVENTS = 100_000
MIN_MS = 60_000
TS_KEYS = ("timestamp", "ts", "datetime", "time_utc", "date_time", "date")
CCY_KEYS = ("currency", "ccy", "country")
IMPACT_KEYS = ("impact", "importance", "volatility")
EVENT_KEYS = ("event", "title", "name")
IMPACT_MAP = {"high": "high", "3": "high", "red": "high", "medium": "medium", "med": "medium", "2": "medium", "orange": "medium", "low": "low", "1": "low", "yellow": "low",
              "holiday": "holiday", "non-economic": "holiday", "gray": "holiday", "grey": "holiday"}
EVENT_KINDS = (("CPI", r"\bcpi\b|consumer price"), ("NFP", r"non-?farm|\bnfp\b|employment change"), ("FOMC", r"fomc|federal funds|fed chair|powell|rate statement"),
               ("PPI", r"\bppi\b|producer price"), ("GDP", r"\bgdp\b"), ("Retail Sales", r"retail sales"), ("PMI", r"\bpmi\b"), ("Jobless Claims", r"jobless claims"))
FOREX = re.compile(r"^[A-Z]{6}$")
KNOWN_CCY = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "CNY"}
WINDOW_GRID = (5, 10, 15, 30, 60)


def _pick(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def _parse_ts(value: Any, row_no: int) -> pd.Timestamp:
    s = str(value).strip()
    try:
        if re.fullmatch(r"\d{9,10}(\.\d+)?", s):
            return pd.Timestamp(float(s), unit="s", tz="UTC")
        if re.fullmatch(r"\d{12,13}", s):
            return pd.Timestamp(int(s), unit="ms", tz="UTC")
        ts = pd.Timestamp(s)
    except (ValueError, OverflowError) as exc:
        raise CalendarError(f"Строка {row_no}: не удалось разобрать время {s!r}") from exc
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def parse_calendar(content: str | bytes) -> pd.DataFrame:
    """Календарь -> DataFrame[ts (UTC), currency, impact, event], по времени. Ошибка формата - CalendarError с номером строки."""
    text = content.decode("utf-8-sig") if isinstance(content, bytes) else content.lstrip("﻿")
    text = text.strip()
    if not text:
        raise CalendarError("Календарь пуст")
    if text[0] in "[{":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise CalendarError(f"Некорректный JSON: {exc}") from exc
        rows = data.get("events") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            raise CalendarError("JSON-календарь должен быть списком событий")
        rows = [{str(k).lower(): v for k, v in r.items()} for r in rows if isinstance(r, dict)]
    else:
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise CalendarError("В CSV нет заголовка")
        rows = [{(k or "").strip().lower(): (v or "").strip() for k, v in r.items()} for r in reader]
    if len(rows) > MAX_EVENTS:
        raise CalendarError(f"Событий больше предела {MAX_EVENTS}")
    out = []
    for i, row in enumerate(rows, start=2):
        raw_ts = _pick(row, TS_KEYS)
        if raw_ts is None:
            raise CalendarError(f"Строка {i}: нет времени события (колонка {'/'.join(TS_KEYS)})")
        if row.get("date") and row.get("time") and _pick(row, TS_KEYS) == row.get("date"):
            raw_ts = f"{row['date']} {row['time']}"
        ccy = str(_pick(row, CCY_KEYS) or "ALL").strip().upper()
        impact = IMPACT_MAP.get(str(_pick(row, IMPACT_KEYS) or "").strip().lower())
        if impact is None:
            raise CalendarError(f"Строка {i}: неизвестная важность {_pick(row, IMPACT_KEYS)!r} (ожидается High/Medium/Low)")
        out.append({"ts": _parse_ts(raw_ts, i), "currency": ccy, "impact": impact, "event": str(_pick(row, EVENT_KEYS) or "")})
    if not out:
        raise CalendarError("В календаре нет событий")
    return pd.DataFrame(out).sort_values("ts", kind="stable").reset_index(drop=True)


def classify_event(title: str) -> str:
    t = title.lower()
    for name, pat in EVENT_KINDS:
        if re.search(pat, t):
            return name
    return "Other"


def currencies_for(coin: str) -> set[str]:
    """Валюты, чьи новости двигают символ: крипта/металлы - USD, пара - обе валюты."""
    c = coin.upper()
    if FOREX.match(c) and c[:3] in KNOWN_CCY and c[3:] in KNOWN_CCY:
        return {c[:3], c[3:]}
    return {"USD"}


def flag_news(df: pd.DataFrame, events: pd.DataFrame, *, before_min: float, after_min: float, mode: str = "touch") -> pd.DataFrame:
    """Для каждой сделки: попала ли в окно новости. Возвращает DataFrame[index df]: `in_news` (bool), `event` (название ближайшего события окна), `event_ts`."""
    if mode not in ("touch", "span"):
        raise ValueError("mode: touch | span")
    flags = pd.DataFrame({"in_news": False, "event": None, "event_ts": pd.NaT}, index=df.index)
    if len(df) == 0 or len(events) == 0:
        return flags
    before, after = int(before_min * MIN_MS), int(after_min * MIN_MS)
    ev_ms = epoch_ms(events["ts"])
    ev_ccy = events["currency"].to_numpy()
    ev_title = events["event"].to_numpy(dtype=object)
    a = epoch_ms(df["entry_ts"])
    b = epoch_ms(df["exit_time"])
    coins = df["coin"].to_numpy()
    in_news = np.zeros(len(df), dtype=bool)
    title = np.full(len(df), None, dtype=object)
    ts_hit = np.zeros(len(df), dtype=np.int64)
    for coin in np.unique(coins):
        pos = np.flatnonzero(coins == coin)
        sel = np.isin(ev_ccy, list(currencies_for(coin) | {"ALL"}))
        e_ms, e_title = ev_ms[sel], ev_title[sel]
        if len(e_ms) == 0:
            continue
        probes = [(a[pos], b[pos])] if mode == "span" else [(a[pos], a[pos]), (b[pos], b[pos])]
        for lo, hi in probes:
            idx = np.searchsorted(e_ms, lo - after, side="left")  # первое событие, чьё окно [e-before, e+after] ещё не закончилось к моменту lo
            ok = idx < len(e_ms)
            cand = np.where(ok, e_ms[np.minimum(idx, len(e_ms) - 1)], 0)
            hit = ok & (cand - before <= hi)
            new = hit & ~in_news[pos]
            in_news[pos[new]] = True
            title[pos[new]] = e_title[np.minimum(idx, len(e_ms) - 1)][new]
            ts_hit[pos[new]] = cand[new]
    flags["in_news"] = in_news
    flags["event"] = title
    flags["event_ts"] = pd.to_datetime(np.where(in_news, ts_hit, 0), unit="ms", utc=True).where(in_news)
    return flags


def compare_news(df: pd.DataFrame, events: pd.DataFrame, cfg: Any, *, before_min: float = 10, after_min: float = 10, mode: str = "touch",
                 impacts: tuple[str, ...] = ("high",)) -> dict[str, Any]:
    """«Результат со всеми сделками» против «если бы бот был выключен на новостях». Сравниваются только сделки внутри периода, покрытого календарём:
    вне покрытия новости неизвестны, и включать их в «без новостей» означало бы подгонять сравнение."""
    ev = events[events["impact"].isin(impacts)].reset_index(drop=True)
    info: dict[str, Any] = {"params": {"before_min": before_min, "after_min": after_min, "mode": mode, "impacts": list(impacts)}, "events": int(len(ev))}
    if len(ev) == 0 or len(df) == 0:
        return {**info, "available": False, "reason": "Нет событий выбранной важности в календаре" if len(df) else "Нет сделок"}
    lo, hi = ev["ts"].min(), ev["ts"].max()
    covered = (df["entry_ts"] >= lo) & (df["exit_time"] <= hi)
    scope = df[covered]
    info.update(coverage={"from": lo, "to": hi, "trades_inside": int(covered.sum()), "trades_outside": int((~covered).sum())})
    if len(scope) == 0:
        return {**info, "available": False, "reason": "Ни одна сделка не лежит внутри периода календаря: загрузите календарь за период сделок"}
    flags = flag_news(scope, ev, before_min=before_min, after_min=after_min, mode=mode)
    news_mask = flags["in_news"].to_numpy()
    kept, dropped = scope[~news_mask], scope[news_mask]
    m_all = compute_metrics(scope, initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n)
    m_kept = compute_metrics(kept, initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n)
    m_news = compute_metrics(dropped, initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n)

    def delta(key: str) -> float | None:
        a, b = m_all.get(key), m_kept.get(key)
        return None if a is None or b is None else b - a

    kinds = flags.loc[news_mask, "event"].map(lambda t: classify_event(str(t or "")))
    by_kind = [{"event_kind": k, **{kk: v for kk, v in compute_metrics(dropped[(kinds == k).to_numpy()], initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions,
                                                                        bootstrap_n=cfg.bootstrap_n).items() if kk in ("n", "winrate", "profit_factor", "expectancy_r", "net_r")}}
               for k in sorted(kinds.unique())]
    return clean({**info, "available": True, "all": m_all, "without_news": m_kept, "news_only": m_news, "n_excluded": int(news_mask.sum()),
                  "delta": {"expectancy_r": delta("expectancy_r"), "profit_factor": delta("profit_factor"), "net_r": delta("net_r"), "max_drawdown_r": delta("max_drawdown_r"),
                            "winrate": delta("winrate")},
                  "by_event_kind": by_kind, "curves": {"all": equity_curve(scope), "without_news": equity_curve(kept)}})


def news_window_grid(df: pd.DataFrame, events: pd.DataFrame, cfg: Any, *, impacts: tuple[str, ...] = ("high",), mode: str = "touch",
                     grid: tuple[int, ...] = WINDOW_GRID) -> list[dict[str, Any]]:
    """Эффект окон ±N минут (симметричных): сколько сделок исключается и как меняются матожидание/PF. Для выбора окна и рекомендаций."""
    rows = []
    for n in grid:
        c = compare_news(df, events, cfg, before_min=n, after_min=n, mode=mode, impacts=impacts)
        if not c.get("available"):
            return []
        rows.append({"window_min": n, "n_excluded": c["n_excluded"], "expectancy_all": c["all"]["expectancy_r"], "expectancy_without": c["without_news"]["expectancy_r"],
                     "pf_all": c["all"]["profit_factor"], "pf_without": c["without_news"]["profit_factor"], "news_only_expectancy": c["news_only"]["expectancy_r"],
                     "news_only_n": c["news_only"]["n"]})
    return rows
