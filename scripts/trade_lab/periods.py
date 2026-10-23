"""Разбивка по дням и версиям: что изменилось - вход (качество сигналов) или выход (план).

«Качество входа» меряется тем, что не зависит от плана выхода: доля сделок, прошедших +0.5R / +1R до выхода, и медиана MFE.
Оговорка: у сделок до 2026-10-19 (безубыток / трейлинг) MFE после безубытка обрезан, поэтому сравнимы пороги до +1R включительно.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from statistics import median

from .data import Trade
from .exits import CURRENT, OLD_PLAN, Plan, run, stats_of, tp

# Даты изменений правил (CLAUDE.md, разд. 3.16-3.24): с этого дня (UTC) действует новая версия
DEFAULT_BREAKS: tuple[tuple[date, str], ...] = (
    (date(2026, 10, 16), "OBI ужесточён, пул монет"),
    (date(2026, 10, 18), "фильтры входа (импульс, 1m, новости), автоселектор"),
    (date(2026, 10, 19), "ВЫХОД 1:4, риск $5, без BE / Scale Out, жёсткий H4"),
    (date(2026, 10, 20), "ICT / VWAP фильтры качества"),
    (date(2026, 10, 21), "CHR v1.3"),
    (date(2026, 10, 23), "1.4.1: CHR поджатие/стоп, VWAP Tolerance Zone, восстановление Shadow"),
)
REF_PLANS: tuple[Plan, ...] = (CURRENT, OLD_PLAN, tp(2.0))


def day_of(t: Trade) -> date:
    return datetime.fromtimestamp(t.signal_ms / 1000, tz=timezone.utc).date()


def version_of(t: Trade, breaks: tuple[tuple[date, str], ...] = DEFAULT_BREAKS) -> str:
    d, label = day_of(t), "до 16.10"
    for start, _ in breaks:
        if d >= start:
            label = f"с {start:%d.%m}"
    return label


def entry_quality(trades: list[Trade]) -> dict[str, float | int | None]:
    n = len(trades)
    if not n:
        return {"n": 0}
    mfes = [max(t.mfe or 0.0, 0.0) for t in trades]
    return {"n": n, "reach05": sum(m >= 0.5 for m in mfes) / n, "reach1": sum(m >= 1.0 for m in mfes) / n, "mfe_med": median(mfes),
            "mae_med": median([t.mae for t in trades if t.mae is not None]) if any(t.mae is not None for t in trades) else None}


def table(trades: list[Trade], key) -> list[tuple[str, dict, dict[str, tuple[float | None, int]]]]:
    """Строки: (группа, качество входа, {план: (средний R, неизвестно)})."""
    groups: dict[str, list[Trade]] = {}
    for t in trades:
        groups.setdefault(key(t), []).append(t)
    out = []
    for label in sorted(groups):
        items = groups[label]
        plans = {}
        for p in REF_PLANS:
            st = stats_of(run(items, p))
            plans[p.name] = (st.mean_r, st.unknown)
        out.append((label, entry_quality(items), plans))
    return out
