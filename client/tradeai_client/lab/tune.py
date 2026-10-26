"""Подбор параметров детекторов под уроки трейдера: перебор сетки по каждому понятию, лучший - по сумме совпадений во ВСЕХ уроках с этим понятием.

Почему так, а не «переписать стратегию под новый урок»: каждый урок - проверочный тест. Новый урок меняет параметры, только если в сумме по всем урокам
согласие растёт; урок, который раньше совпадал, не может «молча» сломаться - отчёт показывает F1 до и после по каждому понятию и уроку.
Понятия без детектора (свои понятия трейдера) не подбираются: под них нужен новый код."""

import itertools
from typing import Any, Callable

from tradeai_client.lab import detect, match

GRID: dict[str, dict[str, list[float]]] = {
    "high": {"k": [2, 3, 4, 5, 6]},
    "low": {"k": [2, 3, 4, 5, 6]},
    "trend": {"k": [3, 4, 5, 6, 8]},
    "level": {"k": [2, 3, 5], "tol_atr": [0.2, 0.3, 0.5], "min_touches": [2, 3, 4]},
    "zone": {"k": [2, 3, 5], "tol_atr": [0.4, 0.6, 0.9], "min_touches": [2, 3], "react_atr": [1.0, 1.5, 2.5]},
    "liquidity": {"k": [2, 3, 4], "eq_atr": [0.08, 0.15, 0.25]},
    "gap": {"min_atr": [0.15, 0.3, 0.5, 0.8]},
    "range": {"min_bars": [10, 16, 24], "max_height_atr": [3, 4, 6], "max_er": [0.25, 0.35, 0.5]},
    "consolidation": {"min_bars": [6, 8, 12], "max_height_atr": [1.5, 2, 3], "decay": [0.8, 0.9, 1.0]},
    "trendline": {"k": [3, 4, 5], "min_span": [5, 8, 12], "tol_atr": [0.15, 0.25, 0.4], "min_touches": [2, 3]},
    "breakout": {"min_break_atr": [0.05, 0.1, 0.2], "min_body_atr": [0.2, 0.4, 0.6]},
    "zone_exit": {"hold_bars": [2, 3, 5]},
    "reversal": {"wick_frac": [0.45, 0.55, 0.65], "near_atr": [0.25, 0.4, 0.6]},
    "volume": {"spike": [1.8, 2.5, 3.5], "window": [4, 6, 10], "buy_max": [0.35, 0.42, 0.48]},
}


def _score(lessons_ctx: list[tuple[dict, detect.Ctx]], term: str, ps: dict) -> tuple[int, int, int]:
    tp = fn = fp = 0
    fn_ = detect.DETECTORS[term]
    for lesson, c in lessons_ctx:
        objs = fn_(c, ps)
        r = match.compare({**lesson, "complete_types": [term]}, objs)["by_type"].get(term)
        if r:
            tp, fn, fp = tp + r["tp"], fn + r["fn"], fp + r["fp"]
    return tp, fn, fp


def _f1(t: tuple[int, int, int]) -> float:
    tp, fn, fp = t
    return 2 * tp / (2 * tp + fn + fp) if tp else 0.0


def tune(lessons: list[dict], overrides: dict[str, dict] | None = None, terms: list[str] | None = None,
         progress: Callable[[str, float], None] | None = None) -> dict[str, Any]:
    """-> {"params": новые переопределения, "report": {term: {before, after, changed, lessons}}}. Уроки без свечей пропускаются."""
    base = detect.params_for(overrides)
    usable = [ls for ls in lessons if ls.get("bars") and len(ls["bars"]) >= 30]
    marked = {d["term"] for ls in usable for d in ls.get("drawings", []) if d.get("term")}
    todo = [t for t in (terms or list(GRID)) if t in marked and t in GRID]
    ctxs = []
    for ls in usable:  # контекст урока считается один раз: зависимости (коридоры, зоны, наклонки) - с текущими параметрами
        _, c = detect.run(ls["bars"], int(ls["iv"]), None, base)
        ctxs.append((ls, c))
    new = {k: dict(v) for k, v in (overrides or {}).items()}
    report: dict[str, Any] = {}
    for n, term in enumerate(todo):
        grid = GRID[term]
        relevant = [(ls, c) for ls, c in ctxs if any(d.get("term") == term for d in ls.get("drawings", []))]
        before = _score(relevant, term, base[term])
        best, best_ps = before, dict(base[term])
        for combo in itertools.product(*grid.values()):
            ps = {**base[term], **dict(zip(grid.keys(), combo))}
            s = _score(relevant, term, ps)
            if _f1(s) > _f1(best) + 1e-9:
                best, best_ps = s, ps
        changed = best_ps != base[term]
        if changed:
            new[term] = {k: v for k, v in best_ps.items() if v != detect.DEFAULT_PARAMS[term].get(k)}
        report[term] = {"before": {"tp": before[0], "fn": before[1], "fp": before[2], "f1": round(_f1(before), 3)},
                        "after": {"tp": best[0], "fn": best[1], "fp": best[2], "f1": round(_f1(best), 3)},
                        "changed": changed, "params": best_ps, "lessons": len(relevant)}
        if progress:
            progress(term, (n + 1) / len(todo))
    return {"params": new, "report": report, "skipped_terms": sorted(marked - set(todo))}
