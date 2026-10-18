"""Сборка отчёта «Deep Analysis»: чистая функция над DataFrame сделок (без БД и сети) - тестируется на синтетике. Тяжёлую часть запускает воркер."""

import time
from collections.abc import Callable
from typing import Any

import pandas as pd

from analyzer.config import Settings
from analyzer.context import sessions_report, success_profile, vsa_bins
from analyzer.metrics import clean, compute_metrics, equity_curve
from analyzer.news import compare_news, news_window_grid
from analyzer.recommend import generate
from analyzer.slices import feature_slices, group_metrics, stop_tightening, target_reach
from analyzer.versions import VersionDef, auto_versions, compare_versions

Progress = Callable[[str, float], None]
AUTO_COMPARE_MAX = 3  # автосравнений соседних автоверсий на стратегию


def _resolve(name: str, user: dict[str, VersionDef], auto: list[dict[str, Any]]) -> VersionDef | None:
    if name in user:
        return user[name]
    for a in auto:
        if a["name"] == name:
            return VersionDef(name=name, strategy_id=a["strategy_id"], plan_hash=a["plan_hash"])
    return None


def run_analysis(df: pd.DataFrame, params: dict[str, Any], cfg: Settings, manifest: dict[str, Any], *, events: pd.DataFrame | None, user_versions: list[VersionDef],
                 progress: Progress | None = None) -> dict[str, Any]:
    """params - тело RunRequest (dict). events - календарь новостей (None / пустой - модуль новостей помечается недоступным)."""
    step = progress or (lambda stage, p: None)
    warnings: list[str] = []
    news_p = params.get("news") or {}
    news_on = bool(news_p.get("enabled", True))
    impacts = tuple(news_p.get("impacts") or ["high"])
    nkw = dict(before_min=news_p.get("before_min", 5), after_min=news_p.get("after_min", 5), mode=news_p.get("mode", "touch"), impacts=impacts)
    kw = dict(initial_equity=cfg.initial_equity, min_conclusions=cfg.min_conclusions, bootstrap_n=cfg.bootstrap_n)

    step("metrics", 0.30)
    report: dict[str, Any] = {"generated_at": pd.Timestamp.now(tz="UTC"), "params": params,
                              "data": {"trades": len(df), "from": df["exit_time"].min() if len(df) else None, "to": df["exit_time"].max() if len(df) else None,
                                       "strategies": sorted(df["strategy_id"].unique()) if len(df) else [], "coins": sorted(df["coin"].unique()) if len(df) else []}}
    if len(df) == 0:
        warnings.append("Нет закрытых сделок по выбранному фильтру: считать нечего.")
        return clean({**report, "overall": compute_metrics(df, **kw), "by_strategy": [], "by_coin": [], "by_strategy_coin": [], "strategies": {}, "news": {"available": False},
                      "versions": {"auto": [], "comparisons": []}, "recommendations": [], "warnings": warnings})
    report["overall"] = compute_metrics(df, **kw)
    report["overall_curve"] = equity_curve(df)
    report["by_strategy"] = group_metrics(df, ["strategy_id"], cfg)
    report["by_coin"] = group_metrics(df, ["coin"], cfg, with_ci=False)
    report["by_strategy_coin"] = group_metrics(df, ["strategy_id", "coin"], cfg, with_ci=False)

    have_events = news_on and events is not None and len(events) > 0
    if news_on and not have_events:
        warnings.append("Календарь новостей не загружен: сравнение «с новостями / без» недоступно (загрузите CSV или JSON ForexFactory).")

    strategies: dict[str, Any] = {}
    sids = sorted(df["strategy_id"].unique())
    for i, sid in enumerate(sids):
        step(f"strategy:{sid}", 0.35 + 0.4 * i / max(len(sids), 1))
        g = df[df["strategy_id"] == sid]
        entry: dict[str, Any] = {"metrics": compute_metrics(g, **kw), "curve": equity_curve(g), "sessions": sessions_report(g, cfg),
                                 "vsa": {"vol_ratio_bins": vsa_bins(g, cfg), "profile": success_profile(g, cfg)},
                                 "what_if": {"stop": stop_tightening(g, cfg), "target": target_reach(g, cfg)}, "feature_slices": feature_slices(g, cfg)}
        if have_events:
            c = compare_news(g, events, cfg, **nkw)
            c.pop("curves", None)
            entry["news"] = c
        strategies[sid] = entry
    report["strategies"] = strategies

    step("news", 0.78)
    if have_events:
        main = compare_news(df, events, cfg, **nkw)
        main["window_grid"] = news_window_grid(df, events, cfg, impacts=impacts, mode=nkw["mode"])
        report["news"] = main
    else:
        report["news"] = {"available": False, "reason": "calendar_not_loaded" if news_on else "disabled"}

    step("versions", 0.85)
    auto = auto_versions(df)
    user = {v.name: v for v in user_versions}
    specs: list[tuple[str, str, str]] = [(c["baseline"], c["candidate"], c.get("align", "overlap")) for c in params.get("compare") or []]
    if not specs:  # по умолчанию: соседние во времени автоверсии одной стратегии (A -> B), чтобы деградация подсвечивалась без настройки
        for sid in sids:
            seq = sorted((a for a in auto if a["strategy_id"] == sid), key=lambda a: a["first_entry"])
            specs += [(a["name"], b["name"], "relative") for a, b in zip(seq, seq[1:])][-AUTO_COMPARE_MAX:]
    comparisons = []
    for a_name, b_name, align in specs:
        a, b = _resolve(a_name, user, auto), _resolve(b_name, user, auto)
        if a is None or b is None:
            warnings.append(f"Сравнение {a_name} → {b_name} пропущено: версия не найдена среди пользовательских и автоверсий этой выборки.")
            continue
        if a.strategy_id != b.strategy_id:
            warnings.append(f"Сравнение {a_name} → {b_name} пропущено: версии разных стратегий.")
            continue
        comparisons.append(compare_versions(df, a, b, cfg, align=align))
    report["versions"] = {"auto": auto, "user": [v.to_json() for v in user.values()], "comparisons": comparisons,
                          "degraded": [c["candidate"]["version"]["name"] for c in comparisons if c["verdict"]["verdict"] in ("degraded", "suspected")]}

    step("recommendations", 0.92)
    report["recommendations"] = generate(df, cfg, manifest, events=events if have_events else None, impacts=impacts)
    if len(df) < cfg.min_conclusions:
        warnings.append(f"Закрыто {len(df)} сделок (нужно порядка {cfg.min_conclusions}+): значимость выводов низкая, рекомендации — гипотезы.")
    report["warnings"] = warnings
    report["elapsed_s"] = None
    return clean(report)


def timed(fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    t0 = time.monotonic()
    out = fn()
    out["elapsed_s"] = round(time.monotonic() - t0, 2)
    return out
