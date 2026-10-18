"""Рекомендации системы и dry-run diff.

Рекомендация строится ТОЛЬКО из честных срезов (analyzer/slices.py) и обязана пройти фильтры против подгонки:
  * убираемая часть выборки (монета / сессия / корзина признака / окно новостей) имеет n >= min_group_trades И верхняя граница 95% bootstrap-интервала её
    матожидания < 0 (значимо убыточная часть, а не просто «хуже среднего»);
  * знак эффекта устойчив: убираемая часть убыточна в ОБЕИХ половинах периода (по времени);
  * подтяжка стопа (по MAE): улучшение >= MIN_GAIN_R и тоже в обеих половинах.
Уверенность: high - после применения >= min_conclusions сделок; medium - >= 2*min_group_trades; иначе low. Это гипотезы на проверку новыми данными, не приказы.

Dry-run: изменения накладываются на `current_params.json` (ручной снимок параметров бота) и показываются unified-diff'ом. Анализатор НИЧЕГО не пишет в конфигурацию бота:
применение - ручная правка кода и push в main (решение пользователя).
"""

import copy
import difflib
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from analyzer.errors import DataError
from analyzer.metrics import bootstrap_mean_ci, clean
from analyzer.news import flag_news, news_window_grid
from analyzer.context import session_of
from analyzer.slices import brief, feature_columns, stop_tightening

MIN_GAIN_R = 0.03  # подтяжка стопа / фильтр должны поднять матожидание хотя бы на столько R
MAX_PER_STRATEGY = 6
MAX_TOTAL = 30


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DataError(f"Манифест текущих параметров {path} нечитаем: {exc}") from exc
    if "strategies" not in data or "strategy_defaults" not in data:
        raise DataError("В манифесте нет разделов strategies / strategy_defaults")
    return data


def strategy_params(manifest: dict[str, Any], strategy_id: str) -> dict[str, Any]:
    base = copy.deepcopy(manifest["strategy_defaults"])
    for k, v in manifest["strategies"].get(strategy_id, {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k].update(copy.deepcopy(v))
        else:
            base[k] = copy.deepcopy(v)
    return base


def _get(tree: dict[str, Any], path: str) -> Any:
    cur: Any = tree
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _set(tree: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cur = tree
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value


def _halves(g: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    g = g.sort_values("exit_time", kind="stable")
    h = len(g) // 2
    return g.iloc[:h], g.iloc[h:]


def _confidence(n_after: int, cfg: Any) -> str:
    return "high" if n_after >= cfg.min_conclusions else "medium" if n_after >= 2 * cfg.min_group_trades else "low"


def _effect(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    b, a = before["expectancy_r"], after["expectancy_r"]
    return {"metric": "expectancy_r", "before": b, "after": a, "delta": None if a is None or b is None else a - b,
            "delta_pct": (a - b) / b * 100.0 if a is not None and b is not None and b > 0 else None,
            "pf_before": before["profit_factor"], "pf_after": after["profit_factor"], "n_before": before["n"], "n_after": after["n"],
            "net_r_before": before["net_r"], "net_r_after": after["net_r"]}


def _rec(strategy_id: str | None, kind: str, param: str, proposed: Any, scope: dict[str, Any], title: str, why: str, effect: dict[str, Any], conf: str, assumptions: str,
         manifest: dict[str, Any]) -> dict[str, Any]:
    tree = manifest["global"] if strategy_id is None else strategy_params(manifest, strategy_id)
    key = hashlib.sha1(json.dumps([strategy_id, kind, param, proposed, scope], sort_keys=True, default=str).encode()).hexdigest()[:10]
    return clean({"id": key, "strategy_id": strategy_id, "kind": kind, "param": param, "current": _get(tree, param), "proposed": proposed, "scope": scope, "title": title,
                  "rationale": why, "effect": effect, "confidence": conf, "assumptions": assumptions})


def _removal(g: pd.DataFrame, remove: np.ndarray, cfg: Any) -> tuple[dict, dict] | None:
    """(до, после) при удалении части; None - если удалять нечего/нечего оставлять."""
    kept = g[~remove]
    if remove.sum() < cfg.min_group_trades or len(kept) < cfg.min_group_trades:
        return None
    return brief(g, cfg), brief(kept, cfg)


def _significantly_negative(part: pd.DataFrame, cfg: Any) -> bool:
    """Часть значимо убыточна (верхняя граница 95% интервала < 0) и убыточна в обеих половинах периода."""
    if len(part) < cfg.min_group_trades:
        return False
    ci = bootstrap_mean_ci(part["r"].to_numpy(), cfg.bootstrap_n)
    if ci is None or ci[1] >= 0:
        return False
    h1, h2 = _halves(part)
    return len(h1) >= 5 and len(h2) >= 5 and h1["r"].mean() < 0 and h2["r"].mean() < 0


def _stop_rec(sid: str, g: pd.DataFrame, cfg: Any, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    table = stop_tightening(g, cfg)
    rows = [r for r in table["rows"] if r["delta_expectancy_r"] is not None and r["delta_expectancy_r"] >= MIN_GAIN_R and r["n_changed"] >= cfg.min_group_trades // 2]
    if not rows:
        return []
    best = max(rows, key=lambda r: r["delta_expectancy_r"])
    h1, h2 = _halves(g)
    for half in (h1, h2):
        row = next((r for r in stop_tightening(half, cfg, (best["stop_r"],))["rows"]), None)
        if row is None or row["delta_expectancy_r"] is None or row["delta_expectancy_r"] <= 0:
            return []
    before = table["baseline"]
    after = {"n": before["n"], "expectancy_r": best["expectancy_r"], "profit_factor": best["profit_factor"], "net_r": best["net_r"]}
    return [_rec(sid, "tighten_stop", "risk.stop_fraction_of_r", best["stop_r"], {},
                 f"{sid}: стоп {best['stop_r']:g}R вместо 1R",
                 f"{best['n_changed']} сделок достигли просадки >= {best['stop_r']:g}R до безубытка; сокращение их убытка поднимает матожидание "
                 f"на {best['delta_expectancy_r']:+.3f}R, но отсекает {best['winners_cut']} сделок, которые потом закрылись в плюс.",
                 _effect(before, after), _confidence(before["n"], cfg), table["assumptions"], manifest)]


def _group_recs(sid: str, g: pd.DataFrame, cfg: Any, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    # монеты
    for coin, part in g.groupby("coin"):
        if not _significantly_negative(part, cfg):
            continue
        res = _removal(g, (g["coin"] == coin).to_numpy(), cfg)
        if res and res[1]["expectancy_r"] is not None and res[0]["expectancy_r"] is not None and res[1]["expectancy_r"] - res[0]["expectancy_r"] >= MIN_GAIN_R:
            cur = list(_get(strategy_params(manifest, sid), "symbols.exclude") or [])
            out.append(_rec(sid, "exclude_coin", "symbols.exclude", sorted({*cur, coin}), {"coin": coin}, f"{sid}: отключить {coin}",
                            f"{coin}: {len(part)} сделок, матожидание {part['r'].mean():+.3f}R с верхней границей интервала < 0 и убытком в обеих половинах периода.",
                            _effect(*res), _confidence(res[1]["n"], cfg), "Убираемая монета не влияет на остальные сделки (блокировка монеты у стратегий раздельная).", manifest))
    # сессии
    sess = session_of(g["entry_ts"])
    for name in sorted(sess.unique()):
        part = g[(sess == name).to_numpy()]
        if not _significantly_negative(part, cfg):
            continue
        res = _removal(g, (sess == name).to_numpy(), cfg)
        if res and res[1]["expectancy_r"] - res[0]["expectancy_r"] >= MIN_GAIN_R:
            cur = list(_get(strategy_params(manifest, sid), "sessions.exclude") or [])
            out.append(_rec(sid, "exclude_session", "sessions.exclude", sorted({*cur, name}), {"session": name}, f"{sid}: не торговать сессию {name}",
                            f"Сессия {name} (UTC): {len(part)} сделок, матожидание {part['r'].mean():+.3f}R, интервал < 0, убыток в обеих половинах.",
                            _effect(*res), _confidence(res[1]["n"], cfg), "Сессия определяется по времени входа (UTC).", manifest))
    # признаки
    for name, s in feature_columns(g).items():
        valid = s.notna().to_numpy()
        if valid.sum() < 2 * cfg.min_group_trades or name in ("hour_utc",):
            continue
        if pd.api.types.is_numeric_dtype(s):
            x = s.to_numpy(dtype=float)
            for q in (0.2, 0.33, 0.5):
                lo_cut = float(np.nanquantile(x, q))
                for side, remove in (("min", valid & (x < lo_cut)), ("max", valid & (x > float(np.nanquantile(x, 1 - q))))):
                    part = g[remove]
                    if not _significantly_negative(part, cfg):
                        continue
                    res = _removal(g, remove, cfg)
                    if res and res[1]["expectancy_r"] - res[0]["expectancy_r"] >= MIN_GAIN_R:
                        thr = lo_cut if side == "min" else float(np.nanquantile(x, 1 - q))
                        out.append(_rec(sid, "feature_filter", f"filters.{name}.{side}", round(thr, 4), {"feature": name, "side": side}, f"{sid}: фильтр {name} {'>=' if side == 'min' else '<='} {thr:.3g}",
                                        f"Сделки с {name} {'<' if side == 'min' else '>'} {thr:.3g}: n={len(part)}, матожидание {part['r'].mean():+.3f}R, интервал < 0, убыток в обеих половинах.",
                                        _effect(*res), _confidence(res[1]["n"], cfg), "Порог подобран по этой же выборке: проверьте на новых данных (риск подгонки).", manifest))
        else:
            for val, part in g[valid].groupby(s[valid].astype(str)):
                if not _significantly_negative(part, cfg):
                    continue
                res = _removal(g, valid & (s.astype(str) == val).to_numpy(), cfg)
                if res and res[1]["expectancy_r"] - res[0]["expectancy_r"] >= MIN_GAIN_R:
                    cur = list(_get(strategy_params(manifest, sid), f"filters.{name}.exclude_values") or [])
                    out.append(_rec(sid, "feature_filter", f"filters.{name}.exclude_values", sorted({*cur, val}), {"feature": name, "value": val}, f"{sid}: исключить {name} = {val}",
                                    f"{name} = {val}: n={len(part)}, матожидание {part['r'].mean():+.3f}R, интервал < 0, убыток в обеих половинах.",
                                    _effect(*res), _confidence(res[1]["n"], cfg), "Значение подобрано по этой же выборке: проверьте на новых данных.", manifest))
    out.sort(key=lambda r: -(r["effect"]["delta"] or 0.0))
    dedup: dict[tuple, dict] = {}
    for r in out:  # из нескольких порогов одного признака оставляем лучший
        k = (r["kind"], r["param"] if r["kind"] != "feature_filter" else r["param"].rsplit(".", 1)[0] + str(r["scope"].get("side", "")))
        dedup.setdefault(k, r)
    return list(dedup.values())[:MAX_PER_STRATEGY]


def _news_rec(df: pd.DataFrame, events: pd.DataFrame | None, cfg: Any, manifest: dict[str, Any], impacts: tuple[str, ...]) -> list[dict[str, Any]]:
    if events is None or len(events) == 0 or len(df) < 2 * cfg.min_group_trades:
        return []
    grid = news_window_grid(df, events, cfg, impacts=impacts)
    cands = [r for r in grid if r["news_only_n"] >= cfg.min_group_trades and r["expectancy_without"] is not None and r["expectancy_all"] is not None
             and r["expectancy_without"] - r["expectancy_all"] >= MIN_GAIN_R]
    if not cands:
        return []
    best = max(cands, key=lambda r: r["expectancy_without"] - r["expectancy_all"])
    flags = flag_news(df[(df["entry_ts"] >= events["ts"].min()) & (df["exit_time"] <= events["ts"].max())], events[events["impact"].isin(impacts)],
                      before_min=best["window_min"], after_min=best["window_min"])
    scope = df.loc[flags.index]
    if not _significantly_negative(scope[flags["in_news"].to_numpy()], cfg):
        return []
    before, after = brief(scope, cfg), brief(scope[~flags["in_news"].to_numpy()], cfg)
    return [_rec(None, "news_blackout", "news.blackout_minutes", best["window_min"], {"impacts": list(impacts)}, f"Блокировать новые сигналы ±{best['window_min']} мин вокруг новостей",
                 f"{best['news_only_n']} сделок у новостей High Impact дают {best['news_only_expectancy']:+.3f}R; без них матожидание {best['expectancy_without']:+.3f}R против {best['expectancy_all']:+.3f}R.",
                 _effect(before, after), _confidence(after["n"], cfg), "Календарь покрывает только часть периода; сравниваются сделки внутри покрытия.", manifest)]


def generate(df: pd.DataFrame, cfg: Any, manifest: dict[str, Any], *, events: pd.DataFrame | None = None, impacts: tuple[str, ...] = ("high",)) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    for sid, g in df.groupby("strategy_id", sort=True):
        if len(g) < 2 * cfg.min_group_trades:
            continue
        found = _stop_rec(sid, g, cfg, manifest) + _group_recs(sid, g, cfg, manifest)
        found.sort(key=lambda r: -(r["effect"]["delta"] or 0.0))
        recs += found[:MAX_PER_STRATEGY]
    recs += _news_rec(df, events, cfg, manifest, impacts)
    recs.sort(key=lambda r: -(r["effect"]["delta"] or 0.0))
    return recs[:MAX_TOTAL]


def _json_lines(tree: Any) -> list[str]:
    return json.dumps(tree, ensure_ascii=False, indent=2, sort_keys=True).splitlines(keepends=False)


def dry_run(recs: list[dict[str, Any]], manifest: dict[str, Any]) -> dict[str, Any]:
    """Накладывает выбранные рекомендации на манифест и возвращает unified-diff по каждой стратегии (и по глобальным параметрам). Никуда ничего не записывает."""
    by_scope: dict[str | None, list[dict[str, Any]]] = {}
    for r in recs:
        by_scope.setdefault(r["strategy_id"], []).append(r)
    files = []
    for sid, items in sorted(by_scope.items(), key=lambda kv: (kv[0] is not None, kv[0] or "")):
        before = copy.deepcopy(manifest["global"] if sid is None else strategy_params(manifest, sid))
        after = copy.deepcopy(before)
        for r in items:
            existing = _get(after, r["param"])
            _set(after, r["param"], sorted({*existing, *r["proposed"]}) if isinstance(existing, list) and isinstance(r["proposed"], list) and not isinstance(existing[:1], list) else r["proposed"])
        name = "global" if sid is None else sid
        diff = "\n".join(difflib.unified_diff(_json_lines(before), _json_lines(after), fromfile=f"current_params/{name} (сейчас)", tofile=f"current_params/{name} (предложение)", lineterm="", n=2))
        files.append({"scope": name, "changes": [{"id": r["id"], "param": r["param"], "current": r["current"], "proposed": r["proposed"], "title": r["title"]} for r in items],
                      "diff": diff})
    return clean({"files": files, "diff": "\n\n".join(f["diff"] for f in files if f["diff"]),
                  "note": "Dry run: ничего не записано. Перенесите значения в код бота вручную (параметры стратегий) и сделайте push в main; обновите analyzer/current_params.json."})
