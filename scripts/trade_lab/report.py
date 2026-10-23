"""Отчёт в Markdown (он же печатается в консоль) и CSV по сделкам."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

from . import correlate, exits, periods, stops
from .data import CATEGORIES, IN_ANALYSIS, Ledger, Trade

ENOUGH = 200  # порог «достаточно данных» проекта (CLAUDE.md, разд. 3.5)
TOP_FINDINGS = 8


def f(v: Any, digits: int = 2, pct: bool = False) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "да" if v else "нет"
    if pct:
        return f"{100 * v:.0f}%"
    if isinstance(v, float):
        return f"{v:+.{digits}f}" if digits and abs(v) < 1e6 else f"{v:.{digits}f}"
    return str(v)


def table(head: list[str], rows: Iterable[list[Any]]) -> str:
    lines = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def small(n: int) -> str:
    return f" ⚠️ мало данных ({n} < {ENOUGH}): это ориентир, не вывод" if n < ENOUGH else ""


def stats_row(label: str, st: exits.Stats) -> list[Any]:
    return [label, st.known, st.unknown or "", f(st.hit_rate, pct=True), f(st.win_rate, pct=True), f(st.sum_r), f(st.mean_r, 3),
            f"{st.usd:+.0f}$", f"{st.pf:.2f}" if st.pf is not None else "—", f"{st.mdd_r:.1f}"]


STATS_HEAD = ["План", "n", "неизв.", "тейк взят", "винрейт", "Σ R", "E[R]/сделку", "USD", "PF", "просадка R"]


# ------------------------------------------------------------------------------------------------ блоки

def ledger_section(ledger: Ledger, title: str) -> str:
    rows = []
    for code, label, used in CATEGORIES:
        n = ledger.counts.get(code, 0)
        ids = ledger.excluded_ids.get(code, [])
        rows.append([label, n, "да" if used else "нет", ", ".join(ids[:5]) + (" …" if len(ids) > 5 else "")])
    rows.append(["**ВСЕГО строк**", ledger.total, "", f"сумма категорий = {sum(ledger.counts.values())} (сверка сошлась)"])
    return f"### {title}\n\n" + table(["Категория", "n", "в анализе", "примеры id"], rows)


def flags_section(trades: list[Trade]) -> str:
    restored = sum(t.restored for t in trades)
    gaps = sum(t.tick_gap for t in trades)
    gap_other = sum(1 for t in trades if t.gap and not t.restored and not t.tick_gap)
    no_mae = sum(1 for t in trades if t.mae is None)
    no_snap = sum(1 for t in trades if not t.candles)
    return (f"В анализе {len(trades)} сделок: восстановлено после рестарта — {restored} (MFE за простой не наблюдался, это нижняя граница), "
            f"обрыв потока тиков — {gaps}, прочие gap_affected — {gap_other}, без MAE — {no_mae}, без свечей снапшота — {no_snap}.")


def mfe_section(trades: list[Trade]) -> str:
    out = ["### MFE: как далеко цена шла в нашу сторону до выхода"]
    losers = [t for t in trades if (t.actual_r or 0) < 0]
    rows = []
    for label, items in [("все", trades), ("убыточные (выбило)", losers)] + [(f"убыточные · {k}", v) for k, v in
                                                                               exits.by(losers, lambda t: t.strategy).items()]:
        p = exits.mfe_profile(items)
        if not p["n"]:
            continue
        reach = p["reach"]
        rows.append([label, p["n"], f"{p['p25']:.2f}", f"{p['p50']:.2f}", f"{p['p75']:.2f}", f"{p['p90']:.2f}"]
                    + [f(reach[x], pct=True) for x in (0.5, 1.0, 1.5, 2.0, 3.0)])
    out.append(table(["Группа", "n", "MFE p25", "медиана", "p75", "p90", "≥0.5R", "≥1R", "≥1.5R", "≥2R", "≥3R"], rows))
    out.append("Читать так: «≥1R = 40%» у убыточных — 40% сделок, закрытых в минус, сначала прошли +1R в нашу сторону и только потом развернулись в стоп.")
    return "\n\n".join(out)


def tp_section(trades: list[Trade], title: str) -> str:
    out = [f"### {title}{small(len(trades))}"]
    rows = [stats_row("**Факт** (r_multiple)", exits.actual_stats(sorted(trades, key=lambda t: t.signal_ms)))]
    for x in exits.TP_GRID:
        rows.append(stats_row(f"TP 1:{x:g}" + (" (сим., сверка с фактом)" if x == 4.0 else ""), exits.stats_of(exits.run(trades, exits.tp(x)))))
    out.append(table(STATS_HEAD, rows))
    new = [t for t in trades if not t.old_plan and t.plan_levels == [(4.0, 1.0)]]
    if new:
        sim = exits.stats_of(exits.run(new, exits.CURRENT))
        fact = exits.actual_stats(new)
        diff = None if sim.mean_r is None or fact.mean_r is None else sim.mean_r - fact.mean_r
        verdict = "сходится" if diff is not None and abs(diff) <= 0.05 else "**РАСХОЖДЕНИЕ > 0.05R: проверить данные/комиссии**"
        out.append(f"Сверка модели: сделки по плану 1:4 (n={len(new)}): сим. E[R] {f(sim.mean_r, 3)} vs факт {f(fact.mean_r, 3)} → {verdict}.")
    best = exits.best_tp(trades)
    if best:
        out.append(f"**Лучший фиксированный тейк на этой выборке: 1:{best[0]:g}** — E[R] {f(best[1].mean_r, 3)}, винрейт {f(best[1].win_rate, pct=True)}, "
                   f"Σ {best[1].usd:+.0f}$ (сетка 0.5…4R шагом 0.25).")
        boot = exits.bootstrap_diff(trades, exits.tp(best[0]), exits.CURRENT)
        if boot:
            sure = "интервал выше нуля — разница устойчива" if boot[1] > 0 else "интервал задевает ноль — разница может быть случайной"
            out.append(f"Разница с 1:4: {f(boot[0], 3)} R/сделку, 90% бутстреп-интервал [{f(boot[1], 3)}; {f(boot[2], 3)}] — {sure}.")
    wf = exits.walk_forward(trades)
    if wf:
        out.append(f"Проверка на «будущем»: тейк выбран по первой половине (n={wf['n_train']}) → 1:{wf['x']:g}; на второй половине (n={wf['n_test']}) он дал "
                   f"E[R] {f(wf['test_mean'], 3)} против {f(wf['test_current'], 3)} у 1:4.")
    return "\n\n".join(out)


def plans_section(trades: list[Trade]) -> str:
    out = ["## Блок 2. Планы выхода: «раньше было лучше?»",
           "Те же сделки, разные правила выхода. «неизв.» — сделки, у которых путь после фактического закрытия не наблюдался "
           "(старые сделки, закрытые безубытком/трейлингом раньше, чем закрылся бы план); они не угадываются."]
    out.append(table(STATS_HEAD, [stats_row(p.name, exits.stats_of(exits.run(trades, p))) for p in exits.plan_grid()]))
    rows = []
    for name, items in exits.by(trades, lambda t: t.strategy).items():
        cur = exits.stats_of(exits.run(items, exits.CURRENT))
        old = exits.stats_of(exits.run(items, exits.OLD_PLAN))
        best_p, best_st = None, None
        for p in exits.plan_grid():
            st = exits.stats_of(exits.run(items, p))
            if st.mean_r is not None and st.known >= max(5, len(items) // 2) and (best_st is None or st.mean_r > (best_st.mean_r or -9)):
                best_p, best_st = p, st
        rows.append([name, len(items), f(cur.mean_r, 3), f(cur.win_rate, pct=True), f(old.mean_r, 3), f(old.win_rate, pct=True),
                     best_p.name if best_p else "—", f(best_st.mean_r, 3) if best_st else "—", f"{best_st.usd:+.0f}$" if best_st else "—"])
    out.append("### По стратегиям")
    out.append(table(["Стратегия", "n", "E[R] 1:4", "винрейт 1:4", "E[R] старый", "винрейт старый", "лучший план", "E[R] лучшего", "USD лучшего"], rows))
    return "\n\n".join(out)


def stops_section(trades: list[Trade], ref: exits.Plan) -> str:
    out = ["## Блок 3. Стоп: стоит ли он за логикой",
           f"Классы: узкий < {stops.TIGHT_ATR} ATR; в шуме — за SL уже заходила хоть одна из {stops.PIERCE_BARS} последних свечей; "
           f"в воздухе — между SL и входом нет структуры ближе {stops.AIR_ATR} ATR; провис — SL дальше якоря (свинга) > {stops.SAG_ATR} ATR; "
           f"широкий > {stops.WIDE_ATR} ATR. R по плану: {ref.name}."]

    def rows_for(groups: dict[str, list[Trade]], label_fn) -> list[list[Any]]:
        rows = []
        for key, items in groups.items():
            n = len(items)
            stopped05 = sum(1 for t in items if (t.mfe or 0) < 0.5 and (t.actual_r or 0) < 0) / n
            stopped1 = sum(1 for t in items if (t.mfe or 0) < 1.0 and (t.actual_r or 0) < 0) / n
            maes = [t.mae for t in items if t.mae is not None]
            st = exits.stats_of(exits.run(items, ref))
            rows.append([label_fn(key), n, f(stopped05, pct=True), f(stopped1, pct=True), f(sum(max(t.mfe or 0, 0) for t in items) / n),
                         f(sum(maes) / len(maes)) if maes else "—", f(st.mean_r, 3), f(exits.actual_stats(items).mean_r, 3)])
        return rows

    head = ["Группа", "n", "выбило до +0.5R", "выбило до +1R", "ср. MFE", "ср. MAE", f"E[R] {ref.name}", "E[R] факт"]
    out.append("### По классу стопа")
    out.append(table(head, rows_for(exits.by(trades, lambda t: t.extra.get("stop_class", "no_data")), lambda k: stops.CLASS_RU.get(k, k))))
    out.append("### По ширине стопа (в ATR сигнальной свечи)")
    out.append(table(head, rows_for(exits.by(trades, lambda t: stops.risk_bucket(t.extra.get("risk_atr"))), lambda k: k)))
    out.append("### Класс стопа × стратегия (n)")
    classes = [c for c in stops.CLASS_RU if any(t.extra.get("stop_class") == c for t in trades)]
    grid = []
    for name, items in exits.by(trades, lambda t: t.strategy).items():
        grid.append([name] + [sum(1 for t in items if t.extra.get("stop_class") == c) or "" for c in classes])
    out.append(table(["Стратегия"] + [stops.CLASS_RU[c] for c in classes], grid))
    winners = [t for t in trades if (t.final_close or {}).get("reason") == "tp" and t.mae is not None]
    if winners:
        maes = sorted(t.mae for t in winners)  # type: ignore[misc]
        q = lambda p: maes[min(len(maes) - 1, int(p * (len(maes) - 1) + 0.5))]  # noqa: E731
        out.append(f"MAE победителей (закрыты по тейку, n={len(maes)}): медиана {q(0.5):.2f}R, p75 {q(0.75):.2f}R, p90 {q(0.9):.2f}R, макс {maes[-1]:.2f}R. "
                   "Если p90 заметно меньше 1R — победители почти не уходят к стопу, и стоп можно поджать (проверка с другим стопом — блок 1m-свечей).")
    sag = [t for t in trades if t.extra.get("stop_class") in ("sag", "air", "noise")]
    if sag:
        rows = [[t.id, t.strategy, t.side, stops.CLASS_RU[t.extra["stop_class"]], f(t.extra.get("risk_atr")), f(t.extra.get("beyond_anchor_atr")),
                 t.extra.get("pierce_20"), f(t.mfe), f(t.actual_r)] for t in sorted(sag, key=lambda t: t.actual_r or 0)[:15]]
        out.append("### Примеры «стоп не по логике» (худшие по факту, до 15) — открыть в History клиента")
        out.append(table(["id", "стратегия", "сторона", "класс", "риск ATR", "за якорем ATR", "пробоев SL", "MFE", "факт R"], rows))
    return "\n\n".join(out)


def correlation_section(trades: list[Trade], ref: exits.Plan) -> str:
    out = ["## Блок 4. Что связано с хорошими и плохими сделками (по стратегиям)",
           f"Цели: «+1R до выхода» (не зависит от плана) и R по плану {ref.name}. ρ — ранговая корреляция Спирмена (−1…+1; |ρ| < 0.2 — почти ничего). "
           f"Группы — только при n ≥ {correlate.MIN_GROUP}. **Всё это гипотезы**: признаков много, сделок мало, часть совпадений случайна."]
    for name, items in exits.by(trades, lambda t: t.strategy).items():
        found = correlate.analyse(items, ref)[:TOP_FINDINGS]
        out.append(f"### {name} (n={len(items)}){small(len(items))}")
        if not found:
            out.append(f"Недостаточно сделок для групп (нужно ≥ {2 * correlate.MIN_GROUP}).")
            continue
        for fd in found:
            rho = f" · ρ(+1R) {f(fd.rho_hit1)} · ρ(R) {f(fd.rho_r)}" if fd.kind == "num" else ""
            groups = "; ".join(f"{g} n={n}: +1R {100 * h:.0f}%, R {f(r)}" for g, n, h, r in fd.groups)
            out.append(f"- **{fd.feature}**{rho} — {groups}")
    return "\n\n".join(out)


def history_section(trades: list[Trade]) -> str:
    out = ["## Блок 5. Вход или выход? Версии и дни",
           "Качество входа — доля сделок, прошедших +0.5R / +1R до выхода, и медиана MFE: от плана выхода не зависит. "
           "Если после 19.10 эти цифры те же, а E[R] упал — виноват выход 1:4. Если упали и они — виноваты правки входов."]
    head = ["Группа", "n", "+0.5R", "+1R", "MFE мед.", "MAE мед."] + [p.name for p in periods.REF_PLANS]

    def rows(groups) -> list[list[Any]]:
        res = []
        for label, q, plans in groups:
            res.append([label, q["n"], f(q.get("reach05"), pct=True), f(q.get("reach1"), pct=True), f(q.get("mfe_med")), f(q.get("mae_med"))]
                       + [f"{f(m, 3)}" + (f" (?{u})" if u else "") for m, u in plans.values()])
        return res

    out.append("### Все стратегии по версиям")
    out.append(table(head, rows(periods.table(trades, periods.version_of))))
    out.append("Версии: " + "; ".join(f"с {d:%d.%m} — {label}" for d, label in periods.DEFAULT_BREAKS))
    for name, items in exits.by(trades, lambda t: t.strategy).items():
        out.append(f"### {name}: по версиям")
        out.append(table(head, rows(periods.table(items, periods.version_of))))
        out.append(f"#### {name}: по дням")
        out.append(table(head, rows(periods.table(items, lambda t: f"{periods.day_of(t):%m-%d}"))))
    out.append("«(?k)» — k сделок с неизвестным исходом по этому плану (не угадываются).")
    return "\n\n".join(out)


def klines_section(res: dict[str, Any]) -> str:
    out = ["## Блок 6. Проверка и что-если по 1m-свечам Binance",
           f"Сделок с путём цены: {res['n']} (пропущено: не крипта — {res['skipped_symbol']}, путь не закончился — {res['unfinished']}). "
           "Внутри минуты стоп считается раньше тейка (пессимистично); минута входа не используется."]
    if res.get("mfe_check"):
        mc = res["mfe_check"]
        out.append(f"Сверка MFE (тики БД vs 1m-свечи): медиана |разницы| {mc['med_abs']:.2f}R, расхождений > 0.3R — {mc['big']} из {mc['n']}. "
                   f"У восстановленных после рестарта (n={mc['restored_n']}) свечи дают MFE выше записанного в {mc['restored_higher']} случаях — "
                   "это движение за время простоя.")
    if res.get("sanity") is not None:
        out.append(f"Сверка симулятора: исходный стоп + TP 1:4 по свечам — E[R] {f(res['sanity'], 3)} vs факт {f(res['sanity_fact'], 3)}.")
    grid = res.get("grid") or {}
    if grid:
        xs = sorted({x for _, x in grid})
        variants = list(dict.fromkeys(v for v, _ in grid))
        rows = []
        for v in variants:
            row = [v]
            for x in xs:
                vals = grid.get((v, x)) or []
                row.append(f"{f(sum(vals) / len(vals), 3)} ({len(vals)})" if vals else "—")
            rows.append(row)
        out.append("### Что-если: стоп × тейк, E[R] на сделку (n), риск тот же $5 → USD = R × 5")
        out.append(table(["Стоп \\ тейк"] + [f"1:{x:g}" for x in xs], rows))
        out.append("«SL ×0.5» — стоп вдвое ближе исходного; «структура + 0.2 ATR» — стоп сразу за якорем (свингом) из блока 3.")
    hunt = res.get("hunt") or []
    if hunt:
        n = len(hunt)
        back = sum(h["back_to_entry"] for h in hunt) / n
        r1 = sum(h["reach1"] for h in hunt) / n
        r2 = sum(h["reach2"] for h in hunt) / n
        ov = sorted(h["overshoot_atr"] for h in hunt if h["overshoot_atr"] is not None)
        med = ov[len(ov) // 2] if ov else None
        out.append(f"### «Выбило и ушло» (стопы, n={n}, окно 6 ч)\n\nПосле стопа цена вернулась ко входу в {back * 100:.0f}% случаев, дошла до +1R — "
                   f"{r1 * 100:.0f}%, до +2R — {r2 * 100:.0f}%. Тень за SL за 30 мин: медиана {f(med)} ATR. "
                   "Много возвратов при маленькой тени — стоп стоит там, где его собирают (за очевидным уровнем без запаса).")
        by_s: dict[str, list[dict]] = {}
        for h in hunt:
            by_s.setdefault(h["strategy"], []).append(h)
        out.append(table(["Стратегия", "стопов", "вернулась ко входу", "+1R после стопа", "+2R после стопа"],
                         [[s, len(v), f(sum(x["back_to_entry"] for x in v) / len(v), pct=True), f(sum(x["reach1"] for x in v) / len(v), pct=True),
                           f(sum(x["reach2"] for x in v) / len(v), pct=True)] for s, v in sorted(by_s.items())]))
    return "\n\n".join(out)


CAVEATS = """## Оговорки (честно)

- Симуляция по MFE точна для сделок, где исход определяется до фактического закрытия; остальные помечены «неизв.» и не угадываются.
- Меньший тейк закрывает сделку раньше → монета у стратегии освобождается раньше → были бы и другие сделки (пропущенные из-за блокировки). Этого симуляция не видит.
- Тейк в симуляции исполняется ровно по уровню; в трекере — по цене тика (иногда чуть лучше). Комиссии — как в трекере (вход 0.02/0.05%, выход 0.05%).
- Старый runner 10% с трейлингом не воспроизводится; «старый план» здесь = 50% на 1R + BE + 50% на 3R.
- У восстановленных после рестарта сделок MFE за простой не наблюдался (нижняя граница); с `--klines` видно, что было за простой.
- Лучший план, подобранный на той же выборке, всегда выглядит лучше, чем будет. Смотрите на проверку второй половиной и бутстреп-интервал.
"""


# ------------------------------------------------------------------------------------------------ CSV

def write_csv(path: Path, trades: list[Trade], ref: exits.Plan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plans = [exits.tp(x) for x in exits.TP_GRID] + [exits.OLD_PLAN] + ([ref] if ref not in exits.plan_grid() else [])
    meta_keys = sorted({k for t in trades for k, v in t.meta.items() if isinstance(v, (int, float, str, bool)) and k not in correlate.SKIP_META})
    geo_keys = ["stop_class", "risk_atr", "beyond_anchor_atr", "near_struct_atr", "pierce_20", "atr"]
    head = (["id", "strategy", "coin", "side", "kind", "mode", "category", "version", "signal_utc", "restored", "tick_gap", "gap_affected", "old_plan",
             "exit_class", "exit_reason", "actual_r", "mfe_r", "mae_r", "risk_usd"] + [f"sim[{p.name}]" for p in plans]
            + geo_keys + [f"meta.{k}" for k in meta_keys])
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(head)
        for t in sorted(trades, key=lambda x: x.signal_ms):
            used = t.category in IN_ANALYSIS
            sims = [exits.simulate(t, p).net_r if used else None for p in plans]
            w.writerow([t.id, t.strategy, t.coin, t.side, t.kind, t.mode, t.category, periods.version_of(t), periods.day_of(t), t.restored, t.tick_gap,
                        t.gap, t.old_plan, exits.exit_class(t) if used else "", t.exit_reason, t.actual_r, t.mfe, t.mae, t.risk_usd]
                       + ["" if s is None else round(s, 4) for s in sims] + [t.extra.get(k, "") for k in geo_keys] + [t.meta.get(k, "") for k in meta_keys])
