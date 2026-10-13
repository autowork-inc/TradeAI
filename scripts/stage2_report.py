"""Отчёт Этапа 2: прогон детекторов SMC Swing и Breakout Buildup по истории 15m (data/raw/klines), пессимистичная
симуляция, воронка состояний, чувствительность к потолку плеча (О8).

Запуск (из корня):  python -m scripts.stage2_report [--start 2026-04-01] [--end 2026-12-01]
Только читает локальные parquet-файлы, в сеть не ходит и боевой процесс не трогает. Пишет out/stage2/*.csv.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

import config
from strategies.runner import RunConfig, load_segments, run_symbol, summarize
from strategies.simulate import SimParams
from strategies.smc_setups import SmcSetupParams

INTERVAL = "15m"
CAPS = (1.0, 2.0, 3.0, 5.0, 10.0, 1e9)  # 1e9 = «без потолка» (только для чувствительности)
OUT = config.BASE_DIR / "out" / "stage2"


def ms(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def md(df: pl.DataFrame) -> str:
    """Таблица Markdown из DataFrame."""
    cols = df.columns
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.iter_rows():
        lines.append("| " + " | ".join("" if v is None else (f"{v:.3f}" if isinstance(v, float) else str(v)) for v in row) + " |")
    return "\n".join(lines)


def funnel(det: pl.DataFrame, side: str) -> dict:
    """Воронка по сетапам (sid > 0): свип -> FVG -> CHoCH -> заявка; причины гибели."""
    d = det.filter(pl.col(f"{side}_sid") > 0)
    per = d.group_by(f"{side}_sid").agg(
        (pl.col(f"{side}_state") >= 2).any().alias("fvg"), pl.col(f"{side}_choch").any().alias("choch"),
        pl.col(f"{side}_signal").any().alias("signal"), pl.col(f"{side}_dead").any().alias("dead"),
        pl.col(f"{side}_nofvg_break").any().alias("nofvg"),
    )
    fvg = per.filter(pl.col("fvg") | pl.col("choch"))
    rejects = det[f"{side}_reject"].drop_nulls().value_counts()
    rej = {r[f"{side}_reject"]: r["count"] for r in rejects.to_dicts()}
    return {
        "setups(sweeps)": per.height, "reached_fvg": fvg.height, "choch": int(per["choch"].sum()),
        "signal": int(per["signal"].sum()),
        "die_invalidated": per.filter(pl.col("dead") & ~pl.col("choch")).height,
        "die_break_without_fvg": per.filter(pl.col("nofvg") & ~pl.col("choch") & ~pl.col("dead")).height,
        "die_ttl_or_other": per.filter(~pl.col("dead") & ~pl.col("nofvg") & ~pl.col("choch")).height,
        **{f"reject_{k}": v for k, v in rej.items()},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-12-01")
    a = ap.parse_args()
    iv = config.INTERVAL_MS[INTERVAL]
    OUT.mkdir(parents=True, exist_ok=True)
    data = {s: load_segments(s, INTERVAL, ms(a.start), ms(a.end), iv) for s in config.SYMBOLS}

    print("## Данные")
    rows = [(s, len(segs), sum(g.height for g in segs), segs[0]["open_time"][0], segs[-1]["open_time"][-1]) for s, segs in data.items()]
    fmt = lambda t: datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")  # noqa: E731
    print(md(pl.DataFrame([(s, n, h, fmt(t0), fmt(t1)) for s, n, h, t0, t1 in rows], schema=["symbol", "segments", "bars", "from", "to"], orient="row")))

    base = RunConfig()
    runs = {s: run_symbol(s, data[s], iv, base) for s in config.SYMBOLS}

    print("\n## Признаки (число событий за период)")
    from features.smc import add_features
    feat_rows = []
    for s in config.SYMBOLS:
        f = pl.concat([add_features(g, base.smc_features, interval_ms=iv) for g in data[s]])
        feat_rows.append((s, f.height, int(f["swing_high"].sum()), int(f["swing_low"].sum()), int(f["fvg_bear"].sum()),
                          int(f["fvg_bull"].sum()), int(f["eqh_ok"].sum()), int(f["hl_ok"].sum()), int(f["buildup"].sum())))
    print(md(pl.DataFrame(feat_rows, schema=["symbol", "bars", "swing_high", "swing_low", "fvg_bear", "fvg_bull", "eqh_ok_bars", "hl_ok_bars", "buildup_bars"], orient="row")))

    print("\n## SMC Swing: воронка состояний S1->S4 (дефолты после О12: потолок плеча 5.0, импульс FVG 1.0*ATR, срок сетапа 30)")
    fr = []
    for s in config.SYMBOLS:
        dets = pl.concat(runs[s][0])
        for side in ("short", "long"):
            fr.append({"symbol": s, "side": side, **funnel(dets, side)})
    print(md(pl.DataFrame(fr).fill_null(0)))

    print("\n## SMC: стоп в % от цены у всех CHoCH (без потолка плеча)")
    wide = RunConfig(smc=SmcSetupParams(max_exposure=1e9), sim=SimParams(max_exposure=1e9))
    risk_rows = []
    for s in config.SYMBOLS:
        dets = pl.concat(run_symbol(s, data[s], iv, wide)[0])
        for side in ("short", "long"):
            r = dets.filter(pl.col(f"{side}_choch"))[f"{side}_risk_pct"]
            if r.len():
                risk_rows.append((s, side, r.len(), r.min() * 100, r.quantile(0.5) * 100, r.max() * 100, int((r >= 0.002).sum())))
    print(md(pl.DataFrame(risk_rows, schema=["symbol", "side", "choch", "min_%", "median_%", "max_%", "stop>=0.2%_(passes cap 5.0)"], orient="row")))

    print("\n## Итоги по стратегиям — дефолтный потолок плеча 5.0")
    all_cap = {}
    for cap in CAPS:
        cfg = RunConfig(smc=SmcSetupParams(max_exposure=cap), sim=SimParams(max_exposure=cap))
        tr = [run_symbol(s, data[s], iv, cfg)[1] for s in config.SYMBOLS]
        tr = [t for t in tr if t.height]
        all_cap[cap] = pl.concat(tr) if tr else pl.DataFrame()
    for cap in (5.0,):
        t = all_cap[cap]
        print(md(pl.DataFrame([{"strategy": st, **summarize(t.filter(pl.col("strategy") == st))} for st in ("smc_short", "smc_long", "breakout") if t.height]).fill_null(0)))

    print("\n## Чувствительность к потолку плеча (НЕ решение, а диагностика): стратегия x потолок")
    sens = []
    for cap, t in all_cap.items():
        if not t.height:
            continue
        for st in ("smc", "breakout"):
            sub = t.filter(pl.col("strategy").str.starts_with(st))
            if sub.height:
                sm = summarize(sub)
                sens.append({"strategy": st, "cap": "inf" if cap > 1e6 else cap, **{k: sm.get(k) for k in ("signals", "locked", "unfilled", "rejected", "trades", "tp", "be", "sl", "timeout", "win_rate_net", "sum_net_r", "expectancy_net_r", "profit_factor", "max_dd_r", "avg_risk_pct")}})
    print(md(pl.DataFrame(sens).fill_null(0)))

    print("\n## Абляция воронки (диагностика, не оптимизация): что душит число сигналов. Потолок плеча снят (inf)")
    from dataclasses import replace
    from features.smc import SmcParams
    variants = {
        "базовый (дефолты после О12)": {},
        "D3: порог амплитуды свинга 1.0*ATR": {"f": {"swing_atr_mult": 1.0}},
        "D3: порог амплитуды свинга 0.5*ATR": {"f": {"swing_atr_mult": 0.5}},
        "импульс FVG 1.5*ATR (старый дефолт)": {"f": {"impulse_atr_mult": 1.5}},
        "импульс FVG 0.5*ATR": {"f": {"impulse_atr_mult": 0.5}},
        "D10: k=3 (было 5)": {"f": {"k": 3}},
        "D8: срок сетапа 15 баров (старый дефолт)": {"s": {"setup_ttl_bars": 15}},
        "О10 снят (опорный лоу любой)": {"s": {"choch_ref_after_pool": False}},
    }
    ab = []
    for name, v in variants.items():
        cfg = RunConfig(
            smc_features=replace(SmcParams(), **v.get("f", {})),
            smc=replace(SmcSetupParams(max_exposure=1e9), **v.get("s", {})),
            sim=SimParams(max_exposure=1e9),
        )
        sw = fv = ch = 0
        tr = []
        for s in config.SYMBOLS:
            dets, t = run_symbol(s, data[s], iv, cfg)
            d = pl.concat(dets)
            for side in ("short", "long"):
                f = funnel(d, side)
                sw += f["setups(sweeps)"]; fv += f["reached_fvg"]; ch += f["choch"]
            if t.height:
                tr.append(t.filter(pl.col("strategy").str.starts_with("smc")))
        t = pl.concat(tr) if tr else pl.DataFrame()
        sm = summarize(t) if t.height else {}
        ab.append({"вариант": name, "sweeps": sw, "reached_fvg": fv, "choch": ch, "signals": sm.get("signals", 0),
                   "unfilled": sm.get("unfilled", 0), "trades": sm.get("trades", 0), "sum_net_r": sm.get("sum_net_r", 0.0),
                   "expectancy_r": sm.get("expectancy_net_r", 0.0)})
    print(md(pl.DataFrame(ab).fill_null(0)))

    for cap in (5.0, 1e9):
        if all_cap[cap].height:
            all_cap[cap].write_csv(OUT / f"trades_cap_{'inf' if cap > 1e6 else int(cap)}.csv")
    print(f"\nСделки записаны в {OUT}")


if __name__ == "__main__":
    main()
