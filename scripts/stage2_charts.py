"""Визуальная проверка паттернов Этапа 2: out/stage2/examples.html со свечными графиками (inline SVG, без библиотек).

Для каждого сигнала SMC/Breakout (потолок плеча снят, чтобы увидеть все): свечи вокруг сетапа, пул ликвидности
(синяя линия), хай свипа (красный пунктир), зона FVG (серая), вход/SL/TP, метки: S1 свип, S3 CHoCH, F заполнение, X выход.
Запуск:  python -m scripts.stage2_charts [--start 2026-04-01] [--limit 40]
"""

import argparse
import html
from datetime import datetime, timezone

import polars as pl

import config
from scripts.stage2_report import INTERVAL, OUT, ms
from strategies.runner import RunConfig, load_segments, run_symbol
from strategies.simulate import SimParams
from strategies.smc_setups import SmcSetupParams

W, H, PAD_L, PAD_R, PAD_T, PAD_B = 980, 330, 60, 12, 14, 22


def chart(det: pl.DataFrame, tr: dict) -> str:
    strat = tr["strategy"]
    side = {"smc_short": "short", "smc_long": "long"}.get(strat)
    idx = tr["idx"]
    start = max(0, (tr["sweep_idx"] if side else idx) - 30)
    fill = idx + tr["fill_off"] if tr["fill_off"] else None
    end_i = min(det.height - 1, (idx + tr["exit_off"] if tr["exit_off"] else idx + 12) + 8)
    d = det.slice(start, end_i - start + 1)
    n = d.height
    lines = [tr["entry"], tr["sl"], tr["tp"]] if tr["tp"] is not None else [tr["entry"], tr["sl"]]
    if side:
        lines += [tr["inval"], d[f"{side}_pool_level"][idx - start]]
    lo, hi = min(d["low"].min(), *[x for x in lines if x]), max(d["high"].max(), *[x for x in lines if x])
    pad = (hi - lo) * 0.04
    lo, hi = lo - pad, hi + pad
    x = lambda i: PAD_L + (i + 0.5) * (W - PAD_L - PAD_R) / n  # noqa: E731
    y = lambda p: PAD_T + (hi - p) * (H - PAD_T - PAD_B) / (hi - lo)  # noqa: E731
    cw = max(2.0, (W - PAD_L - PAD_R) / n * 0.62)
    out = [f'<svg viewBox="0 0 {W} {H}" width="100%" xmlns="http://www.w3.org/2000/svg" font-family="sans-serif" font-size="10">']
    for g in range(5):
        p = lo + (hi - lo) * g / 4
        out.append(f'<line x1="{PAD_L}" x2="{W-PAD_R}" y1="{y(p):.1f}" y2="{y(p):.1f}" stroke="#ddd"/><text x="2" y="{y(p)+3:.1f}" fill="#666">{p:.2f}</text>')
    if side:  # зона FVG: от первой свечи FVG до сигнала
        hi_z, lo_z = d[f"{side}_fvg_hi"][idx - start], d[f"{side}_fvg_lo"][idx - start]
        out.append(f'<rect x="{x(idx - 2 - start):.1f}" y="{y(hi_z):.1f}" width="{x(idx - start) - x(idx - 2 - start) + 40:.1f}" height="{y(lo_z) - y(hi_z):.1f}" fill="#999" opacity=".25"/>')
    for i, r in enumerate(d.iter_rows(named=True)):
        up = r["close"] >= r["open"]
        c = "#1a9850" if up else "#d73027"
        out.append(f'<line x1="{x(i):.1f}" x2="{x(i):.1f}" y1="{y(r["high"]):.1f}" y2="{y(r["low"]):.1f}" stroke="{c}"/>')
        top, bot = y(max(r["open"], r["close"])), y(min(r["open"], r["close"]))
        out.append(f'<rect x="{x(i)-cw/2:.1f}" y="{top:.1f}" width="{cw:.1f}" height="{max(bot-top,1):.1f}" fill="{c}"/>')

    def hline(p, color, dash, label):
        out.append(f'<line x1="{PAD_L}" x2="{W-PAD_R}" y1="{y(p):.1f}" y2="{y(p):.1f}" stroke="{color}" stroke-dasharray="{dash}"/>'
                   f'<text x="{W-PAD_R-2}" y="{y(p)-2:.1f}" text-anchor="end" fill="{color}">{label} {p:.2f}</text>')
    if side:
        hline(d[f"{side}_pool_level"][idx - start], "#1f77b4", "0", "пул")
        hline(tr["inval"], "#d73027", "4 3", "хай свипа")
    hline(tr["entry"], "#444", "0", "вход")
    hline(tr["sl"], "#d73027", "2 2", "SL")
    if tr["tp"] is not None:
        hline(tr["tp"], "#1a9850", "2 2", "TP")

    def mark(i, label, color, below=False):
        if start <= i <= end_i:
            yy = y(d["low"][i - start]) + 12 if below else y(d["high"][i - start]) - 5
            out.append(f'<text x="{x(i - start):.1f}" y="{yy:.1f}" text-anchor="middle" font-weight="bold" fill="{color}">{label}</text>')
    if side:
        mark(tr["sweep_idx"], "S1", "#1f77b4")
    mark(idx, "S3" if side else "BRK", "#9467bd", below=True)
    if fill is not None:
        mark(fill, "F", "#ff7f0e", below=True)
        if tr["exit_off"]:
            mark(idx + tr["exit_off"], "X", "#000", below=True)
    out.append("</svg>")
    return "".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-12-01")
    ap.add_argument("--limit", type=int, default=40)
    a = ap.parse_args()
    iv = config.INTERVAL_MS[INTERVAL]
    cfg = RunConfig(smc=SmcSetupParams(max_exposure=1e9), sim=SimParams(max_exposure=1e9), ownership_lock=False)
    cards = []
    for s in config.SYMBOLS:
        segs = load_segments(s, INTERVAL, ms(a.start), ms(a.end), iv)
        dets, trades = run_symbol(s, segs, iv, cfg)
        det = dets[0]
        for tr in trades.sort("idx").iter_rows(named=True):
            t = datetime.fromtimestamp(tr["open_time"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
            r = f"{tr['net_r']:+.2f}R" if tr["net_r"] is not None else "—"
            head = f'{html.escape(s)} · {tr["strategy"]} · сигнал {t} UTC · исход: <b>{tr["outcome"]}</b> · {r}'
            cards.append(f'<div class="card"><div class="h">{head}</div>{chart(det, tr)}</div>')
    body = "\n".join(cards[: a.limit])
    page = (
        '<!doctype html><meta charset="utf-8"><title>Stage 2 examples</title>'
        '<style>body{font:14px sans-serif;margin:16px;max-width:1040px}.card{margin:0 0 18px;border:1px solid #ccc;border-radius:6px;padding:8px}'
        '.h{margin-bottom:4px}</style>'
        f"<h2>Этап 2: примеры сигналов ({min(len(cards), a.limit)} из {len(cards)})</h2>"
        "<p>S1 — свип, S3 — CHoCH/сигнал, F — заполнение лимитки, X — выход. Синяя линия — пул ликвидности, красный пунктир — хай свипа (инвалидация), "
        "серый прямоугольник — FVG, вход = середина FVG.</p>" + body
    )
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "examples.html").write_text(page, encoding="utf-8")
    print(f"{len(cards)} сигналов, записано {OUT / 'examples.html'}")


if __name__ == "__main__":
    main()
