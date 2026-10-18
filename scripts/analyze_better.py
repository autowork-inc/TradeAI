"""Статистика по better.jsonl (сделки, размеченные нейросетью) без загрузки файла в контекст ИИ.

Запуск:  .venv\\Scripts\\python.exe -X utf8 -m scripts.analyze_better <путь к better.jsonl>
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import statistics
import sys
from pathlib import Path

STOP = set(
    "the a an of to and or in on at for with by is are was be as that this it its from into after before "
    "than then which while when where price candle candles bar bars level".split()
)
# Числа рядом с единицами: 1.5 ATR, 0.2x, 30%, 5 bars, 3R
NUM_UNIT = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(x|×|%|atr|r\b|rr|bars?|candles?|свеч\w*|пункт\w*|pips?|ticks?)", re.I
)
RATIO = re.compile(r"(\d+(?:[.,]\d+)?)\s*[:/]\s*(\d+(?:[.,]\d+)?)")


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower().replace("-", "_").replace(" ", "_"))


def score10(v: float) -> float:
    """Шкала разметки 0–10, но встречаются 0–1 (0.68) и 0–100 (68) — приводим к 0–10."""
    v = float(v)
    if v <= 1.0:
        return v * 10
    if v > 10:
        return v / 10
    return v


def load(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                r = json.loads(line)
                r["score_raw"] = r["success_probability_score"]
                r["success_probability_score"] = score10(r["score_raw"])
                rows.append(r)
    return rows


SHORT_WORDS = re.compile(r"breakdown|below|support|поддерж|под\b|ниже|short|шорт|<", re.I)
LONG_WORDS = re.compile(r"breakout above|above resistance|resistance|сопротивл|выше|над\b|long|лонг|>", re.I)
PCT = re.compile(r"(\d+(?:[.,]\d+)?)\s*%")
RR = re.compile(r"(?:r\s*:\s*r|rr|соотношени\w*|risk[ /:]*reward)[^0-9]{0,25}(\d+(?:[.,]\d+)?)\s*:\s*(\d+(?:[.,]\d+)?)", re.I)
ANY_RATIO = re.compile(r"\b(\d+(?:[.,]\d+)?)\s*:\s*(\d+(?:[.,]\d+)?)\b")
VOLX = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:x|×|раз)", re.I)


def direction(r: dict) -> str:
    t = r["trigger_logic"]
    s, l = len(SHORT_WORDS.findall(t)), len(LONG_WORDS.findall(t))
    if s > l:
        return "short"
    if l > s:
        return "long"
    return "?"


def fnum(s: str) -> float:
    return float(s.replace(",", "."))


def deep_report(g: list[dict], title: str) -> None:
    print(f"\n=== 3a. Числовые закономерности: {title} ===")
    dirs = collections.Counter(direction(r) for r in g)
    print("Направление по trigger_logic:", dict(dirs))
    print("Направление × trend_context:",
          dict(collections.Counter((direction(r), norm(r["trend_context"])) for r in g)))
    sl = [fnum(m) for r in g for m in PCT.findall(r["stop_loss_logic"])[:1]]
    sl = [v for v in sl if 0 < v < 20]
    print(f"Стоп, % от входа (из stop_loss_logic, есть у {len(sl)}/{len(g)}):", describe(sl))
    bins = collections.Counter(min(int(v / 0.25) * 0.25, 2.0) for v in sl)
    print("  гистограмма шагом 0.25%:", {f"{k:.2f}": c for k, c in sorted(bins.items())})
    sl_anchor = collections.Counter()
    for r in g:
        t = r["stop_loss_logic"].lower()
        if re.search(r"breakout candle|breakdown candle|свеч\w* пробо", t):
            sl_anchor["за свечой пробоя"] += 1
        elif re.search(r"consolidation|консолидац|range|диапазон|полк", t):
            sl_anchor["за экстремумом консолидации"] += 1
        elif re.search(r"swing|свинг|локальн\w* (max|min|максимум|минимум)", t):
            sl_anchor["за локальным свингом"] += 1
        elif re.search(r"fixed|фиксир", t):
            sl_anchor["фиксированный %"] += 1
        else:
            sl_anchor["другое"] += 1
    print("Якорь стопа:", sl_anchor.most_common())
    rr = []
    for r in g:
        m = RR.findall(r["notes"]) or ANY_RATIO.findall(r["notes"])
        for a, b in m[:1]:
            a, b = fnum(a), fnum(b)
            if b > 0 and 0.2 <= a / b <= 20:
                rr.append(a / b)
    print(f"R:R из notes (есть у {len(rr)}/{len(g)}):", describe(rr))
    print("  R:R корзины:", dict(collections.Counter(
        "<1" if v < 0.95 else "≈1" if v <= 1.15 else "1.15–2" if v <= 2 else "2–3" if v <= 3 else ">3" for v in rr)))
    vx = [fnum(v) for r in g for v in VOLX.findall(r["notes"] + " " + r["trigger_logic"])[:1]]
    vx = [v for v in vx if 1 < v <= 20]
    print(f"Кратность всплеска объёма (есть у {len(vx)}/{len(g)}):", describe(vx))
    cb = [r["consolidation_bars_approx"] for r in g if r["has_consolidation"]]
    print("Консолидация, баров — корзины:", dict(sorted(collections.Counter(
        "5–9" if v < 10 else "10–19" if v < 20 else "20–39" if v < 40 else "40–99" if v < 100 else "100+"
        for v in cb).items())))
    kw = {
        "многократный тест уровня": r"многократн|multiple test|retest|ретест|тестирован",
        "понижающиеся максимумы / lower highs": r"понижающ\w* максимум|lower high",
        "повышающиеся минимумы / higher lows": r"повышающ\w* минимум|higher low",
        "поджатие / squeeze": r"поджат|squeeze|compression|сжат",
        "импульс / экспансия": r"импульс|expansion|экспанс",
        "закрепление / close-confirmation": r"закреплен|close [<>]|close below|close above|закрыти",
        "ретест после пробоя": r"retest|ретест",
        "ложный пробой / sweep": r"ложн|sweep|свип|снятие ликвидности",
        "по тренду": r"по (общему )?\w* ?тренду|with trend|trend continuation|продолжени",
        "треугольник / клин": r"треугольн|triangle|клин|wedge",
    }
    print("Признаки в notes+trigger (доля сделок):")
    for name, pat in kw.items():
        n = sum(bool(re.search(pat, r["notes"] + " " + r["trigger_logic"], re.I)) for r in g)
        print(f"  {name:40} {n:>4} ({n / len(g):.0%})")
    print("Средний score: vol_anomaly T/F = "
          f"{statistics.mean([r['success_probability_score'] for r in g if r['volume_anomaly']] or [0]):.2f} / "
          f"{statistics.mean([r['success_probability_score'] for r in g if not r['volume_anomaly']] or [0]):.2f}")
    for d in ("short", "long"):
        sub = [r for r in g if direction(r) == d]
        if sub:
            print(f"  {d}: n={len(sub)} score={statistics.mean(r['success_probability_score'] for r in sub):.2f} "
                  f"vol_anomaly={sum(r['volume_anomaly'] for r in sub) / len(sub):.0%}")


def describe(vals: list[float]) -> str:
    if not vals:
        return "—"
    q = statistics.quantiles(vals, n=4) if len(vals) > 1 else [vals[0]] * 3
    return (f"n={len(vals)} mean={statistics.mean(vals):.2f} med={statistics.median(vals):.2f} "
            f"q1={q[0]:.2f} q3={q[2]:.2f} min={min(vals):.2f} max={max(vals):.2f}")


def tokens(text: str) -> list[str]:
    return [w for w in re.findall(r"[a-zа-яё_]+", text.lower()) if w not in STOP and len(w) > 2]


def ngrams(text: str, n: int) -> list[str]:
    t = tokens(text)
    return [" ".join(t[i:i + n]) for i in range(len(t) - n + 1)]


def text_report(name: str, texts: list[str], top: int) -> None:
    print(f"\n--- {name}: {len(texts)} текстов ---")
    nums = collections.Counter()
    for t in texts:
        for v, u in NUM_UNIT.findall(t):
            u = u.lower()
            u = {"bar": "bars", "candle": "bars", "candles": "bars"}.get(u, u)
            if u.startswith("свеч"):
                u = "bars"
            nums[f"{v.replace(',', '.')} {u}"] += 1
        for a, b in RATIO.findall(t):
            nums[f"ratio {a}:{b}"] += 1
    print("Числа с единицами (top):", ", ".join(f"{k}×{c}" for k, c in nums.most_common(top)) or "—")
    for n in (1, 2, 3):
        cnt = collections.Counter()
        for t in texts:
            cnt.update(set(ngrams(t, n)))  # доля документов, а не частота слова
        print(f"{n}-граммы (в скольких текстах):",
              ", ".join(f"{k}={c}" for k, c in cnt.most_common(top)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path", type=Path)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--samples", type=int, default=6)
    a = ap.parse_args()

    rows = load(a.path)
    print(f"Записей: {len(rows)}; is_valid_chart=true: {sum(r['is_valid_chart'] for r in rows)}")
    ts = sorted(r["timestamp"] for r in rows)
    print(f"timestamp: {ts[0]} … {ts[-1]}")

    # 1. Группировка по setup_type
    groups: dict[str, list[dict]] = collections.defaultdict(list)
    raw_names: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for r in rows:
        k = norm(r["setup_type"])
        groups[k].append(r)
        raw_names[k][r["setup_type"]] += 1

    print("\n=== 1. setup_type ===")
    print(f"{'setup_type':42} {'n':>4} {'доля':>6} {'score':>6} {'consol':>7} {'vol_an':>7} {'cons_bars':>9}")
    table = []
    for k, g in groups.items():
        sc = [float(r["success_probability_score"]) for r in g]
        cons = sum(r["has_consolidation"] for r in g) / len(g)
        vol = sum(r["volume_anomaly"] for r in g) / len(g)
        cb = [r["consolidation_bars_approx"] for r in g if r["has_consolidation"]]
        table.append((k, len(g), statistics.mean(sc), cons, vol, statistics.median(cb) if cb else 0))
    table.sort(key=lambda t: (-t[1], -t[2]))
    for k, n, sc, cons, vol, cb in table:
        print(f"{k[:42]:42} {n:>4} {n / len(rows):>6.1%} {sc:>6.2f} {cons:>7.0%} {vol:>7.0%} {cb:>9}")
    for k, _, *_ in table:
        if len(raw_names[k]) > 1:
            print(f"  варианты написания {k}: {dict(raw_names[k])}")

    print("\n=== 1a. Сводная таблица: тип (n \\ всего) и доли внутри типа ===")
    print(f"{'setup_type':22} {'n\\всего':>9} {'score≥7':>8} {'объём':>6} {'консол.':>8} {'short':>6} {'long':>6} {'медиана SL%':>12}")
    for k, g_ in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        hi = sum(r["success_probability_score"] >= 7 for r in g_)
        vol = sum(r["volume_anomaly"] for r in g_)
        con = sum(r["has_consolidation"] for r in g_)
        sh = sum(direction(r) == "short" for r in g_)
        lo = sum(direction(r) == "long" for r in g_)
        sls = [fnum(m) for r in g_ for m in PCT.findall(r["stop_loss_logic"])[:1]]
        sls = [v for v in sls if 0 < v < 20]
        med = f"{statistics.median(sls):.2f} (n={len(sls)})" if sls else "—"
        pc = lambda x: f"{x / len(g_):.0%}"
        print(f"{k:22} {f'{len(g_)}\\{len(rows)}':>9} {pc(hi):>8} {pc(vol):>6} {pc(con):>8} {pc(sh):>6} {pc(lo):>6} {med:>12}")

    # 2. Победитель: частота × средний score × доля консолидации
    print("\n=== 2. Ранжирование (n ≥ 5): score_rank = mean_score × consol_share, вес частоты ===")
    big = [t for t in table if t[1] >= 5]
    ranked = sorted(big, key=lambda t: (t[1] * t[2] * (0.5 + t[3])), reverse=True)
    for k, n, sc, cons, *_ in ranked[:10]:
        print(f"  {k:42} n={n:<4} score={sc:.2f} consol={cons:.0%} composite={n * sc * (0.5 + cons):.0f}")
    winner = ranked[0][0]
    g = groups[winner]
    print(f"\nПобедитель: {winner} (n={len(g)})")

    print("\n=== Профиль победителя ===")
    print("score:", describe([float(r["success_probability_score"]) for r in g]))
    print("score по всем:", describe([float(r["success_probability_score"]) for r in rows]))
    print("trend_context:", collections.Counter(norm(r["trend_context"]) for r in g).most_common(10))
    print("has_consolidation:", collections.Counter(r["has_consolidation"] for r in g))
    print("consolidation_bars (при консолидации):",
          describe([r["consolidation_bars_approx"] for r in g if r["has_consolidation"]]))
    print("volume_anomaly:", collections.Counter(r["volume_anomaly"] for r in g))
    print("score при consol=T / F:",
          describe([float(r["success_probability_score"]) for r in g if r["has_consolidation"]]), "|",
          describe([float(r["success_probability_score"]) for r in g if not r["has_consolidation"]]))
    fn = collections.Counter()
    for r in g:
        fn.update(re.findall(r"[A-Z]{3,}USDT?|[A-Z]{6}|\b\d+[mhdw]\b|\bM\d+\b|\bH\d+\b", r["filename"]))
    print("Токены filename (символ/ТФ):", fn.most_common(15))

    # 3. Текстовые поля победителя
    deep_report(g, winner)
    if "trendline" in groups and winner != "trendline":
        deep_report(groups["trendline"], "trendline (для сравнения)")

    print("\n=== 3. Закономерности в тексте победителя ===")
    for field in ("trigger_logic", "stop_loss_logic", "notes"):
        text_report(field, [r[field] for r in g], a.top)
    print("\n--- Примеры (наивысший score) ---")
    for r in sorted(g, key=lambda r: -float(r["success_probability_score"]))[: a.samples]:
        print(f"* score={r['success_probability_score']} trend={r['trend_context']} "
              f"consol={r['has_consolidation']}/{r['consolidation_bars_approx']} vol={r['volume_anomaly']}")
        for field in ("trigger_logic", "stop_loss_logic", "notes"):
            print(f"    {field}: {r[field][:300]}")

    # Контраст: что отличает победителя от остальных
    print("\n=== Контраст: биграммы trigger_logic, частые у победителя и редкие у остальных ===")
    rest = [r for r in rows if norm(r["setup_type"]) != winner]
    cw, cr = collections.Counter(), collections.Counter()
    for r in g:
        cw.update(set(ngrams(r["trigger_logic"] + " " + r["stop_loss_logic"], 2)))
    for r in rest:
        cr.update(set(ngrams(r["trigger_logic"] + " " + r["stop_loss_logic"], 2)))
    lift = [(k, c / len(g), cr[k] / max(len(rest), 1)) for k, c in cw.items() if c >= max(5, len(g) // 10)]
    lift.sort(key=lambda t: t[1] - t[2], reverse=True)
    for k, pw, pr in lift[: a.top]:
        print(f"  {k:35} победитель {pw:5.0%}  остальные {pr:5.0%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
