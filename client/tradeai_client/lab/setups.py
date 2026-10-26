"""Сетапы бота в реплее: что бот говорит на ПОСЛЕДНЕЙ закрытой свече (только прошлое, без будущего).

Подсказки (alert) - «скоро может быть движение, ищу вход»: цена подходит к зоне по тренду, сжатие у уровня, рядом несобранная ликвидность.
Сигналы (signal) - сетап сложился на этой свече: вход (по рынку на открытии следующей), стоп, цель. Сетапы собраны из понятий словаря:
  * «Отбой от зоны по тренду»: тренд вниз (вверх), свеча коснулась зоны/уровня сопротивления (поддержки) и дала разворот;
  * «Пробой проторговки / консолидации»: закрытие за коридором по тренду (или из боковика с всплеском объёма);
  * «Пробой наклонки»: закрытие за наклонной линией против прежнего движения + подтверждение (тренд не против).
Стоп - за зоной / серединой коридора / последним экстремумом (+0.15 ATR); цель - ближайшая встречная зона, если до неё >= min_rr, иначе rr R."""

from typing import Any

from tradeai_client.lab.detect import Ctx, trend_state

DEFAULT_SETUP = {"rr": 2.0, "min_rr": 1.5, "stop_pad_atr": 0.15, "near_atr": 0.6, "trend_k": 5}
SETUPS = ("zone_bounce", "box_breakout", "trendline_break")
SETUP_RU = {"zone_bounce": "Отбой от зоны по тренду", "box_breakout": "Пробой проторговки", "trendline_break": "Пробой наклонки"}


def _zones(c: Ctx) -> list[tuple[float, float, str]]:
    out = []
    for z in c.out.get("zone", []) + c.out.get("level", []):
        lo, hi = (z["p1"], z["p2"]) if z["kind"] == "box" else (z["p1"], z["p1"])
        out.append((lo, hi, z["label"]))
    return out


def _target(c: Ctx, side: str, entry: float, stop: float, cfg: dict) -> tuple[float, str]:
    risk = abs(entry - stop)
    zs = [((lo if side == "long" else hi), lab) for lo, hi, lab in _zones(c)]
    if side == "long":
        cand = sorted((p, lab) for p, lab in zs if p > entry + cfg["min_rr"] * risk)
    else:
        cand = sorted(((p, lab) for p, lab in zs if p < entry - cfg["min_rr"] * risk), reverse=True)
    if cand:
        return cand[0][0], f"цель - встречная {cand[0][1]}"
    return (entry + cfg["rr"] * risk) if side == "long" else (entry - cfg["rr"] * risk), f"цель {cfg['rr']:g}R"


def _signal(c: Ctx, setup: str, side: str, stop: float, why: list[str], cfg: dict) -> dict[str, Any] | None:
    entry = c.bars[-1]["c"]
    if (side == "long" and stop >= entry) or (side == "short" and stop <= entry):
        return None
    target, tw = _target(c, side, entry, stop, cfg)
    rr = abs(target - entry) / abs(entry - stop)
    return {"kind": "signal", "setup": setup, "name": SETUP_RU[setup], "side": side, "t": c.end_t(), "entry": entry, "stop": stop, "target": target,
            "rr": round(rr, 2), "why": why + [tw],
            "text": f"СИГНАЛ {'ЛОНГ' if side == 'long' else 'ШОРТ'}: {SETUP_RU[setup]}. Вход ~{entry:g} (по рынку), стоп {stop:g}, цель {target:g} ({rr:.1f}R)"}


def evaluate(c: Ctx, cfg: dict | None = None, enabled: tuple[str, ...] = SETUPS) -> list[dict[str, Any]]:
    """Подсказки и сигналы на последней свече контекста (детекторы уже посчитаны на свечах ДО неё включительно)."""
    cfg = {**DEFAULT_SETUP, **(cfg or {})}
    if c.n < 40:
        return []
    b, j, atr = c.bars, c.n - 1, c.atr[-1] or 1e-9
    last = b[j]
    tr = trend_state(c, cfg["trend_k"])
    tr_txt = {1: "тренд вверх", -1: "тренд вниз", 0: "боковик"}[tr]
    pad = cfg["stop_pad_atr"] * atr
    out: list[dict[str, Any]] = []
    rev = {o["side"] for o in c.out.get("reversal", []) if o["t1"] == last["t"]}
    brk = [o for o in c.out.get("breakout", []) if o["t1"] == last["t"]]
    spike = any(o["t1"] == last["t"] and o["label"].startswith("всплеск") for o in c.out.get("volume", []))

    # --- отбой от зоны по тренду
    if "zone_bounce" in enabled:
        for lo, hi, lab in _zones(c):
            if tr <= 0 and "short" in rev and lo - 0.4 * atr <= last["h"] and last["c"] < hi:
                s = _signal(c, "zone_bounce", "short", max(hi, last["h"]) + pad, [tr_txt, f"касание {lab} сверху", "разворотная свеча вниз"], cfg)
                if s:
                    out.append(s)
                    break
            if tr >= 0 and "long" in rev and hi + 0.4 * atr >= last["l"] and last["c"] > lo:
                s = _signal(c, "zone_bounce", "long", min(lo, last["l"]) - pad, [tr_txt, f"касание {lab} снизу", "разворотная свеча вверх"], cfg)
                if s:
                    out.append(s)
                    break

    # --- пробой коридора / наклонки
    for br in brk:
        up = br["side"] == "long"
        is_box = "проторговки" in br["label"] or "консолидации" in br["label"]
        is_tl = "наклонки" in br["label"]
        if is_box and "box_breakout" in enabled and (tr == (1 if up else -1) or (tr == 0 and spike)):
            box = next((x for x in c.out.get("range", []) + c.out.get("consolidation", [])
                        if abs((x["p2"] if up else x["p1"]) - br["meta"]["level"]) < 1e-12), None)
            mid = (box["p1"] + box["p2"]) / 2 if box else br["meta"]["level"]
            stop = (mid - pad) if up else (mid + pad)
            s = _signal(c, "box_breakout", br["side"], stop, [tr_txt, br["label"]] + (["всплеск объёма"] if spike else []), cfg)
            if s:
                out.append(s)
        if is_tl and "trendline_break" in enabled and tr != (-1 if up else 1):
            lows = [i for i in c.pivots(3)[1 if up else 0] if i < j][-1:]
            ext = (b[lows[0]]["l"] - pad) if (up and lows) else (b[lows[0]]["h"] + pad) if lows else (last["l"] - pad if up else last["h"] + pad)
            s = _signal(c, "trendline_break", br["side"], ext, [tr_txt, br["label"]], cfg)
            if s:
                out.append(s)

    # --- подсказки «ищу вход»
    if not out:
        close = last["c"]
        for lo, hi, lab in _zones(c):
            if tr == -1 and lo > close and lo - close <= cfg["near_atr"] * atr:
                out.append({"kind": "alert", "side": "short", "t": c.end_t(), "text": f"{tr_txt}: цена подходит к {lab} ({lo:g}) снизу - ищу шорт, жду разворотную свечу"})
                break
            if tr == 1 and hi < close and close - hi <= cfg["near_atr"] * atr:
                out.append({"kind": "alert", "side": "long", "t": c.end_t(), "text": f"{tr_txt}: цена подходит к {lab} ({hi:g}) сверху - ищу лонг, жду разворотную свечу"})
                break
        for box in c.out.get("consolidation", []):
            if (box.get("meta") or {}).get("open"):
                out.append({"kind": "alert", "side": None, "t": c.end_t(),
                            "text": f"сжатие {box['p1']:g}-{box['p2']:g} ({box['score']:.0f} свечей) - скоро выход, жду пробой" + (f" по тренду ({tr_txt})" if tr else "")})
                break
        for lq in c.out.get("liquidity", []):
            if lq["kind"] == "hline" and lq["t2"] == c.end_t() and abs(lq["p1"] - close) <= 1.0 * atr:
                out.append({"kind": "alert", "side": lq["side"], "t": c.end_t(), "text": f"рядом {lq['label']} на {lq['p1']:g} - возможен вынос стопов и разворот"})
                break
    return out
