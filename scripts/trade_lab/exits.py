"""Симуляция планов выхода по MFE, записанному трекером по тикам (без угадывания порядка событий).

Почему это точно. Трекер обновляет mfe_r на каждом тике, пока позиция открыта, а сделка закрывается при первом из событий: стоп / тейк / тайм-аут
(до 2026-10-19 ещё безубыток и трейлинг). Значит, всё, что вошло в mfe_r, случилось ДО фактического выхода:
  * уровень a <= MFE был бы взят раньше фактического выхода;
  * если сделку выбило исходным стопом, а MFE >= b, цена после +b R прошла через вход - безубыток на b точно сработал бы (остаток = 0R);
  * если уровень выше MFE, остаток доживает так же, как фактическая сделка (стоп / тайм-аут по той же цене).
Неизвестно только то, что было бы ПОСЛЕ фактического закрытия: старая сделка закрылась безубытком/трейлингом/вручную раньше, чем закрылся бы
проверяемый план, или уровень плана выше фактического тейка. Такие результаты - None («неизвестно»), они считаются отдельно, не угадываются.
Тайм-аут после взведённого безубытка с ценой закрытия выше входа неоднозначен (мог сработать безубыток) - берётся 0R (пессимистично, approx).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Iterable

from .data import Trade

EPS = 1e-9
STOP_REASONS = ("sl",)
TIMEOUT_REASONS = ("timeout",)
TP_REASONS = ("tp",)


@dataclass(frozen=True)
class Plan:
    name: str
    levels: tuple[tuple[float, float], ...]  # (R, доля исходного объёма), по возрастанию R; доли в сумме <= 1
    be: float | None = None  # безубыток после MFE >= be (R); None - стоп статичный

    def __post_init__(self) -> None:
        if not self.levels or abs(sum(f for _, f in self.levels) - 1.0) > 1e-6:
            raise ValueError(f"{self.name}: доли уровней должны давать 1")
        if list(self.levels) != sorted(self.levels):
            raise ValueError(f"{self.name}: уровни по возрастанию R")


def tp(x: float) -> Plan:
    return Plan(f"TP 1:{x:g}", ((x, 1.0),))


CURRENT = tp(4.0)
OLD_PLAN = Plan("Старый: 50% 1R + BE + 50% 3R", ((1.0, 0.5), (3.0, 0.5)), be=1.0)
TP_GRID = (1.0, 1.5, 2.0, 2.5, 3.0, 4.0)
SCAN_GRID = tuple(x / 4 for x in range(2, 17))  # 0.5 ... 4.0 шагом 0.25


def plan_grid() -> list[Plan]:
    plans = [tp(x) for x in TP_GRID]
    for b in (0.5, 1.0):
        plans += [Plan(f"BE {b:g}R + TP 1:{x:g}", ((x, 1.0),), be=b) for x in (2.0, 3.0, 4.0)]
    plans += [Plan(f"50% 1R + BE + 50% {x:g}R", ((1.0, 0.5), (x, 0.5)), be=1.0) for x in (2.0, 4.0)]
    plans += [OLD_PLAN, Plan("50% 1.5R + BE + 50% 3R", ((1.5, 0.5), (3.0, 0.5)), be=1.5)]
    return plans


def exit_class(t: Trade) -> str:
    """Как фактически закончилась сделка: stop (исходный стоп), tp, timeout, early (безубыток / трейлинг / вручную / прочее)."""
    reason = (t.final_close or {}).get("reason") or t.exit_reason or ""
    if reason in TP_REASONS:
        return "tp"
    if reason in TIMEOUT_REASONS:
        return "timeout"
    if reason in STOP_REASONS and not t.be_armed:
        return "stop"
    return "early"


@dataclass(frozen=True)
class SimResult:
    net_r: float | None  # None - неизвестно (путь после фактического закрытия не наблюдался)
    hit_all: bool  # взяты все уровни плана
    approx: bool  # пессимистичное допущение (тайм-аут после безубытка)


def simulate(t: Trade, plan: Plan) -> SimResult:
    assert t.entry is not None and t.risk and t.mfe is not None
    mfe, cls, r_final = max(t.mfe, 0.0), exit_class(t), t.final_r_gross
    if r_final is None:
        r_final = t.actual_r if t.actual_r is not None else -1.0
    fees = t.fee_in * t.entry / t.risk
    gross, remaining = 0.0, 1.0

    def exit_fee(r: float, frac: float) -> float:
        return frac * t.fee_out * (t.entry + t.dir * r * t.risk) / t.risk

    for a, f in plan.levels:
        if mfe + EPS < a:
            break
        gross += f * a
        fees += exit_fee(a, f)
        remaining -= f
    if remaining <= EPS:
        return SimResult(gross - fees, True, False)
    armed = plan.be is not None and mfe + EPS >= plan.be
    approx = False
    if cls == "stop":
        rest = 0.0 if armed else r_final
    elif cls == "timeout":
        if not armed:
            rest = r_final
        else:
            rest, approx = 0.0, r_final > 0
    elif cls == "early" and armed and r_final <= 0.05:
        rest = 0.0  # цена вернулась ко входу: безубыток плана тоже сработал бы
    else:
        return SimResult(None, False, False)  # tp: уровень плана выше фактического тейка; early: путь после закрытия неизвестен
    gross += remaining * rest
    fees += exit_fee(rest, remaining)
    return SimResult(gross - fees, False, approx)


# ---------------------------------------------------------------------------------------------- метрики

@dataclass(frozen=True)
class Stats:
    n: int
    known: int
    unknown: int
    approx: int
    hit_rate: float | None
    win_rate: float | None
    sum_r: float
    mean_r: float | None
    usd: float
    pf: float | None
    mdd_r: float


def stats_of(pairs: list[tuple[Trade, SimResult]]) -> Stats:
    """pairs - в хронологическом порядке (просадка считается по накопленному R)."""
    known = [(t, s) for t, s in pairs if s.net_r is not None]
    rs = [s.net_r for _, s in known]
    wins, losses = sum(r for r in rs if r > 0), -sum(r for r in rs if r < 0)
    peak = cum = mdd = 0.0
    for r in rs:
        cum += r
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
    n = len(rs)
    return Stats(
        n=len(pairs), known=n, unknown=len(pairs) - n, approx=sum(1 for _, s in known if s.approx),
        hit_rate=sum(1 for _, s in known if s.hit_all) / n if n else None, win_rate=sum(1 for r in rs if r > 0) / n if n else None,
        sum_r=sum(rs), mean_r=sum(rs) / n if n else None, usd=sum(s.net_r * t.risk_usd for t, s in known),
        pf=wins / losses if losses > 0 else None, mdd_r=mdd)


def actual_stats(trades: list[Trade]) -> Stats:
    """Факт (r_multiple) в той же форме - для строки «Факт» рядом с симуляцией."""
    return stats_of([(t, SimResult(t.actual_r, (t.final_close or {}).get("reason") == "tp", False)) for t in trades])


def run(trades: list[Trade], plan: Plan) -> list[tuple[Trade, SimResult]]:
    return [(t, simulate(t, plan)) for t in sorted(trades, key=lambda x: x.signal_ms)]


def best_tp(trades: list[Trade], grid: Iterable[float] = SCAN_GRID) -> tuple[float, Stats] | None:
    best: tuple[float, Stats] | None = None
    for x in grid:
        st = stats_of(run(trades, tp(x)))
        if st.mean_r is not None and (best is None or st.mean_r > (best[1].mean_r or float("-inf"))):
            best = (x, st)
    return best


def walk_forward(trades: list[Trade], grid: Iterable[float] = SCAN_GRID) -> dict[str, float | None] | None:
    """Лучший тейк выбирается по первой половине (по времени сигнала), проверяется на второй - против 1:4 там же."""
    ordered = sorted(trades, key=lambda x: x.signal_ms)
    if len(ordered) < 10:
        return None
    first, second = ordered[: len(ordered) // 2], ordered[len(ordered) // 2:]
    chosen = best_tp(first, grid)
    if chosen is None:
        return None
    x = chosen[0]
    return {"x": x, "train_mean": chosen[1].mean_r, "test_mean": stats_of(run(second, tp(x))).mean_r,
            "test_current": stats_of(run(second, CURRENT)).mean_r, "n_train": len(first), "n_test": len(second)}


def bootstrap_diff(trades: list[Trade], a: Plan, b: Plan, iters: int = 2000, seed: int = 7) -> tuple[float, float, float] | None:
    """Разница ожиданий (a - b) на одних и тех же сделках: (среднее, 5-й, 95-й перцентиль). Детерминированно (seed)."""
    pairs = [(simulate(t, a).net_r, simulate(t, b).net_r) for t in trades]
    diffs = [x - y for x, y in pairs if x is not None and y is not None]
    if len(diffs) < 10:
        return None
    rng, n = random.Random(seed), len(diffs)
    means = sorted(sum(diffs[rng.randrange(n)] for _ in range(n)) / n for _ in range(iters))
    return sum(diffs) / n, means[int(0.05 * iters)], means[int(0.95 * iters) - 1]


def mfe_profile(trades: list[Trade], thresholds: Iterable[float] = (0.5, 1.0, 1.5, 2.0, 2.5, 3.0)) -> dict[str, object]:
    vals = sorted(max(t.mfe or 0.0, 0.0) for t in trades)
    n = len(vals)
    if not n:
        return {"n": 0}

    def q(p: float) -> float:
        return vals[min(n - 1, int(p * (n - 1) + 0.5))]

    return {"n": n, "p25": q(0.25), "p50": q(0.5), "p75": q(0.75), "p90": q(0.9),
            "reach": {x: sum(1 for v in vals if v + EPS >= x) / n for x in thresholds}}


def by(trades: list[Trade], key: Callable[[Trade], str]) -> dict[str, list[Trade]]:
    out: dict[str, list[Trade]] = {}
    for t in trades:
        out.setdefault(key(t), []).append(t)
    return dict(sorted(out.items()))
