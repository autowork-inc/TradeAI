"""Движок Playground: стратегии бота на истории, свеча за свечой, тем же кодом, что Shadow (R8), без заглядывания в будущее.

Как в живом Shadow (scripts/shadow_run.py), только время идёт по истории:
  * свечи рабочих таймфреймов (15m, 5m у SMC_Pro_Trend) и старших (1h, 4h, 1d) собраны из минуток (playground.history.resample);
  * на каждой границе минуты T: сначала закрытия старших таймфреймов -> `MtfState.update`, затем рабочих -> `MtfState.attach` -> `Orchestrator.on_context`.
    Контекст строится из окна ПОСЛЕДНИХ закрытых свечей (как `SmcKlineFeed`: 300 / 90 баров) - стратегия видит только то, что уже закрылось (R3, R5);
  * между закрытиями минутка T превращается в тики (`VirtualTracker.replay_minute`): open, неблагоприятный экстремум, благоприятный, close -
    исполнение MARKET, лимиток, стопа и тейков; на одном тике стоп раньше тейка (пессимистично);
  * фильтр MARKET-входа на импульсе (MarketEntryGuard) получает те же 1m-свечи; новости не учитываются (календаря на год назад нет), выходные в Shadow не блокируются;
  * риск $5, виртуальный эквити 10 000, планы выхода - текущие (`registry.plans_for`: Scale Out 50% на +1R + финальный тейк, CHR 1:4, VWAP - линия VWAP);
    финальный тейк авто-TP можно задать явно (`tp_r`), иначе - значение по умолчанию.

OBI_Breakout не прогоняется: истории стакана и ленты нет. Монета - крипта Binance (Forex/металлы без минуток).
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

import polars as pl

import config
from features import defaults as D
from features.smc import SmcParams
from orchestrator.auto_tp import scale_plan
from orchestrator.core import Orchestrator, Route
from orchestrator.feature_engine import FeatureEngine
from orchestrator.journal import MemoryEventLog
from orchestrator.minute_bars import MarketEntryGuard, MinuteBars
from features.mtf import D1_BIAS
from orchestrator.mtf import MtfState
from orchestrator.multi_strategies import OBI_BREAKOUT
from orchestrator.registry import AUTO_TP_SKIP, SHADOW_STRATEGIES, build_strategies, ensure_history_depth, plans_for, strategy_allowed
from orchestrator.strategies import SMC_PRO_TREND
from orchestrator.tracker import VirtualTracker
from orchestrator.types import FeatureContext, Mode, Tick
from persistence.recorder import MemoryRecorder, state_event_row
from playground.history import MINUTE_MS, resample
from utils.errors import ConfigError, DataError

log = logging.getLogger("playground.engine")

PLAYGROUND_STRATEGIES = tuple(s for s in SHADOW_STRATEGIES if s != OBI_BREAKOUT)  # OBI: нет истории стакана и ленты
WORK_INTERVAL = "15m"
HISTORY = {"15m": 300, "5m": 300, "1h": D.MTF_STRUCT_HISTORY_BARS, "4h": D.HTF_HISTORY_BARS, "1d": D.MTF_BIAS_HISTORY_BARS}
D1_MIN_BARS = 60  # как scripts/shadow_run.D1_MIN_BARS
WARMUP_MS = 95 * 86_400_000  # 1d-bias: 90 суток истории до первого бара + запас
RECEIVE_DELAY_MS = 1_000  # событие закрытия свечи приходит через ~секунду (сигнал свежий: < 30 с)
TRADE_EVENTS = ("signal_accepted", "trade_registered", "market_filled", "limit_filled", "partial_close", "trade_closed", "trade_cancelled",
                "skipped_stale", "skipped_filter", "skipped_due_to_lock", "skipped_risk", "tagged_news")


@dataclass(frozen=True)
class RunSpec:
    symbol: str
    strategies: tuple[str, ...]
    start_ms: int
    end_ms: int
    tp_r: Mapping[str, float] = field(default_factory=dict)  # финальный тейк Scale Out по стратегиям (как подобрал бы авто-TP); пусто - по умолчанию

    def validate(self) -> None:
        if not self.strategies:
            raise ConfigError("Playground: не выбрано ни одной стратегии")
        bad = [s for s in self.strategies if s not in PLAYGROUND_STRATEGIES]
        if bad:
            raise ConfigError(f"Playground: стратегии {bad} не прогоняются (OBI_Breakout - нет истории стакана); доступны {list(PLAYGROUND_STRATEGIES)}")
        if not self.symbol.endswith("USDT"):
            raise ConfigError(f"Playground: только крипта Binance (USDT), получено {self.symbol}")
        if not self.end_ms > self.start_ms:
            raise ConfigError("Playground: конец периода раньше начала")


@dataclass
class RunResult:
    trades: list[dict[str, Any]]  # строки как в таблице trades БД (MemoryRecorder), итоговое состояние
    closes: dict[str, list[dict[str, Any]]]
    snapshots: dict[str, dict[str, Any]]
    events: dict[str, list[dict[str, Any]]]  # по trade_id: хронология (сигнал, исполнение, закрытия)
    skipped: list[dict[str, Any]]  # сигналы, не ставшие сделками: стратегия, время, причина
    bars: int
    seconds: float


class _Frames:
    """Свечи таймфрейма + окно последних N закрытых на момент закрытия бара (как SmcKlineFeed._context)."""

    def __init__(self, tf: str, minutes: pl.DataFrame):
        self.tf, self.ms = tf, config.INTERVAL_MS[tf]
        self.df = resample(minutes, self.ms)
        if self.df.is_empty():
            raise DataError(f"Playground: нет свечей {tf}")
        self.first = int(self.df["open_time"][0])
        self.n = HISTORY[tf]
        self.engine = FeatureEngine(SmcParams(), min_bars=D1_MIN_BARS) if tf == "1d" else FeatureEngine(SmcParams())

    def index_closing_at(self, t_ms: int) -> int:
        """Индекс свечи, закрывшейся ровно в t_ms (или последней закрытой к t_ms)."""
        return (t_ms - self.ms - self.first) // self.ms

    def context(self, symbol: str, idx: int, received_ms: int) -> FeatureContext:
        if idx < 0 or idx >= self.df.height:
            raise DataError(f"Playground: {symbol} {self.tf}: нет свечи #{idx} (история короче периода)")
        lo = max(0, idx - self.n + 1)
        window = self.df.slice(lo, idx - lo + 1)
        return self.engine.build(symbol, self.tf, self.ms, window, received_ms)


def _plans(names: Sequence[str], tp_r: Mapping[str, float]):
    plans = dict(plans_for(names))
    for name, r in tp_r.items():
        if name in plans and name not in AUTO_TP_SKIP:
            plans[name] = scale_plan(r)
    return plans


def simulate(spec: RunSpec, minutes: pl.DataFrame, on_progress: Callable[[float], None] | None = None) -> RunResult:
    """Прогон стратегий `spec` по минуткам (с прогревом до spec.start_ms не короче WARMUP_MS). Чистая функция: сеть и БД не трогает."""
    spec.validate()
    began = time.monotonic()
    names = tuple(spec.strategies)
    mtf = SMC_PRO_TREND in names
    legacy = any(n != SMC_PRO_TREND for n in names)
    events = MemoryEventLog()
    rec = MemoryRecorder()
    strategies = build_strategies(names, mtf=mtf, interval=WORK_INTERVAL)
    ensure_history_depth(strategies, HISTORY[WORK_INTERVAL], WORK_INTERVAL)
    tracker = VirtualTracker(events, equity=10_000.0, recorder=rec, plans=_plans(names, spec.tp_r))
    bars_1m = MinuteBars()
    orch = Orchestrator(strategies, {s.name: Route(Mode.SHADOW, tracker) for s in strategies}, events, recorder=rec,
                        signal_gates=[MarketEntryGuard(bars_1m)], symbol_filter=strategy_allowed,
                        trend_column=D1_BIAS if D.TREND_ONLY else None)  # как в боте: только по тренду дневки (2026-10-26)
    htf = MtfState(config.INTERVAL_MS, events, base=D.MTF_BASE_INTERVAL if mtf else None, struct=D.MTF_STRUCT_INTERVAL, bias=D.MTF_BIAS_INTERVAL,
                   legacy=WORK_INTERVAL if legacy else None, legacy_bias=D.HTF_INTERVAL)
    state_tfs = list(htf.state_intervals)
    work_tfs = ([WORK_INTERVAL] if legacy else []) + ([D.MTF_BASE_INTERVAL] if mtf else [])
    frames = {tf: _Frames(tf, minutes) for tf in (*state_tfs, *work_tfs)}
    sym = spec.symbol

    if int(minutes["open_time"][0]) > spec.start_ms - WARMUP_MS + 86_400_000:
        raise DataError(f"Playground: {sym}: история начинается {int(minutes['open_time'][0])}, нужен прогрев с {spec.start_ms - WARMUP_MS}")
    for tf in state_tfs:  # состояние старших таймфреймов на начало периода: последняя закрытая свеча
        f = frames[tf]
        htf.update(f.context(sym, f.index_closing_at(spec.start_ms - spec.start_ms % f.ms), spec.start_ms))

    run = minutes.filter((pl.col("open_time") >= spec.start_ms) & (pl.col("open_time") < spec.end_ms))
    T, O, H, L, C = (run[c].to_list() for c in ("open_time", "open", "high", "low", "close"))
    total, step = len(T), max(1, len(T) // 200)
    bars = 0
    for i, t in enumerate(T):
        bars_1m.on_tick(Tick(sym, O[i], t, "trade"))  # первый тик новой минуты закрывает прошлую 1m-свечу (как в живом потоке к моменту события закрытия)
        for tf in state_tfs:
            f = frames[tf]
            if t % f.ms == 0:
                htf.update(f.context(sym, f.index_closing_at(t), t + RECEIVE_DELAY_MS))
        for tf in work_tfs:
            f = frames[tf]
            if t % f.ms == 0 and t > spec.start_ms:
                orch.on_context(htf.attach(f.context(sym, f.index_closing_at(t), t + RECEIVE_DELAY_MS)))
                bars += 1
        tracker.replay_minute(sym, t, O[i], H[i], L[i], C[i])
        for price in (H[i], L[i], C[i]):
            bars_1m.on_tick(Tick(sym, price, t + MINUTE_MS - 1, "trade"))
        if on_progress is not None and i % step == 0:
            on_progress(i / total)

    by_trade: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped: list[dict[str, Any]] = []
    setup: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)  # (стратегия, сторона) -> переходы автомата S0..S4 (SMC)
    for e in events.events:
        if e.kind == "setup_state":
            setup[(e.strategy, str(e.detail.get("side")))].append(state_event_row(e))
            continue
        if e.signal_id is None or e.kind not in TRADE_EVENTS:
            continue
        row = state_event_row(e)  # тот же формат, что state_events БД бота: клиент рисует хронологию как у живых сделок
        if e.kind.startswith("skipped_"):
            skipped.append(row)
        else:
            by_trade[e.signal_id].append(row)
    for t in rec.trades.values():  # путь автомата к сигналу (как SETUP_PATH_SQL API): последние 12 переходов стороны сделки до сигнала
        path = [x for x in setup.get((t["strategy_id"], t["side"]), []) if x["ts"] <= t["signal_time"] + MINUTE_MS]
        if path:
            by_trade[t["id"]] = [*path[-12:], *by_trade[t["id"]]]
    closes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in rec.closes:
        closes[c["trade_id"]].append(c)
    return RunResult(trades=list(rec.trades.values()), closes=dict(closes), snapshots={s["trade_id"]: s for s in rec.snapshots}, events=dict(by_trade),
                     skipped=skipped, bars=bars, seconds=time.monotonic() - began)


# ------------------------------------------------------------------------------------------ итоги

def summarize(trades: Sequence[Mapping[str, Any]], skipped: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Итоги по стратегиям и общий: сделки (закрытые), победы (net R > 0), винрейт, сумма R и $, Profit Factor, средний R, макс. просадка в R, причины выхода."""
    groups: dict[str, list] = defaultdict(list)
    for t in trades:
        groups[t["strategy_id"]].append(t)
    out = {name: _stats(rows) for name, rows in sorted(groups.items())}
    for s in skipped:
        key = s["kind"] if s["kind"] != "skipped_filter" else f"skipped_filter:{(s.get('detail') or {}).get('reason')}"
        bucket = out.setdefault(s["strategy"], _stats([]))["skipped"]
        bucket[key] = bucket.get(key, 0) + 1
    return {"by_strategy": out, "overall": _stats(list(trades))}


def _stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    closed = sorted((t for t in rows if t["status"] == "closed" and t.get("r_multiple") is not None), key=lambda t: t["exit_time"] or 0)
    rs = [float(t["r_multiple"]) for t in closed]
    wins = [r for r in rs if r > 0]
    loss = -sum(r for r in rs if r < 0)
    peak = cum = dd = 0.0
    curve = []
    pnl_cum = 0.0
    for t, r in zip(closed, rs):
        cum += r
        pnl_cum += float(t.get("pnl") or 0.0)
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
        curve.append([t["exit_time"], round(cum, 4), round(pnl_cum, 2)])
    reasons: dict[str, int] = defaultdict(int)
    for t in closed:
        reasons[t.get("exit_reason") or "?"] += 1
    return {
        "signals": len(rows), "closed": len(closed), "wins": len(wins), "losses": len(rs) - len(wins),
        "winrate": len(wins) / len(rs) if rs else None, "sum_r": sum(rs), "avg_r": sum(rs) / len(rs) if rs else None,
        "pnl": sum(float(t.get("pnl") or 0.0) for t in closed), "profit_factor": (sum(wins) / loss) if loss > 0 else None,
        "max_dd_r": dd, "open": sum(1 for t in rows if t["status"] in ("open", "pending_limit", "pending_market")),
        "cancelled": sum(1 for t in rows if t["status"] == "cancelled"), "exit_reasons": dict(reasons), "skipped": {}, "curve": curve,
    }
