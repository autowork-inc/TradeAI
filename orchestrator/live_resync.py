"""Ретроспективная сверка закрытых боевых сделок с Binance (2026-10-18).

Прошлые сделки, закрытые вручную на бирже, остались в БД с `exit_reason='unknown'` и/или `gap_affected=true` (значок «Закрыта ⚠️», вне статистики).
При старте боевого контура каждая такая сделка (`mode='live'`, `status='closed'`) сверяется с фактическими исполнениями счёта (`GET /fapi/v1/userTrades`):
  * закрытия с известным источником цены (биржа / триггер) остаются как есть;
  * неизвестный остаток объёма закрывается по закрывающим исполнениям биржи ПОСЛЕ уже учтённых (самые ранние исполнения принадлежат записанным закрытиям):
    средневзвешенная цена выхода, фактическая комиссия выхода (если в USDT), PnL и net R пересчитываются;
  * причина выхода -> `manual_binance`, `gap_affected` -> false, сделка возвращается в статистику; в `plan.resync` пишется результат.
Исполнений не хватило (старше хранения биржи, неполная выборка) - сделка не меняется, в `plan.resync` пишется попытка (повторно не запрашивается).
Теневые (`shadow`) сделки не трогаются: у них нет исполнений на бирже.

Единственные перехватываемые ошибки - ожидаемые отказы биржи/данных по одной сделке (`BinanceAPIError`, `DataError`): сделка пропускается с предупреждением,
остальные обрабатываются; всё прочее роняет процесс (R1)."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import config
from execution.rest import BinanceAPIError
from orchestrator.live_engine import FILLS_MAX, LEG_SOURCES, LIVE_ID_SUFFIX, MANUAL_EXIT
from orchestrator.types import Direction, Kind
from persistence.recorder import DbOpenTrade, Row
from utils.errors import DataError

log = logging.getLogger("trading.live_resync")
_EPS = 1e-9
WINDOW_AFTER_EXIT_MS = 10 * 60_000  # исполнения ищутся до «момента закрытия по БД» + 10 минут (БД знает время, когда бот увидел закрытие, а не исполнение)


class ResyncStore(Protocol):
    async def replace_closes(self, trade_row: Row, remove_seqs: Sequence[int], add: Sequence[Row]) -> None:
        """Одной транзакцией: удалить закрытия с указанными seq, добавить новые, записать итоговую строку сделки."""
        ...

    async def mark_attempt(self, trade_row: Row) -> None:
        """Записать строку сделки (только `plan.resync` изменился): повторно такая сделка не запрашивается."""
        ...


@dataclass
class ResyncReport:
    fixed: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (id, причина)


def _sources(plan: dict[str, Any], closes: Sequence[Row]) -> list[str]:
    """Источник цены каждого закрытия (по порядку seq). Нет сведений -> по причине: `unknown` - неизвестно, прочее - считается подтверждённым."""
    listed = list(plan.get("exit_price_sources") or [])
    return [listed[i] if i < len(listed) else ("unknown" if c.get("reason") == "unknown" else "trigger") for i, c in enumerate(closes)]


async def resync_closed_trades(client: Any, store: ResyncStore, candidates: Sequence[DbOpenTrade], *, now_ms: int, post_only: bool | None = None,
                               maker: float | None = None, taker: float | None = None) -> ResyncReport:
    """`candidates` - закрытые боевые сделки (`persistence.pg.fetch_resync_candidates`). `client` - `BinanceLiveClient` (нужен `user_trades`)."""
    post_only = config.LIVE_POST_ONLY if post_only is None else post_only
    maker = config.LIVE_MAKER_FEE_RATE if maker is None else maker
    taker = config.LIVE_TAKER_FEE_RATE if taker is None else taker
    report = ResyncReport()
    for cand in candidates:
        row = dict(cand.row)
        tid = str(row["id"])
        if row.get("mode") != "live" or row.get("status") != "closed" or not tid.endswith(LIVE_ID_SUFFIX):
            raise DataError(f"Ретро-сверка: строка {tid!r} не закрытая боевая (mode={row.get('mode')!r}, status={row.get('status')!r})")
        try:
            done = await _resync_one(client, store, cand, row, now_ms, post_only, maker, taker)
        except (BinanceAPIError, DataError) as exc:
            log.warning("Ретро-сверка %s: пропуск (%s)", tid, exc)
            report.skipped.append((tid, f"error: {exc}"))
            continue
        if done is True:
            report.fixed.append(tid)
        else:
            report.skipped.append((tid, str(done)))
    log.info("Ретро-сверка с Binance: исправлено %d, пропущено %d %s", len(report.fixed), len(report.skipped), report.skipped[:10])
    return report


async def _resync_one(client: Any, store: ResyncStore, cand: DbOpenTrade, row: Row, now_ms: int, post_only: bool, maker: float, taker: float) -> bool | str:
    tid, symbol = str(row["id"]), str(row["coin"])
    long = row["side"] == Direction.LONG.value
    side = 1.0 if long else -1.0
    qty, entry, ra = float(row["qty"]), float(row["entry_price"]), row.get("risk_amount")
    plan = dict(row.get("plan") or {})
    closes = sorted(cand.closes, key=lambda c: int(c["seq"]))
    sources = _sources(plan, closes)
    good = [(c, s) for c, s in zip(closes, sources) if s != "unknown"]
    bad_seqs = [int(c["seq"]) for c, s in zip(closes, sources) if s == "unknown"]
    good_qty = sum(float(c["qty"]) for c, _ in good)
    rest = qty - good_qty
    if rest <= _EPS:
        return "no_unknown_remainder"  # все закрытия с известной ценой: нечего пересчитывать (флаг имеет другую причину)
    since = int(row.get("entry_time") or row["signal_time"])
    until = min(now_ms, int(row.get("exit_time") or since) + WINDOW_AFTER_EXIT_MS)
    close_side = "SELL" if long else "BUY"
    skip, got, notional, commission, ts, usdt, used = good_qty, 0.0, 0.0, 0.0, since, True, []
    for u in await client.user_trades(symbol, since, max(until, since)):
        if u.side != close_side or got >= rest - _EPS:
            continue
        q = u.qty
        if skip > _EPS:  # самые ранние закрывающие исполнения принадлежат уже учтённым закрытиям
            cut = min(skip, q)
            skip, q = skip - cut, q - cut
            if q <= _EPS:
                continue
        q = min(q, rest - got)
        got, notional, commission, ts = got + q, notional + q * u.price, commission + u.commission * q / u.qty, u.time_ms
        usdt = usdt and u.commission_asset in ("USDT", "")
        used.append({"ts": u.time_ms, "side": u.side, "qty": q, "price": u.price, "fee": u.commission, "fee_asset": u.commission_asset, "role": "exit"})
    if got < rest - _EPS:
        attempt = {**plan, "resync": {"status": "incomplete", "at": now_ms, "found_qty": got, "need_qty": rest}}
        await store.mark_attempt({**row, "plan": attempt})
        return f"fills_incomplete {got:g}/{rest:g}"
    price = notional / got
    fee_out = commission if usdt else taker * price * rest  # комиссия в BNB в USDT не пересчитывается: оценка по ставке
    fee_in = (maker if row["kind"] == Kind.LIMIT.value and post_only else taker) * entry * rest
    gross = side * (price - entry) * rest
    fees = fee_in + fee_out
    net = gross - fees
    seq = max((int(c["seq"]) for c in closes), default=0) + 1
    leg = {"trade_id": tid, "seq": seq, "ts": ts, "reason": MANUAL_EXIT, "frac": rest / qty, "qty": rest, "price": price,
           "gross_r": gross / ra if ra else 0.0, "fees_r": fees / ra if ra else 0.0, "net_r": net / ra if ra else 0.0, "pnl": net, "fees": fees}
    src_all = [*(s for _, s in good), "exchange"]
    plan.update(exit_price_sources=src_all, exit_price_source=next(s for s in LEG_SOURCES if s in src_all),
                exit_fee_from_exchange=bool(usdt or plan.get("exit_fee_from_exchange")), fills=[*(plan.get("fills") or []), *used][:FILLS_MAX],
                resync={"status": "done", "at": now_ms, "fills": len(used), "was_exit_reason": row.get("exit_reason"), "was_gap_affected": bool(row.get("gap_affected"))})
    pnl = sum(float(c["pnl"] or 0.0) for c, _ in good) + net
    legs = [(float(c["qty"]), float(c["price"])) for c, _ in good] + [(rest, price)]
    row.update(plan=plan, exit_reason=MANUAL_EXIT, gap_affected=False, status="closed", remaining_frac=0.0, stop_price=None,
               exit_time=max([ts, *(int(c["ts"]) for c, _ in good)]), exit_price=sum(q * p for q, p in legs) / sum(q for q, _ in legs), pnl=pnl,
               fees=sum(float(c["fees"] or 0.0) for c, _ in good) + fees, r_multiple=pnl / ra if ra else None,
               gross_r=(sum(float(c["gross_r"] or 0.0) for c, _ in good) + gross / ra) if ra else None)
    await store.replace_closes(row, bad_seqs, [leg])
    return True
