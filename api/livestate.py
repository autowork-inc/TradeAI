"""Живое состояние для Dashboard и WebSocket: автомат сетапа SMC (S0..S4) по монетам и сторонам, открытые заявки/позиции, лента событий.

Источник - только БД (`state_events`, `trades`): Оркестратор пишет переход автомата при каждом ИЗМЕНЕНИИ состояния
(`orchestrator/core.py::_log_setup_states`), поэтому текущее состояние = последний переход по (монета, стратегия, сторона); монета без
переходов в окне - S0. Окно (`state_window_days`) ограничивает выборку: сетап живёт максимум часы (срок 30 баров 15m), старее не нужно.

Один фоновый опрос на процесс (`LiveStateHub`), а не по запросу на клиента: сколько бы окон ни было открыто, БД читается раз в
`poll_interval_s`. Клиентам состояние уходит при изменении и не реже `keepalive_s` (в нём же «возраст пульса» shadow-run).
"""

import asyncio
import json
import logging
import re
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg

from api.serialize import DataIntegrityError, event_out, iso, trade_out
from api.settings import Settings

log = logging.getLogger("api.livestate")

SETUP_STRATEGIES = ("SMC_Pro_Trend",)  # стратегии с автоматом состояний S0..S4; у остальных решение на одной свече - только сделки
SIDES = ("short", "long")
SETUP_LABELS = {0: "нет сетапа", 1: "свип ликвидности", 2: "свип + FVG сформирован", 3: "CHoCH отклонён", 4: "сигнал: LIMIT на 50% FVG"}
REJECT_LABELS = {"not_passive": "лимитка не пассивна (GTX отвергла бы)", "exposure": "плечо выше потолка", "bad_risk": "некорректный риск"}
HEARTBEAT_STALE_S = 120.0  # то же порог, что у healthcheck shadow-run
_STATE_RE = re.compile(r"^(long|short):S([0-4])$")

SETUP_SQL = """
SELECT DISTINCT ON (coin, strategy, side) coin, strategy, side, to_state, ts, reason, detail
  FROM (SELECT coin, strategy, split_part(to_state, ':', 1) AS side, to_state, ts, reason, detail, id
          FROM state_events
         WHERE kind = 'setup_state' AND ts > now() - make_interval(days => $1)) e
 ORDER BY coin, strategy, side, ts DESC, id DESC
"""
POSITIONS_SQL = """
SELECT id, strategy_name, strategy_id, coin, side, kind, mode, status, qty, remaining_frac, signal_time, entry_time, exit_time, planned_entry,
       entry_price, exit_price, sl, tp, stop_price, risk, risk_amount, pnl, fees, r_multiple, gross_r, mfe_r, mae_r, exit_reason,
       be_armed, gap_affected
  FROM trades
 WHERE mode = $1 AND status IN ('pending_limit', 'pending_market', 'open')
 ORDER BY signal_time DESC
"""
EVENTS_SQL = """
SELECT ts, coin, strategy, strategy_id, signal_id, from_state, to_state, kind, reason, detail
  FROM state_events
 ORDER BY ts DESC, id DESC
 LIMIT $1
"""
EVENTS_LIMIT = 30


def heartbeat_age_s(data_dir: Path, now: float | None = None) -> float | None:
    """Секунд с последнего «пульса» shadow-run; None - файла нет (каталог не смонтирован или Shadow ни разу не стартовал)."""
    path = data_dir / ".shadow_heartbeat"
    if not path.exists():
        return None
    return max(0.0, (time.time() if now is None else now) - path.stat().st_mtime)


def freshest_heartbeat_age_s(dirs: "list[Path | None]", now: float | None = None) -> float | None:
    """Самый свежий «пульс» среди каталогов процесса (shadow-run и shadow-live пишут в РАЗНЫЕ каталоги; работает один из них)."""
    ages = [a for a in (heartbeat_age_s(d, now) for d in dirs if d is not None) if a is not None]
    return min(ages) if ages else None


def parse_setup_row(row: Any) -> dict[str, Any]:
    m = _STATE_RE.match(row["to_state"] or "")
    if m is None or m.group(1) != row["side"]:
        raise DataIntegrityError(f"state_events: нечитаемое состояние автомата {row['to_state']!r} (сторона {row['side']!r})")
    state = int(m.group(2))
    detail = row["detail"] or {}
    reject = detail.get("reject")
    return {"strategy": row["strategy"], "side": row["side"], "state": state, "label": SETUP_LABELS[state],
            "reject": reject, "reject_label": REJECT_LABELS.get(reject, reject) if reject else None, "since": iso(row["ts"])}


def position_out(row: Any) -> dict[str, Any]:
    return trade_out(row)


def build_state(coins: tuple[str, ...], setup_rows: list[Any], position_rows: list[Any], event_rows: list[Any], hb_age: float | None,
                now: datetime) -> dict[str, Any]:
    """Полная картина для клиента. Монеты: настроенные + замеченные в БД; для каждой - обе стороны всех стратегий с автоматом."""
    setups: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen = list(coins)
    for row in setup_rows:
        parsed = parse_setup_row(row)
        setups[(row["coin"], parsed["strategy"], parsed["side"])] = parsed
        if row["coin"] not in seen:
            seen.append(row["coin"])
    positions: dict[str, list[dict[str, Any]]] = {}
    for row in position_rows:
        positions.setdefault(row["coin"], []).append(position_out(row))
        if row["coin"] not in seen:
            seen.append(row["coin"])
    out = []
    for coin in seen:
        rows = []
        for strategy in SETUP_STRATEGIES:
            for side in SIDES:
                rows.append(setups.get((coin, strategy, side)) or
                            {"strategy": strategy, "side": side, "state": 0, "label": SETUP_LABELS[0], "reject": None, "reject_label": None, "since": None})
        out.append({"coin": coin, "setups": rows, "positions": positions.get(coin, [])})
    return {
        "type": "state", "server_time": iso(now),
        "shadow": {"heartbeat_age_s": hb_age, "alive": None if hb_age is None else hb_age < HEARTBEAT_STALE_S},
        "coins": out, "events": [event_out(r) for r in event_rows],
    }


def fingerprint(state: dict[str, Any]) -> str:
    """Отпечаток «содержательной» части (без времени сервера и пульса): по нему решается, пора ли рассылать клиентам."""
    return json.dumps({"coins": state.get("coins"), "events": state.get("events"), "alive": state.get("shadow", {}).get("alive"),
                       "type": state.get("type"), "detail": state.get("detail")}, sort_keys=True, default=str)


class LiveStateHub:
    """Фоновый опрос БД + рассылка. `wait_newer(version)` отдаёт клиентскому обработчику следующую версию состояния."""

    def __init__(self, pool: asyncpg.Pool, settings: Settings, clock: Callable[[], float] = time.monotonic):
        self._pool, self._s, self._clock = pool, settings, clock
        self.latest: dict[str, Any] | None = None
        self.version = 0
        self.healthy = False
        self._fp: str | None = None
        self._published_at = 0.0
        self._cond = asyncio.Condition()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self.refresh()
        self._task = asyncio.create_task(self._loop(), name="api_live_state_hub")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def fetch_state(self) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            async with conn.transaction(readonly=True, isolation="repeatable_read"):  # три запроса - один согласованный срез
                setups = await conn.fetch(SETUP_SQL, self._s.state_window_days)
                positions = await conn.fetch(POSITIONS_SQL, "shadow")
                events = await conn.fetch(EVENTS_SQL, EVENTS_LIMIT)
        hb = freshest_heartbeat_age_s([self._s.shadow_data_dir, self._s.live_data_dir])
        return build_state(self._s.coins, setups, positions, events, hb, datetime.now(timezone.utc))

    async def refresh(self) -> None:
        """Один цикл опроса. Недоступность БД и нечитаемые данные не роняют процесс (API только читает), но видны клиентам и в health."""
        try:
            state = await self.fetch_state()
            self.healthy = True
        except (asyncpg.PostgresError, OSError, asyncio.TimeoutError) as exc:
            log.warning("Опрос БД для live_state не удался: %s", type(exc).__name__)
            self.healthy = False
            state = {"type": "error", "server_time": iso(datetime.now(timezone.utc)), "detail": "database_unavailable"}
        except DataIntegrityError as exc:
            log.error("Данные state_events/trades нечитаемы: %s", exc)
            self.healthy = False
            state = {"type": "error", "server_time": iso(datetime.now(timezone.utc)), "detail": "data_integrity"}
        await self._publish(state)

    async def _publish(self, state: dict[str, Any]) -> None:
        fp, now = fingerprint(state), self._clock()
        if fp == self._fp and now - self._published_at < self._s.keepalive_s and self.latest is not None:
            return
        async with self._cond:
            self.latest, self._fp, self._published_at = state, fp, now
            self.version += 1
            self._cond.notify_all()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._s.poll_interval_s)
            await self.refresh()

    async def wait_newer(self, version: int, timeout: float) -> bool:
        """Ждёт версию новее `version`; False - за timeout ничего нового."""
        async with self._cond:
            if self.version > version:
                return True
            try:
                await asyncio.wait_for(self._cond.wait_for(lambda: self.version > version), timeout)
            except asyncio.TimeoutError:
                return False
            return True
