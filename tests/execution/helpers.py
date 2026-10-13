import contextlib
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from backtester.contracts import ExecutionConfig, OrderIntent, OrderType, Side
from execution.live_barrier import LiveBarrier
from execution.rest import Credentials, SignedRest
from execution.state import LocalState, SafeMode, StateStore
from tests.conftest import serve
from tests.execution.fake_exchange import API_KEY, API_SECRET, FakeExchange

EXEC_CFG = ExecutionConfig(order_latency_ms=50, maker_fee_rate=0.0002, taker_fee_rate=0.0005)
ASOF = 1_700_000_000_000


class RecordingAlerter:
    def __init__(self):
        self.alerts: list[tuple[str, dict]] = []

    def critical(self, title, details):
        self.alerts.append((title, dict(details)))


@dataclass
class Env:
    ex: FakeExchange
    host: str
    session: aiohttp.ClientSession
    rest: SignedRest
    safe: SafeMode
    alerter: RecordingAlerter
    store: StateStore
    state: LocalState
    barrier: LiveBarrier
    tmp: Path
    sleeps: list = field(default_factory=list)


@contextlib.asynccontextmanager
async def make_env(tmp_path, *, recv_window_ms=1000, attempts=3, timeout_s=3.0, market_price=60_000.0,
                   secret=API_SECRET, drift_grace_ms=3_000, exchange=None):
    ex = exchange or FakeExchange(market_price)
    async with serve(ex.app) as host:
        timeout = aiohttp.ClientTimeout(total=timeout_s, connect=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rest = SignedRest(session, f"http://{host}", Credentials(API_KEY, secret), recv_window_ms=recv_window_ms)
            alerter = RecordingAlerter()
            safe = SafeMode(tmp_path / ".safe_mode", alerter)
            store = StateStore(tmp_path / "state.json")
            state = store.load()
            sleeps: list[float] = []

            async def fake_sleep(seconds):
                sleeps.append(seconds)

            barrier = LiveBarrier(rest, EXEC_CFG, state, safe, store=store, attempts=attempts, backoff_s=0.0,
                                  drift_grace_ms=drift_grace_ms, sleep=fake_sleep)
            yield Env(ex, host, session, rest, safe, alerter, store, state, barrier, tmp_path, sleeps)


def market(side=Side.BUY, qty=0.5, symbol="BTCUSDT", asof=ASOF) -> OrderIntent:
    return OrderIntent(symbol, side, qty, asof)


def limit(side=Side.BUY, qty=0.5, price=59_000.0, ttl_ms=1_000, symbol="BTCUSDT", asof=ASOF) -> OrderIntent:
    return OrderIntent(symbol, side, qty, asof, OrderType.LIMIT, price, ttl_ms)
