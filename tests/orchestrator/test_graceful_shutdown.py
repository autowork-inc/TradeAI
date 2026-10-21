"""Остановка бота (docker stop / деплой) НЕ закрывает позиции, а следующий запуск подхватывает их ДО поиска новых сигналов (2026-10-21).

Принцип: стоп-ордера живут на бирже (R6), поэтому остановка процесса только прекращает сопровождение; позиции, стопы и тейки остаются как есть, а при старте движок
(`build_live_engine` -> `recover`) читает файл состояния, сверяет его с биржей и БД, подхватывает сделки под их стратегиями с прежними SL/TP и только потом бот начинает искать сигналы.
"""

import asyncio
import re
from pathlib import Path

from orchestrator.live_engine import OPEN
from tests.orchestrator.live_helpers import signal
from tests.orchestrator.test_live_engine import NOW, SYM, Env, run

ROOT = Path(__file__).resolve().parents[2]


def mutating(env: Env, since: int) -> list[str]:
    """Запросы к бирже, меняющие состояние (новый/отменённый ордер, перенос стопа, закрытие), после точки `since`."""
    return [n for n in env.rest.names()[since:] if n.startswith(("POST", "DELETE", "PUT"))]


def test_graceful_shutdown_sends_no_orders_and_leaves_stops_and_positions_on_the_exchange(tmp_path):
    env = Env(tmp_path, poll_interval_s=0.01)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    live_stops = {k for k, a in env.rest.algos.items() if a["algoStatus"] == "NEW"}
    assert live_stops and env.rest.positions[SYM] == 1.0
    n = len(env.rest.requests)

    async def sigterm():
        env.engine.start()  # исполнитель работает в фоне, как в боевом процессе
        await asyncio.sleep(0.1)
        await env.engine.aclose()  # SIGTERM -> отмена главной задачи -> finally -> aclose

    run(sigterm())
    assert mutating(env, n) == []  # ни одного ордера на закрытие, отмену или перенос стопа
    assert env.rest.positions[SYM] == 1.0
    assert {k for k, a in env.rest.algos.items() if a["algoStatus"] == "NEW"} == live_stops  # защита на бирже цела
    assert env.trade().status == OPEN  # сделка осталась записанной в файле состояния для следующего запуска


def test_next_start_recovers_the_position_with_its_strategy_levels_and_without_new_orders(tmp_path):
    env = Env(tmp_path, poll_interval_s=0.01)
    run(env.engine.execute(signal(entry=100.0, sl=98.0), NOW))
    before_trade = env.trade()
    run(env.engine.aclose())  # остановка
    n = len(env.rest.requests)
    engine = env.reborn()  # «деплой»: новый процесс над той же биржей и тем же файлом состояния
    run(engine.recover([SYM]))
    t = engine.trades[SYM]
    assert (t.plan.strategy, t.plan.sl, t.plan.tp, t.status) == (before_trade.plan.strategy, before_trade.plan.sl, before_trade.plan.tp, OPEN)  # стратегия и уровни из состояния
    assert env.risk.busy() == {SYM: t.trade_id}  # монета занята: новых сигналов по ней не будет
    assert mutating(env, n) == []  # подхват только читает биржу
    env.rest.set_price(108.0)  # и продолжает сопровождение: тейк 1:4 закрывает позицию
    run(engine.poll())
    assert SYM not in engine.trades and env.rest.positions[SYM] == 0.0


def test_recovery_runs_before_the_bot_looks_for_new_signals():
    """Порядок запуска в shadow_run: движок собран и сверен с биржей (`build_live_engine` внутри вызывает `recover`) ДО цикла `runner.run()`."""
    src = (ROOT / "scripts" / "shadow_run.py").read_text(encoding="utf-8")
    assert src.index("build_live_engine(") < src.index("await runner.run()")
    engine_src = (ROOT / "orchestrator" / "live_engine.py").read_text(encoding="utf-8")
    body = engine_src[engine_src.index("async def build_live_engine"):]
    assert re.search(r"await engine\.recover\(symbols, db_open\)\s+return engine", body)  # возврат движка только после подхвата позиций


def test_shutdown_path_has_no_close_position_calls():
    """В цепочке остановки (`aclose` движка и `finally` shadow_run) нет закрытия позиций: рыночное закрытие бывает только в аварии (Safe Mode)."""
    engine_src = (ROOT / "orchestrator" / "live_engine.py").read_text(encoding="utf-8")
    aclose = engine_src[engine_src.index("    async def aclose"):engine_src.index("    async def run(self)")]
    assert "close" not in aclose.replace("aclose", "").replace("self._task.cancel()", "") and "order" not in aclose
    run_src = (ROOT / "scripts" / "shadow_run.py").read_text(encoding="utf-8")
    tail = run_src[run_src.index("await runner.run()"):run_src.index("async def main()")]
    assert not re.search(r"market_close|close_position|reduce_only|new_order|emergency", tail)
