"""scripts/test_algo_live.py на имитации биржи: сам скрипт проверки обязан быть исправным, прежде чем его запустят на testnet.

Что проверяется: счастливый путь (защита, безубыток POST -> DELETE, зонды, уборка), отказ биржи от суммы reduceOnly-ордеров
(скрипт обязан честно показать FAIL и убрать за собой), срабатывание настоящего стопа (сценарий trigger), охранные
условия (только testnet, подтверждение, чистый счёт, чужое состояние не просачивается).
"""

import asyncio
import json

import pytest

import config
from execution.exchange_filters import SymbolFilters, config_mismatches, min_order_qty, parse_exchange_info, round_qty
from execution.live_barrier import LiveBarrier
from execution.rest import Credentials, SignedRest
from scripts import test_algo_live as live
from tests.execution.fake_algo import FakeAlgoExchange
from tests.execution.helpers import make_env
from utils.errors import ConfigError, DataError

SYMBOL = "BTCUSDT"


def run(coro):
    return asyncio.run(coro)


def opts(**kw):
    return live.Options(**{"confirm_separate_account": True, **kw})


def algo_env(tmp_path, **kw):
    return make_env(tmp_path, exchange=FakeAlgoExchange(kw.pop("market_price", 60_000.0)), **kw)


async def run_check(e, tmp_path, o=None, *, report=None):
    report = report or live.Report()
    rest = live.RecordingRest(e.session, f"http://{e.host}", Credentials("test-key", "test-secret"), recv_window_ms=1000)
    await live.run_live_check(o or opts(), rest, e.session, tmp_path / "algo_live", report, ws_private_url=f"ws://{e.host}/private", mark_probe=False)
    return report


def statuses(report):
    return {c["name"]: c["status"] for c in report.checks}


def test_happy_path_passes_and_leaves_the_account_clean(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            report = await run_check(e, tmp_path)
            return report, dict(e.ex.positions), [a["status"] for a in e.ex.algo_orders], e.ex.algo_requests

    report, positions, algo_statuses, requests = run(body())
    st = statuses(report)
    assert not report.failed, report.table()
    for name in ("account_clean", "exchange_filters", "user_stream", "entry_fill", "protection_on_exchange", "algo_update_events",
                 "reconcile_with_protection", "breakeven_post_then_delete", "breakeven_final_state", "final_account_clean"):
        assert st[name] == live.PASS, (name, report.table())
    assert positions.get(SYMBOL, 0.0) == 0.0 and "NEW" not in algo_statuses  # позиция закрыта, условные ордера сняты
    assert report.facts["probe_second_close_position_stop"]["code"] == -4130  # зонды отработали и записали факты
    assert report.facts["probe_stop_already_beyond_price"]["code"] == -2021
    assert report.facts["probe_get_unknown_algo_order"]["code"] == -2013 and report.facts["probe_delete_unknown_algo_order"]["code"] == -2011
    assert report.facts["probe_reduce_only_without_position"]["code"] == -2022
    assert report.wire and any(w["path"] == config.ALGO_ORDER_PATH and w["method"] == "POST" for w in report.wire)


def test_breakeven_wire_order_is_post_before_delete_in_the_report(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            return await run_check(e, tmp_path)

    report = run(body())
    assert [m for m, *_ in report.facts["breakeven_wire"]] == ["POST", "DELETE"]


def test_exchange_rejecting_sum_of_reduce_only_orders_is_reported_as_fail_and_cleaned_up(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.strict_reduce_sum = True
            report = await run_check(e, tmp_path)
            return report, dict(e.ex.positions), [a["status"] for a in e.ex.algo_orders]

    report, positions, algo_statuses = run(body())
    assert report.failed and statuses(report)["protection_on_exchange"] == live.FAIL
    assert positions.get(SYMBOL, 0.0) == 0.0 and "NEW" not in algo_statuses  # даже при сбое счёт убран
    assert statuses(report)["final_account_clean"] == live.PASS
    assert any(w.get("code") == -2022 for w in report.wire)  # код отказа сохранён для разбора


def test_breakeven_refusal_is_reported_with_the_exchange_code(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            def refuse_breakeven_stop(params):  # SL и TP пропускаем (их уже 2), отказываем именно новому стопу безубытка
                if not params["clientAlgoId"].startswith("tbprobe") and len([a for a in e.ex.algo_orders if a["status"] == "NEW"]) >= 2:
                    e.ex.faults["algo_post"] = ["reject"]

            e.ex.hooks["pre_algo"] = refuse_breakeven_stop
            return await run_check(e, tmp_path)

    report = run(body())
    st = statuses(report)
    assert st["breakeven_post_then_delete"] == live.FAIL and report.facts["breakeven_post_refused"]["code"] == -4164
    assert st["final_account_clean"] == live.PASS


def test_script_detects_old_stop_left_on_the_exchange_after_breakeven(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.hooks["pre_algo"] = lambda params: (e.ex.faults.__setitem__("algo_delete", ["noop"]) if params["clientAlgoId"].startswith("tba") else None)
            return await run_check(e, tmp_path)

    report = run(body())
    assert statuses(report)["breakeven_final_state"] == live.FAIL  # биржа не сняла старый стоп: скрипт это видит, а не верит локальному учёту


def test_script_detects_a_barrier_that_deletes_before_posting(tmp_path, monkeypatch):
    async def legacy(self, prot, size):  # поведение прошлой версии: сначала DELETE, потом POST (окно без стопа)
        for algo in list(self._state.algo_for(prot.symbol, "sl")):
            await self._cancel_algo_confirmed(algo)
        await self._try_place_algo(prot, "be_sl", self.round_price(prot.symbol, prot.entry_price), size)
        return True

    monkeypatch.setattr(LiveBarrier, "_cover_with_breakeven", legacy)

    async def body():
        async with algo_env(tmp_path) as e:
            return await run_check(e, tmp_path)

    report = run(body())
    assert statuses(report)["breakeven_post_then_delete"] == live.FAIL


def test_trigger_scenario_proves_that_a_real_stop_is_not_foreign(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            async def exchange_moves():  # «рынок дошёл до стопа»: срабатывает только сценарий trigger (второй вход)
                while True:
                    await asyncio.sleep(0.02)
                    entries = [o for o in e.ex.orders if o["type"] == "MARKET" and not o["reduce_only"]]
                    stops = [a for a in e.ex.algo_orders if a["status"] == "NEW" and a["type"] == "STOP_MARKET" and abs(a["trigger"] / 60_000 - 1) < 0.001]
                    if len(entries) >= 2 and stops:
                        await asyncio.sleep(0.1)
                        await e.ex.trigger_algo(stops[0]["cid"], price=stops[0]["trigger"])
                        return

            task = asyncio.create_task(exchange_moves())
            report = await run_check(e, tmp_path, opts(with_trigger=True, trigger_timeout_s=10))
            task.cancel()
            return report

    report = run(body())
    st = statuses(report)
    assert not report.failed, report.table()
    for name in ("trigger_fired", "trigger_not_foreign", "trigger_sibling_cleaned", "trigger_protection_removed"):
        assert st[name] == live.PASS, (name, report.table())
    assert report.facts["exit_role"] == "sl"
    assert "ALGO_UPDATE" in {t for t, *_ in report.facts["trigger_event_sequence"]}


def test_trigger_scenario_without_a_move_is_inconclusive_not_a_pass(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            return await run_check(e, tmp_path, opts(with_trigger=True, trigger_timeout_s=0.5)), dict(e.ex.positions)

    report, positions = run(body())
    assert statuses(report)["trigger_fired"] == live.WARN and "trigger_not_foreign" not in statuses(report)
    assert positions.get(SYMBOL, 0.0) == 0.0  # позиция сценария всё равно закрыта уборкой


def test_account_with_a_position_aborts_before_any_order(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_position("ETHUSDT", 0.3)  # похоже на аккаунт ML-бота
            with pytest.raises(ConfigError, match="не чистый"):
                await run_check(e, tmp_path)
            return e.ex.post_log, e.ex.algo_post_log

    orders, algo = run(body())
    assert orders == [] and algo == []  # ни одного ордера


def test_open_algo_order_also_aborts(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_algo("tba-someone", type_="STOP_MARKET")
            with pytest.raises(ConfigError, match="algo-ордер"):
                await run_check(e, tmp_path)

    run(body())


def test_cleanup_mode_flattens_leftovers(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            e.ex.seed_position(SYMBOL, 0.002)
            e.ex.seed_algo("tba-left", quantity=0.002, close_position=False, reduce_only=True)
            report = await run_check(e, tmp_path, opts(cleanup_only=True))
            return report, dict(e.ex.positions), [a["status"] for a in e.ex.algo_orders]

    report, positions, algo_statuses = run(body())
    assert statuses(report)["cleanup"] == live.PASS and positions[SYMBOL] == 0.0 and "NEW" not in algo_statuses


def test_stale_safe_mode_latch_from_a_previous_run_does_not_leak(tmp_path):
    async def body():
        async with algo_env(tmp_path) as e:
            work = tmp_path / "algo_live"
            work.mkdir()
            (work / ".safe_mode").write_text(json.dumps({"reason": "старое"}), encoding="utf-8")
            (work / "state.json").write_text("мусор", encoding="utf-8")
            return await run_check(e, tmp_path)

    assert not run(body()).failed


def test_guards_testnet_only_and_confirmation(monkeypatch):
    monkeypatch.setattr(config, "IS_TESTNET", False)
    with pytest.raises(ConfigError, match="ТОЛЬКО на testnet"):
        live.guard_testnet()
    monkeypatch.setattr(config, "IS_TESTNET", True)
    monkeypatch.setattr(config, "REST_BASE_URL", "https://fapi.binance.com")
    with pytest.raises(ConfigError, match="ТОЛЬКО на testnet"):
        live.guard_testnet()  # даже профиль testnet с боевым адресом не пропускается
    monkeypatch.setattr(config, "REST_BASE_URL", "https://testnet.binancefuture.com")
    live.guard_testnet()
    with pytest.raises(ConfigError, match="confirm-separate-account"):
        run(live.main_async(live.Options()))


def test_script_never_suppresses_exceptions_around_orders():
    import re
    src = open("scripts/test_algo_live.py", encoding="utf-8").read()
    code = "\n".join(line.split("#")[0] for line in src.splitlines())
    handlers = re.findall(r"except\s*:|except\s+(?:Base)?Exception\b[^\n]*|suppress\(", code)
    # обработчики, которые записывают причину в отчёт и/или пробрасывают дальше, допустимы; голых и молчаливых нет
    assert all(h.startswith("except BaseException") or h.startswith("except Exception as exc") for h in handlers), handlers


# ------------------------------------------------------------------------------ фильтры биржи


def info(**overrides):
    f = {"PRICE_FILTER": {"filterType": "PRICE_FILTER", "tickSize": "0.10"}, "LOT_SIZE": {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
         "MIN_NOTIONAL": {"filterType": "MIN_NOTIONAL", "notional": "100"}}
    f.update(overrides)
    return {"symbols": [{"symbol": SYMBOL, "filters": list(f.values())}]}


def test_exchange_info_parsing_and_config_comparison():
    (btc,) = parse_exchange_info(info(), [SYMBOL]).values()
    assert btc == SymbolFilters(SYMBOL, 0.1, 0.001, 0.001, 100.0) and config_mismatches({SYMBOL: btc}) == []
    bad = SymbolFilters(SYMBOL, 0.5, 0.01, 0.01, 50.0)
    assert len(config_mismatches({SYMBOL: bad})) == 4
    with pytest.raises(DataError):
        parse_exchange_info(info(MIN_NOTIONAL={"filterType": "NOTIONAL"}), [SYMBOL])
    with pytest.raises(DataError):
        parse_exchange_info({"symbols": []}, [SYMBOL])


def test_min_order_qty_covers_min_notional_and_rounds_to_step():
    f = SymbolFilters(SYMBOL, 0.1, 0.001, 0.001, 100.0)
    assert min_order_qty(f, 60_000.0) == 0.002  # 100 * 1.2 / 60000 = 0.002
    assert min_order_qty(f, 500_000.0) == 0.001  # упираемся в minQty
    assert round_qty(0.001, 0.0239) == 0.023 and round_qty(0.001, 0.0231, up=True) == 0.024
