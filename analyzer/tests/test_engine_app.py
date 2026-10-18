import dataclasses
import json
import re
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from analyzer.app import create_app
from analyzer.config import AnalyzerConfigError, Settings, load_settings
from analyzer.db import LoadQuery
from analyzer.engine import run_analysis
from analyzer.frame import build_frame
from analyzer.news import parse_calendar
from analyzer.recommend import load_manifest
from analyzer.store import Store
from analyzer.worker import process_one, query_from

from .conftest import DSN, PLAN, T0, TOKEN, make_trades

CAL = "timestamp,currency,impact,event\n" + "\n".join(f"{(T0 + pd.Timedelta(days=d, hours=3)).isoformat()},USD,High,CPI m/m" for d in range(0, 40, 3)) + "\n"
H = {"X-Analyzer-Token": TOKEN}


def big_frame():
    rows = make_trades(120, strategy="SMC_Pro_Trend") + make_trades(80, strategy="OBI_Breakout", seed=3, plan={**PLAN, "levels": [[1.0, 1.0]]}, coin="ETHUSDT")
    rows += make_trades(60, strategy="SMC_Pro_Trend", coin="ETHUSDT", seed=4, start=T0 + pd.Timedelta(days=40), plan={**PLAN, "trail_atr_mult": 2.0}, prefix="v2-")
    return build_frame(rows)


def test_run_analysis_full_report_is_complete_and_strict_json(cfg):
    from analyzer.news import parse_calendar
    report = run_analysis(big_frame(), {"mode": "shadow", "news": {"enabled": True, "before_min": 10, "after_min": 10, "impacts": ["high"]}}, cfg, load_manifest(cfg.manifest_path),
                          events=parse_calendar(CAL), user_versions=[])
    json.dumps(report, allow_nan=False)  # никаких NaN/Infinity: фронтенд парсит строгий JSON
    assert report["data"]["trades"] == 260 and report["data"]["strategies"] == ["OBI_Breakout", "SMC_Pro_Trend"]
    s = report["strategies"]["SMC_Pro_Trend"]
    assert {"metrics", "curve", "sessions", "vsa", "what_if", "feature_slices", "news"} <= set(s)
    assert {"profit_factor", "sharpe_trade", "sortino_trade", "expectancy_r", "expectancy_usd", "max_drawdown_r", "max_drawdown_pct"} <= set(s["metrics"])
    assert report["news"]["available"] and report["news"]["window_grid"] and "curves" in report["news"] and "curves" not in s["news"]
    assert report["versions"]["comparisons"], "по умолчанию сравниваются соседние автоверсии стратегии"
    assert report["by_strategy_coin"] and report["by_coin"] and report["overall_curve"]["cum_r"]


def test_run_analysis_without_calendar_warns_and_keeps_going(cfg):
    report = run_analysis(big_frame(), {}, cfg, load_manifest(cfg.manifest_path), events=None, user_versions=[])
    assert report["news"]["available"] is False and any("Календарь" in w for w in report["warnings"])
    empty = run_analysis(build_frame([]), {}, cfg, load_manifest(cfg.manifest_path), events=None, user_versions=[])
    assert empty["overall"]["n"] == 0 and empty["warnings"] and empty["recommendations"] == []


def test_unknown_comparison_is_reported_not_crashing(cfg):
    report = run_analysis(big_frame(), {"compare": [{"baseline": "nope", "candidate": "also-nope"}]}, cfg, load_manifest(cfg.manifest_path), events=None, user_versions=[])
    assert any("не найдена" in w for w in report["warnings"]) and report["versions"]["comparisons"] == []


# ---------------------------------------------------------------------------------- настройки и изоляция


def test_dsn_password_comes_from_env_and_is_url_encoded(tmp_path):
    base = {"DATABASE_URL": "postgresql://analyzer_ro@postgres:5432/tradeai", "ANALYZER_TOKEN": TOKEN, "ANALYZER_DATA_DIR": str(tmp_path)}
    s = load_settings({**base, "ANALYZER_DB_PASSWORD": "p@ss:w/rd#1"})
    assert s.database_url == "postgresql://analyzer_ro:p%40ss%3Aw%2Frd%231@postgres:5432/tradeai"
    with pytest.raises(AnalyzerConfigError, match="ANALYZER_DB_PASSWORD"):  # fe_sendauth: no password supplied - теперь ясная ошибка при старте
        load_settings(base)
    with pytest.raises(AnalyzerConfigError, match="ANALYZER_DB_PASSWORD"):
        load_settings({**base, "ANALYZER_DB_PASSWORD": "  "})
    assert load_settings({**base, "DATABASE_URL": DSN}).database_url == DSN  # готовый DSN с паролем не трогается
    with pytest.raises(AnalyzerConfigError, match="analyzer_ro"):  # POSTGRES_PASSWORD не подставляется: у роли свой пароль, а учётка бота запрещена
        load_settings({**base, "DATABASE_URL": "postgresql://tradeai@postgres:5432/tradeai", "ANALYZER_DB_PASSWORD": "x" * 20})


def test_settings_validate_role_token_and_numbers(tmp_path):
    ok = dict(database_url=DSN, token=TOKEN, data_dir=tmp_path)
    Settings(**ok)
    with pytest.raises(AnalyzerConfigError, match="analyzer_ro"):
        Settings(**{**ok, "database_url": "postgresql://tradeai:pw@postgres:5432/tradeai"})  # учётка бота анализатору не разрешена
    for tok in ("", "short", "x" * 20 + " " + "x" * 20):
        with pytest.raises(AnalyzerConfigError, match="ANALYZER_TOKEN"):
            Settings(**{**ok, "token": tok})
    with pytest.raises(AnalyzerConfigError, match="DATABASE_URL"):
        Settings(**{**ok, "database_url": "mysql://x"})
    with pytest.raises(AnalyzerConfigError):
        load_settings({"DATABASE_URL": DSN})
    s = load_settings({"DATABASE_URL": DSN, "ANALYZER_TOKEN": TOKEN, "ANALYZER_DATA_DIR": str(tmp_path), "ANALYZER_BOOTSTRAP_N": "300"})
    assert s.bootstrap_n == 300 and s.data_dir == tmp_path
    with pytest.raises(AnalyzerConfigError, match="Числовая"):
        load_settings({"DATABASE_URL": DSN, "ANALYZER_TOKEN": TOKEN, "ANALYZER_PORT": "abc"})


def test_package_is_isolated_from_the_bot_and_only_reads_postgres():
    pkg = Path(__file__).resolve().parents[1]
    bot = r"(config|orchestrator|strategies|exchange|execution|persistence|features|collectors|pipeline|backtester|utils|api|scripts)"
    for f in pkg.glob("*.py"):
        text = f.read_text(encoding="utf-8")
        assert not re.search(rf"^\s*(from|import)\s+{bot}\b", text, re.M), f"{f.name} импортирует код бота"
    sql = (pkg / "db.py").read_text(encoding="utf-8")
    from analyzer import db
    assert db.LOAD_SQL.strip().upper().startswith("SELECT") and not re.search(r"(INSERT|UPDATE|DELETE|TRUNCATE|DROP|ALTER|CREATE|GRANT)", db.LOAD_SQL, re.I)
    assert all(q.strip().upper().startswith("SELECT") for q in re.findall(r"\.execute\(\s*\"([^\"]+)\"", sql)), "db.py: любой execute - только SELECT"
    assert "default_transaction_read_only=on" in sql and "REPEATABLE_READ" in sql and "read_only = True" in sql


def test_role_script_grants_select_on_four_tables_only():
    text = (Path(__file__).resolve().parents[1] / "sql" / "001_analyzer_ro.sql").read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    grants = re.findall(r"^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+analyzer_ro", code, re.M | re.S)
    privileges = {(p.strip(), o.strip()) for p, o in grants}
    assert ("SELECT", "trades, trade_closes, trade_snapshots, state_events") in privileges
    assert all(p in ("SELECT", "CONNECT", "USAGE") for p, _ in privileges)
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|TRUNCATE|ALL PRIVILEGES|SUPERUSER|CREATEROLE|CREATEDB)\b", re.sub(r"NOSUPERUSER|NOCREATEDB|NOCREATEROLE", "", code)), "лишние права"
    assert "schema_migrations" not in code and "default_transaction_read_only = on" in code
    assert ":'analyzer_password'" in code and not re.search(r"PASSWORD\s+'", code, re.I), "пароль приходит psql-переменной, в файле его нет"


# ---------------------------------------------------------------------------------- REST + воркер


@pytest.fixture
def client(cfg):
    app = create_app(cfg, start_worker=False)
    return TestClient(app), cfg, app.state.store


def test_auth_required_everywhere_but_health(client):
    c, _, _ = client
    assert c.get("/health").status_code == 200
    for method, path in (("get", "/status"), ("post", "/runs"), ("get", "/runs"), ("get", "/versions"), ("get", "/calendar"), ("post", "/dry-run")):
        assert getattr(c, method)(path).status_code == 401
    assert c.get("/status", headers={"X-Analyzer-Token": "x" * 40}).status_code == 401
    assert c.get("/docs").status_code == 404 and c.get("/openapi.json").status_code == 404


def test_job_lifecycle_from_queue_to_report_and_dry_run(client):
    c, cfg, store = client
    store.merge_calendar(parse_calendar(CAL), "test")
    body = {"mode": "shadow", "news": {"before_min": 10, "after_min": 10}}
    job = c.post("/runs", json=body, headers=H)
    assert job.status_code == 202
    jid = job.json()["job_id"]
    assert c.get(f"/runs/{jid}", headers=H).json()["status"] == "queued"
    assert c.get(f"/runs/{jid}/report", headers=H).status_code == 409
    frame = big_frame()
    seen = {}
    assert process_one(cfg, store, loader=lambda s, q, p: (seen.update(q=q), p(1.0), frame)[2]) is True
    assert process_one(cfg, store, loader=lambda s, q, p: frame) is False  # очередь пуста
    assert seen["q"] == LoadQuery(mode="shadow")
    st = c.get(f"/runs/{jid}", headers=H).json()
    assert st["status"] == "done" and st["progress"] == 1
    rep = c.get(f"/runs/{jid}/report", headers=H).json()["report"]
    assert rep["data"]["trades"] == 260 and rep["news"]["available"]
    recs = rep["recommendations"]
    dr = c.post("/dry-run", json={"run_id": jid, "recommendation_ids": [r["id"] for r in recs[:1]]}, headers=H)
    assert dr.status_code == 200 and "Dry run" in dr.json()["note"]
    assert c.post("/dry-run", json={"run_id": jid, "recommendation_ids": ["f" * 10]}, headers=H).status_code == 422
    assert c.post("/dry-run", json={"run_id": "0" * 12}, headers=H).status_code == 404


def test_failed_job_is_recorded_with_reason_and_worker_survives(client):
    c, cfg, store = client
    jid = c.post("/runs", json={}, headers=H).json()["job_id"]

    def boom(*a):
        raise RuntimeError("PostgreSQL недоступен")
    assert process_one(cfg, store, loader=boom) is True
    st = c.get(f"/runs/{jid}", headers=H).json()
    assert st["status"] == "failed" and "RuntimeError: PostgreSQL недоступен" in st["error"]
    r = c.get(f"/runs/{jid}/report", headers=H)
    assert r.status_code == 409 and r.json()["status"] == "failed"
    j2 = c.post("/runs", json={}, headers=H).json()["job_id"]  # следующее задание обрабатывается
    assert process_one(cfg, store, loader=lambda s, q, p: build_frame([])) is True and c.get(f"/runs/{j2}", headers=H).json()["status"] == "done"


def test_timeout_fails_the_job(client):
    c, cfg, store = client
    c.post("/runs", json={}, headers=H)
    ticks = iter([0.0] + [cfg.job_timeout_s + 5] * 50)
    process_one(cfg, store, loader=lambda s, q, p: big_frame(), clock=lambda: next(ticks))
    job = c.get("/runs", headers=H).json()["items"][0]
    assert job["status"] == "failed" and "JobTimeout" in job["error"]


def test_queue_limit_and_request_validation(client):
    c, cfg, _ = client
    for _ in range(cfg.max_queue):
        assert c.post("/runs", json={}, headers=H).status_code == 202
    assert c.post("/runs", json={}, headers=H).status_code == 409
    for bad in ({"mode": "x"}, {"strategies": ["a b"]}, {"news": {"before_min": -1}}, {"unknown": 1}, {"since": "2026-02-01T00:00:00Z", "until": "2026-01-01T00:00:00Z"}):
        assert c.post("/runs", json=bad, headers=H).status_code == 422


def test_orphaned_running_jobs_fail_on_worker_start(client):
    c, cfg, store = client
    jid = c.post("/runs", json={}, headers=H).json()["job_id"]
    store.claim_next()
    assert store.fail_orphans() == 1
    assert c.get(f"/runs/{jid}", headers=H).json()["error"] == "worker_restart"


def test_versions_crud_conflict_and_validation(client):
    c, _, _ = client
    v = {"name": "SMC v1.1 strict", "strategy_id": "SMC_Pro_Trend", "ts_from": "2026-10-18T00:00:00Z", "meta_equals": {"require_buildup": True}, "note": "strict buildup"}
    assert c.post("/versions", json=v, headers=H).status_code == 201
    assert c.post("/versions", json=v, headers=H).status_code == 409
    assert c.post("/versions", json={**v, "overwrite": True, "note": "x"}, headers=H).status_code == 201
    items = c.get("/versions", headers=H).json()["items"]
    assert items[0]["name"] == "SMC v1.1 strict" and items[0]["meta_equals"] == {"require_buildup": True}
    assert c.post("/versions", json={**v, "name": "bad", "ts_from": "2026-12-01T00:00:00Z", "ts_to": "2026-01-01T00:00:00Z"}, headers=H).status_code == 422
    assert c.post("/versions", json={**v, "name": "x/y"}, headers=H).status_code == 422
    assert c.delete("/versions/SMC v1.1 strict", headers=H).status_code == 200
    assert c.delete("/versions/SMC v1.1 strict", headers=H).status_code == 404


def test_user_version_used_in_report_comparison(client):
    c, cfg, store = client
    c.post("/versions", json={"name": "v1", "strategy_id": "SMC_Pro_Trend", "ts_to": (T0 + pd.Timedelta(days=30)).isoformat()}, headers=H)
    c.post("/versions", json={"name": "v2", "strategy_id": "SMC_Pro_Trend", "ts_from": (T0 + pd.Timedelta(days=30)).isoformat()}, headers=H)
    jid = c.post("/runs", json={"compare": [{"baseline": "v1", "candidate": "v2", "align": "relative"}], "news": {"enabled": False}}, headers=H).json()["job_id"]
    process_one(cfg, store, loader=lambda s, q, p: big_frame())
    comp = c.get(f"/runs/{jid}/report", headers=H).json()["report"]["versions"]["comparisons"]
    assert len(comp) == 1 and comp[0]["baseline"]["version"]["name"] == "v1" and comp[0]["verdict"]["verdict"]


def test_calendar_comes_from_bot_storage_and_accumulates(cfg, tmp_path):
    live, shadow = tmp_path / "live", tmp_path / "shadow"
    c = TestClient(create_app(dataclasses.replace(cfg, bot_data_dirs=(live, shadow, tmp_path / "нет")), start_worker=False))
    assert c.post("/calendar", content=CAL, headers=H).status_code == 405  # загрузки файла больше нет
    assert c.delete("/calendar", headers=H).status_code == 405
    live.mkdir(), shadow.mkdir()
    base = int(T0.timestamp() * 1000)
    (live / ".news_calendar.json").write_text(json.dumps({"loaded_ms": base, "events": [[base + 1000, "usd", "CPI m/m"], [base + 2000, "USD", "NFP"]]}), encoding="utf-8")
    (shadow / ".news_calendar.json").write_text("{битый json", encoding="utf-8")  # нечитаемый файл пропускается, остальные работают
    info = c.get("/calendar", headers=H).json()
    assert info["events"] == 2 and info["by_impact"] == {"high": 2} and info["source"] == "bot"
    assert c.get("/status", headers=H).json()["calendar"]["events"] == 2  # повторная синхронизация не дублирует
    (live / ".news_calendar.json").write_text(json.dumps({"loaded_ms": base, "events": [[base + 2000, "USD", "NFP"], [base + 9000, "EUR", "ECB"]]}), encoding="utf-8")
    assert c.get("/calendar", headers=H).json()["events"] == 3  # неделя сменилась: прежние события остаются, новые добавляются


def test_bot_cache_name_matches_bot_config():
    import config
    from analyzer.botcalendar import CACHE_NAME
    assert CACHE_NAME == config.NEWS_CACHE_NAME


def test_news_window_default_is_5_before_5_after():
    from analyzer.models import NewsParams
    p = NewsParams()
    assert (p.before_min, p.after_min) == (5, 5)


def test_query_from_params():
    q = query_from({"mode": "all", "strategies": ["A"], "coins": None, "since": "2026-01-01T00:00:00", "until": None})
    assert q.mode == "all" and q.strategies == ("A",) and q.coins is None and q.since.tzinfo is not None and q.until is None


def test_ready_checks_the_database_not_just_the_http_server(cfg):
    import psycopg

    def broken(_):
        raise psycopg.OperationalError('FATAL:  password authentication failed for user "analyzer_ro"')

    bad = TestClient(create_app(cfg, start_worker=False, db_check=broken))
    assert bad.get("/health").status_code == 200  # процесс жив...
    r = bad.get("/ready")  # ...но готовым не считается: healthcheck контейнера увидит 503
    assert r.status_code == 503 and r.json()["reason"] == "db_unavailable" and "password authentication failed" in r.json()["detail"]
    good = TestClient(create_app(cfg, start_worker=False, db_check=lambda s: None))
    assert good.get("/ready").json() == {"status": "ok"}
