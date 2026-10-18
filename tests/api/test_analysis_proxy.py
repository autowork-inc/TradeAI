"""Прокси /api/analysis/* к Strategy Quant Analyzer: белый список, ключ, подмена секрета, отображение отказов. Без PostgreSQL: lifespan не запускается
(TestClient без `with`), а анализатор подменён httpx.MockTransport."""

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from api.settings import ApiConfigError, Settings, load_settings

DSN = "postgresql://tradeai:pw@postgres:5432/tradeai"
KEY = "k" * 40
ATOKEN = "a" * 40
H = {"X-API-Key": KEY}


def make(handler, **kw):
    s = Settings(DSN, KEY, shadow_data_dir=Path("."), analyzer_url="http://analyzer:8000", analyzer_token=ATOKEN, **kw)
    return TestClient(create_app(s, analyzer_transport=httpx.MockTransport(handler)))


def test_proxy_requires_the_monitor_api_key_and_never_forwards_it():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"ok": True})

    c = make(handler)
    assert c.get("/api/analysis/status").status_code == 401 and not seen
    assert c.get("/api/analysis/status", headers={"X-API-Key": "x" * 40}).status_code == 401
    r = c.get("/api/analysis/status", headers=H)
    assert r.status_code == 200 and r.json() == {"ok": True}
    req = seen[0]
    assert str(req.url) == "http://analyzer:8000/status" and req.headers["X-Analyzer-Token"] == ATOKEN and "x-api-key" not in req.headers


def test_proxy_forwards_method_query_and_body():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, str(req.url), req.content, req.headers.get("content-type")))
        return httpx.Response(202, json={"job_id": "abc"})

    c = make(handler)
    assert c.post("/api/analysis/runs", json={"mode": "shadow"}, headers=H).status_code == 202
    assert c.get("/api/analysis/runs/0123456789ab/report?x=1", headers=H).status_code == 202
    assert c.get("/api/analysis/calendar", headers=H).status_code == 202
    assert c.post("/api/analysis/calendar", content="a,b", headers={**H, "content-type": "text/csv"}).status_code == 405  # загрузки календаря больше нет
    assert c.delete("/api/analysis/calendar", headers=H).status_code == 405
    assert c.delete("/api/analysis/versions/SMC v1.1", headers=H).status_code == 202
    assert seen[0][0] == "POST" and b'"mode"' in seen[0][2]
    assert seen[1][:2] == ("GET", "http://analyzer:8000/runs/0123456789ab/report?x=1")
    assert seen[2][:2] == ("GET", "http://analyzer:8000/calendar")
    assert seen[3][:2] == ("DELETE", "http://analyzer:8000/versions/SMC%20v1.1")


@pytest.mark.parametrize("method, path, code", [("get", "/api/analysis/", 404), ("get", "/api/analysis/secret", 404), ("get", "/api/analysis/runs/../x", 404),
                                                ("get", "/api/analysis/runs/a/b/c", 404), ("delete", "/api/analysis/runs", 405), ("get", "/api/analysis/dry-run", 405),
                                                ("post", "/api/analysis/status", 405), ("get", "/api/analysis/runs/%2e%2e", 404), ("get", "/api/analysis/runs/a;b", 404)])
def test_proxy_whitelist(method, path, code):
    c = make(lambda req: httpx.Response(200, json={}))
    assert getattr(c, method)(path, headers=H).status_code == code


def test_upstream_failures_are_mapped_without_breaking_monitor_auth():
    def refuse(req):
        raise httpx.ConnectError("down")

    def slow(req):
        raise httpx.ReadTimeout("slow")

    assert make(refuse).get("/api/analysis/status", headers=H).json() == {"detail": "analyzer_unavailable"}
    assert make(refuse).get("/api/analysis/status", headers=H).status_code == 503
    assert make(slow).get("/api/analysis/status", headers=H).status_code == 504
    r = make(lambda req: httpx.Response(401, json={"detail": "invalid_token"})).get("/api/analysis/status", headers=H)
    assert r.status_code == 502 and r.json() == {"detail": "analyzer_auth_failed"}  # 401 клиенту не отдаём: он принял бы это за неверный API-ключ
    assert make(lambda req: httpx.Response(200, text="<html>")).get("/api/analysis/status", headers=H).status_code == 502
    assert make(lambda req: httpx.Response(409, json={"detail": "report_not_ready"})).get("/api/analysis/runs/0123456789ab/report", headers=H).status_code == 409


def test_unconfigured_analyzer_and_body_limit():
    s = Settings(DSN, KEY, shadow_data_dir=Path("."))
    c = TestClient(create_app(s))
    r = c.get("/api/analysis/status", headers=H)
    assert r.status_code == 503 and r.json() == {"detail": "analyzer_not_configured"}
    big = make(lambda req: httpx.Response(200, json={}))
    assert big.post("/api/analysis/runs", content=b"x" * 5_000_001, headers=H).status_code == 413


def test_settings_read_analyzer_env():
    s = load_settings({"DATABASE_URL": DSN, "API_KEY": KEY, "ANALYZER_URL": "http://analyzer:8000", "ANALYZER_TOKEN": ATOKEN})
    assert s.analyzer_url == "http://analyzer:8000" and s.analyzer_token == ATOKEN
    assert load_settings({"DATABASE_URL": DSN, "API_KEY": KEY}).analyzer_url == ""
    with pytest.raises(ApiConfigError, match="ANALYZER_URL"):
        load_settings({"DATABASE_URL": DSN, "API_KEY": KEY, "ANALYZER_URL": "analyzer:8000"})
