"""API Create Strategy (api/lab.py): обмен уроками, словарём и стратегиями между трейдерами; проверка формата и размера; хранение в своём каталоге."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import lab


def client(tmp_path):
    app = FastAPI()
    lab.install(app, lambda: None, tmp_path)
    return TestClient(app)


def lesson(lid="20261026-120000-abcdef", n=40):
    return {"id": lid, "symbol": "WLDUSDT", "tf": "15m", "author": "CHR", "bars": [[1_790_000_000 + i * 900, 1, 1, 1, 1, 1, 0.5] for i in range(n)],
            "drawings": [{"id": "a", "tool": "rect", "term": "zone", "note": "отскок"}], "view": {"from_t": 1, "to_t": 2}, "notes": "разбор"}


def test_lesson_put_list_get_delete(tmp_path):
    c = client(tmp_path)
    assert c.put(f"/api/lab/lessons/{lesson()['id']}", json=lesson()).status_code == 200
    items = c.get("/api/lab/lessons").json()["items"]
    assert items[0]["terms"] == {"zone": 1} and items[0]["author"] == "CHR"
    got = c.get(f"/api/lab/lessons/{lesson()['id']}").json()
    assert got["notes"] == "разбор" and got["received_at"]
    assert c.delete(f"/api/lab/lessons/{lesson()['id']}").status_code == 200 and c.get("/api/lab/lessons").json()["items"] == []


def test_lesson_validation(tmp_path):
    c = client(tmp_path)
    lid = lesson()["id"]
    assert c.put(f"/api/lab/lessons/{lid}", json={**lesson(), "id": "other-id-1"}).status_code == 422
    assert c.put(f"/api/lab/lessons/{lid}", json=lesson(n=5)).status_code == 422
    assert c.put(f"/api/lab/lessons/{lid}", json={**lesson(), "symbol": "../etc"}).status_code == 422
    assert c.put("/api/lab/lessons/..%2F..%2Fx", json=lesson()).status_code in (404, 422)
    assert not list(tmp_path.rglob("*.json"))  # отклонённое не записано никуда


def test_vocab_and_strategies(tmp_path):
    c = client(tmp_path)
    assert c.put("/api/lab/vocab", json={"items": [{"id": "u_imbalans", "name": "Имбаланс", "kind": "box"}]}).status_code == 200
    assert c.get("/api/lab/vocab").json()["items"][0]["name"] == "Имбаланс"
    assert c.put("/api/lab/vocab", json={"items": [{"name": "<script>"}]}).status_code == 422
    assert c.put("/api/lab/strategies/Моя стратегия", json={"name": "Моя стратегия", "params": {}}).status_code == 200
    assert c.get("/api/lab/strategies").json()["items"][0]["name"] == "Моя стратегия"
    assert c.put("/api/lab/strategies/x", json={"name": "y"}).status_code == 422


def test_not_configured(tmp_path):
    app = FastAPI()
    lab.install(app, lambda: None, None)
    assert TestClient(app).get("/api/lab/lessons").status_code == 503
