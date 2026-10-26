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
    assert c.put(f"/api/lab/santa/lessons/{lesson()['id']}", json=lesson()).status_code == 200
    items = c.get("/api/lab/santa/lessons").json()["items"]
    assert items[0]["terms"] == {"zone": 1} and items[0]["author"] == "CHR"
    got = c.get(f"/api/lab/santa/lessons/{lesson()['id']}").json()
    assert got["notes"] == "разбор" and got["received_at"]
    assert c.delete(f"/api/lab/santa/lessons/{lesson()['id']}").status_code == 200 and c.get("/api/lab/santa/lessons").json()["items"] == []


def test_lesson_validation(tmp_path):
    c = client(tmp_path)
    lid = lesson()["id"]
    assert c.put(f"/api/lab/santa/lessons/{lid}", json={**lesson(), "id": "other-id-1"}).status_code == 422
    assert c.put(f"/api/lab/santa/lessons/{lid}", json=lesson(n=5)).status_code == 422
    assert c.put(f"/api/lab/santa/lessons/{lid}", json={**lesson(), "symbol": "../etc"}).status_code == 422
    assert c.put("/api/lab/santa/lessons/..%2F..%2Fx", json=lesson()).status_code in (404, 422)
    assert not list(tmp_path.rglob("*.json"))  # отклонённое не записано никуда


def test_vocab_and_strategies(tmp_path):
    c = client(tmp_path)
    assert c.put("/api/lab/santa/vocab", json={"items": [{"id": "u_imbalans", "name": "Имбаланс", "kind": "box"}]}).status_code == 200
    assert c.get("/api/lab/santa/vocab").json()["items"][0]["name"] == "Имбаланс"
    assert c.put("/api/lab/santa/vocab", json={"items": [{"name": "<script>"}]}).status_code == 422
    assert c.put("/api/lab/santa/strategies/Моя стратегия", json={"name": "Моя стратегия", "params": {}}).status_code == 200
    assert c.get("/api/lab/santa/strategies").json()["items"][0]["name"] == "Моя стратегия"
    assert c.put("/api/lab/santa/strategies/x", json={"name": "y"}).status_code == 422


def test_profiles_never_see_each_other(tmp_path):
    """1.9.1: у SANTA и CHR своё видение рынка - уроки, словарь и стратегии в разных каталогах и в разных путях API."""
    c = client(tmp_path)
    santa, chr_ = lesson("20261026-120000-santa1"), {**lesson("20261026-120000-chr001"), "author": "CHR", "symbol": "ETHUSDT"}
    assert c.put(f"/api/lab/santa/lessons/{santa['id']}", json=santa).status_code == 200
    assert c.put(f"/api/lab/chr/lessons/{chr_['id']}", json=chr_).status_code == 200
    assert [x["id"] for x in c.get("/api/lab/santa/lessons").json()["items"]] == [santa["id"]]
    assert [x["id"] for x in c.get("/api/lab/chr/lessons").json()["items"]] == [chr_["id"]]
    assert c.get(f"/api/lab/chr/lessons/{santa['id']}").status_code == 404 and c.get(f"/api/lab/santa/lessons/{chr_['id']}").status_code == 404
    assert c.delete(f"/api/lab/chr/lessons/{santa['id']}").status_code == 404  # удалить чужой урок через своё пространство нельзя
    assert c.put("/api/lab/chr/vocab", json={"items": [{"id": "u_x", "name": "Свое CHR", "kind": "box"}]}).status_code == 200
    assert c.get("/api/lab/santa/vocab").json()["items"] == []
    assert c.put("/api/lab/chr/strategies/Моя стратегия", json={"name": "Моя стратегия", "params": {"level": {"k": 2}}}).status_code == 200
    assert c.get("/api/lab/santa/strategies").json()["items"] == []
    assert c.get("/api/lab/chr/strategies").json()["items"][0]["params"] == {"level": {"k": 2}}
    assert sorted(p.name for p in tmp_path.iterdir()) == ["chr", "santa"]
    assert (tmp_path / "santa" / "lessons" / f"{santa['id']}.json").exists() and (tmp_path / "chr" / "lessons" / f"{chr_['id']}.json").exists()


def test_unknown_space_is_rejected(tmp_path):
    c = client(tmp_path)
    for bad in ("other", "SANTA", "..", "lessons"):
        assert c.get(f"/api/lab/{bad}/lessons").status_code in (404, 422)
    assert c.get("/api/lab/lessons").status_code in (404, 422)  # старый общий путь 1.9.0 закрыт
    assert not list(tmp_path.iterdir())


def test_snapshots_per_profile_with_picture(tmp_path):
    import base64

    c = client(tmp_path)
    png = "data:image/png;base64," + base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"0" * 64).decode()
    body = {"id": "20261026-120000-snap01", "symbol": "WLDUSDT", "tf": "1d", "comment": "вход здесь", "author": "CHR", "t": 1790000000, "image": png, "bars": [], "drawings": []}
    assert c.put("/api/lab/chr/snapshots/20261026-120000-snap01", json=body).status_code == 200
    assert (tmp_path / "chr" / "snapshots" / "20261026-120000-snap01.png").read_bytes().startswith(b"\x89PNG")
    meta = (tmp_path / "chr" / "snapshots" / "20261026-120000-snap01.json").read_text(encoding="utf-8")
    assert "image\"" not in meta and "вход здесь" in meta  # картинка отдельным файлом, не внутри JSON
    got = c.get("/api/lab/chr/snapshots/20261026-120000-snap01").json()
    assert got["image"] == png and got["comment"] == "вход здесь"
    assert [x["id"] for x in c.get("/api/lab/chr/snapshots").json()["items"]] == ["20261026-120000-snap01"]
    assert c.get("/api/lab/santa/snapshots").json()["items"] == [] and c.get("/api/lab/santa/snapshots/20261026-120000-snap01").status_code == 404
    for bad in ({**body, "comment": " "}, {**body, "image": "data:image/png;base64,@@"}, {**body, "image": "data:image/png;base64,AAAA"}, {**body, "tf": "2h"}, {**body, "id": "other-id-123"}):
        assert c.put("/api/lab/chr/snapshots/20261026-120000-snap01", json=bad).status_code in (413, 422)
    assert c.delete("/api/lab/santa/snapshots/20261026-120000-snap01").status_code == 404
    assert c.delete("/api/lab/chr/snapshots/20261026-120000-snap01").status_code == 200 and not list((tmp_path / "chr" / "snapshots").iterdir())


def test_not_configured(tmp_path):
    app = FastAPI()
    lab.install(app, lambda: None, None)
    assert TestClient(app).get("/api/lab/santa/lessons").status_code == 503
