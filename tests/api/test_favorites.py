"""Избранное и раздача обновлений без БД: файлы эталонов, разбор картинки, версия обновления, запись только в таблицу favorites."""

import base64
import json
import re

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api import favorites, updates
from api.main import create_app
from api.settings import Settings, load_settings

DSN = "postgresql://tradeai:pw@postgres:5432/tradeai"
KEY = "k" * 40
H = {"X-API-Key": KEY}
PNG = favorites.PNG_MAGIC + b"\x00" * 40


def test_file_stem_has_no_path_or_colon():
    stem = favorites.file_stem("../../etc/BTCUSDT:SMC:1:live")
    assert not set(stem) & set("/\\:") and not stem.startswith(".")
    assert favorites.file_stem("a:b") == "a_b"


def test_decode_png_accepts_only_real_png_within_limit():
    assert favorites.decode_png(base64.b64encode(PNG).decode()) == PNG
    for bad in ("не base64!", base64.b64encode(b"GIF89a....").decode(), base64.b64encode(favorites.PNG_MAGIC + b"x" * favorites.MAX_PNG_BYTES).decode()):
        with pytest.raises(HTTPException) as exc:
            favorites.decode_png(bad)
        assert exc.value.status_code == 422


def test_write_and_remove_golden_files(tmp_path):
    names = favorites.write_golden(tmp_path / "golden", "BTCUSDT:1", {"trade": {"id": "x"}}, PNG)
    assert names == ["BTCUSDT_1.png", "BTCUSDT_1.json"]
    data = json.loads((tmp_path / "golden" / "BTCUSDT_1.json").read_text(encoding="utf-8"))
    assert data["image"] == "BTCUSDT_1.png" and (tmp_path / "golden" / "BTCUSDT_1.png").read_bytes() == PNG
    assert not list((tmp_path / "golden").glob("*.tmp"))
    favorites.write_golden(tmp_path / "golden", "NOPNG", {}, None)
    assert json.loads((tmp_path / "golden" / "NOPNG.json").read_text())["image"] is None
    favorites.remove_golden(tmp_path / "golden", "BTCUSDT:1")
    favorites.remove_golden(tmp_path / "golden", "BTCUSDT:1")  # повторное снятие безопасно
    assert not (tmp_path / "golden" / "BTCUSDT_1.json").exists() and not (tmp_path / "golden" / "BTCUSDT_1.png").exists()


def test_favorites_module_writes_only_the_favorites_table():
    """Пул избранного не должен уметь менять сделки: единственные операторы записи - над favorites."""
    for name in ("ADD_SQL", "REMOVE_SQL"):
        assert "favorites" in getattr(favorites, name)
    src = open(favorites.__file__, encoding="utf-8").read()
    writes = re.findall(r"\b(INSERT INTO|UPDATE|DELETE FROM|TRUNCATE|DROP|ALTER)\s+(\w+)", src)
    assert writes and {t for _, t in writes} == {"favorites"}


def make_client(tmp_path, with_dir=True):
    s = Settings(DSN, KEY, shadow_data_dir=tmp_path, update_dir=tmp_path / "upd" if with_dir else None)
    return TestClient(create_app(s))  # без `with`: lifespan (БД) не запускается, ручки обновлений от БД не зависят


def test_update_endpoints_need_the_key_and_serve_the_published_files(tmp_path):
    c = make_client(tmp_path)
    assert c.get("/api/update/version.json").status_code == 401 and c.get("/api/update/TradeAI-Monitor.exe").status_code == 401
    assert c.get("/api/update/version.json", headers=H).json() == {"detail": "no_update_published"}  # ещё ничего не опубликовано
    upd = tmp_path / "upd"
    upd.mkdir()
    (upd / "version.json").write_text(json.dumps({"version": "0.2.0", "sha256": "ab", "size": 3}), encoding="utf-8")
    (upd / "TradeAI-Monitor.exe").write_bytes(b"MZ\x90")
    assert c.get("/api/update/version.json", headers=H).json()["version"] == "0.2.0"
    r = c.get("/api/update/TradeAI-Monitor.exe", headers=H)
    assert r.status_code == 200 and r.content == b"MZ\x90"


def test_update_version_json_is_validated_and_dir_is_optional(tmp_path):
    (tmp_path / "upd").mkdir()
    (tmp_path / "upd" / "version.json").write_text('{"version": "../x"}', encoding="utf-8")
    assert make_client(tmp_path).get("/api/update/version.json", headers=H).status_code == 500
    assert make_client(tmp_path, with_dir=False).get("/api/update/version.json", headers=H).json() == {"detail": "updates_not_configured"}
    with pytest.raises(HTTPException):
        updates.read_version(tmp_path / "нет")


def test_settings_read_golden_and_update_dirs():
    s = load_settings({"DATABASE_URL": DSN, "API_KEY": KEY, "GOLDEN_SET_DIR": "/g", "UPDATE_DIR": "/u"})
    assert str(s.golden_set_dir).replace("\\", "/") == "/g" and str(s.update_dir).replace("\\", "/") == "/u"
    assert load_settings({"DATABASE_URL": DSN, "API_KEY": KEY}).golden_set_dir is None
