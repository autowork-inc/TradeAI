import contextlib
import sys
from pathlib import Path

import pytest
from aiohttp import web

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402


@contextlib.asynccontextmanager
async def serve(app: web.Application):
    """Поднимает aiohttp-приложение на свободном локальном порту, отдаёт host:port."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Изолирует пути и сокращает настройки под тесты."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "KLINES_DIR", tmp_path / "klines")
    monkeypatch.setattr(config, "DEPTH_DIR", tmp_path / "depth")
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(config, "SYMBOLS", ("BTCUSDT", "ETHUSDT"))
    return tmp_path
