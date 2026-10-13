import json
import sys
from pathlib import Path

import pytest

CLIENT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(CLIENT_DIR))  # tradeai_client импортируется без установки пакета
FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def fx():
    """Ответы настоящего API (снятые с демо-БД): контракт сервер -> клиент. Обновление: см. client/README.md."""

    def load(name: str):
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    return load
