"""Где Playground хранит кэш свечей и историю прогонов: DATA_DIR/playground (на сервере - том сервиса playground, локально - data/playground)."""

import os
from pathlib import Path

import config


def root() -> Path:
    return Path(os.environ.get("PLAYGROUND_DIR") or (config.DATA_DIR / "playground"))


def klines_root() -> Path:
    return root() / "klines"


def db_path() -> Path:
    return root() / "playground.sqlite"
