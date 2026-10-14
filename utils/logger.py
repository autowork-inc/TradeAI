"""Единая настройка логов: консоль (для docker logs) + ротируемый файл."""

import logging
import sys
import time
from logging.handlers import RotatingFileHandler

import config

# Библиотеки MetaApi (socket.io/engine.io) пишут каждый пакет на уровне INFO ("Received packet MESSAGE") - заглушаем до WARNING.
_NOISY_LOGGERS = ("engineio.client", "socketio.client", "metaapi_cloud_sdk", "httpx", "httpcore")
_FORMAT = "%(asctime)sZ %(levelname)-8s %(name)s: %(message)s"


def setup_logging(name: str) -> logging.Logger:
    """Настраивает корневой логгер (идемпотентно) и возвращает логгер `name`.

    Время в логах - UTC. Файл: <LOG_DIR>/<name>.log, 10 МБ x 5 копий.
    """
    formatter = logging.Formatter(_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        config.LOG_DIR / f"{name}.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(name)
