"""Точка входа контейнера анализатора: `python -m analyzer.main`. Без DATABASE_URL (роль analyzer_ro) и ANALYZER_TOKEN сервис не стартует (код 3)."""

import logging
import multiprocessing
import sys

from analyzer.config import AnalyzerConfigError, load_settings


def main() -> int:
    import uvicorn

    from analyzer.app import create_app

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        settings = load_settings()
    except AnalyzerConfigError as exc:
        print(f"ОШИБКА НАСТРОЙКИ АНАЛИЗАТОРА: {exc}", file=sys.stderr)
        return 3
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_level="info", proxy_headers=False)
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
