"""Настройки анализатора из переменных окружения. Ошибка настройки - `AnalyzerConfigError`, процесс завершается кодом 3 (как у api и shadow-run)."""

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

MIN_TOKEN_LEN = 32


class AnalyzerConfigError(Exception):
    """Неверная или отсутствующая настройка анализатора."""


@dataclass(frozen=True)
class Settings:
    database_url: str  # postgresql://analyzer_ro:...@postgres:5432/tradeai (роль только на чтение, analyzer/sql/001_analyzer_ro.sql)
    token: str  # общий секрет с основным API (заголовок X-Analyzer-Token): анализатор не публикуется наружу, но проверяет вызывающего
    data_dir: Path = Path("/data")  # SQLite (задания, отчёты, версии, календарь новостей) - том самого контейнера анализатора
    initial_equity: float = 10_000.0  # стартовый виртуальный эквити Shadow (VirtualTracker) - для просадки в % и в USD
    min_conclusions: int = 200  # порог «достаточно данных» (как в api/stats.py)
    min_group_trades: int = 30  # меньше сделок в группе - метрики считаются, но рекомендации по группе не строятся
    bootstrap_n: int = 1000  # ресэмплов для доверительных интервалов
    job_timeout_s: float = 600.0  # одно задание не дольше
    max_trades: int = 200_000  # предохранитель памяти воркера (контейнер 1 ГБ)
    max_queue: int = 5  # ждущих заданий не больше
    keep_reports: int = 50  # старые отчёты удаляются
    statement_timeout_s: int = 120
    host: str = "0.0.0.0"
    port: int = 8000
    manifest_path: Path = Path(__file__).with_name("current_params.json")
    bot_data_dirs: tuple[Path, ...] = (Path("/botdata/live"), Path("/botdata/shadow"))  # каталоги данных бота (:ro): оттуда берётся календарь новостей

    def __post_init__(self) -> None:
        parts = urlsplit(self.database_url)
        if parts.scheme not in ("postgresql", "postgres") or not parts.hostname or not parts.path.strip("/"):
            raise AnalyzerConfigError("DATABASE_URL должен иметь вид postgresql://analyzer_ro:password@host:5432/dbname")
        if parts.username and parts.username != "analyzer_ro":
            raise AnalyzerConfigError("DATABASE_URL анализатора должен использовать роль analyzer_ro (только чтение), а не пользователя бота")
        if not parts.password:
            raise AnalyzerConfigError("Не задан пароль роли analyzer_ro: ANALYZER_DB_PASSWORD пуст (создаёт ./deploy/shadow.sh analyzer-init в .env.shadow)")
        if len(self.token) < MIN_TOKEN_LEN or any(c.isspace() for c in self.token):
            raise AnalyzerConfigError(f"ANALYZER_TOKEN обязателен: не короче {MIN_TOKEN_LEN} символов, без пробелов")
        if self.initial_equity <= 0 or self.min_conclusions < 1 or self.min_group_trades < 2 or self.bootstrap_n < 100:
            raise AnalyzerConfigError("initial_equity > 0, min_conclusions >= 1, min_group_trades >= 2, bootstrap_n >= 100")
        if self.job_timeout_s <= 0 or self.max_trades < 1 or self.max_queue < 1 or self.keep_reports < 1:
            raise AnalyzerConfigError("job_timeout_s > 0, max_trades >= 1, max_queue >= 1, keep_reports >= 1")


def build_database_url(e) -> str:
    """DSN роли analyzer_ro. Пароль берётся из DATABASE_URL, а если там его нет - из ANALYZER_DB_PASSWORD (кодируется для URL: спецсимволы не ломают адрес).
    Роль analyzer_ro имеет СВОЙ пароль, поэтому POSTGRES_PASSWORD (пароль пользователя бота) не подставляется."""
    url = e.get("DATABASE_URL", "").strip()
    parts = urlsplit(url)
    password = e.get("ANALYZER_DB_PASSWORD", "").strip()
    if not url or parts.password or not password or not parts.hostname:
        return url  # готовый DSN с паролем / нечего достраивать: проверка - в Settings.__post_init__
    host = parts.hostname + (f":{parts.port}" if parts.port else "")
    return urlunsplit((parts.scheme, f"{quote(parts.username or 'analyzer_ro', safe='')}:{quote(password, safe='')}@{host}", parts.path, parts.query, parts.fragment))


def load_settings(env: dict[str, str] | None = None) -> Settings:
    e = os.environ if env is None else env
    try:
        extra: dict = {}
        if e.get("ANALYZER_INITIAL_EQUITY"):
            extra["initial_equity"] = float(e["ANALYZER_INITIAL_EQUITY"])
        if e.get("ANALYZER_BOOTSTRAP_N"):
            extra["bootstrap_n"] = int(e["ANALYZER_BOOTSTRAP_N"])
        if e.get("ANALYZER_JOB_TIMEOUT_S"):
            extra["job_timeout_s"] = float(e["ANALYZER_JOB_TIMEOUT_S"])
        port = int(e.get("ANALYZER_PORT", "8000"))
        if e.get("ANALYZER_BOT_DATA_DIRS"):
            extra["bot_data_dirs"] = tuple(Path(x) for x in e["ANALYZER_BOT_DATA_DIRS"].split(",") if x.strip())
    except ValueError as exc:
        raise AnalyzerConfigError(f"Числовая настройка анализатора задана неверно: {exc}") from exc
    return Settings(database_url=build_database_url(e), token=e.get("ANALYZER_TOKEN", ""), data_dir=Path(e.get("ANALYZER_DATA_DIR", "/data")),
                    host=e.get("ANALYZER_HOST", "0.0.0.0"), port=port, **extra)
