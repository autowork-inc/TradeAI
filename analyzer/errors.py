"""Типы ошибок анализатора. Ошибки данных не прячутся: задание получает статус failed с понятной причиной, запрос API - 4xx/5xx."""


class AnalyzerError(Exception):
    """Базовая ошибка анализатора."""


class DataError(AnalyzerError):
    """В выборке нет нужных колонок / значений или данные противоречивы."""


class CalendarError(AnalyzerError):
    """Календарь новостей нечитаем (формат, даты, пустой файл)."""


class JobTimeout(AnalyzerError):
    """Задание превысило лимит времени."""


class NotFound(AnalyzerError):
    """Нет задания / версии / отчёта."""


class Conflict(AnalyzerError):
    """Очередь заполнена или имя версии занято."""
