"""SymbolSet: изменяемый набор тикеров, который читают сразу несколько модулей (маршрутизатор live, фильтр стратегий, валидация конфигурации).

Автоселектор монет (orchestrator/universe.py) обновляет набор НА МЕСТЕ: все, кто держит ссылку на объект (`config.LIVE_BINANCE_SYMBOLS`, `ExecutionRouter`),
видят новый состав без пересоздания и без переподключения потоков. Порядок сохраняется (вставка), дублей нет; интерфейс - как у неизменяемого кортежа
(`in`, итерация, `len`, `==` с последовательностью) плюс `replace`.
"""

from collections.abc import Iterable, Iterator


class SymbolSet:
    def __init__(self, symbols: Iterable[str] = ()):
        self._items: dict[str, None] = {}
        self.replace(symbols)

    def replace(self, symbols: Iterable[str]) -> None:
        """Заменить состав целиком (одним присваиванием: читатель в другом месте цикла событий не увидит промежуточного состояния)."""
        self._items = dict.fromkeys(symbols)

    def __contains__(self, symbol: object) -> bool:
        return symbol in self._items

    def __iter__(self) -> Iterator[str]:
        return iter(tuple(self._items))  # снимок: безопасно при изменении состава во время обхода

    def __len__(self) -> int:
        return len(self._items)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SymbolSet):
            return list(self._items) == list(other._items)
        if isinstance(other, (tuple, list)):
            return list(self._items) == list(other)
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]  # изменяемый

    def __repr__(self) -> str:
        return f"SymbolSet({tuple(self._items)!r})"
