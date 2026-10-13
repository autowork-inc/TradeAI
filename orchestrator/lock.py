"""CoinOwnershipLock (D4): «кто первый вошёл, тот и управляет стейтом монеты».

В One-Way режиме на монету одна общая позиция, поэтому одновременно её может вести только один владелец. Блокировка
берётся при приёме сигнала (заявка выставлена) и снимается при закрытии сделки или отмене заявки. Чужие сигналы не
ждут и не ставятся в очередь: оркестратор пишет их в журнал как `skipped_due_to_lock`.

Правила Fail-Fast (R1): снять блокировку может только владелец, взять уже занятую монету без проверки нельзя -
нарушение порядка вызовов - это баг, он бросает LockError, а не молча игнорируется.
"""

from dataclasses import dataclass

from utils.errors import TradingInfraError


class LockError(TradingInfraError):
    """Нарушен протокол блокировки: освобождение чужой/пустой блокировки или повторный захват."""


@dataclass(frozen=True)
class Owner:
    strategy: str
    trade_id: str


@dataclass(frozen=True)
class Acquisition:
    acquired: bool
    held_by: Owner | None  # при acquired=False - кто держит монету


class CoinOwnershipLock:
    def __init__(self, name: str = "lock"):
        self.name = name
        self._owners: dict[str, Owner] = {}

    def owner(self, symbol: str) -> Owner | None:
        return self._owners.get(symbol)

    def try_acquire(self, symbol: str, owner: Owner) -> Acquisition:
        held = self._owners.get(symbol)
        if held is not None:
            if held == owner:
                raise LockError(f"{self.name}: {owner} уже владеет {symbol}: повторный захват")
            return Acquisition(False, held)
        self._owners[symbol] = owner
        return Acquisition(True, owner)

    def release(self, symbol: str, owner: Owner) -> None:
        held = self._owners.get(symbol)
        if held != owner:
            raise LockError(f"{self.name}: {owner} освобождает {symbol}, а владелец {held}")
        del self._owners[symbol]

    def snapshot(self) -> dict[str, Owner]:
        return dict(self._owners)
