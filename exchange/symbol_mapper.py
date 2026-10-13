"""Маппинг внутренних символов системы на символы брокера MT5 и кеш спецификаций (для `MetaApiLiveClient`).

Зачем. Внутри системы актив называется одинаково (`EURUSD`, `XAUUSD`, `US500`), а у брокера точное имя своё: `EURUSDm`, `EURUSD.ecn`,
`XAUUSD#`, `GOLD`, `UKOIL`. Ордер на неточное имя брокер отклонит, а ордер на «похожий» символ (другой контракт) опасен: поэтому
сопоставление строгое, неоднозначность - ошибка конфигурации (Fail-Fast, R1), а не «выбрали первый».

Порядок разрешения имени (для каждого базового имени из списка псевдонимов по приоритету):
  1. явное переопределение (`overrides` или env `MT5_SYMBOL_MAP="BRN=UKOIL,US500=US500.cash"`) - обязано существовать у брокера;
  2. точное совпадение с символом брокера (затем совпадение без учёта регистра, если оно единственное);
  3. базовое имя + суффикс брокера. Суффикс - либо начинается с небуквенно-цифрового знака (`.ecn`, `#`, `_i`, `-`), либо состоит
     только из строчных букв длиной до 5 (`m`, `c`, `pro`, `ecn`). Заглавная буква (`EURUSDT`) суффиксом НЕ считается: это другой
     инструмент. Единственный кандидат - принимается; несколько - `ConfigError` со списком (задайте переопределение);
  затем следующий псевдоним (`BRN` -> `UKOIL`, `BRENT`...). Ничего не найдено - `ConfigError`.
Таблица псевдонимов `DEFAULT_ALIASES` - предположение о типичных именах брокеров (❓ у вашего брокера могут быть другие): проверяйте
`python -m scripts.test_mt5_connection --symbol ...` и фиксируйте результат переопределением.

Класс актива (защита от «тёзок»). Имя ещё не гарантирует инструмент: у реального брокера (проверено живьём 2026-10-13) `BRN` - акция
Barnwell Industries, `GOLD` - акция Barrick Gold, `WTI` - акция W&T Offshore, хотя система ждёт нефть Brent и золото. Поэтому кандидат
принимается, только если `path` его спецификации (`Forex\\EURUSD`, `Metals\\XAUUSD`, `Indexes\\US500`) соответствует ожидаемому классу
(`ASSET_HINTS`; для 6-буквенных валютных пар - `forex`), а в пути нет сегментов `Stock` / `ETF`. Не прошёл - берётся следующий
кандидат; никого не осталось - `ConfigError` со списком отвергнутых. Явное переопределение (`MT5_SYMBOL_MAP`) проверку не проходит:
это осознанное решение человека.

Спецификации. После разрешения имени запрашивается `get_symbol_specification` (MetaApi SDK) и кешируется в `Mt5SymbolSpec`:
`contractSize`, `minVolume`, `maxVolume`, `volumeStep`, `tickSize`, `digits`, `point`, `stopsLevel`, `freezeLevel`, `fillingModes`,
`tradeMode`, валюты. ВАЖНО: в спецификации MetaApi нет полей `tradeTickSize` / `tradeTickValue` (это поля MT5 API терминала); размер
тика - `tickSize`, а стоимость тика в валюте счёта приходит вместе с котировкой (`lossTickValue` / `profitTickValue` у `get_price`),
её использует `exchange.mt5_risk`. Символ с `tradeMode = SYMBOL_TRADE_MODE_DISABLED` отклоняется при загрузке.
"""

import logging
import os
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from utils.errors import ConfigError

log = logging.getLogger(__name__)

SYMBOL_MAP_ENV = "MT5_SYMBOL_MAP"
MAX_SUFFIX_LOWER = 5
# Типичные имена у брокеров (❓ предположение, не проверено на вашем брокере): внутреннее имя -> базовые имена по приоритету.
DEFAULT_ALIASES: dict[str, tuple[str, ...]] = {
    "XAUUSD": ("XAUUSD", "GOLD"),
    "XAGUSD": ("XAGUSD", "SILVER"),
    "BRN": ("BRN", "UKOIL", "BRENT", "XBRUSD", "UKBRENT"),
    "US500": ("US500", "SPX500", "SP500"),
}


# Ожидаемый класс актива: внутреннее имя -> подстроки пути (path) спецификации; хотя бы одна обязана присутствовать (в нижнем регистре).
# Для 6-буквенных пар вне таблицы - `forex`. Имя вне таблицы и не пара - проверка только на «не акция/ETF».
ASSET_HINTS: dict[str, tuple[str, ...]] = {
    "XAUUSD": ("metal",), "XAGUSD": ("metal",), "XPTUSD": ("metal",), "XPDUSD": ("metal",),
    "BRN": ("commod", "energ", "oil"), "US500": ("index", "indic"),
}
FOREX_HINTS = ("forex", "fx", "currenc")
FORBIDDEN_SEGMENTS = frozenset({"stock", "stocks", "etf", "etfs", "share", "shares"})


def asset_class_mismatch(internal: str, raw: Mapping[str, Any], hints: Mapping[str, tuple[str, ...]] | None = None) -> str | None:
    """Почему спецификация НЕ подходит внутреннему символу (строка), либо None, если подходит. Чистая функция."""
    path = str(raw.get("path") or "")
    low = path.lower()
    segments = [seg for seg in low.replace("/", "\\").split("\\") if seg]
    if not segments:
        return "в спецификации нет path - класс актива не проверить"
    if FORBIDDEN_SEGMENTS.intersection(segments):
        return f"это акция/ETF (path {path!r}), а не {internal}"
    key = internal.upper()
    table = ASSET_HINTS if hints is None else hints
    expected = table.get(key) or (FOREX_HINTS if len(key) == 6 and key.isalpha() else ())
    if expected and not any(h in low for h in expected):
        return f"path {path!r} не похож на ожидаемый класс {expected}"
    return None


class SpecClient(Protocol):
    """Что маппер требует от клиента (его реализует `MetaApiLiveClient`)."""

    async def get_symbols(self) -> list[str]: ...

    async def get_symbol_specification(self, symbol: str) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class Mt5SymbolSpec:
    """Спецификация символа у брокера (только то, что нужно торговле и риску)."""

    internal: str  # внутреннее имя системы
    broker: str  # точное имя у брокера
    contract_size: float  # единиц базового актива в 1 лоте (Forex 100000, золото 100 oz)
    min_volume: float
    max_volume: float
    volume_step: float
    tick_size: float  # минимальный шаг цены
    digits: int
    point: float
    stops_level: float  # минимальная дистанция SL/TP от цены, в пунктах (point)
    freeze_level: float
    filling_modes: tuple[str, ...]  # SYMBOL_FILLING_FOK / _IOC / _BOC / _RETURN
    trade_mode: str
    profit_currency: str
    margin_currency: str
    path: str = ""  # путь в дереве символов брокера: Forex\\EURUSD, Metals\\XAUUSD, Indexes\\US500, Nasdaq\\Stock\\BRN
    description: str = ""

    @property
    def min_stop_distance(self) -> float:
        """Минимальная дистанция до SL в единицах цены."""
        return self.stops_level * self.point


def _num(raw: Mapping[str, Any], key: str, symbol: str, *, positive: bool = True, default: float | None = None) -> float:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{symbol}: в спецификации брокера нет числового поля {key!r}: {value!r}")
    if positive and not value > 0:
        raise ConfigError(f"{symbol}: поле {key!r} спецификации должно быть > 0, получено {value}")
    return float(value)


def parse_spec(internal: str, broker: str, raw: Mapping[str, Any]) -> Mt5SymbolSpec:
    """Строгий разбор сырой спецификации MetaApi; любое нарушение - `ConfigError` (торговать по догадке нельзя)."""
    trade_mode = str(raw.get("tradeMode") or "")
    if trade_mode == "SYMBOL_TRADE_MODE_DISABLED":
        raise ConfigError(f"{broker}: торговля символом запрещена брокером (tradeMode {trade_mode})")
    spec = Mt5SymbolSpec(
        internal=internal, broker=broker,
        contract_size=_num(raw, "contractSize", broker), min_volume=_num(raw, "minVolume", broker), max_volume=_num(raw, "maxVolume", broker),
        volume_step=_num(raw, "volumeStep", broker), tick_size=_num(raw, "tickSize", broker), digits=int(_num(raw, "digits", broker, positive=False)),
        point=_num(raw, "point", broker), stops_level=_num(raw, "stopsLevel", broker, positive=False, default=0),
        freeze_level=_num(raw, "freezeLevel", broker, positive=False, default=0),
        filling_modes=tuple(str(m) for m in (raw.get("fillingModes") or ())), trade_mode=trade_mode,
        profit_currency=str(raw.get("profitCurrency") or ""), margin_currency=str(raw.get("marginCurrency") or ""),
        path=str(raw.get("path") or ""), description=str(raw.get("description") or ""),
    )
    if spec.min_volume > spec.max_volume:
        raise ConfigError(f"{broker}: minVolume {spec.min_volume} > maxVolume {spec.max_volume}")
    return spec


def parse_overrides(text: str) -> dict[str, str]:
    """`"BRN=UKOIL, US500=US500.cash"` -> {"BRN": "UKOIL", "US500": "US500.cash"}. Пустая строка - пусто; кривая пара - `ConfigError`."""
    result: dict[str, str] = {}
    for chunk in (text or "").split(","):
        if not chunk.strip():
            continue
        key, sep, value = chunk.partition("=")
        if not sep or not key.strip() or not value.strip():
            raise ConfigError(f"{SYMBOL_MAP_ENV}: ожидается ВНУТРЕННЕЕ=БРОКЕР, получено {chunk.strip()!r}")
        result[key.strip().upper()] = value.strip()
    return result


def is_suffix_variant(base: str, name: str) -> bool:
    """`name` - базовое имя `base` с суффиксом брокера (`EURUSDm`, `EURUSD.ecn`, `XAUUSD#`), а не другой инструмент (`EURUSDT`)."""
    if len(name) <= len(base) or name[: len(base)].upper() != base.upper():
        return False
    rest = name[len(base):]
    if not rest[0].isalnum():
        return True
    return rest.isalpha() and rest.islower() and len(rest) <= MAX_SUFFIX_LOWER


def candidate_groups(internal: str, available: Collection[str], *, overrides: Mapping[str, str] | None = None,
                     aliases: Mapping[str, tuple[str, ...]] | None = None) -> list[list[str]]:
    """Группы кандидатов по убыванию приоритета: на каждое базовое имя - точное совпадение ИЛИ (если точного нет) все варианты с суффиксом.
    Явное переопределение - единственная группа (обязано существовать у брокера). Чистая функция, без сети."""
    key = internal.upper()
    names = set(available)
    forced = (overrides or {}).get(key)
    if forced is not None:
        if forced not in names:
            raise ConfigError(f"{internal}: переопределение {forced!r} (из {SYMBOL_MAP_ENV}/overrides) отсутствует у брокера")
        return [[forced]]
    groups: list[list[str]] = []
    for base in (aliases if aliases is not None else DEFAULT_ALIASES).get(key, (key,)):
        if base in names:
            groups.append([base])
            continue
        same_case = sorted(n for n in names if n.upper() == base.upper())
        if len(same_case) == 1:
            groups.append(same_case)
            continue
        variants = sorted(n for n in names if is_suffix_variant(base, n))
        if variants:
            groups.append(variants)
    return groups


def _not_found(internal: str, available: Collection[str], aliases: Mapping[str, tuple[str, ...]] | None, rejected: list[str]) -> ConfigError:
    key = internal.upper()
    bases = (aliases if aliases is not None else DEFAULT_ALIASES).get(key, (key,))
    near = sorted(n for n in available if key[:3] in n.upper())[:8]
    extra = f"; отвергнуты по классу актива: {rejected}" if rejected else ""
    return ConfigError(f"{internal}: подходящий символ не найден у брокера (искал {list(bases)}){extra}; похожие: {near}; задайте {SYMBOL_MAP_ENV}={key}=<точное имя>")


def _ambiguous(internal: str, names: list[str]) -> ConfigError:
    return ConfigError(f"{internal}: неоднозначно у брокера: {sorted(names)}; задайте {SYMBOL_MAP_ENV}={internal.upper()}=<точное имя>")


def resolve_name(internal: str, available: Collection[str], *, overrides: Mapping[str, str] | None = None,
                 aliases: Mapping[str, tuple[str, ...]] | None = None) -> str:
    """Точное имя символа у брокера ТОЛЬКО по именам (без проверки класса актива - её делает `SymbolMapper.load` по спецификациям).
    Первая непустая группа кандидатов решает: один - принимается, несколько - `ConfigError` (неоднозначно)."""
    groups = candidate_groups(internal, available, overrides=overrides, aliases=aliases)
    if not groups:
        raise _not_found(internal, available, aliases, [])
    if len(groups[0]) > 1:
        raise _ambiguous(internal, groups[0])
    return groups[0][0]


class SymbolMapper:
    """Кеш «внутреннее имя -> имя брокера -> спецификация». `load()` обязателен до торговли; повторный вызов обновляет кеш."""

    def __init__(self, client: SpecClient, *, overrides: Mapping[str, str] | None = None,
                 aliases: Mapping[str, tuple[str, ...]] | None = None, asset_hints: Mapping[str, tuple[str, ...]] | None = None):
        self._client = client
        self._hints = asset_hints
        self._overrides = {k.upper(): v for k, v in (overrides if overrides is not None else parse_overrides(os.environ.get(SYMBOL_MAP_ENV, ""))).items()}
        self._aliases = aliases
        self._specs: dict[str, Mt5SymbolSpec] = {}

    async def load(self, internals: Collection[str]) -> None:
        """Разрешить имена и загрузить спецификации. Любой символ не найден / спецификация негодна - `ConfigError`, кеш не меняется."""
        available = await self._client.get_symbols()
        loaded: dict[str, Mt5SymbolSpec] = {}
        for internal in internals:
            broker, raw = await self._pick(internal, available)
            loaded[internal] = parse_spec(internal, broker, raw)
            log.info("MT5: %s -> %s (%s; контракт %g, лот %g..%g шаг %g, тик %g)", internal, broker, loaded[internal].path,
                     loaded[internal].contract_size, loaded[internal].min_volume, loaded[internal].max_volume, loaded[internal].volume_step,
                     loaded[internal].tick_size)
        self._specs.update(loaded)

    async def _pick(self, internal: str, available: list[str]) -> tuple[str, Mapping[str, Any]]:
        """Первый подходящий по имени И по классу актива кандидат; неоднозначность и отсутствие - `ConfigError`."""
        groups = candidate_groups(internal, available, overrides=self._overrides, aliases=self._aliases)
        explicit = internal.upper() in self._overrides
        rejected: list[str] = []
        for group in groups:
            passing: list[tuple[str, Mapping[str, Any]]] = []
            for name in group:
                raw = await self._client.get_symbol_specification(name)
                why = None if explicit else asset_class_mismatch(internal, raw, self._hints)
                if why:
                    rejected.append(f"{name}: {why}")
                    continue
                passing.append((name, raw))
            if len(passing) == 1:
                return passing[0]
            if len(passing) > 1:
                raise _ambiguous(internal, [n for n, _ in passing])
        raise _not_found(internal, available, self._aliases, rejected)

    def spec(self, internal: str) -> Mt5SymbolSpec:
        try:
            return self._specs[internal]
        except KeyError:
            raise ConfigError(f"{internal}: символ не загружен в SymbolMapper (сначала await load([...]))") from None

    def broker_name(self, internal: str) -> str:
        return self.spec(internal).broker

    def internal_name(self, broker: str) -> str:
        for spec in self._specs.values():
            if spec.broker == broker:
                return spec.internal
        raise ConfigError(f"{broker}: нет в SymbolMapper (позиция чужого символа?)")

    def __contains__(self, internal: str) -> bool:
        return internal in self._specs

    @property
    def internals(self) -> tuple[str, ...]:
        return tuple(self._specs)
