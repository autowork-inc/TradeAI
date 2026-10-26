"""Словарь разметки: понятия, которыми трейдер подписывает рисунки и которыми бот рисует своё.

Встроенные понятия имеют детектор (бот умеет их находить). Свои понятия трейдер добавляет сам: разметка с ними копится в уроках,
но бот их не рисует, пока под понятие не написан детектор (это видно в интерфейсе: «нет детектора»)."""

import re
from dataclasses import asdict, dataclass

KINDS = ("hline", "box", "line", "point", "trade")  # чем рисуется: горизонталь, область, линия (наклонная), точка-маркер, сделка


@dataclass(frozen=True)
class Term:
    id: str
    name: str
    kind: str  # из KINDS: какой инструмент рисования подходит по умолчанию
    color: str
    hint: str
    detector: bool = True
    builtin: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


BUILTIN: tuple[Term, ...] = (
    Term("level", "Уровень", "hline", "#4aa3ff", "Горизонтальная цена, от которой рынок реагировал несколько раз (хаи/лои рядом по цене)"),
    Term("zone", "Зона", "box", "#2bb673", "Ценовая полоса, от которой цена отскакивала: реакция после касаний"),
    Term("range", "Проторговка", "box", "#b07cf0", "Боковик: цена долго ходит в коридоре без направленного движения"),
    Term("consolidation", "Консолидация", "box", "#d9a441", "Сжатие: узкий коридор, размах свечей уменьшается - энергия перед движением"),
    Term("trendline", "Наклонка", "line", "#ffb300", "Наклонная линия по двум и более экстремумам, цена её не закрывала насквозь"),
    Term("breakout", "Пробой", "point", "#4cd1c7", "Закрытие свечи за границей уровня / зоны / проторговки / наклонки"),
    Term("zone_exit", "Выход из зоны", "point", "#9be15d", "После пробоя цена не вернулась в зону несколько свечей - пробой удержан"),
    Term("liquidity", "Ликвидность", "hline", "#e5534b", "Равные хаи / лои: там стоят стопы, цену туда тянет; «свип» - вынос и возврат"),
    Term("high", "High", "point", "#ef9a9a", "Локальный максимум (подтверждённый: справа свечи ниже)"),
    Term("low", "Low", "point", "#80cbc4", "Локальный минимум (подтверждённый: справа свечи выше)"),
    Term("gap", "Гэп", "box", "#f08a4b", "Разрыв: тени свечи до и свечи после импульса не перекрываются (FVG); позже часто заполняется"),
    Term("reversal", "Разворот", "point", "#ff7eb6", "Разворотная свеча у уровня/зоны: длинная тень против движения, закрытие обратно"),
    Term("trend", "Тренд рынка", "line", "#c9d1d9", "Структура хаёв и лоёв: выше-выше = вверх, ниже-ниже = вниз, иначе боковик"),
    Term("volume", "Объём", "point", "#8bb4ff", "Всплеск объёма или его отсутствие с одной стороны (нет покупателей / продавцов)"),
    Term("entry", "Вход", "trade", "#4aa3ff", "Точка входа в сделку", detector=False),
    Term("stop", "Стоп", "trade", "#e5534b", "Где сделка признаётся ошибкой", detector=False),
    Term("target", "Цель", "trade", "#2bb673", "Где фиксировать прибыль", detector=False),
)
BUILTIN_IDS = {t.id for t in BUILTIN}
_ID = re.compile(r"^[a-z0-9_]{2,32}$")
_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")


class VocabError(ValueError):
    """Неверное понятие (текст показывается пользователю)."""


def custom_term(name: str, kind: str, color: str = "#c9d1d9", hint: str = "") -> Term:
    name = (name or "").strip()
    if not 2 <= len(name) <= 40:
        raise VocabError("Название понятия: от 2 до 40 символов")
    if kind not in KINDS:
        raise VocabError(f"Способ рисования: одно из {', '.join(KINDS)}")
    if not _COLOR.match(color or ""):
        color = "#c9d1d9"
    tid = "u_" + re.sub(r"[^a-z0-9_]", "", _translit(name.lower()).replace(" ", "_"))[:28]
    if not _ID.match(tid) or tid == "u_":
        raise VocabError("Название должно содержать буквы или цифры")
    return Term(tid, name, kind, color, (hint or "").strip()[:300], detector=False, builtin=False)


def merge(custom: list[dict]) -> list[Term]:
    """Встроенные + свои (свои с тем же id, что встроенные, игнорируются; битые записи - тоже)."""
    out = list(BUILTIN)
    seen = set(BUILTIN_IDS)
    for raw in custom or []:
        try:
            t = Term(str(raw["id"]), str(raw["name"]), str(raw["kind"]), str(raw.get("color") or "#c9d1d9"), str(raw.get("hint") or ""), False, False)
        except (KeyError, TypeError):
            continue
        if t.id in seen or not _ID.match(t.id) or t.kind not in KINDS:
            continue
        seen.add(t.id)
        out.append(t)
    return out


_TR = dict(zip("абвгдеёжзийклмнопрстуфхцчшщъыьэюя", ["a", "b", "v", "g", "d", "e", "e", "zh", "z", "i", "y", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "f",
                                                   "h", "c", "ch", "sh", "sch", "", "y", "", "e", "yu", "ya"]))


def _translit(s: str) -> str:
    return "".join(_TR.get(ch, ch) for ch in s)
