"""Разбор закрытой сделки (Trade Retro): оценка 1..10 и текстовые выводы по геометрии сделки. Без ML - только арифметика.

Вход - то, что уже есть у клиента: строка сделки (`r_multiple`, `mfe_r`, `mae_r`, `plan`, `exit_reason`, цены), список закрытий и свечи после
выхода (если удалось докачать; без них вывод «стоп был слишком коротким» не делается). Выход - `{score, grade, headline, points[]}`;
`points` - короткие фразы `{tone: good|bad|neutral, text}`. Оценка оценивает КАЧЕСТВО отработки сделки, а не «правоту»: убыток по
сетапу, который сразу пошёл против входа, - это 2-3 из 10, а не 0.
"""

from typing import Any

from tradeai_client import fmt

MIN_SCORE, MAX_SCORE = 1, 10
BE_EPS = 0.05  # |итог| ≤ 0.05R - сделка закрыта «в ноль»


def _num(trade: dict[str, Any], key: str) -> float | None:
    v = trade.get(key)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _went_to_target_after(trade: dict[str, Any], bars_after_exit: list[dict[str, Any]]) -> bool:
    """Цена после выхода дошла до TP сделки (или до +3R от входа, если TP не задан)."""
    entry, sl, tp = _num(trade, "entry_price"), _num(trade, "sl"), _num(trade, "tp")
    if entry is None or sl is None or not bars_after_exit:
        return False
    long = trade["side"] == "long"
    target = tp if tp is not None else entry + (3 if long else -3) * abs(entry - sl)
    return max(b["h"] for b in bars_after_exit) >= target if long else min(b["l"] for b in bars_after_exit) <= target


def review_trade(trade: dict[str, Any], closes: list[dict[str, Any]], bars_after_exit: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    """None - сделка не закрыта (разбирать нечего)."""
    r = _num(trade, "r_multiple")
    if trade.get("status") != "closed" or r is None:
        return None
    mfe = _num(trade, "mfe_r")
    mae = abs(_num(trade, "mae_r") or 0.0) if _num(trade, "mae_r") is not None else None
    reasons = [c["reason"] for c in closes]
    exit_reason = trade.get("exit_reason") or (reasons[-1] if reasons else None)
    points: list[dict[str, str]] = []
    score = 5

    def add(tone: str, text: str, delta: int = 0) -> None:
        nonlocal score
        score += delta
        points.append({"tone": tone, "text": text})

    # 1) итог
    if r >= 2.0:
        add("good", f"Итог {fmt.fmt_r(r)}: сделка отработала с запасом.", 3)
    elif r >= 1.0:
        add("good", f"Итог {fmt.fmt_r(r)}: цель плана достигнута.", 2)
    elif r > BE_EPS:
        add("good", f"Небольшой плюс {fmt.fmt_r(r)}.", 1)
    elif abs(r) <= BE_EPS:
        add("neutral", "Закрыта в ноль: капитал сохранён, прибыли нет.")
    elif r <= -0.9:
        add("bad", f"Полный убыток {fmt.fmt_r(r)}: стоп сработал на весь объём.", -2)
    else:
        add("bad", f"Убыток {fmt.fmt_r(r)} меньше полного стопа.", -1)

    # 2) качество входа: насколько цена сразу пошла против
    if mae is not None:
        if mae < 0.3:
            add("good", f"Точный вход: просадка по сделке всего {mae:.2f}R.", 1)
        elif mae >= 0.8 and r > BE_EPS:
            add("bad", f"Ранний вход: цена прошла {mae:.2f}R против позиции, прежде чем пошла в сторону сделки.", -1)
        elif mae >= 0.8 and r < 0:
            add("bad", f"Вход оказался ранним: цена сразу пошла против ({mae:.2f}R просадки).")

    # 3) что сделала цена в плюсе
    if mfe is not None:
        if r > BE_EPS and mfe > 0:
            share = r / mfe
            if share >= 0.8:
                add("good", f"Выжато {share * 100:.0f}% хода цены (максимум {mfe:.2f}R).", 1)
            elif mfe >= 1.5 and share < 0.4:
                add("bad", f"Отдана большая часть хода: цена доходила до {mfe:.2f}R, забрано {fmt.fmt_r(r)}.", -1)
        if r < 0:
            if mfe >= 1.0:
                add("bad", f"Была в плюсе до {mfe:.2f}R, но закрылась по стопу: защита прибыли не сработала.", -1)
            elif mfe < 0.3:
                add("bad", "Цена почти не пошла в сторону сделки (максимум в плюсе " + f"{mfe:.2f}R): сигнал не подтвердился.")

    # 4) особые случаи по причине выхода
    levels_hit = {x for x in reasons if x.startswith("scale_")}
    if exit_reason in ("trail", "tp") and len(levels_hit) >= 2:
        add("good", "Идеальная отработка Scale Out: оба уровня взяты, остаток закрыт трейлингом." if exit_reason == "trail"
            else "Идеальная отработка Scale Out: уровни взяты, остаток дошёл до тейка.", 1)
    elif exit_reason == "be" and levels_hit:
        add("neutral", "После частичной фиксации стоп в безубытке закрыл остаток: прибыль защищена, продолжения не было.")
    elif exit_reason == "timeout":
        add("bad", "Таймаут: за отведённое время цена не определилась.", -1)
    elif exit_reason == "sl" and r < 0 and bars_after_exit is not None and _went_to_target_after(trade, bars_after_exit):
        add("bad", "Слишком короткий стоп: цена сбила SL и затем ушла к цели - идея была верной, запаса не хватило.", -1)

    score = max(MIN_SCORE, min(MAX_SCORE, score))
    grade = "отлично" if score >= 8 else "хорошо" if score >= 6 else "средне" if score >= 4 else "слабо"
    return {"score": score, "grade": grade, "headline": f"{score}/10 · {grade}", "points": points}
