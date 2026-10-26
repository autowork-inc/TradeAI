"""Текстовая причина входа: из имени стратегии, стороны, `meta` снапшота и геометрии. Чистая функция (тесты без БД).

Текст описывает, ЧТО увидел детектор (поля meta пишет `orchestrator/strategies.py`), а не «почему должно сработать»: никаких
прогнозов. Неизвестная стратегия - общий текст со всеми полями meta, чтобы новая стратегия не ломала просмотр сделки.
"""

from typing import Any

SIDE_RU = {"long": "лонг", "short": "шорт"}


def fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    a = abs(v)  # та же шкала, что в client/tradeai_client/fmt.py::price_digits (образ API клиента не содержит; сверяется тестом)
    return f"{v:.{2 if a >= 100 else 3 if a >= 10 else 5 if a >= 0.1 else 6 if a >= 0.01 else 8}f}"


def fmt_ms(ms: Any) -> str:
    """Момент времени в тексте - метка `{ts:<мс UTC>}`: часового пояса сервер в строку не пишет, клиент подставляет время ПК
    (`client/tradeai_client/fmt.py::localize_utc_text`; для MCP - `mcp_server/analytics.py::plain_time_tokens`)."""
    if isinstance(ms, bool) or not isinstance(ms, (int, float)):
        return "—"
    return f"{{ts:{int(ms)}}}"


def _num(meta: dict[str, Any], key: str) -> float | None:
    v = meta.get(key)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _htf(meta: dict[str, Any]) -> tuple[str | None, str | None]:
    """(метка, текст) контекста старшего таймфрейма из meta снапшота; сделки до 2026-10-11 его не имеют -> (None, None)."""
    raw = str(meta.get("htf_interval", "4h"))
    label, tf = meta.get("htf_label"), f"H{raw[:-1]}" if raw.endswith("h") and raw[:-1].isdigit() else raw.upper()  # 4h -> H4
    if not isinstance(label, str):
        return None, None
    return f"{tf}: {label}", label


def _tf(meta: dict[str, Any]) -> str:
    """Таймфрейм тренда в meta сделки: с 2026-10-26 - 1D у всех стратегий; старые сделки - H4 (у них htf_interval нет или 4h)."""
    raw = str(meta.get("htf_interval", "4h"))
    return f"H{raw[:-1]}" if raw.endswith("h") and raw[:-1].isdigit() else raw.upper()


def _smc(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    short = trade["side"] == "short"
    pool, sweep_t, bars = _num(meta, "pool_level"), meta.get("sweep_time"), _num(meta, "bars_since_sweep")
    risk_pct = _num(meta, "risk_pct")
    extreme = "Swing High" if short else "Swing Low"
    htf_tag, htf_label = _htf(meta)
    tf = "H4" if htf_tag is None else htf_tag.split(":")[0]  # H4 у одно-таймфреймового автомата, 1D - у мульти-таймфреймового
    sweep_tf = meta.get("sweep_interval")  # есть только у MTF-сигналов: свип найден на 1h, FVG и CHoCH - на рабочем таймфрейме
    on_sweep_tf = f" на {sweep_tf}" if isinstance(sweep_tf, str) else ""
    on_work_tf = f" на {interval}" if isinstance(sweep_tf, str) else ""
    htf_step = [] if htf_tag is None else [
        f"0. Контекст старшего таймфрейма ({htf_tag}): "
        + ("структура нейтральная (HH+HL / LH+LL нет), фильтр сторону не запрещает." if htf_label == "Neutral"
           else f"{'шорт' if short else 'лонг'} по тренду - сетап против структуры {tf} не открывается.")]
    steps = htf_step + [
        f"1. Свип ликвидности{on_sweep_tf}: цена {'проколола вверх' if short else 'проколола вниз'} уровень последнего неснятого {extreme} {fmt_price(pool)} "
        f"и закрылась обратно {'ниже' if short else 'выше'} него ({fmt_ms(sweep_t)}"
        + (f", {int(bars)} бар. назад" if bars is not None else "") + ").",
        f"2. {'Медвежий' if short else 'Бычий'} FVG{on_work_tf}: импульс {'вниз' if short else 'вверх'} оставил разрыв в цене на трёх свечах.",
        f"3. CHoCH{on_work_tf}: закрытие {'ниже' if short else 'выше'} последнего подтверждённого {'Swing Low' if short else 'Swing High'} "
        f"после свипа - смена характера рынка.",
        f"4. Заявка LIMIT на 50% FVG = {fmt_price(trade.get('planned_entry'))}; SL {fmt_price(trade.get('sl'))} за экстремум свипа с буфером по ATR; "
        f"TP (1:4) {fmt_price(trade.get('tp'))}.",
    ]
    facts = [("Таймфрейм", interval)] + ([] if htf_tag is None else [(f"Тренд {tf} (HTF Bias)", htf_label)]) + [
             *([("Таймфрейм свипа", sweep_tf)] if isinstance(sweep_tf, str) else []),
             ("Уровень пула ликвидности", fmt_price(pool)), ("Время свипа", fmt_ms(sweep_t)),
             ("Баров от свипа до сигнала", "—" if bars is None else str(int(bars))),
             ("Расстояние до SL, % от цены", "—" if risk_pct is None else f"{risk_pct * 100:.2f}"),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _breakout(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    level, spread, vol, atr = _num(meta, "level"), _num(meta, "eqh_spread"), _num(meta, "vol_ratio"), _num(meta, "atr")
    steps = [
        f"1. Поджатие: три подтверждённых Swing High почти на одном уровне {fmt_price(level)}"
        + (f" (разброс {spread * 100:.3f}%)" if spread is not None else "") + " и три строго растущих Swing Low.",
        "2. Пробой: закрытие свечи выше уровня равных хаёв (предыдущее закрытие было не выше).",
        "3. Объём пробойной свечи превышает 2 × SMA20 объёма предыдущих 20 баров"
        + (f" (сейчас ×{vol:.2f})." if vol is not None else "."),
        f"4. Вход MARKET по открытию следующей свечи (факт: {fmt_price(trade.get('entry_price'))}); SL {fmt_price(trade.get('sl'))} под Low 3 с буфером по ATR.",
    ]
    htf_tag, htf_label = _htf(meta)
    facts = [("Таймфрейм", interval)] + ([] if htf_tag is None else [("Тренд H4 (справочно, фильтр не применяется)", htf_label)]) + [
             ("Уровень равных хаёв", fmt_price(level)),
             ("Разброс хаёв, %", "—" if spread is None else f"{spread * 100:.3f}"), ("Объём / SMA20", "—" if vol is None else f"{vol:.2f}"),
             ("ATR(14) на сигнале", fmt_price(atr))]
    return "\n".join(steps), facts


def _entry_line(trade: dict[str, Any]) -> str:
    return f"Вход MARKET по первому тику после закрытия свечи (факт: {fmt_price(trade.get('entry_price'))}); SL {fmt_price(trade.get('sl'))}."


def _counter_trend(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    short = trade["side"] == "short"
    pool, extreme, depth, vol = _num(meta, "pool_level"), _num(meta, "sweep_extreme"), _num(meta, "sweep_depth_atr"), _num(meta, "vol_ratio")
    htf_tag, htf_label = _htf(meta)
    steps = [
        (f"0. Тренд {_tf(meta)}: {htf_label or '—'} - {'шорт' if short else 'лонг'} только по нему; откат СТРОГО против структуры H4"
         + (f" ({meta['h4_label']})" if meta.get("h4_label") else "") + "." if meta.get("h4_label") or _tf(meta) != "H4"
         else f"0. Контртренд: {'шорт' if short else 'лонг'} СТРОГО против структуры H4" + ("" if htf_tag is None else f" ({htf_tag})") + "."),
        f"1. Глубокий свип: цена {'выше' if short else 'ниже'} уровня {fmt_price(pool)} на {'—' if depth is None else f'{depth:.2f}'} ATR "
        f"(экстремум {fmt_price(extreme)}) и закрытие обратно за уровнем.",
        f"2. Аномальный объём разворотной свечи: ×{'—' if vol is None else f'{vol:.2f}'} от SMA20 предыдущих баров.",
        f"3. {_entry_line(trade)} Цель - скальп 1R.",
    ]
    facts = [("Таймфрейм", interval)] + ([] if htf_tag is None else [(f"Тренд {_tf(meta)} (HTF Bias)", htf_label)]) + (
             [("Структура H4 (откат против неё)", meta["h4_label"])] if meta.get("h4_label") else []) + [
             ("Уровень пула ликвидности", fmt_price(pool)), ("Экстремум свипа", fmt_price(extreme)),
             ("Глубина свипа, ATR", "—" if depth is None else f"{depth:.2f}"), ("Объём / SMA20", "—" if vol is None else f"{vol:.2f}"),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _obi_breakout(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    level, obi, delta, vol = _num(meta, "bos_level"), _num(meta, "obi"), _num(meta, "delta"), _num(meta, "vol_ratio")
    via = [n for n, k in (("дисбаланс стакана (OBI)", "via_obi"), ("всплеск дельты ленты", "via_delta")) if meta.get(k)]
    steps = [
        f"1. BOS: закрытие свечи {'выше' if trade['side'] == 'long' else 'ниже'} локального экстремума {fmt_price(level)} (пересечение), импульсная свеча.",
        "2. Подтверждение микроструктурой: " + (" и ".join(via) if via else "—")
        + f" (OBI {'—' if obi is None else f'{obi:+.2f}'}, дельта {'—' if delta is None else f'{delta:+.2f}'}).",
        f"3. {_entry_line(trade)}",
    ]
    facts = [("Таймфрейм", interval), ("Уровень BOS", fmt_price(level)), ("OBI топ-10 (среднее, -1..+1)", "—" if obi is None else f"{obi:+.3f}"),
             ("Дельта ленты (-1..+1)", "—" if delta is None else f"{delta:+.3f}"), ("Объём / SMA20", "—" if vol is None else f"{vol:.2f}"),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _killzones(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    short = trade["side"] == "short"
    hi, lo, session = _num(meta, "asia_high"), _num(meta, "asia_low"), {"london": "London", "new_york": "New York"}.get(meta.get("session"), "—")
    steps = [
        f"1. Диапазон Азии ({{hm:0}}-{{hm:420}}): High {fmt_price(hi)}, Low {fmt_price(lo)}.",
        f"2. Killzone {session}: ложный пробой {'Asia High' if short else 'Asia Low'} - прокол и закрытие обратно в диапазоне.",
        f"3. {_entry_line(trade)}",
    ]
    facts = [("Таймфрейм", interval), ("Сессия", session), ("Asia High", fmt_price(hi)), ("Asia Low", fmt_price(lo)),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _vwap(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    short = trade["side"] == "short"
    vwap, sd, band, k, r_vwap = _num(meta, "vwap"), _num(meta, "sd"), _num(meta, "band"), _num(meta, "sd_mult"), _num(meta, "target_vwap_r")
    steps = [
        f"1. Anchored VWAP суток {fmt_price(vwap)}; цена коснулась {'верхней' if short else 'нижней'} полосы {'—' if k is None else f'{k:g}'}σ = {fmt_price(band)}.",
        f"2. Разворотный свечной паттерн на {interval} ({'медвежье поглощение / падающая звезда' if short else 'бычье поглощение / молот'}).",
        f"3. {_entry_line(trade)} До VWAP ≈ {'—' if r_vwap is None else f'{r_vwap:.2f}'}R.",
    ]
    facts = [("Таймфрейм", interval), ("VWAP", fmt_price(vwap)), ("σ (взвешенное по объёму)", fmt_price(sd)), ("Полоса входа", fmt_price(band)),
             ("До VWAP, R", "—" if r_vwap is None else f"{r_vwap:.2f}"), ("Закрытие от полосы, ATR (допуск 0.2)", _x(_num(meta, "band_dist_atr"), ".2f")),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts

def _x(v: float | None, fmt: str) -> str:
    return "—" if v is None else format(v, fmt)


def _trend_line(meta: dict[str, Any]) -> str:
    label = meta.get("htf_label")
    return f"старший тренд {_tf(meta)}: {label or '—'}"


def _horizontal(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    long = trade["side"] == "long"
    level, touches, window = _num(meta, "level"), meta.get("touches"), meta.get("window")
    steps = [
        f"1. Консолидация {window or '—'} баров: коробка {fmt_price(_num(meta, 'box_lo'))} - {fmt_price(_num(meta, 'box_hi'))} "
        f"({_x(_num(meta, 'box_atr'), '.1f')} ATR), эффективность движения {_x(_num(meta, 'efficiency'), '.2f')} (боковик).",
        f"2. {'Сопротивление' if long else 'Поддержка'} {fmt_price(level)} протестировано {touches if touches is not None else '—'} раз.",
        f"3. Импульсное закрытие {'выше' if long else 'ниже'} уровня, объём x{_x(_num(meta, 'vol_ratio'), '.2f')} к SMA20; {_trend_line(meta)}.",
        f"4. {_entry_line(trade)}",
    ]
    facts = [("Таймфрейм", interval), ("Уровень", fmt_price(level)), ("Касаний уровня", str(touches if touches is not None else "—")),
             ("Высота коробки, ATR", _x(_num(meta, "box_atr"), ".2f")), ("Эффективность (ER)", _x(_num(meta, "efficiency"), ".2f")),
             ("Плотность коробки", _x(_num(meta, "density"), ".2f")), ("Объём / SMA20", _x(_num(meta, "vol_ratio"), ".2f")),
             ("Риск, ATR", _x(_num(meta, "risk_atr"), ".2f")), ("Качество сетапа (0-1)", _x(_num(meta, "quality"), ".2f")),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _trendline(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    long, sweep = trade["side"] == "long", meta.get("mode") == "sweep"
    line = "поддержки" if long == sweep else "сопротивления"
    what = (f"свип линии {line}: прокол и закрытие обратно" if sweep
            else f"закрытие {'выше' if long else 'ниже'} линии {line} (пробой)")
    steps = [
        f"1. Трендовая линия {line} через два свинга: {fmt_price(_num(meta, 'anchor1_price'))} -> {fmt_price(_num(meta, 'anchor2_price'))}, "
        f"пролёт {_x(_num(meta, 'span_bars'), '.0f')} баров, наклон {_x(_num(meta, 'slope_atr'), '+.3f')} ATR/бар, линия не нарушалась закрытиями.",
        f"2. Сигнал: {what} на уровне {fmt_price(_num(meta, 'line_level'))}, объём x{_x(_num(meta, 'vol_ratio'), '.2f')}; {_trend_line(meta)}.",
        f"3. {_entry_line(trade)}",
    ]
    facts = [("Таймфрейм", interval), ("Режим", "Свип (ложный пробой)" if sweep else "Пробой"), ("Линия на сигнале", fmt_price(_num(meta, "line_level"))),
             ("Наклон, ATR за бар", _x(_num(meta, "slope_atr"), "+.3f")), ("Доп. касаний линии", str(meta.get("touches", "—"))),
             ("Объём / SMA20", _x(_num(meta, "vol_ratio"), ".2f")), ("Риск, ATR", _x(_num(meta, "risk_atr"), ".2f")),
             ("Качество сетапа (0-1)", _x(_num(meta, "quality"), ".2f")), ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _chr_common(long: bool, meta: dict[str, Any]) -> tuple[str, str]:
    pts = [fmt_price(v) for v in meta.get("squeeze_points") or [] if isinstance(v, (int, float))]
    squeeze = (f"локальная база {meta.get('comp_bars') or '—'} баров: волатильность затухает (размах свежей половины = {_x(_num(meta, 'comp_decay'), '.0%')} от старой), "
               f"{'минимумы подтягиваются к уровню' if long else 'максимумы опускаются к уровню'} ({' -> '.join(pts) if pts else '—'}), "
               f"экстремумы трутся об уровень (в среднем {_x(_num(meta, 'comp_rub_atr'), '.2f')} ATR), высота базы {_x(_num(meta, 'comp_height_atr'), '.2f')} ATR")
    return squeeze, f"{'Bullish' if long else 'Bearish'} (жёсткий фильтр: только по тренду {_tf(meta)})"


def _chr_horizontal(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    long = trade["side"] == "long"
    level, touches = _num(meta, "level"), meta.get("touches")
    squeeze, trend = _chr_common(long, meta)
    steps = [
        f"0. Тренд {_tf(meta)}: {meta.get('htf_label') or '—'} - {'лонг' if long else 'шорт'} разрешён только по тренду.",
        f"1. {'Сопротивление' if long else 'Поддержка'} {fmt_price(level)}: {touches if touches is not None else '—'} подтверждённых касаний "
        f"за {meta.get('window') or '—'} баров.",
        f"2. Поджатие к уровню: {squeeze}.",
        f"3. Пробой: закрытие {'выше' if long else 'ниже'} уровня полнотелой свечой (тело {_x(_num(meta, 'body_frac'), '.0%')} размаха).",
        f"4. {_entry_line(trade)} Стоп за ближайшим локальным свингом ({fmt_price(_num(meta, 'sl_extreme'))}) − 0.2 ATR, статичный; тейк 1:4 ({fmt_price(trade.get('tp'))}) на весь объём, частичных фиксаций и безубытка нет.",
    ]
    facts = [("Таймфрейм", interval), (f"Тренд {_tf(meta)}", trend), ("Уровень", fmt_price(level)), ("Касаний уровня", str(touches if touches is not None else "—")),
             ("Поджатие", squeeze), ("Тело пробойной свечи", _x(_num(meta, "body_frac"), ".0%")), ("Риск, ATR", _x(_num(meta, "risk_atr"), ".2f")),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def _chr_trendline(trade: dict[str, Any], meta: dict[str, Any], interval: str) -> tuple[str, list[tuple[str, str]]]:
    long = trade["side"] == "long"
    touches = meta.get("touches")
    squeeze, trend = _chr_common(long, meta)
    line = "сопротивления (нисходящая)" if long else "поддержки (восходящая)"
    steps = [
        f"0. Тренд {_tf(meta)}: {meta.get('htf_label') or '—'} - {'лонг' if long else 'шорт'} разрешён только по тренду.",
        f"1. Наклонная линия {line}: {fmt_price(_num(meta, 'anchor1_price'))} -> {fmt_price(_num(meta, 'anchor2_price'))}, "
        f"наклон {_x(_num(meta, 'slope_atr'), '+.3f')} ATR/бар, касаний {touches if touches is not None else '—'}, экстремумы за линию не выходили.",
        f"2. Поджатие к линии: {squeeze}.",
        f"3. Пробой: закрытие {'выше' if long else 'ниже'} линии ({fmt_price(_num(meta, 'line_level'))}) полнотелой свечой "
        f"(тело {_x(_num(meta, 'body_frac'), '.0%')} размаха).",
        f"4. {_entry_line(trade)} Стоп за ближайшим локальным свингом ({fmt_price(_num(meta, 'sl_extreme'))}) − 0.2 ATR, статичный; тейк 1:4 ({fmt_price(trade.get('tp'))}) на весь объём, частичных фиксаций и безубытка нет.",
    ]
    facts = [("Таймфрейм", interval), (f"Тренд {_tf(meta)}", trend), ("Линия на сигнале", fmt_price(_num(meta, "line_level"))),
             ("Наклон, ATR за бар", _x(_num(meta, "slope_atr"), "+.3f")), ("Касаний линии", str(touches if touches is not None else "—")),
             ("Поджатие", squeeze), ("Тело пробойной свечи", _x(_num(meta, "body_frac"), ".0%")), ("Риск, ATR", _x(_num(meta, "risk_atr"), ".2f")),
             ("ATR(14) на сигнале", fmt_price(_num(meta, "atr")))]
    return "\n".join(steps), facts


def build_reason(trade: dict[str, Any], meta: dict[str, Any] | None, geometry: list[dict[str, Any]]) -> dict[str, Any]:
    """trade - строка сделки (ключи strategy_id/strategy_name, side, coin, planned_entry, sl, tp, entry_price). -> {title, summary, facts[], levels[]}."""
    meta = meta or {}
    interval = str(meta.get("interval", "—"))
    name = trade.get("strategy_id") or trade["strategy_name"]
    if name in ("smc", "SMC_Pro_Trend"):  # 'smc' - сделки до мульти-стратегийного релиза (в БД имя не менялось)
        title, (summary, facts) = ("SMC Swing" if name == "smc" else "SMC Pro Trend"), _smc(trade, meta, interval)
    elif name == "breakout":
        title, (summary, facts) = "Breakout Buildup", _breakout(trade, meta, interval)
    elif name == "SMC_Counter_Trend":
        title, (summary, facts) = "SMC Counter Trend", _counter_trend(trade, meta, interval)
    elif name == "OBI_Breakout":
        title, (summary, facts) = "OBI Breakout", _obi_breakout(trade, meta, interval)
    elif name == "ICT_Killzones":
        title, (summary, facts) = "ICT Killzones", _killzones(trade, meta, interval)
    elif name == "VWAP_Reversion":
        title, (summary, facts) = "VWAP Reversion", _vwap(trade, meta, interval)
    elif name == "Horizontal_Breakout":
        title, (summary, facts) = "Horizontal Breakout", _horizontal(trade, meta, interval)
    elif name == "Trendline_Breakout":
        title, (summary, facts) = "Trendline Breakout", _trendline(trade, meta, interval)
    elif name == "Horizontal_Breakout_CHR":
        title, (summary, facts) = "Пробой (CHR)", _chr_horizontal(trade, meta, interval)
    elif name == "Trendline_Breakout_CHR":
        title, (summary, facts) = "Наклонка (CHR)", _chr_trendline(trade, meta, interval)
    else:
        title = name
        summary = "Причина входа для этой стратегии не описана в API; ниже - все параметры сигнала из снапшота."
        facts = [(str(k), fmt_price(v) if isinstance(v, float) else str(v)) for k, v in sorted(meta.items())]
    labels = [str(g.get("label")) for g in geometry if g.get("label")]
    htf_tag = _htf(meta)[0] if name in ("smc", "SMC_Pro_Trend", "SMC_Counter_Trend", "Horizontal_Breakout", "Trendline_Breakout",
                                         "Horizontal_Breakout_CHR", "Trendline_Breakout_CHR") else None
    return {
        "title": f"{title} · {SIDE_RU.get(trade['side'], trade['side'])} · {trade['coin']}" + ("" if htf_tag is None else f" ({htf_tag})"),
        "summary": summary,
        "facts": [{"label": k, "value": v} for k, v in facts],
        "geometry_labels": labels,
    }
