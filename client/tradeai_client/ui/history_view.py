"""Вкладка History: таблица сделок (GET /api/trades). Двойной клик по строке (или кнопка в строке) открывает окно графика сделки.
Звёздочка в строке - «Избранное» (эталонный сетап, общий для профилей SANTA и CHR). Тот же класс с `favorites=True` - вкладка «Избранное»:
только отмеченные звёздочкой, все режимы, без профильных фильтров (лучшие сделки одного пользователя видны другому как примеры)."""

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

PAGE = 50
COMPACT_PX = 768  # уже этого (page.width) - телефон: вместо широкой таблицы вертикальные карточки
COINS = fmt.COINS
STATUS_FILTERS = {"all": ("Все", None), "closed": ("Закрытые", ["closed"]), "active": ("Открытые и ждущие", ["open", "pending_limit", "pending_market"]),
                  "cancelled": ("Отменённые", ["cancelled"]), "aborted": ("Прерванные", ["aborted"])}
COLS = [("Сигнал", 96), ("Монета", 78), ("Стратегия", 110), ("Режим", 70), ("Сторона", 70), ("Статус", 170), ("Вход", 96), ("Выход", 96), ("Итог R", 80),
        ("PnL USDT", 84), ("Выход по", 110), ("★", 44), ("", 44)]
WIDTHS = [x for _, x in COLS]
TABLE_W = sum(WIDTHS) + 8 * (len(WIDTHS) - 1) + 24  # ширина таблицы = сумма колонок + промежутки + отступы строки: таблица НЕ сжимается под экран, а листается по горизонтали


class HistoryView:
    def __init__(self, host, *, favorites: bool = False) -> None:
        self.host = host
        self.favorites = favorites  # вкладка «Избранное»: только звёздочные сделки, профильные фильтры не применяются
        self._offset, self._total = 0, 0
        self._selected: str | None = None
        self._items: list[dict] = []
        self._favorite_busy = False
        self._server_coins: list[str] = []
        self._boxes: dict[str, ft.Container] = {}  # id сделки -> контейнер строки (подсветка выбранной без пересборки таблицы)
        self._status = w.FilterSelect("Статус", [(k, v[0]) for k, v in STATUS_FILTERS.items()], self._filter_changed, host, width=190)
        self._coin = w.FilterSelect("Монеты", [(c, fmt.coin_short(c)) for c in COINS], self._filter_changed, host, width=190)
        self._strategy = w.FilterSelect("Стратегия", fmt.STRATEGY_OPTIONS, self._filter_changed, host, width=230)
        self._mode = w.FilterSelect("Режим", fmt.MODE_OPTIONS, self._mode_changed, host, width=180)
        self._bar = w.FilterBar([self._mode, self._status, self._coin, self._strategy], self._filter_changed)
        saved = getattr(self.host, "saved_mode", "")  # последний выбор пользователя; пока его нет, режим следует за активным контуром
        self._mode.value, self._mode_touched = saved or self.host.default_mode, bool(saved)
        if favorites:
            self._mode.value, self._mode.visible = "all", False  # избранное показывает сделки обоих контуров
        self._info = ft.Text("", size=12, color=w.MUTED)
        self._prev = ft.IconButton(icon=ft.Icons.ARROW_BACK, tooltip="Новее", on_click=self._prev_page, disabled=True)
        self._next = ft.IconButton(icon=ft.Icons.ARROW_FORWARD, tooltip="Старше", on_click=self._next_page, disabled=True)
        self._rows = ft.Column(spacing=2)
        self._busy = ft.ProgressRing(width=18, height=18, stroke_width=2, visible=False)
        self._profile_coins: list[str] | None = None  # монеты, где у стратегий профиля есть сделки (CHR); None - ещё не загружены
        self._fav_btn = ft.OutlinedButton("Избранное", icon=ft.Icons.STAR_BORDER, on_click=self._toggle_favorites,
                                          tooltip="Показать только сделки со звёздочкой (эталонные сетапы)")
        header = w.table_row([w.cell(t, color=w.MUTED, size=12, align_right=t in ("Итог R", "PnL USDT")) for t, _ in COLS], widths=WIDTHS, height=28)
        tap = getattr(host, "tap_opens_chart", False)
        filter_controls = [self._mode.control, self._status.control, self._coin.control, self._strategy.control, self._bar.control, self._fav_btn,
                           ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Обновить", on_click=self._reload), self._busy]
        pager = [self._info, self._prev, self._next]
        self._table = ft.Row([ft.Column([header, ft.Column([self._rows], scroll=ft.ScrollMode.AUTO, expand=True)], spacing=6, width=TABLE_W)],
                             scroll=ft.ScrollMode.ADAPTIVE, expand=True, vertical_alignment=ft.CrossAxisAlignment.STRETCH)  # одна горизонтально прокручиваемая область: заголовок и строки едут вместе
        self._cards = ft.Column(spacing=8, scroll=ft.ScrollMode.AUTO, expand=True, visible=False)  # телефон: вертикальные карточки, без горизонтального скролла
        self._compact = False
        table = ft.Column([self._table, self._cards], expand=True, spacing=0)  # видна одна из двух раскладок
        if tap:  # телефон: фильтры свёрнуты (иначе они занимают весь экран и строк не видно), «Фильтры» раскрывает; пагинация рядом с кнопкой
            self._filters_box = ft.Container(content=ft.Row(filter_controls, spacing=10, run_spacing=6, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER), visible=False)
            self._filters_btn = ft.TextButton("Фильтры", icon=ft.Icons.FILTER_LIST, on_click=self._toggle_filters)
            self.control = ft.Column([self._filters_btn, self._filters_box, ft.Row(pager, spacing=6, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER), table], expand=True, spacing=6)
        else:
            self.control = ft.Column([
                ft.Container(height=2),
                ft.Row([*filter_controls, *pager], spacing=10, run_spacing=6, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                table,
            ], expand=True, spacing=10)
        self.apply_profile()
        self.restore_state(getattr(host, "view_state", lambda _n: None)("history"))

    # ------------------------------------------------------------------------------------------ память фильтров (1.6.0)

    def state(self) -> dict:
        return {"mode": self._mode.state(), "status": self._status.state(), "coin": self._coin.state(), "strategy": self._strategy.state(),
                "multi": bool(self._bar.switch.value), "favorites": self.favorites}

    def restore_state(self, st: dict | None) -> None:
        """Фильтры, выбранные в прошлый раз (переживают перезапуск, переустановку и перезагрузку страницы)."""
        if not isinstance(st, dict):
            return
        self._bar.set(bool(st.get("multi")))
        self._pending_coin = st.get("coin")  # CHR: варианты монет приходят с сервера позже - выбор вернётся после загрузки
        for key, f in (("status", self._status), ("coin", self._coin), ("strategy", self._strategy)):
            f.restore(st.get(key))
        if not self.favorites and isinstance(st.get("mode"), dict) and self._mode.visible:
            self._mode.restore(st["mode"])
            self._mode_touched = True
        self._set_favorites(bool(st.get("favorites")))

    def _remember(self) -> None:
        remember = getattr(self.host, "remember_view", None)
        if remember is not None:
            remember("history", self.state())

    def _set_favorites(self, on: bool) -> None:
        self.favorites = on
        self._fav_btn.icon = ft.Icons.STAR if on else ft.Icons.STAR_BORDER
        self._fav_btn.style = ft.ButtonStyle(color=w.AMBER if on else None, side=ft.BorderSide(1, w.AMBER) if on else None)
        self._mode.visible = not on and self._scope()[0] is None

    async def _toggle_favorites(self, _=None) -> None:
        """«Избранное» - кнопка в History (1.6.0, вместо отдельной вкладки): только сделки со звёздочкой обоих контуров, профильные фильтры не применяются."""
        self._set_favorites(not self.favorites)
        self._offset = 0
        self._remember()
        if w.attached(self.control):
            self.control.update()
        await self.refresh()

    # ------------------------------------------------------------------------------------------ профиль и фильтры

    def _scope(self) -> tuple[list[str] | None, list[str] | None]:
        """(стратегии, монеты) профиля; у вкладки «Избранное» ограничения нет."""
        if self.favorites:
            return None, None
        return getattr(self.host, "profile_strategies", lambda: None)(), getattr(self.host, "profile_coins", lambda: None)()

    def apply_profile(self) -> None:
        """Профиль сменился (или только что открыт): варианты стратегий и монет, видимость режима."""
        strategies, pool = self._scope()
        options = fmt.STRATEGY_OPTIONS if strategies is None else [("all", "Все")] + [(k, fmt.strategy_label(k)) for k in strategies]
        self._strategy.set_options(options, select_all=True)
        self._profile_coins = None
        if pool is not None:
            self._coin.set_options([(c, fmt.coin_short(c)) for c in pool], select_all=True)
        elif strategies is not None:  # профиль CHR: монеты - только те, где у его стратегий есть сделки (загружаются с сервера при обновлении)
            self._coin.set_options([], select_all=True)
        else:
            self._coin.set_options(self._coin_options(), select_all=True)
        if not self.favorites:
            self._mode.visible = strategies is None  # CHR: только Shadow-стратегии, переключатель режима не нужен

    def _coin_options(self) -> list[tuple[str, str]]:
        return [(c, fmt.coin_short(c)) for c in dict.fromkeys([*COINS, *self._server_coins])]

    def set_coins(self, coins: list[str]) -> None:
        """Монеты с сервера (автоселектор подключает новые на лету): фильтр «Монеты» получает их вместе с прежними; выбор сохраняется. Профиль CHR держит свой пул."""
        self._server_coins = list(coins)
        strategies, pool = self._scope()
        if pool is None and strategies is None:
            self._coin.set_options(self._coin_options())

    def _query_filters(self) -> tuple[list[str] | None, list[str] | None, str]:
        strategies, pool = self._scope()
        picked_coins = self._coin.keys()
        coins = picked_coins if picked_coins is not None else pool
        mode = "all" if self.favorites else ("shadow" if strategies is not None else self._mode_query())
        return (self._strategy.keys() or strategies), coins, mode

    def _mode_query(self) -> str:
        """Параметр `mode` API (shadow | live | all) из фильтра «Режим»: в мультивыборе оба контура = all."""
        keys = self._mode.keys()
        return "all" if keys is None or len(keys) != 1 else keys[0]

    def _statuses(self) -> list[str] | None:
        """Статусы запроса: группы фильтра («Закрытые», «Открытые и ждущие»...) объединяются; None - без фильтра."""
        keys = self._status.keys()
        if keys is None:
            return None
        return list(dict.fromkeys(s for k in keys for s in (STATUS_FILTERS[k][1] or [])))

    def _toggle_filters(self, _=None) -> None:
        self._filters_box.visible = not self._filters_box.visible
        self._filters_btn.content = "Скрыть фильтры" if self._filters_box.visible else "Фильтры"
        self._filters_box.update()
        self._filters_btn.update()

    async def _filter_changed(self, _=None) -> None:
        self._offset = 0
        self._remember()
        await self.refresh()

    def _mode_choice(self) -> str:
        """Режим для запоминания: один выбранный контур, иначе all."""
        keys = self._mode.keys()
        return keys[0] if keys and len(keys) == 1 else "all"

    async def _mode_changed(self, _=None) -> None:
        self._mode_touched = True
        self.host.remember_mode(self._mode_choice())
        self._remember()
        await self._filter_changed()

    def follow_default_mode(self, mode: str) -> bool:
        """Активный контур сменился (боевой счёт появился/пропал): переключить фильтр, если пользователь его не трогал. True - значение изменилось."""
        if self.favorites or self._mode_touched or self._mode.value == mode:
            return False
        self._mode.value = mode
        return True

    async def _reload(self, _=None) -> None:
        await self.refresh()

    async def _prev_page(self, _=None) -> None:
        self._offset = max(0, self._offset - PAGE)
        await self.refresh()

    async def _next_page(self, _=None) -> None:
        self._offset += PAGE
        await self.refresh()

    async def _load_profile_coins(self, client) -> None:
        """CHR: варианты фильтра «Монеты» - монеты, где у его стратегий есть сделки (сохранённый выбор переживает подгрузку)."""
        strategies = self._scope()[0]
        if strategies is None or self._profile_coins is not None:
            return
        try:
            self._profile_coins = await client.trade_coins(strategies)
        except ApiError:
            return
        keep = getattr(self, "_pending_coin", None) or self._coin.state()
        self._coin.set_options([(c, fmt.coin_short(c)) for c in self._profile_coins], select_all=True)
        self._coin.restore(keep)
        self._pending_coin = None

    async def refresh(self) -> None:
        client = self.host.client
        if client is not None and not self.favorites:
            await self._load_profile_coins(client)
        if client is None:
            return
        if any(f.is_empty for f in self._bar.filters):  # «Снять всё» в каком-то фильтре: ничего не выбрано - ничего и не показываем (запрос без фильтра вернул бы всё)
            self._items, self._total, self._offset = [], 0, 0
            self._rebuild()
            self._info.value, self._prev.disabled, self._next.disabled = "0–0 из 0", True, True
            self.control.update()
            return
        strategies, coins, mode = self._query_filters()
        self._busy.visible = True
        self.control.update()
        try:
            data = await client.trades(limit=PAGE, offset=self._offset, statuses=self._statuses(), coins=coins,
                                       strategies=strategies, mode=mode, favorite=self.favorites)
        except ApiError as exc:
            self._rows.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self._busy.visible = False
            self.control.update()
            return
        self._items, self._total = data["items"], data["total"]
        if self._offset >= self._total > 0:  # последняя страница опустела (фильтр сузился): вернуться на последнюю непустую
            self._offset = (self._total - 1) // PAGE * PAGE
            return await self.refresh()
        self._rebuild()
        first, last = (self._offset + 1, self._offset + len(self._items)) if self._items else (0, 0)
        self._info.value = f"{first}–{last} из {self._total}"
        self._prev.disabled, self._next.disabled = self._offset == 0, self._offset + PAGE >= self._total
        self._busy.visible = False
        self.control.update()

    # ------------------------------------------------------------------------------------------ строки

    def is_compact(self) -> bool:
        """Узкий экран (page.width < 768): история выводится карточками. Ширина неизвестна (0 / нет страницы) - обычная таблица."""
        width = getattr(getattr(self.host, "page", None), "width", None)
        return isinstance(width, (int, float)) and 0 < width < COMPACT_PX

    def relayout(self) -> None:
        """Экран сменил ширину (поворот телефона, окно): перестроить список в нужной раскладке, если она изменилась."""
        if self.is_compact() != self._compact:
            self._rebuild()
            if w.attached(self.control):
                self.control.update()

    def _rebuild(self) -> None:
        self._boxes.clear()
        empty = "В избранном пока ничего нет: откройте History и нажмите звёздочку у лучшей сделки" if self.favorites else "Сделок по этому фильтру нет"
        self._compact = self.is_compact()
        self._table.visible, self._cards.visible = not self._compact, self._compact
        if self._compact:
            self._cards.controls = [self._card(t) for t in self._items] or [w.label(empty, size=14)]
            self._rows.controls = []
        else:
            self._rows.controls = [self._row(t) for t in self._items] or [w.label(empty, size=14)]
            self._cards.controls = []

    def _card(self, t: dict) -> ft.Control:
        """Компактная карточка сделки для телефона: этаж 1 - дата и статус, этаж 2 - монета, сторона, стратегия, этаж 3 - PnL и R, отдельный ряд кнопок (звёздочка, график)."""
        r, pnl = t["r_multiple"], t["pnl"]
        restart = t["exit_reason"] == "process_restart"
        status = fmt.EXIT_RU.get(t["exit_reason"], t["exit_reason"]) if restart else fmt.STATUS_RU.get(t["status"], t["status"])
        side_color = w.GREEN if t["side"] == "long" else w.RED
        fav = bool(t.get("is_favorite"))
        pnl_text = fmt.DASH if pnl is None else f"{pnl:+.2f} USDT".replace("-", "−")
        head = ft.Row([ft.Text(fmt.fmt_local(t["signal_time"]), size=12, color=w.MUTED, expand=True),
                       ft.Text(status, size=12, color=w.MUTED if t["status"] in ("cancelled", "aborted") else w.TEXT, text_align=ft.TextAlign.RIGHT)],
                      vertical_alignment=ft.CrossAxisAlignment.CENTER)
        who = ft.Row([ft.Text(fmt.coin_short(t["coin"]), size=16, weight=ft.FontWeight.W_700), ft.Text(fmt.SIDE_RU[t["side"]], size=14, color=side_color, weight=ft.FontWeight.W_600),
                      ft.Text(t["mode"].upper(), size=11, color=w.GREEN if t["mode"] == "live" else w.AMBER)], spacing=10, wrap=True, run_spacing=2)
        result = ft.Row([ft.Text(pnl_text, size=16, weight=ft.FontWeight.W_700, color=fmt.r_color(pnl)),
                         ft.Text(fmt.fmt_r(r), size=13, color=fmt.r_color(r))], spacing=12, wrap=True, run_spacing=2)
        buttons = ft.Row([ft.IconButton(icon=ft.Icons.STAR if fav else ft.Icons.STAR_BORDER, icon_color=w.AMBER if fav else w.MUTED, icon_size=22,
                                        tooltip="Убрать из избранного" if fav else "В избранное: идеальный сетап", on_click=lambda e, tr=t: self.host.run(self._toggle_favorite(tr))),
                          ft.IconButton(icon=ft.Icons.CANDLESTICK_CHART, icon_size=22, tooltip="Открыть график" if t["has_snapshot"] else "У сделки нет снапшота",
                                        disabled=not t["has_snapshot"], on_click=lambda e, tr=t: self.host.run(self.host.open_trade_chart(tr)))],
                         spacing=4, alignment=ft.MainAxisAlignment.END)
        body = ft.Column([head, who, ft.Text(fmt.STRATEGY_RU.get(t["strategy_name"], t["strategy_name"]), size=13, color=w.MUTED), result, buttons], spacing=4, tight=True)
        box = w.card(body, padding=12, bgcolor=w.CARD_HI if t["id"] == self._selected else w.CARD)
        if restart:
            box.tooltip = fmt.RESTART_HINT
        self._boxes[t["id"]] = box
        if getattr(self.host, "tap_opens_chart", False):
            return ft.GestureDetector(content=box, on_tap=lambda e, tr=t: (self._select(tr), self.host.run(self.host.open_trade_chart(tr))) if tr["has_snapshot"] else self._select(tr))
        return ft.GestureDetector(content=box, on_tap=lambda e, tr=t: self._select(tr))

    def _row(self, t: dict) -> ft.Control:
        r, pnl = t["r_multiple"], t["pnl"]
        exit_label = fmt.EXIT_RU.get(t["exit_reason"], t["exit_reason"] or fmt.DASH)
        restart = t["exit_reason"] == "process_restart"
        status = fmt.STATUS_RU.get(t["status"], t["status"])  # gap_affected - только в карточке разбора сделки (chart_spec), не в таблице
        if restart:
            status = exit_label  # «Прервана (Рестарт бота)» вместо голого «Прервана»
        side_color = w.GREEN if t["side"] == "long" else w.RED
        fav = bool(t.get("is_favorite"))
        cells = [w.cell(fmt.fmt_local(t["signal_time"])), w.cell(fmt.coin_short(t["coin"]), weight=ft.FontWeight.W_700),
                 w.cell(fmt.STRATEGY_RU.get(t["strategy_name"], t["strategy_name"])),
                 w.cell(t["mode"].upper(), color=w.GREEN if t["mode"] == "live" else w.AMBER, size=12), w.cell(fmt.SIDE_RU[t["side"]], color=side_color),
                 w.cell(status, color=w.MUTED if t["status"] in ("cancelled", "aborted") else w.TEXT),
                 w.cell(fmt.fmt_price(t["entry_price"])), w.cell(fmt.fmt_price(t["exit_price"])),
                 w.cell(fmt.fmt_r(r), color=fmt.r_color(r), weight=ft.FontWeight.W_600, align_right=True),
                 w.cell(fmt.DASH if pnl is None else f"{pnl:+.2f}".replace("-", "−"), color=fmt.r_color(pnl), align_right=True), w.cell(fmt.DASH if restart else exit_label, color=w.MUTED),
                 ft.IconButton(icon=ft.Icons.STAR if fav else ft.Icons.STAR_BORDER, icon_color=w.AMBER if fav else w.MUTED, icon_size=20,
                               tooltip="Убрать из избранного" if fav else "В избранное: идеальный сетап", on_click=lambda e, tr=t: self.host.run(self._toggle_favorite(tr))),
                 ft.IconButton(icon=ft.Icons.CANDLESTICK_CHART, tooltip="Открыть график" if t["has_snapshot"] else "У сделки нет снапшота", icon_size=18,
                               disabled=not t["has_snapshot"], on_click=lambda e, tr=t: self.host.run(self.host.open_trade_chart(tr)))]
        row = w.table_row(cells, widths=WIDTHS, bgcolor=w.CARD_HI if t["id"] == self._selected else w.CARD)
        if restart:
            row.tooltip = fmt.RESTART_HINT
        self._boxes[t["id"]] = row
        if getattr(self.host, "tap_opens_chart", False):  # телефон: двойного тапа нет, один тап открывает график
            return ft.GestureDetector(content=row, on_tap=lambda e, tr=t: (self._select(tr), self.host.run(self.host.open_trade_chart(tr))))
        return ft.GestureDetector(content=row, mouse_cursor=ft.MouseCursor.CLICK, on_tap=lambda e, tr=t: self._select(tr),
                                  on_double_tap=lambda e, tr=t: self.host.run(self.host.open_trade_chart(tr)))

    async def _toggle_favorite(self, t: dict) -> None:
        """Поставить / снять звёздочку (сервер: БД + golden_set). Пока предыдущий клик не закончен, новый игнорируется."""
        if self._favorite_busy:
            return
        self._favorite_busy = True
        try:
            now = await self.host.toggle_favorite(t)
        finally:
            self._favorite_busy = False
        if now is None:
            return
        if self.favorites and not now:
            await self.refresh()  # снятая звёздочка убирает строку из вкладки
        else:
            self._rebuild()
            self.control.update()

    def set_favorite(self, trade_id: str, value: bool) -> None:
        """Звёздочку изменили в другом месте (окно графика / другая вкладка): обновить строку без перезагрузки."""
        hit = next((t for t in self._items if t["id"] == trade_id), None)
        if hit is not None and bool(hit.get("is_favorite")) != value:
            hit["is_favorite"] = value
            self._rebuild()
            self.control.update()

    def _select(self, t: dict) -> None:
        """Подсветка строки. Таблицу НЕ пересобираем: пересоздание GestureDetector между двумя касаниями сорвало бы двойной клик."""
        previous = self._boxes.get(self._selected or "")
        if previous is not None:
            previous.bgcolor = w.CARD
            previous.update()
        self._selected = t["id"]
        box = self._boxes[t["id"]]
        box.bgcolor = w.CARD_HI
        box.update()
