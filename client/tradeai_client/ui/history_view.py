"""Вкладка History: таблица сделок (GET /api/trades). Двойной клик по строке (или кнопка в строке) открывает окно графика сделки.
Звёздочка в строке - «Избранное» (эталонный сетап, общий для профилей SANTA и CHR). Тот же класс с `favorites=True` - вкладка «Избранное»:
только отмеченные звёздочкой, все режимы, без профильных фильтров (лучшие сделки одного пользователя видны другому как примеры)."""

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

PAGE = 50
COINS = fmt.COINS
STATUS_FILTERS = {"all": ("Все", None), "closed": ("Закрытые", ["closed"]), "active": ("Открытые и ждущие", ["open", "pending_limit", "pending_market"]),
                  "cancelled": ("Отменённые", ["cancelled"]), "aborted": ("Прерванные", ["aborted"])}
COLS = [("Сигнал", 96), ("Монета", 78), ("Стратегия", 110), ("Режим", 70), ("Сторона", 70), ("Статус", 170), ("Вход", 96), ("Выход", 96), ("Итог R", 80),
        ("PnL USDT", 84), ("Выход по", 110), ("★", 44), ("", 44)]
WIDTHS = [x for _, x in COLS]


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
        header = w.table_row([w.cell(t, color=w.MUTED, size=12, align_right=t in ("Итог R", "PnL USDT")) for t, _ in COLS], widths=WIDTHS, height=28)
        hint = ("Эталонные сетапы, отмеченные звёздочкой (общие для всех профилей). Двойной клик по строке — график; звёздочка в строке снимает отметку."
                if favorites else
                "Двойной клик по строке — график сделки (свечи снапшота, вход/выход, SL/TP, зона FVG, причина входа). Звёздочка — пометить сделку как «идеальный сетап» (попадёт во вкладку «Избранное»).")
        self.control = ft.Column([
            ft.Container(height=2),
            ft.Row([self._mode.control, self._status.control, self._coin.control, self._strategy.control, self._bar.control, ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Обновить", on_click=self._reload), self._busy,
                    ft.Container(expand=True), self._info, self._prev, self._next], spacing=10),
            w.label(hint, size=12),
            ft.Row([header], scroll=ft.ScrollMode.AUTO),
            ft.Column([self._rows], scroll=ft.ScrollMode.AUTO, expand=True),
        ], expand=True, spacing=10)
        self.apply_profile()

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
        if pool is not None:
            self._coin.set_options([(c, fmt.coin_short(c)) for c in pool], select_all=True)
        else:
            self._coin.set_options(self._coin_options(), select_all=True)
        if not self.favorites:
            self._mode.visible = strategies is None  # CHR: только Shadow-стратегии, переключатель режима не нужен

    def _coin_options(self) -> list[tuple[str, str]]:
        return [(c, fmt.coin_short(c)) for c in dict.fromkeys([*COINS, *self._server_coins])]

    def set_coins(self, coins: list[str]) -> None:
        """Монеты с сервера (автоселектор подключает новые на лету): фильтр «Монеты» получает их вместе с прежними; выбор сохраняется. Профиль CHR держит свой пул."""
        self._server_coins = list(coins)
        if self._scope()[1] is None:
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

    async def _filter_changed(self, _=None) -> None:
        self._offset = 0
        await self.refresh()

    def _mode_choice(self) -> str:
        """Режим для запоминания: один выбранный контур, иначе all."""
        keys = self._mode.keys()
        return keys[0] if keys and len(keys) == 1 else "all"

    async def _mode_changed(self, _=None) -> None:
        self._mode_touched = True
        self.host.remember_mode(self._mode_choice())
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

    async def refresh(self) -> None:
        client = self.host.client
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

    def _rebuild(self) -> None:
        self._boxes.clear()
        empty = "В избранном пока ничего нет: откройте History и нажмите звёздочку у лучшей сделки" if self.favorites else "Сделок по этому фильтру нет"
        self._rows.controls = [self._row(t) for t in self._items] or [w.label(empty, size=14)]

    def _row(self, t: dict) -> ft.Control:
        r, pnl = t["r_multiple"], t["pnl"]
        exit_label = fmt.EXIT_RU.get(t["exit_reason"], t["exit_reason"] or fmt.DASH)
        restart = t["exit_reason"] == "process_restart"
        status = fmt.STATUS_RU.get(t["status"], t["status"]) + (" ⚠" if t["gap_affected"] else "")
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
