"""Вкладка History: таблица сделок (GET /api/trades). Двойной клик по строке (или кнопка в строке) открывает окно графика сделки."""

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

PAGE = 50
COINS = fmt.COINS
STATUS_FILTERS = {"all": ("Все", None), "closed": ("Закрытые", ["closed"]), "active": ("Открытые и ждущие", ["open", "pending_limit", "pending_market"]),
                  "cancelled": ("Отменённые", ["cancelled"]), "aborted": ("Прерванные", ["aborted"])}
COLS = [("Сигнал", 96), ("Монета", 78), ("Стратегия", 92), ("Сторона", 70), ("Статус", 130), ("Вход", 96), ("Выход", 96), ("Итог R", 80),
        ("PnL USDT", 84), ("Выход по", 110), ("", 44)]
WIDTHS = [x for _, x in COLS]


def _dropdown(label: str, options: list[tuple[str, str]], width: float, on_select) -> ft.Dropdown:
    return ft.Dropdown(label=label, value=options[0][0], options=[ft.DropdownOption(key=k, text=t) for k, t in options], width=width,
                       on_select=on_select, border_color=w.BORDER, bgcolor=w.CARD)


class HistoryView:
    def __init__(self, host) -> None:
        self.host = host
        self._offset, self._total = 0, 0
        self._selected: str | None = None
        self._items: list[dict] = []
        self._boxes: dict[str, ft.Container] = {}  # id сделки -> контейнер строки (подсветка выбранной без пересборки таблицы)
        self._status = _dropdown("Статус", [(k, v[0]) for k, v in STATUS_FILTERS.items()], 190, self._filter_changed)
        self._coin = _dropdown("Монета", [("all", "Все")] + [(c, fmt.coin_short(c)) for c in COINS], 130, self._filter_changed)
        self._strategy = _dropdown("Стратегия", fmt.STRATEGY_OPTIONS, 170, self._filter_changed)
        self._info = ft.Text("", size=12, color=w.MUTED)
        self._prev = ft.IconButton(icon=ft.Icons.ARROW_BACK, tooltip="Новее", on_click=self._prev_page, disabled=True)
        self._next = ft.IconButton(icon=ft.Icons.ARROW_FORWARD, tooltip="Старше", on_click=self._next_page, disabled=True)
        self._rows = ft.Column(spacing=2)
        self._busy = ft.ProgressRing(width=18, height=18, stroke_width=2, visible=False)
        header = w.table_row([w.cell(t, color=w.MUTED, size=12, align_right=t in ("Итог R", "PnL USDT")) for t, _ in COLS], widths=WIDTHS, height=28)
        self.control = ft.Column([
            ft.Container(height=2),
            ft.Row([self._status, self._coin, self._strategy, ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Обновить", on_click=self._reload), self._busy,
                    ft.Container(expand=True), self._info, self._prev, self._next], spacing=10),
            w.label("Двойной клик по строке — график сделки (свечи снапшота, вход/выход, SL/TP, зона FVG, причина входа). Либо кнопка справа в строке.", size=12),
            ft.Row([header], scroll=ft.ScrollMode.AUTO),
            ft.Column([self._rows], scroll=ft.ScrollMode.AUTO, expand=True),
        ], expand=True, spacing=10)

    # ------------------------------------------------------------------------------------------ загрузка

    async def _filter_changed(self, _=None) -> None:
        self._offset = 0
        await self.refresh()

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
        pick = lambda d: None if d.value in (None, "all") else [d.value]
        self._busy.visible = True
        self.control.update()
        try:
            data = await client.trades(limit=PAGE, offset=self._offset, statuses=STATUS_FILTERS[self._status.value or "all"][1], coins=pick(self._coin),
                                       strategies=pick(self._strategy))
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
        self._rows.controls = [self._row(t) for t in self._items] or [w.label("Сделок по этому фильтру нет", size=14)]

    def _row(self, t: dict) -> ft.Control:
        r, pnl = t["r_multiple"], t["pnl"]
        exit_label = fmt.EXIT_RU.get(t["exit_reason"], t["exit_reason"] or fmt.DASH)
        status = fmt.STATUS_RU.get(t["status"], t["status"]) + (" ⚠" if t["gap_affected"] else "")
        side_color = w.GREEN if t["side"] == "long" else w.RED
        cells = [w.cell(fmt.fmt_local(t["signal_time"])), w.cell(fmt.coin_short(t["coin"]), weight=ft.FontWeight.W_700),
                 w.cell(fmt.STRATEGY_RU.get(t["strategy_name"], t["strategy_name"])), w.cell(fmt.SIDE_RU[t["side"]], color=side_color),
                 w.cell(status, color=w.MUTED if t["status"] in ("cancelled", "aborted") else w.TEXT),
                 w.cell(fmt.fmt_price(t["entry_price"])), w.cell(fmt.fmt_price(t["exit_price"])),
                 w.cell(fmt.fmt_r(r), color=fmt.r_color(r), weight=ft.FontWeight.W_600, align_right=True),
                 w.cell(fmt.DASH if pnl is None else f"{pnl:+.2f}".replace("-", "−"), color=fmt.r_color(pnl), align_right=True), w.cell(exit_label, color=w.MUTED),
                 ft.IconButton(icon=ft.Icons.CANDLESTICK_CHART, tooltip="Открыть график" if t["has_snapshot"] else "У сделки нет снапшота", icon_size=18,
                               disabled=not t["has_snapshot"], on_click=lambda e, tr=t: self.host.run(self.host.open_trade_chart(tr)))]
        row = w.table_row(cells, widths=WIDTHS, bgcolor=w.CARD_HI if t["id"] == self._selected else w.CARD)
        self._boxes[t["id"]] = row
        return ft.GestureDetector(content=row, mouse_cursor=ft.MouseCursor.CLICK, on_tap=lambda e, tr=t: self._select(tr),
                                  on_double_tap=lambda e, tr=t: self.host.run(self.host.open_trade_chart(tr)))

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
