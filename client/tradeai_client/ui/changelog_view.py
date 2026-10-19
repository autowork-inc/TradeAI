"""Оформление истории версий: карточка релиза (номер, дата, заголовок, список изменений с цветными метками) для окон «Приложение обновлено», «История обновлений»
и окна предложения обновления. Данные - tradeai_client.changelog (без Flet, тестируется отдельно)."""

import flet as ft

from tradeai_client import changelog
from tradeai_client.ui import widgets as w

TYPE_COLORS = {"new": w.GREEN, "improve": w.ACCENT, "fix": w.AMBER}


def change_row(c: changelog.Change) -> ft.Control:
    return ft.Row([ft.Container(content=ft.Text(changelog.TYPES[c.type], size=10, weight=ft.FontWeight.W_700, color=w.BG, no_wrap=True), width=78, height=20,
                                bgcolor=TYPE_COLORS[c.type], border_radius=10, alignment=ft.Alignment.CENTER),
                   ft.Text(c.text, size=13, color=w.TEXT, expand=True, selectable=True)],
                  spacing=10, vertical_alignment=ft.CrossAxisAlignment.START)


def release_card(r: changelog.Release, *, current: bool = False) -> ft.Control:
    head = [ft.Text(f"v{r.version}", size=18, weight=ft.FontWeight.W_700, color=w.TEXT)]
    if r.title:
        head.append(ft.Text(r.title, size=13, color=w.MUTED))
    if r.date:
        head.append(ft.Text(r.date, size=12, color=w.MUTED))
    if current:
        head.append(w.pill("текущая", w.GREEN, filled=True))
    body = ft.Column([ft.Row(head, spacing=10, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER), *[change_row(c) for c in r.changes]], spacing=8)
    return w.card(body, bgcolor=w.CARD_HI if current else w.CARD, border_color=w.GREEN if current else w.BORDER)


def releases_list(releases: list[changelog.Release], current_version: str, *, height: float = 460) -> ft.Control:
    """Полная история со скроллом (новые версии сверху); текущая отмечена."""
    cards = [release_card(r, current=changelog.release_for([r], current_version) is not None) for r in releases] or [w.label("История версий пуста", size=14)]
    return ft.Container(width=600, height=height, content=ft.Column(cards, spacing=12, scroll=ft.ScrollMode.AUTO))
