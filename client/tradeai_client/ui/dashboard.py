"""Вкладка Dashboard: карточки монет с состоянием автомата сетапа SMC (S0..S4), открытые заявки/позиции и лента событий. Живые данные - WebSocket."""

import flet as ft

from tradeai_client import fmt
from tradeai_client.ui import widgets as w

SIDE_ARROW = {"short": "▼", "long": "▲"}
# колонок на ширине окна (ResponsiveRow, 12 колонок): крипта 4 в ряд (4x2 для 8 монет), MT5 - 3 в ряд; узкое окно - 2 / 1
CRYPTO_COL = {"xs": 12, "sm": 6, "md": 3}
MT5_COL = {"xs": 12, "sm": 6, "md": 4}


def split_coins(coins: list[dict]) -> tuple[list[dict], list[dict]]:
    """Крипта (Binance, тикеры на USDT) и Forex / металлы (MT5: EURUSD, XAUUSD, XAGUSD). Любой не-USDT символ - в MT5-группу."""
    crypto = [c for c in coins if c["coin"].endswith("USDT")]
    return crypto, [c for c in coins if not c["coin"].endswith("USDT")]


class DashboardView:
    def __init__(self, host=None) -> None:
        self.host = host  # Shell: профиль (profile_strategies / profile_coins); без него - без ограничений
        self._cards = ft.Column(spacing=10)
        self._events = ft.Column(spacing=2, tight=True)
        self._head = ft.Row(spacing=12)
        self._error = ft.Container(visible=False)
        self.control = ft.Column(
            [self._head, self._error, self._cards, ft.Container(height=6), w.title("События", 15), w.card(self._events, padding=10)],
            expand=True, scroll=ft.ScrollMode.AUTO, spacing=14)
        self.show_placeholder("Ожидание данных от сервера…")

    # ------------------------------------------------------------------------------------------ состояние

    def show_placeholder(self, text: str) -> None:
        self._cards.controls = [w.label(text, size=14)]

    def on_message(self, msg: dict) -> None:
        kind = msg.get("type")
        if kind == "state":
            self._error.visible = False
            self._render(msg)
        elif kind == "error":
            text = {"database_unavailable": "Сервер не может прочитать базу данных (postgres/shadow-run недоступны?).",
                    "data_integrity": "В базе найдены нечитаемые данные: смотрите лог API (deploy/shadow.sh logs api)."}.get(msg.get("detail"), str(msg.get("detail")))
            self._error.content, self._error.visible = w.banner(text, w.RED, ft.Icons.ERROR), True
        # ping - просто подтверждение, что связь жива

    def _render(self, s: dict) -> None:
        shadow = s["shadow"]
        alive, age = shadow["alive"], shadow["heartbeat_age_s"]
        pulse = ("Бот: нет данных о пульсе" if alive is None else
                 f"Бот жив · пульс {fmt.fmt_age(age)}" if alive else f"Бот НЕ отвечает (процесс остановлен или завис) · пульс {fmt.fmt_age(age)}")
        pulse_color = w.MUTED if alive is None else (w.GREEN if alive else w.RED)
        self._head.controls = [w.dot(pulse_color), ft.Text(pulse, color=pulse_color, size=13, weight=ft.FontWeight.W_600),
                               ft.Text(f"обновлено {fmt.fmt_local(s['server_time'])}", color=w.MUTED, size=12)]
        strategies = getattr(self.host, "profile_strategies", lambda: None)()
        pool = getattr(self.host, "profile_coins", lambda: None)()
        shown = [c for c in s["coins"] if pool is None or c["coin"] in pool]
        if strategies is not None:  # профиль со своим набором стратегий: автомат SMC (S0..S4) и чужие позиции не показываются
            shown = [{**c, "setups": [], "positions": [p for p in c["positions"] if p["strategy_name"] in strategies]} for c in shown]
        crypto, mt5 = split_coins(shown)
        self._cards.controls = [*self._section("Крипта (Binance)", crypto, CRYPTO_COL), *self._section("Форекс / Металлы (cTrader)", mt5, MT5_COL)]
        events = [e for e in s["events"] if (pool is None or e["coin"] in pool) and (strategies is None or e.get("strategy_id") in strategies or e.get("strategy") in strategies)]
        self._events.controls = [self._event_row(e) for e in events] or [w.label("Событий пока нет")]

    # ------------------------------------------------------------------------------------------ карточки

    def _section(self, name: str, coins: list[dict], col: dict) -> list[ft.Control]:
        if not coins:
            return []
        cards = []
        for c in coins:
            card = self._coin_card(c)
            card.col = col
            cards.append(card)
        return [ft.Row([w.title(f"{name} · {len(coins)}", 15), ft.Container(expand=True, height=1, bgcolor=w.BORDER)], spacing=10,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.ResponsiveRow(cards, spacing=14, run_spacing=14, vertical_alignment=ft.CrossAxisAlignment.START)]

    def _coin_card(self, c: dict) -> ft.Control:
        active = max((x["state"] for x in c["setups"]), default=0)
        rows = [ft.Row([ft.Text(fmt.coin_short(c["coin"]), size=20, weight=ft.FontWeight.W_700, color=w.TEXT),
                        ft.Text(c["coin"], size=11, color=w.MUTED)], spacing=8, vertical_alignment=ft.CrossAxisAlignment.END)]
        for s in c["setups"]:
            color = fmt.STATE_COLORS[s["state"]]
            rows.append(ft.Row([w.dot(color), ft.Text(f"{SIDE_ARROW[s['side']]} {fmt.SIDE_RU[s['side']]}", size=12, color=w.MUTED, width=64),
                                ft.Column([ft.Text(fmt.setup_text(s), size=13, color=w.TEXT if s["state"] else w.MUTED, weight=ft.FontWeight.W_600 if s["state"] else None),
                                           ft.Text(f"с {fmt.fmt_local(s['since'])}" if s["since"] else "", size=11, color=w.MUTED, visible=bool(s["since"]))],
                                          spacing=0, expand=True)], spacing=8, vertical_alignment=ft.CrossAxisAlignment.START))
        for p in c["positions"]:
            rows.append(ft.Divider(height=8, color=w.BORDER))
            rows.append(self._position(p))
        return w.card(ft.Column(rows, spacing=8), border_color=fmt.STATE_COLORS[active] if active else w.BORDER)

    @staticmethod
    def _position(p: dict) -> ft.Control:
        head = [w.pill(fmt.STATUS_RU.get(p["status"], p["status"]), w.ACCENT, filled=True), w.pill(fmt.STRATEGY_RU.get(p["strategy_name"], p["strategy_name"]), w.MUTED),
                w.pill(fmt.SIDE_RU[p["side"]], w.GREEN if p["side"] == "long" else w.RED)]
        if p["be_armed"]:
            head.append(w.pill("БУ", w.AMBER))
        lines = [f"Вход: {fmt.fmt_price(p['entry_price'] if p['entry_price'] is not None else p['planned_entry'])}   SL: {fmt.fmt_price(p['sl'])}   TP: {fmt.fmt_price(p['tp'])}"]
        if p["status"] == "open":
            lines.append(f"Остаток: {(p['remaining_frac'] or 0) * 100:.0f}%   Реализовано: {fmt.fmt_r(p['r_multiple'])}")
        return ft.Column([ft.Row(head, spacing=6, wrap=True), *[ft.Text(x, size=12, color=w.MUTED) for x in lines]], spacing=4)

    @staticmethod
    def _event_row(e: dict) -> ft.Control:
        return ft.Row([ft.Text(fmt.fmt_local(e["ts"]), size=12, color=w.MUTED, width=82), ft.Text(fmt.coin_short(e["coin"]), size=12, weight=ft.FontWeight.W_600, width=70),
                       ft.Text(fmt.event_text(e), size=12, color=w.TEXT, expand=True, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS)], spacing=8)
