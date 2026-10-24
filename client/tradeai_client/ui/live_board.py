"""Live-Dashboard профиля SANTA (1.8.0): как бот смотрит рынок прямо сейчас.

Сверху - LIVE: по одному графику на каждую боевую стратегию (APPROVED_STRATEGIES сервера; сейчас одна - SMC Pro Trend), графики рядом слева направо,
у каждого свой выбор монеты (по умолчанию - монета с позицией этой стратегии или с самым продвинутым сетапом). Ниже - одно окно остальных стратегий:
выбор стратегии, подвыбор монеты (первая показывается сразу). На графике - свечи (формирующаяся бледнее), что видит стратегия (уровни, линии, зоны,
полосы VWAP), позиции бота (вход / SL / TP); под графиком - стадия по сторонам (нет сетапа / наблюдает / почти / СИГНАЛ) и чек-лист условий входа.
Данные - сервис watch на сервере (/api/watch/*: те же детекторы, что торгуют), обновление раз в REFRESH_S секунд, пока открыт Dashboard."""

import asyncio
from typing import Any

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w
from tradeai_client.ui.candles import candle_chart, position_levels

REFRESH_S = 20.0
STAGE = {0: ("нет сетапа", w.MUTED), 1: ("наблюдает", w.ACCENT), 2: ("почти", w.AMBER), 3: ("СИГНАЛ", w.GREEN)}
SIDE_ICON = {"long": "▲ Лонг", "short": "▼ Шорт"}
IV_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000}


def _check_row(c: dict) -> ft.Control:
    mark, color = ("✓", w.GREEN) if c["ok"] is True else (("✗", w.RED) if c["ok"] is False else ("·", w.MUTED))
    return ft.Row([ft.Text(mark, size=12, color=color, width=14, weight=ft.FontWeight.W_700),
                   ft.Text(c["label"], size=11, color=w.TEXT, expand=True, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS, tooltip=c["label"]),
                   ft.Text(c.get("value") or "", size=11, color=w.MUTED, no_wrap=True)], spacing=6, vertical_alignment=ft.CrossAxisAlignment.CENTER)


class Panel:
    """Один график: стратегия + монета. live=True - боевая стратегия (метка LIVE); strategies - список для выбора стратегии (окно остальных)."""

    def __init__(self, board: "LiveBoard", strategy: str, *, live: bool, strategies: list[str] | None = None):
        self.board, self.strategy, self.live, self.coin = board, strategy, live, None
        self.view: dict | None = None
        self.error: str | None = None
        self.coins_note = ""
        self._coin_dd = ft.Dropdown(label="Монета", width=170, dense=True, border_color=w.BORDER, bgcolor=w.CARD, on_select=self._coin_changed)
        self._strat_dd = None
        if strategies:
            self._strat_dd = ft.Dropdown(label="Стратегия", width=240, dense=True, border_color=w.BORDER, bgcolor=w.CARD, value=strategy,
                                         options=[ft.DropdownOption(key=s, text=fmt.STRATEGY_RU.get(s, s)) for s in strategies], on_select=self._strategy_changed)
        self._summary = ft.Text("", size=12, color=w.MUTED)
        self._chart = ft.Container(height=330)
        self._sides = ft.ResponsiveRow(spacing=14, run_spacing=8)
        tag = [w.pill("LIVE", w.RED, filled=True)] if live else []
        title = [] if self._strat_dd else [ft.Text(fmt.STRATEGY_RU.get(strategy, strategy), size=15, weight=ft.FontWeight.W_700)]
        head = ft.Row([*tag, *title, *([self._strat_dd] if self._strat_dd else []), self._coin_dd, self._summary], spacing=10, wrap=True, run_spacing=6,
                      vertical_alignment=ft.CrossAxisAlignment.CENTER)
        self.control = w.card(ft.Column([head, self._chart, self._sides], spacing=8), padding=12)

    # ------------------------------------------------------------------------------------------ выбор

    def set_coins(self, coins: list[str], preferred: str | None = None, labels: dict[str, str] | None = None) -> None:
        labels = labels or {}
        self._coin_dd.options = [ft.DropdownOption(key=c, text=labels.get(c, fmt.coin_short(c))) for c in coins]
        if self.coin not in coins:
            self.coin = preferred if preferred in coins else (coins[0] if coins else None)
        self._coin_dd.value = self.coin

    async def refresh_coins(self) -> None:
        """Выбор монет - только где стратегия видит что-то близкое к сетапу (фильтр сервиса watch); монета с позицией бота - всегда."""
        coins, labels, note = await self.board.filtered_coins(self.strategy)
        self.coins_note = note
        self.set_coins(coins, self.board.preferred_coin(self.strategy), labels)

    def _coin_changed(self, e=None) -> None:
        self.coin = self._coin_dd.value
        self.board.remember()
        self.board.host.run(self.load())

    def _strategy_changed(self, e=None) -> None:
        self.strategy = self._strat_dd.value
        self.coin = None
        self.board.host.run(self._switch())

    async def _switch(self) -> None:
        await self.refresh_coins()
        self.board.remember()
        await self.load()

    # ------------------------------------------------------------------------------------------ данные

    async def load(self) -> None:
        client = self.board.host.client
        if client is None or not self.coin:
            return
        try:
            self.view, self.error = await client.watch_view(self.strategy, self.coin), None
        except ApiError as exc:
            self.error = str(exc)
        self.render()
        self.board.update()

    def render(self) -> None:
        width = self.board.chart_width(self)
        if self.error and self.view is None:
            self._chart.content = w.banner(f"Взгляд стратегии недоступен: {self.error}", w.AMBER)
            self._sides.controls = []
            return
        v = self.view
        if v is None:
            self._chart.content = ft.Container(content=ft.ProgressRing(width=22, height=22), alignment=ft.Alignment.CENTER, height=320)
            return
        positions = [p for p in self.board.positions(v["symbol"]) if p.get("strategy_name") == self.strategy]
        self._chart.content = candle_chart(v["candles"], interval_ms=IV_MS.get(v["interval"], 900_000), geometry=v["geometry"],
                                           levels=position_levels(positions), forming=v.get("forming"), width=width, height=320)
        self._summary.value = f"{v['interval']} · {v['summary']}" + (f" · {self.coins_note}" if self.coins_note else "") +             (f" · обновлено с ошибкой: {self.error}" if self.error else "")
        cols = []
        for side in ("long", "short"):
            s = v["sides"][side]
            label, color = STAGE[s["stage"]]
            pos = next((p for p in positions if p["side"] == side), None)
            head = ft.Row([w.dot(color), ft.Text(SIDE_ICON[side], size=13, weight=ft.FontWeight.W_600, color=w.GREEN if side == "long" else w.RED),
                           w.pill(label, color, filled=s["stage"] == 3), ft.Text(s["text"], size=12, color=w.TEXT, expand=True)], spacing=8,
                          vertical_alignment=ft.CrossAxisAlignment.CENTER)
            rows = [head]
            if pos is not None:
                rows.append(w.label(f"Позиция бота: {fmt.STATUS_RU.get(pos['status'], pos['status']).lower()}, вход {fmt.fmt_price(pos.get('entry_price') or pos.get('planned_entry'))}, "
                                    f"SL {fmt.fmt_price(pos.get('sl'))}, TP {fmt.fmt_price(pos.get('tp'))}", size=12, color=w.AMBER))
            rows += [_check_row(c) for c in s["checks"]]
            cols.append(ft.Column(rows, spacing=3, col={"xs": 12, "md": 6}))
        self._sides.controls = cols


class LiveBoard:
    def __init__(self, host, collapsed: dict | None = None, on_collapse=None) -> None:
        self.host = host
        self.meta: dict | None = None
        self._state: dict | None = None
        self.live_panels: list[Panel] = []
        self.other: Panel | None = None
        self._live_row = ft.Row(spacing=14, vertical_alignment=ft.CrossAxisAlignment.START, wrap=False)
        self._other_box = ft.Column(spacing=8)
        self._note = ft.Text("", size=12, color=w.MUTED)
        collapsed, remember = collapsed or {}, on_collapse or (lambda k, c: None)
        self._live_sec = w.Collapsible("LIVE: как бот смотрит рынок", self._live_row, collapsed=collapsed.get("live", False), extra=[self._note],
                                       on_toggle=lambda c: remember("live", c))
        self._other_sec = w.Collapsible("Остальные стратегии: что ищет бот", self._other_box, collapsed=collapsed.get("shadow", False),
                                        on_toggle=lambda c: remember("shadow", c))
        self.control = ft.Column([self._live_sec.control, self._other_sec.control], spacing=10)
        self._loop_started = False

    # ------------------------------------------------------------------------------------------ данные бота (live_state)

    def on_state(self, s: dict) -> None:
        self._state = s
        if self.meta is None:
            return
        for p in self._panels():
            if p.view is not None:
                p.render()

    def positions(self, coin: str) -> list[dict]:
        if not self._state:
            return []
        return next((c["positions"] for c in self._state["coins"] if c["coin"] == coin), [])

    def coins_for(self, strategy: str) -> list[str]:
        return list(((self.meta or {}).get("strategies") or {}).get(strategy, {}).get("coins") or [])

    async def filtered_coins(self, strategy: str) -> tuple[list[str], dict[str, str], str]:
        """(монеты, подписи, пояснение): прошедшие фильтр сервиса (>= 2 условий; больше 15 - >= 3), лучшие сверху, + монеты с позицией этой стратегии.
        Сервис ещё не посчитал монеты (только запустился) или недоступен - все монеты стратегии."""
        every = self.coins_for(strategy)
        held = [c["coin"] for c in (self._state or {}).get("coins", []) if any(p.get("strategy_name") == strategy for p in c.get("positions", []))]
        try:
            data = await self.host.client.watch_coins(strategy)
        except ApiError:
            return every, {}, ""
        if not data["coins"] and data["pending"]:
            return every, {}, f"монеты ещё считаются ({data['total'] - len(data['pending'])} из {data['total']})"
        labels = {r["coin"]: f"{fmt.coin_short(r['coin'])} · {r['score']}✓ {'▲' if r['side'] == 'long' else '▼'}" + (" СИГНАЛ" if r["stage"] == 3 else "")
                  for r in data["coins"]}
        coins = list(dict.fromkeys([*held, *(r["coin"] for r in data["coins"])]))
        if not coins:  # ни одна монета не набрала порог: чтобы окно не было пустым - лучшие по числу условий (их нет в фильтре - так и подписано)
            return every[:5], {}, f"ни одна монета не набрала {data['threshold']} условий - показаны первые"
        note = f"монет {len(coins)} из {data['total']} (≥{data['threshold']} условий)"
        return coins, labels, note

    def preferred_coin(self, strategy: str) -> str | None:
        """Монета, на которую интереснее смотреть: позиция этой стратегии, иначе самый продвинутый сетап автомата (SMC), иначе первая."""
        coins = self.coins_for(strategy)
        if not self._state:
            return coins[0] if coins else None
        for c in self._state["coins"]:
            if c["coin"] in coins and any(p.get("strategy_name") == strategy for p in c["positions"]):
                return c["coin"]
        best = max(((max((x["state"] for x in c.get("setups", []) if x.get("strategy", strategy) == strategy), default=0), c["coin"])
                    for c in self._state["coins"] if c["coin"] in coins), default=(0, None))
        return best[1] if best[0] > 0 else (coins[0] if coins else None)

    # ------------------------------------------------------------------------------------------ раскладка

    def _panels(self) -> list[Panel]:
        return [*self.live_panels, *([self.other] if self.other else [])]

    def chart_width(self, panel: Panel) -> float:
        page_w = getattr(getattr(self.host, "page", None), "width", None)
        avail = (page_w if isinstance(page_w, (int, float)) and page_w > 0 else 1400) - 70
        n = max(1, len(self.live_panels)) if panel.live else 1
        return max(320.0, avail / n - 14 * (n - 1) - 30)

    def update(self) -> None:
        if w.attached(self.control):
            self.control.update()

    def remember(self) -> None:
        state = {"live": {p.strategy: p.coin for p in self.live_panels}, "other": [self.other.strategy, self.other.coin] if self.other else None}
        if hasattr(self.host, "remember_view"):
            self.host.remember_view("live_board", state)

    def _build(self) -> None:
        saved = self.host.view_state("live_board") if hasattr(self.host, "view_state") else None
        saved = saved or {}
        live = list(self.meta["live_strategies"])
        self.live_panels = []
        for name in live:
            p = Panel(self, name, live=True)
            p.coin = (saved.get("live") or {}).get(name)
            p.set_coins(self.coins_for(name), self.preferred_coin(name))
            p.control.expand = True
            self.live_panels.append(p)
        self._live_row.controls = [p.control for p in self.live_panels] or [w.label("Боевых стратегий нет (APPROVED_STRATEGIES пуст)", size=13)]
        others = [s for s in self.meta["strategies"] if s not in live]
        if others:
            first = (saved.get("other") or [None, None])
            strategy = first[0] if first[0] in others else others[0]
            self.other = Panel(self, strategy, live=False, strategies=others)
            self.other.coin = first[1]
            self.other.set_coins(self.coins_for(strategy), self.preferred_coin(strategy))
            self._other_box.controls = [self.other.control]

    async def refresh(self) -> None:
        client = self.host.client
        if client is None:
            return
        if self.meta is None:
            try:
                self.meta = await client.watch_meta()
            except ApiError as exc:
                self._note.value = f"сервис недоступен: {exc}"
                self.update()
                return
            self._build()
        self._note.value = f"обновление раз в {int(REFRESH_S)} с · стратегии считают по закрытым свечам, формирующаяся показана бледнее"
        await asyncio.gather(*(p.refresh_coins() for p in self._panels()))
        await asyncio.gather(*(p.load() for p in self._panels()))
        if not self._loop_started:
            self._loop_started = True
            self.host.run(self._loop())

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(REFRESH_S)
            if self.host.client is None or not self.control.visible or not w.attached(self.control) or getattr(self.host, "_tab", 0) != 0:
                continue
            await asyncio.gather(*(p.refresh_coins() for p in self._panels()))
            await asyncio.gather(*(p.load() for p in self._panels()))
