"""Вкладка Statistics: KPI (Win Rate, Profit Factor, Net R, просадка), рейтинг монет по Net R, кривая капитала, разбивки. Данные - GET /api/stats."""

from datetime import datetime, timedelta, timezone

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

COINS = fmt.COINS
PERIODS = {"all": ("Всё время", None), "30d": ("30 дней", 30), "7d": ("7 дней", 7), "1d": ("24 часа", 1)}
COIN_COLS = [26, 90, 300, 70, 80, 60, 90]


def _dropdown(label: str, options: list[tuple[str, str]], width: float, on_select) -> ft.Dropdown:
    return ft.Dropdown(label=label, value=options[0][0], options=[ft.DropdownOption(key=k, text=t) for k, t in options], width=width,
                       on_select=on_select, border_color=w.BORDER, bgcolor=w.CARD)


class StatsView:
    def __init__(self, host) -> None:
        self.host = host
        self._server_coins: list[str] = []
        self._coin = w.FilterSelect("Монеты", [(c, fmt.coin_short(c)) for c in COINS], self._changed, host, width=190)
        self._strategy = w.FilterSelect("Стратегия", fmt.STRATEGY_OPTIONS, self._changed, host, width=230)
        self._mode = w.FilterSelect("Режим", fmt.MODE_OPTIONS, self._mode_changed, host, width=180)
        saved = getattr(self.host, "saved_mode", "")
        self._mode.value, self._mode_touched = saved or self.host.default_mode, bool(saved)
        self._side = w.FilterSelect("Сторона", [("long", "Лонг"), ("short", "Шорт")], self._changed, host, width=130)
        self._period = _dropdown("Период", [(k, v[0]) for k, v in PERIODS.items()], 170, self._changed)
        self._bar = w.FilterBar([self._mode, self._coin, self._strategy, self._side], self._changed)  # период - диапазон времени: всегда один
        self._stamp = ft.Text("", size=12, color=w.MUTED)
        self._goal = w.GoalBar(fmt.TRADES_GOAL, width=150)
        self._body = ft.Column(spacing=14)
        self.control = ft.Column(
            [ft.Container(height=2),
             # широкий экран: фильтры слева (переносятся), карточка «значимость выводов» - в той же строке у правого края; узкий (телефон) - карточка под фильтрами,
             # а не сжатая рядом с ними (раньше фильтры выдавливались в 200 px и текст налезал друг на друга)
             ft.ResponsiveRow([ft.Row([self._mode.control, self._coin.control, self._strategy.control, self._side.control, self._period, self._bar.control,
                                       ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Обновить", on_click=self._changed), self._stamp],
                                      spacing=10, run_spacing=8, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER, col={"xs": 12, "md": 9}),
                               ft.Row([self._goal.control], alignment=ft.MainAxisAlignment.END, col={"xs": 12, "md": 3})],
                              spacing=12, run_spacing=8, vertical_alignment=ft.CrossAxisAlignment.START),
             self._body], expand=True, scroll=ft.ScrollMode.AUTO, spacing=14)
        self.apply_profile()
        self.restore_state(getattr(host, "view_state", lambda _n: None)("stats"))

    # ------------------------------------------------------------------------------------------ память фильтров (1.6.0)

    def state(self) -> dict:
        return {"mode": self._mode.state(), "coin": self._coin.state(), "strategy": self._strategy.state(), "side": self._side.state(),
                "period": self._period.value, "multi": bool(self._bar.switch.value)}

    def restore_state(self, st: dict | None) -> None:
        if not isinstance(st, dict):
            return
        self._bar.set(bool(st.get("multi")))
        for key, f in (("coin", self._coin), ("strategy", self._strategy), ("side", self._side)):
            f.restore(st.get(key))
        if isinstance(st.get("mode"), dict) and self._mode.visible:
            self._mode.restore(st["mode"])
            self._mode_touched = True
        if st.get("period") in PERIODS:
            self._period.value = st["period"]

    def _remember(self) -> None:
        remember = getattr(self.host, "remember_view", None)
        if remember is not None:
            remember("stats", self.state())

    # ------------------------------------------------------------------------------------------ профиль и фильтры

    def _scope(self) -> tuple[list[str] | None, list[str] | None]:
        return getattr(self.host, "profile_strategies", lambda: None)(), getattr(self.host, "profile_coins", lambda: None)()

    def apply_profile(self) -> None:
        """Профиль сменился (или только что открыт): варианты стратегий и монет, видимость режима."""
        strategies, pool = self._scope()
        options = fmt.STRATEGY_OPTIONS if strategies is None else [("all", "Все")] + [(k, fmt.strategy_label(k)) for k in strategies]
        self._strategy.set_options(options, select_all=True)
        self._coin.set_options([(c, fmt.coin_short(c)) for c in (pool if pool is not None else self._all_coins())], select_all=True)
        self._mode.visible = strategies is None  # CHR: только Shadow-стратегии, переключатель режима не нужен

    def _all_coins(self) -> list[str]:
        return list(dict.fromkeys([*COINS, *self._server_coins]))

    def set_coins(self, coins: list[str]) -> None:
        """Монеты с сервера (автоселектор подключает новые на лету): фильтр «Монеты» получает их вместе с прежними; выбор сохраняется. Профиль CHR держит свой пул."""
        self._server_coins = list(coins)
        if self._scope()[1] is None:
            self._coin.set_options([(c, fmt.coin_short(c)) for c in self._all_coins()])

    async def _changed(self, _=None) -> None:
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
        await self.refresh()

    def follow_default_mode(self, mode: str) -> bool:
        """Активный контур сменился: переключить фильтр, если пользователь его не трогал. True - значение изменилось."""
        if self._mode_touched or self._mode.value == mode:
            return False
        self._mode.value = mode
        return True

    def _filters(self) -> dict:
        strategies, pool = self._scope()
        days = PERIODS[self._period.value or "all"][1]
        since = None if days is None else (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        picked = self._coin.keys()
        modes = self._mode.keys()
        mode = "all" if modes is None or len(modes) != 1 else modes[0]
        return {"coins": picked if picked is not None else pool, "strategies": self._strategy.keys() or strategies, "sides": self._side.keys(), "since": since,
                "mode": "shadow" if strategies is not None else mode}

    async def refresh(self) -> None:
        client = self.host.client
        if client is None:
            return
        empty = next((f for f in self._bar.filters if f.is_empty), None)
        if empty is not None:  # «Снять всё»: ничего не выбрано - показываем пустую статистику, а не всё подряд
            self._goal.set(0)
            self._body.controls = [w.label(f"Не выбрано ни одного значения: {empty.label}", size=14)]
            self.control.update()
            return
        try:
            data = await client.stats(**self._filters())
        except ApiError as exc:
            self._body.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self.control.update()
            return
        self._stamp.value = f"обновлено {fmt.fmt_local(data['server_time'])}"
        self._body.controls = self._render(data)
        self.control.update()

    # ------------------------------------------------------------------------------------------ отрисовка

    def _render(self, d: dict) -> list[ft.Control]:
        o, out = d["overall"], []
        self._goal.set(o["trades"])
        out.append(self._excluded_note(d["counts"], o["trades"]))
        pf = fmt.fmt_pf(o)
        se = f"± {o['avg_r_stderr']:.2f}" if o["avg_r_stderr"] is not None else None
        pf_color = w.MUTED if pf == fmt.DASH else (w.GREEN if pf == "∞" or float(pf) >= 1 else w.RED)
        wr = o["win_rate"]
        chr_profile = self._scope()[0] is not None
        pnl_kpi = [w.kpi("PnL (USDT)", fmt.fmt_usd(o.get("net_pnl")), color=fmt.r_color(o.get("net_pnl")), sub="от нуля, только сделки профиля", tooltip="Сумма результатов сделок в USDT, старт с 0")] if chr_profile else []
        out.append(ft.Row([
            *pnl_kpi,
            w.kpi("Сделок", str(o["trades"]), sub=f"{o['wins']} приб. · {o['losses']} убыт." + (f" · {o['flat']} нулевых" if o["flat"] else "")),
            w.kpi("Win Rate", fmt.fmt_pct(wr), color=w.TEXT if wr is None else (w.GREEN if wr >= 0.5 else w.AMBER), tooltip="Доля сделок с итогом > 0 R"),
            w.kpi("Profit Factor", pf, color=pf_color, sub=f"+{o['gross_profit_r']:.2f} R / −{o['gross_loss_r']:.2f} R",
                  tooltip="Сумма прибыльных R / сумма убыточных R"),
            w.kpi("Net R", fmt.fmt_r(o["net_r"]), color=fmt.r_color(o["net_r"]), sub="сумма итогов сделок"),
            w.kpi("Средний R", fmt.fmt_r(o["avg_r"]), color=fmt.r_color(o["avg_r"]), sub=se, tooltip="Мат. ожидание на сделку ± стандартная ошибка"),
            w.kpi("Макс. просадка", fmt.fmt_r(-o["max_drawdown_r"]) if o["max_drawdown_r"] else "0.00 R", color=w.RED if o["max_drawdown_r"] else w.MUTED,
                  sub="от пика накопленного R"),
        ], wrap=True, spacing=12, run_spacing=12))
        out.append(self._coins_card(d))
        if chr_profile:
            out.append(w.card(ft.Column([w.title("Накопленный PnL (USDT) от нуля, по времени закрытия", 15), w.equity_bars(d["equity_curve"], key="cum_pnl", unit="USDT")], spacing=10)))
        else:
            out.append(w.card(ft.Column([w.title("Накопленный R по времени закрытия", 15), w.equity_bars(d["equity_curve"])], spacing=10)))
        out.append(ft.Row([self._breakdown("По стратегиям", [(fmt.STRATEGY_RU.get(x["strategy"], x["strategy"]), x) for x in d["by_strategy"]]),
                           self._breakdown("По сторонам", [(fmt.SIDE_RU.get(x["side"], x["side"]), x) for x in d["by_side"]]),
                           self._reasons(d["by_exit_reason"]), self._counts(d["counts"])], wrap=True, spacing=12, run_spacing=12,
                          vertical_alignment=ft.CrossAxisAlignment.START))
        return out

    def _coins_card(self, d: dict) -> ft.Control:
        coins = d["by_coin"]
        peak = max([abs(c["net_r"]) for c in coins if c["trades"]] + [1e-9])
        best = next((c for c in coins if c["trades"]), None)
        head: list[ft.Control] = [w.title("Монеты по результату (Net R)", 15)]
        if best:
            head.append(ft.Row([ft.Icon(ft.Icons.EMOJI_EVENTS, color=w.AMBER, size=18),
                                ft.Text(f"Лучшая: {fmt.coin_short(best['coin'])} — {fmt.fmt_r(best['net_r'])}, PF {fmt.fmt_pf(best)}, "
                                        f"Win Rate {fmt.fmt_pct(best['win_rate'])} ({best['trades']} сделок)", size=13, color=w.TEXT)], spacing=6))
        muted = lambda t, right=False: w.cell(t, color=w.MUTED, size=12, align_right=right)
        rows = [w.table_row([muted("#"), muted("Монета"), muted("Net R"), muted("Сделок", True), muted("Win Rate", True), muted("PF", True), muted("Средний R", True)],
                            widths=COIN_COLS, height=26)]
        for c in coins:
            has = c["trades"] > 0
            color = fmt.r_color(c["net_r"]) if has else w.MUTED
            rows.append(w.table_row([
                w.cell(str(c["rank"]) if c["rank"] else fmt.DASH, color=w.MUTED),
                w.cell(fmt.coin_short(c["coin"]), weight=ft.FontWeight.W_700, color=w.TEXT if has else w.MUTED),
                ft.Row([w.hbar(abs(c["net_r"]) / peak if has else 0, color, width=170),
                        w.cell(fmt.fmt_r(c["net_r"]) if has else "нет сделок", color=color, size=12)], spacing=8),
                w.cell(str(c["trades"]), align_right=True, color=w.TEXT if has else w.MUTED), w.cell(fmt.fmt_pct(c["win_rate"]), align_right=True),
                w.cell(fmt.fmt_pf(c), align_right=True), w.cell(fmt.fmt_r(c["avg_r"]), align_right=True, color=fmt.r_color(c["avg_r"]))],
                widths=COIN_COLS, bgcolor=w.CARD_HI if c["rank"] == 1 else None))
        return w.card(ft.Column([*head, ft.Column(rows, spacing=2, scroll=ft.ScrollMode.AUTO)], spacing=10))

    @staticmethod
    def _excluded_note(c: dict, counted: int) -> ft.Control:
        """Почему сделок в расчёте мало: сколько и каких не вошло (без этого «1 сделка» выглядит как ошибка)."""
        parts = [(c.get("aborted", 0), "прервано рестартом"), (c.get("open", 0), "открыто"), (c.get("pending", 0), "ждут входа"),
                 (c.get("cancelled", 0), "отменено"), (c.get("closed_gap_affected", 0), "закрыто с обрывом потока")]
        shown = ", ".join(f"{n} {name}" for n, name in parts if n)
        text = f"В расчёте: {counted}. Не вошли: {shown}." if shown else f"В расчёте: {counted}. Исключённых сделок нет."
        return w.label(text, size=12)

    @staticmethod
    def _breakdown(title: str, items: list[tuple[str, dict]]) -> ft.Control:
        lines: list[ft.Control] = [w.title(title, 15)]
        for name, x in items:
            lines.append(ft.Row([ft.Text(name, size=13, weight=ft.FontWeight.W_600, width=90),
                                 ft.Text(fmt.fmt_r(x["net_r"]), size=13, color=fmt.r_color(x["net_r"]), width=80),
                                 ft.Text(f"{x['trades']} сд · WR {fmt.fmt_pct(x['win_rate'], 0)} · PF {fmt.fmt_pf(x)}", size=12, color=w.MUTED)], spacing=8))
        if len(lines) == 1:
            lines.append(w.label("нет данных"))
        return w.card(ft.Column(lines, spacing=8), width=360)

    @staticmethod
    def _reasons(items: list[dict]) -> ft.Control:
        lines: list[ft.Control] = [w.title("Причины выхода", 15)]
        for x in items:
            lines.append(ft.Row([ft.Text(fmt.EXIT_RU.get(x["reason"], x["reason"]), size=13, width=110), ft.Text(f"{x['trades']} сд", size=12, color=w.MUTED, width=60),
                                 ft.Text(fmt.fmt_r(x["net_r"]), size=13, color=fmt.r_color(x["net_r"]))], spacing=8))
        if len(lines) == 1:
            lines.append(w.label("нет данных"))
        return w.card(ft.Column(lines, spacing=8), width=300)

    @staticmethod
    def _counts(c: dict) -> ft.Control:
        rows = [("Закрыто (в статистике)", c.get("closed", 0)), ("Закрыто с обрывом потока (исключены)", c.get("closed_gap_affected", 0)),
                ("Открыто", c.get("open", 0)), ("Ждут входа", c.get("pending", 0)), ("Отменено", c.get("cancelled", 0)),
                ("Прервано рестартом бота", c.get("aborted", 0))]
        body = [ft.Row([ft.Text(n, size=12, color=w.MUTED, expand=True), ft.Text(str(v), size=13)]) for n, v in rows]
        return w.card(ft.Column([w.title("Все сделки по статусам", 15), *body], spacing=6), width=330)
