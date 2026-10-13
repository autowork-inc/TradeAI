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
        self._coin = _dropdown("Монета", [("all", "Все")] + [(c, fmt.coin_short(c)) for c in COINS], 130, self._changed)
        self._strategy = _dropdown("Стратегия", fmt.STRATEGY_OPTIONS, 170, self._changed)
        self._side = _dropdown("Сторона", [("all", "Все"), ("long", "Лонг"), ("short", "Шорт")], 120, self._changed)
        self._period = _dropdown("Период", [(k, v[0]) for k, v in PERIODS.items()], 170, self._changed)
        self._stamp = ft.Text("", size=12, color=w.MUTED)
        self._body = ft.Column(spacing=14)
        self.control = ft.Column(
            [ft.Container(height=2), ft.Row([self._coin, self._strategy, self._side, self._period,
                     ft.IconButton(icon=ft.Icons.REFRESH, tooltip="Обновить", on_click=self._changed), self._stamp], spacing=10),
             self._body], expand=True, scroll=ft.ScrollMode.AUTO, spacing=14)

    async def _changed(self, _=None) -> None:
        await self.refresh()

    def _filters(self) -> dict:
        pick = lambda d: None if d.value in (None, "all") else [d.value]
        days = PERIODS[self._period.value or "all"][1]
        since = None if days is None else (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {"coins": pick(self._coin), "strategies": pick(self._strategy), "sides": pick(self._side), "since": since}

    async def refresh(self) -> None:
        client = self.host.client
        if client is None:
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
        if d["significance"]["note"]:
            out.append(w.banner(d["significance"]["note"], w.AMBER))
        pf = fmt.fmt_pf(o)
        se = f"± {o['avg_r_stderr']:.2f}" if o["avg_r_stderr"] is not None else None
        pf_color = w.MUTED if pf == fmt.DASH else (w.GREEN if pf == "∞" or float(pf) >= 1 else w.RED)
        wr = o["win_rate"]
        out.append(ft.Row([
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
                ("Прервано перезапуском", c.get("aborted", 0))]
        body = [ft.Row([ft.Text(n, size=12, color=w.MUTED, expand=True), ft.Text(str(v), size=13)]) for n, v in rows]
        return w.card(ft.Column([w.title("Все сделки по статусам", 15), *body], spacing=6), width=330)
