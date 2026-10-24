"""Вкладка «Playground» (1.7.0): прогон стратегий бота по истории - сервер (контейнер playground) показывает стратегиям график свеча за свечой,
как в симуляции TradingView: бот не знает итога, открывает сделки, ведёт их по минуткам и пишет разбор так же, как в живом Shadow.

Форма: монета, стратегии (у профиля CHR - только его две), период. Каждый прогон сохраняется в историю (стратегии + монета + период): итоги по стратегиям,
кривая доходности и все сделки; клик по сделке открывает обычное окно графика с разбором и хронологией. CHR видит только прогоны своих стратегий."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import flet as ft

from tradeai_client import fmt, profiles
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

POLL_S = 3.0
DEFAULT_DAYS = 30
STATUS_RU = {"queued": ("в очереди", w.AMBER), "running": ("считается", w.ACCENT), "done": ("готово", w.GREEN), "failed": ("ошибка", w.RED)}
RUN_COLS = [118, 92, 220, 170, 104, 64, 70, 96, 96]
STRAT_COLS = [200, 70, 70, 74, 96, 100, 70, 84, 150]
TRADE_COLS = [128, 150, 60, 92, 92, 92, 92, 84, 84]
TRADE_STATUS_RU = {"closed": "закрыта", "cancelled": "отменена", "open": "открыта на конец периода", "pending_limit": "ждала лимитку", "pending_market": "ждала вход"}


def _iso(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _r(x: float | None) -> str:
    return fmt.DASH if x is None else f"{x:+.2f} R".replace("-", "−")


def _usd(x: float | None) -> str:
    return fmt.DASH if x is None else f"{x:+.2f} $".replace("-", "−")


def _pct(x: float | None) -> str:
    return fmt.DASH if x is None else f"{100 * x:.0f}%"


def _color(x: float | None) -> str:
    return w.MUTED if not x else (w.GREEN if x > 0 else w.RED)


def _short_names(names: list[str]) -> str:
    return ", ".join(fmt.STRATEGY_RU.get(n, n) for n in names)


class PlaygroundView:
    def __init__(self, host) -> None:
        self.host = host
        today = datetime.now(timezone.utc).date()
        self._coin = ft.TextField(label="Монета", value="BTCUSDT", width=150, dense=True, border_color=w.BORDER, hint_text="BTC, SOLUSDT…")
        self._strategies = w.MultiSelect("Стратегии", [], self._noop, host, width=230)
        self._since = ft.TextField(label="С даты", value=str(today - timedelta(days=DEFAULT_DAYS)), width=140, dense=True, border_color=w.BORDER, hint_text="ГГГГ-ММ-ДД")
        self._until = ft.TextField(label="По дату", value=str(today), width=140, dense=True, border_color=w.BORDER, hint_text="ГГГГ-ММ-ДД")
        self._run_btn = ft.FilledButton("Прогнать по истории", icon=ft.Icons.PLAY_ARROW, on_click=self._run_clicked)
        self._note = ft.Text("", size=12, color=w.MUTED)
        self._runs = ft.Column(spacing=4)
        self._detail = ft.Column(spacing=12)
        head = ft.Row([self._coin, self._strategies.control, self._since, self._until, self._run_btn, self._note], spacing=12, run_spacing=8, wrap=True,
                      vertical_alignment=ft.CrossAxisAlignment.CENTER)
        intro = w.label("Сервер показывает стратегиям историю свеча за свечой (бот видит только закрытые свечи и не знает, что будет дальше), "
                        "сделки ведутся по минуткам: вход, стоп, тейки - тем же кодом, что в живом Shadow. Риск $5 на сделку, план выхода - текущий. "
                        "OBI Breakout не прогоняется (нет истории стакана); только крипта Binance. Год на одной монете считается 1-3 часа.", size=12)
        self.control = ft.Column([ft.Container(height=2), head, intro, w.title("История прогонов", 15), w.card(self._runs, padding=10), self._detail],
                                 expand=True, scroll=ft.ScrollMode.AUTO, spacing=12)
        self._items: list[dict[str, Any]] = []
        self._open_id: str | None = None
        self._polling = False
        self._apply_profile_options()

    # ------------------------------------------------------------------------------------------ профиль

    def _noop(self, _=None) -> None:
        return None

    @property
    def scope(self) -> str:
        return "chr" if getattr(self.host, "settings", None) is not None and self.host.settings.profile == profiles.CHR else "santa"

    def _available(self) -> list[str]:
        own = getattr(self.host, "profile_strategies", lambda: None)()
        names = own if own is not None else [n for n in fmt.STRATEGY_RU if n not in ("OBI_Breakout", "smc", "breakout")]
        return [n for n in names if n != "OBI_Breakout"]

    def _apply_profile_options(self) -> None:
        self._strategies.set_options([(n, fmt.STRATEGY_RU.get(n, n)) for n in self._available()], select_all=True)

    def reset(self) -> None:
        """Профиль сменился: свои стратегии в форме и своя история прогонов."""
        self._apply_profile_options()
        self._open_id, self._items = None, []
        self._detail.controls = []
        self._runs.controls = []
        self._update()

    def relayout(self) -> None:
        return None

    def _update(self) -> None:
        if w.attached(self.control):
            self.control.update()

    # ------------------------------------------------------------------------------------------ данные

    async def refresh(self) -> None:
        client = self.host.client
        if client is None:
            return
        try:
            data = await client.playground_runs(self.scope)
        except ApiError as exc:
            self._runs.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self._update()
            return
        self._items = data["items"]
        self._render_runs()
        if self._open_id and any(r["id"] == self._open_id and r["status"] == "done" for r in self._items) and not self._detail.controls:
            await self._open(self._open_id)
        self._update()
        if any(r["status"] in ("queued", "running") for r in self._items) and not self._polling:
            self.host.run(self._poll())

    async def _poll(self) -> None:
        """Пока есть прогоны в очереди или в работе - обновлять список (прогресс); готовый открытый прогон - показать."""
        self._polling = True
        try:
            while True:
                await asyncio.sleep(POLL_S)
                if self.host.client is None:
                    return
                was_busy = {r["id"] for r in self._items if r["status"] in ("queued", "running")}
                await self.refresh_list_only()
                busy = {r["id"] for r in self._items if r["status"] in ("queued", "running")}
                if self._open_id in was_busy - busy:
                    await self._open(self._open_id)
                if not busy:
                    return
        finally:
            self._polling = False

    async def refresh_list_only(self) -> None:
        try:
            data = await self.host.client.playground_runs(self.scope)
        except ApiError:
            return
        self._items = data["items"]
        self._render_runs()
        self._update()

    # ------------------------------------------------------------------------------------------ запуск

    async def _run_clicked(self, _=None) -> None:
        client = self.host.client
        if client is None:
            self.host.notify("Нет подключения к серверу", error=True)
            return
        try:
            coin = profiles.normalize_coin(self._coin.value or "")
        except profiles.ProfileError as exc:
            self.host.notify(str(exc), error=True)
            return
        if not coin.endswith("USDT"):
            self.host.notify("Playground гоняет только крипту Binance (USDT-перпетуалы): истории минуток Forex/металлов нет", error=True)
            return
        names = self._strategies.selected
        if not names:
            self.host.notify("Выберите хотя бы одну стратегию", error=True)
            return
        try:
            created = await client.playground_create(coin, names, (self._since.value or "").strip(), (self._until.value or "").strip(),
                                                     profiles.normalize_profile(self.host.settings.profile))
        except ApiError as exc:
            hints = {"bad_period": "Период: «с» раньше «по», не длиннее 400 дней", "bad_date": "Даты - в формате ГГГГ-ММ-ДД", "bad_symbol": "Неверный тикер",
                     "strategy_not_in_profile": "Профилю доступны только его стратегии"}
            text = next((v for k, v in hints.items() if k in str(exc)), str(exc))
            self.host.notify(text, error=True)
            return
        self._coin.value = coin
        self._open_id = created["id"]
        self._detail.controls = [w.card(ft.Column([w.title(f"Прогон {coin} поставлен в очередь", 15),
                                                   w.label("Сервер сначала докачивает минутки Binance (с запасом 95 суток на прогрев), затем идёт по истории. "
                                                           "Прогресс - в списке выше; результат откроется здесь сам.", size=12)], spacing=6))]
        await self.refresh()

    # ------------------------------------------------------------------------------------------ список

    def _render_runs(self) -> None:
        if not self._items:
            self._runs.controls = [w.label("Прогонов пока нет. Выберите монету, стратегии и период и нажмите «Прогнать по истории».", size=13)]
            return
        head = w.table_row([w.cell(x, color=w.MUTED, size=12) for x in ("Создан", "Монета", "Стратегии", "Период", "Статус", "Сделок", "Винрейт",
                                                                         "Сумма R", "PnL")], widths=RUN_COLS, height=30)
        rows = [head]
        for r in self._items:
            o = (r.get("summary") or {}).get("overall") or {}
            label, color = STATUS_RU.get(r["status"], (r["status"], w.MUTED))
            if r["status"] == "running":
                label = f"{label} {100 * (r.get('progress') or 0):.0f}%"
            row = w.table_row([
                w.cell(fmt.fmt_local(_iso(r["created_ms"])), size=12), w.cell(fmt.coin_short(r["symbol"]), weight=ft.FontWeight.W_600),
                ft.Text(_short_names(r["strategies"]), size=12, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS, tooltip=_short_names(r["strategies"])),
                w.cell(f"{_day(r['start_ms'])} → {_day(r['end_ms'] - 1)}", size=12), w.cell(label, color=color, size=12),
                w.cell(str(o.get("closed", fmt.DASH))), w.cell(_pct(o.get("winrate"))),
                w.cell(_r(o.get("sum_r")), color=_color(o.get("sum_r"))), w.cell(_usd(o.get("pnl")), color=_color(o.get("pnl")))],
                widths=RUN_COLS, bgcolor=w.CARD_HI if r["id"] == self._open_id else None)
            rows.append(ft.Container(content=row, on_click=lambda e, rid=r["id"]: self.host.run(self._open(rid)), ink=True, border_radius=8,
                                     tooltip=r.get("error") or r.get("label") or "Открыть прогон"))
        self._runs.controls = [ft.Row([ft.Column(rows, spacing=2, width=sum(RUN_COLS) + 8 * len(RUN_COLS) + 24)], scroll=ft.ScrollMode.ADAPTIVE)]

    # ------------------------------------------------------------------------------------------ прогон

    async def _open(self, run_id: str) -> None:
        client = self.host.client
        if client is None:
            return
        self._open_id = run_id
        self._render_runs()
        try:
            run = await client.playground_run(run_id)
            trades = (await client.playground_trades(run_id))["items"] if run["status"] == "done" else []
        except ApiError as exc:
            self._detail.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self._update()
            return
        self._detail.controls = self._run_detail(run, trades)
        self._update()

    def _run_detail(self, run: dict, trades: list[dict]) -> list[ft.Control]:
        title = f"{fmt.coin_short(run['symbol'])} · {_day(run['start_ms'])} → {_day(run['end_ms'] - 1)}"
        delete = ft.TextButton("Удалить прогон", icon=ft.Icons.DELETE_OUTLINE, on_click=lambda e: self.host.run(self._delete(run["id"])),
                               disabled=run["status"] == "running")
        head = ft.Row([w.title(title, 16), ft.Container(expand=True), delete], vertical_alignment=ft.CrossAxisAlignment.CENTER)
        if run["status"] == "failed":
            return [head, w.banner(f"Прогон не выполнен: {run.get('error')}", w.RED, ft.Icons.ERROR)]
        if run["status"] != "done":
            return [head, w.card(ft.Column([w.label(f"{STATUS_RU[run['status']][0]}: {run.get('stage') or ''}", size=13),
                                            ft.ProgressBar(value=run.get("progress") or 0, color=w.ACCENT, bgcolor=w.BORDER, width=320)], spacing=6))]
        s = run["summary"]
        o = s["overall"]
        info = f"стратегии: {_short_names(run['strategies'])}; свечей: {run.get('bars') or 0}; счёт {int(run.get('seconds') or 0) // 60} мин"
        if run.get("code_version"):
            info += f"; код {run['code_version']}"
        kpis = ft.Row([w.kpi("Сделок", str(o["closed"]), sub=f"сигналов {o['signals']}, отменено {o['cancelled']}"),
                       w.kpi("Винрейт", _pct(o["winrate"]), sub=f"{o['wins']} в плюс / {o['losses']} в минус"),
                       w.kpi("Сумма R", _r(o["sum_r"]), color=_color(o["sum_r"]), sub=f"в среднем {_r(o['avg_r'])}"),
                       w.kpi("PnL ($5 риска)", _usd(o["pnl"]), color=_color(o["pnl"])),
                       w.kpi("Profit Factor", fmt.DASH if o["profit_factor"] is None else f"{o['profit_factor']:.2f}"),
                       w.kpi("Макс. просадка", f"{o['max_dd_r']:.2f} R")], spacing=10, wrap=True, run_spacing=10)
        out: list[ft.Control] = [head, w.label(info, size=12), kpis]
        series = []
        for i, (name, st) in enumerate(sorted(s["by_strategy"].items())):
            curve = st.get("curve") or []
            if len(curve) >= 2:
                series.append({"name": name, "color": w.SERIES_COLORS[i % len(w.SERIES_COLORS)], "x": list(range(1, len(curve) + 1)), "y": [p[1] for p in curve]})
        if series:
            legend = ft.Row([ft.Row([w.dot(x["color"]), w.label(fmt.STRATEGY_RU.get(x["name"], x["name"]))], spacing=4) for x in series], wrap=True, spacing=14)
            out.append(w.card(ft.Column([w.title("Накопленный R по стратегиям", 14), legend, w.line_chart(series)], spacing=8)))
        out.append(w.card(self._strategy_table(s["by_strategy"]), padding=10))
        out.append(w.title(f"Сделки ({len(trades)}) — клик открывает график с разбором", 15))
        out.append(w.card(self._trades_table(run["id"], trades), padding=10))
        return out

    @staticmethod
    def _strategy_table(by: dict[str, dict]) -> ft.Control:
        head = w.table_row([w.cell(x, color=w.MUTED, size=12) for x in ("Стратегия", "Сделок", "Винрейт", "Ср. R", "Сумма R", "PnL", "PF", "Просадка",
                                                                         "Выходы")], widths=STRAT_COLS, height=30)
        rows = [head]
        for name, st in sorted(by.items(), key=lambda kv: -(kv[1].get("sum_r") or 0)):
            reasons = ", ".join(f"{fmt.EXIT_RU.get(k, k)} {v}" for k, v in sorted(st["exit_reasons"].items(), key=lambda kv: -kv[1]))
            rows.append(w.table_row([
                w.cell(fmt.STRATEGY_RU.get(name, name), weight=ft.FontWeight.W_600), w.cell(str(st["closed"])), w.cell(_pct(st["winrate"])),
                w.cell(_r(st["avg_r"]), color=_color(st["avg_r"])), w.cell(_r(st["sum_r"]), color=_color(st["sum_r"])),
                w.cell(_usd(st["pnl"]), color=_color(st["pnl"])), w.cell(fmt.DASH if st["profit_factor"] is None else f"{st['profit_factor']:.2f}"),
                w.cell(f"{st['max_dd_r']:.1f} R"), ft.Text(reasons or fmt.DASH, size=12, color=w.MUTED, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS, tooltip=reasons)],
                widths=STRAT_COLS))
        return ft.Row([ft.Column(rows, spacing=2, width=sum(STRAT_COLS) + 8 * len(STRAT_COLS) + 24)], scroll=ft.ScrollMode.ADAPTIVE)

    def _trades_table(self, run_id: str, trades: list[dict]) -> ft.Control:
        if not trades:
            return w.label("Сигналов за период не было.", size=13)
        head = w.table_row([w.cell(x, color=w.MUTED, size=12) for x in ("Сигнал", "Стратегия", "Сторона", "Вход", "SL", "Выход", "Итог", "R", "PnL")],
                           widths=TRADE_COLS, height=30)
        rows = [head]
        for t in sorted(trades, key=lambda x: x["signal_time"] or "", reverse=True):
            outcome = fmt.EXIT_RU.get(t.get("exit_reason"), t.get("exit_reason")) if t["status"] == "closed" else TRADE_STATUS_RU.get(t["status"], t["status"])
            if t["status"] == "cancelled" and t.get("exit_reason"):
                outcome = f"отмена: {t['exit_reason']}"
            row = w.table_row([
                w.cell(fmt.fmt_local(t["signal_time"]), size=12), w.cell(fmt.STRATEGY_RU.get(t["strategy_id"], t["strategy_id"]), size=12),
                w.cell(fmt.SIDE_RU.get(t["side"], t["side"]), color=w.GREEN if t["side"] == "long" else w.RED, size=12),
                w.cell(fmt.fmt_price(t.get("entry_price")), size=12), w.cell(fmt.fmt_price(t.get("sl")), size=12),
                w.cell(fmt.fmt_price(t.get("exit_price")), size=12), w.cell(outcome or fmt.DASH, size=12, color=w.MUTED),
                w.cell(_r(t.get("r_multiple")), color=_color(t.get("r_multiple")), size=12), w.cell(_usd(t.get("pnl")), color=_color(t.get("pnl")), size=12)],
                widths=TRADE_COLS)
            rows.append(ft.Container(content=row, ink=True, border_radius=8, tooltip="Открыть график сделки",
                                     on_click=lambda e, tr=t: self.host.run(self.host.open_trade_chart({**tr, "playground_run": run_id}))))
        return ft.Row([ft.Column(rows, spacing=2, width=sum(TRADE_COLS) + 8 * len(TRADE_COLS) + 24)], scroll=ft.ScrollMode.ADAPTIVE)

    async def _delete(self, run_id: str) -> None:
        try:
            await self.host.client.playground_delete(run_id)
        except ApiError as exc:
            self.host.notify(str(exc), error=True)
            return
        self._open_id = None
        self._detail.controls = []
        await self.refresh()
