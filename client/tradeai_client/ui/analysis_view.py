"""Вкладка Analysis: Strategy Quant Analyzer (CLAUDE.md разд. 3.17). Кнопка «Run Deep Analysis» ставит задание в очередь анализатора (через прокси основного API),
вкладка опрашивает статус и показывает спиннер/прогресс, затем отчёт: метрики по стратегиям, News Impact, сессии, наложение кривых PnL (версии/стратегии),
рекомендации и Dry Run diff. Вкладка ничего не меняет в боте: diff можно только скопировать и перенести в код вручную (решение пользователя)."""

import asyncio
from typing import Any

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

POLL_S = 1.5
POLL_MAX_S = 900.0
STRAT_COLS = [150, 62, 72, 66, 70, 70, 78, 82, 78, 72, 80]
VERDICT_RU = {"degraded": ("ДЕГРАДАЦИЯ", w.RED), "suspected": ("ПОДОЗРЕНИЕ", w.AMBER), "improved": ("УЛУЧШЕНИЕ", w.GREEN), "inconclusive": ("не отличимо от шума", w.MUTED),
              "insufficient_data": ("мало данных", w.MUTED)}
CONF_RU = {"high": ("высокая", w.GREEN), "medium": ("средняя", w.AMBER), "low": ("низкая", w.RED)}
KIND_RU = {"tighten_stop": "Стоп ближе", "exclude_coin": "Отключить монету", "exclude_session": "Не торговать сессию", "feature_filter": "Фильтр входа", "news_blackout": "Блок на новостях"}


def _n(x: float | None, nd: int = 2, signed: bool = False) -> str:
    if x is None:
        return fmt.DASH
    return (f"{x:+.{nd}f}" if signed else f"{x:.{nd}f}").replace("-", "−")


def _val(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, list):
        return "[" + ", ".join(_val(x) for x in v) + "]"
    return str(v)


class AnalysisView:
    def __init__(self, host) -> None:
        self.host = host
        self._mode = ft.Dropdown(label="Режим", value="shadow", width=170, border_color=w.BORDER, bgcolor=w.CARD,
                                 options=[ft.DropdownOption(key=k, text=t) for k, t in fmt.MODE_OPTIONS if k != "all"] + [ft.DropdownOption(key="all", text="Все")])
        self._before = ft.TextField(label="Новости: мин ДО", value="5", width=130, dense=True, border_color=w.BORDER)
        self._after = ft.TextField(label="мин ПОСЛЕ", value="5", width=110, dense=True, border_color=w.BORDER)
        self._run = ft.FilledButton("Run Deep Analysis", icon=ft.Icons.ANALYTICS, on_click=self._run_clicked)
        self._ring = ft.ProgressRing(width=20, height=20, stroke_width=3, visible=False)
        self._bar = ft.ProgressBar(width=260, value=0, visible=False, color=w.ACCENT, bgcolor=w.BORDER)
        self._stage = ft.Text("", size=12, color=w.MUTED)
        self._info = ft.Text("", size=12, color=w.MUTED)
        self._goal = w.GoalBar(fmt.TRADES_GOAL, width=150)  # «значимость выводов»: сделок в отчёте / 200, у правого края строки фильтров
        self._body = ft.Column(spacing=14)
        self.control = ft.Column([ft.Container(height=2),
                                  ft.Row([ft.Row([self._mode, self._before, self._after, self._run, self._ring, ft.Column([self._bar, self._stage], spacing=2, tight=True)],
                                                 spacing=10, run_spacing=8, wrap=True, expand=True, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                                          self._goal.control], spacing=12, vertical_alignment=ft.CrossAxisAlignment.START), self._info, self._body],
                                 expand=True, scroll=ft.ScrollMode.AUTO, spacing=12)
        self._job_id: str | None = None
        self._report: dict | None = None
        self._series: list[dict] = []
        self._checks: dict[str, ft.Checkbox] = {}
        self._chart_slot = ft.Container()
        self._picked: dict[str, ft.Checkbox] = {}
        self._dry_btn = ft.FilledTonalButton("Apply Recommendations (Dry Run)", icon=ft.Icons.DIFFERENCE, disabled=True, on_click=self._dry_run_clicked)
        self._running = False
        self._clipboard: ft.Clipboard | None = None
        self._placeholder("Нажмите «Run Deep Analysis»: анализатор посчитает метрики по закрытым сделкам выбранного режима.")

    # ------------------------------------------------------------------------------------------ состояние

    def _placeholder(self, text: str) -> None:
        self._body.controls = [w.card(w.label(text, size=14))]

    def reset(self) -> None:
        """Профиль сменился: прежний отчёт относился к другому набору стратегий и монет - убираем его."""
        if self._running:
            return
        self._report, self._job_id = None, None
        self._goal.set(0)
        self._placeholder("Нажмите «Run Deep Analysis»: анализатор посчитает метрики по закрытым сделкам выбранного режима.")
        if w.attached(self.control):
            self.control.update()

    def _show_error(self, exc: ApiError) -> None:
        color = w.AMBER if exc.kind == "analyzer" else w.RED
        self._body.controls = [w.banner(str(exc), color, ft.Icons.WARNING_AMBER if exc.kind == "analyzer" else ft.Icons.ERROR)]

    def _busy(self, on: bool, stage: str = "", progress: float | None = None) -> None:
        self._running = on
        self._ring.visible = self._bar.visible = on
        self._run.disabled = on
        self._bar.value = progress
        self._stage.value = stage
        self.control.update()

    async def refresh(self) -> None:
        """Вход на вкладку: статус анализатора (календарь, очередь) и последний готовый отчёт, если свой ещё не запрошен."""
        client = self.host.client
        if client is None or self._running:
            return
        try:
            st = await client.analysis_status()
            if self._report is None:
                runs = (await client.analysis_runs(10))["items"]
                done = next((r for r in runs if r["status"] == "done"), None)
                if done:
                    data = await client.analysis_report(done["id"])
                    self._show_report(done["id"], data["report"])
        except ApiError as exc:
            self._info.value = ""
            self._show_error(exc)
            self.control.update()
            return
        cal = st["calendar"]
        cal_text = (f"календарь: {cal['events']} событий ({fmt.fmt_local(cal['from'])} — {fmt.fmt_local(cal['to'])})" if cal["events"]
                    else "календарь новостей ещё не получен от бота (News Impact недоступен)")
        self._info.value = f"Анализатор v{st['version']} · воркер {'работает' if st['worker_alive'] else 'НЕ работает'} · {cal_text} · порог значимости {st['min_conclusions']} сделок"
        self.control.update()

    # ------------------------------------------------------------------------------------------ запуск

    def _request_body(self) -> dict:
        def minutes(f: ft.TextField) -> float:
            try:
                return max(0.0, min(240.0, float((f.value or "5").replace(",", "."))))
            except ValueError:
                return 5.0
        scope = {}
        strategies, pool = getattr(self.host, "profile_strategies", lambda: None)(), getattr(self.host, "profile_coins", lambda: None)()
        if strategies is not None:  # профиль со своими стратегиями (CHR): анализ только по ним и по их пулу монет
            scope = {"strategies": strategies, "coins": pool, "mode": "shadow"}
        return {**{"mode": self._mode.value or "shadow"}, **scope, "news": {"enabled": True, "before_min": minutes(self._before), "after_min": minutes(self._after), "mode": "touch", "impacts": ["high"]}}

    async def _run_clicked(self, _=None) -> None:
        client = self.host.client
        if client is None or self._running:
            return
        self._busy(True, "постановка в очередь…", None)
        try:
            job = await client.analysis_start(self._request_body())
            await self._poll(client, job["job_id"])
        except ApiError as exc:
            self._show_error(exc)
        finally:
            self._busy(False)

    async def _poll(self, client, job_id: str) -> None:
        waited = 0.0
        while True:
            job = await client.analysis_job(job_id)
            if job["status"] == "failed":
                self._body.controls = [w.banner(f"Анализ не выполнен: {job['error']}", w.RED, ft.Icons.ERROR)]
                return
            if job["status"] == "done":
                data = await client.analysis_report(job_id)
                self._show_report(job_id, data["report"])
                return
            label = "в очереди…" if job["status"] == "queued" else f"считаю: {job['stage'] or 'старт'} ({job['progress'] * 100:.0f}%)"
            self._busy(True, label, job["progress"] if job["status"] == "running" else None)
            if waited >= POLL_MAX_S:
                self._body.controls = [w.banner("Анализатор долго не отвечает. Смотрите ./deploy/shadow.sh logs analyzer", w.AMBER)]
                return
            await asyncio.sleep(POLL_S)
            waited += POLL_S

    # ------------------------------------------------------------------------------------------ отчёт

    def _show_report(self, job_id: str, r: dict) -> None:
        self._job_id, self._report = job_id, r
        out: list[ft.Control] = []
        for text in r.get("warnings") or []:
            out.append(w.banner(text, w.AMBER))
        d, o = r["data"], r["overall"]
        self._goal.set(int(d["trades"]))
        out.append(w.label(f"Выборка: {d['trades']} закрытых сделок · {', '.join(fmt.STRATEGY_RU.get(s, s) for s in d['strategies']) or '—'} · "
                           f"{fmt.fmt_local(d['from'])} — {fmt.fmt_local(d['to'])} · отчёт {job_id}", size=12))
        out.append(self._overall_kpis(o))
        out.append(self._strategy_table(r))
        self._series = self._build_series(r)
        out.append(self._chart_card())
        out.append(self._news_card(r["news"]))
        comps = (r.get("versions") or {}).get("comparisons") or []
        if comps:
            out.append(self._versions_card(comps))
        out.append(self._sessions_card(r))
        out.append(self._recommendations_card(r.get("recommendations") or []))
        self._body.controls = out

    @staticmethod
    def _overall_kpis(m: dict) -> ft.Control:
        sig = m["significance"]
        pf = fmt.fmt_pf(m)
        ci = m.get("expectancy_r_ci")
        return ft.Row([
            w.kpi("Сделок", str(m["n"]), sub="достаточно данных" if sig["enough"] else f"мало (нужно {sig['min_trades']}+)", color=w.TEXT if sig["enough"] else w.AMBER),
            w.kpi("Profit Factor", pf, color=w.MUTED if pf == fmt.DASH else (w.GREEN if pf == "∞" or float(pf) >= 1 else w.RED)),
            w.kpi("Sharpe / Sortino", f"{_n(m.get('sharpe_daily'))} / {_n(m.get('sortino_daily'))}", sub="дневные, годовые (√365)", width=230,
                  tooltip="Sharpe и Sortino по дневным суммам R, нормированные на год (365 дней)"),
            w.kpi("Матожидание", fmt.fmt_r(m["expectancy_r"]), color=fmt.r_color(m["expectancy_r"]),
                  sub=(f"95%: {_n(ci[0], 2, True)} … {_n(ci[1], 2, True)}" if ci else None) or (f"{_n(m.get('expectancy_usd'), 2, True)} $ на сделку"), width=220),
            w.kpi("Макс. просадка", fmt.fmt_r(-m["max_drawdown_r"]) if m.get("max_drawdown_r") else "0.00 R", color=w.RED if m.get("max_drawdown_r") else w.MUTED,
                  sub=f"{_n(m.get('max_drawdown_pct'), 1)}% · {_n(m.get('max_drawdown_usd'), 0)} $"),
        ], wrap=True, spacing=12, run_spacing=12)

    def _strategy_table(self, r: dict) -> ft.Control:
        muted = lambda t, right=False: w.cell(t, color=w.MUTED, size=12, align_right=right)  # noqa: E731
        head = ["Стратегия", "Сделок", "Win Rate", "PF", "Sharpe", "Sortino", "Матожид. R", "Матожид. $", "MDD R", "MDD %", "Значимость"]
        rows = [w.table_row([muted(h, i > 0) for i, h in enumerate(head)], widths=STRAT_COLS, height=26)]
        for sid, s in sorted(r["strategies"].items()):
            m = s["metrics"]
            rows.append(w.table_row([
                w.cell(fmt.STRATEGY_RU.get(sid, sid), weight=ft.FontWeight.W_700), w.cell(str(m["n"]), align_right=True), w.cell(fmt.fmt_pct(m["winrate"]), align_right=True),
                w.cell(fmt.fmt_pf(m), align_right=True), w.cell(_n(m.get("sharpe_daily")), align_right=True), w.cell(_n(m.get("sortino_daily")), align_right=True),
                w.cell(_n(m["expectancy_r"], 3, True), color=fmt.r_color(m["expectancy_r"]), align_right=True),
                w.cell(_n(m.get("expectancy_usd"), 2, True), color=fmt.r_color(m.get("expectancy_usd")), align_right=True),
                w.cell(_n(m.get("max_drawdown_r")), align_right=True), w.cell(_n(m.get("max_drawdown_pct"), 1), align_right=True),
                w.cell("достаточно" if m["significance"]["enough"] else "мало данных", color=w.GREEN if m["significance"]["enough"] else w.AMBER, size=12, align_right=True)],
                widths=STRAT_COLS))
        return w.card(ft.Column([w.title("Метрики по стратегиям", 15), ft.Column(rows, spacing=2, scroll=ft.ScrollMode.AUTO)], spacing=10))

    # ---- график PnL с переключением серий

    def _build_series(self, r: dict) -> list[dict]:
        series: list[dict] = []

        def add(name: str, curve: dict | None) -> None:
            if curve and curve.get("i"):
                series.append({"name": name, "x": curve["i"], "y": curve["cum_r"], "color": w.SERIES_COLORS[len(series) % len(w.SERIES_COLORS)]})
        for sid, s in sorted(r["strategies"].items()):
            add(fmt.STRATEGY_RU.get(sid, sid), s.get("curve"))
        news = r.get("news") or {}
        if news.get("available"):
            add("Все сделки (период календаря)", news["curves"]["all"])
            add(f"Без новостей ±{news['params']['before_min']:g}/{news['params']['after_min']:g} мин", news["curves"]["without_news"])
        for c in (r.get("versions") or {}).get("comparisons") or []:
            add(f"Версия {c['baseline']['version']['name']}", c["baseline"]["curve"])
            add(f"Версия {c['candidate']['version']['name']}", c["candidate"]["curve"])
        for s in series:
            s["visible"] = True
        for s in series[8:]:  # много серий: по умолчанию показываем первые восемь (цветов столько же)
            s["visible"] = False
        return series

    def _chart_card(self) -> ft.Control:
        self._checks = {}
        boxes = []
        for s in self._series:
            cb = ft.Checkbox(label=s["name"], value=s["visible"], active_color=s["color"], on_change=lambda e, name=s["name"]: self._toggle(name, bool(e.control.value)))
            self._checks[s["name"]] = cb
            boxes.append(cb)
        self._redraw()
        return w.card(ft.Column([w.title("Кривая PnL (накопленный R по номеру сделки)", 15), ft.Row(boxes, wrap=True, spacing=12, run_spacing=0), self._chart_slot], spacing=8))

    def _toggle(self, name: str, on: bool) -> None:
        for s in self._series:
            if s["name"] == name:
                s["visible"] = on
        self._redraw()
        self._chart_slot.update()

    def _redraw(self) -> None:
        self._chart_slot.content = w.line_chart([s for s in self._series if s["visible"]])

    # ---- новости, версии, сессии

    def _news_card(self, n: dict) -> ft.Control:
        head = w.title("News Impact: все сделки против «бот выключен на новостях»", 15)
        if not n.get("available"):
            reason = {"calendar_not_loaded": "Календарь ещё не получен: бот обновляет его раз в час, анализатор подхватывает автоматически (CPI, NFP, FOMC).",
                      "disabled": "Анализ новостей выключен."}.get(n.get("reason"), n.get("reason") or "недоступно")
            return w.card(ft.Column([head, w.label(reason, size=13)], spacing=8))
        p, a, k = n["params"], n["all"], n["without_news"]
        muted = lambda t, right=False: w.cell(t, color=w.MUTED, size=12, align_right=right)  # noqa: E731
        cols = [210, 70, 80, 80, 90, 90]
        rows = [w.table_row([muted(h, i > 0) for i, h in enumerate(["Выборка", "Сделок", "Win Rate", "PF", "Матожид. R", "Net R"])], widths=cols, height=26)]
        for name, m in (("Все сделки", a), (f"Без новостей ±{p['before_min']:g}/{p['after_min']:g} мин", k), ("Только у новостей", n["news_only"])):
            rows.append(w.table_row([w.cell(name, weight=ft.FontWeight.W_600), w.cell(str(m["n"]), align_right=True), w.cell(fmt.fmt_pct(m["winrate"]), align_right=True),
                                     w.cell(fmt.fmt_pf(m), align_right=True), w.cell(_n(m["expectancy_r"], 3, True), color=fmt.r_color(m["expectancy_r"]), align_right=True),
                                     w.cell(_n(m["net_r"], 2, True), color=fmt.r_color(m["net_r"]), align_right=True)], widths=cols))
        d = n["delta"]
        cov = n["coverage"]
        lines = [head, ft.Column(rows, spacing=2, scroll=ft.ScrollMode.AUTO),
                 w.label(f"Исключено {n['n_excluded']} сделок. Эффект выключения на новостях: матожидание {_n(d['expectancy_r'], 3, True)} R, Net R {_n(d['net_r'], 2, True)}, "
                         f"PF {_n(d['profit_factor'], 2, True)}. Оценено сделок внутри календаря: {cov['trades_inside']} (вне календаря: {cov['trades_outside']}).", size=12)]
        if n.get("by_event_kind"):
            lines.append(w.label("По типам событий: " + "; ".join(f"{x['event_kind']} — {x['n']} сд., {_n(x['expectancy_r'], 2, True)} R" for x in n["by_event_kind"]), size=12))
        return w.card(ft.Column(lines, spacing=8))

    def _versions_card(self, comps: list[dict]) -> ft.Control:
        lines: list[ft.Control] = [w.title("Версии стратегий: A/B и деградация", 15)]
        for c in comps:
            a, b, v = c["baseline"], c["candidate"], c["verdict"]
            text, color = VERDICT_RU.get(v["verdict"], (v["verdict"], w.MUTED))
            ma, mb = a["metrics"], b["metrics"]
            lines.append(ft.Row([w.pill(text, color, filled=v["verdict"] in ("degraded", "improved")),
                                 ft.Text(f"{a['version']['name']} → {b['version']['name']}", size=13, weight=ft.FontWeight.W_600),
                                 ft.Text(f"Матожид.: {_n(ma['expectancy_r'], 3, True)} → {_n(mb['expectancy_r'], 3, True)} R · PF {fmt.fmt_pf(ma)} → {fmt.fmt_pf(mb)} · сделок {ma['n']} → {mb['n']}",
                                         size=12, color=w.MUTED)], spacing=10, wrap=True))
            note = v["message"] + (f" {c['note']}" if c.get("note") else "")
            lines.append(w.label(note, size=12))
        return w.card(ft.Column(lines, spacing=6))

    def _sessions_card(self, r: dict) -> ft.Control:
        lines: list[ft.Control] = [w.title("Торговые сессии (UTC, по времени входа)", 15)]
        for sid, s in sorted(r["strategies"].items()):
            rep = s["sessions"]
            parts = [f"{'★ ' if x['session'] == rep['best'] else ''}{x['session']}: {x['n']} сд., WR {fmt.fmt_pct(x['winrate'], 0)}, {_n(x['expectancy_r'], 2, True)} R" for x in rep["rows"]]
            lines.append(ft.Row([ft.Text(fmt.STRATEGY_RU.get(sid, sid), size=13, weight=ft.FontWeight.W_600, width=170), ft.Text(" · ".join(parts) or "нет сделок", size=12, color=w.MUTED, expand=True)],
                                spacing=8, vertical_alignment=ft.CrossAxisAlignment.START))
            vsa = s["vsa"]["profile"]["features"]
            top = next((f for f in vsa if f["feature"] == "vol_ratio" and f["p_value"] is not None), None)
            if top:
                lines.append(w.label(f"   VSA: объём сигнальной свечи / среднее за 20 баров — победители {_n(top['winners_median'])}× против {_n(top['others_median'])}× (p≈{_n(top['p_value'], 3)})", size=11))
        return w.card(ft.Column(lines, spacing=6))

    # ---- рекомендации и Dry Run

    def _recommendations_card(self, recs: list[dict]) -> ft.Control:
        head = w.title("Рекомендации системы", 15)
        self._picked = {}
        if not recs:
            self._dry_btn.disabled = True
            return w.card(ft.Column([head, w.label("Рекомендаций нет: нет статистически значимых отклонений (или мало сделок).", size=13)], spacing=8))
        rows: list[ft.Control] = []
        for r in recs:
            conf_text, conf_color = CONF_RU.get(r["confidence"], (r["confidence"], w.MUTED))
            e = r["effect"]
            eff = f"Матожид. {_n(e['before'], 3, True)} → {_n(e['after'], 3, True)} R" + (f" ({e['delta_pct']:+.0f}%)" if e.get("delta_pct") is not None else "")
            cb = ft.Checkbox(value=False, on_change=self._pick_changed)
            self._picked[r["id"]] = cb
            rows.append(ft.Container(content=ft.Row([cb, ft.Column([
                ft.Row([w.pill(KIND_RU.get(r["kind"], r["kind"]), w.ACCENT), ft.Text(r["title"], size=13, weight=ft.FontWeight.W_600, expand=True), w.pill(f"уверенность: {conf_text}", conf_color)], spacing=8),
                ft.Text(f"{r['param']}: {_val(r['current'])} → {_val(r['proposed'])}   ·   {eff}   ·   сделок после: {e['n_after']}", size=12, color=w.MUTED),
                ft.Text(r["rationale"], size=11, color=w.MUTED)], spacing=3, expand=True)], vertical_alignment=ft.CrossAxisAlignment.START),
                padding=ft.Padding.symmetric(horizontal=8, vertical=6), border_radius=8, bgcolor=w.CARD_HI, tooltip=r["assumptions"]))
        self._dry_btn.disabled = True
        return w.card(ft.Column([head, w.label("Это гипотезы по прошлым данным (не прогноз). Отметьте нужные и нажмите Dry Run: анализатор покажет diff, ничего не меняя.", size=12),
                                 *rows, ft.Row([self._dry_btn])], spacing=8))

    def _pick_changed(self, _=None) -> None:
        self._dry_btn.disabled = not any(cb.value for cb in self._picked.values())
        self._dry_btn.update()

    def selected_ids(self) -> list[str]:
        return [rid for rid, cb in self._picked.items() if cb.value]

    async def _dry_run_clicked(self, _=None) -> None:
        client, ids = self.host.client, self.selected_ids()
        if client is None or self._job_id is None or not ids:
            return
        try:
            res = await client.analysis_dry_run(self._job_id, ids)
        except ApiError as exc:
            self.host.notify(str(exc), error=True)
            return
        self._show_diff(res)

    def _show_diff(self, res: dict) -> None:
        diff = res["diff"] or "(изменений нет)"

        async def copy(_=None) -> None:
            if self._clipboard is None:
                self._clipboard = ft.Clipboard()
                self.host.page.services.append(self._clipboard)
            await self._clipboard.set(diff)
            self.host.notify("Diff скопирован в буфер обмена")

        body = ft.Column([
            ft.Text(res["note"], size=12, color=w.AMBER),
            ft.Container(content=ft.Column([ft.Text(diff, font_family="Consolas", size=12, selectable=True, color=w.TEXT)], scroll=ft.ScrollMode.AUTO), height=380, padding=10,
                         bgcolor=w.BG, border_radius=8, border=ft.Border.all(1, w.BORDER))], spacing=10, tight=True)
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Dry Run: что изменится в параметрах"), content=ft.Container(width=760, content=body),
                                actions=[ft.TextButton("Скопировать diff", on_click=copy), ft.FilledButton("Закрыть", on_click=lambda e: self.host.page.pop_dialog())],
                                actions_alignment=ft.MainAxisAlignment.END)
        self.host.page.show_dialog(dialog)
