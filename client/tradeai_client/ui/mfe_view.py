"""Вкладка «MFE Simulator» (2026-10-24): доигрывание ВСЕХ сделок с фактом входа (любой статус, Shadow и Live) по 1m-свечам Binance на сервере
(api/mfe_sim.py). Кнопка запускает расчёт, вкладка опрашивает статус и показывает: вердикт по тейку, матрицу PnL по тейкам 1:1…1:4,
стоп-сквизы и отступ стопа, разбивку по стратегиям и полный учёт сделок (сколько каких статусов вошло, что пропущено и почему)."""

import asyncio
from typing import Any

import flet as ft

from tradeai_client import fmt
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

SINCE = "2026-10-20"
POLL_S = 2.0
POLL_MAX_S = 1800.0
STATUS_RU = {"closed": "закрыта", "aborted": "прервана (рестарт)", "open": "открыта", "pending_limit": "ждёт лимитку", "pending_market": "ждёт вход",
             "cancelled": "отменена"}
TP_COLS = [90, 70, 70, 80, 90, 100, 110, 70]
STRAT_COLS = [190, 60, 90, 110, 110, 90, 120]


def _usd(x: float | None) -> str:
    return fmt.DASH if x is None else f"{x:+.2f} $".replace("-", "−")


def _r(x: float | None, nd: int = 3) -> str:
    return fmt.DASH if x is None else f"{x:+.{nd}f} R".replace("-", "−")


def _pct(x: float | None) -> str:
    return fmt.DASH if x is None else f"{100 * x:.0f}%"


def _color(x: float | None) -> str:
    return w.MUTED if x is None or x == 0 else (w.GREEN if x > 0 else w.RED)


class MfeView:
    def __init__(self, host) -> None:
        self.host = host
        self._run = ft.FilledButton(f"Запустить анализ с {SINCE[8:10]} октября", icon=ft.Icons.PLAY_ARROW, on_click=self._run_clicked)
        self._ring = ft.ProgressRing(width=20, height=20, stroke_width=3, visible=False)
        self._bar = ft.ProgressBar(width=260, value=0, visible=False, color=w.ACCENT, bgcolor=w.BORDER)
        self._stage = ft.Text("", size=12, color=w.MUTED)
        self._body = ft.Column(spacing=14)
        self.control = ft.Column([ft.Container(height=2),
                                  ft.Row([self._run, self._ring, ft.Column([self._bar, self._stage], spacing=2, tight=True)], spacing=12, wrap=True,
                                         vertical_alignment=ft.CrossAxisAlignment.CENTER),
                                  self._body], expand=True, scroll=ft.ScrollMode.AUTO, spacing=12)
        self._running = False
        self._loaded = False
        self._placeholder()

    def _placeholder(self) -> None:
        self._body.controls = [w.card(ft.Column([
            w.title("MFE Simulator"),
            w.label("Сервер берёт ВСЕ сделки с фактом входа с 20 октября - закрытые, прерванные рестартом, с обрывом тиков, ещё открытые, Shadow и Live - "
                    "и доигрывает каждую по минутным свечам Binance: как далеко цена ушла в нашу сторону до стопа, что дал бы каждый тейк и "
                    "как часто стоп выбивало сквизом.", size=13),
            w.label("Расчёт идёт на сервере 1-5 минут (свечи запрашиваются бережно, чтобы не задеть лимиты бота).", size=12)], spacing=8))]

    def relayout(self) -> None:
        return None

    # ------------------------------------------------------------------------------------------ данные

    async def refresh(self) -> None:
        """Вкладка открыта: показать последний результат сервера (если расчёт уже был или идёт)."""
        if self._running or self._loaded or self.host.client is None:
            return
        try:
            st = await self.host.client.mfe_status()
        except ApiError as exc:
            self._error(str(exc))
            return
        if st.get("status") == "running":
            await self._follow()
        elif st.get("status") == "done" and st.get("report"):
            self._show(st["report"])
        elif st.get("status") == "failed":
            self._error(st.get("error") or "расчёт не удался")

    async def _run_clicked(self, e) -> None:
        if self._running or self.host.client is None:
            return
        try:
            await self.host.client.mfe_run(SINCE)
        except ApiError as exc:
            if exc.kind != "conflict":  # уже идёт - просто следим
                self._error(str(exc))
                return
        await self._follow()

    async def _follow(self) -> None:
        self._set_running(True)
        waited = 0.0
        try:
            while waited < POLL_MAX_S:
                st = await self.host.client.mfe_status()
                prog = st.get("progress") or {}
                total, done = prog.get("total") or 0, prog.get("done") or 0
                self._bar.value = (done / total) if total else None
                self._stage.value = f"{prog.get('stage', '')}: {done}/{total} монет" if total else str(prog.get("stage", "запуск"))
                self._update()
                if st.get("status") == "done":
                    self._show(st.get("report") or {})
                    return
                if st.get("status") == "failed":
                    self._error(st.get("error") or "расчёт не удался")
                    return
                await asyncio.sleep(POLL_S)
                waited += POLL_S
            self._error("Расчёт идёт дольше 30 минут: откройте вкладку позже - результат сохранится на сервере.")
        except ApiError as exc:
            self._error(str(exc))
        finally:
            self._set_running(False)

    def _set_running(self, on: bool) -> None:
        self._running = on
        self._run.disabled, self._ring.visible, self._bar.visible = on, on, on
        if not on:
            self._stage.value = ""
        self._update()

    def _update(self) -> None:
        if w.attached(self.control):
            self.control.update()

    def _error(self, text: str) -> None:
        self._body.controls = [w.banner(f"MFE Simulator: {text}", w.RED, ft.Icons.ERROR_OUTLINE)]
        self._update()

    # ------------------------------------------------------------------------------------------ отчёт

    def _show(self, rep: dict[str, Any]) -> None:
        self._loaded = True
        self._body.controls = [self._verdict(rep), self._coverage(rep), self._tp_matrix(rep), self._stops(rep), self._strategies(rep)]
        self._update()

    def _verdict(self, rep: dict[str, Any]) -> ft.Control:
        v = rep.get("verdict") or {}
        best = v.get("best_tp")
        color = w.GREEN if (v.get("best_usd") or 0) > 0 else w.AMBER
        head = ft.Row([ft.Icon(ft.Icons.EMOJI_OBJECTS, color=color, size=30),
                       ft.Text(f"Оптимальный Take-Profit: 1:{best:g}" if best else "Вердикт недоступен", size=22, weight=ft.FontWeight.W_700, color=color)],
                      spacing=10)
        mfe = rep.get("mfe") or {}
        sub = (f"Медиана хода в нашу сторону до стопа (MFE): {mfe.get('median') if mfe.get('median') is not None else fmt.DASH} R · "
               f"до +1R дошли {mfe.get('reach1', 0)} из {v.get('n', 0)} · медиана просадки (MAE): {mfe.get('mae_median') if mfe.get('mae_median') is not None else fmt.DASH} R")
        return w.card(ft.Column([head, ft.Text(v.get("text", ""), size=14, color=w.TEXT, selectable=True), w.label(sub)], spacing=8),
                      border_color=color)

    def _coverage(self, rep: dict[str, Any]) -> ft.Control:
        c = rep.get("counts") or {}
        st = ", ".join(f"{STATUS_RU.get(k, k)}: {n}" for k, n in sorted((c.get("statuses") or {}).items(), key=lambda kv: -kv[1]))
        modes = c.get("modes") or {}
        skipped = []
        if c.get("live_duplicates"):
            skipped.append(f"live-копий теневых сделок (тот же сигнал) - {c['live_duplicates']}")
        if c.get("no_candles"):
            skipped.append(f"без минутных свечей Binance - {c['no_candles']} ({', '.join(c.get('no_candles_symbols') or [])})")
        if c.get("bad_stop"):
            skipped.append(f"стоп не за входом - {c['bad_stop']}")
        kpis = ft.Row([w.kpi("Сделок с входом", str(c.get("with_entry", 0)), sub=f"с {rep.get('since')}", width=180),
                       w.kpi("Доиграно", str(c.get("simulated", 0)), color=w.ACCENT, sub=f"Shadow {modes.get('shadow', 0)} · Live {modes.get('live', 0)}", width=180),
                       w.kpi("Запросов к Binance", str(c.get("binance_requests", 0)), sub=f"расчёт: {rep.get('generated_at', '')[11:16]} UTC", width=180)],
                      spacing=12, wrap=True)
        return w.card(ft.Column([w.title("Учтены все сделки, а не только закрытые", 16), kpis, w.label(f"Статусы в выборке: {st or fmt.DASH}", size=12),
                                 w.label("Не доиграны: " + ("; ".join(skipped) if skipped else "нет"), size=12)], spacing=10))

    def _tp_matrix(self, rep: dict[str, Any]) -> ft.Control:
        rows = rep.get("tp_matrix") or []
        best = (rep.get("verdict") or {}).get("best_tp")
        head = w.table_row([w.cell(t, color=w.MUTED, size=12) for t in ("Тейк", "Сделок", "Побед", "Винрейт", "Σ R", "E[R]/сделку", "Заработок", "PF")],
                           widths=TP_COLS, bgcolor=w.BG)
        lines = [head]
        phrases = []
        for r in rows:
            mark = r["tp"] == best
            lines.append(w.table_row([
                w.cell(f"1:{r['tp']:g}" + ("  ★" if mark else ""), weight=ft.FontWeight.W_700 if mark else None, color=w.GREEN if mark else w.TEXT),
                w.cell(str(r["n"])), w.cell(str(r["wins"])), w.cell(_pct(r["win_rate"])), w.cell(_r(r["sum_r"], 2), color=_color(r["sum_r"])),
                w.cell(_r(r["mean_r"]), color=_color(r["mean_r"])), w.cell(_usd(r["usd"]), color=_color(r["usd"]), weight=ft.FontWeight.W_600),
                w.cell(fmt.DASH if r["pf"] is None else f"{r['pf']:.2f}")], widths=TP_COLS, bgcolor=ft.Colors.with_opacity(0.08, w.GREEN) if mark else None))
            phrases.append(ft.Text(f"Если бы Take-Profit был 1:{r['tp']:g} — заработок {_usd(r['usd'])}", size=13, color=_color(r["usd"])))
        unfinished = max((r.get("unfinished", 0) for r in rows), default=0)
        note = f"Сделок, путь которых ещё не закончился (48 ч не прошли и стоп/тейк не задет): до {unfinished} - в варианте не считаются." if unfinished else ""
        return w.card(ft.Column([w.title("Матрица PnL по тейкам (риск $5 на сделку)", 16), ft.Column(phrases, spacing=4),
                                 ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO), w.label(note)], spacing=10))

    def _stops(self, rep: dict[str, Any]) -> ft.Control:
        sq = rep.get("squeeze") or {}
        pad = rep.get("padding") or {}
        checked = sq.get("checked") or 0
        sq_text = (f"Стоп задет в {sq.get('stopped', 0)} сделках. После стопа цена вернулась ко входу в {sq.get('back_to_entry', 0)}, "
                   f"дошла до +1R в {sq.get('reach1', 0)} ({_pct(sq.get('rate'))}) и до +2R в {sq.get('reach2', 0)} случаях за 6 часов - "
                   f"это выбивание сквизом. Медиана того, насколько тень зашла за стоп: "
                   f"{fmt.DASH if sq.get('overshoot_atr_median') is None else str(sq['overshoot_atr_median']) + ' ATR'}." if checked else "Стопов в выборке нет.")
        tps = [str(x) for x in (1.0, 1.5, 2.0, 2.5, 3.0, 4.0)]
        widths = [150] + [96] * len(tps)
        lines = [w.table_row([w.cell("Доп. отступ стопа", color=w.MUTED, size=12)] + [w.cell(f"TP 1:{float(x):g}", color=w.MUTED, size=12) for x in tps],
                             widths=widths, bgcolor=w.BG)]
        sug = pad.get("suggest") or {}
        for row in pad.get("rows") or []:
            cells = [w.cell("текущий (0.2 ATR)" if row["pad_atr"] == 0 else f"+{row['pad_atr']:g} ATR")]
            for x in tps:
                c = row["cells"].get(x) or {}
                hit = sug and sug.get("pad_atr") == row["pad_atr"] and str(sug.get("tp")) == x
                cells.append(w.cell(_usd(c.get("usd")) if c.get("n") else fmt.DASH, color=w.GREEN if hit else _color(c.get("usd")),
                                    weight=ft.FontWeight.W_700 if hit else None))
            lines.append(w.table_row(cells, widths=widths))
        advice = w.banner(pad.get("text", ""), w.GREEN if sug else w.ACCENT, ft.Icons.TUNE)
        return w.card(ft.Column([w.title("Стоп-лоссы: выбивание сквизом и отступ", 16), ft.Text(sq_text, size=13, color=w.TEXT, selectable=True), advice,
                                 w.label(f"Заработок в $ при стопе, отодвинутом ещё на k·ATR сигнальной свечи (риск тот же $5, объём меньше); сделок с ATR: {pad.get('with_atr', 0)}"),
                                 ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO)], spacing=10))

    def _strategies(self, rep: dict[str, Any]) -> ft.Control:
        head = w.table_row([w.cell(t, color=w.MUTED, size=12) for t in ("Стратегия", "Сделок", "Лучший TP", "Заработок (лучш.)", "Заработок 1:4", "MFE мед.", "Сквизов / стопов")],
                           widths=STRAT_COLS, bgcolor=w.BG)
        lines = [head]
        for s in rep.get("by_strategy") or []:
            cur = next((r for r in s["rows"] if r["tp"] == 4.0), {})
            lines.append(w.table_row([w.cell(fmt.STRATEGY_RU.get(s["strategy"], s["strategy"])), w.cell(str(s["n"])),
                                      w.cell(f"1:{s['best_tp']:g}" if s.get("best_tp") else fmt.DASH, color=w.ACCENT),
                                      w.cell(_usd(s.get("best_usd")), color=_color(s.get("best_usd"))), w.cell(_usd(cur.get("usd")), color=_color(cur.get("usd"))),
                                      w.cell(fmt.DASH if s.get("mfe_median") is None else f"{s['mfe_median']:.2f} R"),
                                      w.cell(f"{s['squeeze']['reach1']} / {s['squeeze']['stopped']}")], widths=STRAT_COLS))
        return w.card(ft.Column([w.title("По стратегиям", 16), ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO),
                                 w.label("Мало сделок у стратегии - лучший тейк по ней случаен; смотрите общий вердикт выше.")], spacing=10))
