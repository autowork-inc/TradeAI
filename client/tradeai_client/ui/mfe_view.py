"""Вкладка «MFE Simulator» (1.5.0, 1.6.0): доигрывание ВСЕХ сделок с фактом входа (любой статус, Shadow и Live, открытые - по текущей цене)
по 1m-свечам Binance на сервере (api/mfe_sim.py). Свой расчёт у каждого профиля: CHR - только стратегии *_CHR, SANTA - остальные.
Период - «с даты по дату» (по времени входа). Показывает: вердикт по тейку (план бота: 50% на +1R, остаток на тейке - и без частичной фиксации),
матрицы PnL, стоп-сквизы и отступ стопа, разбивку по стратегиям, учёт сделок. Сбоку - текущие тейки авто-TP бота, одобренные изменения
и шаблоны предложений (с командой отката/применения)."""

import asyncio
import re
from datetime import datetime, timezone
from typing import Any

import flet as ft

from tradeai_client import fmt, profiles
from tradeai_client.api import ApiError
from tradeai_client.ui import widgets as w

SINCE = "2026-10-20"
POLL_S = 2.0
POLL_MAX_S = 1800.0
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STATUS_RU = {"closed": "закрыта", "aborted": "прервана (рестарт)", "open": "открыта", "pending_limit": "ждёт лимитку", "pending_market": "ждёт вход",
             "cancelled": "отменена"}
PROPOSAL_RU = {"pending": ("ждёт решения", w.AMBER), "applied": ("применено", w.GREEN), "template": ("шаблон", w.ACCENT),
               "rolled_back": ("откачено", w.MUTED), "manual": ("вручную", w.MUTED)}
TP_COLS = [80, 64, 64, 72, 86, 96, 104, 60]
STRAT_COLS = [190, 56, 84, 104, 104, 84, 110]


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
        self._since = ft.TextField(label="С даты", value=SINCE, width=140, dense=True, border_color=w.BORDER, hint_text="ГГГГ-ММ-ДД")
        self._until = ft.TextField(label="По дату", value="", width=140, dense=True, border_color=w.BORDER, hint_text="пусто = сегодня")
        self._run = ft.FilledButton("Запустить анализ", icon=ft.Icons.PLAY_ARROW, on_click=self._run_clicked)
        self._scope_text = ft.Text("", size=12, color=w.MUTED)
        self._ring = ft.ProgressRing(width=20, height=20, stroke_width=3, visible=False)
        self._bar = ft.ProgressBar(width=220, value=0, visible=False, color=w.ACCENT, bgcolor=w.BORDER)
        self._stage = ft.Text("", size=12, color=w.MUTED)
        self._body = ft.Column(spacing=14)
        self._side = ft.Column(spacing=10)
        head = ft.Row([self._since, self._until, self._run, self._ring, ft.Column([self._bar, self._stage], spacing=2, tight=True), self._scope_text],
                      spacing=12, run_spacing=8, wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        self.control = ft.Column([ft.Container(height=2), head,
                                  ft.ResponsiveRow([ft.Column([self._body], col={"xs": 12, "lg": 9}), ft.Column([self._side], col={"xs": 12, "lg": 3})],
                                                   spacing=14, run_spacing=14, vertical_alignment=ft.CrossAxisAlignment.START)],
                                 expand=True, scroll=ft.ScrollMode.AUTO, spacing=12)
        self._running = False
        self._loaded_scope: str | None = None
        self._placeholder()

    # ------------------------------------------------------------------------------------------ профиль

    @property
    def scope(self) -> str:
        return "chr" if getattr(self.host, "settings", None) is not None and self.host.settings.profile == profiles.CHR else "santa"

    def _scope_label(self) -> str:
        return "Профиль CHR: только стратегии CHR" if self.scope == "chr" else "Профиль SANTA: все стратегии, кроме CHR"

    def reset(self) -> None:
        """Профиль сменился: расчёт другого профиля - другой набор сделок."""
        if self._running:
            return
        self._loaded_scope = None
        self._placeholder()
        self._side.controls = []
        self._update()

    def relayout(self) -> None:
        return None

    def _placeholder(self) -> None:
        self._scope_text.value = self._scope_label()
        self._body.controls = [w.card(ft.Column([
            w.title("MFE Simulator"),
            w.label("Сервер берёт ВСЕ сделки профиля с фактом входа за выбранный период - закрытые, прерванные рестартом, с обрывом тиков, открытые "
                    "(по текущей цене), Shadow и Live - и доигрывает каждую по минутным свечам Binance: как далеко цена ушла в нашу сторону до стопа, "
                    "что дал бы каждый тейк (с частичной фиксацией 50% на +1R, как торгует бот, и без неё) и как часто стоп выбивало сквизом.", size=13),
            w.label("Расчёт идёт на сервере 1-5 минут.", size=12)], spacing=8))]

    # ------------------------------------------------------------------------------------------ данные

    async def refresh(self) -> None:
        """Вкладка открыта: последний результат профиля (если расчёт уже был или идёт) и боковая панель."""
        if self._running or self.host.client is None:
            return
        self._scope_text.value = self._scope_label()
        await self._load_side()
        if self._loaded_scope == self.scope:
            return
        try:
            st = await self.host.client.mfe_status(self.scope)
        except ApiError as exc:
            self._error(str(exc))
            return
        self._loaded_scope = self.scope
        if st.get("status") == "running":
            await self._follow()
        elif st.get("status") == "done" and st.get("report"):
            self._show(st["report"])
        elif st.get("status") == "failed":
            self._error(st.get("error") or "расчёт не удался")
        else:
            self._placeholder()
            self._update()

    def _dates(self) -> tuple[str, str | None] | None:
        since, until = (self._since.value or "").strip(), (self._until.value or "").strip() or None
        bad = [x for x in (since, until) if x is not None and not DATE_RE.match(x)]
        if bad or not since:
            self._error("Дата в формате ГГГГ-ММ-ДД, например 2026-10-20 (поле «По дату» можно оставить пустым - до сегодня).")
            return None
        return since, until

    async def _run_clicked(self, e) -> None:
        if self._running or self.host.client is None:
            return
        dates = self._dates()
        if dates is None:
            return
        try:
            await self.host.client.mfe_run(dates[0], dates[1], self.scope)
        except ApiError as exc:
            if exc.kind != "conflict":  # уже идёт - просто следим
                self._error(str(exc))
                return
        self._loaded_scope = self.scope
        await self._follow()

    async def _follow(self) -> None:
        self._set_running(True)
        waited = 0.0
        try:
            while waited < POLL_MAX_S:
                st = await self.host.client.mfe_status(self.scope)
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

    # ------------------------------------------------------------------------------------------ боковая панель: тейки бота и шаблоны

    async def _load_side(self) -> None:
        try:
            st = await self.host.client.strategy_state(self.scope)
        except ApiError as exc:
            self._side.controls = [w.banner(f"Тейки и шаблоны недоступны: {exc}", w.AMBER)]
            self._update()
            return
        self._side.controls = self.side_controls(st)
        self._update()

    @staticmethod
    def side_controls(st: dict[str, Any]) -> list[ft.Control]:
        auto = (st.get("auto_tp") or {})
        rows: list[ft.Control] = [w.title("Тейки бота сейчас (авто-TP)", 15)]
        strategies = auto.get("strategies") or {}
        if strategies:
            upd = auto.get("updated_ms")
            rows.append(w.label(f"подобраны {fmt.fmt_ms_local(upd) if upd else fmt.DASH} по сделкам с {auto.get('since') or fmt.DASH}; 50% закрывается на +1R", size=11))
            for name, v in sorted(strategies.items()):
                rows.append(ft.Row([ft.Text(fmt.STRATEGY_RU.get(name, name), size=12, color=w.TEXT, expand=True, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS),
                                    ft.Text(f"1:{v.get('tp_r'):g}", size=13, weight=ft.FontWeight.W_700, color=w.ACCENT)], spacing=6,
                                   tooltip=f"сделок {v.get('n', 0)}, Σ {v.get('sum_r', '—')} R; прежний 1:{v.get('prev_r', '—')}"))
        elif st.get("scope") == "chr":
            rows.append(w.label("CHR: тейк всегда 1:4 на весь объём (без «сейфа» и авто-подстройки), стоп статичный.", size=12))
        else:
            rows.append(w.label("Бот ещё не подобрал тейки (нужно 5+ завершённых сделок стратегии): действует 1:4 и 50% на +1R.", size=12))
        overrides = st.get("overrides") or {}
        if overrides:
            rows.append(w.title("Одобренные изменения", 15))
            for name, v in sorted(overrides.items()):
                rows.append(w.label(f"{fmt.STRATEGY_RU.get(name, name)}: {v.get('params') or {}}" + (f", тейк 1:{v['tp_r']:g}" if v.get("tp_r") else ""), size=12))
        proposals = st.get("proposals") or []
        rows.append(w.title("Шаблоны и предложения", 15))
        if not proposals:
            rows.append(w.label("Для этого профиля шаблонов пока нет.", size=12))
        for pr in proposals:
            label, color = PROPOSAL_RU.get(pr.get("status") or "", (pr.get("status") or "", w.MUTED))
            reasons = "\n".join(f"• {x}" for x in (pr.get("rationale") or [])[:3])
            rows.append(w.card(ft.Column([ft.Row([w.pill(label, color), ft.Text(fmt.STRATEGY_RU.get(pr.get("strategy"), pr.get("strategy")), size=11, color=w.MUTED)],
                                                 spacing=6, wrap=True),
                                          ft.Text(pr.get("title") or pr.get("id") or "", size=12, color=w.TEXT, weight=ft.FontWeight.W_600),
                                          ft.Text(f"id: {pr.get('id')}", size=10, color=w.MUTED, selectable=True)], spacing=4, tight=True),
                               padding=10))
            if reasons:
                rows[-1].tooltip = reasons
        rows.append(ft.Text(st.get("how_to") or "", size=11, color=w.MUTED, selectable=True))
        return [w.card(ft.Column(rows, spacing=8), padding=12)]

    # ------------------------------------------------------------------------------------------ отчёт

    def _show(self, rep: dict[str, Any]) -> None:
        scale = self._tp_matrix(rep, "scale_matrix", "50% на +1R, остаток на тейке" + ("" if rep.get("scope") == "chr" else " (план бота)"))
        plain = self._tp_matrix(rep, "tp_matrix", "весь объём на тейке" + (" (план CHR, тейк закреплён 1:4)" if rep.get("scope") == "chr" else ""))
        matrices = [plain, scale] if rep.get("scope") == "chr" else [scale, plain]
        self._body.controls = [self._verdict(rep), self._coverage(rep), *matrices, self._stops(rep), self._strategies(rep)]
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
        return w.card(ft.Column([head, ft.Text(v.get("text", ""), size=14, color=w.TEXT, selectable=True), w.label(sub)], spacing=8), border_color=color)

    def _coverage(self, rep: dict[str, Any]) -> ft.Control:
        c = rep.get("counts") or {}
        st = ", ".join(f"{STATUS_RU.get(k, k)}: {n}" for k, n in sorted((c.get("statuses") or {}).items(), key=lambda kv: -kv[1]))
        modes = c.get("modes") or {}
        notes = []
        if c.get("open_now"):
            notes.append(f"ещё открыты - {c['open_now']} (посчитаны по текущей цене)")
        if c.get("live_copies"):
            notes.append(f"live-копий теневых сигналов - {c['live_copies']} (учтены)")
        skipped = []
        if c.get("no_candles"):
            skipped.append(f"без минутных свечей Binance - {c['no_candles']} ({', '.join(c.get('no_candles_symbols') or [])})")
        if c.get("bad_stop"):
            skipped.append(f"стоп не за входом - {c['bad_stop']}")
        period = f"{rep.get('since')} — {rep.get('until') or 'сегодня'}"
        kpis = ft.Row([w.kpi("Сделок с входом", str(c.get("with_entry", 0)), sub=period, width=190),
                       w.kpi("Доиграно", str(c.get("simulated", 0)), color=w.ACCENT, sub=f"Shadow {modes.get('shadow', 0)} · Live {modes.get('live', 0)}", width=180),
                       w.kpi("Запросов к Binance", str(c.get("binance_requests", 0)), sub=f"расчёт: {rep.get('generated_at', '')[11:16]} UTC", width=180)],
                      spacing=12, wrap=True)
        lines = [w.title("Учтены все сделки, а не только закрытые", 16), kpis, w.label(f"Статусы: {st or fmt.DASH}", size=12)]
        if notes:
            lines.append(w.label("; ".join(notes), size=12))
        lines.append(w.label("Не доиграны: " + ("; ".join(skipped) if skipped else "нет"), size=12))
        return w.card(ft.Column(lines, spacing=10))

    def _tp_matrix(self, rep: dict[str, Any], key: str, title: str) -> ft.Control:
        rows = rep.get(key) or []
        v = rep.get("verdict") or {}
        best = ((v.get("best_plain") or {}).get("tp") if key == "tp_matrix" else (v.get("best_tp") if v.get("plan") != "plain" else None))
        head = w.table_row([w.cell(t, color=w.MUTED, size=12) for t in ("Тейк", "Сделок", "Побед", "Винрейт", "Σ R", "E[R]/сделку", "Заработок", "PF")],
                           widths=TP_COLS, bgcolor=w.BG)
        lines = [head]
        phrases = []
        for r in rows:
            mark = best is not None and abs(r["tp"] - best) < 1e-9
            lines.append(w.table_row([
                w.cell(f"1:{r['tp']:g}" + ("  ★" if mark else ""), weight=ft.FontWeight.W_700 if mark else None, color=w.GREEN if mark else w.TEXT),
                w.cell(str(r["n"])), w.cell(str(r["wins"])), w.cell(_pct(r["win_rate"])), w.cell(_r(r["sum_r"], 2), color=_color(r["sum_r"])),
                w.cell(_r(r["mean_r"]), color=_color(r["mean_r"])), w.cell(_usd(r["usd"]), color=_color(r["usd"]), weight=ft.FontWeight.W_600),
                w.cell(fmt.DASH if r["pf"] is None else f"{r['pf']:.2f}")], widths=TP_COLS, bgcolor=ft.Colors.with_opacity(0.08, w.GREEN) if mark else None))
            phrases.append(ft.Text(f"Если бы Take-Profit был 1:{r['tp']:g} — заработок {_usd(r['usd'])}", size=13, color=_color(r["usd"])))
        if best is not None and not any(abs(r["tp"] - best) < 1e-9 for r in rows):
            phrases.append(ft.Text(f"Лучший тейк вне сетки: 1:{best:g} (подбор без потолка, шаг 0.25)", size=13, color=w.GREEN, weight=ft.FontWeight.W_600))
        return w.card(ft.Column([w.title(f"Матрица PnL — {title} (риск $5 на сделку)", 16), ft.Column(phrases, spacing=4),
                                 ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO)], spacing=10))

    def _stops(self, rep: dict[str, Any]) -> ft.Control:
        sq = rep.get("squeeze") or {}
        pad = rep.get("padding") or {}
        checked = sq.get("checked") or 0
        sq_text = (f"Стоп задет в {sq.get('stopped', 0)} сделках. После стопа цена вернулась ко входу в {sq.get('back_to_entry', 0)}, "
                   f"дошла до +1R в {sq.get('reach1', 0)} ({_pct(sq.get('rate'))}) и до +2R в {sq.get('reach2', 0)} случаях за 6 часов - "
                   f"это выбивание сквизом. Медиана того, насколько тень зашла за стоп: "
                   f"{fmt.DASH if sq.get('overshoot_atr_median') is None else str(sq['overshoot_atr_median']) + ' ATR'}." if checked else "Стопов в выборке нет.")
        tps = list(pad["rows"][0]["cells"]) if pad.get("rows") else []  # ключи - тейки сетки ("1.0", "1.5", ...)
        widths = [150] + [86] * len(tps)
        lines = [w.table_row([w.cell("Доп. отступ стопа", color=w.MUTED, size=12)] + [w.cell(f"TP 1:{float(x):g}", color=w.MUTED, size=12) for x in tps],
                             widths=widths, bgcolor=w.BG)]
        sug = pad.get("suggest") or {}
        for row in pad.get("rows") or []:
            cells = [w.cell("текущий стоп" if row["pad_atr"] == 0 else f"+{row['pad_atr']:g} ATR")]
            for x in tps:
                c = row["cells"].get(x) or {}
                hit = sug and sug.get("pad_atr") == row["pad_atr"] and str(sug.get("tp")) == x
                cells.append(w.cell(_usd(c.get("usd")) if c.get("n") else fmt.DASH, color=w.GREEN if hit else _color(c.get("usd")),
                                    weight=ft.FontWeight.W_700 if hit else None))
            lines.append(w.table_row(cells, widths=widths))
        advice = w.banner(pad.get("text", ""), w.GREEN if sug else w.ACCENT, ft.Icons.TUNE)
        return w.card(ft.Column([w.title("Стоп-лоссы: выбивание сквизом и отступ", 16), ft.Text(sq_text, size=13, color=w.TEXT, selectable=True), advice,
                                 w.label(f"Заработок в $ (план бота: 50% на +1R) при стопе, отодвинутом ещё на k·ATR сигнальной свечи (риск тот же $5); сделок с ATR: {pad.get('with_atr', 0)}"),
                                 ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO)], spacing=10))

    def _strategies(self, rep: dict[str, Any]) -> ft.Control:
        head = w.table_row([w.cell(t, color=w.MUTED, size=12) for t in ("Стратегия", "Сделок", "Лучший TP", "Заработок (лучш.)", "Заработок 1:4", "MFE мед.", "Сквизов / стопов")],
                           widths=STRAT_COLS, bgcolor=w.BG)
        lines = [head]
        for s in rep.get("by_strategy") or []:
            at4 = s.get("usd_at_4")
            if at4 is None:
                at4 = next((r for r in s.get("rows") or [] if r["tp"] == 4.0), {}).get("usd")
            lines.append(w.table_row([w.cell(fmt.STRATEGY_RU.get(s["strategy"], s["strategy"])), w.cell(str(s["n"])),
                                      w.cell(f"1:{s['best_tp']:g}" if s.get("best_tp") else fmt.DASH, color=w.ACCENT),
                                      w.cell(_usd(s.get("best_usd")), color=_color(s.get("best_usd"))), w.cell(_usd(at4), color=_color(at4)),
                                      w.cell(fmt.DASH if s.get("mfe_median") is None else f"{s['mfe_median']:.2f} R"),
                                      w.cell(f"{s['squeeze']['reach1']} / {s['squeeze']['stopped']}")], widths=STRAT_COLS))
        return w.card(ft.Column([w.title("По стратегиям (план бота: 50% на +1R)", 16), ft.Row([ft.Column(lines, spacing=2)], scroll=ft.ScrollMode.AUTO),
                                 w.label("Мало сделок у стратегии - лучший тейк по ней случаен; бот подбирает тейк стратегии сам при 5+ завершённых сделках.")],
                                spacing=10))


def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
