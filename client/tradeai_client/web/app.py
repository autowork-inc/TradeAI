"""Веб-приложение TradeAI (Flet web, 2026-10-20): экран входа -> вкладки Dashboard / Statistics / History / Favorites в мобильной раскладке.

Один процесс обслуживает все браузерные сессии; у каждой сессии свой ApiClient и свой WebSocket живого состояния (внутри docker-сети: http://api:8000).
Ключ API живёт только в окружении контейнера и в браузер не попадает. Анализатор и окно интерактивного графика остаются в настольном клиенте: на телефоне
по двойному тапу сделки открывается карточка разбора (результат, оценка, причина входа). Раскладка адаптивна: при узком экране всё идёт в колонку,
фильтры переносятся, таблица истории листается по горизонтали.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass

import flet as ft

from tradeai_client import APP_NAME, __version__, fmt, profiles
from tradeai_client.api import ApiClient, ApiError, LiveStateStream
from tradeai_client.chart_spec import ChartDataError, build_chart_spec
from tradeai_client.settings import ClientSettings
from tradeai_client.ui import widgets as w
from tradeai_client.ui.mfe_view import MfeView
from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.history_view import HistoryView
from tradeai_client.ui.stats_view import StatsView
from tradeai_client.web import auth, charts

log = logging.getLogger("tradeai.web")
TOKEN_KEY = "tradeai_web_token"
NARROW_PX = 700  # уже этого - телефон: компактные отступы
TAB_DASHBOARD, TAB_STATS, TAB_HISTORY, TAB_MFE, TAB_FAVORITES = range(5)
STATUS_COLORS = {"ok": w.GREEN, "live": w.GREEN, "connecting": w.AMBER, "error": w.RED, "auth": w.RED, "throttled": w.RED}


class WebConfigError(RuntimeError):
    """Веб-версия не настроена (контейнер не стартует: Fail-Fast)."""


@dataclass(frozen=True)
class WebConfig:
    login: str
    password: str
    api_url: str
    api_key: str
    secret: bytes
    port: int

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "WebConfig":
        e = os.environ if env is None else env
        login, password, key = e.get("WEB_LOGIN", "").strip(), e.get("WEB_PASSWORD", ""), e.get("TRADEAI_API_KEY", "").strip()
        if not login or not password:
            raise WebConfigError("WEB_LOGIN и WEB_PASSWORD не заданы (пропишите их в .env.shadow на сервере): вход в веб-версию не может быть открытым")
        if len(key) < 32:
            raise WebConfigError("TRADEAI_API_KEY (ключ API бота) не задан или короче 32 символов")
        return cls(login=login, password=password, api_url=e.get("TRADEAI_API_URL", "http://api:8000").strip(), api_key=key,
                   secret=auth.derive_secret(password, key, e.get("WEB_SESSION_SECRET", "")), port=int(e.get("WEB_PORT", "8550")))


class WebShell:
    """Хост для видов (DashboardView / StatsView / HistoryView): то же, что Shell в настольном клиенте, без окон, диалогов настроек и автообновления."""

    def __init__(self, page: ft.Page, cfg: WebConfig, on_logout) -> None:
        self.page, self.cfg, self._on_logout = page, cfg, on_logout
        self.settings = ClientSettings(base_url=cfg.api_url, api_key=cfg.api_key, remember_key=False, profile=profiles.SANTA, profile_chosen=True)
        self.client: ApiClient | None = ApiClient(self.settings)
        self._stream: LiveStateStream | None = LiveStateStream(self.settings, self._on_message, self._on_status)
        self._tab, self._events_seen, self._closed = TAB_DASHBOARD, (), False
        self.saved_mode, self.default_mode = "", "shadow"
        self.tap_opens_chart = True  # на телефоне нет двойного тапа: один тап по сделке открывает график
        self._launcher = ft.UrlLauncher()
        page.services.append(self._launcher)
        self.dashboard, self.stats, self.history = DashboardView(self), StatsView(self), HistoryView(self)
        self.mfe = MfeView(self)
        self.favorites = HistoryView(self, favorites=True)
        self._dot, self._status_text = w.dot(w.MUTED, 12), ft.Text("Подключение…", size=12, color=w.MUTED)
        self._account = ft.Text("", size=12, color=w.AMBER, weight=ft.FontWeight.W_600)
        self.control = self._build()

    # ------------------------------------------------------------------------------------------ каркас

    def _build(self) -> ft.Control:
        # в Row(wrap=True) нельзя класть expand-элементы (Flutter: ErrorWidget), поэтому шапка - две строки: заголовок с выходом и строка статуса (wrap, без expand)
        title = ft.Row([ft.Icon(ft.Icons.CANDLESTICK_CHART, color=w.ACCENT), ft.Text(APP_NAME, size=17, weight=ft.FontWeight.W_700, expand=True, no_wrap=True,
                                                                                      overflow=ft.TextOverflow.ELLIPSIS),
                        ft.IconButton(icon=ft.Icons.LOGOUT, tooltip="Выйти", on_click=lambda e: self.run(self._on_logout()))],
                       spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        status = ft.Row([self._dot, self._status_text, self._account], spacing=8, wrap=True, run_spacing=2, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        header = ft.Column([title, status], spacing=0)
        labels = [("Dashboard", ft.Icons.DASHBOARD), ("Statistics", ft.Icons.BAR_CHART), ("History", ft.Icons.HISTORY), ("MFE Simulator", ft.Icons.SCIENCE),
                  ("Favorites", ft.Icons.STAR)]
        tabs = ft.Tabs(length=5, selected_index=0, expand=True, on_change=self._tab_changed, content=ft.Column([
            ft.TabBar(tabs=[ft.Tab(label=t, icon=i) for t, i in labels], scrollable=True),
            ft.TabBarView(controls=[self.dashboard.control, self.stats.control, self.history.control, self.mfe.control, self.favorites.control],
                          expand=True)], expand=True))
        return ft.Column([header, tabs], expand=True, spacing=6)

    def run(self, coro) -> None:
        async def runner() -> None:
            await coro
        self.page.run_task(runner)

    def notify(self, text: str, *, error: bool = False) -> None:
        self.page.show_dialog(ft.SnackBar(content=ft.Text(text, color=w.TEXT), bgcolor="#3a1d1d" if error else "#1d2b3a", duration=6000 if error else 3500))

    def profile_strategies(self) -> list[str] | None:
        return None

    def profile_coins(self) -> list[str] | None:
        return None

    # ------------------------------------------------------------------------------------------ жизненный цикл

    async def start(self) -> None:
        self.dashboard.show_placeholder("Подключение к серверу…")
        self.page.run_task(self._stream.run)
        await self._refresh_account()
        await self._refresh_tab()

    async def close(self) -> None:
        self._closed = True
        if self._stream is not None:
            await self._stream.stop()
        if self.client is not None:
            await self.client.close()
        self._stream = self.client = None

    def _set_status(self, kind: str, text: str) -> None:
        color = STATUS_COLORS.get(kind, w.MUTED)
        self._dot.bgcolor, self._status_text.value, self._status_text.color = color, text, color
        if not self._closed and w.attached(self._dot):
            self._dot.update()
            self._status_text.update()

    def _on_status(self, kind: str, text: str) -> None:
        self._set_status(kind, text)
        if kind in ("auth", "throttled") and not self._closed:
            self.dashboard.show_placeholder(text)
            self.dashboard.control.update()

    async def _on_message(self, msg: dict) -> None:
        if self._closed:
            return
        self.dashboard.on_message(msg)
        if w.attached(self.dashboard.control):
            self.dashboard.control.update()
        if msg.get("type") == "state":
            latest = tuple((e["ts"], e["kind"]) for e in msg["events"][:3])
            changed, self._events_seen = latest != self._events_seen and bool(self._events_seen), latest
            if changed and self._tab != TAB_DASHBOARD:
                await self._refresh_tab()

    async def _refresh_account(self) -> None:
        client = self.client
        if client is None:
            return
        try:
            info = await client.info()
        except ApiError:
            return
        for view in (self.stats, self.history, self.favorites):
            view.set_coins(list(info.get("coins") or []))
        account = info.get("account")
        self._account.value = fmt.account_text(account, False) if fmt.has_balance(account) else ""
        if w.attached(self._account):
            self._account.update()
        self.default_mode = fmt.default_mode(account)
        if self.stats.follow_default_mode(self.default_mode) | self.history.follow_default_mode(self.default_mode):
            await self._refresh_tab()

    async def _tab_changed(self, e) -> None:
        self._tab = int(e.control.selected_index)
        await self._refresh_tab()

    async def _refresh_tab(self) -> None:
        if self._tab == TAB_STATS:
            await self.stats.refresh()
        elif self._tab == TAB_HISTORY:
            await self.history.refresh()
        elif self._tab == TAB_MFE:
            await self.mfe.refresh()
        elif self._tab == TAB_FAVORITES:
            await self.favorites.refresh()

    def remember_mode(self, mode: str | None) -> None:
        if mode not in fmt.MODE_KEYS:
            return
        self.saved_mode = mode
        for view in (self.stats, self.history):
            view._mode.value, view._mode_touched = mode, True

    # ------------------------------------------------------------------------------------------ сделка: избранное и карточка разбора

    def set_favorite_everywhere(self, trade_id: str, value: bool) -> None:
        for view in (self.history, self.favorites):
            view.set_favorite(trade_id, value)

    async def toggle_favorite(self, trade: dict) -> bool | None:
        client = self.client
        if client is None:
            self.notify("Нет подключения к серверу", error=True)
            return None
        try:
            if trade.get("is_favorite"):
                await client.favorite_remove(trade["id"])
                now = False
            else:
                await client.favorite_add(trade["id"], self.settings.profile, None)  # картинку графика рисует настольный клиент; здесь - только данные
                now = True
        except ApiError as exc:
            self.notify(str(exc), error=True)
            return None
        trade["is_favorite"] = now
        self.set_favorite_everywhere(trade["id"], now)
        self.notify("Добавлено в избранное." if now else "Убрано из избранного.")
        return now

    async def open_trade_chart(self, trade: dict) -> None:
        """Тап по сделке -> интерактивный график (та же страница, что в настольном клиенте, с HTTP-мостом: web/charts.py) в этой же вкладке браузера, кнопка «‹ Назад» вернёт
        к списку. Нет снапшота / график не собрался - запасная карточка разбора (текст)."""
        client = self.client
        if client is None:
            return
        if not trade.get("has_snapshot"):
            self.notify("У сделки нет снапшота: график недоступен.", error=True)
            return
        try:
            spec = await charts.build_spec(client, self.settings, trade)
            await charts.ensure_server(self.settings)
        except (ApiError, ChartDataError, ValueError, OSError) as exc:
            self.notify(f"График недоступен: {exc}", error=True)
            await self._open_card(trade)
            return
        await self._launcher.launch_url(f"/chart/{charts.register(spec, trade)}", web_only_window_name="_self")

    async def _open_card(self, trade: dict) -> None:
        """Запасной вариант без графика: карточка разбора сделки (результат, оценка, причина входа)."""
        client = self.client
        if client is None:
            return
        try:
            snap = await client.snapshot(trade["id"])
            spec = build_chart_spec(snap, None, now_ms=int(time.time() * 1000))
        except (ApiError, ChartDataError, ValueError) as exc:
            self.notify(f"Карточка недоступна: {exc}", error=True)
            return
        self.page.show_dialog(ft.AlertDialog(
            title=ft.Text(f"{fmt.coin_short(trade['coin'])} · {fmt.strategy_label(trade['strategy_id'])}", size=16),
            content=ft.Container(content=ft.Column(panel_controls(spec.get("panel") or {}), spacing=8, scroll=ft.ScrollMode.AUTO, tight=True),
                                 width=min(520, max(280, (self.page.width or 400) - 60)), height=min(520, max(260, (self.page.height or 600) - 200))),
            actions=[ft.TextButton("Закрыть", on_click=lambda e: self.page.pop_dialog())]))


def panel_controls(panel: dict) -> list[ft.Control]:
    """Панель разбора (spec['panel']): результат, оценка, причина входа - простым текстом, чтобы читалось с телефона."""
    out: list[ft.Control] = []
    result = panel.get("result") or {}
    if result:
        out.append(ft.Text(f"{result.get('status', '')} · {result.get('r', '')} · {result.get('pnl', '')}", size=15, weight=ft.FontWeight.W_700))
        for c in result.get("closes") or []:
            out.append(ft.Text(f"{c.get('n')}. {c.get('label')}  {c.get('time')}  @{c.get('price')}  {c.get('r')}", size=12, color=w.MUTED))
        if result.get("warning"):
            out.append(w.banner(str(result["warning"])))  # обрыв потока / проскальзывание - только здесь, не в таблице
    review = panel.get("review") or {}
    if review:
        out.append(ft.Text(str(review.get("headline", "")), size=14, weight=ft.FontWeight.W_600))
        out += [ft.Text("• " + str(p.get("text", "")), size=12) for p in review.get("points") or []]
    reason = panel.get("reason") or {}
    if reason:
        out.append(ft.Text(str(reason.get("title", "")), size=14, weight=ft.FontWeight.W_600))
        out += [ft.Text(str(s), size=12, color=w.MUTED) for s in reason.get("steps") or []]
    return out or [ft.Text("Нет данных для карточки", color=w.MUTED)]


# ================================================================================================ вход и запуск

LOGIN_GUARD = auth.LoginGuard()


class WebApp:
    """Одна браузерная сессия: экран входа -> WebShell. «Запомнить меня» - подписанный токен в хранилище браузера (SharedPreferences = localStorage)."""

    def __init__(self, page: ft.Page, cfg: WebConfig) -> None:
        self.page, self.cfg, self.shell, self._gen = page, cfg, None, 0
        self.prefs = ft.SharedPreferences()
        page.services.append(self.prefs)

    async def run(self) -> None:
        p = self.page
        p.title, p.theme_mode, p.bgcolor = APP_NAME, ft.ThemeMode.DARK, w.BG
        p.theme = ft.Theme(color_scheme_seed=w.ACCENT)
        p.on_resize = lambda e: self._adapt()
        # обрыв связи (телефон ушёл в фон) - НЕ конец сессии: клиент и поток остаются, при возврате страница перерисовывается; чистим только при закрытии сессии
        p.on_connect = lambda e: self._redraw()
        p.on_close = lambda e: asyncio.ensure_future(self._cleanup())
        self._adapt()
        p.update()
        await self._enter()

    async def _enter(self) -> None:
        """Войти по сохранённому токену («Запомнить меня») или показать экран входа. Запрос к хранилищу браузера может оборваться вместе со связью - повторяется при её восстановлении."""
        self._gen += 1
        gen = self._gen
        try:
            token = await asyncio.wait_for(self.prefs.get(TOKEN_KEY), timeout=5)
        except asyncio.TimeoutError:
            token = None
        if gen != self._gen or self.shell is not None:  # пока ждали, запуск повторили (переподключение) или уже вошли
            return
        log.info("Сессия: токен «Запомнить меня» %s", "найден" if token else "не найден")
        if auth.verify_token(self.cfg.secret, token, self.cfg.login):
            await self.open_app()
        else:
            self.show_login()

    def _redraw(self) -> None:
        """Связь с браузером восстановлена (вернулись из фона / перезагрузили страницу): заново отдаём текущий экран - иначе страница остаётся пустой."""
        self.page.controls.clear()
        if self.shell is not None:
            self.page.add(self.shell.control)
            self.page.update()
        else:
            self.page.update()
            asyncio.ensure_future(self._enter())

    def _adapt(self) -> None:
        """Мобильная раскладка: при узком экране отступы страницы минимальны (всё остальное - wrap/ResponsiveRow/горизонтальный скролл таблицы)."""
        self.page.padding = 8 if (self.page.width or 0) < NARROW_PX else 16
        if self.shell is not None:  # поворот телефона: History/Favorites переключаются между таблицей и карточками
            for view in (self.shell.history, self.shell.favorites):
                view.relayout()
        if w.attached(self.page.controls[0]) if self.page.controls else False:
            self.page.update()

    async def _cleanup(self) -> None:
        if self.shell is not None:
            await self.shell.close()
            self.shell = None

    def show_login(self, note: str = "") -> None:
        p = self.page
        login = ft.TextField(label="Логин", autofocus=True, autocorrect=False, enable_suggestions=False, dense=False, border_color=w.BORDER, bgcolor=w.CARD)
        password = ft.TextField(label="Пароль", password=True, can_reveal_password=True, border_color=w.BORDER, bgcolor=w.CARD)
        remember = ft.Checkbox(label="Запомнить меня", value=True)
        error = ft.Text(note, color=w.RED, size=13)
        busy = ft.ProgressRing(width=18, height=18, stroke_width=2, visible=False)

        async def submit(_=None) -> None:
            wait = LOGIN_GUARD.locked_for()
            if wait > 0:
                error.value = f"Слишком много неверных попыток. Подождите {int(wait) + 1} с."
                p.update()
                return
            busy.visible = True
            p.update()
            await asyncio.sleep(0.6)  # замедляет перебор
            if auth.check_credentials(login.value or "", password.value or "", self.cfg.login, self.cfg.password):
                LOGIN_GUARD.succeeded()
                if remember.value:
                    await self.prefs.set(TOKEN_KEY, auth.make_token(self.cfg.secret, self.cfg.login))
                else:
                    await self.prefs.remove(TOKEN_KEY)
                await self.open_app()
                return
            LOGIN_GUARD.failed()
            busy.visible, password.value, error.value = False, "", "Неверный логин или пароль"
            p.update()

        login.on_submit = lambda e: self.page.run_task(password.focus)
        password.on_submit = submit
        card = ft.Container(width=min(360, max(240, (p.width or 360) - 32)), padding=24, border_radius=16, bgcolor=w.CARD_HI, border=ft.Border.all(1, w.BORDER), content=ft.Column([
            ft.Row([ft.Icon(ft.Icons.CANDLESTICK_CHART, color=w.ACCENT, size=30), ft.Text(APP_NAME, size=22, weight=ft.FontWeight.W_700)], alignment=ft.MainAxisAlignment.CENTER),
            ft.Text("Вход", size=14, color=w.MUTED, text_align=ft.TextAlign.CENTER), login, password, remember, error,
            ft.Row([ft.FilledButton("Войти", on_click=submit, expand=True, height=46), busy], spacing=10)], spacing=12, tight=True))
        p.controls.clear()
        p.add(ft.Container(content=card, alignment=ft.Alignment.CENTER, expand=True))
        p.update()

    async def open_app(self) -> None:
        self.shell = WebShell(self.page, self.cfg, self.logout)
        self.page.controls.clear()
        self.page.add(self.shell.control)
        self.page.update()
        await self.shell.start()

    async def logout(self) -> None:
        await self.prefs.remove(TOKEN_KEY)
        await self._cleanup()
        self.show_login()


def run(env: dict[str, str] | None = None) -> None:
    cfg = WebConfig.from_env(env)  # без WEB_LOGIN / WEB_PASSWORD / ключа API процесс не стартует (Fail-Fast)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    async def main(page: ft.Page) -> None:
        await WebApp(page, cfg).run()

    log.info("Веб-версия %s %s: порт %s", APP_NAME, __version__, cfg.port)
    ft.run(main, host="0.0.0.0", port=cfg.port, view=ft.AppView.WEB_BROWSER)
