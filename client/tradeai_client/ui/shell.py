"""Оболочка приложения: заголовок со статусом связи, вкладки, диалог настроек, подключение к API и открытие графиков сделок."""

import asyncio
import logging
import time

import aiohttp
import flet as ft

from tradeai_client import APP_NAME, __version__, changelog, fmt, profiles, updater
from tradeai_client.api import ApiClient, ApiError, LiveStateStream
from tradeai_client.binance import BinanceError, BinanceNoData, fetch_klines_filled
from tradeai_client.chart_spec import ChartDataError, build_chart_spec, interval_to_ms
from tradeai_client.chart_window import open_chart
from tradeai_client.golden_image import render_png
from tradeai_client.settings import ClientSettings, SettingsError, load_settings, save_settings, settings_exist
from tradeai_client.ui import widgets as w
from tradeai_client.ui.analysis_view import AnalysisView
from tradeai_client.ui.changelog_view import change_row, release_card, releases_list
from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.history_view import HistoryView
from tradeai_client.ui.stats_view import StatsView

log = logging.getLogger("tradeai_client")
STATUS_COLORS = {"connected": w.GREEN, "connecting": w.AMBER, "reconnecting": w.AMBER, "auth": w.RED, "throttled": w.RED, "error": w.RED, "idle": w.MUTED}
TAB_DASHBOARD, TAB_STATS, TAB_HISTORY, TAB_ANALYSIS, TAB_FAVORITES = 0, 1, 2, 3, 4
ACCOUNT_REFRESH_S = 30.0  # как часто обновлять баланс в шапке (движок пишет снимок счёта раз в ~30 с)
POST_BARS_MARGIN = 5  # свечей после выхода, которые показываем за последним маркером


class Shell:
    def __init__(self, page: ft.Page) -> None:
        self.page = page
        self.client: ApiClient | None = None
        self._stream: LiveStateStream | None = None
        self._events_seen: tuple = ()
        self._tab = TAB_DASHBOARD
        first_run = not settings_exist()  # самый первый запуск на этом ПК: «Приложение обновлено» показывать не с чего
        try:
            self.settings = load_settings()
            self._settings_error = None
        except SettingsError as exc:
            self.settings, self._settings_error = ClientSettings(), str(exc)
        if first_run:
            self.settings.last_seen_version = __version__
        self.saved_mode = self.settings.mode_filter if self.settings.mode_filter in fmt.MODE_KEYS else ""  # выбор пользователя (Statistics/History) переживает перезапуск
        self.default_mode = "shadow"  # активный контур: live, пока боевой счёт присылает баланс (фильтры History / Statistics стартуют с него)
        self._account_at = 0.0
        self._last_state: dict | None = None  # последнее состояние Dashboard (перерисовывается при смене профиля)
        self._chr_pnl: float | None = None  # баланс профиля CHR: накопленный PnL его сделок от нуля
        self.dashboard, self.stats, self.history, self.analysis = DashboardView(self), StatsView(self), HistoryView(self), AnalysisView(self)
        self.favorites = HistoryView(self, favorites=True)
        self._profile_btn = ft.TextButton(f"Пользователь: {self.settings.profile}", icon=ft.Icons.PERSON, tooltip="Сменить пользователя (SANTA / CHR)", on_click=lambda e: self.show_profile_dialog())
        self._dot = w.dot(w.MUTED, 12)
        self._status_text = ft.Text("Не подключено", size=13, color=w.MUTED)
        self._account_data: dict | None = None
        self._account = ft.Text(fmt.SHADOW_ACCOUNT_TEXT, size=13, color=w.AMBER, weight=ft.FontWeight.W_600,
                                tooltip="Баланс боевого счёта (Binance + MT5). В режиме Shadow ордера не отправляются.")
        self._eye = ft.IconButton(icon=ft.Icons.VISIBILITY_OFF, icon_size=18, tooltip="Показать баланс", visible=False, on_click=self._toggle_balance)
        self._build()

    # ------------------------------------------------------------------------------------------ каркас

    def _build(self) -> None:
        p = self.page
        p.title = f"{APP_NAME} {__version__}"
        p.theme_mode = ft.ThemeMode.DARK
        p.theme = ft.Theme(color_scheme_seed=w.ACCENT)
        p.bgcolor, p.padding = w.BG, 16
        p.on_error = lambda e: log.error("Необработанная ошибка интерфейса: %s", getattr(e, "data", e))
        p.window.width, p.window.height, p.window.min_width, p.window.min_height = 1280, 820, 980, 600
        header = ft.Row([ft.Icon(ft.Icons.CANDLESTICK_CHART, color=w.ACCENT), ft.Text(APP_NAME, size=20, weight=ft.FontWeight.W_700), ft.Container(expand=True),
                         self._profile_btn, self._account, self._eye, ft.VerticalDivider(width=12, color=w.BORDER), self._dot, self._status_text, ft.IconButton(icon=ft.Icons.SETTINGS, tooltip="Настройки подключения", on_click=lambda e: self.show_settings())],
                        spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        tabs = ft.Tabs(length=5, selected_index=0, expand=True, on_change=self._tab_changed, content=ft.Column([
            ft.TabBar(tabs=[ft.Tab(label="Dashboard", icon=ft.Icons.DASHBOARD), ft.Tab(label="Statistics", icon=ft.Icons.BAR_CHART),
                            ft.Tab(label="History", icon=ft.Icons.HISTORY), ft.Tab(label="Analysis", icon=ft.Icons.ANALYTICS), ft.Tab(label="Favorites", icon=ft.Icons.STAR)]),
            ft.TabBarView(controls=[self.dashboard.control, self.stats.control, self.history.control, self.analysis.control, self.favorites.control], expand=True)], expand=True))
        p.add(header, ft.Container(height=6), tabs)

    def run(self, coro) -> None:
        """Запустить корутину из синхронного обработчика Flet."""
        async def runner() -> None:
            await coro
        self.page.run_task(runner)

    def notify(self, text: str, *, error: bool = False) -> None:
        self.page.show_dialog(ft.SnackBar(content=ft.Text(text, color=w.TEXT), bgcolor="#3a1d1d" if error else "#1d2b3a", duration=6000 if error else 3500))

    # ------------------------------------------------------------------------------------------ профили

    @property
    def profile(self) -> str:
        return self.settings.profile

    def profile_strategies(self) -> list[str] | None:
        """Стратегии активного профиля (None - все): SANTA без ограничений, CHR - две свои."""
        return profiles.strategies_for(self.settings.profile)

    def profile_coins(self) -> list[str] | None:
        """Пул монет активного профиля (None - все): CHR - умолчания плюс добавленные пользователем."""
        return profiles.coins_for(self.settings.profile, self.settings.chr_extra_coins)

    async def set_profile(self, profile: str) -> None:
        profile = profiles.normalize_profile(profile)
        if profile == self.settings.profile:
            return
        self.settings.profile, self.settings.profile_chosen = profile, True
        self._persist_prefs()
        self._apply_profile_ui()
        await self._refresh_account()
        await self._refresh_tab()
        self.notify(f"Пользователь: {profile}")

    def _apply_profile_ui(self) -> None:
        """Профиль сменился или изменился его пул монет: фильтры, подписи, Dashboard, баланс."""
        self._profile_btn.content = f"Пользователь: {self.settings.profile}"
        self._profile_btn.update()
        for view in (self.stats, self.history):
            view.apply_profile()
        self.analysis.reset()
        if self._last_state is not None:
            self.dashboard.on_message(self._last_state)
            self.dashboard.control.update()
        self._render_account()

    async def start(self) -> None:
        for goal in (self.stats._goal, self.analysis._goal):  # переливающаяся рамка карточки значимости
            self.page.run_task(goal.shimmer)
        if not self._settings_error and not self.settings.profile_chosen:
            await self._choose_user()  # как экран входа Windows: выбор запоминается, дальше экран не показывается
        if self._settings_error:
            self.show_settings(note=self._settings_error)
        elif not self.settings.api_key:
            self.show_settings(note="Введите адрес сервера и API-ключ (на сервере: ./deploy/shadow.sh apikey).")
        else:
            await self.connect()
            # после обновления сначала «Что нового» (один раз на версию), и только потом предложение следующего обновления - окна не должны наслаиваться
            if changelog.should_show_whats_new(self.settings.last_seen_version, __version__):
                self.show_whats_new(then=lambda: self.page.run_task(self._check_update))
            else:
                self.page.run_task(self._check_update)

    # ------------------------------------------------------------------------------------------ подключение

    def _set_status(self, kind: str, text: str) -> None:
        color = STATUS_COLORS.get(kind, w.MUTED)
        self._dot.bgcolor, self._status_text.value, self._status_text.color = color, text, color
        self._dot.update()
        self._status_text.update()

    async def _refresh_account(self) -> None:
        """Баланс боевого счёта в шапку (GET /api/info) и активный контур для фильтров. Ошибка сети уже видна в статусе связи: плашку не трогаем."""
        client = self.client
        if client is None:
            return
        try:
            info = await client.info()
        except ApiError:
            return
        for view in (self.stats, self.history, self.favorites):
            view.set_coins(list(info.get("coins") or []))
        if self.profile == profiles.CHR:
            await self._refresh_chr_balance(client)
        account = info.get("account")
        live = bool(account and account.get("available"))
        self._account_data = account
        self._render_account()
        self.default_mode = fmt.default_mode(account)
        if self.stats.follow_default_mode(self.default_mode) | self.history.follow_default_mode(self.default_mode):
            await self._refresh_tab()

    async def _refresh_chr_balance(self, client) -> None:
        """Баланс CHR всегда с нуля: сумма PnL закрытых сделок его стратегий (виртуальные Shadow-сделки), а не счёт биржи."""
        try:
            data = await client.stats(coins=self.profile_coins(), strategies=self.profile_strategies(), mode="shadow")
        except ApiError:
            return
        self._chr_pnl = float(data["overall"].get("net_pnl") or 0.0)
        self._render_account()

    def _render_account(self) -> None:
        shown = self.settings.balance_visible
        if self.profile == profiles.CHR:
            self._account.value = fmt.chr_balance_text(self._chr_pnl, shown)
            self._account.color = fmt.r_color(self._chr_pnl) if shown and self._chr_pnl else w.AMBER
            self._account.tooltip = "Баланс профиля CHR: накопленный результат сделок CHR в USDT, отсчёт от 0$ (не счёт биржи)."
            self._eye.visible = True
            self._eye.icon, self._eye.tooltip = (ft.Icons.VISIBILITY, "Скрыть баланс") if shown else (ft.Icons.VISIBILITY_OFF, "Показать баланс")
            if w.attached(self._account):
                self._account.update()
                self._eye.update()
            return
        self._account.tooltip = "Баланс боевого счёта (Binance + MT5). В режиме Shadow ордера не отправляются."
        self._account.value = fmt.account_text(self._account_data, shown)
        self._account.color = w.GREEN if fmt.has_balance(self._account_data) else w.AMBER
        self._eye.visible = fmt.has_balance(self._account_data)
        self._eye.icon, self._eye.tooltip = (ft.Icons.VISIBILITY, "Скрыть баланс") if shown else (ft.Icons.VISIBILITY_OFF, "Показать баланс")
        self._account.update()
        self._eye.update()

    def _persist_prefs(self) -> None:
        try:
            save_settings(self.settings)
        except OSError as exc:
            log.warning("Предпочтения интерфейса не сохранены на диск: %s", exc)

    async def _toggle_balance(self, _=None) -> None:
        self.settings.balance_visible = not self.settings.balance_visible
        self._persist_prefs()
        self._render_account()

    def remember_mode(self, mode: str | None) -> None:
        """Пользователь выбрал режим в Statistics или History: запомнить на диск и применить к обеим вкладкам."""
        if mode not in fmt.MODE_KEYS:
            return
        self.saved_mode, self.settings.mode_filter = mode, mode
        for view in (self.stats, self.history):
            view._mode.value, view._mode_touched = mode, True
        self._persist_prefs()

    async def connect(self) -> None:
        await self.disconnect()
        try:
            self.settings.validate()
        except SettingsError as exc:
            self._set_status("error", str(exc))
            return
        self.client = ApiClient(self.settings)
        self._stream = LiveStateStream(self.settings, self._on_message, self._on_status)
        self.dashboard.show_placeholder("Подключение к серверу…")
        self.dashboard.control.update()
        self.page.run_task(self._stream.run)
        await self._refresh_account()
        await self._refresh_tab()

    async def disconnect(self) -> None:
        if self._stream is not None:
            await self._stream.stop()
            self._stream = None
        if self.client is not None:
            await self.client.close()
            self.client = None

    def _on_status(self, kind: str, text: str) -> None:
        self._set_status(kind, text)
        if kind in ("auth", "throttled"):
            self.dashboard.show_placeholder(text)
            self.dashboard.control.update()
            self.show_settings(note=text)

    async def _on_message(self, msg: dict) -> None:
        if msg.get("type") == "state":
            self._last_state = msg
        self.dashboard.on_message(msg)
        self.dashboard.control.update()
        if time.monotonic() - self._account_at >= ACCOUNT_REFRESH_S:
            self._account_at = time.monotonic()
            await self._refresh_account()
        if msg.get("type") == "state":  # сделка закрылась/появилась: статистика и история на открытой вкладке устарели
            latest = tuple((e["ts"], e["kind"]) for e in msg["events"][:3])
            changed, self._events_seen = latest != self._events_seen and bool(self._events_seen), latest
            if changed and self._tab != TAB_DASHBOARD:
                await self._refresh_tab()

    async def _tab_changed(self, e) -> None:
        self._tab = int(e.control.selected_index)
        await self._refresh_tab()

    async def _refresh_tab(self) -> None:
        if self._tab == TAB_STATS:
            await self.stats.refresh()
        elif self._tab == TAB_HISTORY:
            await self.history.refresh()
        elif self._tab == TAB_ANALYSIS:
            await self.analysis.refresh()
        elif self._tab == TAB_FAVORITES:
            await self.favorites.refresh()

    # ------------------------------------------------------------------------------------------ настройки

    def show_settings(self, note: str | None = None) -> None:
        s = self.settings
        url = ft.TextField(label="Адрес сервера", value=s.base_url, hint_text="http://193.233.75.254:8001", autofocus=True, dense=True)
        key = ft.TextField(label="API-ключ", value=s.api_key, password=True, can_reveal_password=True, dense=True)
        remember = ft.Switch(label="Запомнить ключ на этом ПК", value=s.remember_key)
        user_row = ft.Row([ft.OutlinedButton(f"История обновлений (v{__version__})", icon=ft.Icons.HISTORY_EDU, tooltip="Что и когда добавлено в клиент",
                                             on_click=lambda e: (self.page.pop_dialog(), self.show_changelog())),
                           ft.OutlinedButton(f"Сменить пользователя ({s.profile})", icon=ft.Icons.SWITCH_ACCOUNT, on_click=lambda e: (self.page.pop_dialog(), self.show_profile_dialog())),
                           ft.OutlinedButton("Монеты CHR…", icon=ft.Icons.PLAYLIST_ADD, disabled=s.profile != profiles.CHR,
                                             tooltip="Добавить монеты в пул профиля CHR" if s.profile == profiles.CHR else "Доступно в профиле CHR",
                                             on_click=lambda e: (self.page.pop_dialog(), self.show_chr_coins_dialog()))], spacing=10, wrap=True)
        post = ft.Switch(label="Достраивать свечи после сигнала (публичный REST Binance)", value=s.post_signal_candles)
        msg = ft.Text(note or "", size=12, color=w.AMBER, selectable=True)
        busy = ft.ProgressRing(width=16, height=16, stroke_width=2, visible=False)
        hint = ft.Text("Адрес и ключ сохраняются в settings.json рядом с .exe: при следующем запуске приложение подключится само. "
                       "Ключ передаётся только в заголовке запроса. Нужен прямой доступ к порту API на сервере (файрвол!); "
                       "альтернатива - SSH-туннель и адрес http://127.0.0.1:8000. ", size=11, color=w.MUTED)

        async def save(_=None) -> None:
            candidate = ClientSettings(base_url=url.value or "", api_key=(key.value or "").strip(), remember_key=bool(remember.value),
                                       post_signal_candles=bool(post.value), binance_rest=self.settings.binance_rest,
                                       balance_visible=self.settings.balance_visible, mode_filter=self.settings.mode_filter,
                                       last_seen_version=self.settings.last_seen_version, profile=self.settings.profile, profile_chosen=self.settings.profile_chosen,
                                       chr_extra_coins=list(self.settings.chr_extra_coins))
            msg.color = w.AMBER
            try:
                candidate.validate()
                if not candidate.api_key:
                    raise SettingsError("Введите API-ключ")
            except SettingsError as exc:
                msg.value = str(exc)
                msg.update()
                return
            busy.visible, msg.value = True, "Проверка подключения…"
            busy.update()
            msg.update()
            probe = ApiClient(candidate)
            try:
                info = await probe.info()
            except ApiError as exc:
                busy.visible, msg.value, msg.color = False, str(exc), w.RED
                busy.update()
                msg.update()
                return
            finally:
                await probe.close()
            self.settings = candidate
            try:
                save_settings(candidate)
            except OSError as exc:
                self.notify(f"Настройки применены, но не сохранены на диск: {exc}", error=True)
            self.page.pop_dialog()
            self.notify(f"Подключено. Схема БД v{info['schema_version']}, монет: {len(info['coins'])}.")
            await self.connect()

        warning = ft.Text("⚠ Адрес не локальный и без TLS: ключ пойдёт по сети открытым текстом. Для защиты используйте SSH-туннель или https.", size=12, color=w.RED,
                          visible=s.cleartext_remote)

        def on_url_change(_=None) -> None:
            warning.visible = ClientSettings(base_url=url.value or "").cleartext_remote if (url.value or "").startswith("http") else False
            warning.update()

        url.on_change = on_url_change
        dialog = ft.AlertDialog(modal=True, title=ft.Text("Подключение к серверу мониторинга"),
                                content=ft.Container(width=520, content=ft.Column([url, key, remember, post, user_row, warning, hint, ft.Row([busy, msg], spacing=8)], spacing=12, tight=True)),
                                actions=[ft.TextButton("Отмена", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Проверить и сохранить", on_click=save)],
                                actions_alignment=ft.MainAxisAlignment.END)
        self.page.show_dialog(dialog)

    # ------------------------------------------------------------------------------------------ экран выбора пользователя и всплывающие окна

    def _overlay_show(self, control: ft.Control) -> None:
        self.page.overlay.append(control)
        self.page.update()

    def _overlay_hide(self, control: ft.Control) -> None:
        try:
            self.page.overlay.remove(control)
        except ValueError:  # уже убрано (двойной клик по кнопке)
            pass
        self.page.update()

    async def _choose_user(self) -> None:
        done = asyncio.get_running_loop().create_future()
        self.show_login(lambda name: done.set_result(name) if not done.done() else None)
        await done

    def show_login(self, on_pick) -> ft.Control:
        """Экран выбора пользователя (как при входе в Windows): крупные плитки SANTA / CHR на весь экран. Выбор запоминается в настройках и больше не спрашивается."""
        about = {profiles.SANTA: "Все стратегии и монеты, баланс боевого счёта",
                 profiles.CHR: "Только стратегии CHR, пул монет CHR, баланс с нуля"}
        screen: list[ft.Control] = []

        def pick(name: str) -> None:
            self.settings.profile, self.settings.profile_chosen = profiles.normalize_profile(name), True
            self._persist_prefs()
            self._profile_btn.content = f"Пользователь: {self.settings.profile}"
            self._apply_profile_ui()
            self._overlay_hide(screen[0])
            on_pick(self.settings.profile)

        tiles = [ft.Container(content=ft.Column([ft.Container(content=ft.Text(name[0], size=40, weight=ft.FontWeight.W_700, color=w.BG), width=96, height=96, border_radius=48,
                                                              bgcolor=w.ACCENT if name == profiles.SANTA else w.GREEN, alignment=ft.Alignment.CENTER),
                                                 ft.Text(name, size=20, weight=ft.FontWeight.W_700, color=w.TEXT),
                                                 ft.Text(about[name], size=12, color=w.MUTED, text_align=ft.TextAlign.CENTER)],
                                                horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=10),
                              width=240, padding=24, border_radius=16, bgcolor=w.CARD, border=ft.Border.all(1, w.BORDER), ink=True, on_click=lambda e, n=name: pick(n))
                 for name in profiles.PROFILES]
        screen.append(ft.Container(expand=True, bgcolor=w.BG, alignment=ft.Alignment.CENTER,
                                   content=ft.Column([ft.Text(APP_NAME, size=28, weight=ft.FontWeight.W_700, color=w.TEXT), ft.Text("Выберите пользователя", size=15, color=w.MUTED),
                                                      ft.Container(height=18), ft.Row(tiles, spacing=24, alignment=ft.MainAxisAlignment.CENTER)],
                                                     horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=6, tight=True)))
        self._overlay_show(screen[0])
        return screen[0]

    def show_profile_dialog(self) -> None:
        """Выбор пользователя: SANTA (всё как раньше) или CHR (свои стратегии, свой пул монет, баланс с нуля). Выбор запоминается."""
        def pick(name: str) -> None:
            self.page.pop_dialog()
            self.run(self.set_profile(name))

        about = {profiles.SANTA: "Все стратегии и монеты, баланс боевого счёта.",
                 profiles.CHR: "Только Trendline Breakout (CHR) и Horizontal Breakout (CHR); монеты EUR, GBP, JPY, XAU, BTC, ETH (+ ваши); баланс и PnL с нуля."}
        buttons = [ft.Container(content=ft.Column([ft.Row([ft.Icon(ft.Icons.CHECK_CIRCLE if self.profile == name else ft.Icons.PERSON, color=w.GREEN if self.profile == name else w.MUTED),
                                                          ft.Text(name, size=16, weight=ft.FontWeight.W_700)], spacing=8), ft.Text(about[name], size=12, color=w.MUTED)], spacing=4),
                                   padding=12, border_radius=10, bgcolor=w.CARD_HI if self.profile == name else w.CARD, border=ft.Border.all(1, w.ACCENT if self.profile == name else w.BORDER),
                                   on_click=lambda e, n=name: pick(n)) for name in profiles.PROFILES]
        self.page.show_dialog(ft.AlertDialog(modal=False, title=ft.Text("Сменить пользователя"), content=ft.Container(width=460, content=ft.Column(buttons, spacing=10, tight=True)),
                                             actions=[ft.TextButton("Закрыть", on_click=lambda e: self.page.pop_dialog())]))

    def show_chr_coins_dialog(self, note: str = "") -> None:
        """Пул монет профиля CHR: умолчания фиксированы, свои монеты добавляются и удаляются. Новые символы бот должен торговать (иначе сделок по ним не будет)."""
        field = ft.TextField(label="Тикер", hint_text="SOL, SOLUSDT, GBPUSD…", dense=True, width=240, autofocus=True)
        msg = ft.Text(note, size=12, color=w.AMBER)
        extra = list(self.settings.chr_extra_coins)

        def add(_=None) -> None:
            try:
                coin = profiles.normalize_coin(field.value or "")
            except profiles.ProfileError as exc:
                msg.value = str(exc)
                msg.update()
                return
            if coin in (profiles.coins_for(profiles.CHR, self.settings.chr_extra_coins) or []):
                msg.value = f"{coin} уже в пуле"
                msg.update()
                return
            self.settings.chr_extra_coins = [*self.settings.chr_extra_coins, coin]
            self._after_coin_pool_change(f"Добавлено: {coin}")

        def remove(coin: str) -> None:
            self.settings.chr_extra_coins = [c for c in self.settings.chr_extra_coins if c != coin]
            self._after_coin_pool_change(f"Удалено: {coin}")

        rows = [ft.Row([ft.Text(fmt.coin_short(c), width=110), ft.Text("по умолчанию", size=12, color=w.MUTED)]) for c in profiles.CHR_DEFAULT_COINS]
        rows += [ft.Row([ft.Text(fmt.coin_short(c), width=110), ft.IconButton(icon=ft.Icons.DELETE_OUTLINE, icon_size=18, tooltip="Убрать из пула", on_click=lambda e, c=c: remove(c))]) for c in extra]
        self.page.show_dialog(ft.AlertDialog(
            modal=False, title=ft.Text("Монеты профиля CHR"),
            content=ft.Container(width=420, content=ft.Column([ft.Row([field, ft.FilledButton("Добавить", icon=ft.Icons.ADD, on_click=add)], spacing=8), msg,
                                                               ft.Column(rows, spacing=0, scroll=ft.ScrollMode.AUTO, height=min(300, 40 * len(rows)))], spacing=8, tight=True)),
            actions=[ft.TextButton("Готово", on_click=lambda e: self.page.pop_dialog())]))

    def _after_coin_pool_change(self, note: str) -> None:
        self._persist_prefs()
        self.page.pop_dialog()
        self._apply_profile_ui()
        self.run(self._refresh_tab())
        self.show_chr_coins_dialog(note)

    # ------------------------------------------------------------------------------------------ версии клиента

    def _load_releases(self) -> list["changelog.Release"] | None:
        try:
            return changelog.load()
        except changelog.ChangelogError as exc:
            log.error("%s", exc)
            self.notify(f"История обновлений недоступна: {exc}", error=True)
            return None

    def mark_version_seen(self) -> None:
        self.settings.last_seen_version = __version__
        self._persist_prefs()

    def show_whats_new(self, then=None) -> None:
        """«Приложение обновлено до версии X»: изменения именно этой версии (+ строка про пропущенные). После закрытия last_seen_version = текущая версия."""
        releases = self._load_releases()
        current = changelog.release_for(releases, __version__) if releases else None
        skipped = [r for r in changelog.missed(releases or [], self.settings.last_seen_version, __version__) if r is not current]
        if current is None:
            self.mark_version_seen()
            if then:
                then()
            return

        box: list[ft.Control] = []

        def close(_=None) -> None:
            self.mark_version_seen()
            self._overlay_hide(box[0])
            if then:
                then()

        rows: list[ft.Control] = [change_row(c) for c in current.changes]
        if skipped:
            rows.append(ft.Text("Пропущенные версии: " + ", ".join(f"v{r.version}" for r in skipped) + " — подробности в Настройки → «История обновлений».", size=12, color=w.MUTED))
        head: list[ft.Control] = [ft.Text(f"v{current.version}", size=40, weight=ft.FontWeight.W_700, color=w.TEXT, text_align=ft.TextAlign.CENTER)]
        if current.title:
            head.append(ft.Text(current.title, size=15, color=w.MUTED, text_align=ft.TextAlign.CENTER))
        card = ft.Container(
            width=560, padding=28, border_radius=18, bgcolor=w.CARD, border=ft.Border.all(1, w.BORDER),
            content=ft.Column([ft.Column(head, horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=2),
                               ft.Container(content=ft.Column(rows, spacing=10, scroll=ft.ScrollMode.AUTO), height=min(340, 24 + 54 * len(rows))),
                               ft.Row([ft.FilledButton("Круто!", on_click=close, style=ft.ButtonStyle(bgcolor="#ffffff", color="#000000", padding=ft.Padding.symmetric(horizontal=36, vertical=16),
                                                                                                         shape=ft.RoundedRectangleBorder(radius=10)))], alignment=ft.MainAxisAlignment.CENTER)],
                              spacing=18, tight=True))
        # весь экран: размытый затемнённый фон блокирует интерфейс под окном, карточка по центру
        box.append(ft.Container(expand=True, blur=ft.Blur(14, 14), bgcolor=ft.Colors.with_opacity(0.5, "#000000"), alignment=ft.Alignment.CENTER, content=card))
        self._overlay_show(box[0])

    def show_changelog(self) -> None:
        """«История обновлений»: все версии клиента (с v1.0), новые сверху, окно со скроллом."""
        releases = self._load_releases()
        if releases is None:
            return
        self.page.show_dialog(ft.AlertDialog(
            modal=False, title=ft.Text("История обновлений", size=20, weight=ft.FontWeight.W_700), content=releases_list(releases, __version__),
            actions=[ft.TextButton("Закрыть", on_click=lambda e: self.page.pop_dialog())], actions_alignment=ft.MainAxisAlignment.END))

    # ------------------------------------------------------------------------------------------ автообновление

    async def _check_update(self) -> None:
        client = self.client
        if client is None:
            return
        info = await updater.check_for_update(client, __version__)
        if info is not None:
            self._offer_update(info)

    def _offer_update(self, info: "updater.UpdateInfo") -> None:
        body: list[ft.Control] = [ft.Text(f"Доступна версия {info.version} (у вас {__version__}).", size=14)]
        news = changelog.release_for(list(info.changelog), info.version) if info.changelog else None
        if news is not None:  # история с сервера: что именно изменится - красиво, до загрузки
            body.append(release_card(news))
        elif info.notes:
            body.append(ft.Text(info.notes, size=12, color=w.MUTED, selectable=True))
        body.append(ft.Text(f"Файл {info.size / 1_048_576:.0f} МБ. После загрузки программа перезапустится сама.", size=12, color=w.MUTED))

        def later(_=None) -> None:
            self.page.pop_dialog()

        def go(_=None) -> None:
            self.page.pop_dialog()
            self.run(self._install_update(info))

        self.page.show_dialog(ft.AlertDialog(modal=True, title=ft.Text("Доступно обновление"), content=ft.Container(width=600, content=ft.Column(body, spacing=8, tight=True, scroll=ft.ScrollMode.AUTO), height=min(520, 120 + 48 * (len(news.changes) if news else 2))),
                                             actions=[ft.TextButton("Позже", on_click=later), ft.FilledButton("Обновить", icon=ft.Icons.SYSTEM_UPDATE, on_click=go)]))

    async def _install_update(self, info: "updater.UpdateInfo") -> None:
        client = self.client
        if client is None:
            return
        if not updater.is_frozen():
            self.notify("Обновление ставится только в собранном .exe (из исходников обновите код через git).", error=True)
            return
        bar = ft.ProgressBar(value=0, width=420, color=w.ACCENT, bgcolor=w.BORDER)
        label = ft.Text("Загрузка…", size=12, color=w.MUTED)
        self.page.show_dialog(ft.AlertDialog(modal=True, title=ft.Text(f"Обновление до {info.version}"), content=ft.Container(width=440, content=ft.Column([bar, label], spacing=8, tight=True))))
        last = [0.0]

        def progress(frac: float) -> None:
            if frac - last[0] >= 0.01 or frac >= 1.0:
                last[0], bar.value, label.value = frac, frac, f"{frac * 100:.0f}% · {frac * info.size / 1_048_576:.0f} из {info.size / 1_048_576:.0f} МБ"
                bar.update()
                label.update()

        exe = updater.exe_path()
        try:
            new = await updater.download(client, info, exe.with_name(updater.EXE_NAME + ".new"), progress)
            label.value = "Установка и перезапуск…"
            label.update()
            updater.launch_swap(updater.write_swap_script(new, exe, exe.parent))
        except updater.UpdateError as exc:
            self.page.pop_dialog()
            self.notify(str(exc), error=True)
            return
        from tradeai_client.main import _shutdown  # локально: main импортирует этот модуль
        await asyncio.sleep(0.5)
        _shutdown()

    # ------------------------------------------------------------------------------------------ избранное

    async def toggle_favorite(self, trade: dict) -> bool | None:
        """Звёздочка сделки (общая для профилей; состояние - на сервере, PostgreSQL). Ставится вместе с картинкой графика для golden_set.
        -> новое состояние или None при ошибке (уже показана пользователю)."""
        client = self.client
        if client is None:
            self.notify("Нет подключения к серверу", error=True)
            return None
        tid, note = trade["id"], ""
        try:
            if trade.get("is_favorite"):
                await client.favorite_remove(tid)
                now = False
            else:
                png, note = await self._golden_png(trade)
                await client.favorite_add(tid, self.settings.profile, png)
                now = True
        except ApiError as exc:
            self.notify(str(exc), error=True)
            return None
        trade["is_favorite"] = now
        self.set_favorite_everywhere(tid, now)
        self.notify(("Добавлено в избранное." + (f" {note}" if note else "")) if now else "Убрано из избранного.", error=bool(note) and now)
        return now

    def set_favorite_everywhere(self, trade_id: str, value: bool) -> None:
        for view in (self.history, self.favorites):
            view.set_favorite(trade_id, value)

    async def _golden_png(self, trade: dict) -> tuple[bytes | None, str]:
        """Картинка графика для золотой базы: тот же график, что открывается по двойному клику, но рисуется Pillow без окна. Не вышло - звёздочка ставится без картинки (JSON сервер соберёт сам)."""
        if not trade.get("has_snapshot"):
            return None, "У сделки нет снапшота: сохранены только данные без картинки."
        try:
            spec = await self._chart_spec(trade)
            return await asyncio.to_thread(render_png, spec), ""
        except (ChartDataError, ValueError, OSError) as exc:
            log.warning("Картинка для избранного %s не построена: %s", trade["id"], exc)
            return None, f"Картинку построить не удалось ({exc}): сохранены только данные."

    # ------------------------------------------------------------------------------------------ график сделки

    async def open_trade_chart(self, trade: dict) -> None:
        log.info("График сделки %s", trade["id"])
        client = self.client
        if client is None:
            self.notify("Нет подключения к серверу", error=True)
            return
        if not trade.get("has_snapshot"):
            self.notify("У этой сделки нет снапшота (он пишется при приёме сигнала)", error=True)
            return
        self.notify(f"Строю график {fmt.coin_short(trade['coin'])}…")
        try:
            spec = await self._chart_spec(trade)
        except ApiError as exc:
            self.notify(str(exc), error=True)
            return
        except ChartDataError as exc:
            self.notify(f"Снапшот нельзя нарисовать: {exc}", error=True)
            return
        open_chart(spec)

    async def _chart_spec(self, trade: dict) -> dict:
        """Снапшот сделки + свечи после сигнала -> spec графика. Ошибки: ApiError, ChartDataError."""
        client = self.client
        if client is None:
            raise ApiError("network", "Нет подключения к серверу")
        snap = await client.snapshot(trade["id"])
        iv = interval_to_ms((snap.get("meta") or {}).get("interval", ""))
        now_ms = int(time.time() * 1000)
        post, note = None, None
        if self.settings.post_signal_candles:
            post, note = await self._post_candles(snap, iv, now_ms)
        spec = build_chart_spec(snap, post, now_ms=now_ms, post_note=note)
        coin = trade["coin"]  # источник живых свечей: Binance Futures для крипты, сервер мониторинга (кэш бота из MetaApi) для Forex/металлов
        spec["source"] = ({"kind": "binance", "symbol": coin, "rest": self.settings.binance_rest} if coin.endswith("USDT")
                          else {"kind": "server", "symbol": coin, "url": self.settings.normalized_url, "key": self.settings.api_key})
        spec["favorite"] = {"url": self.settings.normalized_url, "key": self.settings.api_key, "trade_id": trade["id"], "profile": self.settings.profile,
                            "is_favorite": bool(snap["trade"].get("is_favorite"))}  # окно графика (отдельный процесс) ставит звёздочку само
        return spec

    async def _post_candles(self, snap: dict, iv: int, now_ms: int) -> tuple[list[dict] | None, str | None]:
        """Свечи после сигнала из публичного REST Binance (без ключа API мониторинга!). Недоступно - график строится по снапшоту, причина в панели."""
        t = snap["trade"]
        if not t["coin"].endswith("USDT"):
            return None, None  # Forex/металлы (MT5): на Binance Futures их нет, запрос дал бы 400
        start = snap["candles"][-1]["open_time"] + iv
        # прерванная рестартом сделка «закрыта» моментом рестарта, но график должен идти дальше - до сейчас (можно мотать вправо); остальные - до выхода + запас
        end = now_ms if t["status"] == "aborted" else (fmt.to_ms(t["exit_time"]) or now_ms) + POST_BARS_MARGIN * iv
        if start >= min(end, now_ms):
            return None, None
        async with aiohttp.ClientSession() as session:  # отдельная сессия: заголовок X-API-Key к Binance не отправляется никогда
            try:
                klines, gaps = await fetch_klines_filled(session, self.settings.binance_rest, t["coin"], snap["meta"]["interval"], start, min(end, now_ms),
                                                         interval_ms=iv, now_ms=now_ms)  # хвост догружается страницами, разрывы - отдельными запросами
            except BinanceNoData as exc:
                log.info("Свечи после сигнала %s: %s - график по снимку из БД", t["coin"], exc)
                return None, None
            except BinanceError as exc:
                return None, f"Свечи после сигнала не загружены: {exc}."
        note = "Свечи после сигнала — публичные данные Binance (в БД не хранятся)." if klines else None
        if klines and gaps:
            note += f" Не удалось догрузить свечей: {gaps} (разрыв у биржи/сети)."
        return klines, note
