"""Оболочка приложения: заголовок со статусом связи, вкладки, диалог настроек, подключение к API и открытие графиков сделок."""

import logging
import time

import aiohttp
import flet as ft

from tradeai_client import APP_NAME, __version__, fmt
from tradeai_client.api import ApiClient, ApiError, LiveStateStream
from tradeai_client.binance import BinanceError, BinanceNoData, fetch_klines
from tradeai_client.chart_spec import ChartDataError, build_chart_spec, interval_to_ms
from tradeai_client.chart_window import open_chart
from tradeai_client.settings import ClientSettings, SettingsError, load_settings, save_settings
from tradeai_client.ui import widgets as w
from tradeai_client.ui.dashboard import DashboardView
from tradeai_client.ui.history_view import HistoryView
from tradeai_client.ui.stats_view import StatsView

log = logging.getLogger("tradeai_client")
STATUS_COLORS = {"connected": w.GREEN, "connecting": w.AMBER, "reconnecting": w.AMBER, "auth": w.RED, "throttled": w.RED, "error": w.RED, "idle": w.MUTED}
TAB_DASHBOARD, TAB_STATS, TAB_HISTORY = 0, 1, 2
ACCOUNT_REFRESH_S = 30.0  # как часто обновлять баланс в шапке (движок пишет снимок счёта раз в ~30 с)
POST_BARS_MARGIN = 5  # свечей после выхода, которые показываем за последним маркером


class Shell:
    def __init__(self, page: ft.Page) -> None:
        self.page = page
        self.client: ApiClient | None = None
        self._stream: LiveStateStream | None = None
        self._events_seen: tuple = ()
        self._tab = TAB_DASHBOARD
        try:
            self.settings = load_settings()
            self._settings_error = None
        except SettingsError as exc:
            self.settings, self._settings_error = ClientSettings(), str(exc)
        self.saved_mode = self.settings.mode_filter if self.settings.mode_filter in fmt.MODE_KEYS else ""  # выбор пользователя (Statistics/History) переживает перезапуск
        self.default_mode = "shadow"  # активный контур: live, пока боевой счёт присылает баланс (фильтры History / Statistics стартуют с него)
        self._account_at = 0.0
        self.dashboard, self.stats, self.history = DashboardView(), StatsView(self), HistoryView(self)
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
                         self._account, self._eye, ft.VerticalDivider(width=12, color=w.BORDER), self._dot, self._status_text, ft.IconButton(icon=ft.Icons.SETTINGS, tooltip="Настройки подключения", on_click=lambda e: self.show_settings())],
                        spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        tabs = ft.Tabs(length=3, selected_index=0, expand=True, on_change=self._tab_changed, content=ft.Column([
            ft.TabBar(tabs=[ft.Tab(label="Dashboard", icon=ft.Icons.DASHBOARD), ft.Tab(label="Statistics", icon=ft.Icons.BAR_CHART),
                            ft.Tab(label="History", icon=ft.Icons.HISTORY)]),
            ft.TabBarView(controls=[self.dashboard.control, self.stats.control, self.history.control], expand=True)], expand=True))
        p.add(header, ft.Container(height=6), tabs)

    def run(self, coro) -> None:
        """Запустить корутину из синхронного обработчика Flet."""
        async def runner() -> None:
            await coro
        self.page.run_task(runner)

    def notify(self, text: str, *, error: bool = False) -> None:
        self.page.show_dialog(ft.SnackBar(content=ft.Text(text, color=w.TEXT), bgcolor="#3a1d1d" if error else "#1d2b3a", duration=6000 if error else 3500))

    async def start(self) -> None:
        if self._settings_error:
            self.show_settings(note=self._settings_error)
        elif not self.settings.api_key:
            self.show_settings(note="Введите адрес сервера и API-ключ (на сервере: ./deploy/shadow.sh apikey).")
        else:
            await self.connect()

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
        for view in (self.stats, self.history):
            view.set_coins(list(info.get("coins") or []))
        account = info.get("account")
        live = bool(account and account.get("available"))
        self._account_data = account
        self._render_account()
        self.default_mode = fmt.default_mode(account)
        if self.stats.follow_default_mode(self.default_mode) | self.history.follow_default_mode(self.default_mode):
            await self._refresh_tab()

    def _render_account(self) -> None:
        shown = self.settings.balance_visible
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

    # ------------------------------------------------------------------------------------------ настройки

    def show_settings(self, note: str | None = None) -> None:
        s = self.settings
        url = ft.TextField(label="Адрес сервера", value=s.base_url, hint_text="http://193.233.75.254:8001", autofocus=True, dense=True)
        key = ft.TextField(label="API-ключ", value=s.api_key, password=True, can_reveal_password=True, dense=True)
        remember = ft.Switch(label="Запомнить ключ на этом ПК", value=s.remember_key)
        post = ft.Switch(label="Достраивать свечи после сигнала (публичный REST Binance)", value=s.post_signal_candles)
        msg = ft.Text(note or "", size=12, color=w.AMBER, selectable=True)
        busy = ft.ProgressRing(width=16, height=16, stroke_width=2, visible=False)
        hint = ft.Text("Адрес и ключ сохраняются в settings.json рядом с .exe: при следующем запуске приложение подключится само. "
                       "Ключ передаётся только в заголовке запроса. Нужен прямой доступ к порту API на сервере (файрвол!); "
                       "альтернатива - SSH-туннель и адрес http://127.0.0.1:8000. ", size=11, color=w.MUTED)

        async def save(_=None) -> None:
            candidate = ClientSettings(base_url=url.value or "", api_key=(key.value or "").strip(), remember_key=bool(remember.value),
                                       post_signal_candles=bool(post.value), binance_rest=self.settings.binance_rest,
                                       balance_visible=self.settings.balance_visible, mode_filter=self.settings.mode_filter)
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
                                content=ft.Container(width=520, content=ft.Column([url, key, remember, post, warning, hint, ft.Row([busy, msg], spacing=8)], spacing=12, tight=True)),
                                actions=[ft.TextButton("Отмена", on_click=lambda e: self.page.pop_dialog()), ft.FilledButton("Проверить и сохранить", on_click=save)],
                                actions_alignment=ft.MainAxisAlignment.END)
        self.page.show_dialog(dialog)

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
            snap = await client.snapshot(trade["id"])
            iv = interval_to_ms((snap.get("meta") or {}).get("interval", ""))
            now_ms = int(time.time() * 1000)
            post, note = None, None
            if self.settings.post_signal_candles:
                post, note = await self._post_candles(snap, iv, now_ms)
            spec = build_chart_spec(snap, post, now_ms=now_ms, post_note=note)
        except ApiError as exc:
            self.notify(str(exc), error=True)
            return
        except ChartDataError as exc:
            self.notify(f"Снапшот нельзя нарисовать: {exc}", error=True)
            return
        open_chart(spec)

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
                klines = await fetch_klines(session, self.settings.binance_rest, t["coin"], snap["meta"]["interval"], start, min(end, now_ms),
                                            interval_ms=iv, now_ms=now_ms)
            except BinanceNoData as exc:
                log.info("Свечи после сигнала %s: %s - график по снимку из БД", t["coin"], exc)
                return None, None
            except BinanceError as exc:
                return None, f"Свечи после сигнала не загружены: {exc}."
        return klines, ("Свечи после сигнала — публичные данные Binance (в БД не хранятся)." if klines else None)
