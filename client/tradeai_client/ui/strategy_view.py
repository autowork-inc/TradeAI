"""Вкладка «Create Strategy» (1.9.0): обучение бота видеть рынок как трейдер.

Отсюда открывается полигон (окно графика: разметка, подписи, реплей, сделки, «как видит бот»), здесь же - стратегии (понятия, подбор под уроки,
история и откат), библиотека уроков и словарь (свои понятия). Всё - в пространстве АКТИВНОГО профиля (1.9.1): у SANTA и CHR
своё видение рынка, их уроки, словари и стратегии не пересекаются ни на ПК, ни на сервере. Вычисления - пакет tradeai_client.lab."""

import asyncio
from datetime import datetime
from typing import Any

import flet as ft

from tradeai_client import fmt
from tradeai_client import profiles
from tradeai_client.lab import remote, store, tune, vocab
from tradeai_client.lab import sync as lab_sync
from tradeai_client.ui import widgets as w

KIND_RU = {"box": "область", "hline": "горизонталь", "line": "линия", "point": "метка", "trade": "сделка"}


def _when(ts: int | None) -> str:
    return "—" if not ts else datetime.fromtimestamp(ts).strftime("%d.%m.%y %H:%M")


class StrategyLabView:
    def __init__(self, host) -> None:
        self.host = host
        self._coin = ft.TextField(label="Монета", value="BTCUSDT", width=150, dense=True, border_color=w.BORDER)
        self._tf = ft.Dropdown(label="Таймфрейм", value="15m", width=120, dense=True, border_color=w.BORDER,
                               options=[ft.DropdownOption(key=x, text=x) for x in ("1m", "5m", "15m", "1h", "4h")])
        self._date = ft.TextField(label="Момент", hint_text="ГГГГ-ММ-ДД ЧЧ:ММ (пусто - сейчас)", width=210, dense=True, border_color=w.BORDER)
        self._strategy = ft.Dropdown(label="Стратегия", width=260, dense=True, border_color=w.BORDER, on_select=self._strategy_changed)
        self._strategy_box = ft.Column(spacing=6)
        self._tune_box = ft.Column(spacing=6)
        self._lessons = ft.Column(spacing=4)
        self._vocab = ft.Row(spacing=6, run_spacing=6, wrap=True)
        self._term_name = ft.TextField(label="Новое понятие", width=220, dense=True, border_color=w.BORDER)
        self._term_kind = ft.Dropdown(label="Как рисуется", value="box", width=160, dense=True, border_color=w.BORDER,
                                      options=[ft.DropdownOption(key=k, text=v) for k, v in KIND_RU.items() if k != "trade"])
        self._sync_note = ft.Text("", size=12, color=w.MUTED)
        self._space_note = ft.Text("", size=12, color=w.MUTED, italic=True)
        intro = w.card(ft.Column([
            w.title("Create Strategy — учим бота видеть рынок вашими глазами"),
            w.label("1) Откройте полигон: любой отрезок любой монеты или случайный. 2) Рисуйте уровни, зоны, наклонки, гэпы, проторговки, входы и стопы — "
                    "и к каждому рисунку выбирайте понятие и пишите, ПОЧЕМУ вы так видите. 3) «Как видит бот» и «Сравнить» покажут, что бот нарисовал так же, "
                    "что лишнее и что пропустил. 4) Сохраните урок. 5) «Подобрать под мои уроки» подстроит бота сразу под все уроки. "
                    "Реплей (▶) открывает свечи по одной: ставьте сделки, бот подсказывает, где ищет вход, и даёт сигналы — не зная будущего.", size=13),
            ft.Row([self._coin, self._tf, self._date,
                    ft.FilledButton("Открыть полигон", icon=ft.Icons.DRAW, on_click=lambda e: self._open({"symbol": (self._coin.value or "BTCUSDT").upper(),
                                                                                                          "tf": self._tf.value or "15m", "at_s": self._date_s()})),
                    ft.OutlinedButton("🎲 Случайный график", on_click=lambda e: self._open({"random": True}))], spacing=10, wrap=True, run_spacing=8),
            self._space_note,
        ], spacing=10))
        strat = w.card(ft.Column([ft.Row([w.title("Стратегия", 15), self._strategy,
                                          ft.TextButton("Новая", icon=ft.Icons.ADD, on_click=self._new_strategy)], spacing=10, wrap=True),
                                  self._strategy_box,
                                  ft.Row([ft.FilledButton("🧠 Подобрать под мои уроки", on_click=lambda e: self.host.run(self._tune())),
                                          ft.OutlinedButton("↶ Откатить последнее изменение", on_click=lambda e: self.host.run(self._rollback()))], spacing=10, wrap=True),
                                  self._tune_box], spacing=8))
        lessons = w.card(ft.Column([ft.Row([w.title("Уроки", 15), ft.Container(expand=True),
                                            ft.OutlinedButton("⟳ Синхронизировать с сервером", on_click=lambda e: self.host.run(self._sync()))],
                                           vertical_alignment=ft.CrossAxisAlignment.CENTER),
                                    self._sync_note, self._lessons], spacing=8))
        voc = w.card(ft.Column([w.title("Словарь", 15),
                                w.label("Встроенные понятия бот умеет рисовать. Свои понятия копятся в уроках; чтобы бот начал их рисовать, нужен новый детектор "
                                        "(его пишет разработчик по вашим урокам — поэтому важно подписывать причину).", size=12),
                                self._vocab,
                                ft.Row([self._term_name, self._term_kind, ft.OutlinedButton("Добавить", icon=ft.Icons.ADD, on_click=self._add_term)], spacing=10, wrap=True)],
                               spacing=8))
        self.control = ft.Column([ft.Container(height=2), intro, ft.ResponsiveRow([ft.Column([strat, voc], col={"xs": 12, "lg": 6}),
                                                                                  ft.Column([lessons], col={"xs": 12, "lg": 6})], spacing=14, run_spacing=14,
                                                                                 vertical_alignment=ft.CrossAxisAlignment.START)],
                                 expand=True, scroll=ft.ScrollMode.AUTO, spacing=12)
        self._last_tune: dict | None = None

    # ------------------------------------------------------------------------------------------ общее

    def _update(self) -> None:
        if w.attached(self.control):
            self.control.update()

    def reset(self) -> None:
        return None

    def relayout(self) -> None:
        return None

    def _profile(self) -> str:
        return profiles.normalize_profile(getattr(getattr(self.host, "settings", None), "profile", ""))

    def _root(self):
        """Каталог уроков активного профиля (SANTA и CHR - разные папки, 1.9.1)."""
        return store.lab_dir(self._profile())

    def _server(self) -> dict[str, str]:
        s = getattr(self.host, "settings", None)
        return {"url": getattr(s, "normalized_url", "") or "", "key": getattr(s, "api_key", "") or ""}

    def _date_s(self) -> int | None:
        text = (self._date.value or "").strip()
        if not text:
            return None
        for f in ("%Y-%m-%d %H:%M", "%Y-%m-%d", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
            try:
                return int(datetime.strptime(text, f).timestamp())
            except ValueError:
                continue
        self.host.notify("Момент: формат ГГГГ-ММ-ДД ЧЧ:ММ", error=True)
        return None

    def _open(self, open_: dict[str, Any] | None = None, lesson_id: str | None = None) -> None:
        from tradeai_client.lab.lab_window import open_lab

        cfg = {"profile": self._profile(), "author": self._profile(), "server": self._server(), "strategy": self._strategy.value or ""}
        if lesson_id:
            cfg["lesson_id"] = lesson_id
        else:
            cfg["open"] = {k: v for k, v in (open_ or {}).items() if v is not None}
        open_lab(cfg)
        self.host.notify("Полигон открывается в отдельном окне…")

    # ------------------------------------------------------------------------------------------ данные

    async def refresh(self) -> None:
        try:
            root = await asyncio.to_thread(self._root)
            strategies, active, lessons, terms = await asyncio.to_thread(lambda: (store.list_strategies(root), store.active_name(root), store.list_lessons(root), store.load_vocab(root)))
        except store.StoreError as exc:
            self._lessons.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self._update()
            return
        self._strategy.options = [ft.DropdownOption(key=s["name"], text=s["name"]) for s in strategies]
        self._strategy.value = active
        st = next(s for s in strategies if s["name"] == active)
        self._render_strategy(st)
        self._space_note.value = f"Пространство профиля {self._profile()}: уроки, словарь и стратегии только этого профиля (другой профиль их не видит)."
        self._render_lessons(lessons, terms)
        self._render_vocab(terms)
        self._update()

    def _render_strategy(self, st: dict) -> None:
        names = {t.id: t.name for t in vocab.BUILTIN}
        changed = [f"{names.get(k, k)}: " + ", ".join(f"{p}={v:g}" for p, v in ps.items()) for k, ps in (st.get("params") or {}).items() if ps]
        rows = [w.label(f"Бот рисует: {', '.join(names.get(t, t) for t in st.get('enabled', []))}", size=12),
                w.label("Настройки, подобранные под уроки: " + ("; ".join(changed) if changed else "по умолчанию"), size=12)]
        hist = st.get("history") or []
        if hist:
            rows.append(w.label("История изменений (последние): " + " · ".join(f"{_when(h['at'])} {h.get('why', '')}" for h in hist[-4:][::-1]), size=12))
        self._strategy_box.controls = rows

    def _render_lessons(self, items: list[dict], terms: list[vocab.Term]) -> None:
        names = {t.id: t.name for t in terms}
        if not items:
            self._lessons.controls = [w.label("Уроков пока нет. Откройте полигон, разметьте график и нажмите «Сохранить как урок».", size=13)]
            return
        rows = []
        for x in items:
            terms = ", ".join(f"{names.get(k, k)} {v}" for k, v in x["terms"].items()) or "без разметки"
            rows.append(ft.Container(content=ft.Row([
                ft.Column([ft.Text(f"{fmt.coin_short(x['symbol'])} {x['tf']} · {_when(x['to_t'])}", weight=ft.FontWeight.W_600, size=13),
                           ft.Text(terms + (f" · автор {x['author']}" if x.get("author") else ""), size=12, color=w.MUTED),
                           *([ft.Text(x["notes"], size=12, color=w.MUTED, italic=True)] if x.get("notes") else [])], spacing=1, expand=True),
                ft.IconButton(icon=ft.Icons.OPEN_IN_NEW, tooltip="Открыть урок в полигоне", on_click=lambda e, i=x["id"]: self._open(lesson_id=i)),
                ft.IconButton(icon=ft.Icons.DELETE_OUTLINE, tooltip="Удалить урок с ПК", on_click=lambda e, i=x["id"]: self.host.run(self._delete(i)))],
                vertical_alignment=ft.CrossAxisAlignment.START), padding=ft.Padding.symmetric(horizontal=6, vertical=4), border_radius=8, bgcolor=w.CARD_HI))
        self._lessons.controls = rows

    def _render_vocab(self, terms: list[vocab.Term]) -> None:
        chips = []
        for t in terms:
            if t.kind == "trade":
                continue
            chips.append(ft.Container(content=ft.Row([w.dot(t.color), ft.Text(t.name, size=12, italic=not t.detector)], spacing=6, tight=True),
                                      padding=ft.Padding.symmetric(horizontal=10, vertical=4), border=ft.Border.all(1, w.BORDER), border_radius=14,
                                      tooltip=t.hint + ("" if t.detector else " — своё понятие, детектора пока нет")))
        self._vocab.controls = chips

    # ------------------------------------------------------------------------------------------ действия

    async def _strategy_changed(self, e=None) -> None:
        await asyncio.to_thread(store.set_active, self._strategy.value, self._root())
        self._tune_box.controls = []
        await self.refresh()

    def _new_strategy(self, e=None) -> None:
        field = ft.TextField(label="Название", autofocus=True, width=300)

        async def create() -> None:
            try:
                root = self._root()
                st = await asyncio.to_thread(lambda: store.save_strategy(store.default_strategy(field.value or ""), "создана", root=root))
                await asyncio.to_thread(store.set_active, st["name"], root)
            except store.StoreError as exc:
                self.host.notify(str(exc), error=True)
                return
            self.host.page.pop_dialog()
            await self.refresh()

        self.host.page.show_dialog(ft.AlertDialog(title=ft.Text("Новая стратегия"), content=field,
                                                  actions=[ft.TextButton("Отмена", on_click=lambda e: self.host.page.pop_dialog()),
                                                           ft.FilledButton("Создать", on_click=lambda e: self.host.run(create()))]))

    async def _tune(self) -> None:
        name = self._strategy.value
        self._tune_box.controls = [ft.Row([ft.ProgressRing(width=18, height=18, stroke_width=2), w.label("подбираю настройки по всем урокам…")])]
        self._update()
        try:
            root = await asyncio.to_thread(self._root)
            st = await asyncio.to_thread(store.get_strategy, name, root)
            lessons = await asyncio.to_thread(store.load_lessons, root)
        except store.StoreError as exc:
            self._tune_box.controls = [w.banner(str(exc), w.RED, ft.Icons.ERROR)]
            self._update()
            return
        if not lessons:
            self._tune_box.controls = [w.label("Нет уроков: сначала разметьте и сохраните хотя бы один.", size=13)]
            self._update()
            return
        res = await asyncio.to_thread(tune.tune, lessons, st.get("params"))
        self._last_tune = res
        names = {t.id: t.name for t in store.load_vocab(root)}
        rows = [w.table_row([w.cell(x, color=w.MUTED, size=12) for x in ("Понятие", "Уроков", "Было", "Стало")], widths=[180, 70, 70, 70], height=28)]
        for k, v in res["report"].items():
            better = v["after"]["f1"] > v["before"]["f1"]
            rows.append(w.table_row([w.cell(names.get(k, k)), w.cell(str(v["lessons"])), w.cell(f"{v['before']['f1']:.0%}"),
                                     w.cell(f"{v['after']['f1']:.0%}", color=w.GREEN if better else w.TEXT)], widths=[180, 70, 70, 70], height=30))
        extra = [w.label("Без детектора (нужен код): " + ", ".join(names.get(k, k) for k in res["skipped_terms"]), size=12)] if res["skipped_terms"] else []
        self._tune_box.controls = [*rows, *extra, w.label(f"Уроков: {len(lessons)}. Совпадение = доля общих объектов (ваших и бота) с допуском по цене и времени.", size=12),
                                   ft.FilledButton("✅ Применить подбор", on_click=lambda e: self.host.run(self._apply_tune()))]
        self._update()

    async def _apply_tune(self) -> None:
        if not self._last_tune:
            return
        root, params = self._root(), self._last_tune["params"]
        st = await asyncio.to_thread(store.get_strategy, self._strategy.value, root)
        saved = await asyncio.to_thread(lambda: store.save_strategy({**st, "params": params}, "подбор под уроки", root=root))
        self._last_tune = None
        self._tune_box.controls = [w.label("Применено. Прежние настройки - в истории, «Откатить» вернёт их.", size=13)]
        await self._push_strategy(saved)
        await self.refresh()

    async def _rollback(self) -> None:
        try:
            root = self._root()
            await asyncio.to_thread(lambda: store.rollback_strategy(self._strategy.value, root=root))
        except store.StoreError as exc:
            self.host.notify(str(exc), error=True)
            return
        self.host.notify("Откат выполнен")
        await self.refresh()

    async def _delete(self, lesson_id: str) -> None:
        await asyncio.to_thread(store.delete_lesson, lesson_id, self._root())
        await self.refresh()

    async def _add_term(self, e=None) -> None:
        try:
            root = self._root()
            await asyncio.to_thread(lambda: store.add_term(self._term_name.value or "", self._term_kind.value or "box", root=root))
        except (vocab.VocabError, store.StoreError) as exc:
            self.host.notify(str(exc), error=True)
            return
        self._term_name.value = ""
        await self.refresh()

    async def _push_strategy(self, st: dict) -> None:
        srv = self._server()
        try:
            await asyncio.to_thread(remote.push_strategy, srv["url"], srv["key"], store.space_of(self._profile()), st)
        except remote.RemoteError:
            pass  # обмен - необязательный

    async def _sync(self) -> None:
        """Обмен с сервером в пространстве АКТИВНОГО профиля (lab/sync.py): уроки другого профиля не скачиваются и не отправляются."""
        srv, profile = self._server(), self._profile()
        self._sync_note.value = "синхронизация…"
        self._update()
        try:
            res = await asyncio.to_thread(lab_sync.sync, srv["url"], srv["key"], profile)
            self._sync_note.value = (f"{profile}: отправлено уроков {res['up']}, получено {res['down']}. "
                                     f"Своих понятий в словаре: {res['terms']}.")
        except (remote.RemoteError, store.StoreError) as exc:
            self._sync_note.value = f"Сервер: {exc}"
        await self.refresh()

