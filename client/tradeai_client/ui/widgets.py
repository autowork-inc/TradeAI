"""Мелкие переиспользуемые виджеты в одном стиле: карточки, KPI, «таблетки», горизонтальные полосы, столбцы кривой капитала."""

import asyncio
import math

import flet as ft
import flet.canvas as cv

from tradeai_client import fmt

BG, CARD, CARD_HI, BORDER = "#0e1117", "#161b22", "#1c232d", "#2a313c"
TEXT, MUTED, ACCENT, AMBER = "#e6e9ef", "#8b93a3", "#4aa3ff", "#d9a441"
GREEN, RED = fmt.GREEN, fmt.RED


def attached(control: ft.Control) -> bool:
    """Контрол уже на странице? В Flet 1.x обращение к `.page` у неприсоединённого контрола - RuntimeError (а не None): так строятся и тестируются виды без окна."""
    try:
        return control.page is not None
    except RuntimeError:
        return False


def card(content: ft.Control, *, width: float | None = None, padding: float = 16, expand: bool | int | None = None,
         bgcolor: str = CARD, border_color: str = BORDER) -> ft.Container:
    return ft.Container(content=content, width=width, padding=padding, expand=expand, bgcolor=bgcolor, border_radius=12,
                        border=ft.Border.all(1, border_color))


def label(text: str, *, size: float = 12, color: str = MUTED, weight: ft.FontWeight | None = None) -> ft.Text:
    return ft.Text(text, size=size, color=color, weight=weight)


def title(text: str, size: float = 18) -> ft.Text:
    return ft.Text(text, size=size, weight=ft.FontWeight.W_600, color=TEXT)


def kpi(name: str, value: str, *, color: str = TEXT, sub: str | None = None, width: float = 200, tooltip: str | None = None) -> ft.Container:
    body = [label(name), ft.Text(value, size=26, weight=ft.FontWeight.W_700, color=color)]
    if sub:
        body.append(label(sub, size=11))
    return ft.Container(content=ft.Column(body, spacing=2, tight=True), width=width, height=100, padding=ft.Padding.symmetric(horizontal=16, vertical=12),
                        bgcolor=CARD, border_radius=12, border=ft.Border.all(1, BORDER), tooltip=tooltip)


def pill(text: str, color: str, *, filled: bool = False) -> ft.Container:
    return ft.Container(content=ft.Text(text, size=11, color=BG if filled else color, weight=ft.FontWeight.W_600),
                        padding=ft.Padding.symmetric(horizontal=8, vertical=2), border_radius=10,
                        bgcolor=color if filled else None, border=None if filled else ft.Border.all(1, color))


def dot(color: str, size: float = 10) -> ft.Container:
    return ft.Container(width=size, height=size, border_radius=size / 2, bgcolor=color)


def hbar(frac: float, color: str, *, width: float = 240, height: float = 10) -> ft.Container:
    """Горизонтальная полоса: заполненная доля frac (0..1) от width."""
    frac = max(0.0, min(1.0, frac))
    return ft.Container(width=width, height=height, bgcolor=BORDER, border_radius=height / 2,
                        content=ft.Container(width=max(frac * width, 2 if frac > 0 else 0), height=height, bgcolor=color, border_radius=height / 2),
                        alignment=ft.Alignment.CENTER_LEFT)


SHIMMER = ("#8b5cf6", "#4aa3ff", "#2bb673", "#d9a441", "#8b5cf6")  # переливающаяся рамка: фиолетовый -> синий -> зелёный -> золото


class GoalBar:
    """Карточка «значимость выводов»: кубок, прогресс к цели (сделок «в расчёте» / goal) и подпись; рамка переливается градиентом.
    Анимацию двигает `shimmer()` (фоновая задача страницы); без страницы или пока вкладка не на экране кадр просто пропускается."""

    def __init__(self, goal: int, *, width: float = 190, height: float = 12) -> None:
        self.goal, self.width, self._phase = goal, width, 0.0
        self._fill = ft.Container(width=0, height=height, border_radius=height / 2, animate=ft.Animation(700, ft.AnimationCurve.EASE_OUT),
                                  gradient=ft.LinearGradient(begin=ft.Alignment.CENTER_LEFT, end=ft.Alignment.CENTER_RIGHT, colors=["#8b5cf6", GREEN]))
        self._text = ft.Text("", size=13, weight=ft.FontWeight.W_700, color=TEXT)
        self._caption = ft.Text("", size=11, color=MUTED)
        track = ft.Container(width=width, height=height, bgcolor=BORDER, border_radius=height / 2, content=self._fill, alignment=ft.Alignment.CENTER_LEFT)
        trophy = ft.Container(content=ft.Icon(ft.Icons.EMOJI_EVENTS, size=26, color="#ffd166"), width=44, height=44, border_radius=22, alignment=ft.Alignment.CENTER,
                              bgcolor=ft.Colors.with_opacity(0.14, "#ffd166"), border=ft.Border.all(1, ft.Colors.with_opacity(0.5, "#ffd166")))
        inner = ft.Container(padding=ft.Padding.symmetric(horizontal=14, vertical=8), border_radius=13, bgcolor=CARD,
                             content=ft.Row([trophy, ft.Column([ft.Row([ft.Text("Значимость выводов", size=12, color=MUTED, weight=ft.FontWeight.W_600), self._text], spacing=10),
                                                               track, self._caption], spacing=4, tight=True)], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER))
        self._frame = ft.Container(content=inner, padding=2, border_radius=15, gradient=self._gradient(),
                                   tooltip=f"Прогресс к {goal} сделкам: столько нужно для статистически значимых выводов")
        self.control = self._frame
        self.set(0)

    def _gradient(self) -> ft.LinearGradient:
        a = self._phase
        return ft.LinearGradient(begin=ft.Alignment(math.cos(a), math.sin(a)), end=ft.Alignment(-math.cos(a), -math.sin(a)), colors=list(SHIMMER))

    def tick(self) -> None:
        """Один кадр анимации рамки: поворот градиента."""
        self._phase = (self._phase + 0.12) % (2 * math.pi)
        self._frame.gradient = self._gradient()
        if attached(self._frame):
            self._frame.update()

    async def shimmer(self, period_s: float = 0.2) -> None:
        while True:
            self.tick()
            await asyncio.sleep(period_s)

    def set(self, done: int) -> None:
        frac = max(0.0, min(1.0, done / self.goal))
        self._fill.width = max(frac * self.width, 3 if frac > 0 else 0)
        self._text.value = f"{done} / {self.goal}"
        self._caption.value = "Достаточно данных для выводов" if done >= self.goal else f"Ещё {self.goal - done} сделок до надёжной статистики ({self.goal}+)"


class MultiSelect:
    """Выпадающий список с чекбоксами (мультивыбор). Кнопка показывает выбранное; список открывается поверх страницы, высота ограничена, есть прокрутка.
    Клик по чекбоксу меняет выбор сразу и через ~0.25 с вызывает `on_change` (без кнопки «Применить»); «Выделить всё / Снять всё» - там же.
    Выбраны все = фильтра нет (`filter()` -> None); не выбрано ничего = пустой результат (`is_empty`)."""

    MAX_LIST_HEIGHT = 320
    DEBOUNCE_S = 0.25

    def __init__(self, label: str, options: list[tuple[str, str]], on_change, host, *, width: float = 190) -> None:
        self.label, self._on_change, self._host, self._ver = label, on_change, host, 0
        self._options: list[tuple[str, str]] = []
        self._selected: set[str] = set()
        self._boxes: dict[str, ft.Checkbox] = {}
        self._text = ft.Text("", size=13, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS, expand=True)
        self._button = ft.OutlinedButton(content=ft.Row([self._text, ft.Icon(ft.Icons.ARROW_DROP_DOWN, size=20)], spacing=4), width=width, height=56, on_click=self._open,
                                         style=ft.ButtonStyle(color=TEXT, bgcolor=CARD, side=ft.BorderSide(1, BORDER), shape=ft.RoundedRectangleBorder(radius=8)))
        self.control = self._button
        self.set_options(options, select_all=True)

    # ------------------------------------------------------------------------------------------ состояние

    @property
    def selected(self) -> list[str]:
        return [k for k, _ in self._options if k in self._selected]

    @property
    def all_selected(self) -> bool:
        return len(self._selected) == len(self._options)

    @property
    def is_empty(self) -> bool:
        return not self._selected

    def filter(self) -> list[str] | None:
        """Значение для запроса: None - без фильтра (выбраны все), иначе список ключей."""
        return None if self.all_selected else self.selected

    def set_options(self, options: list[tuple[str, str]], *, select_all: bool = False) -> None:
        """Новый набор вариантов. Выбор сохраняется; если были выбраны ВСЕ, новые варианты тоже выбираются (появившаяся монета не «пропадает» из вида)."""
        was_all = select_all or (bool(self._options) and self.all_selected)
        keys = [k for k, _ in options]
        self._selected = set(keys) if was_all else {k for k in self._selected if k in keys}
        self._options = list(options)
        self._render_button()

    def set_selected(self, keys: list[str]) -> None:
        known = {k for k, _ in self._options}
        self._selected = {k for k in keys if k in known}
        self._render_button()

    def summary(self) -> str:
        n, total = len(self._selected), len(self._options)
        if n == total:
            return f"{self.label}: все ({total})"
        if n == 0:
            return f"{self.label}: ничего"
        names = [t for k, t in self._options if k in self._selected]
        return f"{self.label}: {', '.join(names)}" if n <= 2 else f"{self.label}: {n} из {total}"

    def _render_button(self) -> None:
        self._text.value = self.summary()
        if attached(self._button):
            self._button.update()

    # ------------------------------------------------------------------------------------------ список

    def _open(self, _=None) -> None:
        self._boxes = {k: ft.Checkbox(label=t, value=k in self._selected, on_change=lambda e, key=k: self._toggle(key, bool(e.control.value))) for k, t in self._options}
        rows = ft.Column(list(self._boxes.values()), spacing=0, scroll=ft.ScrollMode.AUTO, height=min(self.MAX_LIST_HEIGHT, 40 * max(1, len(self._options))))
        tools = ft.Row([ft.TextButton("Выделить всё", on_click=lambda e: self._set_all(True)), ft.TextButton("Снять всё", on_click=lambda e: self._set_all(False))], spacing=4)
        dialog = ft.AlertDialog(modal=False, title=ft.Text(self.label, size=16), content=ft.Container(width=300, content=ft.Column([tools, rows], spacing=4, tight=True)),
                                actions=[ft.TextButton("Готово", on_click=lambda e: self._host.page.pop_dialog())], actions_alignment=ft.MainAxisAlignment.END)
        self._host.page.show_dialog(dialog)

    def _toggle(self, key: str, on: bool) -> None:
        (self._selected.add if on else self._selected.discard)(key)
        self._changed()

    def _set_all(self, on: bool) -> None:
        self._selected = {k for k, _ in self._options} if on else set()
        for box in self._boxes.values():
            box.value = on
            if attached(box):
                box.update()
        self._changed()

    def _changed(self) -> None:
        self._render_button()
        self._ver += 1
        self._host.run(self._fire(self._ver))

    async def _fire(self, ver: int) -> None:
        await asyncio.sleep(self.DEBOUNCE_S)
        if ver != self._ver:  # за это время кликнули ещё раз: обновится по последнему клику
            return
        res = self._on_change()
        if asyncio.iscoroutine(res):
            await res


def banner(text: str, color: str = AMBER, icon: ft.IconData = ft.Icons.WARNING_AMBER) -> ft.Container:
    return ft.Container(content=ft.Row([ft.Icon(icon, color=color, size=18), ft.Text(text, color=TEXT, size=13, expand=True)], spacing=10,
                                       vertical_alignment=ft.CrossAxisAlignment.START),
                        padding=ft.Padding.symmetric(horizontal=14, vertical=10), border_radius=10, bgcolor=ft.Colors.with_opacity(0.10, color),
                        border=ft.Border.all(1, ft.Colors.with_opacity(0.45, color)))


def equity_bars(points: list[dict], *, height: float = 180, max_bars: int = 140, key: str = "cum_r", unit: str = "R") -> ft.Control:
    """Кривая капитала в R (или в USDT: key="cum_pnl") столбцами от нулевой линии (без внешних графических пакетов: сборка .exe остаётся простой)."""
    if not points:
        return label("Закрытых сделок пока нет")
    step = max(1, -(-len(points) // max_bars))
    sample = points[::step]
    if sample[-1] is not points[-1]:
        sample.append(points[-1])
    peak = max(max(abs(p[key]) for p in sample), 1e-9)
    half = height / 2
    bar_w = int(max(3, min(16, 900 / len(sample))))  # мало сделок - столбцы шире, чтобы кривая читалась
    bars = []
    for p in sample:
        v = p[key]
        h = max(1.0, abs(v) / peak * (half - 2)) if v else 0
        up = ft.Container(height=half, alignment=ft.Alignment.BOTTOM_CENTER,
                          content=ft.Container(width=bar_w, height=h, bgcolor=GREEN, border_radius=1) if v > 0 else None)
        down = ft.Container(height=half, alignment=ft.Alignment.TOP_CENTER,
                            content=ft.Container(width=bar_w, height=h, bgcolor=RED, border_radius=1) if v < 0 else None)
        bars.append(ft.Container(content=ft.Column([up, down], spacing=0), tooltip=f"{fmt.fmt_local(p['ts'])}: {fmt.fmt_r(v) if unit == 'R' else fmt.fmt_usd(v)}"))
    return ft.Column([
        ft.Row(bars, spacing=1, vertical_alignment=ft.CrossAxisAlignment.CENTER, scroll=ft.ScrollMode.AUTO),
        ft.Row([label(f"{fmt.fmt_local(points[0]['ts'])}"), ft.Container(expand=True), label(f"пик ±{peak:.2f} {unit} · {fmt.fmt_local(points[-1]['ts'])}")]),
    ], spacing=4)


def table_row(cells: list[ft.Control], *, widths: list[float | None], bgcolor: str | None = None, height: float = 38) -> ft.Container:
    row = ft.Row([ft.Container(content=c, width=w, expand=w is None, alignment=ft.Alignment.CENTER_LEFT) for c, w in zip(cells, widths)],
                 spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER)
    return ft.Container(content=row, height=height, padding=ft.Padding.symmetric(horizontal=12), bgcolor=bgcolor, border_radius=8)


def cell(text: str, *, color: str = TEXT, weight: ft.FontWeight | None = None, size: float = 13, align_right: bool = False) -> ft.Text:
    return ft.Text(text, color=color, weight=weight, size=size, no_wrap=True, overflow=ft.TextOverflow.ELLIPSIS,
                   text_align=ft.TextAlign.RIGHT if align_right else ft.TextAlign.LEFT)


SERIES_COLORS = ("#4aa3ff", "#2bb673", "#d9a441", "#e05a5a", "#b07cf0", "#4cd1c7", "#f08a4b", "#9aa4b5")


def line_chart(series: list[dict], *, width: float = 860, height: float = 260, x_label: str = "номер сделки", y_label: str = "накопленный R") -> ft.Control:
    """Линейный график нескольких кривых (canvas, без внешних пакетов). series: [{"name", "color", "x": [...], "y": [...]}]; пустой список - подпись."""
    drawable = [s for s in series if len(s["x"]) >= 2]
    if not drawable:
        return label("Нет кривых для отображения (включите хотя бы одну серию; нужно >= 2 сделок)")
    left, bottom, top, right = 52.0, 22.0, 8.0, 12.0
    xs = [x for s in drawable for x in s["x"]]
    ys = [y for s in drawable for y in s["y"]] + [0.0]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    if y1 - y0 < 1e-9:
        y1 = y0 + 1.0
    if x1 - x0 < 1e-9:
        x1 = x0 + 1.0
    pw, ph = width - left - right, height - top - bottom
    px = lambda x: left + (x - x0) / (x1 - x0) * pw  # noqa: E731
    py = lambda y: top + (y1 - y) / (y1 - y0) * ph  # noqa: E731
    muted = ft.Paint(color=BORDER, stroke_width=1, style=ft.PaintingStyle.STROKE)
    shapes: list = [cv.Line(left, py(0.0), width - right, py(0.0), paint=ft.Paint(color=MUTED, stroke_width=1, style=ft.PaintingStyle.STROKE)),
                    cv.Line(left, top, left, top + ph, paint=muted), cv.Line(left, top + ph, width - right, top + ph, paint=muted)]
    tick = ft.TextStyle(size=10, color=MUTED)
    for v in (y0, 0.0, y1):
        shapes.append(cv.Text(2, py(v) - 7, f"{v:+.1f}".replace("-", "−") if v else "0", style=tick))
    shapes.append(cv.Text(left, height - 14, f"{x0:g}", style=tick))
    shapes.append(cv.Text(width - right - 30, height - 14, f"{x1:g} · {x_label}", style=tick, max_width=160))
    for s in drawable:
        pts = list(zip(s["x"], s["y"]))
        elements = [cv.Path.MoveTo(px(pts[0][0]), py(pts[0][1]))] + [cv.Path.LineTo(px(x), py(y)) for x, y in pts[1:]]
        shapes.append(cv.Path(elements, paint=ft.Paint(color=s["color"], stroke_width=2, style=ft.PaintingStyle.STROKE)))
    return ft.Column([cv.Canvas(shapes, width=width, height=height), label(f"Ось Y — {y_label}", size=11)], spacing=2, tight=True)
