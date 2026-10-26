"""Окно полигона (Create Strategy): страница lab_web на TradingView Lightweight Charts в pywebview, отдельный процесс (как окно графика сделки).

Мост JS -> Python (`LabApi`): свечи Binance, случайный график, «как видит бот» (детекторы + сигналы на последней свече), сравнение с разметкой,
уроки, стратегии, подбор параметров, обмен с сервером. Ключ сервера и адрес остаются в Python - страница их не видит."""

import json
import logging
import multiprocessing
import time
from pathlib import Path
from typing import Any

from tradeai_client.lab import detect, market, match, remote, setups, store, tune, vocab

log = logging.getLogger("tradeai_client.lab")
WEB_DIR = Path(__file__).resolve().parent.parent / "lab_web"
CHART_LIB = Path(__file__).resolve().parent.parent / "chart_web" / "lightweight-charts.js"
ANALYZE_BARS = 600  # окно, на котором бот «смотрит» (быстро и как у трейдера на экране)


def _js_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).replace("<", "\\u003c")


def build_html(cfg: dict[str, Any]) -> str:
    html = (WEB_DIR / "lab.html").read_text(encoding="utf-8")
    for marker, text in (("/*__CSS__*/", (WEB_DIR / "lab.css").read_text(encoding="utf-8")), ("/*__LIB__*/", CHART_LIB.read_text(encoding="utf-8")),
                         ("/*__CFG__*/", _js_json(cfg)), ("/*__APP__*/", (WEB_DIR / "lab.js").read_text(encoding="utf-8"))):
        html = html.replace(marker, text, 1)
    return html


def _ok(**kw: Any) -> dict[str, Any]:
    return {"ok": True, **kw}


def _err(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


class LabApi:
    """Публичные методы - API страницы. Ошибки данных/сети возвращаются {"ok": false, "error"}, страница показывает их; ошибка кода - исключение в консоль."""

    def __init__(self, cfg: dict[str, Any]):
        profile = cfg.get("profile") or cfg.get("author") or ""
        self._s = {"server": cfg.get("server") or {}, "author": cfg.get("author", "") or profile}
        self._space = store.space_of(profile)  # 1.9.1: окно работает только в пространстве своего профиля (SANTA и CHR не пересекаются)
        self._root = store.lab_dir(profile)

    # ---- справочники
    def boot(self) -> dict[str, Any]:
        try:
            st = store.list_strategies(self._root)
            return _ok(vocab=[t.to_dict() for t in store.load_vocab(self._root)], strategies=[s["name"] for s in st], active=store.active_name(self._root), space=self._space,
                       author=self._s["author"], setups=list(setups.SETUPS), setup_names=setups.SETUP_RU)
        except (store.StoreError, OSError) as exc:
            return _err(exc)

    def top_coins(self) -> dict[str, Any]:
        try:
            return _ok(items=market.top_coins(15))
        except market.MarketError as exc:
            return _err(exc)

    def random_spot(self) -> dict[str, Any]:
        try:
            return _ok(**market.random_spot(market.top_coins(15)))
        except market.MarketError as exc:
            return _err(exc)

    # ---- свечи
    def history(self, symbol: str, tf: str, end_s: int, bars: int = 800) -> dict[str, Any]:
        try:
            return _ok(bars=market.history(symbol, tf, int(end_s), max(50, min(int(bars), 3000))))
        except market.MarketError as exc:
            return _err(exc)

    def future(self, symbol: str, tf: str, after_s: int, bars: int = 1000) -> dict[str, Any]:
        try:
            return _ok(bars=market.future(symbol, tf, int(after_s), max(1, min(int(bars), 1500))))
        except market.MarketError as exc:
            return _err(exc)

    # ---- бот
    def analyze(self, bars: list, tf: str, strategy: str = "", with_setups: bool = False) -> dict[str, Any]:
        """Разметка бота по свечам (последняя - закрытая на курсоре) + подсказки/сигналы на последней свече."""
        try:
            st = store.get_strategy(strategy or store.active_name(self._root), self._root)
        except store.StoreError:
            st = store.default_strategy()
        bb = [store.to_bar(b) for b in bars][-ANALYZE_BARS:]
        objs, ctx = detect.run(bb, market.IV[tf], st.get("enabled"), st.get("params"))
        ev = setups.evaluate(ctx, st.get("setup"), tuple(st.get("setups") or setups.SETUPS)) if with_setups else []
        return _ok(objects=objs, events=ev, trend=detect.trend_state(ctx, (st.get("setup") or {}).get("trend_k", 5)) if len(bb) >= 30 else 0)

    def compare(self, lesson: dict, strategy: str = "") -> dict[str, Any]:
        try:
            ls = store.clean_lesson(lesson)
            st = store.get_strategy(strategy or store.active_name(self._root), self._root)
        except store.StoreError as exc:
            return _err(exc)
        objs, _ = detect.run(ls["bars"], ls["iv"], st.get("enabled"), st.get("params"))
        return _ok(report=match.compare(ls, objs), objects=objs)

    # ---- уроки
    def save_lesson(self, lesson: dict) -> dict[str, Any]:
        try:
            ls = store.save_lesson({**lesson, "author": lesson.get("author") or self._s["author"]}, self._root)
        except store.StoreError as exc:
            return _err(exc)
        sent = None
        try:
            remote.push_lesson(self._s["server"].get("url", ""), self._s["server"].get("key", ""), self._space, ls)
            sent = True
        except remote.RemoteError as exc:
            sent = str(exc)
        return _ok(id=ls["id"], sent=sent)

    def list_lessons(self) -> dict[str, Any]:
        try:
            return _ok(items=store.list_lessons(self._root))
        except store.StoreError as exc:
            return _err(exc)

    def load_lesson(self, lesson_id: str) -> dict[str, Any]:
        try:
            return _ok(lesson=store.load_lesson(lesson_id, self._root))
        except store.StoreError as exc:
            return _err(exc)

    # ---- словарь и стратегии
    def add_term(self, name: str, kind: str, color: str = "#c9d1d9", hint: str = "") -> dict[str, Any]:
        try:
            t = store.add_term(name, kind, color, hint, root=self._root)
        except (vocab.VocabError, store.StoreError) as exc:
            return _err(exc)
        return _ok(term=t.to_dict(), vocab=[x.to_dict() for x in store.load_vocab(self._root)])

    def get_strategy(self, name: str = "") -> dict[str, Any]:
        try:
            st = store.get_strategy(name or store.active_name(self._root), self._root)
        except store.StoreError as exc:
            return _err(exc)
        return _ok(strategy=st, defaults=detect.DEFAULT_PARAMS)

    def set_active(self, name: str) -> dict[str, Any]:
        try:
            store.set_active(name, self._root)
        except store.StoreError as exc:
            return _err(exc)
        return _ok()

    def save_strategy(self, st: dict, why: str = "") -> dict[str, Any]:
        try:
            saved = store.save_strategy(st, why, root=self._root)
        except store.StoreError as exc:
            return _err(exc)
        try:
            remote.push_strategy(self._s["server"].get("url", ""), self._s["server"].get("key", ""), self._space, saved)
        except remote.RemoteError:
            pass  # обмен с сервером - необязательный; локальная копия сохранена
        return _ok(strategy=saved)

    def tune(self, strategy: str = "") -> dict[str, Any]:
        """Подбор параметров под ВСЕ уроки. Ничего не сохраняет: применяет apply_tune после просмотра отчёта."""
        try:
            st = store.get_strategy(strategy or store.active_name(self._root), self._root)
            lessons = store.load_lessons(self._root)
        except store.StoreError as exc:
            return _err(exc)
        if not lessons:
            return _err(ValueError("Нет уроков: сначала разметьте и сохраните хотя бы один"))
        t0 = time.monotonic()
        res = tune.tune(lessons, st.get("params"))
        return _ok(**res, seconds=round(time.monotonic() - t0, 1), lessons=len(lessons))

    def apply_tune(self, strategy: str, params: dict) -> dict[str, Any]:
        try:
            st = store.get_strategy(strategy, self._root)
        except store.StoreError as exc:
            return _err(exc)
        return self.save_strategy({**st, "params": params}, "подбор под уроки")


def run_lab_window(cfg: dict[str, Any]) -> None:
    import webview

    api = LabApi(cfg)
    page_cfg = {k: v for k, v in cfg.items() if k != "server"}
    who = (cfg.get("profile") or cfg.get("author") or "").upper()
    window = webview.create_window(f"TradeAI · Create Strategy · {who}" if who else "TradeAI · Create Strategy",html=build_html(page_cfg), js_api=api, background_color="#0e1117", maximized=True,
                                   width=1600, height=950, focus=True)

    def show(win: Any) -> None:
        win.maximize()

    webview.start(show, window)


_PROCS: list = []


def open_lab(cfg: dict[str, Any]) -> "multiprocessing.process.BaseProcess":
    """Открыть полигон в отдельном процессе. cfg: profile (SANTA | CHR - пространство уроков), author, server {url, key}, open {symbol, tf, at_s} | lesson_id, strategy."""
    if not _PROCS:
        import atexit
        atexit.register(terminate_all)  # окна полигона закрываются вместе с приложением
    _PROCS[:] = [p for p in _PROCS if p.is_alive()]
    p = multiprocessing.get_context("spawn").Process(target=run_lab_window, args=(cfg,), name="lab")
    p.start()
    _PROCS.append(p)
    return p


def terminate_all() -> None:
    for p in _PROCS:
        if p.is_alive():
            p.terminate()
    _PROCS.clear()
