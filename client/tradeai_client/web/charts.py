"""Графики сделок в веб-версии (2026-10-20): та же страница, что в окне настольного клиента (tradeai_client/chart_web), но вместо моста pywebview - HTTP-мост.

Как это устроено. Тап по сделке -> сервер (контейнер web) строит spec графика (снапшот сделки + свечи после сигнала), кладёт его в реестр под случайным
неугадываемым идентификатором и отдаёт ссылку `/chart/<id>` (nginx проксирует её на aiohttp-сервер внутри контейнера, порт 8551, наружу не опубликован).
Страница открывается в браузере телефона и обращается к тому же адресу: `/chart/<id>/klines` (история и опрос свечей - Binance Futures REST для крипты,
`/api/candles` бота для Forex/металлов), `/chart/<id>/favorite` (звёздочка). Реальное время Binance идёт WebSocket-ом прямо из страницы. Ключ API бота остаётся на сервере
и в страницу не попадает. Рисунки хранятся в браузере (localStorage по монете) - своё у каждого устройства.

Ссылка живёт 1 час (TTL) и в реестре не больше MAX_CHARTS записей: кто не знает идентификатор (192 бита), графика не откроет.
"""

import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web

from tradeai_client import fmt
from tradeai_client.api import ApiClient, ApiError
from tradeai_client.binance import BinanceError, BinanceNoData, fetch_klines_filled
from tradeai_client.chart_spec import build_chart_spec, interval_to_ms
from tradeai_client.chart_window import ChartApi, build_config, build_html
from tradeai_client.settings import ClientSettings

log = logging.getLogger("tradeai.web.charts")
TTL_S = 3600.0
MAX_CHARTS = 200
CHART_PORT = 8551
POST_BARS_MARGIN = 5  # свечей после выхода, которые показываем за последним маркером


@dataclass
class Entry:
    spec: dict
    trade_id: str
    coin: str
    is_favorite: bool
    expires: float = field(default_factory=lambda: time.monotonic() + TTL_S)


REGISTRY: dict[str, Entry] = {}


def register(spec: dict, trade: dict, *, now: float | None = None) -> str:
    """Положить график в реестр -> идентификатор для ссылки. Просроченные удаляются; при переполнении уходят самые старые."""
    t = time.monotonic() if now is None else now
    for k in [k for k, e in REGISTRY.items() if e.expires <= t]:
        del REGISTRY[k]
    while len(REGISTRY) >= MAX_CHARTS:
        REGISTRY.pop(next(iter(REGISTRY)))
    cid = secrets.token_urlsafe(24)
    REGISTRY[cid] = Entry(spec=spec, trade_id=trade["id"], coin=trade["coin"], is_favorite=bool(trade.get("is_favorite")), expires=t + TTL_S)
    return cid


def lookup(cid: str, *, now: float | None = None) -> Entry | None:
    e = REGISTRY.get(cid)
    if e is None or e.expires <= (time.monotonic() if now is None else now):
        REGISTRY.pop(cid, None)
        return None
    return e


# ================================================================================================ spec графика (копия логики Shell._chart_spec без окон)


async def build_spec(client: ApiClient, settings: ClientSettings, trade: dict) -> dict:
    """Снапшот + свечи после сигнала -> spec. Ошибки: ApiError, ChartDataError (chart_spec)."""
    snap = await client.snapshot(trade["id"])
    iv = interval_to_ms((snap.get("meta") or {}).get("interval", ""))
    now_ms = int(time.time() * 1000)
    post, note = None, None
    if settings.post_signal_candles:
        post, note = await _post_candles(settings, snap, iv, now_ms)
    spec = build_chart_spec(snap, post, now_ms=now_ms, post_note=note)
    coin = trade["coin"]
    spec["source"] = ({"kind": "binance", "symbol": coin, "rest": settings.binance_rest} if coin.endswith("USDT")
                      else {"kind": "server", "symbol": coin, "url": settings.normalized_url, "key": settings.api_key})
    return spec


async def _post_candles(settings: ClientSettings, snap: dict, iv: int, now_ms: int) -> tuple[list[dict] | None, str | None]:
    t = snap["trade"]
    if not t["coin"].endswith("USDT"):
        return None, None  # Forex/металлы (MT5): на Binance Futures их нет
    start = snap["candles"][-1]["open_time"] + iv
    end = now_ms if t["status"] == "aborted" else (fmt.to_ms(t["exit_time"]) or now_ms) + POST_BARS_MARGIN * iv
    if start >= min(end, now_ms):
        return None, None
    async with aiohttp.ClientSession() as session:  # заголовок X-API-Key к Binance не отправляется никогда
        try:
            klines, gaps = await fetch_klines_filled(session, settings.binance_rest, t["coin"], snap["meta"]["interval"], start, min(end, now_ms),
                                                     interval_ms=iv, now_ms=now_ms)
        except BinanceNoData as exc:
            log.info("Свечи после сигнала %s: %s - график по снимку из БД", t["coin"], exc)
            return None, None
        except BinanceError as exc:
            return None, f"Свечи после сигнала не загружены: {exc}."
    note = "Свечи после сигнала — публичные данные Binance (в БД не хранятся)." if klines else None
    if klines and gaps:
        note += f" Не удалось догрузить свечей: {gaps} (разрыв у биржи/сети)."
    return klines, note


# ================================================================================================ страница и HTTP-мост

SHIM = """(function () {
  var COIN = %(coin)s, KEY = 'tradeai-drawings:' + COIN, BASE = location.pathname.replace(/\\/+$/, '');
  try { var raw = localStorage.getItem(KEY); if (raw) window.__CFG__.drawings = JSON.parse(raw); } catch (e) { /* хранилище недоступно: рисунков нет */ }
  async function j(url, opt) { var r = await fetch(url, opt); if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }
  window.pywebview = { api: {
    klines: function (iv, end, limit) { return j(BASE + '/klines?interval=' + encodeURIComponent(iv) + (end ? '&end=' + end : '') + '&limit=' + (limit || 1000)); },
    save_drawings: async function (text) { try { localStorage.setItem(KEY, text); } catch (e) { throw new Error('хранилище браузера недоступно'); } return JSON.parse(text).length; },
    toggle_favorite: function (add) { return j(BASE + '/favorite', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ add: !!add }) }); }
  } };
  var back = document.getElementById('b-back');
  if (back) { back.style.display = ''; back.onclick = function () { if (history.length > 1) history.back(); else location.href = '/'; }; }
})();"""


def page_html(entry: Entry) -> str:
    cfg = build_config(entry.spec, [])
    cfg["favorite"] = {"is_favorite": entry.is_favorite}
    return build_html(cfg, shim=SHIM % {"coin": json.dumps(entry.coin).replace("<", "\\u003c")})


def make_app(settings: ClientSettings) -> web.Application:
    async def page(request: web.Request) -> web.Response:
        entry = lookup(request.match_info["cid"])
        if entry is None:
            return web.Response(status=404, text="График не найден или ссылка устарела (живёт 1 час). Откройте сделку в приложении заново.", content_type="text/plain", charset="utf-8")
        return web.Response(text=page_html(entry), content_type="text/html", charset="utf-8", headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

    async def klines(request: web.Request) -> web.Response:
        entry = lookup(request.match_info["cid"])
        if entry is None:
            return web.json_response({"error": "expired"}, status=404)
        q = request.query
        try:
            end = int(q["end"]) if q.get("end") else None
            limit = int(q.get("limit", "1000"))
            rows = await asyncio.to_thread(ChartApi(entry.spec).klines, q.get("interval", ""), end, limit)
        except (ValueError, KeyError) as exc:
            return web.json_response({"error": str(exc)}, status=400)
        except RuntimeError as exc:  # источник свечей недоступен: сообщение уже без секретов
            return web.json_response({"error": str(exc)}, status=502)
        return web.json_response(rows, headers={"Cache-Control": "no-store"})

    async def favorite(request: web.Request) -> web.Response:
        entry = lookup(request.match_info["cid"])
        if entry is None:
            return web.json_response({"ok": False, "error": "ссылка устарела"}, status=404)
        try:
            add = bool((await request.json()).get("add"))
        except (ValueError, json.JSONDecodeError):
            return web.json_response({"ok": False, "error": "bad request"}, status=400)
        client = ApiClient(settings)
        try:
            if add:
                await client.favorite_add(entry.trade_id, settings.profile, None)  # картинку эталона рисует настольный клиент; здесь - данные
            else:
                await client.favorite_remove(entry.trade_id)
        except ApiError as exc:
            return web.json_response({"ok": False, "error": str(exc)})
        finally:
            await client.close()
        entry.is_favorite = add
        return web.json_response({"ok": True, "is_favorite": add})

    app = web.Application()
    app.add_routes([web.get("/chart/{cid}", page), web.get("/chart/{cid}/klines", klines), web.post("/chart/{cid}/favorite", favorite)])
    return app


_runner: web.AppRunner | None = None


async def ensure_server(settings: ClientSettings, port: int = CHART_PORT, host: str = "0.0.0.0") -> None:
    """Поднять aiohttp-сервер графиков в этом же цикле событий (один раз на процесс). Контейнерная сеть: наружу публикует только nginx."""
    global _runner
    if _runner is not None:
        return
    runner = web.AppRunner(make_app(settings))
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    _runner = runner
    log.info("Сервер графиков: %s:%s", host, port)
