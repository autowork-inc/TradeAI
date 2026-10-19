"""Звёздочка «Избранное» на графике сделки. Окно графика - отдельный процесс без доступа к Shell, поэтому оно само отправляет запрос на сервер
(тот же PUT/DELETE /api/favorites/{id}, что и из таблицы History): параметры (адрес, ключ, id сделки, профиль) приходят в `spec["favorite"]`.
Картинку для золотой базы окно строит само из своего spec (Pillow, `golden_image.render_png`), а не снимком окна: так кнопка не зависит от потоков WebView.
Состояние звёздочки на сервере общее: другие вкладки клиента подхватят его при ближайшем обновлении."""

import base64
import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

TIMEOUT_S = 20
LABEL_ON, LABEL_OFF = "★ В избранном", "☆ В избранное"


class FavoriteSendError(Exception):
    """Сервер не принял звёздочку (текст показывается в заголовке окна)."""


def send_favorite(fav: dict[str, Any], add: bool, png: bytes | None, opener=urllib.request.urlopen) -> None:
    url = f"{str(fav['url']).rstrip('/')}/api/favorites/{quote(str(fav['trade_id']), safe=':')}"
    body = None
    if add:
        payload: dict[str, Any] = {"profile": fav.get("profile", "SANTA")}
        if png:
            payload["image_png_b64"] = base64.b64encode(png).decode("ascii")
        body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="PUT" if add else "DELETE", headers={"X-API-Key": str(fav["key"]), "Content-Type": "application/json"})
    try:
        with opener(req, timeout=TIMEOUT_S):
            pass
    except urllib.error.HTTPError as exc:
        raise FavoriteSendError(f"сервер ответил HTTP {exc.code}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise FavoriteSendError("сервер недоступен") from exc


def attach_favorite_button(chart: Any, spec: dict[str, Any], send=send_favorite, render=None) -> None:
    """Кнопка в верхней панели графика. Нажатие: звёздочка ставится (с картинкой) или снимается; подпись кнопки меняется по результату."""
    fav = dict(spec["favorite"])
    state = {"on": bool(fav.get("is_favorite")), "busy": False}
    if render is None:
        from tradeai_client.golden_image import render_png as render

    def label() -> str:
        return LABEL_ON if state["on"] else LABEL_OFF

    def clicked(c: Any) -> None:
        if state["busy"]:
            return
        state["busy"] = True
        add = not state["on"]
        try:
            send(fav, add, render(spec) if add else None)
            state["on"] = add
        except (FavoriteSendError, ValueError, OSError) as exc:
            chart.topbar["fav"].set(f"⚠ {exc}")
            return
        finally:
            state["busy"] = False
        chart.topbar["fav"].set(label())

    chart.topbar.button("fav", label(), separator=False, func=clicked)
