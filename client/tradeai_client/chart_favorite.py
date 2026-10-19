"""Звёздочка «Избранное» на графике сделки. Окно графика - отдельный процесс без доступа к Shell, поэтому его мост (`chart_window.ChartApi.toggle_favorite`)
сам отправляет запрос на сервер (тот же PUT/DELETE /api/favorites/{id}, что и из таблицы History): параметры (адрес, ключ, id сделки, профиль) приходят в `spec["favorite"]`.
Картинку для золотой базы строит Pillow (`golden_image.render_png`) из того же spec. Состояние звёздочки на сервере общее: другие вкладки клиента подхватят его при ближайшем обновлении."""

import base64
import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

TIMEOUT_S = 20


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
