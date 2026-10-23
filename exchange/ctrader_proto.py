"""Минимальный кодек Protobuf (proto2) для cTrader Open API: без Twisted, без сгенерированных `*_pb2` и без привязки к версии пакета `protobuf`.

Почему свой. Официальный Python-пакет `ctrader-open-api` построен на Twisted (второй цикл событий рядом с asyncio проекта), а его `_pb2` привязаны к
конкретной версии protobuf. Нам нужны два десятка сообщений и четыре типа полей, поэтому кодек - это таблица схем ниже + 60 строк разбора. Схемы СНЯТЫ с
официальных файлов Spotware (github.com/spotware/openapi-proto-messages: OpenApiCommonMessages / OpenApiMessages / OpenApiModelMessages, 2026-10-23): номера
полей и payloadType не придуманы. Неизвестные поля при разборе пропускаются (сервер вправе добавлять новые).

Кадр TCP (порт 5035, TLS): 4 байта длины big-endian + `ProtoMessage` {payloadType=1, payload=2 (bytes), clientMsgId=3}.
Цены в событиях и трендбарах - целые, масштаб 1/100000; деньги - целые в `moneyDigits` знаках; объём - в сотых долях единицы (volume 1000 = 10.00 единиц).
"""

import struct
from collections.abc import Mapping
from typing import Any

from utils.errors import DataError

# --------------------------------------------------------------------------------------------- payloadType
PT = {
    "ProtoMessage": 5, "ProtoErrorRes": 50, "ProtoHeartbeatEvent": 51,
    "ApplicationAuthReq": 2100, "ApplicationAuthRes": 2101, "AccountAuthReq": 2102, "AccountAuthRes": 2103,
    "NewOrderReq": 2106, "CancelOrderReq": 2108, "AmendPositionSLTPReq": 2110, "ClosePositionReq": 2111,
    "AssetListReq": 2112, "AssetListRes": 2113, "SymbolsListReq": 2114, "SymbolsListRes": 2115, "SymbolByIdReq": 2116, "SymbolByIdRes": 2117,
    "TraderReq": 2121, "TraderRes": 2122, "ReconcileReq": 2124, "ReconcileRes": 2125, "ExecutionEvent": 2126,
    "SubscribeSpotsReq": 2127, "SubscribeSpotsRes": 2128, "UnsubscribeSpotsReq": 2129, "UnsubscribeSpotsRes": 2130, "SpotEvent": 2131,
    "OrderErrorEvent": 2132, "DealListReq": 2133, "DealListRes": 2134, "GetTrendbarsReq": 2137, "GetTrendbarsRes": 2138, "ErrorRes": 2142,
    "AccountsTokenInvalidatedEvent": 2147, "ClientDisconnectEvent": 2148, "GetAccountListByAccessTokenReq": 2149, "GetAccountListByAccessTokenRes": 2150,
    "AccountDisconnectEvent": 2164, "RefreshTokenReq": 2173, "RefreshTokenRes": 2174, "GetPositionUnrealizedPnLReq": 2187, "GetPositionUnrealizedPnLRes": 2188,
}
NAME_OF = {v: k for k, v in PT.items()}

# перечисления (числа из .proto)
PERIOD = {"1m": 1, "2m": 2, "3m": 3, "4m": 4, "5m": 5, "10m": 6, "15m": 7, "30m": 8, "1h": 9, "4h": 10, "12h": 11, "1d": 12, "1w": 13}
PERIOD_MS = {"1m": 60_000, "2m": 120_000, "3m": 180_000, "4m": 240_000, "5m": 300_000, "10m": 600_000, "15m": 900_000, "30m": 1_800_000,
             "1h": 3_600_000, "4h": 14_400_000, "12h": 43_200_000, "1d": 86_400_000, "1w": 604_800_000}
# максимальное окно [from, to] одного запроса трендбаров (по документации Open API); шире - сервер отвечает ошибкой INCORRECT_BOUNDARIES
PERIOD_MAX_WINDOW_MS = {1: 302_400_000, 2: 302_400_000, 3: 302_400_000, 4: 302_400_000, 5: 302_400_000,
                        6: 21_168_000_000, 7: 21_168_000_000, 8: 21_168_000_000, 9: 21_168_000_000,
                        10: 31_622_400_000, 11: 31_622_400_000, 12: 126_230_400_000, 13: 126_230_400_000}
ORDER_MARKET, ORDER_LIMIT, ORDER_STOP = 1, 2, 3
SIDE_BUY, SIDE_SELL = 1, 2
TIF_GTD, TIF_GTC = 1, 2
EXEC_ACCEPTED, EXEC_FILLED, EXEC_REPLACED, EXEC_CANCELLED, EXEC_EXPIRED, EXEC_REJECTED, EXEC_CANCEL_REJECTED, EXEC_PARTIAL_FILL = 2, 3, 4, 5, 6, 7, 8, 11
POS_OPEN, POS_CLOSED = 1, 2
TRADING_ENABLED, TRADING_CLOSE_ONLY = 0, 3
PRICE_SCALE = 100_000

# --------------------------------------------------------------------------------------------- схемы
# поле: (номер, вид[, повторяющееся]); вид: i - знаковое целое (int32/int64/enum), u - беззнаковое, b - bool, d - double, s - string, y - bytes, "m:Имя" - вложенное
_R = True
SCHEMA: dict[str, dict[str, tuple]] = {
    "ProtoMessage": {"payloadType": (1, "u"), "payload": (2, "y"), "clientMsgId": (3, "s")},
    "ProtoErrorRes": {"payloadType": (1, "u"), "errorCode": (2, "s"), "description": (3, "s"), "maintenanceEndTimestamp": (4, "u")},
    "ProtoHeartbeatEvent": {"payloadType": (1, "u")},
    "ApplicationAuthReq": {"clientId": (2, "s"), "clientSecret": (3, "s")},
    "ApplicationAuthRes": {},
    "AccountAuthReq": {"ctidTraderAccountId": (2, "i"), "accessToken": (3, "s")},
    "AccountAuthRes": {"ctidTraderAccountId": (2, "i")},
    "ErrorRes": {"ctidTraderAccountId": (2, "i"), "errorCode": (3, "s"), "description": (4, "s"), "retryAfter": (6, "u")},
    "GetAccountListByAccessTokenReq": {"accessToken": (2, "s")},
    "GetAccountListByAccessTokenRes": {"accessToken": (2, "s"), "permissionScope": (3, "i"), "ctidTraderAccount": (4, "m:CtidTraderAccount", _R)},
    "CtidTraderAccount": {"ctidTraderAccountId": (1, "u"), "isLive": (2, "b"), "traderLogin": (3, "i"), "brokerTitleShort": (6, "s")},
    "RefreshTokenReq": {"refreshToken": (2, "s")},
    "RefreshTokenRes": {"accessToken": (2, "s"), "tokenType": (3, "s"), "expiresIn": (4, "i"), "refreshToken": (5, "s")},
    "AccountsTokenInvalidatedEvent": {"ctidTraderAccountIds": (2, "i", _R), "reason": (3, "s")},
    "ClientDisconnectEvent": {"reason": (2, "s")},
    "AccountDisconnectEvent": {"ctidTraderAccountId": (2, "i")},
    "SymbolsListReq": {"ctidTraderAccountId": (2, "i"), "includeArchivedSymbols": (3, "b")},
    "SymbolsListRes": {"ctidTraderAccountId": (2, "i"), "symbol": (3, "m:LightSymbol", _R)},
    "LightSymbol": {"symbolId": (1, "i"), "symbolName": (2, "s"), "enabled": (3, "b"), "baseAssetId": (4, "i"), "quoteAssetId": (5, "i"),
                    "symbolCategoryId": (6, "i"), "description": (7, "s")},
    "SymbolByIdReq": {"ctidTraderAccountId": (2, "i"), "symbolId": (3, "i", _R)},
    "SymbolByIdRes": {"ctidTraderAccountId": (2, "i"), "symbol": (3, "m:Symbol", _R)},
    "Symbol": {"symbolId": (1, "i"), "digits": (2, "i"), "pipPosition": (3, "i"), "enableShortSelling": (4, "b"), "maxVolume": (9, "i"),
               "minVolume": (10, "i"), "stepVolume": (11, "i"), "slDistance": (16, "u"), "tpDistance": (17, "u"), "distanceSetIn": (20, "i"),
               "tradingMode": (27, "i"), "lotSize": (30, "i")},
    "AssetListReq": {"ctidTraderAccountId": (2, "i")},
    "AssetListRes": {"ctidTraderAccountId": (2, "i"), "asset": (3, "m:Asset", _R)},
    "Asset": {"assetId": (1, "i"), "name": (2, "s"), "displayName": (3, "s"), "digits": (4, "i")},
    "TraderReq": {"ctidTraderAccountId": (2, "i")},
    "TraderRes": {"ctidTraderAccountId": (2, "i"), "trader": (3, "m:Trader")},
    "Trader": {"ctidTraderAccountId": (1, "i"), "balance": (2, "i"), "accessRights": (7, "i"), "depositAssetId": (8, "i"), "leverageInCents": (10, "u"),
               "traderLogin": (14, "i"), "accountType": (15, "i"), "brokerName": (16, "s"), "moneyDigits": (20, "u")},
    "SubscribeSpotsReq": {"ctidTraderAccountId": (2, "i"), "symbolId": (3, "i", _R)},
    "SubscribeSpotsRes": {"ctidTraderAccountId": (2, "i")},
    "UnsubscribeSpotsReq": {"ctidTraderAccountId": (2, "i"), "symbolId": (3, "i", _R)},
    "SpotEvent": {"ctidTraderAccountId": (2, "i"), "symbolId": (3, "i"), "bid": (4, "u"), "ask": (5, "u"), "trendbar": (6, "m:Trendbar", _R), "timestamp": (8, "i")},
    "GetTrendbarsReq": {"ctidTraderAccountId": (2, "i"), "fromTimestamp": (3, "i"), "toTimestamp": (4, "i"), "period": (5, "i"), "symbolId": (6, "i"),
                        "count": (7, "u")},
    "GetTrendbarsRes": {"ctidTraderAccountId": (2, "i"), "period": (3, "i"), "trendbar": (5, "m:Trendbar", _R), "symbolId": (6, "i"), "hasMore": (7, "b")},
    "Trendbar": {"volume": (3, "i"), "period": (4, "i"), "low": (5, "i"), "deltaOpen": (6, "u"), "deltaClose": (7, "u"), "deltaHigh": (8, "u"),
                 "utcTimestampInMinutes": (9, "u")},
    "NewOrderReq": {"ctidTraderAccountId": (2, "i"), "symbolId": (3, "i"), "orderType": (4, "i"), "tradeSide": (5, "i"), "volume": (6, "i"),
                    "limitPrice": (7, "d"), "stopPrice": (8, "d"), "timeInForce": (9, "i"), "expirationTimestamp": (10, "i"), "stopLoss": (11, "d"),
                    "takeProfit": (12, "d"), "comment": (13, "s"), "label": (16, "s"), "positionId": (17, "i"), "clientOrderId": (18, "s"),
                    "relativeStopLoss": (19, "i"), "relativeTakeProfit": (20, "i")},
    "CancelOrderReq": {"ctidTraderAccountId": (2, "i"), "orderId": (3, "i")},
    "AmendPositionSLTPReq": {"ctidTraderAccountId": (2, "i"), "positionId": (3, "i"), "stopLoss": (4, "d"), "takeProfit": (5, "d")},
    "ClosePositionReq": {"ctidTraderAccountId": (2, "i"), "positionId": (3, "i"), "volume": (4, "i")},
    "ReconcileReq": {"ctidTraderAccountId": (2, "i")},
    "ReconcileRes": {"ctidTraderAccountId": (2, "i"), "position": (3, "m:Position", _R), "order": (4, "m:Order", _R)},
    "Position": {"positionId": (1, "i"), "tradeData": (2, "m:TradeData"), "positionStatus": (3, "i"), "swap": (4, "i"), "price": (5, "d"), "stopLoss": (6, "d"),
                 "takeProfit": (7, "d"), "utcLastUpdateTimestamp": (8, "i"), "commission": (9, "i"), "usedMargin": (13, "u"), "moneyDigits": (15, "u")},
    "TradeData": {"symbolId": (1, "i"), "volume": (2, "i"), "tradeSide": (3, "i"), "openTimestamp": (4, "i"), "label": (5, "s"), "comment": (7, "s"),
                  "closeTimestamp": (9, "u")},
    "Order": {"orderId": (1, "i"), "tradeData": (2, "m:TradeData"), "orderType": (3, "i"), "orderStatus": (4, "i"), "expirationTimestamp": (6, "i"),
              "executionPrice": (7, "d"), "executedVolume": (8, "i"), "utcLastUpdateTimestamp": (9, "i"), "limitPrice": (13, "d"), "stopPrice": (14, "d"),
              "stopLoss": (15, "d"), "takeProfit": (16, "d"), "clientOrderId": (17, "s"), "positionId": (19, "i")},
    "ExecutionEvent": {"ctidTraderAccountId": (2, "i"), "executionType": (3, "i"), "position": (4, "m:Position"), "order": (5, "m:Order"), "deal": (6, "m:Deal"),
                       "errorCode": (9, "s"), "isServerEvent": (10, "b")},
    "Deal": {"dealId": (1, "i"), "orderId": (2, "i"), "positionId": (3, "i"), "volume": (4, "i"), "filledVolume": (5, "i"), "symbolId": (6, "i"),
             "createTimestamp": (7, "i"), "executionTimestamp": (8, "i"), "executionPrice": (10, "d"), "tradeSide": (11, "i"), "dealStatus": (12, "i"),
             "commission": (14, "i"), "closePositionDetail": (16, "m:ClosePositionDetail"), "moneyDigits": (17, "u")},
    "ClosePositionDetail": {"entryPrice": (1, "d"), "grossProfit": (2, "i"), "swap": (3, "i"), "commission": (4, "i"), "balance": (5, "i"),
                            "closedVolume": (7, "i"), "moneyDigits": (9, "u")},
    "OrderErrorEvent": {"errorCode": (2, "s"), "orderId": (3, "i"), "ctidTraderAccountId": (5, "i"), "positionId": (6, "i"), "description": (7, "s")},
    "DealListReq": {"ctidTraderAccountId": (2, "i"), "fromTimestamp": (3, "i"), "toTimestamp": (4, "i"), "maxRows": (5, "i")},
    "DealListRes": {"ctidTraderAccountId": (2, "i"), "deal": (3, "m:Deal", _R), "hasMore": (4, "b")},
    "GetPositionUnrealizedPnLReq": {"ctidTraderAccountId": (2, "i")},
    "GetPositionUnrealizedPnLRes": {"ctidTraderAccountId": (2, "i"), "positionUnrealizedPnL": (3, "m:PositionUnrealizedPnL", _R), "moneyDigits": (4, "u")},
    "PositionUnrealizedPnL": {"positionId": (1, "i"), "grossUnrealizedPnL": (2, "i"), "netUnrealizedPnL": (3, "i")},
}
_BY_NUMBER = {name: {spec[0]: (field, spec[1], len(spec) > 2 and spec[2]) for field, spec in fields.items()} for name, fields in SCHEMA.items()}

_MASK64 = (1 << 64) - 1
_VARINT_KINDS = ("i", "u", "b")


def _varint(value: int) -> bytes:
    value &= _MASK64  # отрицательные int32/int64 кодируются 64-битным дополнением (10 байт)
    out = bytearray()
    while True:
        low = value & 0x7F
        value >>= 7
        if value:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        if pos >= len(data):
            raise DataError("protobuf: оборванный varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 70:
            raise DataError("protobuf: слишком длинный varint")


def _signed(value: int) -> int:
    return value - (1 << 64) if value >= 1 << 63 else value


def encode(name: str, values: Mapping[str, Any]) -> bytes:
    """Словарь -> байты сообщения `name`. Поля со значением None пропускаются; неизвестное имя поля или неверный тип - `DataError`."""
    fields = SCHEMA[name]
    unknown = set(values) - set(fields)
    if unknown:
        raise DataError(f"protobuf {name}: нет полей {sorted(unknown)}")
    out = bytearray()
    for field, spec in sorted(fields.items(), key=lambda kv: kv[1][0]):
        value = values.get(field)
        if value is None:
            continue
        number, kind = spec[0], spec[1]
        for item in (value if len(spec) > 2 and spec[2] else [value]):
            out += _encode_field(name, field, number, kind, item)
    return bytes(out)


def _encode_field(msg: str, field: str, number: int, kind: str, value: Any) -> bytes:
    if kind in _VARINT_KINDS:
        if isinstance(value, bool):
            value = int(value)
        elif kind == "b" or isinstance(value, float) or not isinstance(value, int):
            raise DataError(f"protobuf {msg}.{field}: ожидается целое/bool, получено {value!r}")
        return _varint(number << 3) + _varint(value)
    if kind == "d":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DataError(f"protobuf {msg}.{field}: ожидается число, получено {value!r}")
        return _varint(number << 3 | 1) + struct.pack("<d", float(value))
    if kind == "s":
        if not isinstance(value, str):
            raise DataError(f"protobuf {msg}.{field}: ожидается строка, получено {value!r}")
        raw = value.encode("utf-8")
    elif kind == "y":
        raw = bytes(value)
    elif kind.startswith("m:"):
        raw = encode(kind[2:], value)
    else:
        raise DataError(f"protobuf {msg}.{field}: неизвестный вид {kind!r}")
    return _varint(number << 3 | 2) + _varint(len(raw)) + raw


def decode(name: str, data: bytes) -> dict[str, Any]:
    """Байты -> словарь полей сообщения `name` (повторяющиеся - списки, вложенные - словари; неизвестные поля пропускаются)."""
    by_number = _BY_NUMBER[name]
    out: dict[str, Any] = {}
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            raw, pos = _read_varint(data, pos)
            value: Any = raw
        elif wire == 1:
            raw_bytes, pos = data[pos:pos + 8], pos + 8
            value = raw_bytes
        elif wire == 2:
            size, pos = _read_varint(data, pos)
            if pos + size > len(data):
                raise DataError(f"protobuf {name}: поле длиннее сообщения")
            value, pos = data[pos:pos + size], pos + size
        elif wire == 5:
            value, pos = data[pos:pos + 4], pos + 4
        else:
            raise DataError(f"protobuf {name}: неподдерживаемый wire type {wire}")
        spec = by_number.get(number)
        if spec is None:
            continue
        field, kind, repeated = spec
        items = _decode_value(name, field, kind, wire, value)
        if repeated:
            out.setdefault(field, []).extend(items)
        else:
            out[field] = items[-1]
    return out


def _decode_value(msg: str, field: str, kind: str, wire: int, value: Any) -> list[Any]:
    if kind in _VARINT_KINDS:
        if wire == 0:
            return [bool(value) if kind == "b" else _signed(value) if kind == "i" else value]
        if wire == 2:  # packed repeated
            items, pos = [], 0
            while pos < len(value):
                raw, pos = _read_varint(value, pos)
                items.append(bool(raw) if kind == "b" else _signed(raw) if kind == "i" else raw)
            return items
    elif kind == "d" and wire == 1:
        return [struct.unpack("<d", value)[0]]
    elif kind == "s" and wire == 2:
        return [value.decode("utf-8")]
    elif kind == "y" and wire == 2:
        return [bytes(value)]
    elif kind.startswith("m:") and wire == 2:
        return [decode(kind[2:], value)]
    raise DataError(f"protobuf {msg}.{field}: тип провода {wire} не подходит полю вида {kind!r}")


def wrap(payload_type: int, payload: bytes, client_msg_id: str | None = None) -> bytes:
    """Кадр TCP: длина (4 байта, big-endian) + ProtoMessage."""
    body = encode("ProtoMessage", {"payloadType": payload_type, "payload": payload, "clientMsgId": client_msg_id})
    return struct.pack(">I", len(body)) + body


def build(name: str, values: Mapping[str, Any], client_msg_id: str | None = None) -> bytes:
    return wrap(PT[name], encode(name, values), client_msg_id)


def parse(frame_body: bytes) -> tuple[str, dict[str, Any], str | None]:
    """Тело кадра -> (имя сообщения, поля, clientMsgId). Неизвестный payloadType (сервер шлёт и события, которых мы не моделируем: изменение маржи,
    трейлинг и т.п.) -> имя `?<номер>` и пустые поля: вызывающий игнорирует его, а если это ответ на наш запрос (есть clientMsgId) - отказывает запросу."""
    outer = decode("ProtoMessage", frame_body)
    ptype = outer.get("payloadType")
    name = NAME_OF.get(ptype)
    if name is None:
        return f"?{ptype}", {}, outer.get("clientMsgId")
    return name, decode(name, outer.get("payload", b"")), outer.get("clientMsgId")
