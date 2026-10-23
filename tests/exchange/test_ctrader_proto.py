"""Кодек Protobuf cTrader (exchange/ctrader_proto.py): известные байтовые векторы, обход всех типов полей, устойчивость к неизвестным полям, схема против .proto Spotware."""

import struct

import pytest

from exchange import ctrader_proto as P
from utils.errors import DataError


def test_known_varint_vectors_match_the_protobuf_spec():
    assert P._varint(1) == b"\x01" and P._varint(150) == b"\x96\x01" and P._varint(300) == b"\xac\x02"
    assert P._varint(-1) == b"\xff" * 9 + b"\x01"  # отрицательные int64 - 10 байт, дополнение до 2^64
    assert P._read_varint(b"\x96\x01", 0) == (150, 2)


def test_encode_matches_hand_assembled_bytes():
    # ProtoOAApplicationAuthReq {clientId="a" (поле 2), clientSecret="b" (поле 3)} -> 0x12 0x01 'a' 0x1a 0x01 'b'
    assert P.encode("ApplicationAuthReq", {"clientId": "a", "clientSecret": "b"}) == b"\x12\x01a\x1a\x01b"
    # int64 поле 2 = 7654321 (varint), double поле 4 = 1.5 (тип провода 1)
    body = P.encode("AmendPositionSLTPReq", {"ctidTraderAccountId": 7654321, "stopLoss": 1.5})
    assert body == b"\x10" + P._varint(7654321) + b"\x21" + struct.pack("<d", 1.5)


def test_roundtrip_nested_repeated_negative_double_bool_and_bytes():
    msg = {"ctidTraderAccountId": 7654321, "position": [{"positionId": 7, "tradeData": {"symbolId": 1, "volume": 1_000_000, "tradeSide": 2}, "positionStatus": 1, "swap": -5,
                                                         "price": 1.10005, "stopLoss": 1.0981, "usedMargin": 123456, "moneyDigits": 2}],
           "order": [{"orderId": 9, "tradeData": {"symbolId": 2, "volume": 100, "tradeSide": 1}, "orderType": 2, "orderStatus": 1, "limitPrice": 2000.5, "clientOrderId": "tb_x"}]}
    assert P.decode("ReconcileRes", P.encode("ReconcileRes", msg)) == msg
    assert P.decode("SubscribeSpotsReq", P.encode("SubscribeSpotsReq", {"ctidTraderAccountId": 1, "symbolId": [3, 4, 5]})) == {"ctidTraderAccountId": 1, "symbolId": [3, 4, 5]}
    assert P.decode("SymbolsListReq", P.encode("SymbolsListReq", {"ctidTraderAccountId": 1, "includeArchivedSymbols": True}))["includeArchivedSymbols"] is True
    framed = P.decode("ProtoMessage", P.encode("ProtoMessage", {"payloadType": 2100, "payload": b"\x00\x01\xff", "clientMsgId": "abc"}))
    assert framed == {"payloadType": 2100, "payload": b"\x00\x01\xff", "clientMsgId": "abc"}


def test_unknown_fields_are_skipped_and_packed_repeated_is_accepted():
    raw = P.encode("SubscribeSpotsRes", {"ctidTraderAccountId": 9}) + P._varint(99 << 3) + P._varint(7) + P._varint(98 << 3 | 2) + P._varint(2) + b"hi"
    assert P.decode("SubscribeSpotsRes", raw) == {"ctidTraderAccountId": 9}
    packed = P._varint(3 << 3 | 2) + P._varint(3) + b"\x01\x02\x03"
    assert P.decode("SubscribeSpotsReq", packed) == {"symbolId": [1, 2, 3]}


def test_frame_roundtrip_and_unknown_payload_type_is_tolerated():
    frame = P.build("GetTrendbarsReq", {"ctidTraderAccountId": 1, "period": P.PERIOD["5m"], "symbolId": 2, "count": 10}, "id1")
    assert int.from_bytes(frame[:4], "big") == len(frame) - 4
    name, msg, mid = P.parse(frame[4:])
    assert (name, mid) == ("GetTrendbarsReq", "id1") and msg == {"ctidTraderAccountId": 1, "period": 5, "symbolId": 2, "count": 10}
    unknown = P.encode("ProtoMessage", {"payloadType": 2141, "payload": b"\x08\x01", "clientMsgId": None})
    assert P.parse(unknown) == ("?2141", {}, None)  # событие, которого нет в схеме (маржа и т.п.), не роняет читающий цикл


@pytest.mark.parametrize("bad", [{"nope": 1}, {"clientId": 5}, {"clientId": b"x"}])
def test_invalid_values_are_rejected(bad):
    with pytest.raises(DataError):
        P.encode("ApplicationAuthReq", bad)


def test_truncated_input_raises():
    for raw in (b"\x08", b"\x12\x05ab", b"\x80"):
        with pytest.raises(DataError):
            P.decode("ApplicationAuthReq", raw)


def test_payload_types_and_enums_match_spotware_numbers():
    # номера сняты с OpenApiModelMessages.proto (2026-10-23)
    assert (P.PT["ApplicationAuthReq"], P.PT["AccountAuthReq"], P.PT["NewOrderReq"], P.PT["ExecutionEvent"], P.PT["SpotEvent"], P.PT["GetTrendbarsReq"],
            P.PT["ErrorRes"], P.PT["ReconcileReq"], P.PT["GetPositionUnrealizedPnLReq"]) == (2100, 2102, 2106, 2126, 2131, 2137, 2142, 2124, 2187)
    assert P.PERIOD["5m"] == 5 and P.PERIOD["1h"] == 9 and P.PERIOD["1d"] == 12
    assert (P.ORDER_MARKET, P.ORDER_LIMIT, P.SIDE_BUY, P.SIDE_SELL, P.EXEC_FILLED, P.EXEC_REPLACED) == (1, 2, 1, 2, 3, 4)
    assert P.PERIOD_MAX_WINDOW_MS[P.PERIOD["5m"]] == 302_400_000  # 84 часа
