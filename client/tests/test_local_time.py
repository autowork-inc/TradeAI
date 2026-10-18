"""Время в часовом поясе пользователя (2026-10-18): API отдаёт UTC, клиент показывает время ПК - в таблицах, в боковой панели графика, на оси
графика и в текстах «Причины входа». Тесты не зависят от пояса машины: ожидания считаются через тот же пояс ОС (`astimezone()`)."""

from datetime import datetime, timezone

import pytest

from tradeai_client import fmt
from tradeai_client.chart_spec import build_chart_spec
from tradeai_client.chart_window import _ts

MS = 1_790_000_000_000


def local(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()


def test_offset_and_label_follow_the_os_time_zone():
    off = local(MS).utcoffset().total_seconds() * 1000
    assert fmt.local_offset_ms(MS) == off
    label = fmt.tz_label(MS)
    assert label.startswith("UTC") and (label == "UTC") == (off == 0)


def test_tz_label_formats_half_hours_and_negative_offsets(monkeypatch):
    for off_min, text in ((180, "UTC+3"), (-270, "UTC-4:30"), (330, "UTC+5:30"), (0, "UTC")):
        monkeypatch.setattr(fmt, "local_offset_ms", lambda ms, o=off_min: o * 60_000)
        assert fmt.tz_label(MS) == text


def test_ms_and_iso_are_shown_in_local_time():
    assert fmt.fmt_ms_local(MS) == local(MS).strftime("%d.%m %H:%M") and fmt.fmt_ms_local(None) == fmt.DASH
    iso = datetime.fromtimestamp(MS / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assert fmt.fmt_local(iso) == fmt.fmt_ms_local(MS)


def test_utc_stamps_in_server_texts_are_localized_and_session_hours_are_not():
    stamp = datetime.fromtimestamp(MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out = fmt.localize_utc_text(f"Свип в {stamp}. Диапазон Азии (00:00-07:00 UTC).")
    assert stamp not in out and f"{local(MS).strftime('%d.%m.%Y %H:%M')} {fmt.tz_label(MS)}" in out
    assert out.endswith("Диапазон Азии (00:00-07:00 UTC).")


def test_chart_axis_is_shifted_by_the_local_offset():
    assert _ts(MS, 3 * 3_600_000).timestamp() == MS / 1000 + 3 * 3600
    assert _ts(MS).timestamp() == MS / 1000


def test_chart_spec_carries_one_offset_and_local_panel_times(fx):
    snap = fx("snapshot_smc_short.json")
    sig = fmt.to_ms(snap["trade"]["signal_time"])
    spec = build_chart_spec(snap, None, now_ms=sig + 300 * 900_000)
    assert spec["tz_offset_ms"] == fmt.local_offset_ms(sig) and spec["tz_label"] == fmt.tz_label(sig)
    signal_event = next(e for e in spec["panel"]["timeline"] if e["text"] == "Сигнал стратегии")
    assert signal_event["time"] == f"{fmt.fmt_ms_local(sig)} {fmt.tz_label(sig)}"


@pytest.mark.parametrize("reason, text", [("impulse_bar", "Импульсная свеча"), ("closed_offline", "Закрыта без бота (сверка)")])
def test_new_exit_reasons_have_russian_labels(reason, text):
    assert fmt.EXIT_RU[reason] == text
