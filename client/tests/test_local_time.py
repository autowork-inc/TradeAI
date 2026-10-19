"""Время в часовом поясе пользователя (2026-10-18): API отдаёт UTC, клиент показывает время ПК - в таблицах, в боковой панели графика, на оси
графика и в текстах «Причины входа». Тесты не зависят от пояса машины: ожидания считаются через тот же пояс ОС (`astimezone()`)."""

from datetime import datetime, timezone

import pytest

from tradeai_client import fmt
from tradeai_client.chart_spec import build_chart_spec

MS = 1_790_000_000_000


def local(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()


def test_offset_follows_the_os_time_zone():
    assert fmt.local_offset_ms(MS) == local(MS).utcoffset().total_seconds() * 1000


def test_ms_and_iso_are_shown_in_local_time():
    assert fmt.fmt_ms_local(MS) == local(MS).strftime("%d.%m %H:%M") and fmt.fmt_ms_local(None) == fmt.DASH
    iso = datetime.fromtimestamp(MS / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assert fmt.fmt_local(iso) == fmt.fmt_ms_local(MS)


def test_legacy_utc_stamps_in_server_texts_are_localized_without_a_label():
    stamp = datetime.fromtimestamp(MS / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out = fmt.localize_utc_text(f"Свип в {stamp}.")
    assert stamp not in out and local(MS).strftime("%d.%m.%Y %H:%M") in out and "UTC" not in out


def test_chart_axis_uses_the_pc_time_zone_in_the_page():
    """Ось и перекрестие графика форматируются в самой странице через Date (пояс ОС, с переходами на летнее время): данные остаются в UTC."""
    from tradeai_client.chart_window import WEB_DIR

    js = (WEB_DIR / "chart.js").read_text(encoding="utf-8")
    assert "getHours()" in js and "getDate()" in js and "tickMarkFormatter" in js and "timeFormatter" in js and "getUTCHours" not in js


def test_chart_spec_carries_one_offset_and_local_panel_times(fx):
    snap = fx("snapshot_smc_short.json")
    sig = fmt.to_ms(snap["trade"]["signal_time"])
    spec = build_chart_spec(snap, None, now_ms=sig + 300 * 900_000)
    assert spec["tz_offset_ms"] == fmt.local_offset_ms(sig) and "tz_label" not in spec
    signal_event = next(e for e in spec["panel"]["timeline"] if e["text"] == "Сигнал стратегии")
    assert signal_event["time"] == fmt.fmt_ms_local(sig)


@pytest.mark.parametrize("reason, text", [("impulse_bar", "Импульсная свеча"), ("closed_offline", "Закрыта без бота (сверка)"), ("manual_binance", "Закрыта вручную (Binance)")])
def test_new_exit_reasons_have_russian_labels(reason, text):
    assert fmt.EXIT_RU[reason] == text
