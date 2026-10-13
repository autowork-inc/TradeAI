"""Тесты чистой логики автодеплоя (без SSH). Пропускаются, если paramiko не установлен."""
import pytest

pytest.importorskip("paramiko")

from scripts import auto_deploy as ad  # noqa: E402


def test_scan_logs_clean():
    assert ad.scan_logs("INFO started\nINFO pulse ok\n") == ([], [])


@pytest.mark.parametrize("line", [
    "websockets.exceptions.InvalidStatus: server rejected WebSocket connection: HTTP 451",
    "REST error HTTP 429 Too Many Requests",
    "status code 418",
    "Handshake failed",
    "Service unavailable from a restricted location"])
def test_scan_logs_detects_block(line):
    blocked, errors = ad.scan_logs("INFO ok\n" + line)
    assert blocked == [line] and errors == []


def test_scan_logs_errors_and_no_false_positive_on_numbers():
    blocked, errors = ad.scan_logs("ts=1700004510 price 42950.1\nTraceback (most recent call last):\nConfigError: x\nCRITICAL boom")
    assert blocked == [] and len(errors) == 3


def test_parse_services():
    assert ad.parse_services("postgres running\nshadow-run exited\n") == {"postgres": "running", "shadow-run": "exited"}


def test_remote_dir_validation():
    with pytest.raises(SystemExit):
        ad.get_params(["--host", "h", "--key", "k", "--remote-dir", "../x"])
    assert ad.get_params(["--host", "h", "--key", "k"]).remote_dir == "trading"
