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


def test_metaapi_args_validated_and_optional(monkeypatch):
    monkeypatch.delenv("METAAPI_TOKEN", raising=False)
    monkeypatch.delenv("METAAPI_ACCOUNT_ID", raising=False)
    assert ad.get_params(["--host", "h", "--key", "k"]).metaapi == {}
    a = ad.get_params(["--host", "h", "--key", "k", "--metaapi-token", "tok.en_123", "--metaapi-account", "abcd1234-ef56"])
    assert a.metaapi == {"METAAPI_TOKEN": "tok.en_123", "METAAPI_ACCOUNT_ID": "abcd1234-ef56"}


@pytest.mark.parametrize("args", [
    ["--metaapi-token", "tok.en_123"],  # без id счёта
    ["--metaapi-account", "abcd1234-ef56"],  # без токена
    ["--metaapi-token", "to ken", "--metaapi-account", "abcd1234-ef56"],
    ["--metaapi-token", "to'ken", "--metaapi-account", "abcd1234-ef56"],
    ["--metaapi-token", "token", "--metaapi-account", "bad id;rm"]])
def test_metaapi_args_rejected(monkeypatch, args):
    monkeypatch.delenv("METAAPI_TOKEN", raising=False)
    monkeypatch.delenv("METAAPI_ACCOUNT_ID", raising=False)
    with pytest.raises(SystemExit):
        ad.get_params(["--host", "h", "--key", "k", *args])


def test_metaapi_secret_not_in_command_or_error(capsys):
    cmd = ad.merge_env_command("~/trading")
    assert "METAAPI_TOKEN" in cmd and "chmod 600" in cmd and "rm -f" in cmd
    with pytest.raises(SystemExit):
        ad.validate_metaapi("sekret value", "abcd1234-ef56")
    assert "sekret" not in capsys.readouterr().err
