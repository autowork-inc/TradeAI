"""Тесты чистой логики автодеплоя (без SSH). Пропускаются, если paramiko не установлен."""
import sys

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


def test_parse_health_and_step9_scans_every_required_service():
    out = "postgres running healthy\nshadow-live running\nanalyzer running unhealthy\n"
    assert ad.parse_services(out)["analyzer"] == "running" and ad.parse_health(out) == {"postgres": "healthy", "analyzer": "unhealthy"}
    src = open(ad.__file__, encoding="utf-8").read()
    assert "for s_ in required:" in src and "--profile analytics logs" in src and "unhealthy" in src  # логи всех обязательных сервисов + health
    assert "analyzer" in ad.REQUIRED_SERVICES and "analyzer" in ad.LIVE_REQUIRED_SERVICES


def test_client_build_command_is_the_publishing_powershell_script():
    assert ad.CLIENT_BUILD_CMD == ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", r".\client\build_exe.ps1", "-Publish"]


def test_build_client_waits_for_the_process_and_passes_on_success(tmp_path, capsys):
    ad.build_client([sys.executable, "-c", "import sys; sys.exit(0)"], tmp_path)
    assert "клиент собран" in capsys.readouterr().out


def test_build_client_failure_aborts_the_deploy_with_the_exit_code(tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        ad.build_client([sys.executable, "-c", "import sys; sys.exit(3)"], tmp_path)
    assert e.value.code == 1 and "кодом 3" in capsys.readouterr().err


def test_build_client_without_powershell_aborts(tmp_path, capsys):
    with pytest.raises(SystemExit):
        ad.build_client(["definitely-not-a-real-executable-xyz"], tmp_path)
    assert "--skip-client-build" in capsys.readouterr().err


def test_client_build_runs_before_any_ssh_connection(monkeypatch):
    """Порядок в main: сборка клиента -> архив -> SSH. Упавшая сборка не должна дойти до сборки архива и подключения."""
    calls = []
    monkeypatch.setattr(ad, "get_params", lambda argv=None: __import__("argparse").Namespace(remote_dir="trading", skip_client_build=False, host="h", user="u", port=22,
                                                                                            key=None, password="x", metaapi={}, health_wait=0, keep_archive=True))
    monkeypatch.setattr(ad, "build_client", lambda *a, **k: (calls.append("client"), ad.fail("сборка упала"))[1])
    monkeypatch.setattr(ad, "build_archive", lambda *a, **k: calls.append("archive"))
    monkeypatch.setattr(ad, "connect", lambda a: calls.append("ssh"))
    with pytest.raises(SystemExit):
        ad.main([])
    assert calls == ["client"]


def test_web_credentials_are_validated_together_and_never_echoed(capsys):
    assert ad.validate_web(None, None) == {}
    assert ad.validate_web("Santa", "Cheshir") == {"WEB_LOGIN": "Santa", "WEB_PASSWORD": "Cheshir"}
    for bad in (("Santa", None), (None, "Cheshir"), ("Santa", "pa ss"), ("Santa", 'p"w1'), ("Sa nta", "Cheshir"), ("Santa", "abc")):
        with pytest.raises(SystemExit):
            ad.validate_web(*bad)
    assert "Cheshir" not in capsys.readouterr().err
    assert "WEB_PASSWORD" in ad.METAAPI_KEYS and "WEB_LOGIN" in ad.merge_env_command("~/trading")  # вписываются в .env.shadow тем же безопасным способом
    assert "web" in ad.REQUIRED_SERVICES and "nginx" in ad.LIVE_REQUIRED_SERVICES
