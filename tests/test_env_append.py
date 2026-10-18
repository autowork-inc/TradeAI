"""deploy/shadow.sh: env_append не склеивает новую переменную с последней строкой .env.shadow без перевода строки (инцидент 2026-10-18)."""

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SH = shutil.which("sh")


def _function_text() -> str:
    text = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8").replace("\r\n", "\n")
    return text[text.index("env_append() {"):text.index("ensure_password() {")]


@pytest.mark.skipif(SH is None, reason="нужен sh")
def test_env_append_never_glues_a_new_variable_to_a_line_without_trailing_newline(tmp_path):
    """`APPROVED_STRATEGIES=...VWAP_ReversionANALYZER_DB_PASSWORD=...` уронил shadow-live: файл не кончался переводом строки, а дописывал printf >>."""
    env = tmp_path / ".env.shadow"
    env.write_bytes(b"APPROVED_STRATEGIES=SMC_Pro_Trend,VWAP_Reversion")  # без перевода строки в конце
    script = tmp_path / "t.sh"
    script.write_bytes((f'ENV_FILE="{env.as_posix()}"\n{_function_text()}\nenv_append "ANALYZER_DB_PASSWORD=abc"\nenv_append "API_KEY=k"\n').encode())
    subprocess.run([SH, script.as_posix()], check=True, timeout=30)
    assert env.read_bytes().decode().split("\n") == ["APPROVED_STRATEGIES=SMC_Pro_Trend,VWAP_Reversion", "ANALYZER_DB_PASSWORD=abc", "API_KEY=k", ""]


@pytest.mark.skipif(SH is None, reason="нужен sh")
def test_env_append_creates_missing_file_and_keeps_normal_files_intact(tmp_path):
    env = tmp_path / ".env.shadow"
    script = tmp_path / "t.sh"
    script.write_bytes((f'ENV_FILE="{env.as_posix()}"\n{_function_text()}\nenv_append "A=1"\nenv_append "B=2"\n').encode())
    subprocess.run([SH, script.as_posix()], check=True, timeout=30)
    assert env.read_bytes() == b"A=1\nB=2\n"


def test_all_env_writes_in_shadow_sh_go_through_env_append():
    text = (ROOT / "deploy" / "shadow.sh").read_text(encoding="utf-8")
    assert '>> "$ENV_FILE"' in text and text.count('>> "$ENV_FILE"') == 2  # внутри env_append: перевод строки и сама строка
