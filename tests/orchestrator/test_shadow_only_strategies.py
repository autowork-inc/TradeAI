"""Horizontal_Breakout и Trendline_Breakout (2026-10-18), их CHR-версии (2026-10-19) - только Shadow: их нельзя одобрить к live ни конфигурацией, ни напрямую в менеджере
риска, а боевое «зеркало» не пересылает их сигналы даже при ошибочно одобряющем движке. Shadow-трекер при этом получает их как обычно."""

from types import SimpleNamespace

import pytest

import config
from orchestrator.live_engine import LiveMirrorSink
from orchestrator.multi_strategies import HORIZONTAL_BREAKOUT, HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT, TRENDLINE_BREAKOUT_CHR
from orchestrator.registry import SHADOW_ONLY_STRATEGIES, SHADOW_STRATEGIES, build_strategies, strategy_allowed
from orchestrator.tracker import Submission
from tests.orchestrator.live_helpers import STRATEGY, manager, signal
from utils.errors import ConfigError


def test_shadow_only_list_is_the_same_in_config_and_registry_and_is_part_of_the_shadow_set():
    assert SHADOW_ONLY_STRATEGIES == tuple(config.SHADOW_ONLY_STRATEGIES) == (HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT, HORIZONTAL_BREAKOUT_CHR,
                                                                                TRENDLINE_BREAKOUT_CHR)
    assert set(SHADOW_ONLY_STRATEGIES) <= set(SHADOW_STRATEGIES)
    assert not set(SHADOW_ONLY_STRATEGIES) & set(config.APPROVED_STRATEGIES)  # по умолчанию одобрена только SMC_Pro_Trend


def test_registry_refuses_to_build_when_config_and_registry_lists_diverge(monkeypatch):
    monkeypatch.setattr(config, "SHADOW_ONLY_STRATEGIES", (HORIZONTAL_BREAKOUT,))
    with pytest.raises(ConfigError, match="SHADOW_ONLY_STRATEGIES"):
        build_strategies()


@pytest.mark.parametrize("name", [HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT, HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR])
def test_config_validation_rejects_shadow_only_strategy_in_approved_list(monkeypatch, name):
    monkeypatch.setattr(config, "APPROVED_STRATEGIES", (STRATEGY, name))
    with pytest.raises(ConfigError, match="только в Shadow"):
        config.validate()


@pytest.mark.parametrize("name", [HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT, HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR])
def test_live_risk_manager_cannot_be_built_with_shadow_only_strategy(tmp_path, name):
    with pytest.raises(ConfigError, match="только в Shadow"):
        manager(tmp_path, approved=(STRATEGY, name))
    assert manager(tmp_path, approved=(STRATEGY,)).is_approved(STRATEGY)


class _Primary:
    def __init__(self):
        self.got = []

    def submit(self, s, now_ms):
        self.got.append(s.strategy)
        return Submission(True)


def test_live_mirror_never_forwards_shadow_only_signals_even_if_engine_would_approve():
    """Защита в глубину: даже если менеджер риска (по ошибке) одобряет всё, сигналы теневых стратегий в боевой движок не уходят."""
    enqueued = []
    engine = SimpleNamespace(risk=SimpleNamespace(is_approved=lambda name: True), enqueue=lambda s, now: enqueued.append(s.strategy))
    primary = _Primary()
    sink = LiveMirrorSink(primary, engine)  # type: ignore[arg-type]
    names = (HORIZONTAL_BREAKOUT, TRENDLINE_BREAKOUT, HORIZONTAL_BREAKOUT_CHR, TRENDLINE_BREAKOUT_CHR, STRATEGY)
    for name in names:
        assert sink.submit(signal(strategy=name), 0).accepted
    assert primary.got == list(names)  # Shadow-учёт получает все сигналы
    assert enqueued == [STRATEGY]


def test_shadow_only_strategies_run_on_every_shadow_symbol_including_test_pool():
    for s in config.SHADOW_SYMBOLS:
        assert strategy_allowed(HORIZONTAL_BREAKOUT, s) and strategy_allowed(TRENDLINE_BREAKOUT, s)
