"""Одобренные пользователем изменения стратегий (2026-10-23): параметры детектора и тейк, без правки кода стратегий.

Файл `orchestrator/strategy_overrides.json` (едет в образ вместе с кодом; путь можно сменить переменной STRATEGY_OVERRIDES_PATH) пишет только
`scripts/strategy_proposals.py apply/rollback` по решению пользователя. Формат:
    {"version": 1, "strategies": {"ICT_Killzones": {"params": {"require_htf_trend": false}, "tp_r": null,
                                                    "proposal_id": "...", "applied_at": "2026-10-23T21:00:00Z"}}}
  params - поля dataclass параметров стратегии (KillzoneParams, VwapParams...): применяются через dataclasses.replace, т.е. с той же проверкой
           (__post_init__), что и код; неизвестное поле или плохое значение - ConfigError на старте (R1), бот не стартует с непонятной настройкой;
  tp_r   - тейк в R (один тейк на весь объём, как DEFAULT_PLAN); null - план по умолчанию. Действует одинаково в Shadow и в боевом движке (R8: plans_for).
Нет файла или пустой - всё по умолчанию.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import fields, is_dataclass, replace
from pathlib import Path
from typing import Any, Sequence

from utils.errors import ConfigError

log = logging.getLogger("orchestrator.overrides")
DEFAULT_PATH = Path(__file__).resolve().parent / "strategy_overrides.json"
VERSION = 1
MAX_TP_R = 10.0


def path() -> Path:
    raw = os.environ.get("STRATEGY_OVERRIDES_PATH", "").strip()
    return Path(raw) if raw else DEFAULT_PATH


def load(p: Path | None = None) -> dict[str, dict[str, Any]]:
    """strategy_id -> {"params": {...}, "tp_r": float | None, ...}. Битый файл - ConfigError."""
    p = p or path()
    if not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{p}: не JSON ({exc})") from None
    if not isinstance(data, dict) or data.get("version") != VERSION or not isinstance(data.get("strategies"), dict):
        raise ConfigError(f"{p}: ожидается {{'version': {VERSION}, 'strategies': {{...}}}}")
    out: dict[str, dict[str, Any]] = {}
    for name, entry in data["strategies"].items():
        if not isinstance(entry, dict) or not isinstance(entry.get("params", {}), dict):
            raise ConfigError(f"{p}: {name}: запись должна быть объектом с полем params (объект)")
        tp_r = entry.get("tp_r")
        if tp_r is not None and (isinstance(tp_r, bool) or not isinstance(tp_r, (int, float)) or not 0 < tp_r <= MAX_TP_R):
            raise ConfigError(f"{p}: {name}: tp_r должен быть числом в (0, {MAX_TP_R}] или null, получено {tp_r!r}")
        out[name] = {**entry, "params": dict(entry.get("params", {})), "tp_r": None if tp_r is None else float(tp_r)}
    return out


def apply_params(params: Any, changes: dict[str, Any], strategy: str) -> Any:
    """dataclass параметров + изменения -> новый dataclass (с проверкой __post_init__)."""
    if not changes:
        return params
    if not is_dataclass(params):
        raise ConfigError(f"{strategy}: у стратегии нет dataclass параметров - переопределение params невозможно")
    known = {f.name for f in fields(params)}
    unknown = sorted(set(changes) - known)
    if unknown:
        raise ConfigError(f"{strategy}: неизвестные параметры {unknown}; есть: {sorted(known)}")
    for k, v in changes.items():
        cur = getattr(params, k)
        if isinstance(cur, bool) != isinstance(v, bool) or (isinstance(cur, (int, float)) and not isinstance(v, (int, float))):
            raise ConfigError(f"{strategy}.{k}: тип {type(v).__name__} не совпадает с {type(cur).__name__}")
        if isinstance(cur, int) and not isinstance(cur, bool) and isinstance(v, float) and not v.is_integer():
            raise ConfigError(f"{strategy}.{k}: нужно целое, получено {v!r}")
    coerced = {k: (int(v) if isinstance(getattr(params, k), int) and not isinstance(v, bool) else v) for k, v in changes.items()}
    return replace(params, **coerced)


def apply_to_strategies(strategies: Sequence[Any], table: dict[str, dict[str, Any]]) -> None:
    """Параметры из таблицы -> в построенные стратегии (по strategy.name). Запись про стратегию вне набора не ошибка (набор Shadow/Live разный)."""
    for s in strategies:
        entry = table.get(getattr(s, "name", ""))
        if entry and entry["params"]:
            s.params = apply_params(s.params, entry["params"], s.name)
            log.warning("Стратегия %s: применены одобренные параметры %s (предложение %s от %s)", s.name, entry["params"],
                        entry.get("proposal_id"), entry.get("applied_at"))
