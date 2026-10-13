"""TradeAI Risk Analyst - локальный MCP-сервер (stdio): аналитика торговой статистики Shadow Mode из PostgreSQL. ТОЛЬКО ЧТЕНИЕ.

Запуск (его делает клиент MCP - Cursor / Claude Desktop, см. README.md):
    mcp_server/.venv/Scripts/python.exe mcp_server/server.py

Протокол идёт по stdout, поэтому в stdout ничего не печатаем: логи - только в stderr.
Инструменты:
    get_strategies_overview(time_window_hours)   сводка по всем strategy_id
    get_trade_details(strategy_id, limit)        последние сделки со снапшотом и причиной входа
    analyze_drawdown_causes(strategy_id)         разбор убыточных сделок по HTF Bias, монете, стороне, часу, выходу
"""

import logging
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))  # `python mcp_server/server.py` из любой папки

from mcp.server.fastmcp import FastMCP  # noqa: E402

import analytics as A  # noqa: E402
import queries as Q  # noqa: E402
from db import Database, DbError  # noqa: E402

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tradeai-mcp")

TRADES_LIMIT_MAX = 100
ANALYSIS_ROWS_MAX = 20_000
STRATEGY_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")

mcp = FastMCP("tradeai-risk-analyst")
db = Database()


def _strategy_id(value: str) -> str:
    if not isinstance(value, str) or not STRATEGY_ID_RE.match(value):
        raise ValueError("strategy_id: латиница, цифры, `_`, `-`, `.`, до 64 символов (например SMC_Pro_Trend).")
    return value


def _int_in(name: str, value: int, lo: int, hi: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not lo <= value <= hi:
        raise ValueError(f"{name}: целое число от {lo} до {hi}, получено {value!r}.")
    return value


async def _unknown_strategy_message(strategy_id: str) -> str:
    known = await db.fetch(Q.KNOWN_STRATEGIES, Q.MODE)
    names = ", ".join(f"{r['strategy_id']} ({r['n']})" for r in known) or "в БД пока нет ни одной сделки"
    return f"Сделок стратегии {strategy_id!r} нет. Известные strategy_id (число сделок): {names}."


@mcp.tool()
async def get_strategies_overview(time_window_hours: int = 168) -> dict[str, Any]:
    """Сводка по всем стратегиям (strategy_id) за последние N часов: число сделок, Win Rate, Profit Factor, Net R.

    Учитываются закрытые сделки Shadow Mode без gap_affected (окно - по времени закрытия). Отдельно показано, сколько сделок окна
    в статистику не вошло (открытые, отменённые, aborted, с обрывом потока). Если сделок меньше 200 - в ответе sample_warning.

    Args:
        time_window_hours: окно в часах, 1..8760 (168 = неделя, 720 = 30 суток).
    """
    hours = _int_in("time_window_hours", time_window_hours, 1, A.HOURS_MAX)
    closed = await db.fetch(Q.CLOSED_IN_WINDOW, Q.MODE, hours)
    status_rows = await db.fetch(Q.STATUS_COUNTS_IN_WINDOW, Q.MODE, hours)
    return A.jsonable(A.build_overview(closed, status_rows, hours))


@mcp.tool()
async def get_trade_details(strategy_id: str, limit: int = 20) -> dict[str, Any]:
    """Сырые данные последних сделок стратегии (новые первыми): цены входа/выхода, SL/TP, PnL, net R, MFE/MAE, причина выхода.

    К каждой сделке приложены: текстовая причина входа (`reason`, строится из снапшота `trade_snapshots.meta` теми же правилами,
    что в клиенте), HTF Bias H4 на момент сигнала, сырые параметры сигнала (`signal_meta`) и все частичные закрытия (`closes`).
    Свечи снапшота не включены (слишком большие).

    Args:
        strategy_id: например SMC_Pro_Trend, SMC_Counter_Trend, OBI_Breakout, ICT_Killzones, VWAP_Reversion.
        limit: сколько последних сделок вернуть, 1..100 (по времени сигнала; включает открытые и отменённые - смотрите status).
    """
    sid = _strategy_id(strategy_id)
    n = _int_in("limit", limit, 1, TRADES_LIMIT_MAX)
    rows = await db.fetch(Q.RECENT_TRADES, sid, n)
    if not rows:
        return {"strategy_id": sid, "trades": [], "note": await _unknown_strategy_message(sid)}
    closes = await db.fetch(Q.CLOSES_FOR_TRADES, [r["id"] for r in rows])
    by_trade: dict[str, list[dict[str, Any]]] = {}
    for c in closes:
        by_trade.setdefault(c["trade_id"], []).append(c)
    trades = [A.build_trade_view(r, by_trade.get(r["id"], []), A.reason_for(r, r["snapshot_meta"], r["snapshot_geometry"])) for r in rows]
    return {"strategy_id": sid, "count": len(trades), "trades": trades}


@mcp.tool()
async def analyze_drawdown_causes(strategy_id: str, worst_trades_limit: int = 15) -> dict[str, Any]:
    """Анализ причин убытков стратегии: все убыточные сделки (Net R < 0) в сравнении со всеми закрытыми.

    Возвращает: общую статистику и максимальную просадку (в R, с датами пика и дна), профиль убытков (как быстро цена шла против
    входа, сколько убытков дошло до +1R), разбивки по HTF Bias H4 / по тренду (with_trend, counter_trend) / стороне / монете /
    причине выхода / часу UTC (и по сессии для ICT_Killzones, по подтверждению для OBI_Breakout) с долей каждой группы в общем убытке,
    а также худшие сделки с текстом причины входа. Без gap_affected. Малые группы (<20 сделок) - шум, выборка < 200 - предупреждение.

    Args:
        strategy_id: например SMC_Pro_Trend.
        worst_trades_limit: сколько худших сделок показать подробно, 1..50.
    """
    sid = _strategy_id(strategy_id)
    worst = _int_in("worst_trades_limit", worst_trades_limit, 1, 50)
    rows = await db.fetch(Q.CLOSED_FOR_ANALYSIS, Q.MODE, sid, ANALYSIS_ROWS_MAX + 1)
    if not rows:
        return {"strategy_id": sid, "closed_trades": 0, "note": await _unknown_strategy_message(sid)}
    truncated = len(rows) > ANALYSIS_ROWS_MAX
    return A.build_drawdown_report(sid, rows[:ANALYSIS_ROWS_MAX], worst, truncated)


def main() -> None:
    mcp.run(transport="stdio")  # пул закрывается вместе с процессом; соединения read-only, висящих транзакций нет


if __name__ == "__main__":
    main()
