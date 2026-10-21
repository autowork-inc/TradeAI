"""Текстовые подписи сделки (С, П, В, ...) на графике привязаны к СВЕЧЕ (время + позиция над/под ней), а не к фиксированной точке холста (2026-10-21, v1.3.1).

Раньше подпись «С» вертикальной линии сигнала рисовалась на холсте на высоте y = 12 и «улетала» в верхний левый угол при смене масштаба/таймфрейма. Теперь все подписи -
нативные маркеры lightweight-charts (`series.setMarkers`): библиотека сама держит их у свечи. Функции `snapIndex` и `applyMarkers` берутся из chart.js и исполняются в node."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

JS = Path(__file__).resolve().parents[1] / "tradeai_client" / "chart_web" / "chart.js"
NODE = shutil.which("node")


def _function(src: str, name: str) -> str:
    m = re.search(rf"  function {name}\(.*?\n  }}\n", src, re.S)
    assert m, f"в chart.js нет функции {name}"
    return m.group(0)


def _markers(bars: list[int], spec: dict) -> list[dict]:
    src = JS.read_text(encoding="utf-8")
    script = (f"const S={{bars:{json.dumps([{'time': t} for t in bars])}}};const spec={json.dumps(spec)};let out=null;const series={{setMarkers:(m)=>{{out=m}}}};"
              + _function(src, "snapIndex") + _function(src, "applyMarkers") + "applyMarkers();console.log(JSON.stringify(out));")
    return json.loads(subprocess.run([NODE, "-e", script], capture_output=True, text=True, encoding="utf-8", timeout=30, check=True).stdout)


BARS = [1700000000 + i * 900 for i in range(40)]


@pytest.mark.skipif(NODE is None, reason="node не установлен")
def test_signal_label_is_a_native_marker_on_the_signal_candle():
    spec = {"trade_markers": [{"t": BARS[10] * 1000, "position": "below", "shape": "arrow_up", "color": "#2bb673", "text": "В"}], "setup_markers": [],
            "vlines": [{"t": (BARS[7] + 400) * 1000, "color": "#ffb300", "text": "С"}]}  # сигнал внутри свечи 7 (не на её открытии)
    markers = {m["text"]: m for m in _markers(BARS, spec)}
    assert markers["С"]["time"] == BARS[7] and markers["С"]["position"] == "aboveBar"  # свеча, в которой случился сигнал: подпись не отрывается от неё
    assert markers["В"]["time"] == BARS[10] and markers["В"]["position"] == "belowBar"
    times = [m["time"] for m in _markers(BARS, spec)]
    assert times == sorted(times)  # lightweight-charts требует маркеры по возрастанию времени


@pytest.mark.skipif(NODE is None, reason="node не установлен")
def test_markers_follow_the_timeframe_and_skip_moments_before_the_loaded_history():
    spec = {"trade_markers": [], "setup_markers": [], "vlines": [{"t": BARS[3] * 1000, "color": "#ffb300", "text": "С"}]}
    h1 = [BARS[0] + i * 3600 for i in range(10)]  # другой таймфрейм: маркер встаёт на ЧАСОВУЮ свечу, содержащую сигнал
    assert _markers(h1, spec)[0]["time"] == h1[0]
    assert _markers([BARS[20] + i * 900 for i in range(5)], spec) == []  # сигнал раньше загруженной истории: маркера нет, а не «угол холста»


def test_vline_text_is_no_longer_painted_at_a_fixed_canvas_height():
    src = JS.read_text(encoding="utf-8")
    draw = src[src.index("function drawSpecLayer"):src.index("function drawBot")]
    assert "v.text" not in draw.replace("// текст «С»", "") and "x + 3, 12" not in src  # подписи на холсте у верхнего края больше нет
    assert "position: 'aboveBar', color: v.color" in src
