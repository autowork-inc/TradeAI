"""Пакетный прогон Playground: монеты x группы стратегий отдельными процессами, не больше --jobs одновременно (длинные - первыми).

    python -X utf8 -m playground.batch --symbols BTCUSDT,ETHUSDT --from 2025-10-24 [--to 2026-10-24] [--groups chr,santa] [--jobs 10] --batch top10

Каждый прогон - `playground.cli run` в своём процессе (свой лог в DATA_DIR/playground/logs). Минутки должны быть в кэше (playground.download).
"""

import argparse
import os
import subprocess
import sys
import time

from playground import paths

ORDER = {"chr": 0, "santa": 1, "all": 0}  # CHR (наклонка) - самая медленная: стартует первой


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", required=True)
    p.add_argument("--from", dest="start", required=True)
    p.add_argument("--to", dest="end", default="")
    p.add_argument("--groups", default="chr,santa")
    p.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    p.add_argument("--batch", required=True)
    a = p.parse_args()
    logs = paths.root() / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    queue = sorted(((s.strip().upper(), g.strip()) for g in a.groups.split(",") for s in a.symbols.split(",") if s.strip()), key=lambda x: ORDER.get(x[1], 9))
    running: list[tuple[subprocess.Popen, str]] = []
    failed: list[str] = []
    while queue or running:
        while queue and len(running) < a.jobs:
            sym, group = queue.pop(0)
            name = f"{a.batch}_{sym}_{group}"
            cmd = [sys.executable, "-X", "utf8", "-m", "playground.cli", "run", "--symbol", sym, "--from", a.start, "--group", group, "--batch", a.batch,
                   "--label", f"{a.batch}: {sym} {group}"] + (["--to", a.end] if a.end else [])
            out = open(logs / f"{name}.log", "w", encoding="utf-8")
            running.append((subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT), name))
            print(time.strftime("%H:%M:%S"), "старт", name, flush=True)
        time.sleep(5)
        for proc, name in list(running):
            if proc.poll() is not None:
                running.remove((proc, name))
                print(time.strftime("%H:%M:%S"), "готово" if proc.returncode == 0 else f"ОШИБКА ({proc.returncode})", name, flush=True)
                if proc.returncode:
                    failed.append(name)
    print("ИТОГ: ошибок", len(failed), failed, flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
