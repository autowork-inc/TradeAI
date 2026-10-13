"""Запуск клиента TradeAI Monitor.

    python app.py                        # окно приложения
    flet run --web app.py                # то же в браузере (для разработки; окна графиков откроются отдельно)

Сборка .exe: client/README.md (команда `flet pack`)."""

import multiprocessing
import sys

if __name__ == "__main__":
    # ОБЯЗАТЕЛЬНО первой строкой: в собранном .exe окна графиков запускаются как дочерние процессы (multiprocessing, spawn);
    # без freeze_support() каждое окно графика запустило бы ещё одну копию приложения.
    multiprocessing.freeze_support()
    if len(sys.argv) == 3 and sys.argv[1] == "--selftest-chart":  # проверка окна графика (в т.ч. в собранном .exe): см. client/README.md
        from tradeai_client.chart_window import selftest

        raise SystemExit(selftest(sys.argv[2]))
    from tradeai_client.main import main

    main()
