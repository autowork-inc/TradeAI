"""Оцифровщик графиков: картинки из .xlsx -> Gemini (JSON-паттерны) -> жёсткий фильтр -> CSV.

Запуск:
    set GEMINI_API_KEY=...            (PowerShell: $env:GEMINI_API_KEY="...")
    python -X utf8 scripts/digitize_charts.py "charts.xlsx" --out out/digitized

Результаты:
    <out>/images/            извлечённые из xl/media картинки
    <out>/raw_responses.jsonl   ответы Gemini с рассуждением (кеш: повторный запуск не тратит запросы)
    <out>/report.csv         Имя файла, Тип, Касания, Закругление, Вердикт, Причина

Зависимость: httpx (pip install httpx). SDK Google не нужен — прямой REST.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

# Gemini 1.5 Pro закрыт Google; модель задаётся --model / GEMINI_MODEL.
DEFAULT_MODEL = "gemini-3.6-flash"
API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

IMAGE_EXT = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
             ".webp": "image/webp", ".gif": "image/gif"}

SYSTEM_PROMPT = """Ты — алгоритмический оцифровщик графиков. Ты НЕ трейдер и не даёшь советов: твоя задача — реально рассмотреть изображение и зафиксировать, что на нём нарисовано.

Работай в два этапа.

ЭТАП 1 — рассуждение (обычным текстом, коротко, по пунктам):
1. Тренд: опиши словами, где именно на графике ты видишь направление (какие участки свечей идут вверх/вниз/в боковик, откуда и куда) и сделай вывод LONG, SHORT или FLAT.
2. Структура: что пробивается — наклонная линия (Trendline) или горизонтальный уровень (Horizontal Level)? Укажи, на какой цене/участке находится этот уровень или линия.
3. Касания: перечисли КАЖДОЕ касание ценой пробиваемого уровня до момента пробоя отдельно — опиши, где оно (слева/в центре/справа, какая свеча, тенью или телом). Не считай «на глаз» и не подгоняй число: если явных касаний нет — так и напиши. Итог: целое число.
4. Закругление / поджатие: в чём оно визуально выражается (как менялись максимумы/минимумы и размах свечей при приближении к уровню)? Если перед пробоем плавного закругления или агрессивного поджатия нет — напиши, что его нет.
5. Пробой: есть ли фактический пробой уровня полнотелой свечой? Опиши эту свечу или напиши, что пробоя нет.
Не придерживайся «типичных» ответов: значения касаний и true/false должны следовать из того, что ты описал выше. Обязательное условие системы — минимум 2-3 касания; если их меньше, пиши реальное число.

ЭТАП 2 — итог. После рассуждения выведи ОДИН блок ```json ... ``` строго такого вида (и ничего после него):
```json
{
  "trend": "LONG",
  "setup_type": "Trendline",
  "touches": 3,
  "has_rounding": true,
  "rounding_desc": "плавное поджатие лоев к уровню",
  "is_breakout_confirmed": true
}
```
trend — LONG, SHORT или FLAT; setup_type — "Trendline" или "Horizontal Level"; touches — целое число; has_rounding и is_breakout_confirmed — true/false; rounding_desc — 3-5 слов. Значения в JSON должны совпадать с твоим рассуждением."""

MIN_TOUCHES = 2
VALID = "VALID_SETUP"
INVALID = "INVALID_SETUP"
CSV_FIELDS = ["Имя файла", "Тип", "Касания", "Закругление", "Вердикт", "Причина"]


# ---------------------------------------------------------------- Шаг 2.1

def _natural_key(name: str) -> list:
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


def extract_images(xlsx: Path, dest: Path) -> list[Path]:
    """Достаёт все файлы xl/media/* из .xlsx (это zip) в dest. Только базовые имена (защита от zip-slip)."""
    dest.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    with zipfile.ZipFile(xlsx) as z:
        members = [m for m in z.namelist()
                   if m.startswith("xl/media/") and not m.endswith("/")
                   and Path(m).suffix.lower() in IMAGE_EXT]
        for m in sorted(members, key=_natural_key):
            target = dest / Path(m).name
            target.write_bytes(z.read(m))
            out.append(target)
    return out


# ---------------------------------------------------------------- Шаг 2.2

class GeminiError(RuntimeError):
    pass


def _retry_delay(resp: httpx.Response, attempt: int, base: float) -> float:
    """Пауза перед повтором: Retry-After -> retryDelay из тела ошибки -> экспоненциально."""
    ra = resp.headers.get("retry-after")
    if ra and ra.replace(".", "", 1).isdigit():
        return float(ra) + 1
    try:
        for d in resp.json().get("error", {}).get("details", []):
            m = re.fullmatch(r"(\d+(?:\.\d+)?)s", str(d.get("retryDelay", "")))
            if m:
                return float(m.group(1)) + 1
    except (ValueError, AttributeError):
        pass
    return min(base * (2 ** attempt), 120.0)


_FENCED_JSON = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)


def _parse_json(text: str) -> dict:
    """Достаёт итоговый JSON-объект из ответа, игнорируя рассуждение перед ним.

    1) последний блок ```json {...} ```; 2) запасной путь — последний валидный {...} в тексте.
    """
    candidates = _FENCED_JSON.findall(text)
    obj = None
    for raw in reversed(candidates):
        try:
            obj = json.loads(raw)
            break
        except json.JSONDecodeError:
            continue
    if obj is None:
        dec = json.JSONDecoder()
        for m in reversed(list(re.finditer(r"\{", text))):
            try:
                cand, _ = dec.raw_decode(text, m.start())
            except json.JSONDecodeError:
                continue
            if isinstance(cand, dict) and "touches" in cand:
                obj = cand
                break
    if obj is None:
        raise GeminiError(f"в ответе нет JSON-блока: ...{text[-200:]!r}")
    if not isinstance(obj, dict):
        raise GeminiError("JSON не объект")
    return obj


def thinking_config(model: str, level: str) -> dict | None:
    """Уровень размышлений. Gemini 3 — thinkingLevel; Gemini 2.5 — бюджет токенов (thinkingBudget)."""
    if level == "default":
        return None
    if model.startswith("gemini-3"):
        return {"thinkingLevel": level}  # low | high
    budgets = {"low": 1024, "high": 32768 if "pro" in model else 24576}
    return {"thinkingBudget": budgets[level]}


def digitize(client: httpx.Client, api_key: str, model: str, image: Path,
             max_retries: int, backoff: float, thinking: str = "default") -> tuple[dict, str]:
    import base64
    body = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [
            {"inlineData": {"mimeType": IMAGE_EXT[image.suffix.lower()],
                            "data": base64.b64encode(image.read_bytes()).decode()}},
            {"text": "Рассмотри график и выполни оба этапа инструкции."},
        ]}],
        "generationConfig": {"temperature": 0},
    }
    tc = thinking_config(model, thinking)
    if tc:
        body["generationConfig"]["thinkingConfig"] = tc
    url = API_URL.format(model=model)
    for attempt in range(max_retries + 1):
        try:
            resp = client.post(url, json=body, headers={"x-goog-api-key": api_key})
        except (httpx.TimeoutException, httpx.TransportError) as e:
            if attempt == max_retries:
                raise GeminiError(f"сеть: {e}") from e
            wait = min(backoff * (2 ** attempt), 120.0)
            print(f"  сеть ({type(e).__name__}), повтор через {wait:.0f} с", file=sys.stderr)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            data = resp.json()
            cands = data.get("candidates") or []
            if not cands:
                reason = (data.get("promptFeedback") or {}).get("blockReason", "нет кандидатов")
                raise GeminiError(f"ответ пуст/заблокирован: {reason}")
            parts = (cands[0].get("content") or {}).get("parts") or []
            text = "".join(p.get("text", "") for p in parts)
            return _parse_json(text), text

        if resp.status_code in (429, 500, 502, 503, 504):  # лимиты и временные сбои
            if attempt == max_retries:
                raise GeminiError(f"HTTP {resp.status_code} после {max_retries} повторов: {resp.text[:200]}")
            wait = _retry_delay(resp, attempt, backoff)
            print(f"  HTTP {resp.status_code} (лимит/сбой), повтор через {wait:.0f} с "
                  f"({attempt + 1}/{max_retries})", file=sys.stderr)
            time.sleep(wait)
            continue

        # 400/401/403/404 — повтор не поможет (ключ, модель, формат)
        raise GeminiError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    raise GeminiError("недостижимо")


# ---------------------------------------------------------------- Шаг 2.3

def validate_setup(data: dict) -> tuple[bool, str]:
    """Жёсткий фильтр. Возвращает (прошёл, причина отказа или '')."""
    problems: list[str] = []

    trend = str(data.get("trend", "")).upper()
    if trend not in ("LONG", "SHORT"):
        problems.append(f"trend={data.get('trend')!r} (нужен LONG/SHORT)")
    # Направление пробоя: отдельного поля в формате нет, trend и есть направление.
    # Если модель всё же прислала breakout_direction — сверяем.
    bd = data.get("breakout_direction")
    if bd is not None and str(bd).upper() != trend:
        problems.append(f"trend={trend} не совпадает с направлением пробоя {bd}")

    touches = data.get("touches")
    if isinstance(touches, bool) or not isinstance(touches, int):
        problems.append(f"touches={touches!r} не целое")
    elif touches < MIN_TOUCHES:
        problems.append(f"touches={touches} < {MIN_TOUCHES}")

    if data.get("has_rounding") is not True:
        problems.append(f"has_rounding={data.get('has_rounding')!r} (нужно true)")

    if data.get("is_breakout_confirmed") is not True:
        problems.append("пробой не подтверждён")

    return (not problems, "; ".join(problems))


def report_row(name: str, data: dict | None, error: str | None) -> dict:
    if data is None:
        return {"Имя файла": name, "Тип": "", "Касания": "", "Закругление": "",
                "Вердикт": INVALID, "Причина": f"ошибка оцифровки: {error}"}
    ok, why = validate_setup(data)
    return {"Имя файла": name, "Тип": data.get("setup_type", ""),
            "Касания": data.get("touches", ""), "Закругление": data.get("has_rounding", ""),
            "Вердикт": VALID if ok else INVALID, "Причина": why}


# ---------------------------------------------------------------- main

def load_cache(path: Path) -> dict[str, dict]:
    cache: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rec = json.loads(line)
                cache[rec["file"]] = rec["data"]
    return cache


def row_tag(data: dict) -> str:
    ok, why = validate_setup(data)
    return VALID if ok else f"{INVALID} ({why})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("xlsx", type=Path, help="Excel-файл со скриншотами")
    ap.add_argument("--out", type=Path, default=Path("out/digitized"))
    ap.add_argument("--model", default=os.environ.get("GEMINI_MODEL", DEFAULT_MODEL))
    ap.add_argument("--workers", type=int, default=5, help="параллельных запросов (по умолчанию 5)")
    ap.add_argument("--delay", type=float, default=0.5, help="пауза воркера после каждого запроса, с")
    ap.add_argument("--max-retries", type=int, default=6)
    ap.add_argument("--backoff", type=float, default=5.0, help="база экспоненциальной паузы, с")
    ap.add_argument("--thinking", choices=["default", "low", "high"], default="high",
                    help="уровень размышлений модели (по умолчанию high)")
    ap.add_argument("--limit", type=int, default=0, help="обработать только N первых картинок (проба)")
    args = ap.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("Задайте переменную окружения GEMINI_API_KEY", file=sys.stderr)
        return 3
    if not args.xlsx.is_file():
        print(f"Нет файла: {args.xlsx}", file=sys.stderr)
        return 3

    images = extract_images(args.xlsx, args.out / "images")
    if args.limit:
        images = images[: args.limit]
    print(f"Извлечено картинок: {len(images)} -> {args.out / 'images'}")
    if not images:
        print("В xl/media ничего нет (картинки могли быть вставлены как объекты/диаграммы).", file=sys.stderr)
        return 1

    cache_path = args.out / "raw_responses.jsonl"
    cache = load_cache(cache_path)
    total = len(images)
    lock = threading.Lock()  # защищает запись кеша и вывод
    done = 0

    def work(client: httpx.Client, cache_f, img: Path) -> dict:
        nonlocal done
        if img.name in cache:
            row = report_row(img.name, cache[img.name], None)
            tag = "из кеша"
        else:
            try:
                data, reasoning = digitize(client, api_key, args.model, img, args.max_retries,
                                           args.backoff, args.thinking)
            except GeminiError as e:  # в кеш не пишем — повторим при перезапуске
                row, tag = report_row(img.name, None, str(e)), f"ОШИБКА: {e}"
            else:
                with lock:
                    cache_f.write(json.dumps({"file": img.name, "data": data, "reasoning": reasoning},
                                             ensure_ascii=False) + "\n")
                    cache_f.flush()
                row, tag = report_row(img.name, data, None), row_tag(data)
            time.sleep(args.delay)  # пауза воркера после запроса
        with lock:
            done += 1
            print(f"[{done}/{total}] {img.name}: {tag}", file=sys.stderr if tag.startswith("ОШИБКА") else sys.stdout)
        return row

    # httpx.Client потокобезопасен; pool_size под число воркеров. map сохраняет порядок картинок.
    limits = httpx.Limits(max_connections=args.workers, max_keepalive_connections=args.workers)
    with httpx.Client(timeout=300, limits=limits) as client, cache_path.open("a", encoding="utf-8") as cache_f, \
            ThreadPoolExecutor(max_workers=args.workers) as pool:
        rows = list(pool.map(lambda im: work(client, cache_f, im), images))

    report = args.out / "report.csv"
    with report.open("w", newline="", encoding="utf-8-sig") as f:  # BOM — чтобы Excel понял кириллицу
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, delimiter=";")
        w.writeheader()
        w.writerows(rows)

    valid = sum(r["Вердикт"] == VALID for r in rows)
    print(f"\nГотово: {valid} валидных, {len(rows) - valid} отбраковано. Отчёт: {report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
