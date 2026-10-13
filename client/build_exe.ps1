# Сборка TradeAI-Monitor.exe через `flet pack`. Запуск из каталога client:   powershell -ExecutionPolicy Bypass -File build_exe.ps1
# Результат: dist\TradeAI-Monitor.exe (один файл). Параметр -OneDir соберёт папку dist\TradeAI-Monitor\ (запускается быстрее, антивирусы ругаются реже).
# Дефолты подключения зашиваются в сборку (tradeai_client\build_defaults.py) только на время сборки:
#   -ServerUrl  адрес API (по умолчанию значение из build_defaults.py: http://193.233.75.254:8001)
#   -ApiKey     ключ API (или переменная окружения TRADEAI_BUILD_API_KEY; ключ берётся на сервере: ./deploy/shadow.sh apikey).
#               Без ключа .exe при первом запуске откроет окно настроек и сохранит ввод в settings.json рядом с .exe.
#               ВНИМАНИЕ: ключ внутри .exe читается любым, у кого есть файл - не передавайте такой .exe посторонним.
param([switch]$OneDir, [switch]$SkipTests, [string]$ServerUrl = "", [string]$ApiKey = $env:TRADEAI_BUILD_API_KEY)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path .venv\Scripts\python.exe)) {
    Write-Host "== Создаю окружение .venv"
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Не удалось создать .venv (нужен Python 3.12)" }
}
Write-Host "== Устанавливаю зависимости"
.\.venv\Scripts\python.exe -m pip install --quiet --upgrade pip
.\.venv\Scripts\python.exe -m pip install --quiet -r requirements-build.txt
if ($LASTEXITCODE -ne 0) { throw "pip install завершился с ошибкой" }

if (-not $SkipTests) {
    Write-Host "== Тесты клиента"
    .\.venv\Scripts\python.exe -m pytest tests -q -W ignore::DeprecationWarning
    if ($LASTEXITCODE -ne 0) { throw "Тесты не прошли: сборка отменена" }
}

$defaultsFile = Join-Path $PSScriptRoot "tradeai_client\build_defaults.py"
$defaultsOriginal = [System.IO.File]::ReadAllText($defaultsFile)
if ($ServerUrl -or $ApiKey) {
    $patched = $defaultsOriginal
    if ($ServerUrl) { $patched = [regex]::Replace($patched, '(?m)^SERVER_URL = "[^"]*"', ('SERVER_URL = "' + $ServerUrl + '"')) }
    if ($ApiKey)    { $patched = [regex]::Replace($patched, '(?m)^API_KEY = "[^"]*"', ('API_KEY = "' + $ApiKey + '"')) }
    [System.IO.File]::WriteAllText($defaultsFile, $patched, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host ("== Дефолты сборки: адрес {0}, ключ {1}" -f $(if ($ServerUrl) { $ServerUrl } else { "из build_defaults.py" }), $(if ($ApiKey) { "зашит" } else { "не задан" }))
}
Write-Host "== flet pack"
$packArgs = @("pack", "app.py", "--name", "TradeAI-Monitor", "--product-name", "TradeAI Monitor", "--file-description", "TradeAI Monitor",
          "--product-version", "0.1.0", "--distpath", "dist", "-y",
          # статические файлы lightweight-charts (js/css/html окна графика) PyInstaller сам не находит
          "--pyinstaller-build-args=--collect-all=lightweight_charts")
if ($OneDir) { $packArgs += "-D" }
try {
    .\.venv\Scripts\flet.exe @packArgs
    if ($LASTEXITCODE -ne 0) { throw "flet pack завершился с ошибкой" }
} finally {
    # ключ не должен остаться в исходниках
    [System.IO.File]::WriteAllText($defaultsFile, $defaultsOriginal, (New-Object System.Text.UTF8Encoding($false)))
}

$exe = if ($OneDir) { "dist\TradeAI-Monitor\TradeAI-Monitor.exe" } else { "dist\TradeAI-Monitor.exe" }
if (-not (Test-Path $exe)) { throw "Сборка завершилась, но $exe не найден" }
Write-Host ("== Готово: {0} ({1:N1} МБ)" -f (Resolve-Path $exe), ((Get-Item $exe).Length / 1MB))
