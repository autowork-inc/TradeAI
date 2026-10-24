# Сборка TradeAI-Monitor.exe через `flet pack`. Запуск из каталога client:   powershell -ExecutionPolicy Bypass -File build_exe.ps1
# Результат: dist\TradeAI-Monitor.exe (один файл). Параметр -OneDir соберёт папку dist\TradeAI-Monitor\ (запускается быстрее, антивирусы ругаются реже).
# Дефолты подключения зашиваются в сборку (tradeai_client\build_defaults.py) только на время сборки:
#   -ServerUrl  адрес API (по умолчанию значение из build_defaults.py: http://193.233.75.254:8001)
#   -ApiKey     ключ API (или переменная окружения TRADEAI_BUILD_API_KEY; ключ берётся на сервере: ./deploy/shadow.sh apikey).
#               Без ключа .exe при первом запуске откроет окно настроек и сохранит ввод в settings.json рядом с .exe.
#               ВНИМАНИЕ: ключ внутри .exe читается любым, у кого есть файл - не передавайте такой .exe посторонним.
# Автообновление (2026-10-19): с ключом -Publish после успешной сборки готовый .exe и version.json отправляются на VPS в ./data/updates (по SSH-ключу):
#   -Publish            выложить сборку для автообновления клиентов (клиенты при старте сравнивают версию и предлагают обновиться)
#   -Version 0.3.0      сменить версию клиента (tradeai_client\__init__.py); без параметра берётся текущая. Клиенты обновляются, только если версия на сервере НОВЕЕ.
#   -Notes "..."        краткий текст в version.json (по умолчанию - изменения версии из tradeai_client\changelog.json). История версий (changelog.json) уходит на сервер целиком.
#                       Перед сборкой добавьте запись новой версии в НАЧАЛО tradeai_client\changelog.json: без неё скрипт остановится.
#   -PublishOnly        не собирать: выложить уже собранный dist\TradeAI-Monitor.exe (например, если публикация прервалась)
#   -PublishHost/-PublishUser/-PublishKey/-PublishDir  куда класть (по умолчанию хост из адреса сервера, root, ~/.ssh/id_ed25519, trading/data/updates)
# Публикуется только сборка в один файл (без -OneDir). Ключ API, зашитый в .exe (-ApiKey), попадёт на сервер вместе с файлом; отдаётся он только по ключу API.
param([switch]$OneDir, [switch]$SkipTests, [string]$ServerUrl = "", [string]$ApiKey = $env:TRADEAI_BUILD_API_KEY,
      [switch]$Publish, [switch]$PublishOnly, [string]$Version = "", [string]$Notes = "", [string]$PublishHost = "", [string]$PublishUser = "root",
      [string]$PublishKey = (Join-Path $env:USERPROFILE ".ssh\id_ed25519"), [string]$PublishDir = "trading/data/updates")
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if ($Publish -and $OneDir) { throw "-Publish работает только с однофайловой сборкой (без -OneDir): клиент обновляет один .exe" }
$initFile = Join-Path $PSScriptRoot "tradeai_client\__init__.py"
if ($Version) {
    if ($Version -notmatch '^\d{1,4}(\.\d{1,4}){1,3}$') { throw "Версия вида 0.3.0, получено '$Version'" }
    $initText = [System.IO.File]::ReadAllText($initFile)
    $initText = [regex]::Replace($initText, '(?m)^__version__ = "[^"]*"', ('__version__ = "' + $Version + '"'))
    [System.IO.File]::WriteAllText($initFile, $initText, (New-Object System.Text.UTF8Encoding($false)))
}
$appVersion = [regex]::Match([System.IO.File]::ReadAllText($initFile), '(?m)^__version__ = "([^"]+)"').Groups[1].Value
if (-not $appVersion) { throw "Не найден __version__ в $initFile" }
Write-Host "== Версия клиента: $appVersion"
# История версий (Блок 5): запись для собираемой версии обязана быть в changelog.json - иначе «Что нового» и «История обновлений» были бы пустыми.
$changelogFile = Join-Path $PSScriptRoot "tradeai_client\changelog.json"
$changelog = [System.IO.File]::ReadAllText($changelogFile, (New-Object System.Text.UTF8Encoding($false))) | ConvertFrom-Json
$releaseNotes = @($changelog.versions | Where-Object { $_.version -eq $appVersion })
if ($releaseNotes.Count -ne 1) { throw "В tradeai_client\changelog.json нет (или дважды) записи для версии ${appVersion}: добавьте release notes и повторите сборку" }
$productVersion = (($appVersion.Split(".") + @("0", "0", "0", "0"))[0..3]) -join "."   # ресурс версии Windows: 4 числа (1.0 -> 1.0.0.0)

if (-not (Test-Path .venv\Scripts\python.exe)) {
    Write-Host "== Создаю окружение .venv"
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Не удалось создать .venv (нужен Python 3.12)" }
}
$defaultsFile = Join-Path $PSScriptRoot "tradeai_client\build_defaults.py"
if ($PublishOnly) { $Publish = $true }
if (-not $PublishOnly) {
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
          "--product-version", $productVersion, "--distpath", "dist", "-y",
          # страница графика (chart_web: html/js/css + библиотека lightweight-charts) и ресурсы pywebview PyInstaller сам не находит
          "--add-data", "tradeai_client/chart_web:tradeai_client/chart_web", "tradeai_client/changelog.json:tradeai_client",
          "--pyinstaller-build-args=--collect-all=webview")
if ($OneDir) { $packArgs += "-D" }
try {
    .\.venv\Scripts\flet.exe @packArgs
    if ($LASTEXITCODE -ne 0) { throw "flet pack завершился с ошибкой" }
} finally {
    # ключ не должен остаться в исходниках
    [System.IO.File]::WriteAllText($defaultsFile, $defaultsOriginal, (New-Object System.Text.UTF8Encoding($false)))
}

}  # конец сборки (-PublishOnly её пропускает)

$exe = if ($OneDir) { "dist\TradeAI-Monitor\TradeAI-Monitor.exe" } else { "dist\TradeAI-Monitor.exe" }
if (-not (Test-Path $exe)) { throw "Сборка завершилась, но $exe не найден" }
Write-Host ("== Готово: {0} ({1:N1} МБ)" -f (Resolve-Path $exe), ((Get-Item $exe).Length / 1MB))

if ($Publish) {
    Write-Host "== Публикация обновления на сервер"
    $hash = (Get-FileHash $exe -Algorithm SHA256).Hash.ToLower()
    $size = (Get-Item $exe).Length
    if (-not $PublishHost) { $PublishHost = ([uri]($(if ($ServerUrl) { $ServerUrl } else { [regex]::Match([System.IO.File]::ReadAllText($defaultsFile), '(?m)^SERVER_URL = "([^"]*)"').Groups[1].Value }))).Host }
    if (-not $PublishHost) { throw "Не удалось определить хост сервера: задайте -PublishHost" }
    if (-not (Test-Path $PublishKey)) { throw "Нет SSH-ключа $PublishKey (параметр -PublishKey)" }
    $versionJson = Join-Path $env:TEMP "tradeai_version.json"
    if (-not $Notes) { $Notes = (@($releaseNotes[0].changes | ForEach-Object { if ($_ -is [string]) { $_ } else { $_.text } }) -join " ") }   # краткие заметки - из changelog.json
    # version.json = версия + контрольная сумма + ВСЯ история версий: клиент покажет «что нового» до загрузки обновления
    $payload = [ordered]@{ version = $appVersion; sha256 = $hash; size = $size; notes = $Notes; built_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"); changelog = @($changelog.versions) }
    [System.IO.File]::WriteAllText($versionJson, ($payload | ConvertTo-Json -Depth 8), (New-Object System.Text.UTF8Encoding($false)))
    $target = "$PublishUser@$PublishHost"
    # таймауты: без них зависшее соединение вешает скрипт навсегда; ssh - с -n (ввод из NUL): Windows-ssh из PowerShell иначе может не завершиться после команды
    $sshOpts = @("-i", $PublishKey, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=20", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4")
    Write-Host "   ssh: каталог обновлений"
    & ssh -n @sshOpts $target "mkdir -p $PublishDir"
    if ($LASTEXITCODE -ne 0) { throw "ssh: не удалось создать $PublishDir на $PublishHost" }
    # сначала .exe, потом version.json (и через временные имена): клиент не должен увидеть новую версию раньше файла и не должен скачать недописанный файл
    Write-Host ("   scp: .exe ({0:N1} МБ) - минута-другая в зависимости от канала" -f ($size / 1MB))
    & scp @sshOpts $exe "${target}:$PublishDir/TradeAI-Monitor.exe.part"
    if ($LASTEXITCODE -ne 0) { throw "scp .exe не удался" }
    & scp @sshOpts $versionJson "${target}:$PublishDir/version.json.part"
    if ($LASTEXITCODE -ne 0) { throw "scp version.json не удался" }
    & ssh -n @sshOpts $target "cd $PublishDir && chmod 644 TradeAI-Monitor.exe.part version.json.part && mv -f TradeAI-Monitor.exe.part TradeAI-Monitor.exe && mv -f version.json.part version.json"
    if ($LASTEXITCODE -ne 0) { throw "ssh: не удалось подменить файлы обновления" }
    Remove-Item $versionJson -ErrorAction SilentlyContinue
    Write-Host ("== Опубликовано: версия {0}, {1:N1} МБ, sha256 {2}..." -f $appVersion, ($size / 1MB), $hash.Substring(0, 12))
}
