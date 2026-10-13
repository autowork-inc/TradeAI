<#
.SYNOPSIS
  Key-based SSH tunnel to the PostgreSQL container on the VPS: 127.0.0.1:<LocalPort> -> postgres:5432 (inside Docker on the VPS).

.DESCRIPTION
  * No password: uses the key ~/.ssh/id_ed25519 (BatchMode, never prompts).
  * The postgres port is NOT published on the VPS (by design, tests/test_deploy.py), so the target is the container's
    Docker-network address. It is looked up over ssh on EVERY (re)connect, because it changes when the container is recreated.
    The only commands run on the server are `docker ps` and `docker inspect` (read-only).
  * Self-healing: if the tunnel drops (network, VPS reboot, sleep/wake), it reconnects after -RetrySeconds, forever.
  * Single instance: a second copy exits immediately (named mutex + port check).
  * Log: mcp_server\tunnel.log (trimmed to the last ~500 lines on start).
  Run hidden / at logon: see tunnel-hidden.vbs and install_autostart.ps1.

.EXAMPLE
  .\mcp_server\tunnel.ps1                      # defaults: 193.233.75.254, root, port 15432
  .\mcp_server\tunnel.ps1 -SshHost 10.0.0.5 -User deploy -KeyFile $HOME\.ssh\other_key
#>
param(
    [string]$SshHost = "193.233.75.254",
    [string]$User = "root",
    [int]$LocalPort = 15432,
    [string]$KeyFile = "$HOME\.ssh\id_ed25519",
    [int]$RetrySeconds = 10
)
$ErrorActionPreference = "Stop"
$LogFile = Join-Path $PSScriptRoot "tunnel.log"

function Log([string]$msg) {
    $line = "{0:yyyy-MM-dd HH:mm:ss} {1}" -f (Get-Date), $msg
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
    if ($Host.UI.RawUI.WindowSize.Width -gt 0) { Write-Host $line }
}

if (Test-Path $LogFile) {  # keep the log small
    $tail = Get-Content $LogFile -Tail 500
    Set-Content -Path $LogFile -Value $tail -Encoding UTF8
}
if (-not (Test-Path $KeyFile)) { Log "FATAL: key $KeyFile not found"; exit 2 }

$mutex = New-Object System.Threading.Mutex($false, "Global\TradeAI-MCP-Tunnel-$LocalPort")
if (-not $mutex.WaitOne(0)) { Log "another tunnel instance is already running - exit"; exit 0 }

$sshBase = @("-i", $KeyFile, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
             "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
             "$User@$SshHost")

# resolved on the server through stdin (no quoting problems on Windows)
$remoteScript = @'
c=$(docker ps -q --filter label=com.docker.compose.service=postgres --filter status=running | head -n 1)
[ -n "$c" ] || c=$(docker ps -q --filter name=postgres --filter status=running | head -n 1)
[ -n "$c" ] || exit 3
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "$c"
'@ -replace "`r", ""

try {
    while ($true) {
        try {
            $out = $remoteScript | & ssh @sshBase "sh -s" 2>&1
            $ip = ("$out".Trim() -split "\s+")[0]
            if ($ip -notmatch '^\d{1,3}(\.\d{1,3}){3}$') { throw "cannot resolve postgres container IP (got: '$out')" }
            Log "tunnel 127.0.0.1:$LocalPort -> ${ip}:5432 via $User@$SshHost"
            & ssh @sshBase -N -o ExitOnForwardFailure=yes -L "127.0.0.1:${LocalPort}:${ip}:5432" 2>&1 | ForEach-Object { Log "ssh: $_" }
            Log "ssh exited (code $LASTEXITCODE)"
        } catch {
            Log "ERROR: $($_.Exception.Message)"
        }
        Log "retry in $RetrySeconds s"
        Start-Sleep -Seconds $RetrySeconds
    }
} finally {
    $mutex.ReleaseMutex()
}
