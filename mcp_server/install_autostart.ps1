<#
.SYNOPSIS
  Registers (or removes) a Windows Task Scheduler task that starts the MCP DB tunnel hidden at every logon.

.DESCRIPTION
  Task "TradeAI MCP Tunnel": trigger = logon of the current user, action = wscript.exe tunnel-hidden.vbs (no console window),
  runs as the current user (the SSH key lives in your profile), no admin rights needed, restarts if it fails, no time limit.
  A logon trigger (not "system startup") is used on purpose: the key ~/.ssh/id_ed25519 is only readable in your session.

.EXAMPLE
  .\mcp_server\install_autostart.ps1              # install + start now
  .\mcp_server\install_autostart.ps1 -Uninstall   # remove the task (and stop the tunnel)
#>
param([switch]$Uninstall)
$ErrorActionPreference = "Stop"
$name = "TradeAI MCP Tunnel"
$vbs = Join-Path $PSScriptRoot "tunnel-hidden.vbs"

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "Task removed."
    } else { Write-Host "Task not found." }
    Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" | Where-Object { $_.CommandLine -like "*mcp_server*tunnel.ps1*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host "Stopped tunnel process $($_.ProcessId)" }
    exit 0
}

if (-not (Test-Path $vbs)) { throw "not found: $vbs" }
$who = "$env:USERDOMAIN\$env:USERNAME"
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$vbs`""
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $who
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId $who -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings -Principal $principal `
    -Description "SSH tunnel 127.0.0.1:15432 -> PostgreSQL on the TradeAI VPS (for the MCP analytics server). Key-based, hidden." -Force | Out-Null
Write-Host "Task '$name' registered (starts at logon of $who)."
Start-ScheduledTask -TaskName $name
Write-Host "Started now. Log: $(Join-Path $PSScriptRoot 'tunnel.log')"
