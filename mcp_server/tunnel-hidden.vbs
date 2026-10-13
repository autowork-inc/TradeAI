' Starts mcp_server\tunnel.ps1 completely hidden (no console window). Double-click = start the tunnel in one click.
' Safe to run repeatedly: a second copy of the tunnel exits immediately (mutex).
Dim sh, fso, dir
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
dir = fso.GetParentFolderName(WScript.ScriptFullName)
sh.Run "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & dir & "\tunnel.ps1""", 0, False
