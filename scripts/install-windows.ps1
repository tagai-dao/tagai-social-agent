param([string]$Python = "python", [switch]$RegisterTask)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$venv = Join-Path $root ".venv"
$exe = Join-Path $venv "Scripts\python.exe"
& $Python -c "import sys; sys.exit(sys.version_info < (3,11))"
if ($LASTEXITCODE -ne 0) { throw "Python 3.11+ required; pass -Python with its full path." }
& $Python -m venv $venv
if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
& $exe -m pip install -e $root
if ($LASTEXITCODE -ne 0) { throw "package install failed" }
if (!(Test-Path (Join-Path $root ".env"))) {
    Copy-Item (Join-Path $root ".env.example") (Join-Path $root ".env")
}
if ($RegisterTask) {
    $name = "TagAI Social Feed Worker"
    $user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $action = New-ScheduledTaskAction -Execute $exe -Argument "-u -m tagagent.agents.social.persistent_feed_worker --loop" -WorkingDirectory $root
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
    $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 50 -RestartInterval (New-TimeSpan -Minutes 1) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -ErrorAction Stop | Out-Null
    Write-Output "Registered for next login. To start now: Start-ScheduledTask -TaskName '$name'"
}
Write-Output "Installed. Configure .env before running the worker."
