$procs = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%celery%beat%"'
foreach ($p in $procs) {
    Stop-Process -Id $p.ProcessId -Force
    Write-Host "Killed PID $($p.ProcessId)"
}
if (-not $procs) { Write-Host "No celery beat process found" }
