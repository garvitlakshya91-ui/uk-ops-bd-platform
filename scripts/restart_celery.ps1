# Kill existing celery processes
$procs = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%celery%"'
foreach ($p in $procs) {
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    Write-Host "Killed celery PID $($p.ProcessId)"
}
Start-Sleep 2

# Start worker
Set-Location C:\Users\garvi\uk-ops-bd-platform\backend
Start-Process python -ArgumentList '-m','celery','-A','app.tasks:celery_app','worker','-l','info','-Q','default,scraping,enrichment,scoring','--pool=solo' -NoNewWindow -RedirectStandardOutput celery_worker.log -RedirectStandardError celery_worker_err.log
Write-Host "Celery worker started"

# Start beat
Start-Process python -ArgumentList '-m','celery','-A','app.tasks:celery_app','beat','-l','info' -NoNewWindow -RedirectStandardOutput celery_beat.log -RedirectStandardError celery_beat_err.log
Write-Host "Celery beat started"

Start-Sleep 3
Write-Host "`n=== WORKER STATUS ==="
Get-Content celery_worker_err.log -Tail 5
