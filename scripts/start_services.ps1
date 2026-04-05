<#
.SYNOPSIS
    Start all UK Ops BD Platform services (Redis, Celery Worker, Celery Beat, Backend, Frontend).
.DESCRIPTION
    Starts Redis via Docker, then Celery worker + beat natively alongside the
    existing local PostgreSQL, FastAPI backend, and Next.js frontend.
.EXAMPLE
    .\scripts\start_services.ps1
#>

$ErrorActionPreference = "Stop"
$BackendDir = Join-Path $PSScriptRoot "..\backend"
$FrontendDir = Join-Path $PSScriptRoot "..\frontend"

Write-Host "=== UK Ops BD Platform — Starting Services ===" -ForegroundColor Cyan

# --- 1. Redis via Docker ---
Write-Host "`n[1/5] Redis..." -ForegroundColor Yellow
$redis = docker ps -q -f name=uk-ops-redis 2>$null
if ($redis) {
    Write-Host "  Already running" -ForegroundColor Green
} else {
    $stopped = docker ps -aq -f name=uk-ops-redis 2>$null
    if ($stopped) {
        docker start uk-ops-redis | Out-Null
    } else {
        docker run -d --name uk-ops-redis -p 6379:6379 --restart unless-stopped redis:7 | Out-Null
    }
    Start-Sleep 2
    $pong = docker exec uk-ops-redis redis-cli ping 2>$null
    if ($pong -eq "PONG") {
        Write-Host "  Started and responding" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Redis not responding" -ForegroundColor Red
    }
}

# --- 2. Celery Worker ---
Write-Host "`n[2/5] Celery Worker..." -ForegroundColor Yellow
$existing = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%celery%worker%"' -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  Already running (PID $($existing.ProcessId))" -ForegroundColor Green
} else {
    Push-Location $BackendDir
    Start-Process python -ArgumentList '-m','celery','-A','app.tasks:celery_app','worker','-l','info','-Q','default,scraping,enrichment,scoring','--pool=solo' `
        -NoNewWindow -RedirectStandardOutput celery_worker.log -RedirectStandardError celery_worker_err.log
    Pop-Location
    Write-Host "  Started (logs: backend\celery_worker.log)" -ForegroundColor Green
}

# --- 3. Celery Beat ---
Write-Host "`n[3/5] Celery Beat..." -ForegroundColor Yellow
$existing = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%celery%beat%"' -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  Already running (PID $($existing.ProcessId))" -ForegroundColor Green
} else {
    Push-Location $BackendDir
    Start-Process python -ArgumentList '-m','celery','-A','app.tasks:celery_app','beat','-l','info','--scheduler','celery.beat:PersistentScheduler','--schedule','celerybeat-schedule.db' `
        -NoNewWindow -RedirectStandardOutput celery_beat.log -RedirectStandardError celery_beat_err.log
    Pop-Location
    Write-Host "  Started (logs: backend\celery_beat.log)" -ForegroundColor Green
}

# --- 4. Backend (FastAPI) ---
Write-Host "`n[4/5] Backend API..." -ForegroundColor Yellow
$existing = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%uvicorn app.main%"' -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  Already running on http://localhost:8000 (PID $($existing.ProcessId))" -ForegroundColor Green
} else {
    Push-Location $BackendDir
    Start-Process python -ArgumentList '-m','uvicorn','app.main:app','--host','0.0.0.0','--port','8000' `
        -NoNewWindow
    Pop-Location
    Write-Host "  Started on http://localhost:8000" -ForegroundColor Green
}

# --- 5. Frontend (Next.js) ---
Write-Host "`n[5/5] Frontend..." -ForegroundColor Yellow
$existing = Get-CimInstance Win32_Process -Filter 'CommandLine LIKE "%next%dev%"' -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  Already running on http://localhost:3000 (PID $($existing.ProcessId))" -ForegroundColor Green
} else {
    Write-Host "  Not running — start manually: cd frontend && npm run dev" -ForegroundColor DarkYellow
}

Write-Host "`n=== All services started ===" -ForegroundColor Cyan
Write-Host @"

  Redis:          localhost:6379 (Docker)
  PostgreSQL:     localhost:5432 (local)
  Backend API:    http://localhost:8000
  Frontend:       http://localhost:3000
  Celery Worker:  Listening on queues: default, scraping, enrichment, scoring
  Celery Beat:    26 scheduled tasks active

  Logs:
    backend\celery_worker.log / celery_worker_err.log
    backend\celery_beat.log / celery_beat_err.log
"@
