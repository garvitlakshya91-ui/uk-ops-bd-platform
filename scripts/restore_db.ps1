<#
.SYNOPSIS
    Restore the UK Ops BD Platform database from a backup.
.DESCRIPTION
    Restores a pg_dump backup file. If no file is specified, uses the most recent backup.
.EXAMPLE
    .\scripts\restore_db.ps1
    .\scripts\restore_db.ps1 -BackupFile .\backups\uk_ops_bd_2026-04-05_020000.dump
#>

param(
    [string]$BackupFile = ""
)

$ErrorActionPreference = "Stop"

# Configuration
$PgRestore = "C:\Program Files\PostgreSQL\18\bin\pg_restore.exe"
$Psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
$DbHost = "localhost"
$DbPort = "5432"
$DbUser = "postgres"
$DbPass = "postgres"
$DbName = "uk_ops_bd"
$BackupDir = Join-Path $PSScriptRoot "..\backups"

$env:PGPASSWORD = $DbPass

# Find backup file
if (-not $BackupFile) {
    $Latest = Get-ChildItem $BackupDir -Filter "uk_ops_bd_*.dump" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $Latest) {
        Write-Host "ERROR: No backup files found in $BackupDir" -ForegroundColor Red
        exit 1
    }
    $BackupFile = $Latest.FullName
    Write-Host "Using most recent backup: $($Latest.Name)" -ForegroundColor Cyan
}

if (-not (Test-Path $BackupFile)) {
    Write-Host "ERROR: Backup file not found: $BackupFile" -ForegroundColor Red
    exit 1
}

$SizeMB = [math]::Round((Get-Item $BackupFile).Length / 1MB, 1)
Write-Host "Restoring from: $BackupFile ($SizeMB MB)" -ForegroundColor Cyan
Write-Host ""
Write-Host "WARNING: This will DROP and recreate the '$DbName' database." -ForegroundColor Red
$confirm = Read-Host "Type 'yes' to continue"
if ($confirm -ne "yes") {
    Write-Host "Aborted." -ForegroundColor Yellow
    exit 0
}

# Drop and recreate database
Write-Host "Dropping database '$DbName'..." -ForegroundColor Yellow
& $Psql -h $DbHost -p $DbPort -U $DbUser -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DbName' AND pid <> pg_backend_pid();" 2>$null
& $Psql -h $DbHost -p $DbPort -U $DbUser -d postgres -c "DROP DATABASE IF EXISTS $DbName;"
& $Psql -h $DbHost -p $DbPort -U $DbUser -d postgres -c "CREATE DATABASE $DbName;"

Write-Host "Restoring..." -ForegroundColor Cyan
& $PgRestore -h $DbHost -p $DbPort -U $DbUser -d $DbName --no-owner --no-acl --jobs=4 $BackupFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: pg_restore completed with warnings (exit code $LASTEXITCODE). This is often normal." -ForegroundColor Yellow
} else {
    Write-Host "Restore complete." -ForegroundColor Green
}

# Show table counts
Write-Host ""
Write-Host "Table counts after restore:" -ForegroundColor Cyan
& $Psql -h $DbHost -p $DbPort -U $DbUser -d $DbName -c "SELECT 'existing_schemes' as tbl, count(*) FROM existing_schemes UNION ALL SELECT 'scheme_contracts', count(*) FROM scheme_contracts UNION ALL SELECT 'companies', count(*) FROM companies UNION ALL SELECT 'planning_applications', count(*) FROM planning_applications UNION ALL SELECT 'councils', count(*) FROM councils ORDER BY tbl;"
