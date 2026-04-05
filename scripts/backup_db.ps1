<#
.SYNOPSIS
    Backup the UK Ops BD Platform PostgreSQL database.
.DESCRIPTION
    Creates a compressed pg_dump backup in the backups/ directory.
    Keeps the last 5 backups and removes older ones.
.EXAMPLE
    .\scripts\backup_db.ps1
#>

$ErrorActionPreference = "Stop"

# Configuration
$PgDump = "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe"
$DbHost = "localhost"
$DbPort = "5432"
$DbUser = "postgres"
$DbPass = "postgres"
$DbName = "uk_ops_bd"
$BackupDir = Join-Path $PSScriptRoot "..\backups"
$MaxBackups = 5

# Ensure backup directory exists
New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null

# Generate timestamped filename
$Timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$BackupFile = Join-Path $BackupDir "uk_ops_bd_$Timestamp.dump"

Write-Host "Backing up database '$DbName' to $BackupFile ..." -ForegroundColor Cyan

# Run pg_dump
$env:PGPASSWORD = $DbPass
& $PgDump -h $DbHost -p $DbPort -U $DbUser -d $DbName --format=custom --compress=6 -f $BackupFile

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: pg_dump failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit 1
}

$Size = (Get-Item $BackupFile).Length
$SizeMB = [math]::Round($Size / 1MB, 1)
Write-Host "Backup complete: $BackupFile ($SizeMB MB)" -ForegroundColor Green

# Rotate old backups — keep only the most recent $MaxBackups
$Backups = Get-ChildItem $BackupDir -Filter "uk_ops_bd_*.dump" | Sort-Object LastWriteTime -Descending
if ($Backups.Count -gt $MaxBackups) {
    $ToRemove = $Backups | Select-Object -Skip $MaxBackups
    foreach ($old in $ToRemove) {
        Write-Host "  Removing old backup: $($old.Name)" -ForegroundColor Yellow
        Remove-Item $old.FullName -Force
    }
}

Write-Host "Done. $($Backups.Count) backup(s) in $BackupDir" -ForegroundColor Cyan
