<#
.SYNOPSIS
    Back up the UK Ops BD Platform PostgreSQL database (Windows, native Postgres).
.DESCRIPTION
    Writes three files to backups\ with one timestamp:
      uk_ops_bd_<stamp>.dump          pg_dump custom format (-Fc, compressed) - the restorable backup
      uk_ops_bd_<stamp>.schema.sql    schema-only SQL, for diffing and for the git record
      uk_ops_bd_<stamp>.manifest.txt  size, SHA-256, Postgres version, row counts of the key tables
    Verifies the dump with pg_restore --list before declaring success, then keeps the
    newest -KeepLast backups (default 5) and removes older ones.

    Connection settings come from environment variables when set (PGHOST, PGPORT,
    PGUSER, PGPASSWORD, PGDATABASE), else the local defaults below. pg_dump is found
    on PATH or under C:\Program Files\PostgreSQL\<version>\bin.
.EXAMPLE
    .\scripts\backup_db.ps1
    .\scripts\backup_db.ps1 -KeepLast 10 -BackupDir D:\backups\uk_ops_bd
#>

param(
    [string]$BackupDir = "",
    [int]$KeepLast = 5
)

$ErrorActionPreference = "Stop"

# ---- configuration (environment overrides the defaults) ----------------------
$DbHost = if ($env:PGHOST) { $env:PGHOST } else { "localhost" }
$DbPort = if ($env:PGPORT) { $env:PGPORT } else { "5432" }
$DbUser = if ($env:PGUSER) { $env:PGUSER } else { "postgres" }
$DbPass = if ($env:PGPASSWORD) { $env:PGPASSWORD } else { "postgres" }
$DbName = if ($env:PGDATABASE) { $env:PGDATABASE } else { "uk_ops_bd" }
if (-not $BackupDir) { $BackupDir = Join-Path $PSScriptRoot "..\backups" }

function Find-PgTool([string]$name) {
    $onPath = Get-Command "$name.exe" -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }
    $candidates = Get-ChildItem "C:\Program Files\PostgreSQL" -Directory -ErrorAction SilentlyContinue |
        Sort-Object { [int]($_.Name -replace '\D', '0') } -Descending
    foreach ($dir in $candidates) {
        $exe = Join-Path $dir.FullName "bin\$name.exe"
        if (Test-Path $exe) { return $exe }
    }
    throw "$name.exe not found on PATH or under C:\Program Files\PostgreSQL"
}

$PgDump = Find-PgTool "pg_dump"
$PgRestore = Find-PgTool "pg_restore"
$Psql = Find-PgTool "psql"

New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
$BackupDir = (Resolve-Path $BackupDir).Path
$Stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$Base = Join-Path $BackupDir "uk_ops_bd_$Stamp"
$DumpFile = "$Base.dump"
$SchemaFile = "$Base.schema.sql"
$ManifestFile = "$Base.manifest.txt"

$env:PGPASSWORD = $DbPass
$conn = @("-h", $DbHost, "-p", $DbPort, "-U", $DbUser)

Write-Host "Backing up '$DbName' on ${DbHost}:$DbPort to $DumpFile ..." -ForegroundColor Cyan

# ---- 1. full dump, custom format ---------------------------------------------
& $PgDump @conn -d $DbName --format=custom --compress=6 --no-owner --no-privileges -f $DumpFile
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: pg_dump failed (exit $LASTEXITCODE)" -ForegroundColor Red; exit 1 }

# ---- 2. schema-only SQL --------------------------------------------------------
& $PgDump @conn -d $DbName --schema-only --no-owner --no-privileges -f $SchemaFile
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: schema dump failed (exit $LASTEXITCODE)" -ForegroundColor Red; exit 1 }

# ---- 3. verify the dump reads back ---------------------------------------------
$listing = & $PgRestore --list $DumpFile 2>&1
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: pg_restore --list could not read the dump" -ForegroundColor Red; exit 1 }
$tableData = ($listing | Select-String "TABLE DATA").Count

# ---- 4. manifest: size, checksum, versions, row counts ------------------------
$sizeBytes = (Get-Item $DumpFile).Length
$sizeMB = [math]::Round($sizeBytes / 1MB, 1)
$sha = (Get-FileHash $DumpFile -Algorithm SHA256).Hash.ToLower()
$pgVersion = (& $Psql @conn -d $DbName -tAc "SHOW server_version;").Trim()
$dbSize = (& $Psql @conn -d $DbName -tAc "SELECT pg_size_pretty(pg_database_size(current_database()));").Trim()
$alembic = (& $Psql @conn -d $DbName -tAc "SELECT CASE WHEN to_regclass('public.alembic_version') IS NULL THEN 'none' ELSE (SELECT version_num FROM alembic_version LIMIT 1) END;").Trim()

# Row counts of the tables that matter; tables absent on an older schema are skipped.
$countSql = @"
SELECT t || ',' || (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I', t), false, true, '')))[1]::text
FROM unnest(ARRAY['planning_applications','existing_schemes','scheme_rents','scheme_contracts','scheme_observations',
                  'scheme_room_types','scheme_availability','scheme_events','transactions','companies','councils',
                  'institutions','hesa_enrolments','hesa_term_time_accommodation','maintenance_loans','yield_benchmarks',
                  'pipeline_opportunities','brownfield_sites','title_ownership','scraper_runs','alerts','users']) AS t
WHERE to_regclass('public.' || t) IS NOT NULL;
"@
$counts = & $Psql @conn -d $DbName -tAc $countSql

$now = Get-Date
$lines = @(
    "UK Ops BD Platform - Database backup manifest",
    "================================================",
    "",
    "Backup taken:  $($now.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')) UTC ($($now.ToString('yyyy-MM-dd HH:mm')) local)",
    "Source DB:     $DbName on ${DbHost}:$DbPort",
    "Postgres:      $pgVersion",
    "Source size:   $dbSize on disk",
    "Schema:        alembic revision $alembic",
    "",
    "Files",
    "-----",
    "  $(Split-Path $DumpFile -Leaf)   $sizeMB MB   pg_dump custom format -Fc -Z 6, $tableData tables with data",
    "  $(Split-Path $SchemaFile -Leaf)   schema-only SQL",
    "  $(Split-Path $ManifestFile -Leaf)   this file",
    "  SHA-256 of the dump: $sha",
    "",
    "Restore",
    "-------",
    "  # Into a fresh database (safe, keeps the current one)",
    "  createdb -U postgres uk_ops_bd_restored",
    "  pg_restore -U postgres -d uk_ops_bd_restored --no-owner --no-privileges --jobs=4 $(Split-Path $DumpFile -Leaf)",
    "",
    "  # Over the current database (drops and recreates every object)",
    "  pg_restore -U postgres -d $DbName --clean --if-exists --no-owner --jobs=4 $(Split-Path $DumpFile -Leaf)",
    "",
    "Row counts at time of backup",
    "----------------------------"
)
foreach ($row in $counts) {
    if ($row -and $row.Contains(",")) {
        $parts = $row.Split(",")
        $lines += ("  {0,-32} {1,12:N0}" -f $parts[0], [int64]$parts[1])
    }
}
$lines | Set-Content -Path $ManifestFile -Encoding UTF8

Write-Host "Backup complete: $DumpFile ($sizeMB MB, $tableData tables with data, verified)" -ForegroundColor Green
Write-Host "Manifest:        $ManifestFile" -ForegroundColor Green
Write-Host "Copy the .dump off this machine (cloud drive or another disk) - a backup on the same disk is not a backup." -ForegroundColor Yellow

# ---- 5. rotate: keep the newest $KeepLast sets --------------------------------
$sets = Get-ChildItem $BackupDir -Filter "uk_ops_bd_*.dump" | Sort-Object LastWriteTime -Descending
if ($sets.Count -gt $KeepLast) {
    foreach ($old in ($sets | Select-Object -Skip $KeepLast)) {
        $stem = [System.IO.Path]::GetFileNameWithoutExtension($old.Name)
        Write-Host "  Removing old backup set: $stem" -ForegroundColor Yellow
        Remove-Item (Join-Path $BackupDir "$stem.*") -Force -ErrorAction SilentlyContinue
    }
}
Write-Host "Done. $([math]::Min($sets.Count, $KeepLast)) backup set(s) kept in $BackupDir" -ForegroundColor Cyan
