#!/usr/bin/env bash
# Back up the UK Ops BD Platform database (Linux/macOS, native Postgres or Docker Compose).
#
# Writes three files with one timestamp into the backup directory:
#   uk_ops_bd_<stamp>.dump          pg_dump custom format (-Fc, compressed) - the restorable backup
#   uk_ops_bd_<stamp>.schema.sql    schema-only SQL
#   uk_ops_bd_<stamp>.manifest.txt  size, SHA-256, Postgres version, row counts of the key tables
# Verifies the dump with pg_restore --list, then keeps the newest N sets.
#
# Usage:
#   deploy/backup_db.sh                                   # native Postgres via PG* env / DATABASE_URL, or local defaults
#   deploy/backup_db.sh --compose docker-compose.prod.yml  # run pg_dump inside the compose 'postgres' service
#   deploy/backup_db.sh --dir /backups --keep 10
#
# Native mode reads PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE (or DATABASE_URL), defaulting to
# localhost:5432 postgres/postgres uk_ops_bd. Compose mode reads POSTGRES_PASSWORD from the env file.
set -euo pipefail

COMPOSE=""
BACKUP_DIR="$(cd "$(dirname "$0")/.." && pwd)/backups"
KEEP=5
ENV_FILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --compose) COMPOSE="$2"; shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --dir) BACKUP_DIR="$2"; shift 2 ;;
    --keep) KEEP="$2"; shift 2 ;;
    -h|--help) sed -n 2,16p "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

if [ -n "${DATABASE_URL:-}" ] && [ -z "$COMPOSE" ]; then
  # postgresql://user:pass@host:port/db
  proto_removed="${DATABASE_URL#*://}"
  userpass="${proto_removed%%@*}"; hostpart="${proto_removed#*@}"
  export PGUSER="${userpass%%:*}" PGPASSWORD="${userpass#*:}"
  export PGHOST="${hostpart%%:*}"; rest="${hostpart#*:}"
  export PGPORT="${rest%%/*}" PGDATABASE="${rest#*/}"
fi
export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}" PGDATABASE="${PGDATABASE:-uk_ops_bd}"

mkdir -p "$BACKUP_DIR"
STAMP="$(date +%Y-%m-%d_%H%M%S)"
BASE="$BACKUP_DIR/uk_ops_bd_$STAMP"

if [ -n "$COMPOSE" ]; then
  ENV_ARGS=()
  [ -n "$ENV_FILE" ] && ENV_ARGS=(--env-file "$ENV_FILE")
  dc() { docker compose -f "$COMPOSE" "${ENV_ARGS[@]}" "$@"; }
  pg_dump_cmd()    { dc exec -T postgres pg_dump -U postgres "$@"; }
  psql_cmd()       { dc exec -T postgres psql -U postgres -d uk_ops_bd -tA "$@"; }
  DBLABEL="compose service postgres ($COMPOSE)"
  DBNAME=uk_ops_bd
else
  pg_dump_cmd()    { pg_dump -d "$PGDATABASE" "$@"; }
  psql_cmd()       { psql -d "$PGDATABASE" -tA "$@"; }
  DBLABEL="$PGDATABASE on $PGHOST:$PGPORT"
  DBNAME="$PGDATABASE"
fi

echo "Backing up $DBLABEL to $BASE.dump ..."
pg_dump_cmd --format=custom --compress=6 --no-owner --no-privileges ${COMPOSE:+-d uk_ops_bd} > "$BASE.dump"
pg_dump_cmd --schema-only --no-owner --no-privileges ${COMPOSE:+-d uk_ops_bd} > "$BASE.schema.sql"

# verify
TABLES_WITH_DATA=$(pg_restore --list "$BASE.dump" | grep -c "TABLE DATA" || true)
if [ "$TABLES_WITH_DATA" = "0" ]; then echo "ERROR: dump has no table data" >&2; exit 1; fi

SIZE=$(du -h "$BASE.dump" | cut -f1)
SHA=$(sha256sum "$BASE.dump" | cut -d' ' -f1)
PGV=$(psql_cmd -c "SHOW server_version;" | head -n 1)
DBSIZE=$(psql_cmd -c "SELECT pg_size_pretty(pg_database_size(current_database()));" | head -n 1)
ALEMBIC=$(psql_cmd -c "SELECT CASE WHEN to_regclass('public.alembic_version') IS NULL THEN 'none' ELSE (SELECT version_num FROM alembic_version LIMIT 1) END;" | head -n 1)
COUNTS=$(psql_cmd -c "SELECT t || ',' || (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I', t), false, true, '')))[1]::text
FROM unnest(ARRAY['planning_applications','existing_schemes','scheme_rents','scheme_contracts','scheme_observations','scheme_room_types','scheme_availability','scheme_events','transactions','companies','councils','institutions','hesa_enrolments','hesa_term_time_accommodation','maintenance_loans','yield_benchmarks','pipeline_opportunities','brownfield_sites','title_ownership','scraper_runs','alerts','users']) AS t
WHERE to_regclass('public.' || t) IS NOT NULL;")

{
  echo "UK Ops BD Platform - Database backup manifest"
  echo "================================================"
  echo
  echo "Backup taken:  $(date -u '+%Y-%m-%d %H:%M:%S') UTC ($(date '+%Y-%m-%d %H:%M') local)"
  echo "Source DB:     $DBLABEL"
  echo "Postgres:      $PGV"
  echo "Source size:   $DBSIZE on disk"
  echo "Schema:        alembic revision $ALEMBIC"
  echo
  echo "Files"
  echo "-----"
  echo "  $(basename "$BASE.dump")   $SIZE   pg_dump custom format -Fc -Z 6, $TABLES_WITH_DATA tables with data"
  echo "  $(basename "$BASE.schema.sql")   schema-only SQL"
  echo "  $(basename "$BASE.manifest.txt")   this file"
  echo "  SHA-256 of the dump: $SHA"
  echo
  echo "Restore"
  echo "-------"
  echo "  # Into a fresh database (safe, keeps the current one)"
  echo "  createdb -U postgres uk_ops_bd_restored"
  echo "  pg_restore -U postgres -d uk_ops_bd_restored --no-owner --no-privileges --jobs=4 $(basename "$BASE.dump")"
  echo
  echo "  # Over the current database (drops and recreates every object)"
  echo "  pg_restore -U postgres -d $DBNAME --clean --if-exists --no-owner --jobs=4 $(basename "$BASE.dump")"
  echo
  echo "Row counts at time of backup"
  echo "----------------------------"
  echo "$COUNTS" | awk -F, 'NF==2 { printf "  %-32s %12d\n", $1, $2 }'
} > "$BASE.manifest.txt"

echo "Backup complete: $BASE.dump ($SIZE, $TABLES_WITH_DATA tables with data, verified)"
echo "Manifest:        $BASE.manifest.txt"
echo "Copy the .dump off this machine - a backup on the same disk is not a backup."

# rotate
ls -1t "$BACKUP_DIR"/uk_ops_bd_*.dump 2>/dev/null | tail -n +$((KEEP + 1)) | while read -r old; do
  stem="${old%.dump}"
  echo "  Removing old backup set: $(basename "$stem")"
  rm -f "$stem".dump "$stem".schema.sql "$stem".manifest.txt
done
