#!/usr/bin/env bash
# One-command city data collection: planning pipeline, listings, rents,
# reconciliation, report. Needs outbound network access to:
#   www.planit.org.uk, www.accommodationforstudents.com, sturents.com,
#   www.unitestudents.com, api.postcodes.io
#
# Usage: ./collect_city.sh Birmingham birmingham
#        (council name, city slug)
set -uo pipefail

COUNCIL=${1:?council name, e.g. Birmingham}
SLUG=${2:?city slug, e.g. birmingham}
cd "$(dirname "$0")"
PY=${PY:-.venv/bin/python}
[ -x "$PY" ] || PY=python

step() { echo; echo "===== $* ====="; }

step "1/7 Planning pipeline — PlanIt keyword fetch (8 years)"
$PY fetch_city_pbsa_planit.py --council "$COUNCIL" --years 8

step "2/7 Parse PBSA beds + expected delivery years"
$PY parse_pbsa_beds.py --council "$COUNCIL"

step "3/7 AccommodationForStudents listings"
$PY run_afs_scrape.py --city "$SLUG" || echo "AFS scrape failed — continuing"

step "4/7 StuRents listings"
$PY run_sturents_scrape.py --city "$SLUG" || echo "StuRents scrape failed — continuing"

step "5/7 Unite Students room-level rents (all Unite cities)"
$PY run_operator_room_rents.py --operator unite_students || echo "Unite scrape failed — continuing"

step "6/7 Attach scraped rents to census schemes (append-only)"
AFS_FILE="data/afs/$SLUG.jsonl"; ST_FILE="data/sturents/$SLUG.jsonl"
ARGS=()
[ -f "$AFS_FILE" ] && ARGS+=(--afs "$AFS_FILE")
[ -f "$ST_FILE" ] && ARGS+=(--sturents "$ST_FILE")
if [ ${#ARGS[@]} -gt 0 ]; then
    $PY load_rents_by_match.py --city "$COUNCIL" "${ARGS[@]}"
else
    echo "no listing files to load"
fi

step "7/7 Reconcile census vs benchmark, rebuild report"
BENCH=$(ls data/benchmarks/${SLUG}_*.json 2>/dev/null | head -1)
if [ -n "$BENCH" ]; then
    $PY reconcile_census.py --benchmark "$BENCH" --out "data/benchmarks/reconcile_${SLUG}.json"
    echo "(review the diff, then re-run with --apply if the gaps should be filled)"
fi
$PY report/build_city_report.py --council "$COUNCIL"
echo; echo "Done."
