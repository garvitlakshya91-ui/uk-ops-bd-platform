#!/usr/bin/env bash
# Quarterly rent capture for one city: the observation that builds the
# matched rent panel. Every row is appended (never overwritten), so each
# run is a permanent point in the series. The November run is the
# industry pricing window and must not be skipped.
#
# Usage: ./capture_rents.sh Birmingham birmingham
set -uo pipefail

COUNCIL=${1:?council name}; SLUG=${2:?city slug}
cd "$(dirname "$0")"
PY=${PY:-.venv/bin/python}
[ -x "$PY" ] || PY=python

echo "== AFS listings ($SLUG)"
$PY run_afs_scrape.py --city "$SLUG" || echo "AFS failed — continuing"

echo "== StuRents listings (browser)"
$PY run_sturents_scrape.py --city "$SLUG" --browser || echo "StuRents failed — continuing"

echo "== Operator brand directories (all brands)"
$PY run_operator_directory_scrape.py --all || echo "opdir failed — continuing"

echo "== Unite room-level rents"
$PY run_operator_room_rents.py --operator unite || echo "Unite failed — continuing"

echo "== Attach to census (append-only)"
ARGS=()
[ -f "data/afs/$SLUG.jsonl" ] && ARGS+=(--afs "data/afs/$SLUG.jsonl")
[ -f "data/sturents/$SLUG.jsonl" ] && ARGS+=(--sturents "data/sturents/$SLUG.jsonl")
compgen -G "data/operator_directories/*.jsonl" >/dev/null && ARGS+=(--opdir data/operator_directories/*.jsonl)
$PY load_rents_by_match.py --city "$COUNCIL" "${ARGS[@]}"

if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    echo "== Room-level tiers (AI extraction)"
    $PY ai_room_rents.py --city "$COUNCIL" || echo "room tiers failed — continuing"
else
    echo "== Skipping AI room tiers (no ANTHROPIC_API_KEY)"
fi
echo "Capture complete: $COUNCIL"
