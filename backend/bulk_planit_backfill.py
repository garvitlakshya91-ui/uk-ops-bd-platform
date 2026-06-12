"""Aggressive PlanIt backfill — chunked, rate-limit aware.

Runs multiple sequential 14-day windows of the ``ingest_planit_applications``
celery task locally (in-process, not via celery) so the user can monitor
output directly. Each window pulls fresh records, commits incrementally, and
respects PlanIt's burst-then-cool rate limits.

Strategy:
- Window 1: today - 14d  ->  today
- Window 2: today - 28d  ->  today - 15d
- Window 3: today - 42d  ->  today - 29d
- Window 4: today - 56d  ->  today - 43d
- Window 5: today - 70d  ->  today - 57d
- Window 6: today - 84d  ->  today - 71d

Total: 84-day backfill across 6 windows. Each window aims for ~3000 records.

Usage::

    python bulk_planit_backfill.py [--windows 6] [--window-days 14] [--max-per-window 3000]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

# Run the celery task in-process via .apply() (eager execution)
from app.tasks.scraping_tasks import ingest_planit_applications


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--windows", type=int, default=6)
    p.add_argument("--window-days", type=int, default=14)
    p.add_argument("--max-per-window", type=int, default=3000)
    p.add_argument("--cooldown-secs", type=int, default=15,
                   help="Sleep between windows to let PlanIt rate budget reset")
    args = p.parse_args()

    today = date.today()

    grand_new = 0
    grand_found = 0
    grand_noise = 0

    for w in range(args.windows):
        end = today - timedelta(days=w * args.window_days)
        start = today - timedelta(days=(w + 1) * args.window_days - 1)
        if start > end:
            start, end = end, start
        print()
        print("=" * 60)
        print(f"WINDOW {w+1}/{args.windows}: {start} to {end}  (max {args.max_per_window})")
        print("=" * 60)
        t0 = time.time()
        try:
            apply_result = ingest_planit_applications.apply(kwargs={
                "days_back": 0,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "max_records": args.max_per_window,
                "page_size": 200,
                "states": ("Undecided", "Permitted"),
            })
            result = apply_result.get(disable_sync_subtasks=False)
        except Exception as exc:
            print(f"  WINDOW FAILED: {exc}")
            continue
        elapsed = time.time() - t0
        print(f"  Elapsed: {elapsed:.0f}s")
        print(f"  Result:  found={result.get('found',0):,}, new={result.get('new',0):,}, "
              f"skipped_noise={result.get('skipped_noise',0):,}, "
              f"skipped_existing={result.get('skipped_existing',0):,}, "
              f"errors={result.get('errors',0):,}")
        grand_new += result.get("new", 0)
        grand_found += result.get("found", 0)
        grand_noise += result.get("skipped_noise", 0)

        if w + 1 < args.windows:
            print(f"  Cooling {args.cooldown_secs}s before next window...")
            time.sleep(args.cooldown_secs)

    print()
    print("=" * 60)
    print(f"TOTAL across {args.windows} windows:")
    print(f"  fetched: {grand_found:,}")
    print(f"  new:     {grand_new:,}")
    print(f"  noise:   {grand_noise:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
