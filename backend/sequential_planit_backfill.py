"""Dispatch PlanIt backfill windows one at a time, waiting for each to
finish before sending the next. Avoids saturating PlanIt's rate limit by
keeping concurrency=1 and adding a cooldown between windows.

Usage::

    python sequential_planit_backfill.py --start-window 7 --windows 3 \
        --window-days 14 --cooldown 300 --poll-interval 600
"""
from __future__ import annotations
import argparse, json, subprocess, sys, time
from datetime import date, timedelta

import httpx

PING = "https://www.planit.org.uk/api/applics/json?pg_sz=1&start_date=2026-05-01"


def planit_ok() -> bool:
    try:
        r = httpx.get(PING, timeout=15.0, headers={"User-Agent": "ukops-bd-platform/1.0"})
        return r.status_code == 200
    except Exception:
        return False


def wait_for_planit(poll: int) -> None:
    print(f"Waiting for PlanIt (poll every {poll}s)...")
    n = 0
    while not planit_ok():
        time.sleep(poll)
        n += poll
        print(f"  ...still blocked after {n//60}m {n%60}s")


def worker_busy() -> bool:
    """Check if there's an active ingest_planit_applications task."""
    try:
        out = subprocess.run(
            ["docker", "exec", "uk-ops-bd-platform-celery-worker-1",
             "celery", "-A", "app.tasks:celery_app", "inspect", "active"],
            capture_output=True, text=True, timeout=20,
        )
        return "ingest_planit_applications" in (out.stdout or "")
    except Exception:
        return False


def dispatch(start: date, end: date, max_records: int) -> str:
    kwargs = json.dumps({
        "days_back": 0,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "max_records": max_records,
        "page_size": 200,
    })
    out = subprocess.run([
        "docker", "exec", "uk-ops-bd-platform-celery-worker-1",
        "celery", "-A", "app.tasks:celery_app", "call",
        "app.tasks.scraping_tasks.ingest_planit_applications",
        "--kwargs", kwargs,
    ], capture_output=True, text=True, timeout=30)
    return (out.stdout or out.stderr).strip().splitlines()[-1]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start-window", type=int, required=True)
    p.add_argument("--windows", type=int, required=True)
    p.add_argument("--window-days", type=int, default=14)
    p.add_argument("--max-per-window", type=int, default=3000)
    p.add_argument("--cooldown", type=int, default=300,
                   help="Seconds between windows (default 300s = 5 min)")
    p.add_argument("--poll-interval", type=int, default=600,
                   help="When PlanIt is blocked, how often to re-check (default 600s = 10m)")
    p.add_argument("--worker-poll", type=int, default=30,
                   help="How often to poll worker for task completion (default 30s)")
    args = p.parse_args()

    today = date.today()

    # First, wait for any currently-running task to finish
    if worker_busy():
        print("Worker is busy. Waiting for current task to complete...")
        while worker_busy():
            time.sleep(args.worker_poll)
            print(f"  ...worker still busy")
        print("Worker idle.")

    for i in range(args.windows):
        w = args.start_window + i
        end = today - timedelta(days=w * args.window_days)
        start = today - timedelta(days=(w + 1) * args.window_days - 1)

        # Wait if PlanIt is currently rate-limited
        if not planit_ok():
            wait_for_planit(args.poll_interval)

        print()
        print(f"=== Window {w+1}: {start} to {end} ===")
        task_id = dispatch(start, end, args.max_per_window)
        print(f"  dispatched task: {task_id}")

        # Wait for this window to finish before dispatching the next
        print("  Polling worker for completion...")
        while worker_busy():
            time.sleep(args.worker_poll)
        print(f"  window {w+1} done")

        if i + 1 < args.windows:
            print(f"  cooling {args.cooldown}s before next window...")
            time.sleep(args.cooldown)

    print()
    print("All windows dispatched and completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
