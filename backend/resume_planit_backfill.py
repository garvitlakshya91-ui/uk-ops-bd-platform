"""Wait for PlanIt rate limit to clear, then dispatch remaining backfill windows.

Polls a cheap PlanIt endpoint every 60s. As soon as it returns 200 (not 429/403),
dispatches windows 5-6 (or further back, per --start-window) sequentially via
celery, one at a time with cooldowns to avoid burning the budget again.
"""
from __future__ import annotations
import argparse, json, os, subprocess, sys, time
from datetime import date, timedelta

import httpx

PLANIT_PING = "https://www.planit.org.uk/api/applics/json?pg_sz=1&start_date=2026-05-01"


def planit_ok() -> bool:
    try:
        r = httpx.get(PLANIT_PING, timeout=15.0, headers={"User-Agent": "ukops-bd-platform/1.0"})
        return r.status_code == 200
    except Exception:
        return False


def dispatch_window(start: date, end: date, max_records: int = 3000) -> str:
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
    ], capture_output=True, text=True)
    return (out.stdout or out.stderr).strip()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start-window", type=int, default=5,
                   help="First window index to dispatch (0-indexed; defaults to 5 = continuing the 6-window backfill)")
    p.add_argument("--windows", type=int, default=6,
                   help="How many more 14-day windows to dispatch (default 6)")
    p.add_argument("--window-days", type=int, default=14)
    p.add_argument("--max-per-window", type=int, default=3000)
    p.add_argument("--inter-window-cooldown", type=int, default=120,
                   help="Seconds to wait between dispatching each window (default 120s)")
    p.add_argument("--poll-interval", type=int, default=60,
                   help="Seconds between PlanIt probes (default 60). Use 600+ when "
                        "PlanIt has extended the cooldown to avoid re-triggering it.")
    args = p.parse_args()

    # Wait for unblock
    print(f"Polling PlanIt until unblocked (interval={args.poll_interval}s)...")
    waited = 0
    while not planit_ok():
        time.sleep(args.poll_interval)
        waited += args.poll_interval
        print(f"  ...still rate-limited after {waited//60}m {waited%60}s (interval={args.poll_interval}s)")
    print(f"PlanIt available after {waited//60}m wait.")
    print()

    today = date.today()
    for i in range(args.windows):
        w = args.start_window + i
        end = today - timedelta(days=w * args.window_days)
        start = today - timedelta(days=(w + 1) * args.window_days - 1)
        print(f"Dispatching window {w+1}: {start} -> {end}")
        task_id = dispatch_window(start, end, args.max_per_window)
        print(f"  task: {task_id}")
        if i + 1 < args.windows:
            print(f"  cooldown {args.inter_window_cooldown}s before next window...")
            time.sleep(args.inter_window_cooldown)

    print("\nAll dispatched. Monitor with:")
    print("  docker logs uk-ops-bd-platform-celery-worker-1 --since 10m | grep ingest_planit_completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
