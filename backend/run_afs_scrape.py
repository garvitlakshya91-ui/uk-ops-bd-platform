"""Run the AccommodationForStudents PBSA discovery scrape.

Writes one JSONL per city under data/afs/ and prints per-city summary
with operator breakdown.

Usage:
    python run_afs_scrape.py --city canterbury
    python run_afs_scrape.py --all
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from app.scrapers.afs_scraper import FOCUS_CITY_SLUGS, AfsScraper

OUT_DIR = Path(__file__).parent / "data" / "afs"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="Single city slug")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if not args.city and not args.all:
        ap.error("pass --city <slug> or --all")

    cities = [args.city] if args.city else FOCUS_CITY_SLUGS
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = []

    with AfsScraper() as scraper:
        for city in cities:
            print(f"[scrape] {city}...", flush=True)
            ops = Counter()
            n = 0
            out_path = OUT_DIR / f"{city}.jsonl"
            with open(out_path, "w", encoding="utf-8") as f:
                for rec in scraper.scrape_city(city):
                    f.write(json.dumps(rec.to_json(), ensure_ascii=False) + "\n")
                    n += 1
                    if rec.operator:
                        ops[rec.operator] += 1
            summary.append((city, n, ops))
            print(f"  -> {n} properties")
            for op, cnt in ops.most_common(10):
                print(f"       {cnt:>3}  {op}")

    print("\n=== SUMMARY ===")
    for city, n, ops in summary:
        named = sum(ops.values())
        print(f"  {city:<16} {n:>4} properties   {named:>4} with operator   {len(ops):>3} distinct operators")


if __name__ == "__main__":
    main()
