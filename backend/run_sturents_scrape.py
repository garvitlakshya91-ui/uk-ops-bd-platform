"""Run the StuRents discovery scrape across the focus cities.

Writes one JSONL file per city under data/sturents/ as it goes
(crash-safe), then prints a per-city summary with PBSA-candidate counts.

Usage:
    python run_sturents_scrape.py --city canterbury --limit 30   # pilot
    python run_sturents_scrape.py --city canterbury              # full city
    python run_sturents_scrape.py --all                          # all 16
    python run_sturents_scrape.py --all --skip-done              # resume
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from app.scrapers.sturents_scraper import (
    FOCUS_CITY_SLUGS,
    SturentsScraper,
)

OUT_DIR = Path(__file__).parent / "data" / "sturents"


def scrape_city(scraper: SturentsScraper, city: str, limit=None) -> dict:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{city}.jsonl"
    n = 0
    pbsa = 0
    agents = Counter()
    with open(out_path, "w", encoding="utf-8") as f:
        for listing in scraper.scrape_city(city, limit=limit):
            f.write(json.dumps(listing.to_json(), ensure_ascii=False) + "\n")
            n += 1
            if listing.is_pbsa_candidate:
                pbsa += 1
            if listing.agent:
                agents[listing.agent] += 1
            if n % 25 == 0:
                print(f"    ...{n} listings ({pbsa} PBSA candidates)", flush=True)
    return {"city": city, "listings": n, "pbsa_candidates": pbsa,
            "top_agents": agents.most_common(8), "file": str(out_path)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", help="Single city slug to scrape")
    ap.add_argument("--all", action="store_true", help="All 16 focus cities")
    ap.add_argument("--limit", type=int, help="Max listings per city (pilot)")
    ap.add_argument("--skip-done", action="store_true",
                    help="Skip cities whose JSONL already exists")
    args = ap.parse_args()

    if not args.city and not args.all:
        ap.error("pass --city <slug> or --all")

    cities = [args.city] if args.city else FOCUS_CITY_SLUGS
    results = []
    with SturentsScraper() as scraper:
        for city in cities:
            if args.skip_done and (OUT_DIR / f"{city}.jsonl").exists():
                print(f"[skip] {city} (already scraped)")
                continue
            print(f"[scrape] {city}...", flush=True)
            r = scrape_city(scraper, city, limit=args.limit)
            results.append(r)
            print(f"  -> {r['listings']} listings, {r['pbsa_candidates']} PBSA candidates")
            for agent, cnt in r["top_agents"]:
                print(f"       {cnt:>4}  {agent}")

    print("\n=== SUMMARY ===")
    for r in results:
        print(f"  {r['city']:<16} {r['listings']:>5} listings  "
              f"{r['pbsa_candidates']:>4} PBSA candidates")


if __name__ == "__main__":
    main()
