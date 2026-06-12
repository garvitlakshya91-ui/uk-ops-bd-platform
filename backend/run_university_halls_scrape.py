"""Runner for the university halls discovery scraper.

Usage:
    python run_university_halls_scrape.py --city worcester
    python run_university_halls_scrape.py --all

Writes JSONL per city to data/university_halls/<city>.jsonl and prints a
per-city summary (hall count, partner schemes, operators, follow-ups).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from app.scrapers.university_halls_scraper import (
    CITY_UNIVERSITIES,
    UniversityHallsScraper,
)

OUT_DIR = os.path.join("data", "university_halls")


def run_city(scraper: UniversityHallsScraper, city: str) -> dict:
    halls, stats = scraper.scrape_city(city)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{city}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for hall in halls:
            f.write(json.dumps(hall.to_json(), ensure_ascii=False) + "\n")
    return {"city": city, "path": path, "halls": halls, "stats": stats}


def print_summary(result: dict) -> None:
    city = result["city"]
    halls = result["halls"]
    partner = [h for h in halls if h.ownership == "partner"]
    operators = sorted({h.operator for h in halls if h.operator})
    print(f"\n=== {city}  ({len(halls)} halls, {len(partner)} partner)")
    print(f"    file: {result['path']}")
    for s in result["stats"]:
        print(
            f"    - {s['university']}: {s['halls']} halls "
            f"({s['partner']} partner), {s['pages_fetched']} pages, "
            f"{s['errors']} errors"
        )
        if s.get("operators"):
            print(f"        operators: {', '.join(s['operators'])}")
        if s.get("config_notes"):
            print(f"        note: {s['config_notes']}")
        for fu in s.get("follow_up", []):
            print(f"        FOLLOW-UP: {fu}")
    if operators:
        print(f"    operators found: {', '.join(operators)}")


def main() -> int:
    ap = argparse.ArgumentParser(description="University halls scraper")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--city", choices=sorted(CITY_UNIVERSITIES))
    g.add_argument("--all", action="store_true")
    args = ap.parse_args()

    cities = sorted(CITY_UNIVERSITIES) if args.all else [args.city]
    totals = {"halls": 0, "partner": 0}
    with UniversityHallsScraper() as scraper:
        for city in cities:
            try:
                result = run_city(scraper, city)
            except Exception as e:  # keep going city-by-city
                print(f"\n=== {city}  FAILED: {type(e).__name__}: {e}")
                continue
            print_summary(result)
            totals["halls"] += len(result["halls"])
            totals["partner"] += sum(
                1 for h in result["halls"] if h.ownership == "partner"
            )
    print(
        f"\nTOTAL: {totals['halls']} halls across {len(cities)} cities "
        f"({totals['partner']} partner/nominated schemes)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
