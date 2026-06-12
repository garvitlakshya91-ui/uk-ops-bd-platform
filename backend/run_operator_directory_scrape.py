"""Runner for the PBSA operator-website directory scraper (dev step 3).

Usage:
    python run_operator_directory_scrape.py --brand vita_student
    python run_operator_directory_scrape.py --all
    python run_operator_directory_scrape.py --brand host --limit 3   # parser smoke test

Writes one JSONL file per brand to data/operator_directories/<brand>.jsonl
(one property per line) and prints a per-brand summary.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.scrapers.operator_directory_scraper import (  # noqa: E402
    BRAND_CONFIGS,
    OperatorDirectoryScraper,
)

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
)

OUT_DIR = Path(__file__).resolve().parent / "data" / "operator_directories"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brand", help="single brand key", choices=sorted(BRAND_CONFIGS))
    ap.add_argument("--all", action="store_true", help="scrape every configured brand")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap property pages per brand (for parser testing)")
    args = ap.parse_args()

    if not args.brand and not args.all:
        ap.error("pass --brand <name> or --all")

    brands = [args.brand] if args.brand else list(BRAND_CONFIGS)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    with OperatorDirectoryScraper() as scraper:
        for brand in brands:
            try:
                result = scraper.scrape_brand(brand, limit=args.limit)
            except Exception as e:  # keep going across brands
                print(f"[{brand}] CRASH: {type(e).__name__}: {e}")
                rows.append((brand, "error", 0, 0.0, 0.0))
                continue

            out_path = OUT_DIR / f"{brand}.jsonl"
            if result.properties:
                with out_path.open("w", encoding="utf-8") as fh:
                    for prop in result.properties:
                        fh.write(json.dumps(prop.to_json(), ensure_ascii=False) + "\n")

            rows.append((
                brand, result.status, len(result.properties),
                result.postcode_fill, result.rent_fill,
            ))
            print(
                f"[{brand}] status={result.status} properties={len(result.properties)} "
                f"pages={result.pages_fetched} postcode_fill={result.postcode_fill:.0f}% "
                f"rent_fill={result.rent_fill:.0f}%"
                + (f" -> {out_path}" if result.properties else "")
            )

    print("\n=== SUMMARY ===")
    print(f"{'brand':<28} {'status':<16} {'props':>5} {'pc%':>5} {'rent%':>6}")
    for brand, status, n, pc, rent in rows:
        print(f"{brand:<28} {status:<16} {n:>5} {pc:>4.0f}% {rent:>5.0f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
