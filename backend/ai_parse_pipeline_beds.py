"""AI bed-count extraction for PBSA applications the regex couldn't read.

Outline consents phrase capacity in ways no regex catches ("circa 500
studio units", "up to 850 student rooms across two blocks"). This sends
each NULL-bed PBSA application's description to Claude with a strict
extract-only prompt: a number is returned only when the text states one.

Requires ANTHROPIC_API_KEY.

Usage:
    python ai_parse_pipeline_beds.py --council Birmingham [--dry-run] [--limit 100]
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from sqlalchemy import or_

from app.database import SessionLocal
from app.models.models import Council, PlanningApplication

MODEL = "claude-haiku-4-5"
PROMPT = """Extract the student-accommodation bed capacity from this UK planning
application description. Rules:
- Return the TOTAL number of student bedspaces/beds/student rooms if the text
  states one (studios count as 1 bed each; "N cluster flats of M beds" = N*M).
- If the text gives only dwellings/apartments without saying they are student
  accommodation beds, or states no number at all, return null.
- Never estimate or infer a number the text does not support.
Respond with ONLY a JSON object: {"beds": <int or null>, "quote": "<the phrase
the number came from, or null>"}

Description:
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set — add it to the environment")

    import anthropic
    client = anthropic.Anthropic()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.council)).first()
    if council is None:
        raise SystemExit(f"Council {args.council!r} not found")

    apps = (
        db.query(PlanningApplication)
        .filter(PlanningApplication.council_id == council.id,
                or_(PlanningApplication.is_pbsa.is_(True),
                    PlanningApplication.scheme_type == "PBSA"),
                PlanningApplication.pbsa_beds.is_(None),
                PlanningApplication.description.isnot(None))
        .limit(args.limit)
        .all()
    )
    print(f"{len(apps)} NULL-bed PBSA applications to try")

    set_n = null_n = err_n = 0
    for a in apps:
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=200,
                messages=[{"role": "user", "content": PROMPT + a.description[:4000]}],
            )
            raw = msg.content[0].text.strip()
            raw = raw[raw.find("{"):raw.rfind("}") + 1]
            result = json.loads(raw)
        except Exception as exc:
            err_n += 1
            print(f"  {a.reference}: ERR {str(exc)[:100]}")
            continue
        beds = result.get("beds")
        if isinstance(beds, int) and 10 <= beds <= 4000:
            set_n += 1
            print(f"  {a.reference}: beds={beds}  ({str(result.get('quote'))[:60]})")
            if not args.dry_run:
                a.pbsa_beds = beds
        else:
            null_n += 1
    if not args.dry_run:
        db.commit()
    db.close()
    print(f"\nset {set_n}, no number stated {null_n}, errors {err_n}")


if __name__ == "__main__":
    main()
