"""AI bed-count extraction for PBSA applications the regex extractor could not read.

Outline consents phrase capacity in ways no regex catches ("circa 500
studio units", "up to 850 student rooms across two blocks"). Each
NULL-bed PBSA application's description goes to Claude with an
extract-only schema. Every answer is written to unit_extractions with
the model's quote and a confidence; the cached pbsa_beds is set only
when the quote is found verbatim in the description, contains the
number, and the confidence clears the cache bar. Everything else waits
for a person.

Requires ANTHROPIC_API_KEY (plus ANTHROPIC_WORKSPACE_ID for a user-scoped key).

Usage:
    python ai_parse_pipeline_beds.py --council Birmingham [--dry-run] [--limit 100] [--ids 1 2]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from pydantic import BaseModel, Field
from sqlalchemy import or_

from app.database import SessionLocal
from app.extraction.units import extract_units
from app.models.models import Council, PlanningApplication, UnitExtractionRecord
from app.models.observations import CACHE_MIN_CONFIDENCE

# The bulk extractor runs on the current Haiku: thousands of short
# descriptions, one number each.
MODEL = "claude-haiku-5-5"
PROMPT = """Read this UK planning application description and report the student-accommodation
bed capacity it states. Rules:
- beds is the TOTAL number of student bedspaces/beds/student rooms if the text states one
  (studios count as 1 bed each; "N cluster flats of M beds" = N*M, say so in reason).
- If the text gives only dwellings/apartments without saying they are student accommodation,
  or states no number at all, beds is null.
- is_maximum is true when the number is an envelope ("up to", "a maximum of").
- either_or is true when the permission offers alternative uses (student OR co-living, and/or).
- quote must be copied exactly from the description, the shortest phrase containing the number.
- confidence is your 0-1 belief that beds is the stated total student bed count.
Never estimate or infer a number the text does not support.

Description:
"""
_APPROX = re.compile(r"\b(?:circa|c\.|approx\w*|around|about|over|more than|nearly|almost)\b", re.I)


class BedReading(BaseModel):
    beds: Optional[int] = Field(description="Total student bed count stated, else null")
    is_maximum: bool = Field(description="The number is an 'up to' envelope")
    either_or: bool = Field(description="The permission offers alternative uses")
    quote: Optional[str] = Field(description="Exact phrase from the description containing the number")
    confidence: float = Field(description="0-1 belief that beds is the stated total")
    reason: str = Field(description="One sentence on how the number was read")


def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None, max_retries=3)


def _squash(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def score(reading: BedReading, description: str) -> tuple[float, bool, bool]:
    """(confidence, quote_verified, approximate) after checking the model against the text."""
    if reading.beds is None:
        return 0.0, False, False
    quote = reading.quote or ""
    verified = bool(quote) and _squash(quote) in _squash(description)
    number_in_quote = bool(re.search(rf"\b{reading.beds:,}\b|\b{reading.beds}\b", quote))
    approx = bool(_APPROX.search(quote))
    conf = min(max(reading.confidence, 0.0), 0.9)
    if not verified or not number_in_quote:
        conf *= 0.4
    if approx:
        conf *= 0.8
    # If the regex extractor reads the same number from the quote, the phrase
    # is a plain statement and its score stands; else cap below the cache bar.
    regex = extract_units(quote)
    agrees = any(f.value == reading.beds for f in regex.of_kind("beds"))
    if verified and not agrees:
        conf = min(conf, CACHE_MIN_CONFIDENCE - 0.05)
    return round(conf, 3), verified, approx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--ids", type=int, nargs="+", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set — add it to the environment")
    import anthropic
    client = anthropic_client()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.council)).first()
    if council is None:
        raise SystemExit(f"Council {args.council!r} not found")
    q = (db.query(PlanningApplication)
         .filter(PlanningApplication.council_id == council.id,
                 or_(PlanningApplication.is_pbsa.is_(True), PlanningApplication.scheme_type == "PBSA"),
                 PlanningApplication.pbsa_beds.is_(None),
                 PlanningApplication.description.isnot(None)))
    if args.ids:
        q = q.filter(PlanningApplication.id.in_(args.ids))
    apps = q.limit(args.limit).all()
    print(f"{len(apps)} NULL-bed PBSA applications to try with {MODEL}")

    stats = {"cached": 0, "recorded_low": 0, "null": 0, "error": 0, "refused": 0}
    for a in apps:
        try:
            resp = client.messages.parse(
                model=MODEL, max_tokens=600,
                output_config={"effort": "low"},
                messages=[{"role": "user", "content": PROMPT + a.description[:6000]}],
                output_format=BedReading,
            )
        except anthropic.RateLimitError as exc:
            stats["error"] += 1
            print(f"  {a.reference}: rate limited ({exc.response.headers.get('retry-after', '?')}s); stopping")
            break
        except anthropic.APIStatusError as exc:
            stats["error"] += 1
            print(f"  {a.reference}: API {exc.status_code} {exc.message[:80]}")
            continue
        except anthropic.APIConnectionError as exc:
            stats["error"] += 1
            print(f"  {a.reference}: connection error {str(exc)[:80]}")
            continue
        if resp.stop_reason == "refusal" or resp.parsed_output is None:
            stats["refused"] += 1
            print(f"  {a.reference}: no reading (stop_reason={resp.stop_reason})")
            continue
        r: BedReading = resp.parsed_output
        if r.beds is None or not (10 <= r.beds <= 4000):
            stats["null"] += 1
            continue
        conf, verified, approx = score(r, a.description)
        basis = "up_to" if r.is_maximum else ("either_or" if r.either_or else "stated")
        cache = conf >= CACHE_MIN_CONFIDENCE and verified
        stats["cached" if cache else "recorded_low"] += 1
        print(f"  {a.reference}: beds={r.beds} {basis} conf={conf} "
              f"{'CACHED' if cache else 'recorded only'}  ({str(r.quote)[:60]!r})")
        if args.dry_run:
            continue
        db.add(UnitExtractionRecord(
            entity_type="planning_application", entity_id=a.id, kind="beds", value=r.beds,
            is_maximum=r.is_maximum, is_approximate=approx, alternative=r.either_or,
            confidence=conf, quote=(r.quote or "")[:500], method="ai", model=MODEL,
            source="description", source_reference=a.reference,
        ))
        if cache:
            a.pbsa_beds, a.beds_basis = r.beds, basis
            a.beds_confidence, a.beds_quote, a.beds_extractor = conf, r.quote, "ai"
            if r.is_maximum:
                a.beds_max = r.beds
        db.commit()
    db.close()
    print("\n" + ", ".join(f"{k}={v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
