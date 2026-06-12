"""Two-tier AI enrichment for Birmingham + Manchester schemes.

Pulls all BD-typed (BTR/PBSA/Co-living/Senior) schemes in those two councils
that are missing operator OR contract_end_date, runs Claude with web search
to fill them, and auto-applies the suggestions.

Tiering:
- num_units >= 100 -> Sonnet (better reasoning, ~2.8c/scheme)
- otherwise        -> Haiku  (cheaper, ~2.1c/scheme)

Fields targeted:
- operator_company_name
- owner_company_name
- asset_manager_company_name
- contract_end_date  (extended beyond the default ai_enrichment prompt)
- num_units
- status
- rents (room-type level)

Cost is logged per scheme + at the end.

Usage::

    python enrich_birm_man.py [--council Birmingham] [--limit 50] [--dry-run]
                              [--model-override haiku|sonnet]
"""
from __future__ import annotations
import argparse, json, os, sys, time
from datetime import datetime, date
from typing import Optional

import anthropic
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, joinedload

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
# override=True because the parent shell often pre-sets ANTHROPIC_API_KEY=''
# (empty), which load_dotenv would otherwise refuse to replace.
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

from app.api.ai_enrichment import (
    _build_scheme_context, _extract_json_block, _find_or_note_company,
    WEB_SEARCH_TOOL,
)
from app.models.models import ExistingScheme, Company

SONNET = "claude-sonnet-4-20250514"
HAIKU = "claude-haiku-4-5"  # latest Haiku at time of writing

SONNET_INPUT_PER_TOK = 3.0 / 1_000_000
SONNET_OUTPUT_PER_TOK = 15.0 / 1_000_000
HAIKU_INPUT_PER_TOK = 0.25 / 1_000_000
HAIKU_OUTPUT_PER_TOK = 1.25 / 1_000_000
SEARCH_PER_CALL = 10.0 / 1000  # Anthropic web-search


# Extended system prompt — adds contract_end_date as a target field.
SYSTEM_PROMPT_EXTENDED = """\
You are an expert on UK property developments — Build-to-Rent (BTR), \
Purpose-Built Student Accommodation (PBSA), Co-living, Senior Living, \
and large residential schemes.

You will be given a scheme with some fields known and some missing. \
Use web search to find authoritative information (operator websites, \
press releases, investor reports, Companies House filings, planning \
portals) and propose values for the missing fields.

CALIBRATION (critical — accuracy beats completeness):
- 0.85-1.0: You have specific, source-verifiable knowledge of THIS scheme.
- 0.6-0.85: Strong inference from closely related facts.
- 0.4-0.6: Reasonable guess from context.
- Below 0.4: Speculation — omit.

OWNERSHIP RULES:
- Operator, owner, asset manager, landlord are often DIFFERENT entities for BTR/PBSA. Don't default them to each other.
- If you find the operator from a press release but no separate owner, omit owner.

CONTRACT END DATE:
- Operating leases / management agreements for BTR/PBSA typically run 5-15 years.
- If you find a press release that says "Greystar appointed for 10-year term starting 2022", set contract_end_date to 2032-XX-XX (use the most likely month if mentioned, otherwise 06-30).
- Format: ISO date YYYY-MM-DD.
- Omit if you have no source.

FORMAT:
- scheme_type: BTR | PBSA | Co-living | Senior Living | Residential | Mixed-use
- status: operational | under_construction | planned | decommissioned
- Use trading names for companies (e.g. "Greystar Real Estate Partners").
- num_units: integer.

Respond with ONLY valid JSON (no markdown):
{
  "suggestions": [
    {
      "field": "field_name",
      "suggested_value": "value",
      "confidence": 0.85,
      "reasoning": "Brief; cite source/URL"
    }
  ],
  "rents": [
    {
      "room_type": "Studio",
      "rent_per_week": 250,
      "currency": "GBP",
      "academic_year": "2025/26",
      "confidence": 0.9,
      "reasoning": "Source"
    }
  ],
  "notes": "Anything notable"
}

Suggestable fields:
- owner_company_name, operator_company_name, asset_manager_company_name, landlord_company_name
- num_units, scheme_type, status, address, postcode
- contract_end_date  (ISO format, e.g. 2032-06-30)
- contract_start_date (ISO format, optional)

RENTS:
- One entry per room type. BTR: rent_per_month. PBSA: rent_per_week + academic_year.
- Only include rents from operator website / Rightmove / Zoopla / StuRents.
- If you can't find rent info, return empty array.
"""


def call_claude(scheme, model: str, db) -> tuple[dict, dict]:
    """Returns (parsed_result, cost_dict)."""
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    context = _build_scheme_context(scheme, db=db)
    user_msg = (
        "Research the following UK property scheme via web search and return "
        "structured JSON per the system prompt.\n\n"
        f"{context}"
    )

    msg = client.messages.create(
        model=model,
        max_tokens=4096,
        system=SYSTEM_PROMPT_EXTENDED,
        messages=[{"role": "user", "content": user_msg}],
        tools=[WEB_SEARCH_TOOL],
    )

    # Count web searches by examining content blocks
    n_searches = sum(
        1 for b in msg.content
        if getattr(b, "type", "") == "server_tool_use"
        and getattr(b, "name", "") == "web_search"
    )

    text_parts = [getattr(b, "text", "") for b in msg.content if getattr(b, "type", "") == "text"]
    raw_text = "".join(text_parts).strip()

    # Compute cost
    usage = msg.usage
    if model == SONNET:
        in_cost = usage.input_tokens * SONNET_INPUT_PER_TOK
        out_cost = usage.output_tokens * SONNET_OUTPUT_PER_TOK
    else:
        in_cost = usage.input_tokens * HAIKU_INPUT_PER_TOK
        out_cost = usage.output_tokens * HAIKU_OUTPUT_PER_TOK
    search_cost = n_searches * SEARCH_PER_CALL
    total = in_cost + out_cost + search_cost

    cost = {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "n_searches": n_searches,
        "in_cost": in_cost, "out_cost": out_cost,
        "search_cost": search_cost, "total": total,
    }

    if not raw_text:
        return {}, cost

    json_text = _extract_json_block(raw_text)
    try:
        return json.loads(json_text), cost
    except json.JSONDecodeError:
        return {}, cost


def apply_suggestions(scheme: ExistingScheme, result: dict, db, min_conf: float = 0.6) -> dict:
    """Apply AI suggestions to the scheme. Returns counts applied."""
    applied = {"fields": 0, "rents": 0, "skipped_low_conf": 0}
    if not result:
        return applied

    suggestions = result.get("suggestions", [])
    for s in suggestions:
        field = s.get("field")
        value = s.get("suggested_value")
        conf = float(s.get("confidence") or 0)
        if not field or value is None or conf < min_conf:
            if conf < min_conf:
                applied["skipped_low_conf"] += 1
            continue

        if field == "operator_company_name":
            cid = _find_or_note_company(value, db)
            if cid:
                scheme.operator_company_id = cid
                applied["fields"] += 1
        elif field == "owner_company_name":
            cid = _find_or_note_company(value, db)
            if cid:
                scheme.owner_company_id = cid
                applied["fields"] += 1
        elif field == "asset_manager_company_name":
            cid = _find_or_note_company(value, db)
            if cid:
                scheme.asset_manager_company_id = cid
                applied["fields"] += 1
        elif field == "landlord_company_name":
            cid = _find_or_note_company(value, db)
            if cid:
                scheme.landlord_company_id = cid
                applied["fields"] += 1
        elif field == "num_units":
            try:
                scheme.num_units = int(value)
                applied["fields"] += 1
            except (ValueError, TypeError):
                pass
        elif field == "scheme_type":
            # Whitelist canonical values only
            canonical = str(value).split("(")[0].strip()
            if canonical in {"BTR", "PBSA", "Co-living", "Senior Living",
                             "Senior", "Residential", "Mixed-use", "Mixed",
                             "Affordable", "Unknown"}:
                # Normalise Senior Living -> Senior, Mixed-use -> Mixed
                canonical = {"Senior Living": "Senior", "Mixed-use": "Mixed"}.get(canonical, canonical)
                scheme.scheme_type = canonical
                applied["fields"] += 1
        elif field == "status":
            canonical = str(value).strip().lower()
            mapping = {"operational": "operational", "under_construction": "under_construction",
                       "planned": "planned", "decommissioned": "decommissioned"}
            if canonical in mapping:
                scheme.status = mapping[canonical]
                applied["fields"] += 1
        elif field == "address":
            v = str(value).strip()
            if 5 < len(v) < 500:  # sanity bounds
                scheme.address = v
                applied["fields"] += 1
        elif field == "postcode":
            import re as _re
            # Extract a clean UK postcode from possibly-verbose AI output
            v = str(value).strip().upper()
            m = _re.search(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", v)
            if m:
                pc = m.group(1).strip()
                if len(pc) <= 10:
                    scheme.postcode = pc
                    applied["fields"] += 1
        elif field == "contract_end_date":
            try:
                scheme.contract_end_date = date.fromisoformat(str(value)[:10])
                applied["fields"] += 1
            except ValueError:
                pass
        elif field == "contract_start_date":
            try:
                scheme.contract_start_date = date.fromisoformat(str(value)[:10])
                applied["fields"] += 1
            except ValueError:
                pass

    scheme.data_confidence_score = max(
        scheme.data_confidence_score or 0,
        0.7,  # AI-enriched data confidence baseline
    )
    scheme.last_verified_at = datetime.utcnow()

    return applied


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--council", choices=["Birmingham", "Manchester", "both"], default="both")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--model-override", choices=["haiku", "sonnet"], default=None)
    p.add_argument("--min-conf", type=float, default=0.6)
    p.add_argument("--units-threshold", type=int, default=100,
                   help="Schemes with num_units >= this use Sonnet, else Haiku.")
    args = p.parse_args()

    councils = ["Birmingham", "Manchester"] if args.council == "both" else [args.council]

    engine = create_engine(os.environ["DATABASE_URL"])
    Session = sessionmaker(bind=engine)
    db = Session()

    council_list = ", ".join(f"'{c}'" for c in councils)
    schemes = (
        db.query(ExistingScheme)
        .options(
            joinedload(ExistingScheme.operator_company),
            joinedload(ExistingScheme.owner_company),
            joinedload(ExistingScheme.asset_manager_company),
            joinedload(ExistingScheme.landlord_company),
            joinedload(ExistingScheme.council),
        )
        .filter(ExistingScheme.council.has(name=councils[0]) if len(councils) == 1
                else ExistingScheme.council.has(
                    ExistingScheme.council.property.mapper.class_.name.in_(councils)
                ))
        .filter(ExistingScheme.scheme_type.in_(
            ["BTR", "PBSA", "Co-living", "Senior"]
        ))
        .filter(
            (ExistingScheme.operator_company_id.is_(None)) |
            (ExistingScheme.contract_end_date.is_(None))
        )
        # Skip recently-enriched schemes (within the last 24h) — saves API
        # spend on re-runs after credit top-ups, etc.
        .filter(
            (ExistingScheme.last_verified_at.is_(None)) |
            (ExistingScheme.last_verified_at < datetime.utcnow() - __import__('datetime').timedelta(hours=24))
        )
        .order_by(ExistingScheme.num_units.desc().nullslast())
    )
    if args.limit:
        schemes = schemes.limit(args.limit)
    schemes = schemes.all()

    print(f"Council(s): {councils}")
    print(f"Schemes to enrich: {len(schemes):,}")
    if args.dry_run:
        print("DRY-RUN — no writes, no AI calls.")
        for s in schemes[:5]:
            print(f"  id={s.id} {s.name[:40]:<40s} units={s.num_units}  type={s.scheme_type}")
        return 0
    print()

    total_cost = 0.0
    total_fields = 0
    total_rents = 0
    completed = 0
    errors = 0
    sonnet_n = 0
    haiku_n = 0

    for i, scheme in enumerate(schemes, 1):
        # Pick model
        if args.model_override == "sonnet":
            model = SONNET
        elif args.model_override == "haiku":
            model = HAIKU
        elif (scheme.num_units or 0) >= args.units_threshold:
            model = SONNET
        else:
            model = HAIKU

        try:
            result, cost = call_claude(scheme, model, db)
        except Exception as exc:
            print(f"  [{i}/{len(schemes)}] id={scheme.id} ({scheme.name[:30]}) FAILED: {exc}")
            errors += 1
            continue

        applied = apply_suggestions(scheme, result, db, min_conf=args.min_conf)
        try:
            db.commit()
        except Exception as exc:
            db.rollback()
            print(f"  [{i}/{len(schemes)}] id={scheme.id} ({scheme.name[:30]}) COMMIT FAILED: {str(exc)[:120]}")
            errors += 1
            continue

        total_cost += cost["total"]
        total_fields += applied["fields"]
        if model == SONNET:
            sonnet_n += 1
        else:
            haiku_n += 1
        completed += 1

        # Per-scheme line
        model_short = "Sonnet" if model == SONNET else "Haiku"
        print(f"  [{i}/{len(schemes)}] {scheme.name[:35]:<35s} {model_short:<6s} "
              f"u={scheme.num_units or '?':<5} "
              f"f={applied['fields']} cost=${cost['total']:.4f} "
              f"(in={cost['input_tokens']}, out={cost['output_tokens']}, ws={cost['n_searches']})")

        # Tiny sleep to be gentle on rate limits
        time.sleep(0.5)

    db.close()

    print()
    print("=" * 60)
    print(f"Completed:     {completed:,}")
    print(f"Errors:        {errors:,}")
    print(f"Sonnet calls:  {sonnet_n:,}")
    print(f"Haiku calls:   {haiku_n:,}")
    print(f"Fields applied: {total_fields:,}")
    print(f"Total cost: ${total_cost:.2f} (≈£{total_cost*0.78:.2f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
