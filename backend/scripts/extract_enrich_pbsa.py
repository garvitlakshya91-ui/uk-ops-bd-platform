"""
Extraction-only AI enrichment for PBSA schemes (Plan C).

Difference from force_reenrich_pbsa.py:
  - Uses plain Claude (no web_search tool) → much higher API throughput.
  - Uses the EXTRACTION_SYSTEM_PROMPT → tells Claude to parse values ONLY
    from the grounding context (HMLR owners, Companies House, operator page
    excerpt) rather than rely on general knowledge.

By default, skips schemes already touched by a prior enrichment run (either
force_reenrich or this extract run). Use --re-process-all to re-run everything.

Usage:
    python scripts/extract_enrich_pbsa.py                          # Full run
    python scripts/extract_enrich_pbsa.py --limit 5                # Test on 5
    python scripts/extract_enrich_pbsa.py --start-id 81982 \
        --limit 1 --re-process-all                                 # Kensington
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import joinedload

from app.database import SessionLocal
from app.models.models import (
    Company,
    ExistingScheme,
    SchemeChangeLog,
    SchemeRent,
)
from app.api.ai_enrichment import (
    _call_claude,
    _build_suggestions,
    _build_rent_suggestions,
    _find_or_note_company,
    _infer_company_type,
)
from app.scrapers.field_protection import (
    set_field,
    FieldValidationError,
)

# Re-use the unlock + FK-map helpers from the force-reenrich script
from force_reenrich_pbsa import (  # type: ignore
    FK_MAP,
    unlock_for_reenrichment,
)


CHANGED_BY = "system:extract_enrich"


def apply_extract_ai(
    scheme: ExistingScheme, db, min_confidence: float, max_retries: int = 4
) -> dict:
    """Run plain-Claude extraction AI on one scheme, apply suggestions + rents."""
    attempt = 0
    while True:
        try:
            ai_result = _call_claude(
                scheme, db=db, use_web_search=False, prompt_variant="extract"
            )
            break
        except Exception as exc:
            msg = str(exc)
            is_rate_limit = (
                "429" in msg or "rate_limit" in msg.lower() or "overloaded" in msg.lower()
            )
            attempt += 1
            if is_rate_limit and attempt <= max_retries:
                wait = min(60, 4 * (2 ** (attempt - 1)))  # 4, 8, 16, 32, 60
                time.sleep(wait)
                continue
            return {"error": msg[:200]}

    suggestions = _build_suggestions(scheme, ai_result)
    rents = _build_rent_suggestions(ai_result)

    applied = []
    skipped = []

    for s in suggestions:
        if s.confidence < min_confidence:
            skipped.append(f"{s.field} (conf={s.confidence:.2f})")
            continue

        target_field = FK_MAP.get(s.field, s.field)
        value = s.suggested_value

        if s.field in FK_MAP and value:
            cid = _find_or_note_company(value, db)
            if cid is None:
                new_co = Company(
                    name=str(value)[:255],
                    normalized_name=str(value).strip().lower()[:255],
                    company_type=_infer_company_type(s.field),
                    is_active=True,
                )
                db.add(new_co)
                db.flush()
                cid = new_co.id
            value = cid
        elif s.field == "num_units":
            try:
                value = int(value) if value else None
            except (ValueError, TypeError):
                skipped.append(f"{s.field} (invalid int)")
                continue

        try:
            did = set_field(
                scheme, target_field, value,
                source="ai_enrichment", db=db,
                changed_by=CHANGED_BY,
            )
        except FieldValidationError as e:
            skipped.append(f"{s.field} (validation: {e})")
            continue

        if did:
            applied.append(s.field)
        else:
            skipped.append(f"{s.field} (blocked)")

    rents_saved = 0
    for r in rents:
        if r.confidence < min_confidence:
            continue
        existing = (
            db.query(SchemeRent)
            .filter(
                SchemeRent.scheme_id == scheme.id,
                SchemeRent.room_type == r.room_type,
                SchemeRent.academic_year == r.academic_year,
            )
            .first()
        )
        if existing:
            if r.rent_per_week is not None:
                existing.rent_per_week = r.rent_per_week
            if r.rent_per_month is not None:
                existing.rent_per_month = r.rent_per_month
        else:
            db.add(SchemeRent(
                scheme_id=scheme.id,
                room_type=r.room_type,
                rent_per_week=r.rent_per_week,
                rent_per_month=r.rent_per_month,
                currency=r.currency or "GBP",
                academic_year=r.academic_year,
                contract_length_weeks=r.contract_length_weeks,
                source="ai_enrichment",
            ))
        rents_saved += 1

    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        return {"error": f"Commit failed: {exc}"[:200]}

    return {"applied": applied, "skipped": skipped, "rents_saved": rents_saved}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-confidence", type=float, default=0.7)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-id", type=int, default=None)
    parser.add_argument("--source", default="pbsa_operator")
    parser.add_argument(
        "--re-process-all", action="store_true",
        help="Also re-process schemes that were already enriched in prior runs",
    )
    parser.add_argument("--rate-limit-seconds", type=float, default=1.0)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        q = (
            db.query(ExistingScheme)
            .options(
                joinedload(ExistingScheme.operator_company),
                joinedload(ExistingScheme.owner_company),
                joinedload(ExistingScheme.asset_manager_company),
                joinedload(ExistingScheme.landlord_company),
                joinedload(ExistingScheme.council),
            )
            .filter(ExistingScheme.source == args.source)
            .order_by(ExistingScheme.id)
        )
        if args.start_id:
            q = q.filter(ExistingScheme.id >= args.start_id)

        if not args.re_process_all:
            # Skip schemes already enriched by extract_enrich or force_reenrich
            enriched_ids = {
                r[0] for r in db.query(SchemeChangeLog.scheme_id)
                .filter(SchemeChangeLog.changed_by.in_([
                    CHANGED_BY, "system:force_reenrich",
                ]))
                .distinct()
                .all()
            }
            if enriched_ids:
                q = q.filter(~ExistingScheme.id.in_(enriched_ids))
                print(f"Skipping {len(enriched_ids)} already-enriched schemes")

        if args.limit:
            q = q.limit(args.limit)
        schemes = q.all()

        print(f"[{time.strftime('%H:%M:%S')}] Extracting on {len(schemes)} "
              f"schemes (source={args.source}, min_confidence={args.min_confidence})")

        total_applied = 0
        total_rents = 0
        total_errors = 0
        start = time.time()

        for i, sch in enumerate(schemes, 1):
            unlocked = unlock_for_reenrichment(sch)
            if unlocked:
                db.commit()

            result = apply_extract_ai(sch, db, args.min_confidence)
            err = result.get("error")
            if err:
                total_errors += 1
                print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  "
                      f"ERROR: {err[:80]}")
                time.sleep(args.rate_limit_seconds)
                continue

            applied = result.get("applied", [])
            rents_saved = result.get("rents_saved", 0)
            total_applied += len(applied)
            total_rents += rents_saved

            summary = ", ".join(applied) if applied else "no-op"
            if rents_saved:
                summary += f" + {rents_saved} rent"
            print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  {summary}")

            time.sleep(args.rate_limit_seconds)

        elapsed = time.time() - start
        print(f"\n[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s "
              f"({elapsed/60:.1f} min)")
        print(f"  Schemes processed: {len(schemes)}")
        print(f"  Fields applied:    {total_applied}")
        print(f"  Rents saved:       {total_rents}")
        print(f"  Errors:            {total_errors}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
