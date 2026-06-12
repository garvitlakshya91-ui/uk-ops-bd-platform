"""
Force re-enrich all PBSA schemes using the web-search-enabled AI.

Strategy:
  1. For each PBSA scheme, remove the ai_enrichment-rank locks from the
     non-core fields we want the new web-search AI to be free to revise
     (owner, asset_manager, landlord, num_units, address). Operator is left
     locked because the PBSA scraper confirmed it authoritatively.
  2. Call _call_claude with use_web_search=True.
  3. Apply high-confidence suggestions via set_field. Because we unlocked
     the lower-precedence fields, AI (rank 20) can now write them.
  4. Persist rent suggestions into scheme_rents.

Note: this uses --min-confidence 0.7 by default. The web-search-backed AI
is grounded and generally returns 0.8-1.0 on correct findings, so 0.7 is
a safe threshold.

Usage:
    python scripts/force_reenrich_pbsa.py [--limit N] [--min-confidence 0.7]
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from app.database import SessionLocal
from app.models.models import (
    Company,
    ExistingScheme,
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


# Fields to unlock (pbsa_operator tier) so AI can write them.
# Operator is kept locked — scraper is authoritative.
UNLOCKABLE_FIELDS = {
    "owner_company_id",
    "asset_manager_company_id",
    "landlord_company_id",
    "num_units",
}

# Fields to unlock only if the current lock is 'pbsa_operator' or lower —
# never unlock 'manual' locks.
UNLOCK_MAX_PRECEDENCE = 50  # pbsa_operator rank

FK_MAP = {
    "owner_company_name": "owner_company_id",
    "operator_company_name": "operator_company_id",
    "asset_manager_company_name": "asset_manager_company_id",
    "landlord_company_name": "landlord_company_id",
}


def unlock_for_reenrichment(scheme: ExistingScheme) -> list[str]:
    """Remove locks from UNLOCKABLE_FIELDS if current lock is <= pbsa_operator.

    Returns list of field names that were actually unlocked.
    """
    from app.scrapers.field_protection import precedence_of

    locks = dict(scheme.locked_fields or {})
    unlocked = []
    for field in UNLOCKABLE_FIELDS:
        current = locks.get(field)
        if current and precedence_of(current) <= UNLOCK_MAX_PRECEDENCE:
            del locks[field]
            unlocked.append(field)
    if unlocked:
        scheme.locked_fields = locks
        flag_modified(scheme, "locked_fields")
    return unlocked


def apply_ai_to_scheme(
    scheme: ExistingScheme, db, min_confidence: float, max_retries: int = 4
) -> dict:
    """Run web-search AI on one scheme, apply suggestions + rents.

    Retries 429 rate-limit errors with exponential backoff.
    """
    attempt = 0
    while True:
        try:
            ai_result = _call_claude(scheme, db=db, use_web_search=True)
            break
        except Exception as exc:
            msg = str(exc)
            is_rate_limit = "429" in msg or "rate_limit" in msg.lower() or "overloaded" in msg.lower()
            attempt += 1
            if is_rate_limit and attempt <= max_retries:
                wait = min(60, 5 * (2 ** (attempt - 1)))  # 5, 10, 20, 40, 60
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
            # Resolve or create company
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
                changed_by="system:force_reenrich",
            )
        except FieldValidationError as e:
            skipped.append(f"{s.field} (validation: {e})")
            continue

        if did:
            applied.append(s.field)
        else:
            skipped.append(f"{s.field} (blocked)")

    # Persist rents
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
    parser.add_argument("--skip-already-enriched", action="store_true",
                        help="Skip schemes that already have ai_enrichment change-log entries from the web-search run")
    parser.add_argument("--rate-limit-seconds", type=float, default=2.0,
                        help="Base sleep between requests")
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

        if args.skip_already_enriched:
            # Skip any scheme that already has an ai_enrichment change-log entry
            # from the force_reenrich run (changed_by = 'system:force_reenrich')
            from app.models.models import SchemeChangeLog
            enriched_ids = {
                r[0] for r in db.query(SchemeChangeLog.scheme_id)
                .filter(SchemeChangeLog.changed_by == "system:force_reenrich")
                .distinct()
                .all()
            }
            if enriched_ids:
                q = q.filter(~ExistingScheme.id.in_(enriched_ids))
                print(f"Skipping {len(enriched_ids)} already-enriched schemes")

        if args.limit:
            q = q.limit(args.limit)
        schemes = q.all()
        print(f"[{time.strftime('%H:%M:%S')}] Force re-enriching {len(schemes)} schemes "
              f"(source={args.source}, min_confidence={args.min_confidence})")

        total_applied = 0
        total_rents = 0
        total_errors = 0
        start = time.time()

        for i, sch in enumerate(schemes, 1):
            # Step 1: unlock unlockable fields
            unlocked = unlock_for_reenrichment(sch)
            if unlocked:
                db.commit()

            # Step 2: run AI
            result = apply_ai_to_scheme(sch, db, args.min_confidence)
            err = result.get("error")
            if err:
                total_errors += 1
                print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  ERROR: {err[:80]}")
                continue

            applied = result.get("applied", [])
            rents_saved = result.get("rents_saved", 0)
            total_applied += len(applied)
            total_rents += rents_saved

            summary = ", ".join(applied) if applied else "no-op"
            if rents_saved:
                summary += f" + {rents_saved} rent"
            print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  {summary}")

            # Rate-limit buffer for Claude API / web search
            time.sleep(args.rate_limit_seconds)

        elapsed = time.time() - start
        print(f"\n[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        print(f"  Schemes processed: {len(schemes)}")
        print(f"  Fields applied:    {total_applied}")
        print(f"  Rents saved:       {total_rents}")
        print(f"  Errors:            {total_errors}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
