"""
Batch AI-enrich all PBSA schemes.

Loops over every scheme with source='pbsa_operator', calls Claude to suggest
missing fields, and auto-applies high-confidence suggestions (conf >= 0.5).

Creates Company records as needed for asset_manager / landlord / owner.
Writes SchemeChangeLog entries for audit.

Usage:
    python scripts/batch_enrich_pbsa.py [--min-confidence 0.5] [--limit N]
"""
import argparse
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import joinedload

from app.database import SessionLocal
from app.models.models import (
    Company,
    ExistingScheme,
    SchemeChangeLog,
)
from app.api.ai_enrichment import (
    _call_claude,
    _build_suggestions,
    _find_or_note_company,
    _infer_company_type,
)
from app.scrapers.field_protection import (
    set_field,
    FieldValidationError,
)


FK_FIELD_MAP = {
    "owner_company_name": ("owner_company_id", "owner_company"),
    "operator_company_name": ("operator_company_id", "operator_company"),
    "asset_manager_company_name": ("asset_manager_company_id", "asset_manager_company"),
    "landlord_company_name": ("landlord_company_id", "landlord_company"),
}


def apply_suggestion(scheme: ExistingScheme, field: str, value: str, db) -> tuple[bool, str | None]:
    """Apply a single suggestion via the field_protection chokepoint.

    The chokepoint enforces source precedence, so ai_enrichment (rank 20)
    cannot overwrite any field already locked by a higher-rank source
    (manual, hmlr_ccod, operator_scraper, etc.).

    Returns (applied, old_value).
    """
    # For company-name suggestions, map to the *_company_id column and
    # resolve/create a Company record first.
    target_field = field
    target_value: Any = value

    if field in FK_FIELD_MAP:
        fk_col, _ = FK_FIELD_MAP[field]
        target_field = fk_col
        if value:
            company_id = _find_or_note_company(value, db)
            if company_id is None:
                new_co = Company(
                    name=value[:255],
                    normalized_name=value.strip().lower()[:255],
                    company_type=_infer_company_type(field),
                    is_active=True,
                )
                db.add(new_co)
                db.flush()
                company_id = new_co.id
            target_value = company_id
        else:
            target_value = None
    elif field == "num_units":
        try:
            target_value = int(value) if value else None
        except (ValueError, TypeError):
            return False, None

    old = getattr(scheme, target_field, None)
    try:
        did_apply = set_field(
            scheme, target_field, target_value,
            source="ai_enrichment", db=db,
            changed_by="system:batch_enrich_pbsa",
        )
    except FieldValidationError:
        return False, str(old) if old is not None else None

    return did_apply, str(old) if old is not None else None


def enrich_scheme(
    scheme: ExistingScheme, db, min_confidence: float
) -> dict:
    """Enrich one scheme. Returns counts dict."""
    try:
        ai_result = _call_claude(scheme, db=db)
    except Exception as exc:
        return {"error": f"Claude call failed: {exc}"}

    suggestions = _build_suggestions(scheme, ai_result)
    applied = []
    skipped = []

    for s in suggestions:
        if s.confidence < min_confidence:
            skipped.append(f"{s.field} (conf={s.confidence:.2f} < {min_confidence})")
            continue
        try:
            ok, old = apply_suggestion(scheme, s.field, s.suggested_value, db)
        except Exception as exc:
            skipped.append(f"{s.field} (apply_error: {exc})")
            continue
        if ok:
            applied.append(s.field)
            # set_field already writes the SchemeChangeLog entry
        else:
            skipped.append(s.field)

    if applied:
        try:
            db.commit()
        except Exception as exc:
            db.rollback()
            return {"error": f"Commit failed: {exc}", "applied": applied}

    return {"applied": applied, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-confidence", type=float, default=0.7)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--source", default="pbsa_operator")
    parser.add_argument("--start-id", type=int, default=None, help="Resume from scheme id")
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
        if args.limit:
            q = q.limit(args.limit)

        schemes = q.all()
        print(f"[{time.strftime('%H:%M:%S')}] Enriching {len(schemes)} schemes from source={args.source}")

        total_applied = 0
        total_errors = 0
        start = time.time()

        for i, sch in enumerate(schemes, 1):
            result = enrich_scheme(sch, db, args.min_confidence)
            applied = result.get("applied", [])
            err = result.get("error")

            if err:
                total_errors += 1
                print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  ERROR: {err[:100]}")
            else:
                total_applied += len(applied)
                status = ", ".join(applied) if applied else "no-op"
                print(f"[{i}/{len(schemes)}] [{sch.id}] {sch.name[:50]:50s}  {status}")

            # rough rate limit for Claude API
            time.sleep(0.3)

        elapsed = time.time() - start
        print(f"\n[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        print(f"  Schemes processed: {len(schemes)}")
        print(f"  Total fields applied: {total_applied}")
        print(f"  Errors: {total_errors}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
