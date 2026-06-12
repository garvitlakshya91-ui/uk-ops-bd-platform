"""
Budget-capped web-search AI enrichment for PBSA + top ARL BTR schemes.

Targets high-BD-value schemes (PBSA operator pages + ARL-verified BTR) and
enriches them via Claude + Anthropic web_search, subject to:
  - A hard scheme-count cap (--max-schemes, default 400 = ~$18 at ~$0.045/call)
  - A resumable checkpoint so we can safely Ctrl-C or lose the session
  - Graceful exit on credit exhaustion (400 invalid_request_error)

Reuses the existing pipeline in force_reenrich_pbsa.py:
  - apply_ai_to_scheme() — handles web_search call + retries + rent persist
  - unlock_for_reenrichment() — frees pbsa-rank locks so AI writes land
  - set_field() chokepoint — protects `manual` locks + writes audit log

Usage:
    # Preview targets
    python scripts/web_search_enrich.py --dry-run --max-schemes 10

    # Single-scheme smoke test (uses a separate checkpoint)
    python scripts/web_search_enrich.py --max-schemes 1 \
        --checkpoint-file /tmp/smoke.json

    # Full run
    nohup python scripts/web_search_enrich.py --max-schemes 400 \
        > /tmp/web_search_enrich.log 2>&1 &
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy.orm import joinedload
from sqlalchemy import text

from app.database import SessionLocal
from app.models.models import ExistingScheme

from force_reenrich_pbsa import (  # type: ignore
    apply_ai_to_scheme,
    unlock_for_reenrichment,
)


CHANGED_BY = "system:web_search_enrich"
COST_PER_CALL_USD = 0.045  # empirical observation

DEFAULT_CHECKPOINT = Path(__file__).resolve().parent.parent / ".enrich_checkpoint.json"


def load_checkpoint(path: Path) -> set[int]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(int(x) for x in data.get("processed_ids", []))
    except Exception:
        return set()


def save_checkpoint(path: Path, processed_ids: set[int], stats: dict) -> None:
    path.write_text(json.dumps({
        "processed_ids": sorted(processed_ids),
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        **stats,
    }, indent=2))


def _priority_keyword_filter(priority_keywords: list[str]) -> str:
    """Build an ILIKE-any clause that matches if the name OR address contains
    any of the keywords. Returns a WHERE fragment (with placeholders) or ''."""
    if not priority_keywords:
        return ""
    return " AND (" + " OR ".join(
        f"s.name ILIKE :kw_{i} OR s.address ILIKE :kw_{i}"
        for i, _ in enumerate(priority_keywords)
    ) + ")"


def select_targets(
    db,
    max_schemes: int,
    include_btr: bool,
    min_btr_units: int,
    already_processed: set[int],
    priority_keywords: list[str] | None = None,
) -> list[int]:
    """Return scheme IDs to enrich, in priority order.

    If ``priority_keywords`` are given, schemes whose name or address contains
    any of them are enriched first (e.g. ``["Birmingham", "Edinburgh"]``), then
    the remaining budget falls through to the default PBSA → BTR ordering.

    Priority order:
      1. PBSA schemes matching priority_keywords (name or address)
      2. PBSA schemes (non-matching)
      3. ARL BTR schemes matching priority_keywords (if include_btr)
      4. ARL BTR schemes non-matching (if include_btr)
    """
    excluded_changed_by = ("system:force_reenrich", CHANGED_BY)
    priority_keywords = priority_keywords or []

    kw_clause = _priority_keyword_filter(priority_keywords)
    kw_params = {f"kw_{i}": f"%{kw}%" for i, kw in enumerate(priority_keywords)}

    all_ids: list[int] = []
    seen: set[int] = set(already_processed)

    def _append(rows):
        for r in rows:
            sid = r[0]
            if sid not in seen:
                seen.add(sid)
                all_ids.append(sid)

    # --- 1. PBSA priority (matching keywords) ---
    if priority_keywords:
        rows = db.execute(text(f"""
            SELECT s.id, s.num_units FROM existing_schemes s
            WHERE s.source = 'pbsa_operator'
              AND s.id NOT IN (
                SELECT scheme_id FROM scheme_change_logs
                WHERE changed_by = ANY(:excluded)
              )
              {kw_clause}
            ORDER BY s.num_units DESC NULLS LAST, s.id ASC
        """), {"excluded": list(excluded_changed_by), **kw_params}).fetchall()
        _append(rows)

    # --- 2. PBSA (remaining) ---
    rows = db.execute(text("""
        SELECT s.id, s.num_units FROM existing_schemes s
        WHERE s.source = 'pbsa_operator'
          AND s.id NOT IN (
            SELECT scheme_id FROM scheme_change_logs
            WHERE changed_by = ANY(:excluded)
          )
        ORDER BY s.num_units DESC NULLS LAST, s.id ASC
    """), {"excluded": list(excluded_changed_by)}).fetchall()
    _append(rows)

    # --- 3/4. BTR (only if include_btr) ---
    if include_btr and len(all_ids) < max_schemes:
        if priority_keywords:
            rows = db.execute(text(f"""
                SELECT s.id, s.num_units FROM existing_schemes s
                WHERE s.source = 'arl_btr_open_operating'
                  AND s.num_units >= :min_units
                  AND s.id NOT IN (
                    SELECT scheme_id FROM scheme_change_logs
                    WHERE changed_by = ANY(:excluded)
                  )
                  {kw_clause}
                ORDER BY s.num_units DESC NULLS LAST, s.id ASC
            """), {
                "min_units": min_btr_units,
                "excluded": list(excluded_changed_by),
                **kw_params,
            }).fetchall()
            _append(rows)

        rows = db.execute(text("""
            SELECT s.id, s.num_units FROM existing_schemes s
            WHERE s.source = 'arl_btr_open_operating'
              AND s.num_units >= :min_units
              AND s.id NOT IN (
                SELECT scheme_id FROM scheme_change_logs
                WHERE changed_by = ANY(:excluded)
              )
            ORDER BY s.num_units DESC NULLS LAST, s.id ASC
        """), {"min_units": min_btr_units, "excluded": list(excluded_changed_by)}).fetchall()
        _append(rows)

    return all_ids[:max_schemes]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-schemes", type=int, default=400,
                        help="Hard cap on schemes processed this run (default 400)")
    parser.add_argument("--min-confidence", type=float, default=0.7)
    parser.add_argument("--rate-limit-seconds", type=float, default=3.0)
    parser.add_argument("--checkpoint-file", type=str, default=str(DEFAULT_CHECKPOINT))
    parser.add_argument("--include-btr", action="store_true",
                        help="Include ARL BTR schemes once PBSA list is exhausted")
    parser.add_argument("--min-btr-units", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print target list without calling Claude")
    parser.add_argument("--priority-keywords", type=str, default="",
                        help="Comma-separated city/region keywords; schemes whose name or address contains any of them are enriched first. e.g. 'Birmingham,West Midlands,Edinburgh'")
    args = parser.parse_args()
    priority_keywords = [k.strip() for k in args.priority_keywords.split(",") if k.strip()]
    if priority_keywords:
        print(f"[{time.strftime('%H:%M:%S')}] Priority keywords: {priority_keywords}")

    checkpoint_path = Path(args.checkpoint_file)
    processed_ids = load_checkpoint(checkpoint_path)
    if processed_ids:
        print(f"[checkpoint] {len(processed_ids)} schemes already processed in prior runs")

    db = SessionLocal()
    try:
        target_ids = select_targets(
            db,
            max_schemes=args.max_schemes,
            include_btr=args.include_btr,
            min_btr_units=args.min_btr_units,
            already_processed=processed_ids,
            priority_keywords=priority_keywords,
        )

        print(f"[{time.strftime('%H:%M:%S')}] Selected {len(target_ids)} schemes to enrich")
        print(f"[{time.strftime('%H:%M:%S')}] Est. max cost: ${len(target_ids) * COST_PER_CALL_USD:.2f}")

        if args.dry_run:
            for sid in target_ids[:20]:
                s = db.query(ExistingScheme).get(sid)
                if s:
                    print(f"  [{s.id}] src={s.source[:20]:20s} units={s.num_units or 0:>5}  {s.name[:50]}")
            if len(target_ids) > 20:
                print(f"  ... and {len(target_ids) - 20} more")
            return

        if not target_ids:
            print("Nothing to do.")
            return

        total_applied = 0
        total_rents = 0
        total_errors = 0
        total_calls = 0
        start = time.time()

        for i, sid in enumerate(target_ids, 1):
            sch = (
                db.query(ExistingScheme)
                .options(
                    joinedload(ExistingScheme.operator_company),
                    joinedload(ExistingScheme.owner_company),
                    joinedload(ExistingScheme.asset_manager_company),
                    joinedload(ExistingScheme.landlord_company),
                    joinedload(ExistingScheme.council),
                )
                .filter(ExistingScheme.id == sid)
                .first()
            )
            if not sch:
                continue

            # Unlock pbsa-rank fields so AI writes can land
            if unlock_for_reenrichment(sch):
                db.commit()

            result = apply_ai_to_scheme(sch, db, args.min_confidence)
            total_calls += 1
            err = result.get("error")

            if err:
                total_errors += 1
                # Graceful exit on credit exhaustion
                if "credit balance" in err.lower():
                    print(f"[{time.strftime('%H:%M:%S')}] CREDITS EXHAUSTED — checkpointing and exiting.")
                    save_checkpoint(checkpoint_path, processed_ids, {
                        "schemes_processed_this_run": i - 1,
                        "fields_applied": total_applied,
                        "rents_saved": total_rents,
                        "errors": total_errors,
                        "est_cost_usd": round(total_calls * COST_PER_CALL_USD, 2),
                    })
                    return
                print(f"[{i}/{len(target_ids)}] [{sid}] {sch.name[:40]:40s}  ERROR: {err[:80]}")
            else:
                applied = result.get("applied", [])
                rents_saved = result.get("rents_saved", 0)
                total_applied += len(applied)
                total_rents += rents_saved
                summary = ", ".join(applied) if applied else "no-op"
                if rents_saved:
                    summary += f" + {rents_saved} rent"
                cost_so_far = total_calls * COST_PER_CALL_USD
                print(f"[{i}/{len(target_ids)}] [{sid}] {sch.name[:40]:40s}  {summary}  (~${cost_so_far:.2f})")

            processed_ids.add(sid)

            # Periodic checkpoint every 10 schemes
            if i % 10 == 0:
                save_checkpoint(checkpoint_path, processed_ids, {
                    "schemes_processed_this_run": i,
                    "fields_applied": total_applied,
                    "rents_saved": total_rents,
                    "errors": total_errors,
                    "est_cost_usd": round(cost_so_far, 2),
                })

            time.sleep(args.rate_limit_seconds)

        # Final checkpoint
        save_checkpoint(checkpoint_path, processed_ids, {
            "schemes_processed_this_run": len(target_ids),
            "fields_applied": total_applied,
            "rents_saved": total_rents,
            "errors": total_errors,
            "est_cost_usd": round(total_calls * COST_PER_CALL_USD, 2),
        })

        elapsed = time.time() - start
        print(f"\n[{time.strftime('%H:%M:%S')}] Done in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
        print(f"  Schemes processed: {len(target_ids)}")
        print(f"  Fields applied:    {total_applied}")
        print(f"  Rents saved:       {total_rents}")
        print(f"  Errors:            {total_errors}")
        print(f"  Est. cost:         ${total_calls * COST_PER_CALL_USD:.2f}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
