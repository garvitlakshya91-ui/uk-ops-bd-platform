"""Run the new contract_end_date enrichment pipeline against existing data.

Tier 1 (free, fast): Re-run text-extraction backfill using the new
expanded regex patterns ("expires on", "5+2+2", "option to extend",
"to expire on", "completes on").

Tier 2 (free, fast): CPV-based duration estimates and CF awards
structured extraction (existing tasks — re-runs to pick up newly
parsed data).

Tier 3 (slow, hits Companies House API): Infer end-date from the
charges register. Skipped by default — pass --ch to enable. Limited
to --ch-batch schemes per run to stay within the 600/5-min rate limit.

Tier 4 (free): Count leasehold cohort that needs paid HMLR register
pulls — pure metric, no writes.
"""
import argparse
import datetime
import os
import sys
import traceback

from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Load .env from the worktree root (one level above backend/)
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))
# .env's DATABASE_URL uses the docker hostname `postgres`; when running this
# script from the host we need `localhost`. Rewrite if needed.
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])


def counts() -> tuple[int, int, int]:
    with engine.connect() as c:
        total = c.execute(text("SELECT COUNT(*) FROM existing_schemes")).scalar() or 0
        no_end_scheme = c.execute(
            text("SELECT COUNT(*) FROM existing_schemes WHERE contract_end_date IS NULL")
        ).scalar() or 0
        no_end_contract = c.execute(
            text("SELECT COUNT(*) FROM scheme_contracts WHERE contract_end_date IS NULL")
        ).scalar() or 0
    return total, no_end_scheme, no_end_contract


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ch", action="store_true", help="Run the Companies House charges inference (slow, rate-limited)")
    parser.add_argument("--ch-batch", type=int, default=200, help="Max schemes to process via CH charges per run")
    parser.add_argument("--skip-text", action="store_true", help="Skip text-based backfill (Tier 1)")
    parser.add_argument("--skip-duration", action="store_true", help="Skip duration-based backfill (Tier 2)")
    args = parser.parse_args()

    started = datetime.datetime.now()
    print("=" * 70)
    print(f"CONTRACT END DATE ENRICHMENT  ({started.isoformat(timespec='seconds')})")
    print("=" * 70)

    total, before_scheme, before_contract = counts()
    pct_scheme = before_scheme * 100 // max(total, 1)
    print(f"Before: {total} schemes, {before_scheme} missing end_date ({pct_scheme}%), {before_contract} contracts missing end_date")

    # -------------------------------------------------------------------
    # Tier 1: Text-extraction backfill (uses the new regex patterns)
    # -------------------------------------------------------------------
    if not args.skip_text:
        print("\n--- Tier 1: Description text extraction (new regex patterns) ---")
        try:
            from app.tasks.enrichment_tasks import backfill_contract_dates
            tier1_total = 0
            for batch in range(40):  # up to 20k contracts
                result = backfill_contract_dates()
                u = result.get("updated", 0)
                tier1_total += u
                short = f"scanned={result.get('scanned',0)} updated={u} schemes_updated={result.get('schemes_updated',0)}"
                print(f"  Batch {batch+1}: {short}")
                if result.get("scanned", 0) == 0 or u == 0:
                    break
            print(f"  Tier 1 total updated: {tier1_total}")
        except Exception as exc:
            print(f"  Tier 1 FAILED: {exc}")
            traceback.print_exc()

        print("\n--- Tier 1b: Contracts Finder awards structured extraction ---")
        try:
            from app.tasks.enrichment_tasks import backfill_contract_dates_cf_awards
            result = backfill_contract_dates_cf_awards()
            print(f"  Result: {result}")
        except Exception as exc:
            print(f"  Tier 1b FAILED: {exc}")
            traceback.print_exc()

    # -------------------------------------------------------------------
    # Tier 2: Duration inference
    # -------------------------------------------------------------------
    if not args.skip_duration:
        print("\n--- Tier 2: Duration inference (start + N years -> end) ---")
        try:
            from app.tasks.enrichment_tasks import backfill_dates_from_duration
            result = backfill_dates_from_duration()
            print(f"  Result: {result}")
        except Exception as exc:
            print(f"  Tier 2 FAILED: {exc}")
            traceback.print_exc()

        print("\n--- Tier 2b: CPV-based typical durations ---")
        try:
            from app.tasks.enrichment_tasks import estimate_contract_dates_cpv
            result = estimate_contract_dates_cpv()
            print(f"  Result: {result}")
        except Exception as exc:
            print(f"  Tier 2b FAILED: {exc}")
            traceback.print_exc()

    # -------------------------------------------------------------------
    # Propagate contract-level dates to scheme level
    # -------------------------------------------------------------------
    print("\n--- Propagating contract dates to schemes ---")
    with engine.connect() as c:
        r = c.execute(text("""
            UPDATE existing_schemes es
            SET contract_start_date = COALESCE(es.contract_start_date, sc.contract_start_date),
                contract_end_date   = COALESCE(es.contract_end_date,   sc.contract_end_date)
            FROM scheme_contracts sc
            WHERE sc.scheme_id = es.id
              AND sc.is_current = true
              AND (es.contract_start_date IS NULL OR es.contract_end_date IS NULL)
              AND (sc.contract_start_date IS NOT NULL OR sc.contract_end_date IS NOT NULL)
        """))
        c.commit()
        print(f"  Propagated dates to {r.rowcount} schemes")

    # -------------------------------------------------------------------
    # Tier 3: Companies House charges inference (optional, slow)
    # -------------------------------------------------------------------
    if args.ch:
        print(f"\n--- Tier 3: Companies House charges inference (batch={args.ch_batch}) ---")
        try:
            from app.tasks.enrichment_tasks import backfill_contract_dates_from_ch_charges
            result = backfill_contract_dates_from_ch_charges(batch_size=args.ch_batch)
            print(f"  Result: {result}")
        except Exception as exc:
            print(f"  Tier 3 FAILED: {exc}")
            traceback.print_exc()
    else:
        print("\n--- Tier 3: Skipped (re-run with --ch to enable) ---")

    # -------------------------------------------------------------------
    # Tier 4: Leasehold cohort flag (counts only)
    # -------------------------------------------------------------------
    print("\n--- Tier 4: Leasehold cohort awaiting HMLR register pull ---")
    try:
        from app.tasks.enrichment_tasks import flag_leasehold_schemes_for_review
        result = flag_leasehold_schemes_for_review()
        print(f"  Result: {result}")
    except Exception as exc:
        print(f"  Tier 4 FAILED: {exc}")
        traceback.print_exc()

    # -------------------------------------------------------------------
    # Final report
    # -------------------------------------------------------------------
    total, after_scheme, after_contract = counts()
    pct_scheme = after_scheme * 100 // max(total, 1)
    filled_schemes = before_scheme - after_scheme
    filled_contracts = before_contract - after_contract
    elapsed = (datetime.datetime.now() - started).total_seconds()
    print("\n" + "=" * 70)
    print(f"After: {total} schemes, {after_scheme} missing end_date ({pct_scheme}%), {after_contract} contracts missing end_date")
    print(f"Filled this run: {filled_schemes} schemes, {filled_contracts} contracts ({elapsed:.0f}s elapsed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
