"""
Enrich EPC new-dwelling discovered schemes with owner/operator attribution.

Strategies (applied in priority order):
  1. Exact postcode match to ARL BTR schemes -> inherit developer (owner) & operator
  2. Exact postcode match to planning_applications -> inherit applicant company as owner
  3. Exact postcode match to find_a_tender / contracts_finder schemes -> inherit owner/operator
  4. Postcode match to Companies House tracked SPVs (by registered address postcode)

Run from backend/:
    python scripts/enrich_epc_schemes.py
"""
from __future__ import annotations

import re
import sys
import os

# Ensure the backend package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.database import SessionLocal


def extract_postcode(address: str) -> str | None:
    """Extract a UK postcode from an address string."""
    if not address:
        return None
    m = re.search(
        r"([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})",
        address.upper(),
    )
    return m.group(1).strip() if m else None


def normalise_postcode(pc: str) -> str:
    """Ensure consistent postcode formatting: 'SW1A 1AA'."""
    pc = pc.strip().upper()
    # Remove all spaces, then re-insert the canonical space
    pc = pc.replace(" ", "")
    if len(pc) >= 5:
        return pc[:-3] + " " + pc[-3:]
    return pc


def run_enrichment():
    db = SessionLocal()
    try:
        # ------------------------------------------------------------------
        # Pre-flight: counts before enrichment
        # ------------------------------------------------------------------
        before = db.execute(text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN owner_company_id IS NOT NULL THEN 1 ELSE 0 END) AS has_owner,
                SUM(CASE WHEN operator_company_id IS NOT NULL THEN 1 ELSE 0 END) AS has_operator
            FROM existing_schemes
            WHERE source = 'epc_new_dwelling'
        """)).fetchone()
        total_epc = before[0]
        owner_before = before[1]
        operator_before = before[2]
        print(f"=== EPC scheme enrichment ===")
        print(f"Total EPC schemes: {total_epc}")
        print(f"Before: owner={owner_before}, operator={operator_before}")
        print()

        # ==================================================================
        # Strategy 1: Exact postcode match to ARL BTR schemes
        # ==================================================================
        print("Strategy 1: Match EPC -> ARL BTR by postcode ...")
        r1_owner = db.execute(text("""
            UPDATE existing_schemes epc
            SET owner_company_id = arl.owner_company_id
            FROM existing_schemes arl
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.owner_company_id IS NULL
              AND arl.source = 'arl_btr_open_operating'
              AND arl.owner_company_id IS NOT NULL
              AND epc.postcode = arl.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s1_owner = r1_owner.rowcount
        print(f"  -> owners set: {s1_owner}")

        r1_op = db.execute(text("""
            UPDATE existing_schemes epc
            SET operator_company_id = arl.operator_company_id
            FROM existing_schemes arl
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.operator_company_id IS NULL
              AND arl.source = 'arl_btr_open_operating'
              AND arl.operator_company_id IS NOT NULL
              AND epc.postcode = arl.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s1_op = r1_op.rowcount
        print(f"  -> operators set: {s1_op}")
        db.commit()

        # ==================================================================
        # Strategy 2: Exact postcode match to planning_applications
        # ==================================================================
        print("Strategy 2: Match EPC -> planning_applications by postcode ...")

        # For owner: use the applicant_company_id from the *largest* planning
        # app at that postcode (most likely the actual development).
        r2 = db.execute(text("""
            UPDATE existing_schemes epc
            SET owner_company_id = sub.applicant_company_id
            FROM (
                SELECT DISTINCT ON (p.postcode)
                    p.postcode,
                    p.applicant_company_id
                FROM planning_applications p
                WHERE p.applicant_company_id IS NOT NULL
                  AND p.postcode IS NOT NULL
                  AND p.postcode != ''
                ORDER BY p.postcode, COALESCE(p.num_units, p.total_units, 0) DESC, p.id DESC
            ) sub
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.owner_company_id IS NULL
              AND epc.postcode = sub.postcode
        """))
        s2 = r2.rowcount
        print(f"  -> owners set: {s2}")
        db.commit()

        # ==================================================================
        # Strategy 3: Exact postcode match to find_a_tender / contracts_finder
        # ==================================================================
        print("Strategy 3: Match EPC -> FAT/CF schemes by postcode ...")

        r3_owner = db.execute(text("""
            UPDATE existing_schemes epc
            SET owner_company_id = other.owner_company_id
            FROM existing_schemes other
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.owner_company_id IS NULL
              AND other.source IN ('find_a_tender', 'contracts_finder')
              AND other.owner_company_id IS NOT NULL
              AND epc.postcode = other.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s3_owner = r3_owner.rowcount
        print(f"  -> owners set: {s3_owner}")

        r3_op = db.execute(text("""
            UPDATE existing_schemes epc
            SET operator_company_id = other.operator_company_id
            FROM existing_schemes other
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.operator_company_id IS NULL
              AND other.source IN ('find_a_tender', 'contracts_finder')
              AND other.operator_company_id IS NOT NULL
              AND epc.postcode = other.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s3_op = r3_op.rowcount
        print(f"  -> operators set: {s3_op}")
        db.commit()

        # ==================================================================
        # Strategy 4: Match to Companies House SPVs by registered address postcode
        # ==================================================================
        print("Strategy 4: Match EPC -> tracked SPV companies by registered address postcode ...")

        # Build a mapping from postcode -> company ID for SPV companies
        # (companies that have a parent_company_id, indicating they are tracked SPVs)
        spv_rows = db.execute(text("""
            SELECT c.id, c.registered_address, c.parent_company_id
            FROM companies c
            WHERE c.parent_company_id IS NOT NULL
              AND c.registered_address IS NOT NULL
              AND c.registered_address != ''
              AND c.is_active = true
        """)).fetchall()

        spv_postcode_map: dict[str, int] = {}  # postcode -> parent company id
        for row in spv_rows:
            pc = extract_postcode(row[1])
            if pc:
                pc = normalise_postcode(pc)
                # Use the parent company as the owner (the actual developer group)
                spv_postcode_map[pc] = row[2]

        s4 = 0
        if spv_postcode_map:
            # Get EPC schemes still missing owner
            epc_no_owner = db.execute(text("""
                SELECT id, postcode
                FROM existing_schemes
                WHERE source = 'epc_new_dwelling'
                  AND owner_company_id IS NULL
                  AND postcode IS NOT NULL
                  AND postcode != ''
            """)).fetchall()

            for eid, epc_pc in epc_no_owner:
                norm_pc = normalise_postcode(epc_pc) if epc_pc else None
                if norm_pc and norm_pc in spv_postcode_map:
                    db.execute(
                        text("UPDATE existing_schemes SET owner_company_id = :cid WHERE id = :eid"),
                        {"cid": spv_postcode_map[norm_pc], "eid": eid},
                    )
                    s4 += 1
            db.commit()

        print(f"  -> owners set: {s4}")

        # ==================================================================
        # Strategy 5: BPF BTR pipeline postcode match
        # ==================================================================
        print("Strategy 5: Match EPC -> BPF BTR pipeline by postcode ...")
        r5_owner = db.execute(text("""
            UPDATE existing_schemes epc
            SET owner_company_id = bpf.owner_company_id
            FROM existing_schemes bpf
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.owner_company_id IS NULL
              AND bpf.source = 'bpf_btr_pipeline'
              AND bpf.owner_company_id IS NOT NULL
              AND epc.postcode = bpf.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s5_owner = r5_owner.rowcount
        print(f"  -> owners set: {s5_owner}")

        r5_op = db.execute(text("""
            UPDATE existing_schemes epc
            SET operator_company_id = bpf.operator_company_id
            FROM existing_schemes bpf
            WHERE epc.source = 'epc_new_dwelling'
              AND epc.operator_company_id IS NULL
              AND bpf.source = 'bpf_btr_pipeline'
              AND bpf.operator_company_id IS NOT NULL
              AND epc.postcode = bpf.postcode
              AND epc.postcode IS NOT NULL
              AND epc.postcode != ''
        """))
        s5_op = r5_op.rowcount
        print(f"  -> operators set: {s5_op}")
        db.commit()

        # ==================================================================
        # Strategy 6: Where we set owner but not operator, copy owner -> operator
        # (for EPC schemes, the developer is often also the operator pre-sale)
        # ==================================================================
        print("Strategy 6: Copy owner -> operator where operator still NULL ...")
        r6 = db.execute(text("""
            UPDATE existing_schemes
            SET operator_company_id = owner_company_id
            WHERE source = 'epc_new_dwelling'
              AND operator_company_id IS NULL
              AND owner_company_id IS NOT NULL
        """))
        s6 = r6.rowcount
        print(f"  -> operators set from owner: {s6}")
        db.commit()

        # ==================================================================
        # Strategy 7: Update data confidence score for enriched records
        # ==================================================================
        print("Strategy 7: Update confidence scores ...")
        db.execute(text("""
            UPDATE existing_schemes
            SET data_confidence_score = 0.85,
                last_verified_at = NOW()
            WHERE source = 'epc_new_dwelling'
              AND owner_company_id IS NOT NULL
              AND data_confidence_score < 0.85
        """))
        db.commit()

        # ------------------------------------------------------------------
        # Post-flight: counts after enrichment
        # ------------------------------------------------------------------
        after = db.execute(text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN owner_company_id IS NOT NULL THEN 1 ELSE 0 END) AS has_owner,
                SUM(CASE WHEN operator_company_id IS NOT NULL THEN 1 ELSE 0 END) AS has_operator
            FROM existing_schemes
            WHERE source = 'epc_new_dwelling'
        """)).fetchone()

        total_after = after[0]
        owner_after = after[1]
        operator_after = after[2]

        print()
        print("=" * 60)
        print("ENRICHMENT RESULTS")
        print("=" * 60)
        print(f"Total EPC schemes:        {total_after}")
        print(f"Owner attribution:        {owner_before} -> {owner_after}  (+{owner_after - owner_before})")
        print(f"Operator attribution:     {operator_before} -> {operator_after}  (+{operator_after - operator_before})")
        print(f"Owner %:                  {owner_before/total_after*100:.1f}% -> {owner_after/total_after*100:.1f}%")
        print(f"Operator %:               {operator_before/total_after*100:.1f}% -> {operator_after/total_after*100:.1f}%")
        print()
        print("Breakdown by strategy:")
        print(f"  S1 (ARL BTR postcode):           owner={s1_owner}, operator={s1_op}")
        print(f"  S2 (Planning apps postcode):      owner={s2}")
        print(f"  S3 (FAT/CF postcode):             owner={s3_owner}, operator={s3_op}")
        print(f"  S4 (SPV registered address):      owner={s4}")
        print(f"  S5 (BPF BTR postcode):            owner={s5_owner}, operator={s5_op}")
        print(f"  S6 (Owner copied to operator):    operator={s6}")
        print("=" * 60)

        # Show top companies attributed
        top = db.execute(text("""
            SELECT c.name, COUNT(*) as cnt
            FROM existing_schemes e
            JOIN companies c ON e.owner_company_id = c.id
            WHERE e.source = 'epc_new_dwelling'
            GROUP BY c.name
            ORDER BY cnt DESC
            LIMIT 15
        """)).fetchall()

        if top:
            print()
            print("Top owner companies attributed:")
            for row in top:
                print(f"  {row[0]}: {row[1]} schemes")

    finally:
        db.close()


if __name__ == "__main__":
    run_enrichment()
