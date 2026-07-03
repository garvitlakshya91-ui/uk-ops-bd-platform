"""Map RSH regulatory judgements onto scheme operator-health (credit-free).

Housing associations aren't on Companies House (registered societies), so
the CH-signal arrears score can't cover them. The Regulator of Social
Housing publishes governance (G1-G4) and viability (V1-V4) judgements per
provider — the authoritative health signal for social landlords.

This runner:
  1. fetches current judgements via the existing RSHScraper,
  2. matches providers to our companies by normalised name,
  3. fans out to ALL the provider's BD schemes (operator or owner link),
     setting regulatory_rating ("G1/V2"), financial_health_score, and —
     only where no CH-based score exists — an arrears_risk_score derived
     from the worse of the two grades.

Idempotent; never overwrites a CH-derived arrears score.

Usage:
    python run_rsh_health_map.py [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)

# grade (numeric part, worse of G/V) -> arrears-style risk score
GRADE_RISK = {1: 15.0, 2: 30.0, 3: 65.0, 4: 85.0}
VIABILITY_FIN = {"V1": 95.0, "V2": 75.0, "V3": 45.0, "V4": 20.0}


def norm(s: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())
    s = re.sub(r"\b(the|of|and|ltd|limited|plc|llp|group|housing|association"
               r"|trust|society|homes|registered|provider)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


JUDGEMENTS_XLSX = os.path.join(os.path.dirname(__file__), "data", "rsh",
                               "rsh_judgements.xlsx")


def fetch_judgements():
    """Load the official RSH judgements spreadsheet (gov.uk, updated ~monthly).

    Preferred over scraping the judgement pages: one authoritative file with
    Governance (G1-G4), Viability (V1-V4) and Consumer (C1-C4) grades plus
    group-member aliases. Consumer grades also cover local authorities.
    """
    import openpyxl

    wb = openpyxl.load_workbook(JUDGEMENTS_XLSX, read_only=True)
    ws = wb["Regulatory Judgements"]
    rows = ws.iter_rows(values_only=True)
    header = [str(h or "") for h in next(rows)]
    ix = {h: i for i, h in enumerate(header)}
    out = []
    for r in rows:
        if not r or not r[ix["Landlord"]]:
            continue
        if str(r[ix["Status"]] or "").strip().lower() != "current":
            continue
        aliases = str(r[ix["Other landlords included in the judgement"]] or "")
        out.append({
            "provider_name": str(r[ix["Landlord"]]).strip(),
            "aliases": [a.strip() for a in aliases.split(",")
                        if a.strip() and a.strip().lower() != "none"],
            "governance_rating": str(r[ix["Governance Grade"]] or "").strip(),
            "viability_rating": str(r[ix["Viability Grade"]] or "").strip(),
            "consumer_rating": str(r[ix["Consumer grade"]] or "").strip(),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("Fetching RSH regulatory judgements...")
    judgements = fetch_judgements()
    print(f"  {len(judgements)} judgements fetched")

    engine = create_engine(DB_URL)
    stats = Counter()
    with engine.begin() as c:
        companies = c.execute(text("""
            SELECT co.id, co.name FROM companies co
            WHERE EXISTS (SELECT 1 FROM existing_schemes es
                WHERE co.id IN (es.operator_company_id, es.owner_company_id)
                  AND es.scheme_type IN ('BTR','PBSA','Co-living','Senior'))
        """)).fetchall()
        by_norm: dict[str, list[int]] = {}
        for cid, nm in companies:
            key = norm(nm)
            if key:
                by_norm.setdefault(key, []).append(cid)

        for j in judgements:
            name = (j.get("provider_name") or "").strip()
            g = (j.get("governance_rating") or "").strip().upper()
            v = (j.get("viability_rating") or "").strip().upper()
            cons = (j.get("consumer_rating") or "").strip().upper()
            if not name or not (g or v or cons):
                stats["no_usable_data"] += 1
                continue
            cids: list[int] = []
            for nm in [name] + j.get("aliases", []):
                cids.extend(by_norm.get(norm(nm), []))
            cids = list(set(cids))
            if not cids:
                stats["provider_not_in_db"] += 1
                continue
            stats["providers_matched"] += 1

            grades = [int(x[1]) for x in (g, v, cons)
                      if re.fullmatch(r"[GVC][1-4]", x)]
            risk = GRADE_RISK.get(max(grades)) if grades else None
            fin = VIABILITY_FIN.get(v)
            label = "/".join(x for x in (g, v, cons)
                             if re.fullmatch(r"[GVC][1-4]", x))

            if args.dry_run:
                stats["schemes_would_update"] += c.execute(text("""
                    SELECT COUNT(*) FROM existing_schemes
                    WHERE scheme_type IN ('BTR','PBSA','Co-living','Senior')
                      AND (operator_company_id = ANY(:ids)
                           OR owner_company_id = ANY(:ids))
                """), {"ids": cids}).scalar()
                continue

            # regulatory rating + financial health on all the provider's schemes
            n1 = c.execute(text("""
                UPDATE existing_schemes SET
                    regulatory_rating = :lab,
                    financial_health_score = COALESCE(:fin, financial_health_score),
                    updated_at = NOW()
                WHERE scheme_type IN ('BTR','PBSA','Co-living','Senior')
                  AND (operator_company_id = ANY(:ids)
                       OR owner_company_id = ANY(:ids))
            """), {"lab": label[:20], "fin": fin, "ids": cids}).rowcount
            stats["schemes_rated"] += n1

            # arrears-style health score ONLY where no CH-based score exists
            if risk is not None:
                n2 = c.execute(text("""
                    UPDATE existing_schemes SET
                        arrears_risk_score = :r,
                        arrears_checked_at = NOW(),
                        notes = LEFT(COALESCE(notes,'') ||
                                ' Health from RSH judgement (' || :lab || ').', 1000),
                        updated_at = NOW()
                    WHERE scheme_type IN ('BTR','PBSA','Co-living','Senior')
                      AND arrears_risk_score IS NULL
                      AND (operator_company_id = ANY(:ids)
                           OR owner_company_id = ANY(:ids))
                """), {"r": risk, "lab": label, "ids": cids}).rowcount
                stats["scores_set"] += n2

    print(f"\n=== {'DRY RUN' if args.dry_run else 'APPLIED'} ===")
    for k, val in stats.most_common():
        print(f"   {k:24} {val:,}")


if __name__ == "__main__":
    main()
