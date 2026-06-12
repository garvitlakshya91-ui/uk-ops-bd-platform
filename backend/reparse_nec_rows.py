"""Re-extract status/submission_date/num_units for existing NEC-scraped rows.

Uses the raw_data JSON saved on each row to apply the fixed parsing logic
without re-scraping the council portal.
"""
import json, sys
from datetime import datetime
from sqlalchemy import create_engine, text

sys.path.insert(0, ".")
from app.scrapers.base import BaseScraper

e = create_engine("postgresql://postgres:postgres@localhost:5432/uk_ops_bd")


def _parse_uk_date(s):
    """Parse '23-03-2026' or '23/03/2026' or '23 March 2026' to ISO date."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


with e.connect() as c:
    rows = c.execute(text("""
        SELECT pa.id, pa.reference, pa.raw_data, pa.description,
               pa.applicant_name, pa.status, pa.num_units, pa.submission_date,
               co.name AS council
        FROM planning_applications pa
        JOIN councils co ON pa.council_id = co.id
        WHERE pa.source = 'nec_scraper'
          AND (pa.status IS NULL OR pa.status = '' OR pa.status = 'Unknown'
               OR pa.num_units IS NULL
               OR pa.submission_date IS NULL)
    """)).fetchall()
    print(f"Found {len(rows)} NEC-scraped rows needing re-parse")

updated_count = 0
status_set = 0
date_set = 0
units_set = 0
type_changed = 0

with e.begin() as conn:
    for r in rows:
        raw = r.raw_data
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                continue
        if not isinstance(raw, dict):
            continue

        updates = {}

        # 1) submission_date from "application registered"
        if not r.submission_date:
            for key in (
                "application registered", "application received",
                "application date", "submitted", "date of application",
                "date received", "received", "registered",
            ):
                val = raw.get(key)
                iso = _parse_uk_date(val) if val else None
                if iso:
                    updates["submission_date"] = iso
                    date_set += 1
                    break

        # 2) status — default to Pending if no decision date and we have a recent
        # submission date
        if r.status in (None, "", "Unknown"):
            decision = raw.get("decision date") or raw.get("decided") or raw.get("decision")
            sub_date = updates.get("submission_date") or (r.submission_date and r.submission_date.isoformat())
            if not decision and sub_date:
                updates["status"] = "Pending"
                status_set += 1

        # 3) num_units via broadened regex on description
        if r.num_units is None and r.description:
            units = BaseScraper.extract_unit_count(r.description)
            if units:
                updates["num_units"] = units
                units_set += 1

        # 4) Re-classify scheme_type with applicant_name (broadened classifier)
        if r.applicant_name:
            new_type = BaseScraper.classify_scheme_type(
                r.description, applicant_name=r.applicant_name
            )
            specificity = {"Unknown": 0, "Residential": 1, "Affordable": 2,
                           "Mixed": 3, "Senior": 4, "Co-living": 5,
                           "PBSA": 6, "BTR": 7}
            # current scheme_type from row not selected — re-fetch
            current_type = conn.execute(
                text("SELECT scheme_type FROM planning_applications WHERE id=:id"),
                {"id": r.id},
            ).scalar() or "Unknown"
            if specificity.get(new_type, 0) > specificity.get(current_type, 0):
                updates["scheme_type"] = new_type
                type_changed += 1

        if not updates:
            continue

        set_parts = []
        params = {"id": r.id}
        for k, v in updates.items():
            set_parts.append(f"{k} = :{k}")
            params[k] = v
        conn.execute(
            text(f"UPDATE planning_applications SET {', '.join(set_parts)}, updated_at = NOW() WHERE id = :id"),
            params,
        )
        updated_count += 1

print(f"\nUpdated rows: {updated_count}")
print(f"  submission_date set:  {date_set}")
print(f"  status set to Pending: {status_set}")
print(f"  num_units set:        {units_set}")
print(f"  scheme_type upgraded: {type_changed}")
