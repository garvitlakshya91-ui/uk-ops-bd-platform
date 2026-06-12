"""Tier-3 detail-page enrichment for BD-actionable planning applications.

Fetches the council portal detail page for each application that's missing
``applicant_name`` and/or ``submission_date``, parses out the fields, updates
the DB, and re-runs the broadened classifier.

Scope (default):
- planning_applications WHERE
    status IN ('Pending', 'Pre-Application', 'Submitted')
    AND (num_units >= 20 OR scheme_type IN BD-set)
    AND (applicant_name IS NULL OR applicant_name = '')
    AND council.portal_type IN ('idox', 'nec', 'civica')

Skipped (need different strategy):
- portal_type = 'api' councils (no detail page — would need AI/web search)

Usage::

    python enrich_detail_pages.py [--limit N] [--council "Bradford"] [--portal idox] [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
_db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
if "@postgres:" in _db_url:
    _db_url = _db_url.replace("@postgres:", "@localhost:")
os.environ["DATABASE_URL"] = _db_url

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.scrapers.base import BaseScraper

# ----------------------------------------------------------------------------
# Detail-page extraction helpers (shared across portal types)
# ----------------------------------------------------------------------------

USER_AGENT = "Mozilla/5.0 (compatible; ukops-bd-platform/1.0)"

# Field-label patterns we look for on detail pages. Order matters — first
# match wins per category.
APPLICANT_LABELS = (
    "applicant name", "applicant", "name of applicant",
    "applicants name", "applicant's name",
)
AGENT_LABELS = (
    "agent name", "agent", "name of agent",
)
DATE_RECEIVED_LABELS = (
    "date received", "received date", "application received",
    "application received date", "registered date", "received",
    "valid from", "valid date", "date validated", "submission date",
    "submitted date", "application submitted",
)


def _normalise(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower()).rstrip(":")


def _kv_from_html(html: str) -> dict[str, str]:
    """Walk all <th>/<td>, <span class=label>/<span class=value>, <dt>/<dd>
    pairs and return a lowercase-key map."""
    kv: dict[str, str] = {}
    soup = BeautifulSoup(html, "html.parser")
    for row in soup.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if th and td:
            k = _normalise(th.get_text())
            v = td.get_text(strip=True)
            if k and v and k not in kv:
                kv[k] = v
    for label in soup.select("span.label"):
        v_el = label.find_next_sibling("span", class_="value")
        if not v_el and label.parent:
            v_el = label.parent.find("span", class_="value")
        if v_el:
            k = _normalise(label.get_text())
            v = v_el.get_text(strip=True)
            if k and v and k not in kv:
                kv[k] = v
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            k = _normalise(dt.get_text())
            v = dd.get_text(strip=True)
            if k and v and k not in kv:
                kv[k] = v
    return kv


def _parse_date(s: str) -> Optional[str]:
    """Try a few common UK formats. Returns ISO date string or None."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d %b %Y", "%d %B %Y",
                "%Y-%m-%d", "%d-%m-%Y", "%d %b %y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _pick(kv: dict[str, str], labels: tuple[str, ...]) -> Optional[str]:
    for lbl in labels:
        if lbl in kv:
            return kv[lbl]
    # Substring fallback
    for k, v in kv.items():
        for lbl in labels:
            if lbl in k:
                return v
    return None


# ----------------------------------------------------------------------------
# Portal-specific URL construction
# ----------------------------------------------------------------------------

def build_search_url(portal_type: str, portal_url: str, reference: str) -> Optional[str]:
    """Construct a simple-search URL that surfaces the application's detail
    page (or a single-row results page linking to it).
    """
    if not portal_url:
        return None
    base = portal_url.rstrip("/")
    # Strip /online-applications if present, we'll re-add as needed.
    if portal_type == "idox":
        if "/online-applications" not in base:
            base = base + "/online-applications"
        return (
            f"{base}/simpleSearchResults.do?action=firstPage&"
            f"searchType=Application&caseNo={httpx.URL(reference).raw_path.decode() if False else reference}"
        )
    if portal_type == "nec":
        # NEC PlanningExplorer pattern
        return (
            f"{base}/Northgate/PlanningExplorer/Generic/StdResults.aspx?"
            f"PT=Application&PARAM0={reference}"
        )
    if portal_type == "civica":
        return f"{base}/CAPS/search?simpleSearch={reference}"
    return None


def extract_detail_link(html: str, base_url: str) -> Optional[str]:
    """Given a search-results HTML page, find the link to the application's
    detail tab. Idox uses applicationDetails.do?keyVal=...; NEC uses
    StdDetails.aspx; Civica uses similar patterns."""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "applicationDetails.do" in href or "StdDetails.aspx" in href:
            if href.startswith("http"):
                return href
            if href.startswith("/"):
                # absolute path on same host
                from urllib.parse import urljoin
                return urljoin(base_url, href)
            from urllib.parse import urljoin
            return urljoin(base_url + "/", href)
    return None


# ----------------------------------------------------------------------------
# Main enrichment loop
# ----------------------------------------------------------------------------

async def enrich_row(
    client: httpx.AsyncClient,
    portal_type: str,
    portal_url: str,
    reference: str,
) -> dict[str, Any]:
    """Return {'applicant_name', 'agent_name', 'submission_date',
    'detail_url'} for one row. Empty fields if not found."""
    out = {
        "applicant_name": None, "agent_name": None,
        "submission_date": None, "detail_url": None,
    }
    search_url = build_search_url(portal_type, portal_url, reference)
    if not search_url:
        return out

    try:
        r = await client.get(search_url, follow_redirects=True, timeout=20.0)
    except Exception:
        return out
    if r.status_code != 200:
        return out

    html = r.text
    detail_url = str(r.url)

    # If the response is a results page, walk to detail. Idox detail pages
    # have applicationDetails.do in their URL.
    if "applicationDetails.do" not in detail_url and "StdDetails.aspx" not in detail_url:
        link = extract_detail_link(html, portal_url)
        if link:
            # Switch to the "details" tab for Idox (where applicant lives)
            if "applicationDetails.do" in link and "activeTab=" not in link:
                link = link + ("&" if "?" in link else "?") + "activeTab=details"
            try:
                r2 = await client.get(link, follow_redirects=True, timeout=20.0)
                if r2.status_code == 200:
                    html = r2.text
                    detail_url = str(r2.url)
            except Exception:
                pass
        else:
            return out

    # Switch to details tab on Idox if we're on summary
    if "applicationDetails.do" in detail_url and "activeTab=details" not in detail_url:
        details_url = detail_url.replace("activeTab=summary", "activeTab=details")
        if "activeTab=" not in details_url:
            details_url += ("&" if "?" in details_url else "?") + "activeTab=details"
        try:
            r3 = await client.get(details_url, follow_redirects=True, timeout=20.0)
            if r3.status_code == 200:
                html = r3.text
                detail_url = details_url
        except Exception:
            pass

    out["detail_url"] = detail_url
    kv = _kv_from_html(html)
    applicant = _pick(kv, APPLICANT_LABELS)
    if applicant:
        # Avoid garbage like the case ref number being read as applicant
        if not re.search(r"\d+/|^/\d+", applicant):
            out["applicant_name"] = applicant
    agent = _pick(kv, AGENT_LABELS)
    if agent and not re.search(r"\d+/|^/\d+", agent):
        out["agent_name"] = agent
    date_raw = _pick(kv, DATE_RECEIVED_LABELS)
    if date_raw:
        iso = _parse_date(date_raw)
        out["submission_date"] = iso
    return out


async def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=50, help="Max rows to enrich (default 50)")
    p.add_argument("--council", type=str, default=None, help="Filter to one council by name")
    p.add_argument("--portal", type=str, default=None, choices=["idox", "nec", "civica"],
                   help="Filter to one portal type")
    p.add_argument("--dry-run", action="store_true", help="Fetch but don't update DB")
    p.add_argument("--concurrency", type=int, default=3, help="Concurrent fetches (default 3)")
    args = p.parse_args()

    engine = create_engine(os.environ["DATABASE_URL"])
    Session = sessionmaker(bind=engine)

    # Build the working set
    where_extra = []
    params: dict[str, Any] = {"limit": args.limit}
    if args.council:
        where_extra.append("co.name = :council")
        params["council"] = args.council
    if args.portal:
        where_extra.append("co.portal_type = :portal")
        params["portal"] = args.portal
    extra = (" AND " + " AND ".join(where_extra)) if where_extra else ""

    sql = f"""
        SELECT pa.id, pa.reference, co.id AS council_id, co.name AS council_name,
               co.portal_type, co.portal_url
        FROM planning_applications pa
        JOIN councils co ON pa.council_id = co.id
        WHERE pa.status IN ('Pending', 'Pre-Application', 'Submitted')
          AND (pa.num_units >= 20 OR pa.scheme_type IN
              ('BTR', 'PBSA', 'Co-living', 'Senior', 'Affordable', 'Mixed'))
          AND (pa.applicant_name IS NULL OR pa.applicant_name = '')
          AND co.portal_type IN ('idox', 'nec', 'civica')
          AND pa.reference !~ '^brownfield:'   -- exclude brownfield-register entries
          {extra}
        ORDER BY pa.num_units DESC NULLS LAST
        LIMIT :limit
    """
    with engine.connect() as c:
        rows = c.execute(text(sql), params).fetchall()

    print(f"Working set: {len(rows)} rows")
    if not rows:
        return 0

    semaphore = asyncio.Semaphore(args.concurrency)
    results: list[tuple[int, dict[str, Any]]] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=30.0,
    ) as client:
        async def _process(row):
            async with semaphore:
                res = await enrich_row(
                    client, row.portal_type, row.portal_url, row.reference
                )
                results.append((row.id, {**res, "ref": row.reference,
                                          "council": row.council_name,
                                          "portal_type": row.portal_type}))
                await asyncio.sleep(0.4)

        await asyncio.gather(*(_process(r) for r in rows))

    # Report + update
    hit_app = sum(1 for _, r in results if r["applicant_name"])
    hit_date = sum(1 for _, r in results if r["submission_date"])
    print(f"  applicant_name found: {hit_app}/{len(results)}")
    print(f"  submission_date found: {hit_date}/{len(results)}")

    # Show first 5 successes
    print()
    print("Sample successes:")
    shown = 0
    for pid, r in results:
        if r["applicant_name"] or r["submission_date"]:
            print(f"  id={pid} ref={r['ref']} ({r['council']}/{r['portal_type']})")
            print(f"    applicant: {r['applicant_name']}")
            print(f"    submission_date: {r['submission_date']}")
            print(f"    agent: {r['agent_name']}")
            shown += 1
            if shown >= 5:
                break

    # Show first 5 failures
    print()
    print("Sample failures (no fields extracted):")
    shown = 0
    for pid, r in results:
        if not r["applicant_name"] and not r["submission_date"]:
            print(f"  id={pid} ref={r['ref']} ({r['council']}/{r['portal_type']})  detail_url={r['detail_url']}")
            shown += 1
            if shown >= 5:
                break

    if args.dry_run:
        print("\nDRY-RUN: not writing DB. Pass without --dry-run to commit.")
        return 0

    # Apply updates + reclassify each row
    db = Session()
    updated_app = 0
    updated_date = 0
    reclassified = 0
    for pid, r in results:
        sets = []
        params2 = {"id": pid}
        if r["applicant_name"]:
            sets.append("applicant_name = :ap")
            params2["ap"] = r["applicant_name"]
            updated_app += 1
        if r["agent_name"]:
            sets.append("agent_name = :ag")
            params2["ag"] = r["agent_name"]
        if r["submission_date"]:
            sets.append("submission_date = CAST(:sd AS date)")
            params2["sd"] = r["submission_date"]
            updated_date += 1
        if not sets:
            continue
        db.execute(text(
            f"UPDATE planning_applications SET {', '.join(sets)}, updated_at = NOW() WHERE id = :id"
        ), params2)
        # Re-classify this row using the broadened classifier
        row = db.execute(text("""
            SELECT description, applicant_name, agent_name, scheme_type
            FROM planning_applications WHERE id = :id
        """), {"id": pid}).fetchone()
        if row:
            new_t = BaseScraper.classify_scheme_type(
                row.description,
                applicant_name=row.applicant_name,
                agent_name=row.agent_name,
            )
            if new_t != (row.scheme_type or "Unknown"):
                # Upgrade-only: only overwrite if new is more specific.
                spec = {"Unknown": 0, "Residential": 1, "Affordable": 2,
                        "Mixed": 3, "Senior": 4, "Co-living": 5,
                        "PBSA": 6, "BTR": 7}
                if spec.get(new_t, 0) >= spec.get(row.scheme_type or "Unknown", 0):
                    db.execute(text(
                        "UPDATE planning_applications SET scheme_type = :t WHERE id = :id"
                    ), {"t": new_t, "id": pid})
                    reclassified += 1
    db.commit()
    db.close()

    print()
    print("=" * 60)
    print(f"Applied: applicant_name={updated_app}, submission_date={updated_date}, scheme_type_upgraded={reclassified}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
