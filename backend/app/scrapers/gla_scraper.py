"""
GLA Planning London Datahub scraper.

Fetches planning application data from the Planning London Datahub, which
aggregates planning data across all 33 London boroughs.

The Datahub exposes a public API (backed by the data.london.gov.uk platform /
CKAN) that returns JSON.  We query for residential planning applications with
10+ units and save them to the planning_applications table.

Data source:
    https://planninglondondatahub.london.gov.uk/
    API (CKAN datastore): https://data.london.gov.uk/api/
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
import structlog

from app.config import settings
from app.scrapers.base import BaseScraper

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# London borough name -> council name mapping
# Used to resolve council_id from the councils table
# ---------------------------------------------------------------------------

LONDON_BOROUGHS: dict[str, str] = {
    "Barking and Dagenham": "Barking and Dagenham",
    "Barnet": "Barnet",
    "Bexley": "Bexley",
    "Brent": "Brent",
    "Bromley": "Bromley",
    "Camden": "Camden",
    "City of London": "City of London",
    "City of Westminster": "Westminster",
    "Croydon": "Croydon",
    "Ealing": "Ealing",
    "Enfield": "Enfield",
    "Greenwich": "Greenwich",
    "Hackney": "Hackney",
    "Hammersmith and Fulham": "Hammersmith and Fulham",
    "Haringey": "Haringey",
    "Harrow": "Harrow",
    "Havering": "Havering",
    "Hillingdon": "Hillingdon",
    "Hounslow": "Hounslow",
    "Islington": "Islington",
    "Kensington and Chelsea": "Kensington and Chelsea",
    "Kingston upon Thames": "Kingston upon Thames",
    "Lambeth": "Lambeth",
    "Lewisham": "Lewisham",
    "Merton": "Merton",
    "Newham": "Newham",
    "Redbridge": "Redbridge",
    "Richmond upon Thames": "Richmond upon Thames",
    "Southwark": "Southwark",
    "Sutton": "Sutton",
    "Tower Hamlets": "Tower Hamlets",
    "Waltham Forest": "Waltham Forest",
    "Wandsworth": "Wandsworth",
}

# ---------------------------------------------------------------------------
# Planning London Datahub API configuration
# ---------------------------------------------------------------------------

# The Datahub publishes data via the London Datastore CKAN API.
# The planning decisions dataset resource ID may change — this is the current one.
# Fallback: we also try the Datahub's own API endpoint.

DATAHUB_API_BASE = "https://planninglondondatahub.london.gov.uk/api/applications"

# Alternative: London Datastore CKAN API for the planning decisions dataset
LONDON_DATASTORE_API = "https://data.london.gov.uk/api/3/action/datastore_search"

# Known resource IDs for the London planning decisions dataset on data.london.gov.uk
# These can change when datasets are republished.
PLANNING_DECISIONS_RESOURCE_ID = "a674537f-7754-4626-b02e-7de0d3f15a2b"

# Minimum residential units to consider
MIN_UNITS = 10

# Page size for API queries
PAGE_SIZE = 500


class GLAPlanningDatahubScraper:
    """
    Scraper for the GLA Planning London Datahub.

    Fetches residential planning applications across all London boroughs,
    filtering for schemes with 10+ residential units.
    """

    def __init__(self) -> None:
        self.log = logger.bind(scraper="GLAPlanningDatahubScraper")
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "GLAPlanningDatahubScraper":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    # ------------------------------------------------------------------
    # Primary fetch: Planning London Datahub API
    # ------------------------------------------------------------------

    async def _fetch_datahub_api(
        self,
        page: int = 1,
        page_size: int = PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        """
        Fetch planning applications from the Datahub's own API.

        The Datahub API uses a REST-style endpoint with query parameters
        for filtering.
        """
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        params: dict[str, Any] = {
            "page": page,
            "per_page": page_size,
            "status": "Decided",  # Focus on decided applications
        }

        self.log.info("datahub_api_fetch", page=page, page_size=page_size)

        try:
            resp = await self.client.get(DATAHUB_API_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()

            # The API may return data in different structures
            if isinstance(data, dict):
                records = data.get("data", data.get("results", data.get("records", [])))
            elif isinstance(data, list):
                records = data
            else:
                records = []

            return records if isinstance(records, list) else []

        except (httpx.HTTPStatusError, httpx.ConnectError) as exc:
            self.log.warning(
                "datahub_api_unavailable",
                error=str(exc),
                fallback="london_datastore",
            )
            return []

    # ------------------------------------------------------------------
    # Fallback: London Datastore CKAN API
    # ------------------------------------------------------------------

    async def _fetch_london_datastore(
        self,
        offset: int = 0,
        limit: int = PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Query the London Datastore CKAN datastore_search API.

        Returns (records, total_count).
        """
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        params = {
            "resource_id": PLANNING_DECISIONS_RESOURCE_ID,
            "limit": limit,
            "offset": offset,
        }

        self.log.info("london_datastore_fetch", offset=offset, limit=limit)

        resp = await self.client.get(LONDON_DATASTORE_API, params=params)
        resp.raise_for_status()
        payload = resp.json()

        if not payload.get("success"):
            self.log.warning(
                "london_datastore_api_error",
                error=payload.get("error", "Unknown"),
            )
            return [], 0

        result = payload.get("result", {})
        records = result.get("records", [])
        total = result.get("total", 0)

        return records, total

    # ------------------------------------------------------------------
    # Parsing & normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        """Parse a date string from various formats."""
        if not value or value in ("", "None", "null"):
            return None
        if isinstance(value, date):
            return value
        s = str(value).strip()[:10]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_units(record: dict[str, Any]) -> Optional[int]:
        """
        Extract residential unit count from various possible fields.
        """
        # Try explicit unit fields first
        for field in (
            "residential_units", "total_residential_units",
            "no_of_residential_units", "units_total",
            "proposed_no_of_residential_units", "total_no_of_proposed_residential_units",
            "num_units", "total_units", "proposed_units",
        ):
            val = record.get(field)
            if val is not None:
                try:
                    n = int(float(str(val)))
                    if 1 <= n <= 50000:
                        return n
                except (ValueError, TypeError):
                    continue

        # Fall back to regex on description
        desc = record.get("description", "") or record.get("development_description", "") or ""
        return BaseScraper.extract_unit_count(desc)

    def _normalise_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a raw Datahub/Datastore record into a normalised dict
        matching PlanningApplication model fields.
        """
        # Extract reference
        reference = (
            record.get("reference") or
            record.get("application_reference") or
            record.get("lpa_app_no") or
            record.get("casereference") or
            ""
        ).strip()

        # Extract borough
        borough = (
            record.get("borough") or
            record.get("lpa_name") or
            record.get("planning_authority") or
            ""
        ).strip()

        # Description
        description = (
            record.get("description") or
            record.get("development_description") or
            record.get("proposal") or
            ""
        ).strip()

        # Address
        address = (
            record.get("address") or
            record.get("site_address") or
            record.get("development_address") or
            ""
        ).strip()

        # Postcode — try field or extract from address
        postcode = record.get("postcode", "")
        if not postcode and address:
            postcode = BaseScraper.extract_postcode(address) or ""

        # Status / decision
        status = (
            record.get("status") or
            record.get("application_status") or
            record.get("decision") or
            ""
        ).strip()

        decision = (
            record.get("decision") or
            record.get("decision_description") or
            ""
        ).strip()

        # Dates
        submitted = self._parse_date(
            record.get("submission_date") or
            record.get("date_received") or
            record.get("registered_date")
        )
        decision_date = self._parse_date(
            record.get("decision_date") or
            record.get("date_decision")
        )

        # Unit count
        units = self._extract_units(record)

        # Classify scheme type
        scheme_type = BaseScraper.classify_scheme_type(description)

        # Check for BTR / PBSA / Affordable flags
        desc_lower = (description or "").lower()
        is_btr = any(kw in desc_lower for kw in ("build to rent", "btr", "build-to-rent"))
        is_pbsa = any(kw in desc_lower for kw in ("student", "pbsa", "student accommodation"))
        is_affordable = any(kw in desc_lower for kw in ("affordable", "social rent", "shared ownership"))

        # Application type
        app_type = (
            record.get("application_type") or
            record.get("app_type") or
            record.get("development_type") or
            ""
        ).strip()

        # Applicant
        applicant = (
            record.get("applicant_name") or
            record.get("applicant") or
            ""
        ).strip()

        # Agent
        agent = (
            record.get("agent_name") or
            record.get("agent") or
            record.get("case_officer") or
            ""
        ).strip()

        # Ward
        ward = (
            record.get("ward") or
            record.get("ward_name") or
            ""
        ).strip()

        # Coordinates
        lat = None
        lng = None
        for lat_field in ("latitude", "lat", "northing"):
            v = record.get(lat_field)
            if v is not None:
                try:
                    lat = float(v)
                    if not (-90 <= lat <= 90):
                        lat = None
                except (ValueError, TypeError):
                    pass
        for lng_field in ("longitude", "lng", "lon", "easting"):
            v = record.get(lng_field)
            if v is not None:
                try:
                    lng = float(v)
                    if not (-180 <= lng <= 180):
                        lng = None
                except (ValueError, TypeError):
                    pass

        return {
            "reference": reference,
            "borough": borough,
            "address": address,
            "postcode": postcode,
            "description": description,
            "applicant_name": applicant,
            "agent_name": agent,
            "application_type": app_type,
            "status": BaseScraper.normalise_status(status),
            "decision": decision,
            "scheme_type": scheme_type,
            "total_units": units,
            "submitted_date": submitted,
            "decision_date": decision_date,
            "ward": ward,
            "latitude": lat,
            "longitude": lng,
            "is_btr": is_btr,
            "is_pbsa": is_pbsa,
            "is_affordable": is_affordable,
            "source": "gla_planning_datahub",
            "raw_data": record,
        }

    # ------------------------------------------------------------------
    # Main fetch loop
    # ------------------------------------------------------------------

    async def fetch_all(
        self,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Fetch all residential planning applications with 10+ units
        from the Datahub.

        First tries the Datahub API, falls back to the London Datastore
        CKAN API.

        Returns normalised records ready for DB persistence.
        """
        all_records: list[dict[str, Any]] = []

        # Strategy 1: Try the Datahub's own API
        self.log.info("gla_fetch_start", strategy="datahub_api")
        for page in range(1, max_pages + 1):
            records = await self._fetch_datahub_api(page=page)
            if not records:
                if page == 1:
                    # API returned nothing on first page — fall back
                    self.log.info("datahub_api_empty_first_page", fallback="london_datastore")
                    break
                break  # No more pages
            all_records.extend(records)
            if len(records) < PAGE_SIZE:
                break  # Last page

        # Strategy 2: Fall back to London Datastore if Datahub API didn't work
        if not all_records:
            self.log.info("gla_fetch_fallback", strategy="london_datastore")
            offset = 0
            for _ in range(max_pages):
                try:
                    records, total = await self._fetch_london_datastore(
                        offset=offset, limit=PAGE_SIZE
                    )
                except Exception as exc:
                    self.log.warning("london_datastore_fetch_failed", error=str(exc))
                    break

                if not records:
                    break

                all_records.extend(records)
                offset += len(records)

                if offset >= total:
                    break

        self.log.info("gla_raw_records_fetched", count=len(all_records))

        # Normalise and filter
        normalised: list[dict[str, Any]] = []
        for record in all_records:
            try:
                parsed = self._normalise_record(record)

                # Must have a reference
                if not parsed.get("reference"):
                    continue

                # Filter for 10+ units (or keep if units unknown — will be
                # filtered downstream)
                units = parsed.get("total_units")
                if units is not None and units < MIN_UNITS:
                    continue

                normalised.append(parsed)

            except Exception as exc:
                self.log.warning(
                    "gla_normalise_failed",
                    error=str(exc),
                    reference=record.get("reference", "unknown"),
                )

        self.log.info(
            "gla_fetch_complete",
            raw_count=len(all_records),
            normalised_count=len(normalised),
        )
        return normalised


def save_gla_planning_applications(
    records: list[dict[str, Any]],
    db: "Session",  # noqa: F821
) -> dict[str, int]:
    """
    Persist GLA planning applications to the planning_applications table.

    Resolves borough names to council_id from the councils table.
    Upserts by (reference, council_id).

    Parameters
    ----------
    records : list
        Normalised record dicts from GLAPlanningDatahubScraper.fetch_all().
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict with keys: found, new, updated, errors.
    """
    from app.models.models import Council, PlanningApplication

    found = len(records)
    new = 0
    updated = 0
    errors = 0

    # Build council lookup cache
    council_cache: dict[str, int] = {}
    councils = db.query(Council).all()
    for c in councils:
        council_cache[c.name.lower()] = c.id

    def _resolve_council_id(borough: str) -> Optional[int]:
        """Find council_id for a London borough name."""
        if not borough:
            return None
        # Direct match
        lower = borough.lower()
        if lower in council_cache:
            return council_cache[lower]
        # Try mapped name
        mapped = LONDON_BOROUGHS.get(borough)
        if mapped and mapped.lower() in council_cache:
            return council_cache[mapped.lower()]
        # Fuzzy: look for borough name as substring
        for cname, cid in council_cache.items():
            if lower in cname or cname in lower:
                return cid
        return None

    for rec in records:
        try:
            reference = rec.get("reference", "")
            borough = rec.get("borough", "")
            council_id = _resolve_council_id(borough)

            if not reference:
                errors += 1
                continue

            if not council_id:
                # Create the council if it doesn't exist (London borough)
                if borough and borough in LONDON_BOROUGHS:
                    new_council = Council(
                        name=LONDON_BOROUGHS.get(borough, borough),
                        portal_type="api",
                        region="London",
                        active=True,
                    )
                    db.add(new_council)
                    db.flush()
                    council_id = new_council.id
                    council_cache[new_council.name.lower()] = council_id
                else:
                    # Skip records without a resolvable council
                    errors += 1
                    continue

            # Check for existing record
            existing = (
                db.query(PlanningApplication)
                .filter(
                    PlanningApplication.reference == reference,
                    PlanningApplication.council_id == council_id,
                )
                .first()
            )

            if existing:
                changed = False
                update_fields = {
                    "address": rec.get("address"),
                    "postcode": rec.get("postcode"),
                    "description": rec.get("description"),
                    "applicant_name": rec.get("applicant_name"),
                    "agent_name": rec.get("agent_name"),
                    "application_type": rec.get("application_type"),
                    "status": rec.get("status"),
                    "scheme_type": rec.get("scheme_type"),
                    "num_units": rec.get("total_units"),
                    "submission_date": rec.get("submitted_date"),
                    "decision_date": rec.get("decision_date"),
                }
                for field, value in update_fields.items():
                    if value and value != getattr(existing, field, None):
                        setattr(existing, field, value)
                        changed = True
                if changed:
                    updated += 1
            else:
                app = PlanningApplication(
                    reference=reference,
                    council_id=council_id,
                    address=rec.get("address"),
                    postcode=rec.get("postcode"),
                    description=rec.get("description"),
                    applicant_name=rec.get("applicant_name"),
                    agent_name=rec.get("agent_name"),
                    application_type=rec.get("application_type"),
                    status=rec.get("status"),
                    scheme_type=rec.get("scheme_type", "Unknown"),
                    num_units=rec.get("total_units"),
                    submission_date=rec.get("submitted_date"),
                    decision_date=rec.get("decision_date"),
                )
                db.add(app)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_gla_planning_application_failed",
                reference=rec.get("reference"),
                borough=rec.get("borough"),
            )
            errors += 1
            db.rollback()

    logger.info(
        "save_gla_planning_applications_complete",
        found=found,
        new=new,
        updated=updated,
        errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}
