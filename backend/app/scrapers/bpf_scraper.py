"""
BPF Build-to-Rent pipeline data scraper.

The British Property Federation (BPF) publishes quarterly BTR pipeline data
via https://bfrdata.co.uk/ (formerly buildtorent.info). The interactive map
at that URL is backed by an API that serves scheme-level data as JSON.

This scraper:
1. Queries the BPF/Savills BTR data API for all known BTR schemes
2. Parses scheme details (location, developer, unit count, status)
3. Saves/updates ExistingScheme records with source="bpf_btr_pipeline"

Data source:
    https://bfrdata.co.uk/ (interactive map)
    https://www.bpf.org.uk/what-we-do/bpf-build-to-rent-map/
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
# BPF / BTR API endpoints
# ---------------------------------------------------------------------------

# The BPF BTR interactive map loads data from an API.  These are the known
# endpoint patterns — the map front-end queries these for scheme data.

BPF_MAP_API_BASE = "https://bfrdata.co.uk/api"
BPF_SCHEMES_ENDPOINT = f"{BPF_MAP_API_BASE}/schemes"
BPF_MAP_DATA_ENDPOINT = f"{BPF_MAP_API_BASE}/map-data"

# Alternative endpoints that have been observed
ALT_API_ENDPOINTS = [
    "https://bfrdata.co.uk/api/v1/schemes",
    "https://bfrdata.co.uk/api/developments",
    "https://buildtorent.info/api/schemes",
    "https://www.buildtorent.info/api/schemes",
]

# BPF quarterly report page (for PDF fallback)
BPF_REPORTS_URL = "https://www.bpf.org.uk/what-we-do/bpf-build-to-rent-map/"

# BTR scheme statuses
BTR_STATUS_MAP = {
    "complete": "operational",
    "completed": "operational",
    "operational": "operational",
    "under construction": "under_construction",
    "construction": "under_construction",
    "in construction": "under_construction",
    "in planning": "planned",
    "planning": "planned",
    "planning approved": "planned",
    "planning submitted": "planned",
    "pre-planning": "planned",
    "proposed": "planned",
    "planning granted": "planned",
    "permitted": "planned",
}


class BPFBTRScraper:
    """
    Scraper for BPF Build-to-Rent pipeline data.

    Queries the interactive map API for scheme-level data including
    location, developer, unit count, and pipeline status.
    """

    def __init__(self) -> None:
        self.log = logger.bind(scraper="BPFBTRScraper")
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BPFBTRScraper":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/html, */*",
                "Referer": "https://bfrdata.co.uk/",
            },
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    # ------------------------------------------------------------------
    # API discovery & fetch
    # ------------------------------------------------------------------

    async def _try_endpoint(self, url: str, params: dict | None = None) -> list[dict]:
        """Try a single API endpoint and return records or empty list."""
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code != 200:
                return []

            data = resp.json()

            # Handle various response shapes
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # Common patterns: data, results, schemes, features
                for key in ("data", "results", "schemes", "features", "records", "items"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # GeoJSON format
                if data.get("type") == "FeatureCollection" and "features" in data:
                    return data["features"]
            return []

        except (httpx.HTTPError, ValueError) as exc:
            self.log.debug("endpoint_failed", url=url, error=str(exc))
            return []

    async def _fetch_map_page_data(self) -> list[dict]:
        """
        Try to extract scheme data from the map page's embedded JavaScript
        or inline JSON data.
        """
        if not self.client:
            return []

        try:
            resp = await self.client.get("https://bfrdata.co.uk/")
            if resp.status_code != 200:
                return []

            html = resp.text

            # Look for JSON data embedded in the page
            # Common patterns: window.__data, var schemes =, JSON.parse
            patterns = [
                r'(?:window\.__data|window\.schemes|var\s+schemes)\s*=\s*(\[[\s\S]*?\]);',
                r'data-schemes=["\'](\[[\s\S]*?\])["\']',
                r'"schemes"\s*:\s*(\[[\s\S]*?\])\s*[,}]',
            ]

            import json
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    try:
                        data = json.loads(match.group(1))
                        if isinstance(data, list) and len(data) > 0:
                            self.log.info(
                                "bpf_embedded_data_found",
                                count=len(data),
                            )
                            return data
                    except json.JSONDecodeError:
                        continue

            # Look for API URLs in the JavaScript
            api_urls = re.findall(
                r'["\']((https?://[^"\']*?(?:api|data)[^"\']*?))["\']',
                html,
            )
            for url_match in api_urls:
                url = url_match[0] if isinstance(url_match, tuple) else url_match
                if "scheme" in url.lower() or "development" in url.lower():
                    records = await self._try_endpoint(url)
                    if records:
                        return records

            return []

        except Exception as exc:
            self.log.warning("bpf_map_page_fetch_failed", error=str(exc))
            return []

    async def fetch_schemes(self) -> list[dict[str, Any]]:
        """
        Fetch BTR scheme data from all known API endpoints.

        Tries multiple endpoints in order, returning the first successful
        result set.
        """
        # Try primary endpoints first
        primary_endpoints = [
            BPF_SCHEMES_ENDPOINT,
            BPF_MAP_DATA_ENDPOINT,
        ]

        for url in primary_endpoints:
            self.log.info("bpf_trying_endpoint", url=url)
            records = await self._try_endpoint(url)
            if records:
                self.log.info("bpf_endpoint_success", url=url, count=len(records))
                return records

        # Try alternative endpoints
        for url in ALT_API_ENDPOINTS:
            self.log.info("bpf_trying_alt_endpoint", url=url)
            records = await self._try_endpoint(url)
            if records:
                self.log.info("bpf_alt_endpoint_success", url=url, count=len(records))
                return records

        # Fallback: scrape embedded data from the map page
        self.log.info("bpf_trying_embedded_data")
        records = await self._fetch_map_page_data()
        if records:
            return records

        self.log.warning("bpf_all_endpoints_failed")
        return []

    # ------------------------------------------------------------------
    # Normalise records
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_units(value: Any) -> Optional[int]:
        """Parse a unit count from various field formats."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            n = int(value)
            return n if 1 <= n <= 50000 else None
        s = str(value).strip().replace(",", "")
        # Handle ranges like "200-250" — take the higher number
        range_match = re.match(r"(\d+)\s*[-–]\s*(\d+)", s)
        if range_match:
            try:
                return int(range_match.group(2))
            except ValueError:
                pass
        try:
            n = int(float(s))
            return n if 1 <= n <= 50000 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _normalise_status(raw: str | None) -> str:
        """Map BPF pipeline status to our canonical scheme status."""
        if not raw:
            return "planned"
        lower = raw.strip().lower()
        for key, value in BTR_STATUS_MAP.items():
            if key in lower:
                return value
        return "planned"

    def _normalise_scheme(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a raw BPF scheme record into a normalised dict suitable
        for ExistingScheme persistence.
        """
        # Handle GeoJSON features
        props = record
        geometry = None
        if "properties" in record and "geometry" in record:
            props = record["properties"]
            geometry = record.get("geometry")

        # Name
        name = (
            props.get("name") or
            props.get("scheme_name") or
            props.get("development_name") or
            props.get("title") or
            props.get("project_name") or
            "Unknown BTR Scheme"
        )

        # Address
        address = (
            props.get("address") or
            props.get("site_address") or
            props.get("location") or
            ""
        )

        # Postcode
        postcode = props.get("postcode", "")
        if not postcode and address:
            postcode = BaseScraper.extract_postcode(address) or ""

        # Location
        city = (
            props.get("city") or
            props.get("town") or
            props.get("local_authority") or
            props.get("borough") or
            props.get("region") or
            ""
        )

        # Coordinates
        lat = None
        lng = None
        if geometry and geometry.get("type") == "Point":
            coords = geometry.get("coordinates", [])
            if len(coords) >= 2:
                lng = float(coords[0])
                lat = float(coords[1])
        else:
            for lat_field in ("latitude", "lat", "y"):
                v = props.get(lat_field)
                if v is not None:
                    try:
                        lat = float(v)
                    except (ValueError, TypeError):
                        pass
            for lng_field in ("longitude", "lng", "lon", "x"):
                v = props.get(lng_field)
                if v is not None:
                    try:
                        lng = float(v)
                    except (ValueError, TypeError):
                        pass

        # Developer
        developer = (
            props.get("developer") or
            props.get("developer_name") or
            props.get("owner") or
            props.get("investor") or
            props.get("company") or
            ""
        )

        # Operator
        operator = (
            props.get("operator") or
            props.get("operator_name") or
            props.get("manager") or
            ""
        )

        # Unit count
        units = self._parse_units(
            props.get("units") or
            props.get("total_units") or
            props.get("num_units") or
            props.get("number_of_units") or
            props.get("unit_count") or
            props.get("homes")
        )

        # Status
        status_raw = (
            props.get("status") or
            props.get("pipeline_status") or
            props.get("stage") or
            props.get("phase") or
            ""
        )
        status = self._normalise_status(status_raw)

        # Tenure type (most BPF data is BTR by definition)
        tenure = (
            props.get("tenure") or
            props.get("tenure_type") or
            "BTR"
        )

        # Source reference — unique ID from BPF data
        source_ref = (
            props.get("id") or
            props.get("scheme_id") or
            props.get("_id") or
            props.get("ref") or
            f"bpf_{name[:50]}_{postcode or city}"
        )
        source_ref = f"bpf_{source_ref}"

        # Local authority / borough for council matching
        local_authority = (
            props.get("local_authority") or
            props.get("borough") or
            props.get("council") or
            props.get("lpa") or
            city
        )

        return {
            "name": name.strip(),
            "address": address.strip(),
            "postcode": postcode.strip().upper() if postcode else "",
            "city": city.strip(),
            "lat": lat,
            "lng": lng,
            "developer": developer.strip(),
            "operator": operator.strip(),
            "num_units": units,
            "status": status,
            "status_raw": status_raw,
            "scheme_type": "BTR",
            "tenure": tenure,
            "source": "bpf_btr_pipeline",
            "source_reference": str(source_ref).strip(),
            "local_authority": local_authority.strip(),
            "raw_data": record,
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_and_normalise(self) -> list[dict[str, Any]]:
        """
        Fetch all BTR schemes and normalise them for DB ingestion.

        Returns a list of normalised scheme dicts.
        """
        raw_records = await self.fetch_schemes()
        self.log.info("bpf_raw_records", count=len(raw_records))

        normalised: list[dict[str, Any]] = []
        for record in raw_records:
            try:
                scheme = self._normalise_scheme(record)
                # Must have at minimum a name
                if scheme.get("name") and scheme["name"] != "Unknown BTR Scheme":
                    normalised.append(scheme)
                elif scheme.get("postcode") or scheme.get("address"):
                    # Use address/postcode as name if no name given
                    scheme["name"] = (
                        f"BTR Scheme, {scheme.get('address') or scheme.get('postcode')}"
                    )
                    normalised.append(scheme)
            except Exception as exc:
                self.log.warning(
                    "bpf_normalise_failed",
                    error=str(exc),
                    record_id=record.get("id", "unknown"),
                )

        self.log.info(
            "bpf_normalise_complete",
            raw_count=len(raw_records),
            normalised_count=len(normalised),
        )
        return normalised


def save_bpf_btr_schemes(
    schemes: list[dict[str, Any]],
    db: "Session",  # noqa: F821
) -> dict[str, int]:
    """
    Persist BPF BTR pipeline schemes to existing_schemes.

    Upserts by source_reference.  Also resolves developer/operator names
    to Company records where possible.

    Parameters
    ----------
    schemes : list
        Normalised scheme dicts from BPFBTRScraper.fetch_and_normalise().
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict with keys: found, new, updated, errors.
    """
    from app.models.models import Company, Council, ExistingScheme

    found = len(schemes)
    new = 0
    updated = 0
    errors = 0

    # Build council lookup cache
    council_cache: dict[str, int] = {}
    councils = db.query(Council).all()
    for c in councils:
        council_cache[c.name.lower()] = c.id

    # Build company lookup cache (normalised name -> id)
    company_cache: dict[str, int] = {}
    companies = db.query(Company.id, Company.normalized_name).all()
    for cid, cname in companies:
        company_cache[cname.lower()] = cid

    def _normalise_company_name(name: str) -> str:
        """Simple company name normalisation for matching."""
        n = name.lower().strip()
        for suffix in (" ltd", " limited", " plc", " llp", " inc"):
            n = n.replace(suffix, "")
        n = re.sub(r"[^a-z0-9\s]", "", n)
        return re.sub(r"\s+", " ", n).strip()

    def _find_or_create_company(name: str) -> Optional[int]:
        """Find a company by normalised name, or create a new one."""
        if not name or len(name) < 3:
            return None

        norm = _normalise_company_name(name)
        if norm in company_cache:
            return company_cache[norm]

        # Check DB for partial match
        existing = (
            db.query(Company)
            .filter(Company.normalized_name.ilike(f"%{norm}%"))
            .first()
        )
        if existing:
            company_cache[norm] = existing.id
            return existing.id

        # Create new company
        new_company = Company(
            name=name.strip(),
            normalized_name=norm,
            company_type="Developer",
            is_active=True,
        )
        db.add(new_company)
        db.flush()
        company_cache[norm] = new_company.id
        return new_company.id

    def _resolve_council_id(local_authority: str) -> Optional[int]:
        """Resolve a local authority name to council_id."""
        if not local_authority:
            return None
        lower = local_authority.lower()
        if lower in council_cache:
            return council_cache[lower]
        # Fuzzy substring match
        for cname, cid in council_cache.items():
            if lower in cname or cname in lower:
                return cid
        return None

    for scheme_data in schemes:
        try:
            source_ref = scheme_data.get("source_reference", "")

            # Look up existing scheme by source reference
            existing = (
                db.query(ExistingScheme)
                .filter(
                    ExistingScheme.source == "bpf_btr_pipeline",
                    ExistingScheme.source_reference == source_ref,
                )
                .first()
            )

            # Resolve developer/operator companies
            developer_id = _find_or_create_company(scheme_data.get("developer", ""))
            operator_id = _find_or_create_company(scheme_data.get("operator", ""))

            # If operator specified, mark it as Operator type
            if operator_id and scheme_data.get("operator"):
                op_company = db.query(Company).get(operator_id)
                if op_company and op_company.company_type != "Operator":
                    op_company.company_type = "Operator"

            council_id = _resolve_council_id(
                scheme_data.get("local_authority", "")
            )

            if existing:
                changed = False
                # Update fields
                field_map = {
                    "name": scheme_data.get("name"),
                    "address": scheme_data.get("address"),
                    "postcode": scheme_data.get("postcode"),
                    "num_units": scheme_data.get("num_units"),
                    "status": scheme_data.get("status"),
                    "lat": scheme_data.get("lat"),
                    "lng": scheme_data.get("lng"),
                    "owner_company_id": developer_id,
                    "operator_company_id": operator_id,
                    "council_id": council_id,
                }
                for field, value in field_map.items():
                    if value is not None and value != getattr(existing, field, None):
                        setattr(existing, field, value)
                        changed = True

                if changed:
                    existing.last_verified_at = datetime.utcnow()
                    updated += 1
            else:
                scheme = ExistingScheme(
                    name=scheme_data["name"],
                    address=scheme_data.get("address"),
                    postcode=scheme_data.get("postcode"),
                    lat=scheme_data.get("lat"),
                    lng=scheme_data.get("lng"),
                    council_id=council_id,
                    owner_company_id=developer_id,
                    operator_company_id=operator_id,
                    scheme_type="BTR",
                    status=scheme_data.get("status", "planned"),
                    num_units=scheme_data.get("num_units"),
                    source="bpf_btr_pipeline",
                    source_reference=source_ref,
                    data_confidence_score=0.8,  # BPF data is authoritative for BTR
                )
                db.add(scheme)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_bpf_btr_scheme_failed",
                source_reference=scheme_data.get("source_reference"),
            )
            errors += 1
            db.rollback()

    logger.info(
        "save_bpf_btr_schemes_complete",
        found=found,
        new=new,
        updated=updated,
        errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}
