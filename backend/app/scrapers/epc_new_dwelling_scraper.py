"""
EPC New-Dwelling Discovery Scraper.

Uses the EPC Open Data Communities API to discover new-build residential
schemes by filtering for transaction-type = "new dwelling" and clustering
results by postcode + lodgement period.

Every new dwelling in England & Wales requires an EPC before sale or let,
making this the most comprehensive source for identifying completed and
near-completion private development schemes.

API: https://epc.opendatacommunities.org/api/v1/domestic/search
Auth: Basic auth (free registration required)
Docs: https://epc.opendatacommunities.org/docs/api

Strategy:
    1. Query each local authority for new-dwelling EPCs in a date range
    2. Cluster certificates by postcode + lodgement month
    3. Clusters of 10+ units = likely a multi-unit development scheme
    4. Save discovered schemes to existing_schemes
"""

from __future__ import annotations

import base64
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Optional

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

EPC_API_BASE = "https://epc.opendatacommunities.org/api/v1"

# Minimum units in a postcode cluster to consider it a scheme
MIN_CLUSTER_UNITS = 10

# EPC local-authority codes are ONS codes (e.g. E09000001)
# We'll query by local-authority parameter


class EPCNewDwellingScraper:
    """
    Discovers new-build residential schemes by querying the EPC register
    for new-dwelling transactions and clustering by postcode.

    Each cluster of 10+ new-dwelling EPCs at the same postcode within
    a lodgement window likely represents a multi-unit development.
    """

    def __init__(
        self,
        api_key: str | None = None,
        days_back: int = 365,
        min_cluster_size: int = MIN_CLUSTER_UNITS,
    ) -> None:
        self.log = logger.bind(scraper="EPCNewDwellingScraper")
        self.api_key = api_key or settings.EPC_API_KEY
        self.days_back = days_back
        self.min_cluster_size = min_cluster_size
        self.client: httpx.AsyncClient | None = None

    def _auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Accept": "application/json",
        }
        if self.api_key:
            if ":" in self.api_key:
                token = base64.b64encode(self.api_key.encode()).decode()
            else:
                token = base64.b64encode(f"{self.api_key}:".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        return headers

    async def __aenter__(self) -> "EPCNewDwellingScraper":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            headers={
                "User-Agent": "UK-Ops-BD-Platform/1.0",
                **self._auth_headers(),
            },
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    # ------------------------------------------------------------------
    # API query
    # ------------------------------------------------------------------

    async def _fetch_page(
        self,
        params: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int | None]:
        """
        Fetch a page from the EPC domestic search API.

        Returns (rows, total_count).  total_count may be None if the
        API doesn't provide it in the response headers.
        """
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        url = f"{EPC_API_BASE}/domestic/search"
        resp = await self.client.get(url, params=params)
        resp.raise_for_status()

        if not resp.text or resp.text.strip() == "":
            return [], None, None

        data = resp.json()
        rows = data.get("rows", [])

        # X-Total-Count header gives total matches
        total_str = resp.headers.get("X-Total-Count")
        total = int(total_str) if total_str else None

        # X-Next-Search-After header for cursor-based pagination
        next_cursor = resp.headers.get("X-Next-Search-After")

        return rows, total, next_cursor

    async def fetch_new_dwellings(
        self,
        local_authority: str | None = None,
        from_year: int | None = None,
        from_month: int | None = None,
        to_year: int | None = None,
        to_month: int | None = None,
        max_results: int = 500000,
    ) -> list[dict[str, Any]]:
        """
        Fetch new-dwelling EPCs, optionally filtered by local authority
        and date range.

        Uses the search-after cursor for efficient pagination beyond
        the 5,000-row API page limit.
        """
        start_date = date.today() - timedelta(days=self.days_back)
        if from_year is None:
            from_year = start_date.year
        if from_month is None:
            from_month = start_date.month

        now = date.today()
        if to_year is None:
            to_year = now.year
        if to_month is None:
            to_month = now.month

        params: dict[str, Any] = {
            "transaction-type": "new dwelling",
            "size": 5000,
            "from-year": from_year,
            "from-month": from_month,
            "to-year": to_year,
            "to-month": to_month,
        }
        if local_authority:
            params["local-authority"] = local_authority

        all_rows: list[dict[str, Any]] = []
        search_after: str | None = None

        while len(all_rows) < max_results:
            if search_after:
                params["search-after"] = search_after

            try:
                rows, total, next_cursor = await self._fetch_page(params)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    self.log.warning("epc_rate_limited", la=local_authority)
                    import asyncio
                    await asyncio.sleep(30)
                    continue
                if exc.response.status_code == 404:
                    break
                raise
            except Exception as exc:
                self.log.warning(
                    "epc_fetch_error",
                    error=str(exc),
                    la=local_authority,
                )
                break

            if not rows:
                break

            all_rows.extend(rows)

            self.log.debug(
                "epc_page_fetched",
                la=local_authority,
                page_size=len(rows),
                total_so_far=len(all_rows),
                total_available=total,
            )

            if len(rows) < 5000:
                break

            # Use cursor from X-Next-Search-After header, fall back to lmk-key
            search_after = next_cursor or rows[-1].get("lmk-key", "")
            if not search_after:
                break

        self.log.info(
            "epc_new_dwellings_fetched",
            la=local_authority,
            count=len(all_rows),
        )
        return all_rows

    # ------------------------------------------------------------------
    # Clustering
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_developer_from_address(address: str) -> str | None:
        """Try to extract developer/scheme name from EPC address patterns."""
        if not address:
            return None
        # Common patterns: "Flat 1, The Elms, 123 High Street"
        # We can't reliably extract developer from EPC addresses alone
        return None

    @staticmethod
    def _lodgement_month(date_str: str | None) -> str:
        """Extract YYYY-MM from a lodgement date."""
        if not date_str:
            return "unknown"
        return date_str[:7]  # "2024-03-15" -> "2024-03"

    def cluster_by_postcode(
        self,
        certificates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Cluster new-dwelling certificates by postcode.

        Returns scheme-like dicts for clusters with >= min_cluster_size units.
        Each cluster represents a probable multi-unit development.
        """
        # Group by postcode
        by_postcode: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for cert in certificates:
            pc = (cert.get("postcode") or "").strip().upper()
            if pc:
                by_postcode[pc].append(cert)

        schemes: list[dict[str, Any]] = []
        for postcode, certs in by_postcode.items():
            if len(certs) < self.min_cluster_size:
                continue

            # Aggregate info from the cluster
            addresses = [c.get("address", "") for c in certs]
            ratings = [c.get("current-energy-rating", "") for c in certs]
            lodgement_dates = [c.get("lodgement-date", "") for c in certs]
            property_types = [c.get("property-type", "") for c in certs]
            floor_areas = []
            for c in certs:
                fa = c.get("total-floor-area")
                if fa:
                    try:
                        floor_areas.append(float(fa))
                    except (ValueError, TypeError):
                        pass

            # Date range
            valid_dates = sorted([d for d in lodgement_dates if d])
            earliest = valid_dates[0] if valid_dates else None
            latest = valid_dates[-1] if valid_dates else None

            # Rating distribution
            rating_dist: dict[str, int] = {}
            for r in ratings:
                if r in ("A", "B", "C", "D", "E", "F", "G"):
                    rating_dist[r] = rating_dist.get(r, 0) + 1

            # Most common property type
            type_counts: dict[str, int] = {}
            for pt in property_types:
                if pt:
                    type_counts[pt] = type_counts.get(pt, 0) + 1
            dominant_type = max(type_counts, key=type_counts.get) if type_counts else ""

            # Average floor area
            avg_floor_area = round(sum(floor_areas) / len(floor_areas), 1) if floor_areas else None

            # Local authority from first cert
            la = certs[0].get("local-authority", "")
            la_label = certs[0].get("local-authority-label", "")

            # Try to extract a common scheme name from addresses
            scheme_name = self._infer_scheme_name(addresses, postcode)

            # Classify: mostly flats = likely BTR/PBSA, mostly houses = private dev
            flat_count = sum(1 for pt in property_types if pt and "flat" in pt.lower())
            house_count = sum(1 for pt in property_types if pt and ("house" in pt.lower() or "bungalow" in pt.lower()))

            if flat_count > house_count * 2:
                scheme_type = "BTR"  # Likely BTR or PBSA
            elif house_count > flat_count:
                scheme_type = "Residential"
            else:
                scheme_type = "Mixed"

            schemes.append({
                "postcode": postcode,
                "name": scheme_name,
                "num_units": len(certs),
                "local_authority": la,
                "local_authority_label": la_label,
                "scheme_type": scheme_type,
                "dominant_property_type": dominant_type,
                "avg_floor_area": avg_floor_area,
                "epc_ratings": rating_dist,
                "earliest_lodgement": earliest,
                "latest_lodgement": latest,
                "addresses": addresses[:20],  # Sample for reference
                "source": "epc_new_dwelling",
                "source_reference": f"epc_cluster_{postcode}_{earliest or 'unknown'}",
                "raw_certs_count": len(certs),
            })

        # Sort by unit count descending
        schemes.sort(key=lambda s: s["num_units"], reverse=True)

        self.log.info(
            "epc_clustering_complete",
            total_certs=len(certificates),
            clusters_found=len(schemes),
            total_units_in_clusters=sum(s["num_units"] for s in schemes),
        )
        return schemes

    @staticmethod
    def _infer_scheme_name(addresses: list[str], postcode: str) -> str:
        """
        Try to find a common building/scheme name from a list of addresses.

        E.g., if multiple addresses contain "The Elms" or "Maple House",
        that's likely the scheme name.
        """
        if not addresses:
            return f"New Development, {postcode}"

        # Look for common building names across addresses
        # Split each address and find repeated non-numeric tokens
        name_candidates: dict[str, int] = {}
        for addr in addresses:
            # Remove flat/unit numbers, commas, common suffixes
            parts = re.split(r"[,]", addr)
            for part in parts:
                cleaned = part.strip()
                # Skip purely numeric parts or flat/unit identifiers
                if re.match(r"^(flat|unit|apartment|room)\s+\d", cleaned, re.I):
                    continue
                if re.match(r"^\d+[a-z]?$", cleaned, re.I):
                    continue
                if cleaned and len(cleaned) > 3 and not cleaned.upper() == postcode:
                    name_candidates[cleaned] = name_candidates.get(cleaned, 0) + 1

        if name_candidates:
            # Find the most common non-trivial name component
            # Must appear in at least 30% of addresses
            threshold = max(len(addresses) * 0.3, 2)
            best_name = None
            best_count = 0
            for name, count in name_candidates.items():
                if count >= threshold and count > best_count:
                    # Prefer names that look like building names (not street names)
                    best_name = name
                    best_count = count

            if best_name:
                return best_name

        return f"New Development, {postcode}"

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    async def discover_schemes(
        self,
        local_authorities: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Run the full discovery pipeline:
        1. Fetch new-dwelling EPCs (nationwide or per-LA)
        2. Cluster by postcode
        3. Return scheme-like dicts for 10+ unit clusters

        Parameters
        ----------
        local_authorities : list[str] | None
            ONS codes to query. If None, queries nationwide (no LA filter).
        """
        import asyncio

        all_certs: list[dict[str, Any]] = []

        if local_authorities:
            for la in local_authorities:
                certs = await self.fetch_new_dwellings(local_authority=la)
                all_certs.extend(certs)
                # Be polite — 1s between LA queries
                await asyncio.sleep(1.0)
        else:
            # Nationwide query (no LA filter)
            all_certs = await self.fetch_new_dwellings()

        self.log.info("epc_total_new_dwellings", count=len(all_certs))

        schemes = self.cluster_by_postcode(all_certs)
        return schemes


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

def save_epc_discovered_schemes(
    schemes: list[dict[str, Any]],
    db: "Session",  # noqa: F821
) -> dict[str, int]:
    """
    Persist EPC-discovered schemes to existing_schemes.

    Upserts by (source, source_reference).
    Resolves local_authority_label to council_id.

    Parameters
    ----------
    schemes : list
        Scheme dicts from EPCNewDwellingScraper.discover_schemes().
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict with keys: found, new, updated, errors.
    """
    from app.models.models import Council, ExistingScheme

    found = len(schemes)
    new = 0
    updated = 0
    errors = 0

    # Build council lookup
    council_cache: dict[str, int] = {}
    councils = db.query(Council).all()
    for c in councils:
        council_cache[c.name.lower()] = c.id

    def _resolve_council(la_label: str) -> Optional[int]:
        if not la_label:
            return None
        lower = la_label.lower().strip()
        if lower in council_cache:
            return council_cache[lower]
        # Strip common suffixes
        for suffix in (
            " city council", " borough council", " district council",
            " council", " metropolitan borough council",
            " london borough council",
        ):
            stripped = lower.replace(suffix, "").strip()
            if stripped in council_cache:
                return council_cache[stripped]
        # Fuzzy substring
        for cname, cid in council_cache.items():
            if lower in cname or cname in lower:
                return cid
        return None

    for scheme_data in schemes:
        try:
            source_ref = scheme_data.get("source_reference", "")

            existing = (
                db.query(ExistingScheme)
                .filter(
                    ExistingScheme.source == "epc_new_dwelling",
                    ExistingScheme.source_reference == source_ref,
                )
                .first()
            )

            council_id = _resolve_council(
                scheme_data.get("local_authority_label", "")
            )

            if existing:
                changed = False
                updates = {
                    "num_units": scheme_data.get("num_units"),
                    "epc_ratings": scheme_data.get("epc_ratings"),
                    "council_id": council_id,
                }
                for field, value in updates.items():
                    if value is not None and value != getattr(existing, field, None):
                        setattr(existing, field, value)
                        changed = True
                if changed:
                    existing.last_verified_at = datetime.utcnow()
                    updated += 1
            else:
                scheme = ExistingScheme(
                    name=scheme_data.get("name", f"New Development, {scheme_data.get('postcode', '')}"),
                    address="; ".join(scheme_data.get("addresses", [])[:3]),
                    postcode=scheme_data.get("postcode"),
                    council_id=council_id,
                    scheme_type=scheme_data.get("scheme_type", "Residential"),
                    status="operational",  # EPC lodged = dwelling exists
                    num_units=scheme_data.get("num_units"),
                    epc_ratings=scheme_data.get("epc_ratings"),
                    source="epc_new_dwelling",
                    source_reference=source_ref,
                    data_confidence_score=0.75,
                )
                db.add(scheme)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_epc_scheme_failed",
                source_reference=scheme_data.get("source_reference"),
            )
            errors += 1
            db.rollback()

    logger.info(
        "save_epc_discovered_schemes_complete",
        found=found,
        new=new,
        updated=updated,
        errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}
