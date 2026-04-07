"""
ARL (Association for Residential Letting / Rental Living) BTR Open & Operating
list scraper.

The ARL publishes BTR scheme data via an interactive map powered by REalyse
at https://thearl.org.uk/arl-btr-map-with-bidwells-realyse-homeviews/.

The map is an embedded iframe pointing to https://btr-display-dev.realyse.com/
which is a React SPA.  The scheme data is a GeoJSON FeatureCollection embedded
inside the Vite-built JS bundle at /assets/index-*.js.  The string data is
obfuscated with a rotating string-array lookup pattern (Up/Fp functions).

This scraper:
1. Downloads the SPA entry page to discover the current JS bundle URL
2. Downloads the JS bundle
3. Evaluates the obfuscated data using Node.js subprocess
4. Parses the resulting GeoJSON FeatureCollection (1200+ schemes)
5. Normalises each feature into an ExistingScheme-compatible dict

Data fields per scheme:
    Development Name, Developer, Funder, Operator, Authority, Address,
    Postcode, Status, Total Units, BTR Units, Studio/1/2/3/4 Bed counts,
    Parking, Affordable units, Planning Application ref, PA dates,
    Start date, Expected Completion, Tenure (MFBTR/SFBTR/Co-Living),
    Region, Purchase Price, lat/lng coordinates, HomeViews ratings, etc.

Data source:
    https://thearl.org.uk/arl-btr-map-with-bidwells-realyse-homeviews/
    https://btr-display-dev.realyse.com/ (REalyse map iframe)
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REALYSE_BASE_URL = "https://btr-display-dev.realyse.com"
REALYSE_INDEX_URL = f"{REALYSE_BASE_URL}/"

ARL_STATUS_MAP = {
    "completed": "operational",
    "under construction": "under_construction",
    "granted": "planned",
    "planning submitted": "planned",
    "pre-planning": "planned",
    "refused": "refused",
    "withdrawn": "withdrawn",
}

ARL_TENURE_MAP = {
    "MFBTR": "Multi-Family BTR",
    "SFBTR": "Single-Family BTR",
    "Co-Living": "Co-Living",
    "MFBTR/Co-Living": "Multi-Family BTR / Co-Living",
    "MFBTR/SFBTR": "Multi-Family BTR / Single-Family BTR",
    "SFBTR/Co-Living": "Single-Family BTR / Co-Living",
}


class ARLBTRScraper:
    """
    Scraper for ARL/REalyse Build-to-Rent Open & Operating list.

    Extracts a GeoJSON FeatureCollection of ~1200 BTR schemes from
    the obfuscated JS bundle of the REalyse interactive map.

    Requires Node.js to be available on PATH for deobfuscation.
    """

    def __init__(self) -> None:
        self.log = logger.bind(scraper="ARLBTRScraper")
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "ARLBTRScraper":
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=30.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    # ------------------------------------------------------------------
    # JS bundle discovery & download
    # ------------------------------------------------------------------

    async def _discover_js_bundle_url(self) -> str | None:
        """
        Fetch the REalyse SPA index page and extract the JS bundle URL.

        The index.html contains a script tag like:
            <script type="module" crossorigin src="/assets/index-C7BfFVn0.js">
        """
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        resp = await self.client.get(REALYSE_INDEX_URL)
        resp.raise_for_status()

        # Find the main JS bundle
        match = re.search(
            r'<script[^>]+src="(/assets/index-[^"]+\.js)"', resp.text
        )
        if not match:
            self.log.error("arl_js_bundle_not_found_in_html")
            return None

        bundle_path = match.group(1)
        bundle_url = f"{REALYSE_BASE_URL}{bundle_path}"
        self.log.info("arl_js_bundle_discovered", url=bundle_url)
        return bundle_url

    async def _download_js_bundle(self, url: str) -> str:
        """Download the JS bundle content."""
        if not self.client:
            raise RuntimeError("Client not initialised — use async with")

        self.log.info("arl_downloading_js_bundle", url=url)
        resp = await self.client.get(url)
        resp.raise_for_status()
        self.log.info(
            "arl_js_bundle_downloaded", size_bytes=len(resp.content)
        )
        return resp.text

    # ------------------------------------------------------------------
    # GeoJSON extraction via Node.js
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_geojson_via_node(js_content: str) -> dict[str, Any]:
        """
        Extract the GeoJSON FeatureCollection from the obfuscated JS bundle
        by evaluating the relevant code sections with Node.js.

        The bundle contains:
        - function Up() { const n = [...]; return ... } — string array
        - An IIFE that shuffles the array to match a checksum
        - function Fp(n,t) — lookup function with index offset
        - const e = Fp — alias
        - const Bse = { type: e(...), features: [...] } — the GeoJSON data

        We extract these code sections and evaluate them in Node.js.
        """
        # 1. Find the Up() function with the string array
        func_match = re.search(r'function Up\(\)\{const n=\[', js_content)
        if not func_match:
            raise ValueError(
                "Could not find Up() string array function in JS bundle"
            )
        func_start = func_match.start()

        # 2. Find the Bse GeoJSON constant
        bse_match = re.search(r'const Bse=\{', js_content)
        if not bse_match:
            raise ValueError(
                "Could not find Bse GeoJSON constant in JS bundle"
            )
        bse_start = bse_match.start()

        # 3. Extract the setup code (Up function, IIFE, Fp function, alias)
        setup_code = js_content[func_start:bse_start]

        # 4. Find the end of the Bse object (balanced braces/brackets)
        bse_obj_start = bse_start + len("const Bse=")
        brace_count = 0
        in_str = False
        str_char = None
        i = bse_obj_start

        while i < len(js_content):
            c = js_content[i]
            if c == "\\" and in_str:
                i += 2
                continue
            if not in_str:
                if c in ('"', "'", "`"):
                    in_str = True
                    str_char = c
                elif c in ("{", "["):
                    brace_count += 1
                elif c in ("}", "]"):
                    brace_count -= 1
                    if brace_count == 0:
                        break
            elif c == str_char:
                in_str = False
            i += 1

        bse_end = i + 1
        bse_code = js_content[bse_start:bse_end]

        # 5. Build the Node.js evaluation script
        node_script = (
            setup_code
            + "\n"
            + bse_code
            + "\n"
            + "process.stdout.write(JSON.stringify(Bse));\n"
        )

        # 6. Write to temp file and execute with Node.js
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".js",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp.write(node_script)
            tmp_path = tmp.name

        try:
            result = subprocess.run(
                ["node", tmp_path],
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8",
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Node.js evaluation failed: {result.stderr[:500]}"
                )

            geojson = json.loads(result.stdout)
            return geojson

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Fetch raw GeoJSON
    # ------------------------------------------------------------------

    async def fetch_geojson(self) -> dict[str, Any]:
        """
        Fetch and decode the full GeoJSON FeatureCollection.

        Returns the raw GeoJSON dict with all ~1200 features.
        """
        bundle_url = await self._discover_js_bundle_url()
        if not bundle_url:
            raise RuntimeError("Could not discover JS bundle URL")

        js_content = await self._download_js_bundle(bundle_url)
        geojson = self._extract_geojson_via_node(js_content)

        n_features = len(geojson.get("features", []))
        self.log.info("arl_geojson_extracted", num_features=n_features)

        if n_features == 0:
            self.log.warning("arl_no_features_found")

        return geojson

    # ------------------------------------------------------------------
    # Normalise records
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        """Parse an integer from various formats."""
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value).strip().replace(",", "")
        try:
            n = int(float(s))
            return n if n >= 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _normalise_status(raw: str | None) -> str:
        """Map ARL status to canonical scheme status."""
        if not raw:
            return "planned"
        lower = raw.strip().lower()
        for key, value in ARL_STATUS_MAP.items():
            if key in lower:
                return value
        return "planned"

    @staticmethod
    def _parse_date(date_str: str | None) -> Optional[str]:
        """Parse a date string in DD/MM/YYYY format to ISO format."""
        if not date_str or not date_str.strip():
            return None
        s = date_str.strip()
        # Try DD/MM/YYYY
        for fmt in ("%d/%m/%Y", "%m/%Y", "%Y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _normalise_feature(self, feature: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a raw GeoJSON feature into a normalised dict suitable
        for ExistingScheme persistence.
        """
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        # Coordinates
        lat, lng = None, None
        if geometry and geometry.get("type") == "Point":
            coords = geometry.get("coordinates", [])
            if len(coords) >= 2:
                lng = float(coords[0])
                lat = float(coords[1])

        name = props.get("Development Name", "").strip()
        address = props.get("Address", "").strip()
        raw_postcode = props.get("Postcode", "").strip().upper()
        # Validate postcode format (UK postcodes: 5-8 chars, letters+digits+space)
        postcode = raw_postcode if re.match(
            r'^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$', raw_postcode
        ) else ""
        authority = props.get("Authority", "").strip()
        status_raw = props.get("Status", "").strip()
        status = self._normalise_status(status_raw)

        developer = props.get("Developer", "").strip()
        funder = props.get("Funder", "").strip()
        operator = props.get("Operator", "").strip()

        total_units = self._parse_int(props.get("Total Units"))
        btr_units = self._parse_int(props.get("BTR Units"))
        affordable = self._parse_int(props.get("Affordable"))

        # Unit mix
        studio = self._parse_int(props.get("Studio"))
        bed_1 = self._parse_int(props.get("1 Bed"))
        bed_2 = self._parse_int(props.get("2 Bed"))
        bed_3 = self._parse_int(props.get("3 Bed"))
        bed_4 = self._parse_int(props.get("4 Bed"))
        penthouse = self._parse_int(props.get("Penthouse"))
        parking = self._parse_int(props.get("Parking"))
        cycle_spaces = self._parse_int(props.get("Cycle Spaces"))

        # Dates
        pa_ref = props.get("Planning Application (PA)", "").strip()
        pa_submitted = self._parse_date(props.get("PA Submitted"))
        pa_validated = self._parse_date(props.get("PA Validated"))
        pa_approved = self._parse_date(props.get("PA Approved"))
        start_date = props.get("Start date", "").strip()
        expected_completion = props.get("Expected Completion", "").strip()

        # Tenure & region
        tenure = props.get("Tenure", "").strip()
        region = props.get("Region", "").strip()
        top_10_city = props.get("Top 10 City", "").strip()
        london_borough = props.get("London Borough Location", "").strip()

        # Financial
        purchase_price = props.get("Purchase Price", "").strip()

        # HomeViews ratings
        homeviews_name = props.get("HomeViews Development Name", "").strip()
        homeviews_url = props.get("HomeViews Development URL", "").strip()
        star_rating = props.get("Star Rating", "").strip()

        # Source reference — use postcode + name combo for deduplication
        safe_name = re.sub(r'[^a-zA-Z0-9\s]', '', name)[:50].strip()
        source_ref = f"arl_{postcode}_{safe_name}" if postcode else f"arl_{safe_name}"

        return {
            "name": name,
            "address": address,
            "postcode": postcode,
            "city": "",  # Not directly available; can be derived from region/authority
            "lat": lat,
            "lng": lng,
            "developer": developer,
            "funder": funder,
            "operator": operator,
            "num_units": total_units,
            "btr_units": btr_units,
            "affordable_units": affordable,
            "unit_mix": {
                "studio": studio,
                "1_bed": bed_1,
                "2_bed": bed_2,
                "3_bed": bed_3,
                "4_bed": bed_4,
                "penthouse": penthouse,
            },
            "parking_spaces": parking,
            "cycle_spaces": cycle_spaces,
            "status": status,
            "status_raw": status_raw,
            "scheme_type": "BTR",
            "tenure": tenure,
            "tenure_label": ARL_TENURE_MAP.get(tenure, tenure),
            "region": region,
            "top_10_city": top_10_city.upper() == "YES",
            "london_borough_location": london_borough,
            "planning_ref": pa_ref,
            "pa_submitted": pa_submitted,
            "pa_validated": pa_validated,
            "pa_approved": pa_approved,
            "start_date": start_date,
            "expected_completion": expected_completion,
            "purchase_price": purchase_price,
            "homeviews_name": homeviews_name,
            "homeviews_url": homeviews_url,
            "star_rating": star_rating,
            "source": "arl_btr_open_operating",
            "source_reference": source_ref,
            "local_authority": authority,
            "raw_data": feature,
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_and_normalise(self) -> list[dict[str, Any]]:
        """
        Fetch all BTR schemes and normalise them for DB ingestion.

        Returns a list of normalised scheme dicts.
        """
        geojson = await self.fetch_geojson()
        features = geojson.get("features", [])
        self.log.info("arl_raw_features", count=len(features))

        normalised: list[dict[str, Any]] = []
        for feature in features:
            try:
                scheme = self._normalise_feature(feature)
                if scheme.get("name"):
                    normalised.append(scheme)
                elif scheme.get("postcode") or scheme.get("address"):
                    scheme["name"] = (
                        f"BTR Scheme, {scheme.get('address') or scheme.get('postcode')}"
                    )
                    normalised.append(scheme)
            except Exception as exc:
                self.log.warning(
                    "arl_normalise_failed",
                    error=str(exc),
                    development=feature.get("properties", {}).get(
                        "Development Name", "unknown"
                    ),
                )

        self.log.info(
            "arl_normalise_complete",
            raw_count=len(features),
            normalised_count=len(normalised),
        )
        return normalised


def save_arl_btr_schemes(
    schemes: list[dict[str, Any]],
    db: "Session",  # noqa: F821
) -> dict[str, int]:
    """
    Persist ARL BTR Open & Operating schemes to existing_schemes.

    Upserts by source_reference.  Also resolves developer/operator names
    to Company records where possible.

    Parameters
    ----------
    schemes : list
        Normalised scheme dicts from ARLBTRScraper.fetch_and_normalise().
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
        n = name.lower().strip()
        for suffix in (" ltd", " limited", " plc", " llp", " inc"):
            n = n.replace(suffix, "")
        n = re.sub(r"[^a-z0-9\s]", "", n)
        return re.sub(r"\s+", " ", n).strip()

    def _find_or_create_company(
        name: str, company_type: str = "Developer"
    ) -> Optional[int]:
        if not name or len(name) < 3:
            return None

        norm = _normalise_company_name(name)
        if norm in company_cache:
            return company_cache[norm]

        existing = (
            db.query(Company)
            .filter(Company.normalized_name.ilike(f"%{norm}%"))
            .first()
        )
        if existing:
            company_cache[norm] = existing.id
            return existing.id

        new_company = Company(
            name=name.strip(),
            normalized_name=norm,
            company_type=company_type,
            is_active=True,
        )
        db.add(new_company)
        db.flush()
        company_cache[norm] = new_company.id
        return new_company.id

    def _resolve_council_id(local_authority: str) -> Optional[int]:
        if not local_authority:
            return None
        lower = local_authority.lower()
        if lower in council_cache:
            return council_cache[lower]
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
                    ExistingScheme.source == "arl_btr_open_operating",
                    ExistingScheme.source_reference == source_ref,
                )
                .first()
            )

            developer_id = _find_or_create_company(
                scheme_data.get("developer", ""), "Developer"
            )
            operator_id = _find_or_create_company(
                scheme_data.get("operator", ""), "Operator"
            )

            if operator_id and scheme_data.get("operator"):
                op_company = db.query(Company).get(operator_id)
                if op_company and op_company.company_type != "Operator":
                    op_company.company_type = "Operator"

            council_id = _resolve_council_id(
                scheme_data.get("local_authority", "")
            )

            if existing:
                changed = False
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
                    if value is not None and value != getattr(
                        existing, field, None
                    ):
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
                    source="arl_btr_open_operating",
                    source_reference=source_ref,
                    data_confidence_score=0.85,
                )
                db.add(scheme)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_arl_btr_scheme_failed",
                source_reference=scheme_data.get("source_reference"),
            )
            errors += 1
            db.rollback()

    logger.info(
        "save_arl_btr_schemes_complete",
        found=found,
        new=new,
        updated=updated,
        errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}
