"""
Handlers to persist scraper output into the existing_schemes table.

Each handler takes parsed results from a scraper and upserts into the DB,
creating Company and SchemeContract records as needed.  Every field mutation
is recorded in SchemeChangeLog for full auditability.

Data flows
----------
* Find a Tender  -> ingest_tender_contracts()
* Contracts Finder -> ingest_contracts_finder()
* RSH judgements  -> ingest_rsh_judgements()   (unchanged)
* EPC enrichment  -> enrich_schemes_with_epc() (unchanged)
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Optional

import structlog
from fuzzywuzzy import fuzz
from pydantic import BaseModel, field_validator, model_validator
from sqlalchemy.orm import Session

from app.models.models import Company, ExistingScheme, SchemeChangeLog, SchemeContract
from app.scrapers.base import BaseScraper
from app.scrapers.date_extractor import extract_contract_dates

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Operator extraction from description text
# ---------------------------------------------------------------------------

# Patterns that commonly introduce operator/supplier names in tender descriptions
_OPERATOR_PATTERNS = [
    # "awarded to <company>"
    re.compile(r"(?:awarded|appointed|selected|contracted)\s+to\s+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing))", re.IGNORECASE),
    # "contract with <company>"
    re.compile(r"(?:contract|agreement|arrangement)\s+with\s+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing))", re.IGNORECASE),
    # "managed by <company>"
    re.compile(r"(?:managed|operated|delivered|provided|run)\s+by\s+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing))", re.IGNORECASE),
    # "the provider is <company>" / "the operator is <company>"
    re.compile(r"(?:provider|operator|supplier|contractor|manager)\s+(?:is|will be|shall be)\s+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing))", re.IGNORECASE),
    # "incumbent: <company>" or "current provider: <company>"
    re.compile(r"(?:incumbent|current\s+(?:provider|operator|contractor|supplier))[\s:]+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing))", re.IGNORECASE),
]


def _extract_operator_from_text(text: str) -> str:
    """Try to extract an operator/supplier company name from description text."""
    if not text:
        return ""
    for pattern in _OPERATOR_PATTERNS:
        match = pattern.search(text)
        if match:
            name = match.group(1).strip().rstrip(".,")
            if _is_valid_company_name(name) and len(name) >= 5:
                return name
    return ""


_ASSET_MANAGER_PATTERNS = [
    re.compile(r"asset\s+manag(?:er|ement)\s+(?:is|by|provided by|:)\s+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Housing|Capital|Partners|Advisors))", re.IGNORECASE),
    re.compile(r"(?:asset\s+manager|AM\s+provider|asset\s+management\s+(?:company|provider|firm))[\s:]+([A-Z][A-Za-z\s&'.,\-]+(?:Ltd|Limited|Plc|PLC|LLP|Group|Services|Management|Capital|Partners|Advisors))", re.IGNORECASE),
    re.compile(r"(?:managed|overseen)\s+(?:by|through)\s+([A-Z][A-Za-z\s&'.,\-]+(?:Capital|Partners|Advisors|Asset|Management|Investment))", re.IGNORECASE),
]


def _extract_asset_manager_from_text(text: str) -> str:
    """Try to extract an asset manager company name from description text."""
    if not text:
        return ""
    for pattern in _ASSET_MANAGER_PATTERNS:
        match = pattern.search(text)
        if match:
            name = match.group(1).strip().rstrip(".,")
            if _is_valid_company_name(name) and len(name) >= 5:
                return name
    return ""


def _json_safe(obj: Any) -> Any:
    """Recursively convert date/datetime objects to ISO strings for JSON storage."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj

# ---------------------------------------------------------------------------
# CPV code -> scheme type mapping
# ---------------------------------------------------------------------------

CPV_SCHEME_TYPE_MAP: dict[str, str] = {
    "70332000": "Social Housing",
    "70333000": "Managed Rental",
    "70330000": "Managed Scheme",
    "70300000": "Managed Scheme",
    "98341000": "Supported Housing",
    "79993000": "Facilities Management",
    "50700000": "Facilities Management",
    "50710000": "Facilities Management",
    "50711000": "Facilities Management",
    "50712000": "Facilities Management",
    "45211000": "Residential Development",
    "45211341": "Residential Development",
    "45211340": "Residential Development",
    "90910000": "Estate Services",
    "90911000": "Estate Services",
    "77310000": "Estate Services",
    "45453000": "Housing Refurbishment",
    "45300000": "Housing Maintenance",
    "45330000": "Housing Maintenance",
    "45421000": "Housing Maintenance",
    "55100000": "BTR",
    "55250000": "BTR",
    "55200000": "Co-living",
}

# Viability rating -> financial health score
VIABILITY_SCORE_MAP: dict[str, float] = {
    "V1": 95.0,
    "V2": 75.0,
    "V3": 45.0,
    "V4": 20.0,
}


# ---------------------------------------------------------------------------
# Pydantic validation model for incoming contract data
# ---------------------------------------------------------------------------

class ScrapedContractData(BaseModel):
    """Validated representation of a single scraped contract record.

    All ingest functions normalise their raw dicts into this model before
    further processing.  Records that fail validation are logged and skipped.
    """

    title: str
    notice_id: Optional[str] = None
    contracting_authority: str = ""
    supplier: str = ""
    contract_value: Optional[float] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    cpv_codes: list[str] = []
    description: str = ""
    source: str
    source_reference: Optional[str] = None

    @field_validator("contract_value")
    @classmethod
    def validate_contract_value(cls, v: Optional[float]) -> Optional[float]:
        """Contract value must be either None or a positive number below 10 billion."""
        if v is not None and not (0 < v < 10_000_000_000):
            raise ValueError(
                f"contract_value must be None or 0 < v < 10,000,000,000, got {v}"
            )
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> ScrapedContractData:
        """Ensure end_date is not before start_date when both are provided."""
        if (
            self.start_date is not None
            and self.end_date is not None
            and self.end_date < self.start_date
        ):
            raise ValueError(
                f"end_date ({self.end_date}) must not be before "
                f"start_date ({self.start_date})"
            )
        return self


# ---------------------------------------------------------------------------
# Company name normalisation
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Normalise a company name for matching."""
    text = name.lower().strip()
    for suffix in [
        " limited", " ltd", " plc", " llp", " inc",
        " council", " borough", " county", " city",
        " metropolitan", " district",
    ]:
        text = text.replace(suffix, "")
    return re.sub(r"\s+", " ", text).strip()


def _is_valid_company_name(name: str) -> bool:
    """Reject garbage company names from scraping artefacts."""
    s = name.strip()
    if not s or len(s) < 3:
        return False
    if len(s) > 200:
        return False  # Scraped HTML text
    # Reject planning application references (e.g. "2015/06678/PA")
    if re.match(r"^\d{4}/\d+", s):
        return False
    # Reject strings that look like notice IDs or URLs
    if re.match(r"^(http|www\.|ocds-|ocid)", s, re.IGNORECASE):
        return False
    # Reject if it contains "Notice identifier" or "Published" (scraped HTML)
    if re.search(r"Notice identifier|Published \d|view related|Watch this notice", s, re.IGNORECASE):
        return False
    # Reject if mostly numbers/punctuation
    alpha_ratio = sum(1 for c in s if c.isalpha()) / len(s) if s else 0
    if alpha_ratio < 0.4:
        return False
    return True


def _find_or_create_company(name: str, db: Session, company_type: str = "Operator") -> Company | None:
    """Find existing company by normalised name or create a new one."""
    if not name or not name.strip():
        return None

    # Validate company name quality
    if not _is_valid_company_name(name):
        logger.debug("invalid_company_name", name=name[:80])
        return None

    norm = _normalize_name(name)

    existing = (
        db.query(Company)
        .filter(Company.normalized_name == norm)
        .first()
    )
    if existing:
        return existing

    company = Company(
        name=name.strip(),
        normalized_name=norm,
        company_type=company_type,
        is_active=True,
    )
    db.add(company)
    db.flush()
    return company


# ---------------------------------------------------------------------------
# Housing-related filtering
# ---------------------------------------------------------------------------

HOUSING_KEYWORDS = [
    # Core housing terms
    "housing", "tenant", "residential", "dwelling", "sheltered",
    "supported living", "social housing", "affordable", "rented",
    "lettings", "property management", "estate management",
    "housing management", "housing maintenance", "registered provider",
    "housing association", "accommodation", "homelessness",
    "repairs and maintenance", "voids", "care home", "extra care",
    "retirement", "supported housing", "temporary accommodation",
    # Wider housing / property terms
    "block management", "communal", "leasehold", "freehold",
    "housing stock", "council housing", "almshouse", "hostel",
    "refuge", "domestic property", "housing revenue",
    "housing benefit", "tenant engagement", "resident engagement",
    "right to buy", "shared ownership", "housing register",
    "key worker", "build to rent", "purpose built student",
    "co-living", "senior living", "later living", "assisted living",
    "domiciliary", "warden", "concierge", "landlord services",
    "rent collection", "arrears", "tenancy management",
    "housing officer", "disrepair", "decarbonisation",
    "retrofit", "building safety", "fire safety",
    "housing contract", "managed accommodation", "managed housing",
    "property services", "grounds maintenance housing",
    "cleaning housing", "cleaning communal", "lift maintenance",
    "planned maintenance", "responsive maintenance",
    "asset management housing", "stock condition",
]

HOUSING_CPV_CODES = {
    "70330000", "70332000", "70333000", "79993000", "98341000",
    "50700000", "45211000", "45211341", "45211340",
    "55100000", "55250000", "55200000",
}

# CPV prefixes (first 3-5 digits) that indicate housing-related contracts
HOUSING_CPV_PREFIXES = [
    "703",    # Property management services (all subcategories)
    "7033",   # Property management services
    "98341",  # Accommodation services
    "45211",  # Construction of residential buildings
    "50700",  # Building maintenance
    "50710",  # Repair of electrical/mechanical building installations
    "50711",  # Repair of electrical building installations
    "50712",  # Repair of mechanical building installations
    "79993",  # Building and facilities management
    "45453",  # Overhaul and refurbishment work (common in housing)
    "45300",  # Building installation work
    "45330",  # Plumbing and sanitary work
    "45421",  # Joinery work
    "90910",  # Cleaning services (communal areas)
    "90911",  # Housing/building cleaning
    "77310",  # Grounds maintenance (estates)
    "50000",  # Repair and maintenance services
]


def _is_housing_related(contract: dict[str, Any]) -> bool:
    """Filter: only keep tenders related to housing/property management.

    Uses three checks (any match = pass):
    1. Exact CPV code match
    2. CPV prefix match (e.g. 703* catches all property management)
    3. Keyword match in title + description
    """
    cpv_codes = set(contract.get("cpv_codes", []))

    # Exact CPV match
    if cpv_codes & HOUSING_CPV_CODES:
        return True

    # Prefix CPV match
    for cpv in cpv_codes:
        for prefix in HOUSING_CPV_PREFIXES:
            if cpv.startswith(prefix):
                return True

    # Keyword match
    text = (
        contract.get("title", "") + " " + contract.get("description", "")
    ).lower()
    return any(kw in text for kw in HOUSING_KEYWORDS)


# ---------------------------------------------------------------------------
# Fuzzy deduplication
# ---------------------------------------------------------------------------

def _find_existing_scheme(
    contract_data: ScrapedContractData,
    db: Session,
) -> ExistingScheme | None:
    """Locate an existing scheme that matches the incoming contract data.

    Uses a three-tier cascade to balance precision and recall:

    1. **Exact source_reference match** -- if the incoming record carries a
       notice ID or contract reference that already exists in the database,
       that is an authoritative match.
    2. **Fuzzy name match** -- compare the contract title against scheme names
       using token-sort ratio (>= 85 threshold).  Only the most recent 500
       schemes are checked to keep the query bounded.
    3. **Composite match** -- postcode + normalised operator name.  This
       catches cases where the title has changed but the location and
       operator are stable identifiers.

    Returns the matched :class:`ExistingScheme` or ``None``.
    """
    # --- Tier 1: exact source_reference match ---
    if contract_data.source_reference:
        exact = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.source_reference == contract_data.source_reference)
            .first()
        )
        if exact:
            logger.debug(
                "dedup_exact_match",
                source_reference=contract_data.source_reference,
                scheme_id=exact.id,
            )
            return exact

    # --- Tier 2: fuzzy name match ---
    candidates = (
        db.query(ExistingScheme)
        .order_by(ExistingScheme.id.desc())
        .limit(2000)
        .all()
    )
    best_match: ExistingScheme | None = None
    best_score: int = 0

    for candidate in candidates:
        score = fuzz.token_sort_ratio(contract_data.title, candidate.name)
        if score >= 80 and score > best_score:
            best_score = score
            best_match = candidate

    if best_match is not None:
        logger.debug(
            "dedup_fuzzy_match",
            title=contract_data.title,
            matched_name=best_match.name,
            score=best_score,
            scheme_id=best_match.id,
        )
        return best_match

    # --- Tier 3: composite postcode + operator name match ---
    postcode = BaseScraper.extract_postcode(
        contract_data.contracting_authority
    ) or BaseScraper.extract_postcode(contract_data.description)
    supplier_norm = _normalize_name(contract_data.supplier) if contract_data.supplier else None

    if postcode and supplier_norm:
        postcode_matches = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.postcode == postcode)
            .all()
        )
        for candidate in postcode_matches:
            if candidate.operator_company_id is not None:
                operator = (
                    db.query(Company)
                    .filter(Company.id == candidate.operator_company_id)
                    .first()
                )
                if operator and operator.normalized_name == supplier_norm:
                    logger.debug(
                        "dedup_composite_match",
                        postcode=postcode,
                        operator=supplier_norm,
                        scheme_id=candidate.id,
                    )
                    return candidate

    return None


# ---------------------------------------------------------------------------
# Change detection / audit logging
# ---------------------------------------------------------------------------

def _log_scheme_change(
    scheme: ExistingScheme,
    field: str,
    old_val: Any,
    new_val: Any,
    source: str,
    db: Session,
) -> None:
    """Create a :class:`SchemeChangeLog` entry if a value actually changed.

    Both old and new values are coerced to strings (or ``None``) for
    consistent storage in the ``Text`` columns.
    """
    old_str = str(old_val) if old_val is not None else None
    new_str = str(new_val) if new_val is not None else None

    if old_str == new_str:
        return

    log_entry = SchemeChangeLog(
        scheme_id=scheme.id,
        field_name=field,
        old_value=old_str,
        new_value=new_str,
        source=source,
        changed_by="system",
    )
    db.add(log_entry)
    logger.debug(
        "scheme_field_changed",
        scheme_id=scheme.id,
        field=field,
        old=old_str,
        new=new_str,
        source=source,
    )


# ---------------------------------------------------------------------------
# Data confidence scoring
# ---------------------------------------------------------------------------

_CONFIDENCE_FIELDS: list[str] = [
    "operator_company_id",
    "owner_company_id",
    "contract_end_date",
    "postcode",
    "num_units",
    "scheme_type",
]

_FIELD_WEIGHT: float = 1.0 / len(_CONFIDENCE_FIELDS)


def _calculate_confidence(scheme: ExistingScheme) -> float:
    """Score between 0.0 and 1.0 reflecting how complete a scheme record is.

    Each of the following key fields contributes equally (~0.167):

    * ``operator_company_id``
    * ``owner_company_id``
    * ``contract_end_date``
    * ``postcode``
    * ``num_units``
    * ``scheme_type``

    A fully populated record scores 1.0; an empty one scores 0.0.
    """
    score = 0.0
    for field_name in _CONFIDENCE_FIELDS:
        value = getattr(scheme, field_name, None)
        if value is not None:
            score += _FIELD_WEIGHT
    return round(min(score, 1.0), 3)


# ---------------------------------------------------------------------------
# Scheme type classification helpers
# ---------------------------------------------------------------------------

_KEYWORD_SCHEME_TYPES: list[tuple[list[str], str]] = [
    (["social housing", "council housing", "housing association", "registered provider", "housing revenue"], "Social Housing"),
    (["affordable", "shared ownership", "right to buy"], "Affordable"),
    (["sheltered", "extra care", "retirement", "later living", "senior living", "assisted living", "warden"], "Senior"),
    (["supported housing", "supported living", "hostel", "refuge", "temporary accommodation", "homelessness"], "Supported Housing"),
    (["build to rent", "btr", "co-living", "co living"], "BTR"),
    (["student", "pbsa", "purpose built student"], "PBSA"),
    (["care home", "domiciliary", "nursing"], "Care"),
    (["facilities management", "fm services", "building management"], "Facilities Management"),
    (["repairs", "maintenance", "responsive repair", "planned maintenance", "retrofit", "decarbonisation"], "Housing Maintenance"),
    (["cleaning", "grounds maintenance", "estate services", "communal"], "Estate Services"),
    (["property management", "lettings", "estate management", "block management"], "Managed Scheme"),
    (["housing management", "tenancy management", "tenant", "rent collection", "arrears"], "Housing Management"),
]


def _determine_scheme_type(
    cpv_codes: list[str],
    description: str,
) -> str:
    """Derive a scheme type from CPV codes, falling back to keyword analysis."""
    # Try exact CPV match
    for cpv in cpv_codes:
        if cpv in CPV_SCHEME_TYPE_MAP:
            return CPV_SCHEME_TYPE_MAP[cpv]

    # Try BaseScraper classify
    keyword_type = BaseScraper.classify_scheme_type(description)
    if keyword_type not in ("Unknown", "Residential"):
        return keyword_type

    # Try keyword-based classification
    text = description.lower()
    for keywords, scheme_type in _KEYWORD_SCHEME_TYPES:
        if any(kw in text for kw in keywords):
            return scheme_type

    return "Managed Scheme"


# ---------------------------------------------------------------------------
# SchemeContract creation helper
# ---------------------------------------------------------------------------

def _create_scheme_contract(
    scheme: ExistingScheme,
    contract_data: ScrapedContractData,
    operator: Company | None,
    client: Company | None,
    raw_record: dict[str, Any],
    db: Session,
) -> SchemeContract:
    """Create a :class:`SchemeContract` linked to the given scheme.

    Existing contracts on the same scheme that share the same
    ``source_reference`` are skipped (idempotent).  When a new contract is
    added, any previously-current contracts for the scheme are marked
    ``is_current = False``.
    """
    # Check for duplicate contract by source_reference
    if contract_data.source_reference:
        existing_contract = (
            db.query(SchemeContract)
            .filter(
                SchemeContract.scheme_id == scheme.id,
                SchemeContract.source_reference == contract_data.source_reference,
            )
            .first()
        )
        if existing_contract:
            logger.debug(
                "contract_already_exists",
                scheme_id=scheme.id,
                source_reference=contract_data.source_reference,
            )
            return existing_contract

    # Mark previous contracts as no longer current
    (
        db.query(SchemeContract)
        .filter(
            SchemeContract.scheme_id == scheme.id,
            SchemeContract.is_current.is_(True),
        )
        .update({"is_current": False})
    )

    contract = SchemeContract(
        scheme_id=scheme.id,
        contract_reference=contract_data.source_reference,
        contract_type=_determine_scheme_type(
            contract_data.cpv_codes, contract_data.description
        ),
        operator_company_id=operator.id if operator else None,
        client_company_id=client.id if client else None,
        contract_start_date=contract_data.start_date,
        contract_end_date=contract_data.end_date,
        contract_value=contract_data.contract_value,
        currency="GBP",
        source=contract_data.source,
        source_reference=contract_data.source_reference,
        is_current=True,
        raw_data=_json_safe(raw_record),
    )
    db.add(contract)
    db.flush()

    logger.info(
        "contract_created",
        scheme_id=scheme.id,
        contract_id=contract.id,
        source=contract_data.source,
        source_reference=contract_data.source_reference,
    )
    return contract


# ---------------------------------------------------------------------------
# Scheme update helper (with change logging)
# ---------------------------------------------------------------------------

def _update_scheme_fields(
    scheme: ExistingScheme,
    *,
    operator: Company | None,
    owner: Company | None,
    asset_manager: Company | None = None,
    address: str | None = None,
    postcode: str | None,
    scheme_type: str,
    start_date: date | None,
    end_date: date | None,
    source: str,
    source_reference: str | None,
    db: Session,
) -> None:
    """Apply field updates to a scheme, logging every change."""
    if operator and not scheme.operator_company_id:
        _log_scheme_change(scheme, "operator_company_id", scheme.operator_company_id, operator.id, source, db)
        scheme.operator_company_id = operator.id

    if owner and not scheme.owner_company_id:
        _log_scheme_change(scheme, "owner_company_id", scheme.owner_company_id, owner.id, source, db)
        scheme.owner_company_id = owner.id

    if asset_manager and not scheme.asset_manager_company_id:
        _log_scheme_change(scheme, "asset_manager_company_id", scheme.asset_manager_company_id, asset_manager.id, source, db)
        scheme.asset_manager_company_id = asset_manager.id

    if address and (not scheme.address or scheme.address == scheme.name):
        _log_scheme_change(scheme, "address", scheme.address, address, source, db)
        scheme.address = address

    if postcode and not scheme.postcode:
        _log_scheme_change(scheme, "postcode", scheme.postcode, postcode, source, db)
        scheme.postcode = postcode

    if scheme_type and scheme_type != "Managed Scheme" and scheme.scheme_type in (None, "Managed Scheme"):
        _log_scheme_change(scheme, "scheme_type", scheme.scheme_type, scheme_type, source, db)
        scheme.scheme_type = scheme_type

    if start_date and not scheme.contract_start_date:
        _log_scheme_change(scheme, "contract_start_date", scheme.contract_start_date, start_date, source, db)
        scheme.contract_start_date = start_date

    if end_date and (not scheme.contract_end_date or end_date > scheme.contract_end_date):
        _log_scheme_change(scheme, "contract_end_date", scheme.contract_end_date, end_date, source, db)
        scheme.contract_end_date = end_date

    if source_reference and not scheme.source_reference:
        _log_scheme_change(scheme, "source_reference", scheme.source_reference, source_reference, source, db)
        scheme.source_reference = source_reference

    # Always refresh verification timestamp and confidence
    scheme.source = source
    scheme.last_verified_at = datetime.now(timezone.utc)
    scheme.data_confidence_score = _calculate_confidence(scheme)


# ---------------------------------------------------------------------------
# Find a Tender -> ExistingScheme + SchemeContract
# ---------------------------------------------------------------------------

def ingest_tender_contracts(
    results: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Persist Find a Tender contract notices as ExistingScheme records.

    Processing steps for each record:

    1. Validate through :class:`ScrapedContractData` -- skip invalid rows.
    2. Filter for housing-related contracts only.
    3. Fuzzy-deduplicate against existing schemes.
    4. Create or update the :class:`ExistingScheme` with full change logging.
    5. Create a :class:`SchemeContract` for every valid contract.
    6. Calculate and persist ``data_confidence_score``.

    Parameters
    ----------
    results:
        Raw dicts from the Find a Tender scraper.
    db:
        Active SQLAlchemy session.

    Returns
    -------
    dict:
        Counts of ``created``, ``updated``, ``skipped``, and ``contracts``
        records.
    """
    created = 0
    updated = 0
    skipped = 0
    contracts_created = 0

    for raw in results:
        # --- Extract dates from description if OCDS dates missing ---
        start_date = raw.get("start_date")
        end_date = raw.get("end_date")
        description = raw.get("description", "")

        if (not start_date or not end_date) and description:
            extracted = extract_contract_dates(description)
            if not start_date and extracted.get("start_date"):
                start_date = extracted["start_date"]
            if not end_date and extracted.get("end_date"):
                end_date = extracted["end_date"]

        # --- Extract operator from description if supplier missing ---
        supplier = raw.get("supplier", "")
        if not supplier and description:
            supplier = _extract_operator_from_text(description)
        if not supplier:
            supplier = _extract_operator_from_text(raw.get("title", ""))

        # --- Extract asset manager from description ---
        asset_manager_name = _extract_asset_manager_from_text(description)
        if not asset_manager_name:
            asset_manager_name = _extract_asset_manager_from_text(raw.get("title", ""))

        # --- Validate ---
        try:
            contract_data = ScrapedContractData(
                title=raw.get("title", "").strip(),
                notice_id=raw.get("notice_id"),
                contracting_authority=raw.get("contracting_authority", ""),
                supplier=supplier,
                contract_value=raw.get("contract_value"),
                start_date=start_date,
                end_date=end_date,
                cpv_codes=raw.get("cpv_codes", []),
                description=description,
                source="find_a_tender",
                source_reference=raw.get("notice_id"),
            )
        except Exception as exc:
            logger.warning(
                "tender_validation_failed",
                title=raw.get("title", "")[:120],
                error=str(exc),
            )
            skipped += 1
            continue

        # --- Housing filter ---
        if not _is_housing_related(raw):
            skipped += 1
            continue

        # --- Deduplicate ---
        existing = _find_existing_scheme(contract_data, db)

        # --- Derived fields ---
        scheme_type = _determine_scheme_type(
            contract_data.cpv_codes, contract_data.description
        )
        address = raw.get("address") or ""
        postcode = raw.get("postcode") or ""
        if not postcode:
            postcode = (
                BaseScraper.extract_postcode(address)
                or BaseScraper.extract_postcode(contract_data.contracting_authority)
                or BaseScraper.extract_postcode(contract_data.description)
            )
        operator = (
            _find_or_create_company(contract_data.supplier, db, "Operator")
            if contract_data.supplier
            else None
        )
        owner = (
            _find_or_create_company(contract_data.contracting_authority, db, "RP")
            if contract_data.contracting_authority
            else None
        )
        asset_manager = (
            _find_or_create_company(asset_manager_name, db, "Asset Manager")
            if asset_manager_name
            else None
        )

        if existing:
            _update_scheme_fields(
                existing,
                operator=operator,
                owner=owner,
                asset_manager=asset_manager,
                address=address,
                postcode=postcode,
                scheme_type=scheme_type,
                start_date=contract_data.start_date,
                end_date=contract_data.end_date,
                source="find_a_tender",
                source_reference=contract_data.source_reference,
                db=db,
            )
            scheme = existing
            updated += 1
        else:
            scheme = ExistingScheme(
                name=contract_data.title,
                address=address,
                postcode=postcode,
                scheme_type=scheme_type,
                operator_company_id=operator.id if operator else None,
                owner_company_id=owner.id if owner else None,
                asset_manager_company_id=asset_manager.id if asset_manager else None,
                contract_start_date=contract_data.start_date,
                contract_end_date=contract_data.end_date,
                source="find_a_tender",
                source_reference=contract_data.source_reference,
                last_verified_at=datetime.now(timezone.utc),
            )
            db.add(scheme)
            db.flush()  # Populate scheme.id for contract FK
            scheme.data_confidence_score = _calculate_confidence(scheme)
            created += 1

        # --- Create SchemeContract ---
        _create_scheme_contract(
            scheme, contract_data, operator, owner, raw, db
        )
        contracts_created += 1

    db.commit()
    logger.info(
        "tender_ingest_complete",
        created=created,
        updated=updated,
        skipped=skipped,
        contracts=contracts_created,
    )
    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "contracts": contracts_created,
    }


# ---------------------------------------------------------------------------
# Contracts Finder -> ExistingScheme + SchemeContract
# ---------------------------------------------------------------------------

def ingest_contracts_finder(
    results: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Persist Contracts Finder data as ExistingScheme + SchemeContract records.

    The Contracts Finder API uses slightly different field names compared to
    Find a Tender.  This function normalises:

    * ``buyer_name``      -> ``contracting_authority``
    * ``supplier_name``   -> ``supplier``
    * ``contract_start``  -> ``start_date``
    * ``contract_end``    -> ``end_date``
    * ``value``           -> ``contract_value``

    Otherwise the processing pipeline is identical to
    :func:`ingest_tender_contracts`.

    Parameters
    ----------
    results:
        Raw dicts from the Contracts Finder scraper.
    db:
        Active SQLAlchemy session.

    Returns
    -------
    dict:
        Counts of ``created``, ``updated``, ``skipped``, and ``contracts``
        records.
    """
    created = 0
    updated = 0
    skipped = 0
    contracts_created = 0

    for raw in results:
        # --- Normalise field names ---
        title = (
            raw.get("title", "")
            or raw.get("name", "")
        ).strip()
        notice_id = raw.get("notice_id") or raw.get("id") or raw.get("contract_id")
        contracting_authority = raw.get("buyer_name", "") or raw.get("contracting_authority", "")
        supplier = raw.get("supplier_name", "") or raw.get("supplier", "")
        contract_value = raw.get("value") or raw.get("contract_value")
        start_date = raw.get("contract_start") or raw.get("start_date") or raw.get("contract_start_date")
        end_date = raw.get("contract_end") or raw.get("end_date") or raw.get("contract_end_date")
        cpv_codes = raw.get("cpv_codes", [])
        description = raw.get("description", "")

        # --- Extract dates from description if OCDS dates missing ---
        if (not start_date or not end_date) and description:
            extracted = extract_contract_dates(description)
            if not start_date and extracted.get("start_date"):
                start_date = extracted["start_date"]
            if not end_date and extracted.get("end_date"):
                end_date = extracted["end_date"]

        # --- Extract operator from description if supplier missing ---
        if not supplier and description:
            supplier = _extract_operator_from_text(description)
        if not supplier:
            supplier = _extract_operator_from_text(title)

        # --- Extract asset manager from description ---
        asset_manager_name = _extract_asset_manager_from_text(description)
        if not asset_manager_name:
            asset_manager_name = _extract_asset_manager_from_text(title)

        # --- Validate ---
        try:
            contract_data = ScrapedContractData(
                title=title,
                notice_id=notice_id,
                contracting_authority=contracting_authority,
                supplier=supplier,
                contract_value=float(contract_value) if contract_value is not None else None,
                start_date=start_date,
                end_date=end_date,
                cpv_codes=cpv_codes,
                description=description,
                source="contracts_finder",
                source_reference=str(notice_id) if notice_id else None,
            )
        except Exception as exc:
            logger.warning(
                "contracts_finder_validation_failed",
                title=title[:120] if title else "",
                error=str(exc),
            )
            skipped += 1
            continue

        # --- Housing filter ---
        if not _is_housing_related(raw):
            skipped += 1
            continue

        # --- Deduplicate ---
        existing = _find_existing_scheme(contract_data, db)

        # --- Derived fields ---
        scheme_type = _determine_scheme_type(
            contract_data.cpv_codes, contract_data.description
        )
        address = raw.get("address") or ""
        postcode = raw.get("postcode") or ""
        if not postcode:
            postcode = (
                BaseScraper.extract_postcode(address)
                or BaseScraper.extract_postcode(contract_data.contracting_authority)
                or BaseScraper.extract_postcode(contract_data.description)
            )
        operator = (
            _find_or_create_company(contract_data.supplier, db, "Operator")
            if contract_data.supplier
            else None
        )
        owner = (
            _find_or_create_company(contract_data.contracting_authority, db, "RP")
            if contract_data.contracting_authority
            else None
        )
        asset_manager = (
            _find_or_create_company(asset_manager_name, db, "Asset Manager")
            if asset_manager_name
            else None
        )

        if existing:
            _update_scheme_fields(
                existing,
                operator=operator,
                owner=owner,
                asset_manager=asset_manager,
                address=address,
                postcode=postcode,
                scheme_type=scheme_type,
                start_date=contract_data.start_date,
                end_date=contract_data.end_date,
                source="contracts_finder",
                source_reference=contract_data.source_reference,
                db=db,
            )
            scheme = existing
            updated += 1
        else:
            scheme = ExistingScheme(
                name=contract_data.title,
                address=address,
                postcode=postcode,
                scheme_type=scheme_type,
                operator_company_id=operator.id if operator else None,
                owner_company_id=owner.id if owner else None,
                asset_manager_company_id=asset_manager.id if asset_manager else None,
                contract_start_date=contract_data.start_date,
                contract_end_date=contract_data.end_date,
                source="contracts_finder",
                source_reference=contract_data.source_reference,
                last_verified_at=datetime.now(timezone.utc),
            )
            db.add(scheme)
            db.flush()
            scheme.data_confidence_score = _calculate_confidence(scheme)
            created += 1

        # --- Create SchemeContract ---
        _create_scheme_contract(
            scheme, contract_data, operator, owner, raw, db
        )
        contracts_created += 1

    db.commit()
    logger.info(
        "contracts_finder_ingest_complete",
        created=created,
        updated=updated,
        skipped=skipped,
        contracts=contracts_created,
    )
    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "contracts": contracts_created,
    }


# ---------------------------------------------------------------------------
# RSH -> ExistingScheme (enrichment)  [UNCHANGED]
# ---------------------------------------------------------------------------

def ingest_rsh_judgements(
    results: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """
    Persist RSH regulatory judgements. Creates or enriches scheme records
    for each Registered Provider.
    """
    created = 0
    enriched = 0
    skipped = 0

    # Known GOV.UK navigation-link artefacts that are not real provider names.
    _RSH_GARBAGE = {
        "a to z list of landlords",
        "individual social housing providers and regulatory judgements",
        "regulatory judgements and enforcement notices",
        "regulatory judgements, enforcement notices and gradings",
        "registered providers of social housing",
    }

    for judgement in results:
        provider_name = judgement.get("provider_name", "").strip()
        if not provider_name:
            skipped += 1
            continue
        if provider_name.lower() in _RSH_GARBAGE or not _is_valid_company_name(provider_name):
            skipped += 1
            continue

        governance = judgement.get("governance_rating")
        viability = judgement.get("viability_rating")

        # Find or create company for this provider
        company = _find_or_create_company(provider_name, db, "RP")
        if not company:
            skipped += 1
            continue

        # Find existing scheme for this provider or create one
        existing = (
            db.query(ExistingScheme)
            .filter(ExistingScheme.operator_company_id == company.id)
            .first()
        )

        financial_score = VIABILITY_SCORE_MAP.get(viability, None) if viability else None

        if existing:
            if governance:
                existing.regulatory_rating = governance
            if financial_score is not None:
                existing.financial_health_score = financial_score
            enriched += 1
        else:
            scheme = ExistingScheme(
                name=f"{provider_name} - Social Housing",
                operator_company_id=company.id,
                scheme_type="Social Housing",
                regulatory_rating=governance,
                financial_health_score=financial_score,
            )
            db.add(scheme)
            created += 1

    db.commit()
    logger.info(
        "rsh_ingest_complete",
        created=created,
        enriched=enriched,
        skipped=skipped,
    )
    return {"created": created, "enriched": enriched, "skipped": skipped}


# ---------------------------------------------------------------------------
# EPC -> ExistingScheme (enrichment)  [UNCHANGED]
# ---------------------------------------------------------------------------

async def enrich_schemes_with_epc(db: Session) -> dict[str, int]:
    """
    For each scheme with a postcode, fetch EPC rating distribution
    and update the scheme's epc_ratings and performance_rating fields.
    """
    from app.scrapers.epc_scraper import EPCScraper

    schemes = (
        db.query(ExistingScheme)
        .filter(
            ExistingScheme.postcode.isnot(None),
            ExistingScheme.epc_ratings.is_(None),
        )
        .limit(100)
        .all()
    )

    if not schemes:
        return {"enriched": 0, "total_with_postcode": 0}

    enriched = 0
    scraper = EPCScraper()

    try:
        async with scraper:
            for scheme in schemes:
                try:
                    distribution = await scraper.get_rating_distribution(scheme.postcode)
                    if distribution:
                        scheme.epc_ratings = distribution

                        # Derive performance score from EPC ratings
                        # A=100, B=85, C=70, D=55, E=40, F=25, G=10
                        score_map = {"A": 100, "B": 85, "C": 70, "D": 55, "E": 40, "F": 25, "G": 10}
                        total_certs = sum(distribution.values())
                        if total_certs > 0:
                            weighted = sum(
                                score_map.get(rating, 50) * count
                                for rating, count in distribution.items()
                            )
                            scheme.performance_rating = round(weighted / total_certs, 1)
                        enriched += 1
                except Exception as exc:
                    logger.warning(
                        "epc_enrichment_failed",
                        scheme_id=scheme.id,
                        postcode=scheme.postcode,
                        error=str(exc),
                    )
    except Exception as exc:
        logger.error("epc_scraper_init_failed", error=str(exc))

    db.commit()
    logger.info("epc_enrichment_complete", enriched=enriched)
    return {"enriched": enriched, "total_with_postcode": len(schemes)}


# ---------------------------------------------------------------------------
# HMLR CCOD -> ExistingScheme.owner_company_id (enrichment)
# ---------------------------------------------------------------------------

def ingest_hmlr_ccod(db: Session, local_path: str | None = None) -> dict[str, int]:
    """Match HMLR CCOD corporate ownership records against existing schemes.

    For every :class:`ExistingScheme` that has a postcode, scans the HMLR
    CCOD dataset for matching title entries.  When a match is found:

    * Creates or finds a :class:`Company` for the registered proprietor.
    * Sets ``ExistingScheme.owner_company_id`` (only if currently unset).
    * Stores the title number in ``ExistingScheme.hmlr_title_number``.
    * Stores the tenure (Freehold/Leasehold) in ``ExistingScheme.hmlr_tenure``.
    * Triggers CH number lookup by storing the company registration number on
      the Company record, enabling the ``enrich_company`` task to pick it up.
    * Logs all field changes to :class:`SchemeChangeLog`.

    The function uses :meth:`HMLRCCODScraper.filter_by_postcodes` to stream
    only matching rows from the large dataset, keeping memory usage bounded.

    Parameters
    ----------
    db : Session
        Active SQLAlchemy session.
    local_path : str | None
        Optional override for the CCOD file path.  Falls back to
        ``settings.HMLR_CCOD_LOCAL_PATH`` / ``settings.HMLR_CCOD_DOWNLOAD_URL``.

    Returns
    -------
    dict
        Counts of ``matched``, ``owner_set``, ``already_had_owner``,
        ``companies_created``, and ``schemes_with_postcode``.
    """
    from app.scrapers.hmlr_ccod_scraper import HMLRCCODScraper

    # 1. Collect all scheme postcodes that still lack an owner.
    schemes_needing_owner: list[ExistingScheme] = (
        db.query(ExistingScheme)
        .filter(
            ExistingScheme.postcode.isnot(None),
            ExistingScheme.postcode != "",
        )
        .all()
    )

    if not schemes_needing_owner:
        logger.info("hmlr_ccod_no_schemes_with_postcode")
        return {
            "matched": 0,
            "owner_set": 0,
            "already_had_owner": 0,
            "companies_created": 0,
            "schemes_with_postcode": 0,
        }

    # Build a postcode -> list[scheme] map for O(1) lookup during streaming.
    postcode_map: dict[str, list[ExistingScheme]] = {}
    for scheme in schemes_needing_owner:
        pc = (scheme.postcode or "").strip().upper()
        postcode_map.setdefault(pc, []).append(scheme)

    target_postcodes: set[str] = set(postcode_map.keys())
    logger.info(
        "hmlr_ccod_ingest_start",
        schemes=len(schemes_needing_owner),
        unique_postcodes=len(target_postcodes),
    )

    scraper = HMLRCCODScraper(local_path=local_path)

    matched = 0
    owner_set = 0
    already_had_owner = 0
    companies_created = 0

    for row in scraper.filter_by_postcodes(target_postcodes):
        hit_schemes = postcode_map.get(row.postcode, [])
        if not hit_schemes:
            continue

        prop = row.primary_proprietor
        if not prop or not prop.get("name"):
            continue

        # Skip overseas-incorporated entities for owner_company_id — they are
        # often nominee companies rather than true economic owners.
        country = prop.get("country", "").strip().lower()
        if country and country not in ("", "england and wales", "united kingdom", "scotland", "northern ireland"):
            logger.debug(
                "hmlr_ccod_overseas_proprietor_skipped",
                title=row.title_number,
                country=country,
            )
            continue

        # Find or create Company for the proprietor.
        prop_name = prop["name"].strip()
        prop_reg = prop.get("registration_number", "").strip()

        company: Company | None = None

        # Prefer lookup by Companies House number — it's a stable identifier.
        if prop_reg:
            company = (
                db.query(Company)
                .filter(Company.companies_house_number == prop_reg)
                .first()
            )

        # Fall back to normalised name match.
        if not company:
            norm = _normalize_name(prop_name)
            company = (
                db.query(Company)
                .filter(Company.normalized_name == norm)
                .first()
            )

        if not company:
            company = Company(
                name=prop_name,
                normalized_name=_normalize_name(prop_name),
                companies_house_number=prop_reg if prop_reg else None,
                company_type="Investor",
                is_active=True,
            )
            db.add(company)
            db.flush()
            companies_created += 1
            logger.info(
                "hmlr_ccod_company_created",
                name=prop_name,
                ch_number=prop_reg,
                title=row.title_number,
            )
        elif prop_reg and not company.companies_house_number:
            # Back-fill CH number on an existing name-matched company.
            company.companies_house_number = prop_reg
            db.flush()

        for scheme in hit_schemes:
            matched += 1

            # Store title number and tenure regardless of whether owner was
            # already set — it's useful provenance data.
            if not scheme.hmlr_title_number:
                _log_scheme_change(
                    scheme, "hmlr_title_number", None, row.title_number, "hmlr_ccod", db
                )
                scheme.hmlr_title_number = row.title_number

            if not scheme.hmlr_tenure and row.tenure:
                _log_scheme_change(
                    scheme, "hmlr_tenure", None, row.tenure, "hmlr_ccod", db
                )
                scheme.hmlr_tenure = row.tenure

            if scheme.owner_company_id:
                already_had_owner += 1
                continue

            _log_scheme_change(
                scheme, "owner_company_id", None, company.id, "hmlr_ccod", db
            )
            scheme.owner_company_id = company.id
            scheme.last_verified_at = datetime.now(timezone.utc)
            scheme.data_confidence_score = _calculate_confidence(scheme)
            owner_set += 1
            logger.info(
                "hmlr_ccod_owner_set",
                scheme_id=scheme.id,
                scheme_name=scheme.name,
                owner_name=prop_name,
                title_number=row.title_number,
            )

    db.commit()

    result = {
        "matched": matched,
        "owner_set": owner_set,
        "already_had_owner": already_had_owner,
        "companies_created": companies_created,
        "schemes_with_postcode": len(schemes_needing_owner),
    }
    logger.info("hmlr_ccod_ingest_complete", **result)
    return result


# ---------------------------------------------------------------------------
# RSH Registered Providers list -> Company table (reference dataset)
# ---------------------------------------------------------------------------

def ingest_rsh_registered_providers(
    providers: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Upsert RSH Registered Provider records into the Company table.

    For each provider in the RSH list:
    * Finds an existing Company by normalised name or creates a new one.
    * Sets ``company_type = "RP"`` (Registered Provider).
    * Stores ``companies_house_number`` = RSH registration number in a
      dedicated field (rsh_registration_number lives in sic_codes JSON
      as a temporary store until a dedicated column is added — or we use
      the existing ``companies_house_number`` field as a proxy for the
      RSH number when no CH number is known, flagged by the ``"rsh:"``
      prefix so it isn't confused with a real CH number).
    * Updates registered address when currently blank.
    * Back-fills stock_units into an alias tag for SDR matching.

    Parameters
    ----------
    providers : list[dict]
        Output of :meth:`RSHRegisteredProvidersScraper.fetch_registered_providers`.
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict
        Counts of ``created``, ``updated``, ``skipped``.
    """
    created = 0
    updated = 0
    skipped = 0

    for prov in providers:
        name = (prov.get("name") or "").strip()
        if not name or not _is_valid_company_name(name):
            skipped += 1
            continue

        # Skip de-registered providers — they're no longer active RPs
        status = (prov.get("status") or "").lower()
        if "deregister" in status or "removed" in status:
            skipped += 1
            continue

        norm = _normalize_name(name)
        reg_num = prov.get("registration_number", "").strip()
        address = prov.get("address", "").strip()
        stock_units = prov.get("stock_units")

        # RSH reg numbers look like "L1234" or "4321" — prefix with "rsh:" to
        # distinguish from Companies House numbers in the same column.
        rsh_ref = f"rsh:{reg_num}" if reg_num else None

        # Try to find an existing company record.
        company: Company | None = None

        # 1. Exact match on existing RSH ref stored in sic_codes["rsh_registration"]
        if reg_num:
            company = (
                db.query(Company)
                .filter(Company.sic_codes["rsh_registration"].as_string() == reg_num)
                .first()
            )

        # 2. Normalised name match
        if not company:
            company = (
                db.query(Company)
                .filter(Company.normalized_name == norm)
                .first()
            )

        if company:
            changed = False
            # Ensure company_type is RP
            if company.company_type not in ("RP", "LRP"):
                company.company_type = "RP" if prov.get("provider_type") == "PRP" else "LRP"
                changed = True
            # Back-fill address
            if address and not company.registered_address:
                company.registered_address = address
                changed = True
            # Store RSH registration number in sic_codes JSON
            if reg_num:
                existing_sic = company.sic_codes or {}
                if "rsh_registration" not in existing_sic:
                    existing_sic["rsh_registration"] = reg_num
                    company.sic_codes = existing_sic
                    changed = True
            if changed:
                updated += 1
        else:
            sic_data: dict[str, Any] = {}
            if reg_num:
                sic_data["rsh_registration"] = reg_num
            if stock_units:
                sic_data["stock_units"] = stock_units
            ptype = prov.get("provider_type", "PRP")
            company = Company(
                name=name,
                normalized_name=norm,
                registered_address=address if address else None,
                company_type="RP" if ptype == "PRP" else "LRP",
                sic_codes=sic_data if sic_data else None,
                is_active=True,
            )
            db.add(company)
            created += 1

    db.commit()
    result = {"created": created, "updated": updated, "skipped": skipped}
    logger.info("rsh_rp_list_ingest_complete", **result)
    return result


# ---------------------------------------------------------------------------
# RSH SDR -> Company table (managed-vs-owned differentiation)
# ---------------------------------------------------------------------------

def ingest_rsh_sdr(
    sdr_rows: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Enrich Company records with SDR managed-vs-owned stock figures.

    The SDR is the key dataset for differentiating *operator* from *owner*:
    a provider that manages significantly more units than it owns is a
    management organisation operating on behalf of other owners.

    Stores the stock figures in ``Company.sic_codes`` JSON under the
    keys ``units_owned``, ``units_managed``, ``units_managed_for_others``.

    Parameters
    ----------
    sdr_rows : list[dict]
        Output of :meth:`RSHRegisteredProvidersScraper.fetch_sdr_stock`.
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict
        Counts of ``matched``, ``not_found``, ``operator_flagged``.
    """
    matched = 0
    not_found = 0
    operator_flagged = 0

    for row in sdr_rows:
        name = (row.get("name") or "").strip()
        if not name:
            not_found += 1
            continue

        norm = _normalize_name(name)
        reg_num = row.get("registration_number", "").strip()

        # Find company: RSH reg number first, then name
        company: Company | None = None
        if reg_num:
            company = (
                db.query(Company)
                .filter(Company.sic_codes["rsh_registration"].as_string() == reg_num)
                .first()
            )
        if not company:
            company = db.query(Company).filter(Company.normalized_name == norm).first()
        if not company:
            not_found += 1
            continue

        units_owned = row.get("units_owned")
        units_managed = row.get("units_managed")
        units_managed_for_others = row.get("units_managed_for_others")

        # Use a new dict to ensure SQLAlchemy detects the JSON mutation.
        existing_sic = dict(company.sic_codes or {})
        if units_owned is not None:
            existing_sic["units_owned"] = units_owned
        if units_managed is not None:
            existing_sic["units_managed"] = units_managed
        if units_managed_for_others is not None:
            existing_sic["units_managed_for_others"] = units_managed_for_others
        company.sic_codes = existing_sic

        # If a provider manages substantially more than it owns (>20% more),
        # tag it as an Operator rather than a pure RP — this is the key BD signal.
        if (
            units_managed is not None
            and units_owned is not None
            and units_owned > 0
            and units_managed > units_owned * 1.2
        ):
            if company.company_type in ("RP", "LRP", None):
                company.company_type = "Operator"
                operator_flagged += 1
                logger.info(
                    "rsh_sdr_operator_flagged",
                    company=name,
                    units_owned=units_owned,
                    units_managed=units_managed,
                )

        matched += 1

    db.commit()
    result = {
        "matched": matched,
        "not_found": not_found,
        "operator_flagged": operator_flagged,
    }
    logger.info("rsh_sdr_ingest_complete", **result)
    return result


# ---------------------------------------------------------------------------
# Brownfield Register -> PlanningApplication (reference dataset)
# ---------------------------------------------------------------------------

def ingest_brownfield_sites(
    results: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Persist brownfield development sites as PlanningApplication records.

    The Brownfield Land Register is a high-value nationwide dataset with
    38,000+ development sites including addresses, coordinates, dwelling
    counts, and planning permission details.

    Each site is upserted as a PlanningApplication so it appears in the
    Applications table and can be cross-referenced to schemes and pipeline
    opportunities.

    Parameters
    ----------
    results : list[dict]
        Parsed brownfield site dicts from :meth:`BrownfieldScraper.parse_application`.
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict
        Counts of ``created``, ``updated``, ``skipped``.
    """
    from app.models.models import Council, PlanningApplication

    created = 0
    updated = 0
    skipped = 0

    # Build council cache by organisation-entity for O(1) lookup.
    council_cache: dict[str, int] = {}

    for site in results:
        reference = site.get("reference", "")
        if not reference:
            skipped += 1
            continue

        address = site.get("address", "")
        postcode = site.get("postcode", "")
        num_units = site.get("num_units")

        # Skip sites with no useful location info
        if not address and not postcode:
            skipped += 1
            continue

        # Deduplicate by reference
        brownfield_ref = f"brownfield:{reference}"
        existing = (
            db.query(PlanningApplication)
            .filter(PlanningApplication.reference == brownfield_ref)
            .first()
        )

        if existing:
            # Update if we have new info
            changed = False
            if num_units and not existing.num_units:
                existing.num_units = num_units
                changed = True
            if postcode and not existing.postcode:
                existing.postcode = postcode
                changed = True
            if site.get("latitude") and not existing.latitude:
                existing.latitude = site["latitude"]
                existing.longitude = site.get("longitude")
                changed = True
            if changed:
                updated += 1
            continue

        # Resolve council from organisation_entity if possible
        org_entity = site.get("organisation_entity", "")
        council_id = council_cache.get(org_entity)
        if org_entity and council_id is None:
            # Try to find council - org entities are numeric IDs
            council = (
                db.query(Council)
                .filter(Council.name.ilike(f"%{org_entity[:20]}%"))
                .first()
            )
            if council:
                council_id = council.id
                council_cache[org_entity] = council_id

        app = PlanningApplication(
            reference=brownfield_ref,
            council_id=council_id,
            address=address,
            postcode=postcode,
            latitude=site.get("latitude"),
            longitude=site.get("longitude"),
            application_type=site.get("application_type", "Brownfield"),
            status=site.get("status", "Unknown"),
            scheme_type=site.get("scheme_type", "Residential"),
            num_units=num_units,
            submission_date=site.get("submission_date"),
            decision_date=site.get("decision_date"),
        )
        db.add(app)
        created += 1

    db.commit()
    result = {"created": created, "updated": updated, "skipped": skipped}
    logger.info("brownfield_ingest_complete", **result)
    return result


# ---------------------------------------------------------------------------
# LAHS -> Council records (enrichment)
# ---------------------------------------------------------------------------

def ingest_lahs_council_data(
    lahs_rows: list[dict[str, Any]],
    db: Session,
) -> dict[str, int]:
    """Enrich Council records with LAHS housing statistics.

    Stores housing stock counts, waiting list sizes, and build activity
    in the Council record's metadata.  This data helps BD scoring by
    identifying councils with the most housing activity and opportunity.

    Parameters
    ----------
    lahs_rows : list[dict]
        Output of :meth:`LAHSScraper.fetch_lahs_data`.
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict
        Counts of ``matched``, ``not_found``.
    """
    from app.models.models import Council

    matched = 0
    not_found = 0

    for row in lahs_rows:
        la_name = row.get("la_name", "").strip()
        if not la_name:
            not_found += 1
            continue

        # Try to match by council name (fuzzy)
        norm = la_name.lower().strip()
        council = (
            db.query(Council)
            .filter(Council.name.ilike(f"%{norm[:30]}%"))
            .first()
        )

        if not council:
            # Try shorter match
            short = norm.split()[0] if " " in norm else norm
            council = (
                db.query(Council)
                .filter(Council.name.ilike(f"%{short}%"))
                .first()
            )

        if not council:
            not_found += 1
            continue

        # Store LAHS data in a metadata-style approach using the region field
        # since there's no dedicated JSON column on Council.
        # We'll store a summary string in the region field if not already set.
        lahs_summary = {
            "total_stock": row.get("total_stock"),
            "rp_stock": row.get("rp_stock"),
            "waiting_list": row.get("waiting_list"),
            "new_builds": row.get("new_builds"),
            "affordable_supply": row.get("affordable_supply"),
            "rtb_sales": row.get("rtb_sales"),
        }

        # Store as JSON string in region field (or add dedicated field later)
        import json
        if council.region and not council.region.startswith("{"):
            # Preserve existing region info
            lahs_summary["region"] = council.region

        council.region = json.dumps(lahs_summary)
        matched += 1

    db.commit()
    result = {"matched": matched, "not_found": not_found}
    logger.info("lahs_ingest_complete", **result)
    return result
