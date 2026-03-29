"""Fuzzy company name matching and deduplication.

Normalises company names, performs fuzzy matching against the existing
database, and manages alias records for known alternate names.

Examples
--------
The normaliser and matcher are designed to handle real-world UK property
company variations::

    >>> normalize_company_name("Greystar Development Ltd")
    'greystar'
    >>> normalize_company_name("Greystar Real Estate Partners LLC")
    'greystar'
    >>> normalize_company_name("Greystar")
    'greystar'

All three would match each other with a high similarity score, so the
system would recognise them as the same entity and add the variants as
aliases.

Typical usage::

    from app.matching.company_matcher import CompanyMatcher

    matcher = CompanyMatcher(db_session=session)
    company = await matcher.match_or_create("Greystar Development Ltd")
"""

from __future__ import annotations

import re
from typing import Any, Optional

import structlog
from fuzzywuzzy import fuzz  # type: ignore[import-untyped]
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from app.models.models import Company, CompanyAlias, PlanningApplication, ExistingScheme, PipelineOpportunity, Contact

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

# Suffixes to strip (order matters: longer first to avoid partial matches).
_STRIP_SUFFIXES: list[str] = [
    "real estate partners",
    "real estate",
    "developments",
    "development",
    "properties",
    "holdings",
    "partners",
    "limited liability partnership",
    "limited",
    "group",
    "ltd",
    "llp",
    "plc",
    "inc",
    "llc",
    "uk",
    "co",
]

_STRIP_SUFFIXES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(s) for s in _STRIP_SUFFIXES) + r")\b",
    re.IGNORECASE,
)

_PUNCTUATION_RE = re.compile(r"[^\w\s]", re.UNICODE)
_MULTI_SPACE_RE = re.compile(r"\s+")


def normalize_company_name(name: str) -> str:
    """Normalise a company name for matching.

    Steps:

    1. Lowercase.
    2. Strip common legal suffixes (Ltd, Limited, PLC, LLP, Inc, etc.).
    3. Strip common descriptive suffixes (Group, Holdings, UK, Development,
       Developments, Properties, Real Estate, Partners).
    4. Remove punctuation.
    5. Collapse whitespace and strip.

    Examples
    --------
    >>> normalize_company_name("Greystar Development Ltd")
    'greystar'
    >>> normalize_company_name("Greystar Real Estate Partners LLC")
    'greystar'
    >>> normalize_company_name("Legal & General Investment Management")
    'legal general investment management'
    >>> normalize_company_name("L&Q Housing")
    'lq housing'
    """
    result = name.lower().strip()
    # Remove suffixes iteratively (some names have multiple).
    for _ in range(3):
        previous = result
        result = _STRIP_SUFFIXES_RE.sub("", result)
        if result == previous:
            break
    result = _PUNCTUATION_RE.sub(" ", result)
    result = _MULTI_SPACE_RE.sub(" ", result).strip()
    return result


# ---------------------------------------------------------------------------
# Company matcher
# ---------------------------------------------------------------------------

class CompanyMatcher:
    """Fuzzy company name matching and deduplication engine.

    Parameters
    ----------
    db_session : Session
        SQLAlchemy database session.
    ch_enricher : optional
        An instance of :class:`CompaniesHouseEnricher` for optional
        verification of unmatched names.
    """

    def __init__(
        self,
        db_session: Session,
        ch_enricher: Any | None = None,
    ) -> None:
        self._db = db_session
        self._ch = ch_enricher
        self._alias_index: dict[str, int] | None = None

    # ------------------------------------------------------------------
    # Alias index
    # ------------------------------------------------------------------

    def build_alias_index(self) -> dict[str, int]:
        """Build an in-memory index of all company names and aliases.

        Returns a dict mapping normalised names to ``company.id``.
        """
        index: dict[str, int] = {}

        companies = self._db.query(Company.id, Company.name, Company.normalized_name).all()
        for cid, name, norm_name in companies:
            index[norm_name] = cid
            index[normalize_company_name(name)] = cid

        aliases = self._db.query(CompanyAlias.company_id, CompanyAlias.alias_name).all()
        for cid, alias_name in aliases:
            index[normalize_company_name(alias_name)] = cid

        self._alias_index = index
        logger.info("alias_index_built", entries=len(index))
        return index

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------

    def find_matches(
        self,
        name: str,
        threshold: float = 0.85,
    ) -> list[tuple[Company, float]]:
        """Find existing companies whose names fuzzy-match *name*.

        Uses ``fuzzywuzzy.fuzz.token_sort_ratio`` for comparison.

        Parameters
        ----------
        name : str
            Raw company name to match.
        threshold : float
            Minimum similarity (0-1) to include in results.

        Returns
        -------
        list[tuple[Company, float]]
            Pairs of ``(company, score)`` sorted by score descending.
        """
        norm = normalize_company_name(name)
        if not norm:
            return []

        companies = self._db.query(Company).filter(Company.is_active.is_(True)).all()
        matches: list[tuple[Company, float]] = []

        for company in companies:
            score = fuzz.token_sort_ratio(norm, company.normalized_name) / 100.0

            # Also check aliases.
            alias_names = [normalize_company_name(a.alias_name) for a in company.aliases]
            for alias_norm in alias_names:
                alias_score = fuzz.token_sort_ratio(norm, alias_norm) / 100.0
                score = max(score, alias_score)

            if score >= threshold:
                matches.append((company, score))

        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    async def match_or_create(
        self,
        raw_name: str,
        *,
        threshold: float = 0.85,
        source: str = "scraper",
    ) -> Company:
        """Main entry point: match a raw name to an existing company or create
        a new one.

        Algorithm:

        1. Normalise the name.
        2. Check for exact match on ``normalized_name``.
        3. Check the ``CompanyAlias`` table for an exact normalised match.
        4. Fuzzy-match against all companies (using *threshold*).
        5. If a match is found, add *raw_name* as a new alias and return it.
        6. If no match and a Companies House enricher is available, verify
           via the API.
        7. Create a new :class:`Company`.

        Parameters
        ----------
        raw_name : str
            The company name as found in the wild.
        threshold : float
            Minimum similarity for fuzzy matching.
        source : str
            Label for the alias source.

        Returns
        -------
        Company
            The matched or newly created company.
        """
        log = logger.bind(raw_name=raw_name)
        norm = normalize_company_name(raw_name)
        if not norm:
            raise ValueError(f"Cannot normalise empty company name: {raw_name!r}")

        # 1. Exact match on normalized_name.
        existing = (
            self._db.query(Company)
            .filter(Company.normalized_name == norm)
            .first()
        )
        if existing:
            log.info("company_exact_match", company_id=existing.id)
            self._ensure_alias(existing, raw_name, source)
            return existing

        # 2. Exact match via alias table.
        alias = (
            self._db.query(CompanyAlias)
            .filter(CompanyAlias.alias_name == raw_name)
            .first()
        )
        if alias:
            company = self._db.query(Company).get(alias.company_id)
            if company:
                log.info("company_alias_match", company_id=company.id)
                return company

        # Also check normalised alias.
        all_aliases = self._db.query(CompanyAlias).all()
        for a in all_aliases:
            if normalize_company_name(a.alias_name) == norm:
                company = self._db.query(Company).get(a.company_id)
                if company:
                    log.info("company_alias_norm_match", company_id=company.id)
                    self._ensure_alias(company, raw_name, source)
                    return company

        # 3. Fuzzy match.
        matches = self.find_matches(raw_name, threshold=threshold)
        if matches:
            best_company, best_score = matches[0]
            log.info(
                "company_fuzzy_match",
                company_id=best_company.id,
                score=round(best_score, 3),
            )
            self._ensure_alias(best_company, raw_name, source)
            return best_company

        # 4. Optionally verify via Companies House.
        ch_data: dict[str, Any] | None = None
        if self._ch:
            try:
                ch_data = await self._ch.enrich_company(raw_name)
            except Exception:
                log.exception("companies_house_verification_failed")

        # 5. Create new company.
        company = Company(
            name=raw_name,
            normalized_name=norm,
        )
        if ch_data:
            company.companies_house_number = ch_data.get("companies_house_number")
            company.registered_address = ch_data.get("registered_address")
            company.sic_codes = ch_data.get("sic_codes")
            if ch_data.get("company_type"):
                company.company_type = ch_data["company_type"]
            company.is_active = ch_data.get("is_active", True)

        self._db.add(company)
        self._db.flush()  # Get the ID.

        # Add alias if raw_name differs from stored name.
        self._ensure_alias(company, raw_name, source)

        log.info("company_created", company_id=company.id)
        return company

    # ------------------------------------------------------------------
    # Merge / deduplication
    # ------------------------------------------------------------------

    def merge_companies(self, primary_id: int, duplicate_ids: list[int]) -> Company:
        """Merge duplicate company records into the primary.

        Moves all foreign-key references (applications, schemes, contacts,
        pipeline items) from duplicates to the primary company.  Combines
        alias lists and retains the best data from each duplicate.

        Parameters
        ----------
        primary_id : int
            ID of the company to keep.
        duplicate_ids : list[int]
            IDs of companies to merge into the primary.

        Returns
        -------
        Company
            The updated primary company.
        """
        primary = self._db.query(Company).get(primary_id)
        if not primary:
            raise ValueError(f"Primary company {primary_id} not found")

        for dup_id in duplicate_ids:
            dup = self._db.query(Company).get(dup_id)
            if not dup:
                logger.warning("merge_duplicate_not_found", duplicate_id=dup_id)
                continue

            log = logger.bind(primary_id=primary_id, duplicate_id=dup_id)

            # Move FK references.
            self._db.query(PlanningApplication).filter(
                PlanningApplication.applicant_company_id == dup_id,
            ).update({"applicant_company_id": primary_id}, synchronize_session="fetch")

            self._db.query(PlanningApplication).filter(
                PlanningApplication.agent_company_id == dup_id,
            ).update({"agent_company_id": primary_id}, synchronize_session="fetch")

            self._db.query(ExistingScheme).filter(
                ExistingScheme.operator_company_id == dup_id,
            ).update({"operator_company_id": primary_id}, synchronize_session="fetch")

            self._db.query(ExistingScheme).filter(
                ExistingScheme.owner_company_id == dup_id,
            ).update({"owner_company_id": primary_id}, synchronize_session="fetch")

            self._db.query(PipelineOpportunity).filter(
                PipelineOpportunity.company_id == dup_id,
            ).update({"company_id": primary_id}, synchronize_session="fetch")

            self._db.query(Contact).filter(
                Contact.company_id == dup_id,
            ).update({"company_id": primary_id}, synchronize_session="fetch")

            # Copy useful fields that are empty on primary.
            if not primary.companies_house_number and dup.companies_house_number:
                primary.companies_house_number = dup.companies_house_number
            if not primary.registered_address and dup.registered_address:
                primary.registered_address = dup.registered_address
            if not primary.website and dup.website:
                primary.website = dup.website
            if not primary.sic_codes and dup.sic_codes:
                primary.sic_codes = dup.sic_codes
            if not primary.company_type and dup.company_type:
                primary.company_type = dup.company_type

            # Collect the duplicate's name as an alias.
            self._ensure_alias(primary, dup.name, "merge")
            for alias in dup.aliases:
                self._ensure_alias(primary, alias.alias_name, alias.source)

            # Soft-delete the duplicate.
            dup.is_active = False
            log.info("company_merged")

        self._db.flush()
        return primary

    def batch_deduplicate(
        self,
        threshold: float = 0.85,
    ) -> list[dict[str, Any]]:
        """Scan all active companies for potential duplicates.

        Returns a list of candidate pairs with similarity scores, suitable
        for human review.

        Returns
        -------
        list[dict]
            Each dict has keys ``company_a_id``, ``company_b_id``,
            ``company_a_name``, ``company_b_name``, and ``score``.
        """
        companies = (
            self._db.query(Company)
            .filter(Company.is_active.is_(True))
            .all()
        )

        pairs: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()

        for i, a in enumerate(companies):
            for b in companies[i + 1 :]:
                pair_key = (min(a.id, b.id), max(a.id, b.id))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                score = fuzz.token_sort_ratio(
                    a.normalized_name, b.normalized_name
                ) / 100.0
                if score >= threshold:
                    pairs.append(
                        {
                            "company_a_id": a.id,
                            "company_a_name": a.name,
                            "company_b_id": b.id,
                            "company_b_name": b.name,
                            "score": round(score, 3),
                        }
                    )

        pairs.sort(key=lambda x: x["score"], reverse=True)
        logger.info(
            "batch_deduplicate_complete",
            total_companies=len(companies),
            potential_duplicates=len(pairs),
        )
        return pairs

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_alias(self, company: Company, alias_name: str, source: str) -> None:
        """Add an alias if it does not already exist for the company."""
        if alias_name == company.name:
            return
        existing = (
            self._db.query(CompanyAlias)
            .filter(
                CompanyAlias.company_id == company.id,
                CompanyAlias.alias_name == alias_name,
            )
            .first()
        )
        if not existing:
            self._db.add(
                CompanyAlias(
                    company_id=company.id,
                    alias_name=alias_name,
                    source=source,
                )
            )
