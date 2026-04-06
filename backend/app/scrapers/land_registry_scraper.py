"""Land Registry CCOD integration for cross-referencing SPV property ownership.

Wraps the existing :class:`HMLRCCODScraper` to provide company-number-indexed
lookups against the CCOD dataset.  This enables cross-referencing developer
SPV company numbers (from Companies House) against actual land titles to
discover which sites an SPV owns.

The CCOD (Corporate and Commercial Ownership Data) is a free monthly CSV
from HM Land Registry listing all UK land titles where the registered
proprietor is a company.

Dataset: https://use-land-property-data.service.gov.uk/datasets/ccod

Usage::

    from app.scrapers.land_registry_scraper import LandRegistryCCODIndex

    index = LandRegistryCCODIndex()
    index.build_index()  # Streams and indexes the full CCOD dataset

    # Lookup properties owned by a specific company number
    properties = index.lookup_by_company_number("12345678")

    # Cross-reference a batch of company numbers
    results = index.cross_reference_companies(["12345678", "87654321"])
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator

import structlog

from app.scrapers.hmlr_ccod_scraper import CCODRow, HMLRCCODScraper

logger = structlog.get_logger(__name__)


@dataclass
class PropertyRecord:
    """A property title owned by a company, extracted from CCOD."""

    title_number: str
    property_address: str
    postcode: str
    tenure: str  # Freehold or Leasehold
    proprietor_name: str
    company_registration_number: str
    proprietorship_category: str
    country_incorporated: str
    price_paid: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "title_number": self.title_number,
            "property_address": self.property_address,
            "postcode": self.postcode,
            "tenure": self.tenure,
            "proprietor_name": self.proprietor_name,
            "company_registration_number": self.company_registration_number,
            "proprietorship_category": self.proprietorship_category,
            "country_incorporated": self.country_incorporated,
            "price_paid": self.price_paid,
        }


@dataclass
class CompanyPropertyPortfolio:
    """All properties owned by a specific company from CCOD data."""

    company_registration_number: str
    company_name: str
    properties: list[PropertyRecord] = field(default_factory=list)

    @property
    def title_count(self) -> int:
        return len(self.properties)

    @property
    def postcodes(self) -> set[str]:
        return {p.postcode for p in self.properties if p.postcode}

    @property
    def freehold_count(self) -> int:
        return sum(1 for p in self.properties if p.tenure.upper() == "FREEHOLD")

    @property
    def leasehold_count(self) -> int:
        return sum(1 for p in self.properties if p.tenure.upper() == "LEASEHOLD")

    def to_dict(self) -> dict:
        return {
            "company_registration_number": self.company_registration_number,
            "company_name": self.company_name,
            "title_count": self.title_count,
            "freehold_count": self.freehold_count,
            "leasehold_count": self.leasehold_count,
            "postcodes": sorted(self.postcodes),
            "properties": [p.to_dict() for p in self.properties],
        }


def _normalise_company_number(raw: str) -> str:
    """Normalise a Companies House number for consistent lookups.

    Strips whitespace, upper-cases, and zero-pads numeric-only numbers
    to 8 digits (the standard Companies House format).
    """
    cleaned = raw.strip().upper()
    # Pure numeric numbers should be zero-padded to 8 digits
    if cleaned.isdigit():
        cleaned = cleaned.zfill(8)
    return cleaned


class LandRegistryCCODIndex:
    """In-memory index of the CCOD dataset keyed by company registration number.

    Streams the CCOD CSV (via :class:`HMLRCCODScraper`) and builds a
    dict mapping company registration numbers to their property titles.

    Parameters
    ----------
    local_path : str | None
        Path to a pre-downloaded CCOD file. Passed through to
        :class:`HMLRCCODScraper`.
    """

    def __init__(self, local_path: str | None = None) -> None:
        self._scraper = HMLRCCODScraper(local_path=local_path)
        self._index: dict[str, CompanyPropertyPortfolio] = {}
        self._postcode_index: dict[str, list[PropertyRecord]] = defaultdict(list)
        self._is_built = False

    @property
    def is_built(self) -> bool:
        return self._is_built

    @property
    def company_count(self) -> int:
        return len(self._index)

    @property
    def total_titles(self) -> int:
        return sum(p.title_count for p in self._index.values())

    # ------------------------------------------------------------------
    # Index building
    # ------------------------------------------------------------------

    def build_index(self) -> dict[str, int]:
        """Stream the full CCOD dataset and build the company-number index.

        Returns
        -------
        dict
            Summary stats: total_rows, companies_indexed, titles_indexed.
        """
        logger.info("ccod_index_build_start")
        self._index.clear()
        self._postcode_index.clear()
        total_rows = 0
        skipped_no_reg = 0

        for row in self._scraper.iter_rows():
            total_rows += 1
            for prop in row.proprietors:
                reg_num = _normalise_company_number(prop.get("registration_number", ""))
                if not reg_num:
                    skipped_no_reg += 1
                    continue

                record = PropertyRecord(
                    title_number=row.title_number,
                    property_address=row.property_address,
                    postcode=row.postcode,
                    tenure=row.tenure,
                    proprietor_name=prop["name"],
                    company_registration_number=reg_num,
                    proprietorship_category=prop.get("category", ""),
                    country_incorporated=prop.get("country", ""),
                    price_paid=row.price_paid,
                )

                if reg_num not in self._index:
                    self._index[reg_num] = CompanyPropertyPortfolio(
                        company_registration_number=reg_num,
                        company_name=prop["name"],
                    )
                self._index[reg_num].properties.append(record)

                if row.postcode:
                    self._postcode_index[row.postcode].append(record)

        self._is_built = True
        stats = {
            "total_rows": total_rows,
            "companies_indexed": len(self._index),
            "titles_indexed": self.total_titles,
            "skipped_no_registration": skipped_no_reg,
        }
        logger.info("ccod_index_build_complete", **stats)
        return stats

    def build_index_for_companies(
        self,
        company_numbers: set[str],
    ) -> dict[str, int]:
        """Build a partial index containing only the specified companies.

        Much faster than building a full index when you only need to check
        a known set of SPV company numbers.

        Parameters
        ----------
        company_numbers : set[str]
            Set of company registration numbers to look for.

        Returns
        -------
        dict
            Summary stats.
        """
        normalised = {_normalise_company_number(cn) for cn in company_numbers}
        logger.info("ccod_partial_index_start", target_companies=len(normalised))

        self._index.clear()
        self._postcode_index.clear()
        total_rows = 0
        matched_rows = 0

        for row in self._scraper.iter_rows():
            total_rows += 1
            for prop in row.proprietors:
                reg_num = _normalise_company_number(prop.get("registration_number", ""))
                if reg_num not in normalised:
                    continue

                matched_rows += 1
                record = PropertyRecord(
                    title_number=row.title_number,
                    property_address=row.property_address,
                    postcode=row.postcode,
                    tenure=row.tenure,
                    proprietor_name=prop["name"],
                    company_registration_number=reg_num,
                    proprietorship_category=prop.get("category", ""),
                    country_incorporated=prop.get("country", ""),
                    price_paid=row.price_paid,
                )

                if reg_num not in self._index:
                    self._index[reg_num] = CompanyPropertyPortfolio(
                        company_registration_number=reg_num,
                        company_name=prop["name"],
                    )
                self._index[reg_num].properties.append(record)

                if row.postcode:
                    self._postcode_index[row.postcode].append(record)

        self._is_built = True
        stats = {
            "total_rows_scanned": total_rows,
            "matched_rows": matched_rows,
            "companies_found": len(self._index),
        }
        logger.info("ccod_partial_index_complete", **stats)
        return stats

    # ------------------------------------------------------------------
    # Lookup methods
    # ------------------------------------------------------------------

    def lookup_by_company_number(
        self,
        company_number: str,
    ) -> CompanyPropertyPortfolio | None:
        """Look up all properties owned by a specific company number.

        Parameters
        ----------
        company_number : str
            Companies House registration number.

        Returns
        -------
        CompanyPropertyPortfolio | None
            Portfolio of properties, or None if the company is not in CCOD.
        """
        if not self._is_built:
            raise RuntimeError(
                "Index not built. Call build_index() or build_index_for_companies() first."
            )
        normalised = _normalise_company_number(company_number)
        return self._index.get(normalised)

    def lookup_by_postcode(self, postcode: str) -> list[PropertyRecord]:
        """Look up all company-owned properties at a specific postcode.

        Parameters
        ----------
        postcode : str
            UK postcode (normalised internally).

        Returns
        -------
        list[PropertyRecord]
            Properties at that postcode.
        """
        if not self._is_built:
            raise RuntimeError("Index not built.")
        cleaned = postcode.strip().upper().replace(" ", "")
        if len(cleaned) > 3:
            cleaned = f"{cleaned[:-3]} {cleaned[-3:]}"
        return self._postcode_index.get(cleaned, [])

    def cross_reference_companies(
        self,
        company_numbers: list[str],
    ) -> dict[str, CompanyPropertyPortfolio]:
        """Cross-reference a list of company numbers against the CCOD index.

        Parameters
        ----------
        company_numbers : list[str]
            Company numbers to look up.

        Returns
        -------
        dict[str, CompanyPropertyPortfolio]
            Mapping from company number to their property portfolio.
            Only companies that appear in CCOD are included.
        """
        if not self._is_built:
            raise RuntimeError("Index not built.")

        results: dict[str, CompanyPropertyPortfolio] = {}
        for cn in company_numbers:
            normalised = _normalise_company_number(cn)
            portfolio = self._index.get(normalised)
            if portfolio:
                results[normalised] = portfolio

        logger.info(
            "ccod_cross_reference_complete",
            queried=len(company_numbers),
            found=len(results),
        )
        return results

    def find_companies_near_postcode(
        self,
        target_postcode: str,
        *,
        outcode_match: bool = True,
    ) -> list[PropertyRecord]:
        """Find company-owned properties near a given postcode.

        When ``outcode_match`` is True, matches all properties sharing the
        same outcode (first part of postcode, e.g. "SW1A" from "SW1A 1AA").

        Parameters
        ----------
        target_postcode : str
            Postcode to search near.
        outcode_match : bool
            If True, match the outcode prefix rather than exact postcode.

        Returns
        -------
        list[PropertyRecord]
            Matching property records.
        """
        if not self._is_built:
            raise RuntimeError("Index not built.")

        if not outcode_match:
            return self.lookup_by_postcode(target_postcode)

        # Extract outcode (everything before the space)
        cleaned = target_postcode.strip().upper().replace(" ", "")
        if len(cleaned) > 3:
            outcode = cleaned[:-3]
        else:
            outcode = cleaned

        results: list[PropertyRecord] = []
        for pc, records in self._postcode_index.items():
            pc_outcode = pc.replace(" ", "")[:-3] if len(pc.replace(" ", "")) > 3 else pc.replace(" ", "")
            if pc_outcode == outcode:
                results.extend(records)

        return results
