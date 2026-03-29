"""HM Land Registry CCOD (Corporate and Commercial Ownership Data) scraper.

Downloads and processes the free monthly CCOD dataset published by HMLR,
which lists all UK land titles where the registered proprietor is a company
or corporate body.

This is used to fill ``ExistingScheme.owner_company_id`` with ground-truth
registered proprietor data, matched by postcode.

Dataset homepage:
    https://use-land-property-data.service.gov.uk/datasets/ccod

CSV column headers (in order):
    Title Number, Tenure, Property Address, District, County, Region,
    Postcode, Multiple Address Indicator, Price Paid,
    Proprietor Name (1..4), Company Registration No. (1..4),
    Proprietorship Category (1..4), Country Incorporated (1..4),
    Date Proprietor Added, Additional Proprietor Indicator

Typical file size: ~120 MB compressed / ~500 MB uncompressed.
The file is processed in streaming chunks to avoid loading it entirely into
memory.

Usage::

    scraper = HMLRCCODScraper()
    # Either download automatically:
    rows = await scraper.download_and_parse()
    # Or supply a pre-downloaded local file:
    rows = scraper.parse_local_file("/data/ccod/CCOD_FULL_2024_03.csv")
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from pathlib import Path
from typing import Generator, Iterator

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

# Column names as they appear in the HMLR CCOD CSV download.
_COL_TITLE_NUMBER = "Title Number"
_COL_TENURE = "Tenure"
_COL_PROPERTY_ADDRESS = "Property Address"
_COL_POSTCODE = "Postcode"
_COL_PRICE_PAID = "Price Paid"

# Up to 4 proprietors per title.
_MAX_PROPRIETORS = 4


def _proprietor_cols(n: int) -> tuple[str, str, str, str]:
    """Return the four column names for proprietor slot *n* (1-indexed)."""
    return (
        f"Proprietor Name ({n})",
        f"Company Registration No. ({n})",
        f"Proprietorship Category ({n})",
        f"Country Incorporated ({n})",
    )


class CCODRow:
    """Lightweight parsed representation of a single CCOD CSV row."""

    __slots__ = (
        "title_number",
        "tenure",
        "property_address",
        "postcode",
        "price_paid",
        "proprietors",
    )

    def __init__(
        self,
        title_number: str,
        tenure: str,
        property_address: str,
        postcode: str,
        price_paid: str,
        proprietors: list[dict[str, str]],
    ) -> None:
        self.title_number = title_number
        self.tenure = tenure
        self.property_address = property_address
        self.postcode = _normalise_postcode(postcode)
        self.price_paid = price_paid
        self.proprietors = proprietors  # list of {name, registration_number, category, country}

    @property
    def primary_proprietor(self) -> dict[str, str] | None:
        return self.proprietors[0] if self.proprietors else None


def _normalise_postcode(raw: str) -> str:
    """Upper-case and normalise spacing in a UK postcode."""
    cleaned = raw.strip().upper().replace(" ", "")
    if len(cleaned) > 3:
        return f"{cleaned[:-3]} {cleaned[-3:]}"
    return cleaned


def _s(val: str | None) -> str:
    """Safely strip a value that may be None (DictReader restval default)."""
    return (val or "").strip()


def _parse_row(row: dict[str, str]) -> CCODRow | None:
    """Parse a CSV dict row into a :class:`CCODRow`.

    Returns ``None`` if the row is missing essential fields.
    csv.DictReader sets missing trailing fields to None (not ""),
    so all .get() calls are wrapped with _s() to guard against that.
    """
    title_number = _s(row.get(_COL_TITLE_NUMBER))
    if not title_number:
        return None

    proprietors: list[dict[str, str]] = []
    for n in range(1, _MAX_PROPRIETORS + 1):
        name_col, reg_col, cat_col, country_col = _proprietor_cols(n)
        name = _s(row.get(name_col))
        if not name:
            break  # Slots are filled left-to-right; no more after first empty.
        proprietors.append({
            "name": name,
            "registration_number": _s(row.get(reg_col)),
            "category": _s(row.get(cat_col)),
            "country": _s(row.get(country_col)),
        })

    if not proprietors:
        return None

    return CCODRow(
        title_number=title_number,
        tenure=_s(row.get(_COL_TENURE)),
        property_address=_s(row.get(_COL_PROPERTY_ADDRESS)),
        postcode=_s(row.get(_COL_POSTCODE)),
        price_paid=_s(row.get(_COL_PRICE_PAID)),
        proprietors=proprietors,
    )


class HMLRCCODScraper:
    """Scraper for the HMLR CCOD (corporate land ownership) dataset.

    Parameters
    ----------
    download_url : str
        URL of the CCOD CSV or ZIP download.  Defaults to
        ``settings.HMLR_CCOD_DOWNLOAD_URL``.
    local_path : str | None
        Path to a pre-downloaded CCOD file (CSV or ZIP).  When set,
        the download step is skipped entirely.
    chunk_size : int
        Number of CSV rows to yield per iteration when streaming.
    """

    def __init__(
        self,
        download_url: str | None = None,
        local_path: str | None = None,
        chunk_size: int = 5_000,
    ) -> None:
        self._download_url = download_url or settings.HMLR_CCOD_DOWNLOAD_URL
        self._local_path = local_path or settings.HMLR_CCOD_LOCAL_PATH
        self._chunk_size = chunk_size

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def iter_rows(self) -> Iterator[CCODRow]:
        """Yield parsed :class:`CCODRow` instances from the CCOD dataset.

        Uses a pre-downloaded local file if ``local_path`` is configured,
        otherwise downloads the file synchronously.
        """
        if self._local_path and os.path.exists(self._local_path):
            logger.info("hmlr_ccod_using_local_file", path=self._local_path)
            yield from self._parse_file(Path(self._local_path))
        elif self._download_url:
            logger.info("hmlr_ccod_downloading", url=self._download_url)
            yield from self._download_and_parse()
        else:
            raise RuntimeError(
                "Neither HMLR_CCOD_DOWNLOAD_URL nor HMLR_CCOD_LOCAL_PATH is configured. "
                "Set one in your .env file."
            )

    def filter_by_postcodes(self, postcodes: set[str]) -> Iterator[CCODRow]:
        """Yield only rows whose postcode is in *postcodes*.

        This avoids building a full in-memory index when we only care about
        a known set of scheme postcodes.
        """
        matched = 0
        total = 0
        for row in self.iter_rows():
            total += 1
            if row.postcode and row.postcode in postcodes:
                matched += 1
                yield row

        logger.info(
            "hmlr_ccod_postcode_filter_done",
            total_rows=total,
            matched_rows=matched,
            postcode_count=len(postcodes),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_file(self, path: Path) -> Iterator[CCODRow]:
        """Stream-parse a CCOD file (CSV or ZIP containing a CSV)."""
        suffix = path.suffix.lower()
        if suffix == ".zip":
            yield from self._parse_zip(path)
        else:
            yield from self._parse_csv_path(path)

    def _parse_zip(self, path: Path) -> Iterator[CCODRow]:
        """Extract and stream-parse the first CSV inside a ZIP archive."""
        with zipfile.ZipFile(path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV file found inside ZIP: {path}")
            csv_name = csv_names[0]
            logger.info("hmlr_ccod_extracting_zip", csv_name=csv_name)
            with zf.open(csv_name) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")
                yield from self._parse_csv_stream(text)

    def _parse_csv_path(self, path: Path) -> Iterator[CCODRow]:
        with open(path, encoding="utf-8-sig", errors="replace") as fh:
            yield from self._parse_csv_stream(fh)

    def _parse_csv_stream(self, stream) -> Iterator[CCODRow]:
        reader = csv.DictReader(stream)
        parsed = 0
        skipped = 0
        for raw_row in reader:
            row = _parse_row(raw_row)
            if row is None:
                skipped += 1
                continue
            parsed += 1
            if parsed % 100_000 == 0:
                logger.info("hmlr_ccod_progress", parsed=parsed, skipped=skipped)
            yield row

        logger.info("hmlr_ccod_parse_complete", parsed=parsed, skipped=skipped)

    def _download_and_parse(self) -> Iterator[CCODRow]:
        """Download the CCOD file into a temp buffer and parse it.

        For production use, prefer setting HMLR_CCOD_LOCAL_PATH and running
        a separate download step (e.g. wget/curl) — the file is ~120 MB.
        """
        with httpx.Client(timeout=httpx.Timeout(300.0, connect=30.0)) as client:
            logger.info("hmlr_ccod_download_start", url=self._download_url)
            with client.stream("GET", self._download_url, follow_redirects=True) as resp:
                resp.raise_for_status()
                content_type = resp.headers.get("content-type", "")
                is_zip = "zip" in content_type or self._download_url.endswith(".zip")

                buf = io.BytesIO()
                downloaded = 0
                for chunk in resp.iter_bytes(chunk_size=65536):
                    buf.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10 * 1024 * 1024) == 0:
                        logger.info("hmlr_ccod_download_progress", mb=downloaded // (1024 * 1024))

                buf.seek(0)
                logger.info("hmlr_ccod_download_complete", bytes=downloaded)

        if is_zip:
            with zipfile.ZipFile(buf, "r") as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise ValueError("No CSV found in downloaded CCOD ZIP")
                with zf.open(csv_names[0]) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")
                    yield from self._parse_csv_stream(text)
        else:
            buf.seek(0)
            text = io.TextIOWrapper(buf, encoding="utf-8-sig", errors="replace")
            yield from self._parse_csv_stream(text)
