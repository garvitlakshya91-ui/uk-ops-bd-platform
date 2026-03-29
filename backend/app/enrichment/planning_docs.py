"""Planning document analyser.

Downloads key planning documents (Design & Access Statement, Planning
Statement) and extracts structured data from PDF text using regex/keyword
patterns.  This enriches :class:`PlanningApplication` records where the
``description`` field is sparse.

Typical usage::

    from app.enrichment.planning_docs import PlanningDocumentAnalyzer

    analyzer = PlanningDocumentAnalyzer()
    data = await analyzer.analyze_application(planning_app)
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Document type detection
# ---------------------------------------------------------------------------

_DESIGN_ACCESS_KEYWORDS = [
    "design and access",
    "design & access",
    "d&a statement",
    "d & a statement",
]

_PLANNING_STATEMENT_KEYWORDS = [
    "planning statement",
    "planning supporting statement",
    "planning application statement",
]

# ---------------------------------------------------------------------------
# Extraction patterns
# ---------------------------------------------------------------------------

_UNIT_COUNT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(\d{1,5})\s*(?:residential\s+)?(?:units?|apartments?|flats?|dwellings?|homes?)", re.IGNORECASE),
    re.compile(r"(?:comprising|totalling|total of|providing|delivering)\s*(\d{1,5})\s*(?:units?|apartments?|flats?|dwellings?|homes?)", re.IGNORECASE),
    re.compile(r"(?:number of (?:units?|dwellings?|apartments?|flats?|homes?))\s*[:\-]?\s*(\d{1,5})", re.IGNORECASE),
]

_TENURE_MIX_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"(\d{1,3})\s*%?\s*(?:affordable|social|shared ownership|market|private|build.to.rent|btr|pbsa|intermediate|london affordable rent|discount market)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:affordable|social|shared ownership|market|private|btr|build.to.rent|pbsa|intermediate)\s*[:\-]?\s*(\d{1,5})\s*(?:units?|%)",
        re.IGNORECASE,
    ),
]

_DEVELOPER_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?:applicant|developer|client|on behalf of)\s*[:\-]?\s*([A-Z][A-Za-z\s&\.\,]{2,60}?)(?:\.|,|\n|$)", re.IGNORECASE),
    re.compile(r"(?:prepared for|submitted by|instructed by)\s*[:\-]?\s*([A-Z][A-Za-z\s&\.\,]{2,60}?)(?:\.|,|\n|$)", re.IGNORECASE),
]

_OPERATOR_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?:operator|managed by|management company|operated by)\s*[:\-]?\s*([A-Z][A-Za-z\s&\.\,]{2,60}?)(?:\.|,|\n|$)", re.IGNORECASE),
]

_COMPLETION_DATE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?:expected completion|completion date|anticipated completion|target completion|due for completion)\s*[:\-]?\s*(?:(?:by|in)\s+)?(\w+\s+\d{4}|\d{4}|Q[1-4]\s+\d{4})", re.IGNORECASE),
    re.compile(r"(?:complete(?:d|ion)?|deliver(?:y|ed)?)\s+(?:by|in)\s+(\w+\s+\d{4}|\d{4}|Q[1-4]\s+\d{4})", re.IGNORECASE),
]


@dataclass
class ExtractedDocumentData:
    """Structured data extracted from planning documents."""

    unit_count: int | None = None
    tenure_mix: list[dict[str, Any]] = field(default_factory=list)
    developer_name: str | None = None
    operator_name: str | None = None
    expected_completion: str | None = None
    scheme_type_hints: list[str] = field(default_factory=list)
    raw_extracts: dict[str, list[str]] = field(default_factory=dict)

    def is_empty(self) -> bool:
        """Return ``True`` if no meaningful data was extracted."""
        return (
            self.unit_count is None
            and not self.tenure_mix
            and self.developer_name is None
            and self.operator_name is None
            and self.expected_completion is None
            and not self.scheme_type_hints
        )


class PlanningDocumentAnalyzer:
    """Analyse planning application documents for structured data.

    Downloads PDFs from the application's ``documents_url``, extracts text
    using basic PDF text extraction (PyPDF2/pypdf), and applies regex
    patterns to find key fields.
    """

    # Maximum PDF size we are willing to download (20 MB).
    MAX_PDF_SIZE_BYTES = 20 * 1024 * 1024

    async def analyze_application(
        self,
        documents_url: str | None,
        description: str | None = None,
    ) -> ExtractedDocumentData:
        """Analyse a planning application's documents and description.

        Parameters
        ----------
        documents_url : str, optional
            URL to the documents listing page.
        description : str, optional
            The application's free-text description (analysed first as it is
            cheapest to process).
        """
        result = ExtractedDocumentData()

        # Always analyse the description if available.
        if description:
            self._extract_from_text(description, result)

        # If the description yielded useful data, we may not need to download.
        if not result.is_empty() and result.unit_count is not None:
            logger.info("planning_docs_from_description", unit_count=result.unit_count)
            return result

        # Attempt to download and analyse documents.
        if documents_url:
            try:
                doc_texts = await self._download_and_extract_text(documents_url)
                for text in doc_texts:
                    self._extract_from_text(text, result)
            except Exception:
                logger.exception(
                    "planning_docs_analysis_failed",
                    documents_url=documents_url,
                )

        return result

    # ------------------------------------------------------------------
    # Text extraction from PDFs
    # ------------------------------------------------------------------

    async def _download_and_extract_text(self, documents_url: str) -> list[str]:
        """Download relevant PDFs from a documents listing page and extract text.

        Returns a list of extracted text strings (one per document).
        """
        texts: list[str] = []

        async with httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            headers={"User-Agent": "UKOpsBDBot/1.0 (planning doc analysis)"},
        ) as client:
            # First, fetch the documents listing page to find PDF links.
            try:
                resp = await client.get(documents_url)
                resp.raise_for_status()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.error("planning_docs_listing_fetch_failed", error=str(exc))
                return texts

            pdf_urls = self._find_relevant_pdf_urls(resp.text, documents_url)
            logger.info("planning_docs_pdfs_found", count=len(pdf_urls))

            for pdf_url in pdf_urls[:3]:  # Limit to 3 documents.
                try:
                    pdf_resp = await client.get(pdf_url)
                    pdf_resp.raise_for_status()

                    if len(pdf_resp.content) > self.MAX_PDF_SIZE_BYTES:
                        logger.warning(
                            "planning_docs_pdf_too_large",
                            url=pdf_url,
                            size=len(pdf_resp.content),
                        )
                        continue

                    text = self._extract_text_from_pdf(pdf_resp.content)
                    if text:
                        texts.append(text)
                except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                    logger.warning("planning_docs_pdf_download_failed", url=pdf_url, error=str(exc))
                    continue

        return texts

    @staticmethod
    def _find_relevant_pdf_urls(html: str, base_url: str) -> list[str]:
        """Parse the documents listing HTML to find links to relevant PDFs."""
        from urllib.parse import urljoin

        # Find all <a href="...pdf"> links.
        href_re = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
        all_pdfs = href_re.findall(html)

        relevant: list[str] = []
        other: list[str] = []

        for href in all_pdfs:
            url = urljoin(base_url, href)
            lower = href.lower()

            is_relevant = any(
                kw in lower
                for kw in (
                    "design", "access", "planning-statement", "planning_statement",
                    "planningstatement", "d-and-a", "d_and_a", "d&a",
                )
            )
            if is_relevant:
                relevant.append(url)
            else:
                other.append(url)

        # Prefer targeted documents; fall back to first few generic ones.
        return relevant[:3] or other[:2]

    @staticmethod
    def _extract_text_from_pdf(content: bytes) -> str:
        """Extract plain text from a PDF binary using pypdf.

        Falls back gracefully if pypdf is not installed.
        """
        try:
            from pypdf import PdfReader
        except ImportError:
            try:
                from PyPDF2 import PdfReader  # type: ignore[no-redef]
            except ImportError:
                logger.warning("planning_docs_no_pdf_library")
                return ""

        try:
            reader = PdfReader(io.BytesIO(content))
            pages_text: list[str] = []
            # Read first 30 pages max to keep processing reasonable.
            for page in reader.pages[:30]:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            return "\n".join(pages_text)
        except Exception:
            logger.exception("planning_docs_pdf_extraction_failed")
            return ""

    # ------------------------------------------------------------------
    # Pattern extraction
    # ------------------------------------------------------------------

    def _extract_from_text(self, text: str, result: ExtractedDocumentData) -> None:
        """Apply all extraction patterns to *text* and merge into *result*."""
        # Unit count (take the largest plausible value found).
        if result.unit_count is None:
            for pattern in _UNIT_COUNT_PATTERNS:
                matches = pattern.findall(text)
                for m in matches:
                    try:
                        count = int(m)
                        if 2 <= count <= 10_000:
                            if result.unit_count is None or count > result.unit_count:
                                result.unit_count = count
                    except (ValueError, TypeError):
                        continue

        # Tenure mix.
        if not result.tenure_mix:
            for pattern in _TENURE_MIX_PATTERNS:
                for match in pattern.finditer(text):
                    full = match.group(0)
                    value = match.group(1)
                    result.tenure_mix.append({"text": full.strip(), "value": value})

        # Developer name.
        if result.developer_name is None:
            for pattern in _DEVELOPER_PATTERNS:
                match = pattern.search(text)
                if match:
                    name = match.group(1).strip().rstrip(".,")
                    if len(name) > 3 and not name.lower().startswith(("the ", "this ")):
                        result.developer_name = name
                        break

        # Operator name.
        if result.operator_name is None:
            for pattern in _OPERATOR_PATTERNS:
                match = pattern.search(text)
                if match:
                    name = match.group(1).strip().rstrip(".,")
                    if len(name) > 3:
                        result.operator_name = name
                        break

        # Expected completion date.
        if result.expected_completion is None:
            for pattern in _COMPLETION_DATE_PATTERNS:
                match = pattern.search(text)
                if match:
                    result.expected_completion = match.group(1).strip()
                    break

        # Scheme type hints.
        scheme_keywords = {
            "BTR": ["build to rent", "build-to-rent", "btr"],
            "PBSA": ["student", "pbsa", "purpose built student", "student accommodation"],
            "Co-living": ["co-living", "coliving", "co living"],
            "Senior": ["senior living", "retirement", "care home", "extra care", "later living"],
            "Affordable": ["affordable housing", "social housing", "affordable rent"],
        }
        text_lower = text.lower()
        for scheme_type, keywords in scheme_keywords.items():
            if any(kw in text_lower for kw in keywords):
                if scheme_type not in result.scheme_type_hints:
                    result.scheme_type_hints.append(scheme_type)
