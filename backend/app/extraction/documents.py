"""Read capacity figures out of planning documents.

A description that states no number ("erection of a purpose-built
student accommodation building") usually has the number in the Design
and Access Statement or Planning Statement. This module turns a PDF into
page text, finds the pages that talk about capacity and runs the unit
extractor over them, returning each figure with the page it came from.

    reading = read_document(Path("das.pdf"))
    reading.best("beds")           # UnitFigure with .quote, .confidence
    reading.page_of(figure)        # 1-based page number

Only the text layer is read; scanned PDFs without one yield nothing.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from app.extraction.units import UnitExtraction, UnitFigure, extract_units

# Pages worth reading: the ones that talk about what is being provided.
_CAPACITY_PAGE = re.compile(
    r"\b(?:bed(?:\s?space|room)?s?|studios?|cluster|student|dwellings?|homes?|units?|storey|"
    r"accommodation\s+schedule|schedule\s+of\s+accommodation|quantum)\b", re.I)
# Document titles most likely to state the quantum, best first.
PREFERRED_TITLES = [
    r"design\s*(?:and|&)\s*access", r"planning\s+statement", r"accommodation\s+schedule",
    r"schedule\s+of\s+accommodation", r"application\s+form", r"covering\s+letter",
    r"committee\s+report", r"officer'?s?\s+report", r"decision\s+notice", r"heads\s+of\s+terms",
]
MAX_PAGES = 80


@dataclass
class DocumentReading:
    path: Path
    pages: int
    extractions: list[tuple[int, UnitExtraction]] = field(default_factory=list)

    @property
    def figures(self) -> list[tuple[int, UnitFigure]]:
        return [(p, f) for p, ex in self.extractions for f in ex.figures]

    def best(self, kind: str) -> Optional[UnitFigure]:
        """The committed figure stated most often across pages, then by confidence."""
        cands = [f for _, f in self.figures if f.kind == kind and f.committed() and not f.is_additional]
        if not cands:
            return None
        by_value: dict[int, list[UnitFigure]] = {}
        for f in cands:
            by_value.setdefault(f.value, []).append(f)
        value, group = max(by_value.items(), key=lambda kv: (len(kv[1]), max(f.confidence for f in kv[1])))
        best = max(group, key=lambda f: f.confidence)
        # Repetition across pages is corroboration; a lone mention is not.
        best.confidence = round(min(0.98, best.confidence + 0.03 * (len(group) - 1)), 3)
        return best

    def maximum(self, kind: str) -> Optional[UnitFigure]:
        cands = [f for _, f in self.figures if f.kind == kind and f.is_maximum]
        return max(cands, key=lambda f: (f.confidence, f.value)) if cands else None

    def page_of(self, fig: UnitFigure) -> Optional[int]:
        return next((p for p, f in self.figures if f is fig), None)

    def either_or(self) -> bool:
        return any(ex.either_or for _, ex in self.extractions)


def pdf_pages(path: Path, max_pages: int = MAX_PAGES) -> list[str]:
    """Text per page from the PDF's text layer (empty strings for image pages)."""
    import logging
    from pypdf import PdfReader
    # pypdf logs a warning per embedded font it cannot fully decode; the text
    # still comes through and the noise would bury the run's own output.
    logging.getLogger("pypdf").setLevel(logging.ERROR)
    reader = PdfReader(str(path))
    out = []
    for i, page in enumerate(reader.pages):
        if i >= max_pages:
            break
        try:
            out.append(page.extract_text() or "")
        except Exception:  # a broken page should not sink the document
            out.append("")
    return out


def read_document(path: Path, max_pages: int = MAX_PAGES) -> DocumentReading:
    pages = pdf_pages(path, max_pages)
    reading = DocumentReading(path=path, pages=len(pages))
    for i, text in enumerate(pages, start=1):
        text = re.sub(r"[ \t]+", " ", text)
        if not _CAPACITY_PAGE.search(text):
            continue
        ex = extract_units(text)
        if ex.figures:
            reading.extractions.append((i, ex))
    return reading


def title_rank(title: str) -> int:
    """Lower is better: where a document's title sits in PREFERRED_TITLES."""
    t = title or ""
    for i, rx in enumerate(PREFERRED_TITLES):
        if re.search(rx, t, re.I):
            return i
    return len(PREFERRED_TITLES)
