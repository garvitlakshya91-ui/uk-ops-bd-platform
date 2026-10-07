"""Which census sources may appear in a published report.

Licensed benchmark packs (Student Source etc.) are QA references only:
their figures may be compared against ours but never published as ours.
"""
from __future__ import annotations

PUBLISHABLE_SOURCES = {
    "operator_page",        # operator's own property page (our scrape)
    "planning_consent",     # public planning register (our fetch + parse)
    "afs_directory",        # AccommodationForStudents listing (our scrape)
    "operator_directory",   # operator brand directory (our scrape)
    "sturents",             # StuRents listing (our scrape)
    "operator_unite_students",
    "listing_page_check",   # our availability checker
    "manual",               # our own analyst verification
}

QA_ONLY_SOURCES = {"benchmark_report"}


def publishable(entry: dict | None) -> bool:
    return bool(entry) and entry.get("source") in PUBLISHABLE_SOURCES


def published_value(scheme, field: str):
    """The value of `field` we may publish for a scheme, or None."""
    entry = (scheme.field_provenance or {}).get(field)
    return entry.get("value") if publishable(entry) else None
