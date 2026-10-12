"""Tests for the clean-up and operator-enrichment rules (pure functions, no DB)."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.models.rents import norm_academic_year  # noqa: E402
from app.scrapers.field_protection import _validate  # noqa: E402
from clean_scheme_data import FRAGMENT_PATTERNS, company_key  # noqa: E402
from enrich_operators import brand_key, role_stated, same_operator  # noqa: E402


def fragment(name):
    n = re.sub(r"\s+", " ", name.strip())
    return next((label for label, rx in FRAGMENT_PATTERNS if rx.search(n)), None)


def test_listing_fragments_are_caught():
    assert fragment("A2") == "too short"
    assert fragment("N02") == "unit code"
    assert fragment("234c") == "unit code"
    assert fragment("Flat 4") == "flat or block label"
    assert fragment("Block A") == "flat or block label"
    assert fragment("Premium Studio") == "room tier"
    assert fragment("DDA/Platinum") == "room tier"
    assert fragment("Silver En Suite") == "room tier"
    assert fragment("Arrange a Viewing") == "web page"
    assert fragment("Test") == "web page"
    assert fragment("11") == "too short"


def test_real_scheme_names_are_not_fragments():
    for name in ("Gold Street Studios", "The Studios", "Studio 51", "Platinum Point", "Silver Works",
                 "Classic House", "Crown Place", "iQ Penworks House", "Block Works", "Flatiron Yard",
                 "The Old Fire Station", "Room2 Hometel", "One Park West", "Studios"):
        assert fragment(name) is None, name


def test_company_spellings_collapse_only_when_the_same_company():
    assert company_key("Westfield Housing Association Limited") == company_key("WESTFIELD HOUSING ASSOCIATION LTD")
    assert company_key("Trent & Dove Housing Limited") == company_key("TRENT AND DOVE HOUSING LIMITED")
    assert company_key("The Sheffield City Council") == company_key("Sheffield City Council")
    assert company_key("Torus62 Limited") == company_key("TORUS 62 LIMITED")
    # A group and its subsidiary are different legal entities.
    assert company_key("Platform Housing Limited") != company_key("Platform Housing Group Limited")
    assert company_key("Anchor Hanover") != company_key("Anchor Hanover Group")
    assert company_key("Unite Students Limited") != company_key("Unite Group plc")


def test_academic_year_has_one_spelling():
    assert norm_academic_year("2026-27") == "2026/27"
    assert norm_academic_year("2026/27") == "2026/27"
    assert norm_academic_year("26/27") == "2026/27"
    assert norm_academic_year("2026/2027") == "2026/27"
    assert norm_academic_year("Current") == "Current"
    assert norm_academic_year(None) is None


def test_codes_are_stored_canonically():
    assert _validate("scheme_type", "Senior Living") == "Senior"
    assert _validate("scheme_type", "Mixed-use") == "Mixed"
    assert _validate("status", "Operational") == "operational"
    assert _validate("status", "Under Construction") == "under_construction"


def test_operator_names_compare_by_brand():
    assert same_operator("UNITE STUDENTS LIMITED", "Unite Students")
    assert same_operator("Host Students", "Host")
    assert same_operator("JLL", "JLL (Jones Lang LaSalle)")
    assert same_operator("Simple Life", "Simple Life (private rental)")
    assert not same_operator("Lloyds Living", "Ascend Living")
    assert not same_operator("UNCLE", "QuadReal Residential Properties")
    assert not same_operator(None, "Unite Students")
    assert brand_key("Homes for Students Limited") == brand_key("Homes For Students")


def test_a_quote_must_give_the_operator_a_running_role():
    assert role_stated("Clayworks is managed by LIV Group", "LIV Group")
    assert role_stated("operated by Unite Students", "Unite Students")
    assert role_stated("Juniper Grove, a Simple Life development", "Simple Life")
    # A role word inside the company's own name does not count.
    assert not role_stated("funded by Royal London Asset Management", "Royal London Asset Management")
    assert not role_stated("developed by QuadReal with Realstar", "QuadReal")
    assert not role_stated("Savills Investment Management acquired the block", "Savills Investment Management")


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except Exception as exc:
                failures += 1
                print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
    raise SystemExit(failures)
