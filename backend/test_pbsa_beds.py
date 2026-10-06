"""Tests for PBSA bed parsing (pure functions, no DB)."""
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.scrapers.pbsa_beds import expected_delivery_year, parse_beds


def test_plain_beds():
    assert parse_beds("Erection of purpose built student accommodation "
                      "comprising 540 bedspaces with ancillary facilities") == 540


def test_hyphen_and_comma():
    assert parse_beds("Demolition and construction of a 1,544-bed PBSA scheme") == 1544


def test_takes_largest_total():
    text = ("PBSA development of 814 beds arranged as 115 studios and "
            "cluster flats of 699 bedrooms")
    assert parse_beds(text) == 814


def test_studios_fallback():
    assert parse_beds("Student accommodation of 150 studios") == 150


def test_no_spaces_variant():
    assert parse_beds("student accommodation (236no. bedspaces)") == 236


def test_ignores_hotel_beds():
    assert parse_beds("Mixed scheme: 200 bed hotel and commercial space") is None


def test_ignores_small_and_huge():
    assert parse_beds("extension providing 4 bedrooms") is None
    assert parse_beds("masterplan for 9,000 beds across the city") is None


def test_none_text():
    assert parse_beds(None) is None
    assert parse_beds("Change of use to offices") is None


def test_delivery_year_approved():
    assert expected_delivery_year("Approved", datetime.date(2024, 6, 1)) == 2026


def test_delivery_year_under_construction():
    assert expected_delivery_year("Under Construction", datetime.date(2025, 2, 1)) == 2026


def test_delivery_year_submitted_is_none():
    assert expected_delivery_year("Submitted", datetime.date(2025, 2, 1)) is None
    assert expected_delivery_year("Approved", None) is None


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL  {name}: {exc}")
    raise SystemExit(failures)
