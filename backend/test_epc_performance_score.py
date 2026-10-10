"""Tests for the EPC performance score (no database, no network)."""
import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(__file__))

import app.scrapers.epc_scraper as epc_scraper  # noqa: E402
from app.scrapers.scheme_ingest import enrich_schemes_with_epc, epc_performance_score  # noqa: E402


def test_score_is_the_mean_band_score_weighted_by_certificate_count():
    # (3 * 100 + 10 * 85) / 13 = 88.46...
    assert epc_performance_score({"A": 3, "B": 10}) == 88.5
    assert epc_performance_score({"C": 4}) == 70.0
    assert epc_performance_score({"A": 1, "G": 1}) == 55.0
    # One certificate in every band: (100+85+70+55+40+25+10) / 7 = 55
    assert epc_performance_score(dict.fromkeys("ABCDEFG", 1)) == 55.0


def test_a_band_with_no_certificates_does_not_move_the_score():
    assert epc_performance_score({"B": 2, "D": 0}) == 85.0


def test_no_rated_certificates_gives_no_score():
    assert epc_performance_score({}) is None
    assert epc_performance_score(None) is None
    assert epc_performance_score({"A": 0, "B": 0}) is None


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args):
        return self

    def limit(self, n):
        return self

    def all(self):
        return self.rows


class _Db:
    def __init__(self, rows):
        self.rows = rows
        self.commits = 0

    def query(self, model):
        return _Query(self.rows)

    def commit(self):
        self.commits += 1


def _scraper_returning(by_postcode):
    class _Scraper:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get_rating_distribution(self, postcode=None, address=None):
            return by_postcode[postcode]

    return _Scraper


def test_enrichment_reads_the_ratings_inside_the_distribution():
    rated = {"ratings": {"A": 3, "B": 10}, "average_score": 84.2, "total": 13,
             "certificates": [{"lmk_key": "x"}]}
    empty = {"ratings": {}, "average_score": None, "total": 0, "certificates": []}
    with_certs = SimpleNamespace(id=1, postcode="B1 1AA", epc_ratings=None, performance_rating=None)
    without = SimpleNamespace(id=2, postcode="B2 2BB", epc_ratings=None, performance_rating=None)
    db = _Db([with_certs, without])

    real = epc_scraper.EPCScraper
    epc_scraper.EPCScraper = _scraper_returning({"B1 1AA": rated, "B2 2BB": empty})
    try:
        result = asyncio.run(enrich_schemes_with_epc(db))
    finally:
        epc_scraper.EPCScraper = real

    assert with_certs.epc_ratings == rated
    assert with_certs.performance_rating == 88.5
    # Nothing lodged at the postcode: no score, and not counted as enriched.
    assert without.performance_rating is None
    assert result == {"enriched": 1, "total_with_postcode": 2}
    assert db.commits == 1


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except Exception as exc:                      # noqa: BLE001
                failures += 1
                print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
    raise SystemExit(failures)
