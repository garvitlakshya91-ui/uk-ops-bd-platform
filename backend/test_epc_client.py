"""Tests for the energy-certificate client (no network, no database).

The sample responses in data/epc_samples are of two kinds. The live_* files
were recorded from the live register on 2026-10-10, with addresses,
postcodes, UPRNs and certificate numbers replaced. The others are the
register's own published examples: the search example from its OpenAPI file
and three full certificates from its JSON samples, wrapped in "data" as the
API returns them. verify_epc.py checks the live service.
"""
import asyncio
import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import httpx  # noqa: E402

from app.scrapers.epc_client import (EPCApiError, EPCAuthError, EPCClient, EPCQueryError,  # noqa: E402
                                     certificate_to_legacy, is_new_dwelling, is_new_dwelling_candidate,
                                     search_row_to_legacy)
from app.scrapers.epc_new_dwelling_scraper import EPCNewDwellingScraper  # noqa: E402
from app.scrapers.epc_scraper import EPCScraper  # noqa: E402
from date_schemes_from_epc import agreement, judge  # noqa: E402

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "epc_samples")
# Error bodies as the live service sends them (its OpenAPI file shows them without the "data" wrapper).
NOT_FOUND = {"data": {"error": "No certificates could be found for that query"}}
NO_CERTIFICATE = {"data": {"error": "Certificate not found"}}
BAD_POSTCODE = {"data": {"error": "The search query was invalid - please provide a valid postcode"}}


def sample(name):
    with open(os.path.join(SAMPLES, name), encoding="utf-8") as fh:
        return json.load(fh)


def search_row(number, schema="SAP-Schema-19.1.0", postcode="S11 8BR", lodged="2019-06-01", **more):
    return {**sample("domestic_search.json")["data"][0], "certificateNumber": number, "schemaType": schema,
            "postcode": postcode, "registrationDate": lodged, **more}


def page(rows, current=1, pages=1):
    return {"data": rows, "pagination": {"totalRecords": len(rows) * pages, "currentPage": current,
                                         "totalPages": pages, "nextPage": current + 1 if current < pages else None,
                                         "prevPage": current - 1 if current > 1 else None, "pageSize": 5000}}


class Register:
    """A stand-in for the register: one search answer, certificates by number, and a log of requests."""

    def __init__(self, rows=(), certificates=None):
        self.rows, self.certificates, self.asked = list(rows), certificates or {}, []

    def __call__(self, request):
        self.asked.append(request)
        if request.url.path == "/api/domestic/search":
            return httpx.Response(200, json=page(self.rows)) if self.rows else httpx.Response(404, json=NOT_FOUND)
        doc = self.certificates.get(request.url.params["certificate_number"])
        return httpx.Response(200, json={"data": doc}) if doc else httpx.Response(404, json=NO_CERTIFICATE)

    def fetched(self):
        return sorted(q.url.params["certificate_number"] for q in self.asked if q.url.path == "/api/certificate")


def client_for(register, token="test-token"):
    return EPCClient(token=token, requests_per_second=None, retry_wait=0, transport=httpx.MockTransport(register))


def ask(register, **search):
    async def go():
        async with client_for(register) as client:
            return await client.search_domestic(**search)
    return asyncio.run(go())


def fails_with(error, fn):
    try:
        fn()
    except error:
        return True
    return False


def test_a_search_row_is_read_under_the_old_keys():
    assert search_row_to_legacy(sample("domestic_search.json")["data"][0]) == {
        "certificate-number": "0000-0000-0000-0000-0000",
        "address1": "1 Some Street", "address2": "", "address3": "", "address": "1 Some Street",
        "postcode": "SW10 0AA", "posttown": "Whitbury",
        "local-authority-label": "Hammersmith and Fulham", "constituency-label": "Chelsea and Fulham",
        "current-energy-rating": "B", "lodgement-date": "2020-05-04",
        "uprn": 100121241798, "schema-type": "RdSAP-Schema-20.0.0",
    }
    # The first three address lines keep their old keys; all four are in the joined address.
    four = search_row_to_legacy({"addressLine1": "Flat 12", "addressLine2": "Brook Place",
                                 "addressLine3": "3 Summerfield Street", "addressLine4": "Ecclesall",
                                 "postcode": "s11+8br", "registrationDate": "2019-06-01T00:00:00.000Z"})
    assert (four["address1"], four["address2"], four["address3"]) == ("Flat 12", "Brook Place", "3 Summerfield Street")
    assert four["address"] == "Flat 12, Brook Place, 3 Summerfield Street, Ecclesall"
    assert four["postcode"] == "S11 8BR" and four["lodgement-date"] == "2019-06-01"


def test_the_live_search_response_is_read_under_the_old_keys():
    live = sample("live_domestic_search.json")
    rows = [search_row_to_legacy(r) for r in live["data"]]
    assert len(rows) == live["pagination"]["totalRecords"] == 20
    for row in rows:
        assert row["certificate-number"] and row["address1"] and row["address"].startswith(row["address1"])
        assert row["postcode"] == "A0 0AA" and row["local-authority-label"] == "Tower Hamlets"
        assert len(row["lodgement-date"]) == 10 and row["lodgement-date"][:2] == "20"
        assert row["current-energy-rating"] in "ABCDEFG" and isinstance(row["uprn"], int)
        assert row["address3"] == ""                         # the register sends null for an unused line
    # Rows come newest first, and a block's postcode mixes both kinds of schema.
    assert rows[0]["lodgement-date"] == max(r["lodgement-date"] for r in rows)
    assert sum(is_new_dwelling_candidate(r) for r in rows) == 12
    assert {r["schema-type"] for r in rows if not is_new_dwelling_candidate(r)} == {
        "RdSAP-Schema-17.0", "RdSAP-Schema-17.1", "RdSAP-Schema-18.0", "RdSAP-Schema-19.0", "RdSAP-Schema-20.0.0"}


def test_the_live_certificate_is_read_under_the_old_keys():
    cert = certificate_to_legacy(sample("live_certificate_new_dwelling.json")["data"], "0000-0000-0000-0000-0001")
    assert cert["transaction-type"] == "new dwelling" and is_new_dwelling(cert)
    assert cert["property-type"] == "Flat" and cert["built-form"] == "Detached"
    assert cert["total-floor-area"] == 107.0
    assert (cert["current-energy-rating"], cert["current-energy-efficiency"]) == ("D", 58)
    assert (cert["potential-energy-rating"], cert["potential-energy-efficiency"]) == ("D", 58)
    assert cert["lodgement-date"] == "2017-07-07" and cert["inspection-date"] == "2017-03-09"
    assert cert["assessment-type"] == "SAP" and cert["schema-type"] == "SAP-Schema-17.1"
    assert cert["address1"] == "Flat 1" and cert["postcode"] == "A0 0AA"
    # The certificate itself carries no council; that comes from the search row.
    assert cert["local-authority-label"] == ""


def test_a_full_certificate_is_read_under_the_old_keys():
    cert = certificate_to_legacy(sample("certificate_sap_new_dwelling.json")["data"], "1111-2222-3333-4444-5555")
    assert cert["certificate-number"] == "1111-2222-3333-4444-5555"
    assert cert["transaction-type"] == "new dwelling" and is_new_dwelling(cert)
    assert cert["property-type"] == "House" and cert["built-form"] == "Mid-Terrace"
    assert cert["total-floor-area"] == 96.0
    assert cert["current-energy-rating"] == "B" and cert["current-energy-efficiency"] == 83
    assert cert["potential-energy-efficiency"] == 83
    assert cert["lodgement-date"] == "2014-04-05" and cert["inspection-date"] == "2014-04-04"
    assert cert["address1"] == "17, Street Terrace" and cert["postcode"] == "AA1 1AA"
    assert cert["assessment-type"] == "SAP" and cert["schema-type"] == "SAP-Schema-16.3"


def test_a_sap_schema_alone_does_not_make_a_new_dwelling():
    sale = certificate_to_legacy(sample("certificate_sap_marketed_sale.json")["data"])
    assert sale["transaction-type"] == "marketed sale"
    assert is_new_dwelling_candidate(sale) and not is_new_dwelling(sale)
    existing = certificate_to_legacy(sample("certificate_rdsap.json")["data"])
    assert not is_new_dwelling_candidate(existing) and not is_new_dwelling(existing)
    # From 12 up a transaction code means different things in different schemas, so it is not named.
    assert existing["transaction-type"] == "code 16"
    # Before schema 17, existing-dwelling certificates were lodged under a SAP schema as well.
    assert is_new_dwelling_candidate({"schema-type": "SAP-Schema-16.3"})
    assert not is_new_dwelling_candidate(search_row_to_legacy(sample("domestic_search.json")["data"][0]))


def test_search_sends_the_token_and_follows_the_pages():
    asked = []

    def register(request):
        asked.append(request)
        current = int(request.url.params["current_page"])
        return httpx.Response(200, json=page([search_row(f"0000-0000-0000-0000-000{current}")], current, pages=3))

    rows = ask(register, postcode="s11 8br")
    assert [r["certificate-number"][-1] for r in rows] == ["1", "2", "3"]
    first = asked[0]
    assert str(first.url).startswith("https://api.get-energy-performance-data.communities.gov.uk/api/domestic/search?")
    assert first.headers["authorization"] == "Bearer test-token" and first.headers["accept"] == "application/json"
    assert first.url.params["postcode"] == "S11 8BR" and first.url.params["page_size"] == "5000"
    assert [q.url.params["current_page"] for q in asked] == ["1", "2", "3"]


def test_council_names_and_dates_are_sent_as_the_register_expects():
    register = Register()
    today = dt.date.today()
    assert ask(register, council=["Birmingham", "Bristol, City of"],
               date_start=today - dt.timedelta(days=10), date_end=today) == []
    sent = register.asked[0].url.params
    assert sent.get_list("council[]") == ["Birmingham", "Bristol, City of"]
    assert sent["date_start"] == (today - dt.timedelta(days=10)).isoformat()
    assert sent["date_end"] == (today - dt.timedelta(days=1)).isoformat()      # a range may not reach today
    # A long range is read a week at a time; neighbouring weeks share their boundary day.
    start = today - dt.timedelta(days=30)
    weeks = EPCClient._windows(start, start + dt.timedelta(days=20), 7)
    assert weeks == [(start, start + dt.timedelta(days=7)),
                     (start + dt.timedelta(days=7), start + dt.timedelta(days=14)),
                     (start + dt.timedelta(days=14), start + dt.timedelta(days=20))]


def test_nothing_found_is_empty_but_a_failure_never_is():
    assert ask(Register(), postcode="S11 8BR") == []
    moved = "https://get-energy-performance-data.communities.gov.uk/"
    failures = (
        (lambda: httpx.Response(301, headers={"Location": moved}), EPCApiError),       # the old API's fate
        (lambda: httpx.Response(200, text="<html>Get energy performance data</html>"), EPCApiError),
        (lambda: httpx.Response(404, text="<html>Page not found</html>"), EPCApiError),
        (lambda: httpx.Response(500, json={"errors": [{"code": "boom"}]}), EPCApiError),
        (lambda: httpx.Response(403, json={"errors": [{"code": "UNAUTHORISED"}]}), EPCAuthError),
        (lambda: httpx.Response(400, json=BAD_POSTCODE), EPCQueryError),
    )
    for answer, error in failures:
        assert fails_with(error, lambda: ask(lambda request: answer(), postcode="S11 8BR")), answer().status_code
    # The error body is also understood without the "data" wrapper, as the OpenAPI file documents it.
    assert ask(lambda request: httpx.Response(404, json=NOT_FOUND["data"]), postcode="S11 8BR") == []
    # Without a token nothing is sent at all.
    register = Register()
    assert fails_with(EPCAuthError, lambda: asyncio.run(client_for(register, token="").__aenter__()))
    assert register.asked == []


def test_a_rate_limit_is_waited_out():
    answers = [httpx.Response(429), httpx.Response(503), httpx.Response(200, json=page([search_row("A")]))]
    assert len(ask(lambda request: answers.pop(0), postcode="S11 8BR")) == 1


def test_new_dwellings_are_confirmed_on_the_full_certificate():
    new_home = sample("certificate_sap_new_dwelling.json")["data"]
    resale = sample("certificate_sap_marketed_sale.json")["data"]
    register = Register(
        rows=[search_row("NEW", council="Sheffield"), search_row("RESALE"), search_row("WITHDRAWN", "SAP-Schema-16.3"),
              search_row("EXISTING", "RdSAP-Schema-21.0.1")],
        certificates={"NEW": new_home, "RESALE": resale})

    async def go():
        async with client_for(register) as client:
            return await client.search_new_dwellings(postcode="S11 8BR")
    found = asyncio.run(go())
    assert [r["certificate-number"] for r in found] == ["NEW"]
    # The existing-dwelling certificate costs no request.
    assert register.fetched() == ["NEW", "RESALE", "WITHDRAWN"]
    # The certificate fills in what the search row lacks and leaves the rest alone.
    assert found[0]["property-type"] == "House" and found[0]["total-floor-area"] == 96.0
    assert found[0]["postcode"] == "S11 8BR" and found[0]["local-authority-label"] == "Sheffield"


def test_small_postcodes_can_be_skipped_before_any_certificate_is_fetched():
    new_home = sample("certificate_sap_new_dwelling.json")["data"]
    block = [search_row(f"BLOCK-{i}", postcode="B7 4EH") for i in range(3)]
    register = Register(rows=block + [search_row("HOUSE", postcode="B1 1AA")],
                        certificates={r["certificateNumber"]: new_home for r in block} | {"HOUSE": new_home})

    async def go():
        async with client_for(register) as client:
            return await client.search_new_dwellings(council="Birmingham", min_per_postcode=2)
    assert len(asyncio.run(go())) == 3
    assert register.fetched() == ["BLOCK-0", "BLOCK-1", "BLOCK-2"]


def test_certificates_read_through_the_client_date_a_scheme():
    scheme = {"name": "Brook Place", "postcode": "S11 8BR", "size": 237}
    rows = [search_row(f"FLAT-{i}", addressLine1=f"Flat {i}", addressLine2="Brook Place") for i in range(40)]
    relets = [search_row(f"RELET-{i}", "RdSAP-Schema-21.0.1", lodged="2023-02-01") for i in range(25)]
    register = Register(rows=rows + relets,
                        certificates={r["certificateNumber"]: sample("certificate_sap_new_dwelling.json")["data"]
                                      for r in rows})

    async def go():
        async with client_for(register) as client:
            summary = await client.search_domestic(postcode="S11 8BR")
            return summary, await client.with_details(summary)
    summary, certs = asyncio.run(go())
    got = judge(scheme, certs, 2026)
    assert got and got["year"] == 2019 and got["certificates"] == 40 and got["by_name"]
    # The search rows alone carry no transaction type, so they date nothing.
    assert judge(scheme, summary, 2026) is None
    # Cross-checking a year taken from the build-to-rent feed.
    assert (agreement(2019, got["year"]), agreement(2020, got["year"]), agreement(2016, got["year"])) == \
        ("same year", "a year apart", "more than a year apart")


def test_certificates_that_may_be_a_neighbours_are_a_note_not_a_date():
    lodged = [{"transaction-type": "new dwelling", "lodgement-date": "2011-09-01", "address1": f"Studio {i}",
               "address2": "Dashwood Studios"} for i in range(232)]
    # Found on the live register: two halls on one postcode, and only one of them is in the certificates.
    named = judge({"name": "Dashwood Studios", "postcode": "SE17 1JL", "size": 232,
                   "neighbours": ["Julian Markham House"]}, lodged, 2026)
    neighbour = judge({"name": "Julian Markham House", "postcode": "SE17 1JL", "size": 232,
                       "neighbours": ["Dashwood Studios"]}, lodged, 2026)
    assert named["confidence"] >= 0.75 and named["by_name"]
    assert neighbour["year"] == 2011 and neighbour["confidence"] < 0.75
    assert neighbour["quote"].endswith("another scheme shares the postcode")
    # A street name is in every address on the street, so it singles nothing out.
    walk = [{"transaction-type": "new dwelling", "lodgement-date": "2013-08-01", "address1": f"Flat {i}",
             "address2": "136 New Walk"} for i in range(30)]
    terrace = judge({"name": "134 New Walk", "postcode": "LE1 7JG", "size": 16, "neighbours": ["136 - 138 New Walk"]},
                    walk, 2026)
    assert not terrace["by_name"] and terrace["confidence"] < 0.75
    # One building held under two names: the certificates carry both, so they date neither firmly.
    both = [{"transaction-type": "new dwelling", "lodgement-date": "2014-09-01",
             "address1": f"Flat {i}, Davidson House", "address2": "1 Penrhyn Road"} for i in range(14)]
    for name, other in (("Davidson House", "One Penrhyn Road"), ("One Penrhyn Road", "Davidson House")):
        got = judge({"name": name, "postcode": "KT1 2BT", "size": 100, "neighbours": [other]}, both, 2026)
        assert got["by_name"] and got["confidence"] < 0.75 and "neighbouring scheme's name" in got["quote"]
    # Alone on the postcode the same certificates are taken at face value...
    assert judge({"name": "Julian Markham House", "postcode": "SE17 1JL", "size": 232}, lodged, 2026)["confidence"] >= 0.75
    # ...unless there are far more of them than the scheme has homes.
    small = judge({"name": "Canterbury", "postcode": "CT2 8FE", "size": 5}, lodged[:54], 2026)
    assert small["confidence"] < 0.75 and "more certificates than the scheme has homes" in small["quote"]


def test_rating_distribution_counts_the_bands():
    rows = [search_row(str(i), currentEnergyEfficiencyBand=band) for i, band in enumerate(["B", "B", "C", None])]

    def distribution(register):
        async def go():
            async with EPCScraper(client=client_for(register)) as scraper:
                return await scraper.get_rating_distribution(postcode="s11 8br")
        return asyncio.run(go())

    got = distribution(Register(rows=rows))
    assert got["ratings"] == {"B": 2, "C": 1} and got["total"] == 4
    assert got["certificates"][0] == {"certificate_number": "0", "address": "1 Some Street", "postcode": "S11 8BR",
                                      "current_rating": "B", "lodgement_date": "2019-06-01"}
    empty = {"ratings": {}, "total": 0, "certificates": []}
    assert distribution(Register()) == empty
    # A postcode the register refuses as malformed has no certificates; an outage is not that.
    assert distribution(lambda request: httpx.Response(400, json=BAD_POSTCODE)) == empty
    assert fails_with(EPCApiError, lambda: distribution(lambda request: httpx.Response(301)))


def test_discovery_clusters_confirmed_new_dwellings():
    flat = {**sample("certificate_sap_new_dwelling.json")["data"], "property_type": 2}
    resale = sample("certificate_sap_marketed_sale.json")["data"]
    block = [search_row(f"BLOCK-{i}", postcode="B7 4EH", council="Birmingham", addressLine1=f"Flat {i}",
                        addressLine2="Mill Wharf") for i in range(6)]
    resold = [search_row(f"RESOLD-{i}", postcode="B1 1AA") for i in range(6)]
    pair = [search_row(f"PAIR-{i}", postcode="B2 2BB") for i in range(2)]
    register = Register(rows=block + resold + pair,
                        certificates={r["certificateNumber"]: flat for r in block + pair}
                        | {r["certificateNumber"]: resale for r in resold})

    async def go():
        async with EPCNewDwellingScraper(days_back=5, min_cluster_size=5, client=client_for(register)) as scraper:
            return await scraper.discover_schemes(["Birmingham"])
    schemes = asyncio.run(go())
    assert [(s["postcode"], s["num_units"], s["scheme_type"]) for s in schemes] == [("B7 4EH", 6, "BTR")]
    assert schemes[0]["name"] == "Mill Wharf" and schemes[0]["local_authority_label"] == "Birmingham"
    assert schemes[0]["epc_ratings"] == {"B": 6} and schemes[0]["avg_floor_area"] == 96.0
    assert not any(number.startswith("PAIR") for number in register.fetched())
    assert register.asked[0].url.params.get_list("council[]") == ["Birmingham"]


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
