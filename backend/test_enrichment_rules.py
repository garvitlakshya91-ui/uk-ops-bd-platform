"""Tests for the enrichment rules (pure functions, no database, no network)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from date_btr_from_feed import completion_year, planning_year  # noqa: E402
from date_schemes_from_epc import judge as judge_certificates  # noqa: E402
from enrich_companies import (job_title, judge_number, legal_name, name_verdict, norm_number, norm_phone,  # noqa: E402
                              person_name, read_site_page)
from read_scheme_pages import candidates, match_words, monthly_rents, names_scheme, opening_year  # noqa: E402
from scrape_national_code import brand_tokens, city_key  # noqa: E402


def co(name, n_op=0, n_own=1):
    return {"name": name, "n_op": n_op, "n_own": n_own}


def profile(name, status="active", created="2010-01-01", sic=("68209",), accounts="full", number="01234567"):
    return {"company_name": name, "company_status": status, "date_of_creation": created, "sic_codes": list(sic),
            "accounts": {"last_accounts": {"type": accounts}}, "company_number": number}


def test_a_register_number_is_trusted_only_when_it_looks_like_the_business():
    # The full legal name, as a land register feed writes it.
    assert judge_number(co("COUNTRYSIDE PROPERTIES (UK) LIMITED", n_own=65),
                        profile("COUNTRYSIDE PROPERTIES (UK) LIMITED")) == ("legal name", None)
    # A brand name matched to a property company.
    assert judge_number(co("Mezzino", n_op=47), profile("MEZZINO LIMITED", sic=("68320",)))[1] is None
    # Look-alike registrations: recent, dormant or in another trade.
    assert judge_number(co("Unite Group Plc", n_own=127),
                        profile("UNITE GROUP HOLDINGS LIMITED", created="2024-11-05", accounts="dormant"))[1]
    assert judge_number(co("Yugo", n_op=28), profile("YUGO LIMITED", sic=("56210",), accounts="dormant"))[1]
    assert judge_number(co("Touchstone", n_op=11),
                        profile("TOUCHSTONE LTD", sic=("62020",), accounts="total-exemption-full"))[1]
    assert judge_number(co("Cullross Living", n_op=1), profile("CULLROSS LIVING LIMITED", created="2025-06-03"))[1]
    # Dissolved is never trusted, however good the name.
    assert judge_number(co("iQ Student Accommodation", n_op=79),
                        profile("IQ STUDENT ACCOMMODATION LIMITED", status="dissolved"))[1]
    # The entity a company's own website names is trusted through its alias.
    assert judge_number(co("Simple Life", n_op=92), profile("SIGMA CAPITAL OPERATIONAL MANAGEMENT LIMITED"),
                        ("SIGMA CAPITAL OPERATIONAL MANAGEMENT LIMITED",)) == ("legal name", None)
    # A registered society has almost nothing on file: the name is enough.
    assert judge_number(co("Midland Heart Limited", n_own=18),
                        profile("MIDLAND HEART LIMITED", status="", created="", sic=(), accounts="",
                                number="IP30069R")) == ("registered society", None)


def test_company_names_and_numbers():
    assert legal_name("Trent & Dove Housing Ltd.") == legal_name("TRENT AND DOVE HOUSING LIMITED")
    assert name_verdict("Placefirst", "PLACEFIRST LIMITED") == "match"
    assert name_verdict("Birmingham City Council", "BIRMINGHAM COUNCIL OF MOSQUES (BCM)") == "differs"
    assert norm_number("9840367") == "09840367"
    assert norm_number("oc 315531") == "OC315531"
    assert norm_number("22981R") is None                 # a society number the company register does not serve


def test_people_and_phones():
    assert person_name("GORDON, Helen Christine") == "Helen Christine Gordon"
    assert person_name("MCHUGH, Mr Phil") == "Phil Mchugh"
    assert job_title("director", "Chief Executive") == "Director (Chief Executive)"
    assert job_title("director", "Director") == "Director"
    assert job_title("secretary", None) == "Company secretary"
    assert norm_phone("+44 (0) 191 261 1819") == "01912611819"
    assert norm_phone("0333 999 9926") == "03339999926"
    assert norm_phone("07700 900123") is None            # a mobile is a person's number
    assert norm_phone("12345") is None


def test_a_company_page_gives_switchboard_mailbox_and_number_only():
    html = ('<footer><a href="tel:+441612363344">0161 236 3344</a> <a href="mailto:info@brookliving.co.uk">Email</a>'
            ' <a href="mailto:jane.smith@brookliving.co.uk">Jane</a>'
            ' Brook Living Ltd is registered in England and Wales, company number 06516374.</footer>')
    got = read_site_page(html, "brookliving.co.uk")
    assert got["phones"].most_common(1)[0][0] == "01612363344"
    assert list(got["emails"]) == ["info@brookliving.co.uk"]        # a named person's address is not kept
    assert list(got["numbers"]) == ["06516374"]


def test_monthly_rents_keep_their_home_type():
    grainger = "<p>from £1,050 pcm View all : Studio apartments from £1,500 View all : 1 bedroom apartments</p>"
    assert [(a, b) for a, b, _ in monthly_rents(grainger)] == [("Studio", 1050.0), ("1 bed", 1500.0)]
    uncle = "<p>£1,750 per month for a one bedroom, £2,450 per month for a two bedroom.</p>"
    assert [(a, b) for a, b, _ in monthly_rents(uncle)] == [("1 bed", 1750.0), ("2 bed", 2450.0)]
    listed = "<li>Studio from £900 pcm</li><li>1 bed from £1,100 pcm</li>"
    assert [(a, b) for a, b, _ in monthly_rents(listed)] == [("Studio", 900.0), ("1 bed", 1100.0)]


def test_charges_that_are_not_rent_are_ignored():
    assert monthly_rents("<p>Rooms from £185 per week.</p>") == []
    assert monthly_rents("<p>Studio: £500 per month 2-Bedroom: £600 per month Memberships are optional</p>") == []
    assert monthly_rents("<p>Parking space £550 per month</p>") == []
    assert [(a, b) for a, b, _ in monthly_rents("<p>1 bed from £1,200 pcm. Deposit £1,384.</p>")] == [("1 bed", 1200.0)]
    assert monthly_rents('<script>{"maxPcm":9863,"id":9862}</script>') == []     # script data is not read


def test_a_scheme_is_found_by_its_own_words_not_its_operators_brand():
    assert match_words("UNCLE Manchester", "UNCLE") == {"manchester"}
    assert match_words("Clipper's Quay", "GRAINGER REAL ESTATE LIMITED") == {"clipper", "quay"} or \
        match_words("Clipper's Quay", "GRAINGER REAL ESTATE LIMITED") == {"clippers", "quay"}
    urls = {"https://uncle.co.uk/manchester/", "https://uncle.co.uk/wembley/uncle-wembley-facts/",
            "https://uncle.co.uk/news/manchester-launch/"}
    assert candidates({"manchester"}, urls) == ["https://uncle.co.uk/manchester/"]      # news pages never count
    ok, _ = names_scheme("<h1>The Headline</h1><p>Leeds LS1 4ND</p>", {"headline"}, "LS1 4ND", "Leeds")
    assert ok
    ok, _ = names_scheme("<h1>The Headline</h1><p>Somewhere else</p>", {"headline"}, "LS1 4ND", "Leeds")
    assert not ok                                        # a one-word name needs the postcode or the town too


def test_opening_years():
    assert opening_year("<p>The building opened in September 2019 and is home to 300 students.</p>", 2026)[0] == 2019
    assert opening_year("<p>Completed in 2021.</p>", 2026)[0] == 2021
    assert opening_year("<p>Block A opened in 2015. Block B opened in 2019.</p>", 2026) is None    # two buildings
    assert opening_year("<p>Founded 1998, a family business.</p>", 2026) is None
    # With the scheme's name given, a statement about a neighbour or an office does not count.
    blurb = "<p>Available Rooms Newton Court - Portland Green Student Village Opened in 2018, students flock to it.</p>"
    assert opening_year(blurb, 2026, {"newton", "court", "portland", "green", "village"})[0] == 2018
    assert opening_year(blurb, 2026, {"marshall", "court", "portland", "green", "village"}) is None
    office = "<p>One Embassy Gardens, a 215,000 sq ft office building completed in 2019, was designed by a studio.</p>"
    assert opening_year(office, 2026, {"embassy", "gardens"}) is None
    assert opening_year("<p>Lower Riverside Completed in 2016.</p>", 2026, {"upper", "riverside"}) is None
    assert opening_year("<p>Welcome to Haüs, opening in September 2026.</p>", 2026, {"haus"})[0] == 2026
    assert planning_year("PA/14/02134") == 2014
    assert planning_year("2016/1234/FUL") == 2016
    assert planning_year("17/01234/FUL") == 2017
    assert completion_year("01/2020") == 2020 and completion_year("2027") == 2027 and completion_year("") is None


def test_certificates_date_a_scheme_only_when_they_are_plausibly_its_own():
    scheme = {"name": "Brook Place", "postcode": "S11 8BR", "size": 237}
    lodged = [{"transaction-type": "new dwelling", "lodgement-date": "2019-06-01", "address1": f"Flat {i} Brook Place"}
              for i in range(200)]
    got = judge_certificates(scheme, lodged, 2026)
    assert got and got["year"] == 2019 and got["confidence"] >= 0.75
    # A handful of certificates at a big scheme's postcode is someone else's building.
    few = [{"transaction-type": "new dwelling", "lodgement-date": "2012-03-01", "address1": f"{i} High Street"}
           for i in range(5)]
    assert judge_certificates(scheme, few, 2026) is None
    # Re-lets and sales over many years are not a completion.
    resale = [{"transaction-type": "rental", "lodgement-date": f"20{10 + i % 12}-01-01", "address1": "x"} for i in range(60)]
    assert judge_certificates(scheme, resale, 2026) is None


def test_register_matching_helpers():
    assert city_key("Search Our Developments in Southampton") == "southampton"
    assert city_key("Newcastle upon Tyne City Council") == "newcastle tyne"
    assert city_key("London Borough of Camden") == "london"
    assert brand_tokens("Unite Students Limited") == {"unite"}


def test_a_building_we_hold_under_another_spelling_is_not_added_again():
    from scrape_national_code import distinct_words, joined_name, loose_words, scheme_city_keys, street_parts
    # One name written two ways.
    assert joined_name("The Leather Works") == joined_name("The Leatherworks") == "leatherworks"
    assert joined_name("Chapter Kings Cross", {"chapter"}) == joined_name("Chapter KingsCross", {"chapter"})
    # "Canvas Old Fire Station" against our "The Old Fire Station": the strict list sees one short
    # word in common, the loose list sees three.
    assert len(distinct_words("Canvas Old Fire Station") & distinct_words("The Old Fire Station")) < 2
    assert loose_words("Canvas Old Fire Station") & loose_words("The Old Fire Station") == {"old", "fire", "station"}
    # Words nearly every student building shares never make a match.
    assert not (loose_words("Uni House Court") & loose_words("Calthorpe Court"))
    # A town inside a larger council area is read from the address.
    assert "egham" in scheme_city_keys("Runnymede", "98 Harvest Road, Egham", "The Garage", frozenset({"egham"}))
    assert "london" in scheme_city_keys("Tower Hamlets", None)
    # Same house number on the same road.
    words_a, numbers_a = street_parts("BROOK, 536 Bristol Road")
    words_b, numbers_b = street_parts("536 Bristol Rd, Birmingham")
    assert numbers_a & numbers_b == {"536"} and "bristol" in words_a & words_b


def test_weekly_student_rents_and_what_is_not_a_rent():
    from read_scheme_pages import weekly_rents
    listed = "<li>En-suite from £165 per week</li><li>Studio from £219 pw</li><li>Non en-suite £120 pppw</li>"
    assert [(a, b) for a, b, _ in weekly_rents(listed)] == [("Ensuite", 165.0), ("Studio", 219.0), ("Non en-suite", 120.0)]
    after = "<p>£189 per week for a studio. £155 per week for an en suite.</p>"
    assert [(a, b) for a, b, _ in weekly_rents(after)] == [("Studio", 189.0), ("Ensuite", 155.0)]
    # A discount is not a rent, but a rent that follows a sentence about a deposit still is.
    assert [(a, b) for a, b, _ in weekly_rents("<p>Save £80 per week on studios. Rooms from £175 per week.</p>")] == [(None, 175.0)]
    assert [(a, b) for a, b, _ in weekly_rents("<p>Deposit £250. From £99 pw. Parking £15 per week</p>")] == [(None, 99.0)]
    assert weekly_rents("<p>Apartments from £1,250 pcm</p>") == []          # monthly prices are not weekly ones
    # "£198 per week (en suite room)": the room type follows its price in brackets.
    bracketed = "<p>Price: £198.38 per week (single occupancy en suite room); £223.23 per week (single occupancy studio room)</p>"
    assert [(a, b) for a, b, _ in weekly_rents(bracketed)] == [("Ensuite", 198.38), ("Studio", 223.23)]


def test_a_neighbouring_buildings_price_is_not_taken_for_the_schemes_own():
    from read_scheme_pages import other_house_number, price_keys, weekly_rents
    read = lambda html, words, name, skip=None: [(a, b) for a, b, _ in weekly_rents(html, words, name, skip)]  # noqa: E731
    # A menu lists every building of the operator with its price. Only the price said of this scheme counts.
    menu = ("<nav>Arkwright House From £75 per week <b>x</b> Moor Lane Halls From £82 per week <b>x</b> "
            "Unity Square From £133 per week <b>x</b> The Moor Studios From £132.05 per week</nav>")
    own = ("<h1>The Moor</h1><p>Contact us at The Moor. From £132.05 Per Week</p>"
           "<div>King Studio – The Moor Studios Private bathroom Starting From £141.55 per week</div>")
    values = {v for _, v in read(menu + own, ["moor"], "The Moor")}
    assert values == {132.05, 141.55}                    # not Moor Lane Halls' £82, nor Unity Square's £133
    # Cards for the operator's other buildings, with this scheme named just before them.
    cards = ("<h1>Lilian Knowles House</h1><p>We'd love to have you at Lilian Knowles House.</p>"
             "<div>Manna Ash House 8-16 Pocock Street, London From £340 per week</div>"
             "<div>Don Gratton House From £308 per week</div>")
    assert read(cards, ["lilian", "knowles"], "Lilian Knowles House") == []
    # Several "from" prices and none said of the scheme: the page cannot be read safely.
    assert read("<p>Arkwright House From £75 per week</p><p>Truro Works From £99 per week</p>", ["moor"], "The Moor") == []
    # A price list under the scheme's name is its own, every line of it.
    listed = "<h2>The Gas Works</h2> Classic Studio From £ 195 pw Premier Studio From £ 200 pw Premier Studio Plus From £ 220 pw"
    assert read(listed, ["gas", "works"], "The Gas Works") == [("Studio", 195.0), ("Studio", 200.0), ("Studio", 220.0)]
    # A price card that stands on a sister building's page too is not this building's.
    card = "<p>Large shared kitchen All-inclusive bills £145 Per Week</p>"
    assert read(card + "<p>Our studios start from £172 per week.</p>", ["regent"], "Regent Road", price_keys(card)) == [("Studio", 172.0)]
    # One page listing the rooms of several buildings: only the prices under this scheme's name are its own.
    several = ("<h3>Forest Rise: Studios</h3><p>All bills included £300 deposit £180-£190 Per Week</p>"
               "<h3>Kingfisher Halls: Large Studios</h3><p>Spacious shared kitchen £300 deposit £177 Per Week</p>")
    assert {v for _, v in read(several, ["forest", "rise"], "Forest Rise")} == {180.0, 190.0}
    # A room word that is part of the scheme's name is not the room's type; "pp/pw" is a weekly unit.
    tudor = "<p>Classic En-Suite Tudor Studios from £110 pp/pw</p><p>Deluxe Studio Tudor Studios from £140 pp/pw</p>"
    assert read(tudor, ["tudor"], "Tudor Studios") == [(None, 110.0), (None, 140.0)]
    # The page's structured description of itself.
    assert read('<script type="application/ld+json">{"priceRange":"From £165PW"}</script><h1>Albert Place</h1>',
                ["albert"], "Albert Place") == [(None, 165.0)]
    # A page for number 134 is not the building at 138 to 142.
    assert other_house_number("https://x.co.uk/134-north-sherwood-street/", "138 - 142 North Sherwood St, Nottingham")
    assert not other_house_number("https://x.co.uk/properties/22-upperton-road", "22 Upperton Road, Leicester")
    assert not other_house_number("https://x.co.uk/property/1-bedroom-studio-4-old-silk-yard/", "57-59 Lower Ford Street")


def test_a_researched_figure_is_kept_only_when_the_cited_page_prints_it():
    from load_scheme_research import figure_re, price_is_printed, year_is_stated
    assert figure_re(165.0).search("from £165 per week") and figure_re(165.0).search("£165.00 pw")
    assert not figure_re(165.0).search("£1650 a month") and not figure_re(165.0).search("£165.50 per week")
    kiln = {"name": "Kiln House", "operator": "Brink Riley", "address": "Leek Road, Stoke"}
    own = "<title>Kiln House | student rooms</title><h1>Kiln House</h1><p>Studios and rooms available from £125 pppw</p>"
    assert price_is_printed(own, kiln, 125.0)[0] is True
    assert price_is_printed(own, kiln, 135.0)[0] is False                       # not on the page at all
    monthly = "<title>Kiln House</title><h1>Kiln House</h1><p>Rooms from £125 per month</p>"
    assert price_is_printed(monthly, kiln, 125.0) == (False, "figure is on the page but not as a weekly price")
    # A fees table for every hall: the figure counts only in the scheme's own row.
    table = ("<h1>Residence fees 2026/27</h1><table><tr><th>Residence</th><th>Weekly rent</th></tr>"
             "<tr><td>Kilvey</td><td>£120.00</td></tr><tr><td>Preseli House</td><td>£131.00</td></tr></table>")
    kilvey = {"name": "Kilvey", "operator": "UPP Swansea", "address": "Singleton Park, Swansea"}
    assert price_is_printed(table, kilvey, 120.0) == (True, "printed right after the scheme's name")
    assert price_is_printed(table, kilvey, 131.0)[0] is False                    # that is Preseli House's row
    # An opening year: stated of the scheme, on the cited page.
    moor = {"name": "Crown Place", "operator": "Homes For Students", "address": "Hope Street"}
    news = "<h1>New halls open</h1><p>Crown Place opened in September 2014 and is home to 1,259 students.</p>"
    assert year_is_stated(news, moor, 2014, None, 2026)[0] is True
    assert year_is_stated(news, moor, 2016, "Crown Place opened in September 2016", 2026)[0] is False
    refit = "<h1>Crown Place</h1><p>Crown Place was refurbished throughout in 2019 after its sale.</p>"
    assert year_is_stated(refit, moor, 2019, "Crown Place was refurbished throughout in 2019 after its sale.", 2026)[0] is False
    old = "<h1>Halls history</h1><p>Wessex Lane Hall first welcomed students when it opened in 1965.</p>"
    wessex = {"name": "Wessex Lane Hall", "operator": "University of Southampton", "address": "Wessex Lane"}
    assert year_is_stated(old, wessex, 1965, "Wessex Lane Hall first welcomed students when it opened in 1965.", 2026)[0] is True


def test_room_prices_are_read_from_a_listing_page():
    import json as _json
    from afs_fill_rents import room_tiers, tidy_postcode
    details = {"rooms": {"years": {"currentMarketingYear": {"label": "2026-27", "roomTypes": [
        {"roomType": "Entire Place", "available": [
            {"roomName": "Classic", "roomType": "Entire Place", "isSoldOut": False, "floors": [{"contracts": [
                {"weeks": 51, "amount": 279}, {"weeks": 44, "amount": 306.9}]}]},
            {"roomName": "Deluxe", "roomType": "Entire Place", "isSoldOut": True, "floors": [{"contracts": [
                {"weeks": 51, "amount": 475}]}]},
            {"roomName": "Empty", "roomType": "Entire Place", "floors": [{"contracts": []}]}]}]}}}}
    html = ('<script id="__NEXT_DATA__" type="application/json">'
            + _json.dumps({"props": {"pageProps": {"viewModel": {"propertyDetails": details}}}}) + "</script>")
    year, tiers = room_tiers(html)
    assert year == "2026/27"
    assert [(t["name"], t["rent_ppw"], t["weeks"]) for t in tiers] == [("Classic", 279.0, 51), ("Deluxe", 475.0, 51)]
    assert room_tiers("<html>no data</html>") == (None, [])
    assert tidy_postcode("b29  7es") == "B29 7ES" and tidy_postcode("eh16 6fu") == "EH16 6FU" and tidy_postcode("B29") is None


def test_a_postcode_dates_a_scheme_only_when_it_can():
    from date_schemes_from_postcode import judge as dated
    assert dated("201603", None, 1, 320, 2026) == (2016, "postcode introduced")
    assert dated("199308", None, 1, 320, 2026)[0] is None        # an old postcode says nothing about the building
    assert dated("201603", None, 2, 320, 2026)[0] is None        # two schemes on one postcode: which one?
    assert dated("201603", None, 1, 30, 2026)[0] is None         # a small house conversion is not a new block
    assert dated("201603", "202001", 1, 320, 2026)[0] is None    # postcode withdrawn
    assert dated(None, None, 1, 320, 2026)[0] is None


def test_a_geocoder_answer_is_taken_only_in_the_right_town_and_never_for_a_whole_town():
    from geocode_scheme_addresses import judge as place
    building = {"class": "building", "type": "dormitory", "display_name": "King Street Exchange, 600, King Street, Aberdeen",
                "address": {"country_code": "gb", "city": "Aberdeen City", "house_number": "600", "postcode": "AB24 5SX"}}
    assert place(building, "Aberdeen", True)[0] == "building"
    street = {"class": "highway", "type": "residential", "display_name": "Willowbank Road, Aberdeen",
              "address": {"country_code": "gb", "city": "Aberdeen City"}}
    assert place(street, "Aberdeen", True)[0] == "street"
    assert place(street, "Dundee", True)[0] is None            # right street name, wrong town
    town = {"class": "place", "type": "city", "display_name": "Aberdeen", "address": {"country_code": "gb", "city": "Aberdeen"}}
    assert place(town, "Aberdeen", False)[0] is None           # a town is not a position for a building
    abroad = {"class": "building", "type": "yes", "display_name": "King Street, Aberdeen, South Dakota",
              "address": {"country_code": "us", "city": "Aberdeen"}}
    assert place(abroad, "Aberdeen", True)[0] is None


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
