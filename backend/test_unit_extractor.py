"""Tests for the unified unit extractor (pure functions, no DB)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.extraction.units import extract_units  # noqa: E402


def kinds(text, kind):
    return sorted((f.value, f.kind, f.is_maximum, f.alternative) for f in extract_units(text).of_kind(kind))


# ---------------------------------------------------------------- student beds
def test_plain_bedspaces():
    ex = extract_units("Erection of purpose built student accommodation comprising 540 bedspaces")
    assert ex.beds == 540 and ex.best("beds").confidence >= 0.9 and ex.beds_max is None


def test_hyphen_comma_and_student_context():
    ex = extract_units("Demolition and construction of a 1,544-bed PBSA scheme")
    assert ex.beds == 1544


def test_total_outranks_components():
    ex = extract_units("PBSA for a total of 251 bedspaces, comprising 27 cluster flats (191 bed spaces) "
                       "and 60 no. 1-bed studio flats in a building comprising 10 storeys")
    assert ex.beds == 251 and ex.storeys == 10
    assert ex.best("cluster_flats").value == 27 and ex.best("studios").value == 60


def test_largest_when_equal_confidence():
    ex = extract_units("PBSA development of 814 beds arranged as 115 studios and cluster flats of 699 bedrooms")
    assert ex.beds == 814


def test_studios_fallback_is_derived():
    ex = extract_units("Student accommodation of 150 studios")
    f = ex.best("beds")
    assert f.value == 150 and f.method == "from_studios" and f.confidence < 0.7


def test_unit_style_student_counts():
    assert extract_units("523 units of purpose built student accommodation").beds == 523
    assert extract_units("950 student accommodation apartments").beds == 950
    assert extract_units("change of use to 180 student rooms").beds == 180


def test_no_spaces_variant():
    assert extract_units("student accommodation (236no. bedspaces)").beds == 236


def test_up_to_is_an_envelope_not_a_count():
    ex = extract_units("Demolition and erection of up to 15 storey building for Purpose Built Student "
                       "Accommodation to accommodate up to 604 bedspaces")
    assert ex.beds is None and ex.beds_max == 604
    assert ex.storeys is None and ex.maximum("storeys").value == 15


def test_variation_takes_the_new_total():
    ex = extract_units("Variation to increase the scheme from 540 to 604 student bedspaces")
    f = ex.best("beds")
    assert f.value == 604 and f.variation_from == 540 and f.method == "variation"


def test_unit_size_is_not_a_count():
    ex = extract_units("co-living tower comprising 428 x units (1 x 4-bed cluster, 37 x 5-bed clusters, "
                       "1 x 10-bed cluster) and 20 x studios")
    assert ex.beds is None
    assert ex.best("coliving_rooms").value == 428
    assert all(f.value != 10 for f in ex.figures)


# ---------------------------------------------------------------- other asset types
def test_hotel_care_and_hmo_are_not_student_beds():
    assert extract_units("Mixed scheme: 200 bed hotel and commercial space").beds is None
    assert kinds("Mixed scheme: 200 bed hotel and commercial space", "hotel_rooms")[0][0] == 200
    assert extract_units("Change of use to a care home (C2) of 60 beds").beds is None
    assert kinds("Change of use to a care home (C2) of 60 beds", "care_beds")[0][0] == 60
    assert extract_units("Change of use to a 12 bed HMO").beds is None


def test_coliving_rooms():
    ex = extract_units("Erection of a 5 storey building to provide 159 rooms within co-living accommodation (Sui Generis)")
    assert ex.best("coliving_rooms").value == 159 and ex.storeys == 5 and ex.beds is None


def test_homes_and_envelope():
    ex = extract_units("Outline application for up to 1,850 units of residential accommodation (Use Class C3)")
    assert ex.homes is None and ex.homes_max == 1850
    ex = extract_units("Demolition of existing house and erection of four attached dwellings")
    assert ex.homes == 4
    assert extract_units("136 residential units (Use Class C3) over 7 buildings").homes == 136


def test_homes_components_sum_when_no_total():
    ex = extract_units("private residential accommodation comprising of 39 x 1 bedroom apartments and "
                       "32 x 2 bedroom apartments")
    f = ex.best("homes")
    assert f.value == 71 and f.method == "sum"


def test_explicit_total_beats_component_sum():
    ex = extract_units("76 residential units (mix of 1, 2 and 3 bed flats) comprising 39 x 1 bed flats and 37 x 2 bed flats")
    assert ex.homes == 76


# ---------------------------------------------------------------- storeys
def test_part_storeys_take_the_tallest():
    ex = extract_units("erection of part 5, part 7, part 9 storey purpose built student accommodation block "
                       "containing 88 self contained studio apartments")
    assert ex.storeys == 9 and ex.beds == 88


def test_storey_range_and_ground_plus():
    assert extract_units("buildings ranging from 3 to 25 storeys in height").storeys == 25
    ex = extract_units("erection of a 21-storey building (Ground + 20 storeys) and 8 storey shoulder building")
    assert ex.storeys == 21
    assert extract_units("a new building comprising ground and eight upper floors").storeys == 9
    assert extract_units("Single storey extension to student accommodation building").storeys == 1
    assert extract_units("two buildings up to four and a half storeys in height").maximum("storeys").value == 4


# ---------------------------------------------------------------- either/or
def test_either_or_flags_the_permission_and_its_figures():
    ex = extract_units("erection of four buildings for purpose built student and/or co-living accommodation "
                       "(Sui Generis) (comprising 268 no studios and 87 cluster apartments, providing a total of 864 beds)")
    assert ex.either_or
    f = ex.best("beds")
    assert f.value == 864 and f.alternative and f.confidence < 0.9


def test_either_or_needs_a_use_not_a_side():
    ex = extract_units("Construction of single storey rear kitchen extension with raised terrace, "
                       "timber privacy screen to either side of terrace")
    assert not ex.either_or


def test_temporary_either_or_rooms_envelope():
    ex = extract_units("Temporary use of up to 100 rooms over D and E Block for either student accommodation "
                       "or temporary accommodation for staff")
    assert ex.either_or and ex.beds is None and ex.maximum("beds").value == 100


# ---------------------------------------------------------------- page phrasing
def test_marketing_round_ups_are_low_confidence():
    ex = extract_units("perfectly located in leafy Edgbaston and housing over 200 students")
    f = ex.best("beds")
    assert f.value == 200 and f.is_approximate and f.confidence < 0.6


def test_page_states_study_bedrooms():
    ex = extract_units("The Maltings houses 827 study bedrooms, 809 of which are en suite")
    assert ex.beds == 827


# ---------------------------------------------------------------- increments
def test_additional_beds_are_flagged_as_increments():
    ex = extract_units("Extension to the existing student accommodation to provide an additional 53 student bed spaces")
    f = ex.best("beds")
    assert f.value == 53 and f.is_additional
    ex = extract_units("Erection of three storey rooftop extension to provide 61 no. additional student bedrooms")
    assert ex.beds == 61 and ex.best("beds").is_additional and ex.storeys == 3


def test_student_housing_units():
    assert extract_units("erection of a 3 storey building with 38 student housing units").beds == 38


# ---------------------------------------------------------------- noise
def test_ignores_small_huge_and_none():
    assert extract_units("extension providing 4 bedrooms").beds is None
    assert extract_units("masterplan for 9,000 beds across the city").beds is None
    assert extract_units(None).beds is None
    assert extract_units("Change of use to offices").figures == []


def test_years_are_not_counts():
    assert extract_units("Book your room for 2026/27 bedrooms from £120 a week").beds is None
    assert extract_units("Rooms available from September 2026 beds going fast").beds is None
    assert extract_units("Opening in 2026, the building offers 540 bedspaces").beds == 540


def test_room_noise_words_are_skipped():
    ex = extract_units("student accommodation with 12 study rooms, 3 plant rooms and 540 bedspaces")
    assert ex.beds == 540 and all(f.value not in (12, 3) for f in ex.figures)


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok  {name}")
            except Exception as exc:  # a crash is a failure too
                failures += 1
                print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
    raise SystemExit(failures)
