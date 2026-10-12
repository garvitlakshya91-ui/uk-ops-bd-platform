"""Tests for the publish gates (pure functions over a report context, no DB)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from report.gates import run_gates  # noqa: E402


def ctx(**evidence):
    base = {"packs": [], "shared_postcodes": [], "pipeline_contradictions": [], "stale_pipeline": [], "rows": []}
    base.update(evidence)
    return {"evidence": base, "publication": True}


def gates_of(failures):
    return sorted({f.gate for f in failures})


def test_clean_context_passes():
    assert run_gates(ctx()) == []


def test_benchmark_total_within_five_percent_passes():
    c = ctx(packs=[{"label": "Pack", "our_total": 20246, "total_delta_pct": 4.9, "mismatches": [], "missing_from_ours": []}])
    assert run_gates(c) == []


def test_benchmark_total_over_five_percent_fails_with_detail():
    c = ctx(packs=[{"label": "Pack", "our_total": 21000, "total_delta_pct": 12.6,
                    "mismatches": [{"name": "Crown Place", "ours": 814, "delta_pct": 46.4}],
                    "missing_from_ours": [{"property": "The Maltings", "postcode": "B5 7XX"}]}])
    f = run_gates(c)
    assert gates_of(f) == ["benchmark_total"]
    assert any("Crown Place" in d for d in f[0].detail) and any("Maltings" in d for d in f[0].detail)


def test_benchmark_without_total_is_a_failure_not_a_pass():
    c = ctx(packs=[{"label": "Pack", "our_total": 0, "total_delta_pct": None, "mismatches": [], "missing_from_ours": []}])
    assert gates_of(run_gates(c)) == ["benchmark_total"]


def test_shared_postcode_fails_unless_confirmed():
    c = ctx(shared_postcodes=[{"postcode": "B4 7UP", "schemes": ["James Watt", "Lakeside"], "confirmed": False}])
    assert gates_of(run_gates(c)) == ["shared_postcode"]
    c = ctx(shared_postcodes=[{"postcode": "B4 7UP", "schemes": ["James Watt", "Lakeside"], "confirmed": True}])
    assert run_gates(c) == []


def test_beds_vs_description_pipeline_and_census():
    c = ctx(pipeline_contradictions=[{"reference": "2023/02925/PA", "address": "x", "stored": 759, "basis": "stated",
                                      "extracted": None, "quote": "no figure in the description"}],
            rows=[{"id": 1, "name": "Haus", "beds": 200, "issues": ["stored count differs from its quoted phrase"],
                   "age_days": 1, "verification": "probable"}])
    f = run_gates(c)
    assert gates_of(f) == ["beds_vs_description"] and len(f) == 2


def test_stale_pipeline_status_fails():
    c = ctx(stale_pipeline=[{"reference": "2019/01/PA", "status": "consented_full", "status_at": "2024-01-01", "beds": 300}])
    assert gates_of(run_gates(c)) == ["stale_status"]


def test_unverified_census_only_blocks_publication_builds():
    rows = [{"id": 1, "name": "A", "beds": 100, "issues": [], "age_days": None, "verification": "probable"}]
    assert gates_of(run_gates(ctx(rows=rows))) == ["stale_status"]
    internal = ctx(rows=rows)
    internal["publication"] = False
    assert run_gates(internal) == []


def test_failure_renders_readably():
    f = run_gates(ctx(shared_postcodes=[{"postcode": "B1", "schemes": ["A", "B"], "confirmed": False}]))[0]
    text = str(f)
    assert text.startswith("[shared_postcode]") and "B1: A / B" in text


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
