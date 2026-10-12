"""Publish gates: the checks a report must pass before it may be built.

A publication build refuses when any gate fails; an internal build
prints the failures and renders them in the evidence annex. Gates work
on the assembled report context so they can be tested without a
database.

    failures = run_gates(context)
    if failures and publication: refuse

Gates:
  benchmark_total     the census total differs from a benchmark pack's by
                      more than 5%
  shared_postcode     two census schemes share a postcode and nobody has
                      confirmed they are distinct buildings
  beds_vs_description a pipeline row's stored bed count is not what its
                      description states (or a census count contradicts
                      the phrase it was read from)
  stale_status        a live pipeline status, or a census scheme's
                      verification, is older than the allowed age
"""
from __future__ import annotations

import dataclasses
from typing import Callable

MAX_BENCHMARK_DELTA_PCT = 5.0
MAX_PIPELINE_STATUS_AGE_DAYS = 365
MAX_CENSUS_VERIFICATION_AGE_DAYS = 365


@dataclasses.dataclass
class GateFailure:
    gate: str
    message: str
    detail: list[str] = dataclasses.field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"[{self.gate}] {self.message}"]
        lines += [f"    - {d}" for d in self.detail[:12]]
        if len(self.detail) > 12:
            lines.append(f"    ... and {len(self.detail) - 12} more")
        return "\n".join(lines)


def gate_benchmark_total(ctx: dict) -> list[GateFailure]:
    out = []
    for p in (ctx.get("evidence") or {}).get("packs") or []:
        d = p.get("total_delta_pct")
        if d is None:
            out.append(GateFailure("benchmark_total", f"{p['label']}: no comparable total", []))
        elif abs(d) > MAX_BENCHMARK_DELTA_PCT:
            out.append(GateFailure(
                "benchmark_total",
                f"{p['label']}: census total {p['our_total']:,} beds is {d:+.1f}% from the benchmark "
                f"(limit ±{MAX_BENCHMARK_DELTA_PCT:.0f}%)",
                [f"{m['name']}: ours {m['ours']} {m['delta_pct']:+.0f}%" for m in p.get("mismatches") or []]
                + [f"missing from ours: {m['property']} ({m['postcode']})" for m in p.get("missing_from_ours") or []],
            ))
    return out


def gate_shared_postcode(ctx: dict) -> list[GateFailure]:
    groups = [g for g in (ctx.get("evidence") or {}).get("shared_postcodes") or [] if not g["confirmed"]]
    if not groups:
        return []
    return [GateFailure(
        "shared_postcode",
        f"{len(groups)} postcode(s) carry more than one census scheme without confirmation that they are distinct",
        [f"{g['postcode']}: " + " / ".join(g["schemes"]) for g in groups],
    )]


def gate_beds_vs_description(ctx: dict) -> list[GateFailure]:
    ev = ctx.get("evidence") or {}
    out = []
    pipe = ev.get("pipeline_contradictions") or []
    if pipe:
        out.append(GateFailure(
            "beds_vs_description",
            f"{len(pipe)} pipeline row(s) carry a bed count their description does not state",
            [f"{c['reference']}: stored {c['stored']} ({c['basis']}), text says "
             f"{c['extracted'] if c['extracted'] else '-'}: \"{str(c['quote'])[:60]}\"" for c in pipe],
        ))
    census = [r for r in ev.get("rows") or [] if any("quoted phrase" in i for i in r["issues"])]
    if census:
        out.append(GateFailure(
            "beds_vs_description",
            f"{len(census)} census scheme(s) hold a bed count that contradicts the phrase it was read from",
            [f"#{r['id']} {r['name']}: {r['beds']}" for r in census],
        ))
    return out


def gate_stale_status(ctx: dict) -> list[GateFailure]:
    ev = ctx.get("evidence") or {}
    out = []
    stale = ev.get("stale_pipeline") or []
    if stale:
        out.append(GateFailure(
            "stale_status",
            f"{len(stale)} live pipeline row(s) have a status older than {MAX_PIPELINE_STATUS_AGE_DAYS} days or undated",
            [f"{s['reference']}: {s['status']} as of {s['status_at'] or 'unknown'} ({s['beds'] or '-'} beds)"
             for s in stale],
        ))
    old = [r for r in ev.get("rows") or []
           if r.get("age_days") is None or r["age_days"] > MAX_CENSUS_VERIFICATION_AGE_DAYS]
    if old and ctx.get("publication"):
        out.append(GateFailure(
            "stale_status",
            f"{len(old)} census scheme(s) have no verification within {MAX_CENSUS_VERIFICATION_AGE_DAYS} days",
            [f"#{r['id']} {r['name']}: {r['verification']} "
             f"({'never' if r['age_days'] is None else str(r['age_days']) + ' days ago'})" for r in old],
        ))
    return out


GATES: list[Callable[[dict], list[GateFailure]]] = [
    gate_benchmark_total, gate_shared_postcode, gate_beds_vs_description, gate_stale_status,
]


def run_gates(ctx: dict) -> list[GateFailure]:
    failures: list[GateFailure] = []
    for gate in GATES:
        failures.extend(gate(ctx))
    return failures
