"""Ask Claude to propose registry matches a person then confirms.

Two queues feed it: feed records nothing matched (scheme_source_records
with status "unmatched") and PBSA rows outside the census
(verification_status "unverified"), which may duplicate a census scheme
under another name or be supply the census lacks. For each, the model
sees the record and the census schemes in the same postcode district
and answers with one candidate or none, a confidence and a reason. The
answer is written as a proposal; nothing is linked until
verify_scheme.py --record <id> --link / --duplicate-of is run.

Requires ANTHROPIC_API_KEY.

Usage:
    python propose_matches.py --city Birmingham [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from pydantic import BaseModel, Field

from app.database import SessionLocal
from app.models.models import Company, Council, ExistingScheme, SchemeSourceRecord
from app.scrapers.scheme_matching import name_score, norm_name, norm_pc

# Matching is judgement over a handful of records, not bulk extraction.
MODEL = "claude-opus-5-5"
PROMPT = """You reconcile UK student-accommodation records. Decide whether the RECORD below describes
one of the CANDIDATE schemes (same building, possibly renamed or rebranded) or none of them.
Use names, postcodes, addresses, operators and bed counts. A different building at the same
postcode is NOT a match. Answer with the candidate id or null, a 0-1 confidence and one
sentence of reasoning that cites the evidence.
"""


class Proposal(BaseModel):
    candidate_id: Optional[int] = Field(description="The matching candidate's id, or null")
    confidence: float = Field(description="0-1 belief in the match")
    reason: str = Field(description="One sentence citing the evidence")


def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None, max_retries=3)


def candidates_for(census: list[ExistingScheme], ops: dict, name: str | None, postcode: str | None,
                   exclude_id: Optional[int] = None) -> list[ExistingScheme]:
    pc = norm_pc(postcode)
    tokens = norm_name(name or "")
    scored = []
    for s in census:
        if s.id == exclude_id:
            continue
        spc = norm_pc(s.postcode)
        score = name_score(tokens, norm_name(s.name))
        if pc and spc and pc[:-3] == spc[:-3]:
            score += 0.3
        if pc and pc == spc:
            score += 0.5
        scored.append((score, s))
    scored.sort(key=lambda t: -t[0])
    return [s for sc, s in scored[:8] if sc > 0] or [s for _, s in scored[:5]]


def describe(s: ExistingScheme, ops: dict) -> str:
    op = ops.get(s.operator_company_id)
    return (f"id={s.id} name={s.name!r} postcode={s.postcode} address={(s.address or '')[:60]!r} "
            f"operator={op.name if op else None!r} beds={s.beds_total or s.num_units} status={s.operating_status}")


def ask(client, record: str, cands: list[ExistingScheme], ops: dict) -> Optional[Proposal]:
    body = PROMPT + f"\nRECORD: {record}\n\nCANDIDATES:\n" + "\n".join(describe(c, ops) for c in cands)
    resp = client.messages.parse(
        model=MODEL, max_tokens=800,
        messages=[{"role": "user", "content": body}],
        output_format=Proposal,
    )
    if resp.stop_reason == "refusal":
        return None
    return resp.parsed_output


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set")
    client = anthropic_client()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.city)).first()
    if council is None:
        raise SystemExit(f"Council {args.city!r} not found")
    schemes = (db.query(ExistingScheme)
               .filter(ExistingScheme.council_id == council.id, ExistingScheme.scheme_type == "PBSA",
                       ExistingScheme.merged_into_id.is_(None)).all())
    ops = {c.id: c for c in db.query(Company).filter(
        Company.id.in_([s.operator_company_id for s in schemes if s.operator_company_id] or [0]))}
    census = [s for s in schemes if s.operating_status]
    outwards = {norm_pc(s.postcode)[:-3] for s in schemes if s.postcode}
    slug = council.name.lower().replace(" ", "-")

    print(f"== feed records nothing matched")
    recs = [r for r in db.query(SchemeSourceRecord).filter(SchemeSourceRecord.status == "unmatched")
            if (r.raw_postcode and norm_pc(r.raw_postcode)[:-3] in outwards)
            or slug in (r.source_reference or "").lower()]
    for r in recs:
        cands = candidates_for(census, ops, r.raw_name, r.raw_postcode)
        record = (f"source={r.source} name={r.raw_name!r} postcode={r.raw_postcode} "
                  f"operator={r.raw_operator!r} address={(r.raw_address or '')[:60]!r} ref={r.source_reference[:80]}")
        p = ask(client, record, cands, ops)
        if p is None:
            print(f"  rec {r.id}: model declined")
            continue
        target = p.candidate_id if p.candidate_id in {c.id for c in cands} else None
        print(f"  rec {r.id:<6} {str(r.raw_name)[:30]:30s} -> {('#%d' % target) if target else 'no match':10s} "
              f"{p.confidence:.2f}  {p.reason[:90]}")
        if not args.dry_run:
            r.proposed_scheme_id, r.proposal_confidence, r.proposal_reason = target, p.confidence, p.reason
            r.status = "proposed" if target else "unmatched"

    print(f"\n== PBSA rows outside the census")
    for s in schemes:
        if s.operating_status or s.verification_status != "unverified":
            continue
        cands = candidates_for(census, ops, s.name, s.postcode, exclude_id=s.id)
        p = ask(client, describe(s, ops) + f" source={s.source}", cands, ops)
        if p is None:
            print(f"  #{s.id}: model declined")
            continue
        target = p.candidate_id if p.candidate_id in {c.id for c in cands} else None
        verdict = f"duplicate of #{target}" if target else "not in the census: verify and admit, or discard"
        print(f"  #{s.id:<6} {s.name[:30]:30s} -> {verdict:44s} {p.confidence:.2f}  {p.reason[:80]}")
        if not args.dry_run:
            s.verification_note = (f"proposed {datetime.date.today()}: {verdict} ({p.confidence:.2f}); {p.reason}")[:1000]
    if args.dry_run:
        db.rollback()
        print("\ndry run: nothing written")
    else:
        db.commit()
        print("\nproposals written; confirm with verify_scheme.py --record <id> --link <scheme> "
              "or --id <scheme> --duplicate-of <scheme>")
    db.close()


if __name__ == "__main__":
    main()
