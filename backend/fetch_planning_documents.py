"""Read bed counts from planning documents when the description states none.

"Erection of a purpose-built student accommodation building" says nothing
about size; the Design and Access Statement does. For each live or
pending PBSA application without a bed count this fetches the council's
document list, downloads the few documents most likely to state the
quantum (D&A statement, planning statement, application form, decision
notice), reads their text layer and runs the unit extractor page by
page. Every figure lands in unit_extractions with its quote and page;
the application's cached bed count is set only when the reading clears
the cache bar. ``--ai`` sends the capacity pages of documents the regex
could not read to Claude, under the same quote-verification rule.

Supported portals: NEC (Birmingham and the other councils whose pages
live under /NECSWS/). Other portal types are reported and skipped.
Downloads are kept under data/planning_documents/<council>/<reference>/
and are never re-fetched.

Usage:
    python fetch_planning_documents.py --council Birmingham [--limit 20] [--max-docs 3] [--ai] [--dry-run]
    python fetch_planning_documents.py --council Birmingham --refs 2024/06153/PA 2023/08290/PA
"""
from __future__ import annotations

import argparse
import html as htmllib
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import or_

from app.database import SessionLocal
from app.extraction.documents import DocumentReading, read_document, title_rank
from app.extraction.units import UnitFigure, as_rows
from app.models.models import Council, PlanningApplication, UnitExtractionRecord
from app.models.observations import CACHE_MIN_CONFIDENCE

LIVE = ("under_construction", "pre_letting", "active_consent", "consented_full", "consented_outline", "submitted")
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      "Accept": "*/*", "Accept-Language": "en-GB,en;q=0.9"}
DATA = Path(__file__).parent / "data" / "planning_documents"
REQUEST_GAP = 1.5
MAX_PDF_BYTES = 30 * 1024 * 1024
# Drawings never state the quantum in text; skip them unless nothing else exists.
DRAWING_RE = re.compile(r"drawing|elevation|section|floor\s*plan|site\s*plan|location\s*plan|\bGA\b|landscape", re.I)
NEC_DOC_RE = re.compile(
    r'href="(?P<href>[^"]*DisplaySearchDocument[^"]*)"[^>]*>\s*<u>(?P<title>.*?)</u>\s*</a>\s*</div>\s*'
    r'<div class="col-xs-3">(?P<info>.*?)</div>\s*<div class="col-xs-4">(?P<doctype>.*?)</div>', re.S)


def _sleep() -> None:
    time.sleep(REQUEST_GAP)


def council_page_url(client: httpx.Client, council: str, ref: str) -> Optional[str]:
    """The council's own page for an application, via PlanIt's record."""
    r = client.get(f"https://www.planit.org.uk/planapplic/{council}/{ref}/json")
    _sleep()
    if r.status_code != 200:
        return None
    return r.json().get("url")


def nec_documents(client: httpx.Client, page_url: str, ref: str) -> list[dict]:
    base = page_url.split("/NECSWS/")[0]
    client.get(page_url)   # establishes the portal session
    _sleep()
    r = client.get(f"{base}/NECSWS/ES/Presentation/Planning/OnlinePlanning/GetOnlineDocuments",
                   params={"applicationNumber": ref, "currentPageIndex": 0,
                           "IsDatePublishSortedDescending": "true", "pageSize": 200},
                   headers={"X-Requested-With": "XMLHttpRequest"})
    _sleep()
    if r.status_code != 200:
        return []
    docs = []
    for m in NEC_DOC_RE.finditer(r.text):
        href = htmllib.unescape(m.group("href"))
        ftype = re.search(r"fileType=([^&]+)", href)
        before = r.text[max(0, m.start() - 300):m.start()]
        date = re.search(r"(\d{1,2} \w+ \d{4})\s*$", re.sub(r"<[^>]+>|&nbsp;", " ", before).strip())
        docs.append({
            "title": htmllib.unescape(re.sub(r"\s+", " ", m.group("title"))).strip(),
            "doctype": htmllib.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group("doctype")))).strip(),
            "url": href if href.startswith("http") else base + href,
            "file_type": (ftype.group(1) if ftype else "").lower(),
            "date": date.group(1) if date else None,
        })
    return docs


def rank_documents(docs: list[dict]) -> list[dict]:
    pdfs = [d for d in docs if d["file_type"] == ".pdf"]
    def key(d):
        label = f"{d['title']} {d['doctype']}"
        return (title_rank(label), bool(DRAWING_RE.search(label)), d["title"])
    return sorted(pdfs, key=key)


def download(client: httpx.Client, doc: dict, folder: Path) -> Optional[Path]:
    guid = re.search(r"aspectGuid=([A-Za-z0-9]+)", doc["url"])
    name = (guid.group(1) if guid else re.sub(r"[^A-Za-z0-9]+", "-", doc["title"])[:60]) + ".pdf"
    path = folder / name
    if path.exists() and path.stat().st_size > 0:
        return path
    folder.mkdir(parents=True, exist_ok=True)
    try:
        with client.stream("GET", doc["url"], headers={**UA, "Accept": "application/pdf,*/*"}) as r:
            if r.status_code != 200 or "pdf" not in (r.headers.get("content-type") or ""):
                return None
            size = int(r.headers.get("content-length") or 0)
            if size > MAX_PDF_BYTES:
                return None
            with open(path, "wb") as fh:
                written = 0
                for chunk in r.iter_bytes():
                    written += len(chunk)
                    if written > MAX_PDF_BYTES:
                        fh.close()
                        path.unlink(missing_ok=True)
                        return None
                    fh.write(chunk)
    finally:
        _sleep()
    return path


def pick(reading: DocumentReading, outline: bool) -> Optional[UnitFigure]:
    committed, envelope = reading.best("beds"), reading.maximum("beds")
    return (envelope or committed) if outline else (committed or envelope)


def ai_reading(client_ai, reading: DocumentReading, text_pages: list[str]) -> Optional[dict]:
    """Ask the model to read the capacity pages the regex could not; verified against the text."""
    from ai_parse_pipeline_beds import MODEL, BedReading, score
    text = "\n\n".join(text_pages)[:14000]
    prompt = ("These are pages from a UK planning document (design and access or planning statement). "
              "Report the TOTAL student bed capacity the document states for the proposed development. "
              "Rules: beds is null unless the text states a total; is_maximum for 'up to'; either_or when "
              "alternative uses are offered; quote must be copied exactly from the text; confidence 0-1. "
              "Never infer a number the text does not support.\n\nText:\n")
    resp = client_ai.messages.parse(model=MODEL, max_tokens=600, output_config={"effort": "low"},
                                    messages=[{"role": "user", "content": prompt + text}],
                                    output_format=BedReading)
    if resp.stop_reason == "refusal" or resp.parsed_output is None:
        return None
    r = resp.parsed_output
    if r.beds is None or not (10 <= r.beds <= 4000):
        return None
    conf, verified, approx = score(r, text)
    return {"beds": r.beds, "is_maximum": r.is_maximum, "either_or": r.either_or, "quote": r.quote,
            "confidence": conf, "verified": verified, "approx": approx, "model": MODEL}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--refs", nargs="+", default=None)
    ap.add_argument("--max-docs", type=int, default=3)
    ap.add_argument("--ai", action="store_true")
    ap.add_argument("--nec-base", default=None,
                    help="portal origin, e.g. https://planning.birmingham.gov.uk, when PlanIt's links are stale")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    council = db.query(Council).filter(Council.name.ilike(args.council)).first()
    if council is None:
        raise SystemExit(f"Council {args.council!r} not found")
    q = (db.query(PlanningApplication)
         .filter(PlanningApplication.council_id == council.id,
                 or_(PlanningApplication.is_pbsa.is_(True), PlanningApplication.scheme_type == "PBSA")))
    if args.refs:
        q = q.filter(PlanningApplication.reference.in_(args.refs))
    else:
        q = q.filter(PlanningApplication.pbsa_beds.is_(None), PlanningApplication.delivery_status.in_(LIVE))
    apps = q.order_by(PlanningApplication.decision_date.desc().nullslast()).limit(args.limit).all()
    print(f"{council.name}: {len(apps)} applications to read")
    client_ai = None
    if args.ai:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise SystemExit("--ai needs ANTHROPIC_API_KEY")
        from ai_parse_pipeline_beds import anthropic_client
        client_ai = anthropic_client()

    stats = {"no_page": 0, "unsupported_portal": 0, "no_documents": 0, "no_pdf": 0,
             "read": 0, "cached": 0, "recorded_low": 0, "nothing_found": 0, "ai_cached": 0, "ai_recorded": 0}
    slug = council.name.lower().replace(" ", "-")
    # PlanIt may still hold a council's retired portal URL for older rows;
    # once one application resolves to the NEC portal, its base serves the rest.
    nec_base = args.nec_base
    with httpx.Client(headers=UA, timeout=60, follow_redirects=True) as client:
        for a in apps:
            ref = a.reference
            try:
                page = council_page_url(client, council.name, ref)
                if page and "/NECSWS/" in page:
                    nec_base = nec_base or page.split("/NECSWS/")[0]
                elif nec_base:
                    page = (f"{nec_base}/NECSWS/ES/Presentation/Planning/OnlinePlanning/"
                            f"OnlinePlanningOverview?applicationNumber={ref.replace('/', '%2F')}")
                if not page:
                    stats["no_page"] += 1
                    print(f"  {ref}: no council page via PlanIt")
                    continue
                if "/NECSWS/" not in page:
                    stats["unsupported_portal"] += 1
                    print(f"  {ref}: portal not supported yet ({page[:60]})")
                    continue
                docs = nec_documents(client, page, ref)
            except httpx.HTTPError as exc:
                print(f"  {ref}: fetch failed {type(exc).__name__}: {str(exc)[:80]}")
                continue
            if not docs:
                stats["no_documents"] += 1
                print(f"  {ref}: no documents listed")
                continue
            ranked = rank_documents(docs)
            if not ranked:
                stats["no_pdf"] += 1
                print(f"  {ref}: {len(docs)} documents, none a PDF")
                continue
            folder = DATA / slug / ref.replace("/", "_")
            outline = a.permission_type in ("outline", "hybrid")
            chosen, readings, capacity_text = None, [], []
            for doc in ranked[: args.max_docs]:
                path = download(client, doc, folder)
                if path is None:
                    continue
                try:
                    reading = read_document(path)
                except Exception as exc:
                    print(f"  {ref}: could not read {doc['title'][:40]!r}: {type(exc).__name__}")
                    continue
                readings.append((doc, reading))
                fig = pick(reading, outline)
                if fig and fig.confidence >= 0.6:
                    chosen = (doc, reading, fig)
                    break
            stats["read"] += bool(readings)
            if not args.dry_run:
                for doc, reading in readings:
                    (db.query(UnitExtractionRecord)
                     .filter(UnitExtractionRecord.entity_type == "planning_application",
                             UnitExtractionRecord.entity_id == a.id,
                             UnitExtractionRecord.source == "planning_document",
                             UnitExtractionRecord.source_reference == doc["url"][:500],
                             UnitExtractionRecord.method.notin_(("ai", "manual")))
                     .delete(synchronize_session=False))
                    for page_no, ex in reading.extractions:
                        for row in as_rows(ex, "planning_application", a.id, "planning_document",
                                           source_reference=doc["url"], kinds=("beds", "studios", "storeys", "homes")):
                            row["quote"] = f"{row['quote'][:440]} (p.{page_no} {doc['title'][:40]})"
                            db.add(UnitExtractionRecord(**row))
            if chosen is None and readings:
                fig_any = next((pick(r, outline) for _, r in readings if pick(r, outline)), None)
                if fig_any:
                    chosen = next((d, r, pick(r, outline)) for d, r in readings if pick(r, outline))
            if chosen:
                doc, reading, fig = chosen
                page_no = reading.page_of(fig)
                cache = fig.confidence >= CACHE_MIN_CONFIDENCE and a.pbsa_beds is None
                stats["cached" if cache else "recorded_low"] += 1
                print(f"  {ref}: beds={fig.value}{' (up to)' if fig.is_maximum else ''} conf={fig.confidence} "
                      f"p.{page_no} of {doc['title'][:40]!r} {'CACHED' if cache else 'recorded only'}: {fig.quote[:50]!r}")
                if cache and not args.dry_run:
                    a.pbsa_beds = fig.value
                    a.beds_basis = ("up_to" if fig.is_maximum else "either_or" if fig.alternative
                                    else "additional" if fig.is_additional else "stated")
                    a.beds_confidence = fig.confidence
                    a.beds_quote = f"{fig.quote} (p.{page_no} of {doc['title']}, {doc['date'] or 'undated'})"[:2000]
                    a.beds_extractor = "document"
                    if fig.is_maximum:
                        a.beds_max = fig.value
            elif readings and client_ai is not None:
                pages = [t for _, r in readings for i, t in enumerate(r.pages and [] or [])]  # placeholder
                from app.extraction.documents import pdf_pages, _CAPACITY_PAGE
                for doc, reading in readings[:2]:
                    pages += [t for t in pdf_pages(reading.path) if _CAPACITY_PAGE.search(t)][:8]
                ai = ai_reading(client_ai, readings[0][1], pages) if pages else None
                if ai:
                    cache = ai["confidence"] >= CACHE_MIN_CONFIDENCE and ai["verified"] and a.pbsa_beds is None
                    stats["ai_cached" if cache else "ai_recorded"] += 1
                    print(f"  {ref}: AI beds={ai['beds']} conf={ai['confidence']} "
                          f"{'CACHED' if cache else 'recorded only'}: {str(ai['quote'])[:50]!r}")
                    if not args.dry_run:
                        db.add(UnitExtractionRecord(
                            entity_type="planning_application", entity_id=a.id, kind="beds", value=ai["beds"],
                            is_maximum=ai["is_maximum"], is_approximate=ai["approx"], alternative=ai["either_or"],
                            confidence=ai["confidence"], quote=(ai["quote"] or "")[:500], method="ai",
                            model=ai["model"], source="planning_document", source_reference=readings[0][0]["url"][:500]))
                        if cache:
                            a.pbsa_beds, a.beds_extractor = ai["beds"], "ai"
                            a.beds_basis = "up_to" if ai["is_maximum"] else ("either_or" if ai["either_or"] else "stated")
                            a.beds_confidence, a.beds_quote = ai["confidence"], ai["quote"]
                else:
                    stats["nothing_found"] += 1
                    print(f"  {ref}: {len(readings)} document(s) read, no bed count stated (AI agrees)")
            else:
                stats["nothing_found"] += 1
                print(f"  {ref}: {len(readings)} document(s) read, no bed count found "
                      f"(tried {', '.join(d['title'][:30] for d in ranked[:args.max_docs])})")
            if not args.dry_run:
                db.commit()
    db.close()
    print("\n" + ", ".join(f"{k}={v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
