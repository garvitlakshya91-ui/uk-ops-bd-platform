"""Operator enrichment that says how it knows.

Four modes, each a dry run unless --apply is given:

  --label-rules   Operators that earlier rule passes wrote (operator = owner,
                  brand in the scheme name, same-postcode neighbour) get a
                  source lock naming the rule and a derived observation, so an
                  assumed operator can be told from one a feed supplied. No
                  value changes.
  --rules         Apply the two rules that hold up against feed-supplied
                  operators (owner is a known operator brand; brand in the
                  scheme name) to rows with no operator.
  --validate N    Ask the model, with web search, for the operator of N
                  schemes whose operator a feed already supplied, and score
                  the answers. Writes nothing.
  --ai            Ask for the operator of named schemes that have none. Every
                  answer is stored as an observation with its page and quoted
                  phrase; the operator column is filled only when the page was
                  fetched and names both the scheme and the operator.

Requires ANTHROPIC_API_KEY for --validate and --ai.

Usage:
    python enrich_operators.py --label-rules --rules            # dry run
    python enrich_operators.py --validate 30
    python enrich_operators.py --ai --limit 400 --apply
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import httpx
from sqlalchemy import text

from app.database import SessionLocal
from app.models.models import Company, ExistingScheme
from app.models.observations import CACHE_MIN_CONFIDENCE, record_observation
from app.scrapers.field_protection import set_field
from app.scrapers.scheme_matching import norm_name, norm_pc
from backfill_operator_rules import BRANDS, EXCLUDE_PATTERNS, matches_any

# Bulk lookups run on the current Haiku with the basic web search tool.
MODEL = "claude-haiku-5-5"
SEARCH_TOOL = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}
# Words that say the named company runs the building, not that it owns or built it.
ROLE_WORDS = re.compile(r"\b(manag\w+|operat\w+|let(?:s|ting|tings)?\s+by|run\s+by|part\s+of|brought\s+to\s+you\s+by|"
                        r"book\s+(?:direct\s+)?with)\b", re.I)


def role_stated(quote: str | None, operator: str | None) -> bool:
    """Does the quote give the operator a running role?

    The operator's own name is taken out first: "funded by Royal London Asset
    Management" must not pass on the word in the company's name. "A Simple Life
    development" counts: the brand is claiming the scheme as its own.
    """
    quote, operator = quote or "", operator or ""
    rest = re.sub(re.escape(operator), " ", quote, flags=re.I)
    for word in re.findall(r"[A-Za-z&]{3,}", operator):
        rest = re.sub(rf"\b{re.escape(word)}\b", " ", rest, flags=re.I)
    if ROLE_WORDS.search(rest):
        return True
    return bool(re.search(rf"\b(?:a|an)\s+{re.escape(operator)}\s+(?:property|development|community|home|scheme)\b",
                          quote, re.I))
PRICE_PER_MTOK = {"claude-haiku-5-5": (0.10, 0.50), "claude-sonnet-5-5": (2.0, 10.0), "claude-opus-5-5": (4.0, 20.0)}
SEARCH_FEE = 0.01          # per web search request
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Accept-Language": "en-GB,en;q=0.9"}
SIZE = "coalesce(es.beds_total, es.num_units, es.total_units)"
POOL = (f"es.merged_into_id IS NULL AND es.record_class IN ('scheme', 'development_cluster') AND {SIZE} >= 20")
FEEDS = ("arl_btr_open_operating", "pbsa_operator", "operator_directory", "manual_curation")
# How often each rule agreed with a feed-supplied operator on 10 Oct 2026
# (enrich_pool check, 1,292 schemes): the confidence stored with the label.
RULES = {
    "Operator=owner: self-managed": ("rule:self_managed", 0.21),
    "Operator=owner: self-operated": ("rule:brand_owner", 0.89),
    "Operator from brand in name": ("rule:brand_in_name", 0.98),
    "Operator from same-postcode scheme": ("rule:postcode_neighbour", 0.76),
}
_GENERIC = (r"\b(limited|ltd|plc|llp|the|group|holdings|uk|student|students|living|accommodation|homes|"
            r"property|properties|management|real estate|lettings|residential|build to rent|btr)\b")


def brand_key(name: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", re.sub(_GENERIC, " ", (name or "").lower()))


def same_operator(a: str | None, b: str | None) -> bool:
    ka, kb = brand_key(a), brand_key(b)
    return bool(ka) and bool(kb) and (ka == kb or (len(ka) >= 3 and ka in kb) or (len(kb) >= 3 and kb in ka))


# ------------------------------------------------------------------ rule labels
def label_rules(db, apply: bool) -> None:
    print("== label operators that a rule assumed")
    for marker, (label, agreement) in RULES.items():
        rows = db.execute(text("""
            SELECT es.id, co.name, es.scheme_type FROM existing_schemes es JOIN companies co ON co.id = es.operator_company_id
            WHERE es.merged_into_id IS NULL AND es.notes LIKE :m
              AND NOT (es.locked_fields ? 'operator_company_id')"""), {"m": f"%{marker}%"}).fetchall()
        print(f"  {label:26s} {len(rows):>6,} schemes   agreement with feeds where checkable: {agreement:.0%}")
        if not apply:
            continue
        for sid, name, stype in rows:
            # The agreement was measured on rental schemes a feed describes; for
            # other types the assumption is unmeasured, so no confidence is claimed.
            measured = agreement if (label != "rule:self_managed" or stype in ("BTR", "PBSA", "Co-living", "Senior")) else None
            record_observation(db, sid, "operator", name, label, basis="derived", confidence=measured,
                               quote=marker, extractor="rule", refresh_cache=False)
        db.execute(text("""
            UPDATE existing_schemes SET locked_fields = locked_fields || jsonb_build_object('operator_company_id', CAST(:l AS text))
            WHERE merged_into_id IS NULL AND notes LIKE :m AND operator_company_id IS NOT NULL
              AND NOT (locked_fields ? 'operator_company_id')"""), {"l": label, "m": f"%{marker}%"})
        db.commit()


# ------------------------------------------------------------------ validated rules
def company_for_brand(db, canon: str) -> Optional[int]:
    row = db.execute(text("""
        SELECT id FROM companies WHERE merged_into_id IS NULL AND (lower(name) = lower(:n) OR lower(name) LIKE lower(:like))
        ORDER BY (lower(name) = lower(:n)) DESC, id LIMIT 1"""), {"n": canon, "like": f"%{canon.split()[0]}%"}).first()
    return row[0] if row else None


def apply_rules(db, apply: bool) -> None:
    print("\n== rules that hold up (owner is an operator brand: 89%; brand in the scheme name: 98%)")
    rows = db.execute(text(f"""
        SELECT es.id, es.name, es.owner_company_id, ow.name
        FROM existing_schemes es LEFT JOIN companies ow ON ow.id = es.owner_company_id
        WHERE {POOL} AND es.operator_company_id IS NULL""")).fetchall()
    stats, cache = Counter(), {}
    for sid, name, owner_id, owner in rows:
        target = label = quote = None
        if owner and not matches_any(owner, EXCLUDE_PATTERNS) and matches_any(owner, BRANDS.keys()):
            target, label, quote, conf = owner_id, "rule:brand_owner", f"owner {owner} is an operator brand", 0.89
        else:
            low = f" {(name or '').lower()} "
            tok = next((t for t in BRANDS if t in low), None)
            if tok:
                canon = BRANDS[tok]
                if canon not in cache:
                    cache[canon] = company_for_brand(db, canon)
                if cache[canon]:
                    target, label, quote, conf = cache[canon], "rule:brand_in_name", f"scheme name contains {tok.strip()!r}", 0.98
        if not target:
            continue
        stats[label] += 1
        if apply:
            scheme = db.get(ExistingScheme, sid)
            op = db.get(Company, target)
            record_observation(db, scheme, "operator", op.name, label, basis="derived", confidence=conf,
                               quote=quote, extractor="rule", refresh_cache=False)
            set_field(scheme, "operator_company_id", target, label, db, changed_by="enrich_operators.py")
    if apply:
        db.commit()
    for k, v in stats.items():
        print(f"  {k:26s} {v:>6,} schemes {'filled' if apply else 'would be filled'}")
    print(f"  {len(rows) - sum(stats.values()):,} operator-less pool rows where no validated rule applies")


# ------------------------------------------------------------------ model lookup
def anthropic_client():
    import anthropic
    headers = {}
    if os.environ.get("ANTHROPIC_WORKSPACE_ID"):
        headers["anthropic-workspace-id"] = os.environ["ANTHROPIC_WORKSPACE_ID"]
    return anthropic.Anthropic(default_headers=headers or None, max_retries=3)


class Usage:
    def __init__(self, model: str):
        self.model, self.input, self.output, self.searches, self.calls = model, 0, 0, 0, 0

    def add(self, resp) -> None:
        u = resp.usage
        self.calls += 1
        self.input += (u.input_tokens or 0) + (getattr(u, "cache_read_input_tokens", 0) or 0)
        self.output += u.output_tokens or 0
        tool = getattr(u, "server_tool_use", None)
        self.searches += getattr(tool, "web_search_requests", 0) or 0

    def cost(self) -> float:
        pin, pout = PRICE_PER_MTOK.get(self.model, (4.0, 20.0))
        return self.input / 1e6 * pin + self.output / 1e6 * pout + self.searches * SEARCH_FEE

    def __str__(self) -> str:
        return (f"{self.calls} calls, {self.searches} searches, {self.input:,} input and {self.output:,} output "
                f"tokens, about ${self.cost():.2f}")


def ask_operator(client, model: str, usage: Usage, s: dict) -> dict:
    kind = {"BTR": "build-to-rent", "PBSA": "student accommodation", "Co-living": "co-living",
            "Senior": "later-living"}.get(s["scheme_type"], "residential rental")
    prompt = (f"Find who operates this UK {kind} scheme. OPERATOR means the brand the homes are marketed and let "
              f"under and that residents deal with (for example Unite Students, Simple Life, Way of Life, UNCLE). "
              f"It is not the investor, fund, asset manager, developer, architect or freeholder: if a page only says "
              f"who owns, funds or built the scheme, that is not the operator. If a separate managing agent runs the "
              f"building for that brand, give it as AGENT.\n"
              f"Scheme: {s['name']}\nAddress: {(s['address'] or '')[:120]}\nPostcode: {s['postcode'] or 'unknown'}\n"
              f"Council area: {s['council'] or 'unknown'}\nUnits: {s['size'] or 'unknown'}\n"
              f"Owner or developer on record: {s['owner'] or 'unknown'}\n\n"
              f"Search the web. Then reply with exactly four lines and nothing else:\n"
              f"OPERATOR: <operator brand, or UNKNOWN if no page states it>\n"
              f"AGENT: <managing agent if different, else NONE>\n"
              f"URL: <the page that states the operator>\n"
              f"QUOTE: <up to 25 words copied exactly from that page that name the operator and say it manages, "
              f"operates or lets the scheme>")
    messages = [{"role": "user", "content": prompt}]
    resp = None
    for _ in range(3):
        resp = client.messages.create(model=model, max_tokens=1500, tools=[SEARCH_TOOL], messages=messages)
        usage.add(resp)
        if resp.stop_reason != "pause_turn":
            break
        messages = [messages[0], {"role": "assistant", "content": resp.content}]
    if resp is None or resp.stop_reason == "refusal":
        return {"operator": None}
    answer = "\n".join(b.text for b in resp.content if b.type == "text")
    op = re.search(r"OPERATOR:\s*(.+)", answer)
    url = re.search(r"URL:\s*(https?://\S+)", answer)
    quote = re.search(r"QUOTE:\s*(.+)", answer)
    name = op.group(1).strip().strip("*").strip() if op else None
    if not name or name.upper().startswith("UNKNOWN"):
        return {"operator": None}
    agent = re.search(r"AGENT:\s*(.+)", answer)
    agent = agent.group(1).strip().strip("*").strip() if agent else None
    if agent and agent.upper().startswith(("NONE", "UNKNOWN", "N/A")):
        agent = None
    return {"operator": name[:200], "agent": agent[:200] if agent else None,
            "url": url.group(1).rstrip(".,)") if url else None,
            "quote": quote.group(1).strip().strip('"')[:300] if quote else None}


def verify(found: dict, s: dict) -> tuple[bool, str]:
    """Fetch the cited page: it must name the operator and this scheme (or its postcode)."""
    if brand_key(found["operator"]) == brand_key(s["name"]):
        return False, "the answer is the scheme's own name, not an operator"
    if not found.get("url"):
        return False, "no page cited"
    try:
        r = httpx.get(found["url"], headers=UA, timeout=25, follow_redirects=True)
    except httpx.HTTPError as exc:
        return False, f"page not fetched ({type(exc).__name__})"
    if r.status_code != 200 or len(r.text) < 500:
        return False, f"page not fetched (HTTP {r.status_code})"
    body = re.sub(r"\s+", " ", re.sub(r"(?s)<[^>]+>", " ", r.text)).lower()
    squashed = re.sub(r"[^a-z0-9]+", "", body)
    op_ok = brand_key(found["operator"]) in squashed if brand_key(found["operator"]) else False
    tokens = norm_name(s["name"])
    hits = sum(1 for t in tokens if t in body)
    pc = norm_pc(s["postcode"]).lower()
    scheme_ok = (bool(tokens) and hits >= max(1, (len(tokens) + 1) // 2)) or (bool(pc) and pc in squashed)
    # The quoted phrase must really be on the page, name the operator, and give
    # it a running role: "funded by" or "developed by" is not operating.
    quote = found.get("quote") or ""
    q = re.sub(r"[^a-z0-9]+", "", quote.lower())
    on_page = len(q) >= 12 and q in squashed
    own_site = brand_key(found["operator"]) in re.sub(r"[^a-z0-9]+", "", httpx.URL(found["url"]).host.lower())
    role_ok = brand_key(found["operator"]) in q and role_stated(quote, found["operator"])
    quote_ok = on_page and (role_ok or own_site)
    return op_ok and scheme_ok and quote_ok, (f"operator {'named' if op_ok else 'NOT named'}, "
                                              f"scheme {'named' if scheme_ok else 'NOT named'}, "
                                              f"quote {'ok' if quote_ok else 'not on page / no role'}")


def resolve_company(db, name: str) -> Company:
    """An existing operator of the same brand, else a company of that name, else a new one."""
    key = brand_key(name)
    for cid, cname in db.execute(text("""
            SELECT co.id, co.name FROM companies co
            WHERE co.merged_into_id IS NULL AND co.id IN (
                SELECT operator_company_id FROM existing_schemes WHERE operator_company_id IS NOT NULL)""")):
        if brand_key(cname) == key:
            return db.get(Company, cid)
    row = db.execute(text("SELECT id FROM companies WHERE merged_into_id IS NULL AND lower(name) = lower(:n) LIMIT 1"),
                     {"n": name}).first()
    if row:
        return db.get(Company, row[0])
    co = Company(name=name, normalized_name=re.sub(r"[^a-z0-9]+", "", name.lower()), company_type="Operator",
                 is_active=True)
    db.add(co)
    db.flush()
    return co


def load(db, where: str, limit: int) -> list[dict]:
    rows = db.execute(text(f"""
        SELECT es.id, es.name, es.address, es.postcode, es.scheme_type, {SIZE}, cc.name, ow.name, op.name, es.source
        FROM existing_schemes es
        LEFT JOIN councils cc ON cc.id = es.council_id
        LEFT JOIN companies ow ON ow.id = es.owner_company_id
        LEFT JOIN companies op ON op.id = es.operator_company_id
        WHERE {POOL} AND {where} ORDER BY md5(es.id::text) LIMIT :lim"""), {"lim": limit}).fetchall()
    return [dict(zip(("id", "name", "address", "postcode", "scheme_type", "size", "council", "owner", "operator",
                      "source"), r)) for r in rows]


def validate(db, n: int, model: str) -> None:
    client, usage = anthropic_client(), Usage(model)
    gold = load(db, f"""es.record_class = 'scheme' AND es.operator_company_id IS NOT NULL
                        AND es.source IN {FEEDS} AND es.locked_fields ->> 'operator_company_id' = es.source
                        AND length(es.name) > 5""", n)
    print(f"== validation: {len(gold)} schemes whose operator a feed supplied, asked blind ({model})")
    tally = Counter()
    for s in gold:
        found = ask_operator(client, model, usage, {**s, "owner": None})
        if not found["operator"]:
            tally["no answer"] += 1
            print(f"  {s['name'][:30]:30s} feed: {s['operator'][:24]:24s} model: (none)")
            continue
        ok_page, why = verify(found, s)
        agree = same_operator(found["operator"], s["operator"]) or same_operator(found.get("agent"), s["operator"])
        tally[("verified" if ok_page else "unverified") + (" agree" if agree else " DISAGREE")] += 1
        print(f"  {s['name'][:30]:30s} feed: {s['operator'][:24]:24s} model: {found['operator'][:24]:24s} "
              f"{'agree' if agree else 'DISAGREE'}  {'page ok' if ok_page else why}")
        time.sleep(0.5)
    v_ok, v_bad = tally["verified agree"], tally["verified DISAGREE"]
    u_ok, u_bad = tally["unverified agree"], tally["unverified DISAGREE"]
    print(f"\n  answered {v_ok + v_bad + u_ok + u_bad} of {len(gold)}; no answer {tally['no answer']}")
    if v_ok + v_bad:
        print(f"  page-verified answers: {v_ok}/{v_ok + v_bad} agree with the feed ({100 * v_ok / (v_ok + v_bad):.0f}%)")
    if u_ok + u_bad:
        print(f"  unverified answers:    {u_ok}/{u_ok + u_bad} agree with the feed ({100 * u_ok / (u_ok + u_bad):.0f}%)")
    print(f"  {usage}")


def enrich(db, limit: int, model: str, apply: bool, verified_conf: float) -> None:
    client, usage = anthropic_client(), Usage(model)
    todo = load(db, """es.record_class = 'scheme' AND es.operator_company_id IS NULL AND length(es.name) > 5
                       AND NOT EXISTS (SELECT 1 FROM scheme_observations o WHERE o.scheme_id = es.id
                                       AND o.field = 'operator' AND o.source = 'web_search')""", limit)
    print(f"== named schemes with no operator: {len(todo)} to look up ({model})")
    stats = Counter()
    for s in todo:
        try:
            found = ask_operator(client, model, usage, s)
        except Exception as exc:
            stats["error"] += 1
            message = str(exc)
            print(f"  #{s['id']} {s['name'][:30]}: {type(exc).__name__}: {message[:200]}")
            if "credit balance" in message or "authentication" in message.lower() or "401" in message:
                # No point trying the rest: nothing is recorded for them, so the
                # next run picks them up.
                print("  stopping: the API account cannot be used (credit or key).")
                break
            continue
        if not found["operator"]:
            stats["no answer"] += 1
            if apply:
                record_observation(db, s["id"], "operator", None, "web_search", confidence=0.0,
                                   quote="no page states the operator", extractor="ai", model=model,
                                   refresh_cache=False)
                db.commit()
            continue
        ok_page, why = verify(found, s)
        conf = verified_conf if ok_page else 0.4
        stats["verified" if ok_page else "unverified"] += 1
        print(f"  #{s['id']} {s['name'][:28]:28s} -> {found['operator'][:26]:26s} conf {conf}  {why}  {str(found['url'])[:60]}")
        if apply:
            scheme = db.get(ExistingScheme, s["id"])
            record_observation(db, scheme, "operator", found["operator"], "web_search", reference=found.get("url"),
                               confidence=conf, quote=found.get("quote"), extractor="ai", model=model,
                               refresh_cache=False)
            if found.get("agent"):
                # The brand and the agent who runs the building for it are both
                # worth knowing; feeds name either, so keep them apart.
                record_observation(db, scheme, "managing_agent", found["agent"], "web_search",
                                   reference=found.get("url"), confidence=conf, quote=found.get("quote"),
                                   extractor="ai", model=model, refresh_cache=False)
            if ok_page and conf >= CACHE_MIN_CONFIDENCE:
                co = resolve_company(db, found["operator"])
                if set_field(scheme, "operator_company_id", co.id, "web_search", db, changed_by="enrich_operators.py"):
                    stats["operator filled"] += 1
            db.commit()
        time.sleep(0.5)
    print("\n  " + ", ".join(f"{k}={v}" for k, v in stats.items()))
    print(f"  {usage}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label-rules", action="store_true")
    ap.add_argument("--rules", action="store_true")
    ap.add_argument("--validate", type=int, default=0, metavar="N")
    ap.add_argument("--ai", action="store_true")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--model", default=MODEL)
    ap.add_argument("--verified-confidence", type=float, default=0.85,
                    help="confidence given to a page-verified answer (set from the --validate result)")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if (args.validate or args.ai) and not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit("ANTHROPIC_API_KEY not set")
    db = SessionLocal()
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    try:
        if args.label_rules:
            label_rules(db, args.apply)
        if args.rules:
            apply_rules(db, args.apply)
        if args.validate:
            validate(db, args.validate, args.model)
        if args.ai:
            enrich(db, args.limit, args.model, args.apply, args.verified_confidence)
    finally:
        db.rollback()
        db.close()


if __name__ == "__main__":
    main()
