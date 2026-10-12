"""Mechanical clean-up of existing_schemes and the companies behind them.

Every step is a dry run unless --apply is given, is safe to re-run, and
removes nothing: values that change are written to scheme_change_logs
(source "data_cleanup") and to a JSON run log under data/cleanup/, so
each change can be traced and reversed.

Steps (run all, or pick with --only):
  classify    record_class: ingest_artefact (one energy certificate
              repeated 100 times; its false size is cleared),
              procurement_notice, listing_fragment (room tiers, unit
              codes and FAQ pages stored as student schemes),
              development_cluster (certificate / sales clusters no
              scheme-level source has named). Everything else stays
              "scheme".
  codes       one spelling per code: status case, scheme_type variants
              (the app filters on "Senior" and "Mixed"), academic years
              as 2026/27.
  postcodes   upper-case, single space before the inward code.
  coords      fill lat/lng from the legacy latitude/longitude columns.
  names       collapse whitespace; strip "Welcome to ..." and page-title
              tails; the old spelling is kept as an alias.
  duplicates  retire a row that repeats another's name, postcode, type
              and size, when it carries no rows of its own.
  companies   merge spelling variants of one company (same name once
              case, punctuation and Ltd/Limited are ignored; never when
              two different Companies House numbers are involved).
  flags       send rows carrying an operator's head-office address to the
              review queue.

Usage:
    python clean_scheme_data.py                       # dry run, all steps
    python clean_scheme_data.py --only classify codes
    python clean_scheme_data.py --apply
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import text

from app.database import SessionLocal
from app.models.models import ExistingScheme
from app.registry.core import add_alias, retire_duplicate

SOURCE = "data_cleanup"
BY = "clean_scheme_data.py"
LOG_DIR = Path(__file__).parent / "data" / "cleanup"
NAME_LOCKS_KEPT = ("manual", "registry", "census")
SIZE = "coalesce(beds_total, num_units, total_units)"

# A student "scheme" whose name is a room tier, a unit code or a web page.
_TIER = (r"(?:classic|premium|deluxe|standard|bronze|silver|gold|platinum|diamond|accessible|dda|plus|"
         r"large|small|ground floor|upper floor|shared|\d+ bed|en ?suite|ensuite|studio|room|apartment|"
         r"cluster|flat|double|single|twin|deluxe|superior|penthouse|duplex|wheelchair)")
FRAGMENT_PATTERNS = [
    ("too short", re.compile(r"^.{0,2}$")),
    ("unit code", re.compile(r"^[a-z]?\d+[a-z]?$", re.I)),
    ("flat or block label", re.compile(
        r"^(?:flat|room|unit|block|plot|bedroom|apartment)\s+(?:[a-z]|\d{1,4}[a-z]?|[a-z]\d{1,3})$|^[a-z] block$", re.I)),
    ("room tier", re.compile(rf"^(?:{_TIER}[ /+&-]*)+$", re.I)),
    ("web page", re.compile(
        r"arrange a viewing|am i eligible|general information|living in the property|frequently asked|"
        r"^faqs?$|^contact us$|^book now$|^test$|estate_agent_feeds|^how to book|^virtual tour", re.I)),
]
OWN_CHILD_TABLES = ["scheme_contracts", "pipeline_opportunities", "alerts", "scheme_room_rents",
                    "scheme_advertised_rents", "scheme_incentives", "scheme_availability",
                    "scheme_observations", "scheme_room_types", "scheme_events", "transactions"]


class Run:
    def __init__(self, db, apply: bool):
        self.db, self.apply = db, apply
        self.log: dict[str, list] = defaultdict(list)
        self.counts: dict[str, int] = defaultdict(int)

    def note(self, step: str, **entry) -> None:
        self.log[step].append(entry)

    def scheme_update(self, step: str, label: str, where: str, field: str, new_expr: str,
                      params: dict | None = None) -> int:
        """Set existing_schemes.<field> = <new_expr> where <where>, logging each old value."""
        n = self.db.execute(text(f"""
            WITH target AS (
                SELECT id, ({field})::text AS old_value, ({new_expr})::text AS new_value
                FROM existing_schemes WHERE {where} AND ({field}) IS DISTINCT FROM ({new_expr})),
            logged AS (
                INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
                SELECT id, :field, old_value, new_value, :source, :by, now() FROM target RETURNING 1),
            done AS (
                UPDATE existing_schemes es SET {field} = {new_expr}
                FROM target t WHERE es.id = t.id RETURNING 1)
            SELECT count(*) FROM done"""), {"field": field, "source": SOURCE, "by": BY, **(params or {})}).scalar()
        self.counts[f"{step}: {label}"] += n
        print(f"  {label:58s} {n:>7,}")
        return n


# ------------------------------------------------------------------ classify
def step_classify(run: Run) -> None:
    db = run.db
    artefact = ("source = 'epc_new_dwelling' AND num_units = 100 AND record_class IN ('scheme', 'development_cluster') "
                "AND (SELECT count(*) FROM jsonb_object_keys(epc_ratings::jsonb)) = 1")
    ids = [r[0] for r in db.execute(text(f"SELECT id FROM existing_schemes WHERE {artefact}"))]
    print(f"  one certificate repeated 100 times                         {len(ids):>7,}")
    if ids:
        db.execute(text("CREATE TEMP TABLE IF NOT EXISTS _artefacts (id int PRIMARY KEY) ON COMMIT DROP"))
        db.execute(text("TRUNCATE _artefacts"))
        db.execute(text("INSERT INTO _artefacts SELECT unnest(CAST(:ids AS int[]))"), {"ids": ids})
        scope = "id IN (SELECT id FROM _artefacts)"
        run.scheme_update("classify", "  record_class -> ingest_artefact", scope, "record_class", "'ingest_artefact'")
        run.scheme_update("classify", "  false size of 100 cleared (num_units)", scope, "num_units", "NULL::int")
        run.scheme_update("classify", "  false size of 100 cleared (total_units)", scope + " AND total_units = 100",
                          "total_units", "NULL::int")
        run.scheme_update("classify", "  score built on the false size cleared", scope, "bd_score", "NULL::float")
        n = db.execute(text("""
            WITH t AS (SELECT id, locked_fields ->> 'num_units' AS old FROM existing_schemes
                       WHERE id IN (SELECT id FROM _artefacts) AND locked_fields ? 'num_units'),
            l AS (INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
                  SELECT id, 'locked_fields.num_units', old, NULL, :s, :b, now() FROM t RETURNING 1),
            u AS (UPDATE existing_schemes es SET locked_fields = es.locked_fields - 'num_units'
                  FROM t WHERE es.id = t.id RETURNING 1)
            SELECT count(*) FROM u"""), {"s": SOURCE, "b": BY}).scalar()
        print(f"  {'  size lock released so a true count can be written':58s} {n:>7,}")

    # An artefact row the feed has since given a real count is a cluster again.
    run.scheme_update("classify", "artefacts since re-counted by the feed -> cluster",
                      "record_class = 'ingest_artefact' AND num_units IS NOT NULL AND num_units <> 100",
                      "record_class", "'development_cluster'")

    run.scheme_update("classify", "procurement notices stored as schemes",
                      "source = 'contracts_finder' AND record_class = 'scheme'", "record_class", "'procurement_notice'")

    rows = db.execute(text("""
        SELECT id, name, source FROM existing_schemes
        WHERE scheme_type = 'PBSA' AND operating_status IS NULL AND verification_status = 'unverified'
          AND merged_into_id IS NULL AND record_class = 'scheme'""")).fetchall()
    fragments = []
    for sid, name, source in rows:
        n = re.sub(r"\s+", " ", (name or "").strip())
        why = next((label for label, rx in FRAGMENT_PATTERNS if rx.search(n)), None)
        if why:
            fragments.append((sid, n, source, why))
    by_why = defaultdict(list)
    for f in fragments:
        by_why[f[3]].append(f[1])
    for why, names in sorted(by_why.items(), key=lambda kv: -len(kv[1])):
        print(f"    {why:22s} {len(names):>4}  e.g. {', '.join(sorted(set(names))[:6])[:90]}")
    if fragments:
        db.execute(text("CREATE TEMP TABLE IF NOT EXISTS _fragments (id int PRIMARY KEY) ON COMMIT DROP"))
        db.execute(text("TRUNCATE _fragments"))
        db.execute(text("INSERT INTO _fragments SELECT unnest(CAST(:ids AS int[]))"), {"ids": [f[0] for f in fragments]})
    run.scheme_update("classify", "student rows that are room tiers, unit codes or pages",
                      "id IN (SELECT id FROM _fragments)" if fragments else "false",
                      "record_class", "'listing_fragment'")

    run.scheme_update("classify", "certificate / sales clusters no scheme source has named",
                      "source IN ('epc_new_dwelling', 'land_registry_price_paid') AND record_class = 'scheme' "
                      "AND operating_status IS NULL AND verification_status = 'unverified'",
                      "record_class", "'development_cluster'")
    print("  result:")
    for cls, n, sized in db.execute(text(f"""
            SELECT record_class, count(*), count(*) FILTER (WHERE {SIZE} >= 20)
            FROM existing_schemes WHERE merged_into_id IS NULL GROUP BY 1 ORDER BY 2 DESC""")):
        print(f"    {cls:22s} {n:>7,} rows   {sized:>6,} of 20+ units")


# ------------------------------------------------------------------ codes
def step_codes(run: Run) -> None:
    db = run.db
    for old, new in (("Operational", "operational"), ("Under Construction", "under_construction")):
        run.scheme_update("codes", f"status {old!r} -> {new!r}", f"status = '{old}'", "status", f"'{new}'")
    for old, new in (("Senior Living", "Senior"), ("Mixed-use", "Mixed")):
        run.scheme_update("codes", f"scheme_type {old!r} -> {new!r}", f"scheme_type = '{old}'",
                          "scheme_type", f"'{new}'")
    for table in ("scheme_room_rents", "scheme_advertised_rents", "scheme_incentives",
                  "scheme_availability", "scheme_observations"):
        rows = db.execute(text(f"""
            UPDATE {table} SET academic_year = replace(academic_year, '-', '/')
            WHERE academic_year ~ '^20[0-9]{{2}}-[0-9]{{2}}$' RETURNING id""")).fetchall()
        run.counts[f"codes: academic_year in {table}"] += len(rows)
        run.note("codes", table=table, field="academic_year", change="YYYY-YY -> YYYY/YY", ids=[r[0] for r in rows])
        print(f"  {'academic_year YYYY-YY -> YYYY/YY in ' + table:58s} {len(rows):>7,}")
    # A respelled year can land on a key that already has a current row. Where the
    # two rows say the same thing (price and tenancy length) the older one is
    # superseded, which keeps it in history. Rows that differ are left: the same
    # room tier is often offered at two tenancy lengths.
    twins = db.execute(text("""
        UPDATE scheme_room_rents old SET is_current = false, superseded_at = now()
        FROM scheme_room_rents new
        WHERE old.is_current AND new.is_current
          AND (old.captured_at, old.id) < (new.captured_at, new.id)
          AND old.scheme_id = new.scheme_id AND old.source = new.source AND old.room_type = new.room_type
          AND old.sub_classification IS NOT DISTINCT FROM new.sub_classification
          AND old.academic_year IS NOT DISTINCT FROM new.academic_year
          AND old.rent_ppw IS NOT DISTINCT FROM new.rent_ppw AND old.rent_pcm IS NOT DISTINCT FROM new.rent_pcm
          AND old.contract_length_weeks IS NOT DISTINCT FROM new.contract_length_weeks
        RETURNING old.id""")).fetchall()
    run.counts["codes: identical current room rents superseded"] += len(twins)
    run.note("codes", table="scheme_room_rents", field="is_current", change="true -> false (identical newer row)",
             ids=[r[0] for r in twins])
    print(f"  {'identical current room rents: older row superseded':58s} {len(twins):>7,}")
    clash = db.execute(text("""
        SELECT count(*) FROM (SELECT 1 FROM scheme_room_rents WHERE is_current
            GROUP BY scheme_id, source, room_type, sub_classification, academic_year HAVING count(*) > 1) t""")).scalar()
    print(f"  {'room-rent keys with two current rows that differ (left)':58s} {clash:>7,}")


# ------------------------------------------------------------------ postcodes, coords
def step_postcodes(run: Run) -> None:
    run.scheme_update("postcodes", "postcode upper-cased and spaced",
                      "upper(regexp_replace(postcode, '\\s+', '', 'g')) ~ '^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$'",
                      "postcode",
                      "regexp_replace(upper(regexp_replace(postcode, '\\s+', '', 'g')), '([0-9][A-Z]{2})$', ' \\1')")
    left = run.db.execute(text("""
        SELECT count(*) FROM existing_schemes WHERE nullif(trim(postcode), '') IS NOT NULL
          AND upper(trim(postcode)) !~ '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$'""")).scalar()
    print(f"  {'postcodes that are not a valid UK postcode (left as found)':58s} {left:>7,}")


def step_coords(run: Run) -> None:
    scope = "lat IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"
    run.scheme_update("coords", "lng filled from legacy longitude", scope, "lng", "longitude")
    run.scheme_update("coords", "lat filled from legacy latitude", "lat IS NULL AND latitude IS NOT NULL",
                      "lat", "latitude")


# ------------------------------------------------------------------ names
def step_names(run: Run) -> None:
    db = run.db
    rows = db.execute(text(r"""
        SELECT id, name, coalesce(locked_fields ->> 'name', '') FROM existing_schemes
        WHERE merged_into_id IS NULL AND (name <> btrim(name) OR name ~ '\s{2,}' OR name ~* '^welcome to '
              OR (name LIKE '% | %' AND source IN ('operator_directory', 'pbsa_operator')))""")).fetchall()
    changed = defaultdict(int)
    for sid, name, lock in rows:
        if lock in NAME_LOCKS_KEPT:
            continue
        new = re.sub(r"\s+", " ", name).strip()
        kind = "whitespace collapsed"
        if re.match(r"(?i)^welcome to ", new):
            new, kind = re.sub(r"(?i)^welcome to ", "", new), "'Welcome to' removed"
        if " | " in new:
            head = new.split(" | ")[0].strip()
            if len(head) >= 3:
                new, kind = head, "page-title tail removed"
        if new == name or len(new) < 3:
            continue
        changed[kind] += 1
        run.note("names", id=sid, old=name, new=new, kind=kind)
        db.execute(text("""
            INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
            VALUES (:id, 'name', :old, :new, :s, :b, now())"""), {"id": sid, "old": name, "new": new, "s": SOURCE, "b": BY})
        db.execute(text("UPDATE existing_schemes SET name = :new WHERE id = :id"), {"id": sid, "new": new[:500]})
        if kind != "whitespace collapsed":
            add_alias(db, sid, name, "former_name")
            add_alias(db, sid, new, SOURCE)
    for kind, n in changed.items():
        run.counts[f"names: {kind}"] += n
        print(f"  {kind:58s} {n:>7,}")
    for e in [e for e in run.log["names"] if e["kind"] != "whitespace collapsed"][:6]:
        print(f"      {e['old'][:44]!r} -> {e['new'][:40]!r}")


# ------------------------------------------------------------------ duplicates
def step_duplicates(run: Run) -> None:
    db = run.db
    groups = db.execute(text(f"""
        SELECT array_agg(id ORDER BY id) FROM existing_schemes
        WHERE merged_into_id IS NULL AND nullif(trim(postcode), '') IS NOT NULL
          AND regexp_replace(lower(name), '[^a-z0-9]+', '', 'g') <> ''
        GROUP BY regexp_replace(lower(name), '[^a-z0-9]+', '', 'g'),
                 upper(regexp_replace(postcode, '\\s+', '', 'g')), coalesce(scheme_type, ''), {SIZE}
        HAVING count(*) > 1""")).fetchall()
    ids = [i for g in groups for i in g[0]]
    children: dict[int, int] = defaultdict(int)
    for table in OWN_CHILD_TABLES:
        for sid, n in db.execute(text(f"SELECT scheme_id, count(*) FROM {table} WHERE scheme_id = ANY(:ids) GROUP BY 1"),
                                 {"ids": ids or [0]}):
            children[sid] += n
    retired = skipped = swapped = 0
    for (members,) in groups:
        survivor_id = max(members, key=lambda i: (children[i], -i))
        survivor = db.get(ExistingScheme, survivor_id)
        for dup_id in members:
            if dup_id == survivor_id:
                continue
            if children[dup_id]:
                skipped += 1
                run.note("duplicates", action="left for review", id=dup_id, survivor=survivor_id,
                         reason=f"{children[dup_id]} rows of its own")
                continue
            dup = db.get(ExistingScheme, dup_id)
            if (dup.source == survivor.source and dup.source_reference and survivor.source_reference
                    and dup.source_reference != survivor.source_reference and dup.created_at > survivor.created_at):
                # The feed now writes the newer row's reference. The survivor takes
                # it, so the next ingest updates the live row and not the retired one.
                kept, feeds = survivor.source_reference, dup.source_reference
                for sid, before, after in ((survivor.id, kept, feeds), (dup.id, feeds, kept)):
                    db.execute(text("""
                        INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
                        VALUES (:id, 'source_reference', :o, :n, :s, :b, now())"""),
                               {"id": sid, "o": before, "n": after, "s": SOURCE, "b": BY})
                survivor.source_reference, dup.source_reference = feeds, kept
                swapped += 1
            retire_duplicate(db, dup, survivor, BY,
                             f"same name, postcode, type and size as #{survivor_id} {survivor.name}")
            run.note("duplicates", action="retired", id=dup_id, survivor=survivor_id, name=dup.name,
                     postcode=dup.postcode, source=dup.source)
            retired += 1
    db.flush()
    run.counts["duplicates: retired"] += retired
    print(f"  {'exact duplicates (name, postcode, type and size)':58s} {len(ids) - len(groups):>7,}")
    print(f"  {'  retired into the row that holds the data':58s} {retired:>7,}")
    print(f"  {'  left for review (the duplicate has rows of its own)':58s} {skipped:>7,}")
    print(f"  {'  survivor given the reference the feed now writes':58s} {swapped:>7,}")
    by_source = defaultdict(int)
    for e in run.log["duplicates"]:
        if e["action"] == "retired":
            by_source[e["source"]] += 1
    print("    retired by feed:", dict(by_source))


# ------------------------------------------------------------------ companies
def company_key(name: str | None) -> str:
    k = (name or "").lower().replace("&", " and ")
    k = re.sub(r"\b(limited|ltd)\b\.?", " ltd ", k)
    k = re.sub(r"^\s*the\s+", " ", k)
    return re.sub(r"[^a-z0-9]+", "", k)


def step_companies(run: Run, scope: str) -> None:
    db = run.db
    rows = db.execute(text("""
        WITH refs AS (
            SELECT operator_company_id cid, 'operator' role FROM existing_schemes WHERE operator_company_id IS NOT NULL
            UNION ALL SELECT owner_company_id, 'owner' FROM existing_schemes WHERE owner_company_id IS NOT NULL
            UNION ALL SELECT asset_manager_company_id, 'other' FROM existing_schemes WHERE asset_manager_company_id IS NOT NULL
            UNION ALL SELECT landlord_company_id, 'other' FROM existing_schemes WHERE landlord_company_id IS NOT NULL)
        SELECT co.id, co.name, nullif(trim(co.companies_house_number), ''), count(*),
               count(*) FILTER (WHERE role = 'operator'),
               EXISTS (SELECT 1 FROM ownership_chain_nodes n WHERE n.company_id = co.id)
        FROM refs JOIN companies co ON co.id = refs.cid
        WHERE co.merged_into_id IS NULL GROUP BY co.id""")).fetchall()
    groups: dict[str, list] = defaultdict(list)
    for r in rows:
        key = company_key(r[1])
        if len(key) >= 4:
            groups[key].append(r)
    merged = conflicts = chains = repointed = 0
    for key, members in groups.items():
        if len(members) < 2 or (scope == "operator" and not any(m[4] for m in members)):
            continue
        numbers = {m[2] for m in members if m[2]}
        if len(numbers) > 1:
            conflicts += 1
            run.note("companies", action="left: different Companies House numbers",
                     names=[m[1] for m in members], numbers=sorted(numbers))
            continue
        if sum(1 for m in members if m[5]) > 1:
            chains += 1
            run.note("companies", action="left: two rows carry ownership chains", names=[m[1] for m in members])
            continue
        primary = max(members, key=lambda m: (bool(m[5]), bool(m[2]), m[3], -m[0]))
        for dup in members:
            if dup[0] == primary[0]:
                continue
            p = {"dup": dup[0], "pri": primary[0], "s": SOURCE, "b": BY}
            for col in ("operator_company_id", "owner_company_id", "asset_manager_company_id", "landlord_company_id"):
                repointed += db.execute(text(f"""
                    WITH t AS (SELECT id FROM existing_schemes WHERE {col} = :dup),
                    l AS (INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
                          SELECT id, '{col}', CAST(:dup AS text), CAST(:pri AS text), :s, :b, now() FROM t RETURNING 1),
                    u AS (UPDATE existing_schemes es SET {col} = :pri FROM t WHERE es.id = t.id RETURNING 1)
                    SELECT count(*) FROM u"""), p).scalar()
            for table, col in (("planning_applications", "applicant_company_id"), ("planning_applications", "agent_company_id"),
                               ("contacts", "company_id"), ("company_aliases", "company_id"),
                               ("pipeline_opportunities", "company_id"), ("alerts", "company_id"),
                               ("companies", "parent_company_id"), ("ownership_chain_nodes", "company_id")):
                db.execute(text(f"UPDATE {table} SET {col} = :pri WHERE {col} = :dup"), p)
            if not db.execute(text("SELECT 1 FROM company_aliases WHERE company_id = :pri AND alias_name = :n"),
                              {"pri": primary[0], "n": dup[1]}).first():
                db.execute(text("INSERT INTO company_aliases (company_id, alias_name, source) VALUES (:pri, :n, 'scraper')"),
                           {"pri": primary[0], "n": dup[1]})
            if dup[2] and not primary[2]:
                db.execute(text("UPDATE companies SET companies_house_number = :n WHERE id = :pri "
                                "AND nullif(trim(companies_house_number), '') IS NULL"), {"n": dup[2], "pri": primary[0]})
            db.execute(text("UPDATE companies SET is_active = false, merged_into_id = :pri WHERE id = :dup"), p)
            run.note("companies", action="merged", duplicate_id=dup[0], duplicate=dup[1], into_id=primary[0],
                     into=primary[1], schemes=dup[3])
            merged += 1
    run.counts["companies: merged"] += merged
    print(f"  {'company rows merged into their other spelling':58s} {merged:>7,}")
    print(f"  {'  scheme links re-pointed':58s} {repointed:>7,}")
    print(f"  {'  groups left: different Companies House numbers':58s} {conflicts:>7,}")
    print(f"  {'  groups left: two rows carry ownership chains':58s} {chains:>7,}")
    top = sorted((e for e in run.log["companies"] if e["action"] == "merged"), key=lambda e: -e["schemes"])[:8]
    for e in top:
        print(f"      {e['duplicate'][:40]!r} ({e['schemes']}) -> {e['into'][:40]!r}")


# ------------------------------------------------------------------ flags
def step_flags(run: Run) -> None:
    db = run.db
    rows = db.execute(text("""
        WITH shared AS (
            SELECT upper(regexp_replace(postcode, '\\s+', '', 'g')) pc, lower(trim(address)) addr
            FROM existing_schemes
            WHERE merged_into_id IS NULL AND source = 'operator_directory' AND record_class = 'scheme'
              AND nullif(trim(postcode), '') IS NOT NULL AND nullif(trim(address), '') IS NOT NULL
            GROUP BY 1, 2 HAVING count(*) >= 5 AND count(DISTINCT lower(name)) >= 5)
        SELECT es.id, es.name, es.postcode, es.address FROM existing_schemes es JOIN shared s
          ON upper(regexp_replace(es.postcode, '\\s+', '', 'g')) = s.pc AND lower(trim(es.address)) = s.addr
        WHERE es.merged_into_id IS NULL AND es.source = 'operator_directory'
          AND es.verification_status = 'unverified'""")).fetchall()
    for sid, name, pc, addr in rows:
        note = (f"address and postcode ({addr}, {pc}) are shared by five or more of this operator's "
                f"listings: the head office, not the scheme. Location unknown.")
        db.execute(text("""
            INSERT INTO scheme_change_logs (scheme_id, field_name, old_value, new_value, source, changed_by, changed_at)
            VALUES (:id, 'verification_status', 'unverified', 'disputed', :s, :b, now())"""),
                   {"id": sid, "s": SOURCE, "b": BY})
        db.execute(text("""
            UPDATE existing_schemes SET verification_status = 'disputed', verification_note = :n,
                   verified_by = :by, verified_at = now() WHERE id = :id"""),
                   {"id": sid, "n": note, "by": "auto:" + BY})
        run.note("flags", id=sid, name=name, postcode=pc, address=addr)
    run.counts["flags: head-office address"] += len(rows)
    print(f"  {'rows carrying an operator head-office address -> review':58s} {len(rows):>7,}")
    for sid, name, pc, addr in rows[:5]:
        print(f"      #{sid} {name[:30]!r} at {addr[:40]!r} {pc}")


STEPS = {"classify": step_classify, "codes": step_codes, "postcodes": step_postcodes, "coords": step_coords,
         "names": step_names, "duplicates": step_duplicates, "companies": step_companies, "flags": step_flags}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="+", choices=list(STEPS), default=None)
    ap.add_argument("--company-scope", choices=("operator", "all"), default="operator",
                    help="merge spelling variants only where one is an operator (default), or for every role")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    run = Run(db, args.apply)
    print("APPLYING" if args.apply else "DRY RUN (nothing is written)")
    try:
        for name in args.only or list(STEPS):
            print(f"\n== {name}")
            if name == "companies":
                step_companies(run, args.company_scope)
            else:
                STEPS[name](run)
            if args.apply:
                db.commit()
            else:
                db.rollback()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    if args.apply:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        path = LOG_DIR / f"cleanup_{datetime.datetime.now():%Y%m%d_%H%M%S}.json"
        path.write_text(json.dumps({"applied_at": datetime.datetime.now().isoformat(timespec="seconds"),
                                    "counts": run.counts, "changes": run.log}, indent=1, default=str),
                        encoding="utf-8")
        print(f"\ncommitted; run log {path}")
    else:
        print("\ndry run: every step rolled back")


if __name__ == "__main__":
    main()
