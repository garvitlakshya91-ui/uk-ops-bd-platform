"""Merge city research work from one database into another (e.g. a working
copy into the production crawl database), without losing anything in the target.

What moves, per city (--cities):
  planning_applications  upsert by (reference, council). Rows the target already
                         has keep their own values; only the pipeline columns
                         (pbsa_beds, statuses, envelopes, delivery years...) and
                         empty fields are filled from the source. New rows are
                         inserted.
  existing_schemes       PBSA census schemes only. Each is matched to the target's
                         own PBSA schemes in the same council (name tokens +
                         postcode, conservative); a match receives the census
                         fields (beds_total, build_year, nominations,
                         operating_status, field_provenance) and the target's
                         other values are kept; unmatched schemes are inserted.
                         Target PBSA schemes the census does not contain are left
                         untouched (the report lists them as unreconciled).
  scheme_rents           inserted against the mapped schemes, history preserved;
                         where the source has newer current rows from the same
                         feed, the target's older current rows are superseded.
  scheme_observations, scheme_room_types, scheme_availability, scheme_events,
  transactions           inserted against the mapped schemes (no duplicates).
Shared reference data:
  companies (operators the census uses), institutions, hesa_enrolments,
  hesa_term_time_accommodation, maintenance_loans, yield_benchmarks,
  visa_issuances      upserted by their natural keys.

Not moved: users, BTR/other scheme types, anything outside --cities.

Idempotent: re-running finds its own earlier inserts (schemes carry a
field_provenance "_merge" key; other rows are de-duplicated on content).
Runs in one transaction on the target; --dry-run rolls back.

The target must already be at the source's Alembic revision
(run `alembic upgrade head` against it first).

Usage:
  python merge_city_work.py \
      --source postgresql://postgres:postgres@localhost:5432/uk_ops_bd_sandbox \
      --target postgresql://postgres:postgres@localhost:5432/uk_ops_bd \
      --cities Birmingham Exeter [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import re
from collections import defaultdict

from sqlalchemy import MetaData, Table, and_, create_engine, select, text

from app.scrapers.scheme_matching import name_score, norm_name, norm_pc

PIPELINE_COLUMNS = [
    "pbsa_beds", "expected_delivery_year", "construction_status", "construction_evidence_at",
    "construction_source", "permission_type", "parent_reference", "superseded_by_reference",
    "delivery_status", "delivery_status_basis", "delivery_status_evidence", "delivery_status_at",
    "beds_max", "beds_basis", "delivery_year_basis",
]
APP_FILL_IF_NULL = [
    "description", "address", "postcode", "ward", "applicant_name", "agent_name",
    "application_type", "status", "decision", "decision_date", "submitted_date",
    "validated_date", "submission_date", "latitude", "longitude", "portal_url",
    "documents_url", "total_units", "num_units",
]
CENSUS_COLUMNS = ["beds_total", "build_year", "nominations", "operating_status"]
SCHEME_FILL_IF_NULL = ["postcode", "address", "latitude", "longitude", "lat", "lng",
                       "completion_date", "total_units", "num_units"]
COMPANY_FKS = ["operator_company_id", "owner_company_id", "asset_manager_company_id",
               "landlord_company_id"]


def norm_company(name: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


OP_STOP = {"ltd", "limited", "plc", "the", "student", "students", "living", "accommodation",
           "group", "uk", "properties", "property", "hfs", "and", "co", "of"}


def op_tokens(name: str | None) -> set[str]:
    """Operator name words that identify the brand ('Collegiate AC' ~ 'Collegiate')."""
    return {w for w in re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
            if w not in OP_STOP and len(w) > 1}


def outward(pc: str) -> str:
    """Postcode district from a normalised postcode: 'B47UP' -> 'B4', 'EX44BG' -> 'EX4'."""
    return pc[:-3] if len(pc) > 3 else pc


def common(row: dict, table: Table, drop=("id",)) -> dict:
    cols = set(table.c.keys())
    return {k: v for k, v in row.items() if k in cols and k not in drop}


class Merger:
    def __init__(self, src_url: str, tgt_url: str, cities: list[str], label: str,
                 verbose: bool = False):
        self.verbose = verbose
        self.src = create_engine(src_url)
        self.tgt = create_engine(tgt_url)
        self.cities = cities
        self.label = label
        self.sm, self.tm = MetaData(), MetaData()
        names = ["councils", "companies", "existing_schemes", "planning_applications",
                 "scheme_rents", "scheme_observations", "scheme_room_types",
                 "scheme_availability", "scheme_events", "transactions", "institutions",
                 "hesa_enrolments", "hesa_term_time_accommodation", "maintenance_loans",
                 "yield_benchmarks", "visa_issuances", "alembic_version"]
        self.sm.reflect(self.src, only=lambda n, _: n in names)
        self.tm.reflect(self.tgt, only=lambda n, _: n in names)
        self.stats: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.council_map: dict[int, int] = {}
        self.company_map: dict[int, int] = {}
        self.scheme_map: dict[int, int] = {}
        self.inst_map: dict[int, int] = {}

    def s(self, name: str) -> Table:
        return self.sm.tables[name]

    def t(self, name: str) -> Table:
        return self.tm.tables[name]

    # ------------------------------------------------------------------ checks
    def check_revisions(self, sc, tc) -> None:
        sv = sc.execute(text("select version_num from alembic_version")).scalar()
        tv = tc.execute(text("select version_num from alembic_version")).scalar()
        if sv != tv:
            raise SystemExit(f"target is at {tv}, source at {sv}: run `alembic upgrade head` "
                             f"against the target first")
        print(f"schema revision {tv} on both sides")

    # --------------------------------------------------------------- councils
    def map_councils(self, sc, tc) -> None:
        src = {r.id: r.name for r in sc.execute(select(self.s("councils").c.id, self.s("councils").c.name))}
        tgt = {r.name: r.id for r in tc.execute(select(self.t("councils").c.id, self.t("councils").c.name))}
        for sid, name in src.items():
            if name in tgt:
                self.council_map[sid] = tgt[name]
        missing = [c for c in self.cities if c not in tgt]
        if missing:
            raise SystemExit(f"cities not in target councils: {missing}")
        self.src_city_ids = [sid for sid, n in src.items() if n in self.cities]

    # -------------------------------------------------------------- companies
    def map_companies(self, sc, tc, company_ids: set[int]) -> None:
        if not company_ids:
            return
        sct, tct = self.s("companies"), self.t("companies")
        rows = sc.execute(select(sct).where(sct.c.id.in_(company_ids))).mappings().all()
        for r in rows:
            hit = None
            if r.get("companies_house_number"):
                hit = tc.execute(select(tct.c.id, tct.c.company_type).where(
                    tct.c.companies_house_number == r["companies_house_number"])).first()
            if hit is None:
                nn = r.get("normalized_name") or norm_company(r["name"])
                hit = tc.execute(select(tct.c.id, tct.c.company_type).where(
                    tct.c.normalized_name == nn).order_by(tct.c.id).limit(1)).first()
            if hit is not None:
                self.company_map[r["id"]] = hit.id
                if r.get("company_type") == "University" and hit.company_type != "University":
                    if hit.company_type in (None, "", "Operator", "Unknown"):
                        tc.execute(tct.update().where(tct.c.id == hit.id).values(company_type="University"))
                        self.stats["companies"]["retyped_university"] += 1
                self.stats["companies"]["matched"] += 1
            else:
                vals = common(dict(r), tct)
                vals.setdefault("normalized_name", norm_company(r["name"]))
                new_id = tc.execute(tct.insert().values(**vals).returning(tct.c.id)).scalar()
                self.company_map[r["id"]] = new_id
                self.stats["companies"]["inserted"] += 1

    # ----------------------------------------------------------- reference data
    def merge_reference(self, sc, tc) -> None:
        # institutions (unique name)
        sit, tit = self.s("institutions"), self.t("institutions")
        for r in sc.execute(select(sit)).mappings():
            if r["council_id"] not in self.src_city_ids:
                continue
            hit = tc.execute(select(tit.c.id).where(tit.c.name == r["name"])).scalar()
            if hit is None:
                vals = common(dict(r), tit)
                vals["council_id"] = self.council_map.get(r["council_id"])
                hit = tc.execute(tit.insert().values(**vals).returning(tit.c.id)).scalar()
                self.stats["institutions"]["inserted"] += 1
            else:
                self.stats["institutions"]["matched"] += 1
            self.inst_map[r["id"]] = hit
        # hesa_enrolments (institution, year, level): official tables win
        sht, tht = self.s("hesa_enrolments"), self.t("hesa_enrolments")
        for r in sc.execute(select(sht).where(sht.c.institution_id.in_(list(self.inst_map) or [0]))).mappings():
            iid = self.inst_map[r["institution_id"]]
            cur = tc.execute(select(tht.c.id, tht.c.full_time_students, tht.c.source).where(and_(
                tht.c.institution_id == iid, tht.c.academic_year == r["academic_year"],
                tht.c.level == r["level"]))).first()
            if cur is None:
                vals = common(dict(r), tht)
                vals["institution_id"] = iid
                tc.execute(tht.insert().values(**vals))
                self.stats["hesa_enrolments"]["inserted"] += 1
            elif (cur.full_time_students != r["full_time_students"]
                  and (r["source"] or "").startswith("hesa_table")):
                tc.execute(tht.update().where(tht.c.id == cur.id).values(
                    full_time_students=r["full_time_students"], source=r["source"]))
                self.stats["hesa_enrolments"]["updated"] += 1
        # natural-key tables copied when absent
        for name, keys in (("hesa_term_time_accommodation", ["academic_year", "entrant_marker", "accommodation"]),
                           ("maintenance_loans", ["academic_year", "region"]),
                           ("yield_benchmarks", ["segment", "as_of", "source"]),
                           ("visa_issuances", ["quarter", "nationality", "source"])):
            st, tt = self.s(name), self.t(name)
            for r in sc.execute(select(st)).mappings():
                cond = and_(*[tt.c[k] == r[k] for k in keys])
                if tc.execute(select(tt.c.id).where(cond)).first() is None:
                    tc.execute(tt.insert().values(**common(dict(r), tt)))
                    self.stats[name]["inserted"] += 1

    # ---------------------------------------------------------------- schemes
    def merge_schemes(self, sc, tc) -> None:
        sst, tst = self.s("existing_schemes"), self.t("existing_schemes")
        src_rows = sc.execute(select(sst).where(and_(
            sst.c.council_id.in_(self.src_city_ids), sst.c.scheme_type == "PBSA"))).mappings().all()
        self.map_companies(sc, tc, {r[k] for r in src_rows for k in COMPANY_FKS if r.get(k)})
        by_council: dict[int, list] = defaultdict(list)
        for r in src_rows:
            by_council[r["council_id"]].append(r)
        for scid, rows in by_council.items():
            tcid = self.council_map[scid]
            targets = tc.execute(select(tst).where(and_(
                tst.c.council_id == tcid, tst.c.scheme_type == "PBSA"))).mappings().all()
            prior = {}
            for t in targets:
                m = (t["field_provenance"] or {}).get("_merge")
                if m and m.get("label") == self.label:
                    prior[m["source_id"]] = t["id"]
            candidates = [t for t in targets if t["operating_status"] is None
                          and not (t["field_provenance"] or {}).get("_merge")]
            src_ops = self._operator_names(sc, self.s("companies"), rows)
            tgt_ops = self._operator_names(tc, self.t("companies"), candidates)
            used: set[int] = set()
            # strongest pairs first, so a weak match never takes a better one's target
            pairs = []
            for r in rows:
                if r["id"] in prior:
                    continue
                rt, rpc = norm_name(r["name"]), norm_pc(r["postcode"])
                for t in candidates:
                    tt_, tpc = norm_name(t["name"]), norm_pc(t["postcode"])
                    ns = name_score(rt, tt_)
                    same_pc = bool(rpc) and rpc == tpc
                    same_district = bool(rpc and tpc) and outward(rpc) == outward(tpc)
                    s_beds = r["beds_total"] or 0
                    t_beds = t.get("beds_total") or t.get("num_units") or t.get("total_units") or 0
                    beds_agree = bool(s_beds and t_beds) and 0.75 <= t_beds / s_beds <= 1.33
                    op_agree = bool(op_tokens(src_ops.get(r["id"])) & op_tokens(tgt_ops.get(t["id"])))
                    if rpc and tpc:
                        # Same postcode: any shared name token, or a generic target name
                        # ("Birmingham") or an agreeing bed count. Different postcodes
                        # (feeds disagree on them): a strong name match backed by the
                        # same district, operator or bed count.
                        ok = (same_pc and (ns > 0 or not tt_ or beds_agree)) or \
                             (ns >= 0.66 and (same_district or op_agree or beds_agree))
                    else:
                        # One side has no postcode: exact name and the same operator only.
                        ok = ns >= 0.99 and op_agree
                    if ok:
                        score = ns + (0.5 if same_pc else 0) + (0.25 if beds_agree else 0) \
                            + (0.2 if op_agree else 0) + (0.1 if same_district else 0)
                        pairs.append((score, r["id"], t["id"]))
            pairs.sort(reverse=True)
            matched: dict[int, int] = {}
            for _, sid, tid in pairs:
                if sid in matched or tid in used:
                    continue
                matched[sid] = tid
                used.add(tid)
            tmap = {t["id"]: t for t in targets}
            if self.verbose:
                srcmap = {r["id"]: r for r in rows}
                print(f"  council {tcid}: {len(matched)} census schemes matched to existing PBSA records")
                for sid, tid in sorted(matched.items(), key=lambda kv: srcmap[kv[0]]["name"]):
                    a, b = srcmap[sid], tmap[tid]
                    print(f"    {a['name'][:34]:34s} {a['postcode'] or '':9s} -> {b['name'][:34]:34s} {b['postcode'] or '':9s} [{b['source']}]")
                for t in candidates:
                    if t["id"] not in used:
                        print(f"    unreconciled: {t['name'][:40]:40s} {t['postcode'] or '':9s} [{t['source']}] units {t.get('num_units') or t.get('total_units') or '-'}")
            for r in rows:
                prov = dict(r["field_provenance"] or {})
                if "operator" not in prov and src_ops.get(r["id"]):
                    # Carry the census brand so the report labels the scheme as
                    # the census does, whatever company the target links.
                    prov["operator"] = {"value": src_ops[r["id"]], "source": r["source"] or "census"}
                prov["_merge"] = {"label": self.label, "source_id": r["id"],
                                  "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
                fks = {k: self.company_map.get(r[k]) for k in COMPANY_FKS if r.get(k)}
                tid = prior.get(r["id"]) or matched.get(r["id"])
                if tid:
                    t = tmap[tid]
                    vals = {k: r[k] for k in CENSUS_COLUMNS}
                    tprov = dict(t["field_provenance"] or {})
                    tprov.update(prov)
                    vals["field_provenance"] = tprov
                    vals["scheme_type"] = "PBSA"
                    for k in SCHEME_FILL_IF_NULL:
                        if k in tst.c and t.get(k) in (None, "") and r.get(k) not in (None, ""):
                            vals[k] = r[k]
                    for k, v in fks.items():
                        if t.get(k) is None or (k == "operator_company_id" and self._is_university(sc, r)):
                            vals[k] = v
                    tc.execute(tst.update().where(tst.c.id == tid).values(**vals))
                    self.scheme_map[r["id"]] = tid
                    self.stats["existing_schemes"]["prior" if r["id"] in prior else "matched"] += 1
                else:
                    vals = common(dict(r), tst)
                    vals.update(fks)
                    vals["council_id"] = tcid
                    vals["field_provenance"] = prov
                    new_id = tc.execute(tst.insert().values(**vals).returning(tst.c.id)).scalar()
                    self.scheme_map[r["id"]] = new_id
                    self.stats["existing_schemes"]["inserted"] += 1
            unreconciled = [t for t in candidates if t["id"] not in used]
            self.stats["existing_schemes"]["target_pbsa_left_unreconciled"] += len(unreconciled)

    @staticmethod
    def _operator_names(conn, companies: Table, rows) -> dict[int, str]:
        ids = {r["operator_company_id"] for r in rows if r.get("operator_company_id")}
        if not ids:
            return {}
        names = {c.id: c.name for c in conn.execute(
            select(companies.c.id, companies.c.name).where(companies.c.id.in_(ids)))}
        return {r["id"]: names.get(r["operator_company_id"]) for r in rows}

    def _is_university(self, sc, r) -> bool:
        cid = r.get("operator_company_id")
        if not cid:
            return False
        ct = sc.execute(select(self.s("companies").c.company_type).where(
            self.s("companies").c.id == cid)).scalar()
        return ct == "University"

    # ------------------------------------------------------------ scheme rows
    def merge_scheme_rows(self, sc, tc) -> None:
        sids = list(self.scheme_map)
        if not sids:
            return
        # rents: supersede older current target rows from the same feed, then insert
        srt, trt = self.s("scheme_rents"), self.t("scheme_rents")
        src_rents = sc.execute(select(srt).where(srt.c.scheme_id.in_(sids))).mappings().all()
        newest: dict[tuple[int, str], datetime.datetime] = {}
        for r in src_rents:
            if r["is_current"]:
                k = (self.scheme_map[r["scheme_id"]], r["source"])
                ts = r["scraped_at"] or r["created_at"]
                if ts and (k not in newest or ts > newest[k]):
                    newest[k] = ts
        now = datetime.datetime.now(datetime.timezone.utc)
        for (tid, source), ts in newest.items():
            res = tc.execute(trt.update().where(and_(
                trt.c.scheme_id == tid, trt.c.source == source, trt.c.is_current.is_(True),
                trt.c.scraped_at < ts)).values(is_current=False, superseded_at=now))
            self.stats["scheme_rents"]["target_superseded"] += res.rowcount or 0
        self._insert_children(tc, "scheme_rents", src_rents,
                              ["scheme_id", "source", "room_type", "sub_classification",
                               "academic_year", "rent_per_week", "scraped_at"])
        for name, keys in (("scheme_observations", ["scheme_id", "field", "source", "observed_at", "value_text"]),
                           ("scheme_room_types", ["scheme_id", "room_type", "sub_classification", "source", "observed_at"]),
                           ("scheme_availability", ["scheme_id", "captured_at", "state", "source", "source_reference"]),
                           ("scheme_events", ["scheme_id", "event_type", "event_date", "source"])):
            st = self.s(name)
            rows = sc.execute(select(st).where(st.c.scheme_id.in_(sids))).mappings().all()
            self._insert_children(tc, name, rows, keys)
        # transactions: scheme and council remapped
        stt, ttt = self.s("transactions"), self.t("transactions")
        for r in sc.execute(select(stt).where(stt.c.council_id.in_(self.src_city_ids))).mappings():
            vals = common(dict(r), ttt)
            vals["scheme_id"] = self.scheme_map.get(r["scheme_id"]) if r["scheme_id"] else None
            vals["council_id"] = self.council_map.get(r["council_id"])
            cond = and_(ttt.c.asset_name == r["asset_name"], ttt.c.transaction_date == r["transaction_date"],
                        ttt.c.source == r["source"])
            if tc.execute(select(ttt.c.id).where(cond)).first() is None:
                tc.execute(ttt.insert().values(**vals))
                self.stats["transactions"]["inserted"] += 1

    def _insert_children(self, tc, name: str, rows, keys: list[str]) -> None:
        tt = self.t(name)
        for r in rows:
            vals = common(dict(r), tt)
            vals["scheme_id"] = self.scheme_map[r["scheme_id"]]
            cond = and_(*[(tt.c[k] == vals[k]) if vals.get(k) is not None else tt.c[k].is_(None)
                          for k in keys])
            if tc.execute(select(tt.c.id).where(cond).limit(1)).first() is None:
                tc.execute(tt.insert().values(**vals))
                self.stats[name]["inserted"] += 1
            else:
                self.stats[name]["already_present"] += 1

    # ------------------------------------------------------ planning registry
    def merge_applications(self, sc, tc) -> None:
        sat, tat = self.s("planning_applications"), self.t("planning_applications")
        for scid in self.src_city_ids:
            tcid = self.council_map[scid]
            existing = {r.reference: r.id for r in tc.execute(select(tat.c.reference, tat.c.id).where(
                tat.c.council_id == tcid))}
            for r in sc.execute(select(sat).where(sat.c.council_id == scid)).mappings():
                tid = existing.get(r["reference"])
                if tid is None:
                    vals = common(dict(r), tat)
                    vals["council_id"] = tcid
                    vals["applicant_company_id"] = self.company_map.get(r["applicant_company_id"])
                    vals["agent_company_id"] = self.company_map.get(r["agent_company_id"])
                    tc.execute(tat.insert().values(**vals))
                    self.stats["planning_applications"]["inserted"] += 1
                    continue
                cur = tc.execute(select(tat).where(tat.c.id == tid)).mappings().first()
                vals = {k: r[k] for k in PIPELINE_COLUMNS
                        if k in tat.c and r.get(k) is not None and r.get(k) != cur.get(k)}
                for k in APP_FILL_IF_NULL:
                    if k in tat.c and cur.get(k) in (None, "") and r.get(k) not in (None, ""):
                        vals[k] = r[k]
                if r.get("is_pbsa") and not cur.get("is_pbsa"):
                    vals["is_pbsa"] = True
                if (cur.get("scheme_type") in (None, "", "Unknown")
                        and r.get("scheme_type") not in (None, "", "Unknown")):
                    vals["scheme_type"] = r["scheme_type"]
                if vals:
                    tc.execute(tat.update().where(tat.c.id == tid).values(**vals))
                    self.stats["planning_applications"]["updated"] += 1
                else:
                    self.stats["planning_applications"]["unchanged"] += 1

    # ------------------------------------------------------------------- run
    def run(self, dry_run: bool) -> None:
        with self.src.connect() as sc, self.tgt.connect() as tc:
            trans = tc.begin()
            try:
                self.check_revisions(sc, tc)
                self.map_councils(sc, tc)
                self.merge_reference(sc, tc)
                self.merge_schemes(sc, tc)
                self.merge_scheme_rows(sc, tc)
                self.merge_applications(sc, tc)
                # keep sequences ahead of any explicitly carried ids
                for name in ("existing_schemes", "planning_applications", "companies"):
                    tc.execute(text(f"SELECT setval(pg_get_serial_sequence('{name}', 'id'), "
                                    f"(SELECT COALESCE(MAX(id), 1) FROM {name}))"))
                for table, st in sorted(self.stats.items()):
                    print(f"  {table:30s} " + ", ".join(f"{k} {v:,}" for k, v in st.items()))
                if dry_run:
                    trans.rollback()
                    print("dry run: rolled back, nothing written")
                else:
                    trans.commit()
                    print("committed")
            except Exception:
                trans.rollback()
                raise


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--cities", nargs="+", required=True)
    ap.add_argument("--label", default="city_work_2026-10-08",
                    help="tag written into merged schemes' provenance (keep it constant on re-runs)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", action="store_true", help="list every scheme match and unreconciled record")
    args = ap.parse_args()
    print(f"merging {', '.join(args.cities)}")
    Merger(args.source, args.target, args.cities, args.label, args.verbose).run(args.dry_run)


if __name__ == "__main__":
    main()
