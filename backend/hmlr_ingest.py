"""HMLR CCOD/OCOD ingest — title-level corporate ownership (step 8).

Downloads the latest monthly full file via the Use Land & Property Data
API and bulk-loads it into the ``title_ownership`` table.

Usage:
    python hmlr_ingest.py --dataset ocod            # download + load
    python hmlr_ingest.py --dataset ccod
    python hmlr_ingest.py --dataset ccod --load-only  # zip already on disk
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import httpx
from sqlalchemy import create_engine, text

DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
)
API = "https://use-land-property-data.service.gov.uk/api/v1"
DATA = Path(__file__).parent / "data" / "hmlr"

DDL = """
CREATE TABLE IF NOT EXISTS title_ownership (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,                -- ccod / ocod
    title_number TEXT NOT NULL,
    tenure TEXT,
    address TEXT,
    district TEXT,
    county TEXT,
    region TEXT,
    postcode TEXT,
    pc_key TEXT,                         -- postcode, upper, no spaces
    price_paid BIGINT,
    proprietor_name_1 TEXT, ch_number_1 TEXT, category_1 TEXT, country_1 TEXT,
    proprietor_name_2 TEXT, ch_number_2 TEXT, category_2 TEXT, country_2 TEXT,
    proprietor_name_3 TEXT, ch_number_3 TEXT, category_3 TEXT, country_3 TEXT,
    proprietor_name_4 TEXT, ch_number_4 TEXT, category_4 TEXT, country_4 TEXT,
    date_added DATE,
    file_month TEXT
);
"""


def api_key() -> str:
    key = os.environ.get("HMLR_API_KEY", "")
    if not key:
        for line in (Path(__file__).parent / ".env").read_text().splitlines():
            if line.startswith("HMLR_API_KEY="):
                key = line.split("=", 1)[1].strip()
    if not key:
        sys.exit("HMLR_API_KEY not set")
    return key


def latest_full(client: httpx.Client, dataset: str) -> str:
    r = client.get(f"{API}/datasets/{dataset}")
    r.raise_for_status()
    for f in r.json()["result"].get("resources", []):
        if "_FULL_" in f.get("file_name", ""):
            return f["file_name"]
    sys.exit(f"no FULL file listed for {dataset}")


def download(dataset: str, key: str) -> Path:
    DATA.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(headers={"Authorization": key}, timeout=60)
    fname = latest_full(client, dataset)
    out = DATA / fname
    if out.exists() and out.stat().st_size > 1024:
        print(f"[skip] {fname} already downloaded")
        return out
    r = client.get(f"{API}/datasets/{dataset}/{fname}")
    r.raise_for_status()
    url = r.json()["result"]["download_url"]
    print(f"[dl] {fname} ...")
    with httpx.Client(timeout=None) as dl, open(out, "wb") as fh:
        with dl.stream("GET", url) as resp:
            resp.raise_for_status()
            done = 0
            for chunk in resp.iter_bytes(1 << 20):
                fh.write(chunk)
                done += len(chunk)
                if done % (200 << 20) < (1 << 20):
                    print(f"     {done >> 20} MB")
    print(f"[dl] done: {out.stat().st_size >> 20} MB")
    return out


def load(zip_path: Path, dataset: str):
    import psycopg2

    file_month = "".join(c for c in zip_path.stem.split("FULL_")[-1] if c.isdigit() or c == "_")
    conn = psycopg2.connect(DB_URL.replace("postgresql://", "postgres://"))
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute("DELETE FROM title_ownership WHERE source = %s", (dataset,))

    cols = ("source,title_number,tenure,address,district,county,region,"
            "postcode,pc_key,price_paid,"
            "proprietor_name_1,ch_number_1,category_1,country_1,"
            "proprietor_name_2,ch_number_2,category_2,country_2,"
            "proprietor_name_3,ch_number_3,category_3,country_3,"
            "proprietor_name_4,ch_number_4,category_4,country_4,"
            "date_added,file_month")

    def rows():
        with zipfile.ZipFile(zip_path) as z:
            csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
            with z.open(csv_name) as raw:
                txt = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
                rd = csv.DictReader(txt)
                for rec in rd:
                    tn = (rec.get("Title Number") or "").strip()
                    if not tn or tn.startswith("Row Count"):
                        continue
                    pc = (rec.get("Postcode") or "").strip()
                    price = (rec.get("Price Paid") or "").strip()
                    date_add = (rec.get("Date Proprietor Added") or "").strip()
                    # CCOD has no country columns; OCOD does
                    def g(label, i):
                        return (rec.get(f"{label} ({i})") or "").strip()
                    out = [
                        dataset, tn,
                        (rec.get("Tenure") or "").strip(),
                        (rec.get("Property Address") or "").strip(),
                        (rec.get("District") or "").strip(),
                        (rec.get("County") or "").strip(),
                        (rec.get("Region") or "").strip(),
                        pc, pc.upper().replace(" ", ""),
                        price if price.isdigit() else "",
                    ]
                    for i in (1, 2, 3, 4):
                        out += [
                            g("Proprietor Name", i),
                            g("Company Registration No.", i),
                            g("Proprietorship Category", i),
                            g("Country Incorporated", i),
                        ]
                    # dd-mm-yyyy -> yyyy-mm-dd
                    if date_add and len(date_add.split("-")) == 3:
                        p = date_add.split("-")
                        date_add = f"{p[2]}-{p[1]}-{p[0]}" if len(p[0]) == 2 else date_add
                    out += [date_add, file_month]
                    yield out

    def clean(c: str) -> str:
        if not c:
            return r"\N"
        return (c.replace("\\", "/").replace("\t", " ")
                 .replace("\n", " ").replace("\r", " "))

    buf = io.StringIO()
    n = 0
    copy_sql = (f"COPY title_ownership ({cols}) "
                f"FROM STDIN WITH (FORMAT text, NULL '\\N')")
    for row in rows():
        buf.write("\t".join(clean(c) for c in row) + "\n")
        n += 1
        if n % 500_000 == 0:
            buf.seek(0)
            cur.copy_expert(copy_sql, buf)
            buf = io.StringIO()
            print(f"  ...{n:,} rows")
    buf.seek(0)
    cur.copy_expert(copy_sql, buf)
    print(f"  loaded {n:,} rows for {dataset}")

    cur.execute("CREATE INDEX IF NOT EXISTS ix_title_ownership_pc ON title_ownership (pc_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_title_ownership_p1 ON title_ownership (LOWER(proprietor_name_1))")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_title_ownership_ch1 ON title_ownership (ch_number_1)")
    conn.commit()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", choices=["ccod", "ocod"], required=True)
    ap.add_argument("--load-only", action="store_true")
    args = ap.parse_args()

    if args.load_only:
        zips = sorted(DATA.glob(f"{args.dataset.upper()}_FULL_*.zip"))
        if not zips:
            sys.exit("no zip on disk")
        zp = zips[-1]
    else:
        zp = download(args.dataset, api_key())
    load(zp, args.dataset)
    print("done")


if __name__ == "__main__":
    main()
