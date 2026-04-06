"""
Land Registry Price Paid Data scraper.

Downloads monthly CSV updates from HM Land Registry's Price Paid dataset,
filters for new-build transactions, clusters them by postcode to detect
multi-unit residential developments (5+ sales in a 12-month window at the
same postcode), and creates/updates ExistingScheme records.

Data source:
    https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

CSV columns (no header row in the standard download):
    0  transaction_id       {GUID}
    1  price                int
    2  date_of_transfer     YYYY-MM-DD 00:00
    3  postcode             e.g. SW1A 2AA
    4  property_type        D=Detached, S=Semi, T=Terraced, F=Flat, O=Other
    5  old_new              Y=New build, N=Established
    6  duration             F=Freehold, L=Leasehold
    7  paon                 Primary addressable object name
    8  saon                 Secondary addressable object name
    9  street
   10  locality
   11  town
   12  district
   13  county
   14  ppd_category         A=Standard, B=Additional
   15  record_status        A=Addition, C=Change, D=Delete
"""

from __future__ import annotations

import csv
import io
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Download URLs
# ---------------------------------------------------------------------------

# Complete dataset (~4 GB) — use for initial backfill only
PP_COMPLETE_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1"
    ".amazonaws.com/pp-complete.csv"
)

# Monthly update file — small, for recurring ingestion
PP_MONTHLY_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1"
    ".amazonaws.com/pp-monthly-update-new-version.csv"
)

# CSV column indices
COL_TRANSACTION_ID = 0
COL_PRICE = 1
COL_DATE = 2
COL_POSTCODE = 3
COL_PROPERTY_TYPE = 4
COL_OLD_NEW = 5
COL_DURATION = 6
COL_PAON = 7
COL_SAON = 8
COL_STREET = 9
COL_LOCALITY = 10
COL_TOWN = 11
COL_DISTRICT = 12
COL_COUNTY = 13
COL_PPD_CATEGORY = 14
COL_RECORD_STATUS = 15

# Property type labels
PROPERTY_TYPE_MAP = {
    "D": "Detached",
    "S": "Semi-Detached",
    "T": "Terraced",
    "F": "Flat/Maisonette",
    "O": "Other",
}

# Minimum cluster size to count as a multi-unit development
MIN_CLUSTER_SIZE = 5

# Rolling window for clustering (months)
CLUSTER_WINDOW_DAYS = 365


class PricePaidScraper:
    """Download and parse Land Registry Price Paid CSV data."""

    def __init__(
        self,
        use_monthly: bool = True,
        local_csv_path: str | None = None,
    ) -> None:
        """
        Parameters
        ----------
        use_monthly : bool
            If True (default), download the small monthly update file.
            Set to False to use the complete (~4 GB) dataset for backfill.
        local_csv_path : str | None
            If provided, read from a local file instead of downloading.
        """
        self.use_monthly = use_monthly
        self.local_csv_path = local_csv_path
        self.log = logger.bind(scraper="PricePaidScraper")

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def _download_csv(self) -> str:
        """Download the CSV to a temp file and return its path."""
        url = PP_MONTHLY_URL if self.use_monthly else PP_COMPLETE_URL
        self.log.info("price_paid_download_start", url=url)

        tmp = tempfile.NamedTemporaryFile(
            delete=False, suffix=".csv", prefix="pp_"
        )
        try:
            with httpx.stream("GET", url, timeout=600, follow_redirects=True) as resp:
                resp.raise_for_status()
                total = 0
                for chunk in resp.iter_bytes(chunk_size=1024 * 256):
                    tmp.write(chunk)
                    total += len(chunk)
            tmp.close()
            self.log.info(
                "price_paid_download_complete",
                bytes_downloaded=total,
                path=tmp.name,
            )
            return tmp.name
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------

    def _parse_csv(self, csv_path: str) -> list[dict[str, Any]]:
        """
        Parse the Price Paid CSV file and return only new-build transactions.

        Returns a list of dicts with cleaned fields.
        """
        self.log.info("price_paid_parse_start", path=csv_path)
        new_builds: list[dict[str, Any]] = []
        total_rows = 0
        skipped = 0

        with open(csv_path, "r", encoding="utf-8", errors="replace") as fh:
            reader = csv.reader(fh)
            for row in reader:
                total_rows += 1
                if len(row) < 16:
                    skipped += 1
                    continue

                # Filter: new builds only
                if row[COL_OLD_NEW].strip().upper() != "Y":
                    continue

                # Skip deletion records
                if row[COL_RECORD_STATUS].strip().upper() == "D":
                    continue

                postcode = row[COL_POSTCODE].strip().upper()
                if not postcode:
                    skipped += 1
                    continue

                try:
                    price = int(row[COL_PRICE].strip().strip('"'))
                except (ValueError, IndexError):
                    skipped += 1
                    continue

                # Parse date — format is "YYYY-MM-DD 00:00"
                date_str = row[COL_DATE].strip().strip('"')
                try:
                    transfer_date = datetime.strptime(
                        date_str[:10], "%Y-%m-%d"
                    ).date()
                except ValueError:
                    skipped += 1
                    continue

                transaction_id = row[COL_TRANSACTION_ID].strip().strip('"').strip("{}")

                # Build address from components
                parts = [
                    row[COL_SAON].strip().strip('"'),
                    row[COL_PAON].strip().strip('"'),
                    row[COL_STREET].strip().strip('"'),
                    row[COL_LOCALITY].strip().strip('"'),
                ]
                address_line = ", ".join(p for p in parts if p)

                new_builds.append({
                    "transaction_id": transaction_id,
                    "price": price,
                    "transfer_date": transfer_date,
                    "postcode": postcode,
                    "property_type": row[COL_PROPERTY_TYPE].strip().upper(),
                    "duration": row[COL_DURATION].strip().upper(),
                    "address": address_line,
                    "town": row[COL_TOWN].strip().strip('"'),
                    "district": row[COL_DISTRICT].strip().strip('"'),
                    "county": row[COL_COUNTY].strip().strip('"'),
                    "ppd_category": row[COL_PPD_CATEGORY].strip().upper(),
                })

        self.log.info(
            "price_paid_parse_complete",
            total_rows=total_rows,
            new_builds=len(new_builds),
            skipped=skipped,
        )
        return new_builds

    # ------------------------------------------------------------------
    # Cluster into development schemes
    # ------------------------------------------------------------------

    @staticmethod
    def _cluster_by_postcode(
        transactions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Group new-build transactions by postcode and detect multi-unit
        developments: 5+ sales at the same postcode within a 12-month
        rolling window.

        Returns a list of detected scheme dicts ready for DB persistence.
        """
        # Group by postcode
        by_postcode: dict[str, list[dict]] = defaultdict(list)
        for txn in transactions:
            by_postcode[txn["postcode"]].append(txn)

        schemes: list[dict[str, Any]] = []

        for postcode, txns in by_postcode.items():
            # Sort by date
            txns.sort(key=lambda t: t["transfer_date"])

            # Sliding window: find the densest 12-month window
            best_window: list[dict] = []
            for i, txn in enumerate(txns):
                window_end = txn["transfer_date"] + timedelta(days=CLUSTER_WINDOW_DAYS)
                window = [
                    t for t in txns[i:]
                    if t["transfer_date"] <= window_end
                ]
                if len(window) > len(best_window):
                    best_window = window

            if len(best_window) < MIN_CLUSTER_SIZE:
                continue

            # Derive scheme info from the cluster
            prices = [t["price"] for t in best_window]
            avg_price = int(mean(prices))
            total_units = len(best_window)

            # Use the most common property type for scheme classification
            type_counts: dict[str, int] = defaultdict(int)
            for t in best_window:
                type_counts[t["property_type"]] += 1
            dominant_type = max(type_counts, key=type_counts.get)  # type: ignore[arg-type]

            # Estimate scheme type
            if dominant_type == "F":
                scheme_type = "Residential"  # Flats — could be BTR, but can't tell from price paid alone
            else:
                scheme_type = "Residential"

            # Determine tenure
            duration_counts: dict[str, int] = defaultdict(int)
            for t in best_window:
                duration_counts[t["duration"]] += 1
            dominant_duration = max(duration_counts, key=duration_counts.get)  # type: ignore[arg-type]
            tenure = "Leasehold" if dominant_duration == "L" else "Freehold"

            # Use the earliest transaction's address info for the scheme name
            first = best_window[0]
            street = first.get("address", "").split(",")[-1].strip() or "Unknown Street"
            town = first.get("town", "")
            scheme_name = f"New Build Development, {street}, {town}".strip(", ")
            if not scheme_name or scheme_name == "New Build Development,":
                scheme_name = f"New Build Development at {postcode}"

            # Composite address
            address = f"{first.get('address', '')}, {town}, {first.get('county', '')}"
            address = ", ".join(p.strip() for p in address.split(",") if p.strip())

            earliest_date = best_window[0]["transfer_date"]
            latest_date = best_window[-1]["transfer_date"]

            # Collect all transaction IDs for deduplication
            txn_ids = sorted(set(t["transaction_id"] for t in best_window))

            schemes.append({
                "name": scheme_name,
                "address": address,
                "postcode": postcode,
                "town": town,
                "district": first.get("district", ""),
                "county": first.get("county", ""),
                "total_units": total_units,
                "average_price": avg_price,
                "min_price": min(prices),
                "max_price": max(prices),
                "dominant_property_type": PROPERTY_TYPE_MAP.get(dominant_type, dominant_type),
                "tenure": tenure,
                "scheme_type": scheme_type,
                "earliest_sale": earliest_date,
                "latest_sale": latest_date,
                "transaction_ids": txn_ids,
                "source": "land_registry_price_paid",
                "source_reference": f"pp_cluster_{postcode}_{earliest_date.isoformat()}",
            })

        return schemes

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_and_cluster(self) -> list[dict[str, Any]]:
        """
        Download (or read local), parse, filter new builds, and cluster
        into detected development schemes.

        Returns a list of scheme dicts ready for DB ingestion.
        """
        if self.local_csv_path:
            csv_path = self.local_csv_path
        else:
            csv_path = self._download_csv()

        try:
            transactions = self._parse_csv(csv_path)
            schemes = self._cluster_by_postcode(transactions)
            self.log.info(
                "price_paid_clustering_complete",
                transactions=len(transactions),
                schemes_detected=len(schemes),
            )
            return schemes
        finally:
            # Clean up temp file if we downloaded it
            if not self.local_csv_path and csv_path:
                Path(csv_path).unlink(missing_ok=True)


def save_price_paid_schemes(
    schemes: list[dict[str, Any]],
    db: "Session",  # noqa: F821
) -> dict[str, int]:
    """
    Persist detected price-paid development schemes to existing_schemes.

    Upserts by (source_reference): updates existing records, inserts new ones.

    Parameters
    ----------
    schemes : list
        List of scheme dicts from PricePaidScraper.fetch_and_cluster().
    db : Session
        Active SQLAlchemy session.

    Returns
    -------
    dict with keys: found, new, updated, errors.
    """
    from app.models.models import ExistingScheme

    found = len(schemes)
    new = 0
    updated = 0
    errors = 0

    for scheme_data in schemes:
        try:
            source_ref = scheme_data["source_reference"]

            existing = (
                db.query(ExistingScheme)
                .filter(
                    ExistingScheme.source == "land_registry_price_paid",
                    ExistingScheme.source_reference == source_ref,
                )
                .first()
            )

            if existing:
                # Update fields that may have changed with new transactions
                changed = False
                if scheme_data["total_units"] != existing.num_units:
                    existing.num_units = scheme_data["total_units"]
                    changed = True
                if scheme_data["name"] != existing.name:
                    existing.name = scheme_data["name"]
                    changed = True
                if scheme_data["address"] and scheme_data["address"] != existing.address:
                    existing.address = scheme_data["address"]
                    changed = True
                if changed:
                    existing.last_verified_at = datetime.now(tz=None)
                    updated += 1
            else:
                scheme = ExistingScheme(
                    name=scheme_data["name"],
                    address=scheme_data["address"],
                    postcode=scheme_data["postcode"],
                    scheme_type=scheme_data["scheme_type"],
                    num_units=scheme_data["total_units"],
                    status="operational",
                    source="land_registry_price_paid",
                    source_reference=source_ref,
                    data_confidence_score=0.6,  # Moderate — inferred from transaction clusters
                )
                db.add(scheme)
                new += 1

            db.commit()

        except Exception:
            logger.exception(
                "save_price_paid_scheme_failed",
                source_reference=scheme_data.get("source_reference"),
            )
            errors += 1
            db.rollback()

    logger.info(
        "save_price_paid_schemes_complete",
        found=found,
        new=new,
        updated=updated,
        errors=errors,
    )
    return {"found": found, "new": new, "updated": updated, "errors": errors}
