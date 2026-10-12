"""Separate rent tables: benchmarks, advertised prices, room tiers, incentives.

scheme_rents held four different things apart only by source label:
area averages from ONS and the social-rent return (attached to every
scheme in a council), listing "from" prices, the room-tier prices
operators publish, and nothing for incentives, which sat in
observations as free text. Each now has its own table with the shape it
needs. Existing rows are copied, never moved: scheme_rents stays as the
untouched history and is not written after this revision.

Revision ID: 020_rent_tables
Revises: 019_scheme_registry
Create Date: 2026-10-09
"""
import re
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "020_rent_tables"
down_revision: Union[str, None] = "019_scheme_registry"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

AREA_SOURCES = ("ons_pipr_area", "rsh_sdr_la")
ADVERTISED_ROOM_TYPES = ("From", "To", "From (advertised)", "To (advertised)", "Advertised", "Range",
                         "Various", "General")

_CASH = re.compile(r"£\s?([\d,]{2,6})(?!\s*(?:/|per|a)\s*(?:week|wk|pw))", re.I)
_WEEKLY = re.compile(r"£\s?(\d{1,3})\s*(?:off\s+)?(?:a|per|/)\s*(?:week|wk)|£\s?(\d{1,3})\s*(?:pw|p/w)\s*off", re.I)
_PERCENT = re.compile(r"(\d{1,2})\s?%\s*(?:off|discount|saving)?", re.I)
_FREE_WEEKS = re.compile(r"(\d{1,2}|one|two|three|four)\s+(?:weeks?|months?)\s+(?:rent\s+)?free|free\s+(?:weeks?|month)", re.I)


def classify_incentive(text: str) -> tuple[str, float | None]:
    t = text or ""
    m = _WEEKLY.search(t)
    if m:
        return "weekly_off", float(m.group(1) or m.group(2))
    if re.search(r"cash|voucher|rebate|credit", t, re.I):
        m = _CASH.search(t)
        if m:
            return "cash", float(m.group(1).replace(",", ""))
    m = _PERCENT.search(t)
    if m:
        return "percent", float(m.group(1))
    if _FREE_WEEKS.search(t):
        return "free_weeks", None
    m = _CASH.search(t)
    if m and re.search(r"off|save|discount", t, re.I):
        return "cash", float(m.group(1).replace(",", ""))
    return "other", None


def upgrade() -> None:
    # An "additional N beds" figure is an increment, not a total (018 follow-up).
    op.execute("ALTER TABLE unit_extractions ADD COLUMN IF NOT EXISTS is_additional BOOLEAN NOT NULL DEFAULT FALSE")

    op.execute("""
        CREATE TABLE IF NOT EXISTS area_rent_benchmarks (
            id SERIAL PRIMARY KEY,
            council_id INTEGER REFERENCES councils(id) ON DELETE SET NULL,
            area_code VARCHAR(20),
            area_name VARCHAR(200),
            segment VARCHAR(80) NOT NULL,
            rent_pcm NUMERIC(10,2),
            rent_ppw NUMERIC(10,2),
            period VARCHAR(20),
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            superseded_at TIMESTAMPTZ
        )
    """)
    op.execute("COMMENT ON TABLE area_rent_benchmarks IS "
               "'Area-level rent references (ONS private rents, social rent returns); never a scheme price'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_area_rent_council ON area_rent_benchmarks (council_id, segment, is_current)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS scheme_advertised_rents (
            id SERIAL PRIMARY KEY,
            scheme_id INTEGER NOT NULL REFERENCES existing_schemes(id) ON DELETE CASCADE,
            kind VARCHAR(12) NOT NULL,
            rent_ppw NUMERIC(10,2),
            rent_pcm NUMERIC(10,2),
            currency CHAR(3) NOT NULL DEFAULT 'GBP',
            academic_year VARCHAR(20),
            contract_length_weeks INTEGER,
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            superseded_at TIMESTAMPTZ,
            legacy_rent_id INTEGER
        )
    """)
    op.execute("COMMENT ON TABLE scheme_advertised_rents IS "
               "'Entry prices from listings and directories: a scheme attribute, not a room tier'")
    op.execute("COMMENT ON COLUMN scheme_advertised_rents.kind IS 'from, to, advertised'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_adv_rents_scheme ON scheme_advertised_rents (scheme_id, is_current)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_adv_rents_source ON scheme_advertised_rents (source, is_current)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS scheme_room_rents (
            id SERIAL PRIMARY KEY,
            scheme_id INTEGER NOT NULL REFERENCES existing_schemes(id) ON DELETE CASCADE,
            room_type VARCHAR(100) NOT NULL,
            sub_classification VARCHAR(150),
            rent_ppw NUMERIC(10,2),
            rent_pcm NUMERIC(10,2),
            currency CHAR(3) NOT NULL DEFAULT 'GBP',
            academic_year VARCHAR(20),
            contract_length_weeks INTEGER,
            bills_included BOOLEAN,
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            superseded_at TIMESTAMPTZ,
            legacy_rent_id INTEGER
        )
    """)
    op.execute("COMMENT ON TABLE scheme_room_rents IS "
               "'Room-tier prices (scheme x room type x tier x year x tenancy); the weekly capture appends here'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_room_rents_scheme ON scheme_room_rents (scheme_id, is_current)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_room_rents_source ON scheme_room_rents (source, is_current)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_room_rents_series "
               "ON scheme_room_rents (scheme_id, room_type, sub_classification, captured_at)")

    op.execute("""
        CREATE TABLE IF NOT EXISTS scheme_incentives (
            id SERIAL PRIMARY KEY,
            scheme_id INTEGER NOT NULL REFERENCES existing_schemes(id) ON DELETE CASCADE,
            raw_text VARCHAR(300) NOT NULL,
            kind VARCHAR(20) NOT NULL DEFAULT 'other',
            amount NUMERIC(10,2),
            academic_year VARCHAR(20),
            source VARCHAR(100) NOT NULL,
            source_reference VARCHAR(500),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            superseded_at TIMESTAMPTZ
        )
    """)
    op.execute("COMMENT ON COLUMN scheme_incentives.kind IS 'cash, weekly_off, percent, free_weeks, other'")
    op.execute("CREATE INDEX IF NOT EXISTS ix_incentives_scheme ON scheme_incentives (scheme_id, is_current)")

    # ---- copy existing rows (scheme_rents is left untouched as history) ----
    op.execute("""
        INSERT INTO area_rent_benchmarks
            (council_id, area_name, segment, rent_pcm, rent_ppw, period, source, source_reference,
             captured_at, is_current)
        SELECT es.council_id, co.name, sr.room_type, MAX(sr.rent_per_month), MAX(sr.rent_per_week),
               to_char(MIN(COALESCE(sr.scraped_at, sr.created_at)), 'YYYY-MM'), sr.source,
               MIN(sr.source_reference), MIN(COALESCE(sr.scraped_at, sr.created_at)), bool_or(sr.is_current)
        FROM scheme_rents sr
        JOIN existing_schemes es ON es.id = sr.scheme_id
        LEFT JOIN councils co ON co.id = es.council_id
        WHERE sr.source = ANY(:area) AND sr.room_type IS NOT NULL
        GROUP BY es.council_id, co.name, sr.room_type, sr.source,
                 date_trunc('month', COALESCE(sr.scraped_at, sr.created_at))
    """.replace(":area", "ARRAY['ons_pipr_area','rsh_sdr_la']"))
    op.execute("""
        INSERT INTO scheme_advertised_rents
            (scheme_id, kind, rent_ppw, rent_pcm, currency, academic_year, contract_length_weeks,
             source, source_reference, captured_at, is_current, superseded_at, legacy_rent_id)
        SELECT scheme_id,
               CASE WHEN room_type ILIKE 'from%' OR room_type = 'Range' THEN 'from'
                    WHEN room_type ILIKE 'to%' THEN 'to' ELSE 'advertised' END,
               rent_per_week, rent_per_month, COALESCE(currency, 'GBP'), academic_year, contract_length_weeks,
               COALESCE(source, 'unknown'), source_reference, COALESCE(scraped_at, created_at),
               is_current, superseded_at, id
        FROM scheme_rents
        WHERE source <> ALL(ARRAY['ons_pipr_area','rsh_sdr_la'])
          AND (room_type IS NULL OR room_type = ANY(ARRAY['From','To','From (advertised)','To (advertised)',
                                                          'Advertised','Range','Various','General']))
    """)
    op.execute("""
        INSERT INTO scheme_room_rents
            (scheme_id, room_type, sub_classification, rent_ppw, rent_pcm, currency, academic_year,
             contract_length_weeks, source, source_reference, captured_at, is_current, superseded_at, legacy_rent_id)
        SELECT scheme_id, room_type, sub_classification, rent_per_week, rent_per_month, COALESCE(currency, 'GBP'),
               academic_year, contract_length_weeks, COALESCE(source, 'unknown'), source_reference,
               COALESCE(scraped_at, created_at), is_current, superseded_at, id
        FROM scheme_rents
        WHERE source <> ALL(ARRAY['ons_pipr_area','rsh_sdr_la'])
          AND room_type IS NOT NULL
          AND room_type <> ALL(ARRAY['From','To','From (advertised)','To (advertised)',
                                     'Advertised','Range','Various','General'])
    """)
    conn = op.get_bind()
    rows = conn.execute(sa.text("""
        SELECT scheme_id, value_text, academic_year, source, source_reference, observed_at
        FROM scheme_observations WHERE field = 'incentive' AND value_text IS NOT NULL
        ORDER BY observed_at
    """)).fetchall()
    latest: dict[tuple, int] = {}
    for i, r in enumerate(rows):
        latest[(r[0], (r[1] or "").strip().lower())] = i
    for i, r in enumerate(rows):
        kind, amount = classify_incentive(r[1])
        conn.execute(sa.text("""
            INSERT INTO scheme_incentives
                (scheme_id, raw_text, kind, amount, academic_year, source, source_reference, captured_at, is_current)
            VALUES (:sid, :txt, :kind, :amount, :ay, :src, :ref, :at, :cur)
        """), {"sid": r[0], "txt": r[1][:300], "kind": kind, "amount": amount, "ay": r[2],
               "src": r[3] or "unknown", "ref": r[4], "at": r[5],
               "cur": latest[(r[0], (r[1] or "").strip().lower())] == i})
    op.execute("COMMENT ON TABLE scheme_rents IS "
               "'Legacy, frozen at revision 020: rows copied to area_rent_benchmarks, "
               "scheme_advertised_rents and scheme_room_rents; nothing writes here any more'")


def downgrade() -> None:
    op.execute("COMMENT ON TABLE scheme_rents IS NULL")
    for t in ("scheme_incentives", "scheme_room_rents", "scheme_advertised_rents", "area_rent_benchmarks"):
        op.execute(f"DROP TABLE IF EXISTS {t}")
    op.execute("ALTER TABLE unit_extractions DROP COLUMN IF EXISTS is_additional")
