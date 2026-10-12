-- Fill applicant_name, agent_name and postcode on existing PlanIt rows from
-- the JSON already stored in raw_data.
--
-- The ingest read applicant_name / agent_name from the top level of the
-- PlanIt record, where they do not exist (they are under other_fields), and
-- never wrote postcode at all. This applies the same mapping the fixed
-- ingest now uses (app.scrapers.planit_scraper.planit_party_names):
--
--   * PlanIt redacts personal names to the literal text 'See source'. That
--     placeholder is never written; the name falls back to
--     other_fields.applicant_company / agent_company, which is not redacted.
--   * postcode comes from raw_data.postcode, else is read out of the address.
--
-- Only empty columns are filled, so the old value of everything this writes
-- is NULL (or blank) and no existing value is overwritten. Nothing is
-- deleted. updated_at is deliberately not touched: the decision-alert job
-- reads it and would re-alert on every backfilled row.
--
-- Measured read-only on 2026-10-11 against 47,061 planit_api rows:
--   agent_name      61 filled -> 18,480   (+18,419)
--   applicant_name  72 filled ->     98   (+26; PlanIt redacts nearly all)
--   postcode        66 filled -> 44,178   (+44,112)
--   45,215 rows touched in all
--
-- Take a backup first (scripts/backup_db.ps1).
-- Run:  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/backfill_planit_applicant_agent_postcode.sql
-- Safe to run again.

\set ON_ERROR_STOP on

BEGIN;

SELECT count(*)              AS planit_rows,
       count(applicant_name) AS applicant_before,
       count(agent_name)     AS agent_before,
       count(postcode)       AS postcode_before
FROM planning_applications
WHERE source = 'planit_api';

WITH src AS (
    SELECT
        pa.id,
        CASE
            WHEN lower(btrim(COALESCE(pa.raw_data -> 'other_fields' ->> 'applicant_name', ''))) NOT IN ('', 'see source')
                THEN btrim(pa.raw_data -> 'other_fields' ->> 'applicant_name')
            WHEN lower(btrim(COALESCE(pa.raw_data -> 'other_fields' ->> 'applicant_company', ''))) NOT IN ('', 'see source')
                THEN btrim(pa.raw_data -> 'other_fields' ->> 'applicant_company')
        END AS new_applicant,
        CASE
            WHEN lower(btrim(COALESCE(pa.raw_data -> 'other_fields' ->> 'agent_name', ''))) NOT IN ('', 'see source')
                THEN btrim(pa.raw_data -> 'other_fields' ->> 'agent_name')
            WHEN lower(btrim(COALESCE(pa.raw_data -> 'other_fields' ->> 'agent_company', ''))) NOT IN ('', 'see source')
                THEN btrim(pa.raw_data -> 'other_fields' ->> 'agent_company')
        END AS new_agent,
        COALESCE(
            NULLIF(btrim(pa.raw_data ->> 'postcode'), ''),
            -- UK postcode from the address, normalised to 'AB1 2CD'
            regexp_replace(
                regexp_replace(
                    substring(upper(pa.address) FROM '\m([A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2})\M'),
                    '\s+', '', 'g'),
                '(...)$', ' \1')
        ) AS new_postcode
    FROM planning_applications pa
    WHERE pa.source = 'planit_api'
      AND jsonb_typeof(pa.raw_data) = 'object'
)
UPDATE planning_applications pa
SET applicant_name = COALESCE(NULLIF(btrim(pa.applicant_name), ''), left(src.new_applicant, 255)),
    agent_name     = COALESCE(NULLIF(btrim(pa.agent_name), ''),     left(src.new_agent, 255)),
    postcode       = COALESCE(NULLIF(btrim(pa.postcode), ''),
                              CASE WHEN length(src.new_postcode) <= 10 THEN src.new_postcode END)
FROM src
WHERE src.id = pa.id
  AND (   (NULLIF(btrim(pa.applicant_name), '') IS NULL AND src.new_applicant IS NOT NULL)
       OR (NULLIF(btrim(pa.agent_name), '')     IS NULL AND src.new_agent     IS NOT NULL)
       OR (NULLIF(btrim(pa.postcode), '')       IS NULL AND length(src.new_postcode) <= 10));

-- Check before the commit: the three counts should have risen as in the
-- header, and no row may hold the placeholder.
SELECT count(*)              AS planit_rows,
       count(applicant_name) AS applicant_after,
       count(agent_name)     AS agent_after,
       count(postcode)       AS postcode_after,
       count(*) FILTER (WHERE lower(applicant_name) = 'see source'
                           OR lower(agent_name) = 'see source') AS placeholder_rows
FROM planning_applications
WHERE source = 'planit_api';

COMMIT;
