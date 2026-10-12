-- Mark the planning rows that were copied across councils on 16 May 2026.
--
-- What a clone is: a row with no source, created on 2026-05-16, whose
-- reference appears (among no-source rows) under 200 or more councils.
-- Measured read-only on 2026-10-11: 5,000 references, 1,251,958 rows.
-- The other 363 no-source rows created that day have a reference held by
-- one council only and are left alone.
--
-- Take a backup first (scripts/backup_db.ps1). Nothing is deleted. The only
-- value changed is is_clone, false -> true, so the old value of every row
-- touched is false; undo with the statement at the bottom.
-- updated_at is deliberately not touched: the decision-alert job reads it.
--
-- Run:  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/mark_cloned_planning_rows.sql
-- The UPDATE rewrites 1.25M rows on a table with 14 indexes; allow several
-- minutes. Safe to run again (already-marked rows are skipped).

\set ON_ERROR_STOP on

-- Same statement as migration 022_planning_is_clone, so this file also
-- works on a database where the migration has not been run yet.
ALTER TABLE planning_applications
    ADD COLUMN IF NOT EXISTS is_clone BOOLEAN NOT NULL DEFAULT false;

BEGIN;

CREATE TEMP TABLE _cloned_refs ON COMMIT DROP AS
SELECT reference
FROM planning_applications
WHERE source IS NULL
GROUP BY reference
HAVING count(DISTINCT council_id) >= 200;

CREATE UNIQUE INDEX ON _cloned_refs (reference);
ANALYZE _cloned_refs;

UPDATE planning_applications pa
SET is_clone = true
FROM _cloned_refs c
WHERE pa.reference = c.reference
  AND pa.source IS NULL
  AND pa.created_at >= TIMESTAMP '2026-05-16'
  AND pa.created_at <  TIMESTAMP '2026-05-17'
  AND NOT pa.is_clone;

-- Check before the commit: expect about 5,000 references and 1,251,958 rows
-- marked, and no marked row that a pipeline opportunity or alert points at.
SELECT (SELECT count(*) FROM _cloned_refs)                                AS cloned_references,
       count(*) FILTER (WHERE is_clone)                                   AS rows_marked,
       count(*) FILTER (WHERE NOT is_clone)                               AS rows_kept,
       count(*) FILTER (WHERE is_clone AND source IS NOT NULL)            AS marked_with_a_source
FROM planning_applications;

SELECT (SELECT count(*) FROM pipeline_opportunities o
        JOIN planning_applications p ON p.id = o.planning_application_id
        WHERE p.is_clone) AS pipeline_rows_on_clones,
       (SELECT count(*) FROM alerts a
        JOIN planning_applications p ON p.id = a.planning_application_id
        WHERE p.is_clone) AS alerts_on_clones;

COMMIT;

VACUUM (ANALYZE) planning_applications;

-- ---------------------------------------------------------------------------
-- NOT RUN: the 4 April 2026 copies of the same references.
-- The same 5,000 references also sit under up to 265 councils in 52,726
-- no-source rows created on 4-5 April 2026 (the rows the May run copied
-- from). They are duplicated across councils in the same way, and no copy
-- can be told apart as the real one. To mark them as well, run:
--
--   UPDATE planning_applications pa
--   SET is_clone = true
--   WHERE pa.source IS NULL
--     AND pa.created_at < TIMESTAMP '2026-05-16'
--     AND NOT pa.is_clone
--     AND pa.reference IN (SELECT reference FROM planning_applications
--                          WHERE source IS NULL
--                          GROUP BY reference
--                          HAVING count(DISTINCT council_id) >= 200);
--
-- ---------------------------------------------------------------------------
-- Undo:
--   UPDATE planning_applications SET is_clone = false WHERE is_clone;
