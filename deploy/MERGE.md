# Bringing the Birmingham and Exeter city work into production

The research work (census, rents, room tiers, letting tracker, HESA demand,
pipeline classification, migrations 009–017) was built in a separate working
database. Production is the PostgreSQL 18 database `uk_ops_bd` on the Windows
PC, at Alembic revision `008_add_ownership`. There are two ways to combine
them. Both start from a backup, both were tested on a restored copy of the
8 October 2026 production dump, and neither touches users, BTR data or any
council other than Birmingham and Exeter.

| | Path A: restore the merged dump | Path B: merge into the live database |
| --- | --- | --- |
| File | `uk_ops_bd_merged_2026-10-08.dump` (338 MB) | `city_work_birmingham_exeter_2026-10-08.dump` (2 MB) |
| What you get | Production as of 8 Oct 2026 19:33 UTC + the city work | Production as it is when you run it + the city work |
| Loses | Anything the crawler wrote after 19:33 UTC on 8 Oct | Nothing |
| Effort | One restore | Migrate, restore a small file, run two scripts |

Path B is the right choice if the crawler has run since the backup.

All commands are PowerShell, run from the repository folder. Adjust `18` in the
PostgreSQL path if your version folder differs, and the password if it is not
`postgres`.

```powershell
$PG = "C:\Program Files\PostgreSQL\18\bin"
$env:PGPASSWORD = "postgres"
```

## 0. Before either path

1. Keep the backup you took (`uk_ops_bd_2026-10-08.dump`) somewhere off the PC.
2. Stop the app: close the uvicorn and Celery windows (or stop the scheduled
   tasks). Nothing may be writing to the database during the switch.
3. Get the code and its packages:
   ```powershell
   git fetch origin claude/last-run-steps-i3ivsa
   git checkout claude/last-run-steps-i3ivsa
   cd backend
   pip install -r requirements.txt
   ```

**Do not start the backend on the new code before step A2 or B1.** The app
creates missing tables at start-up; if it creates the new tables before the
migrations run, the migrations stop with "already exists".

## Path A: restore the merged dump

A1. Restore into a new database, so the current one stays intact:
```powershell
& "$PG\createdb.exe" -U postgres -E UTF8 -T template0 uk_ops_bd_v2
& "$PG\pg_restore.exe" -U postgres -d uk_ops_bd_v2 --no-owner --no-privileges --jobs=4 uk_ops_bd_merged_2026-10-08.dump
```
A2. Swap the names (the app keeps using `uk_ops_bd`; the old one stays as `uk_ops_bd_old`):
```powershell
& "$PG\psql.exe" -U postgres -d postgres -c "ALTER DATABASE uk_ops_bd RENAME TO uk_ops_bd_old"
& "$PG\psql.exe" -U postgres -d postgres -c "ALTER DATABASE uk_ops_bd_v2 RENAME TO uk_ops_bd"
```
A3. Check, then start the app as usual:
```powershell
python db_info.py      # expect revision 017_pipeline_status, 1,367,224 applications
```
To roll back, rename the two databases the other way round.

## Path B: merge into the live database

B1. Migrate production to the new schema (2–3 seconds; adds columns and tables, changes no data):
```powershell
alembic upgrade head
python db_info.py      # expect revision 017_pipeline_status
```
B2. Restore the city work into its own database:
```powershell
& "$PG\createdb.exe" -U postgres -E UTF8 -T template0 uk_ops_bd_citywork
& "$PG\pg_restore.exe" -U postgres -d uk_ops_bd_citywork --no-owner --no-privileges city_work_birmingham_exeter_2026-10-08.dump
```
B3. Merge, first as a dry run (nothing written), then for real:
```powershell
$SRC = "postgresql://postgres:postgres@localhost:5432/uk_ops_bd_citywork"
$TGT = "postgresql://postgres:postgres@localhost:5432/uk_ops_bd"
python merge_city_work.py --source $SRC --target $TGT --cities Birmingham Exeter --dry-run --verbose
python merge_city_work.py --source $SRC --target $TGT --cities Birmingham Exeter
```
B4. Classify both cities' pipelines on production's own statuses (always after a merge):
```powershell
python classify_pipeline.py --city Birmingham
python classify_pipeline.py --city Exeter
```
B5. Check and start the app as usual. The merge is idempotent: running B3 and
B4 again changes nothing it has already done.

## What the merge does

- **Planning applications** are matched by reference. Production keeps its own
  values; only the pipeline columns and empty fields are filled. Applications
  production did not have are added (3,527 on the 8 October data).
- **Student schemes**: each census scheme is matched to production's own PBSA
  records (name plus postcode, district, operator or bed count; no-postcode
  records only on an exact name and operator). A match gains the census fields;
  everything else production holds is kept. Unmatched census schemes are added.
- **Production PBSA records the census does not contain** are left untouched
  and listed in the report as "not yet reconciled". On 8 October these included
  William Murdoch (Unite, 487), Chamberlain Place (Prestige, 209), an EPC record
  duplicating Globe Works, and Exeter listing fragments.
- **Rents, observations, room tiers, letting states** move against the matched
  schemes; production's older current rents from the same feed are superseded,
  never deleted.
- **HESA, institutions, maintenance loans, yields** are added by natural key.

## Checks after either path

On the 8 October data, the Birmingham report built from the merged database
matched the working database on every census, pipeline and demand figure
(61 private schemes and 20,246 beds, 21 university properties and 7,741 beds,
7,152 permitted and 3,930 risk-weighted pipeline beds, 88,860 students). Rent
figures include production's own April–July captures as well, and the
ownership chapter populates from production's title and company data.
