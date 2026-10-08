# Going live

The whole platform runs on one VM with Docker Compose behind Caddy (automatic
HTTPS). Elasticsearch is not deployed — it was never wired into the app — so a
**4 GB RAM / 2 vCPU / 80 GB disk** VM is enough (Hetzner CX32 ~€8/mo,
DigitalOcean ~$24/mo, Lightsail ~$20/mo). The database restore needs ~1 GB of
the disk; scrapers will grow it over time.

## 1. Provision

- Create an Ubuntu 24.04 VM, point your domain's DNS A record at its IP.
- Install Docker: `curl -fsSL https://get.docker.com | sh`
- Open ports 80 and 443 (and 22) in the provider firewall.

## 2. Deploy

```bash
git clone https://github.com/garvitlakshya91-ui/uk-ops-bd-platform.git
cd uk-ops-bd-platform
cp .env.production.example .env.production
nano .env.production          # set SITE_ADDRESS, secrets, API keys
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

First build takes ~10 min (the backend image installs Playwright/Chromium).
The app then answers at `https://<SITE_ADDRESS>` with a fresh, empty database
(tables are auto-created on startup).

## 3. Restore the data

The production data (1.35M planning applications, 82K schemes) lives in
`uk_ops_bd_2026-05-29_005414.dump` (91 MB) — it is **not in the git repo**;
it's on the machine where the backup was taken (see `backups/*.manifest.txt`).
Copy it to the server and restore:

```bash
scp uk_ops_bd_2026-05-29_005414.dump root@<server>:~/
docker compose -f docker-compose.prod.yml cp ~/uk_ops_bd_2026-05-29_005414.dump postgres:/tmp/db.dump
docker compose -f docker-compose.prod.yml exec postgres \
  pg_restore -U postgres -d uk_ops_bd --clean --if-exists --no-owner --jobs=4 /tmp/db.dump
docker compose -f docker-compose.prod.yml restart backend celery-worker celery-beat
```

Then bring the restored schema up to date (the dump predates recent
migrations, e.g. 009 rent history):

```bash
docker compose -f docker-compose.prod.yml exec backend alembic upgrade head
```

## 4. Verify

- `https://<SITE_ADDRESS>/docs` — API up
- Log in on the frontend; dashboard shows scheme/application counts
- `docker compose -f docker-compose.prod.yml logs -f celery-beat` — scheduled
  scrapes ticking

## Updating

```bash
git pull
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

## Notes

- Celery beat drives the scheduled scrapers; keep an eye on disk and on
  `scraper_runs` for failures.
- Set `SENTRY_DSN` for error reporting before sharing the URL with users.
- Back up with `deploy/backup_db.sh --compose docker-compose.prod.yml --env-file .env.production`
  (Linux/macOS, native Postgres or Compose) or `.\scripts\backup_db.ps1` (Windows, native
  Postgres). Both write a verified `.dump`, a schema-only `.sql` and a manifest with row counts
  into `backups/`, and keep the newest five sets. Copy the `.dump` off the machine every time;
  a nightly cron or Task Scheduler entry running the script is the minimum for production.

## After the data restore: finance and ownership derivations

Once `title_ownership` exists (loaded by `backend/hmlr_ingest.py` from the
monthly Land Registry CCOD/OCOD files), derive transactions and owner
changes for the report cities:

```bash
docker compose -f docker-compose.prod.yml exec backend \
  python derive_transactions_from_titles.py --council Birmingham
```

Re-run after each monthly CCOD load; it only adds deals and events it
has not seen. Deals are `basis = derived` (price at title registration).
