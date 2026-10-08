"""Print which database this backend is using, resolved exactly as the app does.

Shows the connection (password masked), where it came from (environment
variable, backend/.env, or the built-in default), the server version, the
database size, the Alembic revision and the row counts that tell the
databases apart (the production crawl has ~1.36M applications; the cloud
sandbox had 5,112).

Usage (from the backend folder, with the app's Python environment):
    python db_info.py
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from sqlalchemy import text

from app.config import settings
from app.database import engine

TABLES = ["planning_applications", "existing_schemes", "scheme_rents", "scheme_contracts",
          "scheme_observations", "scheme_room_types", "scheme_availability", "companies",
          "councils", "institutions", "hesa_enrolments", "transactions", "users"]


def mask(url: str) -> str:
    return re.sub(r"://([^:/@]+):([^@]*)@", r"://\1:***@", url)


def main() -> None:
    url = settings.DATABASE_URL
    env_file = Path(__file__).parent / ".env"
    if "DATABASE_URL" in os.environ:
        origin = "environment variable DATABASE_URL"
    elif env_file.exists() and re.search(r"^\s*DATABASE_URL\s*=", env_file.read_text(), re.M):
        origin = f"{env_file} (DATABASE_URL line)"
    else:
        origin = "built-in default in app/config.py (no DATABASE_URL set anywhere)"
    print(f"Connection: {mask(url)}")
    print(f"Set by:     {origin}")
    with engine.connect() as conn:
        name = conn.execute(text("select current_database()")).scalar()
        ver = conn.execute(text("show server_version")).scalar()
        size = conn.execute(text("select pg_size_pretty(pg_database_size(current_database()))")).scalar()
        host = conn.execute(text("select coalesce(inet_server_addr()::text, 'local socket')")).scalar()
        port = conn.execute(text("select inet_server_port()")).scalar()
        alembic = conn.execute(text(
            "select case when to_regclass('public.alembic_version') is null then 'none' "
            "else (select version_num from alembic_version limit 1) end")).scalar()
        print(f"Database:   {name} on {host}:{port}, PostgreSQL {ver}, {size} on disk")
        print(f"Schema:     alembic revision {alembic}")
        print("Row counts:")
        for t in TABLES:
            if conn.execute(text("select to_regclass(:t)"), {"t": f"public.{t}"}).scalar():
                n = conn.execute(text(f'select count(*) from "{t}"')).scalar()
                print(f"  {t:28s} {n:>12,}")
            else:
                print(f"  {t:28s} {'(table absent)':>14}")
    apps = None
    with engine.connect() as conn:
        if conn.execute(text("select to_regclass('public.planning_applications')")).scalar():
            apps = conn.execute(text("select count(*) from planning_applications")).scalar()
    if apps is not None:
        if apps > 100_000:
            print("\nThis is the production crawl database (national planning register).")
        else:
            print("\nThis is a small working database (city-level load), not the production crawl.")


if __name__ == "__main__":
    main()
