"""Set (or create) a user's password from the command line.

For a restored database on a new machine, or when a shared password does
not work. Uses the same bcrypt hashing as the API, so the new password
works immediately at /login.

Usage:
    python reset_password.py --email you@example.com --password 'NewPass!2026'
    python reset_password.py --email you@example.com --password '...' --create --name "Your Name" --role admin

With Docker Compose:
    docker compose exec backend python reset_password.py --email you@example.com --password 'NewPass!2026'
"""
from __future__ import annotations

import argparse

from app.api.auth import get_password_hash
from app.database import SessionLocal
from app.models.user import User


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--create", action="store_true", help="create the user if it does not exist")
    ap.add_argument("--name", default=None)
    ap.add_argument("--role", default="admin")
    args = ap.parse_args()
    if len(args.password) < 8:
        raise SystemExit("password must be at least 8 characters")

    db = SessionLocal()
    user = db.query(User).filter(User.email == args.email).first()
    if user is None:
        if not args.create:
            raise SystemExit(f"no user with email {args.email!r}; add --create to create one")
        user = User(email=args.email, name=args.name or args.email.split("@")[0],
                    hashed_password=get_password_hash(args.password), role=args.role,
                    is_active=True)
        db.add(user)
        action = "created"
    else:
        user.hashed_password = get_password_hash(args.password)
        user.is_active = True
        action = "updated"
    db.commit()
    print(f"{action} {user.email} (role {user.role}, active {user.is_active}); the new password works at /login now")
    db.close()


if __name__ == "__main__":
    main()
