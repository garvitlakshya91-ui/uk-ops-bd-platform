"""Build a city Supply, Pipeline & Ownership report (HTML + PDF).

Usage:
    python report/build_city_report.py --council Birmingham
    python report/build_city_report.py --council Birmingham --out-dir reports --no-pdf
"""
from __future__ import annotations

import argparse
import datetime
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jinja2 import Environment, FileSystemLoader

from app.database import SessionLocal
from report.queries import gather_city_context

TEMPLATE_DIR = Path(__file__).parent


def render_html(context: dict) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=True)
    env.filters["min"] = min
    return env.get_template("template.html").render(**context)


def html_to_pdf(html_path: Path, pdf_path: Path) -> None:
    from playwright.sync_api import sync_playwright

    launch_kwargs = {}
    # Pre-provisioned Chromium (e.g. the cloud sandbox) may not match the
    # pinned Playwright build; fall back to an explicit executable.
    for executable in (None, "/opt/pw-browsers/chromium",
                       os.environ.get("PW_CHROMIUM")):
        try:
            with sync_playwright() as p:
                if executable:
                    launch_kwargs["executable_path"] = executable
                browser = p.chromium.launch(**launch_kwargs)
                page = browser.new_page()
                page.goto(html_path.as_uri(), wait_until="networkidle")
                page.pdf(path=str(pdf_path), format="A4", print_background=True)
                browser.close()
            return
        except Exception as exc:  # try next executable
            last = exc
    raise last


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--council", required=True)
    ap.add_argument("--out-dir", default="reports")
    ap.add_argument("--no-pdf", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    context = gather_city_context(db, args.council)
    db.close()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.date.today().strftime("%Y%m%d")
    slug = context["city"].lower().replace(" ", "_")
    html_path = out_dir / f"{slug}_pbsa_report_{stamp}.html"
    html_path.write_text(render_html(context))
    print(f"HTML -> {html_path}")

    if not args.no_pdf:
        pdf_path = html_path.with_suffix(".pdf")
        html_to_pdf(html_path, pdf_path)
        print(f"PDF  -> {pdf_path}")


if __name__ == "__main__":
    main()
