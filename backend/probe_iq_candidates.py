import sys
sys.path.insert(0, "/app")
from app.scrapers.operator_directory_scraper import BRAND_CONFIGS, OperatorDirectoryScraper

with OperatorDirectoryScraper() as s:
    urls, hint = s.discover(BRAND_CONFIGS["iq_student_accommodation"])
    print(hint, len(urls))
    for u in urls[:40]:
        print(u)
