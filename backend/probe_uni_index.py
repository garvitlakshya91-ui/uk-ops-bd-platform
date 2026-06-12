"""Probe accommodation index pages: list candidate hall/partner links."""
import re
import time
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

INDEXES = [
    ("kent", "https://www.kent.ac.uk/accommodation"),
    ("canterbury-ccu", "https://www.canterbury.ac.uk/study-here/student-life/accommodation"),
    ("lincoln", "https://www.lincoln.ac.uk/studentlife/accommodation/"),
    ("bgu", "https://www.lincolnbishop.ac.uk/student/accommodation"),
    ("bgu-robots", "https://www.lincolnbishop.ac.uk/robots.txt"),
    ("chester", "https://www.chester.ac.uk/student-life/accommodation/"),
    ("worcester", "https://www.worcester.ac.uk/campaigns/accommodation-guide"),
    ("winchester", "https://www.winchester.ac.uk/student-life/Accommodation/"),
    ("lancaster", "https://www.lancaster.ac.uk/accommodation/"),
    ("durham", "https://www.durham.ac.uk/colleges-and-student-experience/colleges/"),
    ("bangor", "https://www.bangor.ac.uk/accommodation"),
    ("aber", "https://www.aber.ac.uk/en/accommodation/"),
    ("york", "https://www.york.ac.uk/study/accommodation/"),
    ("yorksj", "https://www.yorksj.ac.uk/study/accommodation/"),
    ("tees", "https://www.tees.ac.uk/sections/accommodation/"),
]

KEY = re.compile(
    r"accommodat|halls|residen|college|village|court|house|partner|private|"
    r"campus-living|rooms|student-living",
    re.I,
)


def main():
    client = httpx.Client(
        headers={"User-Agent": UA}, follow_redirects=True, timeout=25
    )
    for slug, url in INDEXES:
        print(f"\n########## {slug}  {url}")
        try:
            r = client.get(url)
        except Exception as e:
            print(f"ERROR {type(e).__name__}: {str(e)[:120]}")
            time.sleep(1)
            continue
        print(f"status={r.status_code} final={r.url} len={len(r.text)}")
        if slug.endswith("robots"):
            print(r.text[:1500])
            time.sleep(1)
            continue
        host = urlparse(str(r.url)).netloc
        soup = BeautifulSoup(r.text, "html.parser")
        t = soup.find("title")
        print("title:", t.get_text(strip=True)[:100] if t else "?")
        seen = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(str(r.url), a["href"].split("#")[0])
            if urlparse(href).netloc != host:
                # keep external links that look accommodation/operator related
                if not re.search(r"accommodat|student|halls|residen", href, re.I):
                    continue
            if not KEY.search(href):
                continue
            text = a.get_text(" ", strip=True)[:70]
            key = href
            if key in seen:
                continue
            seen.add(key)
            print(f"   {href}  ||  {text}")
            if len(seen) > 80:
                print("   ...truncated")
                break
        time.sleep(1.5)
    client.close()


if __name__ == "__main__":
    main()
