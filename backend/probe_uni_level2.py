"""Second-level probe: hall listing pages, partner pages, page structure."""
import re
import time
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

PAGES = [
    ("kent-canterbury", "https://www.kent.ac.uk/accommodation/canterbury", "links"),
    ("cccu-index", "https://www.canterbury.ac.uk/study-here/student-life/accommodation", "structure"),
    ("chester-explore", "https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/", "links"),
    ("worcester-life", "https://www.worcester.ac.uk/life/accommodation/", "links"),
    ("worcester-halls", "https://www.worcester.ac.uk/life/accommodation/living-in-halls/", "links"),
    ("lancaster-ug", "https://www.lancaster.ac.uk/accommodation/undergraduate/", "links"),
    ("lancaster-city", "https://www.lancaster.ac.uk/accommodation/city-accommodation/", "links"),
    ("aber-options", "https://www.aber.ac.uk/en/accommodation/accommodation-options/", "links"),
    ("york-rooms", "https://www.york.ac.uk/study/accommodation/rooms-prices/", "links"),
    ("bangor-ffriddoedd", "https://www.bangor.ac.uk/ffriddoedd-village", "links"),
    ("bangor-stmarys", "https://www.bangor.ac.uk/accommodation/st-marys-village", "links"),
    ("winchester-westdowns", "https://www.winchester.ac.uk/student-life/Accommodation/west-downs-student-village/", "structure"),
    ("yorksj-partners", "https://www.yorksj.ac.uk/study/accommodation/apply-for-accommodation/partner-providers/", "structure"),
    ("tees-cornell", "https://www.tees.ac.uk/sections/accommodation/buildings/cornell.cfm", "structure"),
]

KEY = re.compile(
    r"accommodat|halls|residen|college|village|court|house|hall\b|building",
    re.I,
)
PC = re.compile(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2}\b")
MANAGED = re.compile(
    r"(managed|operated|owned|run|provided)\s+by\s+([A-Z][\w&'\. -]{2,60})", re.I
)


def main():
    client = httpx.Client(
        headers={"User-Agent": UA}, follow_redirects=True, timeout=25
    )
    for slug, url, mode in PAGES:
        print(f"\n########## {slug}  {url}")
        try:
            r = client.get(url)
        except Exception as e:
            print(f"ERROR {type(e).__name__}: {str(e)[:120]}")
            time.sleep(1)
            continue
        print(f"status={r.status_code} final={r.url} len={len(r.text)}")
        soup = BeautifulSoup(r.text, "html.parser")
        host = urlparse(str(r.url)).netloc
        if mode == "links":
            seen = set()
            for a in soup.find_all("a", href=True):
                href = urljoin(str(r.url), a["href"].split("#")[0])
                if urlparse(href).netloc != host and "ac.uk" not in href:
                    continue
                if not KEY.search(href):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                text = a.get_text(" ", strip=True)[:70]
                print(f"   {href}  ||  {text}")
                if len(seen) > 70:
                    print("   ...truncated")
                    break
        else:  # structure
            for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
                t = tag.get_text(" ", strip=True)
                if t and len(t) < 90:
                    print(f"   <{tag.name}> {t}")
            text = soup.get_text(" ", strip=True)
            pcs = sorted(set(PC.findall(text)))
            print("   postcodes:", pcs[:25])
            for m in MANAGED.finditer(text):
                print("   managed-by:", m.group(0)[:90])
            ld = re.findall(
                r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>',
                r.text, re.S,
            )
            print(f"   json-ld blocks: {len(ld)}")
            for b in ld[:3]:
                print("   LD:", b.strip()[:300].replace("\n", " "))
        time.sleep(1.5)
    client.close()


if __name__ == "__main__":
    main()
