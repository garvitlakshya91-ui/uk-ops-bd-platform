"""Third probe: sitemaps (worcester, bangor), CCCU embedded JSON, kent halls,
lancaster colleges."""
import re
import time
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

client = httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=30)


def get(url):
    time.sleep(1)
    try:
        r = client.get(url)
        print(f"GET {url} -> {r.status_code} len={len(r.text)}")
        return r
    except Exception as e:
        print(f"GET {url} ERROR {type(e).__name__}: {str(e)[:100]}")
        return None


print("===== worcester sitemap")
r = get("https://www.worcester.ac.uk/sitemap.xml")
if r and r.status_code == 200:
    locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
    print(f"locs={len(locs)}")
    hits = [u for u in locs if re.search(r"accommodation|halls", u, re.I)]
    for u in hits[:60]:
        print("  ", u)
    if not hits and locs[:5]:
        for u in locs[:10]:
            print("  idx:", u)

print("\n===== bangor sitemap")
r = get("https://www.bangor.ac.uk/sitemap.xml")
if r and r.status_code == 200:
    locs = re.findall(r"<loc>([^<]+)</loc>", r.text)
    print(f"locs={len(locs)}")
    for u in locs[:10]:
        print("  idx:", u)
    # if sitemap index, fetch children and grep for halls
    if locs and "sitemap" in locs[0]:
        for child in locs[:8]:
            rc = get(child)
            if not rc:
                continue
            curls = re.findall(r"<loc>([^<]+)</loc>", rc.text)
            hits = [u for u in curls if "/accommodation/halls/" in u or "village" in u]
            for u in hits[:60]:
                print("   ", u)
    else:
        hits = [u for u in locs if "/accommodation/" in u or "village" in u]
        for u in hits[:80]:
            print("  ", u)

print("\n===== cccu embedded data")
r = get("https://www.canterbury.ac.uk/study-here/student-life/accommodation")
if r:
    html = r.text
    for marker in ["__NEXT_DATA__", "window.__", "application/json", "halls", "Petros"]:
        idx = html.find(marker)
        print(f"  marker {marker!r}: pos={idx}")
    # find accommodation-ish slugs in raw html
    slugs = sorted(set(re.findall(r'"(/[^"]*accommodation[^"]*)"', html)))
    for s in slugs[:40]:
        print("  slug:", s[:120])
    names = sorted(set(re.findall(r'"(?:title|name|heading)"\s*:\s*"([^"]{3,60})"', html)))
    print(f"  json titles: {len(names)}")
    for n in names[:60]:
        print("   t:", n)

print("\n===== kent ug accommodation page")
r = get("https://www.kent.ac.uk/accommodation/canterbury/undergraduate-accommodation")
if r:
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(str(r.url), a["href"].split("#")[0])
        if "/accommodation/" not in href or urlparse(href).netloc != "www.kent.ac.uk":
            continue
        if href in seen:
            continue
        seen.add(href)
        print("  ", href, "||", a.get_text(" ", strip=True)[:60])

print("\n===== lancaster colleges")
r = get("https://www.lancaster.ac.uk/about-us/colleges/")
if r:
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(str(r.url), a["href"].split("#")[0])
        if "college" not in href.lower() or urlparse(href).netloc != "www.lancaster.ac.uk":
            continue
        if href in seen:
            continue
        seen.add(href)
        print("  ", href, "||", a.get_text(" ", strip=True)[:60])

print("\n===== chester explore page 2 (pagination check)")
r = get("https://www.chester.ac.uk/student-life/accommodation/explore-accommodation/?page=2")
if r:
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(str(r.url), a["href"].split("#")[0])
        if "/explore-accommodation/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        print("  ", href, "||", a.get_text(" ", strip=True)[:60])

client.close()
