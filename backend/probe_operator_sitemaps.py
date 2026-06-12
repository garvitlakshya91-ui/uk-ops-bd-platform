"""Probe sitemaps for the 15 PBSA operator sites: discover property URL patterns.

For each site: fetch the sitemap (index), expand child sitemaps (capped),
bucket URLs by their first two path segments and print counts + samples.
"""
import re
import time
from collections import Counter, defaultdict

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

SITES = {
    "fresh": ["https://www.thisisfresh.com/sitemap.xml"],
    "vita": ["https://www.vitastudent.com/sitemap_index.xml", "https://www.vitastudent.com/sitemap.xml"],
    "yugo": ["https://yugo.com/service-sitemap-en-gb-sitemap_index.xml"],
    "hfs": ["https://www.homesforstudents.co.uk/sitemap.xml", "https://www.homesforstudents.co.uk/sitemap_index.xml"],
    "crm": ["https://www.crm-students.com/service-sitemap-crm-en-gb-sitemap_index.xml"],
    "prestige": ["https://www.prestigestudentliving.com/sitemap_index.xml", "https://www.prestigestudentliving.com/sitemap.xml"],
    "collegiate": ["https://www.collegiate-ac.com/sitemap_index.xml"],
    "roost": ["https://www.studentroost.co.uk/sitemap.xml", "https://www.studentroost.co.uk/sitemap_index.xml"],
    "iq": ["https://www.iqstudentaccommodation.com/sitemap.xml"],
    "hello": ["https://www.hellostudent.co.uk/sitemap_index.xml"],
    "mezzino": ["https://www.mezzino.com/sitemap_index.xml", "https://www.mezzino.co.uk/sitemap_index.xml"],
    "host": ["https://host-students.com/sitemap_index.xml"],
    "studyinn": ["https://studyinn.com/sitemap_index.xml"],
    "downing": ["https://www.downingstudents.com/sitemap_index.xml"],
    "abodus": ["https://www.abodusstudents.com/sitemap.xml", "https://www.abodusstudents.com/sitemap_index.xml"],
}

LOC_PAT = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")

client = httpx.Client(timeout=25, headers={"User-Agent": UA}, follow_redirects=True)
_last = {}


def get(url):
    import urllib.parse
    host = urllib.parse.urlparse(url).netloc
    now = time.monotonic()
    if host in _last and now - _last[host] < 1.0:
        time.sleep(1.0 - (now - _last[host]))
    _last[host] = time.monotonic()
    try:
        r = client.get(url)
    except Exception as e:
        print(f"  FETCH-ERR {url}: {type(e).__name__} {str(e)[:100]}")
        return None
    if r.status_code != 200:
        print(f"  HTTP {r.status_code} {url}")
        return None
    return r.text


for brand, candidates in SITES.items():
    print(f"\n########## {brand} ##########")
    xml = None
    for c in candidates:
        xml = get(c)
        if xml:
            print(f"  sitemap ok: {c}")
            break
    if not xml:
        print("  NO SITEMAP FOUND")
        continue
    locs = LOC_PAT.findall(xml)
    page_urls = []
    if "<sitemapindex" in xml or all(u.endswith(".xml") for u in locs[:5] if u):
        # sitemap index -> expand children (cap 12)
        print(f"  index with {len(locs)} children")
        for child in locs[:15]:
            print(f"    child: {child}")
        for child in locs[:12]:
            cx = get(child)
            if cx:
                page_urls.extend(LOC_PAT.findall(cx))
    else:
        page_urls = locs
    print(f"  total page urls: {len(page_urls)}")
    buckets = defaultdict(list)
    for u in page_urls:
        path = re.sub(r"https?://[^/]+", "", u).strip("/")
        segs = path.split("/")
        key = "/".join(segs[:2]) if len(segs) >= 2 else (segs[0] or "(root)")
        # generalise: keep seg1 literal, seg2 as count
        buckets[segs[0] or "(root)"].append(u)
    for seg, urls in sorted(buckets.items(), key=lambda kv: -len(kv[1]))[:12]:
        print(f"    /{seg}/: {len(urls)}  e.g. {urls[0]}")
        if len(urls) > 1:
            print(f"              {urls[min(3, len(urls)-1)]}")

client.close()
print("\nDONE")
