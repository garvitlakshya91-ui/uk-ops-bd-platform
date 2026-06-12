"""Round 2 probe: specific child sitemaps, gzip sitemaps, problem sites."""
import gzip
import re
import time

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
LOC_PAT = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")

client = httpx.Client(timeout=25, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-GB,en;q=0.9"}, follow_redirects=True)


def get_bytes(url):
    try:
        r = client.get(url)
    except Exception as e:
        print(f"  FETCH-ERR {url}: {type(e).__name__} {str(e)[:100]}")
        return None, None
    return r.status_code, r.content


def get_xml(url):
    status, content = get_bytes(url)
    if status != 200 or content is None:
        print(f"  HTTP {status} {url}")
        return None
    if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except Exception as e:
            print(f"  gzip fail: {e}")
    return content.decode("utf-8", "replace")


print("=== yugo sitemap1.xml.gz ===")
xml = get_xml("https://yugo.com/service-sitemap-en-gb-sitemap1.xml.gz")
if xml:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    uk = [u for u in locs if "/united-kingdom/" in u or "/en-gb/" in u]
    print(f"  uk-ish: {len(uk)}")
    for u in locs[:10]:
        print("   ", u)
    # bucket depth
    import collections
    c = collections.Counter(len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) for u in locs)
    print("  depth counts:", dict(c))
    for u in [x for x in locs if len(re.sub(r'https?://[^/]+/', '', x).strip('/').split('/')) >= 4][:10]:
        print("   deep:", u)
time.sleep(1)

print("\n=== crm sitemap1.xml.gz ===")
xml = get_xml("https://www.crm-students.com/service-sitemap-crm-en-gb-sitemap1.xml.gz")
if xml:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    for u in locs[:12]:
        print("   ", u)
    deep = [u for u in locs if len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) >= 3]
    print(f"  depth>=3: {len(deep)}")
    for u in deep[:10]:
        print("   deep:", u)
time.sleep(1)

print("\n=== vita developments-sitemap ===")
xml = get_xml("https://www.vitastudent.com/developments-sitemap.xml")
if xml:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    for u in locs[:60]:
        print("   ", u)
time.sleep(1)

print("\n=== roost properties sitemap ===")
xml = get_xml("https://www.studentroost.co.uk/sitemaps-1-section-properties-1-sitemap.xml")
if xml:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    for u in locs[:60]:
        print("   ", u)
time.sleep(1)

print("\n=== collegiate locations sitemap ===")
xml = get_xml("https://www.collegiate-ac.com/locations-sitemap.xml")
if xml:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    for u in locs[:70]:
        print("   ", u)
time.sleep(1)

print("\n=== fresh: thisisfresh.com probes ===")
for path in ["/", "/our-locations", "/locations", "/student-accommodation"]:
    status, content = get_bytes(f"https://www.thisisfresh.com{path}")
    print(f"  {path} -> {status} len={len(content) if content else 0}")
    time.sleep(1.2)

print("\n=== hfs: homesforstudents.co.uk probes ===")
for path in ["/", "/student-accommodation/liverpool", "/liverpool", "/properties"]:
    status, content = get_bytes(f"https://www.homesforstudents.co.uk{path}")
    txt = (content or b"")[:400].decode("utf-8", "replace")
    print(f"  {path} -> {status} len={len(content) if content else 0}")
    if status == 200 and path == "/":
        # find nav links
        links = re.findall(r'href="([^"]+)"', (content or b"").decode("utf-8", "replace"))
        cands = [l for l in links if any(k in l.lower() for k in ("accommodation", "propert", "cit", "location"))]
        for l in list(dict.fromkeys(cands))[:25]:
            print("    link:", l)
    time.sleep(1.2)

print("\n=== abodus probes ===")
status, content = get_bytes("https://www.abodusstudents.com/sitemap.xml")
print(f"  /sitemap.xml -> {status}")
print("  raw head:", (content or b"")[:500])
time.sleep(1)
status, content = get_bytes("https://www.abodusstudents.com/")
print(f"  / -> {status} len={len(content) if content else 0}")
if status == 200:
    html = content.decode("utf-8", "replace")
    links = re.findall(r'href="([^"]+)"', html)
    cands = [l for l in links if any(k in l.lower() for k in ("locations", "propert", "accommodation", "cities"))]
    for l in list(dict.fromkeys(cands))[:25]:
        print("    link:", l)

client.close()
print("\nDONE")
