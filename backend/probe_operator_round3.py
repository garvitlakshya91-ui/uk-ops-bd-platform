"""Round 3: yugo/crm URL structure, wearehomesforstudents.com, sample property pages."""
import gzip
import json
import re
import time

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
LOC_PAT = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
PC_PAT = re.compile(r"\b[A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2}\b")

client = httpx.Client(timeout=25, headers={"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"}, follow_redirects=True)


def get_xml(url):
    r = client.get(url)
    if r.status_code != 200:
        print(f"  HTTP {r.status_code} {url}")
        return None
    content = r.content
    if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return content.decode("utf-8", "replace")


def page_summary(url):
    time.sleep(1.0)
    try:
        r = client.get(url)
    except Exception as e:
        print(f"  ERR {url}: {str(e)[:80]}")
        return
    html = r.text if r.status_code == 200 else ""
    print(f"\n  PAGE {url} -> {r.status_code} len={len(html)}")
    if not html:
        return
    # JSON-LD blocks
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        blob = m.group(1).strip()
        try:
            d = json.loads(blob)
        except Exception:
            print("    JSONLD-unparseable len", len(blob))
            continue
        types = []
        def walk(n):
            if isinstance(n, dict):
                if "@type" in n:
                    types.append(str(n.get("@type")))
                for v in n.values():
                    walk(v)
            elif isinstance(n, list):
                for v in n:
                    walk(v)
        walk(d)
        print(f"    JSONLD types={types[:8]} head={blob[:220].replace(chr(10),' ')}")
    # NEXT_DATA
    if "__NEXT_DATA__" in html:
        print("    HAS __NEXT_DATA__")
    if "__next_f.push" in html:
        print("    HAS next_f.push (RSC)")
    # postcode + price
    pcs = PC_PAT.findall(html)
    print(f"    postcodes found: {pcs[:6]}")
    prices = re.findall(r"£\s?[\d,]+(?:\.\d{2})?\s*(?:/|per\s*)?(?:week|pw|pppw|ppw)?", html)[:8]
    print(f"    price snippets: {prices}")
    addr = re.findall(r'"(?:streetAddress|postalCode|addressLocality)"\s*:\s*"([^"]+)"', html)
    print(f"    addr fields: {addr[:8]}")


print("=== yugo: en-gb deep urls ===")
xml = get_xml("https://yugo.com/service-sitemap-en-gb-sitemap1.xml.gz")
if xml:
    locs = LOC_PAT.findall(xml)
    sa = [u for u in locs if "/resource/" not in u and not u.endswith(".pdf")]
    deep = [u for u in sa if len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) >= 4]
    print(f"  non-pdf deep>=4: {len(deep)}")
    uk = [u for u in deep if "united-kingdom" in u]
    print(f"  united-kingdom: {len(uk)}")
    for u in uk[:15]:
        print("   ", u)
    # unique depth-5 (property pages)
    d5 = sorted(set(u for u in uk if len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) == 5))
    print(f"  uk depth5: {len(d5)}")
    for u in d5[:10]:
        print("   d5:", u)

print("\n=== crm: depth2 urls ===")
xml = get_xml("https://www.crm-students.com/service-sitemap-crm-en-gb-sitemap1.xml.gz")
if xml:
    locs = LOC_PAT.findall(xml)
    d2 = [u for u in locs if "/resource/" not in u and len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) == 2]
    print(f"  depth2: {len(d2)}")
    for u in d2[:25]:
        print("   ", u)

print("\n=== wearehomesforstudents.com robots + sitemap ===")
r = client.get("https://wearehomesforstudents.com/robots.txt")
print(f"  robots -> {r.status_code}")
print(r.text[:1200])
time.sleep(1)
for sm in ["https://wearehomesforstudents.com/sitemap.xml", "https://wearehomesforstudents.com/sitemap_index.xml"]:
    xml = get_xml(sm)
    if xml:
        locs = LOC_PAT.findall(xml)
        print(f"  {sm}: {len(locs)} locs")
        for u in locs[:20]:
            print("   ", u)
        break
    time.sleep(1)

print("\n=== sample property pages ===")
page_summary("https://www.vitastudent.com/en/cities/manchester/first-street/")
page_summary("https://www.studentroost.co.uk/locations/sheffield/hollis-croft")
page_summary("https://prestigestudentliving.com/student-accommodation/coventry/33-parkside")
page_summary("https://www.collegiate-ac.com/uk-student-accommodation/exeter/point-exe/")
page_summary("https://www.iqstudentaccommodation.com/manchester/daisy-bank")
page_summary("https://www.hellostudent.co.uk/student-accommodation/liverpool/the-octagon")
page_summary("https://www.mezzino.com/property/town-hall-camberwell/")
page_summary("https://host-students.com/property/centurion-house/")
page_summary("https://studyinn.com/student-accommodation/nottingham/talbot-street/")
page_summary("https://www.downingstudents.com/student-accommodation/london/")
page_summary("https://abodusstudents.com/accommodation/hope-street-liverpool")

client.close()
print("\nDONE")
