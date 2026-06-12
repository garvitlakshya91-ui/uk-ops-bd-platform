"""Round 4: targeted address/price extraction checks per brand."""
import json
import re
import time

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
PC_SPACED = re.compile(r"\b[A-Z]{1,2}[0-9][0-9A-Z]?\s+[0-9][A-Z]{2}\b")
LOC_PAT = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")

client = httpx.Client(timeout=40, headers={"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"}, follow_redirects=True)


def fetch(url):
    time.sleep(1.0)
    try:
        r = client.get(url)
        return r.status_code, r.text
    except Exception as e:
        return None, f"ERR {type(e).__name__}: {e}"


def show_around(html, pattern, label, n=3, width=160):
    for i, m in enumerate(re.finditer(pattern, html)):
        if i >= n:
            break
        s = max(0, m.start() - width)
        snippet = html[s:m.end() + width].replace("\n", " ")
        print(f"    [{label}] ...{snippet}...")


print("=== wearehomesforstudents.com homepage ===")
st, html = fetch("https://wearehomesforstudents.com/")
print(f"  / -> {st} len={len(html) if html else 0}")
if st == 200:
    print("  title:", re.search(r"<title>(.*?)</title>", html, re.S).group(1)[:100] if re.search(r"<title>(.*?)</title>", html, re.S) else "?")

print("\n=== yugo property page ===")
st, html = fetch("https://yugo.com/en-gb/global/united-kingdom/birmingham/bentley-house")
print(f"  -> {st} len={len(html) if html else 0}")
if st == 200:
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        print("    JSONLD:", m.group(1).strip()[:400].replace("\n", " "))
    print("    spaced postcodes:", PC_SPACED.findall(html)[:6])
    show_around(html, r"address", "addr", 3)
    prices = re.findall(r"(?:from\s*)?£\s?[\d,.]+", html)[:10]
    print("    prices:", prices)

print("\n=== crm property page ===")
st, html = fetch("https://www.crm-students.com/cardiff/glendower-house")
print(f"  -> {st} len={len(html) if html else 0}")
if st == 200:
    for m in re.finditer(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S):
        print("    JSONLD:", m.group(1).strip()[:400].replace("\n", " "))
    print("    spaced postcodes:", PC_SPACED.findall(html)[:6])
    show_around(html, r'"address|streetAddress|addressLocality', "addr", 3)
    prices = re.findall(r"(?:from\s*)?£\s?[\d,.]+", html)[:10]
    print("    prices:", prices)

print("\n=== downing property-sitemap ===")
st, xml = fetch("https://www.downingstudents.com/property-sitemap.xml")
if st == 200:
    locs = LOC_PAT.findall(xml)
    print(f"  {len(locs)} urls")
    for u in locs[:8]:
        print("   ", u)
    props = [u for u in locs if len(re.sub(r'https?://[^/]+/', '', u).strip('/').split('/')) == 3]
    print(f"  3-seg: {len(props)}")
    if props:
        st2, html = fetch(props[0])
        print(f"  sample {props[0]} -> {st2} len={len(html)}")
        if st2 == 200:
            print("    spaced postcodes:", PC_SPACED.findall(html)[:5])
            show_around(html, PC_SPACED, "pc-context", 2)
            prices = re.findall(r"£\s?[\d,.]+\s*(?:/|per\s*)?(?:week|pw)?", html)[:8]
            print("    prices:", prices)

print("\n=== hello: address in RSC payload ===")
st, html = fetch("https://www.hellostudent.co.uk/student-accommodation/liverpool/the-octagon")
if st == 200:
    show_around(html, PC_SPACED, "pc-context", 2, width=300)
    show_around(html, r'\\"address\\"|"address"', "addr-key", 3, width=200)
    m = re.search(r"<title>(.*?)</title>", html, re.S)
    print("    title:", m.group(1)[:120] if m else "?")
    show_around(html, r'fromPrice|price_from|"price"', "price", 3, width=120)

print("\n=== mezzino: address context ===")
st, html = fetch("https://www.mezzino.com/property/town-hall-camberwell/")
if st == 200:
    show_around(html, PC_SPACED, "pc-context", 3, width=240)
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    print("    h1:", re.sub(r"<[^>]+>", "", m.group(1)).strip()[:100] if m else "?")
    show_around(html, r"£\s?[\d,.]+", "price", 4, width=100)

print("\n=== host: address/city context ===")
st, html = fetch("https://host-students.com/property/centurion-house/")
if st == 200:
    show_around(html, PC_SPACED, "pc-context", 3, width=240)
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    print("    h1:", re.sub(r"<[^>]+>", "", m.group(1)).strip()[:100] if m else "?")
    show_around(html, r"£\s?[\d,.]+\s*per week", "ppw", 4, width=100)

print("\n=== studyinn: postcode hunt ===")
st, html = fetch("https://studyinn.com/student-accommodation/nottingham/talbot-street/")
if st == 200:
    print("    spaced:", PC_SPACED.findall(html)[:5])
    show_around(html, r"(?i)address", "addr", 4, width=200)
    show_around(html, r"maps\.google|google\.com/maps|lat|data-lat", "maps", 3, width=150)

print("\n=== studyinn location page ===")
st, html = fetch("https://studyinn.com/location-sitemap.xml")
if st == 200:
    locs = LOC_PAT.findall(html)
    print(f"  {len(locs)} location urls")
    for u in locs[:12]:
        print("   ", u)

print("\n=== abodus property page (long timeout) ===")
st, html = fetch("https://abodusstudents.com/accommodation/hope-street-liverpool")
print(f"  -> {st} len={len(html) if html else 0}")
if st == 200:
    print("    spaced postcodes:", PC_SPACED.findall(html)[:5])
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    print("    h1:", re.sub(r"<[^>]+>", "", m.group(1)).strip()[:100] if m else "?")
    show_around(html, PC_SPACED, "pc-context", 2, width=200)
    show_around(html, r"£\s?[\d,.]+", "price", 4, width=80)

print("\n=== roost: postcode context ===")
st, html = fetch("https://www.studentroost.co.uk/locations/sheffield/hollis-croft")
if st == 200:
    print("    spaced:", PC_SPACED.findall(html)[:8])
    show_around(html, PC_SPACED, "pc-context", 3, width=200)

client.close()
print("\nDONE")
