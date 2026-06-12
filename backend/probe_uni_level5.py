"""Fifth probe: kent inline halls, worcester campaigns page, bangor halltabs,
lancaster accommodation-guide."""
import re
import time

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
        print(f"GET {url} -> {r.status_code} len={len(r.text)} final={r.url}")
        return r
    except Exception as e:
        print(f"GET {url} ERROR {type(e).__name__}: {str(e)[:100]}")
        return None


def headings(html, maxlen=80):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["h2", "h3", "h4"]):
        t = tag.get_text(" ", strip=True)
        if t and len(t) < maxlen:
            print(f"   <{tag.name}> {t}")


print("===== kent UG page headings")
r = get("https://www.kent.ac.uk/accommodation/canterbury/undergraduate-accommodation")
if r:
    headings(r.text)

print("\n===== kent find-my-room json check")
r = get("https://www.kent.ac.uk/accommodation/canterbury/find-my-room")
if r:
    html = r.text
    for marker in ["Turing", "Keynes", "Park Wood", "Becket", "Eliot"]:
        print(f"   marker {marker!r}: {html.count(marker)} hits")
    m = re.search(r"(Turing[^<]{0,120})", html)
    if m:
        print("   ctx:", m.group(1)[:150])

print("\n===== worcester campaigns page structure")
r = get("https://www.worcester.ac.uk/campaigns/accommodation-guide")
if r:
    headings(r.text)
    pcs = sorted(set(re.findall(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2}\b", r.text)))
    print("   postcodes:", pcs)

print("\n===== bangor halls_options")
r = get("https://www.bangor.ac.uk/accommodation/halls_options")
if r and r.status_code == 200:
    headings(r.text)
    slugs = sorted(set(re.findall(r"/accommodation/halls/[a-z0-9\-]+/[a-z0-9\-]+", r.text)))
    for s in slugs[:50]:
        print("  ", s)

print("\n===== bangor ffriddoedd raw slug scan")
r = get("https://www.bangor.ac.uk/ffriddoedd-village")
if r:
    html = r.text.replace("\\u002F", "/").replace("\\/", "/")
    slugs = sorted(set(re.findall(r"/accommodation/halls/[a-z0-9\-]+/[a-z0-9\-]+", html)))
    for s in slugs[:60]:
        print("  ", s)
    slugs2 = sorted(set(re.findall(r"/accommodation/rooms/[a-z0-9\-]+/[a-z0-9\-]+", html)))
    print("   rooms slugs:", slugs2[:30])

print("\n===== lancaster accommodation-guide")
r = get("https://www.lancaster.ac.uk/accommodation-guide/")
if r:
    html = r.text
    links = sorted(set(re.findall(r'href="([^"]+)"', html)))
    hits = [l for l in links if re.search(r"college|hall|wharf|accommodation", l, re.I)]
    for h in hits[:50]:
        print("  ", h[:130])

client.close()
