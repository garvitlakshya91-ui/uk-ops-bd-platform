"""Fourth probe: raw-HTML link patterns for kent/cccu/lancaster, worc.ac.uk,
bangor our-halls."""
import re
import time

import httpx

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


print("===== kent raw hall slugs (ug page)")
r = get("https://www.kent.ac.uk/accommodation/canterbury/undergraduate-accommodation")
if r:
    html = r.text.replace("\\u002F", "/").replace("\\/", "/")
    slugs = sorted(set(re.findall(r"/accommodation/(?:canterbury|medway)/[a-z0-9\-]+", html)))
    for s in slugs:
        print("  ", s)

print("\n===== cccu raw urls (unescaped)")
r = get("https://www.canterbury.ac.uk/study-here/student-life/accommodation")
if r:
    html = r.text.replace("\\u002F", "/").replace("\\/", "/")
    slugs = sorted(set(re.findall(r"(?:https://www\.canterbury\.ac\.uk)?(/[a-z0-9\-/]*accommodation[a-z0-9\-/]*)", html)))
    for s in slugs[:50]:
        print("  ", s)

print("\n===== worc.ac.uk accommodation pages")
for u in [
    "https://www.worc.ac.uk/life/accommodation/home.aspx",
    "https://www.worc.ac.uk/life/accommodation/living-in-halls/home.aspx",
]:
    r = get(u)
    if r and r.status_code == 200:
        html = r.text
        links = sorted(set(re.findall(r'href="([^"]+)"', html)))
        hits = [l for l in links if re.search(r"halls|accommodation", l, re.I)]
        for h in hits[:40]:
            print("  ", h[:130])

print("\n===== bangor our-halls")
r = get("https://www.bangor.ac.uk/accommodation/our-halls")
if r:
    html = r.text
    slugs = sorted(set(re.findall(r"/accommodation/halls/[a-z0-9\-]+/[a-z0-9\-]+", html)))
    for s in slugs[:60]:
        print("  ", s)
    # also any village link patterns
    v = sorted(set(re.findall(r"/(?:ffriddoedd-village|accommodation/st-marys-village)", html)))
    print("  villages:", v)

print("\n===== lancaster colleges raw")
r = get("https://www.lancaster.ac.uk/about-us/colleges/")
if r:
    html = r.text
    slugs = sorted(set(re.findall(r"lancaster\.ac\.uk/([a-z\-]+)/", html)))
    print("  ", [s for s in slugs if s not in (
        "about-us", "study", "accommodation", "schools-and-colleges")][:60])

client.close()
