"""Check Lancaster college microsite URLs."""
import time

import httpx

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
client = httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=30)
for slug in ["bowland", "cartmel", "county", "furness", "fylde", "grizedale",
             "lonsdale", "pendle", "graduate-college"]:
    url = f"https://www.lancaster.ac.uk/{slug}/"
    try:
        r = client.get(url)
        title = ""
        m = r.text.find("<title>")
        if m >= 0:
            title = r.text[m + 7:r.text.find("</title>", m)][:60]
        print(f"{url} -> {r.status_code} final={r.url} title={title!r}")
    except Exception as e:
        print(f"{url} ERROR {type(e).__name__}: {str(e)[:80]}")
    time.sleep(5)
client.close()
