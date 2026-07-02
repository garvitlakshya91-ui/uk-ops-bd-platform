"""Probe candidate operator sites: robots.txt + sitemap.xml discovery."""
import httpx, re, sys, time

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

SITES = [
    "https://www.thisisfresh.com",
    "https://wearehomesforstudents.com",
    "https://www.sanctuary-students.com",
    "https://www.clvuk.com",
    "https://www.dandaraliving.com",
    "https://www.getliving.com",
    "https://www.quintainliving.com",
    "https://www.essentialliving.co.uk",
    "https://www.urbanbubble.co.uk",
    "https://www.wayoflife.com",
    "https://uncle.co.uk",
    "https://www.nidoliving.com",
    "https://www.scape.com",
    "https://www.urbanest.co.uk",
    "https://www.truestudent.com",
]

client = httpx.Client(timeout=25, headers={"User-Agent": UA, "Accept-Language": "en-GB,en;q=0.9"}, follow_redirects=True)

for base in SITES:
    print(f"\n===== {base} =====")
    for path in ("/robots.txt", "/sitemap.xml", "/sitemap_index.xml"):
        url = base + path
        try:
            r = client.get(url)
            body = r.text
            info = f"{r.status_code} ({len(body)}b) final={r.url}"
            print(f"  {path:<20} {info}")
            if path == "/robots.txt" and r.status_code == 200:
                sm = re.findall(r"(?i)^sitemap:\s*(\S+)", body, re.M)
                dis = re.findall(r"(?i)^disallow:\s*(\S+)", body, re.M)
                print(f"    sitemaps in robots: {sm[:5]}")
                print(f"    disallow (first 8): {dis[:8]}")
            elif path != "/robots.txt" and r.status_code == 200 and "<" in body[:200]:
                locs = re.findall(r"<loc>\s*([^<\s]+?)\s*</loc>", body)
                print(f"    locs={len(locs)} first5={locs[:5]}")
        except Exception as e:
            print(f"  {path:<20} ERR {type(e).__name__}: {str(e)[:100]}")
        time.sleep(1.1)
