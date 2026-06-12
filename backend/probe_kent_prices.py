"""Kent accommodation prices page: residence tables?"""
import re
import time

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

client = httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=30)
for url in [
    "https://www.kent.ac.uk/accommodation/canterbury/prices",
    "https://www.kent.ac.uk/accommodation/canterbury",
]:
    r = client.get(url)
    print(f"\n==== {url} -> {r.status_code} len={len(r.text)}")
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")
    print("tables:", len(tables))
    for tb in tables[:6]:
        rows = tb.find_all("tr")
        print(" table rows:", len(rows))
        for row in rows[:14]:
            cells = [c.get_text(" ", strip=True)[:30] for c in row.find_all(["td", "th"])]
            if cells:
                print("   ", cells)
    for tag in soup.find_all(["h2", "h3", "h4"]):
        t = tag.get_text(" ", strip=True)
        if t and len(t) < 80:
            print(f"  <{tag.name}> {t}")
    # known hall-name scan
    names = ["Turing", "Keynes", "Eliot", "Rutherford", "Darwin", "Becket",
             "Tyler Court", "Park Wood", "Woolf", "Houses", "Farmhouse"]
    text = soup.get_text(" ", strip=True)
    for n in names:
        c = text.count(n)
        if c:
            print(f"  name {n!r}: {c}")
    time.sleep(10)
client.close()
