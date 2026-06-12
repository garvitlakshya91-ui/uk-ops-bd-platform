"""Inspect Kent find-my-room page structure for hall names."""
import re

import httpx
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

r = httpx.get(
    "https://www.kent.ac.uk/accommodation/canterbury/find-my-room",
    headers={"User-Agent": UA}, follow_redirects=True, timeout=30,
)
print("status", r.status_code, "len", len(r.text))
soup = BeautifulSoup(r.text, "html.parser")
for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
    t = tag.get_text(" ", strip=True)
    if t and len(t) < 90:
        print(f"<{tag.name}> {t}")
# look at card-like structures
for sel in ['[class*="card"]', '[class*="accordion"]', '[class*="tab"]']:
    els = soup.select(sel)
    print(f"sel {sel}: {len(els)}")
# alt texts mentioning halls
alts = sorted(set(
    img.get("alt", "") for img in soup.find_all("img")
    if img.get("alt") and len(img["alt"]) < 70
))
for a in alts[:40]:
    print("alt:", a)
# strong/b labels
labels = sorted(set(
    el.get_text(" ", strip=True)
    for el in soup.find_all(["strong", "b", "caption", "summary", "button"])
    if 3 < len(el.get_text(strip=True)) < 50
))
for l in labels[:50]:
    print("label:", l)
