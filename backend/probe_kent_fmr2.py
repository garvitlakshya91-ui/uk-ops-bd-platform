"""Kent find-my-room: card contents + raw name contexts."""
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
html = r.text
soup = BeautifulSoup(html, "html.parser")
cards = soup.select('[class*="card"]')
print("cards:", len(cards))
seen = set()
for c in cards:
    cls = " ".join(c.get("class", []))
    t = re.sub(r"\s+", " ", c.get_text(" ", strip=True))[:140]
    key = t[:60]
    if key in seen or not t:
        continue
    seen.add(key)
    print(f"[{cls[:40]}] {t}")
    if len(seen) > 45:
        break

print("\nraw contexts for 'Turing College':")
for m in list(re.finditer(r"Turing College", html))[:6]:
    print("  ...", html[max(0, m.start() - 120):m.end() + 60].replace("\n", " ")[:220])
