"""Lancaster features accommodation page: all headings."""
import re

import httpx
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

r = httpx.get(
    "https://features.lancaster-university.uk/accommodation/",
    headers={"User-Agent": UA}, follow_redirects=True, timeout=30,
)
print("status", r.status_code, "len", len(r.text))
soup = BeautifulSoup(r.text, "html.parser")
for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
    t = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
    if t and len(t) < 90:
        print(f"<{tag.name}> {t}")
# college names mentioned anywhere?
text = soup.get_text(" ", strip=True)
for c in ["Bowland", "Cartmel", "County", "Furness", "Fylde", "Grizedale",
          "Lonsdale", "Pendle", "Graduate College", "Chancellor"]:
    print(c, text.count(c))
