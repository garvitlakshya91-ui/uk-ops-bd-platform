"""Debug: why does Winchester west-downs lose its postcode after stripping?"""
import re

import httpx
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
PC = re.compile(r"\b[A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2}\b")

r = httpx.get(
    "https://www.winchester.ac.uk/student-life/Accommodation/west-downs-student-village/",
    headers={"User-Agent": UA}, follow_redirects=True, timeout=30,
)
soup = BeautifulSoup(r.text, "html.parser")
# find elements containing the postcode and print their ancestry classes
for el in soup.find_all(string=PC):
    chain = []
    p = el.parent
    for _ in range(8):
        if p is None or p.name == "body":
            break
        ident = p.name
        cls = " ".join(p.get("class", [])) if hasattr(p, "get") else ""
        pid = p.get("id", "") if hasattr(p, "get") else ""
        chain.append(f"{ident}[{cls}|{pid}]")
        p = p.parent
    print("TEXT:", re.sub(r"\s+", " ", str(el))[:90])
    print("   ", " < ".join(chain))
