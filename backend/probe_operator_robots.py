"""Probe robots.txt + sitemap availability for 15 PBSA operator sites."""
import sys
import time

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DOMAINS = [
    "www.freshstudentliving.co.uk",
    "www.vitastudent.com",
    "yugo.com",
    "www.homesforstudents.co.uk",
    "www.crm-students.com",
    "www.prestigestudentliving.com",
    "www.collegiate-ac.com",
    "www.studentroost.co.uk",
    "www.iqstudentaccommodation.com",
    "www.hellostudent.co.uk",
    "www.mezzino.co.uk",
    "www.host-students.com",
    "www.studyinn.com",
    "www.downingstudents.com",
    "www.abodusstudents.com",
]

client = httpx.Client(timeout=25, headers={"User-Agent": UA}, follow_redirects=True)

for d in DOMAINS:
    print(f"\n===== {d} =====")
    try:
        r = client.get(f"https://{d}/robots.txt")
        print(f"robots status={r.status_code} final_url={r.url}")
        if r.status_code == 200:
            text = r.text[:3000]
            print(text)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {str(e)[:200]}")
    time.sleep(1.0)
client.close()
