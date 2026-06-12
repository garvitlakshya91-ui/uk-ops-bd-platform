"""Probe robots.txt + accommodation index discovery for university sites."""
import re
import time

import httpx

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

SITES = {
    "www.kent.ac.uk": ["/accommodation", "/student-life/accommodation"],
    "www.canterbury.ac.uk": ["/accommodation", "/study-here/accommodation", "/student-life/accommodation"],
    "www.lincoln.ac.uk": ["/accommodation", "/accommodation/", "/home/studywithus/accommodation/"],
    "www.bgu.ac.uk": ["/accommodation", "/student-life/accommodation", "/study/student-life/accommodation"],
    "www.chester.ac.uk": ["/accommodation", "/student-life/accommodation", "/study/accommodation"],
    "www.worcester.ac.uk": ["/accommodation", "/life/accommodation", "/study/student-life/accommodation"],
    "www.winchester.ac.uk": ["/accommodation", "/student-life/accommodation", "/study/accommodation"],
    "www.lancaster.ac.uk": ["/accommodation", "/study/accommodation", "/student-and-education-services/accommodation"],
    "www.durham.ac.uk": ["/accommodation", "/study/accommodation", "/colleges-and-student-experience/colleges/"],
    "www.bangor.ac.uk": ["/accommodation", "/student-life/accommodation", "/halls"],
    "www.aber.ac.uk": ["/en/accommodation/", "/accommodation"],
    "www.york.ac.uk": ["/study/accommodation/", "/accommodation"],
    "www.yorksj.ac.uk": ["/accommodation", "/student-life/accommodation", "/campus-and-city/accommodation"],
    "www.tees.ac.uk": ["/accommodation", "/sections/accommodation", "/sections/stud/accommodation.cfm"],
}

ACCOM_LINK = re.compile(
    r'href="([^"]*(?:accommodat|halls|residence|college)[^"]*)"', re.I
)


def robots_star_block(text: str) -> list[str]:
    lines = text.splitlines()
    out, active = [], False
    for line in lines:
        ls = line.strip()
        low = ls.lower()
        if low.startswith("user-agent:"):
            active = low.split(":", 1)[1].strip() == "*"
            continue
        if active and low.startswith(("disallow", "allow", "crawl-delay")):
            out.append(ls)
    return out


def main():
    client = httpx.Client(
        headers={"User-Agent": UA}, follow_redirects=True, timeout=20
    )
    for domain, paths in SITES.items():
        print(f"\n########## {domain}")
        try:
            r = client.get(f"https://{domain}/robots.txt")
            rules = robots_star_block(r.text) if r.status_code == 200 else []
            print(f"robots[{r.status_code}] star-block rules: {len(rules)}")
            for rule in rules[:40]:
                print("   " + rule)
        except Exception as e:
            print(f"robots ERROR {type(e).__name__}: {str(e)[:100]}")
        time.sleep(1)
        # homepage accommodation links
        try:
            r = client.get(f"https://{domain}/")
            links = sorted(set(ACCOM_LINK.findall(r.text)))[:15]
            print(f"home[{r.status_code}] accom-ish links:")
            for l in links:
                print("   " + l[:120])
        except Exception as e:
            print(f"home ERROR {type(e).__name__}: {str(e)[:100]}")
        time.sleep(1)
        for p in paths:
            try:
                r = client.get(f"https://{domain}{p}")
                print(f"GET {p} -> {r.status_code} final={r.url} len={len(r.text)}")
            except Exception as e:
                print(f"GET {p} ERROR {type(e).__name__}: {str(e)[:100]}")
            time.sleep(1)
    client.close()


if __name__ == "__main__":
    main()
