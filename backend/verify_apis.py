"""Verify all external API keys are working."""
import os, sys, asyncio
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx

async def test_all():
    from app.config import settings

    print("=" * 60)
    print("API KEY VERIFICATION")
    print("=" * 60)

    # 1. Companies House
    print("\n1. Companies House API...")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.company-information.service.gov.uk/search/companies?q=Swan+Housing",
                auth=(settings.COMPANIES_HOUSE_API_KEY, ""),
                timeout=10,
            )
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                print(f"   OK - Found {len(items)} results for 'Swan Housing'")
            else:
                print(f"   FAILED - Status {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"   FAILED - {e}")

    # 2. Apollo.io
    print("\n2. Apollo.io API...")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.apollo.io/api/v1/auth/health",
                headers={"X-Api-Key": settings.APOLLO_API_KEY},
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"   OK - Auth healthy")
            else:
                # Try people search endpoint as health check
                resp2 = await client.post(
                    "https://api.apollo.io/v1/mixed_people/search",
                    headers={
                        "Content-Type": "application/json",
                        "X-Api-Key": settings.APOLLO_API_KEY,
                    },
                    json={"q_organization_name": "test", "page": 1, "per_page": 1},
                    timeout=10,
                )
                if resp2.status_code in (200, 422):
                    print(f"   OK - API responding (status {resp2.status_code})")
                else:
                    print(f"   WARN - Status {resp2.status_code}: {resp2.text[:100]}")
    except Exception as e:
        print(f"   FAILED - {e}")

    # 3. Hunter.io
    print("\n3. Hunter.io API...")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.hunter.io/v2/account",
                params={"api_key": settings.HUNTER_API_KEY},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                plan = data.get("plan_name", "?")
                requests_left = data.get("requests", {}).get("searches", {}).get("available", "?")
                print(f"   OK - Plan: {plan}, Searches available: {requests_left}")
            else:
                print(f"   FAILED - Status {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"   FAILED - {e}")

    # 4. EPC Open Data Communities
    print("\n4. EPC Open Data Communities API...")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://epc.opendatacommunities.org/api/v1/domestic/search",
                params={"postcode": "SW1A 1AA", "size": 1},
                headers={
                    "Authorization": f"Basic {settings.EPC_API_KEY}",
                    "Accept": "application/json",
                },
                timeout=10,
            )
            if resp.status_code == 200:
                rows = resp.json().get("rows", [])
                print(f"   OK - Found {len(rows)} EPC records for SW1A 1AA")
            elif resp.status_code == 401:
                # Try with token as bearer
                resp2 = await client.get(
                    "https://epc.opendatacommunities.org/api/v1/domestic/search",
                    params={"postcode": "SW1A 1AA", "size": 1},
                    headers={
                        "Authorization": f"Bearer {settings.EPC_API_KEY}",
                        "Accept": "application/json",
                    },
                    timeout=10,
                )
                if resp2.status_code == 200:
                    print(f"   OK - Bearer auth works")
                else:
                    print(f"   WARN - Status {resp.status_code}/{resp2.status_code} - may need email:key base64 auth")
            else:
                print(f"   WARN - Status {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"   FAILED - {e}")

    print("\n" + "=" * 60)

asyncio.run(test_all())
