"""Test EPC API with the scraper's auth format."""
import asyncio, os, sys, base64
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import httpx
from app.config import settings

async def test():
    key = settings.EPC_API_KEY
    print(f"EPC key: {key[:10]}...")

    # Format 1: key: (what scraper does)
    token1 = base64.b64encode(f"{key}:".encode()).decode()
    # Format 2: email:key (common EPC format)
    token2 = base64.b64encode(f":{key}".encode()).decode()

    async with httpx.AsyncClient() as client:
        for label, token in [("key:", token1), (":key", token2), ("raw", key)]:
            resp = await client.get(
                "https://epc.opendatacommunities.org/api/v1/domestic/search",
                params={"postcode": "SW1A 1AA", "size": 1},
                headers={"Authorization": f"Basic {token}", "Accept": "application/json"},
                timeout=10,
            )
            print(f"  Format '{label}': status={resp.status_code}")
            if resp.status_code == 200:
                print(f"    WORKS! Got {len(resp.json().get('rows', []))} rows")
                return

        print("\n  All formats failed. EPC key may be invalid or needs registration email.")
        print("  EPC API requires: Authorization: Basic base64(email:apikey)")
        print("  Set EPC_API_KEY=youremail@example.com:yourapikey in .env")

asyncio.run(test())
