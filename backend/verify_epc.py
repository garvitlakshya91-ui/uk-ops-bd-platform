"""Check the energy-certificate register answers with the configured bearer token.

    python verify_epc.py            # looks up B7 4EH
    python verify_epc.py "M1 1AD"
"""
import asyncio, os, sys
os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/uk_ops_bd")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.scrapers.epc_client import EPCApiError, EPCClient, is_new_dwelling_candidate

async def test(postcode):
    async with EPCClient() as client:
        rows = await client.search_domestic(postcode=postcode)
        print(f"  search: {len(rows)} certificates at {postcode}")
        if not rows:
            return
        print(f"    newest lodged {rows[0]['lodgement-date']}, schema {rows[0]['schema-type']}")
        candidates = [r for r in rows if is_new_dwelling_candidate(r)]
        print(f"    {len(candidates)} lodged under a SAP schema (possible new dwellings)")
        cert = await client.get_certificate((candidates or rows)[0]["certificate-number"])
        if cert:
            print(f"  certificate: transaction type '{cert['transaction-type']}', "
                  f"property type '{cert['property-type']}', floor area {cert['total-floor-area']}")

try:
    asyncio.run(test(sys.argv[1] if len(sys.argv) > 1 else "B7 4EH"))
    print("  OK")
except EPCApiError as exc:
    raise SystemExit(f"  FAILED - {exc}")
