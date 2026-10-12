"""
Client for the Energy Performance of Buildings Data API (England and Wales).

This service replaced epc.opendatacommunities.org, which now redirects every
request to the new site. The two differ in ways the rest of the code relied
on, so the differences are handled here and callers keep reading the
hyphenated keys the old API returned (``lodgement-date``, ``address1`` ...).

* Auth is a bearer token from a GOV.UK One Login account
  (``settings.EPC_BEARER_TOKEN``), not Basic auth.
* A domestic search row is a summary: address, postcode, council, energy
  band, registration date and schema. Transaction type, property type, floor
  area and the numeric score are only on the full certificate, which costs
  one request per certificate.
* There is no transaction-type filter, so "new dwelling" is decided here
  (``is_new_dwelling_candidate`` and ``EPCClient.search_new_dwellings``).
* Councils are filtered by name ("Birmingham"), not ONS code.
* Paging is by page number, up to 5,000 rows a page. A search that matches
  nothing is a 404, not an empty list.

Spec: https://raw.githubusercontent.com/communitiesuk/epb-data-warehouse/main/api/api.yml
Docs: https://get-energy-performance-data.communities.gov.uk/api-technical-documentation
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import date, timedelta
from typing import Any, Callable, Iterable

import httpx
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

EPC_API_BASE = "https://api.get-energy-performance-data.communities.gov.uk"
EPC_ACCOUNT_URL = "https://get-energy-performance-data.communities.gov.uk"

MAX_PAGE_SIZE = 5000
NEW_DWELLING = "new dwelling"

# Transaction-type codes on the full certificate. Codes 1-11 mean the same in
# every SAP and RdSAP schema; from 12 up the meaning depends on the schema, so
# those are reported by number rather than guessed.
TRANSACTION_TYPES = {
    1: "marketed sale",
    2: "non marketed sale",
    3: "rental (social)",
    4: "rental (private)",
    5: "none of the above",
    6: NEW_DWELLING,
    7: "not recorded",
    8: "rental",
    9: "assessment for green deal",
    10: "following green deal",
    11: "FiT application",
}
PROPERTY_TYPES = {0: "House", 1: "Bungalow", 2: "Flat", 3: "Maisonette", 4: "Park home"}
BUILT_FORMS = {
    1: "Detached",
    2: "Semi-Detached",
    3: "End-Terrace",
    4: "Mid-Terrace",
    5: "Enclosed End-Terrace",
    6: "Enclosed Mid-Terrace",
}


class EPCApiError(RuntimeError):
    """The register did not answer the question (outage, moved, bad response)."""


class EPCAuthError(EPCApiError):
    """No bearer token is configured, or the register rejected it."""


class EPCQueryError(EPCApiError):
    """The register refused the query itself, for example a malformed postcode."""


# ---------------------------------------------------------------------------
# Field mapping: new API -> the keys the old API used
# ---------------------------------------------------------------------------

def _label(codes: dict[int, str], value: Any) -> str:
    """The old API's wording for a coded value; unknown codes stay visible as 'code N'."""
    if value is None or value == "":
        return ""
    try:
        return codes.get(int(value)) or f"code {value}"
    except (TypeError, ValueError):
        return str(value)


def _number(value: Any) -> float | None:
    if isinstance(value, dict):
        value = value.get("value")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _address_keys(lines: Iterable[Any]) -> dict[str, str]:
    """address1..3 as the old API had them, with every line joined in ``address``."""
    clean = [str(line).strip() if line else "" for line in lines]
    first_three = (clean + ["", "", ""])[:3]
    return {
        "address1": first_three[0],
        "address2": first_three[1],
        "address3": first_three[2],
        "address": ", ".join(line for line in clean if line),
    }


def _postcode(value: Any) -> str:
    return str(value or "").replace("+", " ").strip().upper()


def search_row_to_legacy(row: dict[str, Any]) -> dict[str, Any]:
    """A domestic search row under the keys the old API used.

    The old ``lmk-key`` has no equivalent; the certificate number is the
    identifier now. ``local-authority`` (the ONS code) is no longer supplied.
    """
    return {
        "certificate-number": row.get("certificateNumber") or "",
        **_address_keys(row.get(f"addressLine{n}") for n in (1, 2, 3, 4)),
        "postcode": _postcode(row.get("postcode")),
        "posttown": row.get("postTown") or "",
        "local-authority-label": row.get("council") or "",
        "constituency-label": row.get("constituency") or "",
        "current-energy-rating": row.get("currentEnergyEfficiencyBand") or "",
        "lodgement-date": str(row.get("registrationDate") or "")[:10],
        "uprn": row.get("uprn"),
        "schema-type": row.get("schemaType") or "",
    }


def certificate_to_legacy(doc: dict[str, Any], certificate_number: str = "") -> dict[str, Any]:
    """A full certificate (``/api/certificate``) under the keys the old API used."""
    return {
        "certificate-number": certificate_number,
        **_address_keys(doc.get(f"address_line_{n}") for n in (1, 2, 3, 4)),
        "postcode": _postcode(doc.get("postcode")),
        "posttown": doc.get("post_town") or "",
        "local-authority-label": doc.get("council") or "",
        "constituency-label": doc.get("constituency") or "",
        "current-energy-rating": doc.get("current_energy_efficiency_band") or "",
        "current-energy-efficiency": doc.get("energy_rating_current"),
        "potential-energy-rating": doc.get("potential_energy_efficiency_band") or "",
        "potential-energy-efficiency": doc.get("energy_rating_potential"),
        "lodgement-date": str(doc.get("registration_date") or "")[:10],
        "inspection-date": str(doc.get("inspection_date") or "")[:10],
        "transaction-type": _label(TRANSACTION_TYPES, doc.get("transaction_type")),
        "property-type": _label(PROPERTY_TYPES, doc.get("property_type")),
        "built-form": _label(BUILT_FORMS, doc.get("built_form")),
        "total-floor-area": _number(doc.get("total_floor_area")),
        "uprn": doc.get("uprn"),
        "schema-type": doc.get("schema_type") or "",
        "assessment-type": doc.get("assessment_type") or "",
    }


def is_new_dwelling_candidate(row: dict[str, Any]) -> bool:
    """Whether a search row could be a new-dwelling certificate.

    New homes are assessed with full SAP, lodged under a ``SAP-Schema-*``
    schema. ``RdSAP-Schema-*`` is the existing-dwelling method. Before schema
    17 both methods were lodged under ``SAP-Schema-*``, and full SAP is
    sometimes used for an existing home, so a SAP schema is necessary but not
    enough: the transaction type on the full certificate decides.
    """
    return str(row.get("schema-type") or "").upper().startswith("SAP-")


def is_new_dwelling(row: dict[str, Any]) -> bool:
    return str(row.get("transaction-type") or "").lower().startswith(NEW_DWELLING)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class EPCClient:
    """
    Async client for the domestic certificate search and certificate fetch.

    Failures raise. Only the register's own "nothing found" answer comes back
    as an empty result, so an outage can never be mistaken for a postcode
    with no certificates.
    """

    def __init__(
        self,
        token: str | None = None,
        *,
        requests_per_second: float | None = 10.0,
        max_retries: int = 4,
        retry_wait: float = 15.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.log = logger.bind(client="EPCClient")
        token = settings.EPC_BEARER_TOKEN if token is None else token
        self._token = token.strip().removeprefix("Bearer ").strip()
        # The register allows 6,000 requests per 5 minutes per IP (20 a second).
        self._interval = 1.0 / requests_per_second if requests_per_second else 0.0
        self._next_at = 0.0
        self._pace_lock = asyncio.Lock()
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self._transport = transport
        self._http: httpx.AsyncClient | None = None
        self.requests_made = 0

    async def __aenter__(self) -> "EPCClient":
        if not self._token:
            raise EPCAuthError(
                "EPC_BEARER_TOKEN is not set. Sign in at "
                f"{EPC_ACCOUNT_URL} with GOV.UK One Login, copy the bearer "
                "token from the account page and add it to backend/.env."
            )
        self._http = httpx.AsyncClient(
            base_url=EPC_API_BASE,
            timeout=httpx.Timeout(60.0, connect=15.0),
            # A redirect means the API has moved again: fail loudly instead of
            # parsing whatever page it leads to.
            follow_redirects=False,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "User-Agent": "UK-Ops-BD-Platform/1.0",
            },
            transport=self._transport,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _pace(self) -> None:
        if not self._interval:
            return
        async with self._pace_lock:
            now = time.monotonic()
            wait = self._next_at - now
            self._next_at = max(now, self._next_at) + self._interval
        if wait > 0:
            await asyncio.sleep(wait)

    @staticmethod
    def _error_text(resp: httpx.Response) -> str:
        """The message in the register's JSON error body, or '' if it is not one.

        The live service wraps it as {"data": {"error": ...}}; its OpenAPI
        file shows it unwrapped. Both are read.
        """
        try:
            body = resp.json()
        except ValueError:
            return ""
        if isinstance(body, dict) and isinstance(body.get("data"), dict):
            body = body["data"]
        if not isinstance(body, dict):
            return ""
        if isinstance(body.get("error"), str):
            return body["error"]
        errors = body.get("errors")
        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
            return str(errors[0].get("title") or errors[0].get("code") or "")
        return ""

    async def _get(self, path: str, params: Any) -> dict[str, Any] | None:
        """One GET. None means the register has nothing for the query (its 404)."""
        if not self._http:
            raise RuntimeError("Client not initialised — use async with")

        for attempt in range(1, self.max_retries + 1):
            last = attempt == self.max_retries
            await self._pace()
            self.requests_made += 1
            try:
                resp = await self._http.get(path, params=params)
            except httpx.TransportError as exc:
                if last:
                    raise EPCApiError(f"{path}: {type(exc).__name__}: {exc}") from exc
                await asyncio.sleep(self.retry_wait * attempt)
                continue

            status = resp.status_code
            message = self._error_text(resp)
            if status == 200:
                try:
                    body = resp.json()
                except ValueError as exc:
                    raise EPCApiError(f"{path}: the response was not JSON") from exc
                if not isinstance(body, dict):
                    raise EPCApiError(f"{path}: unexpected response shape")
                return body
            if status == 404 and message:
                return None
            if status in (401, 403):
                raise EPCAuthError(
                    f"The energy-certificate register rejected the bearer token "
                    f"(HTTP {status}{': ' + message if message else ''}). "
                    "Check EPC_BEARER_TOKEN in backend/.env."
                )
            if status == 400:
                raise EPCQueryError(f"{path}: {message or 'HTTP 400'}")
            if (status == 429 or status >= 500) and not last:
                retry_after = resp.headers.get("Retry-After", "")
                wait = float(retry_after) if retry_after.isdigit() else self.retry_wait * attempt
                self.log.warning("epc_retry", path=path, status=status, wait_seconds=wait)
                await asyncio.sleep(wait)
                continue
            moved = resp.headers.get("Location")
            raise EPCApiError(
                f"{path}: HTTP {status}"
                + (f" redirecting to {moved}" if moved else "")
                + (f": {message}" if message else "")
            )
        raise EPCApiError(f"{path}: gave up after {self.max_retries} attempts")

    # ------------------------------------------------------------------
    # Domestic certificate search
    # ------------------------------------------------------------------

    async def search_domestic(
        self,
        *,
        postcode: str | None = None,
        council: str | Iterable[str] | None = None,
        address: str | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Domestic certificates matching the filters, newest first, under the
        old API's keys (see ``search_row_to_legacy``).

        ``council`` is one name or several, spelled as the register lists
        them. A date range needs ``date_start``; the register refuses a range
        that reaches today, so the end is capped at yesterday.
        """
        params: list[tuple[str, Any]] = []
        if postcode:
            params.append(("postcode", _postcode(postcode)))
        if address:
            params.append(("address", address.strip()))
        for name in [council] if isinstance(council, str) else list(council or []):
            params.append(("council[]", name))
        if date_end and not date_start:
            raise ValueError("date_end needs date_start")
        if date_start:
            yesterday = date.today() - timedelta(days=1)
            end = min(date_end or yesterday, yesterday)
            if date_start > end:
                return []
            params += [("date_start", date_start.isoformat()), ("date_end", end.isoformat())]
        if not params:
            raise ValueError("search_domestic needs at least one filter")

        rows: list[dict[str, Any]] = []
        page = 1
        page_size = min(max_results or MAX_PAGE_SIZE, MAX_PAGE_SIZE)
        while True:
            body = await self._get(
                "/api/domestic/search",
                params + [("current_page", page), ("page_size", page_size)],
            )
            if body is None:
                break
            data = body.get("data")
            if not isinstance(data, list):
                raise EPCApiError("/api/domestic/search: no data list in the response")
            rows.extend(search_row_to_legacy(r) for r in data)
            if max_results and len(rows) >= max_results:
                return rows[:max_results]
            next_page = (body.get("pagination") or {}).get("nextPage")
            if not next_page:
                break
            page = int(next_page)
        return rows

    async def get_certificate(self, certificate_number: str) -> dict[str, Any] | None:
        """The full certificate under the old API's keys, or None if it is not on the register."""
        body = await self._get("/api/certificate", {"certificate_number": certificate_number})
        if body is None:
            return None
        doc = body.get("data")
        if not isinstance(doc, dict):
            raise EPCApiError("/api/certificate: no data object in the response")
        return certificate_to_legacy(doc, certificate_number)

    async def with_details(
        self,
        rows: list[dict[str, Any]],
        only: Callable[[dict[str, Any]], bool] = is_new_dwelling_candidate,
        concurrency: int = 8,
    ) -> list[dict[str, Any]]:
        """
        Fill in what a search row lacks (transaction type, property type,
        floor area, score) from the full certificate.

        One request per row that passes ``only``; other rows come back as
        they are, as does a row whose certificate has since been withdrawn.
        """
        gate = asyncio.Semaphore(concurrency)

        async def one(row: dict[str, Any]) -> dict[str, Any]:
            number = row.get("certificate-number")
            if not number or not only(row):
                return row
            async with gate:
                detail = await self.get_certificate(number)
            if detail is None:
                return row
            filled = dict(row)
            for key, value in detail.items():
                if value not in (None, "") and row.get(key) in (None, ""):
                    filled[key] = value
            return filled

        out: list[dict[str, Any]] = []
        for at in range(0, len(rows), 1000):
            tasks = [asyncio.ensure_future(one(row)) for row in rows[at:at + 1000]]
            try:
                out += await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    task.cancel()
                raise
        return out

    @staticmethod
    def _windows(
        date_start: date | None, date_end: date | None, days: int,
    ) -> list[tuple[date | None, date | None]]:
        """The date range cut into windows; one open window if there is no range.

        Neighbouring windows share their boundary day, because the register
        cuts the last day of a range at noon.
        """
        if date_start is None:
            return [(None, date_end)]
        yesterday = date.today() - timedelta(days=1)
        last = min(date_end or yesterday, yesterday)
        windows: list[tuple[date | None, date | None]] = []
        start = date_start
        while start <= last:
            end = min(start + timedelta(days=days), last)
            windows.append((start, end))
            if end >= last:
                break
            start = end
        return windows

    async def search_new_dwellings(
        self,
        *,
        postcode: str | None = None,
        council: str | Iterable[str] | None = None,
        date_start: date | None = None,
        date_end: date | None = None,
        min_per_postcode: int = 1,
        window_days: int = 7,
    ) -> list[dict[str, Any]]:
        """
        New-dwelling certificates matching the filters.

        The search cannot filter by transaction type, so every certificate
        lodged under a SAP schema is fetched in full and kept only when its
        transaction type is "new dwelling". ``min_per_postcode`` drops
        postcodes with fewer candidates than that before any are fetched, for
        callers that only want blocks.

        A date range is searched ``window_days`` at a time and only the
        candidates are kept, because a year of all certificates nationwide is
        well over a million rows.
        """
        rows_read = 0
        numbers: set[str] = set()
        candidates: list[dict[str, Any]] = []
        for start, end in self._windows(date_start, date_end, window_days):
            found = await self.search_domestic(
                postcode=postcode, council=council, date_start=start, date_end=end,
            )
            rows_read += len(found)
            for row in found:
                if is_new_dwelling_candidate(row) and row["certificate-number"] not in numbers:
                    numbers.add(row["certificate-number"])
                    candidates.append(row)
        if min_per_postcode > 1:
            per_postcode = Counter(r["postcode"] for r in candidates)
            candidates = [r for r in candidates if per_postcode[r["postcode"]] >= min_per_postcode]
        detailed = await self.with_details(candidates)
        new = [r for r in detailed if is_new_dwelling(r)]
        self.log.info(
            "epc_new_dwellings_searched",
            rows_read=rows_read,
            candidates=len(candidates),
            new_dwellings=len(new),
        )
        return new
