"""Google Places API integration for CSAT/review data on operating schemes.

Uses the Google Places API "Find Place" + "Place Details" endpoints to look
up a scheme by name+address and capture:
- rating (0.0-5.0)
- user_ratings_total (review count)
- place_id (cached for re-lookup)

Pricing (Q2 2026):
- Find Place from Text: ~$17/1000  (with text+rating sku)
- Place Details (basic): ~$17/1000
- $200/month free credit covers ~12k lookups/month at no cost.

Quotas:
- Default 600 QPS, but be polite — we target ~5 RPS.

Usage::

    from app.enrichment.google_places import GooglePlacesClient
    client = GooglePlacesClient()
    result = client.lookup_scheme("Square Gardens", "Manchester, M3")
    # -> {'rating': 4.1, 'user_ratings_total': 87, 'place_id': 'ChIJ...'}
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

GOOGLE_PLACES_BASE = "https://maps.googleapis.com/maps/api/place"
DEFAULT_TIMEOUT = 10.0
MIN_REQUEST_INTERVAL = 0.2  # 5 RPS polite


class GooglePlacesClient:
    """Minimal Google Places client for scheme CSAT enrichment.

    Reads ``GOOGLE_PLACES_API_KEY`` from environment. The key must have
    Places API (legacy) enabled in Google Cloud Console.
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("GOOGLE_PLACES_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "GOOGLE_PLACES_API_KEY not set. Get one at "
                "https://console.cloud.google.com/apis/credentials and add to .env"
            )
        self._client = httpx.Client(timeout=DEFAULT_TIMEOUT)
        self._last_request = 0.0

    def __enter__(self) -> "GooglePlacesClient":
        return self

    def __exit__(self, *exc) -> None:
        self._client.close()

    def _throttle(self) -> None:
        now = time.monotonic()
        delta = now - self._last_request
        if delta < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - delta)
        self._last_request = time.monotonic()

    # ------------------------------------------------------------------
    # Find place by text
    # ------------------------------------------------------------------

    def find_place(self, query: str) -> Optional[dict]:
        """Return the top Place match for the given free-text query.

        Returns a dict with at least: place_id, name, formatted_address,
        rating, user_ratings_total (when available). Returns None if no
        match found or on error.
        """
        if not query or len(query.strip()) < 3:
            return None
        self._throttle()
        params = {
            "input": query.strip(),
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,rating,user_ratings_total,types",
            "key": self.api_key,
        }
        try:
            r = self._client.get(f"{GOOGLE_PLACES_BASE}/findplacefromtext/json", params=params)
        except Exception as exc:
            logger.warning("gplaces_find_error", query=query[:60], error=str(exc)[:120])
            return None
        if r.status_code != 200:
            logger.warning("gplaces_find_http_error", status=r.status_code, query=query[:60])
            return None
        data = r.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            logger.warning("gplaces_find_api_status", status=data.get("status"), query=query[:60])
            return None
        candidates = data.get("candidates", []) or []
        if not candidates:
            return None
        return candidates[0]

    # ------------------------------------------------------------------
    # Place details (when find_place didn't return rating directly)
    # ------------------------------------------------------------------

    def place_details(self, place_id: str) -> Optional[dict]:
        """Fetch additional details for a Place ID."""
        if not place_id:
            return None
        self._throttle()
        params = {
            "place_id": place_id,
            "fields": "rating,user_ratings_total,formatted_address,types,name",
            "key": self.api_key,
        }
        try:
            r = self._client.get(f"{GOOGLE_PLACES_BASE}/details/json", params=params)
        except Exception as exc:
            logger.warning("gplaces_details_error", place_id=place_id, error=str(exc)[:120])
            return None
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("status") != "OK":
            return None
        return data.get("result")

    # ------------------------------------------------------------------
    # Scheme lookup convenience method
    # ------------------------------------------------------------------

    def lookup_scheme(
        self,
        scheme_name: str,
        address_or_city: Optional[str] = None,
    ) -> Optional[dict]:
        """Look up a UK scheme. Returns a dict with rating/review_count/place_id
        or None if not found.

        Query strategy:
        1. Try "<scheme_name>, <address>" (most specific)
        2. Fall back to "<scheme_name>" alone if no result
        """
        if not scheme_name:
            return None

        queries = []
        if address_or_city:
            queries.append(f"{scheme_name}, {address_or_city}")
        queries.append(scheme_name)

        for q in queries:
            candidate = self.find_place(q)
            if not candidate:
                continue
            place_id = candidate.get("place_id")
            rating = candidate.get("rating")
            review_count = candidate.get("user_ratings_total")

            # If find_place didn't give us rating, fetch details
            if rating is None and place_id:
                details = self.place_details(place_id)
                if details:
                    rating = details.get("rating")
                    review_count = details.get("user_ratings_total")

            if place_id:
                return {
                    "place_id": place_id,
                    "name": candidate.get("name"),
                    "formatted_address": candidate.get("formatted_address"),
                    "rating": rating,
                    "user_ratings_total": review_count,
                    "types": candidate.get("types", []),
                    "checked_at": datetime.now(timezone.utc),
                }
        return None
