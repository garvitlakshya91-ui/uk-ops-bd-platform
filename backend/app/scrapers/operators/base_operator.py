"""Shared base for per-operator rent scrapers.

Each operator subclass implements ``fetch_all()`` which returns a list of
RentRecord dicts. The base class handles HTTP, retries, and matching back
to ``existing_schemes`` rows for persistence.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class RentRecord:
    """A single rent row to persist into ``scheme_rents``."""
    scheme_name: str                    # operator's name for the property
    address: str = ""
    postcode: str = ""
    council_hint: str = ""              # e.g. "Manchester"
    room_type: str = ""
    rent_per_week: Optional[float] = None
    rent_per_month: Optional[float] = None
    rent_min_per_week: Optional[float] = None
    rent_max_per_week: Optional[float] = None
    academic_year: Optional[str] = None
    contract_length_weeks: Optional[int] = None
    operator_name: str = ""              # canonical operator brand
    source_url: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


class BaseOperatorScraper:
    """Base class — subclass and implement ``fetch_all()``."""

    operator_name: str = "Unknown"
    base_url: str = ""
    default_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    request_interval_sec: float = 1.0   # polite rate

    def __init__(self, *, proxy_url: Optional[str] = None, timeout: float = 30.0):
        self.proxy_url = proxy_url
        self._last_req = 0.0
        client_kwargs: dict[str, Any] = {
            "timeout": timeout,
            "headers": {"User-Agent": self.default_user_agent},
            "follow_redirects": True,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        self.client = httpx.Client(**client_kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.client.close()

    def _throttle(self):
        now = time.monotonic()
        delta = now - self._last_req
        if delta < self.request_interval_sec:
            time.sleep(self.request_interval_sec - delta)
        self._last_req = time.monotonic()

    def get(self, url: str) -> Optional[str]:
        self._throttle()
        try:
            r = self.client.get(url)
        except Exception as e:
            logger.warning("operator_fetch_error", url=url, error=str(e)[:120], op=self.operator_name)
            return None
        if r.status_code != 200:
            logger.warning("operator_fetch_http", url=url, status=r.status_code, op=self.operator_name)
            return None
        return r.text

    # ------------------------------------------------------------------
    # Subclass entry point
    # ------------------------------------------------------------------

    def fetch_all(self) -> Iterable[RentRecord]:  # pragma: no cover
        raise NotImplementedError
