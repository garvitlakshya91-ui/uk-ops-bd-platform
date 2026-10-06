"""Headless-Chromium fetcher for sources that block plain HTTP clients.

Several sources (StuRents detail pages, some operator brands) sit behind
bot protection that 403s httpx but serves a real browser. This fetcher
keeps one Chromium alive across calls and returns rendered HTML.

The Chromium executable defaults to Playwright's own install; set
PW_CHROMIUM (e.g. /opt/pw-browsers/chromium) where the pinned Playwright
build is not present. The proxy CA must be trusted in the NSS store
(~/.pki/nssdb) — see deploy/DEPLOY.md.
"""
from __future__ import annotations

import os
import time
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)

_EXECUTABLE_CANDIDATES = [os.environ.get("PW_CHROMIUM"), None,
                          "/opt/pw-browsers/chromium"]


class BrowserFetcher:
    """Rendered-page fetcher with throttling. Use as a context manager."""

    def __init__(self, request_interval_sec: float = 1.0, timeout_ms: int = 45000):
        self.request_interval_sec = request_interval_sec
        self.timeout_ms = timeout_ms
        self._pw = self._browser = self._page = None
        self._last_req = 0.0

    def _ensure_started(self):
        if self._page is not None:
            return
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        last_exc = None
        for executable in _EXECUTABLE_CANDIDATES:
            try:
                kwargs = {"executable_path": executable} if executable else {}
                self._browser = self._pw.chromium.launch(**kwargs)
                break
            except Exception as exc:
                last_exc = exc
        if self._browser is None:
            self._pw.stop()
            self._pw = None
            raise last_exc
        self._page = self._browser.new_page()

    def get(self, url: str) -> Optional[str]:
        """Fetch a URL; return HTML on 200, None otherwise."""
        self._ensure_started()
        delta = time.monotonic() - self._last_req
        if delta < self.request_interval_sec:
            time.sleep(self.request_interval_sec - delta)
        self._last_req = time.monotonic()
        try:
            r = self._page.goto(url, timeout=self.timeout_ms,
                                wait_until="domcontentloaded")
        except Exception as exc:
            logger.warning("browser_fetch_error", url=url, error=str(exc)[:120])
            return None
        if r is None or r.status != 200:
            logger.warning("browser_fetch_http", url=url,
                           status=r.status if r else None)
            return None
        return self._page.content()

    def close(self):
        for obj in (self._browser, self._pw):
            try:
                if obj is self._browser and obj:
                    obj.close()
                elif obj:
                    obj.stop()
            except Exception:
                pass
        self._pw = self._browser = self._page = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
