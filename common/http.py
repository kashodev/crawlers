"""Polite, retrying HTTP client.

Three guarantees the crawlers rely on:
  1. No request is issued to a path robots.txt disallows.
  2. Requests to a single host are spaced by at least the effective delay
     (config floor, or robots Crawl-delay if longer) and capped in concurrency.
  3. Transient failures retry with exponential backoff, honouring Retry-After.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass

import httpx

from common.config import PolitenessConfig, RetryConfig
from common.robots import RobotsPolicy
from utils.urls import host_of

log = logging.getLogger(__name__)


class RobotsDisallowed(Exception):
    """Raised when robots.txt forbids the URL. Never retried."""


class FetchError(Exception):
    """Raised when a request fails after exhausting retries."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass
class Response:
    url: str
    status_code: int
    content: bytes
    headers: dict
    elapsed_seconds: float

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "").split(";")[0].strip().lower()


class _HostRateLimiter:
    """Serializes and paces requests per host."""

    def __init__(self, min_delay: float, jitter: float, max_concurrent: int):
        self.min_delay = min_delay
        self.jitter = jitter
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self._slots = threading.Semaphore(max_concurrent)

    def _lock_for(self, host: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(host, threading.Lock())

    def wait(self, host: str, delay_override: float | None = None) -> None:
        delay = max(self.min_delay, delay_override or 0.0)
        with self._lock_for(host):
            last = self._last_request.get(host)
            if last is not None:
                # Jitter avoids a lockstep request cadence against the origin.
                elapsed = time.monotonic() - last
                remaining = delay + random.uniform(0, self.jitter) - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_request[host] = time.monotonic()

    def acquire(self):
        return self._slots


class PoliteClient:
    def __init__(self, politeness: PolitenessConfig, retry: RetryConfig):
        self.politeness = politeness
        self.retry = retry
        self.robots = RobotsPolicy(politeness.user_agent, politeness.respect_robots)
        self._limiter = _HostRateLimiter(
            politeness.min_delay_seconds,
            politeness.jitter_seconds,
            politeness.max_concurrent_requests,
        )
        self._client = httpx.Client(
            headers={
                "User-Agent": politeness.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-CA,en;q=0.9",
            },
            timeout=retry.timeout_seconds,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=politeness.max_concurrent_requests),
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _effective_delay(self, url: str) -> float | None:
        declared = self.robots.crawl_delay(url)
        if declared is None:
            return None
        # Never let a site's declared delay be silently ignored, but don't let a
        # pathological value stall the job forever either.
        return min(declared, self.politeness.max_delay_seconds)

    def _sleep_for_retry(self, attempt: int, retry_after: str | None) -> None:
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = self.retry.backoff_base_seconds**attempt
        else:
            delay = self.retry.backoff_base_seconds**attempt
        delay = min(delay, self.retry.backoff_max_seconds)
        delay += random.uniform(0, self.politeness.jitter_seconds)
        log.info("Backing off before retry", extra={"attempt": attempt, "sleepSeconds": delay})
        time.sleep(delay)

    def request(
        self,
        method: str,
        url: str,
        *,
        timeout: float | None = None,
        allow_status: tuple[int, ...] = (),
    ) -> Response:
        """Issue a request with robots enforcement, pacing, and retries.

        `allow_status` lists non-2xx codes the caller handles itself (e.g. 404
        during existence probing), returned rather than raised.
        """
        if not self.robots.can_fetch(url):
            raise RobotsDisallowed(f"robots.txt disallows {url}")

        host = host_of(url)
        delay = self._effective_delay(url)
        last_error: Exception | None = None

        for attempt in range(1, self.retry.max_attempts + 1):
            with self._limiter.acquire():
                self._limiter.wait(host, delay)
                started = time.monotonic()
                try:
                    resp = self._client.request(
                        method, url, timeout=timeout or self.retry.timeout_seconds
                    )
                except httpx.HTTPError as exc:
                    last_error = exc
                    log.warning(
                        "Request error",
                        extra={
                            "url": url,
                            "attempt": attempt,
                            "error": str(exc),
                            "errorType": type(exc).__name__,
                        },
                    )
                    if attempt < self.retry.max_attempts:
                        self._sleep_for_retry(attempt, None)
                    continue

            elapsed = time.monotonic() - started

            if resp.status_code in allow_status or resp.status_code < 400:
                return Response(
                    url=str(resp.url),
                    status_code=resp.status_code,
                    content=resp.content,
                    headers=dict(resp.headers),
                    elapsed_seconds=elapsed,
                )

            if resp.status_code in self.retry.retry_status_codes:
                last_error = FetchError(f"HTTP {resp.status_code} for {url}", resp.status_code)
                log.warning(
                    "Retryable HTTP status",
                    extra={"url": url, "status": resp.status_code, "attempt": attempt},
                )
                if attempt < self.retry.max_attempts:
                    self._sleep_for_retry(attempt, resp.headers.get("retry-after"))
                continue

            # 401/403/404 and friends: retrying will not change the answer.
            raise FetchError(f"HTTP {resp.status_code} for {url}", resp.status_code)

        raise FetchError(
            f"exhausted {self.retry.max_attempts} attempts for {url}: {last_error}",
            getattr(last_error, "status_code", None),
        )

    def get(self, url: str, **kwargs) -> Response:
        return self.request("GET", url, **kwargs)

    def head(self, url: str, **kwargs) -> Response:
        return self.request("HEAD", url, **kwargs)

    def exists(self, url: str) -> tuple[bool, dict]:
        """HEAD-probe a URL. Returns (exists, headers).

        Used for cheap enumeration: a HEAD transfers no body, so probing a large
        keyspace for PDFs costs the origin almost nothing.
        """
        try:
            resp = self.head(url, allow_status=(403, 404, 410))
        except (FetchError, RobotsDisallowed) as exc:
            log.debug("Existence probe failed", extra={"url": url, "error": str(exc)})
            return False, {}
        if resp.status_code >= 400:
            return False, resp.headers
        return True, resp.headers

    def download(self, url: str, max_bytes: int) -> Response:
        """Fetch a document body, refusing anything over max_bytes."""
        resp = self.request("GET", url, timeout=self.retry.download_timeout_seconds)
        if len(resp.content) > max_bytes:
            raise FetchError(f"body {len(resp.content)}B exceeds cap {max_bytes}B for {url}")
        return resp
